/**
 * @file mariadb_binlog_stream.cpp
 * @brief MariaDB-specific binlog stream implementation
 *
 * Uses MYSQL_RPL in non-GTID mode (COM_BINLOG_DUMP) with MariaDB
 * session variables for GTID-based replication.
 */

#include "mysql/mariadb_binlog_stream.h"

#ifdef USE_MYSQL

#include <cstring>
#include <string>

#include "mysql/binlog_reader_internal.h"
#include "utils/structured_log.h"

// NOLINTBEGIN(cppcoreguidelines-pro-*,cppcoreguidelines-avoid-*,readability-magic-numbers)

namespace mygramdb::mysql {

/// Heartbeat period in nanoseconds (3 seconds) for MariaDB connection keepalive
static constexpr uint64_t kHeartbeatPeriodNs = 3000000000;

/// Size of the OK byte prefix in binlog protocol buffer
static constexpr size_t kBinlogOKByteSize = 1;

mygram::utils::Expected<void, mygram::utils::Error> MariaDBBinlogStream::SetupSession(Connection& conn) {
  using mygram::utils::ErrorCode;
  using mygram::utils::MakeError;
  using mygram::utils::MakeUnexpected;

  // MariaDB uses @master_binlog_checksum (not @source_binlog_checksum)
  // Query the server's checksum setting and mirror it
  if (mysql_query(conn.GetHandle(), "SET @master_binlog_checksum = @@global.binlog_checksum") != 0) {
    return MakeUnexpected(
        MakeError(ErrorCode::kMariaDBProtocolError,
                  std::string("Failed to set binlog checksum: ") + conn.GetLastError()));
  }
  mygram::utils::StructuredLog().Event("binlog_debug").Field("action", "mariadb_checksum_configured").Debug();

  // Strict GTID mode: fail if a GTID gap is detected rather than silently skipping
  if (mysql_query(conn.GetHandle(), "SET @slave_gtid_strict_mode = 1") != 0) {
    // Non-fatal: log and continue (older MariaDB versions may not support this)
    mygram::utils::StructuredLog()
        .Event("binlog_debug")
        .Field("action", "mariadb_strict_mode_failed")
        .Field("error", conn.GetLastError())
        .Debug();
  }

  // Don't skip duplicate GTIDs (safe default for single-source replication)
  if (mysql_query(conn.GetHandle(), "SET @slave_gtid_ignore_duplicates = 0") != 0) {
    mygram::utils::StructuredLog()
        .Event("binlog_debug")
        .Field("action", "mariadb_ignore_duplicates_failed")
        .Field("error", conn.GetLastError())
        .Debug();
  }

  // Configure heartbeat to keep connection alive during idle periods
  std::string heartbeat_query = "SET @master_heartbeat_period = " + std::to_string(kHeartbeatPeriodNs);
  if (mysql_query(conn.GetHandle(), heartbeat_query.c_str()) != 0) {
    mygram::utils::StructuredLog()
        .Event("binlog_debug")
        .Field("action", "mariadb_heartbeat_config_failed")
        .Field("error", conn.GetLastError())
        .Debug();
  }

  return {};
}

mygram::utils::Expected<void, mygram::utils::Error> MariaDBBinlogStream::Open(Connection& conn,
                                                                               const std::string& gtid,
                                                                               uint32_t server_id) {
  using mygram::utils::ErrorCode;
  using mygram::utils::MakeError;
  using mygram::utils::MakeUnexpected;

  // Set MariaDB GTID position via session variable BEFORE COM_BINLOG_DUMP.
  // MariaDB reads @slave_connect_state to know which GTIDs the replica already has.
  // Empty string means "start from current binlog position" (receive all new events).
  std::string gtid_query = "SET @slave_connect_state = '" + gtid + "'";
  if (mysql_query(conn.GetHandle(), gtid_query.c_str()) != 0) {
    return MakeUnexpected(MakeError(ErrorCode::kMariaDBProtocolError,
                                    std::string("Failed to set slave_connect_state: ") + conn.GetLastError()));
  }

  mygram::utils::StructuredLog()
      .Event("binlog_debug")
      .Field("action", "mariadb_gtid_state_set")
      .Field("gtid", gtid)
      .Debug();

  // Initialize MYSQL_RPL for COM_BINLOG_DUMP (non-GTID mode).
  // When MYSQL_RPL_GTID is NOT set, mysql_binlog_open() sends COM_BINLOG_DUMP
  // with position, flags, server_id, and filename — exactly what MariaDB expects.
  std::memset(&rpl_, 0, sizeof(rpl_));
  rpl_.file_name_length = 0;  // Empty filename = use current binlog
  rpl_.file_name = nullptr;
  rpl_.start_position = 4;     // Skip magic number at start of binlog
  rpl_.server_id = server_id;
  rpl_.flags = 0;              // No MYSQL_RPL_GTID — triggers COM_BINLOG_DUMP path

  // No GTID callback needed (MariaDB uses session variable, not binary-encoded GTID)
  rpl_.gtid_set_encoded_size = 0;
  rpl_.gtid_set_arg = nullptr;
  rpl_.fix_gtid_set = nullptr;

  // Open binlog stream via COM_BINLOG_DUMP
  if (mysql_binlog_open(conn.GetHandle(), &rpl_) != 0) {
    unsigned int err_no = mysql_errno(conn.GetHandle());
    std::string error_msg =
        "Failed to open MariaDB binlog stream: " + std::string(mysql_error(conn.GetHandle()));

    if (err_no == kMySQLErrBinlogPurged) {
      return MakeUnexpected(MakeError(ErrorCode::kMariaDBProtocolError,
                                      "Binlog position no longer available on MariaDB server. "
                                      "GTID position has been purged. "
                                      "Manual intervention required: run SYNC command to establish new position."));
    }

    return MakeUnexpected(MakeError(ErrorCode::kMariaDBProtocolError, error_msg));
  }

  is_open_ = true;
  return {};
}

BinlogFetchResult MariaDBBinlogStream::Fetch(Connection& conn) {
  BinlogFetchResult result;

  mygram::utils::StructuredLog().Event("binlog_debug").Field("action", "mariadb_calling_binlog_fetch").Debug();
  int rc = mysql_binlog_fetch(conn.GetHandle(), &rpl_);

  if (rc != 0) {
    unsigned int err_no = mysql_errno(conn.GetHandle());
    const char* err_str = mysql_error(conn.GetHandle());
    result.error_code = err_no;
    result.error_message =
        "Failed to fetch MariaDB binlog event: " + std::string(err_str) + " (errno: " + std::to_string(err_no) + ")";

    if (err_no == kMySQLErrServerLost) {
      result.status = BinlogFetchResult::Status::kConnectionLost;
    } else if (err_no == kMySQLErrGoneAway) {
      result.status = BinlogFetchResult::Status::kServerGoneAway;
    } else if (err_no == kMySQLErrBinlogPurged) {
      result.status = BinlogFetchResult::Status::kBinlogPurged;
    } else {
      result.status = BinlogFetchResult::Status::kError;
    }
    return result;
  }

  // Check if we have data
  if (rpl_.size == 0 || rpl_.buffer == nullptr) {
    result.status = BinlogFetchResult::Status::kNoData;
    return result;
  }

  // Strip OK byte prefix (same protocol as MySQL: buffer[0] = 0x00 OK byte)
  result.status = BinlogFetchResult::Status::kOK;
  result.event_data = rpl_.buffer + kBinlogOKByteSize;
  result.event_length = rpl_.size - kBinlogOKByteSize;
  return result;
}

void MariaDBBinlogStream::Close(Connection& conn) {
  if (is_open_ && conn.IsConnected()) {
    mysql_binlog_close(conn.GetHandle(), &rpl_);
  }
  is_open_ = false;
}

}  // namespace mygramdb::mysql

// NOLINTEND(cppcoreguidelines-pro-*,cppcoreguidelines-avoid-*,readability-magic-numbers)

#endif  // USE_MYSQL
