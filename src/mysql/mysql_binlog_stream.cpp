/**
 * @file mysql_binlog_stream.cpp
 * @brief MySQL-specific binlog stream implementation
 */

#include "mysql/mysql_binlog_stream.h"

#ifdef USE_MYSQL

#include <cassert>
#include <cstring>
#include <string>

#include "mysql/binlog_reader_internal.h"
#include "mysql/gtid_encoder.h"
#include "utils/structured_log.h"

// NOLINTBEGIN(cppcoreguidelines-pro-*,cppcoreguidelines-avoid-*,readability-magic-numbers)

namespace mygramdb::mysql {

/// Heartbeat period in nanoseconds (3 seconds) to keep binlog connection alive
static constexpr uint64_t kHeartbeatPeriodNs = 3000000000;

/// Size of the OK byte prefix in MySQL binlog protocol buffer
static constexpr size_t kBinlogOKByteSize = 1;

mygram::utils::Expected<void, mygram::utils::Error> MySQLBinlogStream::SetupSession(Connection& conn) {
  using mygram::utils::ErrorCode;
  using mygram::utils::MakeError;
  using mygram::utils::MakeUnexpected;

  // Request CRC32 checksums so we can verify data integrity on each event
  if (mysql_query(conn.GetHandle(), "SET @source_binlog_checksum='CRC32'") != 0) {
    return MakeUnexpected(
        MakeError(ErrorCode::kMySQLBinlogError,
                  std::string("Failed to set binlog checksum to CRC32: ") + conn.GetLastError()));
  }
  mygram::utils::StructuredLog().Event("binlog_debug").Field("action", "checksum_crc32_enabled").Debug();

  // Configure heartbeat to keep connection alive during idle periods.
  // Non-fatal: heartbeat is optional, log and continue
  std::string heartbeat_query = "SET @master_heartbeat_period = " + std::to_string(kHeartbeatPeriodNs);
  if (mysql_query(conn.GetHandle(), heartbeat_query.c_str()) != 0) {
    mygram::utils::StructuredLog()
        .Event("binlog_debug")
        .Field("action", "heartbeat_config_failed")
        .Field("error", conn.GetLastError())
        .Debug();
  }

  return {};
}

mygram::utils::Expected<void, mygram::utils::Error> MySQLBinlogStream::Open(Connection& conn, const std::string& gtid,
                                                                             uint32_t server_id) {
  using mygram::utils::ErrorCode;
  using mygram::utils::MakeError;
  using mygram::utils::MakeUnexpected;

  // Initialize MYSQL_RPL structure for binlog reading
  std::memset(&rpl_, 0, sizeof(rpl_));
  rpl_.file_name_length = 0;  // 0 means start from current position
  rpl_.file_name = nullptr;
  rpl_.start_position = 4;      // Skip magic number at start of binlog
  rpl_.server_id = server_id;   // Use configured server ID for replica
  rpl_.flags = MYSQL_RPL_GTID;  // Use GTID mode (allow heartbeat events)

  // Encode GTID set to binary format if we have one
  if (!gtid.empty()) {
    auto encode_result = GtidEncoder::Encode(gtid);
    if (!encode_result) {
      return MakeUnexpected(
          MakeError(ErrorCode::kMySQLBinlogError, "Failed to encode GTID set: " + encode_result.error().message()));
    }
    gtid_encoded_data_ = std::move(*encode_result);

    // Use callback approach: MySQL will call our callback to encode the GTID into the packet
    rpl_.gtid_set_encoded_size = gtid_encoded_data_.size();
    rpl_.gtid_set_arg = &gtid_encoded_data_;
    rpl_.fix_gtid_set = &MySQLBinlogStream::FixGtidSetCallback;

    mygram::utils::StructuredLog()
        .Event("binlog_debug")
        .Field("action", "using_gtid_set")
        .Field("gtid", gtid)
        .Field("encoded_bytes", static_cast<uint64_t>(gtid_encoded_data_.size()))
        .Debug();
  } else {
    // Empty GTID set: receive all events from current binlog position
    rpl_.gtid_set_encoded_size = 0;
    rpl_.gtid_set_arg = nullptr;
    rpl_.fix_gtid_set = nullptr;
    mygram::utils::StructuredLog().Event("binlog_debug").Field("action", "using_empty_gtid_set").Debug();
  }

  // Open binlog stream
  if (mysql_binlog_open(conn.GetHandle(), &rpl_) != 0) {
    unsigned int err_no = mysql_errno(conn.GetHandle());
    std::string error_msg = "Failed to open binlog stream: " + std::string(mysql_error(conn.GetHandle()));

    if (err_no == kMySQLErrBinlogPurged) {
      return MakeUnexpected(MakeError(ErrorCode::kMySQLBinlogError,
                                      "Binlog position no longer available on server. "
                                      "GTID position has been purged. "
                                      "Manual intervention required: run SYNC command to establish new position."));
    }

    return MakeUnexpected(MakeError(ErrorCode::kMySQLBinlogError, error_msg));
  }

  is_open_ = true;
  return {};
}

BinlogFetchResult MySQLBinlogStream::Fetch(Connection& conn) {
  BinlogFetchResult result;

  mygram::utils::StructuredLog().Event("binlog_debug").Field("action", "calling_binlog_fetch").Debug();
  int rc = mysql_binlog_fetch(conn.GetHandle(), &rpl_);

  if (rc != 0) {
    unsigned int err_no = mysql_errno(conn.GetHandle());
    const char* err_str = mysql_error(conn.GetHandle());
    result.error_code = err_no;
    result.error_message =
        "Failed to fetch binlog event: " + std::string(err_str) + " (errno: " + std::to_string(err_no) + ")";

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

  // Strip OK byte prefix (MySQL binlog protocol: buffer[0] = 0x00 OK byte)
  result.status = BinlogFetchResult::Status::kOK;
  result.event_data = rpl_.buffer + kBinlogOKByteSize;
  result.event_length = rpl_.size - kBinlogOKByteSize;
  return result;
}

void MySQLBinlogStream::Close(Connection& conn) {
  if (is_open_ && conn.IsConnected()) {
    mysql_binlog_close(conn.GetHandle(), &rpl_);
  }
  is_open_ = false;
}

void MySQLBinlogStream::FixGtidSetCallback(MYSQL_RPL* rpl, unsigned char* packet_gtid_set) {
  auto* encoded_data = static_cast<std::vector<uint8_t>*>(rpl->gtid_set_arg);
  assert(rpl->gtid_set_encoded_size ==
         encoded_data->size());  // NOLINT(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
  std::memcpy(packet_gtid_set, encoded_data->data(), encoded_data->size());
}

}  // namespace mygramdb::mysql

// NOLINTEND(cppcoreguidelines-pro-*,cppcoreguidelines-avoid-*,readability-magic-numbers)

#endif  // USE_MYSQL
