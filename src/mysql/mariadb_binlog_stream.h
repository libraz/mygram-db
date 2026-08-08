/**
 * @file mariadb_binlog_stream.h
 * @brief MariaDB-specific binlog stream using COM_BINLOG_DUMP
 *
 * MariaDB negotiates GTID via session variables before COM_BINLOG_DUMP:
 *   SET @slave_connect_state = 'domain-server-seq,...'
 *   SET @slave_gtid_strict_mode = 1
 *   SET @master_heartbeat_period = N
 *   SET @master_binlog_checksum = 'CRC32'
 *   COM_BINLOG_DUMP (pos=4, flags=0, server_id, filename="")
 *
 * Implementation reuses MYSQL_RPL / mysql_binlog_open() in non-GTID mode:
 * when MYSQL_RPL_GTID is NOT set, mysql_binlog_open() sends COM_BINLOG_DUMP
 * instead of COM_BINLOG_DUMP_GTID. Fetch and Close use the same API.
 */

#pragma once

#ifdef USE_MYSQL

#include <mysql.h>

#include <cstdint>
#include <string>
#include <string_view>

#include "mysql/binlog_stream.h"

namespace mygramdb::mysql {

/**
 * @brief Whether a GTID position is safe to interpolate into a session variable
 *
 * MariaDB takes the resume position through `SET @slave_connect_state = '...'`,
 * so the value reaches the server inside a quoted SQL literal. The position
 * itself comes from a dump, a SYNC or an operator command, none of which the
 * server has validated. Accepting only the characters a MariaDB GTID list can
 * contain — alphanumerics, `-`, `,` and `.` — leaves no quote, backslash,
 * semicolon or whitespace to end the literal with.
 *
 * An empty position is valid and means "no executed GTIDs".
 *
 * @param gtid Position to check
 * @return true if every character is in the accepted set
 */
[[nodiscard]] bool IsSafeMariaDBGtidPosition(std::string_view gtid);

/**
 * @brief MariaDB binlog stream implementation
 *
 * Uses MYSQL_RPL API in non-GTID mode for COM_BINLOG_DUMP.
 * MariaDB GTID negotiation happens via SET @slave_connect_state
 * before opening the stream.
 */
class MariaDBBinlogStream final : public IBinlogStream {
 public:
  MariaDBBinlogStream() = default;
  ~MariaDBBinlogStream() override = default;

  mygram::utils::Expected<void, mygram::utils::Error> SetupSession(Connection& conn) override;
  mygram::utils::Expected<void, mygram::utils::Error> Open(Connection& conn, const std::string& gtid,
                                                           uint32_t server_id) override;
  BinlogFetchResult Fetch(Connection& conn) override;
  void Close(Connection& conn) override;

 private:
  /// MYSQL_RPL structure (must persist between Open and Close)
  MYSQL_RPL rpl_{};

  /// Whether the stream is currently open
  bool is_open_ = false;
};

}  // namespace mygramdb::mysql

#endif  // USE_MYSQL
