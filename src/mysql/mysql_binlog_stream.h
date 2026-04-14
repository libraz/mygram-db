/**
 * @file mysql_binlog_stream.h
 * @brief MySQL-specific binlog stream using MYSQL_RPL / COM_BINLOG_DUMP_GTID
 */

#pragma once

#ifdef USE_MYSQL

#include <mysql.h>

#include <cstdint>
#include <string>
#include <vector>

#include "mysql/binlog_stream.h"

namespace mygramdb::mysql {

/**
 * @brief MySQL binlog stream implementation
 *
 * Uses the MySQL C client library's MYSQL_RPL API:
 * - mysql_binlog_open() for COM_BINLOG_DUMP_GTID
 * - mysql_binlog_fetch() for event reading
 * - mysql_binlog_close() for cleanup
 *
 * GTID encoding uses GtidEncoder to produce binary format for the dump packet.
 */
class MySQLBinlogStream final : public IBinlogStream {
 public:
  MySQLBinlogStream() = default;
  ~MySQLBinlogStream() override = default;

  mygram::utils::Expected<void, mygram::utils::Error> SetupSession(Connection& conn) override;
  mygram::utils::Expected<void, mygram::utils::Error> Open(Connection& conn, const std::string& gtid,
                                                           uint32_t server_id) override;
  BinlogFetchResult Fetch(Connection& conn) override;
  void Close(Connection& conn) override;

 private:
  /// MYSQL_RPL structure (must persist between Open and Close)
  MYSQL_RPL rpl_{};

  /// Encoded GTID data (must persist during mysql_binlog_open callback)
  std::vector<uint8_t> gtid_encoded_data_;

  /// Whether the stream is currently open
  bool is_open_ = false;

  /**
   * @brief Static callback for MySQL binlog API to encode GTID set
   *
   * Copies pre-encoded GTID data into the COM_BINLOG_DUMP_GTID packet.
   * Called by mysql_binlog_open() during packet construction.
   */
  static void FixGtidSetCallback(MYSQL_RPL* rpl, unsigned char* packet_gtid_set);
};

}  // namespace mygramdb::mysql

#endif  // USE_MYSQL
