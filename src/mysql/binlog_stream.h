/**
 * @file binlog_stream.h
 * @brief Abstract interface for binlog stream protocol
 *
 * Strategy pattern: IBinlogStream abstracts the low-level binlog protocol
 * differences between MySQL (COM_BINLOG_DUMP_GTID via MYSQL_RPL) and
 * MariaDB (COM_BINLOG_DUMP via simple_command).
 *
 * The higher-level BinlogReader orchestrates reconnection, event parsing,
 * and queue management - those are shared between MySQL and MariaDB.
 */

#pragma once

#ifdef USE_MYSQL

#include <cstdint>
#include <string>

#include "mysql/connection.h"
#include "utils/error.h"
#include "utils/expected.h"

namespace mygramdb::mysql {

/**
 * @brief Result of a binlog fetch operation
 */
struct BinlogFetchResult {
  enum class Status : uint8_t {
    kOK,              ///< Event data available (event_data/event_length valid)
    kNoData,          ///< No data (heartbeat, keepalive, or EOF)
    kConnectionLost,  ///< Read timeout / CR_SERVER_LOST (2013)
    kServerGoneAway,  ///< CR_SERVER_GONE_ERROR (2006)
    kBinlogPurged,    ///< GTID position purged from binlog (1236)
    kError,           ///< Other non-recoverable error
  };

  Status status = Status::kNoData;

  /// Pointer to event data (excludes OK byte prefix).
  /// Points into the stream's internal buffer; valid until next Fetch() call.
  const unsigned char* event_data = nullptr;
  size_t event_length = 0;

  unsigned int error_code = 0;
  std::string error_message;
};

/**
 * @brief Abstract interface for binlog streaming protocol
 *
 * Encapsulates the MySQL/MariaDB-specific binlog protocol:
 * - Session setup (checksum, heartbeat configuration)
 * - Stream open (GTID negotiation, dump command)
 * - Event fetching (packet reading, OK byte handling)
 * - Stream close
 *
 * BinlogReader owns the Connection; the stream borrows it via reference.
 * Connection may be reconnected between Close() and the next SetupSession()/Open().
 */
class IBinlogStream {
 public:
  virtual ~IBinlogStream() = default;

  // Non-copyable, non-movable (polymorphic base)
  IBinlogStream(const IBinlogStream&) = delete;
  IBinlogStream& operator=(const IBinlogStream&) = delete;
  IBinlogStream(IBinlogStream&&) = delete;
  IBinlogStream& operator=(IBinlogStream&&) = delete;

  /**
   * @brief Configure session variables for binlog replication
   *
   * For MySQL: SET @source_binlog_checksum='CRC32', SET @master_heartbeat_period
   * For MariaDB: SET @slave_connect_state, SET @slave_gtid_strict_mode, etc.
   *
   * @param conn Connection to configure
   * @return Success or error
   */
  virtual mygram::utils::Expected<void, mygram::utils::Error> SetupSession(Connection& conn) = 0;

  /**
   * @brief Open binlog stream from given GTID position
   *
   * For MySQL: Encodes GTID binary, initializes MYSQL_RPL, calls mysql_binlog_open()
   * For MariaDB: Sends COM_BINLOG_DUMP packet
   *
   * @param conn Connection to use
   * @param gtid GTID position to start from (empty = current position)
   * @param server_id Replica server ID
   * @return Success or error (includes binlog purged as specific error)
   */
  virtual mygram::utils::Expected<void, mygram::utils::Error> Open(Connection& conn, const std::string& gtid,
                                                                    uint32_t server_id) = 0;

  /**
   * @brief Fetch next binlog event
   *
   * Blocks until an event is available, an error occurs, or the connection times out.
   * On kOK, event_data points to the event (header + body + checksum) with the
   * protocol OK byte already stripped. The pointer is valid until the next Fetch() call.
   *
   * @param conn Connection to read from
   * @return Fetch result with status and optional event data
   */
  virtual BinlogFetchResult Fetch(Connection& conn) = 0;

  /**
   * @brief Close the binlog stream
   *
   * Safe to call even if Open() was not called or if the connection is already closed.
   *
   * @param conn Connection to close stream on
   */
  virtual void Close(Connection& conn) = 0;

 protected:
  IBinlogStream() = default;
};

}  // namespace mygramdb::mysql

#endif  // USE_MYSQL
