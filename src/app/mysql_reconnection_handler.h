/**
 * @file mysql_reconnection_handler.h
 * @brief MySQL reconnection handler for failover support
 */

#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "mysql/connection_validator.h"
#include "utils/error.h"
#include "utils/expected.h"

#ifdef USE_MYSQL
namespace mygramdb::mysql {
class Connection;
class BinlogReader;
}  // namespace mygramdb::mysql

namespace mygramdb::server::replication_pause {
class Counter;
}
namespace mygramdb::server {
class OperationCoordinator;
}
#endif

namespace mygramdb::app {

/**
 * @brief MySQL reconnection handler for failover
 *
 * Called when mysql.host or mysql.port changes via SET VARIABLE.
 * Performs graceful reconnection with minimal data loss.
 *
 * Failover flow:
 * 1. Connect and validate a candidate without touching live resources
 * 2. Stop BinlogReader and capture its drained GTID
 * 3. Revalidate the candidate against that exact hand-off position
 * 4. Replace the connection while retaining the old one for rollback
 * 5. Resume replication from the drained GTID
 * 6. Commit on success, or restore the old connection and reader on failure
 *
 * Thread Safety:
 * - Sets reconnecting_flag_ to signal the server to block SEARCH queries during reconnection
 * - Updates are queued in BinlogReader (no data loss)
 * - Reconnection typically completes within 1-5 seconds
 *
 * Error Handling:
 * - New connection fails → keep old connection (rollback)
 * - GTID mismatch → error log, manual intervention required
 * - Validation fails → error log, keep old connection
 */
class MysqlReconnectionHandler {
 public:
#ifdef USE_MYSQL
  /**
   * @brief Create reconnection handler
   * @param mysql_connection Current MySQL connection
   * @param binlog_reader Current BinlogReader
   * @param reconnecting_flag Optional flag to set during reconnection (to block manual REPLICATION START)
   */
  MysqlReconnectionHandler(mysql::Connection* mysql_connection, mysql::BinlogReader* binlog_reader,
                           std::atomic<bool>* reconnecting_flag = nullptr,
                           std::vector<mysql::ConnectionValidator::RequiredTable> required_tables = {},
                           std::atomic<bool>* dump_save_in_progress = nullptr,
                           std::atomic<bool>* replication_paused_for_dump = nullptr,
                           server::replication_pause::Counter* replication_pause_counter = nullptr,
                           server::OperationCoordinator* operation_coordinator = nullptr);
#else
  MysqlReconnectionHandler() = default;
#endif

  ~MysqlReconnectionHandler() = default;

  // Non-copyable and non-movable
  MysqlReconnectionHandler(const MysqlReconnectionHandler&) = delete;
  MysqlReconnectionHandler& operator=(const MysqlReconnectionHandler&) = delete;
  MysqlReconnectionHandler(MysqlReconnectionHandler&&) = delete;
  MysqlReconnectionHandler& operator=(MysqlReconnectionHandler&&) = delete;

  /**
   * @brief Reconnect to new MySQL host/port
   * @param new_host New MySQL host
   * @param new_port New MySQL port
   * @return Expected with void or error
   *
   * Steps:
   * The candidate is validated before live resources are disturbed and again
   * against the exact GTID captured after the reader drains. Connection and
   * reader changes are committed together or rolled back together.
   *
   * Note: This is a synchronous operation that blocks until reconnection completes.
   * Expected duration: 1-5 seconds.
   */
  mygram::utils::Expected<void, mygram::utils::Error> Reconnect(const std::string& new_host, int new_port);

 private:
#ifdef USE_MYSQL
  mysql::Connection* mysql_connection_;
  mysql::BinlogReader* binlog_reader_;
  std::atomic<bool>* reconnecting_flag_;  // Flag to set during reconnection (non-owning)
  std::vector<mysql::ConnectionValidator::RequiredTable> required_tables_;  // Tables to validate after reconnection
  std::atomic<bool>* dump_save_in_progress_;
  std::atomic<bool>* replication_paused_for_dump_;
  server::replication_pause::Counter* replication_pause_counter_;
  server::OperationCoordinator* operation_coordinator_;
  std::mutex reconnect_mutex_;
#endif

  /**
   * @brief Validate new MySQL connection for replication compatibility
   * @param connection Connection to validate
   * @return Expected with void or error
   *
   * Checks:
   * - GTID mode is enabled
   * - binlog_format is ROW
   * - binlog_row_image is FULL
   */
#ifdef USE_MYSQL
  mygram::utils::Expected<void, mygram::utils::Error> ValidateConnection(
      mysql::Connection* connection, const std::optional<std::string>& expected_uuid,
      const std::optional<std::string>& last_gtid) const;
#endif
};

}  // namespace mygramdb::app
