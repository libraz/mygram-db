/**
 * @file connection.h
 * @brief MySQL connection wrapper with GTID support
 */

#pragma once

#ifdef USE_MYSQL

#include <mysql.h>
#include <string>
#include <memory>
#include <optional>

namespace mygramdb {
namespace mysql {

/**
 * @brief MySQL GTID representation
 */
struct GTID {
  std::string server_uuid;  // MySQL server UUID
  uint64_t transaction_id;  // Transaction sequence number

  /**
   * @brief Parse GTID from string format (UUID:transaction_id)
   */
  static std::optional<GTID> Parse(const std::string& gtid_str);

  /**
   * @brief Convert GTID to string format
   */
  std::string ToString() const;

  bool operator==(const GTID& other) const {
    return server_uuid == other.server_uuid &&
           transaction_id == other.transaction_id;
  }

  bool operator!=(const GTID& other) const {
    return !(*this == other);
  }
};

/**
 * @brief MySQL connection wrapper
 *
 * Provides RAII wrapper around MySQL connection with GTID support
 */
class Connection {
 public:
  /**
   * @brief Connection configuration
   */
  struct Config {
    std::string host = "localhost";
    uint16_t port = 3306;
    std::string user;
    std::string password;
    std::string database;
    uint32_t connect_timeout = 10;  // seconds
    uint32_t read_timeout = 30;     // seconds
    uint32_t write_timeout = 30;    // seconds
  };

  /**
   * @brief Construct connection (not yet connected)
   */
  explicit Connection(const Config& config);

  /**
   * @brief Destructor - closes connection if open
   */
  ~Connection();

  // Non-copyable
  Connection(const Connection&) = delete;
  Connection& operator=(const Connection&) = delete;

  // Movable
  Connection(Connection&& other) noexcept;
  Connection& operator=(Connection&& other) noexcept;

  /**
   * @brief Connect to MySQL server
   * @return true if connection successful
   */
  bool Connect();

  /**
   * @brief Check if connection is alive
   */
  bool IsConnected() const;

  /**
   * @brief Close connection
   */
  void Close();

  /**
   * @brief Execute SQL query
   * @param query SQL query string
   * @return MySQL result set (caller must free)
   */
  MYSQL_RES* Execute(const std::string& query);

  /**
   * @brief Execute SQL query without result set (INSERT/UPDATE/DELETE)
   * @param query SQL query string
   * @return true if successful
   */
  bool ExecuteUpdate(const std::string& query);

  /**
   * @brief Get current GTID executed on the server
   * @return GTID string (e.g., "uuid:1-10")
   */
  std::optional<std::string> GetExecutedGTID();

  /**
   * @brief Get purged GTID set (GTIDs no longer in binlog)
   * @return GTID string
   */
  std::optional<std::string> GetPurgedGTID();

  /**
   * @brief Get last error message
   */
  const std::string& GetLastError() const { return last_error_; }

  /**
   * @brief Get raw MYSQL handle
   */
  MYSQL* GetHandle() { return mysql_; }

 private:
  Config config_;
  MYSQL* mysql_ = nullptr;
  std::string last_error_;

  /**
   * @brief Set last error message from MySQL
   */
  void SetMySQLError();
};

}  // namespace mysql
}  // namespace mygramdb

#endif  // USE_MYSQL
