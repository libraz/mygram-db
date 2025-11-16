/**
 * @file connection.h
 * @brief MySQL connection wrapper with GTID support
 */

#pragma once

#ifdef USE_MYSQL

#include <mysql.h>

#include <memory>
#include <optional>
#include <string>

namespace mygramdb::mysql {

/**
 * @brief MySQL GTID representation
 */
struct GTID {
  std::string server_uuid;      // MySQL server UUID
  uint64_t transaction_id = 0;  // Transaction sequence number

  /**
   * @brief Parse GTID from string format (UUID:transaction_id)
   */
  static std::optional<GTID> Parse(const std::string& gtid_str);

  /**
   * @brief Convert GTID to string format
   */
  [[nodiscard]] std::string ToString() const;

  bool operator==(const GTID& other) const {
    return server_uuid == other.server_uuid && transaction_id == other.transaction_id;
  }

  bool operator!=(const GTID& other) const { return !(*this == other); }
};

/**
 * @brief RAII wrapper for MYSQL_RES* to prevent memory leaks
 *
 * Custom deleter for std::unique_ptr that calls mysql_free_result
 */
struct MySQLResultDeleter {
  void operator()(MYSQL_RES* res) const {
    if (res != nullptr) {
      mysql_free_result(res);
    }
  }
};

/// Type alias for RAII-managed MYSQL_RES*
using MySQLResult = std::unique_ptr<MYSQL_RES, MySQLResultDeleter>;

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
  // NOLINTBEGIN(readability-magic-numbers,cppcoreguidelines-avoid-magic-numbers) - Default MySQL
  // connection settings
  struct Config {
    std::string host = "localhost";
    uint16_t port = 3306;  // MySQL default port
    std::string user;
    std::string password;
    std::string database;
    uint32_t connect_timeout = 10;  // Default timeout in seconds
    uint32_t read_timeout = 30;     // Default timeout in seconds
    uint32_t write_timeout = 30;    // Default timeout in seconds
    // SSL/TLS settings
    bool ssl_enable = false;
    std::string ssl_ca;
    std::string ssl_cert;
    std::string ssl_key;
    bool ssl_verify_server_cert = true;
  };
  // NOLINTEND(readability-magic-numbers,cppcoreguidelines-avoid-magic-numbers)

  /**
   * @brief Construct connection (not yet connected)
   */
  explicit Connection(Config config);

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
   * @param context Optional context label for logging (e.g., "main", "binlog worker")
   * @return true if connection successful
   */
  bool Connect(const std::string& context = "");

  /**
   * @brief Check if connection is alive
   */
  [[nodiscard]] bool IsConnected() const;

  /**
   * @brief Ping MySQL server to check if connection is still alive
   * @return true if connection is alive
   */
  bool Ping();

  /**
   * @brief Reconnect to MySQL server
   * @return true if reconnection successful
   */
  bool Reconnect();

  /**
   * @brief Close connection
   */
  void Close();

  /**
   * @brief Execute SQL query
   * @param query SQL query string
   * @return RAII-managed MySQL result set (automatically freed)
   */
  MySQLResult Execute(const std::string& query);

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
   * @brief Set session GTID_NEXT for testing
   * @param gtid GTID to set (e.g., "AUTOMATIC" or specific GTID)
   * @return true if successful
   */
  bool SetGTIDNext(const std::string& gtid);

  /**
   * @brief Get server UUID
   * @return Server UUID string
   */
  std::optional<std::string> GetServerUUID();

  /**
   * @brief Check if GTID mode is enabled on the server
   * @return true if GTID_MODE is ON, false otherwise
   */
  bool IsGTIDModeEnabled();

  /**
   * @brief Get latest GTID from SHOW BINARY LOG STATUS
   *
   * Returns the latest GTID position in the binlog.
   * This is useful for starting replication from the current position.
   *
   * @return Latest GTID string, or nullopt if not available
   */
  std::optional<std::string> GetLatestGTID();

  /**
   * @brief Validate that specified column is unique in the table
   *
   * Checks if the specified column is either:
   * - A PRIMARY KEY column
   * - A UNIQUE KEY column
   *
   * @param database Database name
   * @param table Table name
   * @param column Column name to validate
   * @param error_message Output parameter for error message
   * @return true if column is unique, false otherwise
   */
  bool ValidateUniqueColumn(const std::string& database, const std::string& table, const std::string& column,
                            std::string& error_message);

  /**
   * @brief Get last error message
   */
  [[nodiscard]] const std::string& GetLastError() const { return last_error_; }

  /**
   * @brief Get connection configuration
   */
  [[nodiscard]] const Config& GetConfig() const { return config_; }

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

}  // namespace mygramdb::mysql

#endif  // USE_MYSQL
