/**
 * @file connection.cpp
 * @brief MySQL connection wrapper implementation
 */

#include "mysql/connection.h"

#ifdef USE_MYSQL

#include <spdlog/spdlog.h>

#include <cstring>
#include <sstream>
#include <utility>

// NOLINTBEGIN(cppcoreguidelines-pro-bounds-pointer-arithmetic) - UUID binary parsing

namespace mygramdb::mysql {

// GTID implementation

std::optional<GTID> GTID::Parse(const std::string& gtid_str) {
  // Format: "UUID:transaction_id" or "UUID:start-end"
  // For simplicity, we'll parse the simple format
  size_t colon_pos = gtid_str.find(':');
  if (colon_pos == std::string::npos) {
    return std::nullopt;
  }

  GTID gtid;
  gtid.server_uuid = gtid_str.substr(0, colon_pos);

  std::string txn_part = gtid_str.substr(colon_pos + 1);

  // Handle range format (UUID:1-10) - take the end value
  size_t dash_pos = txn_part.find('-');
  if (dash_pos != std::string::npos) {
    txn_part = txn_part.substr(dash_pos + 1);
  }

  try {
    gtid.transaction_id = std::stoull(txn_part);
  } catch (const std::exception& e) {
    return std::nullopt;
  }

  return gtid;
}

std::string GTID::ToString() const {
  return server_uuid + ":" + std::to_string(transaction_id);
}

// Connection implementation

Connection::Connection(Config config) : config_(std::move(config)), mysql_(mysql_init(nullptr)) {
  if (mysql_ == nullptr) {
    last_error_ = "Failed to initialize MySQL handle";
  }
}

Connection::~Connection() {
  Close();
}

Connection::Connection(Connection&& other) noexcept
    : config_(std::move(other.config_)), mysql_(other.mysql_), last_error_(std::move(other.last_error_)) {
  other.mysql_ = nullptr;
}

Connection& Connection::operator=(Connection&& other) noexcept {
  if (this != &other) {
    Close();
    config_ = std::move(other.config_);
    mysql_ = other.mysql_;
    last_error_ = std::move(other.last_error_);
    other.mysql_ = nullptr;
  }
  return *this;
}

bool Connection::Connect(const std::string& context) {
  if (mysql_ == nullptr) {
    last_error_ = "MySQL handle not initialized";
    return false;
  }

  // Set connection timeouts
  mysql_options(mysql_, MYSQL_OPT_CONNECT_TIMEOUT, &config_.connect_timeout);
  mysql_options(mysql_, MYSQL_OPT_READ_TIMEOUT, &config_.read_timeout);
  mysql_options(mysql_, MYSQL_OPT_WRITE_TIMEOUT, &config_.write_timeout);

  // Note: MYSQL_OPT_RECONNECT is deprecated and removed
  // Manual reconnection is handled via Reconnect() method when needed

  // Configure SSL/TLS if enabled
  if (config_.ssl_enable) {
    // Set SSL mode to REQUIRED
    unsigned int ssl_mode = SSL_MODE_REQUIRED;
    if (config_.ssl_verify_server_cert) {
      ssl_mode = SSL_MODE_VERIFY_CA;  // Verify CA certificate
    }
    mysql_options(mysql_, MYSQL_OPT_SSL_MODE, &ssl_mode);

    // Set SSL certificate paths if provided
    if (!config_.ssl_ca.empty()) {
      mysql_options(mysql_, MYSQL_OPT_SSL_CA, config_.ssl_ca.c_str());
    }
    if (!config_.ssl_cert.empty()) {
      mysql_options(mysql_, MYSQL_OPT_SSL_CERT, config_.ssl_cert.c_str());
    }
    if (!config_.ssl_key.empty()) {
      mysql_options(mysql_, MYSQL_OPT_SSL_KEY, config_.ssl_key.c_str());
    }

    spdlog::debug("SSL/TLS enabled for MySQL connection");
  }

  // Connect to MySQL
  if (mysql_real_connect(mysql_, config_.host.c_str(), config_.user.c_str(), config_.password.c_str(),
                         config_.database.empty() ? nullptr : config_.database.c_str(), config_.port, nullptr,
                         0) == nullptr) {
    SetMySQLError();
    std::string context_prefix = context.empty() ? "" : "[" + context + "] ";
    spdlog::error("{}MySQL connection failed: {}", context_prefix, last_error_);
    return false;
  }

  std::string context_prefix = context.empty() ? "" : "[" + context + "] ";
  std::string db_info = config_.database.empty() ? "" : "/" + config_.database;
  std::string ssl_info = config_.ssl_enable ? " (SSL/TLS)" : "";
  spdlog::info("{}Connected to MySQL {}:{}{}{}", context_prefix, config_.host, config_.port, db_info, ssl_info);
  return true;
}

bool Connection::IsConnected() const {
  if (mysql_ == nullptr) {
    return false;
  }

  // Check if connection was established (thread_id will be 0 if not connected)
  return mysql_thread_id(mysql_) != 0;
}

bool Connection::Ping() {
  if (mysql_ == nullptr) {
    last_error_ = "Not connected";
    return false;
  }

  int result = mysql_ping(mysql_);
  if (result != 0) {
    SetMySQLError();
    spdlog::warn("MySQL ping failed: {}", last_error_);
    return false;
  }

  return true;
}

bool Connection::Reconnect() {
  if (mysql_ == nullptr) {
    last_error_ = "MySQL handle not initialized";
    return false;
  }

  spdlog::info("Attempting to reconnect to MySQL {}:{}...", config_.host, config_.port);

  // Close existing connection if any
  if (mysql_ != nullptr) {
    mysql_close(mysql_);
    mysql_ = nullptr;
  }

  // Reinitialize
  mysql_ = mysql_init(nullptr);
  if (mysql_ == nullptr) {
    last_error_ = "Failed to initialize MySQL handle";
    spdlog::error("MySQL reconnection failed: {}", last_error_);
    return false;
  }

  // Reconnect
  return Connect();
}

void Connection::Close() {
  if (mysql_ != nullptr) {
    mysql_close(mysql_);
    mysql_ = nullptr;
    spdlog::debug("MySQL connection closed");
  }
}

MYSQL_RES* Connection::Execute(const std::string& query) {
  if (mysql_ == nullptr) {
    last_error_ = "Not connected";
    return nullptr;
  }

  spdlog::debug("Executing query: {}", query);

  if (mysql_query(mysql_, query.c_str()) != 0) {
    SetMySQLError();
    spdlog::error("Query failed: {}", last_error_);
    return nullptr;
  }

  MYSQL_RES* result = mysql_store_result(mysql_);
  if ((result == nullptr) && mysql_field_count(mysql_) > 0) {
    SetMySQLError();
    spdlog::error("Failed to store result: {}", last_error_);
    return nullptr;
  }

  return result;
}

bool Connection::ExecuteUpdate(const std::string& query) {
  if (mysql_ == nullptr) {
    last_error_ = "Not connected";
    return false;
  }

  spdlog::debug("Executing update: {}", query);

  if (mysql_query(mysql_, query.c_str()) != 0) {
    SetMySQLError();
    spdlog::error("Update failed: {}", last_error_);
    return false;
  }

  return true;
}

std::optional<std::string> Connection::GetExecutedGTID() {
  MYSQL_RES* result = Execute("SELECT @@GLOBAL.gtid_executed");
  if (result == nullptr) {
    return std::nullopt;
  }

  MYSQL_ROW row = mysql_fetch_row(result);
  if ((row == nullptr) || (row[0] == nullptr)) {
    mysql_free_result(result);
    return std::nullopt;
  }

  std::string gtid(row[0]);
  mysql_free_result(result);

  spdlog::debug("Executed GTID: {}", gtid);
  return gtid;
}

std::optional<std::string> Connection::GetPurgedGTID() {
  MYSQL_RES* result = Execute("SELECT @@GLOBAL.gtid_purged");
  if (result == nullptr) {
    return std::nullopt;
  }

  MYSQL_ROW row = mysql_fetch_row(result);
  if ((row == nullptr) || (row[0] == nullptr)) {
    mysql_free_result(result);
    return std::nullopt;
  }

  std::string gtid(row[0]);
  mysql_free_result(result);

  spdlog::debug("Purged GTID: {}", gtid);
  return gtid;
}

bool Connection::SetGTIDNext(const std::string& gtid) {
  std::string query = "SET GTID_NEXT = '" + gtid + "'";
  return ExecuteUpdate(query);
}

std::optional<std::string> Connection::GetServerUUID() {
  MYSQL_RES* result = Execute("SELECT @@GLOBAL.server_uuid");
  if (result == nullptr) {
    return std::nullopt;
  }

  MYSQL_ROW row = mysql_fetch_row(result);
  if ((row == nullptr) || (row[0] == nullptr)) {
    mysql_free_result(result);
    return std::nullopt;
  }

  std::string uuid(row[0]);
  mysql_free_result(result);

  spdlog::debug("Server UUID: {}", uuid);
  return uuid;
}

bool Connection::IsGTIDModeEnabled() {
  MYSQL_RES* result = Execute("SELECT @@GLOBAL.gtid_mode");
  if (result == nullptr) {
    spdlog::warn("Failed to query GTID mode");
    return false;
  }

  MYSQL_ROW row = mysql_fetch_row(result);
  if ((row == nullptr) || (row[0] == nullptr)) {
    mysql_free_result(result);
    return false;
  }

  std::string mode(row[0]);
  mysql_free_result(result);

  spdlog::debug("GTID mode: {}", mode);

  // GTID mode can be ON, OFF, ON_PERMISSIVE, or OFF_PERMISSIVE
  // For replication, we need it to be ON
  return mode == "ON";
}

std::optional<std::string> Connection::GetLatestGTID() {
  // Try new syntax first (MySQL 8.0.23+)
  MYSQL_RES* result = Execute("SHOW BINARY LOG STATUS");

  // Fallback to old syntax for MySQL 5.7 / 8.0 < 8.0.23
  if (result == nullptr) {
    spdlog::debug("SHOW BINARY LOG STATUS failed, trying SHOW MASTER STATUS");
    result = Execute("SHOW MASTER STATUS");
  }

  if (result == nullptr) {
    spdlog::error("Failed to execute SHOW BINARY LOG STATUS / SHOW MASTER STATUS");
    return std::nullopt;
  }

  // Get field names
  unsigned int num_fields = mysql_num_fields(result);
  MYSQL_FIELD* fields = mysql_fetch_fields(result);

  // Find the Executed_Gtid_Set column index
  int gtid_column_index = -1;
  for (unsigned int i = 0; i < num_fields; i++) {
    if (std::string(fields[i].name) == "Executed_Gtid_Set") {
      gtid_column_index = static_cast<int>(i);
      break;
    }
  }

  if (gtid_column_index == -1) {
    spdlog::warn("Executed_Gtid_Set column not found in SHOW BINARY LOG STATUS");
    mysql_free_result(result);
    return std::nullopt;
  }

  // Fetch the row
  MYSQL_ROW row = mysql_fetch_row(result);
  if ((row == nullptr) || (row[gtid_column_index] == nullptr)) {
    mysql_free_result(result);
    return std::nullopt;
  }

  std::string gtid_set(row[gtid_column_index]);
  mysql_free_result(result);

  // The GTID set may be empty or contain multiple ranges
  // For simplicity, we'll return the entire set as-is
  // Example format: "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5"
  if (gtid_set.empty()) {
    spdlog::warn("Executed_Gtid_Set is empty");
    return std::nullopt;
  }

  spdlog::info("Latest GTID from binary log: {}", gtid_set);
  return gtid_set;
}

bool Connection::ValidateUniqueColumn(const std::string& database, const std::string& table, const std::string& column,
                                      std::string& error_message) {
  // Query to check if the column is part of a PRIMARY KEY or UNIQUE KEY
  std::string query = "SELECT COUNT(*) FROM information_schema.KEY_COLUMN_USAGE "
                      "WHERE TABLE_SCHEMA = '" +
                      database + "' AND TABLE_NAME = '" + table + "' AND COLUMN_NAME = '" + column +
                      "' AND (CONSTRAINT_NAME = 'PRIMARY' OR CONSTRAINT_NAME IN "
                      "(SELECT CONSTRAINT_NAME FROM information_schema.TABLE_CONSTRAINTS "
                      "WHERE TABLE_SCHEMA = '" +
                      database + "' AND TABLE_NAME = '" + table +
                      "' AND CONSTRAINT_TYPE = 'UNIQUE' AND CONSTRAINT_NAME IN "
                      "(SELECT CONSTRAINT_NAME FROM information_schema.KEY_COLUMN_USAGE "
                      "WHERE TABLE_SCHEMA = '" +
                      database + "' AND TABLE_NAME = '" + table +
                      "' GROUP BY CONSTRAINT_NAME HAVING COUNT(*) = 1)))";

  MYSQL_RES* result = Execute(query);
  if (result == nullptr) {
    error_message = "Failed to query table schema: " + GetLastError();
    return false;
  }

  MYSQL_ROW row = mysql_fetch_row(result);
  if ((row == nullptr) || (row[0] == nullptr)) {
    mysql_free_result(result);
    error_message = "Failed to fetch result for unique column validation";
    return false;
  }

  int count = std::stoi(row[0]);
  mysql_free_result(result);

  if (count == 0) {
    // Column is not a single-column PRIMARY KEY or UNIQUE KEY
    // Check if column exists and provide more specific error
    std::string column_check_query = "SELECT COUNT(*) FROM information_schema.COLUMNS "
                                     "WHERE TABLE_SCHEMA = '" +
                                     database + "' AND TABLE_NAME = '" + table + "' AND COLUMN_NAME = '" + column + "'";

    MYSQL_RES* col_result = Execute(column_check_query);
    if (col_result != nullptr) {
      MYSQL_ROW col_row = mysql_fetch_row(col_result);
      if ((col_row != nullptr) && (col_row[0] != nullptr) && std::stoi(col_row[0]) == 0) {
        error_message = "Column '" + column + "' does not exist in table '" + database + "." + table + "'";
        mysql_free_result(col_result);
        return false;
      }
      mysql_free_result(col_result);
    }

    // Column exists but is not unique
    error_message = "Column '" + column + "' in table '" + database + "." + table +
                    "' must be a single-column PRIMARY KEY or UNIQUE KEY. "
                    "Composite keys are not supported.";
    return false;
  }

  spdlog::info("Validated unique column: {}.{}.{}", database, table, column);
  return true;
}

void Connection::SetMySQLError() {
  if (mysql_ != nullptr) {
    last_error_ = std::string(mysql_error(mysql_));
  }
}

}  // namespace mygramdb::mysql

// NOLINTEND(cppcoreguidelines-pro-bounds-pointer-arithmetic)

#endif  // USE_MYSQL
