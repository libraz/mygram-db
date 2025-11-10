/**
 * @file connection.cpp
 * @brief MySQL connection wrapper implementation
 */

#include "mysql/connection.h"

#ifdef USE_MYSQL

#include <spdlog/spdlog.h>
#include <sstream>
#include <cstring>

namespace mygramdb {
namespace mysql {

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

Connection::Connection(const Config& config) : config_(config) {
  mysql_ = mysql_init(nullptr);
  if (!mysql_) {
    last_error_ = "Failed to initialize MySQL handle";
  }
}

Connection::~Connection() {
  Close();
}

Connection::Connection(Connection&& other) noexcept
    : config_(std::move(other.config_)),
      mysql_(other.mysql_),
      last_error_(std::move(other.last_error_)) {
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

bool Connection::Connect() {
  if (!mysql_) {
    last_error_ = "MySQL handle not initialized";
    return false;
  }

  // Set connection timeouts
  mysql_options(mysql_, MYSQL_OPT_CONNECT_TIMEOUT,
                &config_.connect_timeout);
  mysql_options(mysql_, MYSQL_OPT_READ_TIMEOUT, &config_.read_timeout);
  mysql_options(mysql_, MYSQL_OPT_WRITE_TIMEOUT, &config_.write_timeout);

  // Enable auto-reconnect
  bool reconnect = true;
  mysql_options(mysql_, MYSQL_OPT_RECONNECT, &reconnect);

  // Connect to MySQL
  if (!mysql_real_connect(mysql_, config_.host.c_str(), config_.user.c_str(),
                          config_.password.c_str(),
                          config_.database.empty() ? nullptr
                                                   : config_.database.c_str(),
                          config_.port, nullptr, 0)) {
    SetMySQLError();
    spdlog::error("MySQL connection failed: {}", last_error_);
    return false;
  }

  spdlog::info("Connected to MySQL at {}:{}", config_.host, config_.port);
  return true;
}

bool Connection::IsConnected() const {
  return mysql_ && mysql_ping(mysql_) == 0;
}

void Connection::Close() {
  if (mysql_) {
    mysql_close(mysql_);
    mysql_ = nullptr;
    spdlog::debug("MySQL connection closed");
  }
}

MYSQL_RES* Connection::Execute(const std::string& query) {
  if (!mysql_) {
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
  if (!result && mysql_field_count(mysql_) > 0) {
    SetMySQLError();
    spdlog::error("Failed to store result: {}", last_error_);
    return nullptr;
  }

  return result;
}

bool Connection::ExecuteUpdate(const std::string& query) {
  if (!mysql_) {
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
  if (!result) {
    return std::nullopt;
  }

  MYSQL_ROW row = mysql_fetch_row(result);
  if (!row || !row[0]) {
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
  if (!result) {
    return std::nullopt;
  }

  MYSQL_ROW row = mysql_fetch_row(result);
  if (!row || !row[0]) {
    mysql_free_result(result);
    return std::nullopt;
  }

  std::string gtid(row[0]);
  mysql_free_result(result);

  spdlog::debug("Purged GTID: {}", gtid);
  return gtid;
}

void Connection::SetMySQLError() {
  if (mysql_) {
    last_error_ = std::string(mysql_error(mysql_));
  }
}

}  // namespace mysql
}  // namespace mygramdb

#endif  // USE_MYSQL
