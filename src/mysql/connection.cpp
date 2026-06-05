/**
 * @file connection.cpp
 * @brief MySQL connection wrapper implementation
 */

#include "mysql/connection.h"

#ifdef USE_MYSQL

#include <spdlog/spdlog.h>

#include <algorithm>
#include <charconv>
#include <cstring>
#include <sstream>
#include <utility>

#include "mysql/gtid_encoder.h"
#include "utils/numeric_parse.h"
#include "utils/structured_log.h"

// NOLINTBEGIN(cppcoreguidelines-pro-bounds-pointer-arithmetic) - UUID binary parsing

namespace mygramdb::mysql {

// GTID implementation

mygram::utils::Expected<GTID, mygram::utils::Error> GTID::Parse(const std::string& gtid_str) {
  using mygram::utils::Error;
  using mygram::utils::ErrorCode;
  using mygram::utils::MakeError;
  using mygram::utils::MakeUnexpected;

  // Format: "UUID:transaction_id" or "UUID:start-end"
  std::string uuid = GtidEncoder::ExtractUuid(gtid_str);
  if (uuid.empty()) {
    return MakeUnexpected(
        MakeError(ErrorCode::kMySQLInvalidGTID, "Invalid GTID format: missing colon separator", gtid_str));
  }

  GTID gtid;
  gtid.server_uuid = uuid;

  size_t colon_pos = gtid_str.find(':');
  std::string txn_part = gtid_str.substr(colon_pos + 1);

  // Handle range format (UUID:1-10) - take the end value
  size_t dash_pos = txn_part.find('-');
  if (dash_pos != std::string::npos) {
    txn_part = txn_part.substr(dash_pos + 1);
  }

  uint64_t txn_id = 0;
  auto [ptr, ec] = std::from_chars(txn_part.data(), txn_part.data() + txn_part.size(), txn_id);
  if (ec != std::errc{} || ptr != txn_part.data() + txn_part.size()) {
    return MakeUnexpected(
        MakeError(ErrorCode::kMySQLInvalidGTID, "Invalid GTID format: invalid transaction ID", gtid_str));
  }
  gtid.transaction_id = txn_id;

  return gtid;
}

std::string GTID::ToString() const {
  return server_uuid + ":" + std::to_string(transaction_id);
}

// Connection implementation

Connection::Connection(Config config) : config_(std::move(config)), mysql_(mysql_init(nullptr)) {}

Connection::~Connection() {
  Close();
}

Connection::Connection(Connection&& other) noexcept
    : config_(std::move(other.config_)),
      mysql_(other.mysql_),
      flavor_(other.flavor_),
      server_version_(std::move(other.server_version_)) {
  other.mysql_ = nullptr;
}

Connection& Connection::operator=(Connection&& other) noexcept {
  if (this != &other) {
    Close();
    config_ = std::move(other.config_);
    mysql_ = other.mysql_;
    flavor_ = other.flavor_;
    server_version_ = std::move(other.server_version_);
    other.mysql_ = nullptr;
  }
  return *this;
}

mygram::utils::Expected<void, mygram::utils::Error> Connection::Connect(const std::string& context) {
  using mygram::utils::Error;
  using mygram::utils::ErrorCode;
  using mygram::utils::MakeError;
  using mygram::utils::MakeUnexpected;

  if (mysql_ == nullptr) {
    return MakeUnexpected(MakeError(ErrorCode::kMySQLConnectionFailed, "MySQL handle not initialized"));
  }

  // Enable RSA public key retrieval for caching_sha2_password without SSL.
  // Required for MySQL 8.4+ where caching_sha2_password is the default plugin
  // and mysql_native_password may be unavailable (removed in MySQL 9.x).
  bool get_pubkey = true;
  mysql_options(mysql_, MYSQL_OPT_GET_SERVER_PUBLIC_KEY, &get_pubkey);

  // Set connection timeouts (with error checking)
  if (mysql_options(mysql_, MYSQL_OPT_CONNECT_TIMEOUT, &config_.connect_timeout) != 0) {
    mygram::utils::StructuredLog()
        .Event("mysql_options_warning")
        .Field("option", "MYSQL_OPT_CONNECT_TIMEOUT")
        .Field("value", static_cast<uint64_t>(config_.connect_timeout))
        .Warn();
  }
  if (mysql_options(mysql_, MYSQL_OPT_READ_TIMEOUT, &config_.read_timeout) != 0) {
    mygram::utils::StructuredLog()
        .Event("mysql_options_warning")
        .Field("option", "MYSQL_OPT_READ_TIMEOUT")
        .Field("value", static_cast<uint64_t>(config_.read_timeout))
        .Warn();
  }
  if (mysql_options(mysql_, MYSQL_OPT_WRITE_TIMEOUT, &config_.write_timeout) != 0) {
    mygram::utils::StructuredLog()
        .Event("mysql_options_warning")
        .Field("option", "MYSQL_OPT_WRITE_TIMEOUT")
        .Field("value", static_cast<uint64_t>(config_.write_timeout))
        .Warn();
  }

  // Note: MYSQL_OPT_RECONNECT is deprecated and removed
  // Manual reconnection is handled via Reconnect() method when needed

  // Configure SSL/TLS if enabled
  if (config_.ssl_enable) {
    // Set SSL mode to REQUIRED
    unsigned int ssl_mode = SSL_MODE_REQUIRED;
    if (config_.ssl_verify_server_cert) {
      ssl_mode = SSL_MODE_VERIFY_CA;  // Verify CA certificate
    }
    if (mysql_options(mysql_, MYSQL_OPT_SSL_MODE, &ssl_mode) != 0) {
      mygram::utils::StructuredLog()
          .Event("mysql_options_warning")
          .Field("option", "MYSQL_OPT_SSL_MODE")
          .Field("value", static_cast<uint64_t>(ssl_mode))
          .Warn();
    }

    // Set SSL certificate paths if provided (with error checking)
    if (!config_.ssl_ca.empty()) {
      if (mysql_options(mysql_, MYSQL_OPT_SSL_CA, config_.ssl_ca.c_str()) != 0) {
        mygram::utils::StructuredLog()
            .Event("mysql_options_warning")
            .Field("option", "MYSQL_OPT_SSL_CA")
            .Field("path", config_.ssl_ca)
            .Warn();
      }
    }
    if (!config_.ssl_cert.empty()) {
      if (mysql_options(mysql_, MYSQL_OPT_SSL_CERT, config_.ssl_cert.c_str()) != 0) {
        mygram::utils::StructuredLog()
            .Event("mysql_options_warning")
            .Field("option", "MYSQL_OPT_SSL_CERT")
            .Field("path", config_.ssl_cert)
            .Warn();
      }
    }
    if (!config_.ssl_key.empty()) {
      if (mysql_options(mysql_, MYSQL_OPT_SSL_KEY, config_.ssl_key.c_str()) != 0) {
        mygram::utils::StructuredLog()
            .Event("mysql_options_warning")
            .Field("option", "MYSQL_OPT_SSL_KEY")
            .Field("path", config_.ssl_key)
            .Warn();
      }
    }

    mygram::utils::StructuredLog().Event("mysql_debug").Field("action", "ssl_enabled").Debug();
  } else {
    // Explicitly disable SSL negotiation for non-SSL connections.
    // Without this, mysql_real_connect() uses SSL_MODE_PREFERRED by default,
    // which attempts SSL handshake and can crash in csm_establish_ssl
    // during concurrent connection establishment.
    unsigned int ssl_mode = SSL_MODE_DISABLED;
    if (mysql_options(mysql_, MYSQL_OPT_SSL_MODE, &ssl_mode) != 0) {
      mygram::utils::StructuredLog().Event("mysql_warning").Field("type", "ssl_mode_disable_failed").Warn();
    }
  }

  // Connect to MySQL
  if (mysql_real_connect(mysql_, config_.host.c_str(), config_.user.c_str(), config_.password.c_str(),
                         config_.database.empty() ? nullptr : config_.database.c_str(), config_.port, nullptr,
                         0) == nullptr) {
    std::string error = GetMySQLErrorMessage();
    // Structured logging
    mygram::utils::StructuredLog()
        .Event("mysql_connection_error")
        .Field("host", config_.host)
        .Field("port", static_cast<uint64_t>(config_.port))
        .Field("context", context)
        .Field("error", error)
        .Error();
    return MakeUnexpected(
        MakeError(ErrorCode::kMySQLConnectionFailed, error, config_.host + ":" + std::to_string(config_.port)));
  }

  // Set character set to utf8mb4 for full Unicode support (including emoji)
  // This is critical for proper handling of multi-byte characters in binlog parsing
  if (mysql_set_character_set(mysql_, "utf8mb4") != 0) {
    std::string error = GetMySQLErrorMessage();
    mygram::utils::StructuredLog()
        .Event("mysql_charset_error")
        .Field("charset", "utf8mb4")
        .Field("error", error)
        .Warn();
    // Continue anyway - most ASCII-based operations will still work
  } else {
    mygram::utils::StructuredLog()
        .Event("mysql_debug")
        .Field("action", "charset_set")
        .Field("charset", "utf8mb4")
        .Debug();
  }

  // Set session timeout to prevent disconnection during long-running operations
  // (e.g., snapshot building which can take several minutes)
  // This only affects the current session and does not impact other MySQL connections
  const std::string set_timeout_query =
      "SET SESSION wait_timeout = " + std::to_string(config_.session_timeout_sec) +
      ", SESSION interactive_timeout = " + std::to_string(config_.session_timeout_sec);
  if (mysql_query(mysql_, set_timeout_query.c_str()) != 0) {
    // Log warning but don't fail connection - timeout setting is not critical
    std::string error = GetMySQLErrorMessage();
    mygram::utils::StructuredLog()
        .Event("mysql_session_timeout_warning")
        .Field("error", error)
        .Field("note", "non-critical")
        .Warn();
  } else {
    mygram::utils::StructuredLog()
        .Event("mysql_debug")
        .Field("action", "session_timeout_set")
        .Field("timeout_sec", static_cast<uint64_t>(config_.session_timeout_sec))
        .Debug();
  }

  // Detect server flavor (MySQL vs MariaDB) from version string
  {
    const char* server_info = mysql_get_server_info(mysql_);
    if (server_info != nullptr) {
      server_version_ = server_info;
      flavor_ = DetectServerFlavor(server_version_);
    }
  }

  std::string context_prefix = context.empty() ? "" : "[" + context + "] ";
  std::string db_info = config_.database.empty() ? "" : "/" + config_.database;
  std::string ssl_info = config_.ssl_enable ? " (SSL/TLS)" : "";
  mygram::utils::StructuredLog()
      .Event("mysql_debug")
      .Field("action", "connected")
      .Field("context", context)
      .Field("host", config_.host)
      .Field("port", static_cast<uint64_t>(config_.port))
      .Field("database", config_.database)
      .Field("ssl", config_.ssl_enable)
      .Field("flavor", GetServerFlavorName(flavor_))
      .Field("version", server_version_)
      .Debug();
  return {};
}

bool Connection::IsConnected() const {
  if (mysql_ == nullptr) {
    return false;
  }

  // Check if connection was established (thread_id will be 0 if not connected)
  return mysql_thread_id(mysql_) != 0;
}

mygram::utils::Expected<void, mygram::utils::Error> Connection::Ping() {
  using mygram::utils::Error;
  using mygram::utils::ErrorCode;
  using mygram::utils::MakeError;
  using mygram::utils::MakeUnexpected;

  if (mysql_ == nullptr) {
    return MakeUnexpected(MakeError(ErrorCode::kMySQLDisconnected, "Not connected"));
  }

  int result = mysql_ping(mysql_);
  if (result != 0) {
    std::string error = GetMySQLErrorMessage();
    mygram::utils::StructuredLog().Event("mysql_warning").Field("operation", "ping").Field("error", error).Warn();
    return MakeUnexpected(MakeError(ErrorCode::kMySQLDisconnected, error));
  }

  return {};
}

mygram::utils::Expected<void, mygram::utils::Error> Connection::Reconnect(bool silent) {
  using mygram::utils::Error;
  using mygram::utils::ErrorCode;
  using mygram::utils::MakeError;
  using mygram::utils::MakeUnexpected;

  if (mysql_ == nullptr) {
    return MakeUnexpected(MakeError(ErrorCode::kMySQLConnectionFailed, "MySQL handle not initialized"));
  }

  if (!silent) {
    mygram::utils::StructuredLog()
        .Event("mysql_reconnecting")
        .Field("host", config_.host)
        .Field("port", static_cast<uint64_t>(config_.port))
        .Info();
  }

  // Close existing connection (mysql_ is guaranteed non-null here by the check above)
  mysql_close(mysql_);
  mysql_ = nullptr;

  // Reinitialize
  mysql_ = mysql_init(nullptr);
  if (mysql_ == nullptr) {
    mygram::utils::StructuredLog()
        .Event("mysql_error")
        .Field("operation", "reconnect")
        .Field("error", "Failed to initialize MySQL handle")
        .Error();
    return MakeUnexpected(MakeError(ErrorCode::kMySQLConnectionFailed, "Failed to initialize MySQL handle"));
  }

  // Reconnect
  return Connect();
}

void Connection::Close() {
  if (mysql_ != nullptr) {
    mysql_close(mysql_);
    mysql_ = nullptr;
    mygram::utils::StructuredLog().Event("mysql_debug").Field("action", "connection_closed").Debug();
  }
}

mygram::utils::Expected<MySQLResult, mygram::utils::Error> Connection::Execute(const std::string& query) {
  using mygram::utils::Error;
  using mygram::utils::ErrorCode;
  using mygram::utils::MakeError;
  using mygram::utils::MakeUnexpected;

  if (mysql_ == nullptr) {
    return MakeUnexpected(MakeError(ErrorCode::kMySQLDisconnected, "Not connected"));
  }

  mygram::utils::StructuredLog().Event("mysql_debug").Field("action", "execute_query").Field("query", query).Debug();

  if (mysql_query(mysql_, query.c_str()) != 0) {
    std::string error = GetMySQLErrorMessage();
    mygram::utils::LogMySQLQueryError(query, error);
    return MakeUnexpected(MakeError(ErrorCode::kMySQLQueryFailed, error, query));
  }

  MYSQL_RES* result = mysql_store_result(mysql_);
  if ((result == nullptr) && mysql_field_count(mysql_) > 0) {
    std::string error = GetMySQLErrorMessage();
    mygram::utils::LogMySQLQueryError(query, "Failed to store result: " + error);
    return MakeUnexpected(MakeError(ErrorCode::kMySQLQueryFailed, error, query));
  }

  return MySQLResult(result);
}

mygram::utils::Expected<void, mygram::utils::Error> Connection::ExecuteUpdate(const std::string& query) {
  using mygram::utils::Error;
  using mygram::utils::ErrorCode;
  using mygram::utils::MakeError;
  using mygram::utils::MakeUnexpected;

  if (mysql_ == nullptr) {
    return MakeUnexpected(MakeError(ErrorCode::kMySQLDisconnected, "Not connected"));
  }

  mygram::utils::StructuredLog().Event("mysql_debug").Field("action", "execute_update").Field("query", query).Debug();

  if (mysql_query(mysql_, query.c_str()) != 0) {
    std::string error = GetMySQLErrorMessage();
    mygram::utils::StructuredLog()
        .Event("mysql_error")
        .Field("operation", "execute_update")
        .Field("query", query)
        .Field("error", error)
        .Error();
    return MakeUnexpected(MakeError(ErrorCode::kMySQLQueryFailed, error, query));
  }

  return {};
}

mygram::utils::Expected<std::string, mygram::utils::Error> Connection::GetExecutedGTID() {
  using mygram::utils::MakeUnexpected;

  const char* query =
      (flavor_ == ServerFlavor::kMariaDB) ? "SELECT @@GLOBAL.gtid_current_pos" : "SELECT @@GLOBAL.gtid_executed";
  auto result = Execute(query);
  if (!result) {
    return MakeUnexpected(result.error());
  }

  MYSQL_ROW row = mysql_fetch_row(result->get());
  if ((row == nullptr) || (row[0] == nullptr)) {
    return std::string{};  // No GTID configured yet — valid state
  }

  std::string gtid(row[0]);
  gtid.erase(std::remove(gtid.begin(), gtid.end(), '\n'), gtid.end());
  gtid.erase(std::remove(gtid.begin(), gtid.end(), '\r'), gtid.end());
  // result is automatically freed by MySQLResult destructor

  mygram::utils::StructuredLog().Event("mysql_debug").Field("action", "get_executed_gtid").Field("gtid", gtid).Debug();
  return gtid;
}

mygram::utils::Expected<std::string, mygram::utils::Error> Connection::GetPurgedGTID() {
  using mygram::utils::MakeUnexpected;

  if (flavor_ == ServerFlavor::kMariaDB) {
    return std::string{};  // MariaDB doesn't have gtid_purged — not an error
  }

  auto result = Execute("SELECT @@GLOBAL.gtid_purged");
  if (!result) {
    return MakeUnexpected(result.error());
  }

  MYSQL_ROW row = mysql_fetch_row(result->get());
  if ((row == nullptr) || (row[0] == nullptr)) {
    return std::string{};
  }

  std::string gtid(row[0]);
  gtid.erase(std::remove(gtid.begin(), gtid.end(), '\n'), gtid.end());
  gtid.erase(std::remove(gtid.begin(), gtid.end(), '\r'), gtid.end());
  // result is automatically freed by MySQLResult destructor

  mygram::utils::StructuredLog().Event("mysql_debug").Field("action", "get_purged_gtid").Field("gtid", gtid).Debug();
  return gtid;
}

mygram::utils::Expected<void, mygram::utils::Error> Connection::SetGTIDNext(const std::string& gtid) {
  using mygram::utils::Error;
  using mygram::utils::ErrorCode;
  using mygram::utils::MakeError;
  using mygram::utils::MakeUnexpected;

  if (gtid == "AUTOMATIC") {
    std::string query = "SET GTID_NEXT = 'AUTOMATIC'";
    return ExecuteUpdate(query);
  }

  // Validate and normalize GTID format to prevent injection
  auto parsed = GTID::Parse(gtid);
  if (!parsed) {
    mygram::utils::StructuredLog()
        .Event("mysql_error")
        .Field("operation", "set_gtid_next")
        .Field("gtid", gtid)
        .Field("error", "Invalid GTID format")
        .Error();
    return MakeUnexpected(MakeError(ErrorCode::kMySQLInvalidGTID, "Invalid GTID format", gtid));
  }

  // Use normalized GTID string to prevent injection
  std::string normalized = parsed->ToString();
  std::string query = "SET GTID_NEXT = '" + normalized + "'";
  return ExecuteUpdate(query);
}

std::optional<std::string> Connection::GetServerUUID() {
  const char* query = (flavor_ == ServerFlavor::kMariaDB) ? "SELECT @@GLOBAL.server_id" : "SELECT @@GLOBAL.server_uuid";
  auto result = Execute(query);
  if (!result) {
    return std::nullopt;
  }

  MYSQL_ROW row = mysql_fetch_row(result->get());
  if ((row == nullptr) || (row[0] == nullptr)) {
    return std::nullopt;
  }

  std::string uuid(row[0]);
  // result is automatically freed by MySQLResult destructor

  mygram::utils::StructuredLog().Event("mysql_debug").Field("action", "get_server_uuid").Field("uuid", uuid).Debug();
  return uuid;
}

mygram::utils::Expected<bool, mygram::utils::Error> Connection::IsGTIDModeEnabled() {
  using mygram::utils::ErrorCode;
  using mygram::utils::MakeError;
  using mygram::utils::MakeUnexpected;

  if (!IsConnected()) {
    return MakeUnexpected(MakeError(ErrorCode::kMySQLDisconnected, "Not connected"));
  }

  if (flavor_ == ServerFlavor::kMariaDB) {
    mygram::utils::StructuredLog()
        .Event("mysql_debug")
        .Field("action", "get_gtid_mode")
        .Field("mode", "always_on")
        .Field("flavor", "MariaDB")
        .Debug();
    return true;
  }

  auto result = Execute("SELECT @@GLOBAL.gtid_mode");
  if (!result) {
    mygram::utils::StructuredLog()
        .Event("mysql_warning")
        .Field("operation", "query_gtid_mode")
        .Field("error", result.error().message())
        .Warn();
    return MakeUnexpected(result.error());
  }

  MYSQL_ROW row = mysql_fetch_row(result->get());
  if ((row == nullptr) || (row[0] == nullptr)) {
    return MakeUnexpected(MakeError(ErrorCode::kMySQLQueryFailed, "Failed to fetch @@GLOBAL.gtid_mode result"));
  }

  std::string mode(row[0]);
  // result is automatically freed by MySQLResult destructor

  mygram::utils::StructuredLog().Event("mysql_debug").Field("action", "get_gtid_mode").Field("mode", mode).Debug();

  // GTID mode can be ON, OFF, ON_PERMISSIVE, or OFF_PERMISSIVE
  // For replication, we need it to be ON
  return mode == "ON";
}

std::optional<std::string> Connection::GetLatestGTID() {
  if (flavor_ == ServerFlavor::kMariaDB) {
    auto result = Execute("SELECT @@GLOBAL.gtid_binlog_pos");
    if (!result) {
      mygram::utils::StructuredLog()
          .Event("mysql_error")
          .Field("operation", "get_latest_gtid")
          .Field("flavor", "MariaDB")
          .Field("error", "Failed to query @@gtid_binlog_pos")
          .Error();
      return std::nullopt;
    }
    MYSQL_ROW row = mysql_fetch_row(result->get());
    if ((row == nullptr) || (row[0] == nullptr)) {
      return std::nullopt;
    }
    std::string gtid_set(row[0]);
    if (gtid_set.empty()) {
      mygram::utils::StructuredLog()
          .Event("mysql_warning")
          .Field("operation", "get_latest_gtid")
          .Field("flavor", "MariaDB")
          .Field("error", "gtid_binlog_pos is empty")
          .Warn();
      return std::nullopt;
    }
    mygram::utils::StructuredLog().Event("mysql_latest_gtid").Field("gtid_set", gtid_set).Info();
    return gtid_set;
  }

  // MySQL: Try new syntax first (MySQL 8.0.23+)
  auto result_exp = Execute("SHOW BINARY LOG STATUS");

  // Fallback to old syntax for MySQL 5.7 / 8.0 < 8.0.23
  if (!result_exp) {
    mygram::utils::StructuredLog().Event("mysql_debug").Field("action", "fallback_show_master_status").Debug();
    result_exp = Execute("SHOW MASTER STATUS");
  }

  if (!result_exp) {
    mygram::utils::StructuredLog()
        .Event("mysql_error")
        .Field("operation", "get_latest_gtid")
        .Field("error", "Failed to execute SHOW BINARY LOG STATUS / SHOW MASTER STATUS")
        .Error();
    return std::nullopt;
  }

  MySQLResult& result = *result_exp;

  // Get field names
  unsigned int num_fields = mysql_num_fields(result.get());
  MYSQL_FIELD* fields = mysql_fetch_fields(result.get());

  // Find the Executed_Gtid_Set column index
  int gtid_column_index = -1;
  for (unsigned int i = 0; i < num_fields; i++) {
    if (std::string(fields[i].name) == "Executed_Gtid_Set") {
      gtid_column_index = static_cast<int>(i);
      break;
    }
  }

  if (gtid_column_index == -1) {
    mygram::utils::StructuredLog()
        .Event("mysql_warning")
        .Field("operation", "get_latest_gtid")
        .Field("error", "Executed_Gtid_Set column not found in SHOW BINARY LOG STATUS")
        .Warn();
    return std::nullopt;
  }

  // Fetch the row
  MYSQL_ROW row = mysql_fetch_row(result.get());
  if ((row == nullptr) || (row[gtid_column_index] == nullptr)) {
    return std::nullopt;
  }

  std::string gtid_set(row[gtid_column_index]);
  // result is automatically freed by MySQLResult destructor

  // The GTID set may be empty or contain multiple ranges
  // For simplicity, we'll return the entire set as-is
  // Example format: "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5"
  if (gtid_set.empty()) {
    mygram::utils::StructuredLog()
        .Event("mysql_warning")
        .Field("operation", "get_latest_gtid")
        .Field("error", "Executed_Gtid_Set is empty")
        .Warn();
    return std::nullopt;
  }

  mygram::utils::StructuredLog().Event("mysql_latest_gtid").Field("gtid_set", gtid_set).Info();
  return gtid_set;
}

mygram::utils::Expected<void, mygram::utils::Error> Connection::ValidateUniqueColumn(const std::string& database,
                                                                                     const std::string& table,
                                                                                     const std::string& column) {
  using mygram::utils::ErrorCode;
  using mygram::utils::MakeError;
  using mygram::utils::MakeUnexpected;

  // Check connection before using mysql_ handle
  if (mysql_ == nullptr) {
    return MakeUnexpected(MakeError(ErrorCode::kMySQLDisconnected, "Not connected to MySQL"));
  }

  // Escape parameters to prevent SQL injection
  std::vector<char> escaped_db(database.length() * 2 + 1);
  std::vector<char> escaped_table(table.length() * 2 + 1);
  std::vector<char> escaped_column(column.length() * 2 + 1);
  mysql_real_escape_string(mysql_, escaped_db.data(), database.c_str(), database.length());
  mysql_real_escape_string(mysql_, escaped_table.data(), table.c_str(), table.length());
  mysql_real_escape_string(mysql_, escaped_column.data(), column.c_str(), column.length());

  // Cache escaped strings to avoid redundant std::string constructions
  std::string esc_db_str(escaped_db.data());
  std::string esc_table_str(escaped_table.data());
  std::string esc_column_str(escaped_column.data());

  // Query to check if the column is part of a PRIMARY KEY or UNIQUE KEY
  std::string query =
      "SELECT COUNT(*) FROM information_schema.KEY_COLUMN_USAGE "
      "WHERE TABLE_SCHEMA = '" +
      esc_db_str + "' AND TABLE_NAME = '" + esc_table_str + "' AND COLUMN_NAME = '" + esc_column_str +
      "' AND (CONSTRAINT_NAME = 'PRIMARY' OR CONSTRAINT_NAME IN "
      "(SELECT CONSTRAINT_NAME FROM information_schema.TABLE_CONSTRAINTS "
      "WHERE TABLE_SCHEMA = '" +
      esc_db_str + "' AND TABLE_NAME = '" + esc_table_str +
      "' AND CONSTRAINT_TYPE = 'UNIQUE' AND CONSTRAINT_NAME IN "
      "(SELECT CONSTRAINT_NAME FROM information_schema.KEY_COLUMN_USAGE "
      "WHERE TABLE_SCHEMA = '" +
      esc_db_str + "' AND TABLE_NAME = '" + esc_table_str + "' GROUP BY CONSTRAINT_NAME HAVING COUNT(*) = 1)))";

  auto result_exp = Execute(query);
  if (!result_exp) {
    return MakeUnexpected(MakeError(ErrorCode::kMySQLQueryFailed,
                                    "Failed to query table schema: " + result_exp.error().message(),
                                    database + "." + table));
  }

  MYSQL_ROW row = mysql_fetch_row(result_exp->get());
  if ((row == nullptr) || (row[0] == nullptr)) {
    return MakeUnexpected(MakeError(ErrorCode::kMySQLQueryFailed, "Failed to fetch result for unique column validation",
                                    database + "." + table + "." + column));
  }

  int count = 0;
  auto [ptr, ec] = std::from_chars(row[0], row[0] + std::strlen(row[0]), count);
  if (ec != std::errc{}) {
    return MakeUnexpected(MakeError(ErrorCode::kMySQLInvalidSchema, "Invalid count value in unique column validation",
                                    database + "." + table + "." + column));
  }
  // result is automatically freed by MySQLResult destructor

  if (count == 0) {
    // Column is not a single-column PRIMARY KEY or UNIQUE KEY
    // Check if column exists and provide more specific error
    std::string column_check_query =
        "SELECT COUNT(*) FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA = '" +
        esc_db_str + "' AND TABLE_NAME = '" + esc_table_str + "' AND COLUMN_NAME = '" + esc_column_str + "'";

    auto col_result_exp = Execute(column_check_query);
    if (col_result_exp) {
      MYSQL_ROW col_row = mysql_fetch_row(col_result_exp->get());
      if ((col_row != nullptr) && (col_row[0] != nullptr)) {
        auto column_count = mygram::utils::ParseNumeric<int>(col_row[0]);
        if (column_count.has_value()) {
          if (*column_count == 0) {
            return MakeUnexpected(
                MakeError(ErrorCode::kMySQLColumnNotFound,
                          "Column '" + column + "' does not exist in table '" + database + "." + table + "'",
                          database + "." + table + "." + column));
          }
        } else {
          return MakeUnexpected(MakeError(ErrorCode::kMySQLInvalidSchema, "Invalid column count value",
                                          database + "." + table + "." + column));
        }
      }
      // col_result is automatically freed by MySQLResult destructor
    }

    // Column exists but is not unique
    return MakeUnexpected(MakeError(ErrorCode::kMySQLDuplicateColumn,
                                    "Column '" + column + "' in table '" + database + "." + table +
                                        "' must be a single-column PRIMARY KEY or UNIQUE KEY. "
                                        "Composite keys are not supported.",
                                    database + "." + table + "." + column));
  }

  mygram::utils::StructuredLog()
      .Event("mysql_unique_column_validated")
      .Field("database", database)
      .Field("table", table)
      .Field("column", column)
      .Info();
  return {};
}

std::string Connection::GetMySQLErrorMessage() const {
  if (mysql_ != nullptr) {
    return std::string(mysql_error(mysql_));
  }
  return "MySQL handle not initialized";
}

}  // namespace mygramdb::mysql

// NOLINTEND(cppcoreguidelines-pro-bounds-pointer-arithmetic)

#endif  // USE_MYSQL
