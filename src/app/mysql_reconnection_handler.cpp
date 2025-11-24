/**
 * @file mysql_reconnection_handler.cpp
 * @brief MySQL reconnection handler implementation
 */

#include "app/mysql_reconnection_handler.h"

#include <spdlog/spdlog.h>

#include "utils/structured_log.h"

#ifdef USE_MYSQL
#include "mysql/binlog_reader.h"
#include "mysql/connection.h"
#include "mysql/connection_validator.h"
#endif

namespace mygramdb::app {

using mygram::utils::Error;
using mygram::utils::ErrorCode;
using mygram::utils::Expected;
using mygram::utils::MakeError;
using mygram::utils::MakeUnexpected;

#ifdef USE_MYSQL

MysqlReconnectionHandler::MysqlReconnectionHandler(mysql::Connection* mysql_connection,
                                                   mysql::BinlogReader* binlog_reader,
                                                   std::atomic<bool>* reconnecting_flag)
    : mysql_connection_(mysql_connection), binlog_reader_(binlog_reader), reconnecting_flag_(reconnecting_flag) {}

Expected<void, Error> MysqlReconnectionHandler::Reconnect(const std::string& new_host, int new_port) {
  // Set reconnecting flag to block manual REPLICATION START
  if (reconnecting_flag_ != nullptr) {
    reconnecting_flag_->store(true);
  }

  mygram::utils::StructuredLog()
      .Event("mysql_reconnection_start")
      .Field("new_host", new_host)
      .Field("new_port", static_cast<int64_t>(new_port))
      .Info();

  // Step 1: Save current GTID position from BinlogReader
  std::string current_gtid;
  if (binlog_reader_ != nullptr && binlog_reader_->IsRunning()) {
    current_gtid = binlog_reader_->GetCurrentGTID();
    mygram::utils::StructuredLog().Event("mysql_reconnection_gtid_saved").Field("gtid", current_gtid).Info();
  }

  // Step 2: Stop BinlogReader (graceful shutdown)
  if (binlog_reader_ != nullptr && binlog_reader_->IsRunning()) {
    spdlog::info("Stopping BinlogReader for reconnection...");
    binlog_reader_->Stop();
    mygram::utils::StructuredLog().Event("mysql_reconnection_binlog_stopped").Info();
  }

  // Step 3: Reconnect to new MySQL host/port
  spdlog::info("Reconnecting to new MySQL host: {}:{}", new_host, new_port);

  // Get current connection config and update host/port
  auto config = mysql_connection_->GetConfig();
  config.host = new_host;
  config.port = static_cast<uint16_t>(new_port);

  // Close old connection
  mysql_connection_->Close();
  mygram::utils::StructuredLog().Event("mysql_reconnection_old_connection_closed").Info();

  // Create new connection with updated config
  *mysql_connection_ = mysql::Connection(config);
  auto connect_result = mysql_connection_->Connect("reconnection");
  if (!connect_result) {
    // Clear reconnecting flag on error
    if (reconnecting_flag_ != nullptr) {
      reconnecting_flag_->store(false);
    }
    mygram::utils::StructuredLog()
        .Event("mysql_reconnection_connect_failed")
        .Field("host", new_host)
        .Field("port", static_cast<int64_t>(new_port))
        .Field("error", connect_result.error().message())
        .Error();
    return connect_result;
  }

  // Step 4: Validate new connection
  auto validate_result = ValidateConnection(mysql_connection_);
  if (!validate_result) {
    // Clear reconnecting flag on error
    if (reconnecting_flag_ != nullptr) {
      reconnecting_flag_->store(false);
    }
    mygram::utils::StructuredLog()
        .Event("mysql_reconnection_validation_failed")
        .Field("host", new_host)
        .Field("port", static_cast<int64_t>(new_port))
        .Field("error", validate_result.error().message())
        .Error();
    return validate_result;
  }

  mygram::utils::StructuredLog()
      .Event("mysql_reconnection_new_connection_established")
      .Field("host", new_host)
      .Field("port", static_cast<int64_t>(new_port))
      .Info();

  // Step 5: Restart BinlogReader with saved GTID
  if (binlog_reader_ != nullptr) {
    if (!current_gtid.empty()) {
      // Resume from saved GTID position
      spdlog::info("Restarting BinlogReader from GTID: {}", current_gtid);
      auto start_result = binlog_reader_->StartFromGtid(current_gtid);
      if (!start_result) {
        // Clear reconnecting flag on error
        if (reconnecting_flag_ != nullptr) {
          reconnecting_flag_->store(false);
        }
        mygram::utils::StructuredLog()
            .Event("mysql_reconnection_binlog_restart_failed")
            .Field("error", start_result.error().message())
            .Error();
        return start_result;
      }
    } else {
      // Start from latest position
      spdlog::info("Restarting BinlogReader from latest position");
      binlog_reader_->Start();
    }

    mygram::utils::StructuredLog().Event("mysql_reconnection_binlog_restarted").Info();
  }

  mygram::utils::StructuredLog()
      .Event("mysql_reconnection_success")
      .Field("new_host", new_host)
      .Field("new_port", static_cast<int64_t>(new_port))
      .Info();

  // Clear reconnecting flag
  if (reconnecting_flag_ != nullptr) {
    reconnecting_flag_->store(false);
  }

  spdlog::info("MySQL reconnection completed successfully");
  return {};
}

Expected<void, Error> MysqlReconnectionHandler::ValidateConnection(mysql::Connection* connection) {
  if (connection == nullptr) {
    return MakeUnexpected(MakeError(ErrorCode::kInternalError, "Connection is null"));
  }

  // Use ConnectionValidator to validate the connection
  // Note: We don't specify required_tables or expected_uuid here since this is a new connection
  auto validation_result = mysql::ConnectionValidator::ValidateServer(*connection, {}, std::nullopt);

  if (!validation_result.valid) {
    return MakeUnexpected(MakeError(ErrorCode::kInternalError, validation_result.error_message));
  }

  return {};
}

#else

// Non-MySQL stub implementation
MysqlReconnectionHandler::MysqlReconnectionHandler(void* mysql_connection, void* binlog_reader)
    : mysql_connection_(mysql_connection), binlog_reader_(binlog_reader) {}

Expected<void, Error> MysqlReconnectionHandler::Reconnect(const std::string& /*new_host*/, int /*new_port*/) {
  return MakeUnexpected(MakeError(ErrorCode::kInternalError, "MySQL support not enabled"));
}

Expected<void, Error> MysqlReconnectionHandler::ValidateConnection(
    void* /*connection*/) {  // NOLINT(readability-convert-member-functions-to-static)
  return MakeUnexpected(MakeError(ErrorCode::kInternalError, "MySQL support not enabled"));
}

#endif

}  // namespace mygramdb::app
