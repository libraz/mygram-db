/**
 * @file mysql_reconnection_handler.cpp
 * @brief MySQL reconnection handler implementation
 */

#include "app/mysql_reconnection_handler.h"

#include <spdlog/spdlog.h>

#include <optional>

#include "server/replication_pause_counter.h"
#include "utils/fd_guard.h"
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

MysqlReconnectionHandler::MysqlReconnectionHandler(
    mysql::Connection* mysql_connection, mysql::BinlogReader* binlog_reader, std::atomic<bool>* reconnecting_flag,
    std::vector<mysql::ConnectionValidator::RequiredTable> required_tables, std::atomic<bool>* dump_save_in_progress,
    std::atomic<bool>* replication_paused_for_dump, server::replication_pause::Counter* replication_pause_counter)
    : mysql_connection_(mysql_connection),
      binlog_reader_(binlog_reader),
      reconnecting_flag_(reconnecting_flag),
      required_tables_(std::move(required_tables)),
      dump_save_in_progress_(dump_save_in_progress),
      replication_paused_for_dump_(replication_paused_for_dump),
      replication_pause_counter_(replication_pause_counter) {}

Expected<void, Error> MysqlReconnectionHandler::Reconnect(const std::string& new_host, int new_port) {
  std::unique_lock<std::mutex> reconnect_lock(reconnect_mutex_, std::try_to_lock);
  if (!reconnect_lock.owns_lock()) {
    return MakeUnexpected(MakeError(ErrorCode::kMySQLReplicationError, "MySQL reconnection is already in progress"));
  }

  if (dump_save_in_progress_ != nullptr && dump_save_in_progress_->load(std::memory_order_acquire)) {
    return MakeUnexpected(
        MakeError(ErrorCode::kMySQLReplicationError, "Cannot reconnect MySQL while DUMP SAVE is in progress"));
  }
  if (replication_paused_for_dump_ != nullptr && replication_paused_for_dump_->load(std::memory_order_acquire)) {
    return MakeUnexpected(MakeError(ErrorCode::kMySQLReplicationError,
                                    "Cannot reconnect MySQL while replication is paused for DUMP/SNAPSHOT"));
  }

  bool expected_reconnecting = false;
  if (reconnecting_flag_ != nullptr &&
      !reconnecting_flag_->compare_exchange_strong(expected_reconnecting, true, std::memory_order_acq_rel,
                                                   std::memory_order_acquire)) {
    return MakeUnexpected(MakeError(ErrorCode::kMySQLReplicationError, "MySQL reconnection is already in progress"));
  }

  // RAII guard to ensure reconnecting flag is always cleared on exit
  struct ReconnectingGuard {
    std::atomic<bool>* flag;
    explicit ReconnectingGuard(std::atomic<bool>* f) : flag(f) {}
    ~ReconnectingGuard() {
      if (flag != nullptr) {
        flag->store(false);
      }
    }
    ReconnectingGuard(const ReconnectingGuard&) = delete;
    ReconnectingGuard& operator=(const ReconnectingGuard&) = delete;
  };
  ReconnectingGuard guard(reconnecting_flag_);

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
  std::optional<server::replication_pause::Scope> pause_scope;
  bool replication_pause_acquired = false;
  if (binlog_reader_ != nullptr && binlog_reader_->IsRunning() && replication_pause_counter_ != nullptr) {
    pause_scope.emplace(*replication_pause_counter_);
    replication_pause_acquired = pause_scope->Acquire();
    if (!replication_pause_acquired) {
      return MakeUnexpected(MakeError(ErrorCode::kMySQLReplicationError,
                                      "Cannot reconnect MySQL while another replication pause is active"));
    }
  }

  auto release_pause = mygram::utils::ScopeGuard([this, &pause_scope, &replication_pause_acquired]() {
    if (replication_pause_acquired && replication_paused_for_dump_ != nullptr) {
      replication_paused_for_dump_->store(false, std::memory_order_release);
    }
    if (pause_scope.has_value() && pause_scope->held()) {
      pause_scope->Release();
    }
  });

  if (binlog_reader_ != nullptr && binlog_reader_->IsRunning()) {
    mygram::utils::StructuredLog().Event("mysql_reconnection_stopping_binlog").Info();
    binlog_reader_->Stop();
    if (replication_pause_acquired && replication_paused_for_dump_ != nullptr) {
      replication_paused_for_dump_->store(true, std::memory_order_release);
    }
    mygram::utils::StructuredLog().Event("mysql_reconnection_binlog_stopped").Info();
  }

  // Step 3: Reconnect to new MySQL host/port
  mygram::utils::StructuredLog()
      .Event("mysql_reconnection_connecting")
      .Field("host", new_host)
      .Field("port", static_cast<int64_t>(new_port))
      .Info();

  // Get current connection config and update host/port
  auto config = mysql_connection_->GetConfig();
  config.host = new_host;
  config.port = static_cast<uint16_t>(new_port);

  // Create new connection in a temporary first, then swap on success (#9)
  // This preserves the old connection if the new one fails to connect.
  mysql::Connection new_connection(config);
  auto connect_result = new_connection.Connect("reconnection");
  if (!connect_result) {
    mygram::utils::StructuredLog()
        .Event("mysql_reconnection_connect_failed")
        .Field("host", new_host)
        .Field("port", static_cast<int64_t>(new_port))
        .Field("error", connect_result.error().message())
        .Error();
    return connect_result;
  }

  // New connection succeeded - replace old connection (old one is closed by move assignment)
  *mysql_connection_ = std::move(new_connection);
  mygram::utils::StructuredLog().Event("mysql_reconnection_old_connection_replaced").Info();

  // Step 4: Validate new connection
  auto validate_result = ValidateConnection(mysql_connection_);
  if (!validate_result) {
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
      mygram::utils::StructuredLog().Event("mysql_reconnection_restarting_binlog").Field("gtid", current_gtid).Info();
      auto start_result = binlog_reader_->StartFromGtid(current_gtid);
      if (!start_result) {
        mygram::utils::StructuredLog()
            .Event("mysql_reconnection_binlog_restart_failed")
            .Field("error", start_result.error().message())
            .Error();
        return start_result;
      }
    } else {
      // Start from latest position
      mygram::utils::StructuredLog().Event("mysql_reconnection_restarting_binlog").Field("position", "latest").Info();
      auto start_result = binlog_reader_->Start();
      if (!start_result) {
        mygram::utils::StructuredLog()
            .Event("mysql_reconnection_binlog_restart_failed")
            .Field("error", start_result.error().message())
            .Error();
        return start_result;
      }
    }

    mygram::utils::StructuredLog().Event("mysql_reconnection_binlog_restarted").Info();
  }
  if (replication_pause_acquired && replication_paused_for_dump_ != nullptr) {
    replication_paused_for_dump_->store(false, std::memory_order_release);
  }
  if (pause_scope.has_value() && pause_scope->held()) {
    pause_scope->Release();
  }
  release_pause.Release();

  mygram::utils::StructuredLog()
      .Event("mysql_reconnection_success")
      .Field("new_host", new_host)
      .Field("new_port", static_cast<int64_t>(new_port))
      .Info();

  // reconnecting flag is automatically cleared by RAII guard
  mygram::utils::StructuredLog().Event("mysql_reconnection_completed").Info();
  return {};
}

Expected<void, Error> MysqlReconnectionHandler::ValidateConnection(mysql::Connection* connection) const {
  if (connection == nullptr) {
    return MakeUnexpected(MakeError(ErrorCode::kMySQLDisconnected, "Connection is null"));
  }

  // Validate connection including required tables check
  auto validation_result = mysql::ConnectionValidator::ValidateServer(*connection, required_tables_, std::nullopt);

  if (!validation_result.valid) {
    return MakeUnexpected(MakeError(ErrorCode::kMySQLConnectionFailed, validation_result.error_message));
  }

  return {};
}

#else

// Non-MySQL stub implementation
Expected<void, Error> MysqlReconnectionHandler::Reconnect(const std::string& /*new_host*/, int /*new_port*/) {
  return MakeUnexpected(MakeError(ErrorCode::kNotImplemented, "MySQL support not enabled"));
}

#endif

}  // namespace mygramdb::app
