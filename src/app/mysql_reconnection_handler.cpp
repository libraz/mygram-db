/**
 * @file mysql_reconnection_handler.cpp
 * @brief MySQL reconnection handler implementation
 */

#include "app/mysql_reconnection_handler.h"

#include <spdlog/spdlog.h>

#include <optional>

#include "server/operation_coordinator.h"
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
    std::atomic<bool>* replication_paused_for_dump, server::replication_pause::Counter* replication_pause_counter,
    server::OperationCoordinator* operation_coordinator)
    : mysql_connection_(mysql_connection),
      binlog_reader_(binlog_reader),
      reconnecting_flag_(reconnecting_flag),
      required_tables_(std::move(required_tables)),
      dump_save_in_progress_(dump_save_in_progress),
      replication_paused_for_dump_(replication_paused_for_dump),
      replication_pause_counter_(replication_pause_counter),
      operation_coordinator_(operation_coordinator) {}

Expected<void, Error> MysqlReconnectionHandler::Reconnect(const std::string& new_host, int new_port) {
  std::unique_lock<std::mutex> reconnect_lock(reconnect_mutex_, std::try_to_lock);
  if (!reconnect_lock.owns_lock()) {
    return MakeUnexpected(MakeError(ErrorCode::kMySQLReplicationError, "MySQL reconnection is already in progress"));
  }

  std::optional<server::OperationCoordinator::Token> operation_token;
  if (operation_coordinator_ != nullptr) {
    operation_token = operation_coordinator_->TryAcquire(server::LongOperation::kMysqlReconnect,
                                                         new_host + ":" + std::to_string(new_port));
    if (!operation_token.has_value()) {
      return MakeUnexpected(
          MakeError(ErrorCode::kMySQLReplicationError,
                    "Cannot reconnect MySQL while " + operation_coordinator_->DescribeActive() + " is in progress"));
    }
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

  // Connect and validate the candidate before disturbing the live connection
  // or replication reader. Most failover errors therefore leave production
  // state completely untouched.
  mygram::utils::StructuredLog()
      .Event("mysql_reconnection_connecting")
      .Field("host", new_host)
      .Field("port", static_cast<int64_t>(new_port))
      .Info();

  if (mysql_connection_ == nullptr) {
    return MakeUnexpected(MakeError(ErrorCode::kMySQLDisconnected, "Current MySQL connection is null"));
  }

  auto old_uuid_result = mysql_connection_->GetServerUUID();
  if (!old_uuid_result) {
    return MakeUnexpected(old_uuid_result.error());
  }
  const std::optional<std::string> old_server_uuid = *old_uuid_result;
  const bool replication_was_running = binlog_reader_ != nullptr && binlog_reader_->IsRunning();
  const std::optional<std::string> preliminary_gtid =
      replication_was_running ? std::optional<std::string>{binlog_reader_->GetCurrentGTID()} : std::nullopt;

  // Get current connection config and update host/port
  auto config = mysql_connection_->GetConfig();
  config.host = new_host;
  config.port = static_cast<uint16_t>(new_port);

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

  auto validate_result = ValidateConnection(&new_connection, old_server_uuid, preliminary_gtid);
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

  std::string current_gtid;
  std::optional<server::replication_pause::Scope> pause_scope;
  bool replication_pause_acquired = false;
  if (replication_was_running) {
    if (replication_pause_counter_ != nullptr) {
      pause_scope.emplace(*replication_pause_counter_);
      replication_pause_acquired = pause_scope->Acquire();
      if (!replication_pause_acquired) {
        return MakeUnexpected(MakeError(ErrorCode::kMySQLReplicationError,
                                        "Cannot reconnect MySQL while another replication pause is active"));
      }
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

  if (replication_was_running) {
    mygram::utils::StructuredLog().Event("mysql_reconnection_stopping_binlog").Info();
    binlog_reader_->Stop();
    current_gtid = binlog_reader_->GetCurrentGTID();
    mygram::utils::StructuredLog().Event("mysql_reconnection_gtid_saved").Field("gtid", current_gtid).Info();
    if (replication_pause_counter_ != nullptr) {
      replication_pause_counter_->PublishDrainedGTID(current_gtid);
    }
    if (replication_pause_acquired && replication_paused_for_dump_ != nullptr) {
      replication_paused_for_dump_->store(true, std::memory_order_release);
    }
    mygram::utils::StructuredLog().Event("mysql_reconnection_binlog_stopped").Info();

    // Events may have drained between the preliminary validation and Stop().
    // Re-check the exact hand-off position before replacing the live
    // connection. A mismatch here still leaves the old connection installed;
    // resume it and report the candidate error.
    auto drained_validation = ValidateConnection(&new_connection, old_server_uuid, current_gtid);
    if (!drained_validation) {
      Expected<void, Error> rollback_start =
          current_gtid.empty() ? binlog_reader_->Start() : binlog_reader_->StartFromGtid(current_gtid);
      if (!rollback_start) {
        return MakeUnexpected(
            MakeError(ErrorCode::kMySQLReplicationError,
                      "Failover validation failed and rollback replication restart also failed: candidate=" +
                          drained_validation.error().message() + "; rollback=" + rollback_start.error().message()));
      }
      return MakeUnexpected(drained_validation.error());
    }
  }

  // Commit the validated candidate while retaining the old connection for
  // rollback until replication has successfully started on the new server.
  mysql::Connection old_connection(std::move(*mysql_connection_));
  *mysql_connection_ = std::move(new_connection);
  mygram::utils::StructuredLog().Event("mysql_reconnection_old_connection_replaced").Info();

  if (replication_was_running) {
    Expected<void, Error> start_result =
        current_gtid.empty() ? binlog_reader_->Start() : binlog_reader_->StartFromGtid(current_gtid);
    if (!start_result) {
      const Error candidate_error = start_result.error();
      mygram::utils::StructuredLog()
          .Event("mysql_reconnection_binlog_restart_failed")
          .Field("error", candidate_error.message())
          .Error();

      // Start() failure leaves the reader stopped. Restore the main connection
      // object in-place so BinlogReader's reference remains valid, then resume
      // from the exact processed GTID on the old server.
      mysql::Connection failed_candidate(std::move(*mysql_connection_));
      *mysql_connection_ = std::move(old_connection);
      Expected<void, Error> rollback_start =
          current_gtid.empty() ? binlog_reader_->Start() : binlog_reader_->StartFromGtid(current_gtid);
      if (!rollback_start) {
        return MakeUnexpected(MakeError(
            ErrorCode::kMySQLReplicationError,
            "Failover failed and rollback replication restart also failed: candidate=" + candidate_error.message() +
                "; rollback=" + rollback_start.error().message()));
      }
      mygram::utils::StructuredLog().Event("mysql_reconnection_rolled_back").Field("gtid", current_gtid).Warn();
      return MakeUnexpected(candidate_error);
    }
    mygram::utils::StructuredLog().Event("mysql_reconnection_binlog_restarted").Info();
  }

  mygram::utils::StructuredLog()
      .Event("mysql_reconnection_success")
      .Field("new_host", new_host)
      .Field("new_port", static_cast<int64_t>(new_port))
      .Info();

  // reconnecting flag is automatically cleared by RAII guard
  mygram::utils::StructuredLog().Event("mysql_reconnection_completed").Info();
  return {};
}

Expected<void, Error> MysqlReconnectionHandler::ValidateConnection(mysql::Connection* connection,
                                                                   const std::optional<std::string>& expected_uuid,
                                                                   const std::optional<std::string>& last_gtid) const {
  if (connection == nullptr) {
    return MakeUnexpected(MakeError(ErrorCode::kMySQLDisconnected, "Connection is null"));
  }

  // Validate connection including required tables check
  auto validation_result =
      mysql::ConnectionValidator::ValidateServer(*connection, required_tables_, expected_uuid, last_gtid);

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
