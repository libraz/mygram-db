/**
 * @file binlog_reader.cpp
 * @brief Binlog reader implementation - lifecycle management
 *
 * Contains constructors, destructor, Start, StartFromGtid, Stop,
 * GetCurrentGTID, SetCurrentGTID, and GetQueueSize.
 *
 * Note: This file contains MySQL binlog protocol parsing code.
 * Some modern C++ guidelines are relaxed for protocol compatibility.
 */

#include "mysql/binlog_reader.h"

#ifdef USE_MYSQL

#include <spdlog/spdlog.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <utility>

#include "mysql/binlog_reader_internal.h"
#include "server/tcp_server.h"  // For TableContext definition
#include "utils/structured_log.h"

// NOLINTBEGIN(cppcoreguidelines-pro-*,cppcoreguidelines-avoid-*,readability-magic-numbers)

namespace mygramdb::mysql {

// Single-table mode constructor (deprecated)
BinlogReader::BinlogReader(Connection& connection, index::Index& index, storage::DocumentStore& doc_store,
                           config::TableConfig table_config, config::MysqlConfig mysql_config, const Config& config,
                           server::ServerStats* stats)
    : connection_(connection),

      index_(&index),
      doc_store_(&doc_store),
      table_config_(std::move(table_config)),
      mysql_config_(std::move(mysql_config)),
      config_(config),
      current_gtid_(config.start_gtid) {
  server_stats_.store(stats, std::memory_order_relaxed);
}

// Multi-table mode constructor
BinlogReader::BinlogReader(Connection& connection,
                           std::unordered_map<std::string, server::TableContext*> table_contexts,
                           config::MysqlConfig mysql_config, const Config& config, server::ServerStats* stats)
    : connection_(connection),
      table_contexts_(std::move(table_contexts)),
      multi_table_mode_(true),
      mysql_config_(std::move(mysql_config)),
      config_(config),
      current_gtid_(config.start_gtid) {
  server_stats_.store(stats, std::memory_order_relaxed);
}

BinlogReader::~BinlogReader() {
  Stop();
}

mygram::utils::Expected<void, mygram::utils::Error> BinlogReader::Start() {
  using mygram::utils::Error;
  using mygram::utils::ErrorCode;
  using mygram::utils::MakeError;
  using mygram::utils::MakeUnexpected;

  // Serialize with Stop() to prevent Start() during cleanup.
  // This ensures all resources from a previous run are fully released
  // before creating new connections and threads.
  std::lock_guard<std::mutex> start_lock(stop_mutex_);

  // Clean up stale state if should_stop_ is set.
  // This can happen in two scenarios:
  // 1. Reader thread self-exited due to non-recoverable error (should_stop_=true, running_=true)
  //    The thread sets should_stop_ but does NOT clear running_ (to prevent Stop() join race).
  // 2. Previous Stop() completed but a race left should_stop_ set (should_stop_=true, running_=false)
  //
  // Since we hold stop_mutex_ here, and Stop() also requires stop_mutex_,
  // Stop() cannot be running concurrently. It's safe to join threads and reset.
  if (should_stop_.load()) {
    if (reader_thread_ && reader_thread_->joinable()) {
      reader_thread_->join();
      reader_thread_.reset();
    }
    if (worker_thread_ && worker_thread_->joinable()) {
      worker_thread_->join();
      worker_thread_.reset();
    }
    binlog_connection_.reset();
    metadata_connection_.reset();
    should_stop_ = false;
    running_ = false;
    mygram::utils::StructuredLog().Event("binlog_reader_stale_state_cleaned").Info();
  }

  // Atomically check and set running_ to prevent concurrent Start() calls
  bool expected = false;
  if (!running_.compare_exchange_strong(expected, true)) {
    SetLastError("Binlog reader is already running");
    return MakeUnexpected(MakeError(ErrorCode::kMySQLBinlogError, GetLastError()));
  }

  // RAII guard to manage all resources on failure
  struct StartupGuard {
    std::atomic<bool>& running_flag;
    std::unique_ptr<std::thread>& worker_thread;
    std::unique_ptr<std::thread>& reader_thread;
    std::unique_ptr<Connection>& binlog_conn;
    std::unique_ptr<Connection>& metadata_conn;
    std::atomic<bool>& should_stop;
    bool success = false;

    StartupGuard(std::atomic<bool>& running, std::unique_ptr<std::thread>& worker, std::unique_ptr<std::thread>& reader,
                 std::unique_ptr<Connection>& conn, std::unique_ptr<Connection>& meta_conn, std::atomic<bool>& stop)
        : running_flag(running),
          worker_thread(worker),
          reader_thread(reader),
          binlog_conn(conn),
          metadata_conn(meta_conn),
          should_stop(stop) {}

    ~StartupGuard() {
      if (success) {
        return;  // Success - don't clean up
      }

      // Failure - clean up all resources
      should_stop = true;

      // Join threads if they were created
      if (worker_thread && worker_thread->joinable()) {
        worker_thread->join();
      }
      if (reader_thread && reader_thread->joinable()) {
        reader_thread->join();
      }

      // Reset thread objects
      worker_thread.reset();
      reader_thread.reset();

      // Reset connections
      binlog_conn.reset();
      metadata_conn.reset();

      // Clear running flag to allow future Start() calls.
      // Note: should_stop is NOT reset here — the next Start() call
      // sets should_stop_ = false explicitly before launching threads.
      running_flag = false;
    }

    StartupGuard(const StartupGuard&) = delete;
    StartupGuard& operator=(const StartupGuard&) = delete;
    StartupGuard(StartupGuard&&) = delete;
    StartupGuard& operator=(StartupGuard&&) = delete;
  };

  StartupGuard guard(running_, worker_thread_, reader_thread_, binlog_connection_, metadata_connection_, should_stop_);

  // Validate server_id (must be non-zero for replication)
  if (config_.server_id == 0) {
    SetLastError("server_id must be set to a non-zero value for binlog replication");
    mygram::utils::LogBinlogError("invalid_server_id", current_gtid_, GetLastError());
    return MakeUnexpected(MakeError(ErrorCode::kMySQLBinlogError, GetLastError()));
  }

  // Check MySQL connection (reconnect if stale)
  if (!connection_.IsConnected()) {
    auto reconnect_result = connection_.Reconnect();
    if (!reconnect_result) {
      SetLastError("MySQL connection not established and reconnect failed");
      mygram::utils::LogBinlogError("startup_failed", current_gtid_, GetLastError());
      return MakeUnexpected(MakeError(ErrorCode::kMySQLDisconnected, GetLastError()));
    }
  } else {
    // Connection appears alive but may be stale; verify with ping
    auto ping_result = connection_.Ping();
    if (!ping_result) {
      auto reconnect_result = connection_.Reconnect();
      if (!reconnect_result) {
        SetLastError("MySQL connection lost and reconnect failed");
        mygram::utils::LogBinlogError("startup_failed", current_gtid_, GetLastError());
        return MakeUnexpected(MakeError(ErrorCode::kMySQLDisconnected, GetLastError()));
      }
    }
  }

  // Check if GTID mode is enabled (using main connection)
  if (!connection_.IsGTIDModeEnabled()) {
    SetLastError(
        "GTID mode is not enabled on MySQL server. "
        "Please enable GTID mode (gtid_mode=ON) for binlog replication.");
    mygram::utils::LogBinlogError("gtid_mode_disabled", current_gtid_, GetLastError());
    return MakeUnexpected(MakeError(ErrorCode::kMySQLBinlogError, GetLastError()));
  }

  // Validate primary keys for all tables
  if (multi_table_mode_) {
    for (const auto& [table_name, ctx] : table_contexts_) {
      std::string validation_error;
      if (!connection_.ValidateUniqueColumn(connection_.GetConfig().database, ctx->config.name, ctx->config.primary_key,
                                            validation_error)) {
        SetLastError("Primary key validation failed for table '" + table_name + "': " + validation_error);
        mygram::utils::StructuredLog()
            .Event("binlog_error")
            .Field("type", "primary_key_validation_failed")
            .Field("table", table_name)
            .Field("gtid", current_gtid_)
            .Field("error", GetLastError())
            .Error();
        return MakeUnexpected(MakeError(ErrorCode::kMySQLBinlogError, GetLastError()));
      }
    }
  } else {
    // Single-table mode
    std::string validation_error;
    if (!connection_.ValidateUniqueColumn(connection_.GetConfig().database, table_config_.name,
                                          table_config_.primary_key, validation_error)) {
      SetLastError("Primary key validation failed: " + validation_error);
      mygram::utils::StructuredLog()
          .Event("binlog_error")
          .Field("type", "primary_key_validation_failed")
          .Field("table", table_config_.name)
          .Field("gtid", current_gtid_)
          .Field("error", GetLastError())
          .Error();
      return MakeUnexpected(MakeError(ErrorCode::kMySQLBinlogError, GetLastError()));
    }
  }

  // Create dedicated connection for binlog reading
  // We need a separate connection because mysql_binlog_* functions
  // are blocking and cannot share a connection with other queries
  mygram::utils::StructuredLog()
      .Event("binlog_connection_init")
      .Field("gtid", current_gtid_)
      .Message("Creating dedicated binlog connection")
      .Info();
  // Use a short read timeout for the binlog connection so that Stop()
  // completes quickly. The reader thread checks should_stop_ after each
  // mysql_binlog_fetch() return, so this value bounds the max Stop() latency.
  binlog_connection_ = std::make_unique<Connection>(internal::MakeSubConfig(connection_.GetConfig(), 5));
  auto connect_result = binlog_connection_->Connect("binlog worker");
  if (!connect_result) {
    SetLastError("Failed to create binlog connection: " + connect_result.error().message());
    mygram::utils::LogBinlogError("connection_failed", current_gtid_, GetLastError());
    // StartupGuard will clean up binlog_connection_
    return MakeUnexpected(connect_result.error());
  }

  // Create dedicated metadata connection for column name queries (thread-safe)
  // This avoids using the main connection_ from the reader thread
  metadata_connection_ = std::make_unique<Connection>(internal::MakeSubConfig(connection_.GetConfig()));
  auto metadata_connect_result = metadata_connection_->Connect("metadata");
  if (!metadata_connect_result) {
    SetLastError("Failed to create metadata connection: " + metadata_connect_result.error().message());
    mygram::utils::LogBinlogError("metadata_connection_failed", current_gtid_, GetLastError());
    // StartupGuard will clean up
    return MakeUnexpected(metadata_connect_result.error());
  }

  // Validate connection before starting replication
  if (!ValidateConnection()) {
    mygram::utils::LogBinlogError("validation_failed", current_gtid_, GetLastError());
    // StartupGuard will clean up binlog_connection_
    return MakeUnexpected(MakeError(ErrorCode::kMySQLBinlogError, GetLastError()));
  }
  mygram::utils::StructuredLog().Event("binlog_connection_validated").Field("gtid", current_gtid_).Info();

  should_stop_ = false;
  // Note: running_ is already set to true by compare_exchange_strong above

  // Reset debug log counters for this run
  no_data_log_count_ = 0;
  skip_log_count_ = 0;

  try {
    // Start worker thread first
    worker_thread_ = std::make_unique<std::thread>(&BinlogReader::WorkerThreadFunc, this);

    // Start reader thread
    reader_thread_ = std::make_unique<std::thread>(&BinlogReader::ReaderThreadFunc, this);

    mygram::utils::StructuredLog()
        .Event("binlog_reader_started")
        .Field("host", connection_.GetConfig().host)
        .Field("port", static_cast<uint64_t>(connection_.GetConfig().port))
        .Field("gtid", current_gtid_)
        .Field("server_id", static_cast<int64_t>(config_.server_id))
        .Info();
    guard.success = true;  // Mark start as successful - StartupGuard won't clean up
    return {};
  } catch (const std::exception& e) {
    SetLastError(std::string("Failed to start threads: ") + e.what());
    mygram::utils::LogBinlogError("thread_start_failed", current_gtid_, GetLastError());
    // StartupGuard destructor will clean up all resources automatically
    return MakeUnexpected(MakeError(ErrorCode::kMySQLBinlogError, GetLastError()));
  }
}

mygram::utils::Expected<void, mygram::utils::Error> BinlogReader::StartFromGtid(const std::string& gtid) {
  // Set the GTID position first
  SetCurrentGTID(gtid);

  // Then start the binlog reader (which will use the set GTID)
  return Start();
}

void BinlogReader::Stop() {
  // Serialize Stop() calls to prevent concurrent join races (#5)
  // This mutex ensures only one Stop() runs at a time, and subsequent
  // callers wait for the first to complete rather than racing on should_stop_.
  std::lock_guard<std::mutex> lock(stop_mutex_);

  // Check if already stopped or never started
  if (!running_.load()) {
    return;
  }

  mygram::utils::StructuredLog().Event("binlog_reader_stopping").Info();
  should_stop_ = true;

  // Wake up worker thread
  queue_cv_.notify_all();
  queue_full_cv_.notify_all();

  // Note: We cannot call binlog_connection_->Close() from this thread
  // because mysql_close() is not thread-safe when mysql_binlog_fetch()
  // is blocking in another thread. Instead, the binlog connection uses
  // read_timeout=5s so the reader thread checks should_stop_ within 5s
  // and exits gracefully.

  // Wait for threads to finish (reader will exit due to read timeout + should_stop_ check)
  // Always attempt to join regardless of running_ state (#6) - threads may have
  // self-exited due to errors but still need to be joined to avoid std::terminate.
  if (reader_thread_ && reader_thread_->joinable()) {
    reader_thread_->join();
    reader_thread_.reset();
  }

  if (worker_thread_ && worker_thread_->joinable()) {
    worker_thread_->join();
    worker_thread_.reset();
  }

  // Now it's safe to destroy connections (all threads have exited)
  binlog_connection_.reset();
  metadata_connection_.reset();

  running_ = false;
  should_stop_ = false;  // Reset for next Start()
  mygram::utils::StructuredLog()
      .Event("binlog_reader_stopped")
      .Field("events_processed", static_cast<int64_t>(processed_events_.load()))
      .Info();
}

std::string BinlogReader::GetCurrentGTID() const {
  std::scoped_lock lock(gtid_mutex_);
  return current_gtid_;
}

void BinlogReader::SetCurrentGTID(const std::string& gtid) {
  std::scoped_lock lock(gtid_mutex_);
  current_gtid_ = gtid;
  // Update executed_gtid_set_ for REPLICATION STATUS display only.
  // Reconnection always uses current_gtid_ (via ConvertSingleGtidToRange),
  // never executed_gtid_set_, to prevent skipping undelivered events.
  if (gtid.find('-') != std::string::npos || gtid.find(',') != std::string::npos) {
    executed_gtid_set_ = gtid;
  }
  mygram::utils::StructuredLog().Event("binlog_gtid_set").Field("gtid", gtid).Info();
}

size_t BinlogReader::GetQueueSize() const {
  std::scoped_lock lock(queue_mutex_);
  return event_queue_.size();
}

}  // namespace mygramdb::mysql

// NOLINTEND(cppcoreguidelines-pro-*,cppcoreguidelines-avoid-*,readability-magic-numbers)

#endif  // USE_MYSQL
