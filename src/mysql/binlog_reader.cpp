/**
 * @file binlog_reader.cpp
 * @brief Binlog reader implementation
 *
 * Note: This file contains MySQL binlog protocol parsing code.
 * Some modern C++ guidelines are relaxed for protocol compatibility.
 */

#include "mysql/binlog_reader.h"

#ifdef USE_MYSQL

#include <spdlog/spdlog.h>

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstring>
#include <utility>

#include "mysql/binlog_event_parser.h"
#include "mysql/binlog_event_processor.h"
#include "mysql/binlog_event_types.h"
#include "mysql/gtid_encoder.h"
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
      current_gtid_(config.start_gtid),
      server_stats_(stats) {}

// Multi-table mode constructor
BinlogReader::BinlogReader(Connection& connection,
                           std::unordered_map<std::string, server::TableContext*> table_contexts,
                           config::MysqlConfig mysql_config, const Config& config, server::ServerStats* stats)
    : connection_(connection),
      table_contexts_(std::move(table_contexts)),
      multi_table_mode_(true),
      mysql_config_(std::move(mysql_config)),
      config_(config),
      current_gtid_(config.start_gtid),
      server_stats_(stats) {}

BinlogReader::~BinlogReader() {
  Stop();
}

mygram::utils::Expected<void, mygram::utils::Error> BinlogReader::Start() {
  using mygram::utils::Error;
  using mygram::utils::ErrorCode;
  using mygram::utils::MakeError;
  using mygram::utils::MakeUnexpected;

  // Check if currently stopping (running_ is true but should_stop_ is also true)
  if (should_stop_.load()) {
    last_error_ = "Binlog reader is stopping, please wait";
    return MakeUnexpected(MakeError(ErrorCode::kMySQLBinlogError, last_error_));
  }

  // Atomically check and set running_ to prevent concurrent Start() calls
  bool expected = false;
  if (!running_.compare_exchange_strong(expected, true)) {
    last_error_ = "Binlog reader is already running";
    return MakeUnexpected(MakeError(ErrorCode::kMySQLBinlogError, last_error_));
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

      // Clear running flag
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
    last_error_ = "server_id must be set to a non-zero value for binlog replication";
    mygram::utils::LogBinlogError("invalid_server_id", current_gtid_, last_error_);
    return MakeUnexpected(MakeError(ErrorCode::kMySQLBinlogError, last_error_));
  }

  // Check MySQL connection
  if (!connection_.IsConnected()) {
    last_error_ = "MySQL connection not established";
    mygram::utils::LogBinlogError("startup_failed", current_gtid_, last_error_);
    return MakeUnexpected(MakeError(ErrorCode::kMySQLDisconnected, last_error_));
  }

  // Check if GTID mode is enabled (using main connection)
  if (!connection_.IsGTIDModeEnabled()) {
    last_error_ =
        "GTID mode is not enabled on MySQL server. "
        "Please enable GTID mode (gtid_mode=ON) for binlog replication.";
    mygram::utils::LogBinlogError("gtid_mode_disabled", current_gtid_, last_error_);
    return MakeUnexpected(MakeError(ErrorCode::kMySQLBinlogError, last_error_));
  }

  // Validate primary keys for all tables
  if (multi_table_mode_) {
    for (const auto& [table_name, ctx] : table_contexts_) {
      std::string validation_error;
      if (!connection_.ValidateUniqueColumn(connection_.GetConfig().database, ctx->config.name, ctx->config.primary_key,
                                            validation_error)) {
        last_error_ = "Primary key validation failed for table '";
        last_error_ += table_name;
        last_error_ += "': ";
        last_error_ += validation_error;
        mygram::utils::StructuredLog()
            .Event("binlog_error")
            .Field("type", "primary_key_validation_failed")
            .Field("table", table_name)
            .Field("gtid", current_gtid_)
            .Field("error", last_error_)
            .Error();
        return MakeUnexpected(MakeError(ErrorCode::kMySQLBinlogError, last_error_));
      }
    }
  } else {
    // Single-table mode
    std::string validation_error;
    if (!connection_.ValidateUniqueColumn(connection_.GetConfig().database, table_config_.name,
                                          table_config_.primary_key, validation_error)) {
      last_error_ = "Primary key validation failed: " + validation_error;
      mygram::utils::StructuredLog()
          .Event("binlog_error")
          .Field("type", "primary_key_validation_failed")
          .Field("table", table_config_.name)
          .Field("gtid", current_gtid_)
          .Field("error", last_error_)
          .Error();
      return MakeUnexpected(MakeError(ErrorCode::kMySQLBinlogError, last_error_));
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
  mysql::Connection::Config binlog_conn_config;
  binlog_conn_config.host = connection_.GetConfig().host;
  binlog_conn_config.port = connection_.GetConfig().port;
  binlog_conn_config.user = connection_.GetConfig().user;
  binlog_conn_config.password = connection_.GetConfig().password;
  binlog_conn_config.database = connection_.GetConfig().database;
  binlog_conn_config.connect_timeout = connection_.GetConfig().connect_timeout;
  // Use a short read timeout (5 seconds) for the binlog connection
  // This allows the reader thread to check should_stop_ periodically
  // and exit gracefully when Stop() is called (within 5 seconds max)
  binlog_conn_config.read_timeout = 5;
  binlog_conn_config.write_timeout = connection_.GetConfig().write_timeout;

  binlog_connection_ = std::make_unique<Connection>(binlog_conn_config);
  auto connect_result = binlog_connection_->Connect("binlog worker");
  if (!connect_result) {
    last_error_ = "Failed to create binlog connection: " + connect_result.error().message();
    mygram::utils::LogBinlogError("connection_failed", current_gtid_, last_error_);
    // StartupGuard will clean up binlog_connection_
    return MakeUnexpected(connect_result.error());
  }

  // Create dedicated metadata connection for column name queries (thread-safe)
  // This avoids using the main connection_ from the reader thread
  mysql::Connection::Config metadata_conn_config;
  metadata_conn_config.host = connection_.GetConfig().host;
  metadata_conn_config.port = connection_.GetConfig().port;
  metadata_conn_config.user = connection_.GetConfig().user;
  metadata_conn_config.password = connection_.GetConfig().password;
  metadata_conn_config.database = connection_.GetConfig().database;
  metadata_conn_config.connect_timeout = connection_.GetConfig().connect_timeout;
  metadata_conn_config.read_timeout = connection_.GetConfig().read_timeout;
  metadata_conn_config.write_timeout = connection_.GetConfig().write_timeout;

  metadata_connection_ = std::make_unique<Connection>(metadata_conn_config);
  auto metadata_connect_result = metadata_connection_->Connect("metadata");
  if (!metadata_connect_result) {
    last_error_ = "Failed to create metadata connection: " + metadata_connect_result.error().message();
    mygram::utils::LogBinlogError("metadata_connection_failed", current_gtid_, last_error_);
    // StartupGuard will clean up
    return MakeUnexpected(metadata_connect_result.error());
  }

  // Validate connection before starting replication
  if (!ValidateConnection()) {
    mygram::utils::LogBinlogError("validation_failed", current_gtid_, last_error_);
    // StartupGuard will clean up binlog_connection_
    return MakeUnexpected(MakeError(ErrorCode::kMySQLBinlogError, last_error_));
  }
  mygram::utils::StructuredLog().Event("binlog_connection_validated").Field("gtid", current_gtid_).Info();

  // Refresh executed GTID set so the initial binlog open has the full set
  RefreshExecutedGtidSet();

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
    last_error_ = std::string("Failed to start threads: ") + e.what();
    mygram::utils::LogBinlogError("thread_start_failed", current_gtid_, last_error_);
    // StartupGuard destructor will clean up all resources automatically
    return MakeUnexpected(MakeError(ErrorCode::kMySQLBinlogError, last_error_));
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
  // is blocking in another thread. Instead, the binlog connection is
  // configured with a short read timeout (5 seconds) so the reader thread
  // will check should_stop_ periodically and exit gracefully.

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
  // If the GTID contains a range (e.g., "uuid:1-100" or "uuid1:1-100,uuid2:1-50"),
  // also update executed_gtid_set_ for reconnection use.
  // Single GTIDs (e.g., "uuid:101") from event processing should NOT overwrite it.
  if (gtid.find('-') != std::string::npos || gtid.find(',') != std::string::npos) {
    executed_gtid_set_ = gtid;
  }
  mygram::utils::StructuredLog().Event("binlog_gtid_set").Field("gtid", gtid).Info();
}

size_t BinlogReader::GetQueueSize() const {
  std::scoped_lock lock(queue_mutex_);
  return event_queue_.size();
}

void BinlogReader::ReaderThreadFunc() {
  mygram::utils::StructuredLog().Event("binlog_reader_thread_started").Info();

  // Get starting GTID
  std::string gtid_set;
  {
    std::scoped_lock lock(gtid_mutex_);
    if (!current_gtid_.empty()) {
      gtid_set = current_gtid_;
      mygram::utils::StructuredLog().Event("binlog_replication_start").Field("gtid", gtid_set).Info();
    }
  }

  // Local GTID tracking for reader thread only - NOT used for reconnection.
  // Only the worker thread updates current_gtid_ (via UpdateCurrentGTID) to
  // ensure reconnection uses the last *processed* position, not the last *received* position.
  std::string reader_last_gtid;
  {
    std::scoped_lock lock(gtid_mutex_);
    reader_last_gtid = current_gtid_;
  }

  // Main reconnection loop (infinite retries)
  int reconnect_attempt = 0;
  bool idle_timeout_reconnect = false;  // Track if reconnecting due to idle timeout

  while (!should_stop_) {
    // Disable binlog checksums for this connection
    // We don't verify checksums yet, so ask the server to send events without them
    if (mysql_query(binlog_connection_->GetHandle(), "SET @source_binlog_checksum='NONE'") != 0) {
      last_error_ = "Failed to disable binlog checksum: " + binlog_connection_->GetLastError();

      // On first attempt (reconnect_attempt == 0), this is likely a read timeout recovery
      // Use debug logging to avoid noisy error logs during normal idle reconnects
      if (reconnect_attempt == 0) {
        mygram::utils::StructuredLog()
            .Event("binlog_debug")
            .Field("action", "checksum_query_failed_idle_reconnect")
            .Field("error", last_error_)
            .Debug();
      } else {
        mygram::utils::LogBinlogError("checksum_disable_failed", GetCurrentGTID(), last_error_, reconnect_attempt);
      }

      // Retry connection after delay with exponential backoff (capped at 10x) (#4)
      reconnect_attempt = std::min(reconnect_attempt + 1, 10);
      int delay_ms = config_.reconnect_delay_ms * reconnect_attempt;
      mygram::utils::StructuredLog()
          .Event("binlog_debug")
          .Field("action", "retry_connection")
          .Field("delay_ms", static_cast<int64_t>(delay_ms))
          .Field("attempt", static_cast<int64_t>(reconnect_attempt))
          .Debug();
      std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));

      // Check if stop was requested during sleep
      if (should_stop_) {
        mygram::utils::StructuredLog().Event("binlog_debug").Field("action", "stop_requested_during_retry").Debug();
        break;
      }

      // Full reconnect (close, reinitialize, connect)
      // Use silent mode on first attempt since this is likely a normal idle timeout recovery
      auto reconnect_result = binlog_connection_->Reconnect(reconnect_attempt == 1 /* silent on first attempt */);
      if (!reconnect_result) {
        mygram::utils::LogBinlogError("reconnect_failed", GetCurrentGTID(), reconnect_result.error().message(),
                                      reconnect_attempt + 1);
        reconnect_attempt++;  // Increment to show error on subsequent failures
        continue;
      }
      // Only log reconnection on non-silent (subsequent) attempts
      if (reconnect_attempt > 0) {
        mygram::utils::StructuredLog().Event("binlog_reconnected").Field("gtid", GetCurrentGTID()).Info();
      }
      mygram::utils::StructuredLog()
          .Event("binlog_debug")
          .Field("action", "reconnected_after_checksum_failure")
          .Debug();
      RefreshExecutedGtidSet();

      // Validate connection after reconnection (detect failover, invalid servers)
      if (!ValidateConnection()) {
        // Validation failed - server is invalid, stop replication
        mygram::utils::StructuredLog()
            .Event("binlog_error")
            .Field("type", "connection_validation_failed")
            .Field("context", "after_reconnect")
            .Field("error", last_error_)
            .Error();
        should_stop_ = true;
        break;
      }
      mygram::utils::StructuredLog()
          .Event("binlog_debug")
          .Field("action", "connection_validated_after_reconnect")
          .Debug();
      RefreshExecutedGtidSet();

      // NOLINTNEXTLINE(clang-analyzer-deadcode.DeadStores) - Used in next iteration after continue
      reconnect_attempt = 0;  // Reset delay counter after successful reconnection
      continue;
    }
    mygram::utils::StructuredLog().Event("binlog_debug").Field("action", "checksums_disabled").Debug();

    // Initialize MYSQL_RPL structure for binlog reading
    MYSQL_RPL rpl{};
    rpl.file_name_length = 0;  // 0 means start from current position
    rpl.file_name = nullptr;
    rpl.start_position = 4;             // Skip magic number at start of binlog
    rpl.server_id = config_.server_id;  // Use configured server ID for replica
    rpl.flags = MYSQL_RPL_GTID;         // Use GTID mode (allow heartbeat events)

    // Use current_gtid_ (last processed position) for reconnection, falling back to executed_gtid_set_
    std::string current_gtid;
    {
      std::scoped_lock lock(gtid_mutex_);
      // Use current_gtid_ (last *processed* position) for reconnection.
      // executed_gtid_set_ (from @@GLOBAL.gtid_executed) may include events
      // the client hasn't processed yet, causing them to be skipped on reconnect.
      // Fall back to executed_gtid_set_ only on initial connection when current_gtid_ is empty.
      if (!current_gtid_.empty()) {
        current_gtid = ConvertSingleGtidToRange(current_gtid_);
      } else {
        current_gtid = executed_gtid_set_;
      }
    }

    // Encode GTID set to binary format if we have one
    // Use local variable to avoid race condition - gtid_encoded_data_ must persist
    // during mysql_binlog_open() call which accesses it via callback
    std::vector<uint8_t> local_gtid_encoded;
    if (!current_gtid.empty()) {
      // Encode GTID set using our encoder
      auto encode_result = mygramdb::mysql::GtidEncoder::Encode(current_gtid);
      if (!encode_result) {
        last_error_ = "Failed to encode GTID set: " + encode_result.error().message();
        mygram::utils::StructuredLog()
            .Event("binlog_error")
            .Field("type", "gtid_encode_failed")
            .Field("gtid", current_gtid)
            .Field("error", last_error_)
            .Error();
        should_stop_ = true;
        break;
      }
      local_gtid_encoded = std::move(*encode_result);

      // Also update member variable under lock for consistency with other readers
      {
        std::lock_guard<std::mutex> lock(gtid_mutex_);
        gtid_encoded_data_ = local_gtid_encoded;
      }

      // Use callback approach: MySQL will call our callback to encode the GTID into the packet
      rpl.gtid_set_encoded_size = local_gtid_encoded.size();
      rpl.gtid_set_arg = &local_gtid_encoded;                // Pass pointer to local data
      rpl.fix_gtid_set = &BinlogReader::FixGtidSetCallback;  // Static callback function

      mygram::utils::StructuredLog()
          .Event("binlog_debug")
          .Field("action", "using_gtid_set")
          .Field("gtid", current_gtid)
          .Field("encoded_bytes", static_cast<uint64_t>(local_gtid_encoded.size()))
          .Debug();
    } else {
      // Empty GTID set: receive all events from current binlog position
      rpl.gtid_set_encoded_size = 0;
      rpl.gtid_set_arg = nullptr;
      rpl.fix_gtid_set = nullptr;
      mygram::utils::StructuredLog().Event("binlog_debug").Field("action", "using_empty_gtid_set").Debug();
    }

    // Open binlog stream (local_gtid_encoded must be alive during this call)
    if (mysql_binlog_open(binlog_connection_->GetHandle(), &rpl) != 0) {
      unsigned int err_no = mysql_errno(binlog_connection_->GetHandle());
      last_error_ = "Failed to open binlog stream: " + binlog_connection_->GetLastError();

      // Check for binlog purged error (errno 1236) - non-recoverable, stop immediately
      if (err_no == 1236) {
        mygram::utils::StructuredLog()
            .Event("binlog_error")
            .Field("type", "binlog_purged")
            .Field("gtid", GetCurrentGTID())
            .Field("error", last_error_)
            .Field("errno", static_cast<int64_t>(err_no))
            .Field("message",
                   "Binlog position no longer available on server. "
                   "GTID position has been purged. "
                   "Manual intervention required: run SYNC command to establish new position.")
            .Error();
        should_stop_ = true;
        break;
      }

      // Other errors - retry with reconnection and exponential backoff
      reconnect_attempt = std::min(reconnect_attempt + 1, 10);
      int delay_ms = config_.reconnect_delay_ms * reconnect_attempt;
      mygram::utils::LogBinlogError("stream_open_failed", GetCurrentGTID(), last_error_, reconnect_attempt);

      mygram::utils::StructuredLog()
          .Event("binlog_debug")
          .Field("action", "retry_connection")
          .Field("delay_ms", static_cast<int64_t>(delay_ms))
          .Field("attempt", static_cast<int64_t>(reconnect_attempt))
          .Debug();
      std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));

      // Check if stop was requested during sleep
      if (should_stop_) {
        mygram::utils::StructuredLog().Event("binlog_debug").Field("action", "stop_requested_during_retry").Debug();
        break;
      }

      // Full reconnect (close + reinit + connect) to avoid reusing corrupted handle (#3)
      auto reconnect_result = binlog_connection_->Reconnect();
      if (!reconnect_result) {
        mygram::utils::LogBinlogError("reconnect_failed", GetCurrentGTID(), reconnect_result.error().message(),
                                      reconnect_attempt + 1);
      } else {
        // Validate connection after reconnection (detect failover, invalid servers)
        if (!ValidateConnection()) {
          // Validation failed - server is invalid, stop replication
          mygram::utils::StructuredLog()
              .Event("binlog_error")
              .Field("type", "connection_validation_failed")
              .Field("context", "after_reconnect")
              .Field("error", last_error_)
              .Error();
          should_stop_ = true;
          break;
        }
        mygram::utils::StructuredLog()
            .Event("binlog_connection_validated")
            .Field("context", "after_reconnect")
            .Field("gtid", GetCurrentGTID())
            .Debug();
        RefreshExecutedGtidSet();

        // NOLINTNEXTLINE(clang-analyzer-deadcode.DeadStores) - Used in next iteration after continue
        reconnect_attempt = 0;  // Reset delay counter after successful reconnection
      }
      continue;
    }

    // Log stream opened - use Debug level for idle timeout reconnects to avoid noisy logs
    if (idle_timeout_reconnect) {
      mygram::utils::StructuredLog().Event("binlog_stream_opened").Field("gtid", GetCurrentGTID()).Debug();
      idle_timeout_reconnect = false;  // Reset the flag
    } else {
      mygram::utils::StructuredLog().Event("binlog_stream_opened").Field("gtid", GetCurrentGTID()).Info();
    }
    reconnect_attempt = 0;  // Reset reconnect counter on success

    // Read binlog events
    int event_count = 0;
    bool connection_lost = false;

    while (!should_stop_ && !connection_lost) {
      // Fetch next binlog event
      mygram::utils::StructuredLog().Event("binlog_debug").Field("action", "calling_binlog_fetch").Debug();
      int result = mysql_binlog_fetch(binlog_connection_->GetHandle(), &rpl);

      // Log first fetch result for debugging
      if (event_count == 0) {
        mygram::utils::StructuredLog()
            .Event("binlog_debug")
            .Field("action", "first_binlog_fetch_result")
            .Field("result", static_cast<int64_t>(result))
            .Field("size", static_cast<uint64_t>(rpl.size))
            .Debug();
      }

      // Check should_stop_ immediately after blocking call to avoid use-after-free
      // (Stop() may have closed the connection while we were blocked)
      if (should_stop_) {
        mygram::utils::StructuredLog().Event("binlog_debug").Field("action", "stop_requested_exiting").Debug();
        break;
      }

      if (result != 0) {
        unsigned int err_no = mysql_errno(binlog_connection_->GetHandle());
        const char* err_str = mysql_error(binlog_connection_->GetHandle());
        last_error_ =
            "Failed to fetch binlog event: " + std::string(err_str) + " (errno: " + std::to_string(err_no) + ")";

        // Check if this is a read timeout (expected with 5-second timeout)
        // errno 2013 (CR_SERVER_LOST) can indicate either timeout or actual connection loss
        if (should_stop_) {
          // Timeout while stopping - exit gracefully
          mygram::utils::StructuredLog().Event("binlog_debug").Field("action", "read_timeout_stop_requested").Debug();
          break;
        }

        // Check if this is likely a read timeout (errno 2013 during idle)
        // Read timeouts are expected every 5 seconds when there are no binlog events
        // The TCP connection is broken by the timeout, so we need a full reconnect
        if (err_no == 2013) {
          // Full reconnect (closes handle, reinitializes, and connects)
          // This is expected behavior during idle periods, so use silent mode
          // Note: Do NOT call mysql_binlog_close() before Reconnect() because
          // the broken TCP connection can leave the handle in an invalid state
          mygram::utils::StructuredLog().Event("binlog_debug").Field("action", "read_timeout_full_reconnect").Debug();
          auto reconnect_result = binlog_connection_->Reconnect(true /* silent */);
          if (!reconnect_result) {
            // Failed to reconnect - log and retry with backoff
            mygram::utils::LogBinlogError("reconnect_failed", GetCurrentGTID(), reconnect_result.error().message(), 1);
            std::this_thread::sleep_for(std::chrono::milliseconds(config_.reconnect_delay_ms));
          } else {
            RefreshExecutedGtidSet();
          }
          idle_timeout_reconnect = true;  // Mark this as an idle timeout reconnect
          // NOLINTNEXTLINE(clang-analyzer-deadcode.DeadStores) - Documents intent; break exits to outer loop
          connection_lost = true;
          break;  // Exit inner loop to retry from outer loop
        }

        // Check if this is a real connection loss (server gone away)
        if (err_no == 2006) {  // Server gone away - actual connection loss
          mygram::utils::StructuredLog()
              .Event("binlog_connection_lost")
              .Field("error", last_error_)
              .Field("fetch_result", static_cast<int64_t>(result))
              .Field("gtid", GetCurrentGTID())
              .Warn();
          // NOLINTNEXTLINE(clang-analyzer-deadcode.DeadStores) - Documents intent; break exits to outer loop
          connection_lost = true;

          // Close current binlog stream
          mysql_binlog_close(binlog_connection_->GetHandle(), &rpl);

          // Wait before reconnecting with exponential backoff (capped at 10x)
          reconnect_attempt = std::min(reconnect_attempt + 1, 10);
          int delay_ms = config_.reconnect_delay_ms * reconnect_attempt;
          mygram::utils::StructuredLog()
              .Event("binlog_debug")
              .Field("action", "reconnect_attempt")
              .Field("attempt", static_cast<int64_t>(reconnect_attempt))
              .Field("delay_ms", static_cast<int64_t>(delay_ms))
              .Debug();
          std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));

          // Check again before reconnecting
          if (should_stop_) {
            mygram::utils::StructuredLog()
                .Event("binlog_debug")
                .Field("action", "stop_requested_during_reconnect")
                .Debug();
            break;
          }

          // Full reconnect (close + reinit + connect) to avoid reusing corrupted handle (#3)
          auto reconnect_result = binlog_connection_->Reconnect();
          if (!reconnect_result) {
            mygram::utils::LogBinlogError("reconnect_failed", GetCurrentGTID(), reconnect_result.error().message(),
                                          reconnect_attempt);
          } else {
            // Validate connection after reconnection (detect failover, invalid servers)
            if (!ValidateConnection()) {
              // Validation failed - server is invalid, stop replication
              mygram::utils::StructuredLog()
                  .Event("binlog_error")
                  .Field("type", "connection_validation_failed")
                  .Field("context", "after_reconnect")
                  .Field("error", last_error_)
                  .Error();
              should_stop_ = true;
              break;
            }
            mygram::utils::StructuredLog().Event("binlog_connection_restored").Info();
            RefreshExecutedGtidSet();

            // NOLINTNEXTLINE(clang-analyzer-deadcode.DeadStores) - Used after break exits inner loop
            reconnect_attempt = 0;  // Reset delay counter after successful reconnection
          }
          break;  // Exit inner loop to retry from outer loop
        }

        // Check for binlog purged error (errno 1236) - special handling required
        // This error means the GTID position we're requesting is no longer available in binlogs
        // Retrying with the same GTID will never succeed - must stop and wait for manual intervention (SYNC)
        if (err_no == 1236) {
          mygram::utils::StructuredLog()
              .Event("binlog_error")
              .Field("type", "binlog_purged")
              .Field("gtid", GetCurrentGTID())
              .Field("error", last_error_)
              .Field("errno", static_cast<int64_t>(err_no))
              .Field("message",
                     "Binlog position no longer available on server. "
                     "GTID position has been purged. "
                     "Manual intervention required: run SYNC command to establish new position.")
              .Error();
          should_stop_ = true;
          break;
        }

        // Other non-recoverable errors - log and stop
        mygram::utils::StructuredLog()
            .Event("binlog_error")
            .Field("type", "fetch_non_recoverable_error")
            .Field("gtid", GetCurrentGTID())
            .Field("error", last_error_)
            .Field("result_code", static_cast<int64_t>(result))
            .Field("errno", static_cast<int64_t>(err_no))
            .Error();
        should_stop_ = true;
        break;
      }

      // Check if we have data
      if (rpl.size == 0 || rpl.buffer == nullptr) {
        // No data available (EOF or keepalive)
        // Log at debug level to diagnose why no events are being received
        int current_no_data = no_data_log_count_.fetch_add(1) + 1;
        if (current_no_data % 100 == 1) {  // Log every 100th occurrence to avoid spam
          mygram::utils::StructuredLog()
              .Event("binlog_debug")
              .Field("action", "no_data_received")
              .Field("count", static_cast<int64_t>(current_no_data))
              .Field("size", static_cast<uint64_t>(rpl.size))
              .Debug();
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        continue;
      }

      event_count++;
      mygram::utils::StructuredLog()
          .Event("binlog_debug")
          .Field("action", "received_binlog_event")
          .Field("event_num", static_cast<int64_t>(event_count))
          .Field("size", static_cast<uint64_t>(rpl.size))
          .Debug();

      // MySQL binlog buffer format (from mysql-8.4.7/sql-common/client.cc:mysql_binlog_fetch):
      // rpl.buffer[0] = OK byte (0x00) from network protocol
      // rpl.buffer[1] = Start of actual binlog event data
      // Event type is at rpl.buffer[1 + EVENT_TYPE_OFFSET] where EVENT_TYPE_OFFSET=4
      // (see mysql-8.4.7/libs/mysql/binlog/event/binlog_event.h:431)
      //
      // We skip the OK byte here and pass rpl.buffer+1 to parsers, so parsers can use
      // standard LOG_EVENT_HEADER_LEN (19 bytes) offsets directly.
      const unsigned char* event_buffer = rpl.buffer + 1;
      const unsigned long event_length = rpl.size - 1;

      // Check for GTID events first (need to update current_gtid)
      if (event_length >= 19) {
        auto event_type = static_cast<MySQLBinlogEventType>(event_buffer[4]);

        if (event_type == MySQLBinlogEventType::GTID_LOG_EVENT) {
          auto gtid_opt = BinlogEventParser::ExtractGTID(event_buffer, event_length);
          if (gtid_opt) {
            reader_last_gtid = gtid_opt.value();
            mygram::utils::StructuredLog()
                .Event("binlog_debug")
                .Field("action", "reader_gtid_received")
                .Field("gtid", reader_last_gtid)
                .Debug();
          }
          continue;  // Skip to next event
        }

        if (event_type == MySQLBinlogEventType::GTID_TAGGED_LOG_EVENT) {
          // MySQL 8.4+ tagged GTID (UUID:TAG:GNO)
          auto gtid_opt = BinlogEventParser::ExtractTaggedGTID(event_buffer, event_length);
          if (gtid_opt) {
            reader_last_gtid = gtid_opt.value();
            mygram::utils::StructuredLog()
                .Event("binlog_debug")
                .Field("action", "reader_tagged_gtid_received")
                .Field("gtid", reader_last_gtid)
                .Debug();
          } else {
            mygram::utils::StructuredLog()
                .Event("binlog_warning")
                .Field("type", "gtid_tagged_parse_fallback")
                .Field("message", "Failed to parse GTID_TAGGED_LOG_EVENT, keeping previous GTID")
                .Warn();
          }
          continue;  // Skip to next event
        }

        if (event_type == MySQLBinlogEventType::TABLE_MAP_EVENT) {
          mygram::utils::StructuredLog()
              .Event("binlog_debug")
              .Field("action", "table_map_event_detected")
              .Field("event_num", static_cast<int64_t>(event_count))
              .Debug();
          auto metadata_opt = BinlogEventParser::ParseTableMapEvent(event_buffer, event_length);
          if (!metadata_opt) {
            mygram::utils::StructuredLog()
                .Event("binlog_error")
                .Field("type", "table_map_parse_failed")
                .Field("event_num", static_cast<int64_t>(event_count))
                .Error();
          } else {
            mygram::utils::StructuredLog()
                .Event("binlog_debug")
                .Field("action", "table_map_parsed")
                .Field("database", metadata_opt->database_name)
                .Field("table", metadata_opt->table_name)
                .Field("table_id", metadata_opt->table_id)
                .Debug();

            // Check if this table is monitored before fetching column names
            // This avoids permission errors for tables we don't have SELECT access to
            bool is_monitored_table = false;
            if (multi_table_mode_) {
              is_monitored_table = table_contexts_.find(metadata_opt->table_name) != table_contexts_.end();
            } else {
              is_monitored_table = (metadata_opt->table_name == table_config_.name);
            }

            if (is_monitored_table) {
              if (!FetchColumnNames(metadata_opt.value())) {
                mygram::utils::StructuredLog()
                    .Event("binlog_warning")
                    .Field("type", "column_fetch_failed")
                    .Field("database", metadata_opt->database_name)
                    .Field("table", metadata_opt->table_name)
                    .Field("gtid", GetCurrentGTID())
                    .Message("Failed to fetch column names, using col_N placeholders")
                    .Warn();
              }
            } else {
              mygram::utils::StructuredLog()
                  .Event("binlog_debug")
                  .Field("action", "skipping_non_monitored_table")
                  .Field("database", metadata_opt->database_name)
                  .Field("table", metadata_opt->table_name)
                  .Debug();
            }

            auto add_result = table_metadata_cache_.AddOrUpdate(metadata_opt->table_id, metadata_opt.value());

            if (add_result == TableMetadataCache::AddResult::kSchemaChanged) {
              // Invalidate column name cache on schema change to avoid stale names
              {
                std::lock_guard<std::mutex> lock(column_names_cache_mutex_);
                std::string cache_key = metadata_opt->database_name + "." + metadata_opt->table_name;
                column_names_cache_.erase(cache_key);
              }
              // Re-fetch column names with fresh data
              if (is_monitored_table) {
                FetchColumnNames(metadata_opt.value());
              }
              mygram::utils::StructuredLog()
                  .Event("binlog_schema_change")
                  .Field("database", metadata_opt->database_name)
                  .Field("table", metadata_opt->table_name)
                  .Field("table_id", metadata_opt->table_id)
                  .Field("gtid", GetCurrentGTID())
                  .Message("Schema change detected, column name cache invalidated")
                  .Warn();
            }

            mygram::utils::StructuredLog()
                .Event("binlog_debug")
                .Field("action",
                       add_result == TableMetadataCache::AddResult::kAdded ? "cached_table_map" : "updated_table_map")
                .Field("database", metadata_opt->database_name)
                .Field("table", metadata_opt->table_name)
                .Field("table_id", metadata_opt->table_id)
                .Debug();
          }
          continue;  // Skip to next event
        }

        // Detect unsupported event types that cause silent data loss at runtime.
        // These are checked at connection time, but settings can be changed dynamically.
        if (event_type == MySQLBinlogEventType::TRANSACTION_PAYLOAD_EVENT) {
          last_error_ =
              "Received TRANSACTION_PAYLOAD_EVENT: binlog_transaction_compression was enabled "
              "on the server after initial validation. Compressed events cannot be decoded. "
              "Disable compression with: SET GLOBAL binlog_transaction_compression=OFF";
          mygram::utils::StructuredLog()
              .Event("binlog_fatal_error")
              .Field("type", "unsupported_runtime_event")
              .Field("event_type", "TRANSACTION_PAYLOAD_EVENT")
              .Error();
          should_stop_ = true;
          break;  // Exit read loop
        }

        if (event_type == MySQLBinlogEventType::PARTIAL_UPDATE_ROWS_EVENT) {
          last_error_ =
              "Received PARTIAL_UPDATE_ROWS_EVENT: binlog_row_value_options=PARTIAL_JSON was enabled "
              "on the server after initial validation. Partial JSON updates cannot be decoded. "
              "Disable with: SET GLOBAL binlog_row_value_options=''";
          mygram::utils::StructuredLog()
              .Event("binlog_fatal_error")
              .Field("type", "unsupported_runtime_event")
              .Field("event_type", "PARTIAL_UPDATE_ROWS_EVENT")
              .Error();
          should_stop_ = true;
          break;  // Exit read loop
        }
      }

      // Parse the binlog event using BinlogEventParser
      // Returns vector of events (empty if none, multiple for batch operations)
      auto events = BinlogEventParser::ParseBinlogEvent(
          event_buffer, event_length, reader_last_gtid, table_metadata_cache_, table_contexts_,
          multi_table_mode_ ? nullptr : &table_config_, multi_table_mode_, mysql_config_.datetime_timezone);

      if (!events.empty()) {
        mygram::utils::StructuredLog()
            .Event("binlog_debug")
            .Field("action", "parsed_events")
            .Field("count", static_cast<int64_t>(events.size()))
            .Debug();

        // Process ALL events in the batch (multi-row support)
        for (auto& event : events) {
          // Log data events at debug level (can be high volume in production)
          if (event.type == BinlogEventType::INSERT || event.type == BinlogEventType::UPDATE ||
              event.type == BinlogEventType::DELETE) {
            const char* event_type_str = "UNKNOWN";
            if (event.type == BinlogEventType::INSERT) {
              event_type_str = "INSERT";
            } else if (event.type == BinlogEventType::UPDATE) {
              event_type_str = "UPDATE";
            } else if (event.type == BinlogEventType::DELETE) {
              event_type_str = "DELETE";
            }
            mygram::utils::StructuredLog()
                .Event("binlog_debug")
                .Field("action", "binlog_data_event")
                .Field("event_type", event_type_str)
                .Field("table", event.table_name)
                .Field("pk", event.primary_key)
                .Debug();
          }

          PushEvent(std::make_unique<BinlogEvent>(std::move(event)));
        }
      } else {
        mygram::utils::StructuredLog().Event("binlog_debug").Field("action", "event_skipped").Debug();
      }
    }

    // Close binlog stream if still connected
    if (binlog_connection_ && binlog_connection_->IsConnected()) {
      mysql_binlog_close(binlog_connection_->GetHandle(), &rpl);
    }

    // Only exit on explicit stop request
    if (should_stop_) {
      break;
    }
    // If connection_lost is false (normal idle), continue loop to wait for events
    // If connection_lost is true (connection failure), continue loop for reconnect
  }

  mygram::utils::StructuredLog().Event("binlog_reader_thread_stopped").Info();

  // Note: Do NOT set running_ = false here. The running_ flag is managed
  // exclusively by Stop() to prevent a race where Stop() cannot join threads
  // because it sees running_ == false and takes the early return path (#6).
  // Stop() will always join threads and set running_ = false afterward.
}

void BinlogReader::WorkerThreadFunc() {
  mygram::utils::StructuredLog().Event("binlog_worker_thread_started").Info();

  // Process events until PopEvent returns nullptr
  // PopEvent returns nullptr only when: should_stop_ is true AND queue is empty
  // This ensures all pending events are processed during graceful shutdown
  while (true) {
    auto event = PopEvent();
    if (!event) {
      // Exit only when shutdown requested AND queue is empty
      break;
    }

    // Only update GTID and counter on successful processing
    if (ProcessEvent(*event)) {
      processed_events_++;
      UpdateCurrentGTID(event->gtid);
    } else {
      mygram::utils::StructuredLog()
          .Event("binlog_error")
          .Field("type", "event_processing_failed")
          .Field("table", event->table_name)
          .Field("primary_key", event->primary_key)
          .Field("gtid", event->gtid)
          .Error();
      // GTID is not updated on failure so event can be retried on reconnect
    }
  }

  mygram::utils::StructuredLog().Event("binlog_worker_thread_stopped").Info();
}

void BinlogReader::PushEvent(std::unique_ptr<BinlogEvent> event) {
  std::unique_lock<std::mutex> lock(queue_mutex_);

  // Wait if queue is full
  queue_full_cv_.wait(lock, [this] { return should_stop_ || event_queue_.size() < config_.queue_size; });

  if (should_stop_) {
    return;
  }

  event_queue_.push(std::move(event));
  queue_cv_.notify_one();
}

std::unique_ptr<BinlogEvent> BinlogReader::PopEvent() {
  std::unique_lock<std::mutex> lock(queue_mutex_);

  // Wait if queue is empty
  queue_cv_.wait(lock, [this] { return should_stop_ || !event_queue_.empty(); });

  if (should_stop_ && event_queue_.empty()) {
    return nullptr;
  }

  auto event = std::move(event_queue_.front());
  event_queue_.pop();

  // Notify reader thread that queue has space
  queue_full_cv_.notify_one();

  return event;
}

bool BinlogReader::ProcessEvent(const BinlogEvent& event) {
  // Determine which index/doc_store/config to use based on mode
  index::Index* current_index = nullptr;
  storage::DocumentStore* current_doc_store = nullptr;
  const config::TableConfig* current_config = nullptr;

  if (multi_table_mode_) {
    // Multi-table mode: lookup table from event
    auto table_iter = table_contexts_.find(event.table_name);
    if (table_iter == table_contexts_.end()) {
      // Event is for a table we're not tracking, skip silently
      // Log first few occurrences for debugging
      int current_count = skip_log_count_.fetch_add(1);
      if (current_count < 10) {
        mygram::utils::StructuredLog()
            .Event("binlog_event_skipped")
            .Field("table", event.table_name)
            .Field("reason", "non-tracked table")
            .Field("skip_count", static_cast<uint64_t>(current_count + 1))
            .Info();
      }
      if (server_stats_ != nullptr) {
        server_stats_->IncrementReplEventsSkippedOtherTables();
      }
      return true;
    }
    if (!table_iter->second->index || !table_iter->second->doc_store) {
      mygram::utils::StructuredLog()
          .Event("binlog_error")
          .Field("type", "null_table_context")
          .Field("table", event.table_name)
          .Field("gtid", event.gtid)
          .Field("error", "Table context has null index or doc_store")
          .Error();
      return false;
    }
    current_index = table_iter->second->index.get();
    current_doc_store = table_iter->second->doc_store.get();
    current_config = &table_iter->second->config;
  } else {
    // Single-table mode: skip events for other tables
    if (event.table_name != table_config_.name) {
      if (server_stats_ != nullptr) {
        server_stats_->IncrementReplEventsSkippedOtherTables();
      }
      return true;
    }
    current_index = index_;
    current_doc_store = doc_store_;
    current_config = &table_config_;
  }

  // Invalidate column names cache on ALTER TABLE DDL to avoid stale column name mappings
  // (TABLE_MAP_EVENT detects column count/type changes but not column renames)
  if (event.type == BinlogEventType::DDL) {
    std::string query_upper = event.text;
    std::transform(query_upper.begin(), query_upper.end(), query_upper.begin(), ::toupper);
    if (query_upper.find("ALTER") != std::string::npos) {
      std::lock_guard<std::mutex> lock(column_names_cache_mutex_);
      // Invalidate all cache entries containing this table name
      for (auto it = column_names_cache_.begin(); it != column_names_cache_.end();) {
        if (it->first.find(event.table_name) != std::string::npos) {
          mygram::utils::StructuredLog()
              .Event("binlog_cache_invalidation")
              .Field("type", "alter_table_column_names")
              .Field("table", event.table_name)
              .Field("cache_key", it->first)
              .Info();
          it = column_names_cache_.erase(it);
        } else {
          ++it;
        }
      }
    }
  }

  // Delegate to BinlogEventProcessor
  return BinlogEventProcessor::ProcessEvent(event, *current_index, *current_doc_store, *current_config, mysql_config_,
                                            server_stats_);
}

void BinlogReader::UpdateCurrentGTID(const std::string& gtid) {
  std::scoped_lock lock(gtid_mutex_);
  current_gtid_ = gtid;
  // Note: executed_gtid_set_ is intentionally NOT updated here.
  // UpdateCurrentGTID is called with single GTIDs from binlog events (e.g., "uuid:101").
  // The full GTID set for reconnection is maintained separately.
}

bool BinlogReader::RefreshExecutedGtidSet() {
  auto gtid_set = binlog_connection_->GetExecutedGTID();
  if (!gtid_set) {
    mygram::utils::StructuredLog()
        .Event("binlog_warning")
        .Field("type", "refresh_executed_gtid_failed")
        .Field("error", "Failed to query @@GLOBAL.gtid_executed")
        .Warn();
    return false;
  }
  {
    std::scoped_lock lock(gtid_mutex_);
    executed_gtid_set_ = *gtid_set;
  }
  mygram::utils::StructuredLog()
      .Event("binlog_debug")
      .Field("action", "refreshed_executed_gtid_set")
      .Field("gtid_set", *gtid_set)
      .Debug();
  return true;
}

// FetchColumnNames implementation (remains in BinlogReader as it accesses connection_)
bool BinlogReader::FetchColumnNames(TableMetadata& metadata) {
  std::string cache_key = metadata.database_name + "." + metadata.table_name;

  // Check cache first
  {
    std::lock_guard<std::mutex> lock(column_names_cache_mutex_);
    auto cache_it = column_names_cache_.find(cache_key);
    if (cache_it != column_names_cache_.end()) {
      // Cache hit: update column names from cache
      const auto& column_names = cache_it->second;
      if (column_names.size() == metadata.columns.size()) {
        for (size_t i = 0; i < metadata.columns.size(); i++) {
          metadata.columns[i].name = column_names[i];
        }
        mygram::utils::StructuredLog()
            .Event("binlog_debug")
            .Field("action", "column_names_cache_hit")
            .Field("database", metadata.database_name)
            .Field("table", metadata.table_name)
            .Debug();
        return true;
      }
      // Cache mismatch (column count changed?), fall through to query
      mygram::utils::StructuredLog()
          .Event("binlog_warning")
          .Field("type", "column_cache_mismatch")
          .Field("database", metadata.database_name)
          .Field("table", metadata.table_name)
          .Field("cached_count", static_cast<int64_t>(column_names.size()))
          .Field("current_count", static_cast<int64_t>(metadata.columns.size()))
          .Message("Cached column names have mismatched count")
          .Warn();
      column_names_cache_.erase(cache_it);  // Remove stale cache entry
    }
  }

  // Cache miss or stale: use SHOW COLUMNS (faster than INFORMATION_SCHEMA)
  // Escape backticks in identifier names
  auto escape_identifier = [](const std::string& identifier) {
    std::string escaped;
    escaped.reserve(identifier.length());
    for (char chr : identifier) {
      if (chr == '`') {
        escaped += "``";  // Double backtick for escaping
      } else {
        escaped += chr;
      }
    }
    return escaped;
  };

  std::string query = "SHOW COLUMNS FROM `" + escape_identifier(metadata.database_name) + "`.`" +
                      escape_identifier(metadata.table_name) + "`";

  // Use dedicated metadata connection to avoid thread safety issues (#1)
  // The main connection_ must not be used from the reader thread.
  if (!metadata_connection_) {
    mygram::utils::StructuredLog()
        .Event("binlog_error")
        .Field("type", "metadata_connection_null")
        .Field("database", metadata.database_name)
        .Field("table", metadata.table_name)
        .Error();
    return false;
  }

  auto result_exp = metadata_connection_->Execute(query);
  if (!result_exp) {
    mygram::utils::StructuredLog()
        .Event("binlog_error")
        .Field("type", "column_query_failed")
        .Field("database", metadata.database_name)
        .Field("table", metadata.table_name)
        .Field("error", metadata_connection_->GetLastError())
        .Error();
    return false;
  }

  std::vector<std::string> column_names;
  column_names.reserve(metadata.columns.size());

  MYSQL_ROW row = nullptr;
  while ((row = mysql_fetch_row(result_exp->get())) != nullptr) {
    column_names.emplace_back(row[0]);
  }

  // result automatically freed by MySQLResult destructor

  if (column_names.size() != metadata.columns.size()) {
    mygram::utils::StructuredLog()
        .Event("binlog_error")
        .Field("type", "column_count_mismatch")
        .Field("database", metadata.database_name)
        .Field("table", metadata.table_name)
        .Field("show_columns_count", static_cast<int64_t>(column_names.size()))
        .Field("binlog_count", static_cast<int64_t>(metadata.columns.size()))
        .Error();
    return false;
  }

  // Update metadata with actual column names
  for (size_t i = 0; i < metadata.columns.size(); i++) {
    metadata.columns[i].name = column_names[i];
  }

  // Store in cache
  {
    std::lock_guard<std::mutex> lock(column_names_cache_mutex_);
    column_names_cache_[cache_key] = std::move(column_names);
  }

  mygram::utils::StructuredLog()
      .Event("binlog_debug")
      .Field("action", "fetched_column_names")
      .Field("column_count", static_cast<uint64_t>(metadata.columns.size()))
      .Field("database", metadata.database_name)
      .Field("table", metadata.table_name)
      .Debug();

  return true;
}

void BinlogReader::FixGtidSetCallback(MYSQL_RPL* rpl, unsigned char* packet_gtid_set) {
  // Copy pre-encoded GTID data into the packet buffer
  // Invariant: rpl->gtid_set_encoded_size == encoded_data->size()
  // This is guaranteed because we set rpl->gtid_set_encoded_size from the same
  // vector before calling mysql_binlog_open(), which allocates packet_gtid_set
  // with exactly that size.
  auto* encoded_data = static_cast<std::vector<uint8_t>*>(rpl->gtid_set_arg);
  assert(rpl->gtid_set_encoded_size == encoded_data->size());  // NOLINT(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
  std::memcpy(packet_gtid_set, encoded_data->data(), encoded_data->size());
}

std::string BinlogReader::ConvertSingleGtidToRange(const std::string& gtid) {
  if (gtid.empty()) {
    return gtid;
  }

  // Check if this looks like a single GTID (uuid:N) vs a range (uuid:1-N) or multi-UUID
  // If it contains a comma, it's multi-UUID - pass through
  if (gtid.find(',') != std::string::npos) {
    return gtid;
  }

  // Find the colon separating UUID from transaction number
  size_t colon_pos = gtid.find(':');
  if (colon_pos == std::string::npos) {
    return gtid;
  }

  std::string after_colon = gtid.substr(colon_pos + 1);

  // If it contains a dash, it's already a range - pass through
  if (after_colon.find('-') != std::string::npos) {
    return gtid;
  }

  // If it contains another colon, it could be tagged GTID or multiple intervals - pass through
  if (after_colon.find(':') != std::string::npos) {
    return gtid;
  }

  // It's a single transaction number (e.g., "uuid:101")
  // Convert to range "uuid:1-101" so the server excludes transactions 1 through 101
  std::string uuid = gtid.substr(0, colon_pos);
  return uuid + ":1-" + after_colon;
}

bool BinlogReader::ValidateConnection() {
  // Collect required table names from configuration
  std::vector<std::string> required_tables;

  if (multi_table_mode_) {
    required_tables.reserve(table_contexts_.size());
    for (const auto& [table_name, ctx] : table_contexts_) {
      required_tables.push_back(ctx->config.name);
    }
  } else {
    required_tables.push_back(table_config_.name);
  }

  // Get expected server UUID (empty on first connection)
  std::optional<std::string> expected_uuid;
  {
    std::lock_guard<std::mutex> lock(uuid_mutex_);
    if (!last_server_uuid_.empty()) {
      expected_uuid = last_server_uuid_;
    }
  }

  // Pass current GTID position for purge check
  // Always use range format for validation so GTID_SUBSET checks the full history
  std::optional<std::string> current_gtid_for_validation;
  {
    std::lock_guard<std::mutex> lock(gtid_mutex_);
    if (!executed_gtid_set_.empty()) {
      current_gtid_for_validation = executed_gtid_set_;
    } else if (!current_gtid_.empty()) {
      current_gtid_for_validation = ConvertSingleGtidToRange(current_gtid_);
    }
  }

  // Validate connection using ConnectionValidator
  auto result = ConnectionValidator::ValidateServer(*binlog_connection_, required_tables, expected_uuid,
                                                    current_gtid_for_validation);

  if (!result.valid) {
    // Validation failed - server is invalid
    last_error_ = "Connection validation failed: " + result.error_message;
    mygram::utils::StructuredLog()
        .Event("binlog_connection_validation_failed")
        .Field("gtid", GetCurrentGTID())
        .Field("error", result.error_message)
        .Error();
    return false;
  }

  // Validation succeeded - update last known server UUID
  if (result.server_uuid) {
    std::lock_guard<std::mutex> lock(uuid_mutex_);
    last_server_uuid_ = *result.server_uuid;
  }

  // Log warnings if any (e.g., failover detected)
  if (!result.warnings.empty()) {
    for (const auto& warning : result.warnings) {
      mygram::utils::StructuredLog()
          .Event("binlog_warning")
          .Field("type", "connection_validation")
          .Field("warning", warning)
          .Warn();
    }
  }

  // Log failover detection if applicable
  if (result.failover_detected) {
    mygram::utils::StructuredLog()
        .Event("mysql_failover_handled")
        .Field("old_uuid", expected_uuid.value_or(""))
        .Field("new_uuid", result.server_uuid.value_or(""))
        .Field("gtid", GetCurrentGTID())
        .Info();
  }

  return true;
}

}  // namespace mygramdb::mysql

// NOLINTEND(cppcoreguidelines-pro-*,cppcoreguidelines-avoid-*,readability-magic-numbers)

#endif  // USE_MYSQL
