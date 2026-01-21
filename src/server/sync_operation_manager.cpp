/**
 * @file sync_operation_manager.cpp
 * @brief SYNC operation manager implementation
 */

#ifdef USE_MYSQL

#include "server/sync_operation_manager.h"

#include <spdlog/spdlog.h>

#include <iomanip>
#include <sstream>
#include <thread>

#include "loader/initial_loader.h"
#include "mysql/binlog_reader.h"
#include "mysql/connection.h"
#include "server/response_formatter.h"
#include "utils/memory_utils.h"
#include "utils/structured_log.h"

namespace mygramdb::server {

namespace {
constexpr int kDefaultSyncWaitTimeoutSec = 30;
constexpr int kSyncPollIntervalMs = 100;
}  // namespace

SyncOperationManager::SyncOperationManager(const std::unordered_map<std::string, TableContext*>& table_contexts,
                                           const config::Config* full_config, mysql::BinlogReader* binlog_reader)
    : table_contexts_(table_contexts), full_config_(full_config), binlog_reader_(binlog_reader) {}

SyncOperationManager::~SyncOperationManager() {
  RequestShutdown();
  WaitForCompletion(kDefaultSyncWaitTimeoutSec);

  // Join all sync threads to ensure clean shutdown
  // IMPORTANT: Copy threads to local variable BEFORE joining to avoid deadlock.
  // If we hold sync_mutex_ while joining, and BuildSnapshotAsync tries to acquire
  // sync_mutex_, we will deadlock.
  std::unordered_map<std::string, std::thread> threads_to_join;
  {
    std::lock_guard<std::mutex> lock(sync_mutex_);
    threads_to_join = std::move(sync_threads_);
    sync_threads_.clear();
  }

  // Join threads WITHOUT holding sync_mutex_
  for (auto& [table_name, thread] : threads_to_join) {
    if (thread.joinable()) {
      mygram::utils::StructuredLog().Event("sync_thread_joining").Field("table", table_name).Debug();
      thread.join();
    }
  }
}

std::string SyncOperationManager::StartSync(const std::string& table_name) {
  std::lock_guard<std::mutex> lock(sync_mutex_);

  // Check if table exists
  if (table_contexts_.find(table_name) == table_contexts_.end()) {
    return ResponseFormatter::FormatError("Table '" + table_name + "' not found");
  }

  // Check if already running
  if (sync_states_[table_name].is_running) {
    return ResponseFormatter::FormatError("SYNC already in progress for '" + table_name + "'");
  }

  // Check memory health
  auto health = utils::GetMemoryHealthStatus();
  if (health == utils::MemoryHealthStatus::CRITICAL) {
    return ResponseFormatter::FormatError("Memory critically low. Cannot start SYNC.");
  }

  // Log session timeout warning
  uint32_t session_timeout = full_config_->mysql.session_timeout_sec;
  mygram::utils::StructuredLog()
      .Event("sync_starting")
      .Field("table", table_name)
      .Field("session_timeout_sec", static_cast<uint64_t>(session_timeout))
      .Field("hint", "ensure session_timeout_sec is sufficient for snapshot duration")
      .Info();

  // Mark as syncing
  {
    std::lock_guard<std::mutex> sync_lock(syncing_tables_mutex_);
    syncing_tables_.insert(table_name);
  }

  // Initialize state
  sync_states_[table_name].is_running = true;
  sync_states_[table_name].status = "STARTING";
  sync_states_[table_name].table_name = table_name;
  sync_states_[table_name].processed_rows = 0;
  sync_states_[table_name].error_message.clear();

  // Clean up old thread if it exists and is joinable
  // Note: Thread access is now protected by sync_mutex_ (same as sync_states_)
  // to prevent race conditions between state updates and thread lifecycle
  auto thread_iter = sync_threads_.find(table_name);
  if (thread_iter != sync_threads_.end() && thread_iter->second.joinable()) {
    thread_iter->second.join();
  }
  // Launch async build (store thread instead of detaching)
  sync_threads_[table_name] = std::thread([this, table_name]() { BuildSnapshotAsync(table_name); });

  return "OK SYNC STARTED table=" + table_name + " job_id=1";
}

std::string SyncOperationManager::GetSyncStatus() {
  std::lock_guard<std::mutex> lock(sync_mutex_);

  std::ostringstream oss;
  bool any_active = false;

  for (const auto& [table_name, state] : sync_states_) {
    if (!state.is_running && state.status.empty()) {
      continue;
    }

    any_active = true;
    oss << "table=" << table_name << " status=" << state.status;

    if (state.status == "IN_PROGRESS") {
      uint64_t processed = state.processed_rows.load();
      double elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - state.start_time).count();
      double rate = elapsed > 0 ? static_cast<double>(processed) / elapsed : 0.0;

      if (state.total_rows > 0) {
        double percent = (100.0 * static_cast<double>(processed)) / static_cast<double>(state.total_rows);
        oss << " progress=" << processed << "/" << state.total_rows << " rows (" << std::fixed << std::setprecision(1)
            << percent << "%)";
      } else {
        oss << " progress=" << processed << " rows";
      }

      oss << " rate=" << std::fixed << std::setprecision(0) << rate << " rows/s";
    } else if (state.status == "COMPLETED") {
      uint64_t processed = state.processed_rows.load();
      double elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - state.start_time).count();

      oss << " rows=" << processed << " time=" << std::fixed << std::setprecision(1) << elapsed << "s";

      if (!state.gtid.empty()) {
        oss << " gtid=" << state.gtid;
      }
      oss << " replication=" << state.replication_status;
    } else if (state.status == "FAILED") {
      oss << " rows=" << state.processed_rows.load() << " error=\"" << state.error_message << "\"";
    } else if (state.status == "CANCELLED") {
      oss << " error=\"" << state.error_message << "\"";
    }

    oss << "\r\n";
  }

  if (!any_active) {
    return "status=IDLE message=\"No sync operation performed\"";
  }

  std::string result = oss.str();
  // Strip trailing CRLF if present (SendResponse will add CRLF)
  while (result.size() >= 2 && result[result.size() - 2] == '\r' && result[result.size() - 1] == '\n') {
    result.erase(result.size() - 2);
  }
  return result;
}

std::string SyncOperationManager::StopSync(const std::string& table_name) {
  // Empty table_name means stop all
  if (table_name.empty()) {
    std::vector<std::string> tables_to_stop;

    // Collect active sync tables
    {
      std::lock_guard<std::mutex> lock(syncing_tables_mutex_);
      tables_to_stop.assign(syncing_tables_.begin(), syncing_tables_.end());
    }

    if (tables_to_stop.empty()) {
      return ResponseFormatter::FormatError("No active SYNC operations to stop");
    }

    // Cancel all active loaders
    {
      std::lock_guard<std::mutex> lock(loaders_mutex_);
      for (const auto& tbl : tables_to_stop) {
        auto iter = active_loaders_.find(tbl);
        if (iter != active_loaders_.end()) {
          mygram::utils::StructuredLog()
              .Event("sync_stop")
              .Field("table", tbl)
              .Field("source", "user_request")
              .Field("scope", "all")
              .Info();
          iter->second->Cancel();
        }
      }
    }

    // Wait for threads to finish
    {
      std::lock_guard<std::mutex> lock(sync_mutex_);
      for (const auto& tbl : tables_to_stop) {
        auto thread_iter = sync_threads_.find(tbl);
        if (thread_iter != sync_threads_.end() && thread_iter->second.joinable()) {
          thread_iter->second.join();
          sync_threads_.erase(thread_iter);
        }
      }
    }

    return "OK SYNC STOPPED count=" + std::to_string(tables_to_stop.size());
  }

  // Stop specific table
  {
    std::lock_guard<std::mutex> lock(syncing_tables_mutex_);
    if (syncing_tables_.find(table_name) == syncing_tables_.end()) {
      return ResponseFormatter::FormatError("No active SYNC operation for table: " + table_name);
    }
  }

  // Cancel the loader
  {
    std::lock_guard<std::mutex> lock(loaders_mutex_);
    auto iter = active_loaders_.find(table_name);
    if (iter != active_loaders_.end()) {
      mygram::utils::StructuredLog()
          .Event("sync_stop")
          .Field("table", table_name)
          .Field("source", "user_request")
          .Info();
      iter->second->Cancel();
    } else {
      return ResponseFormatter::FormatError("SYNC loader not found for table: " + table_name);
    }
  }

  // Wait for thread to finish
  {
    std::lock_guard<std::mutex> lock(sync_mutex_);
    auto thread_iter = sync_threads_.find(table_name);
    if (thread_iter != sync_threads_.end() && thread_iter->second.joinable()) {
      thread_iter->second.join();
      sync_threads_.erase(thread_iter);
    }
  }

  return "OK SYNC STOPPED table=" + table_name;
}

void SyncOperationManager::RequestShutdown() {
  shutdown_requested_ = true;

  // Cancel all active loaders
  std::lock_guard<std::mutex> lock(loaders_mutex_);
  for (auto& [table_name, loader] : active_loaders_) {
    mygram::utils::StructuredLog()
        .Event("sync_cancelling")
        .Field("table", table_name)
        .Field("reason", "shutdown_requested")
        .Info();
    loader->Cancel();
  }
}

bool SyncOperationManager::WaitForCompletion(int timeout_sec) {
  auto start = std::chrono::steady_clock::now();

  while (true) {
    // Check if syncing_tables_ is empty with proper locking
    {
      std::lock_guard<std::mutex> lock(syncing_tables_mutex_);
      if (syncing_tables_.empty()) {
        return true;
      }
    }

    auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - start).count();

    if (elapsed > timeout_sec) {
      mygram::utils::StructuredLog()
          .Event("server_warning")
          .Field("operation", "wait_all_sync_complete")
          .Field("timeout_sec", static_cast<uint64_t>(timeout_sec))
          .Warn();
      return false;
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(kSyncPollIntervalMs));
  }
}

bool SyncOperationManager::IsAnySyncing() const {
  std::lock_guard<std::mutex> lock(syncing_tables_mutex_);
  return !syncing_tables_.empty();
}

std::unordered_set<std::string> SyncOperationManager::GetSyncingTables() const {
  std::lock_guard<std::mutex> lock(syncing_tables_mutex_);
  return syncing_tables_;
}

bool SyncOperationManager::GetSyncingTablesIfAny(std::vector<std::string>& out_tables) const {
  std::lock_guard<std::mutex> lock(syncing_tables_mutex_);
  if (syncing_tables_.empty()) {
    return false;
  }
  out_tables.assign(syncing_tables_.begin(), syncing_tables_.end());
  return true;
}

void SyncOperationManager::BuildSnapshotAsync(const std::string& table_name) {
  // Update state under lock
  {
    std::lock_guard<std::mutex> lock(sync_mutex_);
    sync_states_[table_name].status = "IN_PROGRESS";
    sync_states_[table_name].start_time = std::chrono::steady_clock::now();
  }

  // Helper to update state safely
  auto update_state = [this, &table_name](auto&& updater) {
    std::lock_guard<std::mutex> lock(sync_mutex_);
    updater(sync_states_[table_name]);
  };

  // RAII cleanup guard
  struct SyncGuard {
    SyncOperationManager* mgr;
    std::string table;
    explicit SyncGuard(SyncOperationManager* manager, std::string table_name)
        : mgr(manager), table(std::move(table_name)) {}
    ~SyncGuard() {
      std::lock_guard<std::mutex> lock(mgr->syncing_tables_mutex_);
      mgr->syncing_tables_.erase(table);
    }
    SyncGuard(const SyncGuard&) = delete;
    SyncGuard& operator=(const SyncGuard&) = delete;
    SyncGuard(SyncGuard&&) = delete;
    SyncGuard& operator=(SyncGuard&&) = delete;
  };
  SyncGuard guard(this, table_name);

  try {
    // Validate config
    if (full_config_ == nullptr) {
      update_state([](SyncState& state) {
        state.status = "FAILED";
        state.error_message = "Configuration not available";
        state.is_running = false;
      });
      mygram::utils::StructuredLog()
          .Event("server_error")
          .Field("operation", "sync")
          .Field("table", table_name)
          .Field("error", "Configuration not available")
          .Error();
      return;
    }

    // Connect to MySQL
    mysql::Connection::Config mysql_config{
        .host = full_config_->mysql.host,
        .port = static_cast<uint16_t>(full_config_->mysql.port),
        .user = full_config_->mysql.user,
        .password = full_config_->mysql.password,
        .database = full_config_->mysql.database,
        .session_timeout_sec = static_cast<uint32_t>(full_config_->mysql.session_timeout_sec)};

    auto mysql_conn = std::make_unique<mysql::Connection>(mysql_config);

    if (!mysql_conn->Connect()) {
      std::string error_msg = "Failed to connect: " + mysql_conn->GetLastError();
      update_state([&error_msg](SyncState& state) {
        state.status = "FAILED";
        state.error_message = error_msg;
        state.is_running = false;
      });
      mygram::utils::StructuredLog()
          .Event("server_error")
          .Field("operation", "sync")
          .Field("table", table_name)
          .Field("error", error_msg)
          .Error();
      return;
    }

    // Get table context
    auto table_iter = table_contexts_.find(table_name);
    if (table_iter == table_contexts_.end()) {
      update_state([](SyncState& state) {
        state.status = "FAILED";
        state.error_message = "Table context not found";
        state.is_running = false;
      });
      return;
    }

    auto* ctx = table_iter->second;

    // Check for null pointers
    if (!ctx->index || !ctx->doc_store) {
      update_state([](SyncState& state) {
        state.status = "FAILED";
        state.error_message = "Table context has null index or doc_store";
        state.is_running = false;
      });
      return;
    }

    // Build initial data load
    loader::InitialLoader loader(*mysql_conn, *ctx->index, *ctx->doc_store, ctx->config, full_config_->mysql,
                                 full_config_->build);

    RegisterLoader(table_name, &loader);

    auto result = loader.Load([&](const auto& progress) {
      // Update progress atomically (these fields are atomic in SyncState)
      std::lock_guard<std::mutex> lock(sync_mutex_);
      sync_states_[table_name].total_rows = progress.total_rows;
      sync_states_[table_name].processed_rows = progress.processed_rows;
      // Note: No need to call loader.Cancel() here as RequestShutdown() already handles it
    });

    UnregisterLoader(table_name);

    // Handle cancellation (both shutdown and user-requested SYNC STOP)
    // Check if loader was cancelled OR shutdown was requested
    bool was_cancelled = loader.IsCancelled() || shutdown_requested_;

    if (was_cancelled) {
      // Clean up partial data on cancellation to maintain consistency
      uint64_t partial_rows = loader.GetProcessedRows();
      std::string cancel_reason = shutdown_requested_ ? "shutdown" : "user_stop_request";

      mygram::utils::StructuredLog()
          .Event("sync_cleanup")
          .Field("table", table_name)
          .Field("reason", cancel_reason)
          .Field("partial_rows_discarded", partial_rows)
          .Field("message", "Partial data discarded due to cancellation")
          .Warn();

      // Use Clear() instead of creating new instances to preserve pointers
      // that BinlogReader holds through TableContext (BUG: instance replacement
      // causes replication to use different doc_store than SYNC populated)
      ctx->index->Clear();
      ctx->doc_store->Clear();

      std::string cancel_msg = shutdown_requested_ ? "Server shutdown requested" : "Cancelled by user (SYNC STOP)";
      update_state([&cancel_msg](SyncState& state) {
        state.status = "CANCELLED";
        state.error_message = cancel_msg;
        state.is_running = false;
      });

      mygram::utils::StructuredLog()
          .Event("sync_cancelled")
          .Field("table", table_name)
          .Field("reason", cancel_reason)
          .Field("partial_rows", partial_rows)
          .Info();
      return;
    }

    // Handle result
    if (result) {
      // SYNC succeeded
      std::string gtid = loader.GetStartGTID();
      uint64_t processed = loader.GetProcessedRows();

      update_state([&gtid, processed](SyncState& state) {
        state.status = "COMPLETED";
        state.gtid = gtid;
        state.processed_rows = processed;
      });

      // Start replication if configured
      // NOTE: binlog_reader_ is owned by Application and guaranteed to outlive
      // SyncOperationManager. We check for nullptr for defensive programming.
      // SAFETY: Capture binlog_reader_ to local variable to prevent TOCTOU issues
      // (Time-Of-Check-Time-Of-Use) if binlog_reader_ were to be modified by another thread.
      mysql::BinlogReader* reader = binlog_reader_;

      // Log replication configuration for debugging
      mygram::utils::StructuredLog()
          .Event("sync_replication_check")
          .Field("table", table_name)
          .Field("replication_enable", full_config_->replication.enable)
          .Field("reader_exists", reader != nullptr)
          .Field("gtid_empty", gtid.empty())
          .Field("gtid", gtid)
          .Info();

      if (full_config_->replication.enable && reader != nullptr && !gtid.empty()) {
        // reader is guaranteed non-null here due to the check above
        // If replication is already running, stop it first to update GTID
        // This handles cases where:
        // 1. Replication failed with non-recoverable error but running_ flag wasn't cleared
        // 2. GTID needs to be updated to the snapshot position
        if (reader->IsRunning()) {
          mygram::utils::StructuredLog()
              .Event("replication_restart")
              .Field("operation", "sync")
              .Field("table", table_name)
              .Field("reason", "update_gtid_after_sync")
              .Info();
          reader->Stop();
        }

        // Always set GTID and start replication after SYNC
        mygram::utils::StructuredLog().Event("sync_setting_gtid").Field("table", table_name).Field("gtid", gtid).Info();

        reader->SetCurrentGTID(gtid);

        // Log before attempting to start replication
        mygram::utils::StructuredLog()
            .Event("sync_starting_replication")
            .Field("table", table_name)
            .Field("gtid", gtid)
            .Field("reader_running", reader->IsRunning())
            .Info();

        auto start_result = reader->Start();
        if (start_result) {
          update_state([](SyncState& state) { state.replication_status = "STARTED"; });
          mygram::utils::StructuredLog()
              .Event("sync_completed")
              .Field("table", table_name)
              .Field("rows", processed)
              .Field("gtid", gtid)
              .Field("replication_status", "started")
              .Info();
        } else {
          std::string error_msg = "Snapshot OK but replication failed: " + start_result.error().message();
          update_state([&error_msg](SyncState& state) {
            state.replication_status = "FAILED";
            state.error_message = error_msg;
          });
          mygram::utils::StructuredLog()
              .Event("server_error")
              .Field("operation", "sync_replication")
              .Field("table", table_name)
              .Field("error", start_result.error().message())
              .Error();
        }
      } else {
        update_state([](SyncState& state) { state.replication_status = "DISABLED"; });
        mygram::utils::StructuredLog()
            .Event("sync_completed")
            .Field("table", table_name)
            .Field("rows", processed)
            .Field("replication_status", "disabled")
            .Info();
      }
    } else {
      // SYNC failed - must clean up partial data to maintain consistency
      // The partial data violates time-consistency (snapshot integrity)
      std::string error_msg = result.error().message();

      // Check if error might be session timeout related
      bool is_timeout_related =
          (error_msg.find("disconnected") != std::string::npos || error_msg.find("timeout") != std::string::npos ||
           error_msg.find("connection") != std::string::npos || error_msg.find("Lost connection") != std::string::npos);

      if (is_timeout_related) {
        uint32_t session_timeout = full_config_->mysql.session_timeout_sec;
        error_msg += " (check if session_timeout_sec=" + std::to_string(session_timeout) +
                     " is sufficient for snapshot duration)";
      }

      // Clean up partial data by recreating Index and DocumentStore
      // This ensures no inconsistent partial state remains
      uint64_t partial_rows = loader.GetProcessedRows();

      mygram::utils::StructuredLog()
          .Event("sync_cleanup")
          .Field("table", table_name)
          .Field("reason", "sync_failed")
          .Field("partial_rows_discarded", partial_rows)
          .Field("message", "Partial data discarded to maintain consistency")
          .Warn();

      // Use Clear() instead of creating new instances to preserve pointers
      // that BinlogReader holds through TableContext (BUG: instance replacement
      // causes replication to use different doc_store than SYNC populated)
      ctx->index->Clear();
      ctx->doc_store->Clear();

      update_state([&error_msg](SyncState& state) {
        state.status = "FAILED";
        state.error_message = error_msg;
      });
      mygram::utils::StructuredLog()
          .Event("server_error")
          .Field("operation", "sync")
          .Field("table", table_name)
          .Field("error", error_msg)
          .Error();
    }

  } catch (const std::exception& e) {
    std::string error_msg = e.what();

    // Clean up partial data on exception to maintain consistency
    // Need to re-acquire table context (may have changed during exception)
    auto table_iter = table_contexts_.find(table_name);
    if (table_iter != table_contexts_.end()) {
      auto* ctx = table_iter->second;
      if (ctx != nullptr && ctx->index && ctx->doc_store) {
        mygram::utils::StructuredLog()
            .Event("sync_cleanup")
            .Field("table", table_name)
            .Field("reason", "exception")
            .Field("message", "Partial data discarded due to exception")
            .Warn();

        // Use Clear() instead of creating new instances to preserve pointers
        // that BinlogReader holds through TableContext (BUG: instance replacement
        // causes replication to use different doc_store than SYNC populated)
        ctx->index->Clear();
        ctx->doc_store->Clear();
      }
    }

    update_state([&error_msg](SyncState& state) {
      state.status = "FAILED";
      state.error_message = error_msg;
    });
    mygram::utils::StructuredLog()
        .Event("server_error")
        .Field("operation", "sync_exception")
        .Field("table", table_name)
        .Field("error", error_msg)
        .Error();
  }

  update_state([](SyncState& state) { state.is_running = false; });
}

void SyncOperationManager::RegisterLoader(const std::string& table_name, loader::InitialLoader* loader) {
  std::lock_guard<std::mutex> lock(loaders_mutex_);
  active_loaders_[table_name] = loader;
}

void SyncOperationManager::UnregisterLoader(const std::string& table_name) {
  std::lock_guard<std::mutex> lock(loaders_mutex_);
  active_loaders_.erase(table_name);
}

}  // namespace mygramdb::server

#endif  // USE_MYSQL
