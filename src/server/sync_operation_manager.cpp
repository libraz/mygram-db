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

#include "mysql/binlog_reader.h"
#include "mysql/connection.h"
#include "server/response_formatter.h"
#include "storage/snapshot_builder.h"
#include "utils/memory_utils.h"

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

  // Launch async build
  std::thread([this, table_name]() { BuildSnapshotAsync(table_name); }).detach();

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

    oss << "\n";
  }

  if (!any_active) {
    return "status=IDLE message=\"No sync operation performed\"";
  }

  std::string result = oss.str();
  if (!result.empty() && result.back() == '\n') {
    result.pop_back();
  }
  return result;
}

void SyncOperationManager::RequestShutdown() {
  shutdown_requested_ = true;

  // Cancel all active builders
  std::lock_guard<std::mutex> lock(builders_mutex_);
  for (auto& [table_name, builder] : active_builders_) {
    spdlog::info("Cancelling SYNC for table: {}", table_name);
    builder->Cancel();
  }
}

bool SyncOperationManager::WaitForCompletion(int timeout_sec) {
  auto start = std::chrono::steady_clock::now();

  while (!syncing_tables_.empty()) {
    auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - start).count();

    if (elapsed > timeout_sec) {
      spdlog::warn("Timeout waiting for SYNC operations to complete");
      return false;
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(kSyncPollIntervalMs));
  }

  return true;
}

bool SyncOperationManager::IsAnySyncing() const {
  std::lock_guard<std::mutex> lock(syncing_tables_mutex_);
  return !syncing_tables_.empty();
}

std::unordered_set<std::string> SyncOperationManager::GetSyncingTables() const {
  std::lock_guard<std::mutex> lock(syncing_tables_mutex_);
  return syncing_tables_;
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
      spdlog::error("SYNC failed for {}: Configuration not available", table_name);
      return;
    }

    // Connect to MySQL
    mysql::Connection::Config mysql_config{.host = full_config_->mysql.host,
                                           .port = static_cast<uint16_t>(full_config_->mysql.port),
                                           .user = full_config_->mysql.user,
                                           .password = full_config_->mysql.password,
                                           .database = full_config_->mysql.database};

    auto mysql_conn = std::make_unique<mysql::Connection>(mysql_config);

    if (!mysql_conn->Connect()) {
      std::string error_msg = "Failed to connect: " + mysql_conn->GetLastError();
      update_state([&error_msg](SyncState& state) {
        state.status = "FAILED";
        state.error_message = error_msg;
        state.is_running = false;
      });
      spdlog::error("SYNC failed for {}: {}", table_name, error_msg);
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

    // Build snapshot
    storage::SnapshotBuilder builder(*mysql_conn, *ctx->index, *ctx->doc_store, ctx->config, full_config_->build);

    RegisterBuilder(table_name, &builder);

    bool success = builder.Build([&](const auto& progress) {
      // Update progress atomically (these fields are atomic in SyncState)
      std::lock_guard<std::mutex> lock(sync_mutex_);
      sync_states_[table_name].total_rows = progress.total_rows;
      sync_states_[table_name].processed_rows = progress.processed_rows;

      if (shutdown_requested_) {
        builder.Cancel();
      }
    });

    UnregisterBuilder(table_name);

    // Handle cancellation
    if (shutdown_requested_) {
      update_state([](SyncState& state) {
        state.status = "CANCELLED";
        state.error_message = "Server shutdown requested";
        state.is_running = false;
      });
      spdlog::info("SYNC cancelled for {} due to shutdown", table_name);
      return;
    }

    // Handle result
    if (success) {
      std::string gtid = builder.GetSnapshotGTID();
      uint64_t processed = builder.GetProcessedRows();

      update_state([&gtid, processed](SyncState& state) {
        state.status = "COMPLETED";
        state.gtid = gtid;
        state.processed_rows = processed;
      });

      // Start replication if configured
      if (full_config_->replication.enable && binlog_reader_ != nullptr && !gtid.empty()) {
        if (binlog_reader_->IsRunning()) {
          update_state([](SyncState& state) { state.replication_status = "ALREADY_RUNNING"; });
          spdlog::info("SYNC completed for {} (rows={}, gtid={}). Replication already running.", table_name, processed,
                       gtid);
        } else {
          binlog_reader_->SetCurrentGTID(gtid);
          if (binlog_reader_->Start()) {
            update_state([](SyncState& state) { state.replication_status = "STARTED"; });
            spdlog::info("SYNC completed for {} (rows={}, gtid={}). Replication started.", table_name, processed, gtid);
          } else {
            std::string error_msg = "Snapshot OK but replication failed: " + binlog_reader_->GetLastError();
            update_state([&error_msg](SyncState& state) {
              state.replication_status = "FAILED";
              state.error_message = error_msg;
            });
            spdlog::error("SYNC completed for {} but replication failed: {}", table_name,
                          binlog_reader_->GetLastError());
          }
        }
      } else {
        update_state([](SyncState& state) { state.replication_status = "DISABLED"; });
        spdlog::info("SYNC completed for {} (rows={}, replication disabled)", table_name, processed);
      }
    } else {
      std::string error_msg = builder.GetLastError();
      update_state([&error_msg](SyncState& state) {
        state.status = "FAILED";
        state.error_message = error_msg;
      });
      spdlog::error("SYNC failed for {}: {}", table_name, error_msg);
    }

  } catch (const std::exception& e) {
    std::string error_msg = e.what();
    update_state([&error_msg](SyncState& state) {
      state.status = "FAILED";
      state.error_message = error_msg;
    });
    spdlog::error("SYNC exception for {}: {}", table_name, error_msg);
  }

  update_state([](SyncState& state) { state.is_running = false; });
}

void SyncOperationManager::RegisterBuilder(const std::string& table_name, storage::SnapshotBuilder* builder) {
  std::lock_guard<std::mutex> lock(builders_mutex_);
  active_builders_[table_name] = builder;
}

void SyncOperationManager::UnregisterBuilder(const std::string& table_name) {
  std::lock_guard<std::mutex> lock(builders_mutex_);
  active_builders_.erase(table_name);
}

}  // namespace mygramdb::server

#endif  // USE_MYSQL
