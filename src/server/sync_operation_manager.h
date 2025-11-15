/**
 * @file sync_operation_manager.h
 * @brief Manages MySQL synchronization operations
 */

#pragma once

#ifdef USE_MYSQL

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "config/config.h"
#include "server/server_types.h"

namespace mygramdb::mysql {
class BinlogReader;
}

namespace mygramdb::storage {
class SnapshotBuilder;
}

namespace mygramdb::server {

/**
 * @brief State of a SYNC operation
 */
struct SyncState {
  std::atomic<bool> is_running{false};
  std::string table_name;
  uint64_t total_rows = 0;
  std::atomic<uint64_t> processed_rows{0};
  std::chrono::steady_clock::time_point start_time;
  std::string status;  // "IDLE", "STARTING", "IN_PROGRESS", "COMPLETED", "FAILED", "CANCELLED"
  std::string error_message;
  std::string gtid;
  std::string replication_status;  // "STARTED", "ALREADY_RUNNING", "DISABLED", "FAILED"
};

/**
 * @brief Manages MySQL SYNC operations for tables
 *
 * Responsibilities:
 * - Track sync state per table
 * - Prevent concurrent syncs on same table
 * - Build snapshots asynchronously
 * - Integrate with binlog replication
 * - Support graceful cancellation
 */
class SyncOperationManager {
 public:
  SyncOperationManager(const std::unordered_map<std::string, TableContext*>& table_contexts,
                       const config::Config* full_config, mysql::BinlogReader* binlog_reader);

  ~SyncOperationManager();

  // Non-copyable and non-movable
  SyncOperationManager(const SyncOperationManager&) = delete;
  SyncOperationManager& operator=(const SyncOperationManager&) = delete;
  SyncOperationManager(SyncOperationManager&&) = delete;
  SyncOperationManager& operator=(SyncOperationManager&&) = delete;

  /**
   * @brief Start SYNC operation for a table
   * @param table_name Table to synchronize
   * @return Response string (OK or ERROR)
   */
  std::string StartSync(const std::string& table_name);

  /**
   * @brief Get SYNC status for all tables
   * @return Response string with sync status
   */
  std::string GetSyncStatus();

  /**
   * @brief Request shutdown and cancel all active syncs
   */
  void RequestShutdown();

  /**
   * @brief Wait for all sync operations to complete (with timeout)
   * @param timeout_sec Timeout in seconds
   * @return True if all syncs completed before timeout
   */
  bool WaitForCompletion(int timeout_sec);

  /**
   * @brief Check if any table is currently syncing
   */
  bool IsAnySyncing() const;

  /**
   * @brief Get syncing table names
   */
  std::unordered_set<std::string> GetSyncingTables() const;

 private:
  const std::unordered_map<std::string, TableContext*>& table_contexts_;
  const config::Config* full_config_;
  mysql::BinlogReader* binlog_reader_;

  // State tracking
  std::unordered_map<std::string, SyncState> sync_states_;
  mutable std::mutex sync_mutex_;

  std::unordered_set<std::string> syncing_tables_;
  mutable std::mutex syncing_tables_mutex_;

  std::unordered_map<std::string, storage::SnapshotBuilder*> active_builders_;
  mutable std::mutex builders_mutex_;

  std::atomic<bool> shutdown_requested_{false};

  /**
   * @brief Build snapshot asynchronously for a table
   * @param table_name Table to synchronize
   */
  void BuildSnapshotAsync(const std::string& table_name);

  /**
   * @brief Register active snapshot builder
   */
  void RegisterBuilder(const std::string& table_name, storage::SnapshotBuilder* builder);

  /**
   * @brief Unregister active snapshot builder
   */
  void UnregisterBuilder(const std::string& table_name);
};

}  // namespace mygramdb::server

#endif  // USE_MYSQL
