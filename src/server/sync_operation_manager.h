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
#include <thread>
#include <unordered_map>
#include <unordered_set>

#include "config/config.h"
#include "server/server_types.h"

namespace mygramdb::mysql {
class BinlogReader;
}

namespace mygramdb::loader {
class InitialLoader;
}

namespace mygramdb::server {

/**
 * @brief State of a SYNC operation
 *
 * Thread Safety Requirements:
 * - Atomic members (is_running, total_rows, processed_rows) are thread-safe
 * - Non-atomic members (table_name, status, error_message, gtid, replication_status)
 *   MUST be accessed only while holding sync_mutex_ in SyncOperationManager
 * - The start_time member is set once during initialization and read-only thereafter
 *
 * Typical Access Pattern:
 * 1. Create SyncState under sync_mutex_ protection
 * 2. Read/write non-atomic members only within critical sections
 * 3. Read atomic members without locks (for progress monitoring)
 */
struct SyncState {
  std::atomic<bool> is_running{false};
  std::string table_name;               // Protected by sync_mutex_
  std::atomic<uint64_t> total_rows{0};  // Atomic to prevent data races during concurrent access
  std::atomic<uint64_t> processed_rows{0};
  std::chrono::steady_clock::time_point start_time;  // Set once, read-only after init
  std::string
      status;  // Protected by sync_mutex_ - "IDLE", "STARTING", "IN_PROGRESS", "COMPLETED", "FAILED", "CANCELLED"
  std::string error_message;       // Protected by sync_mutex_
  std::string gtid;                // Protected by sync_mutex_
  std::string replication_status;  // Protected by sync_mutex_ - "STARTED", "ALREADY_RUNNING", "DISABLED", "FAILED"
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
 *
 * Lifetime Requirements:
 * - binlog_reader must outlive this SyncOperationManager instance
 * - binlog_reader is typically owned by Application and remains valid for the
 *   entire application lifetime
 * - Null binlog_reader is allowed (replication will be disabled)
 */
class SyncOperationManager {
 public:
  /**
   * @brief Construct SyncOperationManager
   * @param table_contexts Reference to table contexts (must outlive this instance)
   * @param full_config Pointer to configuration (must outlive this instance)
   * @param binlog_reader Pointer to binlog reader (must outlive this instance, can be nullptr)
   */
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
   * @brief Stop SYNC operation for a table
   * @param table_name Table to stop (empty means stop all)
   * @return Response string (OK or ERROR)
   */
  std::string StopSync(const std::string& table_name);

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
   * @brief Get syncing table names (thread-safe)
   * @return Copy of syncing tables set
   */
  std::unordered_set<std::string> GetSyncingTables() const;

  /**
   * @brief Check if any tables are syncing and get their names (thread-safe)
   * @param out_tables Output parameter for syncing table names (only filled if return is true)
   * @return true if any tables are syncing, false otherwise
   */
  bool GetSyncingTablesIfAny(std::vector<std::string>& out_tables) const;

 private:
  const std::unordered_map<std::string, TableContext*>& table_contexts_;
  const config::Config* full_config_;
  mysql::BinlogReader* binlog_reader_;

  // State tracking
  std::unordered_map<std::string, SyncState> sync_states_;
  mutable std::mutex sync_mutex_;

  std::unordered_set<std::string> syncing_tables_;
  mutable std::mutex syncing_tables_mutex_;

  std::unordered_map<std::string, loader::InitialLoader*> active_loaders_;
  mutable std::mutex loaders_mutex_;

  // Sync threads tracking (non-detached for proper cleanup)
  // Protected by sync_mutex_ to prevent race conditions with sync_states_
  std::unordered_map<std::string, std::thread> sync_threads_;

  std::atomic<bool> shutdown_requested_{false};

  /**
   * @brief Build initial data load asynchronously for a table
   * @param table_name Table to synchronize
   */
  void BuildSnapshotAsync(const std::string& table_name);

  /**
   * @brief Register active initial loader
   */
  void RegisterLoader(const std::string& table_name, loader::InitialLoader* loader);

  /**
   * @brief Unregister active initial loader
   */
  void UnregisterLoader(const std::string& table_name);
};

}  // namespace mygramdb::server

#endif  // USE_MYSQL
