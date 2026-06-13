/**
 * @file sync_operation_manager.h
 * @brief Manages MySQL synchronization operations
 */

#pragma once

#ifdef USE_MYSQL

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "config/config.h"
#include "server/replication_pause_counter.h"
#include "server/server_types.h"
#include "utils/error.h"
#include "utils/expected.h"

namespace mygramdb::cache {
class CacheManager;
}

namespace mygramdb::mysql {
class IBinlogReader;
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
   * @param cache_manager Pointer to cache manager (must outlive this instance, can be nullptr)
   */
  SyncOperationManager(const std::unordered_map<std::string, TableContext*>& table_contexts,
                       const config::Config* full_config, mysql::IBinlogReader* binlog_reader,
                       replication_pause::Counter* replication_pause_counter = nullptr,
                       cache::CacheManager* cache_manager = nullptr);

  ~SyncOperationManager();

  // Non-copyable and non-movable
  SyncOperationManager(const SyncOperationManager&) = delete;
  SyncOperationManager& operator=(const SyncOperationManager&) = delete;
  SyncOperationManager(SyncOperationManager&&) = delete;
  SyncOperationManager& operator=(SyncOperationManager&&) = delete;

  /**
   * @brief Start SYNC operation for a table
   *
   * Returns kServerShuttingDown if RequestShutdown() has already been
   * called: once the manager has begun shutting down, no new SYNC is
   * accepted (and existing syncs are cancelled). Callers should not
   * retry such errors.
   *
   * @param table_name Table to synchronize
   * @return Expected containing success response string, or Error on failure
   */
  mygram::utils::Expected<std::string, mygram::utils::Error> StartSync(const std::string& table_name);

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
   *
   * After this returns, StartSync() will reject any new request with
   * kServerShuttingDown. Existing syncs are cancelled (their loaders are
   * told to abort); the caller is expected to follow up with
   * WaitForCompletion() to drain the worker threads before destruction.
   */
  void RequestShutdown();

  /**
   * @brief Wait for all sync operations to complete (with timeout)
   *
   * On success this also joins completed sync worker threads before returning,
   * so callers may safely tear down collaborators touched by BuildSnapshotAsync.
   *
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

  /**
   * @brief Verify that no SYNC is currently in progress.
   *
   * Convenience wrapper used by handlers (DUMP SAVE/LOAD, REPLICATION START,
   * OPTIMIZE, SET mysql.*) to centralize the conflict-check pattern. When a
   * SYNC is active, returns an Error with a formatted message of the form:
   *   "Cannot {operation} while SYNC is in progress for tables: a b c"
   *
   * @param operation Short verb phrase describing the blocked operation
   *                  (e.g., "save dump", "start replication"). Used inside
   *                  the formatted error message.
   * @return Empty Expected on success (no syncs in progress); Unexpected
   *         containing an `ErrorCode::kSyncAlreadyInProgress` Error otherwise.
   */
  mygram::utils::Expected<void, mygram::utils::Error> CheckNoSyncInProgress(std::string_view operation) const;

  /**
   * @brief Set the cache manager (for deferred initialization)
   * @param cache_manager Pointer to cache manager (must outlive this instance, can be nullptr)
   */
  void SetCacheManager(cache::CacheManager* cache_manager);

#ifdef MYGRAMDB_SYNC_TEST_HOOKS
  void MarkSyncingTableForTest(const std::string& table_name) {
    {
      std::lock_guard<std::mutex> lock(syncing_tables_mutex_);
      syncing_tables_.insert(table_name);
    }
    syncing_tables_cv_.notify_all();
  }

  void ClearSyncingTableForTest(const std::string& table_name) {
    {
      std::lock_guard<std::mutex> lock(syncing_tables_mutex_);
      syncing_tables_.erase(table_name);
    }
    syncing_tables_cv_.notify_all();
  }

  mygram::utils::Expected<void, mygram::utils::Error> RestartReplicationFromGtidForTest(mysql::IBinlogReader* reader,
                                                                                        const std::string& gtid,
                                                                                        const std::string& table_name,
                                                                                        const std::string& reason) {
    return RestartReplicationFromGtid(reader, gtid, table_name, reason);
  }
#endif

 private:
  const std::unordered_map<std::string, TableContext*>& table_contexts_;
  const config::Config* full_config_;
  mysql::IBinlogReader* binlog_reader_;
  replication_pause::Counter* replication_pause_counter_;
  std::atomic<cache::CacheManager*> cache_manager_{nullptr};

  // State tracking
  //
  // Lock ordering (when acquiring multiple locks, follow this order):
  //   sync_mutex_ -> syncing_tables_mutex_ -> loaders_mutex_
  //
  // Actual acquisition patterns:
  //   StartSync:           sync_mutex_ (holds), then syncing_tables_mutex_
  //   StopSync (specific): sync_mutex_ (holds) -> syncing_tables_mutex_ -> loaders_mutex_
  //   StopSync (all):      Each lock acquired and released independently (not nested)
  //   WaitForCompletion:   syncing_tables_mutex_ alone for waiting; then sync_mutex_ -> syncing_tables_mutex_
  //   BuildSnapshotAsync:  sync_mutex_ alone (via update_state); syncing_tables_mutex_ alone (via SyncGuard)
  //   RequestShutdown:     loaders_mutex_ alone
  //   Destructor:          sync_mutex_ alone, then thread join
  //
  // sync_mutex_ also protects sync_states_ and sync_threads_
  std::unordered_map<std::string, SyncState> sync_states_;
  mutable std::mutex sync_mutex_;

  std::unordered_set<std::string> syncing_tables_;
  mutable std::mutex syncing_tables_mutex_;
  std::condition_variable syncing_tables_cv_;  ///< Notified when entries are removed from syncing_tables_

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

  /**
   * @brief Restart replication from a saved GTID position
   *
   * Used to restore replication after SYNC cancellation, failure, or exception.
   * The reader is stopped before calling this method. On success, replication
   * resumes from the given GTID. On failure, returns the Start() error after
   * logging it.
   *
   * @param reader Binlog reader to restart (must not be nullptr)
   * @param gtid GTID position to resume from
   * @param table_name Table name for logging context
   * @param reason Reason for restart (e.g., "sync_cancelled", "sync_failed")
   */
  mygram::utils::Expected<void, mygram::utils::Error> RestartReplicationFromGtid(mysql::IBinlogReader* reader,
                                                                                 const std::string& gtid,
                                                                                 const std::string& table_name,
                                                                                 const std::string& reason);
};

}  // namespace mygramdb::server

#endif  // USE_MYSQL
