/**
 * @file sync_operation_manager.h
 * @brief Manages MySQL synchronization operations
 */

#pragma once

#ifdef USE_MYSQL

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "config/config.h"
#include "server/operation_coordinator.h"
#include "server/replication_pause_counter.h"
#include "server/server_types.h"
#include "utils/error.h"
#include "utils/expected.h"
#include "utils/fd_guard.h"

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
  std::atomic<bool> cancel_requested{false};
  std::string table_name;               // Protected by sync_mutex_
  std::atomic<uint64_t> total_rows{0};  // Atomic to prevent data races during concurrent access
  std::atomic<uint64_t> processed_rows{0};
  std::chrono::steady_clock::time_point start_time;  // Set once, read-only after init
  std::string status;  // Protected by sync_mutex_ - "IDLE", "STARTING", "IN_PROGRESS", "CANCELLING", terminal states
  std::string error_message;       // Protected by sync_mutex_
  std::string gtid;                // Protected by sync_mutex_
  std::string replication_status;  // Protected by sync_mutex_ - "STARTED", "ALREADY_RUNNING", "DISABLED", "FAILED"
};

namespace internal {

/**
 * @brief Whether replication stopped on an event this build cannot decode
 *
 * Such an event is written into the binlog, so it is still there after the
 * server setting that produced it is changed. It is the one stop reason a
 * replay cannot get past, which is why it selects a different restart point.
 *
 * @param reader Reader to inspect; a null reader is not stalled
 */
[[nodiscard]] bool StalledOnUndecodableEvent(const mysql::IBinlogReader* reader);

/**
 * @brief Position replication resumes from once a SYNC has swapped in its snapshot
 *
 * Normally the drained pre-SYNC position, so tables other than the synced one
 * do not lose commits made while the snapshot was being built. When the stream
 * is stalled on an undecodable event that interval cannot be replayed at all,
 * and the snapshot marker is the only position the stream can move forward
 * from.
 *
 * @param stalled_on_undecodable_event Result of StalledOnUndecodableEvent()
 * @param snapshot_gtid Position the candidate snapshot is consistent at
 * @param drained_gtid Position replication had applied before the SYNC
 */
[[nodiscard]] const std::string& ChooseSyncRestartGtid(bool stalled_on_undecodable_event,
                                                       const std::string& snapshot_gtid,
                                                       const std::string& drained_gtid);

}  // namespace internal

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
  using CurrentConfigProvider = std::function<config::Config()>;
  using CompletionCallback = std::function<void(const std::string&)>;

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

  /** Wait without a deadline. Used only by destructors as the final UAF guard. */
  void WaitForCompletion();

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

  /**
   * @brief Set the source of the current runtime configuration.
   *
   * The provider must outlive this manager. It is installed during server
   * initialization, before request processing starts, and allows each SYNC to
   * take one coherent configuration snapshot after runtime SET changes.
   */
  void SetCurrentConfigProvider(CurrentConfigProvider provider);

  /**
   * @brief Set a callback invoked after a table snapshot is committed.
   *
   * Installed before request processing starts. Failed, cancelled, or rolled
   * back SYNC operations never invoke it.
   */
  void SetCompletionCallback(CompletionCallback callback);

  OperationCoordinator& GetOperationCoordinator() { return operation_coordinator_; }
  const OperationCoordinator& GetOperationCoordinator() const { return operation_coordinator_; }

#ifdef MYGRAMDB_SYNC_TEST_HOOKS
  auto RegisterNullLoaderScopedForTest(const std::string& table_name) {
    return RegisterLoaderScoped(table_name, nullptr);
  }

  [[nodiscard]] size_t ActiveLoaderCountForTest() const {
    std::lock_guard<std::mutex> lock(loaders_mutex_);
    return active_loaders_.size();
  }

  [[nodiscard]] std::optional<config::Config> GetCurrentConfigSnapshotForTest() const {
    return GetCurrentConfigSnapshot();
  }

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

  void InstallSyncThreadForTest(const std::string& table_name, std::thread worker) {
    {
      std::lock_guard<std::mutex> lock(sync_mutex_);
      auto& state = sync_states_[table_name];
      state.table_name = table_name;
      state.is_running = true;
      state.cancel_requested = false;
      state.status = "IN_PROGRESS";
      sync_threads_[table_name] = std::move(worker);
    }
    MarkSyncingTableForTest(table_name);
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
  CurrentConfigProvider current_config_provider_;
  CompletionCallback completion_callback_;
  mysql::IBinlogReader* binlog_reader_;
  replication_pause::Counter* replication_pause_counter_;
  std::atomic<cache::CacheManager*> cache_manager_{nullptr};
  OperationCoordinator operation_coordinator_;

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
   * @brief Read one coherent configuration snapshot for a SYNC operation.
   */
  [[nodiscard]] std::optional<config::Config> GetCurrentConfigSnapshot() const;

  /**
   * @brief Register active initial loader
   */
  void RegisterLoader(const std::string& table_name, loader::InitialLoader* loader);

  /**
   * @brief Unregister active initial loader
   */
  void UnregisterLoader(const std::string& table_name);

  /**
   * @brief Register a loader and return an exception-safe unregistration guard.
   */
  mygram::utils::ScopeGuard<std::function<void()>> RegisterLoaderScoped(const std::string& table_name,
                                                                        loader::InitialLoader* loader);

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
