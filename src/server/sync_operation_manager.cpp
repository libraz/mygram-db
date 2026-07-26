/**
 * @file sync_operation_manager.cpp
 * @brief SYNC operation manager implementation
 */
// Logging is exclusively via mygram::utils::StructuredLog. Direct spdlog usage is prohibited in server code.

#ifdef USE_MYSQL

#include "server/sync_operation_manager.h"

#include <iomanip>
#include <optional>
#include <shared_mutex>
#include <sstream>
#include <thread>

#include "cache/cache_manager.h"
#include "loader/initial_loader.h"
#include "mysql/binlog_reader_interface.h"
#include "mysql/connection.h"
#include "mysql/gtid_waiter.h"
#include "server/replication_pause_counter.h"
#include "server/response_formatter.h"
#include "utils/fd_guard.h"
#include "utils/memory_utils.h"
#include "utils/string_utils.h"
#include "utils/structured_log.h"

namespace mygramdb::server {

namespace {
constexpr int kDefaultSyncWaitTimeoutSec = 30;
constexpr auto kSyncCatchupTimeout = std::chrono::seconds(60);
}  // namespace

SyncOperationManager::SyncOperationManager(const std::unordered_map<std::string, TableContext*>& table_contexts,
                                           const config::Config* full_config, mysql::IBinlogReader* binlog_reader,
                                           replication_pause::Counter* replication_pause_counter,
                                           cache::CacheManager* cache_manager)
    : table_contexts_(table_contexts),
      full_config_(full_config),
      binlog_reader_(binlog_reader),
      replication_pause_counter_(replication_pause_counter) {
  cache_manager_.store(cache_manager, std::memory_order_release);
}

void SyncOperationManager::SetCacheManager(cache::CacheManager* cache_manager) {
  cache_manager_.store(cache_manager, std::memory_order_release);
}

void SyncOperationManager::SetCurrentConfigProvider(CurrentConfigProvider provider) {
  current_config_provider_ = std::move(provider);
}

void SyncOperationManager::SetCompletionCallback(CompletionCallback callback) {
  completion_callback_ = std::move(callback);
}

std::optional<config::Config> SyncOperationManager::GetCurrentConfigSnapshot() const {
  if (current_config_provider_) {
    return current_config_provider_();
  }
  if (full_config_ != nullptr) {
    return *full_config_;
  }
  return std::nullopt;
}

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

// StartSync uses a two-phase locking pattern to safely reuse table-name slots
// in sync_threads_ while never holding sync_mutex_ across a thread join.
//
// Step 1 (under sync_mutex_):
//   - Validate table existence and that no sync is currently is_running.
//   - If a stale, non-joined std::thread is parked in sync_threads_ for this
//     table (e.g. a previous SYNC ran to completion but its slot was never
//     reaped), MOVE it out under the lock and tag the state as
//     "JOINING_PREVIOUS". Concurrent StartSync/StopSync that look up this
//     table observe is_running=false but a transitional status, which lets
//     them give a clean error rather than racing into a partially-modified
//     map state.
//
// Step 2 (lock released): join the moved-out thread. This is purely a
//   resource-cleanup step; no other locks are held.
//
// Step 3 (under sync_mutex_ again): re-validate that no concurrent caller
//   has flipped is_running back to true (e.g. another StartSync that ran the
//   same dance), initialize sync_states_[table_name], publish the new thread
//   into sync_threads_ via std::thread, and mark syncing_tables_.
//
// Why we cannot hold sync_mutex_ across the previous_thread.join(): the
// background BuildSnapshotAsync's terminal update_state lambda acquires
// sync_mutex_ to publish "COMPLETED"/"FAILED". Holding sync_mutex_ here would
// deadlock against that.
//
// Why JOINING_PREVIOUS: it ensures concurrent StartSync calls observe a
// distinct, non-IDLE state during the join window and refuse to start a new
// sync rather than seeing is_running=false and racing into a duplicate
// thread launch.
mygram::utils::Expected<std::string, mygram::utils::Error> SyncOperationManager::StartSync(
    const std::string& table_name) {
  using mygram::utils::ErrorCode;
  using mygram::utils::MakeError;
  using mygram::utils::MakeUnexpected;

  // H-C4: Reject any new SYNC if RequestShutdown() has been called.
  //
  // Prior to this check there was a narrow race window where
  // RequestShutdown() set shutdown_requested_ = true but a concurrent
  // StartSync() (still holding a reference to the manager from the
  // request dispatcher) could slip past the early checks, claim the
  // sync slot, and spawn a worker thread — only for the worker to be
  // immediately Cancel()'d by the same RequestShutdown(). The cancelled
  // worker would race the manager destructor's join phase (~30s timeout)
  // and leave the dispatcher waiting on a SYNC that will never make
  // progress. Failing fast here matches the contract documented in
  // RequestShutdown(): once shutdown is requested no new long-running
  // operations are accepted; existing ones are cancelled and joined.
  //
  // The acquire fence pairs with the release-store in RequestShutdown()
  // so any state RequestShutdown() published before flipping the flag
  // (e.g. cancellation of in-flight loaders) is visible here.
  if (shutdown_requested_.load(std::memory_order_acquire)) {
    return MakeUnexpected(MakeError(ErrorCode::kServerShuttingDown,
                                    "Cannot start SYNC for '" + table_name + "': server is shutting down"));
  }

  // Step 1: validation + grab any stale thread under the lock.
  std::thread previous_thread;
  OperationCoordinator::Token operation_token;
  {
    std::unique_lock<std::mutex> lock(sync_mutex_);

    // Check if table exists
    if (table_contexts_.find(table_name) == table_contexts_.end()) {
      return MakeUnexpected(MakeError(ErrorCode::kSyncTableNotFound, "Table '" + table_name + "' not found"));
    }

    auto acquired = operation_coordinator_.TryAcquire(LongOperation::kSync, table_name);
    if (!acquired.has_value()) {
      return MakeUnexpected(
          MakeError(ErrorCode::kSyncAlreadyInProgress,
                    "Cannot start SYNC while " + operation_coordinator_.DescribeActive() + " is in progress"));
    }
    operation_token = std::move(*acquired);

    // Check if already running. is_running is true while a worker thread is
    // active OR while another StartSync is in its JOINING_PREVIOUS window.
    if (sync_states_[table_name].is_running) {
      return MakeUnexpected(
          MakeError(ErrorCode::kSyncAlreadyInProgress, "SYNC already in progress for '" + table_name + "'"));
    }
    if (sync_states_[table_name].status == "JOINING_PREVIOUS") {
      return MakeUnexpected(MakeError(ErrorCode::kSyncAlreadyInProgress,
                                      "SYNC for '" + table_name + "' is being restarted; please retry shortly"));
    }

    // CRITICAL: claim the slot unconditionally before releasing the lock.
    // Concurrent StartSync racers checking is_running above must see this
    // claim and bail out. Earlier versions only claimed when a previous
    // thread existed; for the first concurrent burst (no stale thread), all
    // racers passed the check and Step 3 spawned multiple threads -> abort
    // (regression test ConcurrentStartSyncIsRaceFree).
    sync_states_[table_name].is_running = true;
    sync_states_[table_name].status = "JOINING_PREVIOUS";

    // Move any stale thread out of sync_threads_ for joining outside the
    // lock. The JOINING_PREVIOUS status is set above whether or not a stale
    // thread exists, so concurrent racers always observe a non-IDLE state.
    auto thread_iter = sync_threads_.find(table_name);
    if (thread_iter != sync_threads_.end() && thread_iter->second.joinable()) {
      previous_thread = std::move(thread_iter->second);
      sync_threads_.erase(thread_iter);
    }
  }

  // Step 2: join the previous thread WITHOUT holding sync_mutex_, so that
  // its terminal update_state lambda (which itself acquires sync_mutex_) can
  // make progress.
  if (previous_thread.joinable()) {
    previous_thread.join();
  }

  // Step 3: re-acquire the lock and finish initialization.
  {
    std::unique_lock<std::mutex> lock(sync_mutex_);

    // Memory health check (deferred until after join so we use fresh data).
    auto health = mygram::utils::GetMemoryHealthStatus();
    if (health == mygram::utils::MemoryHealthStatus::CRITICAL) {
      // Roll back the JOINING_PREVIOUS claim if we set it.
      if (sync_states_[table_name].status == "JOINING_PREVIOUS") {
        sync_states_[table_name].is_running = false;
        sync_states_[table_name].status.clear();
      }
      return MakeUnexpected(MakeError(ErrorCode::kSyncMemoryCritical, "Memory critically low. Cannot start SYNC."));
    }

    // Log session timeout from the same live configuration source used by
    // the worker. Runtime SET changes must never fall back to startup config.
    const auto current_config = GetCurrentConfigSnapshot();
    uint32_t session_timeout =
        current_config.has_value() ? static_cast<uint32_t>(current_config->mysql.session_timeout_sec) : 0;
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

    // Initialize state. is_running may already be true if we set it during
    // Step 1 (JOINING_PREVIOUS); in either case we want it true now.
    sync_states_[table_name].is_running = true;
    sync_states_[table_name].cancel_requested = false;
    sync_states_[table_name].status = "STARTING";
    sync_states_[table_name].table_name = table_name;
    sync_states_[table_name].processed_rows = 0;
    sync_states_[table_name].error_message.clear();

    // Launch async build (store thread instead of detaching)
    // Wrap in try/catch to rollback state if thread creation fails
    try {
      sync_threads_[table_name] = std::thread(
          [this, table_name, token = std::move(operation_token)]() mutable { BuildSnapshotAsync(table_name); });
    } catch (const std::system_error& e) {
      // Rollback: remove from syncing_tables_ and reset sync state
      {
        std::lock_guard<std::mutex> sync_lock(syncing_tables_mutex_);
        syncing_tables_.erase(table_name);
      }
      syncing_tables_cv_.notify_all();
      sync_states_[table_name].is_running = false;
      sync_states_[table_name].status = "FAILED";
      sync_states_[table_name].error_message = std::string("Failed to create sync thread: ") + e.what();
      return MakeUnexpected(
          MakeError(ErrorCode::kSyncThreadCreationFailed, "Failed to create sync thread: " + std::string(e.what())));
    }
  }

  return ResponseFormatter::FormatStatus("SYNC STARTED table=" + table_name);
}

std::string SyncOperationManager::GetSyncStatus() {
  std::lock_guard<std::mutex> lock(sync_mutex_);

  std::ostringstream oss;
  bool any_active = false;

  for (const auto& [table_name, state] : sync_states_) {
    if (!state.is_running && state.status.empty()) {
      continue;
    }

    if (!any_active) {
      oss << "SYNC_STATUS\r\n";
    }
    any_active = true;
    oss << "table=" << ResponseFormatter::SanitizeDelimitedField(table_name)
        << " status=" << ResponseFormatter::SanitizeDelimitedField(state.status);

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
        oss << " gtid=" << ResponseFormatter::SanitizeDelimitedField(state.gtid);
      }
      oss << " replication=" << ResponseFormatter::SanitizeDelimitedField(state.replication_status);
    } else if (state.status == "FAILED") {
      oss << " rows=" << state.processed_rows.load() << " error=\""
          << ResponseFormatter::SanitizeDelimitedField(state.error_message) << "\"";
      if (!state.replication_status.empty()) {
        oss << " replication=" << ResponseFormatter::SanitizeDelimitedField(state.replication_status);
      }
    } else if (state.status == "CANCELLED") {
      oss << " error=\"" << ResponseFormatter::SanitizeDelimitedField(state.error_message) << "\"";
      if (!state.replication_status.empty()) {
        oss << " replication=" << ResponseFormatter::SanitizeDelimitedField(state.replication_status);
      }
    }

    oss << "\r\n";
  }

  if (!any_active) {
    return ResponseFormatter::FormatStatus(R"(SYNC_STATUS)"
                                           "\r\n"
                                           R"(status=IDLE message="No sync operation performed")"
                                           "\r\n"
                                           "END\r\n");
  }

  oss << "END\r\n";
  return ResponseFormatter::FormatStatus(oss.str());
}

std::string SyncOperationManager::StopSync(const std::string& table_name) {
  // Empty table_name means stop all.
  //
  // TOCTOU note: The stop-all path intentionally acquires syncing_tables_mutex_,
  // loaders_mutex_, and sync_mutex_ independently (not nested) to avoid deadlock
  // with BuildSnapshotAsync and other paths that hold these locks in different
  // combinations. This creates TOCTOU windows where a sync could complete or
  // start between the separate lock acquisitions. This is acceptable because:
  //   1. InitialLoader::Cancel() is idempotent -- calling it on an already-
  //      finished loader is a harmless no-op.
  //   2. A sync that starts after the snapshot of syncing_tables_ will simply
  //      not be cancelled by this particular StopSync call; the caller can
  //      retry or rely on RequestShutdown() for full cleanup.
  if (table_name.empty()) {
    std::vector<std::string> tables_to_stop;

    // Collect active sync tables (lock released before acquiring loaders_mutex_)
    {
      std::lock_guard<std::mutex> lock(syncing_tables_mutex_);
      tables_to_stop.assign(syncing_tables_.begin(), syncing_tables_.end());
    }

    if (tables_to_stop.empty()) {
      return ResponseFormatter::FormatError("No active SYNC operations to stop");
    }

    // Publish the cancellation request before looking up loaders. A worker
    // that has not registered its stack-local loader yet observes this flag
    // and cancels as soon as the loader is available.
    {
      std::lock_guard<std::mutex> lock(sync_mutex_);
      for (const auto& tbl : tables_to_stop) {
        auto state_iter = sync_states_.find(tbl);
        if (state_iter != sync_states_.end() && state_iter->second.is_running) {
          state_iter->second.cancel_requested.store(true, std::memory_order_release);
          state_iter->second.status = "CANCELLING";
          state_iter->second.error_message = "Cancellation requested by user";
        }
      }
    }

    // Cancel all loaders that are already registered. Cancel() is idempotent.
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

    // Do not join on the request dispatcher. The worker owns terminal-state
    // publication and is reaped by WaitForCompletion, restart, or destruction.
    return ResponseFormatter::FormatStatus("SYNC CANCELLATION REQUESTED count=" +
                                           std::to_string(tables_to_stop.size()));
  }

  // Stop a specific table. Signal only: never join on the request dispatcher.
  {
    std::lock_guard<std::mutex> sync_lock(sync_mutex_);
    {
      std::lock_guard<std::mutex> syncing_lock(syncing_tables_mutex_);
      if (syncing_tables_.find(table_name) == syncing_tables_.end()) {
        return ResponseFormatter::FormatError("No active SYNC operation for table: " + table_name);
      }
    }

    auto state_iter = sync_states_.find(table_name);
    if (state_iter == sync_states_.end() || !state_iter->second.is_running) {
      return ResponseFormatter::FormatError("No active SYNC operation for table: " + table_name);
    }
    state_iter->second.cancel_requested.store(true, std::memory_order_release);
    state_iter->second.status = "CANCELLING";
    state_iter->second.error_message = "Cancellation requested by user";

    // Cancel the loader if it is already registered. If not, the worker will
    // observe cancel_requested immediately after registration.
    {
      std::lock_guard<std::mutex> loader_lock(loaders_mutex_);
      auto iter = active_loaders_.find(table_name);
      if (iter != active_loaders_.end()) {
        mygram::utils::StructuredLog()
            .Event("sync_stop")
            .Field("table", table_name)
            .Field("source", "user_request")
            .Info();
        iter->second->Cancel();
      }
    }
  }

  return ResponseFormatter::FormatStatus("SYNC CANCELLATION REQUESTED table=" + table_name);
}

void SyncOperationManager::RequestShutdown() {
  // Use release-store so the StartSync acquire-load (H-C4) sees this
  // store and refuses any new SYNC after we have begun shutdown. The
  // semantics are: once RequestShutdown returns, no new SYNC can be
  // started and all in-flight syncs have been Cancel()ed.
  shutdown_requested_.store(true, std::memory_order_release);
  operation_coordinator_.BlockNewOperationsForShutdown();

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
  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(timeout_sec);

  {
    std::unique_lock<std::mutex> lock(syncing_tables_mutex_);
    while (!syncing_tables_.empty()) {
      if (syncing_tables_cv_.wait_until(lock, deadline) == std::cv_status::timeout) {
        // Check one more time after timeout (spurious wakeup protection)
        if (syncing_tables_.empty()) {
          break;
        }
        mygram::utils::StructuredLog()
            .Event("wait_all_sync_complete_timeout")
            .Field("timeout_sec", static_cast<uint64_t>(timeout_sec))
            .Warn();
        return false;
      }
    }
  }

  std::unordered_map<std::string, std::thread> completed_threads;
  {
    std::lock_guard<std::mutex> sync_lock(sync_mutex_);
    std::lock_guard<std::mutex> tables_lock(syncing_tables_mutex_);
    for (auto iter = sync_threads_.begin(); iter != sync_threads_.end();) {
      if (syncing_tables_.find(iter->first) == syncing_tables_.end() && iter->second.joinable()) {
        completed_threads.emplace(iter->first, std::move(iter->second));
        iter = sync_threads_.erase(iter);
      } else {
        ++iter;
      }
    }
  }

  for (auto& [table_name, thread] : completed_threads) {
    mygram::utils::StructuredLog().Event("sync_thread_joining").Field("table", table_name).Debug();
    thread.join();
  }

  return true;
}

void SyncOperationManager::WaitForCompletion() {
  {
    std::unique_lock<std::mutex> lock(syncing_tables_mutex_);
    syncing_tables_cv_.wait(lock, [this] { return syncing_tables_.empty(); });
  }

  std::unordered_map<std::string, std::thread> completed_threads;
  {
    std::lock_guard<std::mutex> sync_lock(sync_mutex_);
    std::lock_guard<std::mutex> tables_lock(syncing_tables_mutex_);
    for (auto iter = sync_threads_.begin(); iter != sync_threads_.end();) {
      if (iter->second.joinable()) {
        completed_threads.emplace(iter->first, std::move(iter->second));
      }
      iter = sync_threads_.erase(iter);
    }
  }
  for (auto& entry : completed_threads) {
    entry.second.join();
  }
}

bool SyncOperationManager::IsAnySyncing() const {
  std::lock_guard<std::mutex> lock(syncing_tables_mutex_);
  return !syncing_tables_.empty();
}

std::unordered_set<std::string> SyncOperationManager::GetSyncingTables() const {
  std::lock_guard<std::mutex> lock(syncing_tables_mutex_);
  if (!syncing_tables_.empty() && full_config_ != nullptr && full_config_->replication.enable) {
    std::unordered_set<std::string> all_tables;
    all_tables.reserve(table_contexts_.size());
    for (const auto& [table_name, context] : table_contexts_) {
      (void)context;
      all_tables.insert(table_name);
    }
    return all_tables;
  }
  return syncing_tables_;
}

bool SyncOperationManager::GetSyncingTablesIfAny(std::vector<std::string>& out_tables) const {
  std::lock_guard<std::mutex> lock(syncing_tables_mutex_);
  if (syncing_tables_.empty()) {
    return false;
  }
  if (full_config_ != nullptr && full_config_->replication.enable) {
    out_tables.clear();
    out_tables.reserve(table_contexts_.size());
    for (const auto& [table_name, context] : table_contexts_) {
      (void)context;
      out_tables.push_back(table_name);
    }
  } else {
    out_tables.assign(syncing_tables_.begin(), syncing_tables_.end());
  }
  return true;
}

mygram::utils::Expected<void, mygram::utils::Error> SyncOperationManager::CheckNoSyncInProgress(
    std::string_view operation) const {
  std::vector<std::string> syncing_tables;
  if (!GetSyncingTablesIfAny(syncing_tables)) {
    return {};
  }
  // Build the conflict message in the historical format:
  //   "Cannot {operation} while SYNC is in progress for tables: a b c"
  std::ostringstream oss;
  oss << "Cannot " << operation << " while SYNC is in progress for tables:";
  for (const auto& table : syncing_tables) {
    oss << " " << table;
  }
  return mygram::utils::MakeUnexpected(
      mygram::utils::MakeError(mygram::utils::ErrorCode::kSyncAlreadyInProgress, oss.str()));
}

void SyncOperationManager::BuildSnapshotAsync(const std::string& table_name) {
  // Update state under lock
  {
    std::lock_guard<std::mutex> lock(sync_mutex_);
    sync_states_[table_name].status =
        sync_states_[table_name].cancel_requested.load(std::memory_order_acquire) ? "CANCELLING" : "IN_PROGRESS";
    sync_states_[table_name].start_time = std::chrono::steady_clock::now();
  }

  // Helper to update state safely
  auto update_state = [this, table_name](auto&& updater) {
    std::lock_guard<std::mutex> lock(sync_mutex_);
    updater(sync_states_[table_name]);
  };
  auto cancellation_requested = [this, table_name]() {
    if (shutdown_requested_.load(std::memory_order_acquire)) {
      return true;
    }
    std::lock_guard<std::mutex> lock(sync_mutex_);
    const auto iter = sync_states_.find(table_name);
    return iter != sync_states_.end() && iter->second.cancel_requested.load(std::memory_order_acquire);
  };
  auto publish_cancelled = [this, &update_state]() {
    const bool shutting_down = shutdown_requested_.load(std::memory_order_acquire);
    update_state([shutting_down](SyncState& state) {
      state.status = "CANCELLED";
      state.error_message = shutting_down ? "Server shutdown requested" : "Cancelled by user (SYNC STOP)";
      state.is_running = false;
    });
  };

  // Variables for replication restart on cancellation/failure/exception.
  // Declared before the try block so they are accessible in catch.
  mysql::IBinlogReader* reader = binlog_reader_;
  bool replication_was_running = false;
  std::string saved_gtid;
  std::optional<replication_pause::Scope> replication_pause_scope;
  TableContext* live_ctx = nullptr;
  std::unique_ptr<index::Index> candidate_index;
  std::unique_ptr<storage::DocumentStore> candidate_doc_store;
  bool live_state_swapped = false;
  bool replay_watermark_published = false;
  uint64_t previous_bm25_total_length = 0;
  uint64_t previous_bm25_doc_count = 0;

  // RAII cleanup guard
  struct SyncGuard {
    SyncOperationManager* mgr;
    std::string table;
    explicit SyncGuard(SyncOperationManager* manager, std::string table_name)
        : mgr(manager), table(std::move(table_name)) {}
    ~SyncGuard() {
      {
        std::lock_guard<std::mutex> lock(mgr->syncing_tables_mutex_);
        mgr->syncing_tables_.erase(table);
      }
      mgr->syncing_tables_cv_.notify_all();
    }
    SyncGuard(const SyncGuard&) = delete;
    SyncGuard& operator=(const SyncGuard&) = delete;
    SyncGuard(SyncGuard&&) = delete;
    SyncGuard& operator=(SyncGuard&&) = delete;
  };
  SyncGuard guard(this, table_name);
  auto release_replication_pause = mygram::utils::ScopeGuard([&replication_pause_scope]() {
    if (replication_pause_scope.has_value() && replication_pause_scope->held()) {
      replication_pause_scope->Release();
    }
  });

  try {
    if (cancellation_requested()) {
      publish_cancelled();
      return;
    }

    // Capture exactly one current configuration snapshot for this complete
    // operation. In particular, mysql.host and mysql.port may have changed
    // through SET after startup; reading individual fields lazily could mix
    // values from different revisions.
    const auto current_config = GetCurrentConfigSnapshot();
    if (!current_config.has_value()) {
      update_state([](SyncState& state) {
        state.status = "FAILED";
        state.error_message = "Configuration not available";
        state.is_running = false;
      });
      mygram::utils::StructuredLog()
          .Event("sync_failed")
          .Field("table", table_name)
          .Field("error", "Configuration not available")
          .Error();
      return;
    }

    // Connect to MySQL
    mysql::Connection::Config mysql_config{
        .host = current_config->mysql.host,
        .port = static_cast<uint16_t>(current_config->mysql.port),
        .user = current_config->mysql.user,
        .password = current_config->mysql.password,
        .database = current_config->mysql.database,
        .session_timeout_sec = static_cast<uint32_t>(current_config->mysql.session_timeout_sec)};

    auto mysql_conn = std::make_unique<mysql::Connection>(mysql_config);

    auto connect_result = mysql_conn->Connect();
    if (!connect_result) {
      std::string error_msg = "Failed to connect: " + connect_result.error().message();
      update_state([&error_msg](SyncState& state) {
        state.status = "FAILED";
        state.error_message = error_msg;
        state.is_running = false;
      });
      mygram::utils::StructuredLog()
          .Event("sync_failed")
          .Field("table", table_name)
          .Field("error", error_msg)
          .Field("error_code", static_cast<int64_t>(connect_result.error().code()))
          .Error();
      return;
    }
    if (cancellation_requested()) {
      publish_cancelled();
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
      mygram::utils::StructuredLog()
          .Event("sync_failed")
          .Field("table", table_name)
          .Field("error", "Table context not found")
          .Error();
      return;
    }

    auto* ctx = table_iter->second;
    live_ctx = ctx;

    // Check for null pointers
    if (!ctx->index || !ctx->doc_store) {
      update_state([](SyncState& state) {
        state.status = "FAILED";
        state.error_message = "Table context has null index or doc_store";
        state.is_running = false;
      });
      return;
    }

    // Build into isolated candidates. The live table remains untouched until
    // the complete MySQL snapshot has been validated. Pointer identity of the
    // live objects is preserved because BinlogReader retains TableContext
    // pointers for the lifetime of the server.
    candidate_index = std::make_unique<index::Index>(
        ctx->index->GetNgramSize(), ctx->index->GetKanjiNgramSize(), ctx->index->GetRoaringThreshold(),
        ctx->index->GetCrossBoundaryNgrams(), ctx->index->GetNormalizeNfkc(), ctx->index->GetNormalizeWidth(),
        ctx->index->GetNormalizeLower());
    candidate_doc_store = std::make_unique<storage::DocumentStore>();
    candidate_doc_store->SetStoreTexts(ctx->doc_store->IsStoreTextsEnabled());

    // Stop replication before taking the replacement snapshot. The reader is
    // global across every configured table, so its drained GTID is the only
    // safe restart point: restarting from the target table's snapshot GTID
    // would permanently skip the same interval for all non-target tables.
    // This relies on IBinlogReader::Stop()'s synchronous contract: after Stop()
    // returns, no binlog worker thread may write into index/doc_store.
    // Save the current GTID so we can restore replication if SYNC is
    // cancelled or fails.
    if (current_config->replication.enable && reader != nullptr && reader->IsRunning()) {
      if (replication_pause_counter_ != nullptr) {
        replication_pause_scope.emplace(*replication_pause_counter_);
        const bool first_pauser = replication_pause_scope->Acquire();
        if (!first_pauser) {
          update_state([](SyncState& state) {
            state.status = "FAILED";
            state.error_message = "Replication is already paused by another operation";
            state.is_running = false;
            state.replication_status = "FAILED";
          });
          mygram::utils::StructuredLog()
              .Event("sync_failed")
              .Field("table", table_name)
              .Field("error", "Replication is already paused by another operation")
              .Error();
          return;
        }
      }
      mygram::utils::StructuredLog()
          .Event("replication_stopping")
          .Field("operation", "sync")
          .Field("table", table_name)
          .Field("reason", "stop_before_candidate_snapshot")
          .Info();
      reader->Stop();
      // Stop() is synchronous. Capture the position only after all worker
      // mutations have drained; a pre-Stop read can lag the actual live state.
      saved_gtid = reader->GetCurrentGTID();
      replication_was_running = true;
      if (replication_pause_counter_ != nullptr) {
        replication_pause_counter_->PublishDrainedGTID(saved_gtid);
      }
    } else if (current_config->replication.enable && reader != nullptr) {
      // A stopped reader still owns the only safe lower bound for non-target
      // tables. Starting from the target snapshot would silently skip their
      // backlog. The target watermark below makes replay from this position
      // safe even after a schema-incompatible stop.
      saved_gtid = reader->GetCurrentGTID();
    }

    // Build the initial load off to the side. Failure or cancellation simply
    // destroys these candidates and cannot erase the pre-SYNC live table.
    loader::InitialLoader loader(*mysql_conn, *candidate_index, *candidate_doc_store, ctx->config,
                                 current_config->mysql, current_config->build);

    auto loader_registration = RegisterLoaderScoped(table_name, &loader);
    if (cancellation_requested()) {
      loader.Cancel();
    }

    // Extract atomic pointers under sync_mutex_ to avoid dangling reference
    // if sync_states_ rehashes from concurrent insertions.
    std::atomic<uint64_t>* total_rows_ptr;
    std::atomic<uint64_t>* processed_rows_ptr;
    {
      std::lock_guard<std::mutex> lock(sync_mutex_);
      auto& state = sync_states_[table_name];
      total_rows_ptr = &state.total_rows;
      processed_rows_ptr = &state.processed_rows;
    }
    auto result = loader.Load([total_rows_ptr, processed_rows_ptr](const auto& progress) {
      total_rows_ptr->store(progress.total_rows, std::memory_order_relaxed);
      processed_rows_ptr->store(progress.processed_rows, std::memory_order_relaxed);
    });
    UnregisterLoader(table_name);
    loader_registration.Release();

    // Handle cancellation (both shutdown and user-requested SYNC STOP)
    // Check if loader was cancelled OR shutdown was requested
    // Include the manager-level request flag as well as the loader flag. A
    // stop can race the narrow interval after the loader unregisters and
    // before this terminal-state check; it must still win over completion.
    bool was_cancelled = loader.IsCancelled() || cancellation_requested();

    if (was_cancelled) {
      uint64_t partial_rows = loader.GetProcessedRows();
      std::string cancel_reason = shutdown_requested_ ? "shutdown" : "user_stop_request";

      mygram::utils::StructuredLog()
          .Event("sync_candidate_discarded")
          .Field("table", table_name)
          .Field("reason", cancel_reason)
          .Field("partial_rows_discarded", partial_rows)
          .Field("message", "Candidate data discarded; live table preserved")
          .Warn();

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

      // Restart replication from the saved GTID position (before SYNC started).
      // The cancelled SYNC did not complete, so we restore to the pre-SYNC state.
      if (replication_was_running && !shutdown_requested_) {
        auto restart_result = RestartReplicationFromGtid(reader, saved_gtid, table_name, "sync_cancelled");
        if (!restart_result) {
          std::string restart_error = "Cancelled but replication restart failed: " + restart_result.error().message();
          update_state([&restart_error](SyncState& state) {
            state.replication_status = "FAILED";
            state.error_message = restart_error;
          });
        }
      }
      return;
    }

    // Handle result
    if (result) {
      // SYNC succeeded
      std::string gtid = loader.GetStartGTID();
      uint64_t processed = loader.GetProcessedRows();

      std::string catchup_target_gtid;
      if (current_config->replication.enable && reader != nullptr) {
        auto catchup_target = mysql_conn->GetLatestGTID();
        if (!catchup_target) {
          std::string error_msg =
              "Snapshot built but post-snapshot GTID capture failed: " + catchup_target.error().message();
          if (replication_was_running) {
            auto restart_result = RestartReplicationFromGtid(reader, saved_gtid, table_name, "sync_target_failed");
            if (!restart_result) {
              error_msg += "; replication restart failed: " + restart_result.error().message();
            }
          }
          update_state([&error_msg](SyncState& state) {
            state.status = "FAILED";
            state.replication_status = "FAILED";
            state.error_message = error_msg;
            state.is_running = false;
          });
          mygram::utils::StructuredLog()
              .Event("sync_catchup_target_failed")
              .Field("table", table_name)
              .FieldError(catchup_target.error())
              .Error();
          return;
        }
        catchup_target_gtid = *catchup_target;
      }

      uint64_t candidate_total_length = 0;
      uint64_t candidate_doc_count = 0;
      for (auto doc_id : candidate_doc_store->GetAllDocIds()) {
        auto text = candidate_doc_store->GetNormalizedText(doc_id);
        if (text.has_value() && !text->empty()) {
          candidate_total_length += mygram::utils::CountCodePoints(*text);
          ++candidate_doc_count;
        }
      }

      // Provisional, reversible commit. ReplaceWithLoaded() swaps state, so
      // the candidates retain the complete old live table for rollback until
      // replication has restarted successfully.
      {
        std::unique_lock<std::shared_mutex> generation_lock(*ctx->generation_mutex);
        previous_bm25_total_length = ctx->bm25_stats.total_doc_length.load(std::memory_order_relaxed);
        previous_bm25_doc_count = ctx->bm25_stats.doc_count.load(std::memory_order_relaxed);
        // The shared reader must restart from the older drained position so
        // non-target tables do not lose commits made during this snapshot.
        {
          std::lock_guard<std::mutex> replay_lock(ctx->replay_watermark->mutex);
          ctx->replay_watermark->snapshot_gtid =
              (current_config->replication.enable && reader != nullptr) ? gtid : std::string{};
        }
        ctx->index->ReplaceWithLoaded(*candidate_index);
        ctx->doc_store->ReplaceWithLoaded(*candidate_doc_store);
        ctx->bm25_stats.SetCorpusStats(candidate_total_length, candidate_doc_count);
      }
      replay_watermark_published = current_config->replication.enable && reader != nullptr && !gtid.empty();
      live_state_swapped = true;

      // Restart from the drained pre-SYNC position when the shared reader was
      // running. Replaying the target-table overlap is intentionally
      // idempotent, while advancing to the snapshot marker would skip
      // non-target events that committed during the rebuild.
      // Log replication configuration for debugging
      mygram::utils::StructuredLog()
          .Event("sync_replication_check")
          .Field("table", table_name)
          .Field("replication_enable", current_config->replication.enable)
          .Field("reader_exists", reader != nullptr)
          .Field("gtid_empty", gtid.empty())
          .Field("gtid", gtid)
          .Info();

      const std::string restart_gtid = saved_gtid;
      bool replication_started = false;
      if (current_config->replication.enable && reader != nullptr) {
        // If replication is still running (e.g., was not stopped earlier because
        // it wasn't enabled at that point), stop it now before updating GTID.
        if (!replication_was_running && reader->IsRunning()) {
          reader->Stop();
        }

        // Always set GTID and start replication after SYNC
        mygram::utils::StructuredLog()
            .Event("sync_setting_gtid")
            .Field("table", table_name)
            .Field("gtid", restart_gtid)
            .Info();

        reader->SetCurrentGTID(restart_gtid);

        // Log before attempting to start replication
        mygram::utils::StructuredLog()
            .Event("sync_starting_replication")
            .Field("table", table_name)
            .Field("gtid", restart_gtid)
            .Field("reader_running", reader->IsRunning())
            .Info();

        auto start_result = reader->Start();
        std::optional<mygram::utils::Error> handoff_error;
        if (!start_result) {
          handoff_error = start_result.error();
        } else {
          auto catchup_result = mysql::WaitForAppliedPosition(
              *reader, catchup_target_gtid, kSyncCatchupTimeout, std::chrono::milliseconds(10),
              [this]() { return shutdown_requested_.load(std::memory_order_acquire); });
          if (!catchup_result) {
            handoff_error = catchup_result.error();
          }
        }

        if (handoff_error.has_value()) {
          // Do not expose a snapshot whose replication hand-off failed. Swap
          // the exact previous states back into place before reporting failure.
          reader->Stop();
          {
            std::unique_lock<std::shared_mutex> generation_lock(*ctx->generation_mutex);
            ctx->index->ReplaceWithLoaded(*candidate_index);
            ctx->doc_store->ReplaceWithLoaded(*candidate_doc_store);
            ctx->bm25_stats.SetCorpusStats(previous_bm25_total_length, previous_bm25_doc_count);
            std::lock_guard<std::mutex> replay_lock(ctx->replay_watermark->mutex);
            ctx->replay_watermark->snapshot_gtid.clear();
          }
          replay_watermark_published = false;
          live_state_swapped = false;

          std::string error_msg = "Snapshot discarded because replication hand-off failed: " + handoff_error->message();
          if (replication_was_running) {
            auto rollback_restart = RestartReplicationFromGtid(reader, saved_gtid, table_name, "sync_swap_rollback");
            if (!rollback_restart) {
              error_msg += "; rollback replication restart failed: " + rollback_restart.error().message();
            }
          }
          update_state([&error_msg](SyncState& state) {
            state.status = "FAILED";
            state.replication_status = "FAILED";
            state.error_message = error_msg;
            state.is_running = false;
          });
          mygram::utils::StructuredLog()
              .Event("sync_replication_handoff_failed")
              .Field("table", table_name)
              .Field("target_gtid", catchup_target_gtid)
              .FieldError(*handoff_error)
              .Error();
          return;
        }

        replication_started = true;
        {
          std::lock_guard<std::mutex> replay_lock(ctx->replay_watermark->mutex);
          ctx->replay_watermark->snapshot_gtid.clear();
        }
        replay_watermark_published = false;
        update_state([](SyncState& state) { state.replication_status = "STARTED"; });
        mygram::utils::StructuredLog()
            .Event("sync_completed")
            .Field("table", table_name)
            .Field("rows", processed)
            .Field("gtid", restart_gtid)
            .Field("catchup_target_gtid", catchup_target_gtid)
            .Field("replication_status", "started")
            .Info();
      } else {
        update_state([](SyncState& state) { state.replication_status = "DISABLED"; });
        mygram::utils::StructuredLog()
            .Event("sync_completed")
            .Field("table", table_name)
            .Field("rows", processed)
            .Field("replication_status", "disabled")
            .Info();
      }

      // The replacement and its replication hand-off are now committed.
      live_state_swapped = false;
      update_state([&gtid, processed, replication_started](SyncState& state) {
        state.status = "COMPLETED";
        state.gtid = gtid;
        state.processed_rows = processed;
        state.is_running = false;
        if (!replication_started && state.replication_status.empty()) {
          state.replication_status = "DISABLED";
        }
      });

      auto* cache_mgr = cache_manager_.load(std::memory_order_acquire);
      if (cache_mgr != nullptr) {
        cache_mgr->ClearTable(table_name);
        mygram::utils::StructuredLog()
            .Event("sync_cache_cleared")
            .Field("table", table_name)
            .Field("rows", processed)
            .Info();
      }
      if (completion_callback_) {
        completion_callback_(table_name);
      }
    } else {
      // SYNC failed. Only the isolated candidate contains partial data; the
      // live table remains byte-for-byte unchanged.
      std::string error_msg = result.error().message();

      // Check if error might be session timeout related
      bool is_timeout_related =
          (error_msg.find("disconnected") != std::string::npos || error_msg.find("timeout") != std::string::npos ||
           error_msg.find("connection") != std::string::npos || error_msg.find("Lost connection") != std::string::npos);

      if (is_timeout_related) {
        uint32_t session_timeout = current_config->mysql.session_timeout_sec;
        error_msg += " (check if session_timeout_sec=" + std::to_string(session_timeout) +
                     " is sufficient for snapshot duration)";
      }

      uint64_t partial_rows = loader.GetProcessedRows();

      mygram::utils::StructuredLog()
          .Event("sync_candidate_discarded")
          .Field("table", table_name)
          .Field("reason", "sync_failed")
          .Field("partial_rows_discarded", partial_rows)
          .Field("message", "Partial candidate discarded; live table preserved")
          .Warn();

      update_state([&error_msg](SyncState& state) {
        state.status = "FAILED";
        state.error_message = error_msg;
        state.is_running = false;
      });
      mygram::utils::StructuredLog()
          .Event("sync_failed")
          .Field("table", table_name)
          .Field("error", error_msg)
          .Field("error_code", static_cast<int64_t>(result.error().code()))
          .Error();

      // Restart replication from the saved GTID position (before SYNC started).
      // The failed SYNC did not complete, so we restore to the pre-SYNC state.
      if (replication_was_running) {
        auto restart_result = RestartReplicationFromGtid(reader, saved_gtid, table_name, "sync_failed");
        if (!restart_result) {
          std::string restart_error = error_msg + "; replication restart failed: " + restart_result.error().message();
          update_state([&restart_error](SyncState& state) {
            state.replication_status = "FAILED";
            state.error_message = restart_error;
          });
        }
      }
    }

  } catch (const std::exception& e) {
    std::string error_msg = e.what();

    if (live_state_swapped && live_ctx != nullptr && candidate_index && candidate_doc_store) {
      if (reader != nullptr) {
        reader->Stop();
      }
      {
        std::unique_lock<std::shared_mutex> generation_lock(*live_ctx->generation_mutex);
        live_ctx->index->ReplaceWithLoaded(*candidate_index);
        live_ctx->doc_store->ReplaceWithLoaded(*candidate_doc_store);
        live_ctx->bm25_stats.SetCorpusStats(previous_bm25_total_length, previous_bm25_doc_count);
        std::lock_guard<std::mutex> replay_lock(live_ctx->replay_watermark->mutex);
        live_ctx->replay_watermark->snapshot_gtid.clear();
      }
      live_state_swapped = false;
    }
    if (replay_watermark_published && live_ctx != nullptr && live_ctx->replay_watermark != nullptr) {
      std::lock_guard<std::mutex> replay_lock(live_ctx->replay_watermark->mutex);
      live_ctx->replay_watermark->snapshot_gtid.clear();
      replay_watermark_published = false;
    }

    update_state([&error_msg](SyncState& state) {
      state.status = "FAILED";
      state.error_message = error_msg;
      state.is_running = false;
    });
    mygram::utils::StructuredLog().Event("sync_exception").Field("table", table_name).Field("error", error_msg).Error();

    // Restart replication from the saved GTID position (before SYNC started).
    // The exception prevented SYNC completion, so we restore to the pre-SYNC state.
    if (replication_was_running) {
      auto restart_result = RestartReplicationFromGtid(reader, saved_gtid, table_name, "sync_exception");
      if (!restart_result) {
        std::string restart_error = error_msg + "; replication restart failed: " + restart_result.error().message();
        update_state([&restart_error](SyncState& state) {
          state.replication_status = "FAILED";
          state.error_message = restart_error;
        });
      }
    }
  }

  // Safety net: should already be false from terminal branch above
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

mygram::utils::ScopeGuard<std::function<void()>> SyncOperationManager::RegisterLoaderScoped(
    const std::string& table_name, loader::InitialLoader* loader) {
  RegisterLoader(table_name, loader);
  return mygram::utils::ScopeGuard<std::function<void()>>([this, table_name]() { UnregisterLoader(table_name); });
}

mygram::utils::Expected<void, mygram::utils::Error> SyncOperationManager::RestartReplicationFromGtid(
    mysql::IBinlogReader* reader, const std::string& gtid, const std::string& table_name, const std::string& reason) {
  if (reader == nullptr) {
    mygram::utils::StructuredLog()
        .Event("replication_restart_skipped")
        .Field("table", table_name)
        .Field("reason", reason)
        .Field("reader_null", true)
        .Warn();
    return {};
  }

  mygram::utils::StructuredLog()
      .Event("replication_restart")
      .Field("table", table_name)
      .Field("reason", reason)
      .Field("gtid", gtid)
      .Info();

  // An empty position is a valid conservative lower bound during the first
  // manual SYNC: it means replay from the beginning. Skipping Start() here
  // would leave a previously-running reader permanently stopped.
  reader->SetCurrentGTID(gtid);

  auto start_result = reader->Start();
  if (start_result) {
    mygram::utils::StructuredLog()
        .Event("replication_restarted")
        .Field("table", table_name)
        .Field("reason", reason)
        .Field("gtid", gtid)
        .Info();
    return {};
  } else {
    mygram::utils::StructuredLog()
        .Event("replication_restart_failed")
        .Field("table", table_name)
        .Field("reason", reason)
        .Field("gtid", gtid)
        .FieldError(start_result.error())
        .Error();
    return mygram::utils::MakeUnexpected(start_result.error());
  }
}

}  // namespace mygramdb::server

#endif  // USE_MYSQL
