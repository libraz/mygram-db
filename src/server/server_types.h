/**
 * @file server_types.h
 * @brief Common server type definitions
 */

#pragma once

#include <atomic>
#include <cassert>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "config/config.h"
#include "index/index.h"
#include "query/synonym_dictionary.h"
#include "server/replication_pause_counter.h"
#include "server/server_stats.h"
#include "storage/document_store.h"
#include "utils/constants.h"
#include "utils/network_utils.h"

namespace mygramdb::mysql {
class IBinlogReader;
#ifdef USE_MYSQL
class BinlogReader;
#endif
}  // namespace mygramdb::mysql

namespace mygramdb::cache {
class CacheManager;
}  // namespace mygramdb::cache

namespace mygramdb::config {
class RuntimeVariableManager;
}  // namespace mygramdb::config

namespace mygramdb::server {

// Forward declarations
class TableCatalog;
class SyncOperationManager;

// Default constants
constexpr uint16_t kDefaultPort = static_cast<uint16_t>(config::defaults::kTcpPort);  // MygramDB TCP port
constexpr int kDefaultMaxConnections = 10000;                                         // Maximum concurrent connections
constexpr int kDefaultRecvBufferSize = 4096;                                          // Receive buffer size
constexpr int kDefaultSendBufferSize = 65536;                                         // Send buffer size

/**
 * @brief TCP server configuration
 */
struct ServerConfig {
  std::string host = "127.0.0.1";
  uint16_t port = kDefaultPort;
  int max_connections = kDefaultMaxConnections;
  int worker_threads = 0;  // Number of worker threads (0 = CPU count)
  int recv_buffer_size = kDefaultRecvBufferSize;
  int send_buffer_size = kDefaultSendBufferSize;
  int default_limit = config::defaults::kDefaultLimit;  // Default LIMIT for SEARCH queries (range: 5-1000)
  int max_query_length = config::defaults::kDefaultQueryLengthLimit;  // Max characters for query expressions

  // Connection I/O tunables.
  int recv_timeout_sec = 60;          ///< Initial first-frame timeout seconds (0 = disabled)
  int thread_pool_queue_size = 1000;  ///< ThreadPool task queue bound; 0 = unbounded
  int64_t max_write_queue_bytes =
      16LL *
      static_cast<int64_t>(mygram::constants::kBytesPerMegabyte);  ///< Per-connection slow-reader cap; see config.h

  // TCP keepalive applied per-accepted client socket. See
  // config.h ApiConfig::tcp::keepalive for rationale.
  struct {
    bool enabled = true;
    int idle_sec = 60;
    int interval_sec = 20;
    int probe_count = 3;
  } keepalive;

  std::vector<std::string> allow_cidrs;
  std::vector<mygram::utils::CIDR> parsed_allow_cidrs;
  std::string unix_socket_path;  // Empty = TCP mode, non-empty = UDS mode

  /**
   * @brief Create ServerConfig from application Config
   *
   * Copies TCP server settings, API defaults, network ACLs, and unix socket path
   * from the unified config::Config structure.
   *
   * @param cfg Application configuration
   * @return Populated ServerConfig
   */
  static ServerConfig FromConfig(const config::Config& cfg) {
    ServerConfig sc;
    sc.host = cfg.api.tcp.bind;
    sc.port = cfg.api.tcp.port;
    sc.max_connections = cfg.api.tcp.max_connections;
    sc.worker_threads = cfg.api.tcp.worker_threads;
    sc.recv_timeout_sec = cfg.api.tcp.recv_timeout_sec;
    sc.thread_pool_queue_size = cfg.api.tcp.thread_pool_queue_size;
    sc.keepalive.enabled = cfg.api.tcp.keepalive.enabled;
    sc.keepalive.idle_sec = cfg.api.tcp.keepalive.idle_sec;
    sc.keepalive.interval_sec = cfg.api.tcp.keepalive.interval_sec;
    sc.keepalive.probe_count = cfg.api.tcp.keepalive.probe_count;
    sc.max_write_queue_bytes = cfg.api.tcp.max_write_queue_bytes;
    sc.default_limit = cfg.api.default_limit;
    sc.max_query_length = cfg.api.max_query_length;
    sc.allow_cidrs = cfg.network.allow_cidrs;
    sc.unix_socket_path = cfg.api.unix_socket.path;
    return sc;
  }
};

/**
 * @brief Per-connection context
 */
struct ConnectionContext {
  int client_fd = -1;
  // Atomic because the event-loop thread reads this flag while the drain-task
  // thread (command handler) may write it concurrently (see ReactorConnection).
  std::atomic<bool> debug_mode{false};
};

/**
 * @brief BM25 corpus statistics for a single table
 *
 * Maintains running totals for average document length computation.
 * Uses relaxed atomics since slight inconsistency is acceptable for BM25 quality.
 */
struct BM25Stats {
  std::atomic<uint64_t> total_doc_length{0};  ///< Sum of all doc lengths (code points)
  std::atomic<uint64_t> doc_count{0};         ///< Number of documents with text

  BM25Stats() = default;
  ~BM25Stats() = default;

  // Delete copy
  BM25Stats(const BM25Stats&) = delete;
  BM25Stats& operator=(const BM25Stats&) = delete;

  // Custom move (atomics have no move ctor)
  BM25Stats(BM25Stats&& other) noexcept {
    total_doc_length.store(other.total_doc_length.load(std::memory_order_relaxed), std::memory_order_relaxed);
    doc_count.store(other.doc_count.load(std::memory_order_relaxed), std::memory_order_relaxed);
  }
  BM25Stats& operator=(BM25Stats&& other) noexcept {
    if (this != &other) {
      total_doc_length.store(other.total_doc_length.load(std::memory_order_relaxed), std::memory_order_relaxed);
      doc_count.store(other.doc_count.load(std::memory_order_relaxed), std::memory_order_relaxed);
    }
    return *this;
  }

  /// Get average document length (code points)
  [[nodiscard]] double avg_doc_length() const {
    auto count = doc_count.load(std::memory_order_relaxed);
    return count > 0
               ? static_cast<double>(total_doc_length.load(std::memory_order_relaxed)) / static_cast<double>(count)
               : 0.0;
  }

  /// Add a document with given length
  void AddDocument(uint32_t doc_length) {
    total_doc_length.fetch_add(doc_length, std::memory_order_relaxed);
    doc_count.fetch_add(1, std::memory_order_relaxed);
  }

  /// Remove a document with given length
  void RemoveDocument(uint32_t doc_length) {
    total_doc_length.fetch_sub(doc_length, std::memory_order_relaxed);
    doc_count.fetch_sub(1, std::memory_order_relaxed);
  }
};

/**
 * @brief Table context managing resources for a single table
 */
struct TableContext {
  std::string name;
  config::TableConfig config;
  std::unique_ptr<index::Index> index;
  std::unique_ptr<storage::DocumentStore> doc_store;
  BM25Stats bm25_stats;
  std::unique_ptr<query::SynonymDictionary> synonym_dict;  ///< Synonym dictionary (Step 1B)
  // Note: BinlogReader is shared across all tables (single GTID stream)
};

/**
 * @brief Dump operation status
 */
enum class DumpStatus : uint8_t {
  IDLE,       // No dump operation in progress
  SAVING,     // DUMP SAVE in progress
  LOADING,    // DUMP LOAD in progress
  COMPLETED,  // Last dump operation completed successfully
  FAILED      // Last dump operation failed
};

/**
 * @brief Progress tracking for async dump operations
 */
struct DumpProgress {
  struct Snapshot {
    DumpStatus status = DumpStatus::IDLE;
    std::string filepath;
    std::string current_table;
    size_t tables_processed = 0;
    size_t tables_total = 0;
    std::chrono::steady_clock::time_point start_time;
    std::chrono::steady_clock::time_point end_time;
    std::string error_message;
    std::string last_result_filepath;
    double elapsed_seconds = 0.0;

    [[nodiscard]] bool IsInProgress() const { return status == DumpStatus::SAVING || status == DumpStatus::LOADING; }
  };

  mutable std::mutex mutex;
  DumpStatus status = DumpStatus::IDLE;
  std::string filepath;                              // Target/source file path
  std::string current_table;                         // Currently processing table
  size_t tables_processed = 0;                       // Number of tables processed
  size_t tables_total = 0;                           // Total number of tables
  std::chrono::steady_clock::time_point start_time;  // Operation start time
  std::chrono::steady_clock::time_point end_time;    // Operation end time (if completed/failed)
  std::string error_message;                         // Error message (if failed)
  std::string last_result_filepath;                  // Last completed dump filepath
  std::unique_ptr<std::thread> worker_thread;        // Background worker thread

  /**
   * @brief Destructor - joins worker thread to prevent std::terminate
   *
   * If JoinWorker() was not called before destruction, this ensures the
   * worker thread is properly joined. JoinWorker() is idempotent, so
   * calling it in the destructor is safe even if it was already called.
   */
  ~DumpProgress() { JoinWorker(); }

  // Non-copyable (has mutex and unique_ptr<thread>)
  DumpProgress(const DumpProgress&) = delete;
  DumpProgress& operator=(const DumpProgress&) = delete;

  // Default constructor
  DumpProgress() = default;

  /**
   * @brief Reset progress for a new operation
   */
  void Reset(DumpStatus new_status, const std::string& path, size_t total_tables) {
    std::lock_guard<std::mutex> lock(mutex);
    status = new_status;
    filepath = path;
    current_table.clear();
    tables_processed = 0;
    tables_total = total_tables;
    start_time = std::chrono::steady_clock::now();
    end_time = {};
    error_message.clear();
  }

  /**
   * @brief Update progress for current table
   */
  void UpdateTable(const std::string& table_name, size_t processed) {
    std::lock_guard<std::mutex> lock(mutex);
    current_table = table_name;
    tables_processed = processed;
  }

  /**
   * @brief Mark operation as completed
   */
  void Complete(const std::string& result_path) {
    std::lock_guard<std::mutex> lock(mutex);
    status = DumpStatus::COMPLETED;
    end_time = std::chrono::steady_clock::now();
    last_result_filepath = result_path;
    error_message.clear();
  }

  /**
   * @brief Mark operation as failed
   */
  void Fail(const std::string& error) {
    std::lock_guard<std::mutex> lock(mutex);
    status = DumpStatus::FAILED;
    end_time = std::chrono::steady_clock::now();
    error_message = error;
  }

  /**
   * @brief Return a consistent copy of all progress fields.
   */
  Snapshot GetSnapshot() const {
    std::lock_guard<std::mutex> lock(mutex);
    Snapshot snapshot;
    snapshot.status = status;
    snapshot.filepath = filepath;
    snapshot.current_table = current_table;
    snapshot.tables_processed = tables_processed;
    snapshot.tables_total = tables_total;
    snapshot.start_time = start_time;
    snapshot.end_time = end_time;
    snapshot.error_message = error_message;
    snapshot.last_result_filepath = last_result_filepath;

    auto end = snapshot.IsInProgress() ? std::chrono::steady_clock::now() : snapshot.end_time;
    snapshot.elapsed_seconds = std::chrono::duration<double>(end - snapshot.start_time).count();
    return snapshot;
  }

  /**
   * @brief Get elapsed time in seconds
   */
  double GetElapsedSeconds() const {
    auto snapshot = GetSnapshot();
    return snapshot.elapsed_seconds;
  }

  /**
   * @brief Check if a dump operation is currently in progress
   */
  bool IsInProgress() const {
    auto snapshot = GetSnapshot();
    return snapshot.IsInProgress();
  }

  /**
   * @brief Spawn a new worker thread under DumpProgress::mutex.
   *
   * Race fix: the previous code assigned worker_thread directly from the
   * handler thread without holding mutex, while JoinWorker() reads
   * worker_thread under mutex. If TcpServer::Stop() (which calls JoinWorker)
   * raced with HandleDumpSave's worker_thread assignment, that was a data
   * race on the unique_ptr by the C++ memory model. Centralizing the
   * assignment here eliminates the unguarded write.
   *
   * Pre-condition: caller has already drained any prior worker via
   * JoinWorker(). This method asserts worker_thread is empty so a misuse
   * (forgetting the JoinWorker drain) cannot silently leak a thread.
   */
  void StartWorker(std::function<void()> work) {
    std::lock_guard<std::mutex> lock(mutex);
    // Pre-condition: caller drained prior worker. Misuse should be loud.
    assert(!worker_thread || !worker_thread->joinable());
    worker_thread = std::make_unique<std::thread>(std::move(work));
  }

  /**
   * @brief Join worker thread if exists
   */
  void JoinWorker() {
    std::unique_ptr<std::thread> thread_to_join;
    {
      std::lock_guard<std::mutex> lock(mutex);
      if (worker_thread && worker_thread->joinable()) {
        thread_to_join = std::move(worker_thread);
      }
    }
    // Join outside the lock to avoid blocking other mutex users
    if (thread_to_join) {
      thread_to_join->join();
    }
  }
};

/**
 * @brief Context passed to command handlers
 *
 * Contains all necessary dependencies and state for command execution.
 * Reference members are intentional: this struct does not own the data,
 * it provides access to objects managed by TCPServer.
 */
struct HandlerContext {
  // NOLINTBEGIN(cppcoreguidelines-avoid-const-or-ref-data-members) - Intentional design: context references external
  // state

  // Service-based access
  TableCatalog* table_catalog = nullptr;

  ServerStats& stats;
  const config::Config* full_config;
  std::string dump_dir;
  std::atomic<bool>& dump_load_in_progress;  // True when DUMP LOAD operation is in progress
  std::atomic<bool>& dump_save_in_progress;  // True when DUMP SAVE operation is in progress
  std::atomic<bool>& optimization_in_progress;
  std::atomic<bool>& replication_paused_for_dump;  // True when replication is paused for DUMP SAVE/LOAD
  std::atomic<bool>& mysql_reconnecting;           // True when MySQL reconnection is in progress
  replication_pause::Counter* replication_pause_counter = nullptr;  // Shared DUMP/Snapshot pause counter
  // NOLINTEND(cppcoreguidelines-avoid-const-or-ref-data-members)
  mysql::IBinlogReader* binlog_reader = nullptr;
#ifdef USE_MYSQL
  SyncOperationManager* sync_manager;  // Manages sync operations and state
#endif
  cache::CacheManager* cache_manager = nullptr;
  config::RuntimeVariableManager* variable_manager = nullptr;
  DumpProgress* dump_progress = nullptr;  // Progress tracking for async dump operations

  /// Optional pointer to the server's shutdown flag. When non-null and true,
  /// long-running workers (DumpSaveWorker / DumpLoadWorker) skip
  /// auto-restarting replication after their in-flight operation completes,
  /// because the binlog_reader is about to be torn down by TcpServer::Stop()
  /// (see CR-3 / CR-10 audit, May 2026). Tests may leave this null; in that
  /// case the worker behaves as before (always attempts auto-restart).
  std::atomic<bool>* shutdown_flag = nullptr;
};

}  // namespace mygramdb::server
