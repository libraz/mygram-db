/**
 * @file server_types.h
 * @brief Common server type definitions
 */

#pragma once

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "config/config.h"
#include "index/index.h"
#include "server/server_stats.h"
#include "storage/document_store.h"
#include "utils/network_utils.h"

#ifdef USE_MYSQL
namespace mygramdb::mysql {
class IBinlogReader;
class BinlogReader;
}  // namespace mygramdb::mysql
#endif

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
constexpr uint16_t kDefaultPort = 11016;       // memcached default port
constexpr int kDefaultMaxConnections = 10000;  // Maximum concurrent connections
constexpr int kDefaultRecvBufferSize = 4096;   // Receive buffer size
constexpr int kDefaultSendBufferSize = 65536;  // Send buffer size
constexpr int kDefaultLimit = 100;             // Default LIMIT for SEARCH queries (range: 5-1000)

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
  int default_limit = kDefaultLimit;  // Default LIMIT for SEARCH queries (range: 5-1000)
  int max_query_length = config::defaults::kDefaultQueryLengthLimit;  // Max characters for query expressions
  std::vector<std::string> allow_cidrs;
  std::vector<utils::CIDR> parsed_allow_cidrs;
};

/**
 * @brief Per-connection context
 */
struct ConnectionContext {
  int client_fd = -1;
  bool debug_mode = false;  // Debug mode flag
};

/**
 * @brief Table context managing resources for a single table
 */
struct TableContext {
  std::string name;
  config::TableConfig config;
  std::unique_ptr<index::Index> index;
  std::unique_ptr<storage::DocumentStore> doc_store;
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
   * @brief Get elapsed time in seconds
   */
  double GetElapsedSeconds() const {
    std::lock_guard<std::mutex> lock(mutex);
    auto end =
        (status == DumpStatus::SAVING || status == DumpStatus::LOADING) ? std::chrono::steady_clock::now() : end_time;
    return std::chrono::duration<double>(end - start_time).count();
  }

  /**
   * @brief Check if a dump operation is currently in progress
   */
  bool IsInProgress() const {
    std::lock_guard<std::mutex> lock(mutex);
    return status == DumpStatus::SAVING || status == DumpStatus::LOADING;
  }

  /**
   * @brief Join worker thread if exists
   */
  void JoinWorker() {
    if (worker_thread && worker_thread->joinable()) {
      worker_thread->join();
      worker_thread.reset();
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

  // Direct table access (maintained for backward compatibility)
  std::unordered_map<std::string, TableContext*>& table_contexts;

  ServerStats& stats;
  const config::Config* full_config;
  std::string dump_dir;
  std::atomic<bool>& dump_load_in_progress;  // True when DUMP LOAD operation is in progress
  std::atomic<bool>& dump_save_in_progress;  // True when DUMP SAVE operation is in progress
  std::atomic<bool>& optimization_in_progress;
  std::atomic<bool>& replication_paused_for_dump;  // True when replication is paused for DUMP SAVE/LOAD
  std::atomic<bool>& mysql_reconnecting;           // True when MySQL reconnection is in progress
  // NOLINTEND(cppcoreguidelines-avoid-const-or-ref-data-members)
#ifdef USE_MYSQL
  mysql::IBinlogReader* binlog_reader;
  SyncOperationManager* sync_manager;  // Manages sync operations and state
#else
  void* binlog_reader;
#endif
  cache::CacheManager* cache_manager = nullptr;
  config::RuntimeVariableManager* variable_manager = nullptr;
  DumpProgress* dump_progress = nullptr;  // Progress tracking for async dump operations
};

}  // namespace mygramdb::server
