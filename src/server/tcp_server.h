/**
 * @file tcp_server.h
 * @brief Simple TCP server for text protocol
 */

#pragma once

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "config/config.h"
#include "index/index.h"
#include "query/query_parser.h"
#include "server/server_stats.h"
#include "server/server_types.h"
#include "server/thread_pool.h"
#include "storage/document_store.h"

#ifdef USE_MYSQL
namespace mygramdb::mysql {
class BinlogReader;
}  // namespace mygramdb::mysql
#endif

#ifdef USE_MYSQL
namespace mygramdb::storage {
class SnapshotBuilder;
}  // namespace mygramdb::storage
#endif

namespace mygramdb::server {

// Forward declaration
class CommandHandler;

constexpr uint16_t kDefaultPort = 11016;       // memcached default port
constexpr int kDefaultMaxConnections = 10000;  // Maximum concurrent connections
constexpr int kDefaultRecvBufferSize = 4096;   // Receive buffer size
constexpr int kDefaultSendBufferSize = 65536;  // Send buffer size
constexpr int kDefaultLimit = 100;             // Default LIMIT for SEARCH queries (range: 5-1000)

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
 * @brief TCP server configuration
 */
struct ServerConfig {
  std::string host = "0.0.0.0";
  uint16_t port = kDefaultPort;
  int max_connections = kDefaultMaxConnections;
  int worker_threads = 0;  // Number of worker threads (0 = CPU count)
  int recv_buffer_size = kDefaultRecvBufferSize;
  int send_buffer_size = kDefaultSendBufferSize;
  int default_limit = kDefaultLimit;  // Default LIMIT for SEARCH queries (range: 5-1000)
};

/**
 * @brief Simple TCP server for text protocol
 *
 * Text protocol format:
 * Request: <COMMAND> <args...>\r\n
 * Response: <STATUS> <data>\r\n
 */
class TcpServer {
 public:
  /**
   * @brief Construct TCP server
   * @param config Server configuration
   * @param table_contexts Map of table name to TableContext pointer
   * @param dump_dir Dump directory path
   * @param full_config Full application configuration (for CONFIG command)
   * @param binlog_reader Optional BinlogReader for replication status
   */
  TcpServer(ServerConfig config, std::unordered_map<std::string, TableContext*> table_contexts,
            std::string dump_dir = "./dumps", const config::Config* full_config = nullptr,
#ifdef USE_MYSQL
            mysql::BinlogReader* binlog_reader = nullptr
#else
            void* binlog_reader = nullptr
#endif
  );

  ~TcpServer();

  // Non-copyable and non-movable
  TcpServer(const TcpServer&) = delete;
  TcpServer& operator=(const TcpServer&) = delete;
  TcpServer(TcpServer&&) = delete;
  TcpServer& operator=(TcpServer&&) = delete;

  /**
   * @brief Start server
   * @return true if started successfully
   */
  bool Start();

  /**
   * @brief Stop server
   */
  void Stop();

  /**
   * @brief Check if server is running
   */
  bool IsRunning() const { return running_; }

  /**
   * @brief Get server port
   */
  uint16_t GetPort() const { return actual_port_; }

  /**
   * @brief Get active connection count
   */
  size_t GetConnectionCount() const { return stats_.GetActiveConnections(); }

  /**
   * @brief Get total requests handled
   */
  uint64_t GetTotalRequests() const { return stats_.GetTotalRequests(); }

  /**
   * @brief Get last error message
   */
  const std::string& GetLastError() const { return last_error_; }

  /**
   * @brief Get server start time (Unix timestamp)
   */
  uint64_t GetStartTime() const { return stats_.GetStartTime(); }

  /**
   * @brief Get server statistics
   */
  const ServerStats& GetStats() const { return stats_; }

  /**
   * @brief Get mutable server statistics pointer (for binlog reader)
   */
  ServerStats* GetMutableStats() { return &stats_; }

#ifdef USE_MYSQL
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
#endif

 private:
  friend class SyncHandler;
  ServerConfig config_;
  std::unordered_map<std::string, TableContext*> table_contexts_;
  query::QueryParser query_parser_;

  std::atomic<bool> running_{false};
  std::atomic<bool> should_stop_{false};

  // Statistics
  ServerStats stats_;

  int server_fd_ = -1;
  uint16_t actual_port_ = 0;
  std::unique_ptr<std::thread> accept_thread_;
  std::unique_ptr<ThreadPool> thread_pool_;
  std::set<int> connection_fds_;  // Active connection file descriptors
  mutable std::mutex connections_mutex_;
  std::unordered_map<int, ConnectionContext> connection_contexts_;  // Connection contexts
  mutable std::mutex contexts_mutex_;

  std::string last_error_;
  std::string dump_dir_;                               // Dump directory
  std::atomic<bool> read_only_{false};                 // Read-only mode flag
  std::atomic<bool> loading_{false};                   // Loading mode flag (blocks queries during LOAD)
  std::atomic<bool> optimization_in_progress_{false};  // Global OPTIMIZE operation flag
  const config::Config* full_config_;                  // Full configuration for CONFIG command

  // Auto-save functionality
  std::unique_ptr<std::thread> auto_save_thread_;
  std::atomic<bool> auto_save_running_{false};

#ifdef USE_MYSQL
  mysql::BinlogReader* binlog_reader_;
#else
  void* binlog_reader_;
#endif

  // Command handler context (must outlive handlers)
  std::unique_ptr<HandlerContext> handler_context_;

  // Command handlers
  std::unique_ptr<CommandHandler> search_handler_;
  std::unique_ptr<CommandHandler> document_handler_;
  std::unique_ptr<CommandHandler> dump_handler_;
  std::unique_ptr<CommandHandler> admin_handler_;
  std::unique_ptr<CommandHandler> replication_handler_;
  std::unique_ptr<CommandHandler> debug_handler_;
  std::unique_ptr<CommandHandler> cache_handler_;
#ifdef USE_MYSQL
  std::unique_ptr<CommandHandler> sync_handler_;
#endif

#ifdef USE_MYSQL
  // SYNC operation state management
  std::unordered_map<std::string, SyncState> sync_states_;
  mutable std::mutex sync_mutex_;

  // Track which tables are currently syncing (for conflict detection)
  std::unordered_set<std::string> syncing_tables_;
  mutable std::mutex syncing_tables_mutex_;

  // Active snapshot builders (for shutdown cancellation)
  std::unordered_map<std::string, storage::SnapshotBuilder*> active_snapshot_builders_;
  mutable std::mutex snapshot_builders_mutex_;

  // Shutdown flag for SYNC operations
  std::atomic<bool> shutdown_requested_{false};
#endif

  /**
   * @brief Accept thread function
   */
  void AcceptThreadFunc();

  /**
   * @brief Handle client connection
   */
  void HandleClient(int client_fd);

  /**
   * @brief Process single request
   * @param request Request string
   * @param ctx Connection context (for debug mode, etc.)
   * @return Response string
   */
  std::string ProcessRequest(const std::string& request, ConnectionContext& ctx);

  /**
   * @brief Set socket options
   */
  bool SetSocketOptions(int socket_fd);

  /**
   * @brief Start auto-save thread
   */
  void StartAutoSave();

  /**
   * @brief Stop auto-save thread
   */
  void StopAutoSave();

  /**
   * @brief Auto-save thread function
   */
  void AutoSaveThread();

  /**
   * @brief Clean up old dump files based on retention policy
   */
  void CleanupOldDumps();

  /**
   * @brief Remove connection from active list
   */
  void RemoveConnection(int socket_fd);

#ifdef USE_MYSQL
  /**
   * @brief Build snapshot asynchronously for a table
   * @param table_name Table to synchronize
   */
  void BuildSnapshotAsync(const std::string& table_name);
#endif
};

}  // namespace mygramdb::server
