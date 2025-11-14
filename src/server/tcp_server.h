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
#include "server/connection_acceptor.h"
#include "server/request_dispatcher.h"
#include "server/server_stats.h"
#include "server/server_types.h"
#include "server/snapshot_scheduler.h"
#include "server/table_catalog.h"
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
  bool IsRunning() const { return acceptor_ && acceptor_->IsRunning(); }

  /**
   * @brief Get server port
   */
  uint16_t GetPort() const { return acceptor_ ? acceptor_->GetPort() : 0; }

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

  // Configuration
  ServerConfig config_;
  const config::Config* full_config_;
  std::string dump_dir_;
  std::string last_error_;

  // State
  ServerStats stats_;
  std::atomic<bool> read_only_{false};
  std::atomic<bool> loading_{false};
  std::atomic<bool> optimization_in_progress_{false};

  // Services (composition) - NEW
  std::unique_ptr<TableCatalog> table_catalog_;
  std::unique_ptr<ThreadPool> thread_pool_;
  std::unique_ptr<ConnectionAcceptor> acceptor_;
  std::unique_ptr<RequestDispatcher> dispatcher_;
  std::unique_ptr<SnapshotScheduler> scheduler_;

  // Legacy fields (for backward compatibility during migration)
  std::unordered_map<std::string, TableContext*> table_contexts_;
  std::unordered_map<int, ConnectionContext> connection_contexts_;
  mutable std::mutex contexts_mutex_;

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
   * @brief Handle client connection (callback for ConnectionAcceptor)
   */
  void HandleConnection(int client_fd);

#ifdef USE_MYSQL
  /**
   * @brief Build snapshot asynchronously for a table
   * @param table_name Table to synchronize
   */
  void BuildSnapshotAsync(const std::string& table_name);
#endif
};

}  // namespace mygramdb::server
