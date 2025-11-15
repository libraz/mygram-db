/**
 * @file server_lifecycle_manager.h
 * @brief Manages server component initialization and lifecycle
 */

#pragma once

#include <memory>
#include <string>
#include <unordered_map>

#include "cache/cache_manager.h"
#include "config/config.h"
#include "server/connection_acceptor.h"
#include "server/request_dispatcher.h"
#include "server/server_types.h"
#include "server/snapshot_scheduler.h"
#include "server/table_catalog.h"
#include "server/thread_pool.h"

#ifdef USE_MYSQL
namespace mygramdb::mysql {
class BinlogReader;
}
#endif

namespace mygramdb::server {

class CommandHandler;

/**
 * @brief Result of initialization with success flag and error message
 */
struct InitResult {
  bool success;
  std::string error_message;
};

/**
 * @brief Manages server component lifecycle and initialization order
 *
 * Responsibilities:
 * - Initialize components in correct dependency order
 * - Register command handlers with dispatcher
 * - Provide testable initialization steps
 * - Centralize component ownership
 */
class ServerLifecycleManager {
 public:
  ServerLifecycleManager(
      const ServerConfig& config,
      const std::unordered_map<std::string, TableContext*>& table_contexts,
      const std::string& dump_dir,
      const config::Config* full_config,
#ifdef USE_MYSQL
      mysql::BinlogReader* binlog_reader
#else
      void* binlog_reader
#endif
  );

  ~ServerLifecycleManager() = default;

  // Non-copyable and non-movable
  ServerLifecycleManager(const ServerLifecycleManager&) = delete;
  ServerLifecycleManager& operator=(const ServerLifecycleManager&) = delete;
  ServerLifecycleManager(ServerLifecycleManager&&) = delete;
  ServerLifecycleManager& operator=(ServerLifecycleManager&&) = delete;

  /**
   * @brief Initialize all components
   * @return Result with success flag and error message
   */
  InitResult Initialize();

  /**
   * @brief Shutdown all components in reverse order
   */
  void Shutdown();

  // Component accessors
  ThreadPool* GetThreadPool() const { return thread_pool_.get(); }
  TableCatalog* GetTableCatalog() const { return table_catalog_.get(); }
  ConnectionAcceptor* GetAcceptor() const { return acceptor_.get(); }
  RequestDispatcher* GetDispatcher() const { return dispatcher_.get(); }
  SnapshotScheduler* GetScheduler() const { return scheduler_.get(); }
  cache::CacheManager* GetCacheManager() const { return cache_manager_.get(); }

 private:
  // Configuration
  const ServerConfig& config_;
  const std::unordered_map<std::string, TableContext*>& table_contexts_;
  const std::string& dump_dir_;
  const config::Config* full_config_;
#ifdef USE_MYSQL
  mysql::BinlogReader* binlog_reader_;
#else
  void* binlog_reader_;
#endif

  // Components (in initialization order)
  std::unique_ptr<ThreadPool> thread_pool_;
  std::unique_ptr<TableCatalog> table_catalog_;
  std::unique_ptr<cache::CacheManager> cache_manager_;
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
  
  std::unique_ptr<RequestDispatcher> dispatcher_;
  std::unique_ptr<ConnectionAcceptor> acceptor_;
  std::unique_ptr<SnapshotScheduler> scheduler_;

  // Shared state references (owned by TcpServer)
  ServerStats& stats_ref_;
  std::atomic<bool>& loading_ref_;
  std::atomic<bool>& read_only_ref_;
  std::atomic<bool>& optimization_in_progress_ref_;
#ifdef USE_MYSQL
  std::unordered_set<std::string>& syncing_tables_ref_;
  std::mutex& syncing_tables_mutex_ref_;
#endif

  // Initialization steps (for testability)
  InitResult InitThreadPool();
  InitResult InitTableCatalog();
  InitResult InitCacheManager();
  InitResult InitHandlers(class TcpServer* server);
  InitResult InitDispatcher();
  InitResult InitAcceptor();
  InitResult InitScheduler();
};

}  // namespace mygramdb::server
