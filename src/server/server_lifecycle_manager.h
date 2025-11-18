/**
 * @file server_lifecycle_manager.h
 * @brief Manages server component initialization and lifecycle
 */

#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <unordered_map>

#include "cache/cache_manager.h"
#include "config/config.h"
#include "server/connection_acceptor.h"
#include "server/handlers/command_handler.h"
#include "server/request_dispatcher.h"
#include "server/server_stats.h"
#include "server/server_types.h"
#include "server/snapshot_scheduler.h"
#include "server/table_catalog.h"
#include "server/thread_pool.h"
#include "utils/error.h"
#include "utils/expected.h"

#ifdef USE_MYSQL
namespace mygramdb::mysql {
class BinlogReader;
}
namespace mygramdb::server {
class SyncOperationManager;
}
#endif

namespace mygramdb::server {

/**
 * @brief Result of component initialization
 *
 * This struct contains all components created by ServerLifecycleManager.
 * TcpServer takes ownership of these components after successful initialization.
 */
struct InitializedComponents {
  std::unique_ptr<ThreadPool> thread_pool;
  std::unique_ptr<TableCatalog> table_catalog;
  std::unique_ptr<cache::CacheManager> cache_manager;
  std::unique_ptr<HandlerContext> handler_context;

  // Command handlers
  std::unique_ptr<CommandHandler> search_handler;
  std::unique_ptr<CommandHandler> document_handler;
  std::unique_ptr<CommandHandler> dump_handler;
  std::unique_ptr<CommandHandler> admin_handler;
  std::unique_ptr<CommandHandler> replication_handler;
  std::unique_ptr<CommandHandler> debug_handler;
  std::unique_ptr<CommandHandler> cache_handler;
#ifdef USE_MYSQL
  std::unique_ptr<CommandHandler> sync_handler;
#endif

  std::unique_ptr<RequestDispatcher> dispatcher;
  std::unique_ptr<ConnectionAcceptor> acceptor;
  std::unique_ptr<SnapshotScheduler> scheduler;
};

/**
 * @brief Manages server component lifecycle and initialization order
 *
 * Responsibilities:
 * - Initialize components in correct dependency order
 * - Register command handlers with dispatcher
 * - Provide testable initialization steps
 * - Act as factory/builder pattern - creates components, TcpServer owns them
 *
 * Design Pattern: Factory/Builder
 * - ServerLifecycleManager creates components
 * - Returns InitializedComponents struct
 * - TcpServer takes ownership via std::move
 *
 * Benefits:
 * - Clear separation of initialization logic from server logic
 * - Testable initialization steps
 * - Type-safe error propagation via Expected<T, Error>
 * - No circular dependencies
 */
class ServerLifecycleManager {
 public:
  /**
   * @brief Construct lifecycle manager with all required dependencies
   *
   * @param config Server configuration
   * @param table_contexts Map of table name to TableContext pointer (non-const for HandlerContext)
   * @param dump_dir Dump directory path
   * @param full_config Full application configuration
   * @param stats Reference to server statistics (owned by TcpServer)
   * @param loading Reference to loading flag (owned by TcpServer)
   * @param read_only Reference to read-only flag (owned by TcpServer)
   * @param optimization_in_progress Reference to optimization flag (owned by TcpServer)
   * @param binlog_reader Optional BinlogReader for replication
   * @param sync_manager Optional SyncOperationManager for SYNC operations (MySQL only)
   */
  ServerLifecycleManager(const ServerConfig& config, std::unordered_map<std::string, TableContext*>& table_contexts,
                         const std::string& dump_dir, const config::Config* full_config, ServerStats& stats,
                         std::atomic<bool>& loading, std::atomic<bool>& read_only,
                         std::atomic<bool>& optimization_in_progress,
#ifdef USE_MYSQL
                         mysql::BinlogReader* binlog_reader, SyncOperationManager* sync_manager
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
   * @brief Initialize all components in dependency order
   *
   * Initialization order (follows dependency graph):
   * 1. ThreadPool (no dependencies)
   * 2. TableCatalog (no dependencies)
   * 3. CacheManager (depends on config)
   * 4. HandlerContext (depends on catalog, cache)
   * 5. Handlers (depend on HandlerContext)
   * 6. Dispatcher (depends on handlers)
   * 7. Acceptor (depends on thread pool)
   * 8. Scheduler (depends on catalog)
   *
   * @return Expected with initialized components or error
   */
  mygram::utils::Expected<InitializedComponents, mygram::utils::Error> Initialize();

 private:
  // Configuration (const references)
  const ServerConfig& config_;
  std::unordered_map<std::string, TableContext*>& table_contexts_;  // Non-const: HandlerContext requires non-const ref
  const std::string& dump_dir_;
  const config::Config* full_config_;

  // Shared state (non-const references - passed to handlers via HandlerContext)
  ServerStats& stats_;
  std::atomic<bool>& loading_;
  std::atomic<bool>& read_only_;
  std::atomic<bool>& optimization_in_progress_;

#ifdef USE_MYSQL
  mysql::BinlogReader* binlog_reader_;
  SyncOperationManager* sync_manager_;  // Non-owning pointer for passing to SyncHandler
#else
  void* binlog_reader_;
#endif

  // Initialization steps (each returns Expected<unique_ptr<T>, Error>)
  mygram::utils::Expected<std::unique_ptr<ThreadPool>, mygram::utils::Error> InitThreadPool() const;
  mygram::utils::Expected<std::unique_ptr<TableCatalog>, mygram::utils::Error> InitTableCatalog();
  mygram::utils::Expected<std::unique_ptr<cache::CacheManager>, mygram::utils::Error> InitCacheManager();
  mygram::utils::Expected<std::unique_ptr<HandlerContext>, mygram::utils::Error> InitHandlerContext(
      TableCatalog* table_catalog, cache::CacheManager* cache_manager);
  mygram::utils::Expected<InitializedComponents, mygram::utils::Error> InitHandlers(HandlerContext& handler_context);
  mygram::utils::Expected<std::unique_ptr<RequestDispatcher>, mygram::utils::Error> InitDispatcher(
      HandlerContext& handler_context, const InitializedComponents& handlers);
  mygram::utils::Expected<std::unique_ptr<ConnectionAcceptor>, mygram::utils::Error> InitAcceptor(
      ThreadPool* thread_pool);
  mygram::utils::Expected<std::unique_ptr<SnapshotScheduler>, mygram::utils::Error> InitScheduler(
      TableCatalog* table_catalog);
};

}  // namespace mygramdb::server
