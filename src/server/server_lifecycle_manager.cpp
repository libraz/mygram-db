/**
 * @file server_lifecycle_manager.cpp
 * @brief Server component lifecycle management implementation
 */

#include "server/server_lifecycle_manager.h"

#include <spdlog/spdlog.h>

#include "cache/cache_manager.h"
#include "config/runtime_variable_manager.h"
#include "server/handlers/admin_handler.h"
#include "server/handlers/cache_handler.h"
#include "server/handlers/debug_handler.h"
#include "server/handlers/document_handler.h"
#include "server/handlers/dump_handler.h"
#include "server/handlers/replication_handler.h"
#include "server/handlers/search_handler.h"
#include "server/handlers/variable_handler.h"
#ifdef USE_MYSQL
#include "server/handlers/sync_handler.h"
#include "server/sync_operation_manager.h"
#endif
#include "utils/structured_log.h"

namespace mygramdb::server {

namespace {
// Thread pool queue size for backpressure
constexpr size_t kThreadPoolQueueSize = 1000;
}  // namespace

ServerLifecycleManager::ServerLifecycleManager(const ServerConfig& config,
                                               std::unordered_map<std::string, TableContext*>& table_contexts,
                                               const std::string& dump_dir, const config::Config* full_config,
                                               ServerStats& stats, std::atomic<bool>& loading,
                                               std::atomic<bool>& read_only,
                                               std::atomic<bool>& optimization_in_progress,
#ifdef USE_MYSQL
                                               mysql::BinlogReader* binlog_reader, SyncOperationManager* sync_manager
#else
                                               void* binlog_reader
#endif
                                               )
    : config_(config),
      table_contexts_(table_contexts),
      dump_dir_(dump_dir),
      full_config_(full_config),
      stats_(stats),
      loading_(loading),
      read_only_(read_only),
      optimization_in_progress_(optimization_in_progress),
#ifdef USE_MYSQL
      binlog_reader_(binlog_reader),
      sync_manager_(sync_manager)
#else
      binlog_reader_(binlog_reader)
#endif
{
#ifdef USE_MYSQL
  // Contract: sync_manager must be non-null when USE_MYSQL is defined
  // This is enforced because HandlerContext requires valid references to
  // syncing_tables and syncing_tables_mutex, which are owned by SyncOperationManager
  if (sync_manager_ == nullptr) {
    throw std::invalid_argument("ServerLifecycleManager: sync_manager must be non-null when USE_MYSQL is defined");
  }
#endif
}

mygram::utils::Expected<InitializedComponents, mygram::utils::Error> ServerLifecycleManager::Initialize() {
  using mygram::utils::ErrorCode;
  using mygram::utils::MakeError;
  using mygram::utils::MakeUnexpected;

  InitializedComponents components;

  // Step 1: Initialize ThreadPool (no dependencies)
  {
    auto thread_pool_result = InitThreadPool();
    if (!thread_pool_result) {
      return MakeUnexpected(thread_pool_result.error());
    }
    components.thread_pool = std::move(*thread_pool_result);
    spdlog::info("ServerLifecycleManager: ThreadPool initialized");
  }

  // Step 2: Initialize TableCatalog (no dependencies)
  {
    auto table_catalog_result = InitTableCatalog();
    if (!table_catalog_result) {
      return MakeUnexpected(table_catalog_result.error());
    }
    components.table_catalog = std::move(*table_catalog_result);
    spdlog::info("ServerLifecycleManager: TableCatalog initialized");
  }

  // Step 3: Initialize CacheManager (depends on config)
  {
    auto cache_manager_result = InitCacheManager();
    if (!cache_manager_result) {
      return MakeUnexpected(cache_manager_result.error());
    }
    components.cache_manager = std::move(*cache_manager_result);
    if (components.cache_manager) {
      spdlog::info("ServerLifecycleManager: CacheManager initialized");
    }
  }

  // Step 3.5: Initialize RuntimeVariableManager (depends on config)
  if (full_config_ != nullptr) {
    auto variable_manager_result = config::RuntimeVariableManager::Create(*full_config_);
    if (!variable_manager_result) {
      return MakeUnexpected(variable_manager_result.error());
    }
    components.variable_manager = std::move(*variable_manager_result);

    // Wire up CacheManager for runtime configuration updates
    if (components.cache_manager) {
      components.variable_manager->SetCacheManager(components.cache_manager.get());
    }

    spdlog::info("ServerLifecycleManager: RuntimeVariableManager initialized");
  }

  // Step 4: Initialize HandlerContext (depends on catalog, cache, variable_manager)
  {
    auto handler_context_result = InitHandlerContext(components.table_catalog.get(), components.cache_manager.get(),
                                                     components.variable_manager.get());
    if (!handler_context_result) {
      return MakeUnexpected(handler_context_result.error());
    }
    components.handler_context = std::move(*handler_context_result);
    spdlog::info("ServerLifecycleManager: HandlerContext initialized");
  }

  // Step 5: Initialize Handlers (depend on HandlerContext)
  {
    auto handlers_result = InitHandlers(*components.handler_context);
    if (!handlers_result) {
      return MakeUnexpected(handlers_result.error());
    }

    // Move handlers into components
    components.search_handler = std::move(handlers_result->search_handler);
    components.document_handler = std::move(handlers_result->document_handler);
    components.dump_handler = std::move(handlers_result->dump_handler);
    components.admin_handler = std::move(handlers_result->admin_handler);
    components.replication_handler = std::move(handlers_result->replication_handler);
    components.debug_handler = std::move(handlers_result->debug_handler);
    components.cache_handler = std::move(handlers_result->cache_handler);
    components.variable_handler = std::move(handlers_result->variable_handler);
#ifdef USE_MYSQL
    components.sync_handler = std::move(handlers_result->sync_handler);
#endif
    spdlog::info("ServerLifecycleManager: Command handlers initialized");
  }

  // Step 6: Initialize Dispatcher (depends on handlers)
  {
    auto dispatcher_result = InitDispatcher(*components.handler_context, components);
    if (!dispatcher_result) {
      return MakeUnexpected(dispatcher_result.error());
    }
    components.dispatcher = std::move(*dispatcher_result);
    spdlog::info("ServerLifecycleManager: RequestDispatcher initialized");
  }

  // Step 7: Initialize Acceptor (depends on thread pool)
  {
    auto acceptor_result = InitAcceptor(components.thread_pool.get());
    if (!acceptor_result) {
      return MakeUnexpected(acceptor_result.error());
    }
    components.acceptor = std::move(*acceptor_result);
    spdlog::info("ServerLifecycleManager: ConnectionAcceptor initialized");
  }

  // Step 8: Initialize Scheduler (depends on catalog)
  {
    auto scheduler_result = InitScheduler(components.table_catalog.get());
    if (!scheduler_result) {
      return MakeUnexpected(scheduler_result.error());
    }
    components.scheduler = std::move(*scheduler_result);
    if (components.scheduler) {
      spdlog::info("ServerLifecycleManager: SnapshotScheduler initialized");
    }
  }

  spdlog::info("ServerLifecycleManager: All components initialized successfully");
  return components;
}

mygram::utils::Expected<std::unique_ptr<ThreadPool>, mygram::utils::Error> ServerLifecycleManager::InitThreadPool()
    const {
  using mygram::utils::ErrorCode;
  using mygram::utils::MakeError;
  using mygram::utils::MakeUnexpected;

  try {
    auto pool =
        std::make_unique<ThreadPool>(config_.worker_threads > 0 ? config_.worker_threads : 0, kThreadPoolQueueSize);
    return pool;
  } catch (const std::exception& e) {
    auto error = MakeError(ErrorCode::kInternalError, "Failed to create thread pool: " + std::string(e.what()));
    mygram::utils::StructuredLog()
        .Event("server_error")
        .Field("operation", "init_thread_pool")
        .Field("error", error.to_string())
        .Error();
    return MakeUnexpected(error);
  }
}

mygram::utils::Expected<std::unique_ptr<TableCatalog>, mygram::utils::Error>
ServerLifecycleManager::InitTableCatalog() {
  using mygram::utils::ErrorCode;
  using mygram::utils::MakeError;
  using mygram::utils::MakeUnexpected;

  try {
    auto catalog = std::make_unique<TableCatalog>(table_contexts_);
    return catalog;
  } catch (const std::exception& e) {
    auto error = MakeError(ErrorCode::kInternalError, "Failed to create table catalog: " + std::string(e.what()));
    mygram::utils::StructuredLog()
        .Event("server_error")
        .Field("operation", "init_table_catalog")
        .Field("error", error.to_string())
        .Error();
    return MakeUnexpected(error);
  }
}

mygram::utils::Expected<std::unique_ptr<cache::CacheManager>, mygram::utils::Error>
ServerLifecycleManager::InitCacheManager() {
  using mygram::utils::ErrorCode;
  using mygram::utils::MakeError;
  using mygram::utils::MakeUnexpected;

  // Cache manager is optional (depends on config)
  if (full_config_ == nullptr || !full_config_->cache.enabled) {
    return std::unique_ptr<cache::CacheManager>(nullptr);
  }

  try {
    // Pass table_contexts to support per-table ngram settings
    auto cache_manager = std::make_unique<cache::CacheManager>(full_config_->cache, table_contexts_);
    return cache_manager;
  } catch (const std::exception& e) {
    auto error = MakeError(ErrorCode::kInternalError, "Failed to create cache manager: " + std::string(e.what()));
    mygram::utils::StructuredLog()
        .Event("server_error")
        .Field("operation", "init_cache_manager")
        .Field("error", error.to_string())
        .Error();
    return MakeUnexpected(error);
  }
}

mygram::utils::Expected<std::unique_ptr<HandlerContext>, mygram::utils::Error>
ServerLifecycleManager::InitHandlerContext(TableCatalog* table_catalog, cache::CacheManager* cache_manager,
                                           config::RuntimeVariableManager* variable_manager) {
  using mygram::utils::ErrorCode;
  using mygram::utils::MakeError;
  using mygram::utils::MakeUnexpected;

  try {
#ifdef USE_MYSQL
    // sync_manager_ is guaranteed to be non-null (enforced in constructor)
    auto handler_context = std::make_unique<HandlerContext>(HandlerContext{
        .table_catalog = table_catalog,
        .table_contexts = table_contexts_,
        .stats = stats_,
        .full_config = full_config_,
        .dump_dir = dump_dir_,
        .loading = loading_,
        .read_only = read_only_,
        .optimization_in_progress = optimization_in_progress_,
        .binlog_reader = binlog_reader_,
        .syncing_tables = sync_manager_->GetSyncingTablesRef(),
        .syncing_tables_mutex = sync_manager_->GetSyncingTablesMutex(),
        .cache_manager = cache_manager,
        .variable_manager = variable_manager,
    });
#else
    auto handler_context = std::make_unique<HandlerContext>(HandlerContext{
        .table_catalog = table_catalog,
        .table_contexts = table_contexts_,
        .stats = stats_,
        .full_config = full_config_,
        .dump_dir = dump_dir_,
        .loading = loading_,
        .read_only = read_only_,
        .optimization_in_progress = optimization_in_progress_,
        .binlog_reader = binlog_reader_,
        .cache_manager = cache_manager,
        .variable_manager = variable_manager,
    });
#endif
    return handler_context;
  } catch (const std::exception& e) {
    auto error = MakeError(ErrorCode::kInternalError, "Failed to create handler context: " + std::string(e.what()));
    mygram::utils::StructuredLog()
        .Event("server_error")
        .Field("operation", "init_handler_context")
        .Field("error", error.to_string())
        .Error();
    return MakeUnexpected(error);
  }
}

mygram::utils::Expected<InitializedComponents, mygram::utils::Error> ServerLifecycleManager::InitHandlers(
    HandlerContext& handler_context) {
  using mygram::utils::ErrorCode;
  using mygram::utils::MakeError;
  using mygram::utils::MakeUnexpected;

  InitializedComponents handlers;

  try {
    handlers.search_handler = std::make_unique<SearchHandler>(handler_context);
    handlers.document_handler = std::make_unique<DocumentHandler>(handler_context);
    handlers.dump_handler = std::make_unique<DumpHandler>(handler_context);
    handlers.admin_handler = std::make_unique<AdminHandler>(handler_context);
    handlers.replication_handler = std::make_unique<ReplicationHandler>(handler_context);
    handlers.debug_handler = std::make_unique<DebugHandler>(handler_context);
    handlers.cache_handler = std::make_unique<CacheHandler>(handler_context);
    handlers.variable_handler = std::make_unique<VariableHandler>(handler_context);
#ifdef USE_MYSQL
    // Note: SyncHandler now takes SyncOperationManager* instead of TcpServer&
    // This eliminates circular dependency
    // sync_manager_ is guaranteed to be non-null (enforced in constructor)
    handlers.sync_handler = std::make_unique<SyncHandler>(handler_context, sync_manager_);
#endif

    return handlers;
  } catch (const std::exception& e) {
    auto error = MakeError(ErrorCode::kInternalError, "Failed to create command handlers: " + std::string(e.what()));
    mygram::utils::StructuredLog()
        .Event("server_error")
        .Field("operation", "init_handlers")
        .Field("error", error.to_string())
        .Error();
    return MakeUnexpected(error);
  }
}

mygram::utils::Expected<std::unique_ptr<RequestDispatcher>, mygram::utils::Error>
ServerLifecycleManager::InitDispatcher(HandlerContext& handler_context, const InitializedComponents& handlers) {
  using mygram::utils::ErrorCode;
  using mygram::utils::MakeError;
  using mygram::utils::MakeUnexpected;

  try {
    auto dispatcher = std::make_unique<RequestDispatcher>(handler_context, config_);

    // Register all command handlers
    dispatcher->RegisterHandler(query::QueryType::SEARCH, handlers.search_handler.get());
    dispatcher->RegisterHandler(query::QueryType::COUNT, handlers.search_handler.get());
    dispatcher->RegisterHandler(query::QueryType::GET, handlers.document_handler.get());
    dispatcher->RegisterHandler(query::QueryType::DUMP_SAVE, handlers.dump_handler.get());
    dispatcher->RegisterHandler(query::QueryType::DUMP_LOAD, handlers.dump_handler.get());
    dispatcher->RegisterHandler(query::QueryType::DUMP_VERIFY, handlers.dump_handler.get());
    dispatcher->RegisterHandler(query::QueryType::DUMP_INFO, handlers.dump_handler.get());
    dispatcher->RegisterHandler(query::QueryType::INFO, handlers.admin_handler.get());
    dispatcher->RegisterHandler(query::QueryType::CONFIG_HELP, handlers.admin_handler.get());
    dispatcher->RegisterHandler(query::QueryType::CONFIG_SHOW, handlers.admin_handler.get());
    dispatcher->RegisterHandler(query::QueryType::CONFIG_VERIFY, handlers.admin_handler.get());
    dispatcher->RegisterHandler(query::QueryType::REPLICATION_STATUS, handlers.replication_handler.get());
    dispatcher->RegisterHandler(query::QueryType::REPLICATION_STOP, handlers.replication_handler.get());
    dispatcher->RegisterHandler(query::QueryType::REPLICATION_START, handlers.replication_handler.get());
    dispatcher->RegisterHandler(query::QueryType::DEBUG_ON, handlers.debug_handler.get());
    dispatcher->RegisterHandler(query::QueryType::DEBUG_OFF, handlers.debug_handler.get());
    dispatcher->RegisterHandler(query::QueryType::OPTIMIZE, handlers.debug_handler.get());
    dispatcher->RegisterHandler(query::QueryType::CACHE_CLEAR, handlers.cache_handler.get());
    dispatcher->RegisterHandler(query::QueryType::CACHE_STATS, handlers.cache_handler.get());
    dispatcher->RegisterHandler(query::QueryType::CACHE_ENABLE, handlers.cache_handler.get());
    dispatcher->RegisterHandler(query::QueryType::CACHE_DISABLE, handlers.cache_handler.get());
    dispatcher->RegisterHandler(query::QueryType::SET, handlers.variable_handler.get());
    dispatcher->RegisterHandler(query::QueryType::SHOW_VARIABLES, handlers.variable_handler.get());
#ifdef USE_MYSQL
    // sync_handler is guaranteed to be non-null (sync_manager_ is enforced in constructor)
    dispatcher->RegisterHandler(query::QueryType::SYNC, handlers.sync_handler.get());
    dispatcher->RegisterHandler(query::QueryType::SYNC_STATUS, handlers.sync_handler.get());
#endif

    return dispatcher;
  } catch (const std::exception& e) {
    auto error = MakeError(ErrorCode::kInternalError, "Failed to create dispatcher: " + std::string(e.what()));
    mygram::utils::StructuredLog()
        .Event("server_error")
        .Field("operation", "init_dispatcher")
        .Field("error", error.to_string())
        .Error();
    return MakeUnexpected(error);
  }
}

mygram::utils::Expected<std::unique_ptr<ConnectionAcceptor>, mygram::utils::Error> ServerLifecycleManager::InitAcceptor(
    ThreadPool* thread_pool) {
  using mygram::utils::ErrorCode;
  using mygram::utils::MakeError;
  using mygram::utils::MakeUnexpected;

  try {
    auto acceptor = std::make_unique<ConnectionAcceptor>(config_, thread_pool);

    // Start the acceptor
    auto start_result = acceptor->Start();
    if (!start_result) {
      return MakeUnexpected(start_result.error());
    }

    return acceptor;
  } catch (const std::exception& e) {
    auto error = MakeError(ErrorCode::kInternalError, "Failed to create connection acceptor: " + std::string(e.what()));
    mygram::utils::StructuredLog()
        .Event("server_error")
        .Field("operation", "init_acceptor")
        .Field("error", error.to_string())
        .Error();
    return MakeUnexpected(error);
  }
}

mygram::utils::Expected<std::unique_ptr<SnapshotScheduler>, mygram::utils::Error> ServerLifecycleManager::InitScheduler(
    TableCatalog* table_catalog) {
  using mygram::utils::ErrorCode;
  using mygram::utils::MakeError;
  using mygram::utils::MakeUnexpected;

  // Scheduler is optional (depends on config)
  if (full_config_ == nullptr || full_config_->dump.interval_sec <= 0) {
    return std::unique_ptr<SnapshotScheduler>(nullptr);
  }

  try {
    auto scheduler =
        std::make_unique<SnapshotScheduler>(full_config_->dump, table_catalog, full_config_, dump_dir_, binlog_reader_);

    // Start the scheduler
    scheduler->Start();

    return scheduler;
  } catch (const std::exception& e) {
    auto error = MakeError(ErrorCode::kInternalError, "Failed to create snapshot scheduler: " + std::string(e.what()));
    mygram::utils::StructuredLog()
        .Event("server_error")
        .Field("operation", "init_scheduler")
        .Field("error", error.to_string())
        .Error();
    return MakeUnexpected(error);
  }
}

}  // namespace mygramdb::server
