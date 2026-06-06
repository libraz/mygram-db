/**
 * @file server_lifecycle_manager.cpp
 * @brief Server component lifecycle management implementation
 */

#include "server/server_lifecycle_manager.h"

#include <array>
#include <string>

#include "cache/cache_manager.h"
#include "config/runtime_variable_manager.h"
#include "server/handlers/admin_handler.h"
#include "server/handlers/cache_handler.h"
#include "server/handlers/debug_handler.h"
#include "server/handlers/document_handler.h"
#include "server/handlers/dump_handler.h"
#include "server/handlers/facet_handler.h"
#include "server/handlers/replication_handler.h"
#include "server/handlers/search_handler.h"
#include "server/handlers/variable_handler.h"
#ifdef USE_MYSQL
#include "mysql/binlog_reader.h"
#include "server/handlers/sync_handler.h"
#include "server/sync_operation_manager.h"
#endif
#include "utils/structured_log.h"

namespace mygramdb::server {

namespace {
// Historical default for backpressure queue depth. Used when the operator
// leaves `api.tcp.thread_pool_queue_size` at 0 in YAML. This is NOT the same
// semantic as "0 = unbounded" on ThreadPool; we treat 0 as "keep the legacy
// default" so that existing configs and tests continue to work unchanged.
constexpr size_t kDefaultThreadPoolQueueSize = 1000;
}  // namespace

mygram::utils::Expected<std::unique_ptr<ServerLifecycleManager>, mygram::utils::Error> ServerLifecycleManager::Create(
    const ServerConfig& config, std::unordered_map<std::string, TableContext*>& table_contexts,
    const std::string& dump_dir, const config::Config* full_config, ServerStats& stats,
    std::atomic<bool>& dump_load_in_progress, std::atomic<bool>& dump_save_in_progress,
    std::atomic<bool>& optimization_in_progress, std::atomic<bool>& replication_paused_for_dump,
    std::atomic<bool>& mysql_reconnecting, replication_pause::Counter& replication_pause_counter,
    mysql::IBinlogReader* binlog_reader
#ifdef USE_MYSQL
    ,
    SyncOperationManager* sync_manager
#endif
    ,
    RateLimiter* rate_limiter) {
  using mygram::utils::ErrorCode;
  using mygram::utils::MakeError;
  using mygram::utils::MakeUnexpected;

#ifdef USE_MYSQL
  // Contract: sync_manager must be non-null when USE_MYSQL is defined.
  // Use kServerInitMissingDependency (6xxx Network/Server range) for server
  // init-time dependency checks, distinguishing them from generic internal
  // errors so observability tools can surface configuration mistakes.
  if (sync_manager == nullptr) {
    return MakeUnexpected(MakeError(ErrorCode::kServerInitMissingDependency,
                                    "ServerLifecycleManager: sync_manager must be non-null when USE_MYSQL is defined"));
  }
#endif

  // Constructor is private, so we use unique_ptr with raw new
  auto manager = std::unique_ptr<ServerLifecycleManager>(
      new ServerLifecycleManager(config, table_contexts, dump_dir, full_config, stats, dump_load_in_progress,
                                 dump_save_in_progress, optimization_in_progress, replication_paused_for_dump,
                                 mysql_reconnecting, replication_pause_counter, binlog_reader
#ifdef USE_MYSQL
                                 ,
                                 sync_manager
#endif
                                 ,
                                 rate_limiter));
  return manager;
}

ServerLifecycleManager::ServerLifecycleManager(
    const ServerConfig& config, std::unordered_map<std::string, TableContext*>& table_contexts,
    const std::string& dump_dir, const config::Config* full_config, ServerStats& stats,
    std::atomic<bool>& dump_load_in_progress, std::atomic<bool>& dump_save_in_progress,
    std::atomic<bool>& optimization_in_progress, std::atomic<bool>& replication_paused_for_dump,
    std::atomic<bool>& mysql_reconnecting, replication_pause::Counter& replication_pause_counter,
    mysql::IBinlogReader* binlog_reader
#ifdef USE_MYSQL
    ,
    SyncOperationManager* sync_manager
#endif
    ,
    RateLimiter* rate_limiter)
    : config_(config),
      table_contexts_(table_contexts),
      dump_dir_(dump_dir),
      full_config_(full_config),
      stats_(stats),
      dump_load_in_progress_(dump_load_in_progress),
      dump_save_in_progress_(dump_save_in_progress),
      optimization_in_progress_(optimization_in_progress),
      replication_paused_for_dump_(replication_paused_for_dump),
      mysql_reconnecting_(mysql_reconnecting),
      replication_pause_counter_(replication_pause_counter),
      binlog_reader_(binlog_reader)
#ifdef USE_MYSQL
      ,
      sync_manager_(sync_manager)
#endif
      ,
      rate_limiter_(rate_limiter) {
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
    mygram::utils::StructuredLog().Event("server_component_initialized").Field("component", "ThreadPool").Debug();
  }

  // Step 2: Initialize TableCatalog (no dependencies)
  {
    auto table_catalog_result = InitTableCatalog();
    if (!table_catalog_result) {
      return MakeUnexpected(table_catalog_result.error());
    }
    components.table_catalog = std::move(*table_catalog_result);
    mygram::utils::StructuredLog().Event("server_component_initialized").Field("component", "TableCatalog").Debug();
  }

  // Step 3: Initialize CacheManager (depends on config)
  {
    auto cache_manager_result = InitCacheManager();
    if (!cache_manager_result) {
      return MakeUnexpected(cache_manager_result.error());
    }
    components.cache_manager = std::move(*cache_manager_result);
    if (components.cache_manager) {
      mygram::utils::StructuredLog().Event("server_component_initialized").Field("component", "CacheManager").Debug();
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

    mygram::utils::StructuredLog()
        .Event("server_component_initialized")
        .Field("component", "RuntimeVariableManager")
        .Debug();
  }

  // Step 4: Initialize HandlerContext (depends on catalog, cache, variable_manager)
  {
    auto handler_context_result = InitHandlerContext(components.table_catalog.get(), components.cache_manager.get(),
                                                     components.variable_manager.get());
    if (!handler_context_result) {
      return MakeUnexpected(handler_context_result.error());
    }
    components.handler_context = std::move(*handler_context_result);
    mygram::utils::StructuredLog().Event("server_component_initialized").Field("component", "HandlerContext").Debug();
  }

  // Step 5: Initialize Handlers (depend on HandlerContext)
  {
    auto handlers_result = InitHandlers(*components.handler_context);
    if (!handlers_result) {
      return MakeUnexpected(handlers_result.error());
    }

    // Move handlers into components
    components.search_handler = std::move(handlers_result->search_handler);
    components.facet_handler = std::move(handlers_result->facet_handler);
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
    mygram::utils::StructuredLog().Event("server_component_initialized").Field("component", "CommandHandlers").Debug();
  }

  // Step 6: Initialize Dispatcher (depends on handlers)
  {
    auto dispatcher_result = InitDispatcher(*components.handler_context, components);
    if (!dispatcher_result) {
      return MakeUnexpected(dispatcher_result.error());
    }
    components.dispatcher = std::move(*dispatcher_result);
    if (components.variable_manager != nullptr) {
      auto* dispatcher = components.dispatcher.get();
      components.variable_manager->SetApiConfigCallback([dispatcher](int default_limit, int max_query_length) {
        dispatcher->UpdateApiConfig(default_limit, max_query_length);
      });
    }
    mygram::utils::StructuredLog()
        .Event("server_component_initialized")
        .Field("component", "RequestDispatcher")
        .Debug();
  }

  // Step 7: Initialize Acceptor (depends on thread pool)
  {
    auto acceptor_result = InitAcceptor();
    if (!acceptor_result) {
      return MakeUnexpected(acceptor_result.error());
    }
    components.acceptor = std::move(*acceptor_result);
    mygram::utils::StructuredLog()
        .Event("server_component_initialized")
        .Field("component", "ConnectionAcceptor")
        .Debug();
  }

  // Step 8: Initialize Scheduler (depends on catalog)
  {
    auto scheduler_result = InitScheduler(components.table_catalog.get());
    if (!scheduler_result) {
      return MakeUnexpected(scheduler_result.error());
    }
    components.scheduler = std::move(*scheduler_result);
    if (components.scheduler) {
      mygram::utils::StructuredLog()
          .Event("server_component_initialized")
          .Field("component", "SnapshotScheduler")
          .Debug();
    }
  }

  mygram::utils::StructuredLog()
      .Event("server_lifecycle_complete")
      .Field("status", "all_components_initialized")
      .Debug();
  return components;
}

mygram::utils::Expected<std::unique_ptr<ThreadPool>, mygram::utils::Error> ServerLifecycleManager::InitThreadPool()
    const {
  const size_t queue_size = config_.thread_pool_queue_size > 0 ? static_cast<size_t>(config_.thread_pool_queue_size)
                                                               : kDefaultThreadPoolQueueSize;
  auto pool = std::make_unique<ThreadPool>(config_.worker_threads > 0 ? config_.worker_threads : 0, queue_size);
  return pool;
}

mygram::utils::Expected<std::unique_ptr<TableCatalog>, mygram::utils::Error>
ServerLifecycleManager::InitTableCatalog() {
  auto catalog = std::make_unique<TableCatalog>(table_contexts_);
  return catalog;
}

mygram::utils::Expected<std::unique_ptr<cache::CacheManager>, mygram::utils::Error>
ServerLifecycleManager::InitCacheManager() {
  // Cache manager is optional (depends on config)
  if (full_config_ == nullptr || !full_config_->cache.enabled) {
    return std::unique_ptr<cache::CacheManager>(nullptr);
  }

  // Build NgramConfigMap from table_contexts for cache invalidation
  cache::NgramConfigMap ngram_configs;
  for (const auto& [name, ctx] : table_contexts_) {
    ngram_configs[name] = cache::NgramConfig{
        .ngram_size = ctx->config.ngram_size,
        .kanji_ngram_size = ctx->config.kanji_ngram_size,
        .cross_boundary_ngrams = ctx->config.cross_boundary_ngrams,
    };
  }
  auto cache_manager = std::make_unique<cache::CacheManager>(full_config_->cache, std::move(ngram_configs));
  return cache_manager;
}

mygram::utils::Expected<std::unique_ptr<HandlerContext>, mygram::utils::Error>
ServerLifecycleManager::InitHandlerContext(TableCatalog* table_catalog, cache::CacheManager* cache_manager,
                                           config::RuntimeVariableManager* variable_manager) {
  auto handler_context = std::make_unique<HandlerContext>(HandlerContext{
      .table_catalog = table_catalog,
      .stats = stats_,
      .full_config = full_config_,
      .dump_dir = dump_dir_,
      .dump_load_in_progress = dump_load_in_progress_,
      .dump_save_in_progress = dump_save_in_progress_,
      .optimization_in_progress = optimization_in_progress_,
      .replication_paused_for_dump = replication_paused_for_dump_,
      .mysql_reconnecting = mysql_reconnecting_,
      .replication_pause_counter = &replication_pause_counter_,
      .binlog_reader = binlog_reader_,
#ifdef USE_MYSQL
      .sync_manager = sync_manager_,
#endif
      .cache_manager = cache_manager,
      .variable_manager = variable_manager,
      .rate_limiter = rate_limiter_,
  });
  return handler_context;
}

mygram::utils::Expected<InitializedComponents, mygram::utils::Error> ServerLifecycleManager::InitHandlers(
    HandlerContext& handler_context) {
  using mygram::utils::MakeUnexpected;

  InitializedComponents handlers;

  handlers.search_handler = std::make_unique<SearchHandler>(handler_context);
  handlers.facet_handler = std::make_unique<FacetHandler>(handler_context);
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
  // sync_manager_ is guaranteed to be non-null (enforced in Create())
  auto sync_handler_result = SyncHandler::Create(handler_context, sync_manager_);
  if (!sync_handler_result) {
    return MakeUnexpected(sync_handler_result.error());
  }
  handlers.sync_handler = std::move(*sync_handler_result);
#endif

  return handlers;
}

mygram::utils::Expected<std::unique_ptr<RequestDispatcher>, mygram::utils::Error>
ServerLifecycleManager::InitDispatcher(HandlerContext& handler_context, const InitializedComponents& handlers) {
  using mygram::utils::ErrorCode;
  using mygram::utils::MakeError;
  using mygram::utils::MakeUnexpected;

  auto dispatcher = std::make_unique<RequestDispatcher>(handler_context, config_);

  // Register all command handlers
  dispatcher->RegisterHandler(query::QueryType::SEARCH, handlers.search_handler.get());
  dispatcher->RegisterHandler(query::QueryType::COUNT, handlers.search_handler.get());
  dispatcher->RegisterHandler(query::QueryType::FACET, handlers.facet_handler.get());
  dispatcher->RegisterHandler(query::QueryType::GET, handlers.document_handler.get());
  dispatcher->RegisterHandler(query::QueryType::DUMP_SAVE, handlers.dump_handler.get());
  dispatcher->RegisterHandler(query::QueryType::DUMP_LOAD, handlers.dump_handler.get());
  dispatcher->RegisterHandler(query::QueryType::DUMP_VERIFY, handlers.dump_handler.get());
  dispatcher->RegisterHandler(query::QueryType::DUMP_INFO, handlers.dump_handler.get());
  dispatcher->RegisterHandler(query::QueryType::DUMP_STATUS, handlers.dump_handler.get());
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
  // sync_handler is guaranteed to be non-null (sync_manager_ is enforced in constructor).
  // SYNC_STOP must also be registered: query_parser produces it for "SYNC STOP [table]"
  // and sync_handler dispatches on it. Forgetting any one of SYNC / SYNC_STATUS /
  // SYNC_STOP would leave that command falling through to the dispatcher's
  // "Unknown query type" error instead of reaching the handler.
  dispatcher->RegisterHandler(query::QueryType::SYNC, handlers.sync_handler.get());
  dispatcher->RegisterHandler(query::QueryType::SYNC_STATUS, handlers.sync_handler.get());
  dispatcher->RegisterHandler(query::QueryType::SYNC_STOP, handlers.sync_handler.get());
#endif

  // Startup completeness check: verify every QueryType that callers can
  // actually produce has a registered handler. Forgetting to RegisterHandler
  // a newly added enum value (the historical SYNC_STOP regression) silently
  // turns it into "Unknown query type" at runtime; surfacing it here keeps the
  // dispatcher table in lockstep with the QueryType enum.
  //
  // Intentionally excluded from the required set:
  //   - QueryType::UNKNOWN: sentinel for parser failures.
  //   - QueryType::SAVE / QueryType::LOAD: legacy aliases retained for parser
  //     backward compatibility; the dispatcher rejects them with a normal
  //     "Unknown query type" so users migrate to DUMP SAVE / DUMP LOAD.
  //   - SYNC family entries are required only when USE_MYSQL is defined.
#ifdef USE_MYSQL
  constexpr size_t kRequiredQueryTypeCount = 28;
#else
  constexpr size_t kRequiredQueryTypeCount = 25;
#endif
  static constexpr std::array<query::QueryType, kRequiredQueryTypeCount> kRequiredQueryTypes{{
      query::QueryType::SEARCH,
      query::QueryType::COUNT,
      query::QueryType::FACET,
      query::QueryType::GET,
      query::QueryType::DUMP_SAVE,
      query::QueryType::DUMP_LOAD,
      query::QueryType::DUMP_VERIFY,
      query::QueryType::DUMP_INFO,
      query::QueryType::DUMP_STATUS,
      query::QueryType::INFO,
      query::QueryType::CONFIG_HELP,
      query::QueryType::CONFIG_SHOW,
      query::QueryType::CONFIG_VERIFY,
      query::QueryType::REPLICATION_STATUS,
      query::QueryType::REPLICATION_STOP,
      query::QueryType::REPLICATION_START,
      query::QueryType::DEBUG_ON,
      query::QueryType::DEBUG_OFF,
      query::QueryType::OPTIMIZE,
      query::QueryType::CACHE_CLEAR,
      query::QueryType::CACHE_STATS,
      query::QueryType::CACHE_ENABLE,
      query::QueryType::CACHE_DISABLE,
      query::QueryType::SET,
      query::QueryType::SHOW_VARIABLES,
#ifdef USE_MYSQL
      query::QueryType::SYNC,
      query::QueryType::SYNC_STATUS,
      query::QueryType::SYNC_STOP,
#endif
  }};
  for (const auto qt : kRequiredQueryTypes) {
    if (!dispatcher->HasHandler(qt)) {
      return MakeUnexpected(
          MakeError(ErrorCode::kServerInitMissingDependency,
                    "RequestDispatcher missing handler for QueryType=" + std::to_string(static_cast<int>(qt)) +
                        " (see ServerLifecycleManager::InitDispatcher)"));
    }
  }

  return dispatcher;
}

mygram::utils::Expected<std::unique_ptr<ConnectionAcceptor>, mygram::utils::Error>
ServerLifecycleManager::InitAcceptor() {
  using mygram::utils::MakeUnexpected;

  auto acceptor = std::make_unique<ConnectionAcceptor>(config_);

  // Start the acceptor
  auto start_result = acceptor->Start();
  if (!start_result) {
    return MakeUnexpected(start_result.error());
  }

  return acceptor;
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

  // Precondition: SnapshotScheduler dereferences table_catalog during snapshots,
  // so we reject construction with a null catalog at the call site rather than
  // letting it crash later. This replaces the previous null check that was
  // silently logged inside SnapshotScheduler's constructor.
  if (table_catalog == nullptr) {
    return MakeUnexpected(MakeError(ErrorCode::kInternalError, "TableCatalog must not be null for SnapshotScheduler"));
  }

  auto scheduler = std::make_unique<SnapshotScheduler>(
      full_config_->dump, table_catalog, full_config_, dump_dir_, binlog_reader_, dump_save_in_progress_,
      replication_paused_for_dump_, &replication_pause_counter_, &dump_load_in_progress_);

  // Start the scheduler
  scheduler->Start();

  return scheduler;
}

}  // namespace mygramdb::server
