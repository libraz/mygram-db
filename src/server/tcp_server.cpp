/**
 * @file tcp_server.cpp
 * @brief TCP server implementation
 */

#include "server/tcp_server.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <spdlog/spdlog.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <limits>
#include <sstream>
#include <utility>

#include "query/result_sorter.h"
#include "server/handlers/admin_handler.h"
#include "server/handlers/cache_handler.h"
#include "server/handlers/debug_handler.h"
#include "server/handlers/document_handler.h"
#include "server/handlers/dump_handler.h"
#include "server/handlers/replication_handler.h"
#include "server/handlers/search_handler.h"
#include "utils/structured_log.h"
#ifdef USE_MYSQL
#include "server/handlers/sync_handler.h"
#endif
#include "cache/cache_manager.h"
#include "server/connection_io_handler.h"
#include "server/response_formatter.h"
#include "storage/dump_format_v1.h"
#include "utils/fd_guard.h"
#include "utils/network_utils.h"
#include "utils/string_utils.h"
#include "version.h"

#ifdef USE_MYSQL
#include "mysql/binlog_reader.h"
#include "server/sync_operation_manager.h"
#endif

namespace mygramdb::server {

namespace {
// Thread pool queue size for backpressure
constexpr size_t kThreadPoolQueueSize = 1000;

// Buffer size for IP address formatting
constexpr size_t kIpAddressBufferSize = 64;

// Length of "gtid=\"" prefix in meta content
constexpr size_t kGtidPrefixLength = 7;

// Default timeout values (in seconds)
constexpr int kDefaultSyncShutdownTimeoutSec = 30;
constexpr int kDefaultConnectionRecvTimeoutSec = 60;

/**
 * @brief Helper to safely cast sockaddr_in* to sockaddr* for socket API
 *
 * POSIX socket API requires sockaddr* but we use sockaddr_in for IPv4.
 * This helper centralizes the required reinterpret_cast to a single location.
 *
 * Why reinterpret_cast is necessary here:
 * - POSIX socket functions (bind, accept, getsockname) require struct sockaddr*
 * - We use struct sockaddr_in for IPv4, which is binary-compatible
 * - This is the standard pattern in all POSIX socket programming
 * - The cast is safe as both types share the same memory layout for the address family
 *
 * @param addr Pointer to sockaddr_in structure
 * @return Pointer to sockaddr (same memory location, different type)
 */
inline struct sockaddr* ToSockaddr(struct sockaddr_in* addr) {
  // Suppressing clang-tidy warning for POSIX socket API compatibility
  // This reinterpret_cast is required and safe for socket address structures
  return reinterpret_cast<struct sockaddr*>(addr);  // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
}

std::vector<utils::CIDR> ParseAllowCidrs(const std::vector<std::string>& allow_cidrs) {
  std::vector<utils::CIDR> parsed;
  parsed.reserve(allow_cidrs.size());

  for (const auto& cidr_str : allow_cidrs) {
    auto cidr = utils::CIDR::Parse(cidr_str);
    if (!cidr) {
      mygram::utils::StructuredLog()
          .Event("server_warning")
          .Field("type", "invalid_cidr_entry")
          .Field("cidr", cidr_str)
          .Warn();
      continue;
    }
    parsed.push_back(*cidr);
  }

  return parsed;
}

}  // namespace

TcpServer::TcpServer(ServerConfig config, std::unordered_map<std::string, TableContext*> table_contexts,
                     std::string dump_dir, const config::Config* full_config,
#ifdef USE_MYSQL
                     mysql::BinlogReader* binlog_reader
#else
                     void* binlog_reader
#endif
                     )
    : config_(std::move(config)),
      full_config_(full_config),
      dump_dir_(std::move(dump_dir)),
      table_contexts_(std::move(table_contexts)),
      binlog_reader_(binlog_reader) {
  config_.parsed_allow_cidrs = ParseAllowCidrs(config_.allow_cidrs);
  // NOTE: Component initialization moved to Start() method
  // This allows for better error handling and resource cleanup
}

TcpServer::~TcpServer() {
  Stop();
}

mygram::utils::Expected<void, mygram::utils::Error> TcpServer::Start() {
  using mygram::utils::ErrorCode;
  using mygram::utils::MakeError;
  using mygram::utils::MakeUnexpected;

  // Check if already running
  if (acceptor_ && acceptor_->IsRunning()) {
    auto error = MakeError(ErrorCode::kNetworkAlreadyRunning, "Server already running");
    mygram::utils::StructuredLog()
        .Event("server_error")
        .Field("operation", "tcp_server_start")
        .Field("error", error.to_string())
        .Error();
    return MakeUnexpected(error);
  }

  // 1. Create thread pool
  thread_pool_ =
      std::make_unique<ThreadPool>(config_.worker_threads > 0 ? config_.worker_threads : 0, kThreadPoolQueueSize);

  // 2. Create table catalog
  table_catalog_ = std::make_unique<TableCatalog>(table_contexts_);

  // 2.5. Create cache manager (if configured)
  if (full_config_ != nullptr && full_config_->cache.enabled) {
    // Pass table_contexts to support per-table ngram settings
    cache_manager_ = std::make_unique<cache::CacheManager>(full_config_->cache, table_contexts_);
    spdlog::info("Cache manager initialized with per-table ngram settings");
  }

  // 2.5.1. Create rate limiter (if configured)
  if (full_config_ != nullptr && full_config_->api.rate_limiting.enable) {
    rate_limiter_ = std::make_unique<RateLimiter>(static_cast<size_t>(full_config_->api.rate_limiting.capacity),
                                                  static_cast<size_t>(full_config_->api.rate_limiting.refill_rate),
                                                  static_cast<size_t>(full_config_->api.rate_limiting.max_clients));
    spdlog::info("Rate limiter initialized: capacity={}, refill_rate={}/s, max_clients={}",
                 full_config_->api.rate_limiting.capacity, full_config_->api.rate_limiting.refill_rate,
                 full_config_->api.rate_limiting.max_clients);
  }

#ifdef USE_MYSQL
  // 2.6. Create SYNC operation manager (if MySQL enabled)
  sync_manager_ = std::make_unique<SyncOperationManager>(table_contexts_, full_config_, binlog_reader_);
#endif

  // 3. Initialize handler context
  handler_context_ = std::make_unique<HandlerContext>(HandlerContext{
      .table_catalog = table_catalog_.get(),
      .table_contexts = table_contexts_,
      .stats = stats_,
      .full_config = full_config_,
      .dump_dir = dump_dir_,
      .loading = loading_,
      .read_only = read_only_,
      .optimization_in_progress = optimization_in_progress_,
#ifdef USE_MYSQL
      .binlog_reader = binlog_reader_,
      .syncing_tables = sync_manager_->GetSyncingTablesRef(),
      .syncing_tables_mutex = sync_manager_->GetSyncingTablesMutex(),
#else
      .binlog_reader = binlog_reader_,
#endif
      .cache_manager = cache_manager_.get(),
  });

  // 4. Initialize command handlers
  search_handler_ = std::make_unique<SearchHandler>(*handler_context_);
  document_handler_ = std::make_unique<DocumentHandler>(*handler_context_);
  dump_handler_ = std::make_unique<DumpHandler>(*handler_context_);
  admin_handler_ = std::make_unique<AdminHandler>(*handler_context_);
  replication_handler_ = std::make_unique<ReplicationHandler>(*handler_context_);
  debug_handler_ = std::make_unique<DebugHandler>(*handler_context_);
  cache_handler_ = std::make_unique<CacheHandler>(*handler_context_);
#ifdef USE_MYSQL
  sync_handler_ = std::make_unique<SyncHandler>(*handler_context_, *this);
#endif

  // 5. Setup dispatcher
  dispatcher_ = std::make_unique<RequestDispatcher>(*handler_context_, config_);
  dispatcher_->RegisterHandler(query::QueryType::SEARCH, search_handler_.get());
  dispatcher_->RegisterHandler(query::QueryType::COUNT, search_handler_.get());
  dispatcher_->RegisterHandler(query::QueryType::GET, document_handler_.get());
  dispatcher_->RegisterHandler(query::QueryType::DUMP_SAVE, dump_handler_.get());
  dispatcher_->RegisterHandler(query::QueryType::DUMP_LOAD, dump_handler_.get());
  dispatcher_->RegisterHandler(query::QueryType::DUMP_VERIFY, dump_handler_.get());
  dispatcher_->RegisterHandler(query::QueryType::DUMP_INFO, dump_handler_.get());
  dispatcher_->RegisterHandler(query::QueryType::INFO, admin_handler_.get());
  dispatcher_->RegisterHandler(query::QueryType::CONFIG_HELP, admin_handler_.get());
  dispatcher_->RegisterHandler(query::QueryType::CONFIG_SHOW, admin_handler_.get());
  dispatcher_->RegisterHandler(query::QueryType::CONFIG_VERIFY, admin_handler_.get());
  dispatcher_->RegisterHandler(query::QueryType::REPLICATION_STATUS, replication_handler_.get());
  dispatcher_->RegisterHandler(query::QueryType::REPLICATION_STOP, replication_handler_.get());
  dispatcher_->RegisterHandler(query::QueryType::REPLICATION_START, replication_handler_.get());
  dispatcher_->RegisterHandler(query::QueryType::DEBUG_ON, debug_handler_.get());
  dispatcher_->RegisterHandler(query::QueryType::DEBUG_OFF, debug_handler_.get());
  dispatcher_->RegisterHandler(query::QueryType::OPTIMIZE, debug_handler_.get());
  dispatcher_->RegisterHandler(query::QueryType::CACHE_CLEAR, cache_handler_.get());
  dispatcher_->RegisterHandler(query::QueryType::CACHE_STATS, cache_handler_.get());
  dispatcher_->RegisterHandler(query::QueryType::CACHE_ENABLE, cache_handler_.get());
  dispatcher_->RegisterHandler(query::QueryType::CACHE_DISABLE, cache_handler_.get());
#ifdef USE_MYSQL
  dispatcher_->RegisterHandler(query::QueryType::SYNC, sync_handler_.get());
  dispatcher_->RegisterHandler(query::QueryType::SYNC_STATUS, sync_handler_.get());
#endif

  // 6. Start connection acceptor
  acceptor_ = std::make_unique<ConnectionAcceptor>(config_, thread_pool_.get());
  acceptor_->SetConnectionHandler([this](int client_fd) { HandleConnection(client_fd); });
  auto start_result = acceptor_->Start();
  if (!start_result) {
    return MakeUnexpected(start_result.error());
  }

  // 7. Start snapshot scheduler (if configured)
  if (full_config_ != nullptr && full_config_->dump.interval_sec > 0) {
    scheduler_ = std::make_unique<SnapshotScheduler>(full_config_->dump, table_catalog_.get(), full_config_, dump_dir_,
                                                     binlog_reader_);
    scheduler_->Start();
  }

  spdlog::info("TCP server started on {}:{}", config_.host, acceptor_->GetPort());
  return {};
}

void TcpServer::Stop() {
  spdlog::info("Stopping TCP server...");

  // Signal shutdown to all connection handlers
  shutdown_requested_ = true;

#ifdef USE_MYSQL
  // Request SYNC manager to shutdown
  if (sync_manager_) {
    sync_manager_->RequestShutdown();
    sync_manager_->WaitForCompletion(kDefaultSyncShutdownTimeoutSec);
  }
#endif

  // Stop snapshot scheduler
  if (scheduler_) {
    scheduler_->Stop();
  }

  // Stop connection acceptor
  if (acceptor_) {
    acceptor_->Stop();
  }

  // Shutdown thread pool (completes pending tasks)
  if (thread_pool_) {
    thread_pool_->Shutdown();
  }

  spdlog::info("TCP server stopped. Handled {} total requests", stats_.GetTotalRequests());
}

void TcpServer::HandleConnection(int client_fd) {
  // RAII guard to ensure FD is closed even if exceptions occur
  mygramdb::utils::FDGuard fd_guard(client_fd);

  // Get client IP address for rate limiting
  std::string client_ip;
  struct sockaddr_in addr {};  // NOLINT(cppcoreguidelines-pro-type-member-init) - Zero-init needed for getpeername
  socklen_t addr_len = sizeof(addr);
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) - Required for POSIX socket API
  if (getpeername(client_fd, reinterpret_cast<struct sockaddr*>(&addr), &addr_len) == 0) {
    char ip_str[INET_ADDRSTRLEN];  // NOLINT(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays) - Required for
                                   // inet_ntop
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay) - Required for inet_ntop
    inet_ntop(AF_INET, &(addr.sin_addr), ip_str, INET_ADDRSTRLEN);
    client_ip = ip_str;  // NOLINT(cppcoreguidelines-pro-bounds-array-to-pointer-decay) - Safe implicit conversion
  } else {
    client_ip = "unknown";
  }

  // Check rate limit (if enabled)
  if (rate_limiter_ && !rate_limiter_->AllowRequest(client_ip)) {
    mygram::utils::StructuredLog()
        .Event("server_warning")
        .Field("type", "rate_limit_exceeded")
        .Field("client_ip", client_ip)
        .Warn();
    // Connection will be closed by fd_guard
    return;
  }

  // Initialize connection context
  ConnectionContext ctx;
  ctx.client_fd = client_fd;
  ctx.debug_mode = false;
  {
    std::scoped_lock<std::mutex> lock(contexts_mutex_);
    connection_contexts_[client_fd] = ctx;
  }

  // Increment active connection count and total connections received
  stats_.IncrementConnections();
  stats_.IncrementTotalConnections();

  // RAII guard to ensure stats are decremented even if exceptions occur
  mygramdb::utils::ScopeGuard stats_cleanup([this]() { stats_.DecrementConnections(); });

  // Create I/O handler config
  IOConfig io_config{.recv_buffer_size = static_cast<size_t>(config_.recv_buffer_size),
                     .max_query_length = static_cast<size_t>(config_.max_query_length),
                     .recv_timeout_sec = kDefaultConnectionRecvTimeoutSec};

  // Request processor callback
  auto processor = [this](const std::string& request, ConnectionContext& conn_ctx) -> std::string {
    // Update context from map
    {
      std::scoped_lock<std::mutex> lock(contexts_mutex_);
      conn_ctx = connection_contexts_[conn_ctx.client_fd];
    }

    // Dispatch request
    std::string response = dispatcher_->Dispatch(request, conn_ctx);
    stats_.IncrementRequests();

    // Update context back to map
    {
      std::scoped_lock<std::mutex> lock(contexts_mutex_);
      connection_contexts_[conn_ctx.client_fd] = conn_ctx;
    }

    return response;
  };

  // Delegate to ConnectionIOHandler
  ConnectionIOHandler io_handler(io_config, processor, shutdown_requested_);
  io_handler.HandleConnection(client_fd, ctx);

  // Clean up connection context
  {
    std::scoped_lock<std::mutex> lock(contexts_mutex_);
    connection_contexts_.erase(client_fd);
  }

  spdlog::debug("Connection closed (active: {})", stats_.GetActiveConnections());

  // Note: FD guard will close the FD, stats_cleanup will decrement the connection count
}

#ifdef USE_MYSQL

std::string TcpServer::StartSync(const std::string& table_name) {
  if (!sync_manager_) {
    return ResponseFormatter::FormatError("SYNC manager not initialized");
  }
  return sync_manager_->StartSync(table_name);
}

std::string TcpServer::GetSyncStatus() {
  if (!sync_manager_) {
    return "status=IDLE message=\"SYNC manager not initialized\"";
  }
  return sync_manager_->GetSyncStatus();
}

#endif  // USE_MYSQL

}  // namespace mygramdb::server
