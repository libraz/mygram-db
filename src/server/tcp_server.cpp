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
#include "server/server_lifecycle_manager.h"
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

  // Create rate limiter (if configured) - needed by HandleConnection
  // Note: RateLimiter is NOT managed by ServerLifecycleManager because it's only used in TcpServer
  if (full_config_ != nullptr && full_config_->api.rate_limiting.enable) {
    rate_limiter_ = std::make_unique<RateLimiter>(static_cast<size_t>(full_config_->api.rate_limiting.capacity),
                                                  static_cast<size_t>(full_config_->api.rate_limiting.refill_rate),
                                                  static_cast<size_t>(full_config_->api.rate_limiting.max_clients));
    spdlog::info("Rate limiter initialized: capacity={}, refill_rate={}/s, max_clients={}",
                 full_config_->api.rate_limiting.capacity, full_config_->api.rate_limiting.refill_rate,
                 full_config_->api.rate_limiting.max_clients);
  }

#ifdef USE_MYSQL
  // Create SYNC operation manager (if MySQL enabled) - needed before ServerLifecycleManager
  // Note: Must be created before lifecycle manager because SyncHandler needs it
  sync_manager_ = std::make_unique<SyncOperationManager>(table_contexts_, full_config_, binlog_reader_);
#endif

  // Initialize all server components via ServerLifecycleManager
  // This centralizes component creation and dependency ordering
  ServerLifecycleManager lifecycle_manager(config_, table_contexts_, dump_dir_, full_config_, stats_, loading_,
                                           read_only_, optimization_in_progress_, replication_paused_for_dump_,
                                           mysql_reconnecting_,
#ifdef USE_MYSQL
                                           binlog_reader_, sync_manager_.get()
#else
                                           binlog_reader_
#endif
  );

  auto components_result = lifecycle_manager.Initialize();
  if (!components_result) {
    return MakeUnexpected(components_result.error());
  }

  // Take ownership of all initialized components
  auto& components = *components_result;
  thread_pool_ = std::move(components.thread_pool);
  table_catalog_ = std::move(components.table_catalog);
  cache_manager_ = std::move(components.cache_manager);
  variable_manager_ = std::move(components.variable_manager);
  handler_context_ = std::move(components.handler_context);

  // Update HandlerContext pointer to point to TcpServer-owned variable_manager
  // (The original pointer in HandlerContext pointed to the components.variable_manager
  // which would become invalid after this scope)
  handler_context_->variable_manager = variable_manager_.get();

  search_handler_ = std::move(components.search_handler);
  document_handler_ = std::move(components.document_handler);
  dump_handler_ = std::move(components.dump_handler);
  admin_handler_ = std::move(components.admin_handler);
  replication_handler_ = std::move(components.replication_handler);
  debug_handler_ = std::move(components.debug_handler);
  cache_handler_ = std::move(components.cache_handler);
  variable_handler_ = std::move(components.variable_handler);
#ifdef USE_MYSQL
  sync_handler_ = std::move(components.sync_handler);
#endif
  dispatcher_ = std::move(components.dispatcher);
  acceptor_ = std::move(components.acceptor);
  scheduler_ = std::move(components.scheduler);

  // Set connection handler callback (must be done after acceptor_ is assigned)
  acceptor_->SetConnectionHandler([this](int client_fd) { HandleConnection(client_fd); });

  spdlog::info("TCP server started on {}:{}", config_.host, acceptor_->GetPort());
  return {};
}

void TcpServer::Stop() {
  spdlog::debug("Stopping TCP server...");

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

  spdlog::debug("TCP server stopped. Handled {} total requests", stats_.GetTotalRequests());
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
