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
#include <sys/un.h>
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
#include "server/response_formatter.h"
#include "server/server_lifecycle_manager.h"
#include "storage/dump_format_v1.h"
#include "utils/network_utils.h"
#include "utils/string_utils.h"
#include "version.h"

#ifdef USE_MYSQL
#include "mysql/binlog_reader.h"
#include "server/sync_operation_manager.h"
#endif

namespace mygramdb::server {

// ToSockaddr / ToSockaddrUn are provided by utils/network_utils.h
using mygram::utils::ToSockaddr;

namespace {
// Buffer size for IP address formatting
constexpr size_t kIpAddressBufferSize = 64;

// Default timeout values (in seconds)
constexpr int kDefaultSyncShutdownTimeoutSec = 30;
constexpr int kDefaultConnectionRecvTimeoutSec = 60;

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
  config_.parsed_allow_cidrs = mygram::utils::ParseAllowCidrs(config_.allow_cidrs);
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

  // Create rate limiter (if configured). The reactor_handler lambda below
  // enforces the token bucket per peer IP on every accept.
  // Note: RateLimiter is NOT managed by ServerLifecycleManager because it's only used in TcpServer
  if (full_config_ != nullptr && full_config_->api.rate_limiting.enable) {
    rate_limiter_ = std::make_unique<RateLimiter>(static_cast<size_t>(full_config_->api.rate_limiting.capacity),
                                                  static_cast<size_t>(full_config_->api.rate_limiting.refill_rate),
                                                  static_cast<size_t>(full_config_->api.rate_limiting.max_clients));
    mygram::utils::StructuredLog()
        .Event("rate_limiter_initialized")
        .Field("capacity", static_cast<uint64_t>(full_config_->api.rate_limiting.capacity))
        .Field("refill_rate", static_cast<uint64_t>(full_config_->api.rate_limiting.refill_rate))
        .Field("max_clients", static_cast<uint64_t>(full_config_->api.rate_limiting.max_clients))
        .Info();
  }

#ifdef USE_MYSQL
  // Create SYNC operation manager (if MySQL enabled) - needed before ServerLifecycleManager
  // Note: Must be created before lifecycle manager because SyncHandler needs it
  sync_manager_ = std::make_unique<SyncOperationManager>(table_contexts_, full_config_, binlog_reader_);
#endif

  // Initialize all server components via ServerLifecycleManager
  // This centralizes component creation and dependency ordering
  ServerLifecycleManager lifecycle_manager(config_, table_contexts_, dump_dir_, full_config_, stats_,
                                           dump_load_in_progress_, dump_save_in_progress_, optimization_in_progress_,
                                           replication_paused_for_dump_, mysql_reconnecting_,
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
#ifdef USE_MYSQL
  // Provide cache manager to sync manager for post-SYNC cache invalidation
  if (sync_manager_) {
    sync_manager_->SetCacheManager(cache_manager_.get());
  }
#endif
  variable_manager_ = std::move(components.variable_manager);
  handler_context_ = std::move(components.handler_context);

  // Update HandlerContext pointer to point to TcpServer-owned variable_manager
  // (The original pointer in HandlerContext pointed to the components.variable_manager
  // which would become invalid after this scope)
  handler_context_->variable_manager = variable_manager_.get();

  // Set dump progress tracking pointer
  handler_context_->dump_progress = &dump_progress_;

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

  // Reactor I/O model: create the IoReactor up front. This is the only I/O
  // path; the legacy blocking one-thread-per-connection model was removed in
  // Phase 4. On platforms with no supported event multiplexer (neither epoll
  // nor kqueue), IoReactor::Start() returns kNetworkReactorUnsupported and
  // we propagate that error up — there is no fallback.
  ReactorConfig rcfg;
  if (config_.max_write_queue_bytes > 0) {
    rcfg.max_write_queue_bytes = static_cast<size_t>(config_.max_write_queue_bytes);
  }
  reactor_ = std::make_unique<IoReactor>(thread_pool_.get(), dispatcher_.get(), rcfg);
  // When the reactor tears down a connection, decrement the acceptor's
  // max_connections gate so new accepts can proceed, and the server stats
  // so GetConnectionCount() reflects reality.
  {
    ConnectionAcceptor* accept_ptr = acceptor_.get();
    ServerStats* close_stats_ptr = &stats_;
    reactor_->SetCloseCallback([accept_ptr, close_stats_ptr](int fd) {
      if (accept_ptr != nullptr) {
        accept_ptr->RemoveConnection(fd);
      }
      if (close_stats_ptr != nullptr) {
        close_stats_ptr->DecrementConnections();
      }
    });
  }
  {
    auto r = reactor_->Start();
    if (!r) {
      return MakeUnexpected(r.error());
    }
  }
  mygram::utils::StructuredLog().Event("reactor_mode_enabled").Field("backend", reactor_->BackendName()).Info();

  // Install the acceptor reactor handler.
  //
  // Rate-limit policy:
  //  - TCP acceptor (unix_socket_path empty) + rate_limiter_ set: enforce per
  //    peer-IP token bucket inside the accept handler.
  //  - UDS acceptor (unix_socket_path non-empty): local, trusted, bypass the
  //    AllowRequest() call entirely.
  const bool apply_rate_limit = (rate_limiter_ != nullptr) && config_.unix_socket_path.empty();
  RateLimiter* rate_limiter_ptr = apply_rate_limit ? rate_limiter_.get() : nullptr;

  {
    IoReactor* reactor_ptr = reactor_.get();
    RequestDispatcher* dispatcher_ptr = dispatcher_.get();
    ThreadPool* pool_ptr = thread_pool_.get();
    ServerStats* stats_ptr = &stats_;
    const size_t max_write_bytes = config_.max_write_queue_bytes > 0
                                       ? static_cast<size_t>(config_.max_write_queue_bytes)
                                       : ReactorConnection::kDefaultMaxWriteQueueBytes;
    acceptor_->SetReactorHandler(
        [reactor_ptr, dispatcher_ptr, pool_ptr, stats_ptr, rate_limiter_ptr, max_write_bytes](int client_fd) -> bool {
          // Rate limit check (TCP only; rate_limiter_ptr is null on UDS acceptors).
          // Extract the peer IP via getpeername() and call AllowRequest(). On
          // rejection we return false so the acceptor emits SERVER_BUSY + closes
          // the fd (see ConnectionAcceptor::AcceptLoop).
          if (rate_limiter_ptr != nullptr) {
            struct sockaddr_storage addr_storage {};
            socklen_t addr_len = sizeof(addr_storage);
            std::string client_ip;
            // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) - POSIX socket API
            if (getpeername(client_fd, reinterpret_cast<struct sockaddr*>(&addr_storage), &addr_len) == 0 &&
                addr_storage.ss_family == AF_INET) {
              // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) - POSIX socket API
              auto* addr_in = reinterpret_cast<struct sockaddr_in*>(&addr_storage);
              char ip_str[INET_ADDRSTRLEN];  // NOLINT(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
              // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
              inet_ntop(AF_INET, &addr_in->sin_addr, ip_str, INET_ADDRSTRLEN);
              // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
              client_ip = ip_str;
            } else {
              client_ip = "unknown";
            }
            if (!rate_limiter_ptr->AllowRequest(client_ip)) {
              mygram::utils::StructuredLog()
                  .Event("server_warning")
                  .Field("type", "rate_limit_exceeded")
                  .Field("client_ip", client_ip)
                  .Warn();
              return false;
            }
          }

          auto conn =
              ReactorConnection::Create(client_fd, reactor_ptr, dispatcher_ptr, pool_ptr, stats_ptr, max_write_bytes);
          auto reg = reactor_ptr->Register(conn);
          if (reg.has_value()) {
            // Count this as an active connection so GetConnectionCount() and the
            // mygramdb_active_connections metric report the live reactor
            // population. The matching decrement happens in the close callback
            // installed above.
            stats_ptr->IncrementConnections();
            stats_ptr->IncrementTotalConnections();
            return true;
          }
          return false;
        });
  }

  mygram::utils::StructuredLog()
      .Event("tcp_server_started")
      .Field("host", config_.host)
      .Field("port", static_cast<uint64_t>(acceptor_->GetPort()))
      .Field("unix_socket", config_.unix_socket_path.empty() ? "(disabled)" : config_.unix_socket_path)
      .Info();
  return {};
}

void TcpServer::Stop() {
  mygram::utils::StructuredLog().Event("tcp_server_stopping").Debug();

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

  // Stop the IoReactor BEFORE the acceptor. Rationale: `ConnectionAcceptor::Stop`
  // eagerly close(2)s every fd in its active_fds_ set — which includes the
  // reactor-owned client fds, since the reactor's close_callback removes them
  // only when the connection terminates naturally. If the acceptor ran first,
  // it would close fds the reactor still owns, and drain tasks would then hit
  // EBADF mid-send. Stopping the reactor first gives it the chance to drain
  // and release its fds via Unregister → close_callback → active_fds_.erase.
  //
  // While the reactor is stopping, the acceptor may still accept a stray
  // connection and hand it to reactor_handler_; the handler's Register call
  // returns `kNetworkServerNotStarted`, the handler returns false, and the
  // acceptor sends SERVER_BUSY and closes the socket. Acceptable.
  if (reactor_) {
    reactor_->Stop();
  }

  // Stop connection acceptor (handles either TCP or UDS depending on
  // config_.unix_socket_path; see ConnectionAcceptor::Start).
  if (acceptor_) {
    acceptor_->Stop();
  }

  // Join dump worker thread if still running
  dump_progress_.JoinWorker();

  // Shutdown thread pool (completes pending tasks)
  if (thread_pool_) {
    thread_pool_->Shutdown();
  }

  mygram::utils::StructuredLog().Event("tcp_server_stopped").Field("total_requests", stats_.GetTotalRequests()).Debug();
}

#ifdef USE_MYSQL

std::string TcpServer::StartSync(const std::string& table_name) {
  if (!sync_manager_) {
    return ResponseFormatter::FormatError("SYNC manager not initialized");
  }
  auto result = sync_manager_->StartSync(table_name);
  if (!result) {
    return ResponseFormatter::FormatError(result.error().message());
  }
  return *result;
}

std::string TcpServer::GetSyncStatus() {
  if (!sync_manager_) {
    return "status=IDLE message=\"SYNC manager not initialized\"";
  }
  return sync_manager_->GetSyncStatus();
}

#endif  // USE_MYSQL

}  // namespace mygramdb::server
