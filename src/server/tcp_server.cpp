/**
 * @file tcp_server.cpp
 * @brief TCP server implementation
 */

#include "server/tcp_server.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
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

}  // namespace

TcpServer::TcpServer(ServerConfig config, std::unordered_map<std::string, TableContext*> table_contexts,
                     std::string dump_dir, const config::Config* full_config, mysql::IBinlogReader* binlog_reader)
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
    mygram::utils::StructuredLog().Event("tcp_server_start_failed").Field("error", error.to_string()).Error();
    return MakeUnexpected(error);
  }

  shutdown_in_progress_.store(false, std::memory_order_release);

  // Create rate limiter (if configured). RequestDispatcher enforces the
  // token bucket per peer IP for every TCP command frame.
  //
  // Held as shared_ptr so HttpServer can co-own the same instance via
  // GetSharedRateLimiter() (Fix N-4): a client's quota MUST apply across
  // protocols. Two independent limiters give the client effectively 2x the
  // configured limit, which silently defeats DoS protection.
  if (full_config_ != nullptr) {
    rate_limiter_ = std::make_shared<RateLimiter>(static_cast<size_t>(full_config_->api.rate_limiting.capacity),
                                                  static_cast<size_t>(full_config_->api.rate_limiting.refill_rate),
                                                  static_cast<size_t>(full_config_->api.rate_limiting.max_clients),
                                                  full_config_->api.rate_limiting.enable);
    mygram::utils::StructuredLog()
        .Event("rate_limiter_initialized")
        .Field("enabled", full_config_->api.rate_limiting.enable)
        .Field("capacity", static_cast<uint64_t>(full_config_->api.rate_limiting.capacity))
        .Field("refill_rate", static_cast<uint64_t>(full_config_->api.rate_limiting.refill_rate))
        .Field("max_clients", static_cast<uint64_t>(full_config_->api.rate_limiting.max_clients))
        .Info();
  }

#ifdef USE_MYSQL
  // Create SYNC operation manager (if MySQL enabled) - needed before ServerLifecycleManager
  // Note: Must be created before lifecycle manager because SyncHandler needs it
  sync_manager_ = std::make_unique<SyncOperationManager>(table_contexts_, full_config_, binlog_reader_,
                                                         &replication_pause_counter_);
#endif

  // Initialize all server components via ServerLifecycleManager
  // This centralizes component creation and dependency ordering
#ifdef USE_MYSQL
  auto lifecycle_manager_result = ServerLifecycleManager::Create(
      config_, table_contexts_, dump_dir_, full_config_, stats_, dump_load_in_progress_, dump_save_in_progress_,
      optimization_in_progress_, replication_paused_for_dump_, mysql_reconnecting_, replication_pause_counter_,
      binlog_reader_, sync_manager_.get(), rate_limiter_.get());
#else
  auto lifecycle_manager_result = ServerLifecycleManager::Create(
      config_, table_contexts_, dump_dir_, full_config_, stats_, dump_load_in_progress_, dump_save_in_progress_,
      optimization_in_progress_, replication_paused_for_dump_, mysql_reconnecting_, replication_pause_counter_,
      binlog_reader_, rate_limiter_.get());
#endif
  if (!lifecycle_manager_result) {
    return MakeUnexpected(lifecycle_manager_result.error());
  }

  auto components_result = (*lifecycle_manager_result)->Initialize();
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

  // Wire the shutdown flag through to handlers so that long-running workers
  // (DumpSaveWorker, DumpLoadWorker) can skip post-operation binlog Start()
  // when TcpServer::Stop() has been called (CR-10). Setting this BEFORE the
  // acceptor/reactor start ensures the flag is observable by any worker
  // started via the first DUMP SAVE request.
  handler_context_->shutdown_flag = &shutdown_in_progress_;

  search_handler_ = std::move(components.search_handler);
  document_handler_ = std::move(components.document_handler);
  dump_handler_ = std::move(components.dump_handler);
  admin_handler_ = std::move(components.admin_handler);
  replication_handler_ = std::move(components.replication_handler);
  debug_handler_ = std::move(components.debug_handler);
  cache_handler_ = std::move(components.cache_handler);
  variable_handler_ = std::move(components.variable_handler);
  facet_handler_ = std::move(components.facet_handler);
#ifdef USE_MYSQL
  sync_handler_ = std::move(components.sync_handler);
#endif
  dispatcher_ = std::move(components.dispatcher);
  acceptor_ = std::move(components.acceptor);
  scheduler_ = std::move(components.scheduler);

  // Reactor I/O model: create the IoReactor up front. This is the only I/O
  // path; the legacy blocking one-thread-per-connection model was removed in
  // Step 4. On platforms with no supported event multiplexer (neither epoll
  // nor kqueue), IoReactor::Start() returns kNetworkReactorUnsupported and
  // we propagate that error up — there is no fallback.
  ReactorConfig rcfg;
  if (config_.max_write_queue_bytes > 0) {
    rcfg.max_write_queue_bytes = static_cast<size_t>(config_.max_write_queue_bytes);
  }
  if (config_.recv_timeout_sec > 0) {
    rcfg.initial_read_timeout_sec = config_.recv_timeout_sec;
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
      Stop();
      return MakeUnexpected(r.error());
    }
  }
  mygram::utils::StructuredLog().Event("reactor_mode_enabled").Field("backend", reactor_->BackendName()).Info();

  // Install the acceptor reactor handler.
  //
  {
    IoReactor* reactor_ptr = reactor_.get();
    RequestDispatcher* dispatcher_ptr = dispatcher_.get();
    ThreadPool* pool_ptr = thread_pool_.get();
    ServerStats* stats_ptr = &stats_;
    const size_t max_write_bytes = config_.max_write_queue_bytes > 0
                                       ? static_cast<size_t>(config_.max_write_queue_bytes)
                                       : ReactorConnection::kDefaultMaxWriteQueueBytes;
    acceptor_->SetReactorHandler(
        [reactor_ptr, dispatcher_ptr, pool_ptr, stats_ptr, max_write_bytes](int client_fd) -> bool {
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
          (void)conn->ReleaseFd();
          return false;
        });
  }

  // Spawn the accept loop AFTER SetReactorHandler so the new thread observes a
  // fully-published handler via the std::thread constructor's happens-before
  // edge. Previously the acceptor started its thread inside Start() (during
  // ServerLifecycleManager::InitAcceptor) and then the handler was installed
  // afterwards, which was a documented data race on reactor_handler_.
  {
    auto accept_result = acceptor_->StartAccepting();
    if (!accept_result) {
      Stop();
      return MakeUnexpected(accept_result.error());
    }
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

  // Step 1: announce shutdown.
  //
  // Set shutdown_in_progress_ BEFORE joining any worker so long-running
  // workers (DumpSaveWorker, DumpLoadWorker) observe the flag at every
  // resumable check point and skip post-operation binlog restart. Without
  // this, a worker that captured replication_was_running=true at entry will
  // call binlog_reader_->Start() during shutdown, racing the binlog_reader_
  // destructor that is about to run after Stop() returns (CR-10).
  shutdown_in_progress_.store(true, std::memory_order_release);

  // Step 2: stop ingest paths that may touch binlog_reader_.
  //
  // Order rationale (CR-3 / CR-10):
  //   (a) The dump worker may be inside DumpSaveWorker / DumpLoadWorker,
  //       which call binlog_reader_->Stop() / Start() under
  //       replication_paused_for_dump_. Joining the worker here — while
  //       binlog_reader_ is still alive — guarantees those calls complete
  //       cleanly before binlog_reader_ is torn down.
  //   (b) sync_manager_ also calls binlog_reader_->Stop() / GetCurrentGTID
  //       inside BuildSnapshotAsync; RequestShutdown signals cancellation
  //       and a successful WaitForCompletion joins completed sync threads.
  //   (c) snapshot_scheduler_ wakes its loop and joins, including any
  //       in-flight TakeSnapshot that touches binlog_reader_.
  //
  // After Step 2 completes, no caller other than Stop() itself ever calls
  // into binlog_reader_ again, so its destructor (which fires when this
  // TcpServer is destroyed, after Stop() returns) is race-free.
  dump_progress_.JoinWorker();

#ifdef USE_MYSQL
  // Request SYNC manager to shutdown
  if (sync_manager_) {
    sync_manager_->RequestShutdown();
    sync_manager_->WaitForCompletion(kDefaultSyncShutdownTimeoutSec);
  }
#endif

  // Stop snapshot scheduler (also touches binlog_reader_ in TakeSnapshot)
  if (scheduler_) {
    scheduler_->Stop();
  }

  // Step 3: tear down the network stack.
  //
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

  // Step 4: drain remaining drain tasks.
  //
  // Shutdown thread pool LAST. Tasks queued on the pool may still hold
  // shared_ptr<ReactorConnection> copies that reach into close_callback_
  // (which captures `accept_ptr` and `close_stats_ptr` from this TcpServer);
  // those captured pointers must remain valid for the lifetime of any
  // in-flight callback (CR-3). Joining the pool here ensures every queued
  // task — including the close callbacks Unregister() may have just spawned
  // when the reactor stopped — has fully completed before the captured
  // ServerStats / ConnectionAcceptor objects start destruction.
  if (thread_pool_) {
    thread_pool_->Shutdown();
  }

  // After this point, member destruction order (reverse of declaration in
  // tcp_server.h) tears down handlers / dispatcher / reactor / acceptor /
  // ... / scheduler / sync_manager_ / cache_manager_ / table_catalog_ /
  // thread_pool_ / acceptor_, and finally binlog_reader_ is dropped by the
  // owner that constructed this TcpServer. By Step 1+2 above, no thread
  // managed by this server still calls into binlog_reader_, so that drop
  // is safe.

  mygram::utils::StructuredLog().Event("tcp_server_stopped").Field("total_requests", stats_.GetTotalRequests()).Debug();
}

}  // namespace mygramdb::server
