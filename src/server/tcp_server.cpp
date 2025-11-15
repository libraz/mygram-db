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
#ifdef USE_MYSQL
#include "server/handlers/sync_handler.h"
#endif
#include "server/response_formatter.h"
#include "storage/dump_format_v1.h"
#include "utils/network_utils.h"
#include "utils/string_utils.h"
#include "version.h"

#ifdef USE_MYSQL
#include "mysql/binlog_reader.h"
#include "mysql/connection.h"
#include "storage/snapshot_builder.h"
#include "utils/memory_utils.h"
#endif

namespace mygramdb::server {

namespace {
// Thread pool queue size for backpressure
constexpr size_t kThreadPoolQueueSize = 1000;

// Buffer size for IP address formatting
constexpr size_t kIpAddressBufferSize = 64;

// Length of "gtid=\"" prefix in meta content
constexpr size_t kGtidPrefixLength = 7;

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
      spdlog::warn("Ignoring invalid CIDR entry in network.allow_cidrs: {}", cidr_str);
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

bool TcpServer::Start() {
  // Check if already running
  if (acceptor_ && acceptor_->IsRunning()) {
    last_error_ = "Server already running";
    return false;
  }

  // 1. Create thread pool
  thread_pool_ =
      std::make_unique<ThreadPool>(config_.worker_threads > 0 ? config_.worker_threads : 0, kThreadPoolQueueSize);

  // 2. Create table catalog
  table_catalog_ = std::make_unique<TableCatalog>(table_contexts_);

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
      .syncing_tables = syncing_tables_,
      .syncing_tables_mutex = syncing_tables_mutex_,
#else
      .binlog_reader = binlog_reader_,
#endif
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
  if (!acceptor_->Start()) {
    last_error_ = acceptor_->GetLastError();
    return false;
  }

  // 7. Start snapshot scheduler (if configured)
  if (full_config_ != nullptr && full_config_->dump.interval_sec > 0) {
    scheduler_ = std::make_unique<SnapshotScheduler>(full_config_->dump, table_catalog_.get(), full_config_, dump_dir_,
                                                     binlog_reader_);
    scheduler_->Start();
  }

  spdlog::info("TCP server started on {}:{}", config_.host, acceptor_->GetPort());
  return true;
}

void TcpServer::Stop() {
  spdlog::info("Stopping TCP server...");

#ifdef USE_MYSQL
  // Cancel all active SYNC operations
  shutdown_requested_ = true;
  {
    std::lock_guard<std::mutex> lock(snapshot_builders_mutex_);
    for (auto& [table_name, builder] : active_snapshot_builders_) {
      spdlog::info("Cancelling SYNC for table: {}", table_name);
      builder->Cancel();
    }
  }

  // Wait for SYNC operations to complete (with timeout)
  auto wait_start = std::chrono::steady_clock::now();
  constexpr int kSyncWaitTimeoutSec = 30;

  while (!syncing_tables_.empty()) {
    auto elapsed =
        std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - wait_start).count();

    if (elapsed > kSyncWaitTimeoutSec) {
      spdlog::warn("Timeout waiting for SYNC operations to complete");
      break;
    }

    constexpr int kSyncPollIntervalMs = 100;  // Poll every 100ms
    std::this_thread::sleep_for(std::chrono::milliseconds(kSyncPollIntervalMs));
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
  std::vector<char> buffer(config_.recv_buffer_size);
  std::string accumulated;

  // Initialize connection context
  ConnectionContext ctx;
  ctx.client_fd = client_fd;
  ctx.debug_mode = false;
  {
    std::scoped_lock<std::mutex> lock(contexts_mutex_);
    connection_contexts_[client_fd] = ctx;
  }

  // Increment active connection count
  stats_.IncrementConnections();

  while (true) {
    ssize_t bytes_received = recv(client_fd, buffer.data(), buffer.size() - 1, 0);

    if (bytes_received <= 0) {
      if (bytes_received < 0) {
        spdlog::debug("recv error: {}", strerror(errno));
      }
      break;
    }

    buffer[bytes_received] = '\0';
    accumulated += buffer.data();

    // Process complete requests (ending with \r\n)
    size_t pos = 0;
    while ((pos = accumulated.find("\r\n")) != std::string::npos) {
      std::string request = accumulated.substr(0, pos);
      accumulated = accumulated.substr(pos + 2);

      if (request.empty()) {
        continue;
      }

      // Get connection context
      {
        std::scoped_lock<std::mutex> lock(contexts_mutex_);
        ctx = connection_contexts_[client_fd];
      }

      // Dispatch request
      std::string response = dispatcher_->Dispatch(request, ctx);
      stats_.IncrementRequests();

      // Update connection context (in case debug mode changed)
      {
        std::scoped_lock<std::mutex> lock(contexts_mutex_);
        connection_contexts_[client_fd] = ctx;
      }

      // Send response
      response += "\r\n";
      ssize_t sent = send(client_fd, response.c_str(), response.length(), 0);
      if (sent < 0) {
        spdlog::debug("send error: {}", strerror(errno));
        break;
      }
    }
  }

  // Clean up connection
  {
    std::scoped_lock<std::mutex> lock(contexts_mutex_);
    connection_contexts_.erase(client_fd);
  }
  close(client_fd);
  stats_.DecrementConnections();

  spdlog::debug("Connection closed (active: {})", stats_.GetActiveConnections());
}

#ifdef USE_MYSQL

std::string TcpServer::StartSync(const std::string& table_name) {
  std::lock_guard<std::mutex> lock(sync_mutex_);

  // Check if table exists
  if (table_contexts_.find(table_name) == table_contexts_.end()) {
    return ResponseFormatter::FormatError("Table '" + table_name + "' not found in configuration");
  }

  // Check if already running for this table
  if (sync_states_[table_name].is_running) {
    return ResponseFormatter::FormatError("SYNC already in progress for table '" + table_name + "'");
  }

  // Check memory health before starting
  auto memory_health = utils::GetMemoryHealthStatus();
  if (memory_health == utils::MemoryHealthStatus::CRITICAL) {
    return ResponseFormatter::FormatError("Memory critically low. Cannot start SYNC. Check system memory.");
  }

  // Mark table as syncing
  {
    std::lock_guard<std::mutex> sync_lock(syncing_tables_mutex_);
    syncing_tables_.insert(table_name);
  }

  // Initialize state
  sync_states_[table_name].is_running = true;
  sync_states_[table_name].status = "STARTING";
  sync_states_[table_name].table_name = table_name;
  sync_states_[table_name].processed_rows = 0;
  sync_states_[table_name].error_message.clear();

  // Launch async snapshot build
  std::thread([this, table_name]() { BuildSnapshotAsync(table_name); }).detach();

  return "OK SYNC STARTED table=" + table_name + " job_id=1";
}

std::string TcpServer::GetSyncStatus() {
  std::lock_guard<std::mutex> lock(sync_mutex_);

  std::ostringstream oss;
  bool any_active = false;

  for (const auto& [table_name, state] : sync_states_) {
    if (!state.is_running && state.status.empty()) {
      continue;  // Skip uninitialized states
    }

    any_active = true;

    oss << "table=" << table_name << " status=" << state.status;

    if (state.status == "IN_PROGRESS") {
      uint64_t processed = state.processed_rows.load();
      double elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - state.start_time).count();
      double rate = elapsed > 0 ? static_cast<double>(processed) / elapsed : 0.0;

      if (state.total_rows > 0) {
        double percent = (100.0 * static_cast<double>(processed)) / static_cast<double>(state.total_rows);
        oss << " progress=" << processed << "/" << state.total_rows << " rows (" << std::fixed << std::setprecision(1)
            << percent << "%)";
      } else {
        oss << " progress=" << processed << " rows";
      }

      oss << " rate=" << std::fixed << std::setprecision(0) << rate << " rows/s";
    } else if (state.status == "COMPLETED") {
      uint64_t processed = state.processed_rows.load();
      double elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - state.start_time).count();

      oss << " rows=" << processed << " time=" << std::fixed << std::setprecision(1) << elapsed << "s";

      if (!state.gtid.empty()) {
        oss << " gtid=" << state.gtid;
      }

      oss << " replication=" << state.replication_status;
    } else if (state.status == "FAILED") {
      uint64_t processed = state.processed_rows.load();
      oss << " rows=" << processed << " error=\"" << state.error_message << "\"";
    } else if (state.status == "CANCELLED") {
      oss << " error=\"" << state.error_message << "\"";
    }

    oss << "\n";
  }

  if (!any_active) {
    return "status=IDLE message=\"No sync operation performed\"";
  }

  std::string result = oss.str();
  if (!result.empty() && result.back() == '\n') {
    result.pop_back();  // Remove trailing newline
  }
  return result;
}

void TcpServer::BuildSnapshotAsync(const std::string& table_name) {
  auto& state = sync_states_[table_name];
  state.status = "IN_PROGRESS";
  state.start_time = std::chrono::steady_clock::now();

  // RAII guard to ensure cleanup even on exceptions
  struct SyncGuard {
    TcpServer* server;
    std::string table;
    explicit SyncGuard(TcpServer* srv, std::string tbl) : server(srv), table(std::move(tbl)) {}
    ~SyncGuard() {
      // Remove from syncing set
      std::lock_guard<std::mutex> lock(server->syncing_tables_mutex_);
      server->syncing_tables_.erase(table);
    }
    // Delete copy and move operations - this is an RAII guard
    SyncGuard(const SyncGuard&) = delete;
    SyncGuard& operator=(const SyncGuard&) = delete;
    SyncGuard(SyncGuard&&) = delete;
    SyncGuard& operator=(SyncGuard&&) = delete;
  };
  SyncGuard guard(this, table_name);

  try {
    // Create MySQL connection
    if (full_config_ == nullptr) {
      state.status = "FAILED";
      state.error_message = "Configuration not available";
      state.is_running = false;
      spdlog::error("SYNC failed for table {}: Configuration not available", table_name);
      return;
    }

    mysql::Connection::Config mysql_config{.host = full_config_->mysql.host,
                                           .port = static_cast<uint16_t>(full_config_->mysql.port),
                                           .user = full_config_->mysql.user,
                                           .password = full_config_->mysql.password,
                                           .database = full_config_->mysql.database};

    auto mysql_conn = std::make_unique<mysql::Connection>(mysql_config);

    if (!mysql_conn->Connect()) {
      state.status = "FAILED";
      state.error_message = "Failed to connect to MySQL: " + mysql_conn->GetLastError();
      state.is_running = false;
      spdlog::error("SYNC failed for table {}: {}", table_name, state.error_message);
      return;
    }

    // Get table context
    auto table_iter = table_contexts_.find(table_name);
    if (table_iter == table_contexts_.end()) {
      state.status = "FAILED";
      state.error_message = "Table context not found";
      state.is_running = false;
      return;
    }

    auto* ctx = table_iter->second;

    // Build snapshot with cancellation support
    storage::SnapshotBuilder builder(*mysql_conn, *ctx->index, *ctx->doc_store, ctx->config, full_config_->build);

    // Store builder pointer for shutdown cancellation
    {
      std::lock_guard<std::mutex> lock(snapshot_builders_mutex_);
      active_snapshot_builders_[table_name] = &builder;
    }

    bool success = builder.Build([&](const auto& progress) {
      state.total_rows = progress.total_rows;
      state.processed_rows = progress.processed_rows;

      // Check for shutdown signal
      if (shutdown_requested_) {
        builder.Cancel();
      }
    });

    // Clear builder pointer
    {
      std::lock_guard<std::mutex> lock(snapshot_builders_mutex_);
      active_snapshot_builders_.erase(table_name);
    }

    // Check if cancelled by shutdown
    if (shutdown_requested_) {
      state.status = "CANCELLED";
      state.error_message = "Server shutdown requested";
      state.is_running = false;
      spdlog::info("SYNC cancelled for table {} due to shutdown", table_name);
      return;
    }

    if (success) {
      state.status = "COMPLETED";
      state.gtid = builder.GetSnapshotGTID();
      state.processed_rows = builder.GetProcessedRows();

      // Start or restart binlog replication if enabled and BinlogReader exists
      if (full_config_->replication.enable && binlog_reader_ != nullptr && !state.gtid.empty()) {
        auto* reader = static_cast<mysql::BinlogReader*>(binlog_reader_);

        // Check if already running
        if (reader->IsRunning()) {
          // Already running - just update GTID and mark as already running
          spdlog::info("SYNC completed for table {} (rows={}, gtid={}). Replication already running.", table_name,
                       state.processed_rows.load(), state.gtid);
          state.replication_status = "ALREADY_RUNNING";
        } else {
          // Not running - set GTID and start replication
          reader->SetCurrentGTID(state.gtid);

          if (reader->Start()) {
            state.replication_status = "STARTED";
            spdlog::info("SYNC completed for table {} (rows={}, gtid={}). Replication started.", table_name,
                         state.processed_rows.load(), state.gtid);
          } else {
            state.replication_status = "FAILED";
            state.error_message = "Snapshot succeeded but replication failed to start: " + reader->GetLastError();
            spdlog::error("SYNC completed for table {} but replication failed to start: {}", table_name,
                          reader->GetLastError());
          }
        }
      } else {
        state.replication_status = "DISABLED";
        spdlog::info("SYNC completed for table {} (rows={}, replication disabled)", table_name,
                     state.processed_rows.load());
      }
    } else {
      state.status = "FAILED";
      state.error_message = builder.GetLastError();
      spdlog::error("SYNC failed for table {}: {}", table_name, state.error_message);
    }

  } catch (const std::exception& e) {
    state.status = "FAILED";
    state.error_message = e.what();
    spdlog::error("SYNC exception for table {}: {}", table_name, e.what());
  }

  state.is_running = false;
}

#endif  // USE_MYSQL

}  // namespace mygramdb::server
