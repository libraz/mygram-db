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
#include "server/handlers/debug_handler.h"
#include "server/handlers/document_handler.h"
#include "server/handlers/dump_handler.h"
#include "server/handlers/replication_handler.h"
#include "server/handlers/search_handler.h"
#include "server/response_formatter.h"
#include "storage/dump_format_v1.h"
#include "utils/network_utils.h"
#include "utils/string_utils.h"
#include "version.h"

#ifdef USE_MYSQL
#include "mysql/binlog_reader.h"
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
      table_contexts_(std::move(table_contexts)),
      dump_dir_(std::move(dump_dir)),
      full_config_(full_config),
      binlog_reader_(binlog_reader) {
  // Create thread pool
  thread_pool_ =
      std::make_unique<ThreadPool>(config_.worker_threads > 0 ? config_.worker_threads : 0, kThreadPoolQueueSize);

  // Initialize command handler context (must outlive handlers)
  handler_context_ = std::make_unique<HandlerContext>(HandlerContext{
      .table_contexts = table_contexts_,
      .stats = stats_,
      .full_config = full_config_,
      .dump_dir = dump_dir_,
      .loading = loading_,
      .read_only = read_only_,
      .binlog_reader = binlog_reader_,
  });

  // Initialize command handlers
  search_handler_ = std::make_unique<SearchHandler>(*handler_context_);
  document_handler_ = std::make_unique<DocumentHandler>(*handler_context_);
  dump_handler_ = std::make_unique<DumpHandler>(*handler_context_);
  admin_handler_ = std::make_unique<AdminHandler>(*handler_context_);
  replication_handler_ = std::make_unique<ReplicationHandler>(*handler_context_);
  debug_handler_ = std::make_unique<DebugHandler>(*handler_context_);
}

TcpServer::~TcpServer() {
  Stop();
}

bool TcpServer::Start() {
  if (running_) {
    last_error_ = "Server already running";
    return false;
  }

  // Create socket
  server_fd_ = socket(AF_INET, SOCK_STREAM, 0);
  if (server_fd_ < 0) {
    last_error_ = "Failed to create socket: " + std::string(strerror(errno));
    spdlog::error(last_error_);
    return false;
  }

  // Set socket options
  if (!SetSocketOptions(server_fd_)) {
    close(server_fd_);
    server_fd_ = -1;
    return false;
  }

  // Bind
  struct sockaddr_in address = {};
  std::memset(&address, 0, sizeof(address));
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = INADDR_ANY;
  address.sin_port = htons(config_.port);

  if (bind(server_fd_, ToSockaddr(&address), sizeof(address)) < 0) {
    last_error_ = "Failed to bind to port " + std::to_string(config_.port) + ": " + std::string(strerror(errno));
    spdlog::error(last_error_);
    close(server_fd_);
    server_fd_ = -1;
    return false;
  }

  // Get actual port if port 0 was specified
  if (config_.port == 0) {
    socklen_t addr_len = sizeof(address);
    if (getsockname(server_fd_, ToSockaddr(&address), &addr_len) == 0) {
      actual_port_ = ntohs(address.sin_port);
    }
  } else {
    actual_port_ = config_.port;
  }

  // Listen
  if (listen(server_fd_, config_.max_connections) < 0) {
    last_error_ = "Failed to listen: " + std::string(strerror(errno));
    spdlog::error(last_error_);
    close(server_fd_);
    server_fd_ = -1;
    return false;
  }

  should_stop_ = false;
  running_ = true;

  // Start accept thread
  accept_thread_ = std::make_unique<std::thread>(&TcpServer::AcceptThreadFunc, this);

  // Start auto-save thread if interval is configured
  if (full_config_ != nullptr && full_config_->dump.interval_sec > 0) {
    StartAutoSave();
  }

  spdlog::info("TCP server started on {}:{}", config_.host, actual_port_);
  return true;
}

void TcpServer::Stop() {
  if (!running_) {
    return;
  }

  spdlog::info("Stopping TCP server...");
  should_stop_ = true;

  // Stop auto-save thread
  StopAutoSave();

  // Close server socket to unblock accept()
  if (server_fd_ >= 0) {
    shutdown(server_fd_, SHUT_RDWR);
    close(server_fd_);
    server_fd_ = -1;
  }

  // Wait for accept thread
  if (accept_thread_ && accept_thread_->joinable()) {
    accept_thread_->join();
  }

  // Shutdown thread pool (completes pending tasks)
  if (thread_pool_) {
    thread_pool_->Shutdown();
  }

  // Close all active connections
  {
    std::scoped_lock lock(connections_mutex_);
    for (int file_descriptor : connection_fds_) {
      shutdown(file_descriptor, SHUT_RDWR);
      close(file_descriptor);
    }
    connection_fds_.clear();
  }

  running_ = false;
  spdlog::info("TCP server stopped. Handled {} total requests", stats_.GetTotalRequests());
}

void TcpServer::AcceptThreadFunc() {
  spdlog::info("Accept thread started");

  while (!should_stop_) {
    struct sockaddr_in client_addr = {};
    socklen_t client_len = sizeof(client_addr);

    int client_fd = accept(server_fd_, ToSockaddr(&client_addr), &client_len);

    if (client_fd < 0) {
      if (should_stop_) {
        break;
      }
      spdlog::warn("Accept failed: {}", strerror(errno));
      continue;
    }

    // Get client IP address
    // NOLINTNEXTLINE(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
    char client_ip_str[INET_ADDRSTRLEN];
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
    inet_ntop(AF_INET, &client_addr.sin_addr, client_ip_str, INET_ADDRSTRLEN);

    // Check CIDR allow list
    // NOLINTNEXTLINE(readability-implicit-bool-conversion)
    if (full_config_ && !full_config_->network.allow_cidrs.empty()) {
      // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
      if (!utils::IsIPAllowed(client_ip_str, full_config_->network.allow_cidrs)) {
        spdlog::warn("Connection from {} rejected (not in allow_cidrs)", client_ip_str);
        shutdown(client_fd, SHUT_RDWR);
        close(client_fd);
        continue;
      }
    }

    // Check connection limit
    size_t current_connections = stats_.GetActiveConnections();
    if (current_connections >= static_cast<size_t>(config_.max_connections)) {
      spdlog::warn("Connection limit reached ({}), rejecting new connection", config_.max_connections);
      shutdown(client_fd, SHUT_RDWR);
      close(client_fd);
      continue;
    }

    // Increment active connection count and total connection counter
    stats_.IncrementConnections();
    stats_.IncrementTotalConnections();

    // Add to connection set
    {
      std::scoped_lock lock(connections_mutex_);
      connection_fds_.insert(client_fd);
    }

    // Submit to thread pool
    bool submitted = thread_pool_->Submit([this, client_fd]() { HandleClient(client_fd); });

    if (!submitted) {
      spdlog::warn("Thread pool queue full, rejecting connection");
      RemoveConnection(client_fd);
      shutdown(client_fd, SHUT_RDWR);
      close(client_fd);
      stats_.DecrementConnections();
      continue;
    }

    spdlog::debug("Accepted connection from {}:{} (active: {})", inet_ntoa(client_addr.sin_addr),
                  ntohs(client_addr.sin_port), stats_.GetActiveConnections());
  }

  spdlog::info("Accept thread stopped");
}

void TcpServer::HandleClient(int client_fd) {
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

  while (!should_stop_) {
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

      // Process request
      std::string response = ProcessRequest(request, ctx);
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
  RemoveConnection(client_fd);
  {
    std::scoped_lock<std::mutex> lock(contexts_mutex_);
    connection_contexts_.erase(client_fd);
  }
  close(client_fd);
  stats_.DecrementConnections();

  spdlog::debug("Connection closed (active: {})", stats_.GetActiveConnections());
}

std::string TcpServer::ProcessRequest(const std::string& request, ConnectionContext& ctx) {
  spdlog::debug("Processing request: {}", request);

  // Parse query
  auto query = query_parser_.Parse(request);

  if (!query.IsValid()) {
    return ResponseFormatter::FormatError(query_parser_.GetError());
  }

  // Apply configured default LIMIT if not explicitly specified
  if (!query.limit_explicit && (query.type == query::QueryType::SEARCH)) {
    query.limit = static_cast<uint32_t>(config_.default_limit);
  }

  // Increment command statistics
  stats_.IncrementCommand(query.type);

  // Lookup table from query (for commands that don't use handlers yet)
  index::Index* current_index = nullptr;
  storage::DocumentStore* current_doc_store = nullptr;
  int current_ngram_size = 0;
  int current_kanji_ngram_size = 0;

  // For queries that require a table, validate table exists
  if (!query.table.empty()) {
    auto table_iter = table_contexts_.find(query.table);
    if (table_iter == table_contexts_.end()) {
      return ResponseFormatter::FormatError("Table not found: " + query.table);
    }
    // Note: Each handler fetches its own context via HandlerContext
  }

  try {
    switch (query.type) {
      // Search commands
      case query::QueryType::SEARCH:
      case query::QueryType::COUNT:
        return search_handler_->Handle(query, ctx);

      // Document commands
      case query::QueryType::GET:
        return document_handler_->Handle(query, ctx);

      // Snapshot commands
      case query::QueryType::DUMP_SAVE:
      case query::QueryType::DUMP_LOAD:
      case query::QueryType::DUMP_VERIFY:
      case query::QueryType::DUMP_INFO:
        return dump_handler_->Handle(query, ctx);

      // Admin commands
      case query::QueryType::INFO:
      case query::QueryType::CONFIG:
        return admin_handler_->Handle(query, ctx);

      // Replication commands
      case query::QueryType::REPLICATION_STATUS:
      case query::QueryType::REPLICATION_STOP:
      case query::QueryType::REPLICATION_START:
        return replication_handler_->Handle(query, ctx);

      // Debug commands
      case query::QueryType::DEBUG_ON:
      case query::QueryType::DEBUG_OFF:
      case query::QueryType::OPTIMIZE:
        return debug_handler_->Handle(query, ctx);

      default:
        return ResponseFormatter::FormatError("Unknown query type");
    }
  } catch (const std::exception& e) {
    return ResponseFormatter::FormatError(std::string("Exception: ") + e.what());
  }
}

bool TcpServer::SetSocketOptions(int socket_fd) {
  // Reuse address
  int opt = 1;
  if (setsockopt(socket_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
    last_error_ = "Failed to set SO_REUSEADDR: " + std::string(strerror(errno));
    spdlog::error(last_error_);
    return false;
  }

  // Set receive buffer size
  int recv_buf = config_.recv_buffer_size;
  if (setsockopt(socket_fd, SOL_SOCKET, SO_RCVBUF, &recv_buf, sizeof(recv_buf)) < 0) {
    spdlog::warn("Failed to set SO_RCVBUF: {}", strerror(errno));
  }

  // Set send buffer size
  int send_buf = config_.send_buffer_size;
  if (setsockopt(socket_fd, SOL_SOCKET, SO_SNDBUF, &send_buf, sizeof(send_buf)) < 0) {
    spdlog::warn("Failed to set SO_SNDBUF: {}", strerror(errno));
  }

  return true;
}

void TcpServer::RemoveConnection(int socket_fd) {
  std::scoped_lock lock(connections_mutex_);
  connection_fds_.erase(socket_fd);
}

void TcpServer::StartAutoSave() {
  if (auto_save_running_) {
    return;
  }

  if (full_config_ == nullptr || full_config_->dump.interval_sec <= 0) {
    return;
  }

  spdlog::info("Starting auto-save thread (interval: {}s, retain: {})", full_config_->dump.interval_sec,
               full_config_->dump.retain);

  auto_save_running_ = true;
  auto_save_thread_ = std::make_unique<std::thread>(&TcpServer::AutoSaveThread, this);
}

void TcpServer::StopAutoSave() {
  if (!auto_save_running_) {
    return;
  }

  spdlog::info("Stopping auto-save thread...");
  auto_save_running_ = false;

  if (auto_save_thread_ && auto_save_thread_->joinable()) {
    auto_save_thread_->join();
  }

  spdlog::info("Auto-save thread stopped");
}

void TcpServer::AutoSaveThread() {
  const int interval_sec = full_config_->dump.interval_sec;
  const int check_interval_ms = 1000;  // Check for shutdown every second

  spdlog::info("Auto-save thread started");

  // Calculate next save time
  auto next_save_time = std::chrono::steady_clock::now() + std::chrono::seconds(interval_sec);

  while (auto_save_running_) {
    auto now = std::chrono::steady_clock::now();

    // Check if it's time to save
    if (now >= next_save_time) {
      try {
        // Generate timestamp-based filename
        auto timestamp = std::time(nullptr);
        std::ostringstream filename;
        filename << "auto_" << std::put_time(std::localtime(&timestamp), "%Y%m%d_%H%M%S") << ".dmp";

        std::filesystem::path dump_path = std::filesystem::path(dump_dir_) / filename.str();

        spdlog::info("Auto-saving dump to: {}", dump_path.string());

        // Get current GTID
        std::string gtid;
#ifdef USE_MYSQL
        if (binlog_reader_ != nullptr) {
          auto* reader = static_cast<mysql::BinlogReader*>(binlog_reader_);
          gtid = reader->GetCurrentGTID();
        }
#endif

        // Convert table_contexts to format expected by WriteDumpV1
        std::unordered_map<std::string, std::pair<index::Index*, storage::DocumentStore*>> converted_contexts;
        for (const auto& [table_name, table_ctx] : table_contexts_) {
          converted_contexts[table_name] = {table_ctx->index.get(), table_ctx->doc_store.get()};
        }

        // Perform save using dump_v1 API
        bool success = storage::dump_v1::WriteDumpV1(dump_path.string(), gtid, *full_config_, converted_contexts);

        if (success) {
          spdlog::info("Auto-save completed successfully: {}", dump_path.string());

          // Clean up old dumps
          CleanupOldDumps();
        } else {
          spdlog::error("Auto-save failed: {}", dump_path.string());
        }

      } catch (const std::exception& e) {
        spdlog::error("Exception during auto-save: {}", e.what());
      }

      // Schedule next save
      next_save_time = std::chrono::steady_clock::now() + std::chrono::seconds(interval_sec);
    }

    // Sleep for check interval
    std::this_thread::sleep_for(std::chrono::milliseconds(check_interval_ms));
  }

  spdlog::info("Auto-save thread exiting");
}

void TcpServer::CleanupOldDumps() {
  if (full_config_ == nullptr || full_config_->dump.retain <= 0) {
    return;
  }

  try {
    std::filesystem::path dump_path(dump_dir_);

    if (!std::filesystem::exists(dump_path) || !std::filesystem::is_directory(dump_path)) {
      return;
    }

    // Collect all .dmp files with their modification times
    std::vector<std::pair<std::filesystem::path, std::filesystem::file_time_type>> dump_files;

    for (const auto& entry : std::filesystem::directory_iterator(dump_path)) {
      if (entry.is_regular_file() && entry.path().extension() == ".dmp") {
        // Only manage auto-saved files (starting with "auto_")
        if (entry.path().filename().string().rfind("auto_", 0) == 0) {
          dump_files.emplace_back(entry.path(), std::filesystem::last_write_time(entry));
        }
      }
    }

    // Sort by modification time (newest first)
    std::sort(dump_files.begin(), dump_files.end(),
              [](const auto& lhs, const auto& rhs) { return lhs.second > rhs.second; });

    // Delete old files beyond retain count
    const auto retain_count = static_cast<size_t>(full_config_->dump.retain);
    for (size_t i = retain_count; i < dump_files.size(); ++i) {
      spdlog::info("Removing old dump file: {}", dump_files[i].first.string());
      std::filesystem::remove(dump_files[i].first);
    }

  } catch (const std::exception& e) {
    spdlog::error("Exception during dump cleanup: {}", e.what());
  }
}

}  // namespace mygramdb::server
