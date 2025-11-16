/**
 * @file connection_acceptor.cpp
 * @brief Implementation of ConnectionAcceptor
 */

#include "server/connection_acceptor.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <spdlog/spdlog.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstring>

#include "server/server_types.h"
#include "server/thread_pool.h"
#include "utils/network_utils.h"

namespace mygramdb::server {

namespace {
/**
 * @brief Helper to safely cast sockaddr_in* to sockaddr* for socket API
 *
 * POSIX socket API requires sockaddr* but we use sockaddr_in for IPv4.
 * This helper centralizes the required reinterpret_cast to a single location.
 */
inline struct sockaddr* ToSockaddr(struct sockaddr_in* addr) {
  return reinterpret_cast<struct sockaddr*>(addr);  // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
}
}  // namespace

ConnectionAcceptor::ConnectionAcceptor(ServerConfig config, ThreadPool* thread_pool)
    : config_(std::move(config)), thread_pool_(thread_pool) {
  if (thread_pool_ == nullptr) {
    spdlog::error("ConnectionAcceptor: thread_pool cannot be null");
  }
}

ConnectionAcceptor::~ConnectionAcceptor() {
  Stop();
}

bool ConnectionAcceptor::Start() {
  if (running_) {
    last_error_ = "ConnectionAcceptor already running";
    return false;
  }

  // Create socket
  server_fd_ = socket(AF_INET, SOCK_STREAM, 0);
  if (server_fd_ < 0) {
    last_error_ = "Failed to create socket: " + std::string(strerror(errno));
    spdlog::error("{}", last_error_);
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
  address.sin_port = htons(config_.port);

  // Determine bind address (default to 0.0.0.0 for backward compatibility)
  in_addr bind_addr{};
  if (config_.host.empty() || config_.host == "0.0.0.0") {
    bind_addr.s_addr = INADDR_ANY;
  } else {
    if (inet_pton(AF_INET, config_.host.c_str(), &bind_addr) != 1) {
      last_error_ = "Invalid bind address: " + config_.host;
      spdlog::error("{}", last_error_);
      close(server_fd_);
      server_fd_ = -1;
      return false;
    }
  }
  address.sin_addr = bind_addr;

  if (bind(server_fd_, ToSockaddr(&address), sizeof(address)) < 0) {
    last_error_ = "Failed to bind to port " + std::to_string(config_.port) + ": " + std::string(strerror(errno));
    spdlog::error("{}", last_error_);
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
    spdlog::error("{}", last_error_);
    close(server_fd_);
    server_fd_ = -1;
    return false;
  }

  should_stop_ = false;
  running_ = true;

  // Start accept thread
  accept_thread_ = std::make_unique<std::thread>(&ConnectionAcceptor::AcceptLoop, this);

  spdlog::info("ConnectionAcceptor listening on {}:{}", config_.host, actual_port_);
  return true;
}

void ConnectionAcceptor::Stop() {
  if (!running_) {
    return;
  }

  spdlog::info("Stopping ConnectionAcceptor...");
  should_stop_ = true;
  running_ = false;

  // Close server socket to unblock accept()
  if (server_fd_ >= 0) {
    shutdown(server_fd_, SHUT_RDWR);
    close(server_fd_);
    server_fd_ = -1;
  }

  // Wait for accept thread to finish
  if (accept_thread_ && accept_thread_->joinable()) {
    accept_thread_->join();
  }

  // Close all active connections
  {
    std::lock_guard<std::mutex> lock(fds_mutex_);
    for (int socket_fd : active_fds_) {
      // Shutdown socket to unblock recv/send calls in other threads
      shutdown(socket_fd, SHUT_RDWR);
      close(socket_fd);
    }
    active_fds_.clear();
  }

  spdlog::info("ConnectionAcceptor stopped");
}

void ConnectionAcceptor::SetConnectionHandler(ConnectionHandler handler) {
  connection_handler_ = std::move(handler);
}

void ConnectionAcceptor::AcceptLoop() {
  spdlog::info("Accept loop started");

  while (!should_stop_) {
    struct sockaddr_in client_addr = {};
    socklen_t client_len = sizeof(client_addr);

    int client_fd = accept(server_fd_, ToSockaddr(&client_addr), &client_len);
    if (client_fd < 0) {
      if (!should_stop_) {
        spdlog::error("Accept failed: {}", strerror(errno));
      } else {
        spdlog::debug("Accept interrupted (shutdown in progress)");
      }
      continue;
    }

    // SECURITY: Check connection limit BEFORE any processing to prevent resource exhaustion
    {
      std::lock_guard<std::mutex> lock(fds_mutex_);
      if (static_cast<int>(active_fds_.size()) >= config_.max_connections) {
        spdlog::warn("Connection limit reached ({}/{}), rejecting new connection", active_fds_.size(),
                     config_.max_connections);
        close(client_fd);
        continue;
      }
    }

    // Convert client IP to string for ACL checks
    std::string client_ip;
    // C-style array required by POSIX inet_ntop API
    // NOLINTNEXTLINE(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
    char ip_buffer[INET_ADDRSTRLEN] = {};
    // Array-to-pointer decay required by POSIX inet_ntop API
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
    if (inet_ntop(AF_INET, &client_addr.sin_addr, ip_buffer, sizeof(ip_buffer)) != nullptr) {
      // Array-to-pointer decay required by std::string::assign
      // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
      client_ip.assign(ip_buffer);
    } else {
      spdlog::warn("Failed to parse client address while accepting connection");
    }

    if (!utils::IsIPAllowed(client_ip, config_.parsed_allow_cidrs)) {
      spdlog::warn("Rejected TCP connection from {} (not in network.allow_cidrs)",
                   client_ip.empty() ? "<unknown>" : client_ip);
      close(client_fd);
      continue;
    }

    // Set receive timeout to avoid blocking indefinitely
    struct timeval timeout {};
    timeout.tv_sec = 1;  // 1 second timeout (short for quick shutdown)
    timeout.tv_usec = 0;
    if (setsockopt(client_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) < 0) {
      spdlog::warn("Failed to set SO_RCVTIMEO: {}", strerror(errno));
    }

    // Track connection
    {
      std::lock_guard<std::mutex> lock(fds_mutex_);
      active_fds_.insert(client_fd);
    }

    // Submit to thread pool
    if (thread_pool_ != nullptr && connection_handler_) {
      bool submitted = thread_pool_->Submit([this, client_fd]() {
        connection_handler_(client_fd);
        RemoveConnection(client_fd);
      });

      if (!submitted) {
        // Queue is full - reject connection to prevent FD leak
        spdlog::warn("Thread pool queue full, rejecting connection (fd={})", client_fd);
        close(client_fd);
        RemoveConnection(client_fd);
      }
    } else {
      spdlog::error("No connection handler or thread pool configured");
      close(client_fd);
      RemoveConnection(client_fd);
    }
  }

  spdlog::info("Accept loop exited");
}

bool ConnectionAcceptor::SetSocketOptions(int socket_fd) {
  // SO_REUSEADDR: Allow reuse of local addresses
  int opt = 1;
  if (setsockopt(socket_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
    last_error_ = "Failed to set SO_REUSEADDR: " + std::string(strerror(errno));
    spdlog::error("{}", last_error_);
    return false;
  }

  // SO_KEEPALIVE: Enable TCP keepalive
  if (setsockopt(socket_fd, SOL_SOCKET, SO_KEEPALIVE, &opt, sizeof(opt)) < 0) {
    last_error_ = "Failed to set SO_KEEPALIVE: " + std::string(strerror(errno));
    spdlog::error("{}", last_error_);
    return false;
  }

  // SO_RCVBUF: Set receive buffer size
  int rcvbuf = config_.recv_buffer_size;
  if (setsockopt(socket_fd, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf)) < 0) {
    spdlog::warn("Failed to set SO_RCVBUF: {}", strerror(errno));
    // Non-fatal, continue
  }

  // SO_SNDBUF: Set send buffer size
  int sndbuf = config_.send_buffer_size;
  if (setsockopt(socket_fd, SOL_SOCKET, SO_SNDBUF, &sndbuf, sizeof(sndbuf)) < 0) {
    spdlog::warn("Failed to set SO_SNDBUF: {}", strerror(errno));
    // Non-fatal, continue
  }

  return true;
}

void ConnectionAcceptor::RemoveConnection(int socket_fd) {
  std::lock_guard<std::mutex> lock(fds_mutex_);
  active_fds_.erase(socket_fd);
}

}  // namespace mygramdb::server
