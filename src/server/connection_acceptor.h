/**
 * @file connection_acceptor.h
 * @brief Network connection acceptor
 */

#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>

#include "server/server_types.h"
#include "utils/error.h"
#include "utils/expected.h"

namespace mygramdb::server {

// Forward declarations
class ThreadPool;

/**
 * @brief Network connection acceptor
 *
 * This class handles socket creation, accept loop, and connection dispatch.
 * It is isolated from application logic for independent testing.
 *
 * Key responsibilities:
 * - Create and configure server socket
 * - Accept incoming connections
 * - Dispatch connections to thread pool
 * - Track active connections
 * - Handle graceful shutdown
 *
 * Design principles:
 * - Single responsibility: network I/O only
 * - Testable without real network (can mock handler)
 * - Reusable for HTTP server, gRPC, etc.
 * - Thread-safe connection tracking
 */
class ConnectionAcceptor {
 public:
  /**
   * @brief Connection handler callback type
   *
   * This callback is invoked for each accepted connection.
   * The handler should process the connection and close the file descriptor.
   */
  using ConnectionHandler = std::function<void(int client_fd)>;

  /**
   * @brief Construct a ConnectionAcceptor
   * @param config Server configuration
   * @param thread_pool Thread pool for connection handling
   */
  ConnectionAcceptor(ServerConfig config, ThreadPool* thread_pool);

  // Disable copy and move
  ConnectionAcceptor(const ConnectionAcceptor&) = delete;
  ConnectionAcceptor& operator=(const ConnectionAcceptor&) = delete;
  ConnectionAcceptor(ConnectionAcceptor&&) = delete;
  ConnectionAcceptor& operator=(ConnectionAcceptor&&) = delete;

  ~ConnectionAcceptor();

  /**
   * @brief Start accepting connections
   * @return Expected<void, Error> - Success or error details
   */
  mygram::utils::Expected<void, mygram::utils::Error> Start();

  /**
   * @brief Stop accepting connections
   *
   * Stops the accept loop and closes all active connections.
   */
  void Stop();

  /**
   * @brief Set connection handler callback
   * @param handler Callback to handle accepted connections
   */
  void SetConnectionHandler(ConnectionHandler handler);

  /**
   * @brief Get actual port being listened on
   * @return Port number (useful when config.port = 0)
   */
  uint16_t GetPort() const { return actual_port_; }

  /**
   * @brief Check if acceptor is running
   * @return true if accepting connections
   */
  bool IsRunning() const { return running_; }

 private:
  /**
   * @brief Accept loop (runs in separate thread)
   */
  void AcceptLoop();

  /**
   * @brief Set socket options (SO_REUSEADDR, SO_KEEPALIVE, etc.)
   * @param socket_fd Socket file descriptor
   * @return true if successful
   */
  bool SetSocketOptions(int socket_fd) const;

  /**
   * @brief Remove connection from active list
   * @param socket_fd Socket file descriptor
   */
  void RemoveConnection(int socket_fd);

  ServerConfig config_;
  ThreadPool* thread_pool_;
  ConnectionHandler connection_handler_;

  int server_fd_ = -1;
  uint16_t actual_port_ = 0;
  std::atomic<bool> running_{false};
  std::atomic<bool> should_stop_{false};
  std::unique_ptr<std::thread> accept_thread_;

  std::set<int> active_fds_;
  std::mutex fds_mutex_;
};

}  // namespace mygramdb::server
