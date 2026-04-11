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
   * @brief Connection handler callback type (blocking I/O model).
   *
   * Invoked for each accepted connection when `SetConnectionHandler` has been
   * installed. The handler runs on a thread pool worker and is responsible for
   * processing the connection and closing the file descriptor.
   */
  using ConnectionHandler = std::function<void(int client_fd)>;

  /**
   * @brief Reactor handler callback type (reactor I/O model).
   *
   * Invoked **inline** on the accept thread for each accepted connection when
   * `SetReactorHandler` has been installed. The handler must take ownership of
   * `client_fd` and return true, or return false to reject the connection
   * (the acceptor will then emit `ERR SERVER_BUSY` and close the fd).
   *
   * No thread pool hop: the reactor's `IoReactor::Register` is cheap (map
   * insert + one epoll_ctl/kevent) and latency-sensitive, so bouncing through
   * a worker would add a context switch for no gain.
   *
   * When a reactor handler is installed, the blocking `ConnectionHandler` path
   * is NOT used even if it was also set.
   */
  using ReactorHandler = std::function<bool(int client_fd)>;

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
   * @brief Set connection handler callback (blocking model).
   * @param handler Callback to handle accepted connections
   */
  void SetConnectionHandler(ConnectionHandler handler);

  /**
   * @brief Set reactor handler callback (reactor model).
   *
   * Installing a reactor handler switches the accept path from "submit to
   * thread pool" to "hand off to reactor inline". Used in conjunction with
   * `api.tcp.io_model = "reactor"`.
   *
   * @param handler Callback that takes ownership of the fd on true return.
   */
  void SetReactorHandler(ReactorHandler handler);

  /**
   * @brief Remove a connection from the active set.
   *
   * Exposed publicly so that `IoReactor`'s close callback can decrement the
   * `max_connections` gate when it tears down a reactor connection. Safe to
   * call from any thread.
   */
  void RemoveConnection(int socket_fd);

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

  /**
   * @brief Check if this acceptor uses Unix domain socket
   * @return true if unix socket mode
   */
  bool IsUnixSocket() const { return !unix_socket_path_.empty(); }

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

  ServerConfig config_;
  ThreadPool* thread_pool_;
  ConnectionHandler connection_handler_;
  ReactorHandler reactor_handler_;

  int server_fd_ = -1;
  uint16_t actual_port_ = 0;
  std::atomic<bool> running_{false};
  std::atomic<bool> should_stop_{false};
  std::unique_ptr<std::thread> accept_thread_;

  std::set<int> active_fds_;
  std::mutex fds_mutex_;
  std::string unix_socket_path_;  // Non-empty when UDS mode, used for unlink on Stop
};

}  // namespace mygramdb::server
