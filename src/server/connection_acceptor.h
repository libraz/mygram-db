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
   * @brief Reactor handler callback type.
   *
   * Invoked **inline** on the accept thread for each accepted connection when
   * `SetReactorHandler` has been installed. The handler must take ownership of
   * `client_fd` and return true, or return false to reject the connection
   * (the acceptor will then emit `ERR SERVER_BUSY` and close the fd).
   *
   * No thread pool hop: the reactor's `IoReactor::Register` is cheap (map
   * insert + one epoll_ctl/kevent) and latency-sensitive, so bouncing through
   * a worker would add a context switch for no gain.
   */
  using ReactorHandler = std::function<bool(int client_fd)>;

  /**
   * @brief Construct a ConnectionAcceptor
   * @param config Server configuration
   */
  explicit ConnectionAcceptor(ServerConfig config);

  // Disable copy and move
  ConnectionAcceptor(const ConnectionAcceptor&) = delete;
  ConnectionAcceptor& operator=(const ConnectionAcceptor&) = delete;
  ConnectionAcceptor(ConnectionAcceptor&&) = delete;
  ConnectionAcceptor& operator=(ConnectionAcceptor&&) = delete;

  ~ConnectionAcceptor();

  /**
   * @brief Bind/listen on the configured endpoint.
   *
   * Creates the listening socket, applies socket options, binds, and listens.
   * The accept loop thread is **not** started here — the caller must invoke
   * `StartAccepting()` after `SetReactorHandler()` is wired up. This split
   * exists to fix a data race on `reactor_handler_`: previously the accept
   * thread could read the handler before the embedder finished writing it.
   *
   * After a successful return, `IsRunning()` reports true and `GetPort()`
   * reports the actual bound port. Calling `Start()` again before `Stop()`
   * returns `kNetworkAlreadyRunning`.
   *
   * @return Expected<void, Error> - Success or error details
   */
  mygram::utils::Expected<void, mygram::utils::Error> Start();

  /**
   * @brief Spawn the accept loop thread.
   *
   * Preconditions:
   *  - `Start()` has been called and returned success (i.e. `IsRunning()`)
   *  - `SetReactorHandler()` has been called with a non-null handler
   *
   * The handler must already be installed at this point so the accept thread
   * observes a fully-published `std::function` via the thread-creation
   * happens-before edge. Calling this twice on the same instance returns
   * `kNetworkAlreadyRunning`.
   *
   * @return Expected<void, Error> - Success or error details
   */
  mygram::utils::Expected<void, mygram::utils::Error> StartAccepting();

  /**
   * @brief Stop accepting connections
   *
   * Stops the accept loop and closes all active connections.
   */
  void Stop();

  /**
   * @brief Set reactor handler callback.
   *
   * Must be called **before** `StartAccepting()`. The handler is then invoked
   * inline on the accept thread and must take ownership of the fd on true
   * return. This setter is not thread-safe by design: the contract is that
   * the embedder publishes the handler before spawning the accept thread.
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
  bool IsRunning() const { return running_.load(std::memory_order_acquire); }

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
   * @brief Set socket options on the listening socket (SO_REUSEADDR, SO_KEEPALIVE)
   * @param socket_fd Socket file descriptor
   * @return true if successful
   */
  bool SetSocketOptions(int socket_fd) const;

  /**
   * @brief Set per-client socket options (SO_RCVBUF, SO_SNDBUF)
   *
   * Called after accept() returns a valid client fd. These options are not
   * inherited from the listening socket by the kernel.
   *
   * @param client_fd Client socket file descriptor
   */
  void SetClientSocketOptions(int client_fd) const;

  ServerConfig config_;
  ReactorHandler reactor_handler_;

  // The listening socket fd. Atomic because Stop() (called from any thread)
  // closes the fd and resets it to -1 to unblock the accept loop, which is
  // concurrently reading the same field on the accept thread. A plain int
  // would race under the C++ memory model.
  std::atomic<int> server_fd_{-1};
  uint16_t actual_port_ = 0;
  std::atomic<bool> running_{false};
  std::atomic<bool> should_stop_{false};
  std::unique_ptr<std::thread> accept_thread_;

  std::set<int> active_fds_;
  std::mutex fds_mutex_;
  std::string unix_socket_path_;  // Non-empty when UDS mode, used for unlink on Stop
};

}  // namespace mygramdb::server
