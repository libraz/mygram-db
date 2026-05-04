/**
 * @file connection_acceptor.cpp
 * @brief Implementation of ConnectionAcceptor
 */

#include "server/connection_acceptor.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <spdlog/spdlog.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>

// macOS-specific: Define SO_NOSIGPIPE if not already defined (include order issues)
#if defined(__APPLE__) && !defined(SO_NOSIGPIPE)
// Macro required: system constant for setsockopt, cannot use constexpr
// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define SO_NOSIGPIPE 0x1022
#endif

#include <chrono>
#include <cstring>
#include <thread>

#include "server/server_types.h"
#include "utils/error.h"
#include "utils/expected.h"
#include "utils/network_utils.h"
#include "utils/structured_log.h"

namespace mygramdb::server {

// ToSockaddr / ToSockaddrUn are provided by utils/network_utils.h
using mygram::utils::ToSockaddr;
using mygram::utils::ToSockaddrUn;

ConnectionAcceptor::ConnectionAcceptor(ServerConfig config) : config_(std::move(config)) {}

ConnectionAcceptor::~ConnectionAcceptor() {
  Stop();
}

mygram::utils::Expected<void, mygram::utils::Error> ConnectionAcceptor::Start() {
  using mygram::utils::ErrorCode;
  using mygram::utils::MakeError;
  using mygram::utils::MakeUnexpected;

  if (running_.load(std::memory_order_acquire)) {
    auto error = MakeError(ErrorCode::kNetworkAlreadyRunning, "ConnectionAcceptor already running");
    mygram::utils::StructuredLog()
        .Event("server_error")
        .Field("operation", "connection_acceptor_start")
        .Field("error", error.to_string())
        .Error();
    return MakeUnexpected(error);
  }

  if (!config_.unix_socket_path.empty()) {
    // === Unix Domain Socket mode ===
    unix_socket_path_ = config_.unix_socket_path;

    // Validate path length
    struct sockaddr_un addr_un {};
    if (config_.unix_socket_path.size() >= sizeof(addr_un.sun_path)) {
      auto error =
          MakeError(ErrorCode::kNetworkUnixSocketPathTooLong, "Unix socket path too long: " + config_.unix_socket_path);
      mygram::utils::StructuredLog()
          .Event("server_error")
          .Field("operation", "unix_socket_path_validate")
          .Field("path", config_.unix_socket_path)
          .Field("error", error.to_string())
          .Error();
      return MakeUnexpected(error);
    }

    // Stale socket detection
    if (access(config_.unix_socket_path.c_str(), F_OK) == 0) {
      int probe_fd = socket(AF_UNIX, SOCK_STREAM, 0);
      if (probe_fd >= 0) {
        struct sockaddr_un probe_addr {};
        probe_addr.sun_family = AF_UNIX;
        // Safe: path length already validated above (< sizeof(sun_path)), struct is zero-initialized
        std::memcpy(probe_addr.sun_path, config_.unix_socket_path.c_str(), config_.unix_socket_path.size());
        if (connect(probe_fd, ToSockaddrUn(&probe_addr), sizeof(probe_addr)) == 0) {
          close(probe_fd);
          auto error = MakeError(ErrorCode::kNetworkUnixSocketStale,
                                 "Another server is already listening on: " + config_.unix_socket_path);
          mygram::utils::StructuredLog()
              .Event("server_error")
              .Field("operation", "unix_socket_stale_check")
              .Field("path", config_.unix_socket_path)
              .Field("error", error.to_string())
              .Error();
          return MakeUnexpected(error);
        }
        close(probe_fd);
        // Stale socket file - remove it
        unlink(config_.unix_socket_path.c_str());
        mygram::utils::StructuredLog()
            .Event("unix_socket_stale_removed")
            .Field("path", config_.unix_socket_path)
            .Info();
      }
    }

    // Create socket
    server_fd_ = socket(AF_UNIX, SOCK_STREAM, 0);
    if (server_fd_ < 0) {
      auto error = MakeError(ErrorCode::kNetworkSocketCreationFailed,
                             "Failed to create unix socket: " + std::string(strerror(errno)));
      mygram::utils::StructuredLog()
          .Event("server_error")
          .Field("operation", "unix_socket_create")
          .Field("error", error.to_string())
          .Error();
      return MakeUnexpected(error);
    }

    // Bind
    struct sockaddr_un bind_addr {};
    bind_addr.sun_family = AF_UNIX;
    // Safe: path length already validated above (< sizeof(sun_path)), struct is zero-initialized
    std::memcpy(bind_addr.sun_path, config_.unix_socket_path.c_str(), config_.unix_socket_path.size());

    if (bind(server_fd_, ToSockaddrUn(&bind_addr), sizeof(bind_addr)) < 0) {
      close(server_fd_);
      server_fd_ = -1;
      auto error = MakeError(ErrorCode::kNetworkBindFailed, "Failed to bind unix socket " + config_.unix_socket_path +
                                                                ": " + std::string(strerror(errno)));
      mygram::utils::StructuredLog()
          .Event("server_error")
          .Field("operation", "unix_socket_bind")
          .Field("path", config_.unix_socket_path)
          .Field("error", error.to_string())
          .Error();
      return MakeUnexpected(error);
    }

    // Set socket permissions (owner + group access)
    if (chmod(config_.unix_socket_path.c_str(), 0770) < 0) {
      close(server_fd_);
      server_fd_ = -1;
      unlink(config_.unix_socket_path.c_str());
      auto error = MakeError(ErrorCode::kNetworkBindFailed,
                             "Failed to set unix socket permissions: " + std::string(strerror(errno)));
      mygram::utils::StructuredLog()
          .Event("server_error")
          .Field("operation", "unix_socket_chmod")
          .Field("path", config_.unix_socket_path)
          .Field("error", error.to_string())
          .Error();
      return MakeUnexpected(error);
    }

    // Listen
    if (listen(server_fd_, config_.max_connections) < 0) {
      close(server_fd_);
      server_fd_ = -1;
      unlink(config_.unix_socket_path.c_str());
      auto error = MakeError(ErrorCode::kNetworkListenFailed,
                             "Failed to listen on unix socket: " + std::string(strerror(errno)));
      mygram::utils::StructuredLog()
          .Event("server_error")
          .Field("operation", "unix_socket_listen")
          .Field("error", error.to_string())
          .Error();
      return MakeUnexpected(error);
    }

    actual_port_ = 0;
    should_stop_.store(false, std::memory_order_relaxed);
    running_.store(true, std::memory_order_release);

    // NOTE: accept loop thread is intentionally NOT started here. The caller
    // must invoke `StartAccepting()` after wiring up `SetReactorHandler()`.
    // See header doc for the data-race rationale.

    mygram::utils::StructuredLog()
        .Event("connection_acceptor_listening")
        .Field("unix_socket", config_.unix_socket_path)
        .Debug();
    return {};
  }

  // === TCP mode (existing code below unchanged) ===

  // Create socket
  server_fd_ = socket(AF_INET, SOCK_STREAM, 0);
  if (server_fd_ < 0) {
    auto error =
        MakeError(ErrorCode::kNetworkSocketCreationFailed, "Failed to create socket: " + std::string(strerror(errno)));
    mygram::utils::StructuredLog()
        .Event("server_error")
        .Field("operation", "socket_create")
        .Field("error", error.to_string())
        .Error();
    return MakeUnexpected(error);
  }

  // Set socket options
  if (!SetSocketOptions(server_fd_)) {
    close(server_fd_);
    server_fd_ = -1;
    auto error = MakeError(ErrorCode::kNetworkSocketCreationFailed, "Failed to set socket options");
    mygram::utils::StructuredLog()
        .Event("server_error")
        .Field("operation", "socket_set_options")
        .Field("error", error.to_string())
        .Error();
    return MakeUnexpected(error);
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
      close(server_fd_);
      server_fd_ = -1;
      auto error = MakeError(ErrorCode::kNetworkInvalidBindAddress, "Invalid bind address: " + config_.host);
      mygram::utils::StructuredLog()
          .Event("server_error")
          .Field("operation", "socket_bind")
          .Field("bind_address", config_.host)
          .Field("error", error.to_string())
          .Error();
      return MakeUnexpected(error);
    }
  }
  address.sin_addr = bind_addr;

  if (bind(server_fd_, ToSockaddr(&address), sizeof(address)) < 0) {
    close(server_fd_);
    server_fd_ = -1;
    auto error = MakeError(ErrorCode::kNetworkBindFailed, "Failed to bind to port " + std::to_string(config_.port) +
                                                              ": " + std::string(strerror(errno)));
    mygram::utils::StructuredLog()
        .Event("server_error")
        .Field("operation", "socket_bind")
        .Field("port", static_cast<uint64_t>(config_.port))
        .Field("error", error.to_string())
        .Error();
    return MakeUnexpected(error);
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
    close(server_fd_);
    server_fd_ = -1;
    auto error = MakeError(ErrorCode::kNetworkListenFailed, "Failed to listen: " + std::string(strerror(errno)));
    mygram::utils::StructuredLog()
        .Event("server_error")
        .Field("operation", "socket_listen")
        .Field("error", error.to_string())
        .Error();
    return MakeUnexpected(error);
  }

  should_stop_.store(false, std::memory_order_relaxed);
  running_.store(true, std::memory_order_release);

  // NOTE: accept loop thread is intentionally NOT started here. The caller
  // must invoke `StartAccepting()` after wiring up `SetReactorHandler()`.
  // See header doc for the data-race rationale.

  mygram::utils::StructuredLog()
      .Event("connection_acceptor_listening")
      .Field("host", config_.host)
      .Field("port", static_cast<uint64_t>(actual_port_))
      .Debug();
  return {};
}

mygram::utils::Expected<void, mygram::utils::Error> ConnectionAcceptor::StartAccepting() {
  using mygram::utils::ErrorCode;
  using mygram::utils::MakeError;
  using mygram::utils::MakeUnexpected;

  // Precondition: Start() must have completed (socket bound + listening).
  if (!running_.load(std::memory_order_acquire)) {
    auto error = MakeError(ErrorCode::kNetworkServerNotStarted,
                           "ConnectionAcceptor::StartAccepting called before Start (no listening socket)");
    mygram::utils::StructuredLog()
        .Event("server_error")
        .Field("operation", "connection_acceptor_start_accepting")
        .Field("error", error.to_string())
        .Error();
    return MakeUnexpected(error);
  }

  // Precondition: caller installed a reactor handler. Reading reactor_handler_
  // here is safe because StartAccepting() is required to be called from the
  // same thread that calls SetReactorHandler() (i.e. the embedder), prior to
  // the accept thread existing.
  if (!reactor_handler_) {
    auto error = MakeError(ErrorCode::kNetworkAcceptorNoHandler,
                           "ConnectionAcceptor::StartAccepting called before SetReactorHandler");
    mygram::utils::StructuredLog()
        .Event("server_error")
        .Field("operation", "connection_acceptor_start_accepting")
        .Field("error", error.to_string())
        .Error();
    return MakeUnexpected(error);
  }

  // Idempotency guard: refuse a second StartAccepting() on the same instance.
  // The accept thread, if started, exists until Stop() joins+resets it.
  if (accept_thread_) {
    auto error = MakeError(ErrorCode::kNetworkAlreadyRunning, "ConnectionAcceptor accept thread already started");
    mygram::utils::StructuredLog()
        .Event("server_error")
        .Field("operation", "connection_acceptor_start_accepting")
        .Field("error", error.to_string())
        .Error();
    return MakeUnexpected(error);
  }

  // The thread constructor synchronizes-with the new thread's first action,
  // so any writes to reactor_handler_ that happened-before this call are
  // visible inside AcceptLoop. This is the actual fix for the prior data
  // race on reactor_handler_.
  accept_thread_ = std::make_unique<std::thread>(&ConnectionAcceptor::AcceptLoop, this);
  return {};
}

void ConnectionAcceptor::Stop() {
  // Use compare_exchange to ensure only one thread performs the stop sequence.
  // Without this, two concurrent Stop() calls could both pass a plain load()
  // check and double-join the thread, causing std::terminate.
  bool expected = true;
  if (!running_.compare_exchange_strong(expected, false)) {
    return;
  }

  mygram::utils::StructuredLog().Event("connection_acceptor_stopping").Debug();
  should_stop_.store(true, std::memory_order_release);

  // Close server socket to unblock accept()
  if (server_fd_ >= 0) {
    shutdown(server_fd_, SHUT_RDWR);
    close(server_fd_);
    server_fd_ = -1;
  }

  // Wait for accept thread to finish (if StartAccepting() ever ran).
  if (accept_thread_ && accept_thread_->joinable()) {
    accept_thread_->join();
  }
  // Reset so a subsequent Start() + StartAccepting() can spin up a fresh
  // thread (and so the StartAccepting idempotency guard doesn't spuriously
  // fire after restart).
  accept_thread_.reset();

  // Remove unix socket file
  if (!unix_socket_path_.empty()) {
    unlink(unix_socket_path_.c_str());
    mygram::utils::StructuredLog().Event("unix_socket_removed").Field("path", unix_socket_path_).Debug();
    unix_socket_path_.clear();
  }

  // active_fds_ is cleared after join() to ensure no accept loop iteration
  // can insert new entries after this point. Do NOT close the fds here: they
  // are owned by the reactor (ReactorConnection), which closes them in its
  // destructor during IoReactor::Stop(). Closing here would cause a
  // double-close race with the reactor's teardown path.
  {
    std::lock_guard<std::mutex> lock(fds_mutex_);
    active_fds_.clear();
  }

  mygram::utils::StructuredLog().Event("connection_acceptor_stopped").Debug();
}

void ConnectionAcceptor::SetReactorHandler(ReactorHandler handler) {
  reactor_handler_ = std::move(handler);
}

void ConnectionAcceptor::AcceptLoop() {
  if (IsUnixSocket()) {
    mygram::utils::StructuredLog().Event("accept_loop_started").Field("unix_socket", unix_socket_path_).Debug();
  } else {
    mygram::utils::StructuredLog()
        .Event("accept_loop_started")
        .Field("host", config_.host)
        .Field("port", static_cast<uint64_t>(actual_port_))
        .Debug();
  }

  while (!should_stop_) {
    int client_fd = -1;
    if (IsUnixSocket()) {
      struct sockaddr_un client_addr_un {};
      socklen_t client_len_un = sizeof(client_addr_un);
      client_fd = accept(server_fd_, ToSockaddrUn(&client_addr_un), &client_len_un);
    } else {
      struct sockaddr_in client_addr {};
      socklen_t client_len = sizeof(client_addr);
      client_fd = accept(server_fd_, ToSockaddr(&client_addr), &client_len);
    }

    if (client_fd < 0) {
      if (should_stop_) {
        break;
      }
      int err = errno;
      if (err == EINTR || err == EAGAIN || err == EWOULDBLOCK) {
        continue;  // Transient, retry immediately
      }
      if (err == EMFILE || err == ENFILE) {
        mygram::utils::StructuredLog()
            .Event("server_error")
            .Field("operation", "accept")
            .Field("error", "file_descriptor_exhaustion")
            .Field("errno", static_cast<int64_t>(err))
            .Error();
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        continue;
      }
      // Other errors
      mygram::utils::StructuredLog()
          .Event("server_error")
          .Field("operation", "accept")
          .Field("error", strerror(err))
          .Error();
      continue;
    }

    // SECURITY: Check connection limit BEFORE any processing to prevent resource exhaustion
    {
      std::lock_guard<std::mutex> lock(fds_mutex_);
      if (static_cast<int>(active_fds_.size()) >= config_.max_connections) {
        mygram::utils::StructuredLog()
            .Event("server_warning")
            .Field("type", "connection_limit_reached")
            .Field("active_connections", static_cast<uint64_t>(active_fds_.size()))
            .Field("max_connections", static_cast<uint64_t>(config_.max_connections))
            .Warn();
        close(client_fd);
        continue;
      }
    }

    if (!IsUnixSocket()) {
      // Convert client IP to string for ACL checks
      std::string client_ip = mygram::utils::GetPeerIP(client_fd);
      if (client_ip == "unknown") {
        mygram::utils::StructuredLog().Event("server_warning").Field("type", "client_address_parse_failed").Warn();
      }

      if (!mygram::utils::IsIPAllowed(client_ip, config_.parsed_allow_cidrs)) {
        mygram::utils::StructuredLog()
            .Event("server_warning")
            .Field("type", "connection_rejected_acl")
            .Field("client_ip", client_ip)
            .Warn();
        close(client_fd);
        continue;
      }
    }

    // Note: SO_RCVTIMEO is not set for reactor-mode connections because the
    // reactor uses non-blocking I/O (O_NONBLOCK). Idle connection detection
    // should be implemented at the reactor level, not via socket timeouts.

    // Set per-client SO_RCVBUF and SO_SNDBUF (these are not inherited from the
    // listening socket by the kernel, so they must be applied after accept()).
    SetClientSocketOptions(client_fd);

    // Apply per-connection TCP keepalive on TCP sockets only (not UDS). The
    // stock Linux defaults (2h idle + 9 probes * 75s) are too lax for
    // detecting half-open connections, so tighten them per YAML config.
    if (!IsUnixSocket() && config_.keepalive.enabled) {
      int keepalive_on = 1;
      if (setsockopt(client_fd, SOL_SOCKET, SO_KEEPALIVE, &keepalive_on, sizeof(keepalive_on)) < 0) {
        mygram::utils::StructuredLog()
            .Event("server_warning")
            .Field("type", "setsockopt_failed")
            .Field("option", "SO_KEEPALIVE")
            .Field("error", strerror(errno))
            .Warn();
      }
#if defined(__linux__)
      // Linux exposes TCP_KEEPIDLE/TCP_KEEPINTVL/TCP_KEEPCNT. These are our
      // production target and where this mitigation actually matters.
      int idle_sec = config_.keepalive.idle_sec;
      int intvl_sec = config_.keepalive.interval_sec;
      int probe_cnt = config_.keepalive.probe_count;
      if (setsockopt(client_fd, IPPROTO_TCP, TCP_KEEPIDLE, &idle_sec, sizeof(idle_sec)) < 0) {
        mygram::utils::StructuredLog()
            .Event("server_warning")
            .Field("type", "setsockopt_failed")
            .Field("option", "TCP_KEEPIDLE")
            .Field("error", strerror(errno))
            .Warn();
      }
      if (setsockopt(client_fd, IPPROTO_TCP, TCP_KEEPINTVL, &intvl_sec, sizeof(intvl_sec)) < 0) {
        mygram::utils::StructuredLog()
            .Event("server_warning")
            .Field("type", "setsockopt_failed")
            .Field("option", "TCP_KEEPINTVL")
            .Field("error", strerror(errno))
            .Warn();
      }
      if (setsockopt(client_fd, IPPROTO_TCP, TCP_KEEPCNT, &probe_cnt, sizeof(probe_cnt)) < 0) {
        mygram::utils::StructuredLog()
            .Event("server_warning")
            .Field("type", "setsockopt_failed")
            .Field("option", "TCP_KEEPCNT")
            .Field("error", strerror(errno))
            .Warn();
      }
#elif defined(__APPLE__) && defined(TCP_KEEPALIVE)
      // macOS/BSD only exposes TCP_KEEPALIVE (equivalent to Linux TCP_KEEPIDLE).
      // Interval/count fall back to system defaults. production target is
      // Linux; this branch only keeps dev/CI on macOS functional.
      int idle_sec = config_.keepalive.idle_sec;
      if (setsockopt(client_fd, IPPROTO_TCP, TCP_KEEPALIVE, &idle_sec, sizeof(idle_sec)) < 0) {
        mygram::utils::StructuredLog()
            .Event("server_warning")
            .Field("type", "setsockopt_failed")
            .Field("option", "TCP_KEEPALIVE")
            .Field("error", strerror(errno))
            .Warn();
      }
#endif
    }

#ifdef __APPLE__
    // On macOS, set SO_NOSIGPIPE to prevent SIGPIPE when writing to closed connections
    // Linux uses MSG_NOSIGNAL flag instead, but writev() doesn't support flags
    int nosigpipe = 1;
    if (setsockopt(client_fd, SOL_SOCKET, SO_NOSIGPIPE, &nosigpipe, sizeof(nosigpipe)) < 0) {
      mygram::utils::StructuredLog()
          .Event("server_warning")
          .Field("type", "setsockopt_failed")
          .Field("option", "SO_NOSIGPIPE")
          .Field("error", strerror(errno))
          .Warn();
    }
#endif

    // Track connection
    {
      std::lock_guard<std::mutex> lock(fds_mutex_);
      active_fds_.insert(client_fd);
    }

    // Reactor I/O model: hand off inline on the accept thread. The reactor
    // takes ownership of the fd on success; on failure we emit SERVER_BUSY
    // and close the fd here. The active_fds_ entry stays until IoReactor's
    // close callback invokes RemoveConnection.
    if (!reactor_handler_) {
      // Misconfiguration: reactor handler must be installed before Start().
      // Close the fd and keep looping so the server does not silently leak.
      mygram::utils::StructuredLog()
          .Event("server_error")
          .Field("type", "no_reactor_handler")
          .Field("error", "reactor handler not installed before accept loop started")
          .Error();
      close(client_fd);
      RemoveConnection(client_fd);
      continue;
    }

    const bool accepted = reactor_handler_(client_fd);
    if (!accepted) {
      mygram::utils::StructuredLog()
          .Event("server_warning")
          .Field("type", "reactor_register_rejected")
          .Field("client_fd", static_cast<uint64_t>(client_fd))
          .Warn();
      static constexpr std::string_view kBusyResponse =
          "ERR SERVER_BUSY Server is too busy, please try again later\r\n";
      // NOLINTNEXTLINE(bugprone-unused-return-value,cert-err33-c)
      write(client_fd, kBusyResponse.data(), kBusyResponse.size());
      close(client_fd);
      RemoveConnection(client_fd);
    }
  }

  mygram::utils::StructuredLog().Event("accept_loop_exited").Debug();
}

bool ConnectionAcceptor::SetSocketOptions(int socket_fd) const {
  // SO_REUSEADDR: Allow reuse of local addresses
  int opt = 1;
  if (setsockopt(socket_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
    std::string error_msg = "Failed to set SO_REUSEADDR: " + std::string(strerror(errno));
    mygram::utils::StructuredLog()
        .Event("server_error")
        .Field("operation", "setsockopt")
        .Field("option", "SO_REUSEADDR")
        .Field("error", error_msg)
        .Error();
    return false;
  }

  // SO_KEEPALIVE: Enable TCP keepalive
  if (setsockopt(socket_fd, SOL_SOCKET, SO_KEEPALIVE, &opt, sizeof(opt)) < 0) {
    std::string error_msg = "Failed to set SO_KEEPALIVE: " + std::string(strerror(errno));
    mygram::utils::StructuredLog()
        .Event("server_error")
        .Field("operation", "setsockopt")
        .Field("option", "SO_KEEPALIVE")
        .Field("error", error_msg)
        .Error();
    return false;
  }

  // Note: SO_RCVBUF and SO_SNDBUF are NOT set on the listening socket because
  // the kernel does not inherit them to accepted connections. They are applied
  // per-client in SetClientSocketOptions() after accept().

  return true;
}

void ConnectionAcceptor::SetClientSocketOptions(int client_fd) const {
  int rcvbuf = config_.recv_buffer_size;
  if (setsockopt(client_fd, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf)) < 0) {
    mygram::utils::StructuredLog()
        .Event("server_warning")
        .Field("operation", "setsockopt")
        .Field("option", "SO_RCVBUF")
        .Field("fd", static_cast<uint64_t>(client_fd))
        .Field("error", strerror(errno))
        .Warn();
  }
  int sndbuf = config_.send_buffer_size;
  if (setsockopt(client_fd, SOL_SOCKET, SO_SNDBUF, &sndbuf, sizeof(sndbuf)) < 0) {
    mygram::utils::StructuredLog()
        .Event("server_warning")
        .Field("operation", "setsockopt")
        .Field("option", "SO_SNDBUF")
        .Field("fd", static_cast<uint64_t>(client_fd))
        .Field("error", strerror(errno))
        .Warn();
  }
}

void ConnectionAcceptor::RemoveConnection(int socket_fd) {
  std::lock_guard<std::mutex> lock(fds_mutex_);
  active_fds_.erase(socket_fd);
}

}  // namespace mygramdb::server
