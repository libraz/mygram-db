/**
 * @file io_reactor.h
 * @brief Single-threaded reactor event loop built on `EventMultiplexer`.
 *
 * Phase 2 implementation of docs/ja/design/reactor-io-refactor.md §4.4.
 *
 * Responsibilities:
 *   1. Own one `reactor::EventMultiplexer` instance (epoll on Linux, kqueue
 *      on macOS/BSD, mock in tests).
 *   2. Run a single event-loop thread that repeatedly calls `Poll()` and
 *      dispatches readable/writable/error events to the matching
 *      `ReactorConnection`.
 *   3. Own the connection map keyed by fd. The map holds
 *      `std::shared_ptr<ReactorConnection>` because worker drain tasks may
 *      capture a shared_ptr copy and keep the connection alive while writing
 *      the response (design doc §7 R5).
 *   4. Provide thread-safe `Register`/`Unregister`/`ArmWrite`/`DisarmWrite`
 *      callable from the accept thread or from worker threads.
 *
 * Sharding across multiple event-loop threads is a Phase 3.5 optimisation;
 * Phase 2 ships a single loop and measures first.
 */

#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <thread>
#include <unordered_map>

#include "server/reactor/event_multiplexer.h"
#include "server/reactor_connection.h"
#include "utils/error.h"
#include "utils/expected.h"

namespace mygramdb::server {

class ThreadPool;
class RequestDispatcher;

/**
 * @brief Runtime-tunable reactor settings.
 *
 * Additional fields for sharding count, event-loop CPU affinity, and write
 * queue tuning will land alongside Phase 3 work. Values plumbed from YAML
 * (`api.tcp.*`) feed into this struct.
 */
struct ReactorConfig {
  /// Number of event-loop threads. Phase 2 hard-codes 1 regardless of this
  /// value; sharding is Phase 3.5 (design doc §7 R2).
  int event_loop_threads = 1;

  /// Per-connection soft cap on pending write bytes before the reactor
  /// forcibly closes a slow reader.
  size_t max_write_queue_bytes = ReactorConnection::kDefaultMaxWriteQueueBytes;

  /// Poll timeout in milliseconds. Short enough to react to `Stop()`
  /// promptly, long enough to keep the event loop idle-efficient.
  int poll_timeout_ms = 100;
};

/**
 * @brief Single-threaded I/O reactor.
 */
class IoReactor {
 public:
  /**
   * @param pool        Non-owning thread pool used to run drain tasks. Must
   *                    outlive this reactor.
   * @param dispatcher  Non-owning request dispatcher used by drain tasks.
   *                    May be null if no connections are ever registered
   *                    (e.g. reactor-parity unit tests with a mock mux).
   * @param cfg         Reactor tuning parameters.
   */
  IoReactor(ThreadPool* pool, RequestDispatcher* dispatcher, ReactorConfig cfg);
  ~IoReactor();

  IoReactor(const IoReactor&) = delete;
  IoReactor& operator=(const IoReactor&) = delete;
  IoReactor(IoReactor&&) = delete;
  IoReactor& operator=(IoReactor&&) = delete;

  /**
   * @brief Create the multiplexer and start the event-loop thread.
   * Idempotent: a second call while already running is a no-op success.
   *
   * Failure modes:
   *  - `kNetworkReactorUnsupported` if no multiplexer is available on this
   *    platform (CreateEventMultiplexer returned nullptr).
   *  - `kNetworkReactorInitFailed` propagated from `EventMultiplexer::Open`.
   */
  mygram::utils::Expected<void, mygram::utils::Error> Start();

  /**
   * @brief Stop the event-loop thread, close the multiplexer, and drop all
   *        registered connections. Idempotent.
   *
   * Drain tasks in flight keep their own shared_ptr copies, so the actual
   * socket close happens when the last shared_ptr drops (typically after
   * the drain task finishes writing its final response).
   */
  void Stop();

  /**
   * @brief Register a freshly accepted connection with the reactor.
   *
   * On success the reactor inserts the shared_ptr into its map, sets the fd
   * non-blocking, and arms `kReadable` on the multiplexer. On failure the
   * caller retains the shared_ptr and is responsible for closing it.
   *
   * Failure modes:
   *  - `kNetworkServerNotStarted` if `Start()` has not been called.
   *  - `kInternalError` if the fd is already registered.
   *  - `kNetworkSocketCreationFailed` if fcntl(O_NONBLOCK) fails.
   *  - `kNetworkReactorRegisterFailed` propagated from `Add`.
   */
  mygram::utils::Expected<void, mygram::utils::Error> Register(std::shared_ptr<ReactorConnection> conn);

  /**
   * @brief Remove a connection from the reactor and the multiplexer.
   *
   * Safe to call from any thread. Idempotent: unknown fd is a silent no-op
   * because Unregister races with drain-task teardown.
   *
   * If a close callback is installed via `SetCloseCallback`, it is invoked
   * AFTER the connection is removed from the multiplexer and the map but
   * BEFORE this function returns. The callback runs with no locks held.
   */
  void Unregister(int fd);

  /**
   * @brief Install a callback invoked from `Unregister` after a connection
   *        has been successfully removed.
   *
   * Used by `ConnectionAcceptor` to decrement its `active_fds_` set when the
   * reactor tears down a connection, preserving the `max_connections` gate.
   * Called at most once per fd and never while holding internal locks.
   * Must be set before `Start()` and not mutated afterwards.
   */
  void SetCloseCallback(std::function<void(int fd)> cb);

  /**
   * @brief Factory type for creating an `EventMultiplexer` instance.
   *
   * Used by `SetMultiplexerFactoryForTest` to override the default
   * `reactor::CreateEventMultiplexer()` backend in unit tests.
   */
  using MultiplexerFactory = std::function<std::unique_ptr<reactor::EventMultiplexer>()>;

  /**
   * @brief Override the multiplexer factory used by `Start()`. TEST-ONLY.
   *
   * Must be called before `Start()`. In production code, `Start()` always
   * calls `reactor::CreateEventMultiplexer()`. In tests, inject a factory
   * that returns a `MockEventMultiplexer` instead.
   *
   * @warning Do not call this from production code. It exists solely to
   *          provide a dependency-injection seam for unit tests.
   */
  void SetMultiplexerFactoryForTest(MultiplexerFactory f);

  /**
   * @brief Arm `kWritable` on an already-registered fd. Phase 3 will use
   *        this when a drain task can no longer write synchronously.
   *
   * Phase 2 never calls this from production paths; it exists so the API
   * stays stable across phases.
   */
  mygram::utils::Expected<void, mygram::utils::Error> ArmWrite(int fd);

  /**
   * @brief Disarm `kWritable` on an already-registered fd.
   */
  mygram::utils::Expected<void, mygram::utils::Error> DisarmWrite(int fd);

  /// Number of connections currently registered (for metrics).
  [[nodiscard]] size_t ConnectionCount() const;

  /// Whether the event loop is currently running.
  [[nodiscard]] bool IsRunning() const { return running_.load(std::memory_order_acquire); }

  /// Backend identifier string (forwarded from `EventMultiplexer::Name`).
  /// Returns "unavailable" if the reactor has not been started.
  [[nodiscard]] const char* BackendName() const;

 private:
  void EventLoop();
  void DispatchEvent(const reactor::ReadyEvent& ev);

  /// Look up a connection under a shared lock and return a shared_ptr copy
  /// (or nullptr). The copy is released outside the lock so user code runs
  /// without holding the connections_mutex_.
  std::shared_ptr<ReactorConnection> Lookup(int fd) const;

  ThreadPool* pool_;               // non-owning
  RequestDispatcher* dispatcher_;  // non-owning
  ReactorConfig config_;

  std::unique_ptr<reactor::EventMultiplexer> mux_;
  // shared_mutex around mux_:
  //   - Event loop holds `shared_lock` across Poll.
  //   - Register/Unregister/ArmWrite/DisarmWrite hold `shared_lock` while
  //     calling Add/Modify/Remove. Multiple shared locks coexist, so the
  //     loop's steady-state polling does NOT block connection registration.
  //   - Stop holds `unique_lock` to destroy mux_ after the event-loop thread
  //     has been joined; the exclusive wait is bounded by any in-flight
  //     Register call, not by the 100ms Poll interval.
  // The backends (epoll/kqueue) are kernel-level thread-safe for concurrent
  // poll + ctl/kevent from different threads; KqueueMultiplexer uses its own
  // internal mutex to protect its interest_ map.
  mutable std::shared_mutex mux_lifecycle_;
  std::thread event_loop_thread_;
  std::atomic<bool> running_{false};

  mutable std::shared_mutex connections_mutex_;
  std::unordered_map<int, std::shared_ptr<ReactorConnection>> connections_;

  // Optional teardown callback (see SetCloseCallback). Set-once before Start.
  std::function<void(int)> close_callback_;

  // TEST-ONLY: overrides reactor::CreateEventMultiplexer() when set.
  // Null in production; set by SetMultiplexerFactoryForTest() before Start().
  MultiplexerFactory mux_factory_;
};

}  // namespace mygramdb::server
