/**
 * @file event_multiplexer.h
 * @brief Platform-agnostic level-triggered event multiplexer.
 *
 * Inspired by Redis's `ae.c`, this module provides a minimal abstraction over
 * platform-specific polling primitives (epoll on Linux, kqueue on BSD/macOS,
 * and a deterministic mock for tests). `IoReactor` is written against this
 * interface so that the event loop itself is portable and the same test suite
 * can exercise every backend in parity.
 *
 * Design contracts:
 *
 *  - **Level-triggered semantics.** If a registered fd is still readable or
 *    writable and the interest bit is armed, the next `Poll()` must re-report
 *    the event. The reactor and `ReactorConnection` rely on this to decouple
 *    event notification from consumption (a worker thread may drain the fd on
 *    its own schedule without starving the event loop).
 *
 *  - **`Expected<void, Error>` for all mutating operations.** Syscalls map to
 *    `ErrorCode::kNetworkReactor*`. The `errno` captured at failure time is
 *    copied into the error message.
 *
 *  - **Caller-owned output buffer.** `Poll()` takes a reusable
 *    `std::vector<ReadyEvent>&` so the event-loop hot path allocates nothing
 *    steady-state.
 *
 *  - **Fd ownership.** The multiplexer never closes fds it is given; that
 *    remains `IoReactor`'s job. `Remove()` only unregisters.
 */

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include "utils/error.h"
#include "utils/expected.h"

namespace mygramdb::server::reactor {

/**
 * @brief Interest / ready-event bitmask values.
 *
 * Stored as a plain `uint8_t` so callers can `|` them together without
 * enum-class ceremony. The set is intentionally tiny; sharding or edge-
 * triggered modes are deliberately out of scope and would be a separate
 * abstraction if ever needed.
 */
namespace event {
constexpr uint8_t kNone = 0;
constexpr uint8_t kReadable = 1U << 0;  ///< EPOLLIN  / EVFILT_READ
constexpr uint8_t kWritable = 1U << 1;  ///< EPOLLOUT / EVFILT_WRITE
constexpr uint8_t kError = 1U << 2;     ///< EPOLLERR / EV_ERROR
constexpr uint8_t kHangup = 1U << 3;    ///< EPOLLHUP|EPOLLRDHUP / EV_EOF
}  // namespace event

/**
 * @brief One fd's ready events as produced by `Poll`.
 *
 * `events` is a bit-OR of `event::k*` values. `kReadable`/`kWritable` only
 * appear if the corresponding interest bit was armed.
 */
struct ReadyEvent {
  int fd;
  uint8_t events;
};

/**
 * @brief Abstract interface for backend-specific polling primitives.
 *
 * Thread-safety: instances are NOT thread-safe on their own. `IoReactor` owns
 * the multiplexer from a single event-loop thread and serializes any
 * cross-thread invocations (e.g. `ArmWrite` from a worker) via its own
 * synchronisation before calling into the multiplexer.
 */
class EventMultiplexer {
 public:
  virtual ~EventMultiplexer() = default;

  EventMultiplexer(const EventMultiplexer&) = delete;
  EventMultiplexer& operator=(const EventMultiplexer&) = delete;
  EventMultiplexer(EventMultiplexer&&) = delete;
  EventMultiplexer& operator=(EventMultiplexer&&) = delete;

  /**
   * @brief Create the underlying poller fd (`epoll_create1`, `kqueue`, ...).
   *
   * Must be called exactly once before `Add`/`Poll`. Returns
   * `kNetworkReactorInitFailed` on syscall failure, or
   * `kNetworkReactorAlreadyOpen` if called twice.
   */
  virtual mygram::utils::Expected<void, mygram::utils::Error> Open() = 0;

  /**
   * @brief Register a new fd with the given interest set.
   *
   * The multiplexer does NOT take ownership of the fd. `interest` is a bit-OR
   * of `event::kReadable` and `event::kWritable`; `kError` and `kHangup` are
   * always reported and cannot be masked. Returns
   * `kNetworkReactorRegisterFailed` on syscall failure.
   */
  virtual mygram::utils::Expected<void, mygram::utils::Error> Add(int fd, uint8_t interest) = 0;

  /**
   * @brief Update the interest set for an already-registered fd.
   *
   * This is the hot-path primitive for `ArmWrite`/`DisarmWrite`: flipping
   * `kWritable` on or off without recreating registration state.
   */
  virtual mygram::utils::Expected<void, mygram::utils::Error> Modify(int fd, uint8_t interest) = 0;

  /**
   * @brief Remove a previously-registered fd.
   *
   * Idempotent from the caller's perspective: removing an unknown fd returns
   * success because `IoReactor::Stop()` may race with connection teardown.
   */
  virtual mygram::utils::Expected<void, mygram::utils::Error> Remove(int fd) = 0;

  /**
   * @brief Block until at least one event is ready, or `timeout_ms` elapses.
   *
   * @param timeout_ms  -1 for infinite wait, 0 for non-blocking probe, >0 for
   *                    relative milliseconds.
   * @param out         Output buffer. Cleared and appended to; capacity is
   *                    preserved so the hot path does not allocate steady-state.
   *                    On error, `out` is left empty.
   */
  virtual mygram::utils::Expected<void, mygram::utils::Error> Poll(int timeout_ms, std::vector<ReadyEvent>& out) = 0;

  /**
   * @brief Backend identifier for metrics and log fields.
   *
   * Must return a stable string literal: "epoll", "kqueue", "mock".
   */
  virtual const char* Name() const = 0;

 protected:
  EventMultiplexer() = default;
};

/**
 * @brief Factory: return the best multiplexer available on this build.
 *
 * Selection order (compile-time):
 *   1. Linux -> `EpollMultiplexer`
 *   2. Apple / {Free,Net,Open}BSD -> `KqueueMultiplexer`
 *   3. Otherwise -> `nullptr`
 *
 * A nullptr return is how `TcpServer` decides to emit the "reactor mode not
 * supported on this platform, falling back to blocking" warning at startup.
 */
std::unique_ptr<EventMultiplexer> CreateEventMultiplexer();

}  // namespace mygramdb::server::reactor
