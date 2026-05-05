/**
 * @file kqueue_multiplexer.h
 * @brief kqueue-based EventMultiplexer backend for BSD and macOS.
 *
 * Implements the level-triggered `EventMultiplexer` abstraction on top of
 * FreeBSD/macOS `kqueue(2)`. Compiled only on BSD-family platforms (including
 * Darwin); on other platforms this header compiles away to nothing so that
 * `event_multiplexer.cpp` can `#include` it unconditionally inside the
 * factory's platform guard.
 *
 * Semantics:
 *
 *  - **Level-triggered.** `EV_CLEAR` is deliberately NOT set. kqueue's default
 *    delivery mode matches epoll's level-triggered contract: while the fd is
 *    still readable/writable and the corresponding filter is armed, every
 *    `Poll()` re-reports the event. This preserves the decoupling between
 *    event notification and consumption that `ReactorConnection` depends on.
 *
 *  - **Per-fd interest tracking.** kqueue has no single-call "set the current
 *    interest set of this fd to X" primitive the way `epoll_ctl(MOD)` does.
 *    Instead, we must diff the new interest against the previously-armed
 *    interest and emit `EV_ADD` for newly-armed filters and `EV_DELETE` for
 *    newly-disarmed ones. The mapping from fd to last-known interest lives
 *    in `interest_`.
 *
 *  - **Idempotent teardown.** `Remove()` tolerates `ENOENT`/`EBADF` from
 *    `kevent()` because `IoReactor::Stop()` may race with connection teardown
 *    that already closed the fd (kqueue automatically drops filters on
 *    close).
 */

#pragma once

#if defined(__APPLE__) || defined(__FreeBSD__) || defined(__NetBSD__) || defined(__OpenBSD__)

#include <sys/event.h>

#include <cstdint>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "server/reactor/event_multiplexer.h"
#include "utils/error.h"
#include "utils/expected.h"

namespace mygramdb::server::reactor {

/**
 * @brief kqueue-backed `EventMultiplexer` implementation.
 *
 * Thread-safety: Add/Modify/Remove/Poll are safe to call concurrently from
 * different threads. `interest_mutex_` guards `interest_`; the underlying
 * kqueue fd is itself thread-safe in the kernel for simultaneous
 * EV_ADD/Modify/Poll. This was previously documented as "not thread-safe" —
 * that documentation was out of date relative to the implementation. Do not
 * revert without removing `interest_mutex_`.
 */
class KqueueMultiplexer final : public EventMultiplexer {
 public:
  KqueueMultiplexer();
  ~KqueueMultiplexer() override;

  KqueueMultiplexer(const KqueueMultiplexer&) = delete;
  KqueueMultiplexer& operator=(const KqueueMultiplexer&) = delete;
  KqueueMultiplexer(KqueueMultiplexer&&) = delete;
  KqueueMultiplexer& operator=(KqueueMultiplexer&&) = delete;

  /// @copydoc EventMultiplexer::Open
  mygram::utils::Expected<void, mygram::utils::Error> Open() override;

  /// @copydoc EventMultiplexer::Add
  mygram::utils::Expected<void, mygram::utils::Error> Add(int fd, uint8_t interest) override;

  /// @copydoc EventMultiplexer::Modify
  mygram::utils::Expected<void, mygram::utils::Error> Modify(int fd, uint8_t interest) override;

  /// @copydoc EventMultiplexer::Remove
  mygram::utils::Expected<void, mygram::utils::Error> Remove(int fd) override;

  /// @copydoc EventMultiplexer::Poll
  mygram::utils::Expected<void, mygram::utils::Error> Poll(int timeout_ms, std::vector<ReadyEvent>& out) override;

  /// Backend identifier for metrics and logging.
  const char* Name() const override { return "kqueue"; }

 private:
  /**
   * @brief Diff `old_interest` against `new_interest` and apply the delta.
   *
   * @param fd            Target file descriptor.
   * @param new_interest  Desired interest bitmask (`event::kReadable` |
   *                      `event::kWritable`).
   * @param old_interest  Previously-armed interest bitmask. Ignored when
   *                      `is_add` is true.
   * @param is_add        If true, this is a fresh `Add()` and we unconditionally
   *                      emit `EV_ADD` for every bit set in `new_interest`.
   *                      If false, only the bits that changed are touched.
   *
   * Emits up to two `struct kevent` change records and flushes them in a
   * single `kevent()` call. Returns `kNetworkReactorRegisterFailed` (on add)
   * or `kNetworkReactorModifyFailed` (on modify) if the syscall fails.
   */
  mygram::utils::Expected<void, mygram::utils::Error> ApplyInterest(int fd, uint8_t new_interest, uint8_t old_interest,
                                                                    bool is_add);

  int kqueue_fd_ = -1;

  /// Reusable output buffer for `kevent()`. Sized at construction and
  /// doubled on demand (up to a fixed cap) whenever a Poll() fills it
  /// completely, so sustained bursts do not fragment across multiple Poll
  /// rounds. Touched only by the event-loop thread via `Poll()`; no locking.
  std::vector<struct kevent> events_;

  /// Mutex protecting `interest_`. IoReactor now allows concurrent Poll (on
  /// the event-loop thread) and Add/Modify/Remove (from accept / worker
  /// threads) because the kqueue kernel object is thread-safe for that
  /// pattern. The `interest_` map, however, is process-level state we keep
  /// ourselves and therefore needs its own synchronisation.
  mutable std::mutex interest_mutex_;

  /// Last-known interest mask per registered fd. Populated by `Add()`,
  /// updated by `Modify()`, erased by `Remove()`. Required because kqueue
  /// does not let us read back the currently-armed filter set.
  std::unordered_map<int, uint8_t> interest_;
};

}  // namespace mygramdb::server::reactor

#endif  // __APPLE__ || BSD family
