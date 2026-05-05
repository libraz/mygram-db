/**
 * @file epoll_multiplexer.h
 * @brief Linux epoll backend for the reactor I/O event multiplexer.
 *
 * This backend is compiled in only on Linux builds. On all other platforms
 * the entire translation unit collapses to nothing, and the factory in
 * `event_multiplexer.cpp` selects a different backend (kqueue on BSD/macOS)
 * or returns nullptr.
 *
 * The epoll backend is deliberately level-triggered (no `EPOLLET`): workers
 * drain socket buffers on their own schedule, and the event loop re-reports
 * readiness on each `Poll()` until the interest bit is cleared. This matches
 * the contract documented in `event_multiplexer.h`.
 */

#pragma once

#if defined(__linux__)

#include <sys/epoll.h>

#include <vector>

#include "server/reactor/event_multiplexer.h"

namespace mygramdb::server::reactor {

/**
 * @brief `EventMultiplexer` implementation backed by Linux `epoll(7)`.
 *
 * Thread-safety:
 *  - `Add`, `Modify`, and `Remove` are safe to call concurrently from
 *    multiple threads because each one is a single `epoll_ctl` syscall,
 *    which is documented as MT-safe. Unlike the kqueue backend, this
 *    implementation keeps no per-fd interest map in user space — `epoll_ctl`
 *    accepts `EPOLL_CTL_MOD` directly with the desired mask — so there is
 *    nothing to guard with a user-space mutex.
 *  - `Poll` is **event-loop thread only**. The `events_` scratch buffer
 *    is written without locking and IoReactor guarantees that `Poll()` is
 *    only ever invoked from its single `EventLoop()` thread (sequenced
 *    behind the reactor's `mux_lifecycle_` shared lock that publishes /
 *    republishes the multiplexer pointer). It is not legal to call `Poll`
 *    concurrently from two threads even though the epoll fd itself would
 *    tolerate it.
 *
 *  Note: unlike `KqueueMultiplexer`, this backend does not need an
 *  internal mutex because there is no shared in-process state to guard.
 *  See `epoll_ctl` is unaffected by CR-4 because it operates on a single fd
 *  per call; the multi-record race that motivates kqueue's serialised
 *  ApplyInterest does not apply here.
 */
class EpollMultiplexer : public EventMultiplexer {
 public:
  EpollMultiplexer();
  ~EpollMultiplexer() override;

  EpollMultiplexer(const EpollMultiplexer&) = delete;
  EpollMultiplexer& operator=(const EpollMultiplexer&) = delete;
  EpollMultiplexer(EpollMultiplexer&&) = delete;
  EpollMultiplexer& operator=(EpollMultiplexer&&) = delete;

  mygram::utils::Expected<void, mygram::utils::Error> Open() override;
  mygram::utils::Expected<void, mygram::utils::Error> Add(int fd, uint8_t interest) override;
  mygram::utils::Expected<void, mygram::utils::Error> Modify(int fd, uint8_t interest) override;
  mygram::utils::Expected<void, mygram::utils::Error> Remove(int fd) override;
  mygram::utils::Expected<void, mygram::utils::Error> Poll(int timeout_ms, std::vector<ReadyEvent>& out) override;
  const char* Name() const override;

  /**
   * @brief Trigger a self-wake so a blocked `Poll()` returns now.
   *
   * Implemented by `write(2)`-ing one byte to a registered `eventfd(2)`.
   * `Poll()` drains the eventfd and drops the resulting ready entry from its
   * output vector so callers never see the wake fd as a real ready event.
   */
  mygram::utils::Expected<void, mygram::utils::Error> Wake() override;

 private:
  /// File descriptor returned by `epoll_create1`. -1 until `Open()` succeeds.
  int epoll_fd_{-1};

  /// `eventfd(2)` used to break out of a blocked `epoll_wait`. Registered on
  /// `epoll_fd_` during `Open()`. `Wake()` writes a token to it; `Poll()`
  /// reads the token back and drops the ReadyEvent so callers never see it.
  int wake_fd_{-1};

  /// Reusable scratch buffer for `epoll_wait`. Sized once in the constructor
  /// and doubled on demand (up to a fixed cap) whenever a Poll() fills it
  /// completely, so sustained bursts do not fragment across multiple Poll
  /// rounds. Never shrinks back down.
  std::vector<struct epoll_event> events_;
};

}  // namespace mygramdb::server::reactor

#endif  // defined(__linux__)
