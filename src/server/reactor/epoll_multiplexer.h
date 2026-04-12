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
 * Not thread-safe: the reactor's event-loop thread is the sole owner.
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

 private:
  /// File descriptor returned by `epoll_create1`. -1 until `Open()` succeeds.
  int epoll_fd_{-1};

  /// Reusable scratch buffer for `epoll_wait`. Sized once in the constructor
  /// and doubled on demand (up to a fixed cap) whenever a Poll() fills it
  /// completely, so sustained bursts do not fragment across multiple Poll
  /// rounds. Never shrinks back down.
  std::vector<struct epoll_event> events_;
};

}  // namespace mygramdb::server::reactor

#endif  // defined(__linux__)
