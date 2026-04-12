/**
 * @file epoll_multiplexer.cpp
 * @brief Linux epoll backend implementation.
 */

#include "server/reactor/epoll_multiplexer.h"

#if defined(__linux__)

#include <sys/epoll.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <string>
#include <vector>

#include "utils/error.h"
#include "utils/expected.h"

namespace mygramdb::server::reactor {

namespace {

using mygram::utils::Error;
using mygram::utils::ErrorCode;
using mygram::utils::Expected;
using mygram::utils::MakeError;
using mygram::utils::MakeUnexpected;

/// Starting batch size for `epoll_wait`. The buffer grows on demand up to
/// `kMaxEventsCapacity` whenever a Poll fills it completely — a full batch
/// is a strong signal that the next tick will need more headroom.
constexpr std::size_t kInitialEventsCapacity = 64;

/// Upper bound on the `epoll_wait` output buffer. 4096 keeps the scratch
/// allocation bounded at ~48 KiB (`sizeof(epoll_event) * 4096`) while still
/// covering the server's expected peak concurrency of ~2000 connections
/// with comfortable headroom. Beyond this cap, excess ready events roll
/// over to the next Poll — harmless because epoll is level-triggered.
constexpr std::size_t kMaxEventsCapacity = 4096;

/// Build the `errno`-decorated error message suffix used everywhere in this
/// translation unit. Captures `errno` by value to avoid TOCTOU between the
/// failing syscall and the `strerror` call.
std::string FormatErrno(const char* syscall_label, int captured_errno) {
  std::string msg = syscall_label;
  msg += " failed: ";
  msg += std::strerror(captured_errno);
  msg += " (errno=";
  msg += std::to_string(captured_errno);
  msg += ")";
  return msg;
}

/// Translate the reactor-level interest bitmask to an `epoll_event.events`
/// mask. `EPOLLRDHUP | EPOLLERR | EPOLLHUP` are always armed so the reactor
/// can observe peer-initiated shutdowns even when neither read nor write
/// interest is set (e.g. a fully idle connection that the peer closes).
/// Explicitly level-triggered — no `EPOLLET`.
uint32_t InterestToEpollEvents(uint8_t interest) {
  uint32_t events = EPOLLRDHUP | EPOLLERR | EPOLLHUP;
  if ((interest & event::kReadable) != 0U) {
    events |= EPOLLIN;
  }
  if ((interest & event::kWritable) != 0U) {
    events |= EPOLLOUT;
  }
  return events;
}

/// Translate an `epoll_event.events` mask back to the reactor's bitmask.
uint8_t EpollEventsToReady(uint32_t events) {
  uint8_t ready = event::kNone;
  if ((events & EPOLLIN) != 0U) {
    ready |= event::kReadable;
  }
  if ((events & EPOLLOUT) != 0U) {
    ready |= event::kWritable;
  }
  if ((events & EPOLLERR) != 0U) {
    ready |= event::kError;
  }
  if ((events & (EPOLLHUP | EPOLLRDHUP)) != 0U) {
    ready |= event::kHangup;
  }
  return ready;
}

}  // namespace

EpollMultiplexer::EpollMultiplexer() {
  // Reserve once up front so the Poll() hot path does not allocate. We size
  // the buffer via resize() (not reserve()) because epoll_wait() writes into
  // [data(), data() + size()); the actual returned count is what we iterate.
  events_.resize(kInitialEventsCapacity);
}

EpollMultiplexer::~EpollMultiplexer() {
  if (epoll_fd_ >= 0) {
    // Best-effort close; we are in a destructor and must not throw.
    ::close(epoll_fd_);
    epoll_fd_ = -1;
  }
}

Expected<void, Error> EpollMultiplexer::Open() {
  if (epoll_fd_ >= 0) {
    return MakeUnexpected(
        MakeError(ErrorCode::kNetworkReactorAlreadyOpen, "EpollMultiplexer::Open called on already-open multiplexer"));
  }

  const int fd = ::epoll_create1(EPOLL_CLOEXEC);
  if (fd < 0) {
    const int en = errno;
    return MakeUnexpected(MakeError(ErrorCode::kNetworkReactorInitFailed, FormatErrno("epoll_create1", en)));
  }
  epoll_fd_ = fd;
  return {};
}

Expected<void, Error> EpollMultiplexer::Add(int fd, uint8_t interest) {
  struct epoll_event ev {};
  ev.events = InterestToEpollEvents(interest);
  ev.data.fd = fd;

  if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &ev) != 0) {
    const int en = errno;
    return MakeUnexpected(MakeError(ErrorCode::kNetworkReactorRegisterFailed, FormatErrno("epoll_ctl(ADD)", en)));
  }
  return {};
}

Expected<void, Error> EpollMultiplexer::Modify(int fd, uint8_t interest) {
  struct epoll_event ev {};
  ev.events = InterestToEpollEvents(interest);
  ev.data.fd = fd;

  if (::epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd, &ev) != 0) {
    const int en = errno;
    return MakeUnexpected(MakeError(ErrorCode::kNetworkReactorModifyFailed, FormatErrno("epoll_ctl(MOD)", en)));
  }
  return {};
}

Expected<void, Error> EpollMultiplexer::Remove(int fd) {
  if (::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr) != 0) {
    const int en = errno;
    // Idempotent teardown race: the fd may have been closed (EBADF) or never
    // actually registered (ENOENT) by the time IoReactor::Stop() reaches us.
    // Swallow those two cases; everything else is a real failure.
    if (en == ENOENT || en == EBADF) {
      return {};
    }
    return MakeUnexpected(MakeError(ErrorCode::kNetworkReactorRemoveFailed, FormatErrno("epoll_ctl(DEL)", en)));
  }
  return {};
}

Expected<void, Error> EpollMultiplexer::Poll(int timeout_ms, std::vector<ReadyEvent>& out) {
  out.clear();

  const int n = ::epoll_wait(epoll_fd_, events_.data(), static_cast<int>(events_.size()), timeout_ms);
  if (n < 0) {
    const int en = errno;
    // EINTR is not an error condition: a signal interrupted the wait and the
    // reactor loop will simply call us again. Report success with empty `out`.
    if (en == EINTR) {
      return {};
    }
    return MakeUnexpected(MakeError(ErrorCode::kNetworkReactorPollFailed, FormatErrno("epoll_wait", en)));
  }

  out.reserve(static_cast<std::size_t>(n));
  for (int i = 0; i < n; ++i) {
    const auto& ev = events_[static_cast<std::size_t>(i)];
    out.push_back(ReadyEvent{ev.data.fd, EpollEventsToReady(ev.events)});
  }

  // Dynamic grow: if this Poll() filled the scratch buffer, chances are we
  // are running behind and the next tick will need more slots. Double the
  // capacity up to `kMaxEventsCapacity` so we stop fragmenting high-concurrency
  // bursts across multiple Poll() rounds. Growth is one-shot and monotonic;
  // the buffer never shrinks back down.
  if (static_cast<std::size_t>(n) == events_.size() && events_.size() < kMaxEventsCapacity) {
    const std::size_t new_size = std::min(events_.size() * 2, kMaxEventsCapacity);
    events_.resize(new_size);
  }
  return {};
}

const char* EpollMultiplexer::Name() const {
  return "epoll";
}

}  // namespace mygramdb::server::reactor

#endif  // defined(__linux__)
