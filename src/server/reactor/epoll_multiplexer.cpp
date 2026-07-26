/**
 * @file epoll_multiplexer.cpp
 * @brief Linux epoll backend implementation.
 */

#include "server/reactor/epoll_multiplexer.h"

#if defined(__linux__)

#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <string>
#include <vector>

#include "utils/errno_utils.h"
#include "utils/error.h"
#include "utils/expected.h"

namespace mygramdb::server::reactor {

namespace {

using mygram::utils::Error;
using mygram::utils::ErrorCode;
using mygram::utils::Expected;
using mygram::utils::FormatErrno;
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

/// Translate the reactor-level interest bitmask to an `epoll_event.events`
/// mask. `EPOLLERR | EPOLLHUP` remain armed for terminal socket failures.
/// `EPOLLRDHUP`, like kqueue's EV_EOF on EVFILT_READ, follows readable
/// interest: leaving it armed after recv() reports EOF would make a
/// level-triggered epoll fd fire forever while queued responses drain.
/// Explicitly level-triggered — no `EPOLLET`.
uint32_t InterestToEpollEvents(uint8_t interest) {
  uint32_t events = EPOLLERR | EPOLLHUP;
  if ((interest & event::kReadable) != 0U) {
    events |= EPOLLIN | EPOLLRDHUP;
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
  if (wake_fd_ >= 0) {
    // Note: epoll_fd_ is closed below; the kernel auto-removes wake_fd_ from
    // its interest set when the epoll instance is closed. Closing the
    // eventfd itself releases the userspace handle.
    ::close(wake_fd_);
    wake_fd_ = -1;
  }
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

  // Create a non-blocking eventfd for self-wake, register it on the epoll
  // instance, and stash the descriptor for Wake() to write into. EFD_CLOEXEC
  // mirrors the epoll fd's behaviour; EFD_NONBLOCK makes the Poll() drain
  // syscall safe to issue even if Wake() was never called (read returns
  // EAGAIN instead of blocking).
  const int efd = ::eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
  if (efd < 0) {
    const int en = errno;
    ::close(fd);
    return MakeUnexpected(MakeError(ErrorCode::kNetworkReactorInitFailed, FormatErrno("eventfd", en)));
  }

  struct epoll_event wake_ev {};
  wake_ev.events = EPOLLIN;
  wake_ev.data.u64 = kInvalidRegistrationToken;
  if (::epoll_ctl(fd, EPOLL_CTL_ADD, efd, &wake_ev) != 0) {
    const int en = errno;
    ::close(efd);
    ::close(fd);
    return MakeUnexpected(MakeError(ErrorCode::kNetworkReactorInitFailed, FormatErrno("epoll_ctl(ADD,wake_fd)", en)));
  }

  epoll_fd_ = fd;
  wake_fd_ = efd;
  return {};
}

Expected<void, Error> EpollMultiplexer::Wake() {
  if (wake_fd_ < 0) {
    // Multiplexer torn down; nothing to wake.
    return {};
  }
  // Write a single token. eventfd accumulates the counter, so multiple Wake()
  // calls collapse into one drain on the next Poll(). Ignore EAGAIN: counter
  // saturated at UINT64_MAX-1, which still fires EPOLLIN, so the wake intent
  // is satisfied either way.
  const uint64_t token = 1;
  const ssize_t n = ::write(wake_fd_, &token, sizeof(token));
  if (n < 0) {
    const int en = errno;
    if (en == EAGAIN || en == EWOULDBLOCK) {
      return {};
    }
    return MakeUnexpected(MakeError(ErrorCode::kNetworkReactorPollFailed, FormatErrno("write(wake_fd)", en)));
  }
  return {};
}

Expected<void, Error> EpollMultiplexer::Add(int fd, uint8_t interest, RegistrationToken registration_token) {
  std::lock_guard<std::mutex> lock(registration_mutex_);
  if (registration_token == kInvalidRegistrationToken) {
    registration_token = static_cast<RegistrationToken>(static_cast<uint32_t>(fd)) + 1;
  }
  struct epoll_event ev {};
  ev.events = InterestToEpollEvents(interest);
  ev.data.u64 = registration_token;

  if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &ev) != 0) {
    const int en = errno;
    return MakeUnexpected(MakeError(ErrorCode::kNetworkReactorRegisterFailed, FormatErrno("epoll_ctl(ADD)", en)));
  }
  tokens_by_fd_[fd] = registration_token;
  fds_by_token_[registration_token] = fd;
  return {};
}

Expected<void, Error> EpollMultiplexer::Modify(int fd, uint8_t interest, RegistrationToken registration_token) {
  std::lock_guard<std::mutex> lock(registration_mutex_);
  auto token_it = tokens_by_fd_.find(fd);
  if (token_it == tokens_by_fd_.end() ||
      (registration_token != kInvalidRegistrationToken && registration_token != token_it->second)) {
    return MakeUnexpected(
        MakeError(ErrorCode::kNetworkReactorModifyFailed, "epoll modify rejected for stale or unknown registration"));
  }
  struct epoll_event ev {};
  ev.events = InterestToEpollEvents(interest);
  ev.data.u64 = token_it->second;

  if (::epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd, &ev) != 0) {
    const int en = errno;
    return MakeUnexpected(MakeError(ErrorCode::kNetworkReactorModifyFailed, FormatErrno("epoll_ctl(MOD)", en)));
  }
  return {};
}

Expected<void, Error> EpollMultiplexer::Remove(int fd) {
  std::lock_guard<std::mutex> lock(registration_mutex_);
  auto token_it = tokens_by_fd_.find(fd);
  if (token_it == tokens_by_fd_.end()) {
    return {};
  }
  const RegistrationToken token = token_it->second;
  if (::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr) != 0) {
    const int en = errno;
    // Removing a registration is a terminal userspace operation even when
    // epoll rejects the DEL. Keeping these maps populated makes a later fd
    // reuse look registered and retains an unreachable token forever. The
    // kernel entry, if it still exists, is harmless: Poll drops events whose
    // token is no longer present, and closing fd tears the entry down.
    fds_by_token_.erase(token);
    tokens_by_fd_.erase(token_it);

    // Idempotent teardown race: the fd may have been closed (EBADF) or never
    // actually registered (ENOENT) by the time IoReactor::Stop() reaches us.
    // Swallow those two cases; everything else is a real failure.
    if (en == ENOENT || en == EBADF) {
      return {};
    }
    return MakeUnexpected(MakeError(ErrorCode::kNetworkReactorRemoveFailed, FormatErrno("epoll_ctl(DEL)", en)));
  }
  fds_by_token_.erase(token);
  tokens_by_fd_.erase(token_it);
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

    // Drain and drop the self-wake eventfd so callers never observe it as a
    // bogus ready fd. The eventfd is non-blocking and registered with
    // EPOLLIN only, so this read is safe even on spurious wakeups.
    if (ev.data.u64 == kInvalidRegistrationToken) {
      uint64_t drained = 0;
      // NOLINTNEXTLINE(clang-analyzer-deadcode.DeadStores) - we deliberately
      // ignore the byte count; the side-effect of clearing the counter is
      // what matters.
      (void)::read(wake_fd_, &drained, sizeof(drained));
      continue;
    }

    int fd = -1;
    {
      std::lock_guard<std::mutex> lock(registration_mutex_);
      auto fd_it = fds_by_token_.find(ev.data.u64);
      if (fd_it == fds_by_token_.end()) {
        continue;
      }
      fd = fd_it->second;
    }
    out.push_back(ReadyEvent{fd, EpollEventsToReady(ev.events), ev.data.u64});
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
