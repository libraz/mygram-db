/**
 * @file kqueue_multiplexer.cpp
 * @brief kqueue implementation of `EventMultiplexer` for BSD/macOS.
 */

#include "server/reactor/kqueue_multiplexer.h"

#if defined(__APPLE__) || defined(__FreeBSD__) || defined(__NetBSD__) || defined(__OpenBSD__)

#include <fcntl.h>
#include <sys/event.h>
#include <sys/time.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstring>
#include <string>
#include <vector>

#include "utils/constants.h"
#include "utils/error.h"
#include "utils/expected.h"

namespace mygramdb::server::reactor {

namespace {

using mygram::utils::Error;
using mygram::utils::ErrorCode;
using mygram::utils::Expected;
using mygram::utils::MakeError;
using mygram::utils::MakeUnexpected;

/// Starting size of the `kevent()` output buffer. Grows on demand up to
/// `kMaxEventBufferSize` whenever a Poll fills the buffer completely.
constexpr std::size_t kDefaultEventBufferSize = 64;

/// Upper bound on the `kevent()` output buffer. 4096 keeps the scratch
/// allocation bounded at ~128 KiB (`sizeof(struct kevent) * 4096`) while
/// still covering the server's expected peak concurrency of ~2000
/// connections with comfortable headroom. kqueue is level-triggered, so
/// excess ready events beyond the cap are harmlessly re-reported on the
/// next Poll.
constexpr std::size_t kMaxEventBufferSize = 4096;

/// Number of milliseconds in a second (kept as a named constant to keep the
/// timespec conversion below readable under clang-tidy's magic-number rule).
constexpr long kNanosPerMilli = 1'000'000L;
constexpr int64_t kMillisPerSecond = mygram::constants::kMillisecondsPerSecond;

/// Build an errno-decorated error message suffix. Captures `captured_errno` by
/// value to avoid TOCTOU between the failing syscall and the `strerror` call.
std::string FormatErrno(const char* syscall_label, int captured_errno) {
  std::string msg = syscall_label;
  msg += " failed: ";
  msg += std::strerror(captured_errno);
  msg += " (errno=";
  msg += std::to_string(captured_errno);
  msg += ")";
  return msg;
}

}  // namespace

KqueueMultiplexer::KqueueMultiplexer() {
  // Size the output buffer once up front so the steady-state Poll() path does
  // not allocate. We use resize() (not reserve()) because kevent() writes into
  // [data(), data() + size()); the actual returned count is what we iterate.
  events_.resize(kDefaultEventBufferSize);
}

KqueueMultiplexer::~KqueueMultiplexer() {
  if (kqueue_fd_ >= 0) {
    // Best-effort close; we are in a destructor and must not throw.
    ::close(kqueue_fd_);
    kqueue_fd_ = -1;
  }
}

Expected<void, Error> KqueueMultiplexer::Open() {
  if (kqueue_fd_ >= 0) {
    return MakeUnexpected(
        MakeError(ErrorCode::kNetworkReactorAlreadyOpen, "KqueueMultiplexer::Open called on already-open multiplexer"));
  }

  const int kq = ::kqueue();
  if (kq < 0) {
    const int en = errno;
    return MakeUnexpected(MakeError(ErrorCode::kNetworkReactorInitFailed, FormatErrno("kqueue", en)));
  }

  // Darwin / BSD `kqueue()` does not atomically set FD_CLOEXEC the way Linux's
  // `epoll_create1(EPOLL_CLOEXEC)` does. Apply it now so that any fork/exec
  // performed by the host process does not leak the poller fd to children.
  if (::fcntl(kq, F_SETFD, FD_CLOEXEC) < 0) {
    const int en = errno;
    ::close(kq);
    return MakeUnexpected(
        MakeError(ErrorCode::kNetworkReactorInitFailed, FormatErrno("fcntl(F_SETFD, FD_CLOEXEC)", en)));
  }

  kqueue_fd_ = kq;
  return {};
}

Expected<void, Error> KqueueMultiplexer::ApplyInterest(int fd, uint8_t new_interest, uint8_t old_interest,
                                                       bool is_add) {
  // kqueue has no single-call "set the interest set of this fd to X" primitive
  // the way `epoll_ctl(MOD)` does, so we diff the new interest against the
  // previously-armed interest and emit at most two change records: one per
  // filter (EVFILT_READ, EVFILT_WRITE).
  std::array<struct kevent, 2> changes{};
  int nchanges = 0;

  // --- EVFILT_READ ---
  const bool want_read = (new_interest & event::kReadable) != 0U;
  const bool had_read = (old_interest & event::kReadable) != 0U;
  if (want_read && (is_add || !had_read)) {
    EV_SET(&changes[nchanges], static_cast<uintptr_t>(fd), EVFILT_READ, EV_ADD, 0, 0, nullptr);
    ++nchanges;
  } else if (!want_read && !is_add && had_read) {
    EV_SET(&changes[nchanges], static_cast<uintptr_t>(fd), EVFILT_READ, EV_DELETE, 0, 0, nullptr);
    ++nchanges;
  }

  // --- EVFILT_WRITE ---
  const bool want_write = (new_interest & event::kWritable) != 0U;
  const bool had_write = (old_interest & event::kWritable) != 0U;
  if (want_write && (is_add || !had_write)) {
    EV_SET(&changes[nchanges], static_cast<uintptr_t>(fd), EVFILT_WRITE, EV_ADD, 0, 0, nullptr);
    ++nchanges;
  } else if (!want_write && !is_add && had_write) {
    EV_SET(&changes[nchanges], static_cast<uintptr_t>(fd), EVFILT_WRITE, EV_DELETE, 0, 0, nullptr);
    ++nchanges;
  }

  // Nothing to flush is not an error: e.g. Modify() with the same interest
  // set, or Add() with `kNone`. Note the deliberate absence of `EV_CLEAR` on
  // every EV_SET above: kqueue defaults to level-triggered, which matches the
  // EventMultiplexer contract and the reactor's expectations.
  if (nchanges == 0) {
    return {};
  }

  if (::kevent(kqueue_fd_, changes.data(), nchanges, nullptr, 0, nullptr) < 0) {
    const int en = errno;
    const ErrorCode code = is_add ? ErrorCode::kNetworkReactorRegisterFailed : ErrorCode::kNetworkReactorModifyFailed;
    return MakeUnexpected(
        MakeError(code, FormatErrno(is_add ? "kevent(EV_ADD)" : "kevent(modify)", en), "fd=" + std::to_string(fd)));
  }

  return {};
}

Expected<void, Error> KqueueMultiplexer::Add(int fd, uint8_t interest) {
  if (kqueue_fd_ < 0) {
    return MakeUnexpected(MakeError(ErrorCode::kNetworkReactorRegisterFailed,
                                    "KqueueMultiplexer::Add called before Open", "fd=" + std::to_string(fd)));
  }

  auto result = ApplyInterest(fd, interest, /*old_interest=*/0U, /*is_add=*/true);
  if (!result.has_value()) {
    return result;
  }
  {
    std::lock_guard<std::mutex> lock(interest_mutex_);
    interest_[fd] = interest;
  }
  return {};
}

Expected<void, Error> KqueueMultiplexer::Modify(int fd, uint8_t interest) {
  if (kqueue_fd_ < 0) {
    return MakeUnexpected(MakeError(ErrorCode::kNetworkReactorModifyFailed,
                                    "KqueueMultiplexer::Modify called before Open", "fd=" + std::to_string(fd)));
  }

  uint8_t old_interest = 0;
  {
    std::lock_guard<std::mutex> lock(interest_mutex_);
    const auto it = interest_.find(fd);
    if (it == interest_.end()) {
      return MakeUnexpected(MakeError(ErrorCode::kNetworkReactorModifyFailed,
                                      "KqueueMultiplexer::Modify called with unknown fd", "fd=" + std::to_string(fd)));
    }
    old_interest = it->second;
  }

  auto result = ApplyInterest(fd, interest, old_interest, /*is_add=*/false);
  if (!result.has_value()) {
    return result;
  }
  {
    std::lock_guard<std::mutex> lock(interest_mutex_);
    interest_[fd] = interest;
  }
  return {};
}

Expected<void, Error> KqueueMultiplexer::Remove(int fd) {
  // Drop our interest-tracking entry before touching the kernel state, so
  // that even if the kevent teardown syscall below races with connection
  // close we leave the map consistent.
  uint8_t old_interest = 0;
  {
    std::lock_guard<std::mutex> lock(interest_mutex_);
    const auto it = interest_.find(fd);
    if (it == interest_.end()) {
      // Idempotent: never-added or already-removed fds are a no-op success.
      return {};
    }
    old_interest = it->second;
    interest_.erase(it);
  }

  if (kqueue_fd_ < 0) {
    // Multiplexer already torn down; nothing to unregister.
    return {};
  }

  std::array<struct kevent, 2> changes{};
  int nchanges = 0;
  if ((old_interest & event::kReadable) != 0U) {
    EV_SET(&changes[nchanges], static_cast<uintptr_t>(fd), EVFILT_READ, EV_DELETE, 0, 0, nullptr);
    ++nchanges;
  }
  if ((old_interest & event::kWritable) != 0U) {
    EV_SET(&changes[nchanges], static_cast<uintptr_t>(fd), EVFILT_WRITE, EV_DELETE, 0, 0, nullptr);
    ++nchanges;
  }

  if (nchanges == 0) {
    return {};
  }

  if (::kevent(kqueue_fd_, changes.data(), nchanges, nullptr, 0, nullptr) < 0) {
    const int en = errno;
    // Idempotent teardown race: kqueue auto-removes filters on close (EBADF),
    // and the filter may already be gone from an earlier path (ENOENT).
    // Everything else is a real failure.
    if (en == ENOENT || en == EBADF) {
      return {};
    }
    return MakeUnexpected(MakeError(ErrorCode::kNetworkReactorRemoveFailed, FormatErrno("kevent(EV_DELETE)", en),
                                    "fd=" + std::to_string(fd)));
  }

  return {};
}

Expected<void, Error> KqueueMultiplexer::Poll(int timeout_ms, std::vector<ReadyEvent>& out) {
  out.clear();

  if (kqueue_fd_ < 0) {
    return MakeUnexpected(
        MakeError(ErrorCode::kNetworkReactorPollFailed, "KqueueMultiplexer::Poll called before Open"));
  }

  // Translate the reactor's integer timeout convention to the pointer-or-null
  // form that `kevent` expects:
  //   timeout_ms < 0 => tsp == nullptr => block indefinitely
  //   timeout_ms == 0 => zeroed timespec => non-blocking probe
  //   timeout_ms > 0  => populated timespec => relative wait
  struct timespec ts {};
  struct timespec* tsp = nullptr;
  if (timeout_ms == 0) {
    tsp = &ts;  // ts is already zero-initialised.
  } else if (timeout_ms > 0) {
    ts.tv_sec = timeout_ms / kMillisPerSecond;
    ts.tv_nsec = static_cast<long>(timeout_ms % kMillisPerSecond) * kNanosPerMilli;
    tsp = &ts;
  }

  const int n = ::kevent(kqueue_fd_, nullptr, 0, events_.data(), static_cast<int>(events_.size()), tsp);
  if (n < 0) {
    const int en = errno;
    // EINTR is not an error condition: a signal interrupted the wait and the
    // reactor loop will simply call us again. Report success with empty out.
    if (en == EINTR) {
      return {};
    }
    return MakeUnexpected(MakeError(ErrorCode::kNetworkReactorPollFailed, FormatErrno("kevent", en)));
  }

  out.reserve(static_cast<std::size_t>(n));
  for (int i = 0; i < n; ++i) {
    const struct kevent& kev = events_[static_cast<std::size_t>(i)];
    ReadyEvent ready{};
    ready.fd = static_cast<int>(kev.ident);
    ready.events = event::kNone;

    // Per kevent(2): `EV_ERROR` signals a per-change failure and `kev.data`
    // carries the errno. A zero `kev.data` with `EV_ERROR` set is used by
    // some BSDs as a benign ack of a change-list entry, which we ignore.
    if ((kev.flags & EV_ERROR) != 0 && kev.data != 0) {
      ready.events |= event::kError;
    } else if (kev.filter == EVFILT_READ) {
      ready.events |= event::kReadable;
    } else if (kev.filter == EVFILT_WRITE) {
      ready.events |= event::kWritable;
    }

    // `EV_EOF` means the peer has half-closed (or fully closed) the
    // connection. Mirror the epoll backend's EPOLLHUP/EPOLLRDHUP semantics so
    // the reactor can drain the read side before releasing the connection.
    if ((kev.flags & EV_EOF) != 0) {
      ready.events |= event::kHangup;
    }

    // Multiple kevents for the same fd (e.g. readable and writable delivered
    // in the same Poll) are intentionally emitted as separate ReadyEvent
    // entries. IoReactor ORs them together at dispatch time. Coalescing here
    // would add per-poll bookkeeping for no correctness gain.
    out.push_back(ready);
  }

  // Dynamic grow: if this Poll() filled the scratch buffer, chances are we
  // are running behind and the next tick will need more slots. Double the
  // capacity up to `kMaxEventBufferSize` so high-concurrency bursts are not
  // fragmented across multiple Poll() rounds. Growth is one-shot and
  // monotonic; the buffer never shrinks back down.
  if (static_cast<std::size_t>(n) == events_.size() && events_.size() < kMaxEventBufferSize) {
    const std::size_t new_size = std::min(events_.size() * 2, kMaxEventBufferSize);
    events_.resize(new_size);
  }
  return {};
}

}  // namespace mygramdb::server::reactor

#endif  // defined(__APPLE__) || defined(__FreeBSD__) || defined(__NetBSD__) || defined(__OpenBSD__)
