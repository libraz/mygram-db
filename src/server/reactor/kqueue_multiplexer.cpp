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

  // Register the self-wake user filter. EVFILT_USER is a kqueue-specific
  // filter that lets userspace trigger a wakeup with NOTE_TRIGGER. We add it
  // disabled-but-armed (EV_ADD without EV_ENABLE was historically required;
  // current macOS / *BSD documentation says EV_ADD alone arms it). The
  // filter's ident is `kWakeIdent`, a constant chosen to be far above any
  // realistic fd value so it cannot collide with a registered socket.
  // Wake() then issues an EV_ENABLE | NOTE_TRIGGER kevent to fire it; Poll()
  // drops the resulting ReadyEvent from its output so callers never see it.
  struct kevent wake_kev {};
  EV_SET(&wake_kev, kWakeIdent, EVFILT_USER, EV_ADD | EV_CLEAR, 0, 0, nullptr);
  if (::kevent(kq, &wake_kev, 1, nullptr, 0, nullptr) < 0) {
    const int en = errno;
    ::close(kq);
    return MakeUnexpected(
        MakeError(ErrorCode::kNetworkReactorInitFailed, FormatErrno("kevent(EV_ADD,EVFILT_USER)", en)));
  }

  kqueue_fd_ = kq;
  return {};
}

Expected<void, Error> KqueueMultiplexer::Wake() {
  if (kqueue_fd_ < 0) {
    // Multiplexer torn down; nothing to wake.
    return {};
  }
  // Trigger the user filter registered in Open(). NOTE_TRIGGER is what fires
  // the filter; the matching event will be delivered to the next Poll() and
  // dropped there. EV_CLEAR (set in Open) makes the filter re-arm itself
  // automatically, so subsequent Wake() calls keep working.
  struct kevent kev {};
  EV_SET(&kev, kWakeIdent, EVFILT_USER, 0, NOTE_TRIGGER, 0, nullptr);
  if (::kevent(kqueue_fd_, &kev, 1, nullptr, 0, nullptr) < 0) {
    const int en = errno;
    return MakeUnexpected(MakeError(ErrorCode::kNetworkReactorPollFailed, FormatErrno("kevent(NOTE_TRIGGER)", en)));
  }
  return {};
}

Expected<void, Error> KqueueMultiplexer::ApplyInterest(int fd, uint8_t new_interest, uint8_t* applied_interest,
                                                       uint8_t old_interest, bool is_add) {
  // kqueue has no single-call "set the interest set of this fd to X" primitive
  // the way `epoll_ctl(MOD)` does, so we diff the new interest against the
  // previously-armed interest and emit at most two change records: one per
  // filter (EVFILT_READ, EVFILT_WRITE).
  //
  // CR-4 (audit, May 2026): kevent(2) does not report partial application of
  // the change list. If the syscall is invoked with two change records and
  // the first succeeds while the second fails, the kernel returns -1 with
  // `errno` set from the failing entry but the first entry remains armed in
  // the kernel. Batching both filters into a single kevent() call therefore
  // risks the in-process `interest_` map drifting out of sync with the
  // kernel's filter set on partial failures.
  //
  // We could detect this by passing the change list as `eventlist` with
  // `EV_RECEIPT` and parsing per-entry `EV_ERROR` results, but that adds
  // non-trivial complexity for a path that is not on the steady-state hot
  // loop. Instead we serialise the changes: one kevent() call per change
  // record. After each successful entry we update `*applied_interest`
  // immediately so that on a mid-list failure the caller's record of which
  // filters are armed in the kernel matches reality. The performance impact
  // is bounded at most at two syscalls per Add/Modify, and Add/Modify is
  // already off the per-Poll hot path. In practice both filters changing
  // simultaneously is uncommon (Modify() typically toggles only kWritable
  // for the ArmWrite/DisarmWrite pattern).
  struct Change {
    int16_t filter;
    uint16_t flags;
    uint8_t bit;  // event::kReadable or event::kWritable
  };
  std::array<Change, 2> changes{};
  int nchanges = 0;

  // --- EVFILT_READ ---
  const bool want_read = (new_interest & event::kReadable) != 0U;
  const bool had_read = (old_interest & event::kReadable) != 0U;
  if (want_read && (is_add || !had_read)) {
    changes[nchanges] = {EVFILT_READ, EV_ADD, event::kReadable};
    ++nchanges;
  } else if (!want_read && !is_add && had_read) {
    changes[nchanges] = {EVFILT_READ, EV_DELETE, event::kReadable};
    ++nchanges;
  }

  // --- EVFILT_WRITE ---
  const bool want_write = (new_interest & event::kWritable) != 0U;
  const bool had_write = (old_interest & event::kWritable) != 0U;
  if (want_write && (is_add || !had_write)) {
    changes[nchanges] = {EVFILT_WRITE, EV_ADD, event::kWritable};
    ++nchanges;
  } else if (!want_write && !is_add && had_write) {
    changes[nchanges] = {EVFILT_WRITE, EV_DELETE, event::kWritable};
    ++nchanges;
  }

  // Initialise the running record of "what's currently armed in the kernel".
  // For a fresh Add() we know the kernel knows nothing about this fd yet, so
  // we start from zero. For Modify() we start from the caller-supplied prior
  // interest, then patch each entry as its kevent() syscall succeeds.
  if (applied_interest != nullptr) {
    *applied_interest = is_add ? 0U : old_interest;
  }

  // Nothing to flush is not an error: e.g. Modify() with the same interest
  // set, or Add() with `kNone`. Note the deliberate absence of `EV_CLEAR` on
  // every EV_SET below: kqueue defaults to level-triggered, which matches the
  // EventMultiplexer contract and the reactor's expectations.
  if (nchanges == 0) {
    return {};
  }

  for (int i = 0; i < nchanges; ++i) {
    struct kevent kev {};
    EV_SET(&kev, static_cast<uintptr_t>(fd), changes[i].filter, changes[i].flags, 0, 0, nullptr);
    if (::kevent(kqueue_fd_, &kev, 1, nullptr, 0, nullptr) < 0) {
      const int en = errno;
      const ErrorCode code = is_add ? ErrorCode::kNetworkReactorRegisterFailed : ErrorCode::kNetworkReactorModifyFailed;
      const char* label =
          (changes[i].flags == EV_ADD)
              ? (changes[i].filter == EVFILT_READ ? "kevent(EV_ADD,EVFILT_READ)" : "kevent(EV_ADD,EVFILT_WRITE)")
              : (changes[i].filter == EVFILT_READ ? "kevent(EV_DELETE,EVFILT_READ)" : "kevent(EV_DELETE,EVFILT_WRITE)");
      return MakeUnexpected(MakeError(code, FormatErrno(label, en), "fd=" + std::to_string(fd)));
    }
    if (applied_interest != nullptr) {
      // Patch the running record to reflect that this filter is now in the
      // requested state in the kernel. On a subsequent iteration's failure,
      // the caller can use this value to keep `interest_` in lockstep.
      if (changes[i].flags == EV_ADD) {
        *applied_interest = static_cast<uint8_t>(*applied_interest | changes[i].bit);
      } else {
        *applied_interest = static_cast<uint8_t>(*applied_interest & ~changes[i].bit);
      }
    }
  }

  return {};
}

Expected<void, Error> KqueueMultiplexer::Add(int fd, uint8_t interest) {
  if (kqueue_fd_ < 0) {
    return MakeUnexpected(MakeError(ErrorCode::kNetworkReactorRegisterFailed,
                                    "KqueueMultiplexer::Add called before Open", "fd=" + std::to_string(fd)));
  }

  // Hold interest_mutex_ across both the kevent() syscall and the map update
  // so the in-memory `interest_` view never diverges from the kernel's filter
  // set. ApplyInterest() does not take any other mutex (verified), so there
  // is no lock-order cycle. The syscall is short and only happens on
  // connection register / arm-write / disarm-write paths, which are well
  // outside the steady-state hot loop.
  std::lock_guard<std::mutex> lock(interest_mutex_);
  uint8_t applied = 0U;
  auto result = ApplyInterest(fd, interest, &applied, /*old_interest=*/0U, /*is_add=*/true);
  if (!result.has_value()) {
    // CR-4: a partial Add can leave one filter armed in the kernel while the
    // other failed. Record exactly what the kernel knows about so a follow-up
    // Remove() can clean it up. If nothing was applied we omit the entry
    // entirely so Remove() remains a no-op.
    if (applied != 0U) {
      interest_[fd] = applied;
    }
    return result;
  }
  interest_[fd] = interest;
  return {};
}

Expected<void, Error> KqueueMultiplexer::Modify(int fd, uint8_t interest) {
  if (kqueue_fd_ < 0) {
    return MakeUnexpected(MakeError(ErrorCode::kNetworkReactorModifyFailed,
                                    "KqueueMultiplexer::Modify called before Open", "fd=" + std::to_string(fd)));
  }

  // Hold interest_mutex_ across the read of the previous interest, the
  // kevent() diff syscall, and the in-memory write. Previously the syscall
  // happened outside the lock, which let two concurrent Modify(fd, ...)
  // callers each compute their delta from the same `old_interest` snapshot
  // and race to publish their result, leaving `interest_` and the kernel
  // filter set divergent. The race manifested at Remove() time as residual
  // EVFILT_WRITE filters (see P1-1).
  std::lock_guard<std::mutex> lock(interest_mutex_);
  const auto it = interest_.find(fd);
  if (it == interest_.end()) {
    return MakeUnexpected(MakeError(ErrorCode::kNetworkReactorModifyFailed,
                                    "KqueueMultiplexer::Modify called with unknown fd", "fd=" + std::to_string(fd)));
  }
  const uint8_t old_interest = it->second;
  if (old_interest == interest) {
    // Fast path: caller asked for the already-installed interest set, no
    // syscall needed and no map update needed.
    return {};
  }

  uint8_t applied = old_interest;
  auto result = ApplyInterest(fd, interest, &applied, old_interest, /*is_add=*/false);
  if (!result.has_value()) {
    // CR-4: serialised kevent() calls mean we know exactly which filters were
    // updated before the failure. Persist that partial state so `interest_`
    // matches the kernel; otherwise a follow-up Remove() would emit the wrong
    // EV_DELETE set and leak a filter (or invent a phantom one).
    it->second = applied;
    return result;
  }
  it->second = interest;
  return {};
}

Expected<void, Error> KqueueMultiplexer::Remove(int fd) {
  // Hold the lock across the kevent() teardown so that nothing else can
  // re-register the fd in between erasing from `interest_` and clearing the
  // kernel filters. This matches the new locking discipline in Add/Modify.
  std::lock_guard<std::mutex> lock(interest_mutex_);
  const auto it = interest_.find(fd);
  if (it == interest_.end()) {
    // Idempotent: never-added or already-removed fds are a no-op success.
    return {};
  }
  const uint8_t old_interest = it->second;
  interest_.erase(it);

  if (kqueue_fd_ < 0) {
    // Multiplexer already torn down; nothing to unregister.
    return {};
  }

  // CR-4: emit each EV_DELETE in its own kevent() call so a mid-list failure
  // cannot leave one filter still armed in the kernel while we believe the fd
  // is fully removed. The number of syscalls here is bounded at two and only
  // happens on connection teardown — well off the hot path.
  struct DeleteRec {
    int16_t filter;
    bool present;
  };
  const std::array<DeleteRec, 2> deletes{
      DeleteRec{EVFILT_READ, (old_interest & event::kReadable) != 0U},
      DeleteRec{EVFILT_WRITE, (old_interest & event::kWritable) != 0U},
  };

  for (const auto& d : deletes) {
    if (!d.present) {
      continue;
    }
    struct kevent kev {};
    EV_SET(&kev, static_cast<uintptr_t>(fd), d.filter, EV_DELETE, 0, 0, nullptr);
    if (::kevent(kqueue_fd_, &kev, 1, nullptr, 0, nullptr) < 0) {
      const int en = errno;
      // Idempotent teardown race: kqueue auto-removes filters on close (EBADF),
      // and the filter may already be gone from an earlier path (ENOENT).
      // Everything else is a real failure.
      if (en == ENOENT || en == EBADF) {
        continue;
      }
      const char* label =
          (d.filter == EVFILT_READ) ? "kevent(EV_DELETE,EVFILT_READ)" : "kevent(EV_DELETE,EVFILT_WRITE)";
      return MakeUnexpected(
          MakeError(ErrorCode::kNetworkReactorRemoveFailed, FormatErrno(label, en), "fd=" + std::to_string(fd)));
    }
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

    // Drop the self-wake user filter event so callers never observe it as a
    // bogus ready fd. EV_CLEAR on the EVFILT_USER registration auto-re-arms
    // the filter for the next Wake() call.
    if (kev.filter == EVFILT_USER && kev.ident == kWakeIdent) {
      continue;
    }

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
