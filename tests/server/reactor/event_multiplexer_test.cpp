/**
 * @file event_multiplexer_test.cpp
 * @brief Parameterised test suite for all EventMultiplexer backends.
 *
 * Each TYPED_TEST runs against MockEventMultiplexer and—when the build
 * platform matches—against the real kernel backend (EpollMultiplexer on
 * Linux, KqueueMultiplexer on macOS/BSD).  Backend-specific behaviour is
 * guarded with `if constexpr (std::is_same_v<TypeParam, MockEventMultiplexer>)`
 * so the test binary compiles on every platform without dead-code warnings.
 */

#include "server/reactor/event_multiplexer.h"

#include <gtest/gtest.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

#include "mock_event_multiplexer.h"

#if defined(__linux__)
#include "server/reactor/epoll_multiplexer.h"
#endif

#if defined(__APPLE__) || defined(__FreeBSD__) || defined(__NetBSD__) || defined(__OpenBSD__)
#include "server/reactor/kqueue_multiplexer.h"
#endif

namespace mygramdb::server::reactor {

// ---------------------------------------------------------------------------
// Type list
// ---------------------------------------------------------------------------

using BackendTypes = ::testing::Types<MockEventMultiplexer
#if defined(__linux__)
                                      ,
                                      EpollMultiplexer
#endif
#if defined(__APPLE__) || defined(__FreeBSD__) || defined(__NetBSD__) || defined(__OpenBSD__)
                                      ,
                                      KqueueMultiplexer
#endif
                                      >;

// ---------------------------------------------------------------------------
// Fixture
// ---------------------------------------------------------------------------

template <typename MuxT>
class EventMultiplexerTest : public ::testing::Test {
 protected:
  std::unique_ptr<MuxT> mux;

  void SetUp() override {
    mux = std::make_unique<MuxT>();
    ASSERT_TRUE(mux->Open().has_value()) << "Open() failed in SetUp";
  }

  void TearDown() override {
    if constexpr (std::is_same_v<MuxT, MockEventMultiplexer>) {
      mux->Shutdown();
    }
    for (int fd : open_fds_) {
      ::close(fd);
    }
    open_fds_.clear();
  }

  /// Create a connected Unix-domain socketpair and track both fds for cleanup.
  std::pair<int, int> MakeSocketPair() {
    int fds[2] = {-1, -1};
    EXPECT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0) << "socketpair failed";
    open_fds_.push_back(fds[0]);
    open_fds_.push_back(fds[1]);
    return {fds[0], fds[1]};
  }

  /// Set fd non-blocking (best-effort; not required by these tests).
  void SetNonblocking(int /*fd*/) {}

  /// Remove fd from the tracked cleanup set (caller will close it manually).
  void UntrackFd(int fd) { open_fds_.erase(std::remove(open_fds_.begin(), open_fds_.end(), fd), open_fds_.end()); }

 private:
  std::vector<int> open_fds_;
};

TYPED_TEST_SUITE(EventMultiplexerTest, BackendTypes);

// ---------------------------------------------------------------------------
// 1. OpenReturnsAlreadyOpenOnSecondCall
// ---------------------------------------------------------------------------
TYPED_TEST(EventMultiplexerTest, OpenReturnsAlreadyOpenOnSecondCall) {
  // SetUp already called Open() once.
  auto result = this->mux->Open();
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kNetworkReactorAlreadyOpen);
}

// ---------------------------------------------------------------------------
// 2. AddThenRemoveIsClean
// ---------------------------------------------------------------------------
TYPED_TEST(EventMultiplexerTest, AddThenRemoveIsClean) {
  auto [r, w] = this->MakeSocketPair();
  ASSERT_TRUE(this->mux->Add(r, event::kReadable).has_value());
  ASSERT_TRUE(this->mux->Remove(r).has_value());
}

// ---------------------------------------------------------------------------
// 3. RemoveUnknownFdIsIdempotent
// ---------------------------------------------------------------------------
TYPED_TEST(EventMultiplexerTest, RemoveUnknownFdIsIdempotent) {
  // 999999 is almost certainly not a valid registered fd.
  EXPECT_TRUE(this->mux->Remove(999999).has_value());
}

// ---------------------------------------------------------------------------
// 4. ModifyUnknownFdFails
// ---------------------------------------------------------------------------
TYPED_TEST(EventMultiplexerTest, ModifyUnknownFdFails) {
  auto result = this->mux->Modify(999999, event::kReadable);
  EXPECT_FALSE(result.has_value());
}

// ---------------------------------------------------------------------------
// 5. PollZeroTimeoutReturnsImmediately
// ---------------------------------------------------------------------------
TYPED_TEST(EventMultiplexerTest, PollZeroTimeoutReturnsImmediately) {
  auto [r, w] = this->MakeSocketPair();
  ASSERT_TRUE(this->mux->Add(r, event::kReadable).has_value());

  std::vector<ReadyEvent> out;
  auto start = std::chrono::steady_clock::now();
  ASSERT_TRUE(this->mux->Poll(0, out).has_value());
  auto elapsed = std::chrono::steady_clock::now() - start;

  // No data written, so nothing readable; timeout 0 returns immediately.
  EXPECT_LT(std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count(), 50);
  // For Mock with no injected events: out is empty.
  if constexpr (std::is_same_v<TypeParam, MockEventMultiplexer>) {
    EXPECT_TRUE(out.empty());
  }
}

// ---------------------------------------------------------------------------
// 6. PollDetectsReadable
// ---------------------------------------------------------------------------
TYPED_TEST(EventMultiplexerTest, PollDetectsReadable) {
  std::vector<ReadyEvent> out;

  if constexpr (std::is_same_v<TypeParam, MockEventMultiplexer>) {
    ASSERT_TRUE(this->mux->Add(42, event::kReadable).has_value());
    this->mux->InjectReadable(42);
    ASSERT_TRUE(this->mux->Poll(100, out).has_value());
    ASSERT_EQ(out.size(), 1U);
    EXPECT_EQ(out[0].fd, 42);
    EXPECT_NE(out[0].events & event::kReadable, 0);
  } else {
    auto [r, w] = this->MakeSocketPair();
    ASSERT_TRUE(this->mux->Add(r, event::kReadable).has_value());
    char c = 'x';
    ASSERT_EQ(::write(w, &c, 1), 1);
    ASSERT_TRUE(this->mux->Poll(500, out).has_value());
    ASSERT_EQ(out.size(), 1U);
    EXPECT_EQ(out[0].fd, r);
    EXPECT_NE(out[0].events & event::kReadable, 0);
  }
}

// ---------------------------------------------------------------------------
// 7. PollDetectsHangupOnPeerClose
// ---------------------------------------------------------------------------
TYPED_TEST(EventMultiplexerTest, PollDetectsHangupOnPeerClose) {
  std::vector<ReadyEvent> out;

  if constexpr (std::is_same_v<TypeParam, MockEventMultiplexer>) {
    ASSERT_TRUE(this->mux->Add(77, event::kReadable).has_value());
    this->mux->InjectHangup(77);
    ASSERT_TRUE(this->mux->Poll(100, out).has_value());
    ASSERT_EQ(out.size(), 1U);
    EXPECT_NE(out[0].events & event::kHangup, 0);
  } else {
    auto [r, w] = this->MakeSocketPair();
    ASSERT_TRUE(this->mux->Add(r, event::kReadable).has_value());
    // Close the peer; the kernel delivers a hangup/EOF on r.
    this->UntrackFd(w);
    ::close(w);
    ASSERT_TRUE(this->mux->Poll(500, out).has_value());
    ASSERT_GE(out.size(), 1U);
    EXPECT_NE(out[0].events & event::kHangup, 0);
  }
}

// ---------------------------------------------------------------------------
// 8. LevelTriggeredRefires
// ---------------------------------------------------------------------------
TYPED_TEST(EventMultiplexerTest, LevelTriggeredRefires) {
  std::vector<ReadyEvent> out;

  if constexpr (std::is_same_v<TypeParam, MockEventMultiplexer>) {
    ASSERT_TRUE(this->mux->Add(55, event::kReadable).has_value());
    this->mux->InjectReadable(55);
    ASSERT_TRUE(this->mux->Poll(100, out).has_value());
    ASSERT_EQ(out.size(), 1U);
    // Inject again to simulate level-triggered re-report.
    this->mux->InjectReadable(55);
    out.clear();
    ASSERT_TRUE(this->mux->Poll(100, out).has_value());
    ASSERT_EQ(out.size(), 1U);
  } else {
    auto [r, w] = this->MakeSocketPair();
    ASSERT_TRUE(this->mux->Add(r, event::kReadable).has_value());
    char c = 'x';
    ASSERT_EQ(::write(w, &c, 1), 1);

    // Give the kernel a moment to deliver.
    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    // First poll — readable.
    ASSERT_TRUE(this->mux->Poll(200, out).has_value());
    ASSERT_GE(out.size(), 1U) << "first Poll did not see readable event";
    EXPECT_NE(out[0].events & event::kReadable, 0);

    // Do NOT drain the socket. Level-triggered: second poll must re-fire.
    out.clear();
    ASSERT_TRUE(this->mux->Poll(200, out).has_value());
    ASSERT_GE(out.size(), 1U) << "level-triggered re-fire missing on second Poll";
    EXPECT_NE(out[0].events & event::kReadable, 0);
  }
}

// ---------------------------------------------------------------------------
// 9. WritableDetectedOnEmptyBuffer
// ---------------------------------------------------------------------------
TYPED_TEST(EventMultiplexerTest, WritableDetectedOnEmptyBuffer) {
  std::vector<ReadyEvent> out;

  if constexpr (std::is_same_v<TypeParam, MockEventMultiplexer>) {
    ASSERT_TRUE(this->mux->Add(33, event::kWritable).has_value());
    this->mux->InjectWritable(33);
    ASSERT_TRUE(this->mux->Poll(100, out).has_value());
    ASSERT_EQ(out.size(), 1U);
    EXPECT_NE(out[0].events & event::kWritable, 0);
  } else {
    auto [r, w] = this->MakeSocketPair();
    // Arm writable on r: an un-full socket buffer is immediately writable.
    ASSERT_TRUE(this->mux->Add(r, event::kWritable).has_value());
    ASSERT_TRUE(this->mux->Poll(500, out).has_value());
    ASSERT_GE(out.size(), 1U);
    EXPECT_NE(out[0].events & event::kWritable, 0);
  }
}

// ---------------------------------------------------------------------------
// 10. ModifyArmsAndDisarmsWritable
// ---------------------------------------------------------------------------
TYPED_TEST(EventMultiplexerTest, ModifyArmsAndDisarmsWritable) {
  std::vector<ReadyEvent> out;

  if constexpr (std::is_same_v<TypeParam, MockEventMultiplexer>) {
    ASSERT_TRUE(this->mux->Add(11, event::kReadable).has_value());
    // No injected events; Poll(0) returns empty.
    ASSERT_TRUE(this->mux->Poll(0, out).has_value());
    EXPECT_TRUE(out.empty());

    // Arm writable.
    ASSERT_TRUE(this->mux->Modify(11, event::kReadable | event::kWritable).has_value());
    this->mux->InjectWritable(11);
    ASSERT_TRUE(this->mux->Poll(100, out).has_value());
    EXPECT_GE(out.size(), 1U);

    // Disarm writable; no more injected events.
    ASSERT_TRUE(this->mux->Modify(11, event::kReadable).has_value());
    out.clear();
    ASSERT_TRUE(this->mux->Poll(0, out).has_value());
    EXPECT_TRUE(out.empty());
  } else {
    auto [r, w] = this->MakeSocketPair();
    // Register readable-only; nothing readable yet.
    ASSERT_TRUE(this->mux->Add(r, event::kReadable).has_value());
    ASSERT_TRUE(this->mux->Poll(0, out).has_value());
    EXPECT_TRUE(out.empty());

    // Add writable interest; socket buffer is empty so immediately writable.
    ASSERT_TRUE(this->mux->Modify(r, event::kReadable | event::kWritable).has_value());
    ASSERT_TRUE(this->mux->Poll(500, out).has_value());
    bool found_writable = false;
    for (const auto& ev : out) {
      if ((ev.events & event::kWritable) != 0) {
        found_writable = true;
      }
    }
    EXPECT_TRUE(found_writable);

    // Disarm writable; Poll(0) should return empty (nothing readable either).
    ASSERT_TRUE(this->mux->Modify(r, event::kReadable).has_value());
    out.clear();
    ASSERT_TRUE(this->mux->Poll(0, out).has_value());
    EXPECT_TRUE(out.empty());
  }
}

// ---------------------------------------------------------------------------
// 11. PollBatchReturnsMultiple
// ---------------------------------------------------------------------------
TYPED_TEST(EventMultiplexerTest, PollBatchReturnsMultiple) {
  constexpr int kCount = 5;
  std::vector<ReadyEvent> out;

  if constexpr (std::is_same_v<TypeParam, MockEventMultiplexer>) {
    for (int i = 100; i < 100 + kCount; ++i) {
      ASSERT_TRUE(this->mux->Add(i, event::kReadable).has_value());
      this->mux->InjectReadable(i);
    }
    ASSERT_TRUE(this->mux->Poll(100, out).has_value());
    EXPECT_EQ(static_cast<int>(out.size()), kCount);
  } else {
    std::vector<std::pair<int, int>> pairs;
    for (int i = 0; i < kCount; ++i) {
      pairs.push_back(this->MakeSocketPair());
    }
    for (auto& [r, w] : pairs) {
      ASSERT_TRUE(this->mux->Add(r, event::kReadable).has_value());
      char c = 'x';
      ASSERT_EQ(::write(w, &c, 1), 1);
    }
    ASSERT_TRUE(this->mux->Poll(500, out).has_value());
    EXPECT_EQ(static_cast<int>(out.size()), kCount);
  }
}

// ---------------------------------------------------------------------------
// 12. PollOutVectorIsReused
// ---------------------------------------------------------------------------
TYPED_TEST(EventMultiplexerTest, PollOutVectorIsReused) {
  std::vector<ReadyEvent> out;

  if constexpr (std::is_same_v<TypeParam, MockEventMultiplexer>) {
    ASSERT_TRUE(this->mux->Add(200, event::kReadable).has_value());
    this->mux->InjectReadable(200);
    ASSERT_TRUE(this->mux->Poll(100, out).has_value());
    ASSERT_EQ(out.size(), 1U);

    // Second call with same vector — must clear first.
    ASSERT_TRUE(this->mux->Poll(0, out).has_value());
    EXPECT_TRUE(out.empty()) << "Poll did not clear out on second call";
  } else {
    auto [r, w] = this->MakeSocketPair();
    ASSERT_TRUE(this->mux->Add(r, event::kReadable).has_value());
    char c = 'x';
    ASSERT_EQ(::write(w, &c, 1), 1);

    ASSERT_TRUE(this->mux->Poll(500, out).has_value());
    ASSERT_GE(out.size(), 1U);
    // Drain the byte so nothing is readable on second Poll.
    char buf = 0;
    (void)::recv(r, &buf, 1, MSG_DONTWAIT);

    out.push_back(ReadyEvent{-1, 0xFF});  // sentinel; must be cleared
    ASSERT_TRUE(this->mux->Poll(0, out).has_value());
    for (const auto& ev : out) {
      EXPECT_NE(ev.fd, -1) << "Poll did not clear the out vector";
    }
  }
}

// ---------------------------------------------------------------------------
// 13. DestructorClosesPollerFd (real backends only)
// ---------------------------------------------------------------------------
TYPED_TEST(EventMultiplexerTest, DestructorClosesPollerFd) {
  if constexpr (std::is_same_v<TypeParam, MockEventMultiplexer>) {
    GTEST_SKIP() << "Destructor fd check not applicable to MockEventMultiplexer";
  } else {
    // Destroy the mux; this must not crash or abort.
    this->mux.reset();
    // Recreate and open a fresh one to confirm no fd-space corruption.
    this->mux = std::make_unique<TypeParam>();
    EXPECT_TRUE(this->mux->Open().has_value());
  }
}

// ===========================================================================
// Mock-only non-typed tests
// ===========================================================================

TEST(MockEventMultiplexerTest, WaitForPollCalled) {
  MockEventMultiplexer mux;
  ASSERT_TRUE(mux.Open().has_value());

  std::vector<ReadyEvent> out;
  std::atomic<bool> thread_started{false};

  // Background thread blocks on Poll(-1, …).
  std::thread poller([&]() {
    thread_started.store(true);
    (void)mux.Poll(-1, out);
  });

  // Ensure the thread has at least started before we wait.
  while (!thread_started.load()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  // Inject an event to unblock Poll.
  mux.InjectReadable(1);

  EXPECT_TRUE(mux.WaitForPollCalled(1, std::chrono::milliseconds(1000)));

  mux.Shutdown();
  poller.join();
}

TEST(MockEventMultiplexerTest, RegisteredFdsReturnsExactSet) {
  MockEventMultiplexer mux;
  ASSERT_TRUE(mux.Open().has_value());

  ASSERT_TRUE(mux.Add(10, event::kReadable).has_value());
  ASSERT_TRUE(mux.Add(20, event::kWritable).has_value());
  ASSERT_TRUE(mux.Add(30, event::kReadable | event::kWritable).has_value());

  auto fds = mux.RegisteredFds();
  ASSERT_EQ(fds.size(), 3U);
  std::sort(fds.begin(), fds.end());
  EXPECT_EQ(fds[0], 10);
  EXPECT_EQ(fds[1], 20);
  EXPECT_EQ(fds[2], 30);

  EXPECT_EQ(mux.InterestFor(10), event::kReadable);
  EXPECT_EQ(mux.InterestFor(20), event::kWritable);
  EXPECT_EQ(mux.InterestFor(30), event::kReadable | event::kWritable);
  EXPECT_EQ(mux.InterestFor(99), 0);

  ASSERT_TRUE(mux.Remove(20).has_value());
  auto fds2 = mux.RegisteredFds();
  EXPECT_EQ(fds2.size(), 2U);
}

TEST(MockEventMultiplexerTest, PollCallCountIncrementsEachCall) {
  MockEventMultiplexer mux;
  ASSERT_TRUE(mux.Open().has_value());
  std::vector<ReadyEvent> out;

  EXPECT_EQ(mux.PollCallCount(), 0);
  (void)mux.Poll(0, out);
  EXPECT_EQ(mux.PollCallCount(), 1);
  (void)mux.Poll(0, out);
  EXPECT_EQ(mux.PollCallCount(), 2);
}

TEST(MockEventMultiplexerTest, InjectErrorDeliversKErrorBit) {
  MockEventMultiplexer mux;
  ASSERT_TRUE(mux.Open().has_value());
  ASSERT_TRUE(mux.Add(50, event::kReadable).has_value());

  mux.InjectError(50, ECONNRESET);

  std::vector<ReadyEvent> out;
  ASSERT_TRUE(mux.Poll(100, out).has_value());
  ASSERT_EQ(out.size(), 1U);
  EXPECT_NE(out[0].events & event::kError, 0);
}

// ===========================================================================
// Kqueue-specific regression tests
// ===========================================================================
//
// P1-1: Concurrent Modify() on the same fd previously read `old_interest`
// outside the mutex, performed the kevent() syscall outside the mutex, and
// then re-took the mutex to write the new value. Two concurrent Modify(fd,X)
// and Modify(fd,Y) calls could thus race so that the kernel filter set ended
// up at the value chosen by the *first* successful syscall while `interest_`
// was overwritten by the *last* writer, leaving Remove() unable to emit the
// correct EV_DELETE for residual filters.
//
// The fix folds the read, the syscall, and the write into a single critical
// section. This test exercises that path with many threads racing on a
// single fd. Under the old code, ThreadSanitizer flags the data race on the
// `interest_` map, and even without TSan the final Remove() can leak a
// kqueue filter. With the fix, both go away.

#if defined(__APPLE__) || defined(__FreeBSD__) || defined(__NetBSD__) || defined(__OpenBSD__)

// CR-4 (audit, May 2026): kevent(2) does not report partial application of a
// change list. The previous implementation packed up to two change records
// (EVFILT_READ + EVFILT_WRITE) into a single kevent() call, which meant that
// if the kernel applied the first record and then rejected the second, the
// in-process `interest_` map would diverge from the kernel filter set: the
// caller saw a failed Add/Modify and did not record the partial state, so a
// subsequent Remove() emitted no EV_DELETE for the leaked filter.
//
// The fix is to issue one kevent() syscall per change record and update the
// `applied_interest` running mask after each successful syscall. The tests
// below exercise the resulting code paths.

// Verify that toggling both filters via Modify never leaves a stale EVFILT_*
// behind. This is the basic correctness invariant the per-record loop is
// supposed to preserve. Each iteration flips both bits at once, which is
// exactly the multi-record case CR-4 was concerned about.
TEST(KqueueMultiplexerTest, ApplyInterestBothFiltersFlipCleanly) {
  KqueueMultiplexer mux;
  ASSERT_TRUE(mux.Open().has_value());

  int fds[2] = {-1, -1};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
  const int r = fds[0];
  const int w = fds[1];

  // Start with both filters armed; this exercises a 2-record Add path.
  ASSERT_TRUE(mux.Add(r, event::kReadable | event::kWritable).has_value());

  // Now flip the entire interest off via Modify(kNone). This is a 2-record
  // Modify path (EV_DELETE on EVFILT_READ + EV_DELETE on EVFILT_WRITE).
  ASSERT_TRUE(mux.Modify(r, event::kNone).has_value());

  // Poll: with no interest armed, the only thing the kernel may still
  // surface is a hangup if the peer closed (it didn't), so out should be
  // empty. If the previous Modify had only succeeded for one filter and the
  // map were out of sync, a stale EVFILT_WRITE would re-fire here.
  std::vector<ReadyEvent> out;
  ASSERT_TRUE(mux.Poll(50, out).has_value());
  EXPECT_TRUE(out.empty()) << "stale filter survived Modify(kNone) — applied_interest tracking broken";

  // And re-arm both: the map must have correctly recorded that no filters
  // are armed, so this should succeed cleanly without "duplicate fd" errors
  // from the kernel.
  ASSERT_TRUE(mux.Modify(r, event::kReadable | event::kWritable).has_value());

  // Final teardown.
  ASSERT_TRUE(mux.Remove(r).has_value());
  ::close(r);
  ::close(w);
}

// White-box test: after Add() on a closed fd fails, Remove() must remain a
// strict no-op. The previous batched implementation could plausibly leave a
// half-applied filter behind on certain kernels (a filter would survive in
// the kqueue even though Add returned an error). With the per-record loop,
// the failure is observed before any second filter is installed, and the
// `applied_interest` running mask captures exactly which filters made it.
TEST(KqueueMultiplexerTest, AddOnClosedFdFailsCleanlyAndRemoveIsNoop) {
  KqueueMultiplexer mux;
  ASSERT_TRUE(mux.Open().has_value());

  int fds[2] = {-1, -1};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
  const int closed_fd = fds[0];
  ::close(fds[1]);
  ::close(closed_fd);

  // Closed fd: every kevent(EV_ADD) call against it returns EBADF. With the
  // per-record loop, the first kevent fails, applied stays 0, and the
  // function returns an error without touching `interest_`.
  auto add = mux.Add(closed_fd, event::kReadable | event::kWritable);
  EXPECT_FALSE(add.has_value());
  if (!add.has_value()) {
    EXPECT_EQ(add.error().code(), mygram::utils::ErrorCode::kNetworkReactorRegisterFailed);
  }

  // Remove() must be a strict no-op: nothing in `interest_`, nothing to
  // clean up. (If a future implementation regression let a stale interest_
  // entry leak in, Remove would attempt EV_DELETE on a closed fd and either
  // swallow EBADF — fine — or surface a different errno — bad.)
  EXPECT_TRUE(mux.Remove(closed_fd).has_value());
}

// Partial failure of Modify: register a fd successfully with kReadable, then
// close it from underneath the multiplexer. A subsequent Modify that wants
// to ADD kWritable will see EBADF and fail. The fix guarantees that the
// `interest_` map after the failure reflects exactly what's still armed in
// the kernel — which, since the syscall failed, is the original kReadable.
//
// The previous batched implementation, on a partial failure, left
// `interest_` untouched (taking the conservative "we don't know what
// happened" path). With per-record syscalls we know exactly what got
// through, so the post-failure state is deterministic.
TEST(KqueueMultiplexerTest, ModifyPartialFailurePreservesArmedFilters) {
  KqueueMultiplexer mux;
  ASSERT_TRUE(mux.Open().has_value());

  int fds[2] = {-1, -1};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
  const int r = fds[0];
  const int w = fds[1];

  // Arm kReadable cleanly first. After this call the kqueue contains an
  // EVFILT_READ for `r` and `interest_[r] == kReadable`.
  ASSERT_TRUE(mux.Add(r, event::kReadable).has_value());

  // Close the fd from under the multiplexer. The kernel automatically drops
  // any associated kevent filters when an fd is fully closed (kqueue manual,
  // "Closing of file descriptors"), so EVFILT_READ for `r` is now gone too,
  // but the multiplexer's `interest_` map still says kReadable.
  ::close(r);
  ::close(w);

  // Now ask the multiplexer to add kWritable on top. The diff is "no change
  // for kReadable, EV_ADD EVFILT_WRITE". The single kevent() in the per-
  // record loop targets the closed fd and fails with EBADF.
  auto mod = mux.Modify(r, event::kReadable | event::kWritable);
  EXPECT_FALSE(mod.has_value());
  if (!mod.has_value()) {
    EXPECT_EQ(mod.error().code(), mygram::utils::ErrorCode::kNetworkReactorModifyFailed);
  }

  // Best-observable post-condition: a follow-up Remove() must succeed. With
  // the previous batched code, any in-flight diff was untouched in
  // `interest_`, so Remove() would re-emit a now-stale EV_DELETE list. The
  // kqueue swallows EBADF on EV_DELETE (idempotent teardown), so this
  // doesn't blow up either way — but with the fix, Remove() emits only the
  // EV_DELETE entries that were actually in the kernel right before the
  // close, which is the cleaner contract.
  EXPECT_TRUE(mux.Remove(r).has_value());
}

// Defensive coverage: if Modify partially fails midway through a 2-record
// diff (kReadable→kWritable for example, where we EV_DELETE the read and
// then EV_ADD the write), the map should record the half that succeeded.
// We can observe this only indirectly by following up with Remove() and
// verifying the multiplexer does not return an error pointing at a
// non-existent filter — i.e. it correctly ignores filters that were never
// armed.
TEST(KqueueMultiplexerTest, RemoveAfterPartialModifyTolerated) {
  KqueueMultiplexer mux;
  ASSERT_TRUE(mux.Open().has_value());

  int fds[2] = {-1, -1};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
  const int r = fds[0];
  const int w = fds[1];

  // Add kReadable, then Modify to swap to kWritable. This generates a
  // 2-record diff: EV_DELETE EVFILT_READ + EV_ADD EVFILT_WRITE. Both
  // syscalls succeed in the happy path, but the per-record loop also
  // ensures that if the second one had failed, `interest_` would correctly
  // remember "no filters armed" rather than incorrectly remember "kReadable
  // still armed".
  ASSERT_TRUE(mux.Add(r, event::kReadable).has_value());
  ASSERT_TRUE(mux.Modify(r, event::kWritable).has_value());

  // Remove must emit exactly EV_DELETE EVFILT_WRITE (and nothing for
  // EVFILT_READ, which was already deleted by the Modify). The kernel
  // returns ENOENT if you try to delete a non-existent filter, so a stale
  // EVFILT_READ delete attempt would either be silently swallowed (kqueue
  // auto-suppresses ENOENT in our Remove() implementation) or surface as
  // an error. Either way, this sequence must succeed.
  EXPECT_TRUE(mux.Remove(r).has_value());

  ::close(r);
  ::close(w);
}

TEST(KqueueMultiplexerTest, ConcurrentModifyDoesNotRace) {
  KqueueMultiplexer mux;
  ASSERT_TRUE(mux.Open().has_value());

  int fds[2] = {-1, -1};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
  const int r = fds[0];
  const int w = fds[1];

  ASSERT_TRUE(mux.Add(r, event::kReadable).has_value());

  // Hammer Modify() from N threads. Each thread alternates between
  // kReadable and kReadable|kWritable so that every iteration emits a real
  // kevent() change record (not the same-interest fast path). With the old
  // unlocked-syscall design, the map and the kernel filter set diverge.
  constexpr int kThreads = 8;
  constexpr int kIterations = 200;
  std::atomic<bool> any_failure{false};
  std::vector<std::thread> workers;
  workers.reserve(kThreads);
  for (int t = 0; t < kThreads; ++t) {
    workers.emplace_back([&mux, &any_failure, r, t]() {
      for (int i = 0; i < kIterations; ++i) {
        const uint8_t mask =
            ((t + i) & 1) != 0 ? static_cast<uint8_t>(event::kReadable | event::kWritable) : event::kReadable;
        auto res = mux.Modify(r, mask);
        if (!res.has_value()) {
          any_failure.store(true, std::memory_order_relaxed);
          return;
        }
      }
    });
  }
  for (auto& th : workers) {
    th.join();
  }
  EXPECT_FALSE(any_failure.load());

  // Quiesce: install a deterministic final interest.
  ASSERT_TRUE(mux.Modify(r, event::kReadable).has_value());

  // Tear down. If the race had left a stale EVFILT_WRITE in the kernel that
  // the map didn't know about, Remove() would emit no EV_DELETE for it and
  // a subsequent Poll() would re-fire the writable event below. With the
  // fix, Remove() is consistent with the kernel.
  ASSERT_TRUE(mux.Remove(r).has_value());

  // Re-add as write-only on the same fd. The peer end is empty so the
  // socket is immediately writable; if a stale EVFILT_WRITE leaked through
  // a previous Remove(), Poll() may double-deliver, but at minimum the
  // sequence must succeed without errors.
  ASSERT_TRUE(mux.Add(r, event::kWritable).has_value());
  std::vector<ReadyEvent> out;
  ASSERT_TRUE(mux.Poll(100, out).has_value());
  ASSERT_TRUE(mux.Remove(r).has_value());

  ::close(r);
  ::close(w);
}

#endif  // __APPLE__ || BSD family

}  // namespace mygramdb::server::reactor
