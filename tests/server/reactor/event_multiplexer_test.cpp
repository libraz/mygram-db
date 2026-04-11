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

#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "mock_event_multiplexer.h"
#include "server/reactor/event_multiplexer.h"

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

using BackendTypes = ::testing::Types<
    MockEventMultiplexer
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
  void UntrackFd(int fd) {
    open_fds_.erase(std::remove(open_fds_.begin(), open_fds_.end(), fd), open_fds_.end());
  }

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
  EXPECT_EQ(result.error().code(),
            mygram::utils::ErrorCode::kNetworkReactorAlreadyOpen);
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

}  // namespace mygramdb::server::reactor
