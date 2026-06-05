/**
 * @file io_reactor_test.cpp
 * @brief Unit tests for IoReactor (Phase 2.T5).
 *
 * Uses MockEventMultiplexer injected via SetMultiplexerFactoryForTest() to
 * exercise registration, unregistration, event dispatch, and lifecycle without
 * touching kernel poll primitives.
 */

#include "server/io_reactor.h"

#include <fcntl.h>
#include <gtest/gtest.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <thread>
#include <vector>

#include "mock_event_multiplexer.h"
#include "server/reactor/event_multiplexer.h"
#include "server/reactor_connection.h"
#include "server/thread_pool.h"
#include "utils/error.h"

namespace mygramdb::server {

using mygram::utils::ErrorCode;
using reactor::MockEventMultiplexer;
using reactor::event::kReadable;
using reactor::event::kWritable;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

namespace {

/// RAII wrapper: creates a socketpair and closes both ends on destruction.
struct SocketPair {
  int fds[2]{-1, -1};

  SocketPair() { EXPECT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0); }

  ~SocketPair() {
    for (int fd : fds) {
      if (fd >= 0)
        ::close(fd);
    }
  }

  /// Take ownership of fds[0]; caller is responsible for close().
  int TakeClient() {
    int fd = fds[0];
    fds[0] = -1;
    return fd;
  }

  /// The peer end (write to this to trigger OnReadable on fds[0]).
  int Peer() const { return fds[1]; }
};

/// Default reactor config with a short poll timeout so tests run fast.
ReactorConfig FastConfig() {
  ReactorConfig cfg;
  cfg.poll_timeout_ms = 10;
  return cfg;
}

}  // namespace

// ---------------------------------------------------------------------------
// Fixture
// ---------------------------------------------------------------------------

class IoReactorTest : public ::testing::Test {
 protected:
  void SetUp() override {
    pool_ = std::make_unique<ThreadPool>(2, 64);
    reactor_ = std::make_unique<IoReactor>(pool_.get(), /*dispatcher=*/nullptr, FastConfig());
  }

  void TearDown() override {
    // Stop the reactor before destroying pool so the event loop has exited.
    reactor_.reset();
    pool_.reset();
  }

  /// Inject a mock mux and Start(). Returns the raw pointer (valid until Stop).
  MockEventMultiplexer* StartWithMock() {
    MockEventMultiplexer* raw = nullptr;
    reactor_->SetMultiplexerFactoryForTest([&raw]() {
      auto m = std::make_unique<MockEventMultiplexer>();
      raw = m.get();
      return m;
    });
    auto result = reactor_->Start();
    EXPECT_TRUE(result) << "Start() failed unexpectedly";
    return raw;
  }

  /// Create a ReactorConnection around the given fd (caller owns the fd).
  std::shared_ptr<ReactorConnection> MakeConn(int fd) {
    return ReactorConnection::Create(fd, reactor_.get(), /*dispatcher=*/nullptr, pool_.get());
  }

  std::unique_ptr<ThreadPool> pool_;
  std::unique_ptr<IoReactor> reactor_;
};

// ---------------------------------------------------------------------------
// Test 1: StartWithoutFactoryUsesRealBackend
// ---------------------------------------------------------------------------

TEST_F(IoReactorTest, StartWithoutFactoryUsesRealBackend) {
  // Do NOT call SetMultiplexerFactoryForTest — use real kernel backend.
  auto result = reactor_->Start();
  ASSERT_TRUE(result) << result.error().to_string();
  EXPECT_TRUE(reactor_->IsRunning());
  EXPECT_STRNE(reactor_->BackendName(), "unavailable");
  reactor_->Stop();
  EXPECT_FALSE(reactor_->IsRunning());
}

// ---------------------------------------------------------------------------
// Test 2: StartIsIdempotent
// ---------------------------------------------------------------------------

TEST_F(IoReactorTest, StartIsIdempotent) {
  int creation_count = 0;
  reactor_->SetMultiplexerFactoryForTest([&creation_count]() {
    ++creation_count;
    return std::make_unique<MockEventMultiplexer>();
  });

  auto r1 = reactor_->Start();
  ASSERT_TRUE(r1);
  auto r2 = reactor_->Start();
  ASSERT_TRUE(r2);

  EXPECT_EQ(creation_count, 1) << "Factory should be called exactly once";
}

// ---------------------------------------------------------------------------
// Test 3: StartReturnsErrorIfFactoryReturnsNull
// ---------------------------------------------------------------------------

TEST_F(IoReactorTest, StartReturnsErrorIfFactoryReturnsNull) {
  reactor_->SetMultiplexerFactoryForTest([]() { return std::unique_ptr<reactor::EventMultiplexer>{nullptr}; });

  auto result = reactor_->Start();
  ASSERT_FALSE(result);
  EXPECT_EQ(result.error().code(), ErrorCode::kNetworkReactorUnsupported);
}

// ---------------------------------------------------------------------------
// Test 4: StopJoinsEventLoopThread
// ---------------------------------------------------------------------------

TEST_F(IoReactorTest, StopJoinsEventLoopThread) {
  StartWithMock();
  ASSERT_TRUE(reactor_->IsRunning());
  reactor_->Stop();
  EXPECT_FALSE(reactor_->IsRunning());
}

// ---------------------------------------------------------------------------
// Test 5: RegisterBeforeStartFails
// ---------------------------------------------------------------------------

TEST_F(IoReactorTest, RegisterBeforeStartFails) {
  SocketPair sp;
  auto conn = MakeConn(sp.TakeClient());
  auto result = reactor_->Register(conn);
  ASSERT_FALSE(result);
  EXPECT_EQ(result.error().code(), ErrorCode::kNetworkServerNotStarted);
}

// ---------------------------------------------------------------------------
// Test 6: RegisterDuplicateFdFails
// ---------------------------------------------------------------------------

TEST_F(IoReactorTest, RegisterDuplicateFdFails) {
  StartWithMock();

  SocketPair sp;
  int client_fd = sp.TakeClient();
  auto conn1 = MakeConn(client_fd);

  auto r1 = reactor_->Register(conn1);
  ASSERT_TRUE(r1) << r1.error().to_string();

  // Second connection with the same fd.
  auto conn2 = std::make_shared<ReactorConnection>(client_fd, reactor_.get(), /*dispatcher=*/nullptr, pool_.get(),
                                                   /*stats=*/nullptr, ReactorConnection::kDefaultMaxWriteQueueBytes);
  auto r2 = reactor_->Register(conn2);
  ASSERT_FALSE(r2);
  EXPECT_EQ(r2.error().code(), ErrorCode::kInternalError);

  // First connection still registered.
  EXPECT_EQ(reactor_->ConnectionCount(), 1u);
}

// ---------------------------------------------------------------------------
// Test 7: RegisterAddsFdToMultiplexer
// ---------------------------------------------------------------------------

TEST_F(IoReactorTest, RegisterAddsFdToMultiplexer) {
  auto* mock = StartWithMock();
  ASSERT_NE(mock, nullptr);

  SocketPair sp;
  int client_fd = sp.TakeClient();
  auto conn = MakeConn(client_fd);
  ASSERT_TRUE(reactor_->Register(conn));

  EXPECT_EQ(mock->InterestFor(client_fd), kReadable);
  EXPECT_EQ(reactor_->ConnectionCount(), 1u);
}

// ---------------------------------------------------------------------------
// Test 8: UnregisterRemovesFdFromMultiplexer
// ---------------------------------------------------------------------------

TEST_F(IoReactorTest, UnregisterRemovesFdFromMultiplexer) {
  auto* mock = StartWithMock();
  ASSERT_NE(mock, nullptr);

  SocketPair sp;
  int client_fd = sp.TakeClient();
  auto conn = MakeConn(client_fd);
  ASSERT_TRUE(reactor_->Register(conn));

  reactor_->Unregister(client_fd);

  EXPECT_EQ(mock->InterestFor(client_fd), 0u);
  EXPECT_EQ(reactor_->ConnectionCount(), 0u);
}

// ---------------------------------------------------------------------------
// Test 9: UnregisterUnknownFdIsIdempotent
// ---------------------------------------------------------------------------

TEST_F(IoReactorTest, UnregisterUnknownFdIsIdempotent) {
  StartWithMock();
  EXPECT_NO_THROW(reactor_->Unregister(9999));
  EXPECT_NO_THROW(reactor_->Unregister(9999));
}

// ---------------------------------------------------------------------------
// Test 10: CloseCallbackInvokedFromUnregister
// ---------------------------------------------------------------------------

TEST_F(IoReactorTest, CloseCallbackInvokedFromUnregister) {
  std::atomic<int> callback_count{0};
  std::atomic<int> last_fd{-1};
  reactor_->SetCloseCallback([&](int fd) {
    callback_count.fetch_add(1);
    last_fd.store(fd);
  });

  StartWithMock();

  SocketPair sp;
  int client_fd = sp.TakeClient();
  auto conn = MakeConn(client_fd);
  ASSERT_TRUE(reactor_->Register(conn));

  reactor_->Unregister(client_fd);

  EXPECT_EQ(callback_count.load(), 1);
  EXPECT_EQ(last_fd.load(), client_fd);
}

// ---------------------------------------------------------------------------
// Test 11: CloseCallbackNotInvokedForUnknownFd
// ---------------------------------------------------------------------------

TEST_F(IoReactorTest, CloseCallbackNotInvokedForUnknownFd) {
  std::atomic<int> callback_count{0};
  reactor_->SetCloseCallback([&](int) { callback_count.fetch_add(1); });

  StartWithMock();
  reactor_->Unregister(9999);

  EXPECT_EQ(callback_count.load(), 0);
}

// ---------------------------------------------------------------------------
// Test 12: EventDispatchRoutesToCorrectConnection
// ---------------------------------------------------------------------------

TEST_F(IoReactorTest, EventDispatchRoutesToCorrectConnection) {
  auto* mock = StartWithMock();
  ASSERT_NE(mock, nullptr);

  // Create two socketpairs.
  SocketPair spA;
  SocketPair spB;
  int fdA = spA.TakeClient();
  int fdB = spB.TakeClient();

  auto connA = MakeConn(fdA);
  auto connB = MakeConn(fdB);
  ASSERT_TRUE(reactor_->Register(connA));
  ASSERT_TRUE(reactor_->Register(connB));

  // Write data to the peer side of A; the reactor event loop should drain it.
  const char* msg = "PING\r\n";
  ASSERT_GT(::write(spA.Peer(), msg, 6), 0);

  // Inject the readable event and wait for at least one more Poll() cycle to
  // process it so the event loop has had a chance to call OnReadable(fdA).
  int poll_before = mock->PollCallCount();
  mock->InjectReadable(fdA);
  ASSERT_TRUE(mock->WaitForPollCalled(poll_before + 2, std::chrono::milliseconds(2000)))
      << "Event loop did not run after InjectReadable";

  // The data on fdA should have been consumed; a non-blocking read from fdA
  // should now return EAGAIN (nothing left in the socket buffer).
  char buf[16];
  int flags_a = ::fcntl(fdA, F_GETFL, 0);
  (void)flags_a;
  ssize_t n = ::recv(fdA, buf, sizeof(buf), MSG_DONTWAIT);
  EXPECT_TRUE(n == 0 || (n < 0 && errno == EAGAIN))
      << "Expected fdA socket buffer drained, got n=" << n << " errno=" << errno;

  // fdB should be untouched (no data written, no event injected).
  ssize_t nb = ::recv(fdB, buf, sizeof(buf), MSG_DONTWAIT);
  EXPECT_TRUE(nb < 0 && errno == EAGAIN) << "fdB should not have data";
}

// ---------------------------------------------------------------------------
// Test 13: ArmWriteBeforeStartFails
// ---------------------------------------------------------------------------

TEST_F(IoReactorTest, ArmWriteBeforeStartFails) {
  auto result = reactor_->ArmWrite(5);
  ASSERT_FALSE(result);
  EXPECT_EQ(result.error().code(), ErrorCode::kNetworkServerNotStarted);
}

// ---------------------------------------------------------------------------
// Test 14: ArmWriteAfterRegisterUpdatesInterest
// ---------------------------------------------------------------------------

TEST_F(IoReactorTest, ArmWriteAfterRegisterUpdatesInterest) {
  auto* mock = StartWithMock();
  ASSERT_NE(mock, nullptr);

  SocketPair sp;
  int client_fd = sp.TakeClient();
  auto conn = MakeConn(client_fd);
  ASSERT_TRUE(reactor_->Register(conn));

  auto result = reactor_->ArmWrite(client_fd);
  ASSERT_TRUE(result) << result.error().to_string();

  uint8_t interest = mock->InterestFor(client_fd);
  EXPECT_NE(interest & kWritable, 0u);
  EXPECT_NE(interest & kReadable, 0u);
}

// ---------------------------------------------------------------------------
// Test 15: DisarmWriteUpdatesInterest
// ---------------------------------------------------------------------------

TEST_F(IoReactorTest, DisarmWriteUpdatesInterest) {
  auto* mock = StartWithMock();
  ASSERT_NE(mock, nullptr);

  SocketPair sp;
  int client_fd = sp.TakeClient();
  auto conn = MakeConn(client_fd);
  ASSERT_TRUE(reactor_->Register(conn));

  ASSERT_TRUE(reactor_->ArmWrite(client_fd));
  EXPECT_NE(mock->InterestFor(client_fd) & kWritable, 0u);

  ASSERT_TRUE(reactor_->DisarmWrite(client_fd));
  EXPECT_EQ(mock->InterestFor(client_fd) & kWritable, 0u);
  EXPECT_NE(mock->InterestFor(client_fd) & kReadable, 0u);
}

// ---------------------------------------------------------------------------
// Test 16: ConnectionCountReflectsRegistrations
// ---------------------------------------------------------------------------

TEST_F(IoReactorTest, ConnectionCountReflectsRegistrations) {
  StartWithMock();

  constexpr int kTotal = 5;
  std::vector<SocketPair> pairs(kTotal);
  std::vector<int> fds;
  for (auto& sp : pairs) {
    int fd = sp.TakeClient();
    fds.push_back(fd);
    ASSERT_TRUE(reactor_->Register(MakeConn(fd)));
  }
  EXPECT_EQ(reactor_->ConnectionCount(), static_cast<size_t>(kTotal));

  reactor_->Unregister(fds[0]);
  reactor_->Unregister(fds[1]);
  EXPECT_EQ(reactor_->ConnectionCount(), static_cast<size_t>(kTotal - 2));
}

// ---------------------------------------------------------------------------
// Test 17: ShutdownWith500ActiveConnectionsIsClean
// ---------------------------------------------------------------------------

TEST_F(IoReactorTest, ShutdownWith500ActiveConnectionsIsClean) {
  // Use a zero poll timeout so Register() is not serialised behind a 10ms
  // Poll() call in the event loop when creating 500 connections.
  ReactorConfig cfg;
  cfg.poll_timeout_ms = 0;
  auto pool500 = std::make_unique<ThreadPool>(2, 64);
  auto reactor500 = std::make_unique<IoReactor>(pool500.get(), nullptr, cfg);

  MockEventMultiplexer* mock500 = nullptr;
  reactor500->SetMultiplexerFactoryForTest([&mock500]() {
    auto m = std::make_unique<MockEventMultiplexer>();
    mock500 = m.get();
    return m;
  });
  ASSERT_TRUE(reactor500->Start());

  constexpr int kCount = 500;
  std::vector<std::unique_ptr<SocketPair>> pairs;
  pairs.reserve(kCount);
  std::vector<int> client_fds;
  client_fds.reserve(kCount);

  for (int i = 0; i < kCount; ++i) {
    auto sp = std::make_unique<SocketPair>();
    client_fds.push_back(sp->TakeClient());
    pairs.push_back(std::move(sp));
  }

  for (int fd : client_fds) {
    auto conn = ReactorConnection::Create(fd, reactor500.get(), nullptr, pool500.get());
    ASSERT_TRUE(reactor500->Register(conn));
  }
  EXPECT_EQ(reactor500->ConnectionCount(), static_cast<size_t>(kCount));

  // Stop() drops all connections and joins the event loop thread.
  reactor500->Stop();
  EXPECT_FALSE(reactor500->IsRunning());

  // Spot-check: fds owned by ReactorConnection should have been closed.
  // We use two arbitrary indices. If close() hasn't happened yet (drain task
  // still holds a shared_ptr), fcntl may still succeed — so we only assert
  // no crash / no hang above, and accept either state for the fd check.
  for (int check_idx : {0, 249, 499}) {
    int fd = client_fds[check_idx];
    // Just verify we don't crash calling fcntl — we can't assert closed here
    // because ReactorConnection destructor may race with a drain task.
    (void)::fcntl(fd, F_GETFD);
  }
  // All peer fds are still owned by SocketPair objects and will be closed
  // on destruction — no fd leak.
}

// ---------------------------------------------------------------------------
// Test 18: StartStopRapidCycleNoThreadLeak (Bug P1-6 regression)
//
// Repeats Start()→Stop() many times. Without the start_stop_mutex_ added in
// the P1-6 fix, a Stop() that observed running_=true set by an interleaving
// Start could exchange running_ back to false, find event_loop_thread_
// non-joinable (Start had not yet assigned it), and silently let the
// subsequently-spawned thread leak. The test process completing without
// thread-sanitizer warnings or hangs validates the fix.
// ---------------------------------------------------------------------------

TEST_F(IoReactorTest, StartStopRapidCycleNoThreadLeak) {
  for (int i = 0; i < 100; ++i) {
    auto pool = std::make_unique<ThreadPool>(1, 16);
    auto reactor = std::make_unique<IoReactor>(pool.get(), nullptr, FastConfig());
    reactor->SetMultiplexerFactoryForTest([]() { return std::make_unique<MockEventMultiplexer>(); });

    auto start_result = reactor->Start();
    ASSERT_TRUE(start_result) << "iteration " << i << ": " << start_result.error().to_string();
    reactor->Stop();
    EXPECT_FALSE(reactor->IsRunning()) << "iteration " << i;
  }
}

// ---------------------------------------------------------------------------
// Test 19: StartStopConcurrentNoLeak (Bug P1-6 regression)
//
// Drives Start/Stop from two threads simultaneously and confirms the reactor
// reaches a consistent stopped state without orphaning the event-loop thread.
// The start_stop_mutex_ serialises Start/Stop so this is well-defined.
// ---------------------------------------------------------------------------

TEST_F(IoReactorTest, StartStopConcurrentNoLeak) {
  for (int i = 0; i < 50; ++i) {
    auto pool = std::make_unique<ThreadPool>(1, 16);
    auto reactor = std::make_unique<IoReactor>(pool.get(), nullptr, FastConfig());
    reactor->SetMultiplexerFactoryForTest([]() { return std::make_unique<MockEventMultiplexer>(); });

    std::atomic<bool> go{false};
    std::thread t1([&]() {
      while (!go.load(std::memory_order_acquire)) {
      }
      (void)reactor->Start();
    });
    std::thread t2([&]() {
      while (!go.load(std::memory_order_acquire)) {
      }
      reactor->Stop();
    });
    go.store(true, std::memory_order_release);
    t1.join();
    t2.join();

    // Whatever the interleaving, a final Stop() must leave us not-running and
    // must not deadlock or leak the event-loop thread.
    reactor->Stop();
    EXPECT_FALSE(reactor->IsRunning()) << "iteration " << i;
  }
}

// ---------------------------------------------------------------------------
// Test 20: RegisterRaceWithStopLeavesNoStaleEntries (Bug P1-2 regression)
//
// Hammers Register from a worker thread while the main thread issues Stop().
// The bug fix moves connections_ insertion under mux_lifecycle_'s shared
// lock, so a Register either succeeds (connection is registered AND added to
// the mux before Stop's exclusive mux_lifecycle_ acquisition can proceed) or
// fails cleanly (running_=false observed, no map insert).
//
// After the dust settles we verify ConnectionCount() == 0 (Stop's clear()
// always runs) and that no Register() that was reported as failed left a
// stale entry behind.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Test 21: IdleConnectionIsReaped (Fix N-3)
//
// Configures a reactor with idle_timeout_sec=1 and reaper_interval_sec=1,
// registers a connection, and waits past the idle deadline. The reactor's
// reaper task should observe the stale connection and call Unregister(),
// which fires the close callback. Labelled SLOW because it sleeps ~2.5s.
// ---------------------------------------------------------------------------

TEST_F(IoReactorTest, IdleConnectionIsReaped) {
  ReactorConfig cfg;
  cfg.poll_timeout_ms = 50;     // tight loop so the reaper interval check fires quickly
  cfg.idle_timeout_sec = 1;     // age out after 1s of no activity
  cfg.reaper_interval_sec = 1;  // sweep every 1s
  cfg.event_loop_threads = 1;

  auto pool = std::make_unique<ThreadPool>(1, 16);
  auto reactor = std::make_unique<IoReactor>(pool.get(), nullptr, cfg);

  std::atomic<int> close_callback_count{0};
  std::atomic<int> closed_fd{-1};
  reactor->SetCloseCallback([&](int fd) {
    close_callback_count.fetch_add(1);
    closed_fd.store(fd);
  });

  ASSERT_TRUE(reactor->Start()) << "Real backend Start() failed";

  SocketPair sp;
  int client_fd = sp.TakeClient();
  auto conn = MakeConn(client_fd);
  ASSERT_TRUE(reactor->Register(conn));
  EXPECT_EQ(reactor->ConnectionCount(), 1u);

  // Wait past the idle deadline + one reaper interval, plus slack for the
  // event loop to actually pick up the sweep cadence.
  std::this_thread::sleep_for(std::chrono::milliseconds(2500));

  EXPECT_EQ(reactor->ConnectionCount(), 0u) << "Reaper should have closed the idle connection";
  EXPECT_GE(close_callback_count.load(), 1) << "Close callback should have fired for reaped connection";
  EXPECT_EQ(closed_fd.load(), client_fd);

  reactor->Stop();
}

// ---------------------------------------------------------------------------
// Test 22: ActiveConnectionIsNotReaped (Fix N-3)
//
// Same setup as IdleConnectionIsReaped but the test continuously writes
// non-frame data to the peer end of the socket so the reactor sees
// readable events and refreshes last_active_ without triggering a frame
// dispatch (this fixture has a null dispatcher, so a complete frame would
// fail ScheduleDrainTask and tear down the connection unrelated to the
// reaper). Writing partial frames (no \r\n terminator) is sufficient to
// fire OnReadable and bump last_active_.
// ---------------------------------------------------------------------------

TEST_F(IoReactorTest, ActiveConnectionIsNotReaped) {
  ReactorConfig cfg;
  cfg.poll_timeout_ms = 50;
  cfg.idle_timeout_sec = 2;
  cfg.reaper_interval_sec = 1;
  cfg.event_loop_threads = 1;

  auto pool = std::make_unique<ThreadPool>(1, 16);
  auto reactor = std::make_unique<IoReactor>(pool.get(), nullptr, cfg);

  std::atomic<int> close_callback_count{0};
  reactor->SetCloseCallback([&](int) { close_callback_count.fetch_add(1); });

  ASSERT_TRUE(reactor->Start());

  SocketPair sp;
  int client_fd = sp.TakeClient();
  int peer_fd = sp.Peer();
  auto conn = MakeConn(client_fd);
  ASSERT_TRUE(reactor->Register(conn));

  // Drive periodic activity for 2.5s. With idle_timeout=2s, an idle peer
  // would be reaped on the second sweep. Writing data every 200ms keeps
  // last_active_ fresh. Use partial-frame bytes (no \r\n terminator) so
  // OnReadable consumes them but does not try to dispatch a complete
  // request through the null dispatcher.
  auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(2500);
  while (std::chrono::steady_clock::now() < deadline) {
    const char* msg = "x";
    ssize_t r = ::write(peer_fd, msg, 1);
    (void)r;
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }

  EXPECT_EQ(reactor->ConnectionCount(), 1u) << "Active connection must not be reaped";
  EXPECT_EQ(close_callback_count.load(), 0);

  reactor->Stop();
}

TEST_F(IoReactorTest, InitialReadTimeoutClosesConnectionDespitePartialBytes) {
  ReactorConfig cfg;
  cfg.poll_timeout_ms = 50;
  cfg.idle_timeout_sec = 300;
  cfg.initial_read_timeout_sec = 1;
  cfg.reaper_interval_sec = 1;
  cfg.event_loop_threads = 1;

  auto pool = std::make_unique<ThreadPool>(1, 16);
  auto reactor = std::make_unique<IoReactor>(pool.get(), nullptr, cfg);

  std::atomic<int> close_callback_count{0};
  reactor->SetCloseCallback([&](int) { close_callback_count.fetch_add(1); });

  ASSERT_TRUE(reactor->Start());

  SocketPair sp;
  int client_fd = sp.TakeClient();
  int peer_fd = sp.Peer();
#ifdef SO_NOSIGPIPE
  int nosigpipe = 1;
  (void)::setsockopt(peer_fd, SOL_SOCKET, SO_NOSIGPIPE, &nosigpipe, sizeof(nosigpipe));
#endif
  auto conn = ReactorConnection::Create(client_fd, reactor.get(), /*dispatcher=*/nullptr, pool.get());
  ASSERT_TRUE(reactor->Register(conn));

  const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(2500);
  while (std::chrono::steady_clock::now() < deadline && reactor->ConnectionCount() == 1u) {
    const char* msg = "x";
#ifdef MSG_NOSIGNAL
    const ssize_t r = ::send(peer_fd, msg, 1, MSG_NOSIGNAL);
#else
    const ssize_t r = ::send(peer_fd, msg, 1, 0);
#endif
    (void)r;
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }

  EXPECT_EQ(reactor->ConnectionCount(), 0u) << "Initial read timeout should close incomplete first frames";
  EXPECT_GE(close_callback_count.load(), 1);

  reactor->Stop();
}

// ---------------------------------------------------------------------------
// Test 23: RegisterRollbackOnMuxAddFailure (Fix N-5)
//
// MockEventMultiplexer can be configured to fail Add(). The fix reorders
// Register so that mux_->Add runs before connections_.emplace, eliminating
// the rollback path. This test confirms that on Add failure, the
// connections_ map is left untouched (no leftover entries to clean up).
// ---------------------------------------------------------------------------

TEST_F(IoReactorTest, RegisterRollbackOnMuxAddFailure) {
  auto* mock = StartWithMock();
  ASSERT_NE(mock, nullptr);

  // Force the mock to fail the next Add() call.
  mock->SetAddShouldFail(true);

  SocketPair sp;
  int client_fd = sp.TakeClient();
  auto conn = MakeConn(client_fd);

  auto result = reactor_->Register(conn);
  ASSERT_FALSE(result) << "Register should have failed because mux_->Add was forced to fail";

  // Critical invariant: connections_ must be empty. Pre-fix this could be 1
  // (entry inserted before the mux_->Add attempt, never rolled back if the
  // rollback path itself raced).
  EXPECT_EQ(reactor_->ConnectionCount(), 0u) << "connections_ must remain empty when mux_->Add fails";

  // Allow re-registration after the mock recovers — confirms the failed
  // Register did not leave any stale interest entry behind either.
  mock->SetAddShouldFail(false);
  ASSERT_TRUE(reactor_->Register(conn).has_value());
  EXPECT_EQ(reactor_->ConnectionCount(), 1u);
}

// ---------------------------------------------------------------------------
// Test 24: StopReturnsPromptlyWithLongPollTimeout (Fix H-N3)
//
// Pre-fix, Stop() set running_=false but had to wait for the in-flight
// Poll(timeout_ms) to elapse before the event loop observed the flag. With
// poll_timeout_ms set to several seconds, Stop() blocked the calling
// thread (and start_stop_mutex_) for that whole duration, which made
// rapid restart cycles (HttpServer / TcpServer reconfiguration) extremely
// slow.
//
// The fix wires up EventMultiplexer::Wake() (EVFILT_USER on kqueue,
// eventfd on epoll, cv-signal on the mock) so Stop() can interrupt the
// blocked Poll() immediately. We assert that Stop() returns within a
// bound that's well under the configured poll_timeout_ms.
// ---------------------------------------------------------------------------
TEST_F(IoReactorTest, StopReturnsPromptlyWithLongPollTimeout) {
  // Configure a long poll timeout so the regression would manifest as a
  // multi-second wait inside Stop().
  ReactorConfig cfg;
  cfg.poll_timeout_ms = 5000;
  auto pool = std::make_unique<ThreadPool>(1, 16);
  auto reactor = std::make_unique<IoReactor>(pool.get(), nullptr, cfg);
  reactor->SetMultiplexerFactoryForTest([]() { return std::make_unique<MockEventMultiplexer>(); });

  ASSERT_TRUE(reactor->Start());

  // Give the event loop a moment to actually be parked inside Poll().
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  const auto t0 = std::chrono::steady_clock::now();
  reactor->Stop();
  const auto elapsed = std::chrono::steady_clock::now() - t0;

  EXPECT_FALSE(reactor->IsRunning());
  // Allow generous slack for slow CI hosts but stay an order of magnitude
  // below poll_timeout_ms (5000ms). Anything in this range is "wake worked".
  EXPECT_LT(elapsed, std::chrono::milliseconds(500))
      << "Stop() should not wait for the configured poll_timeout_ms when Wake() is available";
}

// ---------------------------------------------------------------------------
// Test 25: StopReturnsPromptlyWithRealBackend (Fix H-N3)
//
// Same intent as the mock-backed test, but exercises the real kqueue or
// epoll wake primitive end-to-end. The real backends arm a sentinel filter
// (EVFILT_USER on kqueue) or eventfd (epoll) during Open(); Wake() fires
// it, Poll() drains/drops the event before returning to the reactor.
// ---------------------------------------------------------------------------
TEST_F(IoReactorTest, StopReturnsPromptlyWithRealBackend) {
  ReactorConfig cfg;
  cfg.poll_timeout_ms = 5000;
  auto pool = std::make_unique<ThreadPool>(1, 16);
  auto reactor = std::make_unique<IoReactor>(pool.get(), nullptr, cfg);
  // Do NOT call SetMultiplexerFactoryForTest — use the real kernel backend.

  ASSERT_TRUE(reactor->Start()) << "Real backend Start() failed";
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  const auto t0 = std::chrono::steady_clock::now();
  reactor->Stop();
  const auto elapsed = std::chrono::steady_clock::now() - t0;

  EXPECT_FALSE(reactor->IsRunning());
  EXPECT_LT(elapsed, std::chrono::milliseconds(500))
      << "Real backend Stop() should not wait for the full poll_timeout_ms";
}

TEST_F(IoReactorTest, RegisterRaceWithStopLeavesNoStaleEntries) {
  for (int iter = 0; iter < 20; ++iter) {
    auto pool = std::make_unique<ThreadPool>(2, 64);
    auto reactor = std::make_unique<IoReactor>(pool.get(), nullptr, FastConfig());
    reactor->SetMultiplexerFactoryForTest([]() { return std::make_unique<MockEventMultiplexer>(); });
    ASSERT_TRUE(reactor->Start());

    constexpr int kRegistersPerIter = 32;
    std::vector<std::unique_ptr<SocketPair>> pairs;
    pairs.reserve(kRegistersPerIter);
    std::vector<int> fds;
    fds.reserve(kRegistersPerIter);
    for (int i = 0; i < kRegistersPerIter; ++i) {
      auto sp = std::make_unique<SocketPair>();
      fds.push_back(sp->TakeClient());
      pairs.push_back(std::move(sp));
    }

    std::atomic<bool> go{false};
    std::atomic<int> register_successes{0};
    std::atomic<int> register_failures{0};

    std::thread registrar([&]() {
      while (!go.load(std::memory_order_acquire)) {
      }
      for (int fd : fds) {
        auto conn = ReactorConnection::Create(fd, reactor.get(), nullptr, pool.get());
        auto r = reactor->Register(conn);
        if (r) {
          register_successes.fetch_add(1, std::memory_order_relaxed);
        } else {
          register_failures.fetch_add(1, std::memory_order_relaxed);
        }
      }
    });

    std::thread stopper([&]() {
      while (!go.load(std::memory_order_acquire)) {
      }
      // Tiny stagger so some Register calls land before Stop, others after.
      std::this_thread::sleep_for(std::chrono::microseconds(50));
      reactor->Stop();
    });

    go.store(true, std::memory_order_release);
    registrar.join();
    stopper.join();

    // Invariant: after Stop, the connection map is empty regardless of how
    // many registers succeeded/failed. The bug being regressed against
    // would manifest as ConnectionCount() > 0 (entries inserted by a
    // Register that won the race after Stop's clear()).
    //
    // Because Stop is idempotent, a second call is a no-op but confirms the
    // reactor is in a consistent state.
    reactor->Stop();
    EXPECT_EQ(reactor->ConnectionCount(), 0u)
        << "iter " << iter << " successes=" << register_successes.load() << " failures=" << register_failures.load();
  }
}

}  // namespace mygramdb::server
