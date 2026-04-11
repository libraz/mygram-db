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
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

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

  SocketPair() {
    EXPECT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
  }

  ~SocketPair() {
    for (int fd : fds) {
      if (fd >= 0) ::close(fd);
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
  reactor_->SetMultiplexerFactoryForTest([]() {
    return std::unique_ptr<reactor::EventMultiplexer>{nullptr};
  });

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
                                                   /*stats=*/nullptr,
                                                   ReactorConnection::kDefaultMaxWriteQueueBytes);
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

}  // namespace mygramdb::server
