/**
 * @file reactor_integration_test.cpp
 * @brief Integration tests for the reactor I/O model against a real TcpServer.
 *
 * These tests exercise the end-to-end reactor path:
 *   config_.io_model == "reactor"
 *   -> IoReactor (epoll on Linux, kqueue on macOS)
 *   -> ReactorConnection per-fd drain-task pattern
 *   -> RequestDispatcher -> INFO handler -> response written on worker thread
 *
 * On platforms with no supported event multiplexer (not Linux, macOS, or
 * BSD), the fixture skips all tests at runtime via GTEST_SKIP().
 *
 * Labeled "INTEGRATION" — included in `ctest -L INTEGRATION`.
 */

// Platform guard: compile-time skip for unsupported platforms.
// macOS (kqueue) and Linux (epoll) are the two real targets.
#if !defined(__linux__) && !defined(__APPLE__) && !defined(__FreeBSD__) && !defined(__NetBSD__) && \
    !defined(__OpenBSD__)
#include <gtest/gtest.h>
// Provide a trivially-passing placeholder so the binary links cleanly.
TEST(ReactorIntegrationTest, PlatformNotSupported) {
  GTEST_SKIP() << "Reactor I/O model not supported on this platform (no epoll/kqueue).";
}
#else

#include <arpa/inet.h>
#include <fcntl.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <poll.h>
#include <spdlog/spdlog.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "config/config.h"
#include "index/index.h"
#include "server/server_types.h"
#include "server/tcp_server.h"
#include "storage/document_store.h"

namespace mygramdb::server {

// ============================================================================
// Test fixture
// ============================================================================

class ReactorIntegrationTest : public ::testing::Test {
 protected:
  void SetUp() override {
    spdlog::set_level(spdlog::level::debug);  // Enable debug logs for reactor diagnostics
    auto index = std::make_unique<index::Index>(2, 1);
    auto doc_store = std::make_unique<storage::DocumentStore>();
    table_ctx_ = std::make_unique<TableContext>(TableContext{
        .name = "t",
        .config = config::TableConfig{},
        .index = std::move(index),
        .doc_store = std::move(doc_store),
    });
    table_contexts_["t"] = table_ctx_.get();
  }

  void TearDown() override {
    table_contexts_.clear();
    table_ctx_.reset();
  }

  // -------------------------------------------------------------------------
  // Server helpers
  // -------------------------------------------------------------------------

  /**
   * @brief Start a TcpServer in reactor mode and return it.
   *
   * If the platform has no supported event multiplexer the reactor silently
   * falls back to blocking mode. Callers that need reactor mode specifically
   * should check `server->IsReactorActiveForTest()` and GTEST_SKIP if false.
   */
  std::unique_ptr<TcpServer> StartServer(int worker_threads = 8, int max_connections = 512,
                                          int64_t max_write_queue_bytes = 0) {
    ServerConfig cfg;
    cfg.host = "127.0.0.1";
    cfg.port = 0;  // OS assigns
    cfg.worker_threads = worker_threads;
    cfg.max_connections = max_connections;
    cfg.allow_cidrs = {"127.0.0.1/32"};
    cfg.io_model = "reactor";
    if (max_write_queue_bytes > 0) {
      cfg.max_write_queue_bytes = max_write_queue_bytes;
    }

    auto s = std::make_unique<TcpServer>(cfg, table_contexts_);
    auto res = s->Start();
    EXPECT_TRUE(res) << "Failed to start TcpServer: "
                     << (res ? std::string{} : res.error().to_string());
    // Give the accept loop a moment to reach its main loop.
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    return s;
  }

  /**
   * @brief Require reactor mode; skip the test if the server fell back to
   *        blocking (happens when no epoll/kqueue factory is available).
   */
  static void RequireReactor(const TcpServer& server) {
    if (!server.IsReactorActiveForTest()) {
      GTEST_SKIP() << "IoReactor is not active on this build/platform "
                      "(no epoll/kqueue available); skipping reactor-specific test.";
    }
  }

  // -------------------------------------------------------------------------
  // Network helpers — mirrors ThreadPoolSaturationTest for API consistency
  // -------------------------------------------------------------------------

  /**
   * @brief Connect to 127.0.0.1:port with a bounded timeout.
   * @return socket fd on success, -1 on failure.
   */
  int Connect(uint16_t port, int timeout_ms = 2000) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) return -1;

    int flags = fcntl(sock, F_GETFL, 0);
    if (flags >= 0) fcntl(sock, F_SETFL, flags | O_NONBLOCK);

    struct sockaddr_in addr {};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) - POSIX socket API
    int rc = connect(sock, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr));
    if (rc < 0 && errno != EINPROGRESS) {
      close(sock);
      return -1;
    }

    if (rc < 0) {
      // Use poll() rather than select() so this helper works past the
      // FD_SETSIZE cliff (1024 on macOS/glibc).  Tests that open many
      // concurrent sockets quickly exceed that cap.
      struct pollfd pfd {};
      pfd.fd = sock;
      pfd.events = POLLOUT;
      int ready = ::poll(&pfd, 1, timeout_ms);
      if (ready <= 0) {
        close(sock);
        return -1;
      }
      int so_err = 0;
      socklen_t len = sizeof(so_err);
      if (getsockopt(sock, SOL_SOCKET, SO_ERROR, &so_err, &len) < 0 || so_err != 0) {
        close(sock);
        return -1;
      }
    }

    // Restore flags; set I/O timeout for blocking reads
    if (flags >= 0) fcntl(sock, F_SETFL, flags);
    struct timeval io {};
    io.tv_sec = 5;
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &io, sizeof(io));
    setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &io, sizeof(io));
    return sock;
  }

  /** @brief Send `line` followed by CRLF. */
  bool SendLine(int sock, const std::string& line) {
    std::string msg = line + "\r\n";
    ssize_t sent = send(sock, msg.data(), msg.size(), 0);
    return sent == static_cast<ssize_t>(msg.size());
  }

  /**
   * @brief Read one CRLF-terminated response within timeout_ms.
   *
   * WARNING: this helper matches the first `\r\n` and discards anything after
   * it. It only works for single-line responses (e.g. `"OK DEBUG_ON\r\n"`).
   * For multi-line responses like INFO or SEARCH that contain internal `\r\n`
   * and end with `\nEND\r\n`, use `RecvMultilineResponse` instead.
   *
   * @return response without trailing CRLF, or empty string on timeout/error.
   */
  std::string RecvLine(int sock, int timeout_ms = 5000) {
    struct timeval io {};
    io.tv_sec = timeout_ms / 1000;
    io.tv_usec = (timeout_ms % 1000) * 1000;
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &io, sizeof(io));

    std::string out;
    std::array<char, 512> buf{};
    while (out.size() < 65536) {
      ssize_t n = recv(sock, buf.data(), buf.size(), 0);
      if (n <= 0) return "";
      out.append(buf.data(), static_cast<size_t>(n));
      if (out.find("\r\n") != std::string::npos) break;
    }
    auto pos = out.find("\r\n");
    if (pos != std::string::npos) out.resize(pos);
    return out;
  }

  /**
   * @brief Read a full multi-line response ending in `\r\nEND\r\n`.
   *
   * The MygramDB text protocol's INFO, SEARCH, and similar commands return
   * a stream of `\r\n`-separated lines followed by a final `END\r\n`. The
   * test must read the ENTIRE response before issuing the next request —
   * otherwise the next RecvLine will start reading in the middle of this
   * response.
   *
   * @return the full response bytes (including internal CRLFs and the
   *         trailing `\r\nEND\r\n`), or empty string on timeout/error.
   */
  std::string RecvMultilineResponse(int sock, int timeout_ms = 5000) {
    struct timeval io {};
    io.tv_sec = timeout_ms / 1000;
    io.tv_usec = (timeout_ms % 1000) * 1000;
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &io, sizeof(io));

    static constexpr const char kTerminator[] = "\r\nEND\r\n";
    static constexpr size_t kTerminatorLen = 7;

    std::string out;
    std::array<char, 1024> buf{};
    while (out.size() < 1 * 1024 * 1024) {
      ssize_t n = recv(sock, buf.data(), buf.size(), 0);
      if (n <= 0) return "";
      out.append(buf.data(), static_cast<size_t>(n));
      if (out.size() >= kTerminatorLen &&
          out.compare(out.size() - kTerminatorLen, kTerminatorLen, kTerminator) == 0) {
        return out;
      }
    }
    return "";
  }

  /**
   * @brief Raise RLIMIT_NOFILE to at least `desired`.
   * @return the effective soft limit after the attempt.
   */
  static rlim_t RaiseFdLimit(rlim_t desired) {
    struct rlimit rl {};
    if (getrlimit(RLIMIT_NOFILE, &rl) != 0) return 0;
    rlim_t target = std::min<rlim_t>(desired, rl.rlim_max);
    if (rl.rlim_cur < target) {
      rl.rlim_cur = target;
      setrlimit(RLIMIT_NOFILE, &rl);  // best-effort
      getrlimit(RLIMIT_NOFILE, &rl);
    }
    return rl.rlim_cur;
  }

  std::unique_ptr<TableContext> table_ctx_;
  std::unordered_map<std::string, TableContext*> table_contexts_;
};

// ============================================================================
// Test 1 — Basic INFO round-trip
// ============================================================================

/**
 * Start a reactor server, connect once, send INFO, get "OK INFO" response.
 */
TEST_F(ReactorIntegrationTest, InfoCommandRoundtrip) {
  auto server = StartServer();
  RequireReactor(*server);
  uint16_t port = server->GetPort();

  int s = Connect(port);
  ASSERT_GE(s, 0) << "connect() failed";
  ASSERT_TRUE(SendLine(s, "INFO"));
  std::string resp = RecvLine(s);
  EXPECT_EQ(resp.substr(0, 7), "OK INFO") << "Unexpected response: " << resp;
  close(s);
  server->Stop();
}

// ============================================================================
// Test 2 — 50 sequential commands on one connection
// ============================================================================

/**
 * The drain-task-per-connection invariant: a single connection processes
 * multiple commands in order.  50 sequential INFO commands on one fd must
 * all return "OK INFO" and in the order they were sent.
 */
TEST_F(ReactorIntegrationTest, MultipleSequentialCommands) {
  auto server = StartServer();
  RequireReactor(*server);
  uint16_t port = server->GetPort();

  int s = Connect(port);
  ASSERT_GE(s, 0);

  constexpr int kCount = 50;
  for (int i = 0; i < kCount; ++i) {
    ASSERT_TRUE(SendLine(s, "INFO")) << "send failed at i=" << i;
    std::string resp = RecvMultilineResponse(s);
    ASSERT_FALSE(resp.empty()) << "Command " << i << " timed out or got empty response";
    ASSERT_EQ(resp.substr(0, 7), "OK INFO") << "Command " << i << " got: " << resp.substr(0, 40);
  }

  close(s);
  server->Stop();
}

// ============================================================================
// Test 3 — Pipelining: 3 requests in one write(), read 3 responses
// ============================================================================

/**
 * Pipelining test: send "INFO\r\nINFO\r\nINFO\r\n" in a single write() and
 * read back 3 responses.  This exercises the ReactorConnection frame parser
 * handling multiple frames arriving in one recv() chunk.
 */
TEST_F(ReactorIntegrationTest, PersistentConnectionPipelining) {
  auto server = StartServer();
  RequireReactor(*server);
  uint16_t port = server->GetPort();

  int s = Connect(port);
  ASSERT_GE(s, 0);

  // Three INFO requests pipelined into one write
  const std::string burst = "INFO\r\nINFO\r\nINFO\r\n";
  ssize_t sent = send(s, burst.data(), burst.size(), 0);
  ASSERT_EQ(sent, static_cast<ssize_t>(burst.size())) << "partial send";

  // Read 3 responses
  for (int i = 0; i < 3; ++i) {
    std::string resp = RecvMultilineResponse(s, 5000);
    ASSERT_FALSE(resp.empty()) << "Pipelined response " << i << " timed out";
    EXPECT_EQ(resp.substr(0, 7), "OK INFO") << "Pipelined response " << i << ": " << resp.substr(0, 40);
  }

  close(s);
  server->Stop();
}

// ============================================================================
// Test 4 — 100 concurrent clients, 10 commands each
// ============================================================================

/**
 * 100 client threads, all releasing at the same moment (barrier), each
 * sending 10 INFO commands.  All 1000 requests must succeed.  No
 * SERVER_BUSY, no timeouts, no connection refused.
 */
TEST_F(ReactorIntegrationTest, ConcurrentClients100) {
  auto server = StartServer(/*worker_threads=*/8);
  RequireReactor(*server);
  uint16_t port = server->GetPort();

  constexpr int kClients = 100;
  constexpr int kCmdsPerClient = 10;

  std::atomic<int> failures{0};
  // C++17 manual barrier: all kClients threads wait until the last one arrives
  std::mutex barrier_mu;
  std::condition_variable barrier_cv;
  std::atomic<int> barrier_count{0};
  auto wait_for_all = [&]() {
    barrier_count.fetch_add(1, std::memory_order_acq_rel);
    std::unique_lock<std::mutex> lk(barrier_mu);
    barrier_cv.wait(lk, [&] { return barrier_count.load() >= kClients; });
    barrier_cv.notify_all();
  };

  auto client_task = [&]() {
    wait_for_all();

    int s = Connect(port, 3000);
    if (s < 0) {
      failures.fetch_add(1, std::memory_order_relaxed);
      return;
    }

    for (int i = 0; i < kCmdsPerClient; ++i) {
      if (!SendLine(s, "INFO")) {
        failures.fetch_add(1, std::memory_order_relaxed);
        close(s);
        return;
      }
      std::string resp = RecvMultilineResponse(s, 5000);
      if (resp.empty() || resp.substr(0, 7) != "OK INFO") {
        failures.fetch_add(1, std::memory_order_relaxed);
        close(s);
        return;
      }
    }
    close(s);
  };

  std::vector<std::thread> threads;
  threads.reserve(kClients);
  for (int i = 0; i < kClients; ++i) threads.emplace_back(client_task);
  for (auto& t : threads) t.join();

  EXPECT_EQ(failures.load(), 0)
      << failures.load() << " out of " << kClients << " clients encountered errors";
  server->Stop();
}

// ============================================================================
// Test 5 — Scale-up: as many clients as RLIMIT_NOFILE allows (cap 500)
// ============================================================================

/**
 * Scale-up variant.  Tries up to 500 concurrent clients; gracefully
 * degrades if the fd budget doesn't permit it.  Each client sends 5 INFO
 * commands.  All requests must succeed.
 */
TEST_F(ReactorIntegrationTest, ConcurrentClientsMany) {
  auto server = StartServer(/*worker_threads=*/16);
  RequireReactor(*server);
  uint16_t port = server->GetPort();

  // Each client needs ~2 fds (client-side sock + server-side sock) plus
  // headroom for the server's listen fd, test binary fds, etc.
  constexpr rlim_t kDesiredFds = 2048;
  constexpr int kMaxClients = 500;
  constexpr int kMinClientsToRun = 50;  // skip if budget is tiny

  rlim_t effective = RaiseFdLimit(kDesiredFds);
  // Reserve ~64 fds for the test process itself
  int max_clients = std::min<int>(kMaxClients, static_cast<int>((effective - 64) / 2));

  if (max_clients < kMinClientsToRun) {
    GTEST_SKIP() << "RLIMIT_NOFILE too small (" << effective << ") for meaningful concurrency test; "
                 << "re-run with ulimit -n 2048";
  }

  std::cout << "[ConcurrentClientsMany] using " << max_clients << " clients (rlimit=" << effective << ")"
            << std::endl;

  constexpr int kCmdsPerClient = 5;
  std::atomic<int> failures{0};
  // C++17 manual barrier
  std::mutex barrier_mu2;
  std::condition_variable barrier_cv2;
  std::atomic<int> barrier_count2{0};
  int barrier_total = max_clients;
  auto wait_for_all2 = [&]() {
    barrier_count2.fetch_add(1, std::memory_order_acq_rel);
    std::unique_lock<std::mutex> lk(barrier_mu2);
    barrier_cv2.wait(lk, [&] { return barrier_count2.load() >= barrier_total; });
    barrier_cv2.notify_all();
  };

  auto client_task = [&]() {
    wait_for_all2();

    int s = Connect(port, 3000);
    if (s < 0) {
      failures.fetch_add(1, std::memory_order_relaxed);
      return;
    }
    for (int i = 0; i < kCmdsPerClient; ++i) {
      if (!SendLine(s, "INFO")) {
        failures.fetch_add(1, std::memory_order_relaxed);
        close(s);
        return;
      }
      std::string resp = RecvMultilineResponse(s, 5000);
      if (resp.empty() || resp.substr(0, 7) != "OK INFO") {
        failures.fetch_add(1, std::memory_order_relaxed);
        close(s);
        return;
      }
    }
    close(s);
  };

  std::vector<std::thread> threads;
  threads.reserve(max_clients);
  for (int i = 0; i < max_clients; ++i) threads.emplace_back(client_task);
  for (auto& t : threads) t.join();

  EXPECT_EQ(failures.load(), 0)
      << failures.load() << " out of " << max_clients << " clients failed";
  server->Stop();
}

// ============================================================================
// Test 6 — Client disconnects mid-request (incomplete frame)
// ============================================================================

/**
 * Client sends an incomplete frame ("IN" with no CRLF) then closes.
 * The server must handle the HUP gracefully — no hang, no crash.
 * A subsequent client must still get a valid response.
 */
TEST_F(ReactorIntegrationTest, ClientDisconnectMidRequest) {
  auto server = StartServer();
  RequireReactor(*server);
  uint16_t port = server->GetPort();

  // Abrupt client
  {
    int s = Connect(port);
    ASSERT_GE(s, 0);
    // Send partial frame — no CRLF
    const char* partial = "IN";
    send(s, partial, 2, 0);
    close(s);  // close without completing frame
  }

  // Give the reactor event loop a chance to observe the HUP and clean up
  // the connection.  We use a poll loop rather than a blind sleep: wait
  // until the server reports zero active connections (or timeout).
  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
  while (std::chrono::steady_clock::now() < deadline) {
    if (server->GetConnectionCount() == 0) break;
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  // A new client must still work
  int s2 = Connect(port);
  ASSERT_GE(s2, 0) << "Failed to connect after abrupt client disconnect";
  ASSERT_TRUE(SendLine(s2, "INFO"));
  std::string resp = RecvLine(s2, 5000);
  EXPECT_EQ(resp.substr(0, 7), "OK INFO") << "Second client got: " << resp;
  close(s2);

  server->Stop();
}

// ============================================================================
// Test 7 — Client disconnects during response (EPIPE tolerance)
// ============================================================================

/**
 * Client sends INFO, reads exactly 1 byte, then closes.  The server may
 * observe EPIPE on the remainder of the write.  It must clean up gracefully
 * and subsequent clients must still succeed.
 *
 * Inherently racy (timing of the close vs. the send on the worker thread).
 * The test is structured so it passes deterministically: success is defined
 * as "the subsequent client always succeeds", regardless of whether EPIPE
 * was actually triggered for the first client.
 */
TEST_F(ReactorIntegrationTest, ClientDisconnectDuringResponse) {
  auto server = StartServer();
  RequireReactor(*server);
  uint16_t port = server->GetPort();

  // Attempt to trigger EPIPE: send INFO, read 1 byte, close immediately
  {
    int s = Connect(port);
    ASSERT_GE(s, 0);
    ASSERT_TRUE(SendLine(s, "INFO"));
    // Read exactly 1 byte so the socket receive buffer is partially drained,
    // then close without reading the rest.
    char byte = 0;
    recv(s, &byte, 1, 0);
    close(s);
  }

  // Wait for the abrupt client to be cleaned up
  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
  while (std::chrono::steady_clock::now() < deadline) {
    if (server->GetConnectionCount() == 0) break;
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  // Subsequent client must succeed — this is the invariant we are asserting
  int s2 = Connect(port);
  ASSERT_GE(s2, 0);
  ASSERT_TRUE(SendLine(s2, "INFO"));
  std::string resp = RecvLine(s2, 5000);
  EXPECT_EQ(resp.substr(0, 7), "OK INFO") << "Post-EPIPE client got: " << resp;
  close(s2);

  server->Stop();
}

// ============================================================================
// Test 8 — Graceful shutdown with active clients
// ============================================================================

/**
 * Open 10 persistent idle connections (sent INFO, got response, now idle).
 * Call Stop() from a separate thread.  Assert Stop() returns within 3 s.
 * After Stop(), all client recv()s return 0 (FIN) or an error (EBADF/
 * ECONNRESET).
 */
TEST_F(ReactorIntegrationTest, GracefulShutdownWithActiveClients) {
  auto server = StartServer();
  RequireReactor(*server);
  uint16_t port = server->GetPort();

  constexpr int kPersistentClients = 10;
  std::vector<int> socks;
  socks.reserve(kPersistentClients);
  for (int i = 0; i < kPersistentClients; ++i) {
    int s = Connect(port);
    ASSERT_GE(s, 0) << "client " << i;
    ASSERT_TRUE(SendLine(s, "INFO"));
    // Drain the entire multi-line response so the kernel socket buffer is
    // empty by the time Stop() runs. Otherwise the leftover "\r\nEND\r\n"
    // trailer would be returned by the post-shutdown recv() check and mask
    // the actual FIN we want to observe.
    std::string resp = RecvMultilineResponse(s);
    ASSERT_FALSE(resp.empty()) << "client " << i << " timed out";
    ASSERT_EQ(resp.substr(0, 7), "OK INFO") << "client " << i;
    socks.push_back(s);
  }

  // Stop in a separate thread; measure elapsed time
  std::atomic<bool> stop_done{false};
  std::thread stopper([&]() {
    server->Stop();
    stop_done.store(true, std::memory_order_release);
  });

  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!stop_done.load(std::memory_order_acquire)) {
    ASSERT_LT(std::chrono::steady_clock::now(), deadline) << "Stop() did not return within 5 s";
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  stopper.join();

  // After Stop(), all sockets should receive FIN (n==0) or error.
  // Give FINs a moment to propagate.
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  for (int s : socks) {
    char buf = 0;
    ssize_t n = recv(s, &buf, 1, MSG_DONTWAIT);
    // n == 0 → clean FIN; n < 0 → EAGAIN/EBADF/ECONNRESET (all acceptable)
    // n > 0 would mean the server sent unexpected data after Stop(), fail.
    EXPECT_LE(n, 0) << "Expected FIN or error from stopped server, got " << n << " bytes";
    close(s);
  }
}

// ============================================================================
// Test 9 — Confirm reactor backend is active
// ============================================================================

/**
 * After Start(), IsReactorActiveForTest() must return true (handled by
 * RequireReactor which would skip otherwise).  Additionally verify that
 * zero clients are connected at baseline.
 */
TEST_F(ReactorIntegrationTest, BackendIsReactor) {
  auto server = StartServer();
  RequireReactor(*server);

  // Reactor is running; no clients yet.
  EXPECT_TRUE(server->IsReactorActiveForTest());
  EXPECT_EQ(server->GetConnectionCount(), 0U);

  // One quick request to confirm it actually routes through the reactor.
  uint16_t port = server->GetPort();
  int s = Connect(port);
  ASSERT_GE(s, 0);
  ASSERT_TRUE(SendLine(s, "INFO"));
  EXPECT_EQ(RecvLine(s).substr(0, 7), "OK INFO");
  close(s);

  server->Stop();
  EXPECT_FALSE(server->IsReactorActiveForTest());
}

// ============================================================================
// Test 10 — Reactor does not starve a persistent fleet
// ============================================================================

/**
 * Inverse of thread_pool_saturation_test: pin 8 persistent idle clients.
 * A 9th client must get a response in <500 ms.  In reactor mode the 8
 * pinned connections do NOT hold worker threads, so the 9th request is
 * always serviced from the free pool.
 */
TEST_F(ReactorIntegrationTest, ReactorModeDoesNotStarvePersistentFleet) {
  // Keep the thread pool tiny so the starvation would be obvious if the
  // reactor degraded to one-worker-per-connection.
  constexpr int kWorkers = 4;
  auto server = StartServer(kWorkers);
  RequireReactor(*server);
  uint16_t port = server->GetPort();

  // Pin 8 persistent idle clients (> kWorkers to stress-test)
  constexpr int kPinned = 8;
  std::vector<int> pinned;
  pinned.reserve(kPinned);
  for (int i = 0; i < kPinned; ++i) {
    int s = Connect(port);
    ASSERT_GE(s, 0) << "pinned client " << i;
    ASSERT_TRUE(SendLine(s, "INFO"));
    ASSERT_EQ(RecvLine(s).substr(0, 7), "OK INFO") << "pinned client " << i;
    pinned.push_back(s);
  }

  // 9th client: must respond quickly even though there are 8 idle connections
  int late = Connect(port);
  ASSERT_GE(late, 0);
  ASSERT_TRUE(SendLine(late, "INFO"));

  auto t0 = std::chrono::steady_clock::now();
  std::string resp = RecvLine(late, /*timeout_ms=*/500);
  auto elapsed_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();

  EXPECT_EQ(resp.substr(0, 7), "OK INFO")
      << "9th client starved in reactor mode with " << kPinned << " idle connections";
  EXPECT_LT(elapsed_ms, 500)
      << "9th client responded but took " << elapsed_ms << "ms (>500ms) in reactor mode";

  close(late);
  for (int s : pinned) close(s);
  server->Stop();
}

// ============================================================================
// Test 11 — Large frame handling
// ============================================================================

/**
 * Send a command just under 1 MiB (ReactorConnection::kMaxReadBufferBytes).
 * The server should respond normally (it won't recognise the giant command
 * but it should close gracefully, not crash).
 *
 * Send a frame > 1 MiB: the connection must be closed by the server.
 */
TEST_F(ReactorIntegrationTest, ClientSendsLargeFrame) {
  auto server = StartServer();
  RequireReactor(*server);
  uint16_t port = server->GetPort();

  // --- Sub-cap frame (slightly under 1 MiB) ---
  // The server will close when it can't parse the oversized command,
  // but it should NOT crash.
  {
    int s = Connect(port);
    ASSERT_GE(s, 0);

    // 900 KB of 'X' characters + CRLF — under the 1 MiB cap.
    constexpr size_t kSubCapSize = 900 * 1024;
    std::string big_cmd(kSubCapSize, 'X');
    big_cmd += "\r\n";
    // Send in chunks to avoid hitting SO_SNDBUF
    size_t sent_total = 0;
    while (sent_total < big_cmd.size()) {
      ssize_t n = send(s, big_cmd.data() + sent_total, big_cmd.size() - sent_total, 0);
      if (n <= 0) break;
      sent_total += static_cast<size_t>(n);
    }
    // We don't assert a specific response here — just that the connection is
    // handled without crashing (recv returns something or EOF).
    std::string resp = RecvLine(s, 5000);
    // Acceptable: error response or connection closed; NOT a crash.
    (void)resp;
    close(s);
  }

  // --- Over-cap frame (> 1 MiB) ---
  // ReactorConnection must close the connection after exceeding kMaxReadBufferBytes.
  {
    int s = Connect(port);
    ASSERT_GE(s, 0);

    // 1.1 MiB — exceeds the 1 MiB cap, no CRLF yet (to keep it as one frame)
    constexpr size_t kOverCapSize = 1100 * 1024;
    std::string huge_cmd(kOverCapSize, 'Y');
    huge_cmd += "\r\n";

    // Send until we get EPIPE or the server closes
    size_t sent_total = 0;
    bool got_epipe = false;
    while (sent_total < huge_cmd.size()) {
      ssize_t n = send(s, huge_cmd.data() + sent_total,
                       std::min<size_t>(65536, huge_cmd.size() - sent_total), MSG_NOSIGNAL);
      if (n <= 0) {
        got_epipe = (errno == EPIPE || errno == ECONNRESET);
        break;
      }
      sent_total += static_cast<size_t>(n);
    }

    // Either we got EPIPE/ECONNRESET during send, or the server closed the
    // connection which we observe as recv() == 0.
    if (!got_epipe) {
      char buf = 0;
      struct timeval tv {};
      tv.tv_sec = 3;
      setsockopt(s, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
      ssize_t n = recv(s, &buf, 1, 0);
      EXPECT_LE(n, 0) << "Expected server to close over-cap connection";
    }
    close(s);
  }

  // Server must still be operational after both large-frame clients
  int s3 = Connect(port);
  ASSERT_GE(s3, 0);
  ASSERT_TRUE(SendLine(s3, "INFO"));
  EXPECT_EQ(RecvLine(s3).substr(0, 7), "OK INFO");
  close(s3);

  server->Stop();
}

// ============================================================================
// Test 12 — Write backpressure: a slow reader does not starve other clients
// ============================================================================

/**
 * Design-doc §7 R3 invariant: a client that accepts bytes into its kernel
 * recv buffer but never calls recv() must NOT be able to stall the server
 * or starve other clients. The reactor's per-connection write queue
 * decouples the slow reader from the rest of the server:
 *
 *   1. Worker drains frames, Dispatch() runs, EnqueueResponse is called.
 *   2. The inline non-blocking send fills the slow reader's socket buffer
 *      and starts hitting EAGAIN.
 *   3. ReactorConnection arms kWritable on the slow reader's fd and returns
 *      control to the worker, which moves on to other connections.
 *   4. Other clients' requests continue to be served with their normal
 *      latency.
 *
 * This test constructs that exact scenario with a small
 * max_write_queue_bytes cap (64 KiB) so that the cap enforcement path is
 * exercised: once the slow reader's write_queue exceeds 64 KiB, the reactor
 * forcibly closes that connection and logs `reactor_write_queue_overflow`.
 * Meanwhile a "fast client" thread continuously issues INFO requests and
 * verifies they are all served promptly.
 */
TEST_F(ReactorIntegrationTest, WriteBackpressureHandledGracefully) {
  // 64 KiB cap: small enough that a burst of pipelined INFO responses to a
  // non-reading peer will exceed it within a few hundred milliseconds.
  constexpr int64_t kCapBytes = 64 * 1024;
  auto server = StartServer(/*worker_threads=*/4, /*max_connections=*/64, kCapBytes);
  RequireReactor(*server);
  uint16_t port = server->GetPort();

  // -------------------------------------------------------------------------
  // Slow reader: shrink the kernel recv buffer so it fills quickly, pipeline
  // many INFO requests, and NEVER recv().  The server's send() will hit
  // EAGAIN, arm kWritable, and pile bytes into the write_queue until the
  // cap fires.
  // -------------------------------------------------------------------------
  int slow = Connect(port);
  ASSERT_GE(slow, 0);

  // Shrink SO_RCVBUF so the kernel buffer fills after only a few responses.
  // The kernel may round this up but it will still be far below 64 KiB so
  // the ReactorConnection queue bears the brunt of the pressure.
  int rcvbuf = 4096;
  setsockopt(slow, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf));

  // Pipeline a large batch of INFO commands.  Each INFO response is a few
  // hundred bytes, so ~1000 responses is more than enough to blow past the
  // 64 KiB cap once the slow reader's recv buffer is full.
  std::string burst;
  burst.reserve(8 * 1024);
  for (int i = 0; i < 1024; ++i) burst.append("INFO\r\n");
  ssize_t sent = 0;
  while (sent < static_cast<ssize_t>(burst.size())) {
    ssize_t n = send(slow, burst.data() + sent, burst.size() - sent, MSG_NOSIGNAL);
    if (n <= 0) break;
    sent += n;
  }
  // Slow reader deliberately does NOT recv().

  // -------------------------------------------------------------------------
  // Fast client(s): a small pool of concurrent workers that issue INFO and
  // expect responses within 500 ms per request.  If the slow reader were
  // somehow blocking the event loop or starving the worker pool, these
  // requests would time out.
  // -------------------------------------------------------------------------
  constexpr int kFastClients = 4;
  constexpr int kFastCommandsPerClient = 20;
  std::atomic<int> fast_failures{0};
  std::atomic<int> fast_successes{0};

  auto fast_client_task = [&]() {
    int s = Connect(port);
    if (s < 0) {
      fast_failures.fetch_add(1, std::memory_order_relaxed);
      return;
    }
    for (int i = 0; i < kFastCommandsPerClient; ++i) {
      if (!SendLine(s, "INFO")) {
        fast_failures.fetch_add(1, std::memory_order_relaxed);
        close(s);
        return;
      }
      std::string resp = RecvMultilineResponse(s, /*timeout_ms=*/2000);
      if (resp.empty() || resp.substr(0, 7) != "OK INFO") {
        fast_failures.fetch_add(1, std::memory_order_relaxed);
        close(s);
        return;
      }
      fast_successes.fetch_add(1, std::memory_order_relaxed);
    }
    close(s);
  };

  std::vector<std::thread> fast_threads;
  fast_threads.reserve(kFastClients);
  for (int i = 0; i < kFastClients; ++i) fast_threads.emplace_back(fast_client_task);
  for (auto& t : fast_threads) t.join();

  // Invariant: every fast client request must have succeeded.
  EXPECT_EQ(fast_failures.load(), 0)
      << fast_failures.load() << " fast-client failures while slow reader was "
      << "backpressured; reactor should have isolated the slow reader but did not";
  EXPECT_EQ(fast_successes.load(), kFastClients * kFastCommandsPerClient);

  // The slow reader should eventually be force-closed by the reactor once
  // its write queue exceeds 64 KiB.  We detect this by draining the
  // client-side kernel recv buffer until recv() returns 0 (FIN) or
  // errors out.  The drop-on-shutdown is essentially instant from the
  // reactor's perspective, but the client still has to consume any bytes
  // that were sent BEFORE the cap fired — so we drain aggressively with a
  // large buffer so the FIN shows up within the deadline.
  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  bool slow_closed = false;
  while (std::chrono::steady_clock::now() < deadline) {
    std::array<char, 8192> drain{};
    ssize_t n = recv(slow, drain.data(), drain.size(), MSG_DONTWAIT);
    if (n == 0) {
      slow_closed = true;  // FIN observed after draining
      break;
    }
    if (n < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        // Kernel buffer empty but the server hasn't closed yet.  Sleep
        // briefly and recheck.
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        continue;
      }
      slow_closed = true;  // ECONNRESET / EBADF / etc.
      break;
    }
    // n > 0: we just drained a chunk; loop immediately to keep going
    // until the buffer is empty and FIN shows up.
  }

  EXPECT_TRUE(slow_closed)
      << "Slow reader was never force-closed by the reactor within 5s "
      << "despite piling up responses against a 64 KiB write queue cap";

  close(slow);
  server->Stop();
}

// ============================================================================
// Test 13 — G1 goal: many idle persistent clients + 1 active (LOAD)
// ============================================================================

/**
 * Design-doc §3.1 G1: with a small worker pool and a large number of idle
 * persistent connections, a newly-arriving active client must be served in
 * well under 500 ms.  This is the Phase 3 upgrade of
 * `ReactorServesPersistentFleetWithoutStarvation`: instead of 128 idle
 * clients, this test pushes the scale up to the per-process fd budget.
 *
 * Tagged as LOAD because it stresses RLIMIT_NOFILE and may take noticeably
 * longer than the other reactor tests.  Skipped if the fd budget cannot
 * accommodate at least 512 persistent clients.
 */
TEST_F(ReactorIntegrationTest, ManyIdleConnectionsDoNotBlockActiveClient) {
  constexpr rlim_t kDesiredFds = 4096;
  // Target: as many idle clients as the fd budget allows, up to 2048.
  // The design doc targets 10k; most CI runners can't provide that many
  // fds, so we scale to whatever is available and still prove the
  // invariant.
  constexpr int kTargetIdleClients = 2048;
  constexpr int kMinIdleClientsToRun = 256;

  rlim_t effective = RaiseFdLimit(kDesiredFds);
  // Reserve ~128 fds for the test process / server / leak headroom, and
  // each idle client needs ~2 fds (client side + server side).
  int idle_count = std::min<int>(kTargetIdleClients, static_cast<int>((effective - 128) / 2));
  if (idle_count < kMinIdleClientsToRun) {
    GTEST_SKIP() << "RLIMIT_NOFILE too small (" << effective
                 << ") for the G1 starvation test; re-run with ulimit -n 4096 "
                 << "to exercise the many-idle-clients invariant";
  }

  // Small worker pool: the whole point of the reactor refactor is that the
  // worker count does NOT have to scale with the idle-connection count.
  constexpr int kWorkers = 8;
  auto server = StartServer(kWorkers, /*max_connections=*/static_cast<int>(idle_count + 64));
  RequireReactor(*server);
  uint16_t port = server->GetPort();

  // Open idle_count persistent clients: each sends one INFO to prove the
  // round-trip works, drains the full multi-line response, and then goes
  // completely idle (no more sends, no more recvs).
  std::vector<int> idle_socks;
  idle_socks.reserve(idle_count);
  for (int i = 0; i < idle_count; ++i) {
    int s = Connect(port, /*timeout_ms=*/2000);
    if (s < 0) {
      // Couldn't open all the sockets we wanted; run with what we got if
      // we still have at least kMinIdleClientsToRun.
      if (static_cast<int>(idle_socks.size()) >= kMinIdleClientsToRun) break;
      GTEST_SKIP() << "Failed to open idle client " << i << "; fd budget exhausted";
    }
    if (!SendLine(s, "INFO")) {
      close(s);
      continue;
    }
    std::string resp = RecvMultilineResponse(s, /*timeout_ms=*/3000);
    if (resp.empty() || resp.substr(0, 7) != "OK INFO") {
      close(s);
      continue;
    }
    idle_socks.push_back(s);
  }

  std::cout << "[ManyIdleConnectionsDoNotBlockActiveClient] "
            << "idle_clients=" << idle_socks.size() << " workers=" << kWorkers
            << " rlimit=" << effective << std::endl;
  ASSERT_GE(static_cast<int>(idle_socks.size()), kMinIdleClientsToRun)
      << "Not enough idle clients opened to meaningfully stress the server";

  // Give the reactor a moment to re-arm kReadable on all idle fds.
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  // Active client: send INFO and expect a response within 500 ms.  Under
  // the reactor I/O model the idle clients do not consume worker threads,
  // so the active client is served by a free worker immediately.
  int active = Connect(port);
  ASSERT_GE(active, 0);
  ASSERT_TRUE(SendLine(active, "INFO"));

  auto t0 = std::chrono::steady_clock::now();
  std::string resp = RecvMultilineResponse(active, /*timeout_ms=*/500);
  auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now() - t0)
                        .count();

  EXPECT_FALSE(resp.empty())
      << "Active client starved with " << idle_socks.size()
      << " idle persistent connections — the G1 invariant failed";
  if (!resp.empty()) {
    EXPECT_EQ(resp.substr(0, 7), "OK INFO");
    EXPECT_LT(elapsed_ms, 500)
        << "Active client responded but took " << elapsed_ms << "ms (>500ms) with "
        << idle_socks.size() << " idle persistent connections";
  }

  close(active);
  for (int s : idle_socks) close(s);
  server->Stop();
}

}  // namespace mygramdb::server

#endif  // platform guard
