/**
 * @file thread_pool_saturation_test.cpp
 * @brief Regression tests anchoring the thread-pool / I/O-model invariants.
 *
 * Historical context: under the legacy blocking I/O model,
 * ConnectionIOHandler::HandleConnection() held one worker thread for the
 * entire lifetime of a client connection. With a small worker pool,
 * idle-but-persistent clients could pin every worker, at which point newly
 * accepted connections piled up in the task queue until it overflowed and
 * the acceptor began writing "ERR SERVER_BUSY" + close() on new sockets.
 *
 * After the Phase 3 reactor refactor, the default I/O model is "reactor",
 * so persistent idle connections no longer pin workers at all. The tests
 * below therefore split into two groups:
 *
 *   1. Reactor-default invariants (no forced io_model): verify the new
 *      default model serves late clients promptly even when the worker
 *      pool is tiny and many persistent clients are holding connections.
 *   2. Blocking-mode legacy invariants (explicit io_model="blocking"):
 *      preserve the original starvation and queue-overflow regression
 *      gates for the fallback/rollback I/O model. These still matter
 *      because operators can flip back to blocking mode via YAML, and the
 *      blocking path is not going away until Phase 4.
 *
 * Labeled "LOAD" — excluded from `make test-fast` / CI and only run via
 * `make test-load` or `ctest -L LOAD`.
 */

#include <arpa/inet.h>
#include <fcntl.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

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

class ThreadPoolSaturationTest : public ::testing::Test {
 protected:
  void SetUp() override {
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

  /**
   * @brief Connect to 127.0.0.1:port with a bounded timeout.
   * @return socket fd on success, -1 on failure.
   */
  int Connect(uint16_t port, int timeout_ms = 1000) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
      return -1;
    }

    int flags = fcntl(sock, F_GETFL, 0);
    if (flags >= 0) {
      fcntl(sock, F_SETFL, flags | O_NONBLOCK);
    }

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
      fd_set ws;
      FD_ZERO(&ws);
      FD_SET(sock, &ws);
      struct timeval tv {};
      tv.tv_sec = timeout_ms / 1000;
      tv.tv_usec = (timeout_ms % 1000) * 1000;
      int ready = select(sock + 1, nullptr, &ws, nullptr, &tv);
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

    if (flags >= 0) {
      fcntl(sock, F_SETFL, flags);
    }
    struct timeval io {};
    io.tv_sec = 2;
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &io, sizeof(io));
    setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &io, sizeof(io));
    return sock;
  }

  bool SendLine(int sock, const std::string& line) {
    std::string msg = line + "\r\n";
    ssize_t sent = send(sock, msg.data(), msg.size(), 0);
    return sent == static_cast<ssize_t>(msg.size());
  }

  /**
   * @brief Read one \r\n-terminated response within timeout_ms.
   * @return response body without the trailing CRLF, or empty string on
   *         timeout/error.
   */
  std::string RecvLine(int sock, int timeout_ms = 2000) {
    struct timeval io {};
    io.tv_sec = timeout_ms / 1000;
    io.tv_usec = (timeout_ms % 1000) * 1000;
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &io, sizeof(io));

    std::string out;
    std::array<char, 512> buf{};
    while (out.size() < 4096) {
      ssize_t n = recv(sock, buf.data(), buf.size(), 0);
      if (n <= 0) {
        return "";
      }
      out.append(buf.data(), n);
      if (out.find("\r\n") != std::string::npos) {
        break;
      }
    }
    auto pos = out.find("\r\n");
    if (pos != std::string::npos) {
      out.resize(pos);
    }
    return out;
  }

  std::unique_ptr<TcpServer> StartServer(int worker_threads, int max_connections = 256,
                                          const std::string& io_model = std::string{}) {
    ServerConfig cfg;
    cfg.host = "127.0.0.1";
    cfg.port = 0;
    cfg.worker_threads = worker_threads;
    cfg.max_connections = max_connections;
    cfg.allow_cidrs = {"127.0.0.1/32"};
    if (!io_model.empty()) {
      cfg.io_model = io_model;
    }
    auto s = std::make_unique<TcpServer>(cfg, table_contexts_);
    auto res = s->Start();
    EXPECT_TRUE(res) << "Failed to start TcpServer: "
                     << (res ? std::string{} : res.error().to_string());
    // Give the accept loop a moment to reach its main loop.
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    return s;
  }

  /**
   * @brief Try to raise the per-process FD limit (for the queue-overflow
   *        test which opens >1000 sockets). Returns the effective cap.
   */
  static rlim_t RaiseFdLimit(rlim_t desired) {
    struct rlimit rl {};
    if (getrlimit(RLIMIT_NOFILE, &rl) != 0) {
      return 0;
    }
    rlim_t target = std::min<rlim_t>(desired, rl.rlim_max);
    if (rl.rlim_cur < target) {
      rl.rlim_cur = target;
      setrlimit(RLIMIT_NOFILE, &rl);  // Best-effort
      getrlimit(RLIMIT_NOFILE, &rl);
    }
    return rl.rlim_cur;
  }

  std::unique_ptr<TableContext> table_ctx_;
  std::unordered_map<std::string, TableContext*> table_contexts_;
};

/**
 * Phase 3 inversion of the historical starvation gate.
 *
 * Under the legacy blocking I/O model this test asserted that a late client
 * could NOT be served when idle persistent clients had pinned every worker
 * thread. Phase 3 makes "reactor" the default I/O model (see
 * ApiConfig::tcp::io_model), and in that model persistent idle connections
 * live in the epoll/kqueue registration map rather than parking worker
 * threads. The assertion therefore flips: with the default I/O model, a
 * late client MUST be served promptly even when `worker_threads` is tiny
 * and matches (or is below) the idle-persistent-client count.
 *
 * If this test ever starts failing, the reactor no longer decouples
 * connection lifetime from worker lifetime — which would be a Phase 3
 * regression.  The corresponding negative control (blocking mode STILL
 * starves under the same load) lives in
 * `tests/integration/server/reactor_starvation_regression_test.cpp`.
 */
TEST_F(ThreadPoolSaturationTest, LateClientServedDespitePinnedIdleClientsInDefaultMode) {
  // Intentionally tiny worker pool to make the "workers == idle clients"
  // corner case trivially reachable. Under reactor mode this is fine — the
  // event loop handles reads and workers only run briefly per command.
  constexpr int kWorkers = 2;
  auto server = StartServer(kWorkers);  // default io_model = reactor
  uint16_t port = server->GetPort();

  // Open kWorkers idle persistent clients. Each issues one INFO and then
  // sits quietly, just as the production starvation scenario did.
  std::vector<int> pinned;
  pinned.reserve(kWorkers);
  for (int i = 0; i < kWorkers; ++i) {
    int s = Connect(port);
    ASSERT_GE(s, 0) << "Failed to open pinning client " << i;
    ASSERT_TRUE(SendLine(s, "INFO"));
    std::string resp = RecvLine(s);
    ASSERT_EQ(resp.substr(0, 7), "OK INFO")
        << "Pinning client " << i << " did not receive INFO reply; got: " << resp;
    pinned.push_back(s);
  }

  // Give the reactor a moment to re-arm kReadable for the pinned clients.
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // Late client: should be served immediately under reactor mode. Under
  // the old blocking model this was impossible and the request would time
  // out; the inversion is deliberate.
  int late = Connect(port);
  ASSERT_GE(late, 0);
  ASSERT_TRUE(SendLine(late, "INFO"));

  auto t0 = std::chrono::steady_clock::now();
  std::string resp = RecvLine(late, /*timeout_ms=*/1500);
  auto elapsed_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0)
          .count();

  EXPECT_FALSE(resp.empty())
      << "Late INFO was NOT served under the default (reactor) I/O model "
         "even though the worker pool was small and all persistent clients "
         "were idle. This is the Phase 3 invariant — if it fails, the "
         "reactor refactor has regressed.";
  if (!resp.empty()) {
    EXPECT_EQ(resp.substr(0, 7), "OK INFO");
    EXPECT_LT(elapsed_ms, 500)
        << "Late client was served but took " << elapsed_ms
        << "ms (>500ms). Reactor latency degraded?";
  }

  close(late);
  for (int s : pinned) {
    close(s);
  }
  server->Stop();
}

/**
 * Blocking-mode legacy invariant.
 *
 * Forces io_model="blocking" so the worker-count mitigation lever is
 * exercised against its original failure mode. With a generously-sized
 * blocking thread pool, the same client pattern that normally starves
 * under io_model=blocking does NOT starve, confirming that
 * `api.tcp.worker_threads` remains a usable emergency lever for operators
 * who have reverted to the blocking I/O model for any reason.
 */
TEST_F(ThreadPoolSaturationTest, LargerThreadPoolRemovesStarvationInBlockingMode) {
  constexpr int kWorkers = 64;
  auto server = StartServer(kWorkers, /*max_connections=*/256, /*io_model=*/"blocking");
  uint16_t port = server->GetPort();

  // Pin 4 persistent clients — well below kWorkers so there should be plenty
  // of slack to serve the late client immediately.
  std::vector<int> pinned;
  for (int i = 0; i < 4; ++i) {
    int s = Connect(port);
    ASSERT_GE(s, 0);
    ASSERT_TRUE(SendLine(s, "INFO"));
    ASSERT_EQ(RecvLine(s).substr(0, 7), "OK INFO");
    pinned.push_back(s);
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  int late = Connect(port);
  ASSERT_GE(late, 0);
  ASSERT_TRUE(SendLine(late, "INFO"));

  auto t0 = std::chrono::steady_clock::now();
  std::string resp = RecvLine(late, /*timeout_ms=*/1500);
  auto elapsed_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0)
          .count();

  EXPECT_FALSE(resp.empty()) << "Late client starved even with worker_threads=" << kWorkers;
  EXPECT_EQ(resp.substr(0, 7), "OK INFO");
  EXPECT_LT(elapsed_ms, 500)
      << "Late client responded but took " << elapsed_ms << "ms (>500ms)";

  close(late);
  for (int s : pinned) {
    close(s);
  }
  server->Stop();
}

/**
 * Regression test for the default auto-sized thread pool under the
 * default (reactor) I/O model.
 *
 * With the Phase 3 defaults (`io_model=reactor`, `worker_threads=0` →
 * auto-size = max(hw*2, 4)), persistent idle clients do NOT pin workers —
 * they live in the reactor's connection map and only consume a worker
 * briefly when a complete command frame arrives. This test pins 16
 * persistent clients (well above the auto-sized worker count on typical
 * CI runners) and asserts that a late client is still served quickly.
 *
 * If this test ever starts failing, either the reactor default regressed
 * or the thread pool auto-size fell below the "4 worker floor" that the
 * reactor assumes for burst absorption.
 */
TEST_F(ThreadPoolSaturationTest, DefaultAutoSizeServesManyPersistentClients) {
  // worker_threads=0 exercises the auto-sizing path in ThreadPool's constructor.
  auto server = StartServer(/*worker_threads=*/0);  // default io_model = reactor
  uint16_t port = server->GetPort();

  // Pin enough clients that the auto-sized pool (max(hw*2, 4)) could not
  // possibly dedicate one worker each: 16 > any normal CI runner's hw*2.
  // Under reactor mode this is a non-event — workers are only briefly
  // consumed per-request — so the late client is still served promptly.
  constexpr int kPinned = 16;
  std::vector<int> pinned;
  pinned.reserve(kPinned);
  for (int i = 0; i < kPinned; ++i) {
    int s = Connect(port);
    ASSERT_GE(s, 0) << "Failed to open pinning client " << i;
    ASSERT_TRUE(SendLine(s, "INFO"));
    ASSERT_EQ(RecvLine(s).substr(0, 7), "OK INFO")
        << "Pinning client " << i << " did not receive INFO reply";
    pinned.push_back(s);
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  int late = Connect(port);
  ASSERT_GE(late, 0);
  ASSERT_TRUE(SendLine(late, "INFO"));

  auto t0 = std::chrono::steady_clock::now();
  std::string resp = RecvLine(late, /*timeout_ms=*/1500);
  auto elapsed_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0)
          .count();

  EXPECT_FALSE(resp.empty())
      << "Late client starved under the Phase 3 defaults with " << kPinned
      << " pinned persistent clients — has the reactor default regressed to "
         "blocking mode, or did the thread pool auto-size floor drop below 4?";
  if (!resp.empty()) {
    EXPECT_EQ(resp.substr(0, 7), "OK INFO");
    EXPECT_LT(elapsed_ms, 500)
        << "Late client responded but took " << elapsed_ms
        << "ms under the default Phase 3 config";
  }

  close(late);
  for (int s : pinned) {
    close(s);
  }
  server->Stop();
}

/**
 * Blocking-mode legacy invariant: verify that the thread-pool backpressure
 * path in ConnectionAcceptor::AcceptLoop fires when the task queue fills up
 * under `io_model=blocking`, and that the affected sockets receive
 * "ERR SERVER_BUSY" followed by a clean close.
 *
 * The queue_full path is unreachable under `io_model=reactor` because
 * reactor-mode accept hands connections directly to IoReactor::Register
 * without a thread-pool submission per accept. This test therefore forces
 * blocking mode to preserve coverage of the fallback I/O model's rejection
 * behaviour.
 *
 * Caveat: the hardcoded queue size is kThreadPoolQueueSize = 1000
 * (server_lifecycle_manager.cpp:31). Because client-side sockets and the
 * server's accepted-socket fds live in the same test process, reliably
 * overflowing the queue requires ~2200 open fds (1000 queued server-side +
 * 1000 client-side + overhead). On systems where RLIMIT_NOFILE cannot be
 * raised that high (macOS default soft limit is 256, many Linux defaults are
 * 1024), the test degrades gracefully: it opens as many sockets as the
 * per-process fd budget allows and either asserts the BUSY path fired (if
 * we got enough sockets) or skips (if we didn't).
 */
TEST_F(ThreadPoolSaturationTest, QueueOverflowTriggersBusyResponseInBlockingMode) {
  constexpr int kWorkers = 1;
  // To overflow the 1000-slot queue we need at least ~1000 server-side
  // accepted fds + ~1000 client-side fds in the same process + overhead.
  constexpr rlim_t kDesiredFdLimit = 4096;
  constexpr int kMinClientFdsToExercise = 1050;  // queue overflow point
  rlim_t effective_limit = RaiseFdLimit(kDesiredFdLimit);

  auto server = StartServer(kWorkers, /*max_connections=*/4096, /*io_model=*/"blocking");
  uint16_t port = server->GetPort();

  // Pin the single worker thread with a persistent idle client.
  int pinned = Connect(port);
  ASSERT_GE(pinned, 0);
  ASSERT_TRUE(SendLine(pinned, "INFO"));
  ASSERT_EQ(RecvLine(pinned).substr(0, 7), "OK INFO");
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // Burst-open sockets without sending any request. Each accepted connection
  // becomes a task in the 1000-slot queue; the worker is already pinned. Once
  // the queue reaches kThreadPoolQueueSize entries, the next accepts take the
  // queue_full branch: acceptor writes "ERR SERVER_BUSY" and close()s the fd
  // synchronously.
  constexpr int kBurst = 1200;
  std::vector<int> socks;
  socks.reserve(kBurst);
  for (int i = 0; i < kBurst; ++i) {
    int s = Connect(port, /*timeout_ms=*/500);
    if (s < 0) {
      // Out of FDs or connect() timed out; stop opening more.
      break;
    }
    socks.push_back(s);
  }

  // Let the server drain any pending accepts and emit BUSY responses.
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  int busy_msg = 0;
  int closed_by_server = 0;
  for (int s : socks) {
    struct timeval to {};
    to.tv_sec = 0;
    to.tv_usec = 50 * 1000;  // 50 ms
    setsockopt(s, SOL_SOCKET, SO_RCVTIMEO, &to, sizeof(to));
    std::array<char, 256> buf{};
    ssize_t n = recv(s, buf.data(), buf.size(), 0);
    if (n > 0) {
      std::string r(buf.data(), n);
      if (r.find("ERR SERVER_BUSY") != std::string::npos) {
        busy_msg++;
      }
    } else if (n == 0) {
      // Clean close from server side: FIN received.
      closed_by_server++;
    }
    // n < 0 with EAGAIN: socket is still open and queued (not yet serviced).
  }

  std::cout << "[queue_overflow] rlimit=" << effective_limit << " opened=" << socks.size()
            << " busy_msg=" << busy_msg << " closed_by_server=" << closed_by_server << std::endl;

  // Cleanup before any SKIP/assertion so server->Stop() doesn't block on a
  // flood of stale accepts.
  for (int s : socks) {
    close(s);
  }
  close(pinned);
  server->Stop();

  if (static_cast<int>(socks.size()) < kMinClientFdsToExercise) {
    GTEST_SKIP() << "Per-process fd budget is too small to exceed the 1000-slot "
                    "task queue (opened only "
                 << socks.size() << " client sockets, RLIMIT_NOFILE soft=" << effective_limit
                 << "). Re-run with `ulimit -n 8192` or on Linux with a larger "
                    "nofile limit to exercise the queue_full backpressure path.";
  }

  // Got enough sockets: the queue must have overflowed somewhere.
  EXPECT_GT(busy_msg + closed_by_server, 0)
      << "Expected the queue_full backpressure path to fire for at least one "
         "connection after opening "
      << socks.size()
      << " idle sockets against worker_threads=1. Either the queue grew larger "
         "than kThreadPoolQueueSize=1000, or the burst did not actually "
         "exhaust it.";
}

}  // namespace mygramdb::server
