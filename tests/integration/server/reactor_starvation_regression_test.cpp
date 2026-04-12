/**
 * @file reactor_starvation_regression_test.cpp
 * @brief Reactor-mode invariant: decouples the concurrent-client count from
 *        the worker pool size.
 *
 * The reactor I/O model uses an epoll/kqueue event loop + drain-task-per-
 * connection workers, so persistent idle clients do NOT pin worker threads.
 * These tests prove the invariant holds.
 *
 * Test 1: 128 persistent clients + 1 late client, worker_threads=2 — late
 *   client MUST get a response within 500 ms.
 * Test 2: 128 persistent clients each sending 10 INFO requests —
 *   no ERR SERVER_BUSY, all responses are OK INFO.
 *
 * Labeled "INTEGRATION" — runs via `ctest -L INTEGRATION`.
 * RESOURCE_LOCK "server_port" — each test binds a real OS port.
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

// ---------------------------------------------------------------------------
// Shared test fixture
// ---------------------------------------------------------------------------
class ReactorStarvationRegressionTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Raise fd limit to accommodate 256+ client sockets plus server-side fds.
    effective_fd_limit_ = RaiseFdLimit(4096);

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
  // Socket helpers (mirrored from ThreadPoolSaturationTest for isolation)
  // -------------------------------------------------------------------------

  /**
   * @brief Connect to 127.0.0.1:port with a non-blocking connect + select.
   * @return socket fd, or -1 on failure.
   */
  int Connect(uint16_t port, int timeout_ms = 2000) {
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

    // Restore blocking mode after connect
    if (flags >= 0) {
      fcntl(sock, F_SETFL, flags);
    }
    return sock;
  }

  bool SendLine(int sock, const std::string& line) {
    std::string msg = line + "\r\n";
    ssize_t sent = send(sock, msg.data(), msg.size(), 0);
    return sent == static_cast<ssize_t>(msg.size());
  }

  /**
   * @brief Read a full multi-line response ending in `\r\nEND\r\n`.
   *
   * INFO, SEARCH and friends are multi-line: the response body contains
   * internal `\r\n` separators and ends with `\r\nEND\r\n`. A simple
   * read-until-first-CRLF helper would truncate at "OK INFO" and leave the
   * rest of the response in the kernel socket buffer, corrupting any
   * subsequent read on the same connection.
   *
   * Returns the full response bytes (including internal CRLFs and the
   * trailing `\r\nEND\r\n`), or empty string on timeout / peer close.
   */
  std::string RecvMultilineResponse(int sock, int timeout_ms = 5000) {
    struct timeval tv {};
    tv.tv_sec = timeout_ms / 1000;
    tv.tv_usec = (timeout_ms % 1000) * 1000;
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    static constexpr const char kTerminator[] = "\r\nEND\r\n";
    static constexpr size_t kTerminatorLen = 7;

    std::string out;
    std::array<char, 1024> buf{};
    while (out.size() < 1 * 1024 * 1024) {
      ssize_t n = recv(sock, buf.data(), buf.size(), 0);
      if (n <= 0) {
        return "";
      }
      out.append(buf.data(), static_cast<size_t>(n));
      if (out.size() >= kTerminatorLen && out.compare(out.size() - kTerminatorLen, kTerminatorLen, kTerminator) == 0) {
        return out;
      }
    }
    return "";
  }

  // -------------------------------------------------------------------------
  // Server factory
  // -------------------------------------------------------------------------
  std::unique_ptr<TcpServer> StartServer(int worker_threads, int max_connections = 512) {
    ServerConfig cfg;
    cfg.host = "127.0.0.1";
    cfg.port = 0;
    cfg.worker_threads = worker_threads;
    cfg.max_connections = max_connections;
    cfg.allow_cidrs = {"127.0.0.1/32"};
    // Disable recv timeout so idle persistent clients don't self-terminate
    // during the test; the starvation window is deliberately short.
    cfg.recv_timeout_sec = 0;
    auto s = std::make_unique<TcpServer>(cfg, table_contexts_);
    auto res = s->Start();
    EXPECT_TRUE(res) << "Failed to start TcpServer: " << (res ? std::string{} : res.error().to_string());
    // Give the accept loop a moment to reach its main select/epoll/kqueue call.
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    return s;
  }

  // -------------------------------------------------------------------------
  // FD limit helper
  // -------------------------------------------------------------------------
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

  // -------------------------------------------------------------------------
  // State
  // -------------------------------------------------------------------------
  rlim_t effective_fd_limit_{0};
  std::unique_ptr<TableContext> table_ctx_;
  std::unordered_map<std::string, TableContext*> table_contexts_;
};

// ---------------------------------------------------------------------------
// Test 1 — Reactor serves 128 persistent clients without starving a late
//           client (worker_threads = 2).
// ---------------------------------------------------------------------------
/**
 * Pin 128 persistent idle clients against a 2-worker reactor server. The
 * event loop dispatches all 128 initial INFO responses, and the 129th
 * (late) client still gets a reply within 500 ms.
 */
TEST_F(ReactorStarvationRegressionTest, ReactorServesPersistentFleetWithoutStarvation) {
  // Need at least 256 fds: 128 pinned + 1 late + server-side fds + overhead.
  constexpr int kNeededFds = 256;
  // RLIM_INFINITY means unlimited; only skip when we have a real hard cap below what we need.
  if (effective_fd_limit_ != RLIM_INFINITY && effective_fd_limit_ < static_cast<rlim_t>(kNeededFds)) {
    GTEST_SKIP() << "RLIMIT_NOFILE=" << effective_fd_limit_ << " < " << kNeededFds
                 << " — cannot open enough client sockets. Run with ulimit -n 4096.";
  }

  constexpr int kWorkers = 2;
  constexpr int kPinnedClients = 128;
  auto server = StartServer(kWorkers, /*max_connections=*/512);
  uint16_t port = server->GetPort();

  // --- Pin kPinnedClients persistent idle clients ---
  // Each sends INFO once (to exercise the reactor's drain path), then idles.
  std::vector<int> pinned;
  pinned.reserve(kPinnedClients);
  int pin_failures = 0;

  for (int i = 0; i < kPinnedClients; ++i) {
    int s = Connect(port);
    if (s < 0) {
      ++pin_failures;
      continue;
    }
    if (!SendLine(s, "INFO")) {
      close(s);
      ++pin_failures;
      continue;
    }
    pinned.push_back(s);
  }

  if (pin_failures > 0) {
    for (int s : pinned)
      close(s);
    server->Stop();
    GTEST_SKIP() << "Could only open " << pinned.size() << " of " << kPinnedClients
                 << " pinned client sockets (fd budget?). Skipping.";
  }

  // Drain the initial INFO responses for all pinned clients in parallel.
  // Serial draining would take kPinnedClients * timeout_ms worst-case;
  // parallel draining bounds the total wait to one timeout window.
  std::atomic<int> pinned_ok{0};
  {
    std::vector<std::thread> drain_threads;
    drain_threads.reserve(pinned.size());
    for (int s : pinned) {
      drain_threads.emplace_back([this, s, &pinned_ok]() {
        std::string resp = RecvMultilineResponse(s, /*timeout_ms=*/5000);
        if (resp.size() >= 7 && resp.substr(0, 7) == "OK INFO") {
          pinned_ok.fetch_add(1, std::memory_order_relaxed);
        }
      });
    }
    for (auto& t : drain_threads) {
      t.join();
    }
  }

  EXPECT_EQ(pinned_ok.load(), kPinnedClients) << "Only " << pinned_ok.load() << " of " << kPinnedClients
                                              << " pinned clients received their initial INFO response. "
                                                 "The reactor event loop did not dispatch all connections.";

  // All pinned clients are now idle. In reactor mode the workers are free;
  // the event loop is the only thing monitoring those fds.

  // --- Late client ---
  int late = Connect(port);
  ASSERT_GE(late, 0) << "Failed to connect late client";
  ASSERT_TRUE(SendLine(late, "INFO"));

  auto t0 = std::chrono::steady_clock::now();
  std::string late_resp = RecvMultilineResponse(late, /*timeout_ms=*/500);
  auto elapsed_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();

  // --- Assertions ---
  EXPECT_FALSE(late_resp.empty()) << "Late client received no response within 500 ms despite reactor mode "
                                     "being active. Starvation regression: reactor is not decoupling "
                                     "worker count from persistent-client count.";

  if (!late_resp.empty()) {
    EXPECT_EQ(late_resp.substr(0, 7), "OK INFO") << "Late client response was not OK INFO: " << late_resp;
    EXPECT_LT(elapsed_ms, 500) << "Late client responded (" << late_resp << ") but took " << elapsed_ms
                               << "ms (> 500ms limit).";

    std::cout << "[T1] late_client_latency_ms=" << elapsed_ms << " pinned_ok=" << pinned_ok.load()
              << " workers=" << kWorkers << std::endl;
  }

  // Cleanup
  close(late);
  for (int s : pinned)
    close(s);
  server->Stop();
}

// ---------------------------------------------------------------------------
// Test 2 — Throughput: 128 persistent clients × 10 INFO requests, no
//           ERR SERVER_BUSY, all responses are "OK INFO".
// ---------------------------------------------------------------------------
/**
 * Open 128 connections and send 10 sequential INFO requests on each. In
 * reactor mode the event loop processes reads and dispatches drain tasks
 * back-to-back on a small worker pool, and all 1280 responses arrive
 * correctly.
 */
TEST_F(ReactorStarvationRegressionTest, ReactorHighConcurrencyShowsNoQueueFull) {
  // 256 client fds + server-side fds + overhead.
  constexpr int kNeededFds = 256;
  if (effective_fd_limit_ != RLIM_INFINITY && effective_fd_limit_ < static_cast<rlim_t>(kNeededFds)) {
    GTEST_SKIP() << "RLIMIT_NOFILE=" << effective_fd_limit_ << " < " << kNeededFds
                 << " — cannot open enough client sockets.";
  }

  constexpr int kWorkers = 2;
  // Scaled down to 128 (from 256) to fit within macOS soft RLIMIT_NOFILE=256
  // (128 client fds + 128 server-side fds + overhead stays within 256).
  constexpr int kClients = 128;
  constexpr int kRequestsPerClient = 10;
  constexpr int kExpectedTotal = kClients * kRequestsPerClient;

  auto server = StartServer(kWorkers, /*max_connections=*/512);
  uint16_t port = server->GetPort();

  // Open kClients connections.
  std::vector<int> clients;
  clients.reserve(kClients);
  for (int i = 0; i < kClients; ++i) {
    int s = Connect(port);
    if (s < 0)
      break;
    clients.push_back(s);
  }

  if (static_cast<int>(clients.size()) < kClients) {
    for (int s : clients)
      close(s);
    server->Stop();
    GTEST_SKIP() << "Could only open " << clients.size() << " of " << kClients << " client sockets (fd budget?).";
  }

  // Snapshot request counter before the burst.
  uint64_t reqs_before = server->GetStats().GetTotalRequests();

  // Each client sends kRequestsPerClient INFO requests sequentially.
  // We use threads so that the clients exercise the reactor concurrently.
  std::atomic<int> total_ok{0};
  std::atomic<int> total_busy{0};
  std::atomic<int> total_error{0};

  std::vector<std::thread> threads;
  threads.reserve(clients.size());

  for (int i = 0; i < static_cast<int>(clients.size()); ++i) {
    threads.emplace_back([&, i]() {
      int s = clients[static_cast<size_t>(i)];
      for (int j = 0; j < kRequestsPerClient; ++j) {
        if (!SendLine(s, "INFO")) {
          total_error.fetch_add(1, std::memory_order_relaxed);
          return;
        }
        std::string resp = RecvMultilineResponse(s, /*timeout_ms=*/5000);
        if (resp.empty()) {
          total_error.fetch_add(1, std::memory_order_relaxed);
          return;
        }
        if (resp.find("ERR SERVER_BUSY") != std::string::npos) {
          total_busy.fetch_add(1, std::memory_order_relaxed);
        } else if (resp.size() >= 7 && resp.substr(0, 7) == "OK INFO") {
          total_ok.fetch_add(1, std::memory_order_relaxed);
        } else {
          total_error.fetch_add(1, std::memory_order_relaxed);
        }
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  uint64_t reqs_after = server->GetStats().GetTotalRequests();

  std::cout << "[T2] total_ok=" << total_ok.load() << " total_busy=" << total_busy.load()
            << " total_error=" << total_error.load() << " stat_requests_delta=" << (reqs_after - reqs_before)
            << std::endl;

  // --- Assertions ---
  EXPECT_EQ(total_busy.load(), 0) << "Reactor mode sent ERR SERVER_BUSY " << total_busy.load()
                                  << " times. The worker pool should not saturate when the reactor is active.";

  EXPECT_EQ(total_ok.load(), kExpectedTotal)
      << "Expected " << kExpectedTotal << " OK INFO responses but got " << total_ok.load()
      << " (errors=" << total_error.load() << ", busy=" << total_busy.load() << ").";

  EXPECT_GE(reqs_after - reqs_before, static_cast<uint64_t>(kExpectedTotal))
      << "Server stats counted fewer total requests than expected.";

  // Cleanup
  for (int s : clients)
    close(s);
  server->Stop();
}

}  // namespace mygramdb::server
