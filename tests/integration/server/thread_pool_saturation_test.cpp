/**
 * @file thread_pool_saturation_test.cpp
 * @brief Regression tests anchoring the reactor I/O model's thread-pool
 *        invariants.
 *
 * Under the reactor I/O model, persistent idle connections live in the
 * epoll/kqueue registration map and do NOT pin worker threads. The tests
 * below verify that invariant: late clients must be served promptly even
 * when the worker pool is tiny and many persistent clients are holding
 * connections.
 *
 * Labeled "LOAD" — excluded from `make test-fast` / CI and only run via
 * `make test-load` or `ctest -L LOAD`.
 */

#include <arpa/inet.h>
#include <fcntl.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <chrono>
#include <cstring>
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

  std::unique_ptr<TcpServer> StartServer(int worker_threads, int max_connections = 256) {
    ServerConfig cfg;
    cfg.host = "127.0.0.1";
    cfg.port = 0;
    cfg.worker_threads = worker_threads;
    cfg.max_connections = max_connections;
    cfg.allow_cidrs = {"127.0.0.1/32"};
    auto s = std::make_unique<TcpServer>(cfg, table_contexts_);
    auto res = s->Start();
    EXPECT_TRUE(res) << "Failed to start TcpServer: " << (res ? std::string{} : res.error().to_string());
    // Give the accept loop a moment to reach its main loop.
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    return s;
  }

  std::unique_ptr<TableContext> table_ctx_;
  std::unordered_map<std::string, TableContext*> table_contexts_;
};

/**
 * Reactor invariant: a late client MUST be served promptly even when
 * `worker_threads` is tiny and matches (or is below) the idle-persistent-
 * client count. Under the reactor I/O model, persistent idle connections
 * live in the epoll/kqueue registration map rather than parking worker
 * threads.
 *
 * If this test ever starts failing, the reactor no longer decouples
 * connection lifetime from worker lifetime.
 */
TEST_F(ThreadPoolSaturationTest, LateClientServedDespitePinnedIdleClientsInDefaultMode) {
  // Intentionally tiny worker pool to make the "workers == idle clients"
  // corner case trivially reachable. Under reactor mode this is fine — the
  // event loop handles reads and workers only run briefly per command.
  constexpr int kWorkers = 2;
  auto server = StartServer(kWorkers);
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
    ASSERT_EQ(resp.substr(0, 7), "OK INFO") << "Pinning client " << i << " did not receive INFO reply; got: " << resp;
    pinned.push_back(s);
  }

  // Give the reactor a moment to re-arm kReadable for the pinned clients.
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // Late client: should be served immediately under reactor mode.
  int late = Connect(port);
  ASSERT_GE(late, 0);
  ASSERT_TRUE(SendLine(late, "INFO"));

  auto t0 = std::chrono::steady_clock::now();
  std::string resp = RecvLine(late, /*timeout_ms=*/1500);
  auto elapsed_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();

  EXPECT_FALSE(resp.empty()) << "Late INFO was NOT served under the reactor I/O model even though "
                                "the worker pool was small and all persistent clients were idle. "
                                "The reactor must not couple connection lifetime to worker lifetime.";
  if (!resp.empty()) {
    EXPECT_EQ(resp.substr(0, 7), "OK INFO");
    EXPECT_LT(elapsed_ms, 500) << "Late client was served but took " << elapsed_ms
                               << "ms (>500ms). Reactor latency degraded?";
  }

  close(late);
  for (int s : pinned) {
    close(s);
  }
  server->Stop();
}

/**
 * Regression test for the auto-sized thread pool under the reactor I/O
 * model.
 *
 * With `worker_threads=0` → auto-size = max(hw*2, 4), persistent idle
 * clients do NOT pin workers — they live in the reactor's connection map
 * and only consume a worker briefly when a complete command frame arrives.
 * This test pins 16 persistent clients (well above the auto-sized worker
 * count on typical CI runners) and asserts that a late client is still
 * served quickly.
 */
TEST_F(ThreadPoolSaturationTest, DefaultAutoSizeServesManyPersistentClients) {
  // worker_threads=0 exercises the auto-sizing path in ThreadPool's constructor.
  auto server = StartServer(/*worker_threads=*/0);
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
    ASSERT_EQ(RecvLine(s).substr(0, 7), "OK INFO") << "Pinning client " << i << " did not receive INFO reply";
    pinned.push_back(s);
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  int late = Connect(port);
  ASSERT_GE(late, 0);
  ASSERT_TRUE(SendLine(late, "INFO"));

  auto t0 = std::chrono::steady_clock::now();
  std::string resp = RecvLine(late, /*timeout_ms=*/1500);
  auto elapsed_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();

  EXPECT_FALSE(resp.empty()) << "Late client starved with " << kPinned
                             << " pinned persistent clients — has the reactor regressed, or did the "
                                "thread pool auto-size floor drop below 4?";
  if (!resp.empty()) {
    EXPECT_EQ(resp.substr(0, 7), "OK INFO");
    EXPECT_LT(elapsed_ms, 500) << "Late client responded but took " << elapsed_ms << "ms";
  }

  close(late);
  for (int s : pinned) {
    close(s);
  }
  server->Stop();
}

}  // namespace mygramdb::server
