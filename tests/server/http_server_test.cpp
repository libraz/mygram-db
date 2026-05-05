/**
 * @file http_server_test.cpp
 * @brief Tests for HttpServer startup race condition fix (promise/future pattern)
 */

#include "server/http_server.h"

#include <gtest/gtest.h>
#include <httplib.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <future>
#include <nlohmann/json.hpp>
#include <thread>
#include <vector>

#include "config/config.h"
#include "index/index.h"
#include "server/tcp_server.h"  // For TableContext definition
#include "storage/document_store.h"

namespace mygramdb {
namespace server {

class HttpServerStartupTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create minimal table context
    auto index = std::make_unique<index::Index>(1);
    auto doc_store = std::make_unique<storage::DocumentStore>();
    auto doc_id = doc_store->AddDocument("doc_1", {});
    index->AddDocument(*doc_id, "test document");

    table_context_.name = "test";
    table_context_.config.ngram_size = 1;
    table_context_.index = std::move(index);
    table_context_.doc_store = std::move(doc_store);
    table_contexts_["test"] = &table_context_;

    config_ = std::make_unique<config::Config>();
  }

  HttpServerConfig MakeConfig(int port) {
    HttpServerConfig cfg;
    cfg.bind = "127.0.0.1";
    cfg.port = port;
    cfg.allow_cidrs = {"127.0.0.1/32"};
    return cfg;
  }

  TableContext table_context_;
  std::unordered_map<std::string, TableContext*> table_contexts_;
  std::unique_ptr<config::Config> config_;
};

/**
 * @brief Verify that starting on a valid port succeeds without sleep-based polling
 */
TEST_F(HttpServerStartupTest, StartOnValidPortSucceeds) {
  auto cfg = MakeConfig(18090);
  HttpServer server(cfg, table_contexts_, config_.get());

  auto result = server.Start();
  ASSERT_TRUE(result.has_value()) << "Start() should succeed on an available port";
  EXPECT_TRUE(server.IsRunning());

  // Verify the server actually accepts connections
  httplib::Client client("http://127.0.0.1:18090");
  auto res = client.Get("/health");
  ASSERT_TRUE(res);
  EXPECT_EQ(res->status, 200);

  server.Stop();
  EXPECT_FALSE(server.IsRunning());
}

/**
 * @brief Verify that starting on a port already in use returns an error (no crash or hang)
 *
 * Uses a raw socket with SO_REUSEADDR disabled to exclusively occupy the port,
 * preventing httplib's SO_REUSEADDR from allowing a second bind.
 */
TEST_F(HttpServerStartupTest, StartOnOccupiedPortReturnsError) {
  // Occupy the port with a raw socket (without SO_REUSEADDR)
  int sock_fd = ::socket(AF_INET, SOCK_STREAM, 0);
  ASSERT_GE(sock_fd, 0) << "Failed to create socket";

  // Explicitly disable SO_REUSEADDR so httplib cannot share the port
  int opt_off = 0;
  ::setsockopt(sock_fd, SOL_SOCKET, SO_REUSEADDR, &opt_off, sizeof(opt_off));
#ifdef SO_REUSEPORT
  ::setsockopt(sock_fd, SOL_SOCKET, SO_REUSEPORT, &opt_off, sizeof(opt_off));
#endif

  struct sockaddr_in addr {};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = htons(18091);

  int bind_result = ::bind(sock_fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr));
  ASSERT_EQ(bind_result, 0) << "Failed to bind raw socket to port 18091";
  ASSERT_EQ(::listen(sock_fd, 1), 0) << "Failed to listen on raw socket";

  // Now attempt to start HttpServer on the same port
  auto cfg = MakeConfig(18091);
  HttpServer server(cfg, table_contexts_, config_.get());
  auto result = server.Start();

  // Should fail with an error, not crash or hang
  ASSERT_FALSE(result.has_value()) << "Server should fail to bind to occupied port";
  EXPECT_FALSE(server.IsRunning());

  // Verify error message mentions the bind failure
  const auto& error = result.error();
  EXPECT_TRUE(error.message().find("Failed to bind") != std::string::npos)
      << "Error should mention bind failure, got: " << error.message();

  ::close(sock_fd);
}

/**
 * @brief H-N1 regression: bind-failure path returns within milliseconds and
 *        leaves the server in a fresh state ready for reuse.
 *
 * Pre-fix, Start() spawned a worker thread that called bind_to_port and
 * signalled the parent through a promise/future with a 5s timeout. If
 * bind_to_port stalled (or was simply slow to schedule on a loaded host),
 * the parent's wait_for could time out, then it would call
 * server_->stop() (a no-op when listen_after_bind had not started) and
 * server_thread_->join(), which could hang indefinitely. Now bind_to_port
 * runs synchronously on the calling thread, so the failure path returns
 * with no thread to join.
 *
 * The assertion is on wall-clock time: a synchronous bind to an occupied
 * port returns in microseconds; if the join-deadlock regression returns,
 * this test would block on the destructor for tens of seconds.
 */
TEST_F(HttpServerStartupTest, BindFailureReturnsPromptly) {
  // Occupy a port so the second Start() has to fail bind.
  int sock_fd = ::socket(AF_INET, SOCK_STREAM, 0);
  ASSERT_GE(sock_fd, 0);
  int opt_off = 0;
  ::setsockopt(sock_fd, SOL_SOCKET, SO_REUSEADDR, &opt_off, sizeof(opt_off));
#ifdef SO_REUSEPORT
  ::setsockopt(sock_fd, SOL_SOCKET, SO_REUSEPORT, &opt_off, sizeof(opt_off));
#endif
  struct sockaddr_in addr {};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = htons(18097);
  ASSERT_EQ(::bind(sock_fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)), 0);
  ASSERT_EQ(::listen(sock_fd, 1), 0);

  auto cfg = MakeConfig(18097);

  // Wrap Start() in a future so we can assert on completion time without
  // letting a regression deadlock the test process.
  auto start_done = std::async(std::launch::async, [&]() {
    HttpServer server(cfg, table_contexts_, config_.get());
    auto result = server.Start();
    return result.has_value();
  });

  // Synchronous bind should return well under a second on any reasonable
  // platform. The previous 5-second timeout for the worker-thread design is
  // 5x this budget, so a regression manifests as a wait_for hit.
  auto status = start_done.wait_for(std::chrono::seconds(2));
  ASSERT_EQ(status, std::future_status::ready) << "Start() did not return promptly on bind failure";
  EXPECT_FALSE(start_done.get()) << "Start() unexpectedly succeeded on occupied port";

  ::close(sock_fd);
}

/**
 * @brief Verify that double-start returns an appropriate error
 */
TEST_F(HttpServerStartupTest, DoubleStartReturnsError) {
  auto cfg = MakeConfig(18092);
  HttpServer server(cfg, table_contexts_, config_.get());

  auto result1 = server.Start();
  ASSERT_TRUE(result1.has_value());

  auto result2 = server.Start();
  ASSERT_FALSE(result2.has_value()) << "Double-start should return an error";

  server.Stop();
}

/**
 * @brief P0-C regression: concurrent Start() calls must serialize through a
 *        single CAS, with at most one success.
 *
 * Pre-fix, Start() did a relaxed `if (running_) return` followed by a relaxed
 * `running_ = true` store. Two threads could both observe running_=false and
 * both proceed past the gate, racing the spawning of the server thread. The
 * fix replaces the check-then-set with compare_exchange_strong; this test
 * spawns N threads that all call Start() at the same time and asserts that
 * exactly one succeeds while the rest receive the "already running" error.
 */
TEST_F(HttpServerStartupTest, ConcurrentStartCallsNoRace) {
  constexpr int kPort = 18093;
  constexpr int kThreadCount = 8;

  auto cfg = MakeConfig(kPort);
  HttpServer server(cfg, table_contexts_, config_.get());

  // Synchronize all worker threads on a barrier so they collide on the CAS
  // as tightly as possible.
  std::atomic<int> ready_count{0};
  std::atomic<bool> go{false};
  std::vector<std::future<bool>> futures;
  futures.reserve(kThreadCount);

  for (int i = 0; i < kThreadCount; ++i) {
    futures.push_back(std::async(std::launch::async, [&server, &ready_count, &go]() {
      ready_count.fetch_add(1, std::memory_order_release);
      while (!go.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
      auto r = server.Start();
      return r.has_value();
    }));
  }

  // Wait for all threads to be parked at the barrier, then release them.
  while (ready_count.load(std::memory_order_acquire) < kThreadCount) {
    std::this_thread::yield();
  }
  go.store(true, std::memory_order_release);

  int success_count = 0;
  int already_running_count = 0;
  for (auto& f : futures) {
    if (f.get()) {
      ++success_count;
    } else {
      ++already_running_count;
    }
  }

  EXPECT_EQ(success_count, 1) << "Exactly one Start() should win the CAS";
  EXPECT_EQ(already_running_count, kThreadCount - 1) << "All other Start() calls should report already-running";
  EXPECT_TRUE(server.IsRunning());

  // Sanity: server is actually responsive after the race resolves.
  httplib::Client client("http://127.0.0.1:" + std::to_string(kPort));
  auto res = client.Get("/health");
  ASSERT_TRUE(res);
  EXPECT_EQ(res->status, 200);

  server.Stop();
  EXPECT_FALSE(server.IsRunning());
}

/**
 * @brief Fix N-2: HttpServer caps request body size with `set_payload_max_length`.
 *
 * Configuring `max_body_bytes = 1024` and POSTing a 2 KiB body must elicit
 * a 4xx response from cpp-httplib, NOT crash and NOT route into the search
 * handler. cpp-httplib returns 413 Payload Too Large for this case.
 */
TEST_F(HttpServerStartupTest, RejectsBodyExceedingMaxSize) {
  auto cfg = MakeConfig(18094);
  cfg.max_body_bytes = 1024;  // 1 KiB cap

  HttpServer server(cfg, table_contexts_, config_.get());
  ASSERT_TRUE(server.Start().has_value());

  httplib::Client client("http://127.0.0.1:18094");

  // Build a 2 KiB JSON-shaped body. The actual content does not need to be
  // valid JSON because the server should reject it before parsing.
  std::string padding(2048, 'x');
  std::string body = R"({"q":")" + padding + R"("})";
  ASSERT_GT(body.size(), cfg.max_body_bytes);

  auto res = client.Post("/test/search", body, "application/json");
  ASSERT_TRUE(res) << "POST failed at the network layer";
  EXPECT_EQ(res->status, 413) << "Expected 413 Payload Too Large for body > max_body_bytes";

  server.Stop();
}

/**
 * @brief Regression: GET /{table}/:id must apply the same table-name
 *        whitelist as SEARCH/COUNT.
 *
 * Pre-fix, HandleGet only checked table_contexts_.find() for the URL-bound
 * table name. A name containing characters outside the parser-grammar safe
 * set (e.g. `te$st`) would fall through to a 404 ("Table not found") even
 * though the input was syntactically invalid. Now ResolveHttpTableContext
 * is shared, so GET returns a 400 for invalid names just like SEARCH.
 */
TEST_F(HttpServerStartupTest, HandleGetRejectsInvalidTableName) {
  auto cfg = MakeConfig(18096);
  HttpServer server(cfg, table_contexts_, config_.get());
  ASSERT_TRUE(server.Start().has_value());

  httplib::Client client("http://127.0.0.1:18096");

  // `te$st` contains an ASCII punctuation character that is rejected by the
  // table-name whitelist (only [A-Za-z0-9._-] plus non-ASCII bytes are
  // allowed). The cpp-httplib route regex `[^/]+` still matches, so the
  // request reaches HandleGet, where ResolveHttpTableContext rejects it.
  auto res = client.Get("/te$st/42");
  ASSERT_TRUE(res);
  EXPECT_EQ(res->status, 400) << "Invalid table name should produce 400, got " << res->status;

  server.Stop();
}

/**
 * @brief Fix N-2: bodies at or below the cap are still accepted.
 *
 * Companion to RejectsBodyExceedingMaxSize: confirms the cap doesn't break
 * normal traffic. A small valid body should be processed normally (the
 * search itself may yield 0 or N results depending on the test fixture, but
 * the response status must NOT be 413).
 */
TEST_F(HttpServerStartupTest, AcceptsBodyWithinMaxSize) {
  auto cfg = MakeConfig(18095);
  cfg.max_body_bytes = 1024;

  HttpServer server(cfg, table_contexts_, config_.get());
  ASSERT_TRUE(server.Start().has_value());

  httplib::Client client("http://127.0.0.1:18095");
  std::string body = R"({"q":"test"})";
  ASSERT_LE(body.size(), cfg.max_body_bytes);

  auto res = client.Post("/test/search", body, "application/json");
  ASSERT_TRUE(res);
  EXPECT_NE(res->status, 413) << "Body within cap should not be rejected as too large";

  server.Stop();
}

/**
 * @brief H-N8 regression: HttpServerConfig::FromConfig must propagate the
 *        api.http.read_timeout_sec / write_timeout_sec values from the
 *        parsed Config struct into the HttpServerConfig used by HttpServer.
 *
 * Pre-fix, those fields did not exist on `ApiConfig::http` and FromConfig had
 * no source data, so HttpServer always ran with the hardcoded 5s defaults
 * regardless of YAML. This test verifies the wiring all the way from Config
 * to HttpServerConfig (HttpServer itself applies the values to httplib via
 * set_read_timeout / set_write_timeout, which is unit-tested at httplib's
 * own layer).
 */
TEST_F(HttpServerStartupTest, FromConfigPropagatesHttpTimeouts) {
  config::Config cfg;
  cfg.api.http.read_timeout_sec = 17;
  cfg.api.http.write_timeout_sec = 23;

  HttpServerConfig hc = HttpServerConfig::FromConfig(cfg);
  EXPECT_EQ(hc.read_timeout_sec, 17);
  EXPECT_EQ(hc.write_timeout_sec, 23);
}

/**
 * @brief Companion to FromConfigPropagatesHttpTimeouts: 0 / negative values
 *        are treated as "not configured" so an operator opting out by
 *        omitting the key keeps the struct default instead of disabling
 *        timeouts entirely.
 */
TEST_F(HttpServerStartupTest, FromConfigIgnoresNonPositiveTimeouts) {
  config::Config cfg;
  cfg.api.http.read_timeout_sec = 0;
  cfg.api.http.write_timeout_sec = -1;

  HttpServerConfig hc = HttpServerConfig::FromConfig(cfg);
  EXPECT_EQ(hc.read_timeout_sec, defaults::kHttpTimeoutSec);
  EXPECT_EQ(hc.write_timeout_sec, defaults::kHttpTimeoutSec);
}

}  // namespace server
}  // namespace mygramdb
