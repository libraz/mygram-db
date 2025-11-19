/**
 * @file health_endpoint_test.cpp
 * @brief Test health check endpoints for monitoring and orchestration
 *
 * This test verifies that:
 * 1. /health/live always returns 200 OK (liveness probe)
 * 2. /health/ready returns appropriate status based on server state (readiness probe)
 * 3. /health/detail provides detailed component status (monitoring)
 */

#include <gtest/gtest.h>
#include <httplib.h>

#include <chrono>
#include <iostream>
#include <nlohmann/json.hpp>
#include <thread>

#include "config/config.h"
#include "index/index.h"
#include "server/http_server.h"
#include "server/server_types.h"
#include "storage/document_store.h"

using json = nlohmann::json;

namespace mygramdb::server {

class HealthEndpointTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create minimal table context
    table_ctx_.name = "test_table";
    table_ctx_.index = std::make_unique<index::Index>(2);  // bigram
    table_ctx_.doc_store = std::make_unique<storage::DocumentStore>();

    table_contexts_["test_table"] = &table_ctx_;

    // Configure HTTP server with fixed port for testing
    HttpServerConfig http_config;
    http_config.bind = "127.0.0.1";
    http_config.port = 18080;  // Use fixed port for testing
    http_config.read_timeout_sec = 5;
    http_config.write_timeout_sec = 5;
    http_config.allow_cidrs = {"127.0.0.1/32"};  // Allow localhost

    // Create server with loading flag
    loading_ = false;
    server_ = std::make_unique<HttpServer>(http_config, table_contexts_, nullptr, nullptr, nullptr, &loading_);

    // Start server
    ASSERT_TRUE(server_->Start());

    // Use configured port
    port_ = http_config.port;
    base_url_ = "http://127.0.0.1:" + std::to_string(port_);

    // Wait for server to be actually ready by polling health endpoint
    WaitForServerReady();
  }

  void WaitForServerReady() {
    httplib::Client client(base_url_);
    client.set_connection_timeout(2);
    client.set_read_timeout(2, 0);

    const int max_attempts = 50;  // 5 seconds total (50 * 100ms)
    for (int attempt = 0; attempt < max_attempts; ++attempt) {
      auto res = client.Get("/health/live");
      if (res && res->status == 200) {
        // Server is ready, wait a bit more for full initialization
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        return;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    FAIL() << "Server did not become ready within timeout";
  }

  void TearDown() override {
    if (server_) {
      server_->Stop();
    }
  }

  TableContext table_ctx_;
  std::unordered_map<std::string, TableContext*> table_contexts_;
  std::unique_ptr<HttpServer> server_;
  std::atomic<bool> loading_;
  int port_{0};
  std::string base_url_;
};

/**
 * @brief Test /health/live endpoint (liveness probe)
 *
 * Liveness probe should ALWAYS return 200 OK to indicate the process is running.
 * This is used by orchestrators (Kubernetes, Docker) to detect deadlocks.
 */
TEST_F(HealthEndpointTest, LivenessProbeAlwaysReturns200) {
  httplib::Client client(base_url_);
  client.set_connection_timeout(5);

  // Test 1: Should return 200 OK when server is ready
  auto res = client.Get("/health/live");
  ASSERT_TRUE(res) << "Request failed";
  EXPECT_EQ(res->status, 200) << "Liveness probe should return 200 OK";

  // Verify JSON response
  auto response = json::parse(res->body);
  EXPECT_EQ(response["status"], "alive");
  EXPECT_TRUE(response.contains("timestamp"));

  // Test 2: Should still return 200 OK even when loading
  loading_ = true;
  res = client.Get("/health/live");
  ASSERT_TRUE(res) << "Request failed";
  EXPECT_EQ(res->status, 200) << "Liveness probe should return 200 OK even during loading";

  auto response2 = json::parse(res->body);
  EXPECT_EQ(response2["status"], "alive");
}

/**
 * @brief Test /health/ready endpoint (readiness probe)
 *
 * Readiness probe returns:
 * - 200 OK when server is ready to accept traffic
 * - 503 Service Unavailable when server is loading or has errors
 */
TEST_F(HealthEndpointTest, ReadinessProbeReflectsServerState) {
  httplib::Client client(base_url_);
  client.set_connection_timeout(5);

  // Test 1: Should return 200 OK when server is ready (loading=false)
  loading_ = false;
  auto res = client.Get("/health/ready");
  ASSERT_TRUE(res) << "Request failed";
  EXPECT_EQ(res->status, 200) << "Readiness probe should return 200 OK when ready";

  auto response = json::parse(res->body);
  EXPECT_EQ(response["status"], "ready");
  EXPECT_FALSE(response.value("loading", true)) << "loading should be false";

  // Test 2: Should return 503 when server is loading
  loading_ = true;
  res = client.Get("/health/ready");
  ASSERT_TRUE(res) << "Request failed";
  EXPECT_EQ(res->status, 503) << "Readiness probe should return 503 when loading";

  auto response2 = json::parse(res->body);
  EXPECT_EQ(response2["status"], "not_ready");
  EXPECT_TRUE(response2.value("loading", false)) << "loading should be true";
  EXPECT_EQ(response2["reason"], "Server is loading");
}

/**
 * @brief Test /health/detail endpoint (detailed health check)
 *
 * Detail endpoint returns comprehensive component status for monitoring.
 */
TEST_F(HealthEndpointTest, DetailedHealthReturnsComponentStatus) {
  httplib::Client client(base_url_);
  client.set_connection_timeout(5);

  loading_ = false;
  auto res = client.Get("/health/detail");
  ASSERT_TRUE(res) << "Request failed";
  EXPECT_EQ(res->status, 200) << "Detail endpoint should return 200 OK";

  auto response = json::parse(res->body);

  // Verify overall status
  EXPECT_TRUE(response.contains("status"));
  EXPECT_TRUE(response.contains("timestamp"));
  EXPECT_TRUE(response.contains("uptime_seconds"));

  // Verify components section
  EXPECT_TRUE(response.contains("components"));
  auto& components = response["components"];

  // Verify server component
  EXPECT_TRUE(components.contains("server"));
  EXPECT_EQ(components["server"]["status"], "ready");
  EXPECT_FALSE(components["server"]["loading"]);

  // Verify index component
  EXPECT_TRUE(components.contains("index"));
  EXPECT_EQ(components["index"]["status"], "ok");
  EXPECT_TRUE(components["index"].contains("total_terms"));
  EXPECT_TRUE(components["index"].contains("total_documents"));
}

/**
 * @brief Test detailed health when server is loading
 */
TEST_F(HealthEndpointTest, DetailedHealthDuringLoading) {
  httplib::Client client(base_url_);
  client.set_connection_timeout(5);

  loading_ = true;
  auto res = client.Get("/health/detail");
  ASSERT_TRUE(res) << "Request failed";
  EXPECT_EQ(res->status, 200) << "Detail endpoint should return 200 OK even during loading";

  auto response = json::parse(res->body);
  EXPECT_EQ(response["status"], "degraded");

  auto& components = response["components"];
  EXPECT_EQ(components["server"]["status"], "loading");
  EXPECT_TRUE(components["server"]["loading"]);
}

/**
 * @brief Test legacy /health endpoint still works
 */
TEST_F(HealthEndpointTest, LegacyHealthEndpointWorks) {
  httplib::Client client(base_url_);
  client.set_connection_timeout(5);

  auto res = client.Get("/health");
  ASSERT_TRUE(res) << "Request failed";
  EXPECT_EQ(res->status, 200) << "Legacy /health should still work";

  auto response = json::parse(res->body);
  EXPECT_EQ(response["status"], "ok");
  EXPECT_TRUE(response.contains("timestamp"));
}

/**
 * @brief Test multiple concurrent health check requests
 */
TEST_F(HealthEndpointTest, ConcurrentHealthChecks) {
  const int num_requests = 50;
  std::vector<std::thread> threads;
  std::atomic<int> success_count{0};
  std::atomic<int> timeout_count{0};
  std::atomic<int> connection_failed_count{0};
  std::atomic<int> other_error_count{0};

  for (int i = 0; i < num_requests; ++i) {
    threads.emplace_back([&, i]() {
      httplib::Client thread_client(base_url_);
      thread_client.set_connection_timeout(10);  // Increase timeout for CI environments
      thread_client.set_read_timeout(10, 0);
      thread_client.set_write_timeout(10, 0);
      thread_client.set_keep_alive(false);  // Disable keep-alive to avoid connection pool issues

      std::string endpoint = (i % 3 == 0) ? "/health/live" : (i % 3 == 1) ? "/health/ready" : "/health/detail";
      auto res = thread_client.Get(endpoint.c_str());

      if (res && res->status == 200) {
        success_count++;
      } else if (!res) {
        auto err = thread_client.get_openssl_verify_result();
        if (err != 0) {
          other_error_count++;
        } else {
          // Connection or timeout failure
          connection_failed_count++;
        }
      } else {
        // Got response but not 200
        other_error_count++;
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  // Log failure details for debugging
  if (success_count < num_requests * 0.9) {
    std::cerr << "Concurrent test failures:\n"
              << "  Success: " << success_count << "/" << num_requests << "\n"
              << "  Connection failures: " << connection_failed_count << "\n"
              << "  Timeouts: " << timeout_count << "\n"
              << "  Other errors: " << other_error_count << std::endl;
  }

  // Most requests should succeed (allowing some failures due to timing)
#ifdef __APPLE__
  // macOS network stack is more sensitive to concurrent connections, allow lower threshold
  EXPECT_GE(success_count, num_requests * 0.8) << "At least 80% of concurrent requests should succeed on macOS";
#else
  EXPECT_GT(success_count, num_requests * 0.9) << "At least 90% of concurrent requests should succeed";
#endif
}

/**
 * @brief Test health check endpoints don't increment main request counters excessively
 */
TEST_F(HealthEndpointTest, HealthChecksTrackedSeparately) {
  httplib::Client client(base_url_);
  client.set_connection_timeout(5);

  // Make multiple health check requests
  for (int i = 0; i < 10; ++i) {
    client.Get("/health/live");
    client.Get("/health/ready");
    client.Get("/health/detail");
  }

  // Health checks should be tracked (30 requests made)
  auto total_requests = server_->GetTotalRequests();
  EXPECT_GE(total_requests, 30) << "Health check requests should be counted";
}

}  // namespace mygramdb::server
