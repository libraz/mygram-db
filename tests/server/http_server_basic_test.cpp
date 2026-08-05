/**
 * @file http_server_basic_test.cpp
 * @brief HTTP server basic lifecycle and core endpoint tests
 */

#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <httplib.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <nlohmann/json.hpp>
#include <thread>
#include <vector>

#include "cache/cache_manager.h"
#include "config/config.h"
#include "index/index.h"
#include "query/query_parser.h"
#include "server/denial_log_limiter.h"
#include "server/http_server.h"
#include "server/tcp_server.h"  // For TableContext definition
#include "storage/document_store.h"
#include "version.h"

using json = nlohmann::json;

namespace mygramdb {
namespace server {

namespace {

uint16_t FindAvailableLoopbackPort() {
  int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    return 0;
  }

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = htons(0);

  if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
    ::close(fd);
    return 0;
  }

  sockaddr_in actual_addr{};
  socklen_t addr_len = sizeof(actual_addr);
  if (::getsockname(fd, reinterpret_cast<sockaddr*>(&actual_addr), &addr_len) != 0) {
    ::close(fd);
    return 0;
  }

  ::close(fd);
  return ntohs(actual_addr.sin_port);
}

uint16_t FindAvailableIpv6LoopbackPort() {
  int fd = ::socket(AF_INET6, SOCK_STREAM, 0);
  if (fd < 0) {
    return 0;
  }
  sockaddr_in6 addr{};
  addr.sin6_family = AF_INET6;
  addr.sin6_addr = in6addr_loopback;
  addr.sin6_port = htons(0);
  if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
    ::close(fd);
    return 0;
  }
  socklen_t addr_len = sizeof(addr);
  if (::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &addr_len) != 0) {
    ::close(fd);
    return 0;
  }
  ::close(fd);
  return ntohs(addr.sin6_port);
}

}  // namespace

class HttpServerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create index and document store
    auto index = std::make_unique<index::Index>(1);  // ngram_size = 1
    auto doc_store = std::make_unique<storage::DocumentStore>();

    // Add test documents
    storage::FilterMap filters1;
    filters1["status"] = static_cast<int64_t>(1);
    filters1["category"] = std::string("tech");
    filters1["score"] = 3.14159;
    filters1["series"] = std::string("Project X=Beta");
    auto doc_id1 = doc_store->AddDocument("article_1", filters1);

    storage::FilterMap filters2;
    filters2["status"] = static_cast<int64_t>(1);
    filters2["category"] = std::string("news");
    filters2["score"] = 1.61803;
    auto doc_id2 = doc_store->AddDocument("article_2", filters2);

    storage::FilterMap filters3;
    filters3["status"] = static_cast<int64_t>(0);
    auto doc_id3 = doc_store->AddDocument("article_3", filters3);

    // Index documents
    index->AddDocument(*doc_id1, "machine learning");
    index->AddDocument(*doc_id2, "breaking news");
    index->AddDocument(*doc_id3, "old article");

    // Create table context
    table_context_.name = "test";
    table_context_.config.ngram_size = 1;
    table_context_.index = std::move(index);
    table_context_.doc_store = std::move(doc_store);

    // Keep raw pointers for test access
    index_ = table_context_.index.get();
    doc_store_ = table_context_.doc_store.get();

    table_contexts_["test"] = &table_context_;
    port_ = FindAvailableLoopbackPort();
    ASSERT_GT(port_, 0);

    // Create config
    config_ = std::make_unique<config::Config>();
    config_->mysql.host = "127.0.0.1";
    config_->mysql.port = 3306;
    config_->mysql.database = "testdb";
    config_->mysql.user = "test_user";
    config_->api.tcp.bind = "127.0.0.1";
    config_->api.tcp.port = 11016;
    config_->api.http.enable = true;
    config_->api.http.bind = "127.0.0.1";
    config_->api.http.port = port_;
    config_->api.http.enable_cors = false;
    config_->api.http.cors_allow_origin = "*";
    config_->replication.enable = false;
    config_->replication.server_id = 12345;

    // Create HTTP server
    HttpServerConfig http_config;
    http_config.bind = "127.0.0.1";
    http_config.port = port_;
    http_config.allow_cidrs = {"127.0.0.1/32"};  // Allow localhost

    http_config.enable_cors = false;
    http_config.cors_allow_origin = "*";

    http_server_ = std::make_unique<HttpServer>(http_config, table_contexts_, config_.get(), nullptr);
  }

  void TearDown() override {
    if (http_server_ && http_server_->IsRunning()) {
      http_server_->Stop();
    }
    // Wait a bit for server to fully stop
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }

  index::Index* index_;                // Raw pointer to table_context_.index
  storage::DocumentStore* doc_store_;  // Raw pointer to table_context_.doc_store
  TableContext table_context_;
  std::unordered_map<std::string, TableContext*> table_contexts_;
  std::unique_ptr<config::Config> config_;
  std::unique_ptr<HttpServer> http_server_;
  uint16_t port_ = 0;
};

TEST(DenialLogLimiterTest, BoundsDistinctAndRepeatedAttackLogsAndReportsAggregate) {
  DenialLogLimiter limiter(std::chrono::seconds(60), 2);
  const auto start = DenialLogLimiter::Clock::now();

  EXPECT_TRUE(limiter.RecordAt("acl:192.0.2.1", start).should_log);
  EXPECT_FALSE(limiter.RecordAt("acl:192.0.2.1", start).should_log);
  EXPECT_TRUE(limiter.RecordAt("acl:192.0.2.2", start).should_log);
  EXPECT_FALSE(limiter.RecordAt("acl:192.0.2.3", start).should_log);

  const auto aggregate = limiter.RecordAt("acl:192.0.2.4", start + std::chrono::seconds(61));
  EXPECT_TRUE(aggregate.should_log);
  EXPECT_EQ(aggregate.suppressed_count, 2U);
}

TEST(DenialLogLimiterTest, ConcurrentAttackStillRespectsGlobalLogBound) {
  constexpr size_t kLogBound = 10;
  constexpr size_t kAttackers = 64;
  DenialLogLimiter limiter(std::chrono::seconds(60), kLogBound);
  const auto now = DenialLogLimiter::Clock::now();
  std::atomic<size_t> emitted{0};
  std::vector<std::thread> attackers;
  attackers.reserve(kAttackers);
  for (size_t i = 0; i < kAttackers; ++i) {
    attackers.emplace_back([&, i] {
      if (limiter.RecordAt("acl:2001:db8::" + std::to_string(i), now).should_log) {
        emitted.fetch_add(1, std::memory_order_relaxed);
      }
    });
  }
  for (auto& attacker : attackers) {
    attacker.join();
  }
  EXPECT_EQ(emitted.load(std::memory_order_relaxed), kLogBound);
}

TEST_F(HttpServerTest, IPv6LoopbackBindAndAclAcceptHealthRequest) {
  const uint16_t port = FindAvailableIpv6LoopbackPort();
  if (port == 0) {
    GTEST_SKIP() << "IPv6 loopback unavailable";
  }
  HttpServerConfig http_config;
  http_config.bind = "::1";
  http_config.port = port;
  http_config.allow_cidrs = {"::1/128"};
  HttpServer server(http_config, table_contexts_, config_.get());
  auto started = server.Start();
  if (!started) {
    GTEST_SKIP() << "IPv6 loopback bind unavailable: " << started.error().to_string();
  }

  httplib::Client client("::1", port);
  auto response = client.Get("/health/detail");
  ASSERT_TRUE(response);
  EXPECT_EQ(response->status, 200);
  server.Stop();
}

TEST_F(HttpServerTest, IPv6LoopbackBindAndAclRejectDisallowedPeer) {
  const uint16_t port = FindAvailableIpv6LoopbackPort();
  if (port == 0) {
    GTEST_SKIP() << "IPv6 loopback unavailable";
  }
  HttpServerConfig http_config;
  http_config.bind = "::1";
  http_config.port = port;
  http_config.allow_cidrs = {"2001:db8::/32"};
  HttpServer server(http_config, table_contexts_, config_.get());
  auto started = server.Start();
  if (!started) {
    GTEST_SKIP() << "IPv6 loopback bind unavailable: " << started.error().to_string();
  }

  httplib::Client client("::1", port);
  auto response = client.Get("/health/detail");
  ASSERT_TRUE(response);
  EXPECT_EQ(response->status, 403);
  server.Stop();
}

TEST_F(HttpServerTest, StartStop) {
  ASSERT_TRUE(http_server_->Start());
  EXPECT_TRUE(http_server_->IsRunning());
  EXPECT_EQ(http_server_->GetPort(), port_);

  http_server_->Stop();
  EXPECT_FALSE(http_server_->IsRunning());
}

TEST_F(HttpServerTest, HealthEndpoint) {
  ASSERT_TRUE(http_server_->Start());

  httplib::Client client("127.0.0.1", port_);
  auto res = client.Get("/health");

  ASSERT_TRUE(res);
  EXPECT_EQ(res->status, 200);
  EXPECT_EQ(res->get_header_value("Content-Type"), "application/json");

  auto body = json::parse(res->body);
  EXPECT_EQ(body["status"], "ok");
  EXPECT_TRUE(body.contains("timestamp"));
}

TEST_F(HttpServerTest, InfoEndpoint) {
  ASSERT_TRUE(http_server_->Start());

  httplib::Client client("127.0.0.1", port_);
  auto res = client.Get("/info");

  ASSERT_TRUE(res);
  EXPECT_EQ(res->status, 200);

  auto body = json::parse(res->body);

  // Server info
  EXPECT_EQ(body["server"], "MygramDB");
  EXPECT_EQ(body["version"], ::mygramdb::Version::String());
  EXPECT_TRUE(body.contains("uptime_seconds"));

  // Stats
  EXPECT_TRUE(body.contains("total_requests"));
  EXPECT_TRUE(body.contains("total_commands_processed"));

  // Memory object
  EXPECT_TRUE(body.contains("memory"));
  EXPECT_TRUE(body["memory"].contains("used_memory_bytes"));
  EXPECT_TRUE(body["memory"].contains("used_memory_human"));
  EXPECT_TRUE(body["memory"].contains("peak_memory_bytes"));
  EXPECT_TRUE(body["memory"].contains("used_memory_index"));
  EXPECT_TRUE(body["memory"].contains("used_memory_documents"));

  // System memory information
  EXPECT_TRUE(body["memory"].contains("total_system_memory"));
  EXPECT_TRUE(body["memory"].contains("total_system_memory_human"));
  EXPECT_TRUE(body["memory"].contains("available_system_memory"));
  EXPECT_TRUE(body["memory"].contains("available_system_memory_human"));
  EXPECT_TRUE(body["memory"].contains("system_memory_usage_ratio"));

  // Process memory information
  EXPECT_TRUE(body["memory"].contains("process_rss"));
  EXPECT_TRUE(body["memory"].contains("process_rss_human"));
  EXPECT_TRUE(body["memory"].contains("process_rss_peak"));
  EXPECT_TRUE(body["memory"].contains("process_rss_peak_human"));

  // Memory health status
  EXPECT_TRUE(body["memory"].contains("memory_health"));

  // Index object (aggregated across all tables)
  EXPECT_TRUE(body.contains("index"));
  EXPECT_EQ(body["index"]["total_documents"], 3);
  EXPECT_TRUE(body["index"].contains("total_terms"));
  EXPECT_TRUE(body["index"].contains("total_postings"));
  EXPECT_TRUE(body["index"].contains("delta_encoded_lists"));
  EXPECT_TRUE(body["index"].contains("roaring_bitmap_lists"));

  // Tables object (per-table breakdown)
  EXPECT_TRUE(body.contains("tables"));
  EXPECT_TRUE(body["tables"].contains("test"));
  EXPECT_EQ(body["tables"]["test"]["ngram_size"], 1);
  EXPECT_EQ(body["tables"]["test"]["documents"], 3);

  // Cache object (should show cache disabled when no cache manager)
  EXPECT_TRUE(body.contains("cache"));
  EXPECT_EQ(body["cache"]["enabled"], false);
}

TEST_F(HttpServerTest, InfoEndpointExposesAccountedCacheMemoryAndRejectionReasons) {
  config::CacheConfig cache_config;
  cache_config.enabled = true;
  cache_config.max_memory_bytes = 10 * 1024 * 1024;
  cache_config.min_query_cost_ms = 0.0;
  cache::NgramConfigMap ngram_configs;
  ngram_configs["test"] = cache::NgramConfig{
      .ngram_size = 1,
      .kanji_ngram_size = 1,
      .cross_boundary_ngrams = true,
  };
  cache::CacheManager cache_manager(cache_config, std::move(ngram_configs));

  HttpServerConfig http_config;
  http_config.bind = "127.0.0.1";
  http_config.port = FindAvailableLoopbackPort();
  ASSERT_NE(http_config.port, 0);
  http_config.allow_cidrs = {"127.0.0.1/32"};
  http_server_ = std::make_unique<HttpServer>(http_config, table_contexts_, config_.get(), nullptr, &cache_manager);

  ASSERT_TRUE(http_server_->Start());
  httplib::Client client("http://127.0.0.1:" + std::to_string(http_config.port));
  auto res = client.Get("/info");
  ASSERT_TRUE(res);
  ASSERT_EQ(res->status, 200);

  const auto body = json::parse(res->body);
  ASSERT_TRUE(body["cache"]["enabled"]);
  EXPECT_TRUE(body["cache"].contains("memory_bytes"));
  EXPECT_TRUE(body["cache"].contains("invalidation_index_memory_bytes"));
  EXPECT_TRUE(body["cache"].contains("invalidation_queue_memory_bytes"));
  EXPECT_TRUE(body["cache"].contains("accounted_memory_bytes"));
  EXPECT_TRUE(body["cache"].contains("accounted_memory_human"));
  EXPECT_TRUE(body["cache"].contains("rejection_count"));
  EXPECT_TRUE(body["cache"].contains("rejection_oversize"));
  EXPECT_TRUE(body["cache"].contains("rejection_memory_budget"));
  EXPECT_TRUE(body["cache"].contains("rejection_duplicate"));
  EXPECT_TRUE(body["cache"].contains("stale_entry_removals"));
  EXPECT_TRUE(body["cache"].contains("decompression_failures"));
  EXPECT_TRUE(body["cache"].contains("stale_lru_entries"));
}

TEST_F(HttpServerTest, ConfigEndpoint) {
  ASSERT_TRUE(http_server_->Start());

  httplib::Client client("127.0.0.1", port_);
  auto res = client.Get("/config");

  ASSERT_TRUE(res);
  EXPECT_EQ(res->status, 200);

  auto body = json::parse(res->body);
  EXPECT_TRUE(body["mysql"]["configured"].get<bool>());
  EXPECT_TRUE(body["mysql"]["database_defined"].get<bool>());
  EXPECT_TRUE(body["api"]["http"]["enabled"].get<bool>());
  EXPECT_FALSE(body["api"]["http"]["cors_enabled"].get<bool>());
  EXPECT_TRUE(body.contains("network"));
  EXPECT_FALSE(body["network"]["allow_cidrs_configured"].get<bool>());
  EXPECT_TRUE(body.contains("notes"));
}

TEST_F(HttpServerTest, RejectsRequestsOutsideAllowedCidrs) {
  uint16_t port = FindAvailableLoopbackPort();
  ASSERT_GT(port, 0);

  HttpServerConfig restricted_config;
  restricted_config.bind = "127.0.0.1";
  restricted_config.port = port;
  restricted_config.allow_cidrs = {"10.0.0.0/8"};

  auto restricted_server = std::make_unique<HttpServer>(restricted_config, table_contexts_, config_.get(), nullptr);
  ASSERT_TRUE(restricted_server->Start());

  httplib::Client client("http://127.0.0.1:" + std::to_string(port));

  // Non-health endpoints should be rejected by CIDR
  auto res = client.Get("/info");
  ASSERT_TRUE(res);
  EXPECT_EQ(res->status, 403);

  // Health endpoints follow the same ACL as the rest of the HTTP surface.
  auto health_res = client.Get("/health");
  ASSERT_TRUE(health_res);
  EXPECT_EQ(health_res->status, 403);

  auto live_res = client.Get("/health/live");
  ASSERT_TRUE(live_res);
  EXPECT_EQ(live_res->status, 403);

  auto ready_res = client.Get("/health/ready");
  ASSERT_TRUE(ready_res);
  EXPECT_EQ(ready_res->status, 403);

  auto detail_res = client.Get("/health/detail");
  ASSERT_TRUE(detail_res);
  EXPECT_EQ(detail_res->status, 403);

  restricted_server->Stop();
}

TEST_F(HttpServerTest, EmptyAndInvalidAclFailClosedForAllEndpoints) {
  for (const auto& cidrs : std::vector<std::vector<std::string>>{{}, {"not-a-cidr", "999.1.2.3/24"}}) {
    uint16_t port = FindAvailableLoopbackPort();
    ASSERT_GT(port, 0);

    HttpServerConfig restricted_config;
    restricted_config.bind = "127.0.0.1";
    restricted_config.port = port;
    restricted_config.allow_cidrs = cidrs;

    auto restricted_server = std::make_unique<HttpServer>(restricted_config, table_contexts_, config_.get(), nullptr);
    ASSERT_TRUE(restricted_server->Start());

    httplib::Client client("http://127.0.0.1:" + std::to_string(port));
    for (const auto* path : {"/info", "/metrics", "/health", "/health/live", "/health/ready", "/health/detail"}) {
      auto response = client.Get(path);
      ASSERT_TRUE(response) << path;
      EXPECT_EQ(response->status, 403) << path;
    }

    json search_body;
    search_body["q"] = "hello";
    auto search = client.Post("/tables/test/search", search_body.dump(), "application/json");
    ASSERT_TRUE(search);
    EXPECT_EQ(search->status, 403);

    restricted_server->Stop();
  }
}

TEST_F(HttpServerTest, MultipleRequests) {
  ASSERT_TRUE(http_server_->Start());

  httplib::Client client("127.0.0.1", port_);

  // Make multiple non-health requests. Health probes are intentionally not
  // counted in total_requests (fix N-7); use /info as the counted endpoint.
  for (int i = 0; i < 10; i++) {
    auto res = client.Get("/info");
    ASSERT_TRUE(res);
    EXPECT_EQ(res->status, 200);
  }

  // Make some health requests too — these MUST NOT show up in total_requests.
  for (int i = 0; i < 5; i++) {
    auto res = client.Get("/health");
    ASSERT_TRUE(res);
    EXPECT_EQ(res->status, 200);
  }

  // Final /info read for the counter (this one also counts).
  auto res = client.Get("/info");
  ASSERT_TRUE(res);
  auto body = json::parse(res->body);
  // 10 prior /info + 1 final /info; health probes excluded.
  EXPECT_GE(body["total_requests"].get<int>(), 11);
}

/**
 * @brief Test that HTTP API rejects requests during DUMP LOAD
 *
 * This test validates the fix for the issue where HTTP endpoints did not check
 * the loading flag, allowing requests to proceed during snapshot loading.
 */
TEST_F(HttpServerTest, RejectsRequestsDuringLoading) {
  std::atomic<bool> loading_flag{false};
  const uint16_t loading_port = FindAvailableLoopbackPort();
  ASSERT_GT(loading_port, 0);

  // Create HTTP server with loading flag
  HttpServerConfig http_config;
  http_config.bind = "127.0.0.1";
  http_config.port = loading_port;
  http_config.allow_cidrs = {"127.0.0.1/32"};  // Allow localhost

  HttpServer server(http_config, table_contexts_, config_.get(), nullptr, nullptr, &loading_flag);
  ASSERT_TRUE(server.Start());

  httplib::Client client("127.0.0.1", loading_port);

  // Test search when not loading - should succeed
  json request_body;
  request_body["q"] = "machine";
  request_body["limit"] = 10;

  auto res = client.Post("/tables/test/search", request_body.dump(), "application/json");
  ASSERT_TRUE(res);
  EXPECT_EQ(res->status, 200);

  // Set loading flag
  loading_flag.store(true);

  // Test search during loading - should return 503
  res = client.Post("/tables/test/search", request_body.dump(), "application/json");
  ASSERT_TRUE(res);
  EXPECT_EQ(res->status, 503);
  auto body = json::parse(res->body);
  EXPECT_TRUE(body.contains("error"));
  EXPECT_TRUE(body["error"].get<std::string>().find("loading") != std::string::npos);

  // Test GET during loading - should also return 503
  res = client.Get("/tables/test/article_1");
  ASSERT_TRUE(res);
  EXPECT_EQ(res->status, 503);
  body = json::parse(res->body);
  EXPECT_TRUE(body.contains("error"));
  EXPECT_TRUE(body["error"].get<std::string>().find("loading") != std::string::npos);

  // Clear loading flag
  loading_flag.store(false);

  // Test search after loading - should succeed again
  res = client.Post("/tables/test/search", request_body.dump(), "application/json");
  ASSERT_TRUE(res);
  EXPECT_EQ(res->status, 200);

  server.Stop();
}

}  // namespace server
}  // namespace mygramdb
