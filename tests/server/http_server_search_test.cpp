/**
 * @file http_server_search_test.cpp
 * @brief HTTP server search functionality tests
 */

#include <gtest/gtest.h>
#include <httplib.h>

#include <atomic>
#include <chrono>
#include <nlohmann/json.hpp>
#include <set>
#include <thread>

#include "config/config.h"
#include "index/index.h"
#include "query/query_parser.h"
#include "server/http_server.h"
#include "server/tcp_server.h"  // For TableContext definition
#include "storage/document_store.h"

using json = nlohmann::json;

namespace mygramdb {
namespace server {

class HttpServerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create index and document store
    auto index = std::make_unique<index::Index>(1);  // ngram_size = 1
    auto doc_store = std::make_unique<storage::DocumentStore>();

    // Add test documents
    std::unordered_map<std::string, storage::FilterValue> filters1;
    filters1["status"] = static_cast<int64_t>(1);
    filters1["category"] = std::string("tech");
    filters1["score"] = 3.14159;
    filters1["series"] = std::string("Project X=Beta");
    auto doc_id1 = doc_store->AddDocument("article_1", filters1);

    std::unordered_map<std::string, storage::FilterValue> filters2;
    filters2["status"] = static_cast<int64_t>(1);
    filters2["category"] = std::string("news");
    filters2["score"] = 1.61803;
    auto doc_id2 = doc_store->AddDocument("article_2", filters2);

    std::unordered_map<std::string, storage::FilterValue> filters3;
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
    config_->api.http.port = 18080;  // Use different port for testing
    config_->api.http.enable_cors = false;
    config_->api.http.cors_allow_origin = "*";
    config_->replication.enable = false;
    config_->replication.server_id = 12345;

    // Create HTTP server
    HttpServerConfig http_config;
    http_config.bind = "127.0.0.1";
    http_config.port = 18080;
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
};

TEST_F(HttpServerTest, SearchEndpoint) {
  ASSERT_TRUE(http_server_->Start());

  httplib::Client client("http://127.0.0.1:18080");

  auto doc_id1 = doc_store_->GetDocId("article_1");
  auto doc_id2 = doc_store_->GetDocId("article_2");
  auto doc_id3 = doc_store_->GetDocId("article_3");
  ASSERT_TRUE(doc_id1.has_value());
  ASSERT_TRUE(doc_id2.has_value());
  ASSERT_TRUE(doc_id3.has_value());

  json request_body;
  request_body["q"] = "machine";
  request_body["limit"] = 10;

  auto res = client.Post("/test/search", request_body.dump(), "application/json");

  ASSERT_TRUE(res);
  EXPECT_EQ(res->status, 200);

  auto body = json::parse(res->body);
  EXPECT_EQ(body["count"], 1);
  EXPECT_EQ(body["limit"], 10);
  EXPECT_EQ(body["offset"], 0);
  ASSERT_TRUE(body["results"].is_array());
  ASSERT_EQ(body["results"].size(), 1);
  const auto& first_result = body["results"][0];
  EXPECT_EQ(first_result["doc_id"], doc_id1.value());
  EXPECT_EQ(first_result["primary_key"], "article_1");
  ASSERT_TRUE(first_result.contains("filters"));
  EXPECT_EQ(first_result["filters"]["category"], "tech");

  // Query that returns all documents and exercise limit/offset behavior
  json multi_request;
  multi_request["q"] = "e";  // Shared character present in all documents
  multi_request["limit"] = 2;

  auto multi_res = client.Post("/test/search", multi_request.dump(), "application/json");
  ASSERT_TRUE(multi_res);
  EXPECT_EQ(multi_res->status, 200);

  auto multi_body = json::parse(multi_res->body);
  EXPECT_EQ(multi_body["count"], 3);
  EXPECT_EQ(multi_body["limit"], 2);
  EXPECT_EQ(multi_body["offset"], 0);
  ASSERT_EQ(multi_body["results"].size(), 2);
  EXPECT_EQ(multi_body["results"][0]["doc_id"], doc_id1.value());
  EXPECT_EQ(multi_body["results"][1]["doc_id"], doc_id2.value());

  // Offset should advance into the result set and preserve ordering
  json paged_request = multi_request;
  paged_request["offset"] = 1;
  auto paged_res = client.Post("/test/search", paged_request.dump(), "application/json");
  ASSERT_TRUE(paged_res);
  EXPECT_EQ(paged_res->status, 200);

  auto paged_body = json::parse(paged_res->body);
  EXPECT_EQ(paged_body["count"], 3);
  EXPECT_EQ(paged_body["limit"], 2);
  EXPECT_EQ(paged_body["offset"], 1);
  ASSERT_EQ(paged_body["results"].size(), 2);
  EXPECT_EQ(paged_body["results"][0]["doc_id"], doc_id2.value());
  EXPECT_EQ(paged_body["results"][1]["doc_id"], doc_id3.value());
}

TEST_F(HttpServerTest, SearchWithFilters) {
  ASSERT_TRUE(http_server_->Start());

  httplib::Client client("http://127.0.0.1:18080");

  json request_body;
  request_body["q"] = "machine";
  request_body["limit"] = 10;
  request_body["filters"] = {{"series", "Project X=Beta"}};

  auto res = client.Post("/test/search", request_body.dump(), "application/json");

  ASSERT_TRUE(res);
  EXPECT_EQ(res->status, 200);

  auto body = json::parse(res->body);
  EXPECT_EQ(body["count"], 1);
  ASSERT_EQ(body["results"].size(), 1);
  EXPECT_EQ(body["results"][0]["primary_key"], "article_1");
  EXPECT_DOUBLE_EQ(body["results"][0]["filters"]["score"], 3.14159);
  EXPECT_EQ(body["results"][0]["filters"]["series"], "Project X=Beta");
}

TEST_F(HttpServerTest, SearchFilterValueWithSpacesAndEquals) {
  ASSERT_TRUE(http_server_->Start());

  httplib::Client client("http://127.0.0.1:18080");

  json request_body;
  request_body["q"] = "machine";
  request_body["filters"] = {{"series", "Project X=Beta"}};

  auto res = client.Post("/test/search", request_body.dump(), "application/json");

  ASSERT_TRUE(res);
  EXPECT_EQ(res->status, 200);

  auto body = json::parse(res->body);
  ASSERT_EQ(body["results"].size(), 1);
  EXPECT_EQ(body["results"][0]["filters"]["series"], "Project X=Beta");
}

TEST_F(HttpServerTest, SearchMissingQuery) {
  ASSERT_TRUE(http_server_->Start());

  httplib::Client client("http://127.0.0.1:18080");

  json request_body;
  request_body["limit"] = 10;  // Missing "q" field

  auto res = client.Post("/test/search", request_body.dump(), "application/json");

  ASSERT_TRUE(res);
  EXPECT_EQ(res->status, 400);

  auto body = json::parse(res->body);
  EXPECT_TRUE(body.contains("error"));
  EXPECT_EQ(body["error"], "Missing required field: q");
}

TEST_F(HttpServerTest, SearchInvalidJSON) {
  ASSERT_TRUE(http_server_->Start());

  httplib::Client client("http://127.0.0.1:18080");

  auto res = client.Post("/test/search", "invalid json{", "application/json");

  ASSERT_TRUE(res);
  EXPECT_EQ(res->status, 400);

  auto body = json::parse(res->body);
  EXPECT_TRUE(body.contains("error"));
  EXPECT_TRUE(body["error"].get<std::string>().find("Invalid JSON") != std::string::npos);
}

// Regression tests for HTTP API bug fixes
TEST_F(HttpServerTest, SearchWithNumericFilters) {
  ASSERT_TRUE(http_server_->Start());

  httplib::Client client("http://127.0.0.1:18080");

  // Test int64_t filter comparison
  json request_body;
  request_body["q"] = "e";  // Matches all documents
  request_body["limit"] = 10;
  request_body["filters"] = {{"status", "1"}};  // int64_t filter

  auto res = client.Post("/test/search", request_body.dump(), "application/json");

  ASSERT_TRUE(res);
  EXPECT_EQ(res->status, 200);

  auto body = json::parse(res->body);
  // Should match article_1 and article_2 (both have status=1)
  EXPECT_EQ(body["count"], 2);
  ASSERT_EQ(body["results"].size(), 2);

  // Verify correct documents are returned
  std::set<std::string> returned_pks;
  for (const auto& result : body["results"]) {
    returned_pks.insert(result["primary_key"]);
  }
  EXPECT_TRUE(returned_pks.count("article_1") > 0);
  EXPECT_TRUE(returned_pks.count("article_2") > 0);
  EXPECT_TRUE(returned_pks.count("article_3") == 0);  // status=0, should not match

  // Test with status=0
  request_body["filters"] = {{"status", "0"}};
  res = client.Post("/test/search", request_body.dump(), "application/json");
  ASSERT_TRUE(res);
  body = json::parse(res->body);
  EXPECT_EQ(body["count"], 1);
  EXPECT_EQ(body["results"][0]["primary_key"], "article_3");
}

TEST_F(HttpServerTest, SearchWithDoubleFilters) {
  ASSERT_TRUE(http_server_->Start());

  httplib::Client client("http://127.0.0.1:18080");

  // Test double filter comparison
  json request_body;
  request_body["q"] = "e";  // Matches all documents
  request_body["limit"] = 10;
  request_body["filters"] = {{"score", "3.14159"}};  // double filter

  auto res = client.Post("/test/search", request_body.dump(), "application/json");

  ASSERT_TRUE(res);
  EXPECT_EQ(res->status, 200);

  auto body = json::parse(res->body);
  // Should match only article_1
  EXPECT_EQ(body["count"], 1);
  ASSERT_EQ(body["results"].size(), 1);
  EXPECT_EQ(body["results"][0]["primary_key"], "article_1");
  EXPECT_DOUBLE_EQ(body["results"][0]["filters"]["score"], 3.14159);

  // Test with different score
  request_body["filters"] = {{"score", "1.61803"}};
  res = client.Post("/test/search", request_body.dump(), "application/json");
  ASSERT_TRUE(res);
  body = json::parse(res->body);
  EXPECT_EQ(body["count"], 1);
  EXPECT_EQ(body["results"][0]["primary_key"], "article_2");
}

TEST_F(HttpServerTest, SearchWithBoolFilters) {
  ASSERT_TRUE(http_server_->Start());

  httplib::Client client("http://127.0.0.1:18080");

  // Add documents with bool filters
  std::unordered_map<std::string, storage::FilterValue> filters_bool_true;
  filters_bool_true["published"] = true;
  auto doc_id_bool1 = doc_store_->AddDocument("bool_article_1", filters_bool_true);
  index_->AddDocument(*doc_id_bool1, "boolean test");

  std::unordered_map<std::string, storage::FilterValue> filters_bool_false;
  filters_bool_false["published"] = false;
  auto doc_id_bool2 = doc_store_->AddDocument("bool_article_2", filters_bool_false);
  index_->AddDocument(*doc_id_bool2, "boolean test");

  // Test bool filter with "true"
  json request_body;
  request_body["q"] = "boolean";
  request_body["limit"] = 10;
  request_body["filters"] = {{"published", "true"}};

  auto res = client.Post("/test/search", request_body.dump(), "application/json");

  ASSERT_TRUE(res);
  EXPECT_EQ(res->status, 200);

  auto body = json::parse(res->body);
  EXPECT_EQ(body["count"], 1);
  ASSERT_EQ(body["results"].size(), 1);
  EXPECT_EQ(body["results"][0]["primary_key"], "bool_article_1");

  // Test bool filter with "1" (alternative true representation)
  request_body["filters"] = {{"published", "1"}};
  res = client.Post("/test/search", request_body.dump(), "application/json");
  ASSERT_TRUE(res);
  body = json::parse(res->body);
  EXPECT_EQ(body["count"], 1);
  EXPECT_EQ(body["results"][0]["primary_key"], "bool_article_1");

  // Test bool filter with "false" (should match nothing since we use "0" internally)
  request_body["filters"] = {{"published", "0"}};
  res = client.Post("/test/search", request_body.dump(), "application/json");
  ASSERT_TRUE(res);
  body = json::parse(res->body);
  EXPECT_EQ(body["count"], 1);
  EXPECT_EQ(body["results"][0]["primary_key"], "bool_article_2");
}

TEST_F(HttpServerTest, SearchWithSort) {
  ASSERT_TRUE(http_server_->Start());

  httplib::Client client("http://127.0.0.1:18080");

  // Test SORT score DESC
  json request_body;
  request_body["q"] = "e SORT score DESC";
  request_body["limit"] = 10;

  auto res = client.Post("/test/search", request_body.dump(), "application/json");

  ASSERT_TRUE(res);
  EXPECT_EQ(res->status, 200);

  auto body = json::parse(res->body);
  // Should return article_1 (3.14159), article_2 (1.61803), article_3 (no score)
  ASSERT_GE(body["results"].size(), 2);
  EXPECT_EQ(body["results"][0]["primary_key"], "article_1");  // Highest score
  EXPECT_EQ(body["results"][1]["primary_key"], "article_2");  // Second highest

  // Test SORT score ASC
  request_body["q"] = "e SORT score ASC";
  res = client.Post("/test/search", request_body.dump(), "application/json");
  ASSERT_TRUE(res);
  body = json::parse(res->body);
  ASSERT_GE(body["results"].size(), 2);
  // article_3 has no score (NULL) - should be first in ASC
  // Then article_2 (1.61803), then article_1 (3.14159)
  EXPECT_EQ(body["results"][0]["primary_key"], "article_3");  // NULL first in ASC
  EXPECT_EQ(body["results"][1]["primary_key"], "article_2");  // Lowest score

  // Test SORT category ASC (string sorting)
  request_body["q"] = "e SORT category ASC";
  res = client.Post("/test/search", request_body.dump(), "application/json");
  ASSERT_TRUE(res);
  body = json::parse(res->body);
  ASSERT_GE(body["results"].size(), 2);
  // "news" < "tech" in alphabetical order
  EXPECT_EQ(body["results"][0]["primary_key"], "article_3");  // NULL first
  EXPECT_EQ(body["results"][1]["primary_key"], "article_2");  // "news"
  EXPECT_EQ(body["results"][2]["primary_key"], "article_1");  // "tech"
}

/**
 * @brief Test that HTTP search uses CacheManager
 *
 * This test validates the fix for the issue where HTTP search did not utilize
 * the cache, always performing full index scans even when cache was enabled.
 */
TEST_F(HttpServerTest, SearchUsesCacheManager) {
  // Create a simple TcpServer to get cache manager
  ServerConfig tcp_config;
  tcp_config.host = "127.0.0.1";
  tcp_config.port = 11099;  // Use different port
  tcp_config.worker_threads = 2;

  // Enable cache in config
  config_->cache.enabled = true;
  config_->cache.max_memory_bytes = 10 * 1024 * 1024;

  TcpServer tcp_server(tcp_config, table_contexts_, "./dumps", config_.get(), nullptr);
  ASSERT_TRUE(tcp_server.Start());

  // Create HTTP server with cache manager from TCP server
  HttpServerConfig http_config;
  http_config.bind = "127.0.0.1";
  http_config.port = 18084;
  http_config.allow_cidrs = {"127.0.0.1/32"};  // Allow localhost

  HttpServer http_server(http_config, table_contexts_, config_.get(), nullptr, tcp_server.GetCacheManager(),
                         tcp_server.GetLoadingFlag());
  ASSERT_TRUE(http_server.Start());

  httplib::Client client("http://127.0.0.1:18084");

  json request_body;
  request_body["q"] = "machine";
  request_body["limit"] = 10;

  // First request - cache miss
  auto res1 = client.Post("/test/search", request_body.dump(), "application/json");
  ASSERT_TRUE(res1);
  EXPECT_EQ(res1->status, 200);
  auto body1 = json::parse(res1->body);
  EXPECT_GT(body1["count"], 0);

  // Second identical request - should hit cache
  auto res2 = client.Post("/test/search", request_body.dump(), "application/json");
  ASSERT_TRUE(res2);
  EXPECT_EQ(res2->status, 200);
  auto body2 = json::parse(res2->body);

  // Results should be identical
  EXPECT_EQ(body1["count"], body2["count"]);
  EXPECT_EQ(body1["results"].size(), body2["results"].size());

  // Verify cache statistics increased
  auto info_res = client.Get("/info");
  ASSERT_TRUE(info_res);
  auto info_body = json::parse(info_res->body);
  ASSERT_TRUE(info_body.contains("cache"));
  ASSERT_TRUE(info_body["cache"]["enabled"].get<bool>());
  EXPECT_GT(info_body["cache"]["total_queries"].get<int>(), 0);

  http_server.Stop();
  tcp_server.Stop();
}

/**
 * @brief Test for concurrent search requests to detect QueryParser data race
 *
 * This test validates the fix for the issue where HTTP server shared a single
 * QueryParser instance across threads, causing data races when multiple requests
 * were processed concurrently.
 */
TEST_F(HttpServerTest, ConcurrentSearchRequestsNoDataRace) {
  ASSERT_TRUE(http_server_->Start());

  httplib::Client client("http://127.0.0.1:18080");

  // Launch multiple threads making concurrent search requests
  const int num_threads = 10;
  const int requests_per_thread = 5;
  std::vector<std::thread> threads;
  std::atomic<int> success_count{0};
  std::atomic<int> failure_count{0};

  for (int i = 0; i < num_threads; i++) {
    threads.emplace_back([&client, &success_count, &failure_count, requests_per_thread]() {
      httplib::Client thread_client("http://127.0.0.1:18080");
      for (int j = 0; j < requests_per_thread; j++) {
        json request_body;
        request_body["q"] = (j % 2 == 0) ? "machine" : "news";
        request_body["limit"] = 10;

        auto res = thread_client.Post("/test/search", request_body.dump(), "application/json");
        if (res && res->status == 200) {
          auto body = json::parse(res->body);
          if (body.contains("results")) {
            success_count++;
          } else {
            failure_count++;
          }
        } else {
          failure_count++;
        }
      }
    });
  }

  // Wait for all threads to complete
  for (auto& thread : threads) {
    thread.join();
  }

  // All requests should succeed (no data race causing errors)
  EXPECT_EQ(success_count.load(), num_threads * requests_per_thread);
  EXPECT_EQ(failure_count.load(), 0);
}

/**
 * @brief Test HTTP /search respects api.default_limit configuration
 *
 * Regression test for: HTTP /search は api.default_limit の設定を無視して
 * 常に 100 件固定で実行される
 */
TEST(HttpServerIntegrationTest, SearchRespectsDefaultLimit) {
  // Create table context with many documents
  TableContext table_context;
  table_context.name = "test";
  table_context.config.ngram_size = 1;
  table_context.index = std::make_unique<index::Index>(1);
  table_context.doc_store = std::make_unique<storage::DocumentStore>();

  // Add 150 documents (more than default limit)
  for (int i = 0; i < 150; ++i) {
    auto doc_id = table_context.doc_store->AddDocument("doc_" + std::to_string(i), {});
    table_context.index->AddDocument(*doc_id, "test content");
  }

  std::unordered_map<std::string, TableContext*> table_contexts;
  table_contexts["test"] = &table_context;

  // Create config with CUSTOM default_limit = 20 (NOT 100!)
  config::Config full_config;
  full_config.api.default_limit = 20;  // Custom limit!
  full_config.api.max_query_length = 10000;

  // Start TCP server (for completeness, though we're testing HTTP)
  ServerConfig tcp_config;
  tcp_config.host = "127.0.0.1";
  tcp_config.port = 11021;
  tcp_config.default_limit = 20;  // Same limit

  TcpServer tcp_server(tcp_config, table_contexts, "./dumps", &full_config, nullptr);
  ASSERT_TRUE(tcp_server.Start());

  // Start HTTP server
  HttpServerConfig http_config;
  http_config.bind = "127.0.0.1";
  http_config.port = 18086;
  http_config.allow_cidrs = {"127.0.0.1/32"};  // Allow localhost

  HttpServer http_server(http_config, table_contexts, &full_config, nullptr, nullptr, nullptr,
                         tcp_server.GetMutableStats());
  ASSERT_TRUE(http_server.Start());
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  httplib::Client http_client("127.0.0.1", 18086);
  http_client.set_read_timeout(std::chrono::seconds(5));

  // Test 1: Search WITHOUT explicit limit - should use default_limit=20
  {
    json request_body;
    request_body["q"] = "test";
    // NO "limit" field!

    auto res = http_client.Post("/test/search", request_body.dump(), "application/json");
    ASSERT_TRUE(res) << "HTTP search request failed";
    EXPECT_EQ(res->status, 200);

    auto body = json::parse(res->body);
    ASSERT_TRUE(body.contains("limit"));
    ASSERT_TRUE(body.contains("results"));

    // The limit field should reflect default_limit=20, NOT 100!
    EXPECT_EQ(body["limit"].get<int>(), 20) << "Without explicit LIMIT, should use api.default_limit=20";

    // Results should be limited to 20, even though we have 150 documents
    EXPECT_EQ(body["results"].size(), 20) << "Should return only 20 results (default_limit)";
    EXPECT_EQ(body["count"].get<int>(), 150) << "Total count should be 150";
  }

  // Test 2: Search WITH explicit limit=50 - should override default
  {
    json request_body;
    request_body["q"] = "test";
    request_body["limit"] = 50;  // Explicit limit

    auto res = http_client.Post("/test/search", request_body.dump(), "application/json");
    ASSERT_TRUE(res) << "HTTP search request failed";
    EXPECT_EQ(res->status, 200);

    auto body = json::parse(res->body);

    // Should use explicit limit=50
    EXPECT_EQ(body["limit"].get<int>(), 50) << "With explicit LIMIT, should use that value";
    EXPECT_EQ(body["results"].size(), 50) << "Should return 50 results (explicit limit)";
    EXPECT_EQ(body["count"].get<int>(), 150);
  }

  // Test 3: Verify consistency - TCP and HTTP both use same default_limit
  // (We've already verified HTTP uses 20 in Test 1 and Test 2)
  // The important thing is that the config value is respected

  http_server.Stop();
  tcp_server.Stop();
}

}  // namespace server
}  // namespace mygramdb
