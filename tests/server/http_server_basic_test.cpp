/**
 * @file http_server_basic_test.cpp
 * @brief HTTP server basic lifecycle and core endpoint tests
 */

#include <gtest/gtest.h>
#include <httplib.h>

#include <chrono>
#include <nlohmann/json.hpp>
#include <thread>

#include "config/config.h"
#include "index/index.h"
#include "query/query_parser.h"
#include "server/http_server.h"
#include "server/tcp_server.h"  // For TableContext definition
#include "storage/document_store.h"
#include "version.h"

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

TEST_F(HttpServerTest, StartStop) {
  ASSERT_TRUE(http_server_->Start());
  EXPECT_TRUE(http_server_->IsRunning());
  EXPECT_EQ(http_server_->GetPort(), 18080);

  http_server_->Stop();
  EXPECT_FALSE(http_server_->IsRunning());
}

TEST_F(HttpServerTest, HealthEndpoint) {
  ASSERT_TRUE(http_server_->Start());

  httplib::Client client("http://127.0.0.1:18080");
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

  httplib::Client client("http://127.0.0.1:18080");
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

TEST_F(HttpServerTest, ConfigEndpoint) {
  ASSERT_TRUE(http_server_->Start());

  httplib::Client client("http://127.0.0.1:18080");
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
  HttpServerConfig restricted_config;
  restricted_config.bind = "127.0.0.1";
  restricted_config.port = 18082;
  restricted_config.allow_cidrs = {"10.0.0.0/8"};

  auto restricted_server = std::make_unique<HttpServer>(restricted_config, table_contexts_, config_.get(), nullptr);
  ASSERT_TRUE(restricted_server->Start());

  httplib::Client client("http://127.0.0.1:18082");
  auto res = client.Get("/health");

  ASSERT_TRUE(res);
  EXPECT_EQ(res->status, 403);

  restricted_server->Stop();
}

TEST_F(HttpServerTest, MultipleRequests) {
  ASSERT_TRUE(http_server_->Start());

  httplib::Client client("http://127.0.0.1:18080");

  // Make multiple requests
  for (int i = 0; i < 10; i++) {
    auto res = client.Get("/health");
    ASSERT_TRUE(res);
    EXPECT_EQ(res->status, 200);
  }

  // Check total requests increased
  auto res = client.Get("/info");
  ASSERT_TRUE(res);
  auto body = json::parse(res->body);
  EXPECT_GE(body["total_requests"].get<int>(), 11);  // At least 10 health + 1 info
}

/**
 * @brief Test that HTTP API rejects requests during DUMP LOAD
 *
 * This test validates the fix for the issue where HTTP endpoints did not check
 * the loading flag, allowing requests to proceed during snapshot loading.
 */
TEST_F(HttpServerTest, RejectsRequestsDuringLoading) {
  std::atomic<bool> loading_flag{false};

  // Create HTTP server with loading flag
  HttpServerConfig http_config;
  http_config.bind = "127.0.0.1";
  http_config.port = 18083;
  http_config.allow_cidrs = {"127.0.0.1/32"};  // Allow localhost

  HttpServer server(http_config, table_contexts_, config_.get(), nullptr, nullptr, &loading_flag);
  ASSERT_TRUE(server.Start());

  httplib::Client client("http://127.0.0.1:18083");

  // Test search when not loading - should succeed
  json request_body;
  request_body["q"] = "machine";
  request_body["limit"] = 10;

  auto res = client.Post("/test/search", request_body.dump(), "application/json");
  ASSERT_TRUE(res);
  EXPECT_EQ(res->status, 200);

  // Set loading flag
  loading_flag.store(true);

  // Test search during loading - should return 503
  res = client.Post("/test/search", request_body.dump(), "application/json");
  ASSERT_TRUE(res);
  EXPECT_EQ(res->status, 503);
  auto body = json::parse(res->body);
  EXPECT_TRUE(body.contains("error"));
  EXPECT_TRUE(body["error"].get<std::string>().find("loading") != std::string::npos);

  // Test GET during loading - should also return 503
  res = client.Get("/test/1");
  ASSERT_TRUE(res);
  EXPECT_EQ(res->status, 503);
  body = json::parse(res->body);
  EXPECT_TRUE(body.contains("error"));
  EXPECT_TRUE(body["error"].get<std::string>().find("loading") != std::string::npos);

  // Clear loading flag
  loading_flag.store(false);

  // Test search after loading - should succeed again
  res = client.Post("/test/search", request_body.dump(), "application/json");
  ASSERT_TRUE(res);
  EXPECT_EQ(res->status, 200);

  server.Stop();
}

}  // namespace server
}  // namespace mygramdb
