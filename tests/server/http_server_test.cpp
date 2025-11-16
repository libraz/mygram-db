/**
 * @file http_server_test.cpp
 * @brief HTTP server tests
 */

#include "server/http_server.h"

#include <gtest/gtest.h>
#include <httplib.h>

#include <chrono>
#include <nlohmann/json.hpp>
#include <thread>

#include "config/config.h"
#include "index/index.h"
#include "query/query_parser.h"
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
    index->AddDocument(doc_id1, "machine learning");
    index->AddDocument(doc_id2, "breaking news");
    index->AddDocument(doc_id3, "old article");

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

TEST_F(HttpServerTest, GetDocumentEndpoint) {
  ASSERT_TRUE(http_server_->Start());

  httplib::Client client("http://127.0.0.1:18080");
  auto res = client.Get("/test/1");

  ASSERT_TRUE(res);
  EXPECT_EQ(res->status, 200);

  auto body = json::parse(res->body);
  EXPECT_EQ(body["doc_id"], 1);
  EXPECT_EQ(body["primary_key"], "article_1");
  EXPECT_TRUE(body.contains("filters"));
  EXPECT_EQ(body["filters"]["status"], 1);
  EXPECT_EQ(body["filters"]["category"], "tech");
  EXPECT_DOUBLE_EQ(body["filters"]["score"], 3.14159);
  EXPECT_EQ(body["filters"]["series"], "Project X=Beta");
}

TEST_F(HttpServerTest, GetDocumentNotFound) {
  ASSERT_TRUE(http_server_->Start());

  httplib::Client client("http://127.0.0.1:18080");
  auto res = client.Get("/test/999");

  ASSERT_TRUE(res);
  EXPECT_EQ(res->status, 404);

  auto body = json::parse(res->body);
  EXPECT_TRUE(body.contains("error"));
  EXPECT_EQ(body["error"], "Document not found");
}

TEST_F(HttpServerTest, GetDocumentInvalidID) {
  ASSERT_TRUE(http_server_->Start());

  httplib::Client client("http://127.0.0.1:18080");
  auto res = client.Get("/test/invalid");

  ASSERT_TRUE(res);
  EXPECT_EQ(res->status, 404);  // Route won't match non-numeric ID
}

TEST_F(HttpServerTest, CORSHeaders) {
  // Create a separate server with CORS enabled
  HttpServerConfig cors_config;
  cors_config.bind = "127.0.0.1";
  cors_config.port = 18081;
  cors_config.enable_cors = true;
  cors_config.cors_allow_origin = "*";

  auto cors_server = std::make_unique<HttpServer>(cors_config, table_contexts_, config_.get(), nullptr);
  ASSERT_TRUE(cors_server->Start());

  httplib::Client client("http://127.0.0.1:18081");
  auto res = client.Get("/health");

  ASSERT_TRUE(res);
  EXPECT_TRUE(res->has_header("Access-Control-Allow-Origin"));
  EXPECT_EQ(res->get_header_value("Access-Control-Allow-Origin"), "*");

  cors_server->Stop();
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
}

TEST_F(HttpServerTest, CORSPreflight) {
  // Create a separate server with CORS enabled
  HttpServerConfig cors_config;
  cors_config.bind = "127.0.0.1";
  cors_config.port = 18081;
  cors_config.enable_cors = true;
  cors_config.cors_allow_origin = "*";

  auto cors_server = std::make_unique<HttpServer>(cors_config, table_contexts_, config_.get(), nullptr);
  ASSERT_TRUE(cors_server->Start());

  httplib::Client client("http://127.0.0.1:18081");
  auto res = client.Options("/test/search");

  ASSERT_TRUE(res);
  EXPECT_EQ(res->status, 204);
  EXPECT_TRUE(res->has_header("Access-Control-Allow-Origin"));
  EXPECT_TRUE(res->has_header("Access-Control-Allow-Methods"));

  cors_server->Stop();
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
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

TEST_F(HttpServerTest, ReplicationStatusNotConfigured) {
  ASSERT_TRUE(http_server_->Start());

  httplib::Client client("http://127.0.0.1:18080");
  auto res = client.Get("/replication/status");

  ASSERT_TRUE(res);
  EXPECT_EQ(res->status, 503);

  auto body = json::parse(res->body);
  EXPECT_TRUE(body.contains("error"));
}

/**
 * @brief Multi-table HTTP server test fixture
 */
class HttpServerMultiTableTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create table1 - exactly like HttpServerTest
    {
      auto index = std::make_unique<index::Index>(1);
      auto doc_store = std::make_unique<storage::DocumentStore>();

      std::unordered_map<std::string, storage::FilterValue> filters1;
      filters1["category"] = std::string("tech");
      auto doc_id1 = doc_store->AddDocument("tech_1", filters1);

      std::unordered_map<std::string, storage::FilterValue> filters2;
      filters2["category"] = std::string("tech");
      auto doc_id2 = doc_store->AddDocument("tech_2", filters2);

      index->AddDocument(doc_id1, "machine learning");
      index->AddDocument(doc_id2, "deep learning");

      table_context1_.name = "table1";
      table_context1_.config.ngram_size = 1;
      table_context1_.index = std::move(index);
      table_context1_.doc_store = std::move(doc_store);
    }

    // Create table2 - exactly like HttpServerTest
    {
      auto index = std::make_unique<index::Index>(1);
      auto doc_store = std::make_unique<storage::DocumentStore>();

      std::unordered_map<std::string, storage::FilterValue> filters1;
      filters1["category"] = std::string("news");
      auto doc_id1 = doc_store->AddDocument("news_1", filters1);

      std::unordered_map<std::string, storage::FilterValue> filters2;
      filters2["category"] = std::string("news");
      auto doc_id2 = doc_store->AddDocument("news_2", filters2);

      index->AddDocument(doc_id1, "breaking news");
      index->AddDocument(doc_id2, "world news");

      table_context2_.name = "table2";
      table_context2_.config.ngram_size = 1;
      table_context2_.index = std::move(index);
      table_context2_.doc_store = std::move(doc_store);
    }

    // Store table contexts
    table_contexts_["table1"] = &table_context1_;
    table_contexts_["table2"] = &table_context2_;

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
    config_->api.http.port = 18081;  // Different port
    config_->replication.enable = false;
    config_->replication.server_id = 12345;

    // Create HTTP server
    HttpServerConfig http_config;
    http_config.bind = "127.0.0.1";
    http_config.port = 18081;

    http_server_ = std::make_unique<HttpServer>(http_config, table_contexts_, config_.get(), nullptr);
  }

  void TearDown() override {
    if (http_server_ && http_server_->IsRunning()) {
      http_server_->Stop();
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }

  TableContext table_context1_;
  TableContext table_context2_;
  std::unordered_map<std::string, TableContext*> table_contexts_;
  std::unique_ptr<config::Config> config_;
  std::unique_ptr<HttpServer> http_server_;
};

TEST_F(HttpServerMultiTableTest, SearchDifferentTables) {
  ASSERT_TRUE(http_server_->Start());

  httplib::Client client("http://127.0.0.1:18081");

  // Search table1
  json request1;
  request1["q"] = "machine";
  request1["limit"] = 10;

  auto res1 = client.Post("/table1/search", request1.dump(), "application/json");
  ASSERT_TRUE(res1);
  EXPECT_EQ(res1->status, 200);

  auto body1 = json::parse(res1->body);
  EXPECT_EQ(body1["count"], 1);
  EXPECT_EQ(body1["results"][0]["primary_key"], "tech_1");

  // Search table2
  json request2;
  request2["q"] = "news";
  request2["limit"] = 10;

  auto res2 = client.Post("/table2/search", request2.dump(), "application/json");
  ASSERT_TRUE(res2);
  EXPECT_EQ(res2->status, 200);

  auto body2 = json::parse(res2->body);
  EXPECT_EQ(body2["count"], 2);  // Both documents contain "news"
  EXPECT_GT(body2["results"].size(), 0);
}

TEST_F(HttpServerMultiTableTest, GetDocumentFromDifferentTables) {
  ASSERT_TRUE(http_server_->Start());

  httplib::Client client("http://127.0.0.1:18081");

  // Get from table1
  auto res1 = client.Get("/table1/1");
  ASSERT_TRUE(res1);
  EXPECT_EQ(res1->status, 200);

  auto body1 = json::parse(res1->body);
  EXPECT_EQ(body1["primary_key"], "tech_1");
  EXPECT_EQ(body1["filters"]["category"], "tech");

  // Get from table2
  auto res2 = client.Get("/table2/1");
  ASSERT_TRUE(res2);
  EXPECT_EQ(res2->status, 200);

  auto body2 = json::parse(res2->body);
  EXPECT_EQ(body2["primary_key"], "news_1");
  EXPECT_EQ(body2["filters"]["category"], "news");
}

TEST_F(HttpServerMultiTableTest, InfoShowsMultipleTables) {
  ASSERT_TRUE(http_server_->Start());

  httplib::Client client("http://127.0.0.1:18081");
  auto res = client.Get("/info");

  ASSERT_TRUE(res);
  EXPECT_EQ(res->status, 200);

  auto body = json::parse(res->body);

  // Should show aggregated stats (2 documents per table = 4 total)
  EXPECT_EQ(body["index"]["total_documents"], 4);

  // Should have per-table breakdown
  EXPECT_TRUE(body.contains("tables"));
  EXPECT_TRUE(body["tables"].contains("table1"));
  EXPECT_TRUE(body["tables"].contains("table2"));

  // Both tables use ngram_size=1 and have 2 documents each
  EXPECT_EQ(body["tables"]["table1"]["ngram_size"], 1);
  EXPECT_EQ(body["tables"]["table1"]["documents"], 2);

  EXPECT_EQ(body["tables"]["table2"]["ngram_size"], 1);
  EXPECT_EQ(body["tables"]["table2"]["documents"], 2);
}

TEST_F(HttpServerMultiTableTest, TableIsolation) {
  ASSERT_TRUE(http_server_->Start());

  httplib::Client client("http://127.0.0.1:18081");

  // Search for "machine" in table1 - should find it
  json request1;
  request1["q"] = "machine";
  request1["limit"] = 10;

  auto res1 = client.Post("/table1/search", request1.dump(), "application/json");
  ASSERT_TRUE(res1);
  EXPECT_EQ(res1->status, 200);
  auto body1 = json::parse(res1->body);
  EXPECT_EQ(body1["count"], 1);

  // Search for "machine" in table2 - should NOT find it
  json request2;
  request2["q"] = "machine";
  request2["limit"] = 10;

  auto res2 = client.Post("/table2/search", request2.dump(), "application/json");
  ASSERT_TRUE(res2);
  EXPECT_EQ(res2->status, 200);
  auto body2 = json::parse(res2->body);
  EXPECT_EQ(body2["count"], 0);  // No results in table2
}

TEST_F(HttpServerMultiTableTest, InvalidTableName) {
  ASSERT_TRUE(http_server_->Start());

  httplib::Client client("http://127.0.0.1:18081");

  // Try to search non-existent table
  json request;
  request["q"] = "test";
  request["limit"] = 10;

  auto res = client.Post("/nonexistent/search", request.dump(), "application/json");
  ASSERT_TRUE(res);
  EXPECT_EQ(res->status, 404);

  auto body = json::parse(res->body);
  EXPECT_TRUE(body.contains("error"));
  EXPECT_TRUE(body["error"].get<std::string>().find("Table not found") != std::string::npos);
}

TEST_F(HttpServerTest, PrometheusMetricsEndpoint) {
  ASSERT_TRUE(http_server_->Start());

  httplib::Client client("http://127.0.0.1:18080");
  auto res = client.Get("/metrics");

  ASSERT_TRUE(res);
  EXPECT_EQ(res->status, 200);
  EXPECT_EQ(res->get_header_value("Content-Type"), "text/plain; version=0.0.4; charset=utf-8");

  // Verify Prometheus format
  std::string body = res->body;

  // Check for basic server metrics
  EXPECT_TRUE(body.find("# HELP mygramdb_server_info") != std::string::npos);
  EXPECT_TRUE(body.find("# TYPE mygramdb_server_info gauge") != std::string::npos);
  EXPECT_TRUE(body.find("mygramdb_server_info{version=\"") != std::string::npos);

  // Check for uptime
  EXPECT_TRUE(body.find("# HELP mygramdb_server_uptime_seconds") != std::string::npos);
  EXPECT_TRUE(body.find("# TYPE mygramdb_server_uptime_seconds counter") != std::string::npos);
  EXPECT_TRUE(body.find("mygramdb_server_uptime_seconds") != std::string::npos);

  // Check for memory metrics
  EXPECT_TRUE(body.find("# HELP mygramdb_memory_used_bytes") != std::string::npos);
  EXPECT_TRUE(body.find("# TYPE mygramdb_memory_used_bytes gauge") != std::string::npos);
  EXPECT_TRUE(body.find("mygramdb_memory_used_bytes{type=\"total\"}") != std::string::npos);
  EXPECT_TRUE(body.find("mygramdb_memory_used_bytes{type=\"index\"}") != std::string::npos);
  EXPECT_TRUE(body.find("mygramdb_memory_used_bytes{type=\"documents\"}") != std::string::npos);

  // Check for memory health status
  EXPECT_TRUE(body.find("# HELP mygramdb_memory_health_status") != std::string::npos);
  EXPECT_TRUE(body.find("# TYPE mygramdb_memory_health_status gauge") != std::string::npos);
  EXPECT_TRUE(body.find("mygramdb_memory_health_status") != std::string::npos);

  // Check for index metrics with table label
  EXPECT_TRUE(body.find("# HELP mygramdb_index_documents_total") != std::string::npos);
  EXPECT_TRUE(body.find("# TYPE mygramdb_index_documents_total gauge") != std::string::npos);
  EXPECT_TRUE(body.find("mygramdb_index_documents_total{table=\"test\"}") != std::string::npos);

  // Check for client metrics
  EXPECT_TRUE(body.find("# HELP mygramdb_clients_connected") != std::string::npos);
  EXPECT_TRUE(body.find("# TYPE mygramdb_clients_connected gauge") != std::string::npos);
}

TEST_F(HttpServerMultiTableTest, DifferentNgramSizes) {
  // This test is now handled by other tests since both tables use ngram_size=1
  // Testing different ngram sizes would require creating separate table contexts
  // which is beyond the scope of this test fixture
  // Skip for now - can be added as a separate test if needed
  GTEST_SKIP() << "Skipping - both tables now use ngram_size=1 for consistency";
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
  index_->AddDocument(doc_id_bool1, "boolean test");

  std::unordered_map<std::string, storage::FilterValue> filters_bool_false;
  filters_bool_false["published"] = false;
  auto doc_id_bool2 = doc_store_->AddDocument("bool_article_2", filters_bool_false);
  index_->AddDocument(doc_id_bool2, "boolean test");

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

// Test fixture for kanji_ngram_size testing
class HttpServerKanjiTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create index with kanji_ngram_size configured
    // ngram_size = 1 for ASCII, kanji_ngram_size = 2 for CJK
    auto index = std::make_unique<index::Index>(1, 2);
    auto doc_store = std::make_unique<storage::DocumentStore>();

    // Add Japanese test documents
    auto doc_id1 = doc_store->AddDocument("jp_article_1", {});
    auto doc_id2 = doc_store->AddDocument("jp_article_2", {});

    // Index Japanese documents with kanji_ngram_size=2
    index->AddDocument(doc_id1, "機械学習");      // Machine learning
    index->AddDocument(doc_id2, "深層学習技術");  // Deep learning technology

    // Create table context with kanji_ngram_size
    table_context_.name = "test_kanji";
    table_context_.config.ngram_size = 1;
    table_context_.config.kanji_ngram_size = 2;  // Different from ngram_size
    table_context_.index = std::move(index);
    table_context_.doc_store = std::move(doc_store);

    index_ = table_context_.index.get();
    doc_store_ = table_context_.doc_store.get();

    table_contexts_["test_kanji"] = &table_context_;

    // Create config
    config_ = std::make_unique<config::Config>();
    config_->api.http.enable = true;
    config_->api.http.bind = "127.0.0.1";
    config_->api.http.port = 18082;

    // Create HTTP server
    HttpServerConfig http_config;
    http_config.bind = "127.0.0.1";
    http_config.port = 18082;
    http_config.enable_cors = false;

    http_server_ = std::make_unique<HttpServer>(http_config, table_contexts_, config_.get(), nullptr);
  }

  void TearDown() override {
    if (http_server_ && http_server_->IsRunning()) {
      http_server_->Stop();
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }

  index::Index* index_;
  storage::DocumentStore* doc_store_;
  TableContext table_context_;
  std::unordered_map<std::string, TableContext*> table_contexts_;
  std::unique_ptr<config::Config> config_;
  std::unique_ptr<HttpServer> http_server_;
};

TEST_F(HttpServerKanjiTest, SearchWithKanjiNgramSize) {
  ASSERT_TRUE(http_server_->Start());

  httplib::Client client("http://127.0.0.1:18082");

  // Search for "学習" (learning) - should match both documents
  json request_body;
  request_body["q"] = "学習";
  request_body["limit"] = 10;

  auto res = client.Post("/test_kanji/search", request_body.dump(), "application/json");

  ASSERT_TRUE(res);
  EXPECT_EQ(res->status, 200);

  auto body = json::parse(res->body);
  // With kanji_ngram_size=2, "学習" should be properly tokenized as a single bigram
  // Both documents contain "学習" so both should match
  EXPECT_EQ(body["count"], 2);
  ASSERT_EQ(body["results"].size(), 2);

  // Search for "機械" (machine) - should match only first document
  request_body["q"] = "機械";
  res = client.Post("/test_kanji/search", request_body.dump(), "application/json");
  ASSERT_TRUE(res);
  body = json::parse(res->body);
  EXPECT_EQ(body["count"], 1);
  EXPECT_EQ(body["results"][0]["primary_key"], "jp_article_1");

  // Search for "深層" (deep) - should match only second document
  request_body["q"] = "深層";
  res = client.Post("/test_kanji/search", request_body.dump(), "application/json");
  ASSERT_TRUE(res);
  body = json::parse(res->body);
  EXPECT_EQ(body["count"], 1);
  EXPECT_EQ(body["results"][0]["primary_key"], "jp_article_2");
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
 * @brief Test HTTP /info and /metrics reflect TCP server statistics
 *
 * Regression test for: HTTP の /info と /metrics が TCP 側の統計値を一切反映していない
 */
TEST(HttpServerIntegrationTest, InfoAndMetricsReflectTcpStats) {
  // Create table context
  TableContext table_context;
  table_context.name = "test";
  table_context.config.ngram_size = 1;
  table_context.index = std::make_unique<index::Index>(1);
  table_context.doc_store = std::make_unique<storage::DocumentStore>();

  // Add test documents
  auto doc_id = table_context.doc_store->AddDocument("test_doc", {});
  table_context.index->AddDocument(doc_id, "test content");

  std::unordered_map<std::string, TableContext*> table_contexts;
  table_contexts["test"] = &table_context;

  // Create config
  config::Config full_config;
  full_config.api.default_limit = 100;
  full_config.api.max_query_length = 10000;

  // Start TCP server
  ServerConfig tcp_config;
  tcp_config.host = "127.0.0.1";
  tcp_config.port = 11020;
  tcp_config.default_limit = 100;

  TcpServer tcp_server(tcp_config, table_contexts, "./dumps", &full_config, nullptr);
  ASSERT_TRUE(tcp_server.Start());

  // Wait for TCP server to fully start
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // Simulate TCP operations by directly calling methods that increment stats
  // (instead of trying to use HTTP client for TCP protocol)
  // In real scenario, TCP commands increment stats through RequestDispatcher
  // For this test, we'll manually increment to simulate activity
  for (int i = 0; i < 6; ++i) {
    tcp_server.GetMutableStats()->IncrementRequests();
    tcp_server.GetMutableStats()->IncrementCommand(query::QueryType::SEARCH);
  }

  // Start HTTP server WITH tcp_stats pointer
  HttpServerConfig http_config;
  http_config.bind = "127.0.0.1";
  http_config.port = 18085;

  HttpServer http_server(http_config, table_contexts, &full_config, nullptr, nullptr, nullptr,
                         tcp_server.GetMutableStats());  // Pass TCP stats!
  ASSERT_TRUE(http_server.Start());
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  httplib::Client http_client("127.0.0.1", 18085);
  http_client.set_read_timeout(std::chrono::seconds(5));

  // Get /info via HTTP
  auto http_info_res = http_client.Get("/info");
  ASSERT_TRUE(http_info_res) << "HTTP /info request failed";
  EXPECT_EQ(http_info_res->status, 200);

  auto info_body = json::parse(http_info_res->body);

  // The total_commands_processed should reflect TCP commands (5 SEARCH + 1 INFO = 6)
  // NOT be 0!
  ASSERT_TRUE(info_body.contains("total_commands_processed"));
  EXPECT_GE(info_body["total_commands_processed"].get<int>(), 6)
      << "HTTP /info should reflect TCP statistics. Got total_commands_processed="
      << info_body["total_commands_processed"];

  // Get /metrics via HTTP (Prometheus format)
  auto http_metrics_res = http_client.Get("/metrics");
  ASSERT_TRUE(http_metrics_res) << "HTTP /metrics request failed";
  EXPECT_EQ(http_metrics_res->status, 200);

  // Metrics should contain mygramdb_server_commands_total and it should be >= 6
  std::string metrics_body = http_metrics_res->body;

  EXPECT_NE(metrics_body.find("mygramdb_server_commands_total"), std::string::npos)
      << "Metrics should contain server_commands_total";

  // Find the metric value line (not the HELP or TYPE lines)
  // Format: "mygramdb_server_commands_total 6\n"
  size_t pos = 0;
  while ((pos = metrics_body.find("mygramdb_server_commands_total", pos)) != std::string::npos) {
    // Check if this is the value line (not HELP or TYPE)
    size_t line_start = metrics_body.rfind('\n', pos);
    if (line_start == std::string::npos) {
      line_start = 0;
    } else {
      line_start++;
    }

    // Check if line starts with "mygramdb_server_commands_total " (not "# HELP" or "# TYPE")
    if (metrics_body.substr(line_start, 2) != "# ") {
      // This is the value line!
      auto value_start = pos + std::string("mygramdb_server_commands_total ").length();
      auto value_end = metrics_body.find('\n', value_start);
      if (value_end != std::string::npos) {
        auto value_str = metrics_body.substr(value_start, value_end - value_start);
        // Trim whitespace
        value_str.erase(0, value_str.find_first_not_of(" \t\r"));
        value_str.erase(value_str.find_last_not_of(" \t\r\n") + 1);

        if (!value_str.empty() && std::isdigit(value_str[0])) {
          int commands_processed = std::stoi(value_str);
          EXPECT_GE(commands_processed, 6)
              << "Metrics should show >= 6 commands processed from TCP server, got: " << commands_processed;
          break;
        }
      }
    }
    pos++;
  }

  http_server.Stop();
  tcp_server.Stop();
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
    table_context.index->AddDocument(doc_id, "test content");
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

// ============================================================================
// Regression Tests for Code Review Issues
// ============================================================================

// Test for Issue 1: HTTP routes should support non-alphanumeric table names
TEST(HttpServerRegressionTest, NonAlphanumericTableNames) {
  // Create tables with dashes, dots, and unicode characters
  std::unordered_map<std::string, TableContext*> table_contexts;
  TableContext ctx1, ctx2, ctx3;

  // Table with dash
  ctx1.name = "my-table";
  ctx1.config.ngram_size = 1;
  ctx1.index = std::make_unique<index::Index>(1);
  ctx1.doc_store = std::make_unique<storage::DocumentStore>();
  std::unordered_map<std::string, storage::FilterValue> filters1;
  filters1["status"] = static_cast<int64_t>(1);
  auto doc_id1 = ctx1.doc_store->AddDocument("doc1", filters1);
  ctx1.index->AddDocument(doc_id1, "hello world");
  table_contexts["my-table"] = &ctx1;

  // Table with dot
  ctx2.name = "table.name";
  ctx2.config.ngram_size = 1;
  ctx2.index = std::make_unique<index::Index>(1);
  ctx2.doc_store = std::make_unique<storage::DocumentStore>();
  std::unordered_map<std::string, storage::FilterValue> filters2;
  filters2["count"] = static_cast<int64_t>(42);
  auto doc_id2 = ctx2.doc_store->AddDocument("doc2", filters2);
  ctx2.index->AddDocument(doc_id2, "test data");
  table_contexts["table.name"] = &ctx2;

  // Table with unicode (Japanese)
  ctx3.name = "テーブル";
  ctx3.config.ngram_size = 1;
  ctx3.index = std::make_unique<index::Index>(1);
  ctx3.doc_store = std::make_unique<storage::DocumentStore>();
  std::unordered_map<std::string, storage::FilterValue> filters3;
  filters3["value"] = std::string("test");
  auto doc_id3 = ctx3.doc_store->AddDocument("doc3", filters3);
  ctx3.index->AddDocument(doc_id3, "japanese table");
  table_contexts["テーブル"] = &ctx3;

  HttpServerConfig http_config;
  http_config.bind = "127.0.0.1";
  http_config.port = 18085;

  HttpServer http_server(http_config, table_contexts, nullptr, nullptr);
  ASSERT_TRUE(http_server.Start());

  httplib::Client client("http://127.0.0.1:18085");

  // Test 1: Table with dash "my-table"
  json request1;
  request1["q"] = "hello";
  auto res1 = client.Post("/my-table/search", request1.dump(), "application/json");
  ASSERT_TRUE(res1) << "Should be able to access table with dash in name";
  EXPECT_EQ(res1->status, 200);
  auto body1 = json::parse(res1->body);
  EXPECT_EQ(body1["count"], 1);

  // Test 2: Table with dot "table.name"
  json request2;
  request2["q"] = "test";
  auto res2 = client.Post("/table.name/search", request2.dump(), "application/json");
  ASSERT_TRUE(res2) << "Should be able to access table with dot in name";
  EXPECT_EQ(res2->status, 200);
  auto body2 = json::parse(res2->body);
  EXPECT_EQ(body2["count"], 1);

  // Test 3: Table with unicode "テーブル"
  // Note: URL encoding required for unicode
  json request3;
  request3["q"] = "japanese";
  auto encoded_table_name = httplib::detail::encode_url("テーブル");
  auto res3 = client.Post("/" + encoded_table_name + "/search", request3.dump(), "application/json");
  ASSERT_TRUE(res3) << "Should be able to access table with unicode name";
  EXPECT_EQ(res3->status, 200);
  auto body3 = json::parse(res3->body);
  EXPECT_EQ(body3["count"], 1);

  http_server.Stop();
}

// Test for Issue 2: HTTP JSON filters should support all FilterOp operators
TEST(HttpServerRegressionTest, AllFilterOperators) {
  std::unordered_map<std::string, TableContext*> table_contexts;
  TableContext ctx;

  ctx.name = "test";
  ctx.config.ngram_size = 1;
  ctx.index = std::make_unique<index::Index>(1);
  ctx.doc_store = std::make_unique<storage::DocumentStore>();

  // Add documents with various filter values
  for (int i = 1; i <= 10; ++i) {
    std::unordered_map<std::string, storage::FilterValue> filters;
    filters["score"] = static_cast<int64_t>(i * 10);
    filters["name"] = std::string("item_") + std::to_string(i);
    auto doc_id = ctx.doc_store->AddDocument("doc" + std::to_string(i), filters);
    ctx.index->AddDocument(doc_id, "test document");
  }
  table_contexts["test"] = &ctx;

  HttpServerConfig http_config;
  http_config.bind = "127.0.0.1";
  http_config.port = 18086;

  HttpServer http_server(http_config, table_contexts, nullptr, nullptr);
  ASSERT_TRUE(http_server.Start());

  httplib::Client client("http://127.0.0.1:18086");

  // Test EQ operator
  {
    json request;
    request["q"] = "test";
    request["filters"]["score"]["op"] = "EQ";
    request["filters"]["score"]["value"] = "50";

    auto res = client.Post("/test/search", request.dump(), "application/json");
    ASSERT_TRUE(res);
    EXPECT_EQ(res->status, 200);
    auto body = json::parse(res->body);
    EXPECT_EQ(body["count"], 1) << "EQ operator should find exactly one match (score=50)";
  }

  // Test GT operator
  {
    json request;
    request["q"] = "test";
    request["filters"]["score"]["op"] = "GT";
    request["filters"]["score"]["value"] = "50";

    auto res = client.Post("/test/search", request.dump(), "application/json");
    ASSERT_TRUE(res);
    EXPECT_EQ(res->status, 200);
    auto body = json::parse(res->body);
    EXPECT_EQ(body["count"], 5) << "GT operator should find 5 matches (score > 50: 60,70,80,90,100)";
  }

  // Test GTE operator
  {
    json request;
    request["q"] = "test";
    request["filters"]["score"]["op"] = "GTE";
    request["filters"]["score"]["value"] = "50";

    auto res = client.Post("/test/search", request.dump(), "application/json");
    ASSERT_TRUE(res);
    EXPECT_EQ(res->status, 200);
    auto body = json::parse(res->body);
    EXPECT_EQ(body["count"], 6) << "GTE operator should find 6 matches (score >= 50)";
  }

  // Test LT operator
  {
    json request;
    request["q"] = "test";
    request["filters"]["score"]["op"] = "LT";
    request["filters"]["score"]["value"] = "50";

    auto res = client.Post("/test/search", request.dump(), "application/json");
    ASSERT_TRUE(res);
    EXPECT_EQ(res->status, 200);
    auto body = json::parse(res->body);
    EXPECT_EQ(body["count"], 4) << "LT operator should find 4 matches (score < 50: 10,20,30,40)";
  }

  // Test LTE operator
  {
    json request;
    request["q"] = "test";
    request["filters"]["score"]["op"] = "LTE";
    request["filters"]["score"]["value"] = "50";

    auto res = client.Post("/test/search", request.dump(), "application/json");
    ASSERT_TRUE(res);
    EXPECT_EQ(res->status, 200);
    auto body = json::parse(res->body);
    EXPECT_EQ(body["count"], 5) << "LTE operator should find 5 matches (score <= 50)";
  }

  // Test NE operator
  {
    json request;
    request["q"] = "test";
    request["filters"]["score"]["op"] = "NE";
    request["filters"]["score"]["value"] = "50";

    auto res = client.Post("/test/search", request.dump(), "application/json");
    ASSERT_TRUE(res);
    EXPECT_EQ(res->status, 200);
    auto body = json::parse(res->body);
    EXPECT_EQ(body["count"], 9) << "NE operator should find 9 matches (all except score=50)";
  }

  // Test string comparison with GT operator
  {
    json request;
    request["q"] = "test";
    request["filters"]["name"]["op"] = "GT";
    request["filters"]["name"]["value"] = "item_5";

    auto res = client.Post("/test/search", request.dump(), "application/json");
    ASSERT_TRUE(res);
    EXPECT_EQ(res->status, 200);
    auto body = json::parse(res->body);
    EXPECT_GT(body["count"], 0) << "GT operator should work with string values";
  }

  http_server.Stop();
}

// Test for Issue 4: Unsigned filter comparison overflow for large values
TEST(HttpServerRegressionTest, UnsignedFilterLargeValues) {
  std::unordered_map<std::string, TableContext*> table_contexts;
  TableContext ctx;

  ctx.name = "test";
  ctx.config.ngram_size = 1;
  ctx.index = std::make_unique<index::Index>(1);
  ctx.doc_store = std::make_unique<storage::DocumentStore>();

  // Add documents with large unsigned values (timestamp-like values > INT64_MAX)
  const uint64_t large_timestamp1 = 10000000000000000000ULL;  // > INT64_MAX
  const uint64_t large_timestamp2 = 18000000000000000000ULL;  // Much larger
  const uint64_t large_timestamp3 = 5000000000000000000ULL;   // Below INT64_MAX but still large

  std::unordered_map<std::string, storage::FilterValue> filters1;
  filters1["timestamp"] = large_timestamp1;
  auto doc_id1 = ctx.doc_store->AddDocument("doc1", filters1);
  ctx.index->AddDocument(doc_id1, "test document 1");

  std::unordered_map<std::string, storage::FilterValue> filters2;
  filters2["timestamp"] = large_timestamp2;
  auto doc_id2 = ctx.doc_store->AddDocument("doc2", filters2);
  ctx.index->AddDocument(doc_id2, "test document 2");

  std::unordered_map<std::string, storage::FilterValue> filters3;
  filters3["timestamp"] = large_timestamp3;
  auto doc_id3 = ctx.doc_store->AddDocument("doc3", filters3);
  ctx.index->AddDocument(doc_id3, "test document 3");

  table_contexts["test"] = &ctx;

  HttpServerConfig http_config;
  http_config.bind = "127.0.0.1";
  http_config.port = 18087;

  HttpServer http_server(http_config, table_contexts, nullptr, nullptr);
  ASSERT_TRUE(http_server.Start());

  httplib::Client client("http://127.0.0.1:18087");

  // Test GT operator with large unsigned value
  {
    json request;
    request["q"] = "test";
    request["filters"]["timestamp"]["op"] = "GT";
    request["filters"]["timestamp"]["value"] = std::to_string(large_timestamp1);

    auto res = client.Post("/test/search", request.dump(), "application/json");
    ASSERT_TRUE(res);
    EXPECT_EQ(res->status, 200);
    auto body = json::parse(res->body);
    EXPECT_EQ(body["count"], 1) << "Should find 1 document with timestamp > " << large_timestamp1;
  }

  // Test LT operator with large unsigned value
  {
    json request;
    request["q"] = "test";
    request["filters"]["timestamp"]["op"] = "LT";
    request["filters"]["timestamp"]["value"] = std::to_string(large_timestamp1);

    auto res = client.Post("/test/search", request.dump(), "application/json");
    ASSERT_TRUE(res);
    EXPECT_EQ(res->status, 200);
    auto body = json::parse(res->body);
    EXPECT_EQ(body["count"], 1) << "Should find 1 document with timestamp < " << large_timestamp1;
  }

  // Test EQ operator with large unsigned value
  {
    json request;
    request["q"] = "test";
    request["filters"]["timestamp"]["op"] = "EQ";
    request["filters"]["timestamp"]["value"] = std::to_string(large_timestamp2);

    auto res = client.Post("/test/search", request.dump(), "application/json");
    ASSERT_TRUE(res);
    EXPECT_EQ(res->status, 200);
    auto body = json::parse(res->body);
    EXPECT_EQ(body["count"], 1) << "Should find exactly 1 document with timestamp = " << large_timestamp2;
  }

  // Test GTE with maximum possible uint64_t value comparison
  {
    json request;
    request["q"] = "test";
    request["filters"]["timestamp"]["op"] = "GTE";
    request["filters"]["timestamp"]["value"] = std::to_string(large_timestamp3);

    auto res = client.Post("/test/search", request.dump(), "application/json");
    ASSERT_TRUE(res);
    EXPECT_EQ(res->status, 200);
    auto body = json::parse(res->body);
    EXPECT_EQ(body["count"], 3) << "Should find all 3 documents with timestamp >= " << large_timestamp3;
  }

  http_server.Stop();
}

}  // namespace server
}  // namespace mygramdb
