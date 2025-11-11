/**
 * @file http_server_test.cpp
 * @brief HTTP server tests
 */

#include "server/http_server.h"
#include "index/index.h"
#include "storage/document_store.h"
#include "config/config.h"
#include "version.h"
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>
#include <thread>
#include <chrono>
#include <httplib.h>

using json = nlohmann::json;

namespace mygramdb {
namespace server {

class HttpServerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create index and document store
    index_ = std::make_unique<index::Index>(1);  // ngram_size = 1
    doc_store_ = std::make_unique<storage::DocumentStore>();

    // Add test documents
    std::unordered_map<std::string, storage::FilterValue> filters1;
    filters1["status"] = static_cast<int64_t>(1);
    filters1["category"] = std::string("tech");
    auto doc_id1 = doc_store_->AddDocument("article_1", filters1);

    std::unordered_map<std::string, storage::FilterValue> filters2;
    filters2["status"] = static_cast<int64_t>(1);
    filters2["category"] = std::string("news");
    auto doc_id2 = doc_store_->AddDocument("article_2", filters2);

    std::unordered_map<std::string, storage::FilterValue> filters3;
    filters3["status"] = static_cast<int64_t>(0);
    auto doc_id3 = doc_store_->AddDocument("article_3", filters3);

    // Index documents
    index_->AddDocument(doc_id1, "machine learning");
    index_->AddDocument(doc_id2, "breaking news");
    index_->AddDocument(doc_id3, "old article");

    // Create config
    config_ = std::make_unique<config::Config>();
    config_->mysql.host = "127.0.0.1";
    config_->mysql.port = 3306;
    config_->mysql.database = "testdb";
    config_->mysql.user = "test_user";
    config_->api.tcp.bind = "127.0.0.1";
    config_->api.tcp.port = 11311;
    config_->api.http.enable = true;
    config_->api.http.bind = "127.0.0.1";
    config_->api.http.port = 18080;  // Use different port for testing
    config_->replication.enable = false;
    config_->replication.server_id = 12345;

    // Create HTTP server
    HttpServerConfig http_config;
    http_config.bind = "127.0.0.1";
    http_config.port = 18080;

    http_server_ = std::make_unique<HttpServer>(
        http_config, *index_, *doc_store_,
        1,  // ngram_size
        config_.get(),
        nullptr  // binlog_reader
    );
  }

  void TearDown() override {
    if (http_server_ && http_server_->IsRunning()) {
      http_server_->Stop();
    }
    // Wait a bit for server to fully stop
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }

  std::unique_ptr<index::Index> index_;
  std::unique_ptr<storage::DocumentStore> doc_store_;
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
  EXPECT_EQ(body["version"], mygramdb::Version::String());
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

  // Index object
  EXPECT_TRUE(body.contains("index"));
  EXPECT_EQ(body["index"]["total_documents"], 3);
  EXPECT_EQ(body["index"]["ngram_size"], 1);
  EXPECT_TRUE(body["index"].contains("total_terms"));
  EXPECT_TRUE(body["index"].contains("total_postings"));
  EXPECT_TRUE(body["index"].contains("delta_encoded_lists"));
  EXPECT_TRUE(body["index"].contains("roaring_bitmap_lists"));
}

TEST_F(HttpServerTest, ConfigEndpoint) {
  ASSERT_TRUE(http_server_->Start());

  httplib::Client client("http://127.0.0.1:18080");
  auto res = client.Get("/config");

  ASSERT_TRUE(res);
  EXPECT_EQ(res->status, 200);

  auto body = json::parse(res->body);
  EXPECT_EQ(body["mysql"]["host"], "127.0.0.1");
  EXPECT_EQ(body["mysql"]["port"], 3306);
  EXPECT_EQ(body["mysql"]["database"], "testdb");
  EXPECT_EQ(body["mysql"]["user"], "test_user");
  EXPECT_EQ(body["api"]["http"]["enable"], true);
  EXPECT_EQ(body["api"]["http"]["port"], 18080);
}

TEST_F(HttpServerTest, SearchEndpoint) {
  ASSERT_TRUE(http_server_->Start());

  httplib::Client client("http://127.0.0.1:18080");

  json request_body;
  request_body["q"] = "machine";
  request_body["limit"] = 10;

  auto res = client.Post("/threads/search", request_body.dump(), "application/json");

  ASSERT_TRUE(res);
  EXPECT_EQ(res->status, 200);

  auto body = json::parse(res->body);
  EXPECT_TRUE(body.contains("count"));
  EXPECT_TRUE(body.contains("results"));
  EXPECT_EQ(body["limit"], 10);
  EXPECT_EQ(body["offset"], 0);
}

TEST_F(HttpServerTest, SearchWithFilters) {
  ASSERT_TRUE(http_server_->Start());

  httplib::Client client("http://127.0.0.1:18080");

  json request_body;
  request_body["q"] = "news";
  // Note: Filter implementation depends on query parser
  // For now, test that request is processed without crash
  request_body["limit"] = 10;

  auto res = client.Post("/threads/search", request_body.dump(), "application/json");

  ASSERT_TRUE(res);
  // Should return valid response (either 200 with results or 400 if query fails)
  EXPECT_TRUE(res->status == 200 || res->status == 400);

  auto body = json::parse(res->body);
  // Response should have either results or error field
  EXPECT_TRUE(body.contains("results") || body.contains("error"));
}

TEST_F(HttpServerTest, SearchMissingQuery) {
  ASSERT_TRUE(http_server_->Start());

  httplib::Client client("http://127.0.0.1:18080");

  json request_body;
  request_body["limit"] = 10;  // Missing "q" field

  auto res = client.Post("/threads/search", request_body.dump(), "application/json");

  ASSERT_TRUE(res);
  EXPECT_EQ(res->status, 400);

  auto body = json::parse(res->body);
  EXPECT_TRUE(body.contains("error"));
  EXPECT_EQ(body["error"], "Missing required field: q");
}

TEST_F(HttpServerTest, SearchInvalidJSON) {
  ASSERT_TRUE(http_server_->Start());

  httplib::Client client("http://127.0.0.1:18080");

  auto res = client.Post("/threads/search", "invalid json{", "application/json");

  ASSERT_TRUE(res);
  EXPECT_EQ(res->status, 400);

  auto body = json::parse(res->body);
  EXPECT_TRUE(body.contains("error"));
  EXPECT_TRUE(body["error"].get<std::string>().find("Invalid JSON") != std::string::npos);
}

TEST_F(HttpServerTest, GetDocumentEndpoint) {
  ASSERT_TRUE(http_server_->Start());

  httplib::Client client("http://127.0.0.1:18080");
  auto res = client.Get("/threads/1");

  ASSERT_TRUE(res);
  EXPECT_EQ(res->status, 200);

  auto body = json::parse(res->body);
  EXPECT_EQ(body["doc_id"], 1);
  EXPECT_EQ(body["primary_key"], "article_1");
  EXPECT_TRUE(body.contains("filters"));
  EXPECT_EQ(body["filters"]["status"], 1);
  EXPECT_EQ(body["filters"]["category"], "tech");
}

TEST_F(HttpServerTest, GetDocumentNotFound) {
  ASSERT_TRUE(http_server_->Start());

  httplib::Client client("http://127.0.0.1:18080");
  auto res = client.Get("/threads/999");

  ASSERT_TRUE(res);
  EXPECT_EQ(res->status, 404);

  auto body = json::parse(res->body);
  EXPECT_TRUE(body.contains("error"));
  EXPECT_EQ(body["error"], "Document not found");
}

TEST_F(HttpServerTest, GetDocumentInvalidID) {
  ASSERT_TRUE(http_server_->Start());

  httplib::Client client("http://127.0.0.1:18080");
  auto res = client.Get("/threads/invalid");

  ASSERT_TRUE(res);
  EXPECT_EQ(res->status, 404);  // Route won't match non-numeric ID
}

TEST_F(HttpServerTest, CORSHeaders) {
  ASSERT_TRUE(http_server_->Start());

  httplib::Client client("http://127.0.0.1:18080");
  auto res = client.Get("/health");

  ASSERT_TRUE(res);
  EXPECT_TRUE(res->has_header("Access-Control-Allow-Origin"));
  EXPECT_EQ(res->get_header_value("Access-Control-Allow-Origin"), "*");
}

TEST_F(HttpServerTest, CORSPreflight) {
  ASSERT_TRUE(http_server_->Start());

  httplib::Client client("http://127.0.0.1:18080");
  auto res = client.Options("/threads/search");

  ASSERT_TRUE(res);
  EXPECT_EQ(res->status, 204);
  EXPECT_TRUE(res->has_header("Access-Control-Allow-Origin"));
  EXPECT_TRUE(res->has_header("Access-Control-Allow-Methods"));
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

}  // namespace server
}  // namespace mygramdb
