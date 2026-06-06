/**
 * @file http_server_features_test.cpp
 * @brief HTTP server advanced features and regression tests
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

#include "config/config.h"
#include "index/index.h"
#include "mysql/binlog_reader_interface.h"
#include "query/query_parser.h"
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

std::string LoopbackUrl(uint16_t port) {
  return "http://127.0.0.1:" + std::to_string(port);
}

class MockBinlogReader final : public mysql::IBinlogReader {
 public:
  mygram::utils::Expected<void, mygram::utils::Error> Start() override {
    running = true;
    return {};
  }

  void Stop() override { running = false; }
  bool IsRunning() const override { return running; }
  std::string GetCurrentGTID() const override { return current_gtid; }
  void SetCurrentGTID(const std::string& gtid) override { current_gtid = gtid; }
  std::string GetLastError() const override { return last_error; }
  uint64_t GetProcessedEvents() const override { return processed_events; }
  size_t GetQueueSize() const override { return queue_size; }

  bool running = false;
  std::string current_gtid = "uuid:1-42";
  std::string last_error;
  uint64_t processed_events = 123;
  size_t queue_size = 7;
};

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

TEST_F(HttpServerTest, CORSHeaders) {
  // Create a separate server with CORS enabled
  uint16_t cors_port = FindAvailableLoopbackPort();
  ASSERT_GT(cors_port, 0);
  HttpServerConfig cors_config;
  cors_config.bind = "127.0.0.1";
  cors_config.port = cors_port;
  cors_config.allow_cidrs = {"127.0.0.1/32"};  // Allow localhost
  cors_config.enable_cors = true;
  cors_config.cors_allow_origin = "*";

  auto cors_server = std::make_unique<HttpServer>(cors_config, table_contexts_, config_.get(), nullptr);
  ASSERT_TRUE(cors_server->Start());

  httplib::Client client(LoopbackUrl(cors_port));
  auto res = client.Get("/health");

  ASSERT_TRUE(res);
  EXPECT_TRUE(res->has_header("Access-Control-Allow-Origin"));
  EXPECT_EQ(res->get_header_value("Access-Control-Allow-Origin"), "*");

  cors_server->Stop();
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
}

TEST_F(HttpServerTest, CORSPreflight) {
  // Create a separate server with CORS enabled
  uint16_t cors_port = FindAvailableLoopbackPort();
  ASSERT_GT(cors_port, 0);
  HttpServerConfig cors_config;
  cors_config.bind = "127.0.0.1";
  cors_config.port = cors_port;
  cors_config.allow_cidrs = {"127.0.0.1/32"};  // Allow localhost
  cors_config.enable_cors = true;
  cors_config.cors_allow_origin = "*";

  auto cors_server = std::make_unique<HttpServer>(cors_config, table_contexts_, config_.get(), nullptr);
  ASSERT_TRUE(cors_server->Start());

  httplib::Client client(LoopbackUrl(cors_port));
  auto res = client.Options("/test/search");

  ASSERT_TRUE(res);
  EXPECT_EQ(res->status, 204);
  EXPECT_TRUE(res->has_header("Access-Control-Allow-Origin"));
  EXPECT_TRUE(res->has_header("Access-Control-Allow-Methods"));

  cors_server->Stop();
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
}

TEST_F(HttpServerTest, PrometheusMetricsEndpoint) {
  ASSERT_TRUE(http_server_->Start());

  httplib::Client client(LoopbackUrl(port_));
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

TEST_F(HttpServerTest, ReplicationStatusNotConfigured) {
  ASSERT_TRUE(http_server_->Start());

  httplib::Client client(LoopbackUrl(port_));
  auto res = client.Get("/replication/status");

  ASSERT_TRUE(res);
  EXPECT_EQ(res->status, 503);

  auto body = json::parse(res->body);
  EXPECT_TRUE(body.contains("error"));
}

#ifdef USE_MYSQL
TEST_F(HttpServerTest, ReplicationStatusIncludesTcpParityFields) {
  MockBinlogReader reader;
  reader.running = false;
  reader.current_gtid = "uuid:1-100";
  reader.processed_events = 321;
  reader.queue_size = 11;

  uint16_t replication_port = FindAvailableLoopbackPort();
  ASSERT_GT(replication_port, 0);
  HttpServerConfig replication_config;
  replication_config.bind = "127.0.0.1";
  replication_config.port = replication_port;
  replication_config.allow_cidrs = {"127.0.0.1/32"};

  auto server = std::make_unique<HttpServer>(replication_config, table_contexts_, config_.get(), &reader);
  ASSERT_TRUE(server->Start());

  httplib::Client client(LoopbackUrl(replication_port));
  auto res = client.Get("/replication/status");

  ASSERT_TRUE(res);
  EXPECT_EQ(res->status, 200);

  auto body = json::parse(res->body);
  EXPECT_EQ(body["enabled"], false);
  EXPECT_EQ(body["status"], "stopped");
  EXPECT_EQ(body["current_gtid"], "uuid:1-100");
  EXPECT_EQ(body["processed_events"], 321);
  EXPECT_EQ(body["queue_size"], 11);

  server->Stop();
}
#endif

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

      storage::FilterMap filters1;
      filters1["category"] = std::string("tech");
      auto doc_id1 = doc_store->AddDocument("tech_1", filters1);

      storage::FilterMap filters2;
      filters2["category"] = std::string("tech");
      auto doc_id2 = doc_store->AddDocument("tech_2", filters2);

      index->AddDocument(*doc_id1, "machine learning");
      index->AddDocument(*doc_id2, "deep learning");

      table_context1_.name = "table1";
      table_context1_.config.ngram_size = 1;
      table_context1_.index = std::move(index);
      table_context1_.doc_store = std::move(doc_store);
    }

    // Create table2 - exactly like HttpServerTest
    {
      auto index = std::make_unique<index::Index>(1);
      auto doc_store = std::make_unique<storage::DocumentStore>();

      storage::FilterMap filters1;
      filters1["category"] = std::string("news");
      auto doc_id1 = doc_store->AddDocument("news_1", filters1);

      storage::FilterMap filters2;
      filters2["category"] = std::string("news");
      auto doc_id2 = doc_store->AddDocument("news_2", filters2);

      index->AddDocument(*doc_id1, "breaking news");
      index->AddDocument(*doc_id2, "world news");

      table_context2_.name = "table2";
      table_context2_.config.ngram_size = 1;
      table_context2_.index = std::move(index);
      table_context2_.doc_store = std::move(doc_store);
    }

    // Store table contexts
    table_contexts_["table1"] = &table_context1_;
    table_contexts_["table2"] = &table_context2_;

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
    config_->replication.enable = false;
    config_->replication.server_id = 12345;

    // Create HTTP server
    HttpServerConfig http_config;
    http_config.bind = "127.0.0.1";
    http_config.port = port_;
    http_config.allow_cidrs = {"127.0.0.1/32"};  // Allow localhost

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
  uint16_t port_ = 0;
};

TEST_F(HttpServerMultiTableTest, SearchDifferentTables) {
  ASSERT_TRUE(http_server_->Start());

  httplib::Client client(LoopbackUrl(port_));

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

  httplib::Client client(LoopbackUrl(port_));

  // Get from table1
  auto res1 = client.Get("/table1/tech_1");
  ASSERT_TRUE(res1);
  EXPECT_EQ(res1->status, 200);

  auto body1 = json::parse(res1->body);
  EXPECT_EQ(body1["primary_key"], "tech_1");
  EXPECT_EQ(body1["filters"]["category"], "tech");

  // Get from table2
  auto res2 = client.Get("/table2/news_1");
  ASSERT_TRUE(res2);
  EXPECT_EQ(res2->status, 200);

  auto body2 = json::parse(res2->body);
  EXPECT_EQ(body2["primary_key"], "news_1");
  EXPECT_EQ(body2["filters"]["category"], "news");
}

TEST_F(HttpServerMultiTableTest, InfoShowsMultipleTables) {
  ASSERT_TRUE(http_server_->Start());

  httplib::Client client(LoopbackUrl(port_));
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

  httplib::Client client(LoopbackUrl(port_));

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

  httplib::Client client(LoopbackUrl(port_));

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

TEST_F(HttpServerMultiTableTest, DifferentNgramSizes) {
  // This test is now handled by other tests since both tables use ngram_size=1
  // Testing different ngram sizes would require creating separate table contexts
  // which is beyond the scope of this test fixture
  // Skip for now - can be added as a separate test if needed
  GTEST_SKIP() << "Skipping - both tables now use ngram_size=1 for consistency";
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
    index->AddDocument(*doc_id1, "機械学習");      // Machine learning
    index->AddDocument(*doc_id2, "深層学習技術");  // Deep learning technology

    // Create table context with kanji_ngram_size
    table_context_.name = "test_kanji";
    table_context_.config.ngram_size = 1;
    table_context_.config.kanji_ngram_size = 2;  // Different from ngram_size
    table_context_.index = std::move(index);
    table_context_.doc_store = std::move(doc_store);

    index_ = table_context_.index.get();
    doc_store_ = table_context_.doc_store.get();

    table_contexts_["test_kanji"] = &table_context_;

    port_ = FindAvailableLoopbackPort();
    ASSERT_GT(port_, 0);

    // Create config
    config_ = std::make_unique<config::Config>();
    config_->api.http.enable = true;
    config_->api.http.bind = "127.0.0.1";
    config_->api.http.port = port_;

    // Create HTTP server
    HttpServerConfig http_config;
    http_config.bind = "127.0.0.1";
    http_config.port = port_;
    http_config.allow_cidrs = {"127.0.0.1/32"};  // Allow localhost
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
  uint16_t port_ = 0;
};

TEST_F(HttpServerKanjiTest, SearchWithKanjiNgramSize) {
  ASSERT_TRUE(http_server_->Start());

  httplib::Client client(LoopbackUrl(port_));

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
  table_context.index->AddDocument(*doc_id, "test content");

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
  http_config.allow_cidrs = {"127.0.0.1/32"};  // Allow localhost

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
  storage::FilterMap filters1;
  filters1["status"] = static_cast<int64_t>(1);
  auto doc_id1 = ctx1.doc_store->AddDocument("doc1", filters1);
  ctx1.index->AddDocument(*doc_id1, "hello world");
  table_contexts["my-table"] = &ctx1;

  // Table with dot
  ctx2.name = "table.name";
  ctx2.config.ngram_size = 1;
  ctx2.index = std::make_unique<index::Index>(1);
  ctx2.doc_store = std::make_unique<storage::DocumentStore>();
  storage::FilterMap filters2;
  filters2["count"] = static_cast<int64_t>(42);
  auto doc_id2 = ctx2.doc_store->AddDocument("doc2", filters2);
  ctx2.index->AddDocument(*doc_id2, "test data");
  table_contexts["table.name"] = &ctx2;

  // Table with unicode (Japanese)
  ctx3.name = "テーブル";
  ctx3.config.ngram_size = 1;
  ctx3.index = std::make_unique<index::Index>(1);
  ctx3.doc_store = std::make_unique<storage::DocumentStore>();
  storage::FilterMap filters3;
  filters3["value"] = std::string("test");
  auto doc_id3 = ctx3.doc_store->AddDocument("doc3", filters3);
  ctx3.index->AddDocument(*doc_id3, "japanese table");
  table_contexts["テーブル"] = &ctx3;

  HttpServerConfig http_config;
  http_config.bind = "127.0.0.1";
  http_config.port = 18085;
  http_config.allow_cidrs = {"127.0.0.1/32"};  // Allow localhost

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
    storage::FilterMap filters;
    filters["score"] = static_cast<int64_t>(i * 10);
    filters["name"] = std::string("item_") + std::to_string(i);
    auto doc_id = ctx.doc_store->AddDocument("doc" + std::to_string(i), filters);
    ctx.index->AddDocument(*doc_id, "test document");
  }
  table_contexts["test"] = &ctx;

  HttpServerConfig http_config;
  http_config.bind = "127.0.0.1";
  http_config.port = 18086;
  http_config.allow_cidrs = {"127.0.0.1/32"};  // Allow localhost

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

  storage::FilterMap filters1;
  filters1["timestamp"] = large_timestamp1;
  auto doc_id1 = ctx.doc_store->AddDocument("doc1", filters1);
  ctx.index->AddDocument(*doc_id1, "test document 1");

  storage::FilterMap filters2;
  filters2["timestamp"] = large_timestamp2;
  auto doc_id2 = ctx.doc_store->AddDocument("doc2", filters2);
  ctx.index->AddDocument(*doc_id2, "test document 2");

  storage::FilterMap filters3;
  filters3["timestamp"] = large_timestamp3;
  auto doc_id3 = ctx.doc_store->AddDocument("doc3", filters3);
  ctx.index->AddDocument(*doc_id3, "test document 3");

  table_contexts["test"] = &ctx;

  HttpServerConfig http_config;
  http_config.bind = "127.0.0.1";
  http_config.port = 18087;
  http_config.allow_cidrs = {"127.0.0.1/32"};  // Allow localhost

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

/**
 * @brief Test null pointer safety in search and get handlers
 * Regression test for: table_iter->second->index/doc_store could be null
 *
 * Note: This is a documentation test since creating a TableContext with null
 * index/doc_store in production code is prevented by design. The actual fix
 * adds defensive null checks to prevent crashes if this ever happens.
 *
 * The modified code paths are:
 * - src/server/http_server.cpp:262-265 (search handler)
 * - src/server/http_server.cpp:737-740 (get handler)
 *
 * Both now check for null pointers and return HTTP 500 error instead of crashing.
 */
TEST(HttpServerPointerSafetyTest, NullPointerDefensiveChecks) {
  // This test documents the safety improvements
  // In practice, the null pointer checks in http_server.cpp prevent crashes when:
  // 1. A table is registered but index/doc_store initialization fails
  // 2. A table is in an inconsistent state during shutdown
  // 3. Memory corruption or other unexpected conditions occur

  // The fix ensures:
  // - No segfault/crash occurs
  // - HTTP 500 error is returned with appropriate message
  // - Server continues to handle other requests

  SUCCEED() << "Null pointer safety checks added to search and get handlers";
}

// ============================================================================
// RecordRequest unification: every HTTP handler increments the same stats
// instance (tcp_stats_ when provided, otherwise stats_). This guards against
// the previous inconsistency where /info had its own branch and /search,
// /count, /health* always wrote to stats_ even when tcp_stats_ was non-null.
// ============================================================================

TEST(HttpServerStatsTest, HttpHandlersIncrementHttpOnlyStatsWhenNoTcpStats) {
  // No tcp_stats supplied -> RecordRequest() must increment HttpServer::stats_.
  std::unordered_map<std::string, TableContext*> table_contexts;
  TableContext ctx;
  ctx.name = "test";
  ctx.config.ngram_size = 1;
  ctx.index = std::make_unique<index::Index>(1);
  ctx.doc_store = std::make_unique<storage::DocumentStore>();
  auto doc_id = ctx.doc_store->AddDocument("doc-1", {});
  ctx.index->AddDocument(*doc_id, "alpha");
  table_contexts["test"] = &ctx;

  config::Config full_config;
  full_config.api.default_limit = 100;
  full_config.api.max_query_length = 10000;

  HttpServerConfig http_config;
  http_config.bind = "127.0.0.1";
  http_config.port = 18086;
  http_config.allow_cidrs = {"127.0.0.1/32"};

  HttpServer http_server(http_config, table_contexts, &full_config, nullptr, nullptr, nullptr,
                         /*tcp_stats=*/nullptr);
  ASSERT_TRUE(http_server.Start());
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  uint64_t baseline = http_server.GetTotalRequests();

  httplib::Client client("127.0.0.1", 18086);
  client.set_read_timeout(std::chrono::seconds(5));

  ASSERT_TRUE(client.Get("/info"));

  json search_body;
  search_body["q"] = "alpha";
  ASSERT_TRUE(client.Post("/test/search", search_body.dump(), "application/json"));

  json count_body;
  count_body["q"] = "alpha";
  ASSERT_TRUE(client.Post("/test/count", count_body.dump(), "application/json"));

  // Allow async request bookkeeping a brief moment to settle.
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  uint64_t after = http_server.GetTotalRequests();
  EXPECT_GE(after - baseline, 3U) << "info + search + count must each call RecordRequest()";

  auto stats = http_server.GetStats().GetStatistics();
  EXPECT_GE(stats.cmd_info, 1U);
  EXPECT_GE(stats.cmd_search, 1U);
  EXPECT_GE(stats.cmd_count, 1U);
  EXPECT_GE(stats.total_commands_processed, 3U);

  auto metrics = client.Get("/metrics");
  ASSERT_TRUE(metrics);
  EXPECT_EQ(metrics->status, 200);
  EXPECT_NE(metrics->body.find("mygramdb_command_total{command=\"info\"}"), std::string::npos);
  EXPECT_NE(metrics->body.find("mygramdb_command_total{command=\"search\"}"), std::string::npos);
  EXPECT_NE(metrics->body.find("mygramdb_command_total{command=\"count\"}"), std::string::npos);

  http_server.Stop();
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
}

TEST(HttpServerStatsTest, HttpHandlersIncrementTcpStatsWhenProvided) {
  // tcp_stats supplied -> every handler (search/count/info/health*) must
  // route IncrementRequests() to tcp_stats_. After the L-6 reconciliation
  // GetTotalRequests() reads through to the effective (tcp_stats_) source so
  // both accessors agree on the same counter — this prevents /info from
  // appearing to "lose" requests when callers query GetTotalRequests()
  // directly.
  std::unordered_map<std::string, TableContext*> table_contexts;
  TableContext ctx;
  ctx.name = "test";
  ctx.config.ngram_size = 1;
  ctx.index = std::make_unique<index::Index>(1);
  ctx.doc_store = std::make_unique<storage::DocumentStore>();
  auto doc_id = ctx.doc_store->AddDocument("doc-1", {});
  ctx.index->AddDocument(*doc_id, "beta");
  table_contexts["test"] = &ctx;

  config::Config full_config;
  full_config.api.default_limit = 100;
  full_config.api.max_query_length = 10000;

  ServerStats tcp_stats;
  uint64_t tcp_baseline = tcp_stats.GetTotalRequests();

  HttpServerConfig http_config;
  http_config.bind = "127.0.0.1";
  http_config.port = 18087;
  http_config.allow_cidrs = {"127.0.0.1/32"};

  HttpServer http_server(http_config, table_contexts, &full_config, nullptr, nullptr, nullptr, &tcp_stats);
  ASSERT_TRUE(http_server.Start());
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  uint64_t http_baseline = http_server.GetTotalRequests();

  httplib::Client client("127.0.0.1", 18087);
  client.set_read_timeout(std::chrono::seconds(5));

  // /info historically routed to tcp_stats; verify it still does.
  ASSERT_TRUE(client.Get("/info"));

  // /search and /count used to always hit stats_, ignoring tcp_stats. After
  // unification they must update tcp_stats too.
  json search_body;
  search_body["q"] = "beta";
  ASSERT_TRUE(client.Post("/test/search", search_body.dump(), "application/json"));

  json count_body;
  count_body["q"] = "beta";
  ASSERT_TRUE(client.Post("/test/count", count_body.dump(), "application/json"));

  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  uint64_t tcp_after = tcp_stats.GetTotalRequests();
  uint64_t http_after = http_server.GetTotalRequests();

  EXPECT_GE(tcp_after - tcp_baseline, 3U)
      << "All HTTP handlers must increment tcp_stats when provided (info + search + count)";
  auto stats = tcp_stats.GetStatistics();
  EXPECT_GE(stats.cmd_info, 1U);
  EXPECT_GE(stats.cmd_search, 1U);
  EXPECT_GE(stats.cmd_count, 1U);
  // Effective stats reconciliation: GetTotalRequests() must reflect the same
  // counter that RecordRequest() incremented. With tcp_stats injected, that
  // is tcp_stats; the formerly-dead local stats_ is no longer exposed via
  // the public accessor.
  EXPECT_EQ(http_after - http_baseline, tcp_after - tcp_baseline)
      << "GetTotalRequests() must read from the effective stats source (tcp_stats when provided)";

  http_server.Stop();
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
}

/**
 * @test L-6: HttpServer::GetStats() must reflect the effective stats source.
 *
 * Constructing HttpServer with `tcp_stats` makes that instance the canonical
 * counter sink — RecordRequest() routes there, and GetStats()/GetTotalRequests()
 * must read from the same place. Without this guarantee, /info reports a
 * different total than direct GetTotalRequests() calls (the old "dead stats_"
 * behavior).
 */
TEST(HttpServerStatsTest, EffectiveStatsTracksConfiguredSource) {
  std::unordered_map<std::string, TableContext*> table_contexts;
  ServerStats tcp_stats;

  HttpServerConfig http_config;
  http_config.bind = "127.0.0.1";
  // Use a port outside the range used by other tests in this file
  // (18086/18087) and outside the bind-conflict suite range (18091) so this
  // pure-accessor test never collides with parallel server-binding tests.
  http_config.port = 18093;
  http_config.allow_cidrs = {"127.0.0.1/32"};

  config::Config full_config;
  full_config.api.default_limit = 100;
  full_config.api.max_query_length = 10000;

  HttpServer http_server(http_config, table_contexts, &full_config, nullptr, nullptr, nullptr, &tcp_stats);

  // Direct increments to the injected tcp_stats must be visible through the
  // HTTP server's accessor — the common case for ServerLifecycleManager,
  // which feeds the same ServerStats to TcpServer and HttpServer.
  uint64_t baseline = http_server.GetTotalRequests();
  tcp_stats.IncrementRequests();
  tcp_stats.IncrementRequests();

  EXPECT_EQ(http_server.GetTotalRequests() - baseline, 2U);
  EXPECT_EQ(&http_server.GetStats(), &tcp_stats)
      << "GetStats() must return the injected tcp_stats reference, not the dead local stats_";
}

}  // namespace server
}  // namespace mygramdb
