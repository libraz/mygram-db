/**
 * @file http_tcp_consistency_test.cpp
 * @brief Cross-protocol consistency tests for the HTTP and TCP search paths
 *
 * Both the HTTP and TCP search handlers should produce equivalent result
 * counts because they share the same search_pipeline::ExecuteFullPipeline
 * implementation. This file pins that contract by querying both endpoints
 * against the same in-memory table and asserting that the result/count
 * matches.
 */

#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <httplib.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <memory>
#include <nlohmann/json.hpp>
#include <sstream>
#include <thread>

#include "config/config.h"
#include "server/http_server.h"
#include "server/tcp_server.h"
#include "tcp_server_test_helpers.h"

using json = nlohmann::json;

namespace mygramdb {
namespace server {

namespace {

// Send a single TCP request and read the full response (until "OK ..." or
// "ERROR ..." line is received). Returns empty string on failure.
std::string SendTcpRequest(uint16_t port, const std::string& request) {
  int sock = socket(AF_INET, SOCK_STREAM, 0);
  if (sock < 0) {
    return "";
  }
  struct sockaddr_in addr {};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
  if (connect(sock, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
    close(sock);
    return "";
  }

  std::string msg = request + "\r\n";
  if (send(sock, msg.c_str(), msg.length(), 0) < 0) {
    close(sock);
    return "";
  }

  std::string response;
  char buffer[4096];
  // Read with a soft deadline so the test does not block forever if the
  // server fails to respond.
  for (int attempt = 0; attempt < 10; ++attempt) {
    ssize_t received = recv(sock, buffer, sizeof(buffer) - 1, MSG_DONTWAIT);
    if (received > 0) {
      buffer[received] = '\0';
      response.append(buffer, received);
      if (response.rfind("OK FACET", 0) == 0) {
        if (response.find("\r\n\r\n") != std::string::npos) {
          break;
        }
      } else if (response.find("\r\n") != std::string::npos) {
        break;
      }
    } else {
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
  }
  close(sock);
  return response;
}

// Extract the numeric "OK SEARCH N" / "OK COUNT N" prefix.
size_t ParseTcpCount(const std::string& response, const std::string& verb) {
  std::string prefix = "OK " + verb + " ";
  auto pos = response.find(prefix);
  if (pos == std::string::npos) {
    return 0;
  }
  return std::stoul(response.substr(pos + prefix.size()));
}

std::vector<std::string> ParseTcpSearchPrimaryKeys(const std::string& response) {
  std::istringstream iss(response);
  std::string ok;
  std::string results;
  size_t count = 0;
  iss >> ok >> results >> count;

  std::vector<std::string> primary_keys;
  std::string pk;
  while (iss >> pk) {
    primary_keys.push_back(pk);
  }
  return primary_keys;
}

std::vector<std::pair<std::string, uint64_t>> ParseTcpFacetValues(const std::string& response) {
  std::istringstream stream(response);
  std::string header;
  std::getline(stream, header);

  std::vector<std::pair<std::string, uint64_t>> values;
  std::string line;
  while (std::getline(stream, line)) {
    if (!line.empty() && line.back() == '\r') {
      line.pop_back();
    }
    if (line.empty()) {
      continue;
    }
    auto tab_pos = line.find('\t');
    if (tab_pos == std::string::npos) {
      continue;
    }
    values.emplace_back(line.substr(0, tab_pos), std::stoull(line.substr(tab_pos + 1)));
  }
  return values;
}

}  // namespace

class HttpTcpConsistencyTest : public ::testing::Test {
 protected:
  void SetUp() override {
    mygramdb::test::SkipIfSocketCreationBlocked();

    // Build a single shared table context fed to both servers.
    auto index = std::make_unique<index::Index>(2);
    auto doc_store = std::make_unique<storage::DocumentStore>();

    storage::FilterMap filters1;
    filters1["category"] = std::string("ai");
    filters1["status"] = std::string("published");
    storage::FilterMap filters2;
    filters2["category"] = std::string("industry");
    filters2["status"] = std::string("published");
    storage::FilterMap filters3;
    filters3["category"] = std::string("ai");
    filters3["status"] = std::string("draft");
    storage::FilterMap filters4;
    filters4["category"] = std::string("misc");
    filters4["status"] = std::string("published");

    auto id1 = doc_store->AddDocument("doc_1", filters1);
    auto id2 = doc_store->AddDocument("doc_2", filters2);
    auto id3 = doc_store->AddDocument("doc_3", filters3);
    auto id4 = doc_store->AddDocument("doc_4", filters4);

    index->AddDocument(*id1, "machine learning models");
    index->AddDocument(*id2, "machine production lines");
    index->AddDocument(*id3, "deep learning research");
    index->AddDocument(*id4, "unrelated topic");

    table_ctx_.name = "articles";
    table_ctx_.config.name = "articles";
    table_ctx_.config.database = "app";
    table_ctx_.config.ngram_size = 2;
    table_ctx_.config.primary_key = "id";
    table_ctx_.index = std::move(index);
    table_ctx_.doc_store = std::move(doc_store);
    table_contexts_["app.articles"] = &table_ctx_;

    // Minimal config required for the unified pipeline.
    config_ = std::make_unique<config::Config>();
    config_->api.tcp.bind = "127.0.0.1";
    config_->api.tcp.port = 0;
    config_->api.http.enable = true;
    config_->api.http.bind = "127.0.0.1";
    config_->api.http.port = 0;
    config_->api.default_limit = 10;
    config_->tables.push_back(table_ctx_.config);

    // TCP server (let OS pick a port).
    ServerConfig tcp_cfg;
    tcp_cfg.host = "127.0.0.1";
    tcp_cfg.port = 0;
    tcp_cfg.allow_cidrs = {"127.0.0.1/32"};
    tcp_server_ = std::make_unique<TcpServer>(tcp_cfg, table_contexts_, "./dumps", config_.get());
    ASSERT_TRUE(tcp_server_->Start());
    tcp_port_ = tcp_server_->GetPort();

    // HTTP server -- pick a fixed high port to avoid collision with other tests
    // running concurrently. RESOURCE_LOCK in CMake serialises us with other
    // server-port consumers.
    HttpServerConfig http_cfg;
    http_cfg.bind = "127.0.0.1";
    http_cfg.port = 18091;
    http_cfg.allow_cidrs = {"127.0.0.1/32"};
    http_server_ = std::make_unique<HttpServer>(http_cfg, table_contexts_, config_.get(), nullptr);
    ASSERT_TRUE(http_server_->Start());
    http_port_ = http_server_->GetPort();

    // Allow servers to fully bind.
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }

  void TearDown() override {
    if (tcp_server_ && tcp_server_->IsRunning()) {
      tcp_server_->Stop();
    }
    if (http_server_ && http_server_->IsRunning()) {
      http_server_->Stop();
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  TableContext table_ctx_;
  std::unordered_map<std::string, TableContext*> table_contexts_;
  std::unique_ptr<config::Config> config_;
  std::unique_ptr<TcpServer> tcp_server_;
  std::unique_ptr<HttpServer> http_server_;
  uint16_t tcp_port_ = 0;
  uint16_t http_port_ = 0;
};

TEST_F(HttpTcpConsistencyTest, SearchHitCountMatches) {
  // "machine" should match doc_1 and doc_2 -- expect identical totals.
  auto tcp_response = SendTcpRequest(tcp_port_, "SEARCH app.articles machine");
  ASSERT_FALSE(tcp_response.empty()) << "TCP server returned empty response";
  // SEARCH responses use "OK RESULTS <count> <pk1> <pk2> ...".
  size_t tcp_count = ParseTcpCount(tcp_response, "RESULTS");

  httplib::Client http_client("http://127.0.0.1:" + std::to_string(http_port_));
  json req_body;
  req_body["q"] = "machine";
  auto http_res = http_client.Post("/tables/app.articles/search", req_body.dump(), "application/json");
  ASSERT_TRUE(http_res != nullptr) << "HTTP request returned null";
  ASSERT_EQ(http_res->status, 200) << "HTTP body: " << http_res->body;
  json http_body = json::parse(http_res->body);
  size_t http_count = http_body["count"].get<size_t>();

  EXPECT_EQ(tcp_count, http_count) << "TCP count " << tcp_count << " != HTTP count " << http_count
                                   << "; tcp_response=" << tcp_response << " http_body=" << http_res->body;
  EXPECT_GT(tcp_count, 0u);
}

TEST_F(HttpTcpConsistencyTest, SearchDefaultOrderAndLimitMatches) {
  auto tcp_response = SendTcpRequest(tcp_port_, "SEARCH app.articles machine LIMIT 1");
  ASSERT_FALSE(tcp_response.empty()) << "TCP server returned empty response";
  auto tcp_primary_keys = ParseTcpSearchPrimaryKeys(tcp_response);
  ASSERT_EQ(tcp_primary_keys.size(), 1U) << "tcp_response=" << tcp_response;

  httplib::Client http_client("http://127.0.0.1:" + std::to_string(http_port_));
  json req_body;
  req_body["q"] = "machine";
  req_body["limit"] = 1;
  auto http_res = http_client.Post("/tables/app.articles/search", req_body.dump(), "application/json");
  ASSERT_TRUE(http_res != nullptr) << "HTTP request returned null";
  ASSERT_EQ(http_res->status, 200) << "HTTP body: " << http_res->body;

  json http_body = json::parse(http_res->body);
  ASSERT_EQ(http_body["results"].size(), 1U);
  EXPECT_EQ(http_body["results"][0]["primary_key"].get<std::string>(), tcp_primary_keys[0])
      << "tcp_response=" << tcp_response << " http_body=" << http_res->body;
}

TEST_F(HttpTcpConsistencyTest, QuotedBooleanKeywordPhraseMatches) {
  auto tcp_response = SendTcpRequest(tcp_port_, R"(SEARCH app.articles "machine OR unrelated")");
  ASSERT_FALSE(tcp_response.empty()) << "TCP server returned empty response";
  ASSERT_EQ(tcp_response.rfind("OK RESULTS", 0), 0U) << "tcp_response=" << tcp_response;
  size_t tcp_count = ParseTcpCount(tcp_response, "RESULTS");

  httplib::Client http_client("http://127.0.0.1:" + std::to_string(http_port_));
  json req_body;
  req_body["q"] = R"("machine OR unrelated")";
  auto http_res = http_client.Post("/tables/app.articles/search", req_body.dump(), "application/json");
  ASSERT_TRUE(http_res != nullptr) << "HTTP request returned null";
  ASSERT_EQ(http_res->status, 200) << "HTTP body: " << http_res->body;

  json http_body = json::parse(http_res->body);
  size_t http_count = http_body["count"].get<size_t>();

  EXPECT_EQ(tcp_count, 0u) << "tcp_response=" << tcp_response;
  EXPECT_EQ(http_count, tcp_count) << "tcp_response=" << tcp_response << " http_body=" << http_res->body;
}

TEST_F(HttpTcpConsistencyTest, SearchLargeResultTopNOrderAndLimitMatches) {
  for (int i = 0; i < 80; ++i) {
    auto id = table_ctx_.doc_store->AddDocument("bulk_" + std::to_string(i), {});
    ASSERT_TRUE(id);
    table_ctx_.index->AddDocument(*id, "bulk indexed topic");
  }

  auto tcp_response = SendTcpRequest(tcp_port_, "SEARCH app.articles bulk LIMIT 5");
  ASSERT_FALSE(tcp_response.empty()) << "TCP server returned empty response";
  auto tcp_primary_keys = ParseTcpSearchPrimaryKeys(tcp_response);
  ASSERT_EQ(tcp_primary_keys.size(), 5U) << "tcp_response=" << tcp_response;

  httplib::Client http_client("http://127.0.0.1:" + std::to_string(http_port_));
  json req_body;
  req_body["q"] = "bulk";
  req_body["limit"] = 5;
  auto http_res = http_client.Post("/tables/app.articles/search", req_body.dump(), "application/json");
  ASSERT_TRUE(http_res != nullptr) << "HTTP request returned null";
  ASSERT_EQ(http_res->status, 200) << "HTTP body: " << http_res->body;

  json http_body = json::parse(http_res->body);
  EXPECT_EQ(http_body["count"].get<size_t>(), 80u);
  ASSERT_EQ(http_body["results"].size(), 5U);
  for (size_t i = 0; i < tcp_primary_keys.size(); ++i) {
    EXPECT_EQ(http_body["results"][i]["primary_key"].get<std::string>(), tcp_primary_keys[i])
        << "tcp_response=" << tcp_response << " http_body=" << http_res->body;
  }
}

TEST_F(HttpTcpConsistencyTest, GetByPrimaryKeyMatches) {
  auto tcp_response = SendTcpRequest(tcp_port_, "GET app.articles doc_1");
  ASSERT_FALSE(tcp_response.empty()) << "TCP server returned empty response";
  EXPECT_EQ(tcp_response.rfind("OK DOC doc_1", 0), 0U) << "tcp_response=" << tcp_response;

  httplib::Client http_client("http://127.0.0.1:" + std::to_string(http_port_));
  auto http_res = http_client.Get("/tables/app.articles/doc_1");
  ASSERT_TRUE(http_res != nullptr) << "HTTP request returned null";
  ASSERT_EQ(http_res->status, 200) << "HTTP body: " << http_res->body;

  json http_body = json::parse(http_res->body);
  EXPECT_EQ(http_body["primary_key"].get<std::string>(), "doc_1") << "http_body=" << http_res->body;
}

TEST_F(HttpTcpConsistencyTest, CountMatches) {
  auto tcp_response = SendTcpRequest(tcp_port_, "COUNT app.articles learning");
  ASSERT_FALSE(tcp_response.empty()) << "TCP server returned empty response";
  size_t tcp_count = ParseTcpCount(tcp_response, "COUNT");

  httplib::Client http_client("http://127.0.0.1:" + std::to_string(http_port_));
  json req_body;
  req_body["q"] = "learning";
  auto http_res = http_client.Post("/tables/app.articles/count", req_body.dump(), "application/json");
  ASSERT_TRUE(http_res != nullptr) << "HTTP request returned null";
  ASSERT_EQ(http_res->status, 200) << "HTTP body: " << http_res->body;
  json http_body = json::parse(http_res->body);
  size_t http_count = http_body["count"].get<size_t>();

  EXPECT_EQ(tcp_count, http_count) << "TCP count " << tcp_count << " != HTTP count " << http_count;
  EXPECT_GT(tcp_count, 0u);
}

TEST_F(HttpTcpConsistencyTest, FacetMatches) {
  auto tcp_response = SendTcpRequest(tcp_port_, "FACET app.articles category learning");
  ASSERT_FALSE(tcp_response.empty()) << "TCP server returned empty response";
  ASSERT_EQ(tcp_response.rfind("OK FACET", 0), 0U) << "tcp_response=" << tcp_response;
  auto tcp_facets = ParseTcpFacetValues(tcp_response);

  httplib::Client http_client("http://127.0.0.1:" + std::to_string(http_port_));
  json req_body;
  req_body["column"] = "category";
  req_body["q"] = "learning";
  auto http_res = http_client.Post("/tables/app.articles/facet", req_body.dump(), "application/json");
  ASSERT_TRUE(http_res != nullptr) << "HTTP request returned null";
  ASSERT_EQ(http_res->status, 200) << "HTTP body: " << http_res->body;

  json http_body = json::parse(http_res->body);
  std::vector<std::pair<std::string, uint64_t>> http_facets;
  for (const auto& facet : http_body["facets"]) {
    http_facets.emplace_back(facet["value"].get<std::string>(), facet["count"].get<uint64_t>());
  }

  EXPECT_EQ(tcp_facets, http_facets) << "tcp_response=" << tcp_response << " http_body=" << http_res->body;
  ASSERT_EQ(http_facets.size(), 1u);
  EXPECT_EQ(http_facets[0].first, "ai");
  EXPECT_EQ(http_facets[0].second, 2u);
}

TEST_F(HttpTcpConsistencyTest, NoMatchReturnsZeroOnBothPaths) {
  auto tcp_response = SendTcpRequest(tcp_port_, "SEARCH app.articles xyznotfoundabc");
  ASSERT_FALSE(tcp_response.empty()) << "TCP server returned empty response";
  size_t tcp_count = ParseTcpCount(tcp_response, "RESULTS");

  httplib::Client http_client("http://127.0.0.1:" + std::to_string(http_port_));
  json req_body;
  req_body["q"] = "xyznotfoundabc";
  auto http_res = http_client.Post("/tables/app.articles/search", req_body.dump(), "application/json");
  ASSERT_TRUE(http_res != nullptr);
  ASSERT_EQ(http_res->status, 200);
  json http_body = json::parse(http_res->body);
  size_t http_count = http_body["count"].get<size_t>();

  EXPECT_EQ(tcp_count, 0u);
  EXPECT_EQ(http_count, 0u);
}

}  // namespace server
}  // namespace mygramdb
