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

#include <algorithm>
#include <array>
#include <chrono>
#include <map>
#include <memory>
#include <nlohmann/json.hpp>
#include <sstream>
#include <thread>

#include "client/mygramclient.h"
#include "client/mygramclient_c.h"
#include "config/config.h"
#include "server/http_server.h"
#include "server/response_formatter.h"
#include "server/tcp_server.h"
#include "support/network_test_utils.h"
#include "tcp_server_test_helpers.h"

using json = nlohmann::json;

namespace mygramdb {
namespace server {

namespace {

// Send a single TCP request and read the full response (until "OK ..." or
// "ERROR ..." line is received). Frames that carry a body after the header
// line — FACET counts and highlighted SEARCH results — end with a blank line;
// pass multiline_response to read up to it. Returns empty string on failure.
std::string SendTcpRequest(uint16_t port, const std::string& request, bool multiline_response = false) {
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
      if ((response.rfind("OK FACET", 0) == 0 || multiline_response) && response.rfind("ERROR", 0) != 0) {
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

// Extract the numeric code from an "ERROR <code> <message>" frame, or -1 when
// the response is not a coded error frame.
int ParseTcpErrorCode(const std::string& response) {
  std::istringstream stream(response);
  std::string tag;
  int code = -1;
  stream >> tag >> code;
  return tag == "ERROR" ? code : -1;
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

std::string QuoteLiteral(const std::string& value) {
  std::string quoted{"\""};
  for (const char chr : value) {
    if (chr == '\\' || chr == '"') {
      quoted.push_back('\\');
    }
    quoted.push_back(chr);
  }
  quoted.push_back('"');
  return quoted;
}

void SortPrimaryKeys(std::vector<std::string>& primary_keys) {
  std::sort(primary_keys.begin(), primary_keys.end());
}

std::string RepeatUtf8(std::string_view character, size_t count) {
  std::string text;
  text.reserve(character.size() * count);
  for (size_t i = 0; i < count; ++i) {
    text.append(character);
  }
  return text;
}

// Bring up a TCP and an HTTP server over one shared set of table contexts.
// Returns false when either server fails to bind.
bool StartSurfacePair(const std::unordered_map<std::string, TableContext*>& table_contexts, config::Config& config,
                      std::unique_ptr<TcpServer>& tcp_server, std::unique_ptr<HttpServer>& http_server,
                      uint16_t& tcp_port, uint16_t& http_port) {
  ServerConfig tcp_config;
  tcp_config.host = "127.0.0.1";
  tcp_config.port = 0;
  tcp_config.allow_cidrs = {"127.0.0.1/32"};
  tcp_config.max_query_length = config.api.max_query_length;
  tcp_server = std::make_unique<TcpServer>(tcp_config, table_contexts, "./dumps", &config);
  if (!tcp_server->Start()) {
    return false;
  }
  tcp_port = tcp_server->GetPort();

  const uint16_t requested_http_port = testing::FindAvailableLoopbackPort();
  if (requested_http_port == 0) {
    return false;
  }
  HttpServerConfig http_config;
  http_config.bind = "127.0.0.1";
  http_config.port = requested_http_port;
  http_config.allow_cidrs = {"127.0.0.1/32"};
  http_server = std::make_unique<HttpServer>(http_config, table_contexts, &config, nullptr);
  if (!http_server->Start()) {
    return false;
  }
  http_port = http_server->GetPort();

  // Allow servers to fully bind.
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  return true;
}

void StopSurfacePair(TcpServer* tcp_server, HttpServer* http_server) {
  if (tcp_server != nullptr && tcp_server->IsRunning()) {
    tcp_server->Stop();
  }
  if (http_server != nullptr && http_server->IsRunning()) {
    http_server->Stop();
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
}

std::vector<std::string> HttpSearchPrimaryKeys(const std::string& body) {
  const json parsed = json::parse(body);
  std::vector<std::string> primary_keys;
  for (const auto& result : parsed["results"]) {
    primary_keys.push_back(result["primary_key"].get<std::string>());
  }
  return primary_keys;
}

// Map the "<pk>\t<snippet>" body lines of a highlighted SEARCH frame.
std::map<std::string, std::string> ParseTcpHighlightSnippets(const std::string& response) {
  std::istringstream stream(response);
  std::string header;
  std::getline(stream, header);

  std::map<std::string, std::string> snippets;
  std::string line;
  while (std::getline(stream, line)) {
    if (!line.empty() && line.back() == '\r') {
      line.pop_back();
    }
    const auto tab_pos = line.find('\t');
    if (tab_pos == std::string::npos) {
      continue;
    }
    snippets.emplace(line.substr(0, tab_pos), line.substr(tab_pos + 1));
  }
  return snippets;
}

std::map<std::string, std::string> HttpHighlightSnippets(const std::string& body) {
  const json parsed = json::parse(body);
  std::map<std::string, std::string> snippets;
  for (const auto& result : parsed["results"]) {
    snippets.emplace(result["primary_key"].get<std::string>(), result["highlight"].get<std::string>());
  }
  return snippets;
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

    auto id1 = doc_store->AddDocument("doc_1", filters1, "machine learning models");
    auto id2 = doc_store->AddDocument("doc_2", filters2, "machine production lines");
    auto id3 = doc_store->AddDocument("doc_3", filters3, "deep learning research");
    auto id4 = doc_store->AddDocument("doc_4", filters4, "unrelated topic");
    auto id5 = doc_store->AddDocument("doc_5", filters4, "COVID-19 status");
    auto id6 = doc_store->AddDocument("doc_6", filters4, "C++ compiler");
    auto id7 = doc_store->AddDocument("doc_7", filters4, "foo@example.com inbox");
    auto id8 = doc_store->AddDocument("doc_8", filters4, "alpha AND beta");
    auto id9 = doc_store->AddDocument("doc_9", filters4, "alpha only");
    auto id10 = doc_store->AddDocument("doc_10", filters4, "beta only");

    index->AddDocument(*id1, "machine learning models");
    index->AddDocument(*id2, "machine production lines");
    index->AddDocument(*id3, "deep learning research");
    index->AddDocument(*id4, "unrelated topic");
    index->AddDocument(*id5, "COVID-19 status");
    index->AddDocument(*id6, "C++ compiler");
    index->AddDocument(*id7, "foo@example.com inbox");
    index->AddDocument(*id8, "alpha AND beta");
    index->AddDocument(*id9, "alpha only");
    index->AddDocument(*id10, "beta only");

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
    config_->api.admin_token = "maintenance-secret";
    config_->tables.push_back(table_ctx_.config);

    // TCP server (let OS pick a port).
    ServerConfig tcp_cfg;
    tcp_cfg.host = "127.0.0.1";
    tcp_cfg.port = 0;
    tcp_cfg.allow_cidrs = {"127.0.0.1/32"};
    tcp_cfg.admin_token = config_->api.admin_token;
    tcp_server_ = std::make_unique<TcpServer>(tcp_cfg, table_contexts_, "./dumps", config_.get());
    ASSERT_TRUE(tcp_server_->Start());
    tcp_port_ = tcp_server_->GetPort();

    // HTTP server -- use an isolated loopback port.
    const uint16_t http_port = testing::FindAvailableLoopbackPort();
    ASSERT_GT(http_port, 0);
    HttpServerConfig http_cfg;
    http_cfg.bind = "127.0.0.1";
    http_cfg.port = http_port;
    http_cfg.allow_cidrs = {"127.0.0.1/32"};
    http_server_ = std::make_unique<HttpServer>(http_cfg, table_contexts_, config_.get(), nullptr);
    http_server_->SetOptimizeCallback(
        [this](const std::string& table) { return tcp_server_->HandleOptimizeRequest(table); });
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

TEST_F(HttpTcpConsistencyTest, HttpOptimizeUsesSharedMaintenanceHandler) {
  httplib::Client client("127.0.0.1", http_port_);
  httplib::Headers headers{{"Authorization", "Bearer maintenance-secret"}};

  auto all_tables = client.Post("/optimize", headers, "{}", "application/json");
  ASSERT_TRUE(all_tables);
  ASSERT_EQ(all_tables->status, 200) << all_tables->body;
  auto all_body = json::parse(all_tables->body);
  EXPECT_EQ(all_body["status"], "ok");
  EXPECT_NE(all_body["result"].get<std::string>().find("OPTIMIZED tables=1"), std::string::npos);

  auto one_table = client.Post("/optimize", headers, R"({"table":"app.articles"})", "application/json; charset=utf-8");
  ASSERT_TRUE(one_table);
  ASSERT_EQ(one_table->status, 200) << one_table->body;
  auto one_body = json::parse(one_table->body);
  EXPECT_NE(one_body["result"].get<std::string>().find("OPTIMIZED tables=1"), std::string::npos);

  EXPECT_EQ(http_server_->GetStats().GetCommandCount(query::QueryType::OPTIMIZE), 2U);
}

TEST_F(HttpTcpConsistencyTest, HttpOptimizePreservesCodedCallbackError) {
  http_server_->SetOptimizeCallback([](const std::string&) {
    return ResponseFormatter::FormatError(
        mygram::utils::MakeError(mygram::utils::ErrorCode::kInternalError, "Optimization failed"));
  });

  httplib::Client client("127.0.0.1", http_port_);
  httplib::Headers headers{{"Authorization", "Bearer maintenance-secret"}};
  auto response = client.Post("/optimize", headers, "{}", "application/json");

  ASSERT_TRUE(response);
  ASSERT_EQ(response->status, 503);
  const auto body = json::parse(response->body);
  EXPECT_EQ(body["error_code"], static_cast<int>(mygram::utils::ErrorCode::kInternalError));
  EXPECT_EQ(body["error"], "Optimization failed");
}

TEST_F(HttpTcpConsistencyTest, ErrorCodesMatchAcrossProtocols) {
  httplib::Client client("127.0.0.1", http_port_);

  // An unknown table is the same fault whichever surface reports it.
  const auto tcp_missing = SendTcpRequest(tcp_port_, "SEARCH app.missing machine");
  EXPECT_EQ(ParseTcpErrorCode(tcp_missing), static_cast<int>(mygram::utils::ErrorCode::kTableNotFound)) << tcp_missing;
  auto http_missing = client.Post("/tables/app.missing/search", R"({"q":"machine"})", "application/json");
  ASSERT_TRUE(http_missing);
  ASSERT_EQ(http_missing->status, 404) << http_missing->body;
  EXPECT_EQ(json::parse(http_missing->body)["error_code"], static_cast<int>(mygram::utils::ErrorCode::kTableNotFound));

  // So is an administrative request that carries no credentials.
  const auto tcp_unauthenticated = SendTcpRequest(tcp_port_, "OPTIMIZE");
  EXPECT_EQ(ParseTcpErrorCode(tcp_unauthenticated), static_cast<int>(mygram::utils::ErrorCode::kPermissionDenied))
      << tcp_unauthenticated;
  auto http_unauthorized = client.Post("/optimize", "{}", "application/json");
  ASSERT_TRUE(http_unauthorized);
  ASSERT_EQ(http_unauthorized->status, 401) << http_unauthorized->body;
  EXPECT_EQ(json::parse(http_unauthorized->body)["error_code"],
            static_cast<int>(mygram::utils::ErrorCode::kPermissionDenied));
}

TEST_F(HttpTcpConsistencyTest, ScoreSortRejectionCodesMatchAcrossProtocols) {
  httplib::Client client("127.0.0.1", http_port_);
  json request;
  request["q"] = "machine";
  request["sort"] = {{"column", "_score"}, {"order", "DESC"}};

  // BM25 is disabled in this fixture's configuration.
  const auto tcp_bm25_disabled = SendTcpRequest(tcp_port_, "SEARCH app.articles machine SORT _score DESC");
  EXPECT_EQ(ParseTcpErrorCode(tcp_bm25_disabled), static_cast<int>(mygram::utils::ErrorCode::kQueryInvalidSort))
      << tcp_bm25_disabled;
  auto http_bm25_disabled = client.Post("/tables/app.articles/search", request.dump(), "application/json");
  ASSERT_TRUE(http_bm25_disabled);
  EXPECT_EQ(json::parse(http_bm25_disabled->body)["error_code"],
            static_cast<int>(mygram::utils::ErrorCode::kQueryInvalidSort))
      << http_bm25_disabled->body;

  // With BM25 enabled, scoring still needs the stored normalized text.
  config_->bm25.enable = true;
  table_ctx_.doc_store->SetStoreTexts(false);

  const auto tcp_no_text = SendTcpRequest(tcp_port_, "SEARCH app.articles machine SORT _score DESC");
  EXPECT_EQ(ParseTcpErrorCode(tcp_no_text), static_cast<int>(mygram::utils::ErrorCode::kNotImplemented)) << tcp_no_text;
  auto http_no_text = client.Post("/tables/app.articles/search", request.dump(), "application/json");
  ASSERT_TRUE(http_no_text);
  EXPECT_EQ(json::parse(http_no_text->body)["error_code"], static_cast<int>(mygram::utils::ErrorCode::kNotImplemented))
      << http_no_text->body;
}

TEST_F(HttpTcpConsistencyTest, HttpRequestErrorsNameTheirCause) {
  httplib::Client client("127.0.0.1", http_port_);

  struct RejectedRequest {
    const char* body;
    const char* content_type;
    mygram::utils::ErrorCode expected;
  };
  const std::array<RejectedRequest, 5> cases = {{
      {R"({"q":"machine","limit":0})", "application/json", mygram::utils::ErrorCode::kQueryInvalidLimit},
      {R"({"q":"machine","offset":-1})", "application/json", mygram::utils::ErrorCode::kQueryInvalidOffset},
      {R"({"q":"machine","filters":[]})", "application/json", mygram::utils::ErrorCode::kQueryInvalidFilter},
      {"{not json", "application/json", mygram::utils::ErrorCode::kQuerySyntaxError},
      {R"({"q":"machine"})", "text/plain", mygram::utils::ErrorCode::kNetworkInvalidRequest},
  }};

  for (const auto& rejected : cases) {
    auto response = client.Post("/tables/app.articles/search", rejected.body, rejected.content_type);
    ASSERT_TRUE(response) << rejected.body;
    const auto body = json::parse(response->body);
    EXPECT_EQ(body["error_code"], static_cast<int>(rejected.expected)) << response->body;
  }
}

TEST_F(HttpTcpConsistencyTest, HttpOptimizeValidatesPayloadAndAdminToken) {
  httplib::Client client("127.0.0.1", http_port_);
  httplib::Headers headers{{"Authorization", "Bearer maintenance-secret"}};

  auto invalid_type = client.Post("/optimize", headers, R"({"table":42})", "application/json");
  ASSERT_TRUE(invalid_type);
  EXPECT_EQ(invalid_type->status, 400);

  auto missing_table = client.Post("/optimize", headers, R"({"table":"missing"})", "application/json");
  ASSERT_TRUE(missing_table);
  EXPECT_EQ(missing_table->status, 404);

  auto unauthorized = client.Post("/optimize", "{}", "application/json");
  ASSERT_TRUE(unauthorized);
  EXPECT_EQ(unauthorized->status, 401);
  EXPECT_EQ(unauthorized->get_header_value("WWW-Authenticate"), "Bearer");

  auto authorized = client.Post("/optimize", headers, "{}", "application/json");
  ASSERT_TRUE(authorized);
  EXPECT_EQ(authorized->status, 200) << authorized->body;
}

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

TEST_F(HttpTcpConsistencyTest, LiteralQueriesMatchAcrossHttpTcpCppAndCClients) {
  client::ClientConfig cpp_config;
  cpp_config.host = "127.0.0.1";
  cpp_config.port = tcp_port_;
  cpp_config.timeout_ms = 2000;
  client::MygramClient cpp_client(cpp_config);
  ASSERT_TRUE(cpp_client.Connect());

  MygramClientConfig_C c_config{};
  c_config.host = "127.0.0.1";
  c_config.port = tcp_port_;
  c_config.timeout_ms = 2000;
  c_config.recv_buffer_size = 64 * 1024;
  std::unique_ptr<MygramClient_C, decltype(&mygramclient_destroy)> c_client(mygramclient_create(&c_config),
                                                                            mygramclient_destroy);
  ASSERT_NE(c_client, nullptr);
  ASSERT_EQ(mygramclient_connect(c_client.get()), 0) << mygramclient_get_last_error(c_client.get());

  httplib::Client http_client("http://127.0.0.1:" + std::to_string(http_port_));
  const std::vector<std::string> queries = {
      "machine", "COVID-19", "C++", "foo@example.com", "alpha AND beta",
  };
  for (const auto& query : queries) {
    SCOPED_TRACE(query);

    auto tcp_primary_keys =
        ParseTcpSearchPrimaryKeys(SendTcpRequest(tcp_port_, "SEARCH app.articles " + QuoteLiteral(query)));
    SortPrimaryKeys(tcp_primary_keys);

    json request_body;
    request_body["q"] = query;
    request_body["limit"] = 100;
    auto http_result = http_client.Post("/tables/app.articles/search", request_body.dump(), "application/json");
    ASSERT_NE(http_result, nullptr);
    ASSERT_EQ(http_result->status, 200) << http_result->body;
    std::vector<std::string> http_primary_keys;
    const json http_body = json::parse(http_result->body);
    for (const auto& result : http_body["results"]) {
      http_primary_keys.push_back(result["primary_key"].get<std::string>());
    }
    SortPrimaryKeys(http_primary_keys);

    auto cpp_result = cpp_client.Search("app.articles", query, 100);
    ASSERT_TRUE(cpp_result) << cpp_result.error().message();
    std::vector<std::string> cpp_primary_keys;
    for (const auto& result : cpp_result->results) {
      cpp_primary_keys.push_back(result.primary_key);
    }
    SortPrimaryKeys(cpp_primary_keys);

    MygramSearchResult_C* raw_c_result = nullptr;
    ASSERT_EQ(mygramclient_search(c_client.get(), "app.articles", query.c_str(), 100, 0, &raw_c_result), 0)
        << mygramclient_get_last_error(c_client.get());
    std::unique_ptr<MygramSearchResult_C, decltype(&mygramclient_free_search_result)> c_result(
        raw_c_result, mygramclient_free_search_result);
    std::vector<std::string> c_primary_keys;
    for (size_t index = 0; index < c_result->count; ++index) {
      c_primary_keys.emplace_back(c_result->primary_keys[index]);
    }
    SortPrimaryKeys(c_primary_keys);

    EXPECT_EQ(http_primary_keys, tcp_primary_keys) << "http_body=" << http_result->body;
    EXPECT_EQ(cpp_primary_keys, tcp_primary_keys);
    EXPECT_EQ(c_primary_keys, tcp_primary_keys);
    if (query == "machine") {
      EXPECT_FALSE(tcp_primary_keys.empty());
    }
  }
}

TEST_F(HttpTcpConsistencyTest, BooleanQueriesMatchAcrossHttpTcpCppAndCClients) {
  client::ClientConfig cpp_config;
  cpp_config.host = "127.0.0.1";
  cpp_config.port = tcp_port_;
  cpp_config.timeout_ms = 2000;
  client::MygramClient cpp_client(cpp_config);
  ASSERT_TRUE(cpp_client.Connect());

  MygramClientConfig_C c_config{};
  c_config.host = "127.0.0.1";
  c_config.port = tcp_port_;
  c_config.timeout_ms = 2000;
  c_config.recv_buffer_size = 64 * 1024;
  std::unique_ptr<MygramClient_C, decltype(&mygramclient_destroy)> c_client(mygramclient_create(&c_config),
                                                                            mygramclient_destroy);
  ASSERT_NE(c_client, nullptr);
  ASSERT_EQ(mygramclient_connect(c_client.get()), 0) << mygramclient_get_last_error(c_client.get());

  httplib::Client http_client("http://127.0.0.1:" + std::to_string(http_port_));
  const std::vector<std::string> expressions = {"alpha OR beta", "alpha or beta", "alpha Or beta", "NOT beta"};
  for (const auto& expression : expressions) {
    SCOPED_TRACE(expression);
    auto tcp_primary_keys = ParseTcpSearchPrimaryKeys(SendTcpRequest(tcp_port_, "SEARCH app.articles " + expression));
    SortPrimaryKeys(tcp_primary_keys);

    json request_body;
    request_body["q"] = expression;
    request_body["mode"] = "boolean";
    request_body["limit"] = 100;
    auto http_result = http_client.Post("/tables/app.articles/search", request_body.dump(), "application/json");
    ASSERT_NE(http_result, nullptr);
    ASSERT_EQ(http_result->status, 200) << http_result->body;
    std::vector<std::string> http_primary_keys;
    const json http_body = json::parse(http_result->body);
    for (const auto& result : http_body["results"]) {
      http_primary_keys.push_back(result["primary_key"].get<std::string>());
    }
    SortPrimaryKeys(http_primary_keys);

    auto cpp_result = cpp_client.SearchRaw("app.articles", expression, 100);
    ASSERT_TRUE(cpp_result) << cpp_result.error().message();
    std::vector<std::string> cpp_primary_keys;
    for (const auto& result : cpp_result->results) {
      cpp_primary_keys.push_back(result.primary_key);
    }
    SortPrimaryKeys(cpp_primary_keys);

    MygramSearchResult_C* raw_c_result = nullptr;
    ASSERT_EQ(mygramclient_search_raw(c_client.get(), "app.articles", expression.c_str(), 100, 0, &raw_c_result), 0)
        << mygramclient_get_last_error(c_client.get());
    std::unique_ptr<MygramSearchResult_C, decltype(&mygramclient_free_search_result)> c_result(
        raw_c_result, mygramclient_free_search_result);
    std::vector<std::string> c_primary_keys;
    for (size_t index = 0; index < c_result->count; ++index) {
      c_primary_keys.emplace_back(c_result->primary_keys[index]);
    }
    SortPrimaryKeys(c_primary_keys);

    EXPECT_EQ(http_primary_keys, tcp_primary_keys) << "http_body=" << http_result->body;
    EXPECT_EQ(cpp_primary_keys, tcp_primary_keys);
    EXPECT_EQ(c_primary_keys, tcp_primary_keys);
  }
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

TEST_F(HttpTcpConsistencyTest, FuzzySearchPreservesResultsAcrossTcpAndHttp) {
  auto tcp_response = SendTcpRequest(tcp_port_, "SEARCH app.articles machime FUZZY 1 LIMIT 1");
  ASSERT_FALSE(tcp_response.empty()) << "TCP server returned empty response";
  EXPECT_EQ(ParseTcpCount(tcp_response, "RESULTS"), 2U) << "tcp_response=" << tcp_response;
  auto tcp_primary_keys = ParseTcpSearchPrimaryKeys(tcp_response);
  ASSERT_EQ(tcp_primary_keys, (std::vector<std::string>{"doc_2"})) << "tcp_response=" << tcp_response;

  httplib::Client http_client("http://127.0.0.1:" + std::to_string(http_port_));
  json req_body;
  req_body["q"] = "machime";
  req_body["fuzzy"] = 1;
  req_body["limit"] = 1;
  auto http_res = http_client.Post("/tables/app.articles/search", req_body.dump(), "application/json");
  ASSERT_TRUE(http_res != nullptr) << "HTTP request returned null";
  ASSERT_EQ(http_res->status, 200) << "HTTP body: " << http_res->body;

  json http_body = json::parse(http_res->body);
  ASSERT_EQ(http_body["count"].get<size_t>(), 2U);
  ASSERT_EQ(http_body["results"].size(), 1U);
  EXPECT_EQ(http_body["results"][0]["primary_key"].get<std::string>(), tcp_primary_keys[0])
      << "tcp_response=" << tcp_response << " http_body=" << http_res->body;
}

TEST_F(HttpTcpConsistencyTest, BooleanNotSearchAndCountPreserveResultsAcrossTcpAndHttp) {
  const std::string expression = "machine AND (NOT production)";
  auto tcp_search = SendTcpRequest(tcp_port_, "SEARCH app.articles " + expression + " LIMIT 1");
  ASSERT_FALSE(tcp_search.empty()) << "TCP server returned empty response";
  EXPECT_EQ(ParseTcpCount(tcp_search, "RESULTS"), 1U) << "tcp_response=" << tcp_search;
  auto tcp_primary_keys = ParseTcpSearchPrimaryKeys(tcp_search);
  ASSERT_EQ(tcp_primary_keys, (std::vector<std::string>{"doc_1"})) << "tcp_response=" << tcp_search;

  httplib::Client http_client("http://127.0.0.1:" + std::to_string(http_port_));
  json search_body;
  search_body["q"] = expression;
  search_body["mode"] = "boolean";
  search_body["limit"] = 1;
  auto http_search = http_client.Post("/tables/app.articles/search", search_body.dump(), "application/json");
  ASSERT_TRUE(http_search != nullptr) << "HTTP request returned null";
  ASSERT_EQ(http_search->status, 200) << "HTTP body: " << http_search->body;

  json http_search_body = json::parse(http_search->body);
  ASSERT_EQ(http_search_body["count"].get<size_t>(), 1U);
  ASSERT_EQ(http_search_body["results"].size(), 1U);
  EXPECT_EQ(http_search_body["results"][0]["primary_key"].get<std::string>(), tcp_primary_keys[0])
      << "tcp_response=" << tcp_search << " http_body=" << http_search->body;

  auto tcp_count_response = SendTcpRequest(tcp_port_, "COUNT app.articles " + expression);
  ASSERT_FALSE(tcp_count_response.empty()) << "TCP server returned empty response";
  const size_t tcp_count = ParseTcpCount(tcp_count_response, "COUNT");
  EXPECT_EQ(tcp_count, 1U) << "tcp_response=" << tcp_count_response;

  json count_body;
  count_body["q"] = expression;
  count_body["mode"] = "boolean";
  auto http_count = http_client.Post("/tables/app.articles/count", count_body.dump(), "application/json");
  ASSERT_TRUE(http_count != nullptr) << "HTTP request returned null";
  ASSERT_EQ(http_count->status, 200) << "HTTP body: " << http_count->body;
  EXPECT_EQ(json::parse(http_count->body)["count"].get<size_t>(), tcp_count);
}

TEST_F(HttpTcpConsistencyTest, LiteralIsDefaultAndBooleanModeIsExplicit) {
  const std::string input = "machine AND production";
  auto tcp_literal = SendTcpRequest(tcp_port_, "SEARCH app.articles \"" + input + "\"");
  ASSERT_FALSE(tcp_literal.empty());

  httplib::Client http_client("http://127.0.0.1:" + std::to_string(http_port_));
  json literal_body;
  literal_body["q"] = input;
  auto http_literal = http_client.Post("/tables/app.articles/search", literal_body.dump(), "application/json");
  ASSERT_TRUE(http_literal != nullptr);
  ASSERT_EQ(http_literal->status, 200) << http_literal->body;
  EXPECT_EQ(json::parse(http_literal->body)["count"].get<size_t>(), ParseTcpCount(tcp_literal, "RESULTS"));

  auto tcp_boolean = SendTcpRequest(tcp_port_, "SEARCH app.articles " + input);
  ASSERT_FALSE(tcp_boolean.empty());
  json boolean_body;
  boolean_body["q"] = input;
  boolean_body["mode"] = "boolean";
  auto http_boolean = http_client.Post("/tables/app.articles/search", boolean_body.dump(), "application/json");
  ASSERT_TRUE(http_boolean != nullptr);
  ASSERT_EQ(http_boolean->status, 200) << http_boolean->body;
  EXPECT_EQ(json::parse(http_boolean->body)["count"].get<size_t>(), ParseTcpCount(tcp_boolean, "RESULTS"));
  EXPECT_GT(ParseTcpCount(tcp_boolean, "RESULTS"), ParseTcpCount(tcp_literal, "RESULTS"));
}

TEST_F(HttpTcpConsistencyTest, LiteralQuotingOfSpecialCharactersMatchesAcrossSurfaces) {
  httplib::Client client("127.0.0.1", http_port_);

  // Text carrying the characters the command grammar quotes with must survive
  // both surfaces as a literal phrase rather than becoming extra tokens.
  const std::array<const char*, 3> texts = {R"(say "hi")", R"(back\slash)", R"(a "quoted" \ tail)"};
  for (const auto* text : texts) {
    const auto tcp_response = SendTcpRequest(tcp_port_, "SEARCH app.articles " + QuoteLiteral(text));
    ASSERT_FALSE(tcp_response.empty()) << text;
    EXPECT_EQ(ParseTcpErrorCode(tcp_response), -1) << tcp_response;

    json request;
    request["q"] = text;
    auto http_response = client.Post("/tables/app.articles/search", request.dump(), "application/json");
    ASSERT_TRUE(http_response) << text;
    ASSERT_EQ(http_response->status, 200) << http_response->body;
    EXPECT_EQ(json::parse(http_response->body)["count"].get<size_t>(), ParseTcpCount(tcp_response, "RESULTS")) << text;
  }
}

TEST_F(HttpTcpConsistencyTest, FilterConditionRulesMatchAcrossSurfaces) {
  httplib::Client client("127.0.0.1", http_port_);

  // A column outside the shared grammar.
  const auto tcp_column = SendTcpRequest(tcp_port_, R"(SEARCH app.articles machine FILTER "bad col" = ai)");
  EXPECT_EQ(ParseTcpErrorCode(tcp_column), static_cast<int>(mygram::utils::ErrorCode::kQueryInvalidFilter))
      << tcp_column;
  json bad_column;
  bad_column["q"] = "machine";
  bad_column["filters"]["bad col"] = "ai";
  auto http_column = client.Post("/tables/app.articles/search", bad_column.dump(), "application/json");
  ASSERT_TRUE(http_column);
  EXPECT_EQ(json::parse(http_column->body)["error_code"],
            static_cast<int>(mygram::utils::ErrorCode::kQueryInvalidFilter))
      << http_column->body;

  // A value one byte past the shared cap.
  const std::string oversize_value(query::QueryParser::kMaxFilterValueLength + 1, 'a');
  const auto tcp_value = SendTcpRequest(tcp_port_, "SEARCH app.articles machine FILTER category = " + oversize_value);
  EXPECT_EQ(ParseTcpErrorCode(tcp_value), static_cast<int>(mygram::utils::ErrorCode::kQueryInvalidFilter)) << tcp_value;
  json oversize;
  oversize["q"] = "machine";
  oversize["filters"]["category"] = oversize_value;
  auto http_value = client.Post("/tables/app.articles/search", oversize.dump(), "application/json");
  ASSERT_TRUE(http_value);
  EXPECT_EQ(json::parse(http_value->body)["error_code"],
            static_cast<int>(mygram::utils::ErrorCode::kQueryInvalidFilter))
      << http_value->body;
}

TEST_F(HttpTcpConsistencyTest, SortOrderKeywordCaseIsIgnoredOnBothSurfaces) {
  httplib::Client client("127.0.0.1", http_port_);

  const auto tcp_upper = SendTcpRequest(tcp_port_, "SEARCH app.articles learning SORT id ASC");
  const auto tcp_lower = SendTcpRequest(tcp_port_, "SEARCH app.articles learning SORT id asc");
  const auto upper_keys = ParseTcpSearchPrimaryKeys(tcp_upper);
  ASSERT_FALSE(upper_keys.empty()) << tcp_upper;
  EXPECT_EQ(ParseTcpSearchPrimaryKeys(tcp_lower), upper_keys) << tcp_lower;

  for (const auto* order : {"ASC", "asc"}) {
    json request;
    request["q"] = "learning";
    request["sort"] = {{"column", "id"}, {"order", order}};
    auto response = client.Post("/tables/app.articles/search", request.dump(), "application/json");
    ASSERT_TRUE(response) << order;
    ASSERT_EQ(response->status, 200) << response->body;
    EXPECT_EQ(HttpSearchPrimaryKeys(response->body), upper_keys) << response->body;
  }
}

TEST_F(HttpTcpConsistencyTest, HighlightSnippetsMatchAcrossSurfaces) {
  const auto tcp_response =
      SendTcpRequest(tcp_port_, "SEARCH app.articles learning HIGHLIGHT TAG <b> </b>", /*multiline_response=*/true);
  const auto tcp_snippets = ParseTcpHighlightSnippets(tcp_response);
  ASSERT_FALSE(tcp_snippets.empty()) << tcp_response;

  httplib::Client client("127.0.0.1", http_port_);
  json request;
  request["q"] = "learning";
  request["highlight"] = {{"open_tag", "<b>"}, {"close_tag", "</b>"}};
  auto http_response = client.Post("/tables/app.articles/search", request.dump(), "application/json");
  ASSERT_TRUE(http_response);
  ASSERT_EQ(http_response->status, 200) << http_response->body;

  EXPECT_EQ(HttpHighlightSnippets(http_response->body), tcp_snippets) << http_response->body;
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

TEST_F(HttpTcpConsistencyTest, FacetOffsetAndLimitMatch) {
  const auto tcp_response = SendTcpRequest(tcp_port_, "FACET app.articles category OFFSET 1 LIMIT 1");
  ASSERT_EQ(tcp_response.rfind("OK FACET 1 3", 0), 0U) << tcp_response;
  const auto tcp_facets = ParseTcpFacetValues(tcp_response);

  httplib::Client http_client("http://127.0.0.1:" + std::to_string(http_port_));
  const json request = {{"column", "category"}, {"offset", 1}, {"limit", 1}};
  auto http_res = http_client.Post("/tables/app.articles/facet", request.dump(), "application/json");
  ASSERT_TRUE(http_res);
  ASSERT_EQ(http_res->status, 200) << http_res->body;
  const auto body = json::parse(http_res->body);

  std::vector<std::pair<std::string, uint64_t>> http_facets;
  for (const auto& facet : body["facets"]) {
    http_facets.emplace_back(facet["value"].get<std::string>(), facet["count"].get<uint64_t>());
  }
  EXPECT_EQ(tcp_facets, http_facets);
  EXPECT_EQ(http_facets.size(), 1U);
  EXPECT_EQ(body["count"], 1U);
  EXPECT_EQ(body["total_count"], 3U);
}

TEST_F(HttpTcpConsistencyTest, HttpFacetUsesConfiguredDefaultLimit) {
  http_server_->UpdateApiConfig(/*default_limit=*/1, /*max_query_length=*/10000);

  httplib::Client http_client("http://127.0.0.1:" + std::to_string(http_port_));
  const json request = {{"column", "category"}};
  auto http_res = http_client.Post("/tables/app.articles/facet", request.dump(), "application/json");

  ASSERT_TRUE(http_res);
  ASSERT_EQ(http_res->status, 200) << http_res->body;
  const auto body = json::parse(http_res->body);
  EXPECT_EQ(body["count"].get<size_t>(), 1u);
  ASSERT_EQ(body["facets"].size(), 1u);
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

// A table whose primary key is not called "id" while an ordinary column is.
// Both surfaces must resolve the sort column the client named, and both must
// offer the same shorthand for ordering by the primary key.
class HttpTcpAlternatePrimaryKeyTest : public ::testing::Test {
 protected:
  void SetUp() override {
    mygramdb::test::SkipIfSocketCreationBlocked();

    auto index = std::make_unique<index::Index>(2);
    auto doc_store = std::make_unique<storage::DocumentStore>();

    // The "id" column runs opposite to the primary key, so the two orderings
    // are distinguishable.
    for (const auto& [primary_key, id_value] :
         std::vector<std::pair<std::string, std::string>>{{"article_1", "c"}, {"article_2", "b"}, {"article_3", "a"}}) {
      storage::FilterMap filters;
      filters["id"] = id_value;
      auto doc_id = doc_store->AddDocument(primary_key, filters, "shared topic");
      ASSERT_TRUE(doc_id.has_value());
      index->AddDocument(*doc_id, "shared topic");
    }

    table_ctx_.name = "papers";
    table_ctx_.config.name = "papers";
    table_ctx_.config.database = "app";
    table_ctx_.config.ngram_size = 2;
    table_ctx_.config.primary_key = "article_id";
    table_ctx_.index = std::move(index);
    table_ctx_.doc_store = std::move(doc_store);
    table_contexts_["app.papers"] = &table_ctx_;

    config_ = std::make_unique<config::Config>();
    config_->api.default_limit = 10;
    config_->tables.push_back(table_ctx_.config);

    ASSERT_TRUE(StartSurfacePair(table_contexts_, *config_, tcp_server_, http_server_, tcp_port_, http_port_));
  }

  void TearDown() override { StopSurfacePair(tcp_server_.get(), http_server_.get()); }

  TableContext table_ctx_;
  std::unordered_map<std::string, TableContext*> table_contexts_;
  std::unique_ptr<config::Config> config_;
  std::unique_ptr<TcpServer> tcp_server_;
  std::unique_ptr<HttpServer> http_server_;
  uint16_t tcp_port_ = 0;
  uint16_t http_port_ = 0;
};

TEST_F(HttpTcpAlternatePrimaryKeyTest, NamedSortColumnResolvesIdenticallyOnBothSurfaces) {
  const auto tcp_response = SendTcpRequest(tcp_port_, "SEARCH app.papers shared SORT id ASC LIMIT 10");
  ASSERT_EQ(tcp_response.rfind("OK RESULTS", 0), 0U) << tcp_response;
  const auto tcp_primary_keys = ParseTcpSearchPrimaryKeys(tcp_response);
  ASSERT_EQ(tcp_primary_keys, (std::vector<std::string>{"article_3", "article_2", "article_1"})) << tcp_response;

  httplib::Client client("127.0.0.1", http_port_);
  const json request = {{"q", "shared"}, {"limit", 10}, {"sort", {{"column", "id"}, {"order", "asc"}}}};
  auto http_response = client.Post("/tables/app.papers/search", request.dump(), "application/json");
  ASSERT_TRUE(http_response);
  ASSERT_EQ(http_response->status, 200) << http_response->body;
  EXPECT_EQ(HttpSearchPrimaryKeys(http_response->body), tcp_primary_keys) << http_response->body;
}

TEST_F(HttpTcpAlternatePrimaryKeyTest, PrimaryKeyShorthandMatchesTheTcpForm) {
  const auto tcp_response = SendTcpRequest(tcp_port_, "SEARCH app.papers shared SORT ASC LIMIT 10");
  ASSERT_EQ(tcp_response.rfind("OK RESULTS", 0), 0U) << tcp_response;
  const auto tcp_primary_keys = ParseTcpSearchPrimaryKeys(tcp_response);
  ASSERT_EQ(tcp_primary_keys, (std::vector<std::string>{"article_1", "article_2", "article_3"})) << tcp_response;

  httplib::Client client("127.0.0.1", http_port_);
  const json request = {{"q", "shared"}, {"limit", 10}, {"sort", {{"order", "asc"}}}};
  auto http_response = client.Post("/tables/app.papers/search", request.dump(), "application/json");
  ASSERT_TRUE(http_response);
  ASSERT_EQ(http_response->status, 200) << http_response->body;
  EXPECT_EQ(HttpSearchPrimaryKeys(http_response->body), tcp_primary_keys) << http_response->body;
}

// api.max_query_length is documented in characters, so the same query must be
// accepted or rejected identically whatever encoding it uses, on both surfaces.
class HttpTcpQueryLengthTest : public ::testing::Test {
 protected:
  static constexpr int kMaxQueryLength = 128;

  void SetUp() override {
    mygramdb::test::SkipIfSocketCreationBlocked();

    auto index = std::make_unique<index::Index>(2);
    auto doc_store = std::make_unique<storage::DocumentStore>();
    auto doc_id = doc_store->AddDocument("doc_1", {}, "machine learning models");
    ASSERT_TRUE(doc_id.has_value());
    index->AddDocument(*doc_id, "machine learning models");

    table_ctx_.name = "articles";
    table_ctx_.config.name = "articles";
    table_ctx_.config.database = "app";
    table_ctx_.config.ngram_size = 2;
    table_ctx_.config.primary_key = "id";
    table_ctx_.index = std::move(index);
    table_ctx_.doc_store = std::move(doc_store);
    table_contexts_["app.articles"] = &table_ctx_;

    config_ = std::make_unique<config::Config>();
    config_->api.default_limit = 10;
    config_->api.max_query_length = kMaxQueryLength;
    config_->tables.push_back(table_ctx_.config);

    ASSERT_TRUE(StartSurfacePair(table_contexts_, *config_, tcp_server_, http_server_, tcp_port_, http_port_));
  }

  void TearDown() override { StopSurfacePair(tcp_server_.get(), http_server_.get()); }

  TableContext table_ctx_;
  std::unordered_map<std::string, TableContext*> table_contexts_;
  std::unique_ptr<config::Config> config_;
  std::unique_ptr<TcpServer> tcp_server_;
  std::unique_ptr<HttpServer> http_server_;
  uint16_t tcp_port_ = 0;
  uint16_t http_port_ = 0;
};

TEST_F(HttpTcpQueryLengthTest, QueryLengthLimitCountsCharactersOnBothSurfaces) {
  const std::string within_limit = RepeatUtf8("あ", kMaxQueryLength);
  const std::string over_limit = RepeatUtf8("あ", kMaxQueryLength + 1);

  httplib::Client client("127.0.0.1", http_port_);

  const auto tcp_accepted = SendTcpRequest(tcp_port_, "SEARCH app.articles " + QuoteLiteral(within_limit));
  EXPECT_EQ(tcp_accepted.rfind("OK RESULTS", 0), 0U) << tcp_accepted;
  const json accepted_request = {{"q", within_limit}};
  auto http_accepted = client.Post("/tables/app.articles/search", accepted_request.dump(), "application/json");
  ASSERT_TRUE(http_accepted);
  EXPECT_EQ(http_accepted->status, 200) << http_accepted->body;

  const auto tcp_rejected = SendTcpRequest(tcp_port_, "SEARCH app.articles " + QuoteLiteral(over_limit));
  EXPECT_EQ(ParseTcpErrorCode(tcp_rejected), static_cast<int>(mygram::utils::ErrorCode::kQueryTooLong)) << tcp_rejected;
  const json rejected_request = {{"q", over_limit}};
  auto http_rejected = client.Post("/tables/app.articles/search", rejected_request.dump(), "application/json");
  ASSERT_TRUE(http_rejected);
  const auto rejected_body = json::parse(http_rejected->body);
  EXPECT_EQ(rejected_body["error_code"], static_cast<int>(mygram::utils::ErrorCode::kQueryTooLong))
      << http_rejected->body;
  // A single length check on each surface, reporting the assembled expression.
  EXPECT_NE(rejected_body["error"].get<std::string>().find("Query expression length (" +
                                                           std::to_string(kMaxQueryLength + 1) + ")"),
            std::string::npos)
      << http_rejected->body;
}

}  // namespace server
}  // namespace mygramdb
