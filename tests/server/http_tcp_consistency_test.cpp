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

// Send several requests over one connection so per-connection state — AUTH and
// DEBUG have no per-request equivalent on the HTTP surface — can be exercised.
// Returns one response per request, in order.
std::vector<std::string> SendTcpSession(uint16_t port, const std::vector<std::string>& requests,
                                        bool multiline_response = false) {
  std::vector<std::string> responses;
  int sock = socket(AF_INET, SOCK_STREAM, 0);
  if (sock < 0) {
    return responses;
  }
  struct sockaddr_in addr {};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
  if (connect(sock, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
    close(sock);
    return responses;
  }

  for (const auto& request : requests) {
    const std::string msg = request + "\r\n";
    if (send(sock, msg.c_str(), msg.length(), 0) < 0) {
      break;
    }
    std::string response;
    char buffer[4096];
    for (int attempt = 0; attempt < 50; ++attempt) {
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
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      }
    }
    responses.push_back(response);
  }
  close(sock);
  return responses;
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

TEST(HttpTcpErrorRenderingTest, BothSurfacesCarryTheErrorContext) {
  const auto error = mygram::utils::MakeError(mygram::utils::ErrorCode::kMySQLConnectionFailed,
                                              "Failed to connect to MySQL", "db.internal:3306");

  const std::string tcp_frame = ResponseFormatter::FormatError(error);
  EXPECT_NE(tcp_frame.find("db.internal:3306"), std::string::npos) << tcp_frame;

  httplib::Response res;
  HttpServer::SendErrorForTesting(res, 503, error);
  const auto body = json::parse(res.body);
  EXPECT_EQ(body["error_code"], static_cast<int>(mygram::utils::ErrorCode::kMySQLConnectionFailed));
  EXPECT_NE(body["error"].get<std::string>().find("db.internal:3306"), std::string::npos)
      << "HTTP dropped the identifying detail the TCP frame carries: " << res.body;
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

// ===========================================================================
// The divergence inventory in spec/http-routes.md, held to the code
// ===========================================================================
//
// That file's "Divergences from the TCP surface" section is a list of places
// the two surfaces answer the same question differently. Each case below names
// the row it covers. A row that describes a deliberate difference is pinned as
// a difference, so closing it by accident fails here; a row whose difference is
// only in wording or framing is asserted as agreement on the part that matters.

// Rows 1 and 2 of that table record the two read-only administrative reports as
// gated by api.admin_token on TCP and by nothing at all on HTTP. That is no
// longer what the code does: both routes carry requires_admin_token in the
// route descriptor table and are refused before the handler runs. The two
// surfaces now agree that these are administrative, so this pins the agreement
// and those two rows are stale.
TEST_F(HttpTcpConsistencyTest, AdministrativeReportsAreTokenGatedOnBothSurfaces) {
  httplib::Client client("127.0.0.1", http_port_);
  httplib::Headers headers{{"Authorization", "Bearer maintenance-secret"}};

  struct AdministrativeReport {
    const char* tcp_command;
    const char* http_path;
  };
  const std::array<AdministrativeReport, 2> reports = {{
      {"CONFIG SHOW", "/config"},
      {"REPLICATION STATUS", "/replication/status"},
  }};

  for (const auto& report : reports) {
    SCOPED_TRACE(report.tcp_command);

    const auto unauthenticated = SendTcpRequest(tcp_port_, report.tcp_command);
    EXPECT_EQ(ParseTcpErrorCode(unauthenticated), static_cast<int>(mygram::utils::ErrorCode::kPermissionDenied))
        << unauthenticated;
    auto http_unauthorized = client.Get(report.http_path);
    ASSERT_TRUE(http_unauthorized);
    EXPECT_EQ(http_unauthorized->status, 401) << http_unauthorized->body;
    EXPECT_EQ(json::parse(http_unauthorized->body)["error_code"],
              static_cast<int>(mygram::utils::ErrorCode::kPermissionDenied))
        << http_unauthorized->body;
    EXPECT_EQ(http_unauthorized->get_header_value("WWW-Authenticate"), "Bearer");

    const auto authenticated = SendTcpSession(tcp_port_, {"AUTH maintenance-secret", report.tcp_command});
    ASSERT_EQ(authenticated.size(), 2U);
    EXPECT_EQ(authenticated[0].rfind("OK", 0), 0U) << authenticated[0];
    EXPECT_EQ(ParseTcpErrorCode(authenticated[1]), -1) << authenticated[1];
    // With credentials the request reaches the handler; what the handler then
    // reports about an unconfigured subsystem is not this test's subject.
    auto http_authorized = client.Get(report.http_path, headers);
    ASSERT_TRUE(http_authorized);
    EXPECT_NE(http_authorized->status, 401) << http_authorized->body;
    const auto authorized_body = json::parse(http_authorized->body);
    if (authorized_body.contains("error_code")) {
      EXPECT_NE(authorized_body["error_code"].get<int>(), static_cast<int>(mygram::utils::ErrorCode::kPermissionDenied))
          << http_authorized->body;
    }
  }
}

// Row 3: TCP authentication is a connection-scoped AUTH command; HTTP carries a
// bearer token per request and keeps no session.
TEST_F(HttpTcpConsistencyTest, TcpAuthenticationIsPerConnectionAndHttpIsPerRequest) {
  // One connection: the AUTH survives to the next command on it.
  const auto same_connection = SendTcpSession(tcp_port_, {"AUTH maintenance-secret", "OPTIMIZE"});
  ASSERT_EQ(same_connection.size(), 2U);
  EXPECT_EQ(same_connection[0].rfind("OK", 0), 0U) << same_connection[0];
  EXPECT_EQ(ParseTcpErrorCode(same_connection[1]), -1) << same_connection[1];

  // A fresh connection starts unauthenticated.
  const auto new_connection = SendTcpRequest(tcp_port_, "OPTIMIZE");
  EXPECT_EQ(ParseTcpErrorCode(new_connection), static_cast<int>(mygram::utils::ErrorCode::kPermissionDenied))
      << new_connection;

  // HTTP has no equivalent of the AUTH command, and a request that omits the
  // header is refused even right after one that carried it.
  httplib::Client client("127.0.0.1", http_port_);
  httplib::Headers headers{{"Authorization", "Bearer maintenance-secret"}};
  auto with_header = client.Post("/optimize", headers, "{}", "application/json");
  ASSERT_TRUE(with_header);
  EXPECT_EQ(with_header->status, 200) << with_header->body;
  auto without_header = client.Post("/optimize", "{}", "application/json");
  ASSERT_TRUE(without_header);
  EXPECT_EQ(without_header->status, 401) << without_header->body;
}

// Row 4 records /optimize as checking Content-Type before the token, so that a
// credential-free request with the wrong content type is answered 415. The
// credential check now runs in the shared route wrapper, ahead of every
// handler, so the answer is 401 whatever the content type — the row is stale
// and the two surfaces now agree that missing credentials come first.
TEST_F(HttpTcpConsistencyTest, HttpOptimizeChecksTheTokenBeforeTheContentType) {
  httplib::Client client("127.0.0.1", http_port_);

  for (const auto* content_type : {"text/plain", "application/json"}) {
    SCOPED_TRACE(content_type);
    auto no_token = client.Post("/optimize", "{}", content_type);
    ASSERT_TRUE(no_token);
    EXPECT_EQ(no_token->status, 401) << no_token->body;
    EXPECT_EQ(json::parse(no_token->body)["error_code"], static_cast<int>(mygram::utils::ErrorCode::kPermissionDenied))
        << no_token->body;
  }

  // With credentials, the content-type check is the next one to apply.
  httplib::Headers headers{{"Authorization", "Bearer maintenance-secret"}};
  auto wrong_type = client.Post("/optimize", headers, "{}", "text/plain");
  ASSERT_TRUE(wrong_type);
  EXPECT_EQ(wrong_type->status, 415) << wrong_type->body;
  EXPECT_EQ(json::parse(wrong_type->body)["error_code"],
            static_cast<int>(mygram::utils::ErrorCode::kNetworkInvalidRequest))
      << wrong_type->body;
}

// Row 5: HTTP applies a table-name whitelist that TCP does not have. Neither
// surface finds the table; they disagree on which fault it is.
TEST_F(HttpTcpConsistencyTest, HttpAppliesATableNameWhitelistTcpDoesNotHave) {
  httplib::Client client("127.0.0.1", http_port_);

  const auto tcp_response = SendTcpRequest(tcp_port_, R"(SEARCH "app.art icles" machine)");
  EXPECT_EQ(ParseTcpErrorCode(tcp_response), static_cast<int>(mygram::utils::ErrorCode::kTableNotFound))
      << tcp_response;

  auto http_response = client.Post("/tables/app.art%20icles/search", R"({"q":"machine"})", "application/json");
  ASSERT_TRUE(http_response);
  EXPECT_EQ(http_response->status, 400) << http_response->body;
  EXPECT_EQ(json::parse(http_response->body)["error_code"],
            static_cast<int>(mygram::utils::ErrorCode::kQueryInvalidToken))
      << http_response->body;
}

// Row 8: FILTER clauses are a list on TCP, so one column can carry more than
// one condition; the HTTP `filters` object cannot express that at all.
TEST_F(HttpTcpConsistencyTest, RepeatedConditionsOnOneColumnAreExpressibleOnlyOnTcp) {
  // Two mutually exclusive conditions on the same column select nothing.
  const auto tcp_response =
      SendTcpRequest(tcp_port_, "SEARCH app.articles machine FILTER category = ai FILTER category = industry");
  ASSERT_EQ(tcp_response.rfind("OK RESULTS", 0), 0U) << tcp_response;
  EXPECT_EQ(ParseTcpCount(tcp_response, "RESULTS"), 0U) << tcp_response;

  // The JSON object keeps only the last value for a repeated key, so the
  // request that reaches the handler carries one condition and matches.
  httplib::Client client("127.0.0.1", http_port_);
  auto http_response =
      client.Post("/tables/app.articles/search", R"({"q":"machine","filters":{"category":"ai","category":"industry"}})",
                  "application/json");
  ASSERT_TRUE(http_response);
  ASSERT_EQ(http_response->status, 200) << http_response->body;
  EXPECT_EQ(json::parse(http_response->body)["count"].get<size_t>(), 1U) << http_response->body;
}

// Row 9: TCP refuses a filter value that begins with an operator character
// because the clause grammar cannot tell it from a misplaced operator. JSON has
// no such ambiguity and the value is taken as written.
TEST_F(HttpTcpConsistencyTest, FilterValueStartingWithAnOperatorIsRefusedOnlyOnTcp) {
  const auto tcp_response = SendTcpRequest(tcp_port_, "SEARCH app.articles machine FILTER category = >ai");
  EXPECT_EQ(ParseTcpErrorCode(tcp_response), static_cast<int>(mygram::utils::ErrorCode::kQueryInvalidFilter))
      << tcp_response;

  httplib::Client client("127.0.0.1", http_port_);
  json request;
  request["q"] = "machine";
  request["filters"]["category"] = ">ai";
  auto http_response = client.Post("/tables/app.articles/search", request.dump(), "application/json");
  ASSERT_TRUE(http_response);
  ASSERT_EQ(http_response->status, 200) << http_response->body;
  // Accepted and evaluated: no document has that literal category value.
  EXPECT_EQ(json::parse(http_response->body)["count"].get<size_t>(), 0U) << http_response->body;
}

// Row 10: JSON filter values that are not strings are coerced, and a boolean
// becomes "1" or "0" — which is the same condition the TCP clause expresses
// with those characters written out.
TEST_F(HttpTcpConsistencyTest, NonStringFilterValuesAreCoercedToTheTcpSpelling) {
  httplib::Client client("127.0.0.1", http_port_);

  // A column whose stored value is the string the coercion produces.
  const auto tcp_response = SendTcpRequest(tcp_port_, "SEARCH app.articles machine FILTER flag = 1");
  ASSERT_EQ(tcp_response.rfind("OK RESULTS", 0), 0U) << tcp_response;

  json boolean_request;
  boolean_request["q"] = "machine";
  boolean_request["filters"]["flag"] = true;
  auto boolean_response = client.Post("/tables/app.articles/search", boolean_request.dump(), "application/json");
  ASSERT_TRUE(boolean_response);
  ASSERT_EQ(boolean_response->status, 200) << boolean_response->body;
  EXPECT_EQ(json::parse(boolean_response->body)["count"].get<size_t>(), ParseTcpCount(tcp_response, "RESULTS"))
      << boolean_response->body;

  // An integer coerces through std::to_string, which is the TCP spelling too.
  const auto tcp_integer = SendTcpRequest(tcp_port_, "SEARCH app.articles machine FILTER views = 120");
  ASSERT_EQ(tcp_integer.rfind("OK RESULTS", 0), 0U) << tcp_integer;
  json integer_request;
  integer_request["q"] = "machine";
  integer_request["filters"]["views"] = 120;
  auto integer_response = client.Post("/tables/app.articles/search", integer_request.dump(), "application/json");
  ASSERT_TRUE(integer_response);
  ASSERT_EQ(integer_response->status, 200) << integer_response->body;
  EXPECT_EQ(json::parse(integer_response->body)["count"].get<size_t>(), ParseTcpCount(tcp_integer, "RESULTS"))
      << integer_response->body;
}

// Row 12: `mode:"boolean"` assigns the expression straight through, so the
// checks the TCP parser applies to a search expression do not run.
TEST_F(HttpTcpConsistencyTest, BooleanExpressionValidationRunsOnlyOnTcp) {
  const std::string unclosed = "machine AND (learning";

  const auto tcp_response = SendTcpRequest(tcp_port_, "SEARCH app.articles " + unclosed);
  EXPECT_NE(ParseTcpErrorCode(tcp_response), -1) << "the TCP parser accepted an unclosed parenthesis: " << tcp_response;

  httplib::Client client("127.0.0.1", http_port_);
  json request;
  request["q"] = unclosed;
  request["mode"] = "boolean";
  auto http_response = client.Post("/tables/app.articles/search", request.dump(), "application/json");
  ASSERT_TRUE(http_response);
  // The expression reaches the pipeline, which reports its own AST failure
  // rather than the parser's clause-level one.
  EXPECT_EQ(http_response->status, 400) << http_response->body;
  EXPECT_EQ(json::parse(http_response->body)["error_code"],
            static_cast<int>(mygram::utils::ErrorCode::kQueryExpressionParseError))
      << http_response->body;
}

// Row 14: COUNT rejects the clauses it cannot honour on both surfaces, and both
// call it a syntax error. The rejected sets are different: TCP rejects any
// clause outside AND/NOT/FILTER, HTTP rejects exactly five named fields.
TEST_F(HttpTcpConsistencyTest, CountRejectsPaginationOnBothSurfacesWithTheSameCode) {
  httplib::Client client("127.0.0.1", http_port_);

  const auto tcp_limit = SendTcpRequest(tcp_port_, "COUNT app.articles machine LIMIT 5");
  EXPECT_EQ(ParseTcpErrorCode(tcp_limit), static_cast<int>(mygram::utils::ErrorCode::kQuerySyntaxError)) << tcp_limit;

  for (const auto* body : {R"({"q":"machine","limit":5})", R"({"q":"machine","offset":5})",
                           R"({"q":"machine","sort":{"order":"ASC"}})", R"({"q":"machine","fuzzy":1})"}) {
    SCOPED_TRACE(body);
    auto response = client.Post("/tables/app.articles/count", body, "application/json");
    ASSERT_TRUE(response);
    EXPECT_EQ(response->status, 400) << response->body;
    EXPECT_EQ(json::parse(response->body)["error_code"], static_cast<int>(mygram::utils::ErrorCode::kQuerySyntaxError))
        << response->body;
  }
}

// Row 15: an unrecognised clause is a hard error on TCP; an unrecognised JSON
// body field is ignored on every route except /optimize. The TCP side of this
// is the FACET clause loop: SEARCH has no clause allowlist and absorbs trailing
// tokens into its search text instead, which is asserted here so the two
// behaviours are not confused for each other.
TEST_F(HttpTcpConsistencyTest, UnknownInputIsRejectedOnTcpAndIgnoredOnHttp) {
  httplib::Client client("127.0.0.1", http_port_);

  // The unknown-clause branch is reached once the search text has been
  // consumed, so the stray token has to sit after a recognised clause.
  const auto tcp_facet = SendTcpRequest(tcp_port_, "FACET app.articles category learning LIMIT 1 BOGUSCLAUSE 3");
  EXPECT_EQ(ParseTcpErrorCode(tcp_facet), static_cast<int>(mygram::utils::ErrorCode::kQuerySyntaxError)) << tcp_facet;

  auto http_facet =
      client.Post("/tables/app.articles/facet", R"({"column":"category","bogus_field":3})", "application/json");
  ASSERT_TRUE(http_facet);
  EXPECT_EQ(http_facet->status, 200) << http_facet->body;

  // SEARCH takes the trailing tokens as more search text rather than refusing
  // them, so it is not a case of this rule on either surface.
  const auto tcp_search = SendTcpRequest(tcp_port_, "SEARCH app.articles machine BOGUSCLAUSE 3");
  EXPECT_EQ(ParseTcpErrorCode(tcp_search), -1) << tcp_search;
  EXPECT_EQ(ParseTcpCount(tcp_search, "RESULTS"), 0U) << tcp_search;

  auto http_search =
      client.Post("/tables/app.articles/search", R"({"q":"machine","bogus_field":3})", "application/json");
  ASSERT_TRUE(http_search);
  ASSERT_EQ(http_search->status, 200) << http_search->body;
  EXPECT_EQ(json::parse(http_search->body)["count"].get<size_t>(),
            ParseTcpCount(SendTcpRequest(tcp_port_, "SEARCH app.articles machine"), "RESULTS"))
      << http_search->body;

  // /optimize is the exception: it enforces an allowlist.
  httplib::Headers headers{{"Authorization", "Bearer maintenance-secret"}};
  auto rejected = client.Post("/optimize", headers, R"({"bogus_field":3})", "application/json");
  ASSERT_TRUE(rejected);
  EXPECT_EQ(rejected->status, 400) << rejected->body;
}

// Rows 16, 17 and 18: three validation limits both surfaces enforce, each
// reported under a different error code.
TEST_F(HttpTcpConsistencyTest, SharedValidationLimitsAreReportedUnderDifferentCodes) {
  httplib::Client client("127.0.0.1", http_port_);

  // Row 16: LIMIT above the shared ceiling of 1000.
  const auto tcp_limit = SendTcpRequest(tcp_port_, "SEARCH app.articles machine LIMIT 1001");
  EXPECT_EQ(ParseTcpErrorCode(tcp_limit), static_cast<int>(mygram::utils::ErrorCode::kQuerySyntaxError)) << tcp_limit;
  auto http_limit = client.Post("/tables/app.articles/search", R"({"q":"machine","limit":1001})", "application/json");
  ASSERT_TRUE(http_limit);
  EXPECT_EQ(json::parse(http_limit->body)["error_code"], static_cast<int>(mygram::utils::ErrorCode::kQueryInvalidLimit))
      << http_limit->body;

  // Row 17: more than the shared cap of 64 filter conditions.
  std::string tcp_filters = "SEARCH app.articles machine";
  json many_filters;
  many_filters["q"] = "machine";
  for (int i = 0; i <= query::QueryParser::kMaxTermCount; ++i) {
    const std::string column = "col" + std::to_string(i);
    tcp_filters += " FILTER " + column + " = value";
    many_filters["filters"][column] = "value";
  }
  const auto tcp_filter_cap = SendTcpRequest(tcp_port_, tcp_filters);
  EXPECT_EQ(ParseTcpErrorCode(tcp_filter_cap), static_cast<int>(mygram::utils::ErrorCode::kQuerySyntaxError))
      << tcp_filter_cap;
  auto http_filter_cap = client.Post("/tables/app.articles/search", many_filters.dump(), "application/json");
  ASSERT_TRUE(http_filter_cap);
  EXPECT_EQ(json::parse(http_filter_cap->body)["error_code"],
            static_cast<int>(mygram::utils::ErrorCode::kQueryInvalidFilter))
      << http_filter_cap->body;

  // Row 18: a facet column outside the shared column-name grammar.
  const auto tcp_facet = SendTcpRequest(tcp_port_, R"(FACET app.articles "bad col")");
  EXPECT_EQ(ParseTcpErrorCode(tcp_facet), static_cast<int>(mygram::utils::ErrorCode::kQuerySyntaxError)) << tcp_facet;
  auto http_facet = client.Post("/tables/app.articles/facet", R"({"column":"bad col"})", "application/json");
  ASSERT_TRUE(http_facet);
  EXPECT_EQ(json::parse(http_facet->body)["error_code"], static_cast<int>(mygram::utils::ErrorCode::kQueryInvalidToken))
      << http_facet->body;
}

// Row 19: an over-long highlight tag is a syntax error on both surfaces; only
// the message names a different field. (Only the rejection is compared: a tag
// at the 256-byte cap pushes the assembled TCP command past this fixture's
// api.max_query_length, so an accepted tag is not a shared case.)
TEST_F(HttpTcpConsistencyTest, AnOverLongHighlightTagIsASyntaxErrorOnBothSurfaces) {
  const std::string over(257, 'x');

  const auto tcp_response = SendTcpRequest(tcp_port_, "SEARCH app.articles learning HIGHLIGHT TAG " + over + " </b>");
  EXPECT_EQ(ParseTcpErrorCode(tcp_response), static_cast<int>(mygram::utils::ErrorCode::kQuerySyntaxError))
      << tcp_response;
  EXPECT_NE(tcp_response.find("HIGHLIGHT TAG open tag"), std::string::npos) << tcp_response;

  httplib::Client client("127.0.0.1", http_port_);
  json request;
  request["q"] = "learning";
  request["highlight"] = {{"open_tag", over}, {"close_tag", "</b>"}};
  auto http_response = client.Post("/tables/app.articles/search", request.dump(), "application/json");
  ASSERT_TRUE(http_response);
  const auto body = json::parse(http_response->body);
  EXPECT_EQ(body["error_code"], static_cast<int>(mygram::utils::ErrorCode::kQuerySyntaxError)) << http_response->body;
  EXPECT_NE(body["error"].get<std::string>().find("highlight.open_tag"), std::string::npos) << http_response->body;
}

// Row 21: `q` carrying a control character is refused on HTTP. The TCP framing
// consumes the line terminator before the parser sees it, so there is nothing
// equivalent to compare against.
TEST_F(HttpTcpConsistencyTest, ControlCharactersInSearchTextAreRefusedOnHttp) {
  httplib::Client client("127.0.0.1", http_port_);
  const std::vector<std::string> texts = {
      "machine\nlearning",
      "machine\rlearning",
      std::string("machine\0learning", 16),
  };
  for (const auto& text : texts) {
    SCOPED_TRACE(text);
    json request;
    request["q"] = text;
    auto response = client.Post("/tables/app.articles/search", request.dump(), "application/json");
    ASSERT_TRUE(response);
    EXPECT_EQ(response->status, 400) << response->body;
    EXPECT_EQ(json::parse(response->body)["error_code"], static_cast<int>(mygram::utils::ErrorCode::kQueryInvalidToken))
        << response->body;
  }
}

// Row 27: transport-level rejections come from the embedded HTTP library and
// are not JSON, unlike everything the handlers produce. On TCP every failure is
// an ERROR frame.
TEST_F(HttpTcpConsistencyTest, UnmatchedHttpRoutesAreNotJsonErrorFrames) {
  httplib::Client client("127.0.0.1", http_port_);

  auto unmatched = client.Get("/no/such/route");
  ASSERT_TRUE(unmatched);
  EXPECT_EQ(unmatched->status, 404);
  EXPECT_TRUE(unmatched->body.empty()) << unmatched->body;

  // A trailing slash does not match a fixed path.
  auto trailing_slash = client.Get("/info/");
  ASSERT_TRUE(trailing_slash);
  EXPECT_EQ(trailing_slash->status, 404);

  // An unknown TCP command is still a coded ERROR frame.
  const auto tcp_unknown = SendTcpRequest(tcp_port_, "NOSUCHCOMMAND");
  EXPECT_NE(ParseTcpErrorCode(tcp_unknown), -1) << tcp_unknown;
}

// Row 30: the HTTP path is percent-decoded before routing, so a primary key
// spelled with escapes resolves to the same document the TCP token names.
TEST_F(HttpTcpConsistencyTest, HttpPathIsPercentDecodedBeforeRouting) {
  const auto tcp_response = SendTcpRequest(tcp_port_, "GET app.articles doc_1");
  ASSERT_EQ(tcp_response.rfind("OK DOC doc_1", 0), 0U) << tcp_response;

  httplib::Client client("127.0.0.1", http_port_);
  auto encoded = client.Get("/tables/app%2Earticles/doc%5F1");
  ASSERT_TRUE(encoded);
  ASSERT_EQ(encoded->status, 200) << encoded->body;
  EXPECT_EQ(json::parse(encoded->body)["primary_key"].get<std::string>(), "doc_1");
}

// Row 31: a floating-point filter value is rendered to six fixed decimals in
// the TCP document frame and as a JSON number on HTTP, so the same stored value
// reads differently on the two surfaces.
TEST_F(HttpTcpConsistencyTest, FloatingPointFilterValuesAreRenderedDifferently) {
  storage::FilterMap filters;
  filters["category"] = std::string("misc");
  filters["rating"] = 4.5;
  auto doc_id = table_ctx_.doc_store->AddDocument("doc_float", filters, "floating topic");
  ASSERT_TRUE(doc_id.has_value());
  table_ctx_.index->AddDocument(*doc_id, "floating topic");

  const auto tcp_response = SendTcpRequest(tcp_port_, "GET app.articles doc_float");
  ASSERT_EQ(tcp_response.rfind("OK DOC doc_float", 0), 0U) << tcp_response;
  EXPECT_NE(tcp_response.find("rating=4.500000"), std::string::npos) << tcp_response;

  httplib::Client client("127.0.0.1", http_port_);
  auto http_response = client.Get("/tables/app.articles/doc_float");
  ASSERT_TRUE(http_response);
  ASSERT_EQ(http_response->status, 200) << http_response->body;
  const auto body = json::parse(http_response->body);
  ASSERT_TRUE(body["filters"]["rating"].is_number()) << http_response->body;
  EXPECT_DOUBLE_EQ(body["filters"]["rating"].get<double>(), 4.5) << http_response->body;
  // The JSON serialization does not pad to a fixed precision.
  EXPECT_EQ(http_response->body.find("4.500000"), std::string::npos) << http_response->body;

  // Both surfaces still name the same document.
  EXPECT_EQ(body["primary_key"].get<std::string>(), "doc_float");
}

// Row 32: a TCP SEARCH frame lists primary keys only; the HTTP response carries
// each document's filter values alongside its key. The key sets still agree.
TEST_F(HttpTcpConsistencyTest, HttpResultsCarryFilterValuesTheTcpFrameDoesNot) {
  auto tcp_primary_keys = ParseTcpSearchPrimaryKeys(SendTcpRequest(tcp_port_, "SEARCH app.articles machine"));
  SortPrimaryKeys(tcp_primary_keys);

  httplib::Client client("127.0.0.1", http_port_);
  auto http_response = client.Post("/tables/app.articles/search", R"({"q":"machine"})", "application/json");
  ASSERT_TRUE(http_response);
  ASSERT_EQ(http_response->status, 200) << http_response->body;
  const auto body = json::parse(http_response->body);

  auto http_primary_keys = HttpSearchPrimaryKeys(http_response->body);
  SortPrimaryKeys(http_primary_keys);
  EXPECT_EQ(http_primary_keys, tcp_primary_keys) << http_response->body;

  ASSERT_FALSE(body["results"].empty());
  for (const auto& result : body["results"]) {
    ASSERT_TRUE(result.contains("filters")) << http_response->body;
    EXPECT_TRUE(result["filters"].contains("category")) << http_response->body;
  }
}

// Row 34: DEBUG is a connection-scoped TCP mode with no HTTP equivalent, so a
// debug-enabled search carries a block the JSON response has no field for.
TEST_F(HttpTcpConsistencyTest, DebugOutputExistsOnlyOnTcp) {
  // DEBUG is administrative, so the connection authenticates first.
  const auto session = SendTcpSession(tcp_port_, {"AUTH maintenance-secret", "DEBUG ON", "SEARCH app.articles machine"},
                                      /*multiline_response=*/true);
  ASSERT_EQ(session.size(), 3U);
  EXPECT_EQ(session[0].rfind("OK", 0), 0U) << session[0];
  EXPECT_EQ(session[1].rfind("OK", 0), 0U) << session[1];
  EXPECT_NE(session[2].find("# DEBUG"), std::string::npos) << session[2];

  httplib::Client client("127.0.0.1", http_port_);
  auto http_response = client.Post("/tables/app.articles/search", R"({"q":"machine"})", "application/json");
  ASSERT_TRUE(http_response);
  ASSERT_EQ(http_response->status, 200) << http_response->body;
  const auto body = json::parse(http_response->body);
  EXPECT_FALSE(body.contains("debug")) << http_response->body;
  EXPECT_EQ(body.size(), 4U) << "the search response gained a field: " << http_response->body;
}

// Row 35: the HTTP facet response names the column; the TCP frame does not.
// The values and counts are the same either way.
TEST_F(HttpTcpConsistencyTest, HttpFacetResponseNamesTheColumnTheTcpFrameOmits) {
  const auto tcp_response = SendTcpRequest(tcp_port_, "FACET app.articles category learning");
  ASSERT_EQ(tcp_response.rfind("OK FACET", 0), 0U) << tcp_response;
  EXPECT_EQ(tcp_response.find("category"), std::string::npos)
      << "the TCP facet frame gained a column name: " << tcp_response;

  httplib::Client client("127.0.0.1", http_port_);
  auto http_response =
      client.Post("/tables/app.articles/facet", R"({"column":"category","q":"learning"})", "application/json");
  ASSERT_TRUE(http_response);
  ASSERT_EQ(http_response->status, 200) << http_response->body;
  const auto body = json::parse(http_response->body);
  EXPECT_EQ(body["column"].get<std::string>(), "category") << http_response->body;

  std::vector<std::pair<std::string, uint64_t>> http_facets;
  for (const auto& facet : body["facets"]) {
    http_facets.emplace_back(facet["value"].get<std::string>(), facet["count"].get<uint64_t>());
  }
  EXPECT_EQ(http_facets, ParseTcpFacetValues(tcp_response)) << http_response->body;
}

// Rows 39, 41 and 42: the health routes are accounted for on neither counter on
// HTTP, whatever the outcome, while every dispatched TCP request is counted.
TEST_F(HttpTcpConsistencyTest, HealthRoutesAreCountedOnNeitherHttpCounter) {
  httplib::Client client("127.0.0.1", http_port_);

  const uint64_t before = http_server_->GetStats().GetTotalRequests();
  for (const auto* path : {"/health", "/health/live", "/health/ready"}) {
    auto response = client.Get(path);
    ASSERT_TRUE(response) << path;
  }
  EXPECT_EQ(http_server_->GetStats().GetTotalRequests(), before) << "a health route incremented total_requests";

  // A counted route moves it, so the assertion above is about the health routes
  // and not about the counter being frozen.
  auto counted = client.Get("/info");
  ASSERT_TRUE(counted);
  EXPECT_GT(http_server_->GetStats().GetTotalRequests(), before);
}

// Row 47: a request that is faulty in two ways at once reports whichever fault
// its surface checks first — the highlight storage check runs before the page
// is ordered on TCP and after it on HTTP.
TEST_F(HttpTcpConsistencyTest, TheSurfacesReportDifferentFaultsForADoublyInvalidRequest) {
  // Neither highlighting nor _score ordering is available.
  table_ctx_.doc_store->SetStoreTexts(false);
  ASSERT_FALSE(config_->bm25.enable);

  const auto tcp_response =
      SendTcpRequest(tcp_port_, "SEARCH app.articles machine HIGHLIGHT TAG <b> </b> SORT _score DESC");
  EXPECT_EQ(ParseTcpErrorCode(tcp_response), static_cast<int>(mygram::utils::ErrorCode::kNotImplemented))
      << tcp_response;

  httplib::Client client("127.0.0.1", http_port_);
  json request;
  request["q"] = "machine";
  request["highlight"] = {{"open_tag", "<b>"}, {"close_tag", "</b>"}};
  request["sort"] = {{"column", "_score"}, {"order", "DESC"}};
  auto http_response = client.Post("/tables/app.articles/search", request.dump(), "application/json");
  ASSERT_TRUE(http_response);
  EXPECT_EQ(json::parse(http_response->body)["error_code"],
            static_cast<int>(mygram::utils::ErrorCode::kQueryInvalidSort))
      << http_response->body;
}

// A table set spread over two databases, so the bare-name rule (row 6) can be
// reached: it only applies when a bare name cannot be resolved unambiguously.
class HttpTcpMultiDatabaseTest : public ::testing::Test {
 protected:
  void SetUp() override {
    mygramdb::test::SkipIfSocketCreationBlocked();

    for (auto* entry : {&first_, &second_}) {
      auto index = std::make_unique<index::Index>(2);
      auto doc_store = std::make_unique<storage::DocumentStore>();
      auto doc_id = doc_store->AddDocument("doc_1", {}, "shared topic");
      EXPECT_TRUE(doc_id.has_value());
      index->AddDocument(*doc_id, "shared topic");
      entry->index = std::move(index);
      entry->doc_store = std::move(doc_store);
    }

    first_.name = "articles";
    first_.config.name = "articles";
    first_.config.database = "app";
    first_.config.ngram_size = 2;
    first_.config.primary_key = "id";
    second_.name = "articles";
    second_.config.name = "articles";
    second_.config.database = "archive";
    second_.config.ngram_size = 2;
    second_.config.primary_key = "id";

    table_contexts_["app.articles"] = &first_;
    table_contexts_["archive.articles"] = &second_;

    config_ = std::make_unique<config::Config>();
    config_->api.default_limit = 10;
    config_->tables.push_back(first_.config);
    config_->tables.push_back(second_.config);

    ASSERT_TRUE(StartSurfacePair(table_contexts_, *config_, tcp_server_, http_server_, tcp_port_, http_port_));
  }

  void TearDown() override { StopSurfacePair(tcp_server_.get(), http_server_.get()); }

  TableContext first_;
  TableContext second_;
  std::unordered_map<std::string, TableContext*> table_contexts_;
  std::unique_ptr<config::Config> config_;
  std::unique_ptr<TcpServer> tcp_server_;
  std::unique_ptr<HttpServer> http_server_;
  uint16_t tcp_port_ = 0;
  uint16_t http_port_ = 0;
};

// Row 6: both surfaces refuse a bare table name under a multi-database
// configuration and both say the same thing; they file it under different
// codes.
TEST_F(HttpTcpMultiDatabaseTest, ABareTableNameIsRefusedUnderDifferentCodes) {
  const auto tcp_response = SendTcpRequest(tcp_port_, "SEARCH articles shared");
  EXPECT_EQ(ParseTcpErrorCode(tcp_response), static_cast<int>(mygram::utils::ErrorCode::kTableNotFound))
      << tcp_response;
  EXPECT_NE(tcp_response.find("Bare table names are not supported"), std::string::npos) << tcp_response;

  httplib::Client client("127.0.0.1", http_port_);
  auto http_response = client.Post("/tables/articles/search", R"({"q":"shared"})", "application/json");
  ASSERT_TRUE(http_response);
  EXPECT_EQ(http_response->status, 400) << http_response->body;
  const auto body = json::parse(http_response->body);
  EXPECT_EQ(body["error_code"], static_cast<int>(mygram::utils::ErrorCode::kQuerySyntaxError)) << http_response->body;
  EXPECT_NE(body["error"].get<std::string>().find("Bare table names are not supported"), std::string::npos)
      << http_response->body;

  // A qualified name resolves on both surfaces.
  const auto tcp_qualified = SendTcpRequest(tcp_port_, "SEARCH app.articles shared");
  EXPECT_EQ(tcp_qualified.rfind("OK RESULTS", 0), 0U) << tcp_qualified;
  auto http_qualified = client.Post("/tables/app.articles/search", R"({"q":"shared"})", "application/json");
  ASSERT_TRUE(http_qualified);
  EXPECT_EQ(http_qualified->status, 200) << http_qualified->body;
}

}  // namespace server
}  // namespace mygramdb
