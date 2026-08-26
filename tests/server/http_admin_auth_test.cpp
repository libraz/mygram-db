/**
 * @file http_admin_auth_test.cpp
 * @brief Bearer-token enforcement and replication-detail exposure on the HTTP surface
 *
 * The HTTP routes that mirror an administrative TCP command must demand the
 * configured `api.admin_token` before they read any replication or
 * configuration state, and the routes that stay open must not serve the same
 * state through a different name. These tests pin both halves, including the
 * near-miss token shapes (prefix, superstring, empty) that a naive comparison
 * would accept.
 */

#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <httplib.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <memory>
#include <nlohmann/json.hpp>
#include <string>
#include <unordered_map>

#include "config/config.h"
#include "mysql/binlog_reader_interface.h"
#include "server/http_server.h"
#include "server/server_types.h"
#include "support/network_test_utils.h"
#include "utils/error.h"

using json = nlohmann::json;

namespace mygramdb::server {
namespace {

constexpr const char* kAdminToken = "http-admin-secret";

/// Replication error text of the shape MySQL produces for a credential
/// mismatch. It names the replication account and the address the server sees.
constexpr const char* kCredentialErrorText = "Access denied for user 'mygram_repl'@'10.1.2.3' (using password: YES)";

class StubBinlogReader final : public mysql::IBinlogReader {
 public:
  mygram::utils::Expected<void, mygram::utils::Error> Start() override { return {}; }
  void Stop() override {}
  bool IsRunning() const override { return running; }
  std::string GetCurrentGTID() const override { return current_gtid; }
  void SetCurrentGTID(const std::string& gtid) override { current_gtid = gtid; }
  std::string GetLastError() const override { return last_error; }
  uint64_t GetProcessedEvents() const override { return processed_events; }
  size_t GetQueueSize() const override { return queue_size; }
  uint64_t GetCRCErrors() const override { return crc_errors; }
  bool HasSchemaIncompatibleError() const override { return schema_incompatible; }
  mygram::utils::ErrorCode GetLastErrorCode() const override { return last_error_code; }
  int64_t GetLastAppliedUnixTime() const override { return 1722840000; }
  int64_t GetSecondsSinceLastApplied() const override { return 42; }

  bool running = false;
  std::string current_gtid = "8e3f4b0a-0000-0000-0000-000000000001:1-9876";
  std::string last_error = kCredentialErrorText;
  uint64_t processed_events = 4321;
  size_t queue_size = 17;
  uint64_t crc_errors = 5;
  bool schema_incompatible = true;
  mygram::utils::ErrorCode last_error_code = mygram::utils::ErrorCode::kMySQLAuthFailed;
};

httplib::Headers AdminHeaders() {
  return httplib::Headers{{"Authorization", std::string("Bearer ") + kAdminToken}};
}

/**
 * @brief Issue a GET over a raw socket and return the whole response.
 *
 * cpp-httplib's client drops a header whose value is empty after trimming, so
 * an `Authorization: Bearer ` line — a caller presenting an empty token — can
 * only be put on the wire by writing the request bytes directly.
 */
std::string RawGet(uint16_t port, const std::string& path, const std::string& authorization) {
  const int sock = ::socket(AF_INET, SOCK_STREAM, 0);
  if (sock < 0) {
    return {};
  }
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_port = htons(port);
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  if (::connect(sock, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0) {
    ::close(sock);
    return {};
  }

  const std::string request = "GET " + path + " HTTP/1.1\r\nHost: 127.0.0.1\r\nAuthorization: " + authorization +
                              "\r\nConnection: close\r\n\r\n";
  if (::send(sock, request.data(), request.size(), 0) < 0) {
    ::close(sock);
    return {};
  }

  std::string response;
  std::array<char, 4096> buffer{};
  while (true) {
    const ssize_t received = ::recv(sock, buffer.data(), buffer.size(), 0);
    if (received <= 0) {
      break;
    }
    response.append(buffer.data(), static_cast<size_t>(received));
  }
  ::close(sock);
  return response;
}

}  // namespace

class HttpAdminAuthTest : public ::testing::Test {
 protected:
  void SetUp() override {
    port_ = mygramdb::testing::FindAvailableLoopbackPort();
    ASSERT_GT(port_, 0);

    config_ = std::make_unique<config::Config>();
    config_->api.admin_token = kAdminToken;
    config_->api.http.enable = true;
    config_->api.http.bind = "127.0.0.1";
    config_->api.http.port = port_;
    config_->mysql.host = "10.1.2.3";
    config_->mysql.user = "mygram_repl";
    config_->mysql.password = "repl-password";
    config_->mysql.database = "app";
    config_->network.allow_cidrs = {"127.0.0.1/32"};

    HttpServerConfig http_config;
    http_config.bind = "127.0.0.1";
    http_config.port = port_;
    http_config.allow_cidrs = {"127.0.0.1/32"};

    server_ = std::make_unique<HttpServer>(http_config, table_contexts_, config_.get(), &binlog_reader_);
    auto started = server_->Start();
    if (!started) {
      GTEST_SKIP() << "HTTP server bind unavailable: " << started.error().to_string();
    }
  }

  void TearDown() override {
    if (server_ && server_->IsRunning()) {
      server_->Stop();
    }
  }

  httplib::Client Client() const {
    httplib::Client client("127.0.0.1", port_);
    client.set_connection_timeout(5);
    client.set_read_timeout(5);
    return client;
  }

  StubBinlogReader binlog_reader_;
  std::unordered_map<std::string, TableContext*> table_contexts_;
  std::unique_ptr<config::Config> config_;
  std::unique_ptr<HttpServer> server_;
  uint16_t port_ = 0;
};

TEST_F(HttpAdminAuthTest, AdministrativeRoutesRejectRequestsWithoutABearerToken) {
  auto client = Client();

  for (const auto* path : {"/config", "/replication/status"}) {
    auto response = client.Get(path);
    ASSERT_TRUE(response) << path;
    EXPECT_EQ(response->status, 401) << path << " body: " << response->body;
    EXPECT_EQ(response->get_header_value("WWW-Authenticate"), "Bearer") << path;
    const auto body = json::parse(response->body);
    EXPECT_EQ(body["error_code"], static_cast<int>(mygram::utils::ErrorCode::kPermissionDenied)) << response->body;
    EXPECT_EQ(response->body.find(binlog_reader_.current_gtid), std::string::npos) << response->body;
  }
}

TEST_F(HttpAdminAuthTest, AdministrativeRoutesRejectNearMissTokens) {
  const std::string valid(kAdminToken);
  const std::array<std::string, 3> rejected = {
      valid.substr(0, valid.size() - 1),  // prefix of the configured token
      valid + "x",                        // superstring of the configured token
      "an-unrelated-token",
  };

  auto client = Client();
  for (const auto& token : rejected) {
    const httplib::Headers headers{{"Authorization", "Bearer " + token}};
    for (const auto* path : {"/config", "/replication/status"}) {
      auto response = client.Get(path, headers);
      ASSERT_TRUE(response) << path << " token=" << token;
      EXPECT_EQ(response->status, 401) << path << " token=" << token << " body: " << response->body;
      const auto body = json::parse(response->body);
      EXPECT_EQ(body["error_code"], static_cast<int>(mygram::utils::ErrorCode::kPermissionDenied)) << response->body;
    }
  }

  // An empty token behind a well-formed prefix, and the bare scheme with no
  // token at all.
  for (const auto* authorization : {"Bearer ", "Bearer"}) {
    for (const auto* path : {"/config", "/replication/status"}) {
      const std::string response = RawGet(port_, path, authorization);
      ASSERT_FALSE(response.empty()) << path << " authorization=[" << authorization << "]";
      EXPECT_EQ(response.rfind("HTTP/1.1 401", 0), 0U)
          << path << " authorization=[" << authorization << "] " << response;
      EXPECT_NE(response.find("\"error_code\":7"), std::string::npos) << response;
    }
  }

  // A rejected request never reached a handler, so it must leave no trace in
  // the command counters an operator would use to spot administrative traffic.
  EXPECT_EQ(server_->GetStats().GetCommandCount(query::QueryType::CONFIG_SHOW), 0U);
  EXPECT_EQ(server_->GetStats().GetCommandCount(query::QueryType::REPLICATION_STATUS), 0U);
}

TEST_F(HttpAdminAuthTest, TheConfigRouteServesItsSummaryWithAValidBearerToken) {
  auto client = Client();

  auto config_response = client.Get("/config", AdminHeaders());
  ASSERT_TRUE(config_response);
  EXPECT_EQ(config_response->status, 200) << config_response->body;
  EXPECT_EQ(server_->GetStats().GetCommandCount(query::QueryType::CONFIG_SHOW), 1U);
}

// The remaining cases read replication state, which only exists in a build
// that links the MySQL client.
#ifdef USE_MYSQL

TEST_F(HttpAdminAuthTest, TheReplicationRouteServesItsStateWithAValidBearerToken) {
  auto client = Client();
  const auto headers = AdminHeaders();

  auto replication_response = client.Get("/replication/status", headers);
  ASSERT_TRUE(replication_response);
  EXPECT_EQ(replication_response->status, 200) << replication_response->body;
  const auto body = json::parse(replication_response->body);
  EXPECT_EQ(body["current_gtid"], binlog_reader_.current_gtid);
  EXPECT_EQ(body["processed_events"], binlog_reader_.processed_events);

  EXPECT_EQ(server_->GetStats().GetCommandCount(query::QueryType::REPLICATION_STATUS), 1U);
}

TEST_F(HttpAdminAuthTest, OpenRoutesWithholdTheReplicationDetailTheAdminRoutesProtect) {
  auto client = Client();

  auto detail = client.Get("/health/detail");
  ASSERT_TRUE(detail);
  ASSERT_EQ(detail->status, 200) << detail->body;
  const auto detail_body = json::parse(detail->body);
  ASSERT_TRUE(detail_body["components"].contains("binlog")) << detail->body;
  const auto& binlog = detail_body["components"]["binlog"];
  EXPECT_FALSE(binlog.contains("current_gtid")) << detail->body;
  EXPECT_FALSE(binlog.contains("processed_events")) << detail->body;
  EXPECT_FALSE(binlog.contains("queue_size")) << detail->body;
  EXPECT_FALSE(binlog.contains("crc_errors")) << detail->body;
  EXPECT_FALSE(binlog.contains("schema_incompatible")) << detail->body;

  auto ready = client.Get("/health/ready");
  ASSERT_TRUE(ready);
  const auto ready_body = json::parse(ready->body);
  EXPECT_FALSE(ready_body.contains("replication_crc_errors")) << ready->body;
  EXPECT_FALSE(ready_body.contains("replication_schema_incompatible")) << ready->body;
}

TEST_F(HttpAdminAuthTest, OpenRoutesServeTheReplicationDetailToACredentialedCaller) {
  auto client = Client();
  const auto headers = AdminHeaders();

  auto detail = client.Get("/health/detail", headers);
  ASSERT_TRUE(detail);
  ASSERT_EQ(detail->status, 200) << detail->body;
  const auto detail_body = json::parse(detail->body);
  const auto& binlog = detail_body["components"]["binlog"];
  EXPECT_EQ(binlog["crc_errors"], binlog_reader_.crc_errors) << detail->body;
  EXPECT_EQ(binlog["schema_incompatible"], binlog_reader_.schema_incompatible) << detail->body;

  auto ready = client.Get("/health/ready", headers);
  ASSERT_TRUE(ready);
  const auto ready_body = json::parse(ready->body);
  EXPECT_EQ(ready_body["replication_crc_errors"], binlog_reader_.crc_errors) << ready->body;
}

TEST_F(HttpAdminAuthTest, OpenRoutesNameTheReplicationFaultByCodeInsteadOfMySQLText) {
  auto client = Client();

  auto ready = client.Get("/health/ready");
  ASSERT_TRUE(ready);
  const auto ready_body = json::parse(ready->body);
  ASSERT_TRUE(ready_body.contains("replication_last_error_code")) << ready->body;
  EXPECT_EQ(ready_body["replication_last_error_code"], static_cast<int>(mygram::utils::ErrorCode::kMySQLAuthFailed));
  EXPECT_EQ(ready->body.find("'mygram_repl'@'10.1.2.3'"), std::string::npos) << ready->body;
  EXPECT_EQ(ready->body.find(config_->mysql.user), std::string::npos) << ready->body;
  EXPECT_EQ(ready->body.find(config_->mysql.host), std::string::npos) << ready->body;
  EXPECT_EQ(ready->body.find(config_->mysql.password), std::string::npos) << ready->body;

  auto detail = client.Get("/health/detail");
  ASSERT_TRUE(detail);
  EXPECT_EQ(detail->body.find("'mygram_repl'@'10.1.2.3'"), std::string::npos) << detail->body;
  EXPECT_EQ(detail->body.find(config_->mysql.user), std::string::npos) << detail->body;
}

TEST_F(HttpAdminAuthTest, TheCredentialedReplicationRouteStillCarriesTheVerbatimMySQLError) {
  auto client = Client();
  auto response = client.Get("/replication/status", AdminHeaders());
  ASSERT_TRUE(response);
  ASSERT_EQ(response->status, 200) << response->body;
  EXPECT_EQ(json::parse(response->body)["last_error"], kCredentialErrorText);
}

TEST_F(HttpAdminAuthTest, OpenRoutesKeepTheirReadinessVerdictAndReason) {
  auto client = Client();

  auto ready = client.Get("/health/ready");
  ASSERT_TRUE(ready);
  EXPECT_EQ(ready->status, 503) << ready->body;
  const auto body = json::parse(ready->body);
  EXPECT_EQ(body["status"], "not_ready");
  ASSERT_TRUE(body.contains("reason"));
  EXPECT_EQ(body["reason"].get<std::string>().find(config_->mysql.user), std::string::npos) << ready->body;
}

#endif  // USE_MYSQL

}  // namespace mygramdb::server
