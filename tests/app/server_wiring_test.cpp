/**
 * @file server_wiring_test.cpp
 * @brief Cross-protocol wiring of the components the TCP server owns.
 *
 * The TCP server creates the rate limiter and the thread pool while starting,
 * which is later than the HTTP surface is constructed. These tests drive a full
 * ServerOrchestrator and assert on state only observable when both surfaces end
 * up sharing the very same components.
 */

#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>

#include "app/server_orchestrator.h"
#include "app/signal_manager.h"
#include "config/config.h"
#include "server/http_server.h"
#include "server/rate_limiter.h"
#include "server/tcp_server.h"
#include "support/network_test_utils.h"

namespace {

constexpr const char* kTableKey = "catalog.posts";
constexpr const char* kCountPath = "/tables/catalog.posts/count";
constexpr const char* kCountBody = R"({"q":"wiring"})";

/// Minimal single-table server configuration that needs neither MySQL nor a
/// snapshot, listening on loopback with the loopback ACL configured.
mygramdb::config::Config MakeServerConfig() {
  mygramdb::config::Config config;
  config.replication.enable = false;
  config.replication.auto_initial_snapshot = false;
  config.dump.load_on_startup = false;
  config.network.allow_cidrs = {"127.0.0.1/32"};
  config.api.tcp.bind = "127.0.0.1";
  config.api.tcp.port = mygramdb::testing::FindAvailableLoopbackPort();
  config.api.http.enable = true;
  config.api.http.bind = "127.0.0.1";
  config.api.http.port = mygramdb::testing::FindAvailableLoopbackPort();

  mygramdb::config::TableConfig table;
  table.database = "catalog";
  table.name = "posts";
  table.text_source.column = "body";
  config.tables.push_back(std::move(table));
  return config;
}

/// Send one CRLF-terminated command and return the first chunk of the reply.
/// The assertions below are on server state rather than on the response body,
/// so a single read is enough to know the command reached the dispatcher.
std::string SendTcpCommand(uint16_t port, const std::string& command) {
  const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    return {};
  }
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  ::inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
  if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
    ::close(fd);
    return {};
  }
  timeval timeout{};
  timeout.tv_sec = 5;
  ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));

  const std::string wire = command + "\r\n";
  if (::send(fd, wire.data(), wire.size(), 0) < 0) {
    ::close(fd);
    return {};
  }
  std::array<char, 4096> buffer{};
  const ssize_t received = ::recv(fd, buffer.data(), buffer.size(), 0);
  ::close(fd);
  if (received <= 0) {
    return {};
  }
  return std::string(buffer.data(), static_cast<size_t>(received));
}

/// Read a Prometheus gauge value out of a /metrics body.
/// Returns -1 when the metric is absent so a missing gauge fails loudly.
int64_t MetricValue(const std::string& body, const std::string& metric_name) {
  const std::string needle = "\n" + metric_name + " ";
  const size_t pos = body.find(needle);
  if (pos == std::string::npos) {
    return -1;
  }
  const size_t value_start = pos + needle.size();
  const size_t value_end = body.find('\n', value_start);
  return std::stoll(body.substr(value_start, value_end - value_start));
}

class ServerWiringTest : public ::testing::Test {
 protected:
  void StartOrchestrator(mygramdb::config::Config config) {
    auto signal_manager = mygramdb::app::SignalManager::Create();
    ASSERT_TRUE(signal_manager) << signal_manager.error().to_string();
    signal_manager_ = std::move(*signal_manager);

    config_ = std::make_unique<mygramdb::config::Config>(std::move(config));
    dump_dir_ = std::filesystem::temp_directory_path().string();

    mygramdb::app::ServerOrchestrator::Dependencies deps{
        .config = *config_,
        .signal_manager = *signal_manager_,
        .dump_dir = dump_dir_,
    };
    auto orchestrator = mygramdb::app::ServerOrchestrator::Create(deps);
    ASSERT_TRUE(orchestrator) << orchestrator.error().to_string();
    orchestrator_ = std::move(*orchestrator);
    ASSERT_TRUE(orchestrator_->Initialize());
    ASSERT_TRUE(orchestrator_->Start());
  }

  void TearDown() override {
    if (orchestrator_) {
      EXPECT_TRUE(orchestrator_->Stop());
    }
  }

  int HttpPort() const { return config_->api.http.port; }

  std::unique_ptr<mygramdb::app::SignalManager> signal_manager_;
  std::unique_ptr<mygramdb::config::Config> config_;
  std::string dump_dir_;
  std::unique_ptr<mygramdb::app::ServerOrchestrator> orchestrator_;
};

TEST_F(ServerWiringTest, RateLimitQuotaIsSharedAcrossTcpAndHttp) {
  auto config = MakeServerConfig();
  config.api.rate_limiting.enable = true;
  config.api.rate_limiting.capacity = 100;
  config.api.rate_limiting.refill_rate = 100;
  config.api.rate_limiting.max_clients = 16;
  ASSERT_NO_FATAL_FAILURE(StartOrchestrator(std::move(config)));

  auto* tcp_server = orchestrator_->GetTcpServerForTesting();
  ASSERT_NE(tcp_server, nullptr);
  auto* limiter = tcp_server->GetRateLimiter();
  ASSERT_NE(limiter, nullptr) << "the TCP server never created a rate limiter";

  const std::string tcp_reply = SendTcpCommand(tcp_server->GetPort(), std::string("COUNT ") + kTableKey + " wiring");
  ASSERT_FALSE(tcp_reply.empty()) << "the TCP surface did not answer";

  httplib::Client client("127.0.0.1", HttpPort());
  auto http_reply = client.Post(kCountPath, kCountBody, "application/json");
  ASSERT_TRUE(http_reply) << "the HTTP surface did not answer";

  // One token bucket, one client identity: both requests must have been
  // checked against the limiter the TCP server owns. Two independent limiters
  // give the client twice the configured quota.
  EXPECT_EQ(limiter->GetStats().total_requests, 2U)
      << "the HTTP surface is not drawing from the rate limiter the TCP surface owns";
  EXPECT_EQ(limiter->GetTrackedClientCount(), 1U) << "the two surfaces tracked the same client in separate limiters";
}

TEST_F(ServerWiringTest, MetricsReportLiveThreadPoolState) {
  auto config = MakeServerConfig();
  config.api.tcp.worker_threads = 3;
  config.api.tcp.thread_pool_queue_size = 777;
  ASSERT_NO_FATAL_FAILURE(StartOrchestrator(std::move(config)));

  httplib::Client client("127.0.0.1", HttpPort());
  auto metrics = client.Get("/metrics");
  ASSERT_TRUE(metrics) << "the HTTP surface did not answer";
  ASSERT_EQ(metrics->status, 200);

  EXPECT_EQ(MetricValue(metrics->body, "mygramdb_thread_pool_workers"), 3)
      << "/metrics does not see the thread pool the TCP server owns";
  EXPECT_EQ(MetricValue(metrics->body, "mygramdb_thread_pool_queue_capacity"), 777)
      << "/metrics does not report the configured queue capacity";
  EXPECT_GE(MetricValue(metrics->body, "mygramdb_thread_pool_queue_depth"), 0);
}

TEST_F(ServerWiringTest, RuntimeRateLimitChangeAppliesToHttp) {
  auto config = MakeServerConfig();
  config.api.rate_limiting.enable = true;
  config.api.rate_limiting.capacity = 100;
  config.api.rate_limiting.refill_rate = 100;
  config.api.rate_limiting.max_clients = 16;
  ASSERT_NO_FATAL_FAILURE(StartOrchestrator(std::move(config)));

  auto* tcp_server = orchestrator_->GetTcpServerForTesting();
  ASSERT_NE(tcp_server, nullptr);

  const std::string set_reply =
      SendTcpCommand(tcp_server->GetPort(), "SET api.rate_limiting.capacity = 1, api.rate_limiting.refill_rate = 1");
  ASSERT_NE(set_reply.find("OK"), std::string::npos) << set_reply;

  // A one-token bucket refilling at one token per second: the second request
  // within the same second is rejected, with no sleep and no wall-clock
  // dependency.
  httplib::Client client("127.0.0.1", HttpPort());
  auto first = client.Post(kCountPath, kCountBody, "application/json");
  ASSERT_TRUE(first) << "the HTTP surface did not answer";
  EXPECT_NE(first->status, 429);

  auto second = client.Post(kCountPath, kCountBody, "application/json");
  ASSERT_TRUE(second) << "the HTTP surface did not answer";
  EXPECT_EQ(second->status, 429) << "a runtime SET api.rate_limiting.* did not reach the HTTP surface";
}

}  // namespace
