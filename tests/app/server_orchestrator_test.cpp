/**
 * @file server_orchestrator_test.cpp
 * @brief ServerOrchestrator teardown after a shutdown that timed out.
 *
 * TcpServer::Stop() gives up after its bounded drain while the reactor keeps
 * serving, and ~TcpServer then drains without a deadline. Everything the TCP
 * callbacks reach must outlive that drain.
 */

#include "app/server_orchestrator.h"

#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>

#include "app/signal_manager.h"
#include "config/config.h"
#include "server/operation_coordinator.h"
#include "server/tcp_server.h"
#include "support/network_test_utils.h"

namespace {

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

/**
 * @brief Destroying the orchestrator after Stop() timed out keeps INFO and SET api.* callbacks valid
 *
 * INFO reads the orchestrator's readiness tracker and SET api.default_limit
 * calls into the HTTP server; both are served by the TCP reactor while
 * ~TcpServer waits for the held long operation.
 */
TEST(ServerOrchestratorShutdownTest, DestructionAfterStopTimeoutKeepsTcpCallbackTargetsAlive) {
  auto signal_manager = mygramdb::app::SignalManager::Create();
  ASSERT_TRUE(signal_manager) << signal_manager.error().to_string();
  const auto config = MakeServerConfig();
  const std::string dump_dir =
      (std::filesystem::temp_directory_path() / ("server_orchestrator_" + std::to_string(getpid()))).string();
  std::filesystem::remove_all(dump_dir);
  std::filesystem::create_directories(dump_dir);

  mygramdb::app::ServerOrchestrator::Dependencies deps{
      .config = config,
      .signal_manager = **signal_manager,
      .dump_dir = dump_dir,
  };
  auto orchestrator_result = mygramdb::app::ServerOrchestrator::Create(deps);
  ASSERT_TRUE(orchestrator_result) << orchestrator_result.error().to_string();
  auto orchestrator = std::move(*orchestrator_result);
  ASSERT_TRUE(orchestrator->Initialize());
  ASSERT_TRUE(orchestrator->Start());

  auto* tcp_server = orchestrator->GetTcpServerForTesting();
  ASSERT_NE(tcp_server, nullptr);
  const uint16_t port = tcp_server->GetPort();
  auto held_operation =
      tcp_server->GetOperationCoordinator()->TryAcquire(mygramdb::server::LongOperation::kOptimize, "held by test");
  ASSERT_TRUE(held_operation.has_value());

  EXPECT_FALSE(orchestrator->Stop()) << "Stop() must time out while a long operation is held";

  std::thread destroyer([&orchestrator]() { orchestrator.reset(); });
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  const std::string info_reply = SendTcpCommand(port, "INFO");
  EXPECT_FALSE(info_reply.empty()) << "INFO was not served during the destructor drain";
  const std::string set_reply = SendTcpCommand(port, "SET api.default_limit = 50");
  EXPECT_NE(set_reply.find("OK"), std::string::npos) << set_reply;

  held_operation.reset();
  destroyer.join();
  std::filesystem::remove_all(dump_dir);
}

}  // namespace
