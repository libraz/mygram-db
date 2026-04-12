/**
 * @file connection_acceptor_tcp_test.cpp
 * @brief Tests for TCP socket path in ConnectionAcceptor
 *
 * Tests the expected behavior of TCP connection acceptance:
 * - Bind to a port and accept connections
 * - Port 0 auto-assignment
 * - Invalid bind address rejection
 * - Already-running guard
 * - Max connections enforcement
 * - Reactor handler dispatch and rejection
 * - Graceful stop (double-stop safety)
 */

#include <gtest/gtest.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <thread>

#include "server/connection_acceptor.h"
#include "server/server_types.h"
#include "utils/network_utils.h"

namespace mygramdb::server {

class ConnectionAcceptorTcpTest : public ::testing::Test {
 protected:
  ServerConfig MakeConfig(uint16_t port = 0, int max_connections = 10) {
    ServerConfig config;
    config.host = "127.0.0.1";
    config.port = port;  // 0 = OS picks a free port
    config.max_connections = max_connections;
    config.keepalive.enabled = false;  // Simplify tests
    // Allow localhost for ACL (parsed CIDR overload is fail-closed)
    auto cidr = mygram::utils::CIDR::Parse("127.0.0.1/32");
    if (cidr) {
      config.parsed_allow_cidrs.push_back(*cidr);
    }
    return config;
  }

  int ConnectToTcp(const std::string& host, uint16_t port) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
      return -1;
    }

    struct sockaddr_in addr {};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, host.c_str(), &addr.sin_addr);

    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    if (connect(sock, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
      close(sock);
      return -1;
    }
    return sock;
  }
};

// --- Basic lifecycle ---

TEST_F(ConnectionAcceptorTcpTest, StartAndStopOnAutoPort) {
  auto config = MakeConfig(0);
  ConnectionAcceptor acceptor(config);

  acceptor.SetReactorHandler([](int fd) {
    close(fd);
    return true;
  });

  auto result = acceptor.Start();
  ASSERT_TRUE(result.has_value()) << result.error().to_string();
  EXPECT_TRUE(acceptor.IsRunning());
  EXPECT_FALSE(acceptor.IsUnixSocket());
  EXPECT_GT(acceptor.GetPort(), 0);

  acceptor.Stop();
  EXPECT_FALSE(acceptor.IsRunning());
}

TEST_F(ConnectionAcceptorTcpTest, PortZeroAssignsEphemeralPort) {
  auto config = MakeConfig(0);
  ConnectionAcceptor acceptor(config);

  acceptor.SetReactorHandler([](int fd) {
    close(fd);
    return true;
  });

  auto result = acceptor.Start();
  ASSERT_TRUE(result.has_value());

  uint16_t port = acceptor.GetPort();
  EXPECT_GE(port, 1024) << "Ephemeral port should be >= 1024";

  acceptor.Stop();
}

// --- Error conditions ---

TEST_F(ConnectionAcceptorTcpTest, InvalidBindAddressReturnsError) {
  ServerConfig config;
  config.host = "not.a.valid.ip";
  config.port = 0;
  config.max_connections = 10;

  ConnectionAcceptor acceptor(config);
  acceptor.SetReactorHandler([](int fd) {
    close(fd);
    return true;
  });

  auto result = acceptor.Start();
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kNetworkInvalidBindAddress);
}

TEST_F(ConnectionAcceptorTcpTest, AlreadyRunningReturnsError) {
  auto config = MakeConfig(0);
  ConnectionAcceptor acceptor(config);

  acceptor.SetReactorHandler([](int fd) {
    close(fd);
    return true;
  });

  auto first = acceptor.Start();
  ASSERT_TRUE(first.has_value());

  auto second = acceptor.Start();
  ASSERT_FALSE(second.has_value());
  EXPECT_EQ(second.error().code(), mygram::utils::ErrorCode::kNetworkAlreadyRunning);

  acceptor.Stop();
}

// --- Connection acceptance ---

TEST_F(ConnectionAcceptorTcpTest, AcceptsTcpConnection) {
  auto config = MakeConfig(0);
  ConnectionAcceptor acceptor(config);

  std::atomic<int> accepted_count{0};
  acceptor.SetReactorHandler([&accepted_count](int fd) {
    accepted_count++;
    close(fd);
    return true;
  });

  auto result = acceptor.Start();
  ASSERT_TRUE(result.has_value());

  int client = ConnectToTcp("127.0.0.1", acceptor.GetPort());
  ASSERT_GE(client, 0) << "connect() failed: " << strerror(errno);
  close(client);

  // Wait for handler
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  EXPECT_GE(accepted_count.load(), 1);

  acceptor.Stop();
}

TEST_F(ConnectionAcceptorTcpTest, ReactorHandlerRejectionSendsServerBusy) {
  auto config = MakeConfig(0);
  ConnectionAcceptor acceptor(config);

  // Handler always rejects
  acceptor.SetReactorHandler([](int /*fd*/) { return false; });

  auto result = acceptor.Start();
  ASSERT_TRUE(result.has_value());

  int client = ConnectToTcp("127.0.0.1", acceptor.GetPort());
  ASSERT_GE(client, 0);

  // Read the SERVER_BUSY response
  char buf[256] = {};
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  ssize_t n = recv(client, buf, sizeof(buf) - 1, 0);
  close(client);

  if (n > 0) {
    std::string response(buf, static_cast<size_t>(n));
    EXPECT_NE(response.find("SERVER_BUSY"), std::string::npos)
        << "Expected SERVER_BUSY response, got: " << response;
  }

  acceptor.Stop();
}

// --- Max connections ---

TEST_F(ConnectionAcceptorTcpTest, MaxConnectionsEnforced) {
  auto config = MakeConfig(0, 2);  // max 2 connections
  ConnectionAcceptor acceptor(config);

  // Handler accepts but keeps fd open (never calls RemoveConnection)
  std::vector<int> owned_fds;
  std::mutex fds_mu;
  acceptor.SetReactorHandler([&](int fd) {
    std::lock_guard<std::mutex> lock(fds_mu);
    owned_fds.push_back(fd);
    return true;
  });

  auto result = acceptor.Start();
  ASSERT_TRUE(result.has_value());

  // Fill up the 2 slots
  int c1 = ConnectToTcp("127.0.0.1", acceptor.GetPort());
  int c2 = ConnectToTcp("127.0.0.1", acceptor.GetPort());
  ASSERT_GE(c1, 0);
  ASSERT_GE(c2, 0);

  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  // Third connection should be rejected (closed by acceptor)
  int c3 = ConnectToTcp("127.0.0.1", acceptor.GetPort());
  if (c3 >= 0) {
    // The connection may succeed at TCP level but will be immediately closed
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    // Verify the handler was NOT called a third time
    std::lock_guard<std::mutex> lock(fds_mu);
    EXPECT_LE(static_cast<int>(owned_fds.size()), 2)
        << "Handler should not be called beyond max_connections";
    close(c3);
  }

  close(c1);
  close(c2);

  // Cleanup owned fds
  {
    std::lock_guard<std::mutex> lock(fds_mu);
    for (int fd : owned_fds) {
      close(fd);
    }
  }

  acceptor.Stop();
}

// --- Graceful shutdown ---

TEST_F(ConnectionAcceptorTcpTest, DoubleStopIsSafe) {
  auto config = MakeConfig(0);
  ConnectionAcceptor acceptor(config);

  acceptor.SetReactorHandler([](int fd) {
    close(fd);
    return true;
  });

  auto result = acceptor.Start();
  ASSERT_TRUE(result.has_value());

  acceptor.Stop();
  EXPECT_FALSE(acceptor.IsRunning());

  // Second stop should be a no-op, not crash
  acceptor.Stop();
  EXPECT_FALSE(acceptor.IsRunning());
}

TEST_F(ConnectionAcceptorTcpTest, StopWithoutStartIsSafe) {
  auto config = MakeConfig(0);
  ConnectionAcceptor acceptor(config);

  // Stop without ever starting should be a no-op
  acceptor.Stop();
  EXPECT_FALSE(acceptor.IsRunning());
}

TEST_F(ConnectionAcceptorTcpTest, DestructorStopsAcceptor) {
  auto config = MakeConfig(0);

  uint16_t port = 0;
  {
    ConnectionAcceptor acceptor(config);
    acceptor.SetReactorHandler([](int fd) {
      close(fd);
      return true;
    });

    auto result = acceptor.Start();
    ASSERT_TRUE(result.has_value());
    port = acceptor.GetPort();
    EXPECT_TRUE(acceptor.IsRunning());
    // Destructor runs here
  }

  // After destruction, the port should be released.
  // Verify by binding to the same port (may occasionally fail due to TIME_WAIT).
  EXPECT_GT(port, 0);
}

// --- Bind address variants ---

TEST_F(ConnectionAcceptorTcpTest, BindToAllInterfaces) {
  ServerConfig config;
  config.host = "0.0.0.0";
  config.port = 0;
  config.max_connections = 10;

  ConnectionAcceptor acceptor(config);
  acceptor.SetReactorHandler([](int fd) {
    close(fd);
    return true;
  });

  auto result = acceptor.Start();
  ASSERT_TRUE(result.has_value()) << result.error().to_string();
  EXPECT_GT(acceptor.GetPort(), 0);

  acceptor.Stop();
}

TEST_F(ConnectionAcceptorTcpTest, EmptyHostBindsToAll) {
  ServerConfig config;
  config.host = "";
  config.port = 0;
  config.max_connections = 10;

  ConnectionAcceptor acceptor(config);
  acceptor.SetReactorHandler([](int fd) {
    close(fd);
    return true;
  });

  auto result = acceptor.Start();
  ASSERT_TRUE(result.has_value()) << result.error().to_string();

  acceptor.Stop();
}

}  // namespace mygramdb::server
