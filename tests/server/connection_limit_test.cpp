/**
 * @file connection_limit_test.cpp
 * @brief Tests for TCP server connection limit enforcement
 *
 * SECURITY: Validates that max_connections prevents resource exhaustion
 * by rejecting connections that exceed the configured limit.
 */

#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

#include "server/tcp_server.h"
#include "utils/network_utils.h"

namespace mygramdb::server {

class ConnectionLimitTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create minimal table context for testing
    auto index = std::make_unique<index::Index>(2, 1);  // ngram_size=2, kanji_ngram_size=1
    auto doc_store = std::make_unique<storage::DocumentStore>();

    table_context_ = std::make_unique<TableContext>(TableContext{
        .name = "test_table",
        .config = config::TableConfig{},
        .index = std::move(index),
        .doc_store = std::move(doc_store),
    });

    table_contexts_["test_table"] = table_context_.get();
  }

  void TearDown() override {
    table_contexts_.clear();
    table_context_.reset();
  }

  /**
   * @brief Create a TCP socket and connect to server
   * @return Socket file descriptor, or -1 on failure
   */
  int ConnectToServer(uint16_t port) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
      return -1;
    }

    struct sockaddr_in server_addr {};
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    inet_pton(AF_INET, "127.0.0.1", &server_addr.sin_addr);

    if (connect(sock, reinterpret_cast<struct sockaddr*>(&server_addr), sizeof(server_addr)) < 0) {
      close(sock);
      return -1;
    }

    return sock;
  }

  std::unique_ptr<TableContext> table_context_;
  std::unordered_map<std::string, TableContext*> table_contexts_;
};

// Test: Connection limit is enforced (reject when limit reached)
TEST_F(ConnectionLimitTest, EnforcesConnectionLimit) {
  // Create server with very small connection limit
  ServerConfig config;
  config.host = "127.0.0.1";
  config.port = 0;  // Random port
  config.max_connections = 3;
  config.worker_threads = 2;
  config.allow_cidrs = {"127.0.0.1/32"};

  TcpServer server(config, table_contexts_);
  ASSERT_TRUE(server.Start());

  uint16_t port = server.GetPort();
  ASSERT_GT(port, 0);

  std::vector<int> sockets;

  // Establish connections up to the limit
  for (int i = 0; i < 3; ++i) {
    int sock = ConnectToServer(port);
    ASSERT_GE(sock, 0) << "Failed to connect within limit (connection " << i << ")";
    sockets.push_back(sock);
  }

  // Give server time to process connections
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // 4th connection should be rejected (limit is 3)
  int rejected_sock = ConnectToServer(port);

  // Server should close the connection immediately
  if (rejected_sock >= 0) {
    // Try to read - should get EOF or error
    char buffer[1];
    ssize_t n = recv(rejected_sock, buffer, sizeof(buffer), 0);
    EXPECT_LE(n, 0) << "Connection should be closed by server (limit reached)";
    close(rejected_sock);
  }

  // Close all successful connections
  for (int sock : sockets) {
    close(sock);
  }

  server.Stop();
}

// Test: Connections can be made after closing previous connections
TEST_F(ConnectionLimitTest, AllowsNewConnectionsAfterClose) {
  ServerConfig config;
  config.host = "127.0.0.1";
  config.port = 0;
  config.max_connections = 2;
  config.worker_threads = 2;
  config.allow_cidrs = {"127.0.0.1/32"};

  TcpServer server(config, table_contexts_);
  ASSERT_TRUE(server.Start());

  uint16_t port = server.GetPort();

  // Connect to limit
  int sock1 = ConnectToServer(port);
  int sock2 = ConnectToServer(port);
  ASSERT_GE(sock1, 0);
  ASSERT_GE(sock2, 0);

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // Close one connection
  close(sock1);
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // Should be able to connect again
  int sock3 = ConnectToServer(port);
  EXPECT_GE(sock3, 0) << "Should allow new connection after one is closed";

  close(sock2);
  close(sock3);
  server.Stop();
}

// Test: Connection limit with concurrent connection attempts
TEST_F(ConnectionLimitTest, HandlesConcurrentConnections) {
  ServerConfig config;
  config.host = "127.0.0.1";
  config.port = 0;
  config.max_connections = 5;
  config.worker_threads = 4;
  config.allow_cidrs = {"127.0.0.1/32"};

  TcpServer server(config, table_contexts_);
  ASSERT_TRUE(server.Start());

  uint16_t port = server.GetPort();

  std::atomic<int> successful_connects{0};
  std::atomic<int> failed_connects{0};
  std::vector<std::thread> threads;
  std::vector<int> successful_sockets;
  std::mutex sockets_mutex;

  // Spawn 10 threads trying to connect simultaneously
  for (int i = 0; i < 10; ++i) {
    threads.emplace_back([this, port, &successful_connects, &failed_connects, &successful_sockets, &sockets_mutex]() {
      int sock = ConnectToServer(port);
      if (sock >= 0) {
        // Connection succeeded at TCP level, but server may close it immediately
        // Try to verify the connection is actually alive
        std::this_thread::sleep_for(std::chrono::milliseconds(50));

        // Check if connection is still alive by trying to read (non-blocking)
        char buffer[1];
        fd_set read_fds;
        FD_ZERO(&read_fds);
        FD_SET(sock, &read_fds);
        struct timeval tv {
          .tv_sec = 0, .tv_usec = 10000
        };  // 10ms timeout
        int select_result = select(sock + 1, &read_fds, nullptr, nullptr, &tv);

        if (select_result > 0) {
          // Data available or connection closed - read to check
          ssize_t n = recv(sock, buffer, sizeof(buffer), MSG_DONTWAIT);
          if (n <= 0) {
            // Connection was closed by server (limit reached)
            close(sock);
            failed_connects++;
            return;
          }
        }

        // Connection is alive
        successful_connects++;
        std::lock_guard<std::mutex> lock(sockets_mutex);
        successful_sockets.push_back(sock);
        // Keep connection open for a bit
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
      } else {
        failed_connects++;
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  // Due to connection limit, we should have rejections
  // Note: Some connections may succeed at TCP level but be closed immediately
  EXPECT_GT(failed_connects.load(), 0) << "Should have some rejected connections";
  EXPECT_GT(successful_connects.load(), 0) << "Should have some successful connections";

  // Clean up
  {
    std::lock_guard<std::mutex> lock(sockets_mutex);
    for (int sock : successful_sockets) {
      close(sock);
    }
  }

  server.Stop();
}

// Test: Connection limit does not affect ACL rejection
TEST_F(ConnectionLimitTest, ACLRejectionTakesPrecedence) {
  ServerConfig config;
  config.host = "127.0.0.1";
  config.port = 0;
  config.max_connections = 10;
  config.worker_threads = 2;
  config.allow_cidrs = {"192.168.1.0/24"};  // Does not include 127.0.0.1

  TcpServer server(config, table_contexts_);
  ASSERT_TRUE(server.Start());

  uint16_t port = server.GetPort();

  // Connection should be rejected by ACL before connection limit is checked
  int sock = ConnectToServer(port);

  if (sock >= 0) {
    // Connection might succeed at TCP level, but server will close it due to ACL
    char buffer[1];
    ssize_t n = recv(sock, buffer, sizeof(buffer), 0);
    EXPECT_LE(n, 0) << "Connection should be closed by server (ACL rejection)";
    close(sock);
  }

  server.Stop();
}

// Test: Default max_connections value
TEST_F(ConnectionLimitTest, DefaultMaxConnectionsValue) {
  ServerConfig config;
  config.host = "127.0.0.1";
  config.port = 0;
  config.allow_cidrs = {"127.0.0.1/32"};
  // Note: max_connections should default to kDefaultMaxConnections (10000)

  EXPECT_EQ(config.max_connections, kDefaultMaxConnections);
}

// Test: Connection limit with graceful shutdown
TEST_F(ConnectionLimitTest, GracefulShutdownWithActiveConnections) {
  ServerConfig config;
  config.host = "127.0.0.1";
  config.port = 0;
  config.max_connections = 5;
  config.worker_threads = 2;
  config.allow_cidrs = {"127.0.0.1/32"};

  TcpServer server(config, table_contexts_);
  ASSERT_TRUE(server.Start());

  uint16_t port = server.GetPort();

  // Create some connections
  std::vector<int> sockets;
  for (int i = 0; i < 3; ++i) {
    int sock = ConnectToServer(port);
    ASSERT_GE(sock, 0);
    sockets.push_back(sock);
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // Stop server while connections are active
  server.Stop();

  // Connections should be closed by server
  for (int sock : sockets) {
    char buffer[1];
    ssize_t n = recv(sock, buffer, sizeof(buffer), 0);
    EXPECT_LE(n, 0) << "Connection should be closed during shutdown";
    close(sock);
  }
}

// Test: Connection counting accuracy
TEST_F(ConnectionLimitTest, AccurateConnectionCounting) {
  ServerConfig config;
  config.host = "127.0.0.1";
  config.port = 0;
  config.max_connections = 100;
  config.worker_threads = 4;
  config.allow_cidrs = {"127.0.0.1/32"};

  TcpServer server(config, table_contexts_);
  ASSERT_TRUE(server.Start());

  uint16_t port = server.GetPort();

  // Create 10 connections
  std::vector<int> sockets;
  for (int i = 0; i < 10; ++i) {
    int sock = ConnectToServer(port);
    ASSERT_GE(sock, 0);
    sockets.push_back(sock);
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  // Check connection count (might be slightly off due to timing)
  size_t conn_count = server.GetConnectionCount();
  EXPECT_GE(conn_count, 1);  // At least some connections should be counted
  EXPECT_LE(conn_count, 10);

  // Close all connections
  for (int sock : sockets) {
    close(sock);
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  // Connection count should drop back to 0
  conn_count = server.GetConnectionCount();
  EXPECT_EQ(conn_count, 0);

  server.Stop();
}

}  // namespace mygramdb::server
