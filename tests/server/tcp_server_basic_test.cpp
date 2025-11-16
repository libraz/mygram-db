/**
 * @file tcp_server_basic_test.cpp
 * @brief Unit tests for TCP server - Lifecycle & basic operations
 */

#include <arpa/inet.h>
#include <fcntl.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <sstream>
#include <thread>

#include "config/config.h"
#include "server/tcp_server.h"

using namespace mygramdb::server;
using namespace mygramdb;

/**
 * @brief Test fixture for TCP server tests
 */
class TcpServerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    SkipIfSocketCreationBlocked();

    // Create index and doc_store as unique_ptrs
    auto index = std::make_unique<index::Index>(1);
    auto doc_store = std::make_unique<storage::DocumentStore>();

    // Store raw pointers in table context
    table_context_.name = "test";
    table_context_.config.ngram_size = 1;
    table_context_.index = std::move(index);
    table_context_.doc_store = std::move(doc_store);

    // Keep raw pointers for test access
    index_ = table_context_.index.get();
    doc_store_ = table_context_.doc_store.get();

    table_contexts_["test"] = &table_context_;

    config_.port = 0;  // Let OS assign port
    config_.host = "127.0.0.1";
    config_.allow_cidrs = {"127.0.0.1/32"};  // Allow localhost

    server_ = std::make_unique<TcpServer>(config_, table_contexts_);
  }

  void TearDown() override {
    if (server_ && server_->IsRunning()) {
      server_->Stop();
    }
  }

  int CreateClientSocket(uint16_t port);

  // Helper to send request and receive response
  std::string SendRequest(int sock, const std::string& request) {
    std::string msg = request + "\r\n";
    ssize_t sent = send(sock, msg.c_str(), msg.length(), 0);
    if (sent < 0) {
      return "";
    }

    char buffer[4096];
    ssize_t received = recv(sock, buffer, sizeof(buffer) - 1, 0);
    if (received <= 0) {
      return "";
    }

    buffer[received] = '\0';
    std::string response(buffer);

    // Remove trailing \r\n
    if (response.size() >= 2 && response[response.size() - 2] == '\r' && response[response.size() - 1] == '\n') {
      response = response.substr(0, response.size() - 2);
    }

    return response;
  }

  ServerConfig config_;
  index::Index* index_;                // Raw pointer to table_context_.index
  storage::DocumentStore* doc_store_;  // Raw pointer to table_context_.doc_store
  TableContext table_context_;
  std::unordered_map<std::string, TableContext*> table_contexts_;
  std::unique_ptr<TcpServer> server_;

  static void SkipIfSocketCreationBlocked();
  void StartServerOrSkip();
};

void TcpServerTest::SkipIfSocketCreationBlocked() {
  static bool checked = false;
  static bool skip_due_to_permissions = false;
  static std::string permission_error;

  if (!checked) {
    checked = true;
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd >= 0) {
      close(fd);
    } else {
      if (errno == EPERM || errno == EACCES) {
        skip_due_to_permissions = true;
        permission_error = std::strerror(errno);
      }
    }
  }

  if (skip_due_to_permissions) {
    GTEST_SKIP() << "Skipping TcpServerTest: unable to create AF_INET socket (" << permission_error
                 << "). WSL/OS is blocking TCP sockets; enable networking to run this test.";
  }
}

int TcpServerTest::CreateClientSocket(uint16_t port) {
  constexpr int kConnectTimeoutSec = 5;
  constexpr int kSocketIoTimeoutSec = 5;

  int sock = socket(AF_INET, SOCK_STREAM, 0);
  if (sock < 0) {
    return -1;
  }

  // Set socket non-blocking to implement custom connect timeout
  int flags = fcntl(sock, F_GETFL, 0);
  if (flags >= 0) {
    fcntl(sock, F_SETFL, flags | O_NONBLOCK);
  }

  struct sockaddr_in server_addr {};
  server_addr.sin_family = AF_INET;
  server_addr.sin_port = htons(port);
  inet_pton(AF_INET, "127.0.0.1", &server_addr.sin_addr);

  int result = connect(sock, reinterpret_cast<struct sockaddr*>(&server_addr), sizeof(server_addr));
  if (result < 0) {
    if (errno != EINPROGRESS) {
      close(sock);
      return -1;
    }

    fd_set write_fds;
    FD_ZERO(&write_fds);
    FD_SET(sock, &write_fds);

    struct timeval timeout {};
    timeout.tv_sec = kConnectTimeoutSec;

    int ready = select(sock + 1, nullptr, &write_fds, nullptr, &timeout);
    if (ready <= 0) {
      close(sock);
      errno = (ready == 0) ? ETIMEDOUT : errno;
      return -1;
    }

    int so_error = 0;
    socklen_t len = sizeof(so_error);
    if (getsockopt(sock, SOL_SOCKET, SO_ERROR, &so_error, &len) < 0 || so_error != 0) {
      if (so_error != 0) {
        errno = so_error;
      }
      close(sock);
      return -1;
    }
  }

  // Restore blocking mode if we changed it
  if (flags >= 0) {
    fcntl(sock, F_SETFL, flags);
  }

  // Apply send/receive timeouts to avoid hangs on recv()
  struct timeval io_timeout {};
  io_timeout.tv_sec = kSocketIoTimeoutSec;
  setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &io_timeout, sizeof(io_timeout));
  setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &io_timeout, sizeof(io_timeout));

  return sock;
}

void TcpServerTest::StartServerOrSkip() {
  if (server_->Start()) {
    return;
  }

  const std::string& error = server_->GetLastError();
  if (error.find("Operation not permitted") != std::string::npos ||
      error.find("Permission denied") != std::string::npos) {
    GTEST_SKIP() << "Skipping TcpServerTest: " << error << ". This environment does not allow creating TCP sockets.";
  }

  FAIL() << "Failed to start TCP server: " << (error.empty() ? "unknown error" : error);
}

/**
 * @brief Test server construction
 */
TEST_F(TcpServerTest, Construction) {
  EXPECT_FALSE(server_->IsRunning());
  EXPECT_EQ(server_->GetConnectionCount(), 0);
  EXPECT_EQ(server_->GetTotalRequests(), 0);
}

/**
 * @brief Test server start and stop
 */
TEST_F(TcpServerTest, StartStop) {
  StartServerOrSkip();
  EXPECT_TRUE(server_->IsRunning());
  EXPECT_GT(server_->GetPort(), 0);

  server_->Stop();
  EXPECT_FALSE(server_->IsRunning());
}

/**
 * @brief Test double start
 */
TEST_F(TcpServerTest, DoubleStart) {
  StartServerOrSkip();
  EXPECT_FALSE(server_->Start());  // Should fail
  EXPECT_TRUE(server_->IsRunning());
}

/**
 * @brief Test GET request for non-existent document
 */
TEST_F(TcpServerTest, GetNonExistent) {
  StartServerOrSkip();
  uint16_t port = server_->GetPort();

  // Wait for server to be ready
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  int sock = CreateClientSocket(port);
  ASSERT_GE(sock, 0);

  std::string response = SendRequest(sock, "GET test 999");
  EXPECT_EQ(response, "ERROR Document not found");

  close(sock);
}

/**
 * @brief Test GET with document
 */
TEST_F(TcpServerTest, GetDocument) {
  // Add document
  std::unordered_map<std::string, storage::FilterValue> filters;
  filters["status"] = static_cast<int64_t>(1);
  auto doc_id = doc_store_->AddDocument("test123", filters);
  index_->AddDocument(static_cast<index::DocId>(doc_id), "hello world");

  StartServerOrSkip();
  uint16_t port = server_->GetPort();

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  int sock = CreateClientSocket(port);
  ASSERT_GE(sock, 0);

  std::string response = SendRequest(sock, "GET test test123");
  EXPECT_TRUE(response.find("OK DOC test123") == 0);
  EXPECT_TRUE(response.find("status=1") != std::string::npos);

  close(sock);
}

/**
 * @brief Test invalid command
 */
TEST_F(TcpServerTest, InvalidCommand) {
  StartServerOrSkip();
  uint16_t port = server_->GetPort();

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  int sock = CreateClientSocket(port);
  ASSERT_GE(sock, 0);

  std::string response = SendRequest(sock, "INVALID");
  EXPECT_TRUE(response.find("ERROR") == 0);

  close(sock);
}

/**
 * @brief Test multiple requests on same connection
 */
TEST_F(TcpServerTest, MultipleRequests) {
  // Add document
  auto doc_id = doc_store_->AddDocument("1", {});
  index_->AddDocument(static_cast<index::DocId>(doc_id), "test");

  StartServerOrSkip();
  uint16_t port = server_->GetPort();

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  int sock = CreateClientSocket(port);
  ASSERT_GE(sock, 0);

  std::string response1 = SendRequest(sock, "SEARCH test test");
  EXPECT_EQ(response1, "OK RESULTS 1 1");

  std::string response2 = SendRequest(sock, "COUNT test test");
  EXPECT_EQ(response2, "OK COUNT 1");

  close(sock);
}
