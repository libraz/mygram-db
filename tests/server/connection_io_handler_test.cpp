/**
 * @file connection_io_handler_test.cpp
 * @brief Unit tests for ConnectionIOHandler
 */

#include "server/connection_io_handler.h"

#include <gtest/gtest.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <thread>

#include "server/server_types.h"

namespace mygramdb::server {

class ConnectionIOHandlerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create socket pair for testing
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, sockets_) != 0) {
      FAIL() << "Failed to create socket pair";
    }

    config_.recv_buffer_size = 1024;
    config_.max_query_length = 4096;
    config_.recv_timeout_sec = 1;
  }

  void TearDown() override {
    close(sockets_[0]);
    close(sockets_[1]);
  }

  int sockets_[2];
  IOConfig config_;
  std::atomic<bool> shutdown_flag_{false};
};

TEST_F(ConnectionIOHandlerTest, HandlesSingleRequest) {
  std::string received_request;
  int call_count = 0;

  auto processor = [&](const std::string& req, ConnectionContext& ctx) {
    received_request = req;
    call_count++;
    return "OK";
  };

  ConnectionIOHandler handler(config_, processor, shutdown_flag_);

  // Send request from client side
  std::thread client_thread([this]() {
    const char* request = "SEARCH table=test query=\"hello\"\r\n";
    send(sockets_[1], request, strlen(request), 0);

    char buffer[1024];
    ssize_t bytes = recv(sockets_[1], buffer, sizeof(buffer) - 1, 0);
    ASSERT_GT(bytes, 0);
    buffer[bytes] = '\0';
    EXPECT_STREQ("OK\r\n", buffer);

    // Close properly to unblock recv
    shutdown(sockets_[1], SHUT_RDWR);
    close(sockets_[1]);
  });

  ConnectionContext ctx;
  handler.HandleConnection(sockets_[0], ctx);

  client_thread.join();

  EXPECT_EQ(1, call_count);
  EXPECT_EQ("SEARCH table=test query=\"hello\"", received_request);
}

TEST_F(ConnectionIOHandlerTest, HandlesMultipleRequests) {
  std::vector<std::string> received_requests;
  std::atomic<int> response_count{0};

  auto processor = [&](const std::string& req, ConnectionContext& ctx) {
    received_requests.push_back(req);
    response_count++;
    return "OK " + std::to_string(response_count.load());
  };

  ConnectionIOHandler handler(config_, processor, shutdown_flag_);

  std::thread server_thread([&]() {
    ConnectionContext ctx;
    handler.HandleConnection(sockets_[0], ctx);
  });

  // Give server time to start
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  // Send all requests at once
  const char* requests = "SEARCH query=\"test1\"\r\nSEARCH query=\"test2\"\r\nSEARCH query=\"test3\"\r\n";
  send(sockets_[1], requests, strlen(requests), 0);

  // Read all responses
  std::string all_responses;
  char buffer[1024];
  int expected_responses = 3;
  int received = 0;

  while (received < expected_responses) {
    ssize_t bytes = recv(sockets_[1], buffer, sizeof(buffer) - 1, 0);
    if (bytes <= 0)
      break;
    buffer[bytes] = '\0';
    all_responses += buffer;

    // Count \r\n occurrences
    size_t pos = 0;
    while ((pos = all_responses.find("\r\n", pos)) != std::string::npos) {
      received++;
      pos += 2;
    }
  }

  // Close connection
  shutdown(sockets_[1], SHUT_RDWR);
  close(sockets_[1]);

  server_thread.join();

  ASSERT_EQ(3, received_requests.size());
  EXPECT_EQ("SEARCH query=\"test1\"", received_requests[0]);
  EXPECT_EQ("SEARCH query=\"test2\"", received_requests[1]);
  EXPECT_EQ("SEARCH query=\"test3\"", received_requests[2]);
}

TEST_F(ConnectionIOHandlerTest, RejectsOversizedRequest) {
  config_.max_query_length = 100;  // Small limit

  auto processor = [&](const std::string& req, ConnectionContext& ctx) { return "OK"; };

  ConnectionIOHandler handler(config_, processor, shutdown_flag_);

  std::thread client_thread([this]() {
    // Send request larger than limit without newline
    std::string large_request(1500, 'X');
    send(sockets_[1], large_request.c_str(), large_request.length(), 0);

    char buffer[1024];
    ssize_t bytes = recv(sockets_[1], buffer, sizeof(buffer) - 1, 0);
    ASSERT_GT(bytes, 0);
    buffer[bytes] = '\0';
    EXPECT_TRUE(std::string(buffer).find("ERROR") != std::string::npos);

    shutdown(sockets_[1], SHUT_RDWR);
  });

  ConnectionContext ctx;
  handler.HandleConnection(sockets_[0], ctx);

  client_thread.join();
}

TEST_F(ConnectionIOHandlerTest, RespectsShutdownFlag) {
  auto processor = [&](const std::string& req, ConnectionContext& ctx) { return "OK"; };

  ConnectionIOHandler handler(config_, processor, shutdown_flag_);

  std::thread handler_thread([&]() {
    ConnectionContext ctx;
    handler.HandleConnection(sockets_[0], ctx);
  });

  // Let handler start
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // Signal shutdown
  shutdown_flag_ = true;

  // Close socket to unblock recv
  close(sockets_[1]);

  handler_thread.join();

  // Test passes if no hang occurs
  SUCCEED();
}

TEST_F(ConnectionIOHandlerTest, HandlesPartialReceives) {
  std::string received_request;

  auto processor = [&](const std::string& req, ConnectionContext& ctx) {
    received_request = req;
    return "OK";
  };

  ConnectionIOHandler handler(config_, processor, shutdown_flag_);

  std::thread client_thread([this]() {
    // Send request in parts
    const char* part1 = "SEARCH ";
    const char* part2 = "query=\"hello\"";
    const char* part3 = "\r\n";

    send(sockets_[1], part1, strlen(part1), 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    send(sockets_[1], part2, strlen(part2), 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    send(sockets_[1], part3, strlen(part3), 0);

    char buffer[1024];
    recv(sockets_[1], buffer, sizeof(buffer) - 1, 0);

    shutdown(sockets_[1], SHUT_RDWR);
    close(sockets_[1]);
  });

  ConnectionContext ctx;
  handler.HandleConnection(sockets_[0], ctx);

  client_thread.join();

  EXPECT_EQ("SEARCH query=\"hello\"", received_request);
}

}  // namespace mygramdb::server
