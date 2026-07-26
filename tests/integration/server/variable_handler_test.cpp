/**
 * @file variable_handler_test.cpp
 * @brief Integration tests for SET/SHOW VARIABLES commands
 */

#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <cerrno>
#include <charconv>
#include <cstring>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "cache/cache_manager.h"
#include "config/config.h"
#include "config/runtime_variable_manager.h"
#include "server/tcp_server.h"

using namespace mygramdb::server;
using namespace mygramdb::config;
using namespace mygramdb;

/**
 * @brief Helper class for TCP client connections
 */
class TcpClient {
 public:
  TcpClient(const std::string& host, uint16_t port) {
    sock_ = socket(AF_INET, SOCK_STREAM, 0);
    if (sock_ < 0) {
      throw std::runtime_error("Failed to create socket");
    }

    struct timeval receive_timeout {};
    receive_timeout.tv_sec = 5;
    if (setsockopt(sock_, SOL_SOCKET, SO_RCVTIMEO, &receive_timeout, sizeof(receive_timeout)) < 0) {
      close(sock_);
      sock_ = -1;
      throw std::runtime_error("Failed to set receive timeout");
    }

    struct sockaddr_in server_addr {};
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    if (inet_pton(AF_INET, host.c_str(), &server_addr.sin_addr) != 1) {
      close(sock_);
      sock_ = -1;
      throw std::runtime_error("Invalid host address: " + host);
    }

    if (connect(sock_, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
      close(sock_);
      sock_ = -1;
      throw std::runtime_error("Failed to connect to " + host + ":" + std::to_string(port));
    }
  }

  ~TcpClient() {
    if (sock_ >= 0) {
      close(sock_);
    }
  }

  std::string SendCommand(const std::string& command) {
    const std::string request = command + "\r\n";
    size_t sent = 0;
    while (sent < request.size()) {
      const ssize_t result = send(sock_, request.data() + sent, request.size() - sent, 0);
      if (result > 0) {
        sent += static_cast<size_t>(result);
        continue;
      }
      if (result < 0 && errno == EINTR) {
        continue;
      }
      throw std::runtime_error("Failed to send command: " + std::string(std::strerror(errno)));
    }

    std::string response;
    std::array<char, 4096> buffer{};
    while (response.size() < kMaxResponseBytes) {
      const ssize_t received = recv(sock_, buffer.data(), buffer.size(), 0);
      if (received > 0) {
        response.append(buffer.data(), static_cast<size_t>(received));
        if (IsResponseComplete(response)) {
          return response;
        }
        continue;
      }
      if (received < 0 && errno == EINTR) {
        continue;
      }
      if (received < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
        throw std::runtime_error("Timed out waiting for complete response");
      }
      if (received == 0) {
        throw std::runtime_error("Connection closed before complete response");
      }
      throw std::runtime_error("Failed to receive response: " + std::string(std::strerror(errno)));
    }
    throw std::runtime_error("Response exceeded maximum size");
  }

 private:
  static bool IsResponseComplete(const std::string& response) {
    if (response.rfind("ERROR ", 0) == 0) {
      return response.find("\r\n") != std::string::npos;
    }
    return response.size() >= 4 && response.compare(response.size() - 4, 4, "\r\n\r\n") == 0;
  }

  static constexpr size_t kMaxResponseBytes = 1024 * 1024;
  int sock_ = -1;
};

/**
 * @brief Test fixture for variable handler integration tests
 */
class VariableHandlerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create minimal table context
    auto index = std::make_unique<mygramdb::index::Index>(3, 2);
    auto doc_store = std::make_unique<mygramdb::storage::DocumentStore>();

    table_context_.name = "test_table";
    table_context_.config.ngram_size = 3;
    table_context_.config.kanji_ngram_size = 2;
    table_context_.index = std::move(index);
    table_context_.doc_store = std::move(doc_store);

    table_contexts_["test_table"] = &table_context_;

    // Create server config
    config_.port = 0;  // Random port
    config_.host = "127.0.0.1";
    config_.allow_cidrs = {"127.0.0.1/32"};

    server_ = std::make_unique<TcpServer>(config_, table_contexts_, "./dumps", &full_config_);
    auto start_result = server_->Start();
    ASSERT_TRUE(start_result) << "Failed to start TcpServer: " << start_result.error().to_string();
    ASSERT_NE(server_->GetVariableManager(), nullptr);

    port_ = server_->GetPort();
    ASSERT_NE(port_, 0);
  }

  void TearDown() override {
    if (server_) {
      auto stop_result = server_->Stop();
      EXPECT_TRUE(stop_result) << "Failed to stop TcpServer: " << stop_result.error().to_string();
    }
  }

  TableContext table_context_;
  std::unordered_map<std::string, TableContext*> table_contexts_;
  ServerConfig config_;
  Config full_config_;
  std::unique_ptr<TcpServer> server_;
  uint16_t port_ = 0;
};

/**
 * @brief Test SHOW VARIABLES command (all variables)
 */
TEST_F(VariableHandlerTest, ShowVariablesAll) {
  TcpClient client("127.0.0.1", port_);

  std::string response = client.SendCommand("SHOW VARIABLES");

  // Response should contain variable table
  EXPECT_NE(response.find("Variable_name"), std::string::npos);
  EXPECT_NE(response.find("Value"), std::string::npos);
  EXPECT_NE(response.find("Mutable"), std::string::npos);

  // Should contain known variables
  EXPECT_NE(response.find("logging.level"), std::string::npos);
  EXPECT_NE(response.find("mysql.host"), std::string::npos);
  EXPECT_NE(response.find("api.default_limit"), std::string::npos);
}

/**
 * @brief Test SHOW VARIABLES LIKE pattern
 */
TEST_F(VariableHandlerTest, ShowVariablesLikePattern) {
  TcpClient client("127.0.0.1", port_);

  // Show logging variables
  std::string response1 = client.SendCommand("SHOW VARIABLES LIKE 'logging%'");
  EXPECT_NE(response1.find("logging.level"), std::string::npos);
  EXPECT_NE(response1.find("logging.format"), std::string::npos);
  EXPECT_EQ(response1.find("mysql.host"), std::string::npos);  // Should not contain mysql vars

  // Show mysql variables
  std::string response2 = client.SendCommand("SHOW VARIABLES LIKE 'mysql%'");
  EXPECT_NE(response2.find("mysql.host"), std::string::npos);
  EXPECT_NE(response2.find("mysql.port"), std::string::npos);
  EXPECT_EQ(response2.find("logging.level"), std::string::npos);  // Should not contain logging vars

  // Show cache variables
  std::string response3 = client.SendCommand("SHOW VARIABLES LIKE 'cache%'");
  EXPECT_NE(response3.find("cache.enabled"), std::string::npos);
  EXPECT_NE(response3.find("cache.min_query_cost_ms"), std::string::npos);
  EXPECT_EQ(response3.find("mysql.host"), std::string::npos);  // Should not contain mysql vars
}

/**
 * @brief Test SET command for logging.level
 */
TEST_F(VariableHandlerTest, SetLoggingLevel) {
  TcpClient client("127.0.0.1", port_);

  // Set to debug
  std::string response1 = client.SendCommand("SET logging.level = 'debug'");
  EXPECT_NE(response1.find("OK"), std::string::npos);

  // Verify change
  std::string response2 = client.SendCommand("SHOW VARIABLES LIKE 'logging.level'");
  EXPECT_NE(response2.find("debug"), std::string::npos);

  // Set to error
  std::string response3 = client.SendCommand("SET logging.level = 'error'");
  EXPECT_NE(response3.find("OK"), std::string::npos);

  // Verify change
  std::string response4 = client.SendCommand("SHOW VARIABLES LIKE 'logging.level'");
  EXPECT_NE(response4.find("error"), std::string::npos);
}

/**
 * @brief Test SET command for api.default_limit
 */
TEST_F(VariableHandlerTest, SetApiDefaultLimit) {
  TcpClient client("127.0.0.1", port_);

  // Set to 200
  std::string response1 = client.SendCommand("SET api.default_limit = 200");
  EXPECT_NE(response1.find("OK"), std::string::npos);

  // Verify change
  std::string response2 = client.SendCommand("SHOW VARIABLES LIKE 'api.default_limit'");
  EXPECT_NE(response2.find("200"), std::string::npos);

  // Set to 50
  std::string response3 = client.SendCommand("SET api.default_limit = 50");
  EXPECT_NE(response3.find("OK"), std::string::npos);

  // Verify change
  std::string response4 = client.SendCommand("SHOW VARIABLES LIKE 'api.default_limit'");
  EXPECT_NE(response4.find("50"), std::string::npos);
}

/**
 * @brief Test SET command with multiple variables
 */
TEST_F(VariableHandlerTest, SetMultipleVariables) {
  TcpClient client("127.0.0.1", port_);

  // Set multiple variables at once
  std::string response1 = client.SendCommand("SET logging.level = 'debug', api.default_limit = 150");
  EXPECT_NE(response1.find("OK"), std::string::npos);

  // Verify both changes
  std::string response2 = client.SendCommand("SHOW VARIABLES LIKE 'logging.level'");
  EXPECT_NE(response2.find("debug"), std::string::npos);

  std::string response3 = client.SendCommand("SHOW VARIABLES LIKE 'api.default_limit'");
  EXPECT_NE(response3.find("150"), std::string::npos);
}

/**
 * @brief Test SET command with invalid variable name
 */
TEST_F(VariableHandlerTest, SetInvalidVariableName) {
  TcpClient client("127.0.0.1", port_);

  std::string response = client.SendCommand("SET unknown.variable = 'value'");
  EXPECT_NE(response.find("ERROR"), std::string::npos);
  EXPECT_NE(response.find("Unknown variable"), std::string::npos);
}

/**
 * @brief Test SET command with immutable variable
 */
TEST_F(VariableHandlerTest, SetImmutableVariable) {
  TcpClient client("127.0.0.1", port_);

  // Try to set mysql.user (immutable)
  std::string response1 = client.SendCommand("SET mysql.user = 'new_user'");
  EXPECT_NE(response1.find("ERROR"), std::string::npos);
  EXPECT_NE(response1.find("immutable"), std::string::npos);

  // Try to set mysql.database (immutable)
  std::string response2 = client.SendCommand("SET mysql.database = 'new_db'");
  EXPECT_NE(response2.find("ERROR"), std::string::npos);
  EXPECT_NE(response2.find("immutable"), std::string::npos);
}

/**
 * @brief Test SET command with invalid value type
 */
TEST_F(VariableHandlerTest, SetInvalidValueType) {
  TcpClient client("127.0.0.1", port_);

  // Try to set integer variable with string value
  std::string response1 = client.SendCommand("SET api.default_limit = 'not_a_number'");
  EXPECT_NE(response1.find("ERROR"), std::string::npos);

  // Try to set boolean variable with invalid value
  std::string response2 = client.SendCommand("SET cache.enabled = 'maybe'");
  EXPECT_NE(response2.find("ERROR"), std::string::npos);
}

/**
 * @brief Test SET command with out-of-range value
 */
TEST_F(VariableHandlerTest, SetOutOfRangeValue) {
  TcpClient client("127.0.0.1", port_);

  // Try to set api.default_limit below minimum
  std::string response1 = client.SendCommand("SET api.default_limit = 4");
  EXPECT_NE(response1.find("ERROR"), std::string::npos);

  // Try to set api.default_limit above maximum
  std::string response2 = client.SendCommand("SET api.default_limit = 1001");
  EXPECT_NE(response2.find("ERROR"), std::string::npos);

  // Verify original value unchanged
  std::string response3 = client.SendCommand("SHOW VARIABLES LIKE 'api.default_limit'");
  EXPECT_NE(response3.find("100"), std::string::npos);
}

/**
 * @brief Test SET command for cache.enabled (toggle)
 */
TEST_F(VariableHandlerTest, SetCacheEnabled) {
  TcpClient client("127.0.0.1", port_);

  // Disable cache
  std::string response1 = client.SendCommand("SET cache.enabled = false");
  EXPECT_NE(response1.find("OK"), std::string::npos);

  // Verify change
  std::string response2 = client.SendCommand("SHOW VARIABLES LIKE 'cache.enabled'");
  EXPECT_NE(response2.find("false"), std::string::npos);

  // Enable cache
  std::string response3 = client.SendCommand("SET cache.enabled = true");
  EXPECT_NE(response3.find("OK"), std::string::npos);

  // Verify change
  std::string response4 = client.SendCommand("SHOW VARIABLES LIKE 'cache.enabled'");
  EXPECT_NE(response4.find("true"), std::string::npos);
}

/**
 * @brief Test SET command for cache.min_query_cost_ms
 */
TEST_F(VariableHandlerTest, SetCacheMinQueryCost) {
  TcpClient client("127.0.0.1", port_);

  // Set to 20.0
  std::string response1 = client.SendCommand("SET cache.min_query_cost_ms = 20.0");
  EXPECT_NE(response1.find("OK"), std::string::npos);

  // Verify change
  std::string response2 = client.SendCommand("SHOW VARIABLES LIKE 'cache.min_query_cost_ms'");
  EXPECT_NE(response2.find("20"), std::string::npos);

  // Set to 0.0 (disable cost-based caching)
  std::string response3 = client.SendCommand("SET cache.min_query_cost_ms = 0.0");
  EXPECT_NE(response3.find("OK"), std::string::npos);

  // Verify change
  std::string response4 = client.SendCommand("SHOW VARIABLES LIKE 'cache.min_query_cost_ms'");
  EXPECT_NE(response4.find("0"), std::string::npos);
}

/**
 * @brief Test SET command for logging.format
 */
TEST_F(VariableHandlerTest, SetLoggingFormat) {
  TcpClient client("127.0.0.1", port_);

  // Set to text
  std::string response1 = client.SendCommand("SET logging.format = 'text'");
  EXPECT_NE(response1.find("OK"), std::string::npos);

  // Verify change
  std::string response2 = client.SendCommand("SHOW VARIABLES LIKE 'logging.format'");
  EXPECT_NE(response2.find("text"), std::string::npos);

  // Set to json
  std::string response3 = client.SendCommand("SET logging.format = 'json'");
  EXPECT_NE(response3.find("OK"), std::string::npos);

  // Verify change
  std::string response4 = client.SendCommand("SHOW VARIABLES LIKE 'logging.format'");
  EXPECT_NE(response4.find("json"), std::string::npos);
}

/**
 * @brief Test SET command with invalid logging.format
 */
TEST_F(VariableHandlerTest, SetInvalidLoggingFormat) {
  TcpClient client("127.0.0.1", port_);

  std::string response = client.SendCommand("SET logging.format = 'xml'");
  EXPECT_NE(response.find("ERROR"), std::string::npos);

  // Verify original value unchanged
  std::string response2 = client.SendCommand("SHOW VARIABLES LIKE 'logging.format'");
  EXPECT_NE(response2.find("json"), std::string::npos);
}

/**
 * @brief Test SHOW VARIABLES output format (MySQL-compatible table)
 */
TEST_F(VariableHandlerTest, ShowVariablesOutputFormat) {
  TcpClient client("127.0.0.1", port_);

  std::string response = client.SendCommand("SHOW VARIABLES LIKE 'logging.level'");

  // Check table header
  EXPECT_NE(response.find("Variable_name"), std::string::npos);
  EXPECT_NE(response.find("Value"), std::string::npos);
  EXPECT_NE(response.find("Mutable"), std::string::npos);

  // Check table separators
  EXPECT_NE(response.find("+"), std::string::npos);  // Table border
  EXPECT_NE(response.find("-"), std::string::npos);  // Horizontal separator

  // Check data row
  EXPECT_NE(response.find("logging.level"), std::string::npos);
  EXPECT_NE(response.find("info"), std::string::npos);
  EXPECT_NE(response.find("YES"), std::string::npos);  // Mutable = YES
}

/**
 * @brief Test concurrent SET commands (thread safety)
 */
TEST_F(VariableHandlerTest, ConcurrentSetCommands) {
  const int num_threads = 5;
  const int num_iterations = 10;
  std::vector<std::thread> threads;
  std::atomic<int> errors{0};

  // Spawn multiple threads issuing SET commands
  for (int i = 0; i < num_threads; ++i) {
    threads.emplace_back([&, i]() {
      try {
        TcpClient client("127.0.0.1", port_);
        for (int j = 0; j < num_iterations; ++j) {
          int value = 50 + (i * 10) + j;
          std::string response = client.SendCommand("SET api.default_limit = " + std::to_string(value));
          if (response.rfind("+OK", 0) != 0) {
            errors++;
          }
        }
      } catch (...) {
        errors++;
      }
    });
  }

  // Wait for all threads
  for (auto& thread : threads) {
    thread.join();
  }

  // No errors should occur
  EXPECT_EQ(errors, 0);

  // Final value should be valid
  TcpClient client("127.0.0.1", port_);
  std::string response = client.SendCommand("SHOW VARIABLES LIKE 'api.default_limit'");
  const auto name_pos = response.find("api.default_limit");
  ASSERT_NE(name_pos, std::string::npos) << "Response: " << response;
  const auto value_separator = response.find('|', name_pos);
  ASSERT_NE(value_separator, std::string::npos) << "Response: " << response;
  const auto value_begin = response.find_first_not_of(' ', value_separator + 1);
  ASSERT_NE(value_begin, std::string::npos) << "Response: " << response;
  const auto value_end = response.find_first_of(" |", value_begin);
  ASSERT_NE(value_end, std::string::npos) << "Response: " << response;

  const std::string value_text = response.substr(value_begin, value_end - value_begin);
  int final_value = 0;
  const auto [end, parse_error] =
      std::from_chars(value_text.data(), value_text.data() + value_text.size(), final_value);
  ASSERT_EQ(parse_error, std::errc()) << "Response: " << response;
  ASSERT_EQ(end, value_text.data() + value_text.size()) << "Response: " << response;
  EXPECT_GE(final_value, 50);
  EXPECT_LE(final_value, 99);
}
