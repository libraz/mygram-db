/**
 * @file tcp_server_test.cpp
 * @brief Unit tests for TCP server
 */

#include "server/tcp_server.h"

#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

#include <chrono>
#include <cstdio>
#include <fstream>
#include <sstream>
#include <thread>

#include "config/config.h"

using namespace mygramdb::server;
using namespace mygramdb;

/**
 * @brief Test fixture for TCP server tests
 */
class TcpServerTest : public ::testing::Test {
 protected:
  void SetUp() override {
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

    server_ = std::make_unique<TcpServer>(config_, table_contexts_);
  }

  void TearDown() override {
    if (server_ && server_->IsRunning()) {
      server_->Stop();
    }
  }

  // Helper to create client socket
  int CreateClientSocket(uint16_t port) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
      return -1;
    }

    struct sockaddr_in server_addr;
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    inet_pton(AF_INET, "127.0.0.1", &server_addr.sin_addr);

    if (connect(sock, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
      close(sock);
      return -1;
    }

    return sock;
  }

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
};

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
  EXPECT_TRUE(server_->Start());
  EXPECT_TRUE(server_->IsRunning());
  EXPECT_GT(server_->GetPort(), 0);

  server_->Stop();
  EXPECT_FALSE(server_->IsRunning());
}

/**
 * @brief Test double start
 */
TEST_F(TcpServerTest, DoubleStart) {
  EXPECT_TRUE(server_->Start());
  EXPECT_FALSE(server_->Start());  // Should fail
  EXPECT_TRUE(server_->IsRunning());
}

/**
 * @brief Test GET request for non-existent document
 */
TEST_F(TcpServerTest, GetNonExistent) {
  ASSERT_TRUE(server_->Start());
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
 * @brief Test SEARCH on empty index
 */
TEST_F(TcpServerTest, SearchEmpty) {
  ASSERT_TRUE(server_->Start());
  uint16_t port = server_->GetPort();

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  int sock = CreateClientSocket(port);
  ASSERT_GE(sock, 0);

  std::string response = SendRequest(sock, "SEARCH test test");
  EXPECT_EQ(response, "OK RESULTS 0");

  close(sock);
}

/**
 * @brief Test COUNT on empty index
 */
TEST_F(TcpServerTest, CountEmpty) {
  ASSERT_TRUE(server_->Start());
  uint16_t port = server_->GetPort();

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  int sock = CreateClientSocket(port);
  ASSERT_GE(sock, 0);

  std::string response = SendRequest(sock, "COUNT test test");
  EXPECT_EQ(response, "OK COUNT 0");

  close(sock);
}

/**
 * @brief Test SEARCH with documents
 */
TEST_F(TcpServerTest, SearchWithDocuments) {
  // Add documents
  auto doc_id1 = doc_store_->AddDocument("1", {});
  index_->AddDocument(static_cast<index::DocId>(doc_id1), "hello world");

  auto doc_id2 = doc_store_->AddDocument("2", {});
  index_->AddDocument(static_cast<index::DocId>(doc_id2), "hello there");

  ASSERT_TRUE(server_->Start());
  uint16_t port = server_->GetPort();

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  int sock = CreateClientSocket(port);
  ASSERT_GE(sock, 0);

  std::string response = SendRequest(sock, "SEARCH test hello");
  // Default SORT: PRIMARY KEY DESC (descending order: 2, 1)
  EXPECT_EQ(response, "OK RESULTS 2 2 1");

  close(sock);
}

/**
 * @brief Test COUNT with documents
 */
TEST_F(TcpServerTest, CountWithDocuments) {
  // Add documents
  auto doc_id1 = doc_store_->AddDocument("1", {});
  index_->AddDocument(static_cast<index::DocId>(doc_id1), "hello world");

  auto doc_id2 = doc_store_->AddDocument("2", {});
  index_->AddDocument(static_cast<index::DocId>(doc_id2), "hello there");

  ASSERT_TRUE(server_->Start());
  uint16_t port = server_->GetPort();

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  int sock = CreateClientSocket(port);
  ASSERT_GE(sock, 0);

  std::string response = SendRequest(sock, "COUNT test hello");
  EXPECT_EQ(response, "OK COUNT 2");

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

  ASSERT_TRUE(server_->Start());
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
 * @brief Test SEARCH with LIMIT
 */
TEST_F(TcpServerTest, SearchWithLimit) {
  // Add 5 documents
  for (int i = 1; i <= 5; i++) {
    auto doc_id = doc_store_->AddDocument(std::to_string(i), {});
    index_->AddDocument(static_cast<index::DocId>(doc_id), "test");
  }

  ASSERT_TRUE(server_->Start());
  uint16_t port = server_->GetPort();

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  int sock = CreateClientSocket(port);
  ASSERT_GE(sock, 0);

  std::string response = SendRequest(sock, "SEARCH test test LIMIT 3");
  // Default SORT: PRIMARY KEY DESC (descending order: 5, 4, 3)
  EXPECT_EQ(response, "OK RESULTS 5 5 4 3");

  close(sock);
}

/**
 * @brief Test SEARCH with OFFSET
 */
TEST_F(TcpServerTest, SearchWithOffset) {
  // Add 5 documents
  for (int i = 1; i <= 5; i++) {
    auto doc_id = doc_store_->AddDocument(std::to_string(i), {});
    index_->AddDocument(static_cast<index::DocId>(doc_id), "test");
  }

  ASSERT_TRUE(server_->Start());
  uint16_t port = server_->GetPort();

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  int sock = CreateClientSocket(port);
  ASSERT_GE(sock, 0);

  std::string response = SendRequest(sock, "SEARCH test test OFFSET 2");
  // Default SORT: PRIMARY KEY DESC (descending order: 5, 4, 3, 2, 1)
  // OFFSET 2 skips first 2 results (5, 4), returns: 3, 2, 1
  EXPECT_EQ(response, "OK RESULTS 5 3 2 1");

  close(sock);
}

/**
 * @brief Test SEARCH with NOT
 */
TEST_F(TcpServerTest, SearchWithNot) {
  // Add documents
  auto doc_id1 = doc_store_->AddDocument("1", {});
  index_->AddDocument(static_cast<index::DocId>(doc_id1), "abc xyz");

  auto doc_id2 = doc_store_->AddDocument("2", {});
  index_->AddDocument(static_cast<index::DocId>(doc_id2), "abc def");

  auto doc_id3 = doc_store_->AddDocument("3", {});
  index_->AddDocument(static_cast<index::DocId>(doc_id3), "ghi jkl");

  ASSERT_TRUE(server_->Start());
  uint16_t port = server_->GetPort();

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  int sock = CreateClientSocket(port);
  ASSERT_GE(sock, 0);

  // Search for documents containing 'a' but not containing 'x'
  // Should match doc_id2 only (has 'a' from "abc" but not 'x')
  std::string response = SendRequest(sock, "SEARCH test a NOT x");
  EXPECT_EQ(response, "OK RESULTS 1 2");

  close(sock);
}

/**
 * @brief Test SEARCH with AND operator
 */
TEST_F(TcpServerTest, SearchWithAnd) {
  // Add documents
  auto doc_id1 = doc_store_->AddDocument("1", {});
  index_->AddDocument(static_cast<index::DocId>(doc_id1), "abc xyz");

  auto doc_id2 = doc_store_->AddDocument("2", {});
  index_->AddDocument(static_cast<index::DocId>(doc_id2), "abc def");

  auto doc_id3 = doc_store_->AddDocument("3", {});
  index_->AddDocument(static_cast<index::DocId>(doc_id3), "xyz def");

  ASSERT_TRUE(server_->Start());
  uint16_t port = server_->GetPort();

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  int sock = CreateClientSocket(port);
  ASSERT_GE(sock, 0);

  // Search for documents containing both 'a' AND 'd'
  // Should match doc_id2 only (has both 'a' and 'd')
  std::string response = SendRequest(sock, "SEARCH test a AND d");
  EXPECT_EQ(response, "OK RESULTS 1 2");

  close(sock);
}

/**
 * @brief Test SEARCH with multiple AND operators
 */
TEST_F(TcpServerTest, SearchWithMultipleAnds) {
  // Add documents
  auto doc_id1 = doc_store_->AddDocument("1", {});
  index_->AddDocument(static_cast<index::DocId>(doc_id1), "abc xyz pqr");

  auto doc_id2 = doc_store_->AddDocument("2", {});
  index_->AddDocument(static_cast<index::DocId>(doc_id2), "abc def");

  auto doc_id3 = doc_store_->AddDocument("3", {});
  index_->AddDocument(static_cast<index::DocId>(doc_id3), "abc xyz");

  ASSERT_TRUE(server_->Start());
  uint16_t port = server_->GetPort();

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  int sock = CreateClientSocket(port);
  ASSERT_GE(sock, 0);

  // Search for documents containing 'a' AND 'x' AND 'p'
  // Should match doc_id1 only
  std::string response = SendRequest(sock, "SEARCH test a AND x AND p");
  EXPECT_EQ(response, "OK RESULTS 1 1");

  close(sock);
}

/**
 * @brief Test SEARCH with AND and NOT combined
 */
TEST_F(TcpServerTest, SearchWithAndAndNot) {
  // Add documents
  auto doc_id1 = doc_store_->AddDocument("1", {});
  index_->AddDocument(static_cast<index::DocId>(doc_id1), "abc xyz old");

  auto doc_id2 = doc_store_->AddDocument("2", {});
  index_->AddDocument(static_cast<index::DocId>(doc_id2), "abc xyz new");

  auto doc_id3 = doc_store_->AddDocument("3", {});
  index_->AddDocument(static_cast<index::DocId>(doc_id3), "abc def");

  ASSERT_TRUE(server_->Start());
  uint16_t port = server_->GetPort();

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  int sock = CreateClientSocket(port);
  ASSERT_GE(sock, 0);

  // Search for documents containing 'a' AND 'x' but NOT 'o'
  // Should match doc_id2 only
  std::string response = SendRequest(sock, "SEARCH test a AND x NOT o");
  EXPECT_EQ(response, "OK RESULTS 1 2");

  close(sock);
}

/**
 * @brief Test COUNT with AND operator
 */
TEST_F(TcpServerTest, CountWithAnd) {
  // Add documents
  auto doc_id1 = doc_store_->AddDocument("1", {});
  index_->AddDocument(static_cast<index::DocId>(doc_id1), "abc xyz");

  auto doc_id2 = doc_store_->AddDocument("2", {});
  index_->AddDocument(static_cast<index::DocId>(doc_id2), "abc def");

  auto doc_id3 = doc_store_->AddDocument("3", {});
  index_->AddDocument(static_cast<index::DocId>(doc_id3), "xyz def");

  ASSERT_TRUE(server_->Start());
  uint16_t port = server_->GetPort();

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  int sock = CreateClientSocket(port);
  ASSERT_GE(sock, 0);

  // Count documents containing both 'a' AND 'd'
  std::string response = SendRequest(sock, "COUNT test a AND d");
  EXPECT_EQ(response, "OK COUNT 1");

  close(sock);
}

/**
 * @brief Test SEARCH with quoted strings
 */
TEST_F(TcpServerTest, SearchWithQuotedString) {
  // Add documents
  auto doc_id1 = doc_store_->AddDocument("1", {});
  index_->AddDocument(static_cast<index::DocId>(doc_id1), "hello world");

  auto doc_id2 = doc_store_->AddDocument("2", {});
  index_->AddDocument(static_cast<index::DocId>(doc_id2), "hello");

  auto doc_id3 = doc_store_->AddDocument("3", {});
  index_->AddDocument(static_cast<index::DocId>(doc_id3), "world");

  ASSERT_TRUE(server_->Start());
  uint16_t port = server_->GetPort();

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  int sock = CreateClientSocket(port);
  ASSERT_GE(sock, 0);

  // Search for exact phrase "hello world"
  std::string response = SendRequest(sock, "SEARCH test \"hello world\"");
  EXPECT_EQ(response, "OK RESULTS 1 1");

  close(sock);
}

/**
 * @brief Test multiple requests on same connection
 */
TEST_F(TcpServerTest, MultipleRequests) {
  // Add document
  auto doc_id = doc_store_->AddDocument("1", {});
  index_->AddDocument(static_cast<index::DocId>(doc_id), "test");

  ASSERT_TRUE(server_->Start());
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

/**
 * @brief Test invalid command
 */
TEST_F(TcpServerTest, InvalidCommand) {
  ASSERT_TRUE(server_->Start());
  uint16_t port = server_->GetPort();

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  int sock = CreateClientSocket(port);
  ASSERT_GE(sock, 0);

  std::string response = SendRequest(sock, "INVALID");
  EXPECT_TRUE(response.find("ERROR") == 0);

  close(sock);
}

/**
 * @brief Test concurrent connections
 */
TEST_F(TcpServerTest, ConcurrentConnections) {
  // Add document
  auto doc_id = doc_store_->AddDocument("1", {});
  index_->AddDocument(static_cast<index::DocId>(doc_id), "test");

  ASSERT_TRUE(server_->Start());
  uint16_t port = server_->GetPort();

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // Create 3 concurrent clients
  std::vector<std::thread> threads;
  std::atomic<int> success_count{0};

  for (int i = 0; i < 3; i++) {
    threads.emplace_back([this, port, &success_count]() {
      int sock = CreateClientSocket(port);
      if (sock < 0)
        return;

      std::string response = SendRequest(sock, "COUNT test test");
      if (response == "OK COUNT 1") {
        success_count++;
      }

      close(sock);
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  EXPECT_EQ(success_count, 3);
  EXPECT_EQ(server_->GetTotalRequests(), 3);
}

/**
 * @brief Test INFO command
 */
TEST_F(TcpServerTest, InfoCommand) {
  ASSERT_TRUE(server_->Start());
  uint16_t port = server_->GetPort();

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  int sock = CreateClientSocket(port);
  ASSERT_GE(sock, 0);

  // Send INFO command
  std::string response = SendRequest(sock, "INFO");

  // Should return OK INFO with server statistics (Redis-style)
  EXPECT_TRUE(response.find("OK INFO") == 0);

  // Server section
  EXPECT_TRUE(response.find("# Server") != std::string::npos);
  EXPECT_TRUE(response.find("version:") != std::string::npos);
  EXPECT_TRUE(response.find("uptime_seconds:") != std::string::npos);

  // Stats section
  EXPECT_TRUE(response.find("# Stats") != std::string::npos);
  EXPECT_TRUE(response.find("total_commands_processed:") != std::string::npos);
  EXPECT_TRUE(response.find("total_requests:") != std::string::npos);

  // Commandstats section
  EXPECT_TRUE(response.find("# Commandstats") != std::string::npos);

  // Memory section
  EXPECT_TRUE(response.find("# Memory") != std::string::npos);
  EXPECT_TRUE(response.find("used_memory_bytes:") != std::string::npos);
  EXPECT_TRUE(response.find("used_memory_human:") != std::string::npos);

  // System memory information
  EXPECT_TRUE(response.find("total_system_memory:") != std::string::npos);
  EXPECT_TRUE(response.find("available_system_memory:") != std::string::npos);
  EXPECT_TRUE(response.find("system_memory_usage_ratio:") != std::string::npos);

  // Process memory information
  EXPECT_TRUE(response.find("process_rss:") != std::string::npos);
  EXPECT_TRUE(response.find("process_rss_peak:") != std::string::npos);

  // Memory health status
  EXPECT_TRUE(response.find("memory_health:") != std::string::npos);

  // Index section
  EXPECT_TRUE(response.find("# Index") != std::string::npos);
  EXPECT_TRUE(response.find("total_documents:") != std::string::npos);
  EXPECT_TRUE(response.find("total_terms:") != std::string::npos);
  EXPECT_TRUE(response.find("delta_encoded_lists:") != std::string::npos);
  EXPECT_TRUE(response.find("roaring_bitmap_lists:") != std::string::npos);

  // Clients section
  EXPECT_TRUE(response.find("# Clients") != std::string::npos);
  EXPECT_TRUE(response.find("connected_clients:") != std::string::npos);

  // Cache section (should show cache disabled when no cache manager)
  EXPECT_TRUE(response.find("# Cache") != std::string::npos);
  EXPECT_TRUE(response.find("cache_enabled: 0") != std::string::npos);

  EXPECT_TRUE(response.find("END") != std::string::npos);

  close(sock);
}

/**
 * @brief Test SAVE command
 */
TEST_F(TcpServerTest, DebugOn) {
  ASSERT_TRUE(server_->Start());
  uint16_t port = server_->GetPort();
  int sock = CreateClientSocket(port);
  ASSERT_GE(sock, 0);

  // Send DEBUG ON command
  std::string response = SendRequest(sock, "DEBUG ON");

  // Should return OK DEBUG_ON
  EXPECT_EQ(response, "OK DEBUG_ON");

  close(sock);
}

/**
 * @brief Test DEBUG OFF command
 */
TEST_F(TcpServerTest, DebugOff) {
  ASSERT_TRUE(server_->Start());
  uint16_t port = server_->GetPort();
  int sock = CreateClientSocket(port);
  ASSERT_GE(sock, 0);

  // Send DEBUG OFF command
  std::string response = SendRequest(sock, "DEBUG OFF");

  // Should return OK DEBUG_OFF
  EXPECT_EQ(response, "OK DEBUG_OFF");

  close(sock);
}

/**
 * @brief Test DEBUG mode with SEARCH command
 */
TEST_F(TcpServerTest, DebugModeWithSearch) {
  // Add test documents
  auto doc_id1 = doc_store_->AddDocument("100", {});
  auto doc_id2 = doc_store_->AddDocument("200", {});
  index_->AddDocument(doc_id1, "hello world");
  index_->AddDocument(doc_id2, "test data");

  ASSERT_TRUE(server_->Start());
  uint16_t port = server_->GetPort();
  int sock = CreateClientSocket(port);
  ASSERT_GE(sock, 0);

  // Enable debug mode
  std::string debug_on = SendRequest(sock, "DEBUG ON");
  EXPECT_EQ(debug_on, "OK DEBUG_ON");

  // Search with debug mode enabled
  std::string response = SendRequest(sock, "SEARCH test hello LIMIT 10");

  // Should contain results
  EXPECT_TRUE(response.find("OK RESULTS") == 0);

  // Should contain debug information (multi-line format)
  EXPECT_TRUE(response.find("# DEBUG") != std::string::npos);
  EXPECT_TRUE(response.find("query_time:") != std::string::npos);
  EXPECT_TRUE(response.find("index_time:") != std::string::npos);
  EXPECT_TRUE(response.find("terms:") != std::string::npos);
  EXPECT_TRUE(response.find("ngrams:") != std::string::npos);
  EXPECT_TRUE(response.find("candidates:") != std::string::npos);
  EXPECT_TRUE(response.find("final:") != std::string::npos);

  // Disable debug mode
  std::string debug_off = SendRequest(sock, "DEBUG OFF");
  EXPECT_EQ(debug_off, "OK DEBUG_OFF");

  // Search without debug mode
  std::string response2 = SendRequest(sock, "SEARCH test hello LIMIT 10");

  // Should contain results but NO debug info
  EXPECT_TRUE(response2.find("OK RESULTS") == 0);
  EXPECT_TRUE(response2.find("DEBUG") == std::string::npos);

  close(sock);
}

/**
 * @brief Test DEBUG mode is per-connection
 */
TEST_F(TcpServerTest, DebugModePerConnection) {
  // Add test document
  auto doc_id = doc_store_->AddDocument("100", {});
  index_->AddDocument(doc_id, "hello world");

  ASSERT_TRUE(server_->Start());
  uint16_t port = server_->GetPort();

  // Connection 1: Enable debug
  int sock1 = CreateClientSocket(port);
  ASSERT_GE(sock1, 0);
  std::string debug_on = SendRequest(sock1, "DEBUG ON");
  EXPECT_EQ(debug_on, "OK DEBUG_ON");

  // Connection 2: Debug should be off by default
  int sock2 = CreateClientSocket(port);
  ASSERT_GE(sock2, 0);

  // Search from connection 1 (debug enabled)
  std::string response1 = SendRequest(sock1, "SEARCH test hello LIMIT 10");
  EXPECT_TRUE(response1.find("DEBUG") != std::string::npos);

  // Search from connection 2 (debug disabled)
  std::string response2 = SendRequest(sock2, "SEARCH test hello LIMIT 10");
  EXPECT_TRUE(response2.find("DEBUG") == std::string::npos);

  close(sock1);
  close(sock2);
}

/**
 * @brief Test INFO command with table names
 */
TEST_F(TcpServerTest, InfoCommandWithTables) {
  // Create additional table contexts
  auto index2 = std::make_unique<index::Index>(1);
  auto doc_store2 = std::make_unique<storage::DocumentStore>();
  TableContext table_context2;
  table_context2.name = "users";
  table_context2.config.ngram_size = 1;
  table_context2.index = std::move(index2);
  table_context2.doc_store = std::move(doc_store2);

  auto index3 = std::make_unique<index::Index>(1);
  auto doc_store3 = std::make_unique<storage::DocumentStore>();
  TableContext table_context3;
  table_context3.name = "comments";
  table_context3.config.ngram_size = 1;
  table_context3.index = std::move(index3);
  table_context3.doc_store = std::move(doc_store3);

  // Add to table contexts
  std::unordered_map<std::string, TableContext*> multi_table_contexts;
  multi_table_contexts["test"] = &table_context_;
  multi_table_contexts["users"] = &table_context2;
  multi_table_contexts["comments"] = &table_context3;

  // Create a config with table information
  config::Config full_config;
  config::TableConfig table1;
  table1.name = "test";
  config::TableConfig table2;
  table2.name = "users";
  config::TableConfig table3;
  table3.name = "comments";
  full_config.tables.push_back(table1);
  full_config.tables.push_back(table2);
  full_config.tables.push_back(table3);

  // Create server with config
  auto server_with_config = std::make_unique<TcpServer>(config_, multi_table_contexts, "./snapshots", &full_config);

  ASSERT_TRUE(server_with_config->Start());
  uint16_t port = server_with_config->GetPort();

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  int sock = CreateClientSocket(port);
  ASSERT_GE(sock, 0);

  // Send INFO command
  std::string response = SendRequest(sock, "INFO");

  // Should return OK INFO
  EXPECT_TRUE(response.find("OK INFO") == 0);

  // Should contain Tables section
  EXPECT_TRUE(response.find("# Tables") != std::string::npos);

  // Should contain all table names (order not guaranteed with unordered_map)
  EXPECT_TRUE(response.find("tables: ") != std::string::npos);
  EXPECT_TRUE(response.find("test") != std::string::npos);
  EXPECT_TRUE(response.find("users") != std::string::npos);
  EXPECT_TRUE(response.find("comments") != std::string::npos);

  close(sock);
  server_with_config->Stop();
}

/**
 * @brief Test INFO command without tables (null config)
 */
TEST_F(TcpServerTest, InfoCommandWithoutTables) {
  // Server created in SetUp has nullptr for config
  ASSERT_TRUE(server_->Start());
  uint16_t port = server_->GetPort();

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  int sock = CreateClientSocket(port);
  ASSERT_GE(sock, 0);

  // Send INFO command
  std::string response = SendRequest(sock, "INFO");

  // Should return OK INFO
  EXPECT_TRUE(response.find("OK INFO") == 0);

  // Should contain Tables section (even if empty)
  EXPECT_TRUE(response.find("# Tables") != std::string::npos);

  // Should not crash when full_config_ is nullptr
  // The tables line should be omitted when config is null

  close(sock);
}

/**
 * @brief Test INFO command with single table
 */
TEST_F(TcpServerTest, InfoCommandWithSingleTable) {
  // Create a config with single table
  config::Config full_config;
  config::TableConfig table;
  table.name = "products";
  full_config.tables.push_back(table);

  // Create server with config
  auto server_with_config = std::make_unique<TcpServer>(config_, table_contexts_, "./snapshots", &full_config);

  ASSERT_TRUE(server_with_config->Start());
  uint16_t port = server_with_config->GetPort();

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  int sock = CreateClientSocket(port);
  ASSERT_GE(sock, 0);

  // Send INFO command
  std::string response = SendRequest(sock, "INFO");

  // Should return OK INFO
  EXPECT_TRUE(response.find("OK INFO") == 0);

  // Should contain table name from table_contexts (actual loaded tables)
  EXPECT_TRUE(response.find("tables: ") != std::string::npos);
  EXPECT_TRUE(response.find("test") != std::string::npos);

  close(sock);
  server_with_config->Stop();
}

/**
 * @brief Test that queries are blocked during LOAD
 */
TEST_F(TcpServerTest, HybridNgramSearchWithKanjiNgramSize) {
  // Set up index with hybrid n-gram configuration
  // ngram_size = 2 (for ASCII, hiragana, katakana)
  // kanji_ngram_size = 1 (for kanji)
  table_context_.config.ngram_size = 2;
  table_context_.config.kanji_ngram_size = 1;

  // Recreate index with hybrid configuration
  table_context_.index = std::make_unique<index::Index>(2, 1);
  index_ = table_context_.index.get();

  // Add test documents with Japanese text
  // Document 1: Contains kanji "東"
  doc_store_->AddDocument("1", {});
  index_->AddDocument(1, "東京タワー");  // Tokyo Tower (all kanji)

  // Document 2: Contains kanji "料"
  doc_store_->AddDocument("2", {});
  index_->AddDocument(2, "日本料理");  // Japanese cuisine (all kanji)

  // Document 3: Contains hiragana "ひまわり"
  doc_store_->AddDocument("3", {});
  index_->AddDocument(3, "ひまわり畑");  // sunflower field (hiragana + kanji)

  // Document 4: Contains same kanji as doc 1
  doc_store_->AddDocument("4", {});
  index_->AddDocument(4, "東北地方");  // Tohoku region (kanji only)

  // Start server
  ASSERT_TRUE(server_->Start());
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  uint16_t port = server_->GetPort();
  ASSERT_GT(port, 0);

  // Test 1: Search for single kanji "東" (should use unigram)
  int sock1 = CreateClientSocket(port);
  ASSERT_GE(sock1, 0);

  std::string response1 = SendRequest(sock1, "SEARCH test 東");
  EXPECT_TRUE(response1.find("OK") == 0) << "Response: " << response1;

  // Should find documents 1 and 4 (both contain "東")
  // Parse IDs from response: "OK RESULTS <count> <id1> <id2> ..."
  std::istringstream iss1(response1);
  std::string ok, results;
  int count;
  iss1 >> ok >> results >> count;  // Skip "OK RESULTS <count>"
  std::vector<int> ids1;
  int id;
  while (iss1 >> id)
    ids1.push_back(id);

  EXPECT_TRUE(std::find(ids1.begin(), ids1.end(), 1) != ids1.end()) << "Doc 1 not found";
  EXPECT_TRUE(std::find(ids1.begin(), ids1.end(), 4) != ids1.end()) << "Doc 4 not found";
  EXPECT_TRUE(std::find(ids1.begin(), ids1.end(), 2) == ids1.end()) << "Doc 2 should not match";
  EXPECT_TRUE(std::find(ids1.begin(), ids1.end(), 3) == ids1.end()) << "Doc 3 should not match";

  close(sock1);

  // Test 2: Search for single kanji "料" (should use unigram)
  int sock2 = CreateClientSocket(port);
  ASSERT_GE(sock2, 0);

  std::string response2 = SendRequest(sock2, "SEARCH test 料");
  EXPECT_TRUE(response2.find("OK") == 0) << "Response: " << response2;

  // Should find only document 2
  // Parse IDs from response: "OK RESULTS <count> <id1> <id2> ..."
  std::istringstream iss2(response2);
  std::string ok2, results2;
  int count2;
  iss2 >> ok2 >> results2 >> count2;  // Skip "OK RESULTS <count>"
  std::vector<int> ids2;
  int id2;
  while (iss2 >> id2)
    ids2.push_back(id2);

  EXPECT_TRUE(std::find(ids2.begin(), ids2.end(), 2) != ids2.end()) << "Doc 2 not found";
  EXPECT_TRUE(std::find(ids2.begin(), ids2.end(), 1) == ids2.end()) << "Doc 1 should not match";
  EXPECT_TRUE(std::find(ids2.begin(), ids2.end(), 3) == ids2.end()) << "Doc 3 should not match";
  EXPECT_TRUE(std::find(ids2.begin(), ids2.end(), 4) == ids2.end()) << "Doc 4 should not match";

  close(sock2);

  // Test 3: Search for hiragana "ひまわり" (should use bigram)
  int sock3 = CreateClientSocket(port);
  ASSERT_GE(sock3, 0);

  std::string response3 = SendRequest(sock3, "SEARCH test ひまわり");
  EXPECT_TRUE(response3.find("OK") == 0) << "Response: " << response3;

  // Should find only document 3
  // Parse IDs from response: "OK RESULTS <count> <id1> <id2> ..."
  std::istringstream iss3(response3);
  std::string ok3, results3;
  int count3;
  iss3 >> ok3 >> results3 >> count3;  // Skip "OK RESULTS <count>"
  std::vector<int> ids3;
  int id3;
  while (iss3 >> id3)
    ids3.push_back(id3);

  EXPECT_TRUE(std::find(ids3.begin(), ids3.end(), 3) != ids3.end()) << "Doc 3 not found";
  EXPECT_TRUE(std::find(ids3.begin(), ids3.end(), 1) == ids3.end()) << "Doc 1 should not match";
  EXPECT_TRUE(std::find(ids3.begin(), ids3.end(), 2) == ids3.end()) << "Doc 2 should not match";
  EXPECT_TRUE(std::find(ids3.begin(), ids3.end(), 4) == ids3.end()) << "Doc 4 should not match";

  close(sock3);

  // Test 4: Search for mixed text "東京" (both kanji, should use unigrams)
  int sock4 = CreateClientSocket(port);
  ASSERT_GE(sock4, 0);

  std::string response4 = SendRequest(sock4, "SEARCH test 東京");
  EXPECT_TRUE(response4.find("OK") == 0) << "Response: " << response4;

  // Should find only document 1 (contains both "東" and "京")
  // Parse IDs from response: "OK RESULTS <count> <id1> <id2> ..."
  std::istringstream iss4(response4);
  std::string ok4, results4;
  int count4;
  iss4 >> ok4 >> results4 >> count4;  // Skip "OK RESULTS <count>"
  std::vector<int> ids4;
  int id4;
  while (iss4 >> id4)
    ids4.push_back(id4);

  EXPECT_TRUE(std::find(ids4.begin(), ids4.end(), 1) != ids4.end()) << "Doc 1 not found";
  EXPECT_TRUE(std::find(ids4.begin(), ids4.end(), 2) == ids4.end()) << "Doc 2 should not match";
  EXPECT_TRUE(std::find(ids4.begin(), ids4.end(), 3) == ids4.end()) << "Doc 3 should not match";
  EXPECT_TRUE(std::find(ids4.begin(), ids4.end(), 4) == ids4.end()) << "Doc 4 should not match";

  close(sock4);
}

/**
 * @brief Test INFO command includes replication statistics
 */
TEST_F(TcpServerTest, InfoCommandReplicationStatistics) {
  ASSERT_TRUE(server_->Start());

  // Get server statistics and increment some replication counters
  ServerStats* stats = server_->GetMutableStats();
  ASSERT_NE(stats, nullptr);

  stats->IncrementReplInsertApplied();
  stats->IncrementReplInsertApplied();
  stats->IncrementReplInsertSkipped();

  stats->IncrementReplUpdateAdded();
  stats->IncrementReplUpdateRemoved();
  stats->IncrementReplUpdateModified();
  stats->IncrementReplUpdateSkipped();

  stats->IncrementReplDeleteApplied();
  stats->IncrementReplDeleteSkipped();

  stats->IncrementReplDdlExecuted();
  stats->IncrementReplEventsSkippedOtherTables();

  int port = server_->GetPort();
  int sock = CreateClientSocket(port);
  ASSERT_GE(sock, 0);

  // Send INFO command
  std::string response = SendRequest(sock, "INFO");

  // Should return OK INFO
  EXPECT_TRUE(response.find("OK INFO") == 0);

  // Check if replication statistics are included
  EXPECT_TRUE(response.find("replication_inserts_applied: 2") != std::string::npos);
  EXPECT_TRUE(response.find("replication_inserts_skipped: 1") != std::string::npos);
  EXPECT_TRUE(response.find("replication_updates_applied: 3") != std::string::npos);  // Added + Removed + Modified
  EXPECT_TRUE(response.find("replication_updates_added: 1") != std::string::npos);
  EXPECT_TRUE(response.find("replication_updates_removed: 1") != std::string::npos);
  EXPECT_TRUE(response.find("replication_updates_modified: 1") != std::string::npos);
  EXPECT_TRUE(response.find("replication_updates_skipped: 1") != std::string::npos);
  EXPECT_TRUE(response.find("replication_deletes_applied: 1") != std::string::npos);
  EXPECT_TRUE(response.find("replication_deletes_skipped: 1") != std::string::npos);
  EXPECT_TRUE(response.find("replication_ddl_executed: 1") != std::string::npos);
  EXPECT_TRUE(response.find("replication_events_skipped_other_tables: 1") != std::string::npos);

  close(sock);
}

/**
 * @brief Test INFO command replication statistics initially zero
 */
TEST_F(TcpServerTest, InfoCommandReplicationStatisticsInitiallyZero) {
  ASSERT_TRUE(server_->Start());

  int port = server_->GetPort();
  int sock = CreateClientSocket(port);
  ASSERT_GE(sock, 0);

  // Send INFO command
  std::string response = SendRequest(sock, "INFO");

  // Should return OK INFO
  EXPECT_TRUE(response.find("OK INFO") == 0);

  // All replication statistics should be 0 initially
  EXPECT_TRUE(response.find("replication_inserts_applied: 0") != std::string::npos);
  EXPECT_TRUE(response.find("replication_inserts_skipped: 0") != std::string::npos);
  EXPECT_TRUE(response.find("replication_updates_applied: 0") != std::string::npos);
  EXPECT_TRUE(response.find("replication_updates_added: 0") != std::string::npos);
  EXPECT_TRUE(response.find("replication_updates_removed: 0") != std::string::npos);
  EXPECT_TRUE(response.find("replication_updates_modified: 0") != std::string::npos);
  EXPECT_TRUE(response.find("replication_updates_skipped: 0") != std::string::npos);
  EXPECT_TRUE(response.find("replication_deletes_applied: 0") != std::string::npos);
  EXPECT_TRUE(response.find("replication_deletes_skipped: 0") != std::string::npos);
  EXPECT_TRUE(response.find("replication_ddl_executed: 0") != std::string::npos);
  EXPECT_TRUE(response.find("replication_events_skipped_other_tables: 0") != std::string::npos);

  close(sock);
}

/**
 * @brief Test debug output shows (default) marker for implicit parameters
 */
TEST_F(TcpServerTest, DebugModeDefaultParameterMarkers) {
  // Add test documents
  auto doc_id1 = doc_store_->AddDocument("100", {});
  index_->AddDocument(doc_id1, "hello world");
  auto doc_id2 = doc_store_->AddDocument("101", {});
  index_->AddDocument(doc_id2, "hello universe");

  ASSERT_TRUE(server_->Start());
  uint16_t port = server_->GetPort();

  int sock = CreateClientSocket(port);
  ASSERT_GE(sock, 0);

  // Enable debug mode
  std::string debug_on = SendRequest(sock, "DEBUG ON");
  EXPECT_EQ(debug_on, "OK DEBUG_ON");

  // Test 1: Search without explicit LIMIT, OFFSET, or SORT
  // Should show all as (default)
  std::string response1 = SendRequest(sock, "SEARCH test hello");
  EXPECT_TRUE(response1.find("OK RESULTS") == 0);
  EXPECT_TRUE(response1.find("# DEBUG") != std::string::npos);
  EXPECT_TRUE(response1.find("sort: id DESC (default)") != std::string::npos)
      << "Should show default SORT with (default) marker";
  EXPECT_TRUE(response1.find("limit: 100 (default)") != std::string::npos)
      << "Should show default LIMIT with (default) marker";
  // OFFSET should not be shown when it's 0
  EXPECT_TRUE(response1.find("offset:") == std::string::npos) << "OFFSET should not be shown when 0";

  // Test 2: Search with explicit LIMIT
  // LIMIT should NOT have (default), but SORT should
  std::string response2 = SendRequest(sock, "SEARCH test hello LIMIT 50");
  EXPECT_TRUE(response2.find("OK RESULTS") == 0);
  EXPECT_TRUE(response2.find("sort: id DESC (default)") != std::string::npos)
      << "SORT should still have (default) marker";
  EXPECT_TRUE(response2.find("limit: 50\r\n") != std::string::npos)
      << "Explicit LIMIT should NOT have (default) marker";
  EXPECT_TRUE(response2.find("limit: 50 (default)") == std::string::npos)
      << "Explicit LIMIT should NOT have (default) marker";

  // Test 3: Search with explicit SORT
  // SORT should NOT have (default), but LIMIT should
  std::string response3 = SendRequest(sock, "SEARCH test hello SORT id ASC");
  EXPECT_TRUE(response3.find("OK RESULTS") == 0);
  EXPECT_TRUE(response3.find("sort: id ASC\r\n") != std::string::npos)
      << "Explicit SORT should NOT have (default) marker";
  EXPECT_TRUE(response3.find("sort: id ASC (default)") == std::string::npos)
      << "Explicit SORT should NOT have (default) marker";
  EXPECT_TRUE(response3.find("limit: 100 (default)") != std::string::npos)
      << "Default LIMIT should have (default) marker";

  // Test 4: Search with explicit OFFSET
  // OFFSET should NOT have (default) when explicitly set
  std::string response4 = SendRequest(sock, "SEARCH test hello OFFSET 10");
  EXPECT_TRUE(response4.find("OK RESULTS") == 0);
  EXPECT_TRUE(response4.find("offset: 10\r\n") != std::string::npos)
      << "Explicit OFFSET should NOT have (default) marker";
  EXPECT_TRUE(response4.find("offset: 10 (default)") == std::string::npos)
      << "Explicit OFFSET should NOT have (default) marker";

  // Test 5: Search with all explicit parameters
  // Nothing should have (default)
  std::string response5 = SendRequest(sock, "SEARCH test hello SORT id DESC LIMIT 25 OFFSET 5");
  EXPECT_TRUE(response5.find("OK RESULTS") == 0);
  EXPECT_TRUE(response5.find("sort: id DESC\r\n") != std::string::npos);
  EXPECT_TRUE(response5.find("(default)") == std::string::npos)
      << "No parameters should have (default) when all are explicit";
  EXPECT_TRUE(response5.find("limit: 25\r\n") != std::string::npos);
  EXPECT_TRUE(response5.find("offset: 5\r\n") != std::string::npos);

  close(sock);
}

/**
 * @brief Test optimization strategy selection based on result set size and LIMIT
 *
 * This test verifies that the server correctly chooses between:
 * - GetTopN optimization (for large result sets with small LIMIT)
 * - reuse-fetch optimization (for small result sets or high LIMIT ratio)
 */
TEST_F(TcpServerTest, OptimizationStrategySelection) {
  // Prepare test data: insert documents with varying characteristics
  // Small result set: 10 documents with "small"
  // Large result set: 1000 documents with "large"

  for (int i = 1; i <= 10; ++i) {
    auto doc_id = doc_store_->AddDocument(std::to_string(i), {});
    index_->AddDocument(static_cast<index::DocId>(doc_id), "small unique text");
  }

  for (int i = 11; i <= 1010; ++i) {
    auto doc_id = doc_store_->AddDocument(std::to_string(i), {});
    index_->AddDocument(static_cast<index::DocId>(doc_id), "large dataset text");
  }

  ASSERT_TRUE(server_->Start());
  uint16_t port = server_->GetPort();
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  int sock = CreateClientSocket(port);
  ASSERT_GE(sock, 0);

  // Enable debug mode to see optimization strategy
  std::string debug_response = SendRequest(sock, "DEBUG ON");
  EXPECT_EQ(debug_response, "OK DEBUG_ON");

  // Test 1: Small result set (10 docs) with small LIMIT (2 docs = 20%)
  // Expected: GetTopN optimization (ratio 20% < 50%)
  std::string response1 = SendRequest(sock, "SEARCH test small LIMIT 2");
  EXPECT_TRUE(response1.find("OK RESULTS 10") == 0) << "Should return total of 10 matching documents";
  EXPECT_TRUE(response1.find("optimization: Index GetTopN") != std::string::npos ||
              response1.find("optimization: reuse-fetch") != std::string::npos)
      << "Should use GetTopN or reuse-fetch optimization";

  // Test 2: Small result set (10 docs) with high LIMIT (9 docs = 90%)
  // Expected: reuse-fetch optimization (ratio 90% > 50%)
  std::string response2 = SendRequest(sock, "SEARCH test small LIMIT 9");
  EXPECT_TRUE(response2.find("OK RESULTS 10") == 0)
      << "Should return total of 10 matching documents. Response: " << response2;
  EXPECT_TRUE(response2.find("optimization: reuse-fetch") != std::string::npos)
      << "Should use reuse-fetch optimization for high LIMIT ratio (90% > 50%). Response: " << response2;

  // Test 3: Large result set (1000 docs) with small LIMIT (10 docs = 1%)
  // Expected: GetTopN optimization (ratio 1% < 50%)
  std::string response3 = SendRequest(sock, "SEARCH test large LIMIT 10");
  EXPECT_TRUE(response3.find("OK RESULTS 1000") == 0) << "Should return total of 1000 matching documents";
  EXPECT_TRUE(response3.find("optimization: Index GetTopN") != std::string::npos)
      << "Should use GetTopN optimization for low LIMIT ratio (1% < 50%)";

  // Test 4: Large result set (1000 docs) with high LIMIT (600 docs = 60%)
  // Expected: reuse-fetch optimization (ratio 60% > 50%)
  std::string response4 = SendRequest(sock, "SEARCH test large LIMIT 600");
  EXPECT_TRUE(response4.find("OK RESULTS 1000") == 0) << "Should return total of 1000 matching documents";
  EXPECT_TRUE(response4.find("optimization: reuse-fetch") != std::string::npos)
      << "Should use reuse-fetch optimization for high LIMIT ratio (60% > 50%)";

  // Test 5: Verify total_results accuracy with optimization
  // Even when using GetTopN, total_results should be accurate (not limited)
  std::string response5 = SendRequest(sock, "SEARCH test large LIMIT 5");
  EXPECT_TRUE(response5.find("OK RESULTS 1000") == 0) << "Total results should be 1000 (accurate count), not 5 (LIMIT)";

  // Count number of IDs in response (should be 5, not 1000)
  size_t id_count = 0;
  size_t pos = response5.find("OK RESULTS 1000");
  if (pos != std::string::npos) {
    std::string ids_part = response5.substr(pos + 16);  // Skip "OK RESULTS 1000 "
    std::istringstream iss(ids_part);
    std::string id;
    while (iss >> id && id_count < 100) {  // Limit check to avoid infinite loop
      if (id.find("\r") != std::string::npos || id.find("#") != std::string::npos) {
        break;  // Stop at debug section
      }
      id_count++;
    }
  }
  EXPECT_EQ(id_count, 5) << "Should return exactly 5 document IDs (LIMIT applied)";

  close(sock);
}

/**
 * @brief Test that COUNT and SEARCH return consistent total results
 */
TEST_F(TcpServerTest, CountSearchConsistency) {
  // Insert test documents
  for (int i = 1; i <= 100; ++i) {
    auto doc_id = doc_store_->AddDocument(std::to_string(i), {});
    index_->AddDocument(static_cast<index::DocId>(doc_id), "test document");
  }

  ASSERT_TRUE(server_->Start());
  uint16_t port = server_->GetPort();
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  int sock = CreateClientSocket(port);
  ASSERT_GE(sock, 0);

  // Get COUNT
  std::string count_response = SendRequest(sock, "COUNT test test");
  EXPECT_TRUE(count_response.find("OK COUNT 100") == 0) << "COUNT should return 100";

  // Get SEARCH total (with small LIMIT)
  std::string search_response = SendRequest(sock, "SEARCH test test LIMIT 10");
  EXPECT_TRUE(search_response.find("OK RESULTS 100") == 0) << "SEARCH total_results should match COUNT (100)";

  // Get SEARCH total (with large LIMIT)
  std::string search_response2 = SendRequest(sock, "SEARCH test test LIMIT 90");
  EXPECT_TRUE(search_response2.find("OK RESULTS 100") == 0)
      << "SEARCH total_results should be consistent regardless of LIMIT";

  close(sock);
}
