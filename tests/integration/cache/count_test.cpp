/**
 * @file count_test.cpp
 * @brief Integration tests for COUNT query caching
 *
 * Test Coverage:
 * - COUNT query cache miss and hit
 * - COUNT cache with different search terms
 * - COUNT cache invalidation
 * - COUNT and SEARCH cache coexistence
 */

#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <sstream>
#include <thread>

#include "config/config.h"
#include "server/tcp_server.h"

using namespace mygramdb;
using namespace mygramdb::server;

/**
 * @brief Test fixture for COUNT cache integration tests
 */
class CacheCountIntegrationTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create index and document store
    auto index = std::make_unique<index::Index>(1);  // Unigram
    auto doc_store = std::make_unique<storage::DocumentStore>();

    // Setup table
    table_context_.name = "articles";
    table_context_.config.ngram_size = 1;
    table_context_.index = std::move(index);
    table_context_.doc_store = std::move(doc_store);

    // Insert test documents
    // 50 documents with "test"
    // 30 documents with "article"
    // 20 documents with both "test" and "article"
    for (int i = 1; i <= 50; ++i) {
      std::string doc_id = std::to_string(i);
      std::string text = "test document " + std::to_string(i);
      if (i <= 20) {
        text += " article";  // First 20 have both
      }
      auto internal_doc_id = *table_context_.doc_store->AddDocument(doc_id, {});
      table_context_.index->AddDocument(internal_doc_id, text);
    }

    for (int i = 51; i <= 60; ++i) {
      std::string doc_id = std::to_string(i);
      std::string text = "article only " + std::to_string(i);
      auto internal_doc_id = *table_context_.doc_store->AddDocument(doc_id, {});
      table_context_.index->AddDocument(internal_doc_id, text);
    }

    table_contexts_["articles"] = &table_context_;

    // Configure server with cache enabled
    config::Config full_config;
    full_config.cache.enabled = true;
    full_config.cache.max_memory_bytes = 10 * 1024 * 1024;  // 10MB
    full_config.cache.min_query_cost_ms = 0.0;              // Cache all queries for testing
    full_config_ = std::make_unique<config::Config>(full_config);

    // Create TCP server
    server_config_.port = 0;  // OS assigns port
    server_config_.host = "127.0.0.1";
    server_config_.allow_cidrs = {"127.0.0.1/32"};

    server_ = std::make_unique<TcpServer>(server_config_, table_contexts_, "./test_snapshots", full_config_.get());
    ASSERT_TRUE(server_->Start());

    port_ = server_->GetPort();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  void TearDown() override {
    if (server_ && server_->IsRunning()) {
      server_->Stop();
    }
  }

  int CreateClientSocket() {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
      return -1;
    }

    struct sockaddr_in server_addr;
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port_);
    inet_pton(AF_INET, "127.0.0.1", &server_addr.sin_addr);

    if (connect(sock, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
      close(sock);
      return -1;
    }

    return sock;
  }

  std::string SendCommand(int sock, const std::string& command) {
    std::string request = command + "\r\n";
    ssize_t sent = send(sock, request.c_str(), request.length(), 0);
    if (sent < 0) {
      return "";
    }

    char buffer[8192];
    ssize_t received = recv(sock, buffer, sizeof(buffer) - 1, 0);
    if (received <= 0) {
      return "";
    }

    buffer[received] = '\0';
    return std::string(buffer);
  }

  struct CountResult {
    uint64_t count = 0;
    bool success = false;
  };

  CountResult ParseCountResponse(const std::string& response) {
    CountResult result;

    if (response.find("OK COUNT") != 0) {
      return result;
    }

    std::istringstream iss(response);
    std::string ok, count_keyword;
    iss >> ok >> count_keyword >> result.count;

    result.success = true;
    return result;
  }

  TableContext table_context_;
  std::unordered_map<std::string, TableContext*> table_contexts_;
  ServerConfig server_config_;
  std::unique_ptr<config::Config> full_config_;
  std::unique_ptr<TcpServer> server_;
  uint16_t port_ = 0;
};

/**
 * @brief Test COUNT query cache miss
 */
TEST_F(CacheCountIntegrationTest, CountCacheMiss) {
  int sock = CreateClientSocket();
  ASSERT_GE(sock, 0);

  // First COUNT query (cache miss)
  auto response = SendCommand(sock, "COUNT articles test");
  auto result = ParseCountResponse(response);

  ASSERT_TRUE(result.success) << "Response: " << response;
  EXPECT_EQ(result.count, 50) << "Should find 50 documents with 'test'";

  close(sock);
}

/**
 * @brief Test COUNT query cache hit
 */
TEST_F(CacheCountIntegrationTest, CountCacheHit) {
  int sock = CreateClientSocket();
  ASSERT_GE(sock, 0);

  // First query (cache miss)
  auto response1 = SendCommand(sock, "COUNT articles test");
  auto result1 = ParseCountResponse(response1);
  ASSERT_TRUE(result1.success);
  EXPECT_EQ(result1.count, 50);

  // Verify cache was populated
  auto stats1 = SendCommand(sock, "CACHE STATS");
  EXPECT_TRUE(stats1.find("current_entries: 1") != std::string::npos) << "Cache should have 1 entry. Stats: " << stats1;

  // Second query (cache hit)
  auto response2 = SendCommand(sock, "COUNT articles test");
  auto result2 = ParseCountResponse(response2);
  ASSERT_TRUE(result2.success);
  EXPECT_EQ(result2.count, 50) << "Cache hit should return same count";

  // Verify cache hit occurred
  auto stats2 = SendCommand(sock, "CACHE STATS");
  EXPECT_TRUE(stats2.find("cache_hits: ") != std::string::npos);

  close(sock);
}

/**
 * @brief Test different search terms produce different counts
 */
TEST_F(CacheCountIntegrationTest, DifferentSearchTerms) {
  int sock = CreateClientSocket();
  ASSERT_GE(sock, 0);

  // Count "test"
  auto response1 = SendCommand(sock, "COUNT articles test");
  auto result1 = ParseCountResponse(response1);
  ASSERT_TRUE(result1.success);
  EXPECT_EQ(result1.count, 50);

  // Count "article"
  auto response2 = SendCommand(sock, "COUNT articles article");
  auto result2 = ParseCountResponse(response2);
  ASSERT_TRUE(result2.success);
  EXPECT_EQ(result2.count, 30);

  // Verify both are cached separately
  auto stats = SendCommand(sock, "CACHE STATS");
  EXPECT_TRUE(stats.find("current_entries: 2") != std::string::npos)
      << "Should have 2 separate cache entries. Stats: " << stats;

  close(sock);
}

/**
 * @brief Test COUNT cache clears correctly
 */
TEST_F(CacheCountIntegrationTest, CountCacheClear) {
  int sock = CreateClientSocket();
  ASSERT_GE(sock, 0);

  // Populate cache
  SendCommand(sock, "COUNT articles test");

  // Verify cache entry
  auto stats1 = SendCommand(sock, "CACHE STATS");
  EXPECT_TRUE(stats1.find("current_entries: 1") != std::string::npos);

  // Clear cache
  auto clear_response = SendCommand(sock, "CACHE CLEAR");
  EXPECT_TRUE(clear_response.find("OK") == 0);

  // Verify cache is empty
  auto stats2 = SendCommand(sock, "CACHE STATS");
  EXPECT_TRUE(stats2.find("current_entries: 0") != std::string::npos);

  // Next query should be cache miss
  auto response = SendCommand(sock, "COUNT articles test");
  auto result = ParseCountResponse(response);
  ASSERT_TRUE(result.success);
  EXPECT_EQ(result.count, 50);

  close(sock);
}

/**
 * @brief Test COUNT and SEARCH queries can coexist in cache
 */
TEST_F(CacheCountIntegrationTest, CountAndSearchCacheCoexistence) {
  int sock = CreateClientSocket();
  ASSERT_GE(sock, 0);

  // Execute COUNT query
  auto count_response = SendCommand(sock, "COUNT articles test");
  auto count_result = ParseCountResponse(count_response);
  ASSERT_TRUE(count_result.success);
  EXPECT_EQ(count_result.count, 50);

  // Execute SEARCH query with same search term
  auto search_response = SendCommand(sock, "SEARCH articles test LIMIT 10");
  EXPECT_TRUE(search_response.find("OK RESULTS 50") == 0);

  // Both should be cached separately
  auto stats = SendCommand(sock, "CACHE STATS");
  EXPECT_TRUE(stats.find("current_entries: 2") != std::string::npos)
      << "COUNT and SEARCH should be cached separately. Stats: " << stats;

  // Verify both can be retrieved from cache
  auto count_response2 = SendCommand(sock, "COUNT articles test");
  auto count_result2 = ParseCountResponse(count_response2);
  EXPECT_EQ(count_result2.count, 50);

  auto search_response2 = SendCommand(sock, "SEARCH articles test LIMIT 10");
  EXPECT_TRUE(search_response2.find("OK RESULTS 50") == 0);

  close(sock);
}

/**
 * @brief Test COUNT with cache disabled
 */
TEST_F(CacheCountIntegrationTest, CountWithCacheDisabled) {
  int sock = CreateClientSocket();
  ASSERT_GE(sock, 0);

  // Disable cache
  SendCommand(sock, "CACHE DISABLE");

  // Execute COUNT query
  auto response = SendCommand(sock, "COUNT articles test");
  auto result = ParseCountResponse(response);
  ASSERT_TRUE(result.success);
  EXPECT_EQ(result.count, 50);

  // Verify no cache entry was created
  auto stats = SendCommand(sock, "CACHE STATS");
  EXPECT_TRUE(stats.find("enabled: false") != std::string::npos);
  EXPECT_TRUE(stats.find("current_entries: 0") != std::string::npos);

  close(sock);
}
