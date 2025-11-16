/**
 * @file pagination_test.cpp
 * @brief Integration tests for cache behavior with OFFSET/LIMIT pagination
 *
 * This test suite verifies the fix for the bug where cache hits would return
 * unpaginated results, ignoring OFFSET and LIMIT parameters.
 *
 * Test Coverage:
 * - Cache miss with OFFSET/LIMIT
 * - Cache hit with OFFSET/LIMIT (critical bug fix verification)
 * - Multiple pagination combinations
 * - Cache statistics tracking
 * - Edge cases (offset beyond results, limit exceeding results)
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
 * @brief Test fixture for cache OFFSET/LIMIT integration tests
 */
class CacheOffsetLimitIntegrationTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create index and document store
    auto index = std::make_unique<index::Index>(1);  // Unigram
    auto doc_store = std::make_unique<storage::DocumentStore>();

    // Setup table
    table_context_.name = "test";
    table_context_.config.ngram_size = 1;
    table_context_.index = std::move(index);
    table_context_.doc_store = std::move(doc_store);

    // Insert 100 test documents
    for (int i = 1; i <= 100; ++i) {
      std::string doc_id = std::to_string(i);
      std::string text = "test document " + std::to_string(i);
      auto internal_doc_id = table_context_.doc_store->AddDocument(doc_id, {});
      table_context_.index->AddDocument(internal_doc_id, text);
    }

    table_contexts_["test"] = &table_context_;

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

  struct SearchResult {
    size_t total_count = 0;
    std::vector<std::string> ids;
    bool success = false;
  };

  SearchResult ParseSearchResponse(const std::string& response) {
    SearchResult result;

    if (response.find("OK RESULTS") != 0) {
      return result;
    }

    std::istringstream iss(response);
    std::string ok, results_keyword;
    iss >> ok >> results_keyword >> result.total_count;

    std::string id;
    while (iss >> id) {
      // Stop at debug section or end markers
      if (id.find('#') == 0 || id == "END") {
        break;
      }
      result.ids.push_back(id);
    }

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
 * @brief Test that cache miss respects OFFSET/LIMIT
 */
TEST_F(CacheOffsetLimitIntegrationTest, CacheMissRespectsOffsetLimit) {
  int sock = CreateClientSocket();
  ASSERT_GE(sock, 0);

  // First query (cache miss): OFFSET 10 LIMIT 5
  auto response = SendCommand(sock, "SEARCH test test OFFSET 10 LIMIT 5");
  auto result = ParseSearchResponse(response);

  ASSERT_TRUE(result.success) << "Response: " << response;
  EXPECT_EQ(result.total_count, 100);  // Total 100 documents
  EXPECT_EQ(result.ids.size(), 5) << "Should return exactly 5 results with LIMIT 5";

  close(sock);
}

/**
 * @brief Test that cache HIT respects OFFSET/LIMIT (this is the critical bug fix test)
 */
TEST_F(CacheOffsetLimitIntegrationTest, CacheHitRespectsOffsetLimit) {
  int sock = CreateClientSocket();
  ASSERT_GE(sock, 0);

  // First query: populate cache with OFFSET 0 LIMIT 100
  auto response1 = SendCommand(sock, "SEARCH test test OFFSET 0 LIMIT 100");
  auto result1 = ParseSearchResponse(response1);
  ASSERT_TRUE(result1.success);
  EXPECT_EQ(result1.total_count, 100);
  EXPECT_EQ(result1.ids.size(), 100);

  // Second query: cache hit with different OFFSET/LIMIT
  auto response2 = SendCommand(sock, "SEARCH test test OFFSET 10 LIMIT 5");
  auto result2 = ParseSearchResponse(response2);

  ASSERT_TRUE(result2.success) << "Response: " << response2;
  EXPECT_EQ(result2.total_count, 100);
  // BUG FIX: Before fix, this would return 100 IDs. After fix, should return 5.
  EXPECT_EQ(result2.ids.size(), 5) << "Cache hit should respect LIMIT 5, got " << result2.ids.size() << " results";

  close(sock);
}

/**
 * @brief Test multiple OFFSET/LIMIT combinations on same cached query
 */
TEST_F(CacheOffsetLimitIntegrationTest, MultiplePaginationCombinations) {
  int sock = CreateClientSocket();
  ASSERT_GE(sock, 0);

  // Populate cache
  auto initial_response = SendCommand(sock, "SEARCH test test");
  auto initial_result = ParseSearchResponse(initial_response);
  ASSERT_TRUE(initial_result.success);

  // Test various OFFSET/LIMIT combinations
  struct TestCase {
    int offset;
    int limit;
    size_t expected_count;
  };

  std::vector<TestCase> test_cases = {
      {0, 10, 10},   // First 10
      {10, 5, 5},    // Items 11-15
      {50, 20, 20},  // Items 51-70
      {90, 20, 10},  // Items 91-100 (only 10 remaining)
      {100, 10, 0},  // Beyond range
      {95, 10, 5},   // Partial range at end
  };

  for (const auto& test_case : test_cases) {
    std::ostringstream cmd;
    cmd << "SEARCH test test OFFSET " << test_case.offset << " LIMIT " << test_case.limit;

    auto response = SendCommand(sock, cmd.str());
    auto result = ParseSearchResponse(response);

    ASSERT_TRUE(result.success) << "Command: " << cmd.str() << "\nResponse: " << response;
    EXPECT_EQ(result.total_count, 100) << "Command: " << cmd.str();
    EXPECT_EQ(result.ids.size(), test_case.expected_count)
        << "Command: " << cmd.str() << " - Expected " << test_case.expected_count << " results, got "
        << result.ids.size();
  }

  close(sock);
}

/**
 * @brief Test cache statistics are correctly updated
 */
TEST_F(CacheOffsetLimitIntegrationTest, CacheStatisticsTracking) {
  int sock = CreateClientSocket();
  ASSERT_GE(sock, 0);

  // First query (cache miss)
  SendCommand(sock, "SEARCH test test OFFSET 0 LIMIT 10");

  // Check cache stats
  auto stats_response1 = SendCommand(sock, "CACHE STATS");
  EXPECT_TRUE(stats_response1.find("cache_misses: 1") != std::string::npos)
      << "Should have 1 cache miss. Response: " << stats_response1;
  EXPECT_TRUE(stats_response1.find("cache_hits: 0") != std::string::npos)
      << "Should have 0 cache hits. Response: " << stats_response1;

  // Second query with different OFFSET/LIMIT (should be cache hit or miss depending on implementation)
  SendCommand(sock, "SEARCH test test OFFSET 10 LIMIT 5");

  // Third query with same parameters as second (should definitely be cache hit if caching OFFSET/LIMIT separately)
  SendCommand(sock, "SEARCH test test OFFSET 10 LIMIT 5");

  auto stats_response2 = SendCommand(sock, "CACHE STATS");
  // At minimum, we should have increased total queries
  EXPECT_TRUE(stats_response2.find("total_queries:") != std::string::npos);

  close(sock);
}

/**
 * @brief Test edge case: OFFSET beyond available results
 */
TEST_F(CacheOffsetLimitIntegrationTest, OffsetBeyondResults) {
  int sock = CreateClientSocket();
  ASSERT_GE(sock, 0);

  // Populate cache
  SendCommand(sock, "SEARCH test test");

  // Query with OFFSET beyond available results
  auto response = SendCommand(sock, "SEARCH test test OFFSET 200 LIMIT 10");
  auto result = ParseSearchResponse(response);

  ASSERT_TRUE(result.success);
  EXPECT_EQ(result.total_count, 100);
  EXPECT_EQ(result.ids.size(), 0) << "Should return 0 results when OFFSET is beyond available results";

  close(sock);
}

/**
 * @brief Test edge case: LIMIT exceeding available results
 */
TEST_F(CacheOffsetLimitIntegrationTest, LimitExceedingResults) {
  int sock = CreateClientSocket();
  ASSERT_GE(sock, 0);

  // Populate cache
  SendCommand(sock, "SEARCH test test");

  // Query with LIMIT larger than available results
  auto response = SendCommand(sock, "SEARCH test test OFFSET 0 LIMIT 1000");
  auto result = ParseSearchResponse(response);

  ASSERT_TRUE(result.success);
  EXPECT_EQ(result.total_count, 100);
  EXPECT_EQ(result.ids.size(), 100) << "Should return all 100 results when LIMIT exceeds available results";

  close(sock);
}

/**
 * @brief Test cache invalidation after CACHE CLEAR
 */
TEST_F(CacheOffsetLimitIntegrationTest, CacheClearInvalidation) {
  int sock = CreateClientSocket();
  ASSERT_GE(sock, 0);

  // Populate cache
  auto response1 = SendCommand(sock, "SEARCH test test OFFSET 0 LIMIT 10");
  auto result1 = ParseSearchResponse(response1);
  ASSERT_TRUE(result1.success);

  // Verify cache has entries
  auto stats1 = SendCommand(sock, "CACHE STATS");
  EXPECT_TRUE(stats1.find("current_entries: 1") != std::string::npos) << "Cache should have 1 entry. Stats: " << stats1;

  // Clear cache
  auto clear_response = SendCommand(sock, "CACHE CLEAR");
  EXPECT_TRUE(clear_response.find("OK") == 0) << "CACHE CLEAR should succeed";

  // Verify cache is empty
  auto stats2 = SendCommand(sock, "CACHE STATS");
  EXPECT_TRUE(stats2.find("current_entries: 0") != std::string::npos)
      << "Cache should be empty after CLEAR. Stats: " << stats2;

  // Next query should be cache miss
  auto response2 = SendCommand(sock, "SEARCH test test OFFSET 0 LIMIT 10");
  auto result2 = ParseSearchResponse(response2);
  ASSERT_TRUE(result2.success);
  EXPECT_EQ(result2.ids.size(), 10);

  close(sock);
}

/**
 * @brief Test cache ENABLE/DISABLE commands
 */
TEST_F(CacheOffsetLimitIntegrationTest, CacheEnableDisable) {
  int sock = CreateClientSocket();
  ASSERT_GE(sock, 0);

  // Populate cache
  SendCommand(sock, "SEARCH test test OFFSET 0 LIMIT 10");

  // Verify cache is enabled
  auto stats1 = SendCommand(sock, "CACHE STATS");
  EXPECT_TRUE(stats1.find("enabled: true") != std::string::npos);
  EXPECT_TRUE(stats1.find("current_entries: 1") != std::string::npos);

  // Disable cache
  auto disable_response = SendCommand(sock, "CACHE DISABLE");
  EXPECT_TRUE(disable_response.find("OK") == 0);

  // Verify cache is disabled
  auto stats2 = SendCommand(sock, "CACHE STATS");
  EXPECT_TRUE(stats2.find("enabled: false") != std::string::npos);

  // Query should not hit cache (cache disabled)
  auto response = SendCommand(sock, "SEARCH test test OFFSET 0 LIMIT 10");
  auto result = ParseSearchResponse(response);
  ASSERT_TRUE(result.success);
  EXPECT_EQ(result.ids.size(), 10);

  // Re-enable cache
  auto enable_response = SendCommand(sock, "CACHE ENABLE");
  EXPECT_TRUE(enable_response.find("OK") == 0);

  // Verify cache is enabled again
  auto stats3 = SendCommand(sock, "CACHE STATS");
  EXPECT_TRUE(stats3.find("enabled: true") != std::string::npos);

  close(sock);
}

/**
 * @brief Test pagination consistency across cache hits
 */
TEST_F(CacheOffsetLimitIntegrationTest, PaginationConsistency) {
  int sock = CreateClientSocket();
  ASSERT_GE(sock, 0);

  // Get first page (cache miss)
  auto response1 = SendCommand(sock, "SEARCH test test OFFSET 0 LIMIT 10");
  auto result1 = ParseSearchResponse(response1);
  ASSERT_TRUE(result1.success);
  ASSERT_EQ(result1.ids.size(), 10);

  // Get second page (cache hit with different offset)
  auto response2 = SendCommand(sock, "SEARCH test test OFFSET 10 LIMIT 10");
  auto result2 = ParseSearchResponse(response2);
  ASSERT_TRUE(result2.success);
  ASSERT_EQ(result2.ids.size(), 10);

  // Get third page (cache hit)
  auto response3 = SendCommand(sock, "SEARCH test test OFFSET 20 LIMIT 10");
  auto result3 = ParseSearchResponse(response3);
  ASSERT_TRUE(result3.success);
  ASSERT_EQ(result3.ids.size(), 10);

  // Verify no overlap between pages
  std::set<std::string> page1_ids(result1.ids.begin(), result1.ids.end());
  std::set<std::string> page2_ids(result2.ids.begin(), result2.ids.end());
  std::set<std::string> page3_ids(result3.ids.begin(), result3.ids.end());

  // Check no duplicates across pages
  for (const auto& id : result2.ids) {
    EXPECT_EQ(page1_ids.count(id), 0) << "ID " << id << " appears in both page 1 and page 2";
  }
  for (const auto& id : result3.ids) {
    EXPECT_EQ(page1_ids.count(id), 0) << "ID " << id << " appears in both page 1 and page 3";
    EXPECT_EQ(page2_ids.count(id), 0) << "ID " << id << " appears in both page 2 and page 3";
  }

  close(sock);
}

/**
 * @brief Test OFFSET 0 with different LIMITs
 */
TEST_F(CacheOffsetLimitIntegrationTest, SameOffsetDifferentLimits) {
  int sock = CreateClientSocket();
  ASSERT_GE(sock, 0);

  // Populate cache with LIMIT 50
  auto response1 = SendCommand(sock, "SEARCH test test OFFSET 0 LIMIT 50");
  auto result1 = ParseSearchResponse(response1);
  ASSERT_TRUE(result1.success);
  EXPECT_EQ(result1.ids.size(), 50);

  // Request with smaller LIMIT (should use cache)
  auto response2 = SendCommand(sock, "SEARCH test test OFFSET 0 LIMIT 10");
  auto result2 = ParseSearchResponse(response2);
  ASSERT_TRUE(result2.success);
  EXPECT_EQ(result2.ids.size(), 10);

  // Verify first 10 IDs match
  for (size_t i = 0; i < 10; ++i) {
    EXPECT_EQ(result2.ids[i], result1.ids[i]) << "ID mismatch at position " << i;
  }

  // Request with larger LIMIT (should use cache)
  auto response3 = SendCommand(sock, "SEARCH test test OFFSET 0 LIMIT 100");
  auto result3 = ParseSearchResponse(response3);
  ASSERT_TRUE(result3.success);
  EXPECT_EQ(result3.ids.size(), 100);

  // Verify first 50 IDs match
  for (size_t i = 0; i < 50; ++i) {
    EXPECT_EQ(result3.ids[i], result1.ids[i]) << "ID mismatch at position " << i;
  }

  close(sock);
}

/**
 * @brief Test cache metadata (age and saved time) in debug mode
 */
TEST_F(CacheOffsetLimitIntegrationTest, CacheMetadataDebugInfo) {
  int sock = CreateClientSocket();
  ASSERT_GE(sock, 0);

  // Enable debug mode
  SendCommand(sock, "DEBUG ON");

  // First query (cache miss)
  auto response1 = SendCommand(sock, "SEARCH test test OFFSET 0 LIMIT 10");
  EXPECT_TRUE(response1.find("OK RESULTS") == 0);
  EXPECT_TRUE(response1.find("# DEBUG") != std::string::npos) << "Debug info should be present";

  // Wait a bit to ensure cache_age_ms > 0
  std::this_thread::sleep_for(std::chrono::milliseconds(10));

  // Second query (cache hit)
  auto response2 = SendCommand(sock, "SEARCH test test OFFSET 0 LIMIT 10");
  EXPECT_TRUE(response2.find("OK RESULTS") == 0);
  EXPECT_TRUE(response2.find("# DEBUG") != std::string::npos);

  // Verify cache hit status
  EXPECT_TRUE(response2.find("cache: hit") != std::string::npos) << "Should indicate cache hit. Response:\n"
                                                                 << response2;

  // Verify cache_age_ms is present
  EXPECT_TRUE(response2.find("cache_age_ms: ") != std::string::npos) << "cache_age_ms should be present. Response:\n"
                                                                     << response2;

  // Verify cache_saved_ms is present
  EXPECT_TRUE(response2.find("cache_saved_ms: ") != std::string::npos)
      << "cache_saved_ms should be present. Response:\n"
      << response2;

  close(sock);
}

/**
 * @brief Test COUNT cache metadata in debug mode
 */
TEST_F(CacheOffsetLimitIntegrationTest, CountCacheMetadataDebugInfo) {
  int sock = CreateClientSocket();
  ASSERT_GE(sock, 0);

  // Enable debug mode
  SendCommand(sock, "DEBUG ON");

  // First COUNT query (cache miss)
  auto response1 = SendCommand(sock, "COUNT test test");
  EXPECT_TRUE(response1.find("OK COUNT") == 0);
  EXPECT_TRUE(response1.find("# DEBUG") != std::string::npos);

  // Wait a bit
  std::this_thread::sleep_for(std::chrono::milliseconds(10));

  // Second COUNT query (cache hit)
  auto response2 = SendCommand(sock, "COUNT test test");
  EXPECT_TRUE(response2.find("OK COUNT") == 0);
  EXPECT_TRUE(response2.find("# DEBUG") != std::string::npos);

  // Verify cache hit with metadata
  EXPECT_TRUE(response2.find("cache: hit") != std::string::npos) << "Should indicate cache hit. Response:\n"
                                                                 << response2;
  EXPECT_TRUE(response2.find("cache_age_ms: ") != std::string::npos) << "cache_age_ms should be present. Response:\n"
                                                                     << response2;
  EXPECT_TRUE(response2.find("cache_saved_ms: ") != std::string::npos)
      << "cache_saved_ms should be present. Response:\n"
      << response2;

  close(sock);
}
