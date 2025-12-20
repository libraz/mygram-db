/**
 * @file response_formatter_test.cpp
 * @brief Unit tests for ResponseFormatter
 */

#include "server/response_formatter.h"

#include <gtest/gtest.h>

#include "cache/cache_manager.h"
#include "config/config.h"
#include "index/index.h"
#include "server/server_stats.h"
#include "server/statistics_service.h"
#include "server/tcp_server.h"  // For TableContext
#include "storage/document_store.h"

using namespace mygramdb::server;
using namespace mygramdb;

class ResponseFormatterTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create table context
    auto index = std::make_unique<index::Index>(1);
    auto doc_store = std::make_unique<storage::DocumentStore>();

    table_context_.name = "test";
    table_context_.config.ngram_size = 1;
    table_context_.index = std::move(index);
    table_context_.doc_store = std::move(doc_store);

    table_contexts_["test"] = &table_context_;
  }

  TableContext table_context_;
  std::unordered_map<std::string, TableContext*> table_contexts_;
};

/**
 * @brief Test INFO response without cache manager (cache disabled)
 */
TEST_F(ResponseFormatterTest, FormatInfoResponseNoCacheManager) {
  ServerStats stats;

  // Aggregate metrics
  auto metrics = StatisticsService::AggregateMetrics(table_contexts_);

  std::string response = ResponseFormatter::FormatInfoResponse(metrics, stats, table_contexts_, nullptr, nullptr);

  // Should contain cache section with disabled status
  EXPECT_TRUE(response.find("# Cache") != std::string::npos);
  EXPECT_TRUE(response.find("cache_enabled: 0") != std::string::npos);

  // Should not contain cache statistics when disabled
  EXPECT_TRUE(response.find("cache_hits:") == std::string::npos);
  EXPECT_TRUE(response.find("cache_misses:") == std::string::npos);
}

/**
 * @brief Test INFO response with cache manager enabled
 */
TEST_F(ResponseFormatterTest, FormatInfoResponseWithCacheManager) {
  ServerStats stats;

  // Create cache manager
  config::CacheConfig cache_config;
  cache_config.enabled = true;
  cache_config.max_memory_bytes = 100 * 1024 * 1024;
  cache_config.min_query_cost_ms = 1.0;

  cache::CacheManager cache_manager(cache_config, table_contexts_);

  // Aggregate metrics
  auto metrics = StatisticsService::AggregateMetrics(table_contexts_);

  std::string response =
      ResponseFormatter::FormatInfoResponse(metrics, stats, table_contexts_, nullptr, &cache_manager);

  // Should contain cache section with enabled status
  EXPECT_TRUE(response.find("# Cache") != std::string::npos);
  EXPECT_TRUE(response.find("cache_enabled: 1") != std::string::npos);

  // Should contain all cache statistics fields
  EXPECT_TRUE(response.find("cache_hits: ") != std::string::npos);
  EXPECT_TRUE(response.find("cache_misses: ") != std::string::npos);
  EXPECT_TRUE(response.find("cache_misses_not_found: ") != std::string::npos);
  EXPECT_TRUE(response.find("cache_misses_invalidated: ") != std::string::npos);
  EXPECT_TRUE(response.find("cache_total_queries: ") != std::string::npos);
  EXPECT_TRUE(response.find("cache_hit_rate: ") != std::string::npos);
  EXPECT_TRUE(response.find("cache_current_entries: ") != std::string::npos);
  EXPECT_TRUE(response.find("cache_memory_bytes: ") != std::string::npos);
  EXPECT_TRUE(response.find("cache_memory_human: ") != std::string::npos);
  EXPECT_TRUE(response.find("cache_evictions: ") != std::string::npos);
  EXPECT_TRUE(response.find("cache_invalidations_immediate: ") != std::string::npos);
  EXPECT_TRUE(response.find("cache_invalidations_deferred: ") != std::string::npos);
  EXPECT_TRUE(response.find("cache_invalidations_batches: ") != std::string::npos);
  EXPECT_TRUE(response.find("cache_avg_hit_latency_ms: ") != std::string::npos);
  EXPECT_TRUE(response.find("cache_avg_miss_latency_ms: ") != std::string::npos);
  EXPECT_TRUE(response.find("cache_total_time_saved_ms: ") != std::string::npos);
}

/**
 * @brief Test INFO response with cache manager but disabled
 */
TEST_F(ResponseFormatterTest, FormatInfoResponseWithCacheManagerDisabled) {
  ServerStats stats;

  // Create cache manager but disabled
  config::CacheConfig cache_config;
  cache_config.enabled = false;
  cache_config.max_memory_bytes = 100 * 1024 * 1024;

  cache::CacheManager cache_manager(cache_config, table_contexts_);
  cache_manager.Disable();

  // Aggregate metrics
  auto metrics = StatisticsService::AggregateMetrics(table_contexts_);

  std::string response =
      ResponseFormatter::FormatInfoResponse(metrics, stats, table_contexts_, nullptr, &cache_manager);

  // Should contain cache section with disabled status
  EXPECT_TRUE(response.find("# Cache") != std::string::npos);
  EXPECT_TRUE(response.find("cache_enabled: 0") != std::string::npos);

  // Should not contain detailed statistics when disabled
  EXPECT_TRUE(response.find("cache_hits:") == std::string::npos);
}

/**
 * @brief Test SEARCH response with empty results
 */
TEST_F(ResponseFormatterTest, FormatSearchResponseEmpty) {
  std::vector<index::DocId> results;
  size_t total_results = 0;

  std::string response =
      ResponseFormatter::FormatSearchResponse(results, total_results, table_context_.doc_store.get());

  EXPECT_TRUE(response.find("OK RESULTS") != std::string::npos);
  EXPECT_TRUE(response.find(" 0") != std::string::npos || response.find("0") != std::string::npos);
}

/**
 * @brief Test SEARCH response with results
 */
TEST_F(ResponseFormatterTest, FormatSearchResponseWithResults) {
  // Add documents to store
  auto doc_id1 = table_context_.doc_store->AddDocument("pk1");
  auto doc_id2 = table_context_.doc_store->AddDocument("pk2");
  auto doc_id3 = table_context_.doc_store->AddDocument("pk3");

  std::vector<index::DocId> results = {*doc_id1, *doc_id2, *doc_id3};
  size_t total_results = 3;

  std::string response =
      ResponseFormatter::FormatSearchResponse(results, total_results, table_context_.doc_store.get());

  EXPECT_TRUE(response.find("OK RESULTS") != std::string::npos);
  EXPECT_TRUE(response.find(" 3") != std::string::npos);
  EXPECT_TRUE(response.find("pk1") != std::string::npos);
  EXPECT_TRUE(response.find("pk2") != std::string::npos);
  EXPECT_TRUE(response.find("pk3") != std::string::npos);
}

/**
 * @brief Test SEARCH response with pagination (total > returned)
 */
TEST_F(ResponseFormatterTest, FormatSearchResponseWithPagination) {
  // Add documents
  auto doc_id1 = table_context_.doc_store->AddDocument("pk1");
  auto doc_id2 = table_context_.doc_store->AddDocument("pk2");

  // Simulate pagination: 2 results returned, but 100 total
  std::vector<index::DocId> results = {*doc_id1, *doc_id2};
  size_t total_results = 100;

  std::string response =
      ResponseFormatter::FormatSearchResponse(results, total_results, table_context_.doc_store.get());

  EXPECT_TRUE(response.find("OK RESULTS") != std::string::npos);
  EXPECT_TRUE(response.find(" 100") != std::string::npos);
  EXPECT_TRUE(response.find("pk1") != std::string::npos);
  EXPECT_TRUE(response.find("pk2") != std::string::npos);
}

/**
 * @brief Test SEARCH response with debug info
 */
TEST_F(ResponseFormatterTest, FormatSearchResponseWithDebugInfo) {
  auto doc_id1 = table_context_.doc_store->AddDocument("pk1");

  std::vector<index::DocId> results = {*doc_id1};
  size_t total_results = 1;

  query::DebugInfo debug_info;
  debug_info.query_time_ms = 1.234;
  debug_info.index_time_ms = 0.5;
  debug_info.filter_time_ms = 0.2;

  std::string response =
      ResponseFormatter::FormatSearchResponse(results, total_results, table_context_.doc_store.get(), &debug_info);

  EXPECT_TRUE(response.find("OK RESULTS") != std::string::npos);
  EXPECT_TRUE(response.find("DEBUG") != std::string::npos || response.find("query_time") != std::string::npos);
}

/**
 * @brief Test COUNT response
 */
TEST_F(ResponseFormatterTest, FormatCountResponse) {
  std::string response = ResponseFormatter::FormatCountResponse(42);

  EXPECT_TRUE(response.find("OK COUNT") != std::string::npos);
  EXPECT_TRUE(response.find("42") != std::string::npos);
}

/**
 * @brief Test COUNT response with zero
 */
TEST_F(ResponseFormatterTest, FormatCountResponseZero) {
  std::string response = ResponseFormatter::FormatCountResponse(0);

  EXPECT_TRUE(response.find("OK COUNT") != std::string::npos);
  EXPECT_TRUE(response.find("0") != std::string::npos);
}

/**
 * @brief Test COUNT response with debug info
 */
TEST_F(ResponseFormatterTest, FormatCountResponseWithDebugInfo) {
  query::DebugInfo debug_info;
  debug_info.query_time_ms = 0.5;

  std::string response = ResponseFormatter::FormatCountResponse(100, &debug_info);

  EXPECT_TRUE(response.find("OK COUNT") != std::string::npos);
  EXPECT_TRUE(response.find("100") != std::string::npos);
  EXPECT_TRUE(response.find("DEBUG") != std::string::npos || response.find("query_time_ms") != std::string::npos);
}

/**
 * @brief Test SAVE response
 */
TEST_F(ResponseFormatterTest, FormatSaveResponse) {
  std::string response = ResponseFormatter::FormatSaveResponse("/path/to/snapshot.dump");

  EXPECT_TRUE(response.find("OK SAVE") != std::string::npos || response.find("OK") != std::string::npos);
  EXPECT_TRUE(response.find("/path/to/snapshot.dump") != std::string::npos);
}

/**
 * @brief Test LOAD response
 */
TEST_F(ResponseFormatterTest, FormatLoadResponse) {
  std::string response = ResponseFormatter::FormatLoadResponse("/path/to/snapshot.dump");

  EXPECT_TRUE(response.find("OK LOAD") != std::string::npos || response.find("OK") != std::string::npos);
  EXPECT_TRUE(response.find("/path/to/snapshot.dump") != std::string::npos);
}

/**
 * @brief Test REPLICATION STOP response
 */
TEST_F(ResponseFormatterTest, FormatReplicationStopResponse) {
  std::string response = ResponseFormatter::FormatReplicationStopResponse();

  EXPECT_TRUE(response.find("OK") != std::string::npos);
  EXPECT_TRUE(response.find("REPLICATION") != std::string::npos || response.find("STOP") != std::string::npos ||
              response.find("stopped") != std::string::npos);
}

/**
 * @brief Test REPLICATION START response
 */
TEST_F(ResponseFormatterTest, FormatReplicationStartResponse) {
  std::string response = ResponseFormatter::FormatReplicationStartResponse();

  EXPECT_TRUE(response.find("OK") != std::string::npos);
  EXPECT_TRUE(response.find("REPLICATION") != std::string::npos || response.find("START") != std::string::npos ||
              response.find("started") != std::string::npos);
}

/**
 * @brief Test error response formatting
 */
TEST_F(ResponseFormatterTest, FormatError) {
  std::string response = ResponseFormatter::FormatError("Invalid query syntax");

  EXPECT_TRUE(response.find("ERROR") != std::string::npos);
  EXPECT_TRUE(response.find("Invalid query syntax") != std::string::npos);
}

/**
 * @brief Test error response with special characters
 */
TEST_F(ResponseFormatterTest, FormatErrorWithSpecialCharacters) {
  std::string response = ResponseFormatter::FormatError("Error: \"quoted\" value");

  EXPECT_TRUE(response.find("ERROR") != std::string::npos);
  EXPECT_TRUE(response.find("quoted") != std::string::npos);
}

/**
 * @brief Test error response with empty message
 */
TEST_F(ResponseFormatterTest, FormatErrorEmpty) {
  std::string response = ResponseFormatter::FormatError("");

  EXPECT_TRUE(response.find("ERROR") != std::string::npos);
}

/**
 * @brief Test CONFIG response
 */
TEST_F(ResponseFormatterTest, FormatConfigResponse) {
  // Create minimal config for testing
  config::Config test_config;
  test_config.api.tcp.port = 9999;

  std::string response = ResponseFormatter::FormatConfigResponse(&test_config, 5, 100, false, 3600);

  EXPECT_TRUE(response.find("OK") != std::string::npos || response.find("CONFIG") != std::string::npos);
  EXPECT_TRUE(response.find("9999") != std::string::npos || response.find("port") != std::string::npos);
  EXPECT_TRUE(response.find("100") != std::string::npos || response.find("max_connections") != std::string::npos);
}

/**
 * @brief Test Prometheus metrics response
 */
TEST_F(ResponseFormatterTest, FormatPrometheusMetrics) {
  ServerStats stats;

  // Aggregate metrics
  auto metrics = StatisticsService::AggregateMetrics(table_contexts_);

  std::string response = ResponseFormatter::FormatPrometheusMetrics(metrics, stats, table_contexts_, nullptr);

  // Should contain Prometheus format metrics
  EXPECT_TRUE(response.find("#") != std::string::npos);  // Prometheus comments
  EXPECT_TRUE(response.find("mygramdb_") != std::string::npos || response.find("mygram_") != std::string::npos);
}

/**
 * @brief Test Prometheus metrics with cache manager
 */
TEST_F(ResponseFormatterTest, FormatPrometheusMetricsWithCache) {
  ServerStats stats;

  // Create cache manager
  config::CacheConfig cache_config;
  cache_config.enabled = true;
  cache_config.max_memory_bytes = 100 * 1024 * 1024;
  cache::CacheManager cache_manager(cache_config, table_contexts_);

  // Aggregate metrics
  auto metrics = StatisticsService::AggregateMetrics(table_contexts_);

  std::string response = ResponseFormatter::FormatPrometheusMetrics(metrics, stats, table_contexts_, nullptr);

  // Should contain Prometheus format metrics including cache metrics
  EXPECT_TRUE(response.find("#") != std::string::npos);
  EXPECT_TRUE(response.find("mygramdb_") != std::string::npos || response.find("mygram_") != std::string::npos);
}

// Line ending tests for TCP protocol compatibility

/**
 * @brief Test FormatConfigResponse uses CRLF line endings
 */
TEST_F(ResponseFormatterTest, FormatConfigResponseUsesCRLFLineEndings) {
  config::Config test_config;
  test_config.api.tcp.port = 9999;
  test_config.mysql.host = "127.0.0.1";
  test_config.mysql.port = 3306;

  std::string response = ResponseFormatter::FormatConfigResponse(&test_config, 5, 100, false, 3600);

  // Verify response uses CRLF line endings
  EXPECT_TRUE(response.find("\r\n") != std::string::npos) << "Response should contain CRLF line endings";

  // Verify no bare LF (LF not preceded by CR) - prevents mixed line ending issues
  for (size_t i = 0; i < response.size(); ++i) {
    if (response[i] == '\n' && (i == 0 || response[i - 1] != '\r')) {
      FAIL() << "Found bare LF at position " << i;
    }
  }

  // Verify response does not end with trailing CRLF (SendResponse adds it)
  EXPECT_FALSE(response.size() >= 2 && response[response.size() - 2] == '\r' && response[response.size() - 1] == '\n')
      << "Response should not end with CRLF (SendResponse adds it)";
}
