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

  std::string response = ResponseFormatter::FormatInfoResponse(table_contexts_, stats, nullptr, nullptr);

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
  cache_config.max_memory_mb = 100;
  cache_config.min_query_cost_ms = 1.0;

  cache::CacheManager cache_manager(cache_config, 1, 0);

  std::string response = ResponseFormatter::FormatInfoResponse(table_contexts_, stats, nullptr, &cache_manager);

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
  cache_config.max_memory_mb = 100;

  cache::CacheManager cache_manager(cache_config, 1, 0);
  cache_manager.Disable();

  std::string response = ResponseFormatter::FormatInfoResponse(table_contexts_, stats, nullptr, &cache_manager);

  // Should contain cache section with disabled status
  EXPECT_TRUE(response.find("# Cache") != std::string::npos);
  EXPECT_TRUE(response.find("cache_enabled: 0") != std::string::npos);

  // Should not contain detailed statistics when disabled
  EXPECT_TRUE(response.find("cache_hits:") == std::string::npos);
}
