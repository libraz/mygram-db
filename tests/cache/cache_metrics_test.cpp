/**
 * @file cache_metrics_test.cpp
 * @brief Tests for cache performance metrics
 *
 * Validates that cache statistics (hit rate, invalidations, evictions, etc.)
 * are properly tracked and exposed for production monitoring.
 */

#include <gtest/gtest.h>

#include <thread>

#include "cache/cache_manager.h"
#include "cache/cache_types.h"
#include "cache/query_cache.h"
#include "config/config.h"

namespace mygramdb::cache {

class CacheMetricsTest : public ::testing::Test {
 protected:
  void SetUp() override {
    ngram_configs_["test_table"] = NgramConfig{.ngram_size = 2, .kanji_ngram_size = 1, .cross_boundary_ngrams = true};

    // Create cache config
    cache_config_.enabled = true;
    cache_config_.max_memory_bytes = 1024 * 1024;  // 1MB
    cache_config_.min_query_cost_ms = 0.0;         // Cache everything for testing
  }

  NgramConfigMap ngram_configs_;
  config::CacheConfig cache_config_;
};

// Test: Cache statistics are initialized to zero
TEST_F(CacheMetricsTest, InitialStatistics) {
  CacheManager manager(cache_config_, ngram_configs_);
  auto stats = manager.GetStatistics();

  EXPECT_EQ(stats.total_queries, 0);
  EXPECT_EQ(stats.cache_hits, 0);
  EXPECT_EQ(stats.cache_misses, 0);
  EXPECT_EQ(stats.cache_misses_not_found, 0);
  EXPECT_EQ(stats.cache_misses_invalidated, 0);
  EXPECT_EQ(stats.invalidations_immediate, 0);
  EXPECT_EQ(stats.invalidations_deferred, 0);
  EXPECT_EQ(stats.invalidations_batches, 0);
  EXPECT_EQ(stats.current_entries, 0);
  EXPECT_EQ(stats.current_memory_bytes, 0);
  EXPECT_EQ(stats.evictions, 0);
}

// Test: Hit rate calculation
TEST_F(CacheMetricsTest, HitRateCalculation) {
  CacheManager manager(cache_config_, ngram_configs_);

  // Initially hit rate should be 0
  auto stats = manager.GetStatistics();
  EXPECT_DOUBLE_EQ(stats.HitRate(), 0.0);

  // After some hits and misses, hit rate should be calculated correctly
  // Note: We can't easily inject hits/misses without more infrastructure,
  // so we just verify the calculation logic
  CacheStatisticsSnapshot test_stats;
  test_stats.total_queries = 100;
  test_stats.cache_hits = 75;
  test_stats.cache_misses = 25;

  EXPECT_DOUBLE_EQ(test_stats.HitRate(), 0.75);  // 75% hit rate
}

// Test: Average latency calculations
TEST_F(CacheMetricsTest, LatencyCalculations) {
  CacheStatisticsSnapshot stats;

  // Test with no queries
  EXPECT_DOUBLE_EQ(stats.AverageCacheHitLatency(), 0.0);
  EXPECT_DOUBLE_EQ(stats.AverageCacheMissLatency(), 0.0);

  // Test with some queries
  stats.cache_hits = 10;
  stats.total_cache_hit_time_ms = 50.0;                   // 50ms total for 10 hits
  EXPECT_DOUBLE_EQ(stats.AverageCacheHitLatency(), 5.0);  // 5ms average

  stats.cache_misses = 5;
  stats.total_cache_miss_time_ms = 100.0;                   // 100ms total for 5 misses
  EXPECT_DOUBLE_EQ(stats.AverageCacheMissLatency(), 20.0);  // 20ms average
}

// Test: Time saved calculation
TEST_F(CacheMetricsTest, TimeSavedCalculation) {
  CacheStatisticsSnapshot stats;

  stats.total_query_saved_time_ms = 1234.56;
  EXPECT_DOUBLE_EQ(stats.TotalTimeSaved(), 1234.56);
}

// Test: Metrics are thread-safe (basic concurrency test)
TEST_F(CacheMetricsTest, ThreadSafety) {
  CacheManager manager(cache_config_, ngram_configs_);

  // Launch multiple threads to get statistics concurrently
  std::vector<std::thread> threads;
  std::atomic<int> success_count{0};

  for (int i = 0; i < 10; ++i) {
    threads.emplace_back([&manager, &success_count]() {
      for (int j = 0; j < 100; ++j) {
        auto stats = manager.GetStatistics();
        // Statistics should be consistent (no torn reads)
        if (stats.total_queries >= stats.cache_hits + stats.cache_misses) {
          success_count++;
        }
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  // All reads should be consistent
  EXPECT_EQ(success_count.load(), 1000);
}

// Test: Metrics can be retrieved from QueryCache directly
TEST_F(CacheMetricsTest, QueryCacheStatistics) {
  QueryCache cache(1024 * 1024, 0.0);  // 1MB, cache everything
  auto stats = cache.GetStatistics();

  // Initial state
  EXPECT_EQ(stats.total_queries, 0);
  EXPECT_EQ(stats.cache_hits, 0);
  EXPECT_EQ(stats.cache_misses, 0);
  EXPECT_EQ(stats.current_entries, 0);
  EXPECT_EQ(stats.evictions, 0);
}

// Test: Statistics snapshot is copyable
TEST_F(CacheMetricsTest, SnapshotCopyable) {
  CacheStatisticsSnapshot stats1;
  stats1.total_queries = 100;
  stats1.cache_hits = 75;
  stats1.cache_misses = 25;

  // Copy constructor
  CacheStatisticsSnapshot stats2 = stats1;
  EXPECT_EQ(stats2.total_queries, 100);
  EXPECT_EQ(stats2.cache_hits, 75);
  EXPECT_EQ(stats2.cache_misses, 25);

  // Copy assignment
  CacheStatisticsSnapshot stats3;
  stats3 = stats1;
  EXPECT_EQ(stats3.total_queries, 100);
  EXPECT_EQ(stats3.cache_hits, 75);
  EXPECT_EQ(stats3.cache_misses, 25);
}

// Test: Hit rate edge cases
TEST_F(CacheMetricsTest, HitRateEdgeCases) {
  CacheStatisticsSnapshot stats;

  // No queries - hit rate should be 0
  EXPECT_DOUBLE_EQ(stats.HitRate(), 0.0);

  // All hits
  stats.total_queries = 100;
  stats.cache_hits = 100;
  stats.cache_misses = 0;
  EXPECT_DOUBLE_EQ(stats.HitRate(), 1.0);  // 100%

  // All misses
  stats.total_queries = 100;
  stats.cache_hits = 0;
  stats.cache_misses = 100;
  EXPECT_DOUBLE_EQ(stats.HitRate(), 0.0);  // 0%
}

// Test: InvalidationManager memory is reflected in statistics
TEST_F(CacheMetricsTest, InvalidationIndexMemoryInStatistics) {
  CacheManager manager(cache_config_, ngram_configs_);

  // Insert a cache entry to generate invalidation tracking data
  query::Query query;
  query.type = query::QueryType::SEARCH;
  query.table = "test_table";
  query.search_text = "golang";

  std::vector<DocId> result = {1, 2, 3};
  std::vector<std::string> ngrams = {"ang", "gol", "lan", "ola"};

  manager.Insert(query, result, ngrams, 10.0);

  auto stats = manager.GetStatistics();
  // InvalidationManager should report non-zero memory usage after tracking an entry
  EXPECT_GT(stats.invalidation_index_memory_bytes, 0);
}

}  // namespace mygramdb::cache
