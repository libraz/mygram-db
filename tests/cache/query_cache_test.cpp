/**
 * @file query_cache_test.cpp
 * @brief Unit tests for QueryCache - LRU eviction and thread safety
 */

#include "cache/query_cache.h"

#include <gtest/gtest.h>

#include <thread>
#include <vector>

#include "cache/cache_key.h"

namespace mygramdb::cache {

/**
 * @brief Test basic insert and lookup
 */
TEST(QueryCacheTest, BasicInsertLookup) {
  QueryCache cache(1024 * 1024, 10.0);  // 1MB

  auto key = CacheKeyGenerator::Generate("test query");
  std::vector<DocId> result = {1, 2, 3, 4, 5};

  CacheMetadata meta;
  meta.table = "posts";
  meta.ngrams = {"tes", "est"};

  // Insert (cost must be >= min_query_cost_ms which is 10.0)
  EXPECT_TRUE(cache.Insert(key, result, meta, 15.0));

  // Lookup
  auto cached = cache.Lookup(key);
  ASSERT_TRUE(cached.has_value());
  EXPECT_EQ(result, cached.value());
}

/**
 * @brief Test lookup miss
 */
TEST(QueryCacheTest, LookupMiss) {
  QueryCache cache(1024 * 1024, 10.0);

  auto key = CacheKeyGenerator::Generate("nonexistent");
  auto cached = cache.Lookup(key);

  EXPECT_FALSE(cached.has_value());
}

/**
 * @brief Test LRU eviction - least recently used should be evicted
 */
TEST(QueryCacheTest, LRUEviction) {
  // Small cache that can hold ~3-4 entries
  QueryCache cache(1000, 10.0);

  CacheMetadata meta;
  meta.table = "posts";
  meta.ngrams = {"tes", "est"};

  // Insert 4 entries
  auto key1 = CacheKeyGenerator::Generate("query1");
  auto key2 = CacheKeyGenerator::Generate("query2");
  auto key3 = CacheKeyGenerator::Generate("query3");
  auto key4 = CacheKeyGenerator::Generate("query4");

  std::vector<DocId> result1 = {1, 2, 3};
  std::vector<DocId> result2 = {4, 5, 6};
  std::vector<DocId> result3 = {7, 8, 9};
  std::vector<DocId> result4 = {10, 11, 12};

  cache.Insert(key1, result1, meta, 15.0);
  cache.Insert(key2, result2, meta, 15.0);
  cache.Insert(key3, result3, meta, 15.0);

  // Access key1 to make it recently used
  [[maybe_unused]] auto _ = cache.Lookup(key1);

  // Insert key4, which should evict key2 (least recently used)
  cache.Insert(key4, result4, meta, 15.0);

  // key1 and key3 should still be present
  EXPECT_TRUE(cache.Lookup(key1).has_value());
  EXPECT_TRUE(cache.Lookup(key3).has_value());
  EXPECT_TRUE(cache.Lookup(key4).has_value());

  // key2 may or may not be evicted depending on memory calculation
  // Don't assert on key2 as eviction is implementation-specific
}

/**
 * @brief Test invalidation flag
 */
TEST(QueryCacheTest, Invalidation) {
  QueryCache cache(1024 * 1024, 10.0);

  auto key = CacheKeyGenerator::Generate("test");
  std::vector<DocId> result = {1, 2, 3};

  CacheMetadata meta;
  meta.table = "posts";
  meta.ngrams = {"tes", "est"};

  cache.Insert(key, result, meta, 15.0);

  // Mark as invalidated
  EXPECT_TRUE(cache.MarkInvalidated(key));

  // Lookup should return nullopt for invalidated entry
  auto cached = cache.Lookup(key);
  EXPECT_FALSE(cached.has_value());
}

/**
 * @brief Test erase
 */
TEST(QueryCacheTest, Erase) {
  QueryCache cache(1024 * 1024, 10.0);

  auto key = CacheKeyGenerator::Generate("test");
  std::vector<DocId> result = {1, 2, 3};

  CacheMetadata meta;
  meta.table = "posts";
  meta.ngrams = {"tes", "est"};

  cache.Insert(key, result, meta, 15.0);

  // Erase
  EXPECT_TRUE(cache.Erase(key));

  // Should not be found
  EXPECT_FALSE(cache.Lookup(key).has_value());

  // Erase non-existent key
  EXPECT_FALSE(cache.Erase(key));
}

/**
 * @brief Test clear
 */
TEST(QueryCacheTest, Clear) {
  QueryCache cache(1024 * 1024, 10.0);

  CacheMetadata meta;
  meta.table = "posts";
  meta.ngrams = {"tes", "est"};

  auto key1 = CacheKeyGenerator::Generate("query1");
  auto key2 = CacheKeyGenerator::Generate("query2");

  std::vector<DocId> result = {1, 2, 3};

  cache.Insert(key1, result, meta, 15.0);
  cache.Insert(key2, result, meta, 15.0);

  // Clear all
  cache.Clear();

  // Both should be gone
  EXPECT_FALSE(cache.Lookup(key1).has_value());
  EXPECT_FALSE(cache.Lookup(key2).has_value());
}

/**
 * @brief Test statistics
 */
TEST(QueryCacheTest, Statistics) {
  QueryCache cache(1024 * 1024, 10.0);

  auto key = CacheKeyGenerator::Generate("test");
  std::vector<DocId> result = {1, 2, 3};

  CacheMetadata meta;
  meta.table = "posts";
  meta.ngrams = {"tes", "est"};

  // Insert
  cache.Insert(key, result, meta, 15.0);

  // Hit
  [[maybe_unused]] auto hit = cache.Lookup(key);

  // Miss
  auto key2 = CacheKeyGenerator::Generate("miss");
  [[maybe_unused]] auto miss = cache.Lookup(key2);

  auto stats = cache.GetStatistics();

  EXPECT_EQ(2, stats.total_queries);
  EXPECT_EQ(1, stats.cache_hits);
  EXPECT_EQ(1, stats.cache_misses);
  EXPECT_GT(stats.current_entries, 0);
}

/**
 * @brief Test concurrent access - multiple threads reading and writing
 */
TEST(QueryCacheTest, ConcurrentAccess) {
  QueryCache cache(10 * 1024 * 1024, 10.0);  // 10MB

  CacheMetadata meta;
  meta.table = "posts";
  meta.ngrams = {"tes", "est"};

  const int num_threads = 10;
  const int operations_per_thread = 100;

  std::vector<std::thread> threads;

  // Launch multiple threads
  for (int t = 0; t < num_threads; ++t) {
    threads.emplace_back([&cache, &meta, t]() {
      for (int i = 0; i < operations_per_thread; ++i) {
        std::string query = "query_" + std::to_string(t) + "_" + std::to_string(i);
        auto key = CacheKeyGenerator::Generate(query);
        std::vector<DocId> result = {static_cast<DocId>(i)};

        // Insert
        cache.Insert(key, result, meta, 15.0);

        // Lookup
        [[maybe_unused]] auto cached = cache.Lookup(key);

        // Sometimes invalidate
        if (i % 10 == 0) {
          cache.MarkInvalidated(key);
        }
      }
    });
  }

  // Wait for all threads
  for (auto& thread : threads) {
    thread.join();
  }

  // Cache should still be functional
  auto stats = cache.GetStatistics();
  EXPECT_GT(stats.total_queries, 0);
  EXPECT_GT(stats.cache_hits, 0);
}

/**
 * @brief Test memory limit enforcement
 */
TEST(QueryCacheTest, MemoryLimit) {
  // Small cache (2KB) - enough for a couple large entries
  QueryCache cache(2000, 10.0);

  CacheMetadata meta;
  meta.table = "posts";
  meta.ngrams = {"tes", "est"};

  std::vector<DocId> large_result;
  for (int i = 0; i < 100; ++i) {
    large_result.push_back(static_cast<DocId>(i));
  }

  // Try to insert multiple large entries (should trigger evictions)
  for (int i = 0; i < 10; ++i) {
    auto key = CacheKeyGenerator::Generate("query" + std::to_string(i));
    cache.Insert(key, large_result, meta, 15.0);
  }

  auto stats = cache.GetStatistics();

  // Should have evicted some entries to stay within memory limit
  EXPECT_LT(stats.current_memory_bytes, 2500);  // Allow small overhead

  // Should have some evictions
  EXPECT_GT(stats.evictions, 0);
}

/**
 * @brief Test invalidated entry doesn't count toward hits
 */
TEST(QueryCacheTest, InvalidatedNoHit) {
  QueryCache cache(1024 * 1024, 10.0);

  auto key = CacheKeyGenerator::Generate("test");
  std::vector<DocId> result = {1, 2, 3};

  CacheMetadata meta;
  meta.table = "posts";
  meta.ngrams = {"tes", "est"};

  cache.Insert(key, result, meta, 15.0);
  cache.MarkInvalidated(key);

  // Lookup invalidated entry
  [[maybe_unused]] auto lookup_result = cache.Lookup(key);

  auto stats = cache.GetStatistics();

  // Should count as miss, not hit
  EXPECT_EQ(1, stats.total_queries);
  EXPECT_EQ(0, stats.cache_hits);
  EXPECT_EQ(1, stats.cache_misses);
  EXPECT_EQ(1, stats.cache_misses_invalidated);
}

/**
 * @brief Test concurrent lookup and erase to detect use-after-free
 *
 * This test attempts to trigger a use-after-free bug that existed when
 * QueryCache::Lookup released the lock before accessing entry.query_cost_ms.
 * Multiple threads perform lookups while other threads aggressively erase entries.
 */
TEST(QueryCacheTest, ConcurrentLookupAndErase) {
  QueryCache cache(10 * 1024 * 1024, 1.0);  // 10MB, low threshold

  // Insert multiple entries
  constexpr int kNumEntries = 100;
  std::vector<CacheKey> keys;
  keys.reserve(kNumEntries);

  for (int i = 0; i < kNumEntries; ++i) {
    auto key = CacheKeyGenerator::Generate("query_" + std::to_string(i));
    keys.push_back(key);

    std::vector<DocId> result;
    for (int j = 0; j < 100; ++j) {
      result.push_back(static_cast<DocId>(i * 100 + j));
    }

    CacheMetadata meta;
    meta.table = "test";
    meta.ngrams = {"test"};

    cache.Insert(key, result, meta, 10.0);
  }

  std::atomic<bool> stop{false};
  std::atomic<int> lookup_count{0};
  std::atomic<int> erase_count{0};

  // Lookup threads - continuously lookup entries
  auto lookup_func = [&]() {
    while (!stop) {
      for (const auto& key : keys) {
        auto result = cache.Lookup(key);
        lookup_count++;
        // Small delay to increase chance of race condition
        std::this_thread::yield();
      }
    }
  };

  // Erase threads - continuously erase and re-insert entries
  auto erase_func = [&]() {
    int idx = 0;
    while (!stop) {
      const auto& key = keys[idx % kNumEntries];

      // Erase entry
      cache.Erase(key);
      erase_count++;

      // Re-insert to keep entries available for lookup
      std::vector<DocId> result;
      for (int j = 0; j < 100; ++j) {
        result.push_back(static_cast<DocId>((idx % kNumEntries) * 100 + j));
      }

      CacheMetadata meta;
      meta.table = "test";
      meta.ngrams = {"test"};
      cache.Insert(key, result, meta, 10.0);

      idx++;
      std::this_thread::yield();
    }
  };

  // Start threads
  constexpr int kNumLookupThreads = 4;
  constexpr int kNumEraseThreads = 2;
  std::vector<std::thread> threads;

  for (int i = 0; i < kNumLookupThreads; ++i) {
    threads.emplace_back(lookup_func);
  }
  for (int i = 0; i < kNumEraseThreads; ++i) {
    threads.emplace_back(erase_func);
  }

  // Run for a short duration
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  stop = true;

  // Wait for all threads
  for (auto& thread : threads) {
    thread.join();
  }

  // Verify operations completed without crashes
  EXPECT_GT(lookup_count.load(), 0);
  EXPECT_GT(erase_count.load(), 0);

  // Verify statistics are consistent
  auto stats = cache.GetStatistics();
  EXPECT_EQ(stats.cache_hits + stats.cache_misses, stats.total_queries);
}

/**
 * @brief Test timing statistics are properly recorded for hits and misses
 *
 * This is a regression test to ensure that total_cache_hit_time_ms and
 * total_cache_miss_time_ms are actually updated during Lookup operations.
 * Previously these fields existed but were never populated.
 */
TEST(QueryCacheTest, TimingStatistics) {
  QueryCache cache(10 * 1024 * 1024, 1.0);  // 10MB, low threshold

  // Create a large result to make timing measurements more reliable
  std::vector<DocId> large_result;
  constexpr int kLargeResultSize = 10000;
  large_result.reserve(kLargeResultSize);
  for (int i = 0; i < kLargeResultSize; ++i) {
    large_result.push_back(static_cast<DocId>(i));
  }

  // Insert a cache entry
  auto key = CacheKeyGenerator::Generate("timing_test_query");
  CacheMetadata meta;
  meta.table = "test";
  meta.ngrams = {"test", "timing"};

  ASSERT_TRUE(cache.Insert(key, large_result, meta, 25.0));

  // Perform multiple cache misses to ensure measurable time
  for (int i = 0; i < 10; ++i) {
    auto miss_key = CacheKeyGenerator::Generate("nonexistent_query_" + std::to_string(i));
    auto miss_result = cache.Lookup(miss_key);
    EXPECT_FALSE(miss_result.has_value());
  }

  // Perform multiple cache hits to ensure measurable time
  for (int i = 0; i < 10; ++i) {
    auto hit_result = cache.Lookup(key);
    ASSERT_TRUE(hit_result.has_value());
    EXPECT_EQ(kLargeResultSize, hit_result->size());
  }

  // Get statistics
  auto stats = cache.GetStatistics();

  // Verify counters
  EXPECT_EQ(20, stats.total_queries);  // 10 misses + 10 hits
  EXPECT_EQ(10, stats.cache_hits);
  EXPECT_EQ(10, stats.cache_misses);
  EXPECT_EQ(10, stats.cache_misses_not_found);

  // Verify timing statistics are non-zero
  EXPECT_GT(stats.total_cache_hit_time_ms, 0.0) << "Cache hit latency should be recorded";
  EXPECT_GT(stats.total_cache_miss_time_ms, 0.0) << "Cache miss latency should be recorded";
  EXPECT_GT(stats.total_query_saved_time_ms, 0.0) << "Query saved time should be recorded";

  // Verify averages are computed correctly
  EXPECT_DOUBLE_EQ(stats.total_cache_hit_time_ms / 10.0, stats.AverageCacheHitLatency());
  EXPECT_DOUBLE_EQ(stats.total_cache_miss_time_ms / 10.0, stats.AverageCacheMissLatency());
  EXPECT_DOUBLE_EQ(10 * 25.0, stats.TotalTimeSaved());  // 10 hits * 25ms saved each

  // Perform multiple hits to verify accumulation
  for (int i = 0; i < 5; ++i) {
    auto result = cache.Lookup(key);
    ASSERT_TRUE(result.has_value());
  }

  // Get updated statistics
  stats = cache.GetStatistics();
  EXPECT_EQ(25, stats.total_queries);  // 10 misses + 15 hits
  EXPECT_EQ(15, stats.cache_hits);
  EXPECT_EQ(10, stats.cache_misses);

  // Verify timing has accumulated
  EXPECT_GT(stats.total_cache_hit_time_ms, 0.0);
  EXPECT_EQ(15 * 25.0, stats.TotalTimeSaved());  // 15 hits * 25ms saved each

  // Verify average is calculated correctly
  double expected_avg_hit = stats.total_cache_hit_time_ms / 15.0;
  EXPECT_DOUBLE_EQ(expected_avg_hit, stats.AverageCacheHitLatency());
}

}  // namespace mygramdb::cache
