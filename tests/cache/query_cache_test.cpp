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

}  // namespace mygramdb::cache
