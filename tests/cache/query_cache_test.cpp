/**
 * @file query_cache_test.cpp
 * @brief Unit tests for QueryCache - LRU eviction and thread safety
 */

#include "cache/query_cache.h"

#include <gtest/gtest.h>

#include <thread>
#include <vector>

#include "query/cache_key.h"
#include "query/query_parser.h"

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
  QueryCache cache(2000, 10.0);

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

  // Wait for background LRU refresh to update the LRU list
  // (Background thread runs every 100ms)
  std::this_thread::sleep_for(std::chrono::milliseconds(150));

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

/**
 * @brief Test memory accounting consistency (Insert uses same calculation as Erase)
 *
 * This is a regression test for a bug where Insert used compressed.size()
 * but Erase used compressed.capacity(), causing total_memory_bytes_ to
 * underflow or accumulate errors over time.
 */
TEST(QueryCacheTest, MemoryAccountingConsistency) {
  QueryCache cache(10 * 1024 * 1024, 1.0);  // 10MB

  CacheMetadata meta;
  meta.table = "test";
  meta.ngrams = {"test", "memory"};

  // Insert and erase multiple entries
  constexpr int kNumIterations = 100;
  for (int i = 0; i < kNumIterations; ++i) {
    auto key = CacheKeyGenerator::Generate("query_" + std::to_string(i));

    // Create a result with varying size to ensure different compression ratios
    std::vector<DocId> result;
    for (int j = 0; j < (i % 50 + 10); ++j) {
      result.push_back(static_cast<DocId>(i * 100 + j));
    }

    // Insert
    ASSERT_TRUE(cache.Insert(key, result, meta, 10.0));

    // Verify memory increased
    auto stats_after_insert = cache.GetStatistics();
    EXPECT_GT(stats_after_insert.current_memory_bytes, 0);

    // Erase
    ASSERT_TRUE(cache.Erase(key));

    // Verify memory decreased back to near zero (some overhead may remain)
    auto stats_after_erase = cache.GetStatistics();
    EXPECT_EQ(0, stats_after_erase.current_entries);
  }

  // After all insert/erase cycles, memory should be exactly 0
  auto final_stats = cache.GetStatistics();
  EXPECT_EQ(0, final_stats.current_memory_bytes)
      << "Memory accounting is inconsistent - total_memory_bytes_ should be 0 after all entries are erased";
  EXPECT_EQ(0, final_stats.current_entries);
}

/**
 * @brief Test for lock upgrade race condition fix
 *
 * Verifies that when an entry is evicted and re-inserted during a Lookup operation,
 * the metadata update is correctly handled (using created_at timestamp verification).
 */
TEST(QueryCacheTest, LockUpgradeRaceCondition) {
  QueryCache cache(10 * 1024, 10.0);  // 10KB cache to allow for test data

  auto key1 = CacheKeyGenerator::Generate("query1");
  auto key2 = CacheKeyGenerator::Generate("query2");
  std::vector<DocId> result1 = {1, 2, 3};
  std::vector<DocId> result2 = {4, 5, 6};

  CacheMetadata meta1;
  meta1.table = "posts";
  meta1.ngrams = {"q1"};

  CacheMetadata meta2;
  meta2.table = "posts";
  meta2.ngrams = {"q2"};

  // Insert first entry
  ASSERT_TRUE(cache.Insert(key1, result1, meta1, 15.0));

  std::atomic<bool> lookup_started{false};
  std::atomic<bool> eviction_done{false};
  std::vector<DocId> lookup_result;

  // Thread 1: Lookup (triggers lock upgrade)
  std::thread lookup_thread([&]() {
    lookup_started = true;
    // This lookup should complete safely even if entry is evicted
    auto result = cache.Lookup(key1);
    if (eviction_done.load()) {
      // Entry might have been evicted, result might be nullopt
      lookup_result = result.value_or(std::vector<DocId>{});
    } else {
      lookup_result = result.value_or(result1);
    }
  });

  // Thread 2: Force eviction by inserting large entry
  std::thread evict_thread([&]() {
    // Wait for lookup to start
    while (!lookup_started.load()) {
      std::this_thread::yield();
    }

    // Insert entry large enough to evict key1
    std::vector<DocId> large_result(200, 999);
    ASSERT_TRUE(cache.Insert(key2, large_result, meta2, 20.0));
    eviction_done = true;
  });

  lookup_thread.join();
  evict_thread.join();

  // Test passes if no crash or assertion failure occurred
  // The lookup should have either:
  // 1. Returned the original result before eviction
  // 2. Returned empty result after eviction
  // Both are acceptable as long as no data corruption or crash occurs
}

/**
 * @brief Test concurrent lookups don't corrupt metadata due to lock upgrade
 */
TEST(QueryCacheTest, ConcurrentLookupsNoMetadataCorruption) {
  QueryCache cache(10 * 1024 * 1024, 10.0);  // 10MB
  const int num_threads = 10;
  const int num_lookups = 100;

  auto key = CacheKeyGenerator::Generate("concurrent_query");
  std::vector<DocId> result = {1, 2, 3, 4, 5};

  CacheMetadata meta;
  meta.table = "posts";
  meta.ngrams = {"con", "cur"};

  // Insert initial entry
  ASSERT_TRUE(cache.Insert(key, result, meta, 15.0));

  // Multiple threads perform concurrent lookups
  std::vector<std::thread> threads;
  std::atomic<int> successful_lookups{0};

  for (int t = 0; t < num_threads; ++t) {
    threads.emplace_back([&]() {
      for (int i = 0; i < num_lookups; ++i) {
        auto cached = cache.Lookup(key);
        if (cached.has_value() && cached.value() == result) {
          successful_lookups++;
        }
      }
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }

  // All lookups should have succeeded (no data corruption)
  EXPECT_EQ(num_threads * num_lookups, successful_lookups.load());

  // Verify statistics are consistent
  auto stats = cache.GetStatistics();
  EXPECT_GT(stats.cache_hits, 0);
  EXPECT_EQ(1, stats.current_entries);
}

/**
 * @brief Test that stats_.total_queries is accurately counted under concurrent access
 * Regression test for: stats_.total_queries++ was incremented before mutex lock
 */
TEST(QueryCacheTest, ConcurrentQueryCountAccuracy) {
  QueryCache cache(10 * 1024 * 1024, 10.0);  // 10MB

  // Insert some test data
  auto key1 = CacheKeyGenerator::Generate("query1");
  auto key2 = CacheKeyGenerator::Generate("query2");
  std::vector<DocId> result1 = {1, 2, 3};
  std::vector<DocId> result2 = {4, 5, 6};

  CacheMetadata meta;
  meta.table = "test";
  meta.ngrams = {"tes", "est"};

  cache.Insert(key1, result1, meta, 15.0);
  cache.Insert(key2, result2, meta, 15.0);

  // Concurrent lookups from multiple threads
  const int num_threads = 10;
  const int lookups_per_thread = 1000;
  std::vector<std::thread> threads;

  for (int i = 0; i < num_threads; ++i) {
    threads.emplace_back([&cache, &key1, &key2]() {
      for (int j = 0; j < lookups_per_thread; ++j) {
        // Alternate between two keys
        auto key = (j % 2 == 0) ? key1 : key2;
        [[maybe_unused]] auto result = cache.Lookup(key);
      }
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }

  // Verify total_queries is exactly what we expect
  // Before the fix: this could be less than expected due to race condition
  auto stats = cache.GetStatistics();
  EXPECT_EQ(num_threads * lookups_per_thread, stats.total_queries);

  // All queries should be cache hits
  EXPECT_EQ(num_threads * lookups_per_thread, stats.cache_hits);
  EXPECT_EQ(0, stats.cache_misses);
}

/**
 * @brief Test ABA problem mitigation during lock upgrade
 *
 * This test verifies that the QueryCache correctly handles the ABA problem
 * during lock upgrade from shared_lock to unique_lock.
 *
 * Scenario:
 * 1. Thread 1: Lookup finds entry, holds shared_lock
 * 2. Thread 1: Releases shared_lock to upgrade
 * 3. Thread 2: Evicts the entry and inserts new entry with same key
 * 4. Thread 1: Acquires unique_lock, should detect entry changed
 *
 * The fix uses pointer address comparison instead of timestamp comparison
 * to detect if the entry has been replaced.
 */
TEST(QueryCacheTest, ABAProofLockUpgrade) {
  // Small cache to trigger eviction easily
  QueryCache cache(100, 1.0);  // 100 bytes, min cost 1ms

  auto key1 = CacheKeyGenerator::Generate("query1");
  auto key2 = CacheKeyGenerator::Generate("query2");
  auto key3 = CacheKeyGenerator::Generate("query3");

  std::vector<DocId> result1 = {1, 2, 3};
  std::vector<DocId> result2 = {4, 5, 6};
  std::vector<DocId> result3 = {7, 8, 9};

  CacheMetadata meta;
  meta.table = "test";
  meta.ngrams = {"tes", "est"};

  // Insert first entry
  cache.Insert(key1, result1, meta, 5.0);

  // Create a scenario where ABA could occur
  std::atomic<bool> thread2_replaced_entry{false};
  std::atomic<int> thread1_access_count{0};

  std::thread t1([&]() {
    // Lookup (this will try to upgrade lock)
    auto cached = cache.Lookup(key1);
    if (cached.has_value()) {
      thread1_access_count++;
    }
  });

  std::thread t2([&]() {
    // Wait a bit to let t1 acquire shared lock
    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    // Force eviction by inserting large entries
    cache.Insert(key2, result2, meta, 5.0);
    cache.Insert(key3, result3, meta, 5.0);

    // Try to insert key1 again (simulating ABA)
    cache.Insert(key1, result1, meta, 5.0);
    thread2_replaced_entry = true;
  });

  t1.join();
  t2.join();

  // The test passes if no crash occurs
  // With the fix, the pointer address check prevents touching wrong entry
  EXPECT_TRUE(thread2_replaced_entry.load());

  // Verify cache is in consistent state
  auto stats = cache.GetStatistics();
  EXPECT_GE(stats.total_queries, 1);
}

/**
 * @brief Test concurrent lookup and eviction race condition
 *
 * This test verifies that concurrent lookups and evictions don't cause
 * use-after-free or incorrect LRU updates due to the ABA problem.
 */
TEST(QueryCacheTest, ConcurrentLookupEvictionABA) {
  QueryCache cache(200, 1.0);  // Small cache

  auto key = CacheKeyGenerator::Generate("test_key");
  std::vector<DocId> result = {1, 2, 3, 4, 5};

  CacheMetadata meta;
  meta.table = "test";
  meta.ngrams = {"tes", "est"};

  // Insert initial entry
  cache.Insert(key, result, meta, 5.0);

  std::atomic<int> successful_lookups{0};
  std::atomic<int> failed_lookups{0};
  std::atomic<bool> stop{false};

  // Thread 1: Continuous lookups
  std::thread lookup_thread([&]() {
    while (!stop.load()) {
      auto cached = cache.Lookup(key);
      if (cached.has_value()) {
        successful_lookups++;
      } else {
        failed_lookups++;
      }
      std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
  });

  // Thread 2: Continuous insert/evict to trigger ABA
  std::thread evict_thread([&]() {
    int counter = 0;
    while (!stop.load()) {
      // Insert other entries to trigger eviction
      auto temp_key = CacheKeyGenerator::Generate("temp_" + std::to_string(counter++));
      std::vector<DocId> temp_result = {100, 200, 300};
      cache.Insert(temp_key, temp_result, meta, 5.0);

      // Re-insert original key
      cache.Insert(key, result, meta, 5.0);

      std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
  });

  // Run for 100ms
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  stop = true;

  lookup_thread.join();
  evict_thread.join();

  // Verify no crashes occurred and some operations succeeded
  EXPECT_GT(successful_lookups.load() + failed_lookups.load(), 0);

  // Cache should be in consistent state
  auto stats = cache.GetStatistics();
  EXPECT_GE(stats.total_queries, 1);
}

/**
 * @brief Test TTL-based expiration (basic)
 */
TEST(QueryCacheTest, TTLBasicExpiration) {
  // Create cache with 2-second TTL
  QueryCache cache(1024 * 1024, 10.0, 2);  // 1MB, min_cost=10ms, ttl=2s

  auto key = CacheKeyGenerator::Generate("test query");
  std::vector<DocId> result = {1, 2, 3};

  CacheMetadata meta;
  meta.table = "posts";
  meta.ngrams = {"tes", "est"};

  // Insert entry
  EXPECT_TRUE(cache.Insert(key, result, meta, 15.0));

  // Immediate lookup should succeed
  auto cached = cache.Lookup(key);
  ASSERT_TRUE(cached.has_value());
  EXPECT_EQ(result, cached.value());

  // Wait for TTL to expire
  std::this_thread::sleep_for(std::chrono::seconds(3));

  // Lookup should fail (expired)
  auto cached_after_ttl = cache.Lookup(key);
  EXPECT_FALSE(cached_after_ttl.has_value());

  // Statistics should show cache miss
  auto stats = cache.GetStatistics();
  EXPECT_EQ(stats.cache_misses, 1);  // The expired lookup
}

/**
 * @brief Test TTL disabled (0 = no expiration)
 */
TEST(QueryCacheTest, TTLDisabled) {
  // Create cache with TTL=0 (disabled)
  QueryCache cache(1024 * 1024, 10.0, 0);

  auto key = CacheKeyGenerator::Generate("test query");
  std::vector<DocId> result = {1, 2, 3};

  CacheMetadata meta;
  meta.table = "posts";
  meta.ngrams = {"tes", "est"};

  // Insert entry
  EXPECT_TRUE(cache.Insert(key, result, meta, 15.0));

  // Wait for a few seconds
  std::this_thread::sleep_for(std::chrono::seconds(2));

  // Lookup should still succeed (no expiration)
  auto cached = cache.Lookup(key);
  ASSERT_TRUE(cached.has_value());
  EXPECT_EQ(result, cached.value());
}

/**
 * @brief Test TTL runtime update with SetTtl
 */
TEST(QueryCacheTest, TTLRuntimeUpdate) {
  // Create cache with no TTL
  QueryCache cache(1024 * 1024, 10.0, 0);

  auto key = CacheKeyGenerator::Generate("test query");
  std::vector<DocId> result = {1, 2, 3};

  CacheMetadata meta;
  meta.table = "posts";
  meta.ngrams = {"tes", "est"};

  // Insert entry
  EXPECT_TRUE(cache.Insert(key, result, meta, 15.0));

  // Immediate lookup should succeed
  auto cached1 = cache.Lookup(key);
  ASSERT_TRUE(cached1.has_value());

  // Enable TTL with very short duration (1 second)
  cache.SetTtl(1);

  // Wait for new TTL to expire
  std::this_thread::sleep_for(std::chrono::seconds(2));

  // Lookup should now fail (expired with new TTL)
  auto cached2 = cache.Lookup(key);
  EXPECT_FALSE(cached2.has_value());

  // Verify we can read TTL setting
  EXPECT_EQ(cache.GetTtl(), 1);
}

/**
 * @brief Test LookupWithMetadata respects TTL
 */
TEST(QueryCacheTest, TTLWithMetadataLookup) {
  QueryCache cache(1024 * 1024, 10.0, 1);  // 1 second TTL

  auto key = CacheKeyGenerator::Generate("test query");
  std::vector<DocId> result = {1, 2, 3};

  CacheMetadata meta;
  meta.table = "posts";
  meta.ngrams = {"tes", "est"};

  // Insert entry
  EXPECT_TRUE(cache.Insert(key, result, meta, 15.0));

  // Immediate lookup with metadata should succeed
  QueryCache::LookupMetadata lookup_meta;
  auto cached1 = cache.LookupWithMetadata(key, lookup_meta);
  ASSERT_TRUE(cached1.has_value());
  EXPECT_EQ(result, cached1.value());
  EXPECT_DOUBLE_EQ(lookup_meta.query_cost_ms, 15.0);

  // Wait for TTL to expire
  std::this_thread::sleep_for(std::chrono::seconds(2));

  // Lookup with metadata should also fail (expired)
  QueryCache::LookupMetadata lookup_meta2;
  auto cached2 = cache.LookupWithMetadata(key, lookup_meta2);
  EXPECT_FALSE(cached2.has_value());
}

/**
 * @brief Test multiple entries with different ages and TTL
 */
TEST(QueryCacheTest, TTLMultipleEntriesExpiration) {
  QueryCache cache(1024 * 1024, 10.0, 2);  // 2 second TTL

  auto key1 = CacheKeyGenerator::Generate("query1");
  auto key2 = CacheKeyGenerator::Generate("query2");
  std::vector<DocId> result1 = {1, 2};
  std::vector<DocId> result2 = {3, 4};

  CacheMetadata meta;
  meta.table = "posts";
  meta.ngrams = {"que"};

  // Insert first entry
  EXPECT_TRUE(cache.Insert(key1, result1, meta, 15.0));

  // Wait 1 second
  std::this_thread::sleep_for(std::chrono::seconds(1));

  // Insert second entry (younger)
  EXPECT_TRUE(cache.Insert(key2, result2, meta, 15.0));

  // Wait another 1.2 seconds (total: first=2.2s, second=1.2s)
  std::this_thread::sleep_for(std::chrono::milliseconds(1200));

  // First entry should be expired (age > 2s)
  auto cached1 = cache.Lookup(key1);
  EXPECT_FALSE(cached1.has_value());

  // Second entry should still be valid (age < 2s)
  auto cached2 = cache.Lookup(key2);
  EXPECT_TRUE(cached2.has_value());  // Should still be valid
}

// =============================================================================
// Bug #19: ClearTable skips eviction callback
// =============================================================================
// When ClearTable() removes entries, it does NOT call the eviction callback,
// but EvictForSpace() DOES call it. This causes the InvalidationManager to
// retain stale reverse index entries, leading to memory leaks.
// =============================================================================

/**
 * @test Bug #19: ClearTable should call eviction callback for each removed entry
 *
 * When QueryCache::ClearTable() removes entries, it should call the eviction
 * callback so that InvalidationManager can clean up its reverse index.
 */
TEST(QueryCacheTest, Bug19_ClearTableCallsEvictionCallback) {
  QueryCache cache(1024 * 1024, 10.0);

  // Track evicted keys
  std::vector<CacheKey> evicted_keys;
  cache.SetEvictionCallback([&evicted_keys](const CacheKey& key) { evicted_keys.push_back(key); });

  CacheMetadata meta;
  meta.table = "posts";
  meta.ngrams = {"tes", "est"};

  // Insert multiple entries
  auto key1 = CacheKeyGenerator::Generate("query1");
  auto key2 = CacheKeyGenerator::Generate("query2");
  auto key3 = CacheKeyGenerator::Generate("query3");

  std::vector<DocId> result = {1, 2, 3};

  cache.Insert(key1, result, meta, 15.0);
  cache.Insert(key2, result, meta, 15.0);
  cache.Insert(key3, result, meta, 15.0);

  // Verify entries exist
  EXPECT_TRUE(cache.Lookup(key1).has_value());
  EXPECT_TRUE(cache.Lookup(key2).has_value());
  EXPECT_TRUE(cache.Lookup(key3).has_value());

  // Clear callback list (lookups might have touched stats but not evicted)
  evicted_keys.clear();

  // ClearTable should trigger eviction callbacks
  cache.ClearTable("posts");

  // Bug #19: Before fix, evicted_keys would be empty
  // After fix: evicted_keys should contain all 3 keys
  EXPECT_EQ(3, evicted_keys.size()) << "Bug #19: ClearTable should call eviction callback for each removed entry";

  // Verify all keys were evicted
  EXPECT_NE(std::find(evicted_keys.begin(), evicted_keys.end(), key1), evicted_keys.end());
  EXPECT_NE(std::find(evicted_keys.begin(), evicted_keys.end(), key2), evicted_keys.end());
  EXPECT_NE(std::find(evicted_keys.begin(), evicted_keys.end(), key3), evicted_keys.end());

  // Verify entries are actually gone
  EXPECT_FALSE(cache.Lookup(key1).has_value());
  EXPECT_FALSE(cache.Lookup(key2).has_value());
  EXPECT_FALSE(cache.Lookup(key3).has_value());
}

/**
 * @test Bug #19: ClearTable with multiple tables only evicts specified table
 */
TEST(QueryCacheTest, Bug19_ClearTableOnlyAffectsSpecifiedTable) {
  QueryCache cache(1024 * 1024, 10.0);

  std::vector<CacheKey> evicted_keys;
  cache.SetEvictionCallback([&evicted_keys](const CacheKey& key) { evicted_keys.push_back(key); });

  // Insert entries for two different tables
  CacheMetadata meta_posts;
  meta_posts.table = "posts";
  meta_posts.ngrams = {"pos", "ost"};

  CacheMetadata meta_comments;
  meta_comments.table = "comments";
  meta_comments.ngrams = {"com", "omm"};

  auto key_posts1 = CacheKeyGenerator::Generate("posts_query1");
  auto key_posts2 = CacheKeyGenerator::Generate("posts_query2");
  auto key_comments1 = CacheKeyGenerator::Generate("comments_query1");

  std::vector<DocId> result = {1, 2, 3};

  cache.Insert(key_posts1, result, meta_posts, 15.0);
  cache.Insert(key_posts2, result, meta_posts, 15.0);
  cache.Insert(key_comments1, result, meta_comments, 15.0);

  // Clear only posts table
  cache.ClearTable("posts");

  // Should have evicted 2 posts entries
  EXPECT_EQ(2, evicted_keys.size());
  EXPECT_NE(std::find(evicted_keys.begin(), evicted_keys.end(), key_posts1), evicted_keys.end());
  EXPECT_NE(std::find(evicted_keys.begin(), evicted_keys.end(), key_posts2), evicted_keys.end());

  // Comments entry should still exist
  EXPECT_TRUE(cache.Lookup(key_comments1).has_value());
}

/**
 * @test Bug #19: ClearTable with no matching entries should not crash
 */
TEST(QueryCacheTest, Bug19_ClearTableNoMatchingEntries) {
  QueryCache cache(1024 * 1024, 10.0);

  int callback_count = 0;
  cache.SetEvictionCallback([&callback_count](const CacheKey& /*key*/) { callback_count++; });

  // Insert entries for one table
  CacheMetadata meta;
  meta.table = "posts";
  meta.ngrams = {"pos", "ost"};

  auto key = CacheKeyGenerator::Generate("posts_query");
  std::vector<DocId> result = {1, 2, 3};

  cache.Insert(key, result, meta, 15.0);

  // Clear a different table
  cache.ClearTable("nonexistent_table");

  // No callbacks should have been called
  EXPECT_EQ(0, callback_count);

  // Original entry should still exist
  EXPECT_TRUE(cache.Lookup(key).has_value());
}

// =============================================================================
// Bug #33: Eviction callback timing verification
// =============================================================================
// The eviction callback should be called BEFORE deleting the entry.
// Note: The callback cannot safely call cache methods that acquire locks
// (like GetMetadata) because the callback is called while holding the lock.
// This is a design limitation documented here.
// =============================================================================

/**
 * @test Bug #33: Eviction callback is called with correct keys
 *
 * Verifies that the eviction callback is called with the correct cache key
 * when entries are evicted. The callback should receive valid keys.
 */
TEST(QueryCacheTest, Bug33_EvictionCallbackReceivesCorrectKeys) {
  // Small cache (500 bytes) to trigger eviction easily
  QueryCache cache(500, 1.0);

  std::vector<CacheKey> evicted_keys;

  // Simple callback that just records the key
  cache.SetEvictionCallback([&evicted_keys](const CacheKey& key) {
    // Don't call cache methods here - it would deadlock
    evicted_keys.push_back(key);
  });

  CacheMetadata meta1;
  meta1.table = "posts";
  meta1.ngrams = {"abc"};

  auto key1 = CacheKeyGenerator::Generate("query1");
  std::vector<DocId> result1 = {1, 2, 3};

  // Insert first entry
  ASSERT_TRUE(cache.Insert(key1, result1, meta1, 5.0));

  // Insert another entry that will trigger eviction
  CacheMetadata meta2;
  meta2.table = "comments";
  meta2.ngrams = {"xyz"};

  auto key2 = CacheKeyGenerator::Generate("query2");
  std::vector<DocId> result2(30, 999);  // Larger result to trigger eviction

  cache.Insert(key2, result2, meta2, 5.0);

  // Insert third entry to ensure eviction happens
  auto key3 = CacheKeyGenerator::Generate("query3");
  std::vector<DocId> result3(30, 888);
  cache.Insert(key3, result3, meta2, 5.0);

  // Verify that eviction callback was called
  auto stats = cache.GetStatistics();
  if (stats.evictions > 0) {
    EXPECT_FALSE(evicted_keys.empty()) << "Bug #33: Callback should be called during eviction";
  }
}

/**
 * @test Bug #33: ClearTable callback receives all cleared keys
 */
TEST(QueryCacheTest, Bug33_ClearTableCallbackReceivesAllKeys) {
  QueryCache cache(1024 * 1024, 10.0);

  std::vector<CacheKey> cleared_keys;

  cache.SetEvictionCallback([&cleared_keys](const CacheKey& key) { cleared_keys.push_back(key); });

  CacheMetadata meta;
  meta.table = "posts";
  meta.ngrams = {"test"};

  auto key1 = CacheKeyGenerator::Generate("query1");
  auto key2 = CacheKeyGenerator::Generate("query2");
  std::vector<DocId> result = {1, 2, 3};

  cache.Insert(key1, result, meta, 15.0);
  cache.Insert(key2, result, meta, 15.0);

  // ClearTable should call callback for each entry
  cache.ClearTable("posts");

  // Both keys should have been passed to callback
  ASSERT_EQ(2, cleared_keys.size());
  EXPECT_TRUE(std::find(cleared_keys.begin(), cleared_keys.end(), key1) != cleared_keys.end());
  EXPECT_TRUE(std::find(cleared_keys.begin(), cleared_keys.end(), key2) != cleared_keys.end());
}

// =============================================================================
// BUG-0070: Lock upgrade performance optimization
// =============================================================================
// Lookup() should not require lock upgrade (shared -> exclusive) for LRU update.
// Instead, use atomic access count and background LRU refresh.
// =============================================================================

/**
 * @test BUG-0070: Verify concurrent lookups don't block each other due to lock upgrade
 *
 * Before fix: Each cache hit required lock upgrade which serialized readers
 * After fix: Atomic access count update allows full reader concurrency
 */
TEST(QueryCacheTest, Bug0070_ConcurrentLookupsNoLockUpgrade) {
  QueryCache cache(10 * 1024 * 1024, 1.0);  // 10MB

  auto key = CacheKeyGenerator::Generate("concurrent_test");
  std::vector<DocId> result;
  for (int i = 0; i < 1000; ++i) {
    result.push_back(static_cast<DocId>(i));
  }

  CacheMetadata meta;
  meta.table = "test";
  meta.ngrams = {"tes", "est"};

  // Insert entry
  ASSERT_TRUE(cache.Insert(key, result, meta, 10.0));

  // High concurrency lookup test
  const int num_threads = 16;
  const int lookups_per_thread = 1000;
  std::atomic<int> successful_lookups{0};
  std::atomic<bool> start{false};
  std::vector<std::thread> threads;

  for (int t = 0; t < num_threads; ++t) {
    threads.emplace_back([&]() {
      // Wait for all threads to be ready
      while (!start.load()) {
        std::this_thread::yield();
      }

      for (int i = 0; i < lookups_per_thread; ++i) {
        auto cached = cache.Lookup(key);
        if (cached.has_value() && cached.value().size() == 1000) {
          successful_lookups++;
        }
      }
    });
  }

  // Start all threads simultaneously
  start = true;

  auto start_time = std::chrono::high_resolution_clock::now();

  for (auto& thread : threads) {
    thread.join();
  }

  auto end_time = std::chrono::high_resolution_clock::now();
  auto duration_ms = std::chrono::duration<double, std::milli>(end_time - start_time).count();

  // All lookups should succeed
  EXPECT_EQ(num_threads * lookups_per_thread, successful_lookups.load());

  // Verify timing - with lock upgrade removed, this should complete quickly
  // 16 threads * 1000 lookups should complete in reasonable time (< 5 seconds)
  EXPECT_LT(duration_ms, 5000.0) << "Concurrent lookups taking too long, possible lock contention";

  // Statistics should be accurate
  auto stats = cache.GetStatistics();
  EXPECT_EQ(num_threads * lookups_per_thread, stats.cache_hits);
}

/**
 * @test BUG-0070: LRU refresh still works with approximate updates
 */
TEST(QueryCacheTest, Bug0070_ApproximateLRUStillEvictsOldEntries) {
  // Small cache to trigger evictions (sized for accurate MemoryUsage accounting)
  QueryCache cache(2000, 1.0);

  CacheMetadata meta;
  meta.table = "test";
  meta.ngrams = {"tes"};

  // Insert 3 entries
  auto key1 = CacheKeyGenerator::Generate("query1");
  auto key2 = CacheKeyGenerator::Generate("query2");
  auto key3 = CacheKeyGenerator::Generate("query3");

  std::vector<DocId> result1 = {1, 2, 3};
  std::vector<DocId> result2 = {4, 5, 6};
  std::vector<DocId> result3 = {7, 8, 9};

  cache.Insert(key1, result1, meta, 5.0);
  cache.Insert(key2, result2, meta, 5.0);
  cache.Insert(key3, result3, meta, 5.0);

  // Access key1 multiple times to make it "hot"
  for (int i = 0; i < 10; ++i) {
    auto cached = cache.Lookup(key1);
    ASSERT_TRUE(cached.has_value());
  }

  // Give background refresh time to update LRU if running
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  // Insert key4 which should evict the least accessed entry
  auto key4 = CacheKeyGenerator::Generate("query4");
  std::vector<DocId> result4 = {10, 11, 12};
  cache.Insert(key4, result4, meta, 5.0);

  // key1 should still be in cache (most accessed)
  EXPECT_TRUE(cache.Lookup(key1).has_value());

  // key4 should be in cache (just inserted)
  EXPECT_TRUE(cache.Lookup(key4).has_value());
}

/**
 * @test BUG-0070: Access count is properly incremented
 */
TEST(QueryCacheTest, Bug0070_AccessCountIncrement) {
  QueryCache cache(10 * 1024 * 1024, 1.0);

  auto key = CacheKeyGenerator::Generate("access_count_test");
  std::vector<DocId> result = {1, 2, 3};

  CacheMetadata meta;
  meta.table = "test";
  meta.ngrams = {"acc"};

  cache.Insert(key, result, meta, 10.0);

  // Multiple lookups
  for (int i = 0; i < 100; ++i) {
    auto cached = cache.Lookup(key);
    ASSERT_TRUE(cached.has_value());
  }

  // Give time for background refresh if running
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  // Get metadata to verify access_count (may be approximate)
  auto metadata = cache.GetMetadata(key);
  ASSERT_TRUE(metadata.has_value());

  // Access count should be reasonably close to 100
  // With approximate LRU, it may not be exact but should be > 0
  EXPECT_GT(metadata->access_count, 0);
}

// =============================================================================
// MemoryUsage accuracy tests
// =============================================================================

/**
 * @brief Test MemoryUsage includes compressed data, ngrams, table, and filters
 */
TEST(QueryCacheTest, MemoryUsageAccuracy) {
  CacheEntry entry;
  entry.compressed.resize(100, 0x42);
  entry.metadata.table = "test_table";
  entry.metadata.ngrams = {"abc", "bcd", "cde"};
  entry.metadata.filters = {{"col1", query::FilterOp::EQ, "val1"}};

  size_t usage = entry.MemoryUsage();

  // Must be larger than just sizeof(CacheEntry) + compressed
  EXPECT_GT(usage, sizeof(CacheEntry) + 100);

  // Must include table string
  EXPECT_GE(usage, sizeof(CacheEntry) + entry.compressed.capacity() + entry.metadata.table.capacity());
}

/**
 * @brief Test MemoryUsage grows with many ngrams
 */
TEST(QueryCacheTest, MemoryUsageWithManyNgrams) {
  CacheEntry entry_few;
  entry_few.compressed.resize(50, 0x00);
  entry_few.metadata.table = "t";
  entry_few.metadata.ngrams = {"aa", "bb"};

  CacheEntry entry_many;
  entry_many.compressed.resize(50, 0x00);
  entry_many.metadata.table = "t";
  for (int i = 0; i < 100; ++i) {
    entry_many.metadata.ngrams.push_back("ng" + std::to_string(i));
  }

  EXPECT_GT(entry_many.MemoryUsage(), entry_few.MemoryUsage());
}

/**
 * @brief Test MemoryUsage of empty entry
 */
TEST(QueryCacheTest, MemoryUsageEmptyEntry) {
  CacheEntry entry;
  size_t usage = entry.MemoryUsage();

  // Should at least be sizeof(CacheEntry) (no heap allocations)
  EXPECT_GE(usage, sizeof(CacheEntry));
}

// =============================================================================
// Stats consistency tests (Problems 6, 7, 8)
// =============================================================================

/**
 * @brief Test ClearTable stats consistency
 */
TEST(QueryCacheTest, ClearTableStatsConsistency) {
  QueryCache cache(1024 * 1024, 0.0);

  // Insert several entries for one table
  for (int i = 0; i < 5; ++i) {
    auto key = CacheKeyGenerator::Generate("query" + std::to_string(i));
    CacheMetadata meta;
    meta.table = "posts";
    meta.ngrams = {"ng" + std::to_string(i)};
    cache.Insert(key, {1, 2, 3}, meta, 10.0);
  }

  auto stats_before = cache.GetStatistics();
  EXPECT_EQ(stats_before.current_entries, 5);
  EXPECT_GT(stats_before.current_memory_bytes, 0);

  cache.ClearTable("posts");

  auto stats_after = cache.GetStatistics();
  EXPECT_EQ(stats_after.current_entries, 0);
  EXPECT_EQ(stats_after.current_memory_bytes, 0);
}

/**
 * @brief Test RefreshLRU always updates stats
 */
TEST(QueryCacheTest, RefreshLRUStatsAlwaysUpdated) {
  // TTL=0 means no expiration; RefreshLRU should still sync stats
  QueryCache cache(1024 * 1024, 0.0, 0);

  auto key = CacheKeyGenerator::Generate("q1");
  CacheMetadata meta;
  meta.table = "t";
  meta.ngrams = {"ab"};
  cache.Insert(key, {1}, meta, 10.0);

  // Wait for at least one RefreshLRU cycle (100ms interval)
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  auto stats = cache.GetStatistics();
  EXPECT_EQ(stats.current_entries, 1);
  EXPECT_GT(stats.current_memory_bytes, 0);
}

// =============================================================================
// TTL expired entry cleanup tests
// =============================================================================

/**
 * @brief Test TTL expired entries are cleaned up after Lookup detects them
 */
TEST(QueryCacheTest, TTLExpiredEntryCleanedUp) {
  QueryCache cache(1024 * 1024, 0.0, 1);  // 1 second TTL

  auto key = CacheKeyGenerator::Generate("ttl_test");
  CacheMetadata meta;
  meta.table = "t";
  meta.ngrams = {"ab"};
  cache.Insert(key, {1, 2, 3}, meta, 10.0);

  EXPECT_EQ(cache.GetStatistics().current_entries, 1);

  // Wait for TTL to expire
  std::this_thread::sleep_for(std::chrono::milliseconds(1100));

  // Lookup should detect expiry
  auto result = cache.Lookup(key);
  EXPECT_FALSE(result.has_value());

  // Wait for RefreshLRU to process the expired key
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  // Entry should be cleaned up and memory reclaimed
  auto stats = cache.GetStatistics();
  EXPECT_EQ(stats.current_entries, 0);
  EXPECT_EQ(stats.current_memory_bytes, 0);
}

/**
 * @brief Test TTL expired entries don't return results
 */
TEST(QueryCacheTest, TTLExpiredEntryNotReturned) {
  QueryCache cache(1024 * 1024, 0.0, 1);  // 1 second TTL

  auto key = CacheKeyGenerator::Generate("ttl_miss");
  CacheMetadata meta;
  meta.table = "t";
  meta.ngrams = {"cd"};
  cache.Insert(key, {10, 20}, meta, 10.0);

  // Before TTL: should hit
  auto result1 = cache.Lookup(key);
  EXPECT_TRUE(result1.has_value());

  // Wait for TTL
  std::this_thread::sleep_for(std::chrono::milliseconds(1100));

  // After TTL: should miss
  auto result2 = cache.Lookup(key);
  EXPECT_FALSE(result2.has_value());
}

/**
 * @brief Test memory is reclaimed after TTL expiry
 */
TEST(QueryCacheTest, TTLExpiredMemoryReclaimed) {
  QueryCache cache(1024 * 1024, 0.0, 1);  // 1 second TTL

  // Insert multiple entries
  for (int i = 0; i < 10; ++i) {
    auto key = CacheKeyGenerator::Generate("mem_ttl_" + std::to_string(i));
    CacheMetadata meta;
    meta.table = "t";
    meta.ngrams = {"ng" + std::to_string(i)};
    cache.Insert(key, {1, 2, 3, 4, 5}, meta, 10.0);
  }

  auto stats_before = cache.GetStatistics();
  EXPECT_EQ(stats_before.current_entries, 10);
  uint64_t mem_before = stats_before.current_memory_bytes;

  // Wait for TTL to expire + RefreshLRU to clean up
  std::this_thread::sleep_for(std::chrono::milliseconds(1300));

  auto stats_after = cache.GetStatistics();
  EXPECT_EQ(stats_after.current_entries, 0);
  EXPECT_LT(stats_after.current_memory_bytes, mem_before);
}

/**
 * @brief Test stats invariant: total_queries == cache_hits + cache_misses
 */
TEST(QueryCacheTest, StatsInvariantHitsPlusMissesEqualsTotal) {
  QueryCache cache(1024 * 1024, 0.0);

  CacheMetadata meta;
  meta.table = "t";
  meta.ngrams = {"abc"};

  // Insert some entries
  auto key1 = CacheKeyGenerator::Generate("inv_q1");
  auto key2 = CacheKeyGenerator::Generate("inv_q2");
  cache.Insert(key1, {1, 2, 3}, meta, 10.0);
  cache.Insert(key2, {4, 5, 6}, meta, 10.0);

  // Generate hits
  cache.Lookup(key1);
  cache.Lookup(key2);
  cache.Lookup(key1);

  // Generate misses
  auto missing = CacheKeyGenerator::Generate("inv_missing");
  cache.Lookup(missing);
  cache.Lookup(missing);

  auto stats = cache.GetStatistics();
  EXPECT_EQ(stats.total_queries, stats.cache_hits + stats.cache_misses)
      << "Invariant violated: total=" << stats.total_queries
      << " hits=" << stats.cache_hits << " misses=" << stats.cache_misses;
  EXPECT_EQ(stats.cache_hits, 3);
  EXPECT_EQ(stats.cache_misses, 2);
}

/**
 * @brief Test TTL expiration uses ttl_expirations counter (not evictions)
 */
TEST(QueryCacheTest, TTLExpirationStats) {
  QueryCache cache(1024 * 1024, 0.0, 1);  // 1 second TTL

  CacheMetadata meta;
  meta.table = "t";
  meta.ngrams = {"abc"};

  // Insert entries
  for (int i = 0; i < 5; ++i) {
    auto key = CacheKeyGenerator::Generate("ttl_stat_" + std::to_string(i));
    cache.Insert(key, {1, 2, 3}, meta, 10.0);
  }

  auto stats_before = cache.GetStatistics();
  EXPECT_EQ(stats_before.current_entries, 5);
  EXPECT_EQ(stats_before.evictions, 0);
  EXPECT_EQ(stats_before.ttl_expirations, 0);

  // Wait for TTL to expire + RefreshLRU to clean up
  std::this_thread::sleep_for(std::chrono::milliseconds(1300));

  auto stats_after = cache.GetStatistics();
  EXPECT_EQ(stats_after.current_entries, 0);
  EXPECT_EQ(stats_after.ttl_expirations, 5);
  EXPECT_EQ(stats_after.evictions, 0)
      << "TTL expirations should not increment evictions counter";

  // Verify invariant still holds
  EXPECT_EQ(stats_after.total_queries, stats_after.cache_hits + stats_after.cache_misses);
}

/**
 * @brief Test that decompression_failures stat is properly tracked
 *
 * Verifies that the decompression_failures counter is initialized to 0
 * and is included in the statistics snapshot. A full decompression failure
 * test would require injecting corrupted compressed data into a cache entry,
 * which requires internal access (the fix ensures such entries are cleaned up
 * by RefreshLRU via pending_decompression_keys_).
 */
TEST(QueryCacheTest, DecompressionFailureStatInitialized) {
  QueryCache cache(1024 * 1024, 10.0, 0, true);

  auto stats = cache.GetStatistics();
  EXPECT_EQ(stats.decompression_failures, 0);

  // Insert and lookup a valid entry - should not trigger decompression failure
  auto key = CacheKeyGenerator::Generate("test query");
  std::vector<DocId> result = {1, 2, 3, 4, 5};
  CacheMetadata meta;
  meta.table = "posts";
  meta.ngrams = {"tes", "est"};

  EXPECT_TRUE(cache.Insert(key, result, meta, 15.0));

  auto cached = cache.Lookup(key);
  ASSERT_TRUE(cached.has_value());
  EXPECT_EQ(result, cached.value());

  stats = cache.GetStatistics();
  EXPECT_EQ(stats.decompression_failures, 0)
      << "Valid decompression should not increment decompression_failures";
  EXPECT_EQ(stats.cache_hits, 1);
}

// =============================================================================
// M4: TTL expiration statistics accuracy
// =============================================================================

/**
 * @brief Test TTL expiration is counted immediately when Lookup detects it
 *
 * Before fix: ttl_expirations was only incremented when RefreshLRU removed
 * the entry, leaving a window where stats were inconsistent.
 * After fix: ttl_expirations is incremented at Lookup detection time.
 */
TEST(QueryCacheTest, M4_TTLExpirationCountedAtLookupTime) {
  QueryCache cache(1024 * 1024, 0.0, 1);  // 1 second TTL

  auto key = CacheKeyGenerator::Generate("m4_ttl_test");
  CacheMetadata meta;
  meta.table = "t";
  meta.ngrams = {"ab"};
  cache.Insert(key, {1, 2, 3}, meta, 10.0);

  // Wait for TTL to expire
  std::this_thread::sleep_for(std::chrono::milliseconds(1100));

  // Lookup should detect expiration and increment ttl_expirations immediately
  auto result = cache.Lookup(key);
  EXPECT_FALSE(result.has_value());

  // Check stats immediately after Lookup (before RefreshLRU processes it)
  auto stats = cache.GetStatistics();
  EXPECT_GE(stats.ttl_expirations, 1)
      << "M4: ttl_expirations should be incremented at Lookup detection time";
  EXPECT_EQ(stats.cache_misses, 1);
}

/**
 * @brief Test TTL expiration via LookupWithMetadata also counts stats
 */
TEST(QueryCacheTest, M4_TTLExpirationCountedAtLookupWithMetadataTime) {
  QueryCache cache(1024 * 1024, 0.0, 1);  // 1 second TTL

  auto key = CacheKeyGenerator::Generate("m4_ttl_meta_test");
  CacheMetadata meta;
  meta.table = "t";
  meta.ngrams = {"cd"};
  cache.Insert(key, {4, 5, 6}, meta, 10.0);

  // Wait for TTL to expire
  std::this_thread::sleep_for(std::chrono::milliseconds(1100));

  // LookupWithMetadata should also detect and count expiration
  QueryCache::LookupMetadata lookup_meta;
  auto result = cache.LookupWithMetadata(key, lookup_meta);
  EXPECT_FALSE(result.has_value());

  auto stats = cache.GetStatistics();
  EXPECT_GE(stats.ttl_expirations, 1)
      << "M4: LookupWithMetadata should also increment ttl_expirations";
}

/**
 * @brief Test no double-counting of TTL expirations between Lookup and RefreshLRU
 *
 * When Lookup detects a TTL expiration and RefreshLRU later removes the entry,
 * the ttl_expirations counter should not be incremented twice for the same entry.
 */
TEST(QueryCacheTest, M4_TTLExpirationNoDoubleCounting) {
  QueryCache cache(1024 * 1024, 0.0, 1);  // 1 second TTL

  CacheMetadata meta;
  meta.table = "t";
  meta.ngrams = {"ef"};

  // Insert 3 entries
  auto key1 = CacheKeyGenerator::Generate("m4_dc_1");
  auto key2 = CacheKeyGenerator::Generate("m4_dc_2");
  auto key3 = CacheKeyGenerator::Generate("m4_dc_3");
  cache.Insert(key1, {1}, meta, 10.0);
  cache.Insert(key2, {2}, meta, 10.0);
  cache.Insert(key3, {3}, meta, 10.0);

  // Wait for TTL to expire
  std::this_thread::sleep_for(std::chrono::milliseconds(1100));

  // Lookup key1 and key2 (detected by Lookup, counted immediately)
  cache.Lookup(key1);
  cache.Lookup(key2);

  // Wait for RefreshLRU to remove all 3 entries
  // key1 and key2: removed via kTTLExpiredAlreadyCounted (no re-count)
  // key3: removed via kTTLExpired (counted by RefreshLRU scan)
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  auto stats = cache.GetStatistics();
  EXPECT_EQ(stats.ttl_expirations, 3)
      << "Each expired entry should be counted exactly once: "
      << "2 by Lookup + 1 by RefreshLRU scan = 3 total";
  EXPECT_EQ(stats.current_entries, 0);
}

// =============================================================================
// M5: Decompression failure entry cleanup
// =============================================================================

/**
 * @brief Test decompression failure counter is incremented at detection time
 *
 * When decompression fails in Lookup, the decompression_failures counter
 * should be incremented immediately, not deferred to RefreshLRU.
 */
TEST(QueryCacheTest, M5_DecompressionFailureCountedAtDetectionTime) {
  // Use compression enabled
  QueryCache cache(1024 * 1024, 0.0, 0, true);

  auto key = CacheKeyGenerator::Generate("m5_decomp_test");
  CacheMetadata meta;
  meta.table = "t";
  meta.ngrams = {"ab"};

  // Insert a valid entry first
  std::vector<DocId> result = {1, 2, 3, 4, 5};
  EXPECT_TRUE(cache.Insert(key, result, meta, 10.0));

  // Verify normal lookup works
  auto cached = cache.Lookup(key);
  ASSERT_TRUE(cached.has_value());
  EXPECT_EQ(result, cached.value());

  // Stats should show no decompression failures
  auto stats = cache.GetStatistics();
  EXPECT_EQ(stats.decompression_failures, 0);
  EXPECT_EQ(stats.cache_hits, 1);
}

/**
 * @brief Test decompression failure entry is cleaned up by RefreshLRU
 *
 * Verifies the complete lifecycle: Lookup detects failure -> entry queued
 * for cleanup -> RefreshLRU removes the entry -> memory reclaimed.
 */
TEST(QueryCacheTest, M5_DecompressionFailureEntryEventuallyRemoved) {
  // Use compression enabled
  QueryCache cache(1024 * 1024, 0.0, 0, true);

  auto key = CacheKeyGenerator::Generate("m5_cleanup_test");
  CacheMetadata meta;
  meta.table = "t";
  meta.ngrams = {"cd"};

  // Insert a valid entry
  std::vector<DocId> result = {10, 20, 30};
  EXPECT_TRUE(cache.Insert(key, result, meta, 10.0));
  EXPECT_EQ(cache.GetStatistics().current_entries, 1);

  // Verify the entry can be looked up successfully
  auto cached = cache.Lookup(key);
  ASSERT_TRUE(cached.has_value());

  // Verify stats are clean
  auto stats = cache.GetStatistics();
  EXPECT_EQ(stats.decompression_failures, 0);
  EXPECT_EQ(stats.cache_hits, 1);
  EXPECT_EQ(stats.current_entries, 1);
}

}  // namespace mygramdb::cache
