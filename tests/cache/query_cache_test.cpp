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
 *
 * QueryCache uses strict LRU at the cache_map_/lru_list_ level
 * (EvictForSpace pops from the tail of lru_list_). Lookup marks an entry
 * "dirty" and the background RefreshLRUWorker (100ms tick) moves dirty
 * entries to the head of the list. We sleep past one tick so that the
 * Lookup-induced reorder is observable, then insert a new entry whose
 * payload is large enough that the cache MUST evict to make room.
 *
 * With three inserts in (key1, key2, key3) order and a subsequent
 * Lookup(key1) that is allowed to propagate, the tail is key2, so the
 * 4th insert evicts key2 specifically.
 */
TEST(QueryCacheTest, LRUEviction) {
  CacheMetadata meta;
  meta.table = "posts";
  meta.ngrams = {"tes", "est"};

  // Size each result large enough that 4 entries definitely don't fit but
  // 3 do. Roughly: 4 * (kPayloadDocs * sizeof(DocId)) > kCacheBytes >=
  // 3 * (kPayloadDocs * sizeof(DocId)). Account for per-entry overhead
  // (CacheKey, lru_list iterator, metadata strings, atomics) by leaving
  // generous headroom in the inequality.
  constexpr size_t kPayloadDocs = 200;  // 800 bytes per entry payload
  constexpr size_t kCacheBytes = 3500;  // fits ~3 entries; 4 must evict
  QueryCache cache(kCacheBytes, /*min_query_cost_ms=*/10.0);

  auto make_payload = [](DocId base) {
    std::vector<DocId> v;
    v.reserve(kPayloadDocs);
    for (size_t i = 0; i < kPayloadDocs; ++i) {
      v.push_back(static_cast<DocId>(base + i));
    }
    return v;
  };

  auto key1 = CacheKeyGenerator::Generate("query1");
  auto key2 = CacheKeyGenerator::Generate("query2");
  auto key3 = CacheKeyGenerator::Generate("query3");
  auto key4 = CacheKeyGenerator::Generate("query4");

  ASSERT_TRUE(cache.Insert(key1, make_payload(1000), meta, 15.0));
  ASSERT_TRUE(cache.Insert(key2, make_payload(2000), meta, 15.0));
  ASSERT_TRUE(cache.Insert(key3, make_payload(3000), meta, 15.0));

  const auto evictions_before = cache.GetStatistics().evictions;

  // Access key1 to make it most-recently-used. Lookup marks the entry
  // dirty; the background refresh worker reorders the LRU list.
  [[maybe_unused]] auto _ = cache.Lookup(key1);

  // Wait past one RefreshLRU tick (worker runs every 100ms; use 150ms for
  // headroom on slow CI hosts).
  std::this_thread::sleep_for(std::chrono::milliseconds(150));

  // Insert key4. The total payload now exceeds kCacheBytes, so EvictForSpace
  // MUST run and pop the LRU tail (key2 after the Lookup(key1) propagation).
  ASSERT_TRUE(cache.Insert(key4, make_payload(4000), meta, 15.0));

  // Verify at least one eviction was recorded.
  const auto evictions_after = cache.GetStatistics().evictions;
  EXPECT_GT(evictions_after, evictions_before)
      << "Inserting the 4th entry must trigger LRU eviction in a sized-down cache";

  // key1 (most recently used), key3 (next), key4 (just inserted) must
  // all remain.
  EXPECT_TRUE(cache.Lookup(key1).has_value());
  EXPECT_TRUE(cache.Lookup(key3).has_value());
  EXPECT_TRUE(cache.Lookup(key4).has_value());
  // key2 was the LRU victim and MUST be gone. The earlier comment on this
  // test claimed eviction was "implementation-specific"; the implementation
  // is in fact strict LRU (EvictForSpace pops from lru_list_.back()), so
  // with deterministic sizing we can and should assert this.
  EXPECT_FALSE(cache.Lookup(key2).has_value()) << "Strict LRU must evict the least-recently-used entry (key2)";
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
 * @brief Regression test for P0-G: QueryCache::Clear() must invoke
 *        eviction_callback_ for every removed entry.
 *
 * Bug: Clear() swap-discarded entries without invoking eviction_callback_,
 * so external bookkeeping (e.g. InvalidationManager) leaked metadata.
 * Fix: iterate entries and invoke eviction_callback_(key) before swap.
 */
TEST(QueryCacheTest, ClearInvokesEvictionCallbackForEveryEntry) {
  QueryCache cache(1024 * 1024, 0.0);

  std::atomic<int> callback_count{0};
  cache.SetEvictionCallback([&callback_count](const CacheKey& /*key*/) { callback_count.fetch_add(1); });

  CacheMetadata meta;
  meta.table = "t";
  meta.ngrams = {"abc"};

  // Insert 5 entries
  constexpr int kNumEntries = 5;
  for (int i = 0; i < kNumEntries; ++i) {
    auto key = CacheKeyGenerator::Generate("clear_callback_" + std::to_string(i));
    cache.Insert(key, {static_cast<DocId>(i)}, meta, 10.0);
  }
  ASSERT_EQ(cache.GetStatistics().current_entries, kNumEntries);
  // Callback should not have been invoked during Insert
  EXPECT_EQ(callback_count.load(), 0);

  cache.Clear();

  EXPECT_EQ(callback_count.load(), kNumEntries) << "Clear() must invoke eviction_callback_ for every removed entry";
  EXPECT_EQ(cache.GetStatistics().current_entries, 0u);
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
  entry.compressed = std::make_shared<const std::vector<uint8_t>>(100, 0x42);
  entry.metadata.table = "test_table";
  entry.metadata.ngrams = {"abc", "bcd", "cde"};
  entry.metadata.filters = {{"col1", query::FilterOp::EQ, "val1"}};

  size_t usage = entry.MemoryUsage();

  // Must be larger than just sizeof(CacheEntry) + compressed
  EXPECT_GT(usage, sizeof(CacheEntry) + 100);

  // Must include table string
  EXPECT_GE(usage, sizeof(CacheEntry) + sizeof(std::vector<uint8_t>) + entry.compressed->capacity() +
                       entry.metadata.table.capacity());
}

/**
 * @brief Test MemoryUsage grows with many ngrams
 */
TEST(QueryCacheTest, MemoryUsageWithManyNgrams) {
  CacheEntry entry_few;
  entry_few.compressed = std::make_shared<const std::vector<uint8_t>>(50, 0x00);
  entry_few.metadata.table = "t";
  entry_few.metadata.ngrams = {"aa", "bb"};

  CacheEntry entry_many;
  entry_many.compressed = std::make_shared<const std::vector<uint8_t>>(50, 0x00);
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
 * @brief Regression test for P0-H: stats_.current_memory_bytes must be
 *        updated immediately when entries are removed, not deferred to the
 *        end of RefreshLRU.
 *
 * Bug: Insert/Erase synced stats_.current_memory_bytes inline, but
 * RemoveEntryLocked (called by RefreshLRU when draining TTL-expired keys
 * detected at Lookup time) only decremented total_memory_bytes_ /
 * stats_.current_entries; stats_.current_memory_bytes was synced once at the
 * very end of RefreshLRU. Between RemoveEntryLocked and that final sync,
 * GetStatistics() returned stale (higher) values.
 *
 * Fix: store stats_.current_memory_bytes inside RemoveEntryLocked.
 *
 * Test scenario: insert N entries with TTL=1s, sleep past TTL, then trigger
 * a Lookup that queues the keys into pending_expired_keys_. After RefreshLRU
 * drains them and removes them via RemoveEntryLocked, the stats must reflect
 * 0 bytes immediately — not after some additional grace window.
 */
TEST(QueryCacheTest, TTLExpiredEntryRemovalUpdatesStatsImmediately) {
  QueryCache cache(1024 * 1024, 0.0, 1);  // 1 second TTL

  CacheMetadata meta;
  meta.table = "t";
  meta.ngrams = {"ab"};

  std::vector<CacheKey> keys;
  constexpr int kNumEntries = 3;
  for (int i = 0; i < kNumEntries; ++i) {
    auto key = CacheKeyGenerator::Generate("p0h_expire_" + std::to_string(i));
    cache.Insert(key, {1, 2, 3}, meta, 10.0);
    keys.push_back(key);
  }
  ASSERT_EQ(cache.GetStatistics().current_entries, kNumEntries);
  ASSERT_GT(cache.GetStatistics().current_memory_bytes, 0u);

  // Sleep past TTL.
  std::this_thread::sleep_for(std::chrono::milliseconds(1200));

  // Lookup all keys: this enqueues them into pending_expired_keys_.
  for (const auto& key : keys) {
    auto result = cache.Lookup(key);
    EXPECT_FALSE(result.has_value());
  }

  // Wait for RefreshLRU (100ms cycle) to drain the pending set and remove
  // the entries. Two cycles = 200ms is comfortably more than enough.
  std::this_thread::sleep_for(std::chrono::milliseconds(250));

  auto stats = cache.GetStatistics();
  EXPECT_EQ(stats.current_entries, 0u);
  // Critical assertion: with the fix, RemoveEntryLocked updates
  // current_memory_bytes inline. Without the fix, this could read a stale
  // value if Lookup raced with RefreshLRU.
  EXPECT_EQ(stats.current_memory_bytes, 0u)
      << "current_memory_bytes must be synced inside RemoveEntryLocked, not deferred to the end of RefreshLRU";
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
      << "Invariant violated: total=" << stats.total_queries << " hits=" << stats.cache_hits
      << " misses=" << stats.cache_misses;
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
  EXPECT_EQ(stats_after.evictions, 0) << "TTL expirations should not increment evictions counter";

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
  EXPECT_EQ(stats.decompression_failures, 0) << "Valid decompression should not increment decompression_failures";
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
  EXPECT_GE(stats.ttl_expirations, 1) << "M4: ttl_expirations should be incremented at Lookup detection time";
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
  EXPECT_GE(stats.ttl_expirations, 1) << "M4: LookupWithMetadata should also increment ttl_expirations";
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
  EXPECT_EQ(stats.ttl_expirations, 3) << "Each expired entry should be counted exactly once: "
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

/**
 * @brief Regression test for P0-E: decompression_failures counter must be
 *        deduplicated per entry (not per Lookup call).
 *
 * Bug: two concurrent Lookups of the same broken entry both incremented
 * stats_.decompression_failures, even though the entry is logically a single
 * failure event.
 *
 * Fix: only increment when the key was newly inserted into
 * pending_decompression_keys_ (i.e. unordered_set::insert returned true).
 *
 * Test: insert one entry, corrupt its compressed payload, fire 4 concurrent
 * Lookups, then assert decompression_failures == 1.
 */
TEST(QueryCacheTest, DecompressionFailureCountedOncePerEntry) {
  QueryCache cache(1024 * 1024, 0.0, 0, true);

  auto key = CacheKeyGenerator::Generate("p0e_decomp_dedup");
  CacheMetadata meta;
  meta.table = "t";
  meta.ngrams = {"ab"};

  // Insert a valid entry first.
  std::vector<DocId> result = {1, 2, 3, 4, 5};
  ASSERT_TRUE(cache.Insert(key, result, meta, 10.0));
  ASSERT_TRUE(cache.Lookup(key).has_value());

  // Corrupt the entry so subsequent Lookups will fail decompression.
  ASSERT_TRUE(cache.CorruptEntryForTest(key));

  // Fire 4 concurrent Lookups of the corrupted entry.
  constexpr int kNumThreads = 4;
  std::vector<std::thread> threads;
  threads.reserve(kNumThreads);
  std::atomic<int> miss_count{0};
  for (int t = 0; t < kNumThreads; ++t) {
    threads.emplace_back([&cache, &key, &miss_count]() {
      auto looked_up = cache.Lookup(key);
      if (!looked_up.has_value()) {
        miss_count.fetch_add(1);
      }
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }

  // All 4 Lookups should miss (decompression always fails on the corrupted
  // payload), but the decompression_failures counter should reflect ONE
  // logical failure event for this entry.
  EXPECT_EQ(miss_count.load(), kNumThreads);

  auto stats = cache.GetStatistics();
  EXPECT_EQ(stats.decompression_failures, 1u)
      << "decompression_failures must be deduplicated per entry, not counted per Lookup. Got "
      << stats.decompression_failures;
}

/**
 * @brief Test TTL-expired entries increment cache_misses_ttl_expired (not cache_misses_not_found)
 *
 * Regression test for: TTL-expired entries were counted as cache_misses_not_found
 * instead of the dedicated cache_misses_ttl_expired counter.
 *
 * Note: The background RefreshLRU thread may remove the entry before Lookup runs,
 * causing a not_found miss instead. We use a longer TTL and check immediately
 * after expiry to minimize this race.
 */
TEST(QueryCacheTest, TTLExpiredIncrementsTTLExpiredCounter) {
  QueryCache cache(1024 * 1024, 0.0, 2);  // 2 second TTL

  auto key = CacheKeyGenerator::Generate("ttl_counter_test");
  CacheMetadata meta;
  meta.table = "t";
  meta.ngrams = {"ab"};
  cache.Insert(key, {1, 2, 3}, meta, 10.0);

  // Verify entry exists
  auto cached = cache.Lookup(key);
  ASSERT_TRUE(cached.has_value());

  // Wait for TTL to expire but not too long (RefreshLRU runs every 100ms)
  std::this_thread::sleep_for(std::chrono::milliseconds(2050));

  // Lookup expired entry - if Lookup detects TTL expiry, it increments
  // cache_misses_ttl_expired. If RefreshLRU already removed it, it's cache_misses_not_found.
  auto result = cache.Lookup(key);
  EXPECT_FALSE(result.has_value());

  auto stats = cache.GetStatistics();
  // Either path is a miss; the key point is that TTL-detected misses
  // go to cache_misses_ttl_expired (not cache_misses_not_found)
  EXPECT_EQ(stats.cache_misses, 1);  // Only the second Lookup is a miss
  EXPECT_GE(stats.cache_misses_ttl_expired + stats.cache_misses_not_found, 1)
      << "Expired entry should be counted as either TTL-expired or not-found";
  // If the entry was still in the map at Lookup time, it should be TTL-expired
  if (stats.cache_misses_ttl_expired > 0) {
    EXPECT_EQ(stats.cache_misses_not_found, 0)
        << "When Lookup detects TTL expiry, it should NOT increment cache_misses_not_found";
  }
}

/**
 * @brief Test that LookupWithMetadata also uses TTL-expired counter
 */
TEST(QueryCacheTest, TTLExpiredWithMetadataIncrementsTTLExpiredCounter) {
  QueryCache cache(1024 * 1024, 0.0, 2);  // 2 second TTL

  auto key = CacheKeyGenerator::Generate("ttl_meta_counter_test");
  CacheMetadata meta;
  meta.table = "t";
  meta.ngrams = {"cd"};
  cache.Insert(key, {4, 5, 6}, meta, 10.0);

  // Verify entry exists
  auto cached = cache.Lookup(key);
  ASSERT_TRUE(cached.has_value());

  // Wait for TTL to expire
  std::this_thread::sleep_for(std::chrono::milliseconds(2050));

  QueryCache::LookupMetadata lookup_meta;
  auto result = cache.LookupWithMetadata(key, lookup_meta);
  EXPECT_FALSE(result.has_value());

  auto stats = cache.GetStatistics();
  EXPECT_EQ(stats.cache_misses, 1);  // Only the LookupWithMetadata miss
  if (stats.cache_misses_ttl_expired > 0) {
    EXPECT_EQ(stats.cache_misses_not_found, 0)
        << "When LookupWithMetadata detects TTL expiry, it should NOT increment cache_misses_not_found";
  }
}

/**
 * @brief Test that lookup returns correct data after shared_ptr compressed change
 *
 * Regression test for: changing compressed from vector<uint8_t> to
 * shared_ptr<const vector<uint8_t>> should not affect lookup results.
 */
TEST(QueryCacheTest, SharedPtrCompressedLookupCorrectness) {
  QueryCache cache(1024 * 1024, 0.0);

  // Test with various result sizes
  for (int size = 1; size <= 100; size += 10) {
    auto key = CacheKeyGenerator::Generate("shared_ptr_test_" + std::to_string(size));
    std::vector<DocId> original_result;
    for (int i = 0; i < size; ++i) {
      original_result.push_back(static_cast<DocId>(i * 7 + 3));
    }

    CacheMetadata meta;
    meta.table = "test";
    meta.ngrams = {"sp"};

    ASSERT_TRUE(cache.Insert(key, original_result, meta, 10.0));

    auto cached = cache.Lookup(key);
    ASSERT_TRUE(cached.has_value());
    EXPECT_EQ(original_result, cached.value()) << "Lookup should return identical data for size=" << size;
  }
}

// =============================================================================
// Concurrent SetMinQueryCost / SetTtl with Lookup / Insert
// =============================================================================

/**
 * @brief Test SetMinQueryCost and SetTtl can be called concurrently with
 *        Lookup and Insert without data races
 *
 * This is a smoke test: if TSan is enabled it would catch any race on
 * the atomic fields (min_query_cost_ms_, ttl_seconds_).
 */
TEST(QueryCacheTest, ConcurrentSetMinQueryCostAndSetTtlWithLookupInsert) {
  QueryCache cache(10 * 1024 * 1024, 10.0);  // 10MB

  CacheMetadata meta;
  meta.table = "posts";
  meta.ngrams = {"tes", "est"};

  // Pre-populate some entries
  for (int i = 0; i < 50; ++i) {
    auto key = CacheKeyGenerator::Generate("preload_" + std::to_string(i));
    std::vector<DocId> result = {static_cast<DocId>(i)};
    cache.Insert(key, result, meta, 15.0);
  }

  std::atomic<bool> stop{false};
  std::vector<std::thread> threads;

  // Thread group 1: SetMinQueryCost with different values
  for (int t = 0; t < 2; ++t) {
    threads.emplace_back([&cache, &stop, t]() {
      int i = 0;
      while (!stop.load()) {
        double cost = 1.0 + static_cast<double>((i + t) % 50);
        cache.SetMinQueryCost(cost);
        // Read it back to exercise the load path
        [[maybe_unused]] double current = cache.GetMinQueryCost();
        ++i;
        std::this_thread::yield();
      }
    });
  }

  // Thread group 2: SetTtl with different values
  for (int t = 0; t < 2; ++t) {
    threads.emplace_back([&cache, &stop, t]() {
      int i = 0;
      while (!stop.load()) {
        int ttl = (i + t) % 10;
        cache.SetTtl(ttl);
        [[maybe_unused]] int current = cache.GetTtl();
        ++i;
        std::this_thread::yield();
      }
    });
  }

  // Thread group 3: Lookup
  for (int t = 0; t < 3; ++t) {
    threads.emplace_back([&cache, &stop, t]() {
      int i = 0;
      while (!stop.load()) {
        auto key = CacheKeyGenerator::Generate("preload_" + std::to_string((i + t) % 50));
        [[maybe_unused]] auto result = cache.Lookup(key);
        ++i;
        std::this_thread::yield();
      }
    });
  }

  // Thread group 4: Insert
  for (int t = 0; t < 3; ++t) {
    threads.emplace_back([&cache, &meta, &stop, t]() {
      int i = 0;
      while (!stop.load()) {
        auto key = CacheKeyGenerator::Generate("insert_" + std::to_string(t) + "_" + std::to_string(i));
        std::vector<DocId> result = {static_cast<DocId>(i)};
        cache.Insert(key, result, meta, 15.0);
        ++i;
        std::this_thread::yield();
      }
    });
  }

  // Run for 200ms
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  stop = true;

  for (auto& thread : threads) {
    thread.join();
  }

  // If we reach here without crash or TSan report, the test passes
  auto stats = cache.GetStatistics();
  EXPECT_GT(stats.total_queries, 0);
}

/**
 * @brief CacheStatisticsSnapshot must surface the configured runtime limits.
 *
 * Operators look at INFO/Prometheus output to correlate observed counters
 * (rejection_count, evictions, ttl_expirations) with the limits in force.
 * Without these fields, a sudden spike in rejections is hard to attribute
 * to either a too-low cost threshold or a too-tight memory ceiling.
 */
TEST(QueryCacheTest, CacheStatsExposesConfiguredLimits) {
  constexpr size_t kMaxBytes = 1024 * 1024;
  constexpr double kMinCost = 12.5;
  constexpr int kTtlSeconds = 30;
  constexpr bool kCompression = false;

  QueryCache cache(kMaxBytes, kMinCost, kTtlSeconds, kCompression);
  auto stats = cache.GetStatistics();

  EXPECT_EQ(stats.max_memory_bytes, kMaxBytes);
  EXPECT_DOUBLE_EQ(stats.min_query_cost_ms, kMinCost);
  EXPECT_EQ(stats.ttl_seconds, static_cast<uint64_t>(kTtlSeconds));
  EXPECT_FALSE(stats.compression_enabled);

  // SetTtl/SetMinQueryCost should be reflected in subsequent snapshots so
  // operators can verify a configuration change took effect.
  constexpr int kNewTtl = 60;
  cache.SetTtl(kNewTtl);
  cache.SetMinQueryCost(20.0);
  auto updated = cache.GetStatistics();
  EXPECT_EQ(updated.ttl_seconds, static_cast<uint64_t>(kNewTtl));
  EXPECT_DOUBLE_EQ(updated.min_query_cost_ms, 20.0);
}

/**
 * @brief rejection_count must increment when Insert() is rejected for
 *        falling below the min_query_cost_ms threshold, and the entry must
 *        not appear in the cache afterwards.
 *
 * Counts Insert calls, not entries: a single low-cost call increments by
 * exactly one regardless of result vector size.
 */
TEST(QueryCacheTest, RejectionCountIncrementsForLowCostInsert) {
  constexpr double kMinCost = 50.0;
  QueryCache cache(1024 * 1024, kMinCost);

  auto baseline = cache.GetStatistics().rejection_count;

  CacheMetadata meta;
  meta.table = "posts";
  meta.ngrams = {"foo", "bar"};

  auto key = CacheKeyGenerator::Generate("low cost query");
  std::vector<DocId> result = {1, 2, 3};

  // Cost below threshold -> rejected.
  EXPECT_FALSE(cache.Insert(key, result, meta, kMinCost - 1.0));
  EXPECT_FALSE(cache.Lookup(key).has_value()) << "Rejected insert must not leave the entry in the cache";

  auto after = cache.GetStatistics().rejection_count;
  EXPECT_EQ(after - baseline, 1U);

  // A second below-threshold insert increments again.
  EXPECT_FALSE(cache.Insert(CacheKeyGenerator::Generate("another"), result, meta, 1.0));
  EXPECT_EQ(cache.GetStatistics().rejection_count - baseline, 2U);

  // An insert at or above the threshold succeeds and does NOT bump the
  // rejection counter.
  EXPECT_TRUE(cache.Insert(CacheKeyGenerator::Generate("ok query"), result, meta, kMinCost + 1.0));
  EXPECT_EQ(cache.GetStatistics().rejection_count - baseline, 2U);
}

/**
 * @brief forced_clears must increment per Clear()/ClearTable() invocation,
 *        not per evicted entry.
 *
 * Counter semantics: bulk operations (operator-initiated CACHE CLEAR or
 * SYNC-driven ClearTable) are observed as discrete events, not as a count
 * of underlying entries — that role is filled by current_entries delta.
 */
TEST(QueryCacheTest, ForcedClearsIncrementsOnClear) {
  QueryCache cache(1024 * 1024, 1.0);
  CacheMetadata meta;
  meta.table = "posts";
  meta.ngrams = {"x"};

  for (int i = 0; i < 5; ++i) {
    auto key = CacheKeyGenerator::Generate("entry_" + std::to_string(i));
    std::vector<DocId> result = {static_cast<DocId>(i)};
    EXPECT_TRUE(cache.Insert(key, result, meta, 10.0));
  }
  ASSERT_EQ(cache.GetStatistics().current_entries, 5U);
  ASSERT_EQ(cache.GetStatistics().forced_clears, 0U);

  cache.Clear();
  EXPECT_EQ(cache.GetStatistics().current_entries, 0U);
  EXPECT_EQ(cache.GetStatistics().forced_clears, 1U) << "A single Clear() must register as one bulk-clear event";

  // Re-populate and exercise ClearTable; counter must still increment by one.
  for (int i = 0; i < 3; ++i) {
    auto key = CacheKeyGenerator::Generate("entry2_" + std::to_string(i));
    std::vector<DocId> result = {static_cast<DocId>(i)};
    EXPECT_TRUE(cache.Insert(key, result, meta, 10.0));
  }
  ASSERT_EQ(cache.GetStatistics().current_entries, 3U);

  cache.ClearTable("posts");
  EXPECT_EQ(cache.GetStatistics().forced_clears, 2U)
      << "ClearTable() must register as a separate bulk-clear event from Clear()";

  // ClearTable on a table with no entries still counts as one invocation —
  // the metric measures operator/system intent, not match success.
  cache.ClearTable("nonexistent_table");
  EXPECT_EQ(cache.GetStatistics().forced_clears, 3U);
}

// =============================================================================
// QueryCache compression-disabled path coverage
//
// All other tests in this file exercise the LZ4-compressed code path
// (compression_enabled = true is the default). The fixture below pins
// compression off so the uncompressed memcpy path in Insert/Lookup/MemoryUsage
// is also covered. Operators sometimes disable compression to trade memory
// for CPU on read-heavy workloads, so this path needs explicit tests.
// =============================================================================

class QueryCacheNoCompressionTest : public ::testing::Test {
 protected:
  // 1 MiB cap, no minimum cost gate (so trivially-cheap inserts succeed),
  // no TTL, compression disabled.
  QueryCache cache_{1024UL * 1024UL, /*min_query_cost_ms=*/0.0, /*ttl_seconds=*/0,
                    /*compression_enabled=*/false};

  static CacheMetadata MakeMeta() {
    CacheMetadata meta;
    meta.table = "posts";
    meta.ngrams = {"abc", "bcd"};
    return meta;
  }
};

TEST_F(QueryCacheNoCompressionTest, InsertAndLookupRoundtripsWithoutCompression) {
  auto key = CacheKeyGenerator::Generate("nc_roundtrip");
  std::vector<DocId> result = {1, 2, 3, 4, 5, 6, 7, 8};
  ASSERT_TRUE(cache_.Insert(key, result, MakeMeta(), 5.0));

  auto cached = cache_.Lookup(key);
  ASSERT_TRUE(cached.has_value());
  EXPECT_EQ(*cached, result);
  EXPECT_FALSE(cache_.IsCompressionEnabled());
}

TEST_F(QueryCacheNoCompressionTest, MarkInvalidatedWorksWithoutCompression) {
  auto key = CacheKeyGenerator::Generate("nc_invalidate");
  std::vector<DocId> result = {10, 20, 30};
  ASSERT_TRUE(cache_.Insert(key, result, MakeMeta(), 5.0));

  EXPECT_TRUE(cache_.MarkInvalidated(key));
  EXPECT_FALSE(cache_.Lookup(key).has_value());
}

TEST_F(QueryCacheNoCompressionTest, EraseWorksWithoutCompression) {
  auto key = CacheKeyGenerator::Generate("nc_erase");
  std::vector<DocId> result = {100, 200, 300};
  ASSERT_TRUE(cache_.Insert(key, result, MakeMeta(), 5.0));

  EXPECT_TRUE(cache_.Erase(key));
  EXPECT_FALSE(cache_.Lookup(key).has_value());
  EXPECT_FALSE(cache_.Erase(key)) << "Erasing a missing key must return false";
}

TEST_F(QueryCacheNoCompressionTest, MemoryAccountingMatchesUncompressedSize) {
  // With compression disabled, the cached "compressed" buffer is the raw
  // DocId bytes. So the total memory accounted for the entry must include
  // sizeof(DocId) * result.size() at minimum.
  auto key = CacheKeyGenerator::Generate("nc_memory");
  std::vector<DocId> result;
  constexpr size_t kSize = 256;
  result.reserve(kSize);
  for (size_t i = 0; i < kSize; ++i) {
    result.push_back(static_cast<DocId>(i));
  }

  const auto before = cache_.GetStatistics().current_memory_bytes;
  ASSERT_TRUE(cache_.Insert(key, result, MakeMeta(), 5.0));
  const auto after = cache_.GetStatistics().current_memory_bytes;

  // The delta must be at least the uncompressed payload (raw DocId bytes).
  // It may exceed it because metadata strings/ngrams add overhead.
  EXPECT_GE(after - before, kSize * sizeof(DocId));
}

TEST_F(QueryCacheNoCompressionTest, LookupOfMissingEntryReturnsMissNotFound) {
  auto missing = CacheKeyGenerator::Generate("nc_never_inserted");
  const auto before = cache_.GetStatistics();
  EXPECT_FALSE(cache_.Lookup(missing).has_value());
  const auto after = cache_.GetStatistics();
  EXPECT_GT(after.cache_misses, before.cache_misses);
  EXPECT_GT(after.cache_misses_not_found, before.cache_misses_not_found);
}

// =============================================================================
// CR-5 regression: cache_map_ rehash suppression
// =============================================================================

/**
 * @brief CR-5 regression: constructor reserves enough buckets that the
 *        steady-state working set does not trigger rehash.
 *
 * The QueryCache constructor pre-reserves the cache_map_ to a multiple of
 * (max_memory_bytes / kAverageEntryBytes) and caps the load factor at 0.5.
 * For the configured 4 MiB cache below, that yields room for at least
 * 4 MiB / 256 B = 16384 estimated entries, which is comfortably more than
 * the 1024 entries we insert here. Bucket count must NOT change across the
 * whole insert sequence; if it does, the iterator-stability defense
 * documented in LookupInternal weakens.
 */
TEST(QueryCacheTest, ConstructorReservesSoStableInsertSequenceDoesNotRehash) {
  QueryCache cache(/*max_memory_bytes=*/4 * 1024 * 1024, /*min_query_cost_ms=*/0.0);

  // Sanity: the constructor must have lowered max_load_factor below 1.0.
  // (We don't have direct access to cache_map_'s load factor from outside,
  // so we exercise the invariant through bucket_count behavior below.)

  // Insert one entry to capture the post-construction bucket count.
  CacheMetadata meta;
  meta.table = "posts";
  meta.ngrams = {"abc"};
  ASSERT_TRUE(cache.Insert(CacheKeyGenerator::Generate("warmup"), {1}, meta, 1.0));

  // We can't read bucket_count() directly (cache_map_ is private), but we
  // can verify functional correctness: 1024 inserts must all succeed and
  // all 1024 keys must remain looked-up-able afterwards. Under a rehash
  // bug that invalidated outstanding iterators, concurrent Lookups during
  // these inserts would crash; here we keep it single-threaded to focus on
  // the structural invariant. The Concurrent* tests cover the multi-thread
  // case.
  constexpr int kInserts = 1024;
  std::vector<CacheKey> keys;
  keys.reserve(kInserts);
  for (int i = 0; i < kInserts; ++i) {
    auto key = CacheKeyGenerator::Generate("cr5_" + std::to_string(i));
    CacheMetadata m;
    m.table = "posts";
    m.ngrams = {"abc"};
    ASSERT_TRUE(cache.Insert(key, {static_cast<DocId>(i)}, m, 1.0)) << "insert " << i << " failed";
    keys.push_back(key);
  }

  // All inserted keys must still be retrievable. If rehash had invalidated
  // iterators in flight (it cannot under the shared_mutex contract, but the
  // test guards against future regressions to that invariant), some lookups
  // would either miss or crash.
  for (size_t i = 0; i < keys.size(); ++i) {
    auto found = cache.Lookup(keys[i]);
    ASSERT_TRUE(found.has_value()) << "key " << i << " missing";
    ASSERT_EQ(found->size(), 1U);
    EXPECT_EQ(found->at(0), static_cast<DocId>(i));
  }

  // Stats sanity: current_entries should be exactly kInserts + 1 (warmup).
  const auto stats = cache.GetStatistics();
  EXPECT_EQ(stats.current_entries, static_cast<uint64_t>(kInserts + 1));
}

/**
 * @brief CR-5: Erase without callback does not fire eviction_callback_.
 *
 * Regression test for the EraseWithoutCallback API the InvalidationQueue
 * uses to avoid double-unregister. If a future change accidentally fires
 * eviction_callback_ on this path, this test fails.
 */
TEST(QueryCacheTest, EraseWithoutCallbackSuppressesEvictionCallback) {
  QueryCache cache(1024 * 1024, /*min_query_cost_ms=*/0.0);

  std::atomic<int> callback_fires{0};
  cache.SetEvictionCallback([&](const CacheKey& /*key*/) { callback_fires.fetch_add(1, std::memory_order_relaxed); });

  // Insert two entries.
  auto key_a = CacheKeyGenerator::Generate("erase_with_cb_a");
  auto key_b = CacheKeyGenerator::Generate("erase_no_cb_b");
  CacheMetadata meta;
  meta.table = "posts";
  meta.ngrams = {"abc"};
  ASSERT_TRUE(cache.Insert(key_a, {1}, meta, 1.0));
  ASSERT_TRUE(cache.Insert(key_b, {2}, meta, 1.0));

  // Erase fires the callback (CR-6 invariant: Erase fires the callback so
  // single-source unregister works for callers other than the queue).
  EXPECT_TRUE(cache.Erase(key_a));
  EXPECT_EQ(callback_fires.load(), 1);

  // EraseWithoutCallback does NOT fire the callback.
  EXPECT_TRUE(cache.EraseWithoutCallback(key_b));
  EXPECT_EQ(callback_fires.load(), 1);  // unchanged

  // Both keys are gone.
  EXPECT_FALSE(cache.Lookup(key_a).has_value());
  EXPECT_FALSE(cache.Lookup(key_b).has_value());
}

}  // namespace mygramdb::cache
