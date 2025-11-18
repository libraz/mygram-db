/**
 * @file invalidation_manager_test.cpp
 * @brief Unit tests for InvalidationManager - critical ngram-based invalidation logic
 */

#include "cache/invalidation_manager.h"

#include <gtest/gtest.h>

#include <thread>

#include "query/cache_key.h"
#include "cache/query_cache.h"

namespace mygramdb::cache {

/**
 * @brief Test basic registration and tracking
 */
TEST(InvalidationManagerTest, BasicRegistration) {
  QueryCache cache(1024 * 1024, 10.0);
  InvalidationManager mgr(&cache);

  // Create cache keys for different queries
  auto key1 = CacheKeyGenerator::Generate("query1");
  auto key2 = CacheKeyGenerator::Generate("query2");

  // Register cache entries with their ngrams
  CacheMetadata meta1;
  meta1.table = "posts";
  meta1.ngrams = {"hel", "ell", "llo"};  // "hello"
  mgr.RegisterCacheEntry(key1, meta1);

  CacheMetadata meta2;
  meta2.table = "posts";
  meta2.ngrams = {"wor", "orl", "rld"};  // "world"
  mgr.RegisterCacheEntry(key2, meta2);

  // Invalidate entries containing "hel"
  auto invalidated = mgr.InvalidateAffectedEntries("posts", "", "help", 3, 2);

  // Only key1 should be invalidated (contains "hel")
  EXPECT_EQ(1, invalidated.size());
  EXPECT_TRUE(invalidated.find(key1) != invalidated.end());
  EXPECT_FALSE(invalidated.find(key2) != invalidated.end());
}

/**
 * @brief Test precise invalidation - only affected queries should be invalidated
 */
TEST(InvalidationManagerTest, PreciseInvalidation) {
  QueryCache cache(1024 * 1024, 10.0);
  InvalidationManager mgr(&cache);

  // Query 1: "golang programming"
  auto key1 = CacheKeyGenerator::Generate("query1");
  CacheMetadata meta1;
  meta1.table = "posts";
  meta1.ngrams = {"gol", "ola", "lan", "ang", "pro", "rog", "ogr", "gra", "ram", "amm", "mmi", "min", "ing"};
  mgr.RegisterCacheEntry(key1, meta1);

  // Query 2: "python tutorial"
  auto key2 = CacheKeyGenerator::Generate("query2");
  CacheMetadata meta2;
  meta2.table = "posts";
  meta2.ngrams = {"pyt", "yth", "tho", "hon", "tut", "uto", "tor", "ori", "ria", "ial"};
  mgr.RegisterCacheEntry(key2, meta2);

  // Query 3: "golang tutorial"
  auto key3 = CacheKeyGenerator::Generate("query3");
  CacheMetadata meta3;
  meta3.table = "posts";
  meta3.ngrams = {"gol", "ola", "lan", "ang", "tut", "uto", "tor", "ori", "ria", "ial"};
  mgr.RegisterCacheEntry(key3, meta3);

  // INSERT new document with "golang tips"
  auto invalidated = mgr.InvalidateAffectedEntries("posts", "", "golang tips", 3, 2);

  // Should invalidate key1 and key3 (both contain "gol", "ola", "lan", "ang")
  // Should NOT invalidate key2 (no overlap with "golang")
  EXPECT_EQ(2, invalidated.size());
  EXPECT_TRUE(invalidated.find(key1) != invalidated.end());
  EXPECT_FALSE(invalidated.find(key2) != invalidated.end());
  EXPECT_TRUE(invalidated.find(key3) != invalidated.end());
}

/**
 * @brief Test UPDATE invalidation - both old and new text affect queries
 */
TEST(InvalidationManagerTest, UpdateInvalidation) {
  QueryCache cache(1024 * 1024, 10.0);
  InvalidationManager mgr(&cache);

  // Query for "rust"
  auto key1 = CacheKeyGenerator::Generate("query1");
  CacheMetadata meta1;
  meta1.table = "posts";
  meta1.ngrams = {"rus", "ust"};
  mgr.RegisterCacheEntry(key1, meta1);

  // Query for "golang"
  auto key2 = CacheKeyGenerator::Generate("query2");
  CacheMetadata meta2;
  meta2.table = "posts";
  meta2.ngrams = {"gol", "ola", "lan", "ang"};
  mgr.RegisterCacheEntry(key2, meta2);

  // UPDATE: change "rust programming" to "golang programming"
  // This affects both queries: key1 (old text) and key2 (new text)
  auto invalidated = mgr.InvalidateAffectedEntries("posts", "rust programming", "golang programming", 3, 2);

  // Both should be invalidated
  EXPECT_EQ(2, invalidated.size());
  EXPECT_TRUE(invalidated.find(key1) != invalidated.end());
  EXPECT_TRUE(invalidated.find(key2) != invalidated.end());
}

/**
 * @brief Test DELETE invalidation - only old text affects queries
 */
TEST(InvalidationManagerTest, DeleteInvalidation) {
  QueryCache cache(1024 * 1024, 10.0);
  InvalidationManager mgr(&cache);

  // Query for "docker"
  auto key1 = CacheKeyGenerator::Generate("query1");
  CacheMetadata meta1;
  meta1.table = "posts";
  meta1.ngrams = {"doc", "ock", "cke", "ker"};
  mgr.RegisterCacheEntry(key1, meta1);

  // Query for "kubernetes"
  auto key2 = CacheKeyGenerator::Generate("query2");
  CacheMetadata meta2;
  meta2.table = "posts";
  meta2.ngrams = {"kub", "ube", "ber", "ern", "rne", "net", "ete", "tes"};
  mgr.RegisterCacheEntry(key2, meta2);

  // DELETE document with "docker tutorial"
  auto invalidated = mgr.InvalidateAffectedEntries("posts", "docker tutorial", "", 3, 2);

  // Only key1 should be invalidated
  EXPECT_EQ(1, invalidated.size());
  EXPECT_TRUE(invalidated.find(key1) != invalidated.end());
  EXPECT_FALSE(invalidated.find(key2) != invalidated.end());
}

/**
 * @brief Test table isolation - changes to one table don't affect others
 */
TEST(InvalidationManagerTest, TableIsolation) {
  QueryCache cache(1024 * 1024, 10.0);
  InvalidationManager mgr(&cache);

  // Query for "posts" table
  auto key1 = CacheKeyGenerator::Generate("query1");
  CacheMetadata meta1;
  meta1.table = "posts";
  meta1.ngrams = {"gol", "ola", "lan", "ang"};
  mgr.RegisterCacheEntry(key1, meta1);

  // Query for "comments" table with same ngrams
  auto key2 = CacheKeyGenerator::Generate("query2");
  CacheMetadata meta2;
  meta2.table = "comments";
  meta2.ngrams = {"gol", "ola", "lan", "ang"};
  mgr.RegisterCacheEntry(key2, meta2);

  // INSERT into "posts" table
  auto invalidated = mgr.InvalidateAffectedEntries("posts", "", "golang", 3, 2);

  // Only posts table query should be invalidated
  EXPECT_EQ(1, invalidated.size());
  EXPECT_TRUE(invalidated.find(key1) != invalidated.end());
  EXPECT_FALSE(invalidated.find(key2) != invalidated.end());
}

/**
 * @brief Test no false positives - queries with no overlap should not be invalidated
 */
TEST(InvalidationManagerTest, NoFalsePositives) {
  QueryCache cache(1024 * 1024, 10.0);
  InvalidationManager mgr(&cache);

  // Query for "javascript"
  auto key1 = CacheKeyGenerator::Generate("query1");
  CacheMetadata meta1;
  meta1.table = "posts";
  meta1.ngrams = {"jav", "ava", "vas", "asc", "scr", "cri", "rip", "ipt"};
  mgr.RegisterCacheEntry(key1, meta1);

  // Query for "typescript"
  auto key2 = CacheKeyGenerator::Generate("query2");
  CacheMetadata meta2;
  meta2.table = "posts";
  meta2.ngrams = {"typ", "ype", "pes", "esc", "scr", "cri", "rip", "ipt"};
  mgr.RegisterCacheEntry(key2, meta2);

  // INSERT "golang" - completely different ngrams
  auto invalidated = mgr.InvalidateAffectedEntries("posts", "", "golang tutorial", 3, 2);

  // Neither should be invalidated
  EXPECT_EQ(0, invalidated.size());
}

/**
 * @brief Test partial overlap - only exact ngram matches invalidate
 */
TEST(InvalidationManagerTest, PartialOverlap) {
  QueryCache cache(1024 * 1024, 10.0);
  InvalidationManager mgr(&cache);

  // Query for "test"
  auto key1 = CacheKeyGenerator::Generate("query1");
  CacheMetadata meta1;
  meta1.table = "posts";
  meta1.ngrams = {"tes", "est"};
  mgr.RegisterCacheEntry(key1, meta1);

  // Query for "testing"
  auto key2 = CacheKeyGenerator::Generate("query2");
  CacheMetadata meta2;
  meta2.table = "posts";
  meta2.ngrams = {"tes", "est", "sti", "tin", "ing"};
  mgr.RegisterCacheEntry(key2, meta2);

  // INSERT "tes" - only 1 ngram overlap with "test", 1 ngram overlap with "testing"
  auto invalidated = mgr.InvalidateAffectedEntries("posts", "", "tes", 3, 2);

  // Both should be invalidated (both contain "tes")
  EXPECT_EQ(2, invalidated.size());
  EXPECT_TRUE(invalidated.find(key1) != invalidated.end());
  EXPECT_TRUE(invalidated.find(key2) != invalidated.end());
}

/**
 * @brief Test unregister - ensure entries are properly removed
 */
TEST(InvalidationManagerTest, Unregister) {
  QueryCache cache(1024 * 1024, 10.0);
  InvalidationManager mgr(&cache);

  auto key1 = CacheKeyGenerator::Generate("query1");
  CacheMetadata meta1;
  meta1.table = "posts";
  meta1.ngrams = {"gol", "ola", "lan", "ang"};
  mgr.RegisterCacheEntry(key1, meta1);

  // Unregister the entry
  mgr.UnregisterCacheEntry(key1);

  // Now invalidation should not affect the unregistered entry
  auto invalidated = mgr.InvalidateAffectedEntries("posts", "", "golang", 3, 2);

  EXPECT_EQ(0, invalidated.size());
}

/**
 * @brief Test kanji/CJK ngram handling (different ngram size)
 */
TEST(InvalidationManagerTest, KanjiNgrams) {
  QueryCache cache(1024 * 1024, 10.0);
  InvalidationManager mgr(&cache);

  // Query with Japanese text (using 2-gram for CJK)
  auto key1 = CacheKeyGenerator::Generate("query1");
  CacheMetadata meta1;
  meta1.table = "posts";
  // Simulating "日本語" with 2-grams: "日本", "本語"
  meta1.ngrams = {"日本", "本語"};
  mgr.RegisterCacheEntry(key1, meta1);

  // Query with different Japanese text
  auto key2 = CacheKeyGenerator::Generate("query2");
  CacheMetadata meta2;
  meta2.table = "posts";
  // "中国語" with 2-grams: "中国", "国語"
  meta2.ngrams = {"中国", "国語"};
  mgr.RegisterCacheEntry(key2, meta2);

  // INSERT "本語勉強" (contains "本語")
  // Using kanji_ngram_size=2 for CJK characters
  auto invalidated = mgr.InvalidateAffectedEntries("posts", "", "本語勉強", 3, 2);

  // Only key1 should be invalidated (contains "本語")
  EXPECT_EQ(1, invalidated.size());
  EXPECT_TRUE(invalidated.find(key1) != invalidated.end());
  EXPECT_FALSE(invalidated.find(key2) != invalidated.end());
}

/**
 * @brief Test multiple queries with same ngrams
 */
TEST(InvalidationManagerTest, MultipleQueriesSameNgrams) {
  QueryCache cache(1024 * 1024, 10.0);
  InvalidationManager mgr(&cache);

  // Both queries search for "golang"
  auto key1 = CacheKeyGenerator::Generate("query1");
  auto key2 = CacheKeyGenerator::Generate("query2");

  CacheMetadata meta;
  meta.table = "posts";
  meta.ngrams = {"gol", "ola", "lan", "ang"};

  mgr.RegisterCacheEntry(key1, meta);
  mgr.RegisterCacheEntry(key2, meta);

  // INSERT new golang post
  auto invalidated = mgr.InvalidateAffectedEntries("posts", "", "golang tips", 3, 2);

  // Both queries should be invalidated
  EXPECT_EQ(2, invalidated.size());
  EXPECT_TRUE(invalidated.find(key1) != invalidated.end());
  EXPECT_TRUE(invalidated.find(key2) != invalidated.end());
}

/**
 * @brief Test for deadlock risk fix in ClearTable()
 *
 * Verifies that ClearTable() uses internal unlocked helper to avoid deadlock
 * when calling UnregisterCacheEntry() while holding the mutex.
 */
TEST(InvalidationManagerTest, ClearTableNoDeadlock) {
  QueryCache cache(1024 * 1024, 10.0);
  InvalidationManager mgr(&cache);

  const int num_entries = 100;
  std::vector<CacheKey> keys;

  // Register many cache entries for the same table
  for (int i = 0; i < num_entries; ++i) {
    auto key = CacheKeyGenerator::Generate("query" + std::to_string(i));
    keys.push_back(key);

    CacheMetadata meta;
    meta.table = "posts";
    meta.ngrams = {"ngram" + std::to_string(i)};

    mgr.RegisterCacheEntry(key, meta);
  }

  // Verify entries are registered
  EXPECT_EQ(static_cast<size_t>(num_entries), mgr.GetTrackedEntryCount());

  // ClearTable should complete without deadlock
  mgr.ClearTable("posts");

  // All entries should be removed
  EXPECT_EQ(0, mgr.GetTrackedEntryCount());
  EXPECT_EQ(0, mgr.GetTrackedNgramCount("posts"));
}

/**
 * @brief Test concurrent ClearTable() calls don't cause deadlock
 */
TEST(InvalidationManagerTest, ConcurrentClearTableNoDeadlock) {
  QueryCache cache(10 * 1024 * 1024, 10.0);
  InvalidationManager mgr(&cache);

  const int num_threads = 5;
  const int entries_per_table = 50;

  // Register entries for multiple tables
  for (int t = 0; t < num_threads; ++t) {
    std::string table = "table" + std::to_string(t);
    for (int i = 0; i < entries_per_table; ++i) {
      auto key = CacheKeyGenerator::Generate(table + "_query" + std::to_string(i));
      CacheMetadata meta;
      meta.table = table;
      meta.ngrams = {"ng" + std::to_string(i)};
      mgr.RegisterCacheEntry(key, meta);
    }
  }

  // Concurrent ClearTable calls for different tables
  std::vector<std::thread> threads;
  std::atomic<int> successful_clears{0};

  for (int t = 0; t < num_threads; ++t) {
    threads.emplace_back([&, t]() {
      std::string table = "table" + std::to_string(t);
      try {
        mgr.ClearTable(table);
        successful_clears++;
      } catch (...) {
        // Should not throw
      }
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }

  // All clears should succeed
  EXPECT_EQ(num_threads, successful_clears.load());

  // All entries should be removed
  EXPECT_EQ(0, mgr.GetTrackedEntryCount());
}

}  // namespace mygramdb::cache
