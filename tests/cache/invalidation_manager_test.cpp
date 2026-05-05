/**
 * @file invalidation_manager_test.cpp
 * @brief Unit tests for InvalidationManager - critical ngram-based invalidation logic
 */

#include "cache/invalidation_manager.h"

#include <gtest/gtest.h>

#include <thread>

#include "cache/query_cache.h"
#include "query/cache_key.h"
#include "query/query_parser.h"

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

// =============================================================================
// Bug #17: InvalidationManager metadata leak when re-registering cache entry
// =============================================================================

/**
 * @brief Test that re-registering a cache entry cleans up stale reverse index entries
 *
 * Bug #17: When a cache entry is re-registered with different ngrams, the old ngrams
 * in the reverse index are not cleaned up, causing a memory leak.
 */
TEST(InvalidationManagerTest, Bug17_ReRegisterCacheEntryCleansUpStaleNgrams) {
  QueryCache cache(1024 * 1024, 10.0);
  InvalidationManager mgr(&cache);

  auto key = CacheKeyGenerator::Generate("query1");

  // Register with ngrams {"aaa", "bbb"}
  CacheMetadata meta1;
  meta1.table = "posts";
  meta1.ngrams = {"aaa", "bbb"};
  mgr.RegisterCacheEntry(key, meta1);

  EXPECT_EQ(1, mgr.GetTrackedEntryCount());
  EXPECT_EQ(2, mgr.GetTrackedNgramCount("posts"));  // aaa, bbb

  // Re-register same key with different ngrams {"bbb", "ccc"}
  // (bbb is shared, aaa is removed, ccc is added)
  CacheMetadata meta2;
  meta2.table = "posts";
  meta2.ngrams = {"bbb", "ccc"};
  mgr.RegisterCacheEntry(key, meta2);

  // Entry count should still be 1
  EXPECT_EQ(1, mgr.GetTrackedEntryCount());

  // Bug #17: Before fix, ngram count would be 3 (aaa still in reverse index)
  // After fix: ngram count should be 2 (bbb, ccc)
  EXPECT_EQ(2, mgr.GetTrackedNgramCount("posts")) << "Bug #17: Stale ngram 'aaa' should be removed from reverse index";

  // Verify invalidation works correctly with new ngrams
  auto invalidated_aaa = mgr.InvalidateAffectedEntries("posts", "", "aaa", 3, 2);
  EXPECT_EQ(0, invalidated_aaa.size()) << "Should not invalidate on old ngram 'aaa'";

  auto invalidated_ccc = mgr.InvalidateAffectedEntries("posts", "", "ccc", 3, 2);
  EXPECT_EQ(1, invalidated_ccc.size()) << "Should invalidate on new ngram 'ccc'";
}

/**
 * @brief Test re-registration with completely different ngrams
 */
TEST(InvalidationManagerTest, Bug17_ReRegisterCompletelyDifferentNgrams) {
  QueryCache cache(1024 * 1024, 10.0);
  InvalidationManager mgr(&cache);

  auto key = CacheKeyGenerator::Generate("query1");

  // Register with ngrams {"xxx", "yyy", "zzz"}
  CacheMetadata meta1;
  meta1.table = "posts";
  meta1.ngrams = {"xxx", "yyy", "zzz"};
  mgr.RegisterCacheEntry(key, meta1);

  EXPECT_EQ(3, mgr.GetTrackedNgramCount("posts"));

  // Re-register with completely different ngrams {"aaa", "bbb"}
  CacheMetadata meta2;
  meta2.table = "posts";
  meta2.ngrams = {"aaa", "bbb"};
  mgr.RegisterCacheEntry(key, meta2);

  // Bug #17: Before fix, ngram count would be 5
  // After fix: ngram count should be 2
  EXPECT_EQ(2, mgr.GetTrackedNgramCount("posts")) << "Bug #17: All old ngrams should be cleaned up";

  // Verify old ngrams don't cause invalidation
  auto invalidated_xxx = mgr.InvalidateAffectedEntries("posts", "", "xxx", 3, 2);
  EXPECT_EQ(0, invalidated_xxx.size()) << "Old ngram 'xxx' should not cause invalidation";
}

/**
 * @brief Test re-registration preserves table cleanup
 */
TEST(InvalidationManagerTest, Bug17_ReRegisterDifferentTable) {
  QueryCache cache(1024 * 1024, 10.0);
  InvalidationManager mgr(&cache);

  auto key = CacheKeyGenerator::Generate("query1");

  // Register in table "table1"
  CacheMetadata meta1;
  meta1.table = "table1";
  meta1.ngrams = {"aaa", "bbb"};
  mgr.RegisterCacheEntry(key, meta1);

  EXPECT_EQ(2, mgr.GetTrackedNgramCount("table1"));
  EXPECT_EQ(0, mgr.GetTrackedNgramCount("table2"));

  // Re-register in table "table2"
  CacheMetadata meta2;
  meta2.table = "table2";
  meta2.ngrams = {"ccc", "ddd"};
  mgr.RegisterCacheEntry(key, meta2);

  // table1 should be empty, table2 should have the new ngrams
  EXPECT_EQ(0, mgr.GetTrackedNgramCount("table1")) << "Bug #17: Old table's ngrams should be cleaned up";
  EXPECT_EQ(2, mgr.GetTrackedNgramCount("table2"));
}

// =============================================================================
// Bug #18: Cache invalidation uses symmetric difference
// =============================================================================
// The concern is that symmetric difference might miss invalidations where
// unchanged ngrams are involved. However, analysis shows symmetric difference
// is correct because:
// - Unchanged ngrams mean the document still matches those queries
// - Only changed ngrams affect query result validity
// =============================================================================

/**
 * @test Bug #18: Verify symmetric difference handles partial updates correctly
 *
 * When text is updated and some ngrams remain unchanged, caches for those
 * unchanged ngrams should NOT be invalidated (the document still matches).
 */
TEST(InvalidationManagerTest, Bug18_PartialUpdateUnchangedNgramsNotInvalidated) {
  QueryCache cache(1024 * 1024, 10.0);
  InvalidationManager mgr(&cache);

  // Query for "hello" (will be in both old and new text)
  auto key1 = CacheKeyGenerator::Generate("query_hello");
  CacheMetadata meta1;
  meta1.table = "posts";
  meta1.ngrams = {"hel", "ell", "llo"};  // ngrams for "hello"
  mgr.RegisterCacheEntry(key1, meta1);

  // Query for "world" (only in old text)
  auto key2 = CacheKeyGenerator::Generate("query_world");
  CacheMetadata meta2;
  meta2.table = "posts";
  meta2.ngrams = {"wor", "orl", "rld"};  // ngrams for "world"
  mgr.RegisterCacheEntry(key2, meta2);

  // Query for "earth" (only in new text)
  auto key3 = CacheKeyGenerator::Generate("query_earth");
  CacheMetadata meta3;
  meta3.table = "posts";
  meta3.ngrams = {"ear", "art", "rth"};  // ngrams for "earth"
  mgr.RegisterCacheEntry(key3, meta3);

  // UPDATE: "hello world" -> "hello earth"
  // "hello" ngrams unchanged, "world" removed, "earth" added
  auto invalidated = mgr.InvalidateAffectedEntries("posts", "hello world", "hello earth", 3, 2);

  // key1 (hello) should NOT be invalidated - document still matches
  EXPECT_EQ(invalidated.find(key1), invalidated.end()) << "Bug #18: Unchanged ngrams should not cause invalidation";

  // key2 (world) SHOULD be invalidated - removed from document
  EXPECT_NE(invalidated.find(key2), invalidated.end()) << "Removed ngrams should cause invalidation";

  // key3 (earth) SHOULD be invalidated - new match
  EXPECT_NE(invalidated.find(key3), invalidated.end()) << "Added ngrams should cause invalidation";

  // Exactly 2 invalidations (world and earth, not hello)
  EXPECT_EQ(2, invalidated.size()) << "Should invalidate exactly 2 caches (changed ngrams only)";
}

/**
 * @test Bug #18: Verify no invalidation when text is identical
 *
 * When old and new text are the same, no caches should be invalidated.
 */
TEST(InvalidationManagerTest, Bug18_IdenticalTextNoInvalidation) {
  QueryCache cache(1024 * 1024, 10.0);
  InvalidationManager mgr(&cache);

  auto key = CacheKeyGenerator::Generate("query1");
  CacheMetadata meta;
  meta.table = "posts";
  meta.ngrams = {"hel", "ell", "llo"};
  mgr.RegisterCacheEntry(key, meta);

  // UPDATE with identical text (no actual change)
  auto invalidated = mgr.InvalidateAffectedEntries("posts", "hello", "hello", 3, 2);

  // No invalidation should occur
  EXPECT_EQ(0, invalidated.size()) << "Bug #18: Identical text should not cause any invalidation";
}

/**
 * @test Bug #18: Verify complete text replacement invalidates correctly
 */
TEST(InvalidationManagerTest, Bug18_CompleteTextReplacementInvalidatesAll) {
  QueryCache cache(1024 * 1024, 10.0);
  InvalidationManager mgr(&cache);

  // Query for "rust"
  auto key1 = CacheKeyGenerator::Generate("query_rust");
  CacheMetadata meta1;
  meta1.table = "posts";
  meta1.ngrams = {"rus", "ust"};
  mgr.RegisterCacheEntry(key1, meta1);

  // Query for "golang"
  auto key2 = CacheKeyGenerator::Generate("query_golang");
  CacheMetadata meta2;
  meta2.table = "posts";
  meta2.ngrams = {"gol", "ola", "lan", "ang"};
  mgr.RegisterCacheEntry(key2, meta2);

  // Complete replacement: "rust" -> "golang" (no overlap)
  auto invalidated = mgr.InvalidateAffectedEntries("posts", "rust", "golang", 3, 2);

  // Both should be invalidated (old text removed, new text added)
  EXPECT_EQ(2, invalidated.size());
  EXPECT_NE(invalidated.find(key1), invalidated.end());
  EXPECT_NE(invalidated.find(key2), invalidated.end());
}

// =============================================================================
// InvalidationMetadata minimal storage tests
// =============================================================================

/**
 * @brief Test that InvalidationManager stores only table + ngrams (minimal metadata)
 */
TEST(InvalidationManagerTest, MetadataStorageMinimal) {
  QueryCache cache(1024 * 1024, 10.0);
  InvalidationManager mgr(&cache);

  auto key = CacheKeyGenerator::Generate("query1");

  CacheMetadata meta;
  meta.table = "posts";
  meta.ngrams = {"hel", "ell", "llo"};
  meta.filters = {{"col", query::FilterOp::EQ, "val"}};
  meta.access_count = 42;  // Should NOT be stored in InvalidationManager
  mgr.RegisterCacheEntry(key, meta);

  EXPECT_EQ(1, mgr.GetTrackedEntryCount());
  EXPECT_EQ(3, mgr.GetTrackedNgramCount("posts"));

  // Verify invalidation still works correctly
  auto invalidated = mgr.InvalidateAffectedEntries("posts", "", "hello", 3, 2);
  EXPECT_GE(invalidated.size(), 1);
}

/**
 * @brief Test register/unregister memory consistency
 */
TEST(InvalidationManagerTest, RegisterUnregisterMemoryConsistency) {
  QueryCache cache(1024 * 1024, 10.0);
  InvalidationManager mgr(&cache);

  size_t base_mem = mgr.MemoryUsage();

  auto key = CacheKeyGenerator::Generate("query1");
  CacheMetadata meta;
  meta.table = "posts";
  meta.ngrams = {"aaa", "bbb", "ccc"};
  mgr.RegisterCacheEntry(key, meta);

  size_t after_register = mgr.MemoryUsage();
  EXPECT_GT(after_register, base_mem);

  mgr.UnregisterCacheEntry(key);

  size_t after_unregister = mgr.MemoryUsage();
  // Memory may not return exactly to base due to bucket retention,
  // but tracked entry count should be zero
  EXPECT_EQ(0, mgr.GetTrackedEntryCount());
  EXPECT_LE(after_unregister, after_register);
}

// =============================================================================
// MemoryUsage tests
// =============================================================================

/**
 * @brief Test memory usage grows with entries
 */
TEST(InvalidationManagerTest, MemoryUsageGrowsWithEntries) {
  QueryCache cache(1024 * 1024, 10.0);
  InvalidationManager mgr(&cache);

  size_t mem0 = mgr.MemoryUsage();

  for (int i = 0; i < 10; ++i) {
    auto key = CacheKeyGenerator::Generate("q" + std::to_string(i));
    CacheMetadata meta;
    meta.table = "t";
    meta.ngrams = {"ng" + std::to_string(i)};
    mgr.RegisterCacheEntry(key, meta);
  }

  size_t mem10 = mgr.MemoryUsage();
  EXPECT_GT(mem10, mem0);
}

/**
 * @brief Test memory usage decreases on unregister
 */
TEST(InvalidationManagerTest, MemoryUsageDecreasesOnUnregister) {
  QueryCache cache(1024 * 1024, 10.0);
  InvalidationManager mgr(&cache);

  std::vector<CacheKey> keys;
  for (int i = 0; i < 20; ++i) {
    auto key = CacheKeyGenerator::Generate("q" + std::to_string(i));
    keys.push_back(key);
    CacheMetadata meta;
    meta.table = "t";
    meta.ngrams = {"ng" + std::to_string(i), "xg" + std::to_string(i)};
    mgr.RegisterCacheEntry(key, meta);
  }

  size_t mem_full = mgr.MemoryUsage();

  // Remove half
  for (int i = 0; i < 10; ++i) {
    mgr.UnregisterCacheEntry(keys[static_cast<size_t>(i)]);
  }

  size_t mem_half = mgr.MemoryUsage();
  EXPECT_LT(mem_half, mem_full);
}

/**
 * @brief Test entries with more ngrams use more memory
 */
TEST(InvalidationManagerTest, MemoryUsageReflectsNgramCount) {
  QueryCache cache(1024 * 1024, 10.0);
  InvalidationManager mgr1(&cache);
  InvalidationManager mgr2(&cache);

  // mgr1: few ngrams
  auto key1 = CacheKeyGenerator::Generate("q1");
  CacheMetadata meta1;
  meta1.table = "t";
  meta1.ngrams = {"a", "b"};
  mgr1.RegisterCacheEntry(key1, meta1);

  // mgr2: many ngrams
  auto key2 = CacheKeyGenerator::Generate("q2");
  CacheMetadata meta2;
  meta2.table = "t";
  for (int i = 0; i < 50; ++i) {
    meta2.ngrams.push_back("ngram_" + std::to_string(i));
  }
  mgr2.RegisterCacheEntry(key2, meta2);

  EXPECT_GT(mgr2.MemoryUsage(), mgr1.MemoryUsage());
}

// =============================================================================
// IsCJK consistency - Hiragana/Katakana invalidation
// =============================================================================

/**
 * @brief Test that Hiragana text invalidation works correctly
 *
 * Verifies that N-gram generation for cache invalidation uses the same
 * CJK classification as indexing (Hiragana = non-CJK = ascii_ngram_size).
 */
TEST(InvalidationManagerTest, HiraganaTextInvalidation) {
  QueryCache cache(1024 * 1024, 0.0);  // min_query_cost=0 to allow all inserts
  InvalidationManager mgr(&cache);

  // Register a cache entry with bigram ngrams for hiragana text "あいうえお"
  // Using ascii_ngram_size=2, kanji_ngram_size=1 (hiragana uses ascii_ngram_size)
  auto key1 = CacheKeyGenerator::Generate("hiragana_query");
  CacheMetadata meta1;
  meta1.table = "posts";
  // Hiragana bigrams: "あい", "いう", "うえ", "えお"
  meta1.ngrams = {"あい", "いう", "うえ", "えお"};
  std::sort(meta1.ngrams.begin(), meta1.ngrams.end());
  mgr.RegisterCacheEntry(key1, meta1);

  // Invalidate with hiragana text - ExtractNgrams should generate same bigrams
  auto invalidated = mgr.InvalidateAffectedEntries("posts", "", "あいうえお", 2, 1);

  // Cache entry should be invalidated because N-grams match
  EXPECT_GE(invalidated.size(), 1) << "Hiragana text should invalidate matching cache entries";
  EXPECT_TRUE(invalidated.find(key1) != invalidated.end());
}

/**
 * @brief Test that Katakana text invalidation works correctly
 */
TEST(InvalidationManagerTest, KatakanaTextInvalidation) {
  QueryCache cache(1024 * 1024, 0.0);
  InvalidationManager mgr(&cache);

  auto key1 = CacheKeyGenerator::Generate("katakana_query");
  CacheMetadata meta1;
  meta1.table = "posts";
  // Katakana bigrams: "アイ", "イウ", "ウエ", "エオ"
  meta1.ngrams = {"アイ", "イウ", "ウエ", "エオ"};
  std::sort(meta1.ngrams.begin(), meta1.ngrams.end());
  mgr.RegisterCacheEntry(key1, meta1);

  auto invalidated = mgr.InvalidateAffectedEntries("posts", "", "アイウエオ", 2, 1);

  EXPECT_GE(invalidated.size(), 1) << "Katakana text should invalidate matching cache entries";
  EXPECT_TRUE(invalidated.find(key1) != invalidated.end());
}

/**
 * @brief Test mixed CJK (Kanji) + Hiragana text invalidation
 *
 * "東京の空" has:
 * - Kanji: 東, 京, 空 (kanji_ngram_size=1 -> unigrams "東", "京", "空")
 * - Hiragana: の (ascii_ngram_size=2 -> but single char, no bigram possible alone)
 * - Cross-boundary ngrams may be generated
 */
TEST(InvalidationManagerTest, MixedCJKHiraganaInvalidation) {
  QueryCache cache(1024 * 1024, 0.0);
  InvalidationManager mgr(&cache);

  auto key1 = CacheKeyGenerator::Generate("mixed_query");
  CacheMetadata meta1;
  meta1.table = "posts";
  // Register with unigrams for kanji characters
  meta1.ngrams = {"東", "京", "空"};
  std::sort(meta1.ngrams.begin(), meta1.ngrams.end());
  mgr.RegisterCacheEntry(key1, meta1);

  // Invalidate with the mixed text
  auto invalidated = mgr.InvalidateAffectedEntries("posts", "", "東京の空", 2, 1);

  // Should invalidate because kanji unigrams match
  EXPECT_GE(invalidated.size(), 1) << "Mixed CJK+Hiragana should invalidate via kanji unigrams";
}

// =============================================================================
// C2: N-gram config change invalidation consistency
// =============================================================================

/**
 * @brief Test that ngram settings are stored in InvalidationMetadata
 *
 * When cache entries are created with specific ngram settings, those settings
 * should be preserved so that invalidation can use them if the config changes.
 */
TEST(InvalidationManagerTest, NgramSettingsStoredInMetadata) {
  QueryCache cache(1024 * 1024, 0.0);
  InvalidationManager mgr(&cache);

  auto key = CacheKeyGenerator::Generate("query1");

  CacheMetadata meta;
  meta.table = "posts";
  meta.ngrams = {"he", "el", "ll", "lo"};  // bigrams from "hello"
  meta.ngram_size = 2;
  meta.kanji_ngram_size = 1;
  meta.cross_boundary_ngrams = false;
  mgr.RegisterCacheEntry(key, meta);

  EXPECT_EQ(1, mgr.GetTrackedEntryCount());
  EXPECT_EQ(4, mgr.GetTrackedNgramCount("posts"));
}

/**
 * @brief Test invalidation with changed ngram_size finds entries created with old settings
 *
 * Scenario: Cache entry was created with ngram_size=2 (bigrams).
 * Config is then changed to ngram_size=3 (trigrams).
 * When invalidation occurs with the new config, it should still find and
 * invalidate entries whose reverse index was built with the old bigram settings.
 */
TEST(InvalidationManagerTest, C2_ConfigChangeInvalidatesOldEntries) {
  QueryCache cache(1024 * 1024, 0.0);
  InvalidationManager mgr(&cache);

  // Register cache entry created with ngram_size=2 (bigrams)
  auto key1 = CacheKeyGenerator::Generate("query_bigram");
  CacheMetadata meta1;
  meta1.table = "posts";
  // Bigrams for "hello": "he", "el", "ll", "lo"
  meta1.ngrams = {"el", "he", "ll", "lo"};
  meta1.ngram_size = 2;
  meta1.kanji_ngram_size = 1;
  meta1.cross_boundary_ngrams = true;
  mgr.RegisterCacheEntry(key1, meta1);

  // Now config changes to ngram_size=3. Invalidation is called with new settings.
  // INSERT "hello world" - trigrams would be: "hel", "ell", "llo", "wor", "orl", "rld"
  // With bigrams (old settings): "he", "el", "ll", "lo", "wo", "or", "rl", "ld"
  // The reverse index has bigrams, so trigram-based lookup would miss them.
  // With the fix, historical settings (ngram_size=2) are also checked.
  auto invalidated = mgr.InvalidateAffectedEntries("posts", "", "hello world", 3, 1, true);

  // key1 should be invalidated because historical bigram generation finds "he", "el", etc.
  EXPECT_EQ(1, invalidated.size());
  EXPECT_TRUE(invalidated.find(key1) != invalidated.end())
      << "C2: Entry created with old ngram_size should be invalidated after config change";
}

/**
 * @brief Test invalidation with multiple historical ngram settings
 *
 * Entries created with different ngram settings should all be properly
 * invalidated when the config has changed.
 */
TEST(InvalidationManagerTest, C2_MultipleHistoricalSettings) {
  QueryCache cache(1024 * 1024, 0.0);
  InvalidationManager mgr(&cache);

  // Entry 1: created with ngram_size=2
  auto key1 = CacheKeyGenerator::Generate("query_bigram");
  CacheMetadata meta1;
  meta1.table = "posts";
  meta1.ngrams = {"el", "he", "ll", "lo"};  // bigrams for "hello"
  meta1.ngram_size = 2;
  meta1.kanji_ngram_size = 1;
  meta1.cross_boundary_ngrams = true;
  mgr.RegisterCacheEntry(key1, meta1);

  // Entry 2: created with ngram_size=3 (maybe after a first config change)
  auto key2 = CacheKeyGenerator::Generate("query_trigram");
  CacheMetadata meta2;
  meta2.table = "posts";
  meta2.ngrams = {"ell", "hel", "llo"};  // trigrams for "hello"
  meta2.ngram_size = 3;
  meta2.kanji_ngram_size = 1;
  meta2.cross_boundary_ngrams = true;
  mgr.RegisterCacheEntry(key2, meta2);

  // Now config changes to ngram_size=4. Invalidation with new settings.
  // 4-grams for "hello": "hell", "ello"
  // Neither matches bigrams nor trigrams directly. But with historical settings,
  // bigrams and trigrams are also generated and matched.
  auto invalidated = mgr.InvalidateAffectedEntries("posts", "", "hello", 4, 1, true);

  EXPECT_EQ(2, invalidated.size());
  EXPECT_TRUE(invalidated.find(key1) != invalidated.end()) << "C2: Bigram entry should be invalidated";
  EXPECT_TRUE(invalidated.find(key2) != invalidated.end()) << "C2: Trigram entry should be invalidated";
}

/**
 * @brief Test that entries with current settings still work normally
 *
 * When all entries use the same ngram settings as the current config,
 * invalidation should work exactly as before (no regression).
 */
TEST(InvalidationManagerTest, C2_CurrentSettingsNoRegression) {
  QueryCache cache(1024 * 1024, 0.0);
  InvalidationManager mgr(&cache);

  // Entry with ngram_size=3 matching current config
  auto key1 = CacheKeyGenerator::Generate("query1");
  CacheMetadata meta1;
  meta1.table = "posts";
  meta1.ngrams = {"gol", "ola", "lan", "ang"};
  meta1.ngram_size = 3;
  meta1.kanji_ngram_size = 2;
  meta1.cross_boundary_ngrams = true;
  mgr.RegisterCacheEntry(key1, meta1);

  // Invalidation with same settings
  auto invalidated = mgr.InvalidateAffectedEntries("posts", "", "golang tips", 3, 2, true);

  EXPECT_EQ(1, invalidated.size());
  EXPECT_TRUE(invalidated.find(key1) != invalidated.end());
}

/**
 * @brief Test that entries with zero ngram_size (legacy) are handled gracefully
 *
 * Entries created before this fix will have ngram_size=0. They should not
 * trigger historical settings lookup (treated as "unknown/current").
 */
TEST(InvalidationManagerTest, C2_LegacyZeroNgramSizeHandled) {
  QueryCache cache(1024 * 1024, 0.0);
  InvalidationManager mgr(&cache);

  // Legacy entry with ngram_size=0 (not set)
  auto key1 = CacheKeyGenerator::Generate("query_legacy");
  CacheMetadata meta1;
  meta1.table = "posts";
  meta1.ngrams = {"hel", "ell", "llo"};
  // ngram_size defaults to 0 (legacy behavior)
  mgr.RegisterCacheEntry(key1, meta1);

  // Invalidation with current settings should work via direct ngram match
  auto invalidated = mgr.InvalidateAffectedEntries("posts", "", "hello", 3, 2, true);

  EXPECT_EQ(1, invalidated.size());
  EXPECT_TRUE(invalidated.find(key1) != invalidated.end());
}

// =============================================================================
// Bug 3: Filter-only changes don't invalidate cache
// =============================================================================

/**
 * @brief Test that filter change invalidates entries with filters when text is unchanged
 *
 * When only filter columns change (text unchanged), cache entries with filter
 * conditions should be invalidated because their results may differ.
 */
TEST(InvalidationManagerTest, FilterChangeInvalidatesFilteredEntries) {
  QueryCache cache(1024 * 1024, 0.0);
  InvalidationManager mgr(&cache);

  // Entry with filters (simulated by setting has_filters via CacheMetadata)
  auto key1 = CacheKeyGenerator::Generate("query_with_filter");
  CacheMetadata meta1;
  meta1.table = "posts";
  meta1.ngrams = {"hel", "ell", "llo"};
  meta1.filters = {{"status", query::FilterOp::EQ, "1"}};  // Has filters
  mgr.RegisterCacheEntry(key1, meta1);

  // Entry without filters
  auto key2 = CacheKeyGenerator::Generate("query_no_filter");
  CacheMetadata meta2;
  meta2.table = "posts";
  meta2.ngrams = {"hel", "ell", "llo"};
  // No filters
  mgr.RegisterCacheEntry(key2, meta2);

  // Filter-only change: same text, filter_columns_changed=true
  auto invalidated = mgr.InvalidateAffectedEntries("posts", "hello", "hello", 3, 2, true, true);

  // Only the entry with filters should be invalidated
  EXPECT_EQ(1, invalidated.size());
  EXPECT_TRUE(invalidated.find(key1) != invalidated.end())
      << "Bug 3: Entry with filters should be invalidated on filter-only change";
  EXPECT_FALSE(invalidated.find(key2) != invalidated.end())
      << "Bug 3: Entry without filters should NOT be invalidated on filter-only change";
}

/**
 * @brief Test that filter change does not invalidate entries without filters
 */
TEST(InvalidationManagerTest, FilterChangeDoesNotInvalidateUnfilteredEntries) {
  QueryCache cache(1024 * 1024, 0.0);
  InvalidationManager mgr(&cache);

  // Only entries without filters
  auto key1 = CacheKeyGenerator::Generate("query1");
  CacheMetadata meta1;
  meta1.table = "posts";
  meta1.ngrams = {"hel", "ell", "llo"};
  mgr.RegisterCacheEntry(key1, meta1);

  auto key2 = CacheKeyGenerator::Generate("query2");
  CacheMetadata meta2;
  meta2.table = "posts";
  meta2.ngrams = {"wor", "orl", "rld"};
  mgr.RegisterCacheEntry(key2, meta2);

  // Filter-only change with identical text
  auto invalidated = mgr.InvalidateAffectedEntries("posts", "hello", "hello", 3, 2, true, true);

  // No entries should be invalidated (none have filters)
  EXPECT_EQ(0, invalidated.size()) << "Bug 3: Entries without filters should not be invalidated on filter-only change";
}

/**
 * @brief Test that text change with filter change uses normal ngram invalidation
 *
 * When both text and filters change, normal ngram-based invalidation handles
 * the text part, and filter_columns_changed is additive (only matters when
 * changed_ngrams is empty).
 */
TEST(InvalidationManagerTest, TextChangeWithFilterChangeUsesNormalInvalidation) {
  QueryCache cache(1024 * 1024, 0.0);
  InvalidationManager mgr(&cache);

  // Entry for "hello"
  auto key1 = CacheKeyGenerator::Generate("query_hello");
  CacheMetadata meta1;
  meta1.table = "posts";
  meta1.ngrams = {"hel", "ell", "llo"};
  meta1.filters = {{"status", query::FilterOp::EQ, "1"}};
  mgr.RegisterCacheEntry(key1, meta1);

  // Entry for "world" without filters
  auto key2 = CacheKeyGenerator::Generate("query_world");
  CacheMetadata meta2;
  meta2.table = "posts";
  meta2.ngrams = {"wor", "orl", "rld"};
  mgr.RegisterCacheEntry(key2, meta2);

  // Text changes from "hello" to "world" AND filters changed
  auto invalidated = mgr.InvalidateAffectedEntries("posts", "hello", "world", 3, 2, true, true);

  // Both should be invalidated via normal ngram-based invalidation
  EXPECT_EQ(2, invalidated.size());
  EXPECT_TRUE(invalidated.find(key1) != invalidated.end());
  EXPECT_TRUE(invalidated.find(key2) != invalidated.end());
}

// =============================================================================
// P1-2: table_ngram_settings_ auxiliary index consistency
// =============================================================================

/**
 * @brief Test that historical settings lookup is consistent after register/unregister cycles
 *
 * Regression test for P1-2 optimization: After switching from O(N) scan to
 * O(1) refcounted lookup via table_ngram_settings_, verify that config change
 * invalidation still works after entries are unregistered.
 */
TEST(InvalidationManagerTest, P1_2_HistoricalSettingsAfterUnregister) {
  QueryCache cache(1024 * 1024, 0.0);
  InvalidationManager mgr(&cache);

  // Register two entries with ngram_size=2 (old config)
  auto key1 = CacheKeyGenerator::Generate("q_bigram1");
  CacheMetadata meta1;
  meta1.table = "posts";
  meta1.ngrams = {"he", "el", "ll", "lo"};
  meta1.ngram_size = 2;
  meta1.kanji_ngram_size = 1;
  meta1.cross_boundary_ngrams = true;
  mgr.RegisterCacheEntry(key1, meta1);

  auto key2 = CacheKeyGenerator::Generate("q_bigram2");
  CacheMetadata meta2;
  meta2.table = "posts";
  meta2.ngrams = {"wo", "or", "rl", "ld"};
  meta2.ngram_size = 2;
  meta2.kanji_ngram_size = 1;
  meta2.cross_boundary_ngrams = true;
  mgr.RegisterCacheEntry(key2, meta2);

  // Unregister one entry - should decrement refcount but keep settings tracked
  mgr.UnregisterCacheEntry(key1);

  // Invalidation with new config (ngram_size=3) should still find key2
  // because historical settings (ngram_size=2) are still tracked
  auto invalidated = mgr.InvalidateAffectedEntries("posts", "", "world", 3, 1, true);

  EXPECT_EQ(1, invalidated.size());
  EXPECT_TRUE(invalidated.find(key2) != invalidated.end())
      << "P1-2: Historical settings should survive partial unregister";
}

/**
 * @brief Test that historical settings are removed when all entries with those settings are gone
 */
TEST(InvalidationManagerTest, P1_2_HistoricalSettingsFullyRemoved) {
  QueryCache cache(1024 * 1024, 0.0);
  InvalidationManager mgr(&cache);

  // Register one entry with ngram_size=2
  auto key1 = CacheKeyGenerator::Generate("q_bigram");
  CacheMetadata meta1;
  meta1.table = "posts";
  meta1.ngrams = {"he", "el"};
  meta1.ngram_size = 2;
  meta1.kanji_ngram_size = 1;
  meta1.cross_boundary_ngrams = true;
  mgr.RegisterCacheEntry(key1, meta1);

  // Register one entry with current ngram_size=3
  auto key2 = CacheKeyGenerator::Generate("q_trigram");
  CacheMetadata meta2;
  meta2.table = "posts";
  meta2.ngrams = {"hel", "ell"};
  meta2.ngram_size = 3;
  meta2.kanji_ngram_size = 1;
  meta2.cross_boundary_ngrams = true;
  mgr.RegisterCacheEntry(key2, meta2);

  // Unregister the bigram entry
  mgr.UnregisterCacheEntry(key1);

  // Now only trigram entries exist. Invalidation with ngram_size=3 should
  // NOT generate historical bigrams (no entries with old settings remain).
  // This verifies the refcount properly drops to 0 and the setting is removed.
  // key2 should still be found via direct trigram match.
  auto invalidated = mgr.InvalidateAffectedEntries("posts", "", "hello", 3, 1, true);

  EXPECT_EQ(1, invalidated.size());
  EXPECT_TRUE(invalidated.find(key2) != invalidated.end());
}

/**
 * @brief Test table_ngram_settings_ consistency through re-registration
 *
 * When a cache entry is re-registered with different ngram settings,
 * the old settings refcount should be decremented and new settings incremented.
 */
TEST(InvalidationManagerTest, P1_2_ReRegisterWithDifferentSettings) {
  QueryCache cache(1024 * 1024, 0.0);
  InvalidationManager mgr(&cache);

  auto key = CacheKeyGenerator::Generate("query1");

  // Register with ngram_size=2
  CacheMetadata meta1;
  meta1.table = "posts";
  meta1.ngrams = {"he", "el"};
  meta1.ngram_size = 2;
  meta1.kanji_ngram_size = 1;
  meta1.cross_boundary_ngrams = true;
  mgr.RegisterCacheEntry(key, meta1);

  // Re-register same key with ngram_size=3
  CacheMetadata meta2;
  meta2.table = "posts";
  meta2.ngrams = {"hel", "ell"};
  meta2.ngram_size = 3;
  meta2.kanji_ngram_size = 1;
  meta2.cross_boundary_ngrams = true;
  mgr.RegisterCacheEntry(key, meta2);

  // Invalidation with ngram_size=4 should NOT look up historical ngram_size=2
  // (it was replaced by ngram_size=3). Only ngram_size=3 should be historical.
  auto invalidated = mgr.InvalidateAffectedEntries("posts", "", "hello", 4, 1, true);

  // key should be found via historical trigram generation
  EXPECT_EQ(1, invalidated.size());
  EXPECT_TRUE(invalidated.find(key) != invalidated.end());
}

/**
 * @brief Test table_ngram_settings_ consistency through ClearTable
 */
TEST(InvalidationManagerTest, P1_2_ClearTableCleansSettings) {
  QueryCache cache(1024 * 1024, 0.0);
  InvalidationManager mgr(&cache);

  // Register entries with different settings for "posts"
  auto key1 = CacheKeyGenerator::Generate("q1");
  CacheMetadata meta1;
  meta1.table = "posts";
  meta1.ngrams = {"he", "el"};
  meta1.ngram_size = 2;
  meta1.kanji_ngram_size = 1;
  mgr.RegisterCacheEntry(key1, meta1);

  auto key2 = CacheKeyGenerator::Generate("q2");
  CacheMetadata meta2;
  meta2.table = "posts";
  meta2.ngrams = {"hel", "ell"};
  meta2.ngram_size = 3;
  meta2.kanji_ngram_size = 1;
  mgr.RegisterCacheEntry(key2, meta2);

  // Register entry for different table
  auto key3 = CacheKeyGenerator::Generate("q3");
  CacheMetadata meta3;
  meta3.table = "comments";
  meta3.ngrams = {"abc"};
  meta3.ngram_size = 2;
  meta3.kanji_ngram_size = 1;
  mgr.RegisterCacheEntry(key3, meta3);

  // Clear only "posts" table
  mgr.ClearTable("posts");

  EXPECT_EQ(1, mgr.GetTrackedEntryCount());  // Only comments entry remains

  // Invalidation on posts should find nothing (no historical settings)
  auto invalidated = mgr.InvalidateAffectedEntries("posts", "", "hello", 3, 1, true);
  EXPECT_EQ(0, invalidated.size());

  // Comments entry should still work
  auto invalidated2 = mgr.InvalidateAffectedEntries("comments", "", "abc", 3, 1, true);
  EXPECT_EQ(1, invalidated2.size());
}

// =============================================================================
// C-8: Filter invalidation bypassed when text also changes
// =============================================================================

/**
 * @brief Test that filter-bearing entries are invalidated even when text also changes
 *
 * Bug C-8: When both filter columns AND text change simultaneously, filter-bearing
 * cache entries must still be invalidated. Previously, the condition
 * `filter_columns_changed && changed_ngrams.empty()` meant filter-only entries
 * were skipped when text also changed.
 */
TEST(InvalidationManagerTest, C8_FilterInvalidationNotBypassedWhenTextAlsoChanges) {
  QueryCache cache(1024 * 1024, 0.0);
  InvalidationManager mgr(&cache);

  // Entry with filters but ngrams that do NOT overlap with the text change
  auto key_filter = CacheKeyGenerator::Generate("query_filter_only");
  CacheMetadata meta_filter;
  meta_filter.table = "posts";
  meta_filter.ngrams = {"zzz", "yyy"};  // No overlap with "hello" or "world"
  meta_filter.filters = {{"status", query::FilterOp::EQ, "active"}};
  mgr.RegisterCacheEntry(key_filter, meta_filter);

  // Entry without filters
  auto key_no_filter = CacheKeyGenerator::Generate("query_no_filter");
  CacheMetadata meta_no_filter;
  meta_no_filter.table = "posts";
  meta_no_filter.ngrams = {"zzz", "yyy"};
  // No filters
  mgr.RegisterCacheEntry(key_no_filter, meta_no_filter);

  // Both text AND filter columns changed: "hello" -> "world" with filter_columns_changed=true
  // changed_ngrams is NOT empty (text changed), so the old bug would skip filter invalidation
  auto invalidated = mgr.InvalidateAffectedEntries("posts", "hello", "world", 3, 2, true, true);

  // The filter-bearing entry should be invalidated even though changed_ngrams is non-empty
  EXPECT_TRUE(invalidated.find(key_filter) != invalidated.end())
      << "C-8: Filter-bearing entry must be invalidated when both text and filters change";

  // The no-filter entry should NOT be invalidated (no ngram overlap, no filters)
  EXPECT_FALSE(invalidated.find(key_no_filter) != invalidated.end())
      << "C-8: Entry without filters should not be invalidated by filter change alone";
}

// =============================================================================
// Perf-1: ClearTable uses table -> cache_keys reverse index for O(k) cost
// =============================================================================

/**
 * @brief Verify ClearTable correctly removes only the target table's entries
 *        when many tables share the manager.
 *
 * The performance improvement (O(k) instead of O(N)) is verified by reading
 * the algorithm in invalidation_manager.cpp; this test guards the functional
 * contract that must hold under either implementation: clearing one table out
 * of many leaves the others completely untouched.
 */
TEST(InvalidationManagerTest, ClearTableScalesWithTableSizeNotTotalSize) {
  QueryCache cache(64 * 1024 * 1024, 0.0);
  InvalidationManager mgr(&cache);

  constexpr int kNumTables = 10;
  constexpr int kEntriesPerTable = 1000;
  constexpr int kTotalEntries = kNumTables * kEntriesPerTable;

  // Register kEntriesPerTable entries across kNumTables tables.
  for (int t = 0; t < kNumTables; ++t) {
    std::string table = "t" + std::to_string(t);
    for (int i = 0; i < kEntriesPerTable; ++i) {
      auto key = CacheKeyGenerator::Generate(table + "_q" + std::to_string(i));
      CacheMetadata meta;
      meta.table = table;
      meta.ngrams = {"ng" + std::to_string(i)};
      mgr.RegisterCacheEntry(key, meta);
    }
  }

  ASSERT_EQ(static_cast<size_t>(kTotalEntries), mgr.GetTrackedEntryCount());

  // Clear one table; only its entries should disappear.
  mgr.ClearTable("t1");

  EXPECT_EQ(static_cast<size_t>(kTotalEntries - kEntriesPerTable), mgr.GetTrackedEntryCount())
      << "ClearTable must remove exactly the cleared table's entries";
  EXPECT_EQ(0, mgr.GetTrackedNgramCount("t1")) << "Cleared table must have no ngrams left";

  // Other tables must remain fully intact.
  for (int t = 0; t < kNumTables; ++t) {
    if (t == 1) {
      continue;
    }
    std::string table = "t" + std::to_string(t);
    EXPECT_EQ(static_cast<size_t>(kEntriesPerTable), mgr.GetTrackedNgramCount(table))
        << "Untouched table " << table << " must retain all its ngrams";
  }
}

/**
 * @brief Verify the table -> cache_keys reverse index is updated by
 *        ClearTable so subsequent invalidations against that table find
 *        zero entries.
 */
TEST(InvalidationManagerTest, ClearTableUpdatesReverseIndex) {
  QueryCache cache(1024 * 1024, 0.0);
  InvalidationManager mgr(&cache);

  // Two tables; we only clear one.
  for (int i = 0; i < 50; ++i) {
    auto key_t1 = CacheKeyGenerator::Generate("t1_q" + std::to_string(i));
    CacheMetadata meta_t1;
    meta_t1.table = "t1";
    meta_t1.ngrams = {"hel", "ell", "llo"};  // overlap with "hello"
    mgr.RegisterCacheEntry(key_t1, meta_t1);

    auto key_t2 = CacheKeyGenerator::Generate("t2_q" + std::to_string(i));
    CacheMetadata meta_t2;
    meta_t2.table = "t2";
    meta_t2.ngrams = {"hel", "ell", "llo"};
    mgr.RegisterCacheEntry(key_t2, meta_t2);
  }

  // Sanity: invalidating t1 with "hello" hits all 50 entries.
  auto pre_clear = mgr.InvalidateAffectedEntries("t1", "", "hello", 3, 2);
  EXPECT_EQ(50, pre_clear.size());

  mgr.ClearTable("t1");

  // After ClearTable, no t1 entries should exist in the reverse index.
  auto post_clear = mgr.InvalidateAffectedEntries("t1", "", "hello", 3, 2);
  EXPECT_EQ(0, post_clear.size()) << "ClearTable must remove t1 entries from the reverse index";

  // t2 should be unaffected.
  auto t2_invalidated = mgr.InvalidateAffectedEntries("t2", "", "hello", 3, 2);
  EXPECT_EQ(50, t2_invalidated.size()) << "ClearTable on t1 must not touch t2";
}

}  // namespace mygramdb::cache
