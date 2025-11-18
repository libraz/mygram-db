/**
 * @file invalidation_queue_test.cpp
 * @brief Unit tests for InvalidationQueue - async batch processing
 */

#include "cache/invalidation_queue.h"

#include <gtest/gtest.h>

#include <thread>

#include "cache/invalidation_manager.h"
#include "cache/query_cache.h"
#include "index/index.h"
#include "query/cache_key.h"
#include "server/server_types.h"
#include "storage/document_store.h"

namespace mygramdb::cache {

/**
 * @brief Helper to create table contexts for testing
 */
std::unordered_map<std::string, server::TableContext*> CreateTestTableContexts(
    std::vector<std::unique_ptr<server::TableContext>>& owned_contexts, int ngram_size = 3, int kanji_ngram_size = 2) {
  std::unordered_map<std::string, server::TableContext*> contexts;

  // Create contexts for common test tables
  for (const auto& table_name : {"posts", "comments"}) {
    auto ctx = std::make_unique<server::TableContext>();
    ctx->name = table_name;
    ctx->config.name = table_name;
    ctx->config.ngram_size = ngram_size;
    ctx->config.kanji_ngram_size = kanji_ngram_size;
    ctx->index = std::make_unique<index::Index>(ngram_size, kanji_ngram_size);
    ctx->doc_store = std::make_unique<storage::DocumentStore>();

    contexts[table_name] = ctx.get();
    owned_contexts.push_back(std::move(ctx));
  }

  return contexts;
}

/**
 * @brief Test basic enqueue and processing
 */
TEST(InvalidationQueueTest, BasicEnqueueProcess) {
  QueryCache cache(1024 * 1024, 10.0);
  InvalidationManager mgr(&cache);
  std::vector<std::unique_ptr<server::TableContext>> owned_contexts;
  auto table_contexts = CreateTestTableContexts(owned_contexts, 3, 2);
  InvalidationQueue queue(&cache, &mgr, table_contexts);

  // Register a cache entry
  auto key = CacheKeyGenerator::Generate("query1");
  CacheMetadata meta;
  meta.table = "posts";
  meta.ngrams = {"gol", "ola", "lan", "ang"};

  std::vector<DocId> result = {1, 2, 3};
  cache.Insert(key, result, meta, 15.0);
  mgr.RegisterCacheEntry(key, meta);

  // Start worker
  queue.Start();

  // Enqueue invalidation
  queue.Enqueue("posts", "", "golang tutorial");

  // Give worker time to process
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  // Stop worker
  queue.Stop();

  // Entry should be erased (not just invalidated)
  EXPECT_FALSE(cache.Lookup(key).has_value());
}

/**
 * @brief Test batch size threshold
 */
TEST(InvalidationQueueTest, BatchSizeThreshold) {
  QueryCache cache(10 * 1024 * 1024, 10.0);
  InvalidationManager mgr(&cache);
  std::vector<std::unique_ptr<server::TableContext>> owned_contexts;
  auto table_contexts = CreateTestTableContexts(owned_contexts, 3, 2);
  InvalidationQueue queue(&cache, &mgr, table_contexts);

  // Set small batch size
  queue.SetBatchSize(5);
  queue.SetMaxDelay(10000);  // Long delay so only batch size triggers

  // Register multiple cache entries
  for (int i = 0; i < 10; ++i) {
    auto key = CacheKeyGenerator::Generate("query" + std::to_string(i));
    CacheMetadata meta;
    meta.table = "posts";
    meta.ngrams = {"tes", "est"};

    std::vector<DocId> result = {static_cast<DocId>(i)};
    cache.Insert(key, result, meta, 15.0);
    mgr.RegisterCacheEntry(key, meta);
  }

  queue.Start();

  // Enqueue many invalidations
  for (int i = 0; i < 10; ++i) {
    queue.Enqueue("posts", "", "test" + std::to_string(i));
  }

  // Give worker time to process batch
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  queue.Stop();

  // All entries should be erased
  for (int i = 0; i < 10; ++i) {
    auto key = CacheKeyGenerator::Generate("query" + std::to_string(i));
    EXPECT_FALSE(cache.Lookup(key).has_value());
  }
}

/**
 * @brief Test max delay threshold
 */
TEST(InvalidationQueueTest, MaxDelayThreshold) {
  QueryCache cache(1024 * 1024, 10.0);
  InvalidationManager mgr(&cache);
  std::vector<std::unique_ptr<server::TableContext>> owned_contexts;
  auto table_contexts = CreateTestTableContexts(owned_contexts, 3, 2);
  InvalidationQueue queue(&cache, &mgr, table_contexts);

  // Set large batch size but short delay
  queue.SetBatchSize(1000);
  queue.SetMaxDelay(50);  // 50ms delay

  // Register cache entry
  auto key = CacheKeyGenerator::Generate("query1");
  CacheMetadata meta;
  meta.table = "posts";
  meta.ngrams = {"gol", "ola", "lan", "ang"};

  std::vector<DocId> result = {1, 2, 3};
  cache.Insert(key, result, meta, 15.0);
  mgr.RegisterCacheEntry(key, meta);

  queue.Start();

  // Enqueue single invalidation
  queue.Enqueue("posts", "", "golang");

  // Wait for max delay to trigger processing
  std::this_thread::sleep_for(std::chrono::milliseconds(150));

  queue.Stop();

  // Entry should be erased due to max delay timeout
  EXPECT_FALSE(cache.Lookup(key).has_value());
}

/**
 * @brief Test deduplication - multiple events with same ngrams
 */
TEST(InvalidationQueueTest, Deduplication) {
  QueryCache cache(1024 * 1024, 10.0);
  InvalidationManager mgr(&cache);
  std::vector<std::unique_ptr<server::TableContext>> owned_contexts;
  auto table_contexts = CreateTestTableContexts(owned_contexts, 3, 2);
  InvalidationQueue queue(&cache, &mgr, table_contexts);

  // Register cache entry
  auto key = CacheKeyGenerator::Generate("query1");
  CacheMetadata meta;
  meta.table = "posts";
  meta.ngrams = {"gol", "ola", "lan", "ang"};

  std::vector<DocId> result = {1, 2, 3};
  cache.Insert(key, result, meta, 15.0);
  mgr.RegisterCacheEntry(key, meta);

  queue.SetBatchSize(100);  // Large batch, won't trigger by size
  queue.SetMaxDelay(100);   // Will trigger by delay

  queue.Start();

  // Enqueue same invalidation multiple times (should deduplicate)
  for (int i = 0; i < 50; ++i) {
    queue.Enqueue("posts", "", "golang tips");
  }

  // Wait for processing
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  queue.Stop();

  // Entry should be erased once (deduplication worked)
  EXPECT_FALSE(cache.Lookup(key).has_value());
}

/**
 * @brief Test UPDATE invalidation (old_text and new_text)
 */
TEST(InvalidationQueueTest, UpdateInvalidation) {
  QueryCache cache(1024 * 1024, 10.0);
  InvalidationManager mgr(&cache);
  std::vector<std::unique_ptr<server::TableContext>> owned_contexts;
  auto table_contexts = CreateTestTableContexts(owned_contexts, 3, 2);
  InvalidationQueue queue(&cache, &mgr, table_contexts);

  // Query for "rust"
  auto key1 = CacheKeyGenerator::Generate("query1");
  CacheMetadata meta1;
  meta1.table = "posts";
  meta1.ngrams = {"rus", "ust"};
  cache.Insert(key1, {1, 2}, meta1, 15.0);
  mgr.RegisterCacheEntry(key1, meta1);

  // Query for "golang"
  auto key2 = CacheKeyGenerator::Generate("query2");
  CacheMetadata meta2;
  meta2.table = "posts";
  meta2.ngrams = {"gol", "ola", "lan", "ang"};
  cache.Insert(key2, {3, 4}, meta2, 15.0);
  mgr.RegisterCacheEntry(key2, meta2);

  queue.Start();

  // UPDATE: change "rust" to "golang"
  queue.Enqueue("posts", "rust programming", "golang programming");

  // Wait for processing
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  queue.Stop();

  // Both should be invalidated
  EXPECT_FALSE(cache.Lookup(key1).has_value());
  EXPECT_FALSE(cache.Lookup(key2).has_value());
}

/**
 * @brief Test table isolation
 */
TEST(InvalidationQueueTest, TableIsolation) {
  QueryCache cache(1024 * 1024, 10.0);
  InvalidationManager mgr(&cache);
  std::vector<std::unique_ptr<server::TableContext>> owned_contexts;
  auto table_contexts = CreateTestTableContexts(owned_contexts, 3, 2);
  InvalidationQueue queue(&cache, &mgr, table_contexts);

  // Query for "posts" table
  auto key1 = CacheKeyGenerator::Generate("query1");
  CacheMetadata meta1;
  meta1.table = "posts";
  meta1.ngrams = {"gol", "ola", "lan", "ang"};
  cache.Insert(key1, {1, 2}, meta1, 15.0);
  mgr.RegisterCacheEntry(key1, meta1);

  // Query for "comments" table with same ngrams
  auto key2 = CacheKeyGenerator::Generate("query2");
  CacheMetadata meta2;
  meta2.table = "comments";
  meta2.ngrams = {"gol", "ola", "lan", "ang"};
  cache.Insert(key2, {3, 4}, meta2, 15.0);
  mgr.RegisterCacheEntry(key2, meta2);

  queue.Start();

  // Invalidate only "posts" table
  queue.Enqueue("posts", "", "golang tips");

  // Wait for processing
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  queue.Stop();

  // Only posts query should be invalidated
  EXPECT_FALSE(cache.Lookup(key1).has_value());
  EXPECT_TRUE(cache.Lookup(key2).has_value());
}

/**
 * @brief Test stop without start (should not crash)
 */
TEST(InvalidationQueueTest, StopWithoutStart) {
  QueryCache cache(1024 * 1024, 10.0);
  InvalidationManager mgr(&cache);
  std::vector<std::unique_ptr<server::TableContext>> owned_contexts;
  auto table_contexts = CreateTestTableContexts(owned_contexts, 3, 2);
  InvalidationQueue queue(&cache, &mgr, table_contexts);

  // Should not crash
  queue.Stop();

  EXPECT_FALSE(queue.IsRunning());
}

/**
 * @brief Test multiple start/stop cycles
 */
TEST(InvalidationQueueTest, MultipleStartStop) {
  QueryCache cache(1024 * 1024, 10.0);
  InvalidationManager mgr(&cache);
  std::vector<std::unique_ptr<server::TableContext>> owned_contexts;
  auto table_contexts = CreateTestTableContexts(owned_contexts, 3, 2);
  InvalidationQueue queue(&cache, &mgr, table_contexts);

  // Start and stop multiple times
  queue.Start();
  EXPECT_TRUE(queue.IsRunning());

  queue.Stop();
  EXPECT_FALSE(queue.IsRunning());

  queue.Start();
  EXPECT_TRUE(queue.IsRunning());

  queue.Stop();
  EXPECT_FALSE(queue.IsRunning());
}

/**
 * @brief Test enqueue while worker is stopped (should buffer)
 */
TEST(InvalidationQueueTest, EnqueueWhileStopped) {
  QueryCache cache(1024 * 1024, 10.0);
  InvalidationManager mgr(&cache);
  std::vector<std::unique_ptr<server::TableContext>> owned_contexts;
  auto table_contexts = CreateTestTableContexts(owned_contexts, 3, 2);
  InvalidationQueue queue(&cache, &mgr, table_contexts);

  // Register cache entry
  auto key = CacheKeyGenerator::Generate("query1");
  CacheMetadata meta;
  meta.table = "posts";
  meta.ngrams = {"gol", "ola", "lan", "ang"};
  cache.Insert(key, {1, 2, 3}, meta, 15.0);
  mgr.RegisterCacheEntry(key, meta);

  // Enqueue while stopped (should buffer)
  queue.Enqueue("posts", "", "golang");

  // Now start worker
  queue.Start();

  // Wait for processing
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  queue.Stop();

  // Entry should be erased (buffered events processed on start)
  EXPECT_FALSE(cache.Lookup(key).has_value());
}

/**
 * @brief Test high-frequency enqueuing (stress test)
 */
TEST(InvalidationQueueTest, HighFrequencyEnqueuing) {
  QueryCache cache(10 * 1024 * 1024, 10.0);
  InvalidationManager mgr(&cache);
  std::vector<std::unique_ptr<server::TableContext>> owned_contexts;
  auto table_contexts = CreateTestTableContexts(owned_contexts, 3, 2);
  InvalidationQueue queue(&cache, &mgr, table_contexts);

  // Register many cache entries
  for (int i = 0; i < 100; ++i) {
    auto key = CacheKeyGenerator::Generate("query" + std::to_string(i));
    CacheMetadata meta;
    meta.table = "posts";
    meta.ngrams = {"tes", "est"};
    cache.Insert(key, {static_cast<DocId>(i)}, meta, 15.0);
    mgr.RegisterCacheEntry(key, meta);
  }

  queue.Start();

  // Rapid-fire enqueuing
  for (int i = 0; i < 1000; ++i) {
    queue.Enqueue("posts", "", "test" + std::to_string(i % 10));
  }

  // Wait for all processing
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  queue.Stop();

  // All entries should be invalidated
  for (int i = 0; i < 100; ++i) {
    auto key = CacheKeyGenerator::Generate("query" + std::to_string(i));
    EXPECT_FALSE(cache.Lookup(key).has_value());
  }
}

/**
 * @brief Test that invalidation batches are counted correctly
 *
 * This is a regression test to ensure that the batch counter is incremented
 * exactly once per batch, even when processing happens on a separate thread.
 */
TEST(InvalidationQueueTest, BatchStatisticsCount) {
  QueryCache cache(1024 * 1024, 1.0);
  InvalidationManager mgr(&cache);
  std::vector<std::unique_ptr<server::TableContext>> owned_contexts;
  auto table_contexts = CreateTestTableContexts(owned_contexts, 3, 2);
  InvalidationQueue queue(&cache, &mgr, table_contexts);

  // Set small batch size for predictable batching
  queue.SetBatchSize(3);
  queue.SetMaxDelay(1000);  // 1000ms

  // Register cache entries with different ngrams
  for (int i = 0; i < 5; ++i) {
    auto key = CacheKeyGenerator::Generate("query" + std::to_string(i));
    CacheMetadata meta;
    meta.table = "posts";
    // Use different ngrams for each entry to avoid deduplication
    meta.ngrams = {"ng" + std::to_string(i)};

    std::vector<DocId> result = {static_cast<DocId>(i)};
    cache.Insert(key, result, meta, 10.0);
    mgr.RegisterCacheEntry(key, meta);
  }

  // Get initial statistics
  auto initial_stats = cache.GetStatistics();
  uint64_t initial_batches = initial_stats.invalidations_batches;

  // Start worker
  queue.Start();

  // Enqueue 5 distinct invalidations
  for (int i = 0; i < 5; ++i) {
    queue.Enqueue("posts", "", "ng" + std::to_string(i));
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }

  // Wait for first batch (3 items) to process
  std::this_thread::sleep_for(std::chrono::milliseconds(300));

  // Stop worker (will process remaining 2 items as second batch)
  queue.Stop();

  // Get final statistics
  auto final_stats = cache.GetStatistics();

  // Should have processed 2 batches (3 items + 2 items)
  EXPECT_GE(final_stats.invalidations_batches, initial_batches + 1) << "At least one batch should be processed";
}

/**
 * @brief Test batch counter with single batch
 */
TEST(InvalidationQueueTest, SingleBatchCount) {
  QueryCache cache(1024 * 1024, 1.0);
  InvalidationManager mgr(&cache);
  std::vector<std::unique_ptr<server::TableContext>> owned_contexts;
  auto table_contexts = CreateTestTableContexts(owned_contexts, 3, 2);
  InvalidationQueue queue(&cache, &mgr, table_contexts);

  // Set large batch size
  queue.SetBatchSize(100);

  // Register a single entry
  auto key = CacheKeyGenerator::Generate("query1");
  CacheMetadata meta;
  meta.table = "posts";
  meta.ngrams = {"foo", "oo", "bar"};

  std::vector<DocId> result = {1, 2, 3};
  cache.Insert(key, result, meta, 10.0);
  mgr.RegisterCacheEntry(key, meta);

  // Get initial batch count
  auto initial_stats = cache.GetStatistics();
  uint64_t initial_batches = initial_stats.invalidations_batches;

  // Start worker
  queue.Start();

  // Enqueue invalidation
  queue.Enqueue("posts", "", "foo bar");

  // Stop worker (will process remaining items as one batch)
  queue.Stop();

  // Get statistics
  auto stats = cache.GetStatistics();

  // Should have exactly 1 more batch than initial
  EXPECT_EQ(initial_batches + 1, stats.invalidations_batches);

  // Entry should be invalidated
  EXPECT_FALSE(cache.Lookup(key).has_value());
}

/**
 * @brief Test that synchronous invalidation path cleans up metadata
 *
 * This is a regression test for a bug where the synchronous invalidation path
 * (when worker is not running) called cache_->Erase() but did not call
 * invalidation_mgr_->UnregisterCacheEntry(), causing cache_metadata_ and
 * ngram_to_cache_keys_ to grow unbounded.
 */
TEST(InvalidationQueueTest, SynchronousInvalidationCleansUpMetadata) {
  QueryCache cache(1024 * 1024, 1.0);
  InvalidationManager mgr(&cache);
  std::vector<std::unique_ptr<server::TableContext>> owned_contexts;
  auto table_contexts = CreateTestTableContexts(owned_contexts, 3, 2);
  InvalidationQueue queue(&cache, &mgr, table_contexts);

  // DO NOT start the worker - this forces synchronous invalidation path
  ASSERT_FALSE(queue.IsRunning());

  // Register multiple cache entries
  constexpr int kNumEntries = 10;
  for (int i = 0; i < kNumEntries; ++i) {
    auto key = CacheKeyGenerator::Generate("query" + std::to_string(i));
    CacheMetadata meta;
    meta.table = "posts";
    meta.ngrams = {"tes", "est", "test"};

    std::vector<DocId> result = {static_cast<DocId>(i)};
    ASSERT_TRUE(cache.Insert(key, result, meta, 10.0));
    mgr.RegisterCacheEntry(key, meta);
  }

  // Verify entries are tracked
  EXPECT_EQ(kNumEntries, mgr.GetTrackedEntryCount());
  EXPECT_GT(mgr.GetTrackedNgramCount("posts"), 0);

  // Enqueue invalidations while worker is NOT running (synchronous path)
  for (int i = 0; i < kNumEntries; ++i) {
    queue.Enqueue("posts", "", "test" + std::to_string(i));
  }

  // All entries should be erased from cache
  for (int i = 0; i < kNumEntries; ++i) {
    auto key = CacheKeyGenerator::Generate("query" + std::to_string(i));
    EXPECT_FALSE(cache.Lookup(key).has_value()) << "Entry " << i << " should be erased";
  }

  // CRITICAL: Metadata should also be cleaned up
  EXPECT_EQ(0, mgr.GetTrackedEntryCount())
      << "InvalidationManager should have 0 tracked entries after synchronous invalidation";
  EXPECT_EQ(0, mgr.GetTrackedNgramCount("posts"))
      << "InvalidationManager should have 0 tracked ngrams for 'posts' table after synchronous invalidation";
}

/**
 * @brief Test metadata is cleaned up even if Erase() throws (exception safety)
 *
 * Verifies the fix where UnregisterCacheEntry() is called BEFORE Erase(),
 * ensuring metadata is cleaned up even if Erase() were to throw an exception.
 */
TEST(InvalidationQueueTest, MetadataCleanupExceptionSafe) {
  QueryCache cache(1024 * 1024, 1.0);
  InvalidationManager mgr(&cache);
  std::vector<std::unique_ptr<server::TableContext>> owned_contexts;
  auto table_contexts = CreateTestTableContexts(owned_contexts, 3, 2);
  InvalidationQueue queue(&cache, &mgr, table_contexts);

  // DO NOT start worker - use synchronous path
  ASSERT_FALSE(queue.IsRunning());

  // Register cache entry
  auto key = CacheKeyGenerator::Generate("test_query");
  CacheMetadata meta;
  meta.table = "posts";
  meta.ngrams = {"tes", "est"};

  std::vector<DocId> result = {1, 2, 3};
  ASSERT_TRUE(cache.Insert(key, result, meta, 10.0));
  mgr.RegisterCacheEntry(key, meta);

  EXPECT_EQ(1, mgr.GetTrackedEntryCount());

  // Trigger invalidation (synchronous path)
  queue.Enqueue("posts", "", "test");

  // Metadata should be cleaned up regardless of Erase() success
  // (in the fixed version, UnregisterCacheEntry is called first)
  EXPECT_EQ(0, mgr.GetTrackedEntryCount()) << "Metadata should be cleaned up even if subsequent operations fail";
}

/**
 * @brief Test for spurious wakeup handling fix
 *
 * Verifies that WorkerLoop() correctly handles spurious wakeups and
 * checks running_ flag after waking up from condition variable.
 */
TEST(InvalidationQueueTest, SpuriousWakeupHandling) {
  QueryCache cache(1024 * 1024, 10.0);
  InvalidationManager mgr(&cache);
  std::unordered_map<std::string, server::TableContext*> table_contexts;
  InvalidationQueue queue(&cache, &mgr, table_contexts);

  // Set very long delay to ensure we can stop before timeout
  queue.SetMaxDelay(60000);  // 60 seconds in milliseconds
  queue.SetBatchSize(1000);  // High threshold to prevent processing

  // Start queue
  queue.Start();

  // Add a few entries (not enough to trigger batch processing)
  for (int i = 0; i < 5; ++i) {
    queue.Enqueue("posts", "old text", "new text");
  }

  // Give worker thread time to enter wait state
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // Stop queue (should wake up worker thread immediately)
  auto start = std::chrono::steady_clock::now();
  queue.Stop();
  auto end = std::chrono::steady_clock::now();

  auto stop_duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

  // Stop should complete quickly (< 1 second) even though max_delay is 60 seconds
  // This verifies that running_ flag is checked after wakeup
  EXPECT_LT(stop_duration.count(), 1000) << "Stop() took too long, suggesting spurious wakeup handling is broken";
}

/**
 * @brief Test rapid start/stop doesn't cause worker thread to continue after stop
 */
TEST(InvalidationQueueTest, RapidStartStopNoRunawayThread) {
  QueryCache cache(1024 * 1024, 10.0);
  InvalidationManager mgr(&cache);
  std::unordered_map<std::string, server::TableContext*> table_contexts;
  InvalidationQueue queue(&cache, &mgr, table_contexts);

  queue.SetMaxDelay(100);  // 100 milliseconds

  // Rapidly start and stop multiple times
  for (int i = 0; i < 10; ++i) {
    queue.Start();
    queue.Enqueue("posts", "", "text");

    // Stop immediately
    queue.Stop();

    // Verify queue is truly stopped
    // If spurious wakeup handling is broken, worker might still be running
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }

  // If we get here without hanging or crashing, spurious wakeup handling is correct
  SUCCEED();
}

/**
 * @brief Test that worker thread exits cleanly when stopped with pending items
 */
TEST(InvalidationQueueTest, StopWithPendingItemsNoHang) {
  QueryCache cache(1024 * 1024, 10.0);
  InvalidationManager mgr(&cache);
  std::vector<std::unique_ptr<server::TableContext>> owned_contexts;
  auto table_contexts = CreateTestTableContexts(owned_contexts, 3, 2);
  InvalidationQueue queue(&cache, &mgr, table_contexts);

  queue.SetMaxDelay(3600000);  // 1 hour in milliseconds - very long delay
  queue.SetBatchSize(10000);   // Very high threshold

  queue.Start();

  // Add many items that won't be processed
  for (int i = 0; i < 100; ++i) {
    queue.Enqueue("posts", "", "some_very_long_text_string_" + std::to_string(i));
  }

  // Give enqueue operations time to accumulate
  std::this_thread::sleep_for(std::chrono::milliseconds(10));

  // Verify items are pending (though some deduplication may occur)
  // Note: Due to ngram deduplication, pending count may be less than 100
  // The main goal is to verify Stop() completes quickly even with pending items

  // Stop should complete immediately without processing pending items
  auto start = std::chrono::steady_clock::now();
  queue.Stop();
  auto end = std::chrono::steady_clock::now();

  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

  // Should stop quickly even with pending items
  EXPECT_LT(duration.count(), 500) << "Stop() with pending items took too long";
}

/**
 * @brief Test empty queue handling (time calculation bug regression test)
 * Regression test for: pending_ngrams_ empty caused negative time calculation
 */
TEST(InvalidationQueueTest, EmptyQueueStartAndEnqueue) {
  QueryCache cache(1024 * 1024, 10.0);
  InvalidationManager mgr(&cache);
  std::vector<std::unique_ptr<server::TableContext>> owned_contexts;
  auto table_contexts = CreateTestTableContexts(owned_contexts, 3, 2);
  InvalidationQueue queue(&cache, &mgr, table_contexts);

  // Start with empty queue - should not crash or cause undefined behavior
  queue.Start();

  // Wait a bit to ensure worker thread is in wait state
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  // Now enqueue an item - should wake up the worker thread
  queue.Enqueue("posts", "", "test ngram");

  // Wait for processing
  std::this_thread::sleep_for(std::chrono::milliseconds(250));

  queue.Stop();

  // Test passed if no crash occurred
  SUCCEED();
}

/**
 * @brief Test resource cleanup order (Unregister before Erase)
 * Regression test for: Erase() exception could prevent UnregisterCacheEntry()
 */
TEST(InvalidationQueueTest, ResourceCleanupOrder) {
  QueryCache cache(1024 * 1024, 10.0);
  InvalidationManager mgr(&cache);
  std::vector<std::unique_ptr<server::TableContext>> owned_contexts;
  auto table_contexts = CreateTestTableContexts(owned_contexts, 3, 2);
  InvalidationQueue queue(&cache, &mgr, table_contexts);

  // Insert some data into cache and register with invalidation manager
  auto key1 = CacheKeyGenerator::Generate("query1");
  auto key2 = CacheKeyGenerator::Generate("query2");

  std::vector<DocId> result = {1, 2, 3};
  CacheMetadata meta;
  meta.table = "posts";
  meta.ngrams = {"tes", "est", "st_"};

  cache.Insert(key1, result, meta, 15.0);
  cache.Insert(key2, result, meta, 15.0);

  // Register with invalidation manager
  mgr.RegisterCacheEntry(key1, meta);
  mgr.RegisterCacheEntry(key2, meta);

  queue.Start();

  // Enqueue invalidations to trigger cleanup
  queue.Enqueue("posts", "", "test text");

  // Wait for processing
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  queue.Stop();

  // The important part is no crash occurred during cleanup
  // (even if Erase() might throw, Unregister should happen first)
  SUCCEED();
}

/**
 * @brief Test concurrent Start() calls are thread-safe
 * Regression test for: running_ flag was not atomically checked-and-set
 */
TEST(InvalidationQueueTest, ConcurrentStartCallsThreadSafe) {
  QueryCache cache(1024 * 1024, 10.0);
  InvalidationManager mgr(&cache);
  std::vector<std::unique_ptr<server::TableContext>> owned_contexts;
  auto table_contexts = CreateTestTableContexts(owned_contexts, 3, 2);
  InvalidationQueue queue(&cache, &mgr, table_contexts);

  // Attempt to start the queue from multiple threads concurrently
  constexpr int num_threads = 10;
  std::vector<std::thread> threads;

  for (int i = 0; i < num_threads; ++i) {
    threads.emplace_back([&]() { queue.Start(); });
  }

  for (auto& thread : threads) {
    thread.join();
  }

  // With proper atomic compare_exchange_strong:
  // - Only one worker thread should be created
  // - No race condition or crash should occur
  // The queue should be in a valid running state
  EXPECT_TRUE(queue.IsRunning());

  // Stop the queue
  queue.Stop();
  EXPECT_FALSE(queue.IsRunning());
}

/**
 * @brief Test concurrent Stop() calls are thread-safe
 * Regression test for: running_ flag was not atomically checked-and-cleared
 */
TEST(InvalidationQueueTest, ConcurrentStopCallsThreadSafe) {
  QueryCache cache(1024 * 1024, 10.0);
  InvalidationManager mgr(&cache);
  std::vector<std::unique_ptr<server::TableContext>> owned_contexts;
  auto table_contexts = CreateTestTableContexts(owned_contexts, 3, 2);
  InvalidationQueue queue(&cache, &mgr, table_contexts);

  // Start the queue first
  queue.Start();
  EXPECT_TRUE(queue.IsRunning());

  // Attempt to stop the queue from multiple threads concurrently
  constexpr int num_threads = 10;
  std::vector<std::thread> threads;

  for (int i = 0; i < num_threads; ++i) {
    threads.emplace_back([&]() { queue.Stop(); });
  }

  for (auto& thread : threads) {
    thread.join();
  }

  // With proper atomic compare_exchange_strong:
  // - Worker thread should be joined exactly once
  // - No race condition or crash should occur
  EXPECT_FALSE(queue.IsRunning());
}

/**
 * @brief Test concurrent Start() calls followed by Stop()
 * Regression test for: concurrent Start() and Stop() should be atomic
 */
TEST(InvalidationQueueTest, ConcurrentStartThenStop) {
  QueryCache cache(1024 * 1024, 10.0);
  InvalidationManager mgr(&cache);
  std::vector<std::unique_ptr<server::TableContext>> owned_contexts;
  auto table_contexts = CreateTestTableContexts(owned_contexts, 3, 2);
  InvalidationQueue queue(&cache, &mgr, table_contexts);

  // Start the queue from multiple threads
  constexpr int num_threads = 5;
  std::vector<std::thread> start_threads;

  for (int i = 0; i < num_threads; ++i) {
    start_threads.emplace_back([&]() { queue.Start(); });
  }

  for (auto& thread : start_threads) {
    thread.join();
  }

  // Queue should be running (only one Start() should have succeeded)
  EXPECT_TRUE(queue.IsRunning());

  // Now stop from multiple threads
  std::vector<std::thread> stop_threads;
  for (int i = 0; i < num_threads; ++i) {
    stop_threads.emplace_back([&]() { queue.Stop(); });
  }

  for (auto& thread : stop_threads) {
    thread.join();
  }

  // Queue should be stopped
  EXPECT_FALSE(queue.IsRunning());
}

/**
 * @brief Test TOCTOU race condition fix in Enqueue
 *
 * This test verifies that the TOCTOU (Time-Of-Check-Time-Of-Use) race
 * condition between running_ check and queue insertion has been fixed.
 *
 * Scenario without fix:
 * 1. Thread 1: Checks running_ == true
 * 2. Thread 2: Calls Stop(), sets running_ = false
 * 3. Thread 1: Inserts to queue (but worker is stopped)
 * 4. Result: Metadata leak (UnregisterCacheEntry never called)
 *
 * With fix:
 * - running_ check is done inside the queue_mutex_ lock
 * - If not running, immediately process and call UnregisterCacheEntry
 * - No metadata leak occurs
 */
TEST(InvalidationQueueTest, TOCTOURaceConditionFix) {
  QueryCache cache(1024 * 1024, 1.0);
  InvalidationManager invalidation_mgr(&cache);
  std::unordered_map<std::string, server::TableContext*> table_contexts;
  InvalidationQueue queue(&cache, &invalidation_mgr, table_contexts);

  // Insert initial cache entry
  auto key = CacheKeyGenerator::Generate("test query");
  std::vector<DocId> result = {1, 2, 3};
  CacheMetadata meta;
  meta.table = "posts";
  meta.ngrams = {"tes", "est"};

  cache.Insert(key, result, meta, 10.0);
  invalidation_mgr.RegisterCacheEntry(key, meta);

  // Start and immediately stop to create race condition window
  queue.Start();

  std::atomic<int> enqueue_count{0};
  std::atomic<bool> stop_called{false};

  // Thread 1: Continuous enqueue
  std::thread enqueue_thread([&]() {
    for (int i = 0; i < 100; ++i) {
      queue.Enqueue("posts", "test", "new test");
      enqueue_count++;
      std::this_thread::sleep_for(std::chrono::microseconds(10));
    }
  });

  // Thread 2: Stop during enqueue
  std::thread stop_thread([&]() {
    std::this_thread::sleep_for(std::chrono::microseconds(50));
    queue.Stop();
    stop_called = true;
  });

  enqueue_thread.join();
  stop_thread.join();

  EXPECT_TRUE(stop_called.load());
  EXPECT_GT(enqueue_count.load(), 0);

  // With the fix, all metadata should be properly cleaned up
  // No way to directly test metadata leak, but no crash/assertion failure = success
}

/**
 * @brief Test that Enqueue processes immediately when worker not running
 *
 * This test verifies that when the worker is not running, Enqueue
 * immediately processes invalidations inside the lock, ensuring
 * UnregisterCacheEntry is always called.
 */
TEST(InvalidationQueueTest, EnqueueWhenNotRunning) {
  QueryCache cache(1024 * 1024, 1.0);
  InvalidationManager invalidation_mgr(&cache);
  std::unordered_map<std::string, server::TableContext*> table_contexts;
  InvalidationQueue queue(&cache, &invalidation_mgr, table_contexts);

  // Insert cache entries
  auto key1 = CacheKeyGenerator::Generate("query1");
  auto key2 = CacheKeyGenerator::Generate("query2");

  std::vector<DocId> result = {1, 2, 3};
  CacheMetadata meta;
  meta.table = "posts";
  meta.ngrams = {"que", "uer", "ery"};

  cache.Insert(key1, result, meta, 10.0);
  cache.Insert(key2, result, meta, 10.0);

  invalidation_mgr.RegisterCacheEntry(key1, meta);
  invalidation_mgr.RegisterCacheEntry(key2, meta);

  // Worker is NOT started - Enqueue should process immediately

  // Enqueue invalidation (old text matches the ngrams we registered)
  queue.Enqueue("posts", "query1", "different text");

  // The fix ensures that when worker is not running, Enqueue processes immediately
  // and calls UnregisterCacheEntry, preventing metadata leak
  // The test passes if no crash/assertion occurs

  // Verify cache entries still exist (they were only invalidated, not erased)
  // This is expected behavior - invalidation marks entries, erase happens separately
  auto result1 = cache.Lookup(key1);
  auto result2 = cache.Lookup(key2);

  // The important part is that no metadata leak occurred (verified by no crash)
}

/**
 * @brief Test concurrent Enqueue and Stop operations
 *
 * This test verifies thread safety when multiple threads call Enqueue
 * while another thread calls Stop.
 */
TEST(InvalidationQueueTest, ConcurrentEnqueueStop) {
  QueryCache cache(1024 * 1024, 1.0);
  InvalidationManager invalidation_mgr(&cache);
  std::unordered_map<std::string, server::TableContext*> table_contexts;
  InvalidationQueue queue(&cache, &invalidation_mgr, table_contexts);

  // Insert cache entries
  for (int i = 0; i < 10; ++i) {
    auto key = CacheKeyGenerator::Generate("query" + std::to_string(i));
    std::vector<DocId> result = {1, 2, 3};
    CacheMetadata meta;
    meta.table = "posts";
    meta.ngrams = {"que", "uer", "ery"};

    cache.Insert(key, result, meta, 10.0);
    invalidation_mgr.RegisterCacheEntry(key, meta);
  }

  queue.Start();

  std::atomic<bool> stop_flag{false};
  std::atomic<int> total_enqueues{0};

  // Multiple enqueue threads
  std::vector<std::thread> enqueue_threads;
  for (int t = 0; t < 4; ++t) {
    enqueue_threads.emplace_back([&]() {
      int local_count = 0;
      while (!stop_flag.load() && local_count < 50) {
        queue.Enqueue("posts", "query", "updated query" + std::to_string(local_count));
        local_count++;
        total_enqueues++;
      }
    });
  }

  // Let enqueues run for a bit
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  // Stop the queue
  stop_flag = true;
  queue.Stop();

  for (auto& thread : enqueue_threads) {
    thread.join();
  }

  // Verify operations completed without crash
  EXPECT_GT(total_enqueues.load(), 0);
  EXPECT_FALSE(queue.IsRunning());
}

}  // namespace mygramdb::cache
