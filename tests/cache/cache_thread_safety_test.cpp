/**
 * @file cache_thread_safety_test.cpp
 * @brief Thread safety tests for cache module atomic fields
 *
 * Validates that CacheManager::enabled_/ttl_seconds_ and
 * QueryCache::ttl_seconds_/min_query_cost_ms_ can be safely
 * read and written from multiple threads concurrently.
 */

#include <gtest/gtest.h>

#include <atomic>
#include <thread>
#include <vector>

#include "cache/cache_manager.h"
#include "cache/query_cache.h"
#include "config/config.h"
#include "index/index.h"
#include "query/query_parser.h"
#include "server/server_types.h"
#include "storage/document_store.h"

namespace mygramdb::cache {

namespace {

/// Number of iterations per thread in stress tests
constexpr int kIterationsPerThread = 1000;
/// Number of threads for concurrent tests
constexpr int kNumThreads = 4;

/**
 * @brief Helper to create table contexts for testing
 */
std::unordered_map<std::string, server::TableContext*> CreateTestTableContexts(
    std::vector<std::unique_ptr<server::TableContext>>& owned_contexts) {
  std::unordered_map<std::string, server::TableContext*> contexts;

  auto ctx = std::make_unique<server::TableContext>();
  ctx->name = "posts";
  ctx->config.name = "posts";
  ctx->config.ngram_size = 3;
  ctx->config.kanji_ngram_size = 2;
  ctx->index = std::make_unique<index::Index>(3, 2);
  ctx->doc_store = std::make_unique<storage::DocumentStore>();

  contexts["posts"] = ctx.get();
  owned_contexts.push_back(std::move(ctx));

  return contexts;
}

/**
 * @brief Helper to create a basic search query
 */
query::Query CreateQuery(const std::string& table, const std::string& search_text) {
  query::Query query;
  query.type = query::QueryType::SEARCH;
  query.table = table;
  query.search_text = search_text;
  query.limit = 100;
  query.limit_explicit = false;
  return query;
}

}  // namespace

/**
 * @brief Test that CacheManager IsEnabled can be read from multiple threads
 * while one thread toggles Enable/Disable
 *
 * Validates that the atomic enabled_ field prevents data races.
 * Note: Enable/Disable also start/stop the invalidation queue worker,
 * so we use a single control thread for toggling and multiple reader threads.
 */
TEST(CacheThreadSafetyTest, CacheManagerEnableDisableConcurrent) {
  config::CacheConfig config;
  config.enabled = true;
  config.max_memory_bytes = 1024 * 1024;

  std::vector<std::unique_ptr<server::TableContext>> owned_contexts;
  auto table_contexts = CreateTestTableContexts(owned_contexts);

  CacheManager mgr(config, table_contexts);

  std::atomic<bool> done{false};
  std::atomic<int> read_count{0};

  // Reader threads: continuously read IsEnabled() concurrently
  std::vector<std::thread> readers;
  for (int t = 0; t < kNumThreads; ++t) {
    readers.emplace_back([&mgr, &done, &read_count]() {
      while (!done.load(std::memory_order_acquire)) {
        // NOLINTNEXTLINE(bugprone-unused-return-value)
        mgr.IsEnabled();
        read_count.fetch_add(1, std::memory_order_relaxed);
      }
    });
  }

  // Single writer thread: toggle Enable/Disable
  // Using a small number of toggles since each involves starting/stopping a thread
  constexpr int kToggleCount = 10;
  for (int i = 0; i < kToggleCount; ++i) {
    mgr.Disable();
    mgr.Enable();
  }

  done.store(true, std::memory_order_release);
  for (auto& thread : readers) {
    thread.join();
  }

  // Verify readers ran without crash
  EXPECT_GT(read_count.load(), 0);
}

/**
 * @brief Test that CacheManager Lookup works concurrently with Enable/Disable toggling
 *
 * Validates no data race between toggling enabled_ and reading it in Lookup.
 */
TEST(CacheThreadSafetyTest, CacheManagerLookupWithEnableDisable) {
  config::CacheConfig config;
  config.enabled = true;
  config.max_memory_bytes = 1024 * 1024;

  std::vector<std::unique_ptr<server::TableContext>> owned_contexts;
  auto table_contexts = CreateTestTableContexts(owned_contexts);

  CacheManager mgr(config, table_contexts);

  // Insert an entry so Lookup has something to find
  auto query = CreateQuery("posts", "thread safety test");
  std::vector<DocId> result = {1, 2, 3};
  std::vector<std::string> ngrams = {"ead", "hre", "rea", "thr"};
  mgr.Insert(query, result, ngrams, 10.0);

  std::atomic<bool> done{false};
  std::atomic<int> lookup_count{0};

  // Lookup threads
  std::vector<std::thread> lookup_threads;
  for (int t = 0; t < kNumThreads; ++t) {
    lookup_threads.emplace_back([&mgr, &query, &done, &lookup_count]() {
      while (!done.load(std::memory_order_acquire)) {
        // Lookup may return value or nullopt depending on enabled_ state
        mgr.Lookup(query);
        lookup_count.fetch_add(1, std::memory_order_relaxed);
      }
    });
  }

  // Toggle enable/disable a few times while lookups are running
  constexpr int kToggleCount = 5;
  for (int i = 0; i < kToggleCount; ++i) {
    mgr.Disable();
    mgr.Enable();
  }

  done.store(true, std::memory_order_release);
  for (auto& thread : lookup_threads) {
    thread.join();
  }

  EXPECT_GT(lookup_count.load(), 0);
}

/**
 * @brief Test that QueryCache SetTtl can be called concurrently with Lookup
 *
 * Validates that atomic ttl_seconds_ prevents data races when one thread
 * updates TTL while others perform lookups.
 */
TEST(CacheThreadSafetyTest, QueryCacheSetTtlConcurrentWithLookup) {
  // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
  QueryCache cache(1024 * 1024, 0.0, 0, false);

  // Insert an entry
  CacheKey key(0x1234567890ABCDEFULL, 0xFEDCBA0987654321ULL);
  std::vector<DocId> result = {10, 20, 30};
  CacheMetadata metadata;
  metadata.key = key;
  metadata.table = "posts";
  metadata.created_at = std::chrono::steady_clock::now();
  metadata.last_accessed = metadata.created_at;

  // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
  cache.Insert(key, result, metadata, 5.0);

  std::atomic<int> lookup_count{0};
  std::atomic<int> set_ttl_count{0};

  std::vector<std::thread> threads;

  // Lookup threads
  for (int t = 0; t < kNumThreads / 2; ++t) {
    threads.emplace_back([&cache, &key, &lookup_count]() {
      for (int i = 0; i < kIterationsPerThread; ++i) {
        cache.Lookup(key);
        lookup_count.fetch_add(1, std::memory_order_relaxed);
      }
    });
  }

  // SetTtl threads
  for (int t = 0; t < kNumThreads / 2; ++t) {
    threads.emplace_back([&cache, &set_ttl_count]() {
      for (int i = 0; i < kIterationsPerThread; ++i) {
        // Alternate between various TTL values
        cache.SetTtl(i % 10);  // NOLINT(cppcoreguidelines-avoid-magic-numbers)
        set_ttl_count.fetch_add(1, std::memory_order_relaxed);
      }
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }

  EXPECT_EQ(lookup_count.load(), (kNumThreads / 2) * kIterationsPerThread);
  EXPECT_EQ(set_ttl_count.load(), (kNumThreads / 2) * kIterationsPerThread);
}

/**
 * @brief Test that QueryCache SetMinQueryCost can be called concurrently with Insert
 *
 * Validates that atomic min_query_cost_ms_ prevents data races when one thread
 * updates the threshold while others perform inserts.
 */
TEST(CacheThreadSafetyTest, QueryCacheSetMinQueryCostConcurrentWithInsert) {
  // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
  QueryCache cache(10 * 1024 * 1024, 1.0, 0, false);

  std::atomic<int> insert_count{0};
  std::atomic<int> set_cost_count{0};
  std::atomic<uint64_t> key_counter{0};

  std::vector<std::thread> threads;

  // Insert threads
  for (int t = 0; t < kNumThreads / 2; ++t) {
    threads.emplace_back([&cache, &insert_count, &key_counter]() {
      for (int i = 0; i < kIterationsPerThread; ++i) {
        uint64_t k = key_counter.fetch_add(1, std::memory_order_relaxed);
        CacheKey key(k, k + 1);
        std::vector<DocId> result = {static_cast<DocId>(k)};
        CacheMetadata metadata;
        metadata.key = key;
        metadata.table = "posts";
        metadata.created_at = std::chrono::steady_clock::now();
        metadata.last_accessed = metadata.created_at;

        // query_cost_ms varies; some will be below threshold, some above
        double cost = static_cast<double>(i % 20);  // NOLINT(cppcoreguidelines-avoid-magic-numbers)
        cache.Insert(key, result, metadata, cost);
        insert_count.fetch_add(1, std::memory_order_relaxed);
      }
    });
  }

  // SetMinQueryCost threads
  for (int t = 0; t < kNumThreads / 2; ++t) {
    threads.emplace_back([&cache, &set_cost_count]() {
      for (int i = 0; i < kIterationsPerThread; ++i) {
        // Alternate between various thresholds
        double cost = static_cast<double>(i % 15);  // NOLINT(cppcoreguidelines-avoid-magic-numbers)
        cache.SetMinQueryCost(cost);
        set_cost_count.fetch_add(1, std::memory_order_relaxed);
      }
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }

  EXPECT_EQ(insert_count.load(), (kNumThreads / 2) * kIterationsPerThread);
  EXPECT_EQ(set_cost_count.load(), (kNumThreads / 2) * kIterationsPerThread);
}

/**
 * @brief Test that CacheManager SetTtl can be called concurrently from multiple threads
 *
 * Validates that atomic ttl_seconds_ in both CacheManager and QueryCache
 * prevents data races during concurrent TTL updates.
 */
TEST(CacheThreadSafetyTest, CacheManagerSetTtlConcurrent) {
  config::CacheConfig config;
  config.enabled = true;
  config.max_memory_bytes = 1024 * 1024;

  std::vector<std::unique_ptr<server::TableContext>> owned_contexts;
  auto table_contexts = CreateTestTableContexts(owned_contexts);

  CacheManager mgr(config, table_contexts);

  std::atomic<int> completed{0};

  std::vector<std::thread> threads;
  threads.reserve(kNumThreads);

  for (int t = 0; t < kNumThreads; ++t) {
    threads.emplace_back([&mgr, &completed, t]() {
      for (int i = 0; i < kIterationsPerThread; ++i) {
        mgr.SetTtl((t * kIterationsPerThread + i) % 300);  // NOLINT(cppcoreguidelines-avoid-magic-numbers)
        completed.fetch_add(1, std::memory_order_relaxed);
      }
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }

  EXPECT_EQ(completed.load(), kNumThreads * kIterationsPerThread);
}

}  // namespace mygramdb::cache
