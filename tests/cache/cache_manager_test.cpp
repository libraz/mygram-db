/**
 * @file cache_manager_test.cpp
 * @brief Integration tests for CacheManager - end-to-end cache behavior
 */

#include "cache/cache_manager.h"

#include <gtest/gtest.h>

#include <atomic>
#include <condition_variable>
#include <future>
#include <iostream>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <thread>

#if defined(__APPLE__)
#include <mach/mach.h>
#elif defined(__linux__)
#include <unistd.h>

#include <fstream>
#endif

#include "cache/cache_key.h"
#include "cache/cache_types.h"
#include "config/config.h"
#include "query/query_normalizer.h"
#include "query/query_parser.h"
#include "support/deterministic_gate.h"

namespace mygramdb::cache {

namespace {

constexpr bool IsAddressSanitizerBuild() {
#if defined(__has_feature)
#if __has_feature(address_sanitizer)
  return true;
#endif
#endif
#if defined(__SANITIZE_ADDRESS__)
  return true;
#else
  return false;
#endif
}

std::optional<size_t> CurrentResidentBytes() {
#if defined(__APPLE__)
  mach_task_basic_info_data_t info{};
  mach_msg_type_number_t count = MACH_TASK_BASIC_INFO_COUNT;
  if (task_info(mach_task_self(), MACH_TASK_BASIC_INFO, reinterpret_cast<task_info_t>(&info), &count) != KERN_SUCCESS) {
    return std::nullopt;
  }
  return static_cast<size_t>(info.resident_size);
#elif defined(__linux__)
  std::ifstream statm("/proc/self/statm");
  size_t total_pages = 0;
  size_t resident_pages = 0;
  if (!(statm >> total_pages >> resident_pages)) {
    return std::nullopt;
  }
  const long page_size = ::sysconf(_SC_PAGESIZE);
  if (page_size <= 0) {
    return std::nullopt;
  }
  return resident_pages * static_cast<size_t>(page_size);
#else
  return std::nullopt;
#endif
}

}  // namespace

/**
 * @brief Helper to create NgramConfigMap for testing
 */
NgramConfigMap CreateTestNgramConfigs(int ngram_size = 3, int kanji_ngram_size = 2) {
  NgramConfigMap configs;
  for (const auto& table_name : {"posts", "comments"}) {
    configs[table_name] = NgramConfig{
        .ngram_size = ngram_size,
        .kanji_ngram_size = kanji_ngram_size,
        .cross_boundary_ngrams = true,
    };
  }
  return configs;
}

/**
 * @brief Helper to create a basic query
 */
query::Query CreateQuery(const std::string& table, const std::string& search_text) {
  query::Query query;
  query.type = query::QueryType::SEARCH;
  query.table = table;
  query.search_text = search_text;
  query.limit = 100;
  query.limit_explicit = false;
  const std::string normalized = QueryNormalizer::Normalize(query);
  const CacheKey key = CacheKeyGenerator::Generate(normalized);
  query.cache_key = std::make_pair(key.hash_high, key.hash_low);
  query.cache_key_discriminator = normalized;
  query.cache_key_is_canonical = true;
  return query;
}

/**
 * @brief Test basic cache workflow: insert, lookup, invalidate
 */
TEST(CacheManagerTest, BasicWorkflow) {
  config::CacheConfig config;
  config.enabled = true;
  config.max_memory_bytes = 10 * 1024 * 1024;

  auto ngram_configs = CreateTestNgramConfigs(3, 2);

  CacheManager mgr(config, std::move(ngram_configs));

  auto query = CreateQuery("posts", "golang");
  std::vector<DocId> result = {1, 2, 3, 4, 5};
  std::vector<std::string> ngrams = {"ang", "gol", "lan", "ola"};

  // Insert
  EXPECT_TRUE(mgr.Insert(query, result, ngrams, 15.0));

  // Lookup - should hit
  auto cached = mgr.Lookup(query);
  ASSERT_TRUE(cached.has_value());
  EXPECT_EQ(result, cached.value());

  // Invalidate by inserting new document with "golang"
  mgr.Invalidate("posts", "", "new golang post");

  // Lookup - should miss (invalidated)
  cached = mgr.Lookup(query);
  EXPECT_FALSE(cached.has_value());
}

TEST(CacheManagerTest, FacetAndSearchShareUnderlyingDocIdEntry) {
  config::CacheConfig config;
  config.enabled = true;
  config.max_memory_bytes = 10 * 1024 * 1024;
  config.min_query_cost_ms = 0.0;
  CacheManager mgr(config, CreateTestNgramConfigs(3, 2));

  auto search = CreateQuery("posts", "golang");
  query::Query facet = search;
  facet.type = query::QueryType::FACET;
  facet.facet_column = "category";

  const std::vector<DocId> result = {1, 2, 3};
  ASSERT_TRUE(mgr.Insert(search, result, {"gol", "ola", "lan", "ang"}, 1.0));
  auto facet_hit = mgr.Lookup(facet);
  ASSERT_TRUE(facet_hit.has_value());
  EXPECT_EQ(*facet_hit, result);

  mgr.Clear();
  ASSERT_TRUE(mgr.Insert(facet, result, {"gol", "ola", "lan", "ang"}, 1.0));
  auto search_hit = mgr.Lookup(search);
  ASSERT_TRUE(search_hit.has_value());
  EXPECT_EQ(*search_hit, result);
}

TEST(CacheManagerTest, ConditionalEraseDoesNotRemoveReinsertedGeneration) {
  config::CacheConfig config;
  config.enabled = true;
  config.max_memory_bytes = 10 * 1024 * 1024;
  config.min_query_cost_ms = 0.0;
  CacheManager mgr(config, CreateTestNgramConfigs(3, 2));

  const auto query = CreateQuery("posts", "stale");
  ASSERT_TRUE(mgr.Insert(query, {1}, {"sta", "tal", "ale"}, 1.0));
  auto old = mgr.LookupWithMetadata(query);
  ASSERT_TRUE(old.has_value());
  ASSERT_NE(old->entry_generation, 0U);

  ASSERT_TRUE(mgr.Erase(query, old->entry_generation));
  const auto after_stale_erase = mgr.GetStatistics();
  EXPECT_EQ(after_stale_erase.stale_entry_removals, 1U);
  EXPECT_EQ(after_stale_erase.invalidations_deferred, 0U);
  ASSERT_TRUE(mgr.Insert(query, {2}, {"sta", "tal", "ale"}, 1.0));
  EXPECT_FALSE(mgr.Erase(query, old->entry_generation));

  auto current = mgr.Lookup(query);
  ASSERT_TRUE(current.has_value());
  EXPECT_EQ(*current, (std::vector<DocId>{2}));
}

TEST(CacheManagerTest, WorkerStartFailureKeepsCacheDisabledAtConstructionAndEnable) {
  config::CacheConfig config;
  config.enabled = true;
  config.max_memory_bytes = 10 * 1024 * 1024;

  CacheManager mgr(config, CreateTestNgramConfigs(3, 2), [](std::function<void()>) -> std::thread {
    throw std::runtime_error("deterministic thread factory failure");
  });

  EXPECT_FALSE(mgr.IsEnabled());
  EXPECT_FALSE(mgr.Enable());
  EXPECT_FALSE(mgr.IsEnabled());

  auto query = CreateQuery("posts", "golang");
  EXPECT_FALSE(mgr.Insert(query, {1, 2, 3}, {"gol", "ola", "lan", "ang"}, 15.0));
}

TEST(CacheManagerTest, ConcurrentEnableDisableSerializesWholeLifecycleTransition) {
  config::CacheConfig config;
  config.enabled = false;
  config.max_memory_bytes = 10 * 1024 * 1024;

  std::mutex gate_mutex;
  std::condition_variable gate_cv;
  bool factory_entered = false;
  bool release_factory = false;
  CacheManager mgr(config, CreateTestNgramConfigs(3, 2), [&](std::function<void()> worker) {
    {
      std::unique_lock<std::mutex> lock(gate_mutex);
      factory_entered = true;
      gate_cv.notify_all();
      gate_cv.wait(lock, [&] { return release_factory; });
    }
    return std::thread(std::move(worker));
  });

  auto enable_result = std::async(std::launch::async, [&] { return mgr.Enable(); });
  {
    std::unique_lock<std::mutex> lock(gate_mutex);
    gate_cv.wait(lock, [&] { return factory_entered; });
  }

  std::promise<void> disable_invoked;
  auto disable_result = std::async(std::launch::async, [&] {
    disable_invoked.set_value();
    mgr.Disable();
  });
  disable_invoked.get_future().wait();

  {
    std::lock_guard<std::mutex> lock(gate_mutex);
    release_factory = true;
  }
  gate_cv.notify_all();

  EXPECT_TRUE(enable_result.get());
  disable_result.get();
  EXPECT_FALSE(mgr.IsEnabled());

  ASSERT_TRUE(mgr.Enable());
  EXPECT_TRUE(mgr.IsEnabled());
}

TEST(CacheManagerTest, InsertIfVersionRejectsStaleResultAfterInvalidate) {
  config::CacheConfig config;
  config.enabled = true;
  config.max_memory_bytes = 10 * 1024 * 1024;

  auto ngram_configs = CreateTestNgramConfigs(3, 2);

  CacheManager mgr(config, std::move(ngram_configs));

  auto query = CreateQuery("posts", "golang");
  std::vector<DocId> result = {1, 2, 3};
  std::vector<std::string> ngrams = {"ang", "gol", "lan", "ola"};

  const uint64_t version_before_mutation = mgr.CaptureDataVersion();
  mgr.Invalidate("posts", "", "unrelated mutation text");

  EXPECT_FALSE(mgr.InsertIfVersion(query, result, ngrams, 15.0, version_before_mutation));
  EXPECT_FALSE(mgr.Lookup(query).has_value());

  const uint64_t current_version = mgr.CaptureDataVersion();
  EXPECT_TRUE(mgr.InsertIfVersion(query, result, ngrams, 15.0, current_version));

  auto cached = mgr.Lookup(query);
  ASSERT_TRUE(cached.has_value());
  EXPECT_EQ(result, cached.value());
}

TEST(CacheManagerTest, InsertIfVersionAllowsUnrelatedTableInvalidation) {
  config::CacheConfig config;
  config.enabled = true;
  config.max_memory_bytes = 10 * 1024 * 1024;

  auto ngram_configs = CreateTestNgramConfigs(3, 2);

  CacheManager mgr(config, std::move(ngram_configs));

  auto query = CreateQuery("posts", "golang");
  std::vector<DocId> result = {1, 2, 3};
  std::vector<std::string> ngrams = {"ang", "gol", "lan", "ola"};

  const uint64_t posts_version = mgr.CaptureDataVersion("posts");
  mgr.Invalidate("comments", "", "unrelated comment mutation");

  EXPECT_TRUE(mgr.InsertIfVersion(query, result, ngrams, 15.0, posts_version, 3, 2, true));

  auto cached = mgr.Lookup(query);
  ASSERT_TRUE(cached.has_value());
  EXPECT_EQ(result, cached.value());
}

TEST(CacheManagerTest, EmptyGlobalClearRejectsPreviouslyCapturedTableVersion) {
  config::CacheConfig config;
  config.enabled = true;
  config.max_memory_bytes = 10 * 1024 * 1024;
  CacheManager mgr(config, CreateTestNgramConfigs(3, 2));

  auto query = CreateQuery("posts", "golang");
  const uint64_t before_clear = mgr.CaptureDataVersion("posts");
  mgr.Clear();  // Empty clear must still advance the table-visible generation.

  EXPECT_GT(mgr.CaptureDataVersion("posts"), before_clear);
  EXPECT_FALSE(mgr.InsertIfVersion(query, {1, 2, 3}, {"gol", "ola"}, 15.0, before_clear, 3, 2, true));
}

/**
 * @brief Test precise invalidation - only affected queries invalidated
 */
TEST(CacheManagerTest, PreciseInvalidation) {
  config::CacheConfig config;
  config.enabled = true;
  config.max_memory_bytes = 10 * 1024 * 1024;

  auto ngram_configs = CreateTestNgramConfigs(3, 2);

  CacheManager mgr(config, std::move(ngram_configs));

  // Query 1: "golang"
  auto query1 = CreateQuery("posts", "golang");
  std::vector<DocId> result1 = {1, 2, 3};
  std::vector<std::string> ngrams1 = {"ang", "gol", "lan", "ola"};
  mgr.Insert(query1, result1, ngrams1, 15.0);

  // Query 2: "python"
  auto query2 = CreateQuery("posts", "python");
  std::vector<DocId> result2 = {4, 5, 6};
  std::vector<std::string> ngrams2 = {"hon", "pyt", "tho", "yth"};
  mgr.Insert(query2, result2, ngrams2, 15.0);

  // INSERT document with "golang" - should only invalidate query1
  mgr.Invalidate("posts", "", "golang tutorial");

  // Query1 should be invalidated
  EXPECT_FALSE(mgr.Lookup(query1).has_value());

  // Query2 should still be cached
  EXPECT_TRUE(mgr.Lookup(query2).has_value());
}

TEST(CacheManagerTest, TableInvalidationStrategyClearsOnlyModifiedTable) {
  config::CacheConfig config;
  config.enabled = true;
  config.max_memory_bytes = 10 * 1024 * 1024;
  config.invalidation_strategy = "table";

  auto ngram_configs = CreateTestNgramConfigs(3, 2);

  CacheManager mgr(config, std::move(ngram_configs));

  auto posts_query1 = CreateQuery("posts", "golang");
  std::vector<DocId> posts_result1 = {1, 2, 3};
  std::vector<std::string> posts_ngrams1 = {"ang", "gol", "lan", "ola"};
  EXPECT_TRUE(mgr.Insert(posts_query1, posts_result1, posts_ngrams1, 15.0));

  auto posts_query2 = CreateQuery("posts", "python");
  std::vector<DocId> posts_result2 = {4, 5, 6};
  std::vector<std::string> posts_ngrams2 = {"hon", "pyt", "tho", "yth"};
  EXPECT_TRUE(mgr.Insert(posts_query2, posts_result2, posts_ngrams2, 15.0));

  auto comments_query = CreateQuery("comments", "python");
  std::vector<DocId> comments_result = {7, 8};
  EXPECT_TRUE(mgr.Insert(comments_query, comments_result, posts_ngrams2, 15.0));

  mgr.Invalidate("posts", "", "golang tutorial");

  EXPECT_FALSE(mgr.Lookup(posts_query1).has_value());
  EXPECT_FALSE(mgr.Lookup(posts_query2).has_value());
  EXPECT_TRUE(mgr.Lookup(comments_query).has_value());
}

/**
 * @brief Test UPDATE invalidation - both old and new text considered
 */
TEST(CacheManagerTest, UpdateInvalidation) {
  config::CacheConfig config;
  config.enabled = true;
  config.max_memory_bytes = 10 * 1024 * 1024;

  auto ngram_configs = CreateTestNgramConfigs(3, 2);

  CacheManager mgr(config, std::move(ngram_configs));

  // Query for "rust"
  auto query1 = CreateQuery("posts", "rust");
  std::vector<DocId> result1 = {1, 2};
  std::vector<std::string> ngrams1 = {"rus", "ust"};
  mgr.Insert(query1, result1, ngrams1, 15.0);

  // Query for "golang"
  auto query2 = CreateQuery("posts", "golang");
  std::vector<DocId> result2 = {3, 4};
  std::vector<std::string> ngrams2 = {"ang", "gol", "lan", "ola"};
  mgr.Insert(query2, result2, ngrams2, 15.0);

  // UPDATE: change "rust" to "golang"
  mgr.Invalidate("posts", "rust tutorial", "golang tutorial");

  // Both queries should be invalidated
  EXPECT_FALSE(mgr.Lookup(query1).has_value());
  EXPECT_FALSE(mgr.Lookup(query2).has_value());
}

/**
 * @brief Test DELETE invalidation - only old text considered
 */
TEST(CacheManagerTest, DeleteInvalidation) {
  config::CacheConfig config;
  config.enabled = true;
  config.max_memory_bytes = 10 * 1024 * 1024;

  auto ngram_configs = CreateTestNgramConfigs(3, 2);

  CacheManager mgr(config, std::move(ngram_configs));

  // Query for "docker"
  auto query1 = CreateQuery("posts", "docker");
  std::vector<DocId> result1 = {1, 2};
  std::vector<std::string> ngrams1 = {"cke", "doc", "ker", "ock"};
  mgr.Insert(query1, result1, ngrams1, 15.0);

  // Query for "kubernetes"
  auto query2 = CreateQuery("posts", "kubernetes");
  std::vector<DocId> result2 = {3, 4};
  std::vector<std::string> ngrams2 = {"ber", "ern", "ete", "kub", "net", "rne", "tes", "ube"};
  mgr.Insert(query2, result2, ngrams2, 15.0);

  // DELETE document with "docker"
  mgr.Invalidate("posts", "docker container", "");

  // Only query1 should be invalidated
  EXPECT_FALSE(mgr.Lookup(query1).has_value());
  EXPECT_TRUE(mgr.Lookup(query2).has_value());
}

/**
 * @brief Test table isolation
 */
TEST(CacheManagerTest, TableIsolation) {
  config::CacheConfig config;
  config.enabled = true;
  config.max_memory_bytes = 10 * 1024 * 1024;

  auto ngram_configs = CreateTestNgramConfigs(3, 2);

  CacheManager mgr(config, std::move(ngram_configs));

  // Query for "posts" table
  auto query1 = CreateQuery("posts", "golang");
  std::vector<DocId> result1 = {1, 2};
  std::vector<std::string> ngrams = {"ang", "gol", "lan", "ola"};
  mgr.Insert(query1, result1, ngrams, 15.0);

  // Query for "comments" table with same search text
  auto query2 = CreateQuery("comments", "golang");
  std::vector<DocId> result2 = {3, 4};
  mgr.Insert(query2, result2, ngrams, 15.0);

  // INSERT into "posts" table only
  mgr.Invalidate("posts", "", "golang tips");

  // Only posts query should be invalidated
  EXPECT_FALSE(mgr.Lookup(query1).has_value());
  EXPECT_TRUE(mgr.Lookup(query2).has_value());
}

/**
 * @brief Test ClearTable
 */
TEST(CacheManagerTest, ClearTable) {
  config::CacheConfig config;
  config.enabled = true;
  config.max_memory_bytes = 10 * 1024 * 1024;

  auto ngram_configs = CreateTestNgramConfigs(3, 2);

  CacheManager mgr(config, std::move(ngram_configs));

  std::vector<std::string> ngrams = {"est", "tes"};

  // Insert into multiple tables
  auto query1 = CreateQuery("posts", "test");
  mgr.Insert(query1, {1, 2}, ngrams, 15.0);

  auto query2 = CreateQuery("comments", "test");
  mgr.Insert(query2, {3, 4}, ngrams, 15.0);

  // Clear only "posts" table
  mgr.ClearTable("posts");

  // posts query should be gone
  EXPECT_FALSE(mgr.Lookup(query1).has_value());

  // comments query should remain
  EXPECT_TRUE(mgr.Lookup(query2).has_value());
}

/**
 * @brief Test Clear all
 */
TEST(CacheManagerTest, ClearAll) {
  config::CacheConfig config;
  config.enabled = true;
  config.max_memory_bytes = 10 * 1024 * 1024;

  auto ngram_configs = CreateTestNgramConfigs(3, 2);

  CacheManager mgr(config, std::move(ngram_configs));

  std::vector<std::string> ngrams = {"est", "tes"};

  auto query1 = CreateQuery("posts", "test");
  mgr.Insert(query1, {1, 2}, ngrams, 15.0);

  auto query2 = CreateQuery("comments", "test");
  mgr.Insert(query2, {3, 4}, ngrams, 15.0);

  // Clear all
  mgr.Clear();

  // Both should be gone
  EXPECT_FALSE(mgr.Lookup(query1).has_value());
  EXPECT_FALSE(mgr.Lookup(query2).has_value());
}

/**
 * @brief Test Enable/Disable
 *
 * Disable() now clears the cache to avoid stale entries (any invalidation
 * events delivered while disabled would be silently dropped). Re-enabling
 * therefore produces a cold cache.
 */
TEST(CacheManagerTest, EnableDisable) {
  config::CacheConfig config;
  config.enabled = true;
  config.max_memory_bytes = 10 * 1024 * 1024;

  auto ngram_configs = CreateTestNgramConfigs(3, 2);

  CacheManager mgr(config, std::move(ngram_configs));

  auto query = CreateQuery("posts", "test");
  std::vector<std::string> ngrams = {"est", "tes"};

  // Insert while enabled
  mgr.Insert(query, {1, 2}, ngrams, 15.0);
  EXPECT_TRUE(mgr.Lookup(query).has_value());

  // Disable
  mgr.Disable();
  EXPECT_FALSE(mgr.IsEnabled());

  // Lookup should fail when disabled
  EXPECT_FALSE(mgr.Lookup(query).has_value());

  // Re-enable: cache is cold (Disable cleared it)
  mgr.Enable();
  EXPECT_TRUE(mgr.IsEnabled());

  // Cache was cleared on Disable, should miss
  EXPECT_FALSE(mgr.Lookup(query).has_value());

  // Re-inserting should succeed and be visible
  mgr.Insert(query, {1, 2}, ngrams, 15.0);
  EXPECT_TRUE(mgr.Lookup(query).has_value());
}

TEST(CacheManagerTest, DisableRetainsDiagnosticStatistics) {
  config::CacheConfig config;
  config.enabled = true;
  config.max_memory_bytes = 10 * 1024 * 1024;

  CacheManager mgr(config, CreateTestNgramConfigs(3, 2));
  const auto query = CreateQuery("posts", "test");
  const std::vector<std::string> ngrams = {"est", "tes"};
  ASSERT_TRUE(mgr.Insert(query, {1, 2}, ngrams, 15.0));
  ASSERT_TRUE(mgr.Lookup(query).has_value());
  const auto stats_before_disable = mgr.GetStatistics();
  ASSERT_GT(stats_before_disable.total_queries, 0U);
  ASSERT_GT(stats_before_disable.cache_hits, 0U);

  mgr.Disable();
  const auto stats_after_disable = mgr.GetStatistics();

  EXPECT_EQ(stats_after_disable.current_entries, 0U);
  EXPECT_EQ(stats_after_disable.current_memory_bytes, 0U);
  EXPECT_EQ(stats_after_disable.total_queries, stats_before_disable.total_queries);
  EXPECT_EQ(stats_after_disable.cache_hits, stats_before_disable.cache_hits);
}

/**
 * @brief Disable() clears all entries to avoid serving stale results
 *
 * Regression test for the cache-correctness fix: invalidation events
 * arriving while the cache is disabled would be silently dropped (the
 * InvalidationQueue is stopped). To prevent stale entries from surviving
 * a Disable/Enable cycle, Disable() now drains the queue, clears all
 * entries, then disables.
 */
TEST(CacheManagerTest, DisableClearsCacheToAvoidStaleness) {
  config::CacheConfig config;
  config.enabled = true;
  config.max_memory_bytes = 10 * 1024 * 1024;

  auto ngram_configs = CreateTestNgramConfigs(3, 2);

  CacheManager mgr(config, std::move(ngram_configs));

  std::vector<std::string> ngrams = {"est", "tes"};

  // Insert 5 entries
  std::vector<query::Query> queries;
  for (int i = 0; i < 5; ++i) {
    auto query = CreateQuery("posts", "test_" + std::to_string(i));
    ASSERT_TRUE(mgr.Insert(query, {static_cast<DocId>(i)}, ngrams, 15.0));
    queries.push_back(query);
  }

  // Verify all are present
  for (const auto& q : queries) {
    EXPECT_TRUE(mgr.Lookup(q).has_value());
  }

  // Disable
  mgr.Disable();
  EXPECT_FALSE(mgr.IsEnabled());

  // Re-enable
  mgr.Enable();
  EXPECT_TRUE(mgr.IsEnabled());

  // All 5 lookups must miss — Disable() must have cleared the cache
  for (size_t i = 0; i < queries.size(); ++i) {
    EXPECT_FALSE(mgr.Lookup(queries[i]).has_value()) << "Entry " << i << " should have been cleared by Disable()";
  }
}

/**
 * @brief Test query normalization - equivalent queries should hit cache
 * NOTE: Removed due to implementation complexity. Query normalization
 * is tested indirectly through other tests and is handled by the
 * query normalizer component.
 */

/**
 * @brief Test statistics
 */
TEST(CacheManagerTest, Statistics) {
  config::CacheConfig config;
  config.enabled = true;
  config.max_memory_bytes = 10 * 1024 * 1024;

  auto ngram_configs = CreateTestNgramConfigs(3, 2);

  CacheManager mgr(config, std::move(ngram_configs));

  auto query = CreateQuery("posts", "test");
  std::vector<std::string> ngrams = {"est", "tes"};

  // Insert
  mgr.Insert(query, {1, 2, 3}, ngrams, 15.0);

  // Hit
  mgr.Lookup(query);

  // Miss
  auto query2 = CreateQuery("posts", "other");
  mgr.Lookup(query2);

  auto stats = mgr.GetStatistics();

  EXPECT_EQ(2, stats.total_queries);
  EXPECT_EQ(1, stats.cache_hits);
  EXPECT_EQ(1, stats.cache_misses);
}

/**
 * @brief Test min_query_cost_ms threshold
 */
TEST(CacheManagerTest, MinQueryCostThreshold) {
  config::CacheConfig config;
  config.enabled = true;
  config.max_memory_bytes = 10 * 1024 * 1024;
  config.min_query_cost_ms = 20.0;  // Only cache queries > 20ms

  auto ngram_configs = CreateTestNgramConfigs(3, 2);

  CacheManager mgr(config, std::move(ngram_configs));

  auto query = CreateQuery("posts", "test");
  std::vector<std::string> ngrams = {"est", "tes"};

  // Try to insert query with cost < threshold (should fail)
  EXPECT_FALSE(mgr.Insert(query, {1, 2, 3}, ngrams, 10.0));

  // Insert query with cost >= threshold (should succeed)
  EXPECT_TRUE(mgr.Insert(query, {1, 2, 3}, ngrams, 25.0));
}

/**
 * @brief Test enabling cache when started with cache disabled
 */
TEST(CacheManagerTest, EnableWhenDisabledAtStartup) {
  config::CacheConfig config;
  config.enabled = false;  // Start with cache disabled
  config.max_memory_bytes = 10 * 1024 * 1024;

  auto ngram_configs = CreateTestNgramConfigs(3, 2);

  CacheManager mgr(config, std::move(ngram_configs));

  // Initially disabled
  EXPECT_FALSE(mgr.IsEnabled());

  // Enable should initialize runtime use even when startup config was disabled.
  EXPECT_TRUE(mgr.Enable());

  EXPECT_TRUE(mgr.IsEnabled());

  auto query = CreateQuery("posts", "test");
  std::vector<std::string> ngrams = {"est", "tes"};
  EXPECT_TRUE(mgr.Insert(query, {1, 2, 3}, ngrams, 25.0));

  auto result = mgr.Lookup(query);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value(), std::vector<DocId>({1, 2, 3}));
}

/**
 * @brief Test per-table ngram settings for cache invalidation
 *
 * Regression test for: テーブルごとに異なる n-gram 設定を使っていても
 * キャッシュ無効化は常に最初のテーブルのサイズで計算される
 */
TEST(CacheManagerTest, PerTableNgramSettings) {
  config::CacheConfig config;
  config.enabled = true;
  config.max_memory_bytes = 10 * 1024 * 1024;

  // Create two tables with DIFFERENT ngram settings
  NgramConfigMap ngram_configs;
  ngram_configs["posts"] = NgramConfig{.ngram_size = 3, .kanji_ngram_size = 2, .cross_boundary_ngrams = true};
  ngram_configs["comments"] = NgramConfig{.ngram_size = 2, .kanji_ngram_size = 1, .cross_boundary_ngrams = true};

  CacheManager mgr(config, std::move(ngram_configs));

  // Cache query for "posts" table (ngram_size=3)
  auto query1 = CreateQuery("posts", "test");
  std::vector<DocId> result1 = {1, 2, 3};
  // With ngram_size=3, "test" generates: "tes", "est"
  std::vector<std::string> ngrams1 = {"est", "tes"};
  mgr.Insert(query1, result1, ngrams1, 15.0);

  // Cache query for "comments" table (ngram_size=2)
  auto query2 = CreateQuery("comments", "test");
  std::vector<DocId> result2 = {4, 5, 6};
  // With ngram_size=2, "test" generates: "te", "es", "st"
  std::vector<std::string> ngrams2 = {"es", "st", "te"};
  mgr.Insert(query2, result2, ngrams2, 15.0);

  // Verify both queries are cached
  EXPECT_TRUE(mgr.Lookup(query1).has_value());
  EXPECT_TRUE(mgr.Lookup(query2).has_value());

  // Invalidate "posts" table with "test" using POSTS' ngram settings (size=3)
  // This should generate ngrams: "tes", "est" and invalidate query1
  mgr.Invalidate("posts", "", "testing");

  // Give time for async invalidation
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // Query1 (posts) should be invalidated
  EXPECT_FALSE(mgr.Lookup(query1).has_value());

  // Query2 (comments) should STILL be cached (different table)
  EXPECT_TRUE(mgr.Lookup(query2).has_value());

  // Now invalidate "comments" table with "test" using COMMENTS' ngram settings (size=2)
  // This should generate ngrams: "te", "es", "st" and invalidate query2
  mgr.Invalidate("comments", "", "test");

  // Give time for async invalidation
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // Query2 (comments) should NOW be invalidated
  EXPECT_FALSE(mgr.Lookup(query2).has_value());
}

/**
 * @brief Test that LRU eviction cleans up invalidation metadata
 *
 * This is a regression test for a bug where LRU eviction removed entries
 * from the cache but did not unregister them from the InvalidationManager,
 * causing metadata to leak.
 */
TEST(CacheManagerTest, LRUEvictionCleansUpMetadata) {
  config::CacheConfig config;
  config.enabled = true;
  config.max_memory_bytes = 10 * 1024;  // 10KB to trigger evictions

  auto ngram_configs = CreateTestNgramConfigs(3, 2);

  CacheManager cache_mgr(config, std::move(ngram_configs));

  // Insert 50 entries to trigger evictions (each ~1KB, 10KB cache can fit ~10)
  constexpr int kNumEntries = 50;
  std::vector<query::Query> queries;

  for (int i = 0; i < kNumEntries; ++i) {
    auto query = CreateQuery("posts", "test query " + std::to_string(i));

    std::vector<DocId> result;
    for (int j = 0; j < 200; ++j) {
      result.push_back(static_cast<DocId>(i * 1000 + j));
    }

    std::vector<std::string> ngrams = {"est", "tes", "test"};
    cache_mgr.Insert(query, result, ngrams, 10.0);
    queries.push_back(query);
  }

  // Get statistics - many entries should have been evicted due to memory limit
  auto stats = cache_mgr.GetStatistics();
  EXPECT_GT(stats.evictions, 0) << "LRU eviction should have occurred (10KB cache, inserted " << kNumEntries
                                << " entries)";
  EXPECT_LT(stats.current_entries, kNumEntries) << "Not all entries should fit in cache";

  // CRITICAL: Verify that invalidation metadata was cleaned up during eviction
  // We can't directly access InvalidationManager from here, but we can
  // verify that the cache is still functioning correctly after evictions

  // Trigger more evictions by inserting more entries
  for (int i = kNumEntries; i < kNumEntries + 50; ++i) {
    auto query = CreateQuery("posts", "test query " + std::to_string(i));

    std::vector<DocId> result;
    for (int j = 0; j < 200; ++j) {
      result.push_back(static_cast<DocId>(i * 1000 + j));
    }

    std::vector<std::string> ngrams = {"est", "tes", "test"};
    cache_mgr.Insert(query, result, ngrams, 10.0);
  }

  // Verify cache is still functional
  auto final_stats = cache_mgr.GetStatistics();
  EXPECT_GT(final_stats.evictions, stats.evictions) << "More evictions should have occurred";

  // Test invalidation still works correctly (evicted entries don't interfere)
  auto new_query = CreateQuery("posts", "latest query");

  std::vector<DocId> new_result = {999};
  std::vector<std::string> new_ngrams = {"ate", "lat", "test"};

  ASSERT_TRUE(cache_mgr.Insert(new_query, new_result, new_ngrams, 10.0));

  // Lookup should work
  auto cached = cache_mgr.Lookup(new_query);
  ASSERT_TRUE(cached.has_value());
  EXPECT_EQ(new_result, cached.value());

  // Invalidate should work
  cache_mgr.Invalidate("posts", "", "latest query update");
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // Verify invalidation worked
  auto after_invalidation = cache_mgr.Lookup(new_query);
  EXPECT_FALSE(after_invalidation.has_value());
}

/**
 * @brief Test CacheManager destructor safety with short TTL entries
 *
 * Regression test for P0 use-after-free: QueryCache's LRU background thread
 * could fire eviction callbacks referencing already-destroyed InvalidationManager
 * during CacheManager destruction. The fix explicitly destroys QueryCache before
 * InvalidationManager, letting QueryCache join its LRU worker before the
 * callback target disappears.
 */
TEST(CacheManagerTest, DestructorSafeWithShortTTLEntries) {
  config::CacheConfig config;
  config.enabled = true;
  config.max_memory_bytes = 10 * 1024 * 1024;
  config.ttl_seconds = 1;  // Short TTL to increase eviction probability

  auto ngram_configs = CreateTestNgramConfigs(3, 2);

  {
    CacheManager mgr(config, std::move(ngram_configs));

    // Insert many entries with short TTL
    for (int i = 0; i < 100; ++i) {
      auto query = CreateQuery("posts", "destructor test query " + std::to_string(i));
      std::vector<DocId> result;
      for (int j = 0; j < 50; ++j) {
        result.push_back(static_cast<DocId>(i * 100 + j));
      }
      std::vector<std::string> ngrams = {"des", "est", "str", "tru"};
      mgr.Insert(query, result, ngrams, 15.0);
    }

    // Wait for some entries to expire via TTL
    std::this_thread::sleep_for(std::chrono::milliseconds(1200));

    // CacheManager destructor runs here - should not crash with use-after-free
  }

  // If we reach here without crash/ASAN/TSAN error, the fix works
  SUCCEED();
}

/**
 * @brief Regression test for P0-B: Clear/Insert race must not leave
 *        phantom invalidation metadata.
 *
 * Bug: CacheManager::Clear() called query_cache_->Clear() then
 * invalidation_mgr_->Clear() as two independent locked sections. A concurrent
 * Insert() between them registered a key in InvalidationManager pointing to
 * a now-empty QueryCache, leaving phantom metadata.
 *
 * Fix: serialize_mutex_ protects the combined Insert / Clear / ClearTable.
 *
 * Invariant verified: after concurrent Insert + Clear traffic, the
 * InvalidationManager's tracked entry count must equal the QueryCache's
 * entry count. (Both counts may be 0 or any equal positive number depending
 * on which thread won the last race.)
 */
TEST(CacheManagerTest, ClearDoesNotLeavePhantomInvalidationMetadata) {
  config::CacheConfig config;
  config.enabled = true;
  config.max_memory_bytes = 10 * 1024 * 1024;
  config.min_query_cost_ms = 0.0;  // Cache everything

  auto ngram_configs = CreateTestNgramConfigs(3, 2);

  CacheManager mgr(config, std::move(ngram_configs));

  std::atomic<bool> stop{false};
  std::vector<std::thread> threads;

  // 8 inserter threads
  constexpr int kInserterThreads = 8;
  threads.reserve(kInserterThreads + 1);
  for (int t = 0; t < kInserterThreads; ++t) {
    threads.emplace_back([&mgr, &stop, t]() {
      int counter = 0;
      while (!stop.load()) {
        auto query = CreateQuery("posts", "phantom_" + std::to_string(t) + "_" + std::to_string(counter));
        std::vector<DocId> result = {static_cast<DocId>(counter)};
        std::vector<std::string> ngrams = {"pha", "han", "ant"};
        mgr.Insert(query, result, ngrams, 15.0);
        ++counter;
      }
    });
  }

  // 1 clearer thread
  threads.emplace_back([&mgr, &stop]() {
    while (!stop.load()) {
      mgr.Clear();
      std::this_thread::yield();
    }
  });

  // Run for 200ms
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  stop = true;

  for (auto& thread : threads) {
    thread.join();
  }

  // Final state: invalidation metadata count must match QueryCache entry count
  auto stats = mgr.GetStatistics();
  size_t invalidation_entries = mgr.GetTrackedInvalidationEntries();

  EXPECT_EQ(invalidation_entries, stats.current_entries)
      << "Phantom metadata detected: InvalidationManager has " << invalidation_entries << " entries but QueryCache has "
      << stats.current_entries << " entries";
}

TEST(CacheManagerTest, ClearWaitsForAtomicInsertRegistrationBoundary) {
  constexpr int kRaceRepetitions = 100;
  for (int repetition = 0; repetition < kRaceRepetitions; ++repetition) {
    config::CacheConfig config;
    config.enabled = true;
    config.max_memory_bytes = 1024 * 1024;
    config.min_query_cost_ms = 0.0;
    CacheManager mgr(config, CreateTestNgramConfigs(3, 2));
    mygramdb::testing::DeterministicGate insert_gate;
    mgr.SetAfterQueryCacheInsertHookForTest([&]() { insert_gate.ArriveAndWait(); });

    const auto query = CreateQuery("posts", "deterministic_clear_insert_" + std::to_string(repetition));
    auto insert = std::async(std::launch::async, [&]() { return mgr.Insert(query, {1}, {"det"}, 1.0); });
    ASSERT_TRUE(insert_gate.WaitUntilArrived(std::chrono::seconds(2))) << repetition;

    auto clear = std::async(std::launch::async, [&]() { mgr.Clear(); });
    EXPECT_EQ(clear.wait_for(std::chrono::milliseconds(1)), std::future_status::timeout) << repetition;
    insert_gate.Release();
    ASSERT_TRUE(insert.get()) << repetition;
    ASSERT_EQ(clear.wait_for(std::chrono::seconds(2)), std::future_status::ready) << repetition;
    clear.get();

    EXPECT_FALSE(mgr.Lookup(query).has_value()) << repetition;
    EXPECT_EQ(mgr.GetStatistics().current_entries, 0U) << repetition;
    EXPECT_EQ(mgr.GetTrackedInvalidationEntries(), 0U) << repetition;
  }
}

/**
 * @brief Test CacheManager destructor safety with small cache triggering evictions
 *
 * Regression test for Exercises LRU eviction during destruction by using
 * a very small cache that has active evictions happening.
 */
TEST(CacheManagerTest, DestructorSafeWithActiveEvictions) {
  config::CacheConfig config;
  config.enabled = true;
  config.max_memory_bytes = 5 * 1024;  // Very small cache to trigger evictions
  config.ttl_seconds = 1;

  auto ngram_configs = CreateTestNgramConfigs(3, 2);

  {
    CacheManager mgr(config, std::move(ngram_configs));

    // Flood the cache to ensure constant evictions
    for (int i = 0; i < 200; ++i) {
      auto query = CreateQuery("posts", "eviction test " + std::to_string(i));
      std::vector<DocId> result;
      for (int j = 0; j < 100; ++j) {
        result.push_back(static_cast<DocId>(i * 1000 + j));
      }
      std::vector<std::string> ngrams = {"evi", "ict", "vic"};
      mgr.Insert(query, result, ngrams, 10.0);
    }

    // Destructor runs here with eviction callbacks potentially still active
  }

  SUCCEED();
}

TEST(CacheManagerTest, RejectsNonCanonicalPrecomputedCacheKey) {
  config::CacheConfig config;
  config.enabled = true;
  config.max_memory_bytes = 10 * 1024 * 1024;

  auto ngram_configs = CreateTestNgramConfigs(3, 2);

  CacheManager mgr(config, std::move(ngram_configs));

  auto canonical_query = CreateQuery("posts", "test");
  auto query = canonical_query;
  query.cache_key = std::make_pair(uint64_t{12345}, uint64_t{67890});
  query.cache_key_discriminator = "parser-only-key";
  query.cache_key_is_canonical = false;

  std::vector<DocId> results = {1, 2, 3};
  std::vector<std::string> ngrams = {"tes", "est"};
  EXPECT_FALSE(mgr.Insert(query, results, ngrams, 15.0, 3, 2, true));
  EXPECT_FALSE(mgr.Lookup(query).has_value());

  ASSERT_TRUE(mgr.Insert(canonical_query, results, ngrams, 15.0, 3, 2, true));
  ASSERT_TRUE(mgr.Lookup(canonical_query).has_value());
  EXPECT_EQ(*mgr.Lookup(canonical_query), results);
}

TEST(CacheManagerTest, ExactCaseTableIdentitiesDoNotShareEntries) {
  config::CacheConfig config;
  config.enabled = true;
  config.max_memory_bytes = 10 * 1024 * 1024;
  config.min_query_cost_ms = 0.0;

  NgramConfigMap ngram_configs;
  ngram_configs["Db.Users"] = NgramConfig{.ngram_size = 3, .kanji_ngram_size = 2, .cross_boundary_ngrams = true};
  ngram_configs["db.users"] = NgramConfig{.ngram_size = 3, .kanji_ngram_size = 2, .cross_boundary_ngrams = true};
  CacheManager mgr(config, std::move(ngram_configs));

  auto upper = CreateQuery("Db.Users", "alice");
  auto lower = CreateQuery("db.users", "alice");
  ASSERT_TRUE(mgr.Insert(upper, {1, 2}, {"ali", "lic", "ice"}, 1.0));

  EXPECT_FALSE(mgr.Lookup(lower).has_value());
  ASSERT_TRUE(mgr.Lookup(upper).has_value());
  EXPECT_EQ(*mgr.Lookup(upper), (std::vector<DocId>{1, 2}));
}

// =============================================================================
// CR-7 regression: Disable race
// =============================================================================

/**
 * @brief CR-7 regression: a concurrent Insert during Disable() must not leave
 *        entries in the cache after Disable() returns.
 *
 * Before the fix, Disable() ran:
 *   Stop()  -> Clear()  -> enabled_ = false
 *
 * That ordering left a window where Clear() released serialize_mutex_ but
 * enabled_ was still true; an Insert that arrived between Clear's release
 * and the enabled_ flip could re-populate the cache. After the fix, the
 * order is:
 *
 *   Stop()  -> enabled_ = false  -> Clear()
 *
 * so any Insert observing enabled_ == false short-circuits and can't
 * resurrect entries.
 *
 * This test fires concurrent Inserts during Disable() and asserts the cache
 * contains zero entries afterwards.
 */
TEST(CacheManagerTest, DisableConcurrentInsertNoResidualEntries) {
  config::CacheConfig config;
  config.enabled = true;
  config.max_memory_bytes = 16 * 1024 * 1024;

  auto ngram_configs = CreateTestNgramConfigs(3, 2);
  CacheManager mgr(config, std::move(ngram_configs));

  // Pre-populate the cache so Disable() has work to do.
  std::vector<query::Query> queries;
  for (int i = 0; i < 20; ++i) {
    auto query = CreateQuery("posts", "warm_" + std::to_string(i));
    std::vector<DocId> result = {static_cast<DocId>(i)};
    std::vector<std::string> ngrams = {"war", "arm"};
    ASSERT_TRUE(mgr.Insert(query, result, ngrams, 15.0));
    queries.push_back(query);
  }

  std::atomic<bool> stop_inserts{false};
  std::atomic<int> insert_attempts{0};
  std::atomic<int> insert_successes{0};

  // Insert thread: continually try to insert NEW queries.
  std::thread inserter([&]() {
    int counter = 0;
    while (!stop_inserts.load(std::memory_order_acquire)) {
      auto query = CreateQuery("posts", "race_" + std::to_string(counter));
      std::vector<DocId> result = {static_cast<DocId>(counter)};
      std::vector<std::string> ngrams = {"rac", "ace"};
      const bool ok = mgr.Insert(query, result, ngrams, 15.0);
      insert_attempts.fetch_add(1, std::memory_order_relaxed);
      if (ok) {
        insert_successes.fetch_add(1, std::memory_order_relaxed);
      }
      ++counter;
    }
  });

  // Let the inserter thread get going.
  std::this_thread::sleep_for(std::chrono::milliseconds(20));

  // Disable while inserts are racing.
  mgr.Disable();
  EXPECT_FALSE(mgr.IsEnabled());

  // Stop the inserter and join.
  stop_inserts.store(true, std::memory_order_release);
  inserter.join();

  // Sanity: at least some inserts ran (otherwise the test isn't exercising
  // the race).
  EXPECT_GT(insert_attempts.load(), 0);

  // CR-7 invariant: after Disable() returns, the cache must be empty.
  // No prior Insert may persist beyond Disable's Clear, regardless of the
  // race timing.
  const auto stats = mgr.GetStatistics();
  EXPECT_EQ(stats.current_entries, 0U) << "Disable() left entries behind after concurrent Insert (CR-7)";

  // Every pre-populated query must miss.
  for (const auto& q : queries) {
    EXPECT_FALSE(mgr.Lookup(q).has_value());
  }

  // Tracked invalidation entries must be zero (no phantom metadata).
  EXPECT_EQ(mgr.GetTrackedInvalidationEntries(), 0U)
      << "Disable() left InvalidationManager metadata behind (CR-7 phantom)";
}

// =============================================================================
// H-C2 regression: Enable order — Invalidate after Enable goes through queue
// =============================================================================

/**
 * @brief H-C2 regression: an Invalidate() called immediately after Enable()
 *        must not be silently dropped.
 *
 * Before the fix, Enable() set enabled_ = true BEFORE starting the queue.
 * In combination with H-M5 (stopped_ not reset on Start()), an Invalidate()
 * arriving in that window would observe running_ == false, fall into the
 * synchronous Enqueue path — which checked stopped_ first and dropped the
 * call. After the fix, Enable() starts the queue first (which resets
 * stopped_) and only then flips enabled_ = true.
 *
 * Combined coverage of H-C2 and H-M5 via CacheManager: Disable -> Enable ->
 * Invalidate must work end-to-end.
 */
TEST(CacheManagerTest, EnableThenImmediateInvalidateDelivers) {
  config::CacheConfig config;
  config.enabled = true;
  config.max_memory_bytes = 4 * 1024 * 1024;

  auto ngram_configs = CreateTestNgramConfigs(3, 2);
  CacheManager mgr(config, std::move(ngram_configs));

  // Insert, Disable, Enable, then immediately Insert + Invalidate. The
  // Invalidate must reach the entry and erase it.
  auto query = CreateQuery("posts", "golang");
  std::vector<DocId> result = {1, 2, 3};
  std::vector<std::string> ngrams = {"gol", "ola", "lan", "ang"};

  // Cycle: Disable then Enable.
  mgr.Disable();
  ASSERT_FALSE(mgr.IsEnabled());
  ASSERT_TRUE(mgr.Enable());
  ASSERT_TRUE(mgr.IsEnabled());

  // Insert AFTER the Disable/Enable cycle.
  ASSERT_TRUE(mgr.Insert(query, result, ngrams, 15.0));
  ASSERT_TRUE(mgr.Lookup(query).has_value());

  // Immediately invalidate. Under H-M5/H-C2 bugs, this would have been
  // silently dropped (stopped_ stuck at true OR Enqueue's running_ check
  // failing because Start hadn't run yet).
  mgr.Invalidate("posts", "", "golang tutorial");

  // Wait briefly for the worker to process the invalidation.
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  EXPECT_FALSE(mgr.Lookup(query).has_value()) << "Invalidate after Disable/Enable cycle was dropped (H-C2 + H-M5)";
}

/**
 * @brief Combined H-C2 + H-M5 regression: many Disable/Enable cycles each
 *        leave Invalidate() functional.
 */
TEST(CacheManagerTest, DisableEnableCycleInvalidationStillWorks) {
  config::CacheConfig config;
  config.enabled = true;
  config.max_memory_bytes = 4 * 1024 * 1024;

  auto ngram_configs = CreateTestNgramConfigs(3, 2);
  CacheManager mgr(config, std::move(ngram_configs));

  for (int cycle = 0; cycle < 3; ++cycle) {
    mgr.Disable();
    ASSERT_TRUE(mgr.Enable());

    auto query = CreateQuery("posts", "cycle_" + std::to_string(cycle));
    std::vector<DocId> result = {static_cast<DocId>(cycle)};
    std::vector<std::string> ngrams = {"cyc", "ycl"};
    ASSERT_TRUE(mgr.Insert(query, result, ngrams, 15.0));
    ASSERT_TRUE(mgr.Lookup(query).has_value());

    mgr.Invalidate("posts", "", "cycle invalidation text " + std::to_string(cycle));

    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    EXPECT_FALSE(mgr.Lookup(query).has_value()) << "cycle " << cycle << " Invalidate dropped";
  }
}

TEST(CacheManagerTest, SharedMemoryBudgetIncludesInvalidationAndContainerOverhead) {
  const auto rss_before = CurrentResidentBytes();
  config::CacheConfig config;
  config.enabled = true;
  config.max_memory_bytes = 64 * 1024;
  config.min_query_cost_ms = 0.0;
  CacheManager mgr(config, CreateTestNgramConfigs(3, 2));

  for (int i = 0; i < 500; ++i) {
    auto query = CreateQuery("posts", "memory_budget_" + std::to_string(i));
    std::vector<std::string> ngrams;
    ngrams.reserve(64);
    for (int n = 0; n < 64; ++n) {
      ngrams.push_back("ngram_" + std::to_string(i) + "_" + std::to_string(n));
    }
    ASSERT_TRUE(mgr.Insert(query, {static_cast<DocId>(i)}, ngrams, 1.0, 3, 2, true));
    const auto stats = mgr.GetStatistics();
    EXPECT_LE(stats.accounted_memory_bytes, stats.max_memory_bytes)
        << "shared cache budget exceeded after insertion " << i;
  }

  const auto final_stats = mgr.GetStatistics();
  EXPECT_GT(final_stats.evictions, 0U);
  EXPECT_LT(final_stats.current_entries, 500U);
  const auto rss_after = CurrentResidentBytes();
  // ASan's allocator quarantine and shadow-memory commits dominate process
  // RSS at this scale. The shared accounted budget remains asserted above;
  // compare physical RSS only in a production-style allocator build.
  if (!IsAddressSanitizerBuild() && rss_before.has_value() && rss_after.has_value()) {
    constexpr size_t kAllocatorAndMeasurementTolerance = 8 * 1024 * 1024;
    const size_t rss_growth = *rss_after > *rss_before ? *rss_after - *rss_before : 0;
    EXPECT_LE(rss_growth, config.max_memory_bytes + kAllocatorAndMeasurementTolerance)
        << "cache RSS growth exceeded the shared budget plus allocator tolerance";
  }
}

}  // namespace mygramdb::cache
