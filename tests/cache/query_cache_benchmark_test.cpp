/**
 * @file query_cache_benchmark_test.cpp
 * @brief Cost of the cache maintenance and invalidation sweeps, by cache size.
 *
 * Invalidation runs once per row event and periodic maintenance runs on a fixed
 * tick, so both are paid whether or not the cache is being read. Their cost has
 * to be governed by how much actually matches, not by how much is cached. These
 * cases measure the same operation against caches an order of magnitude apart
 * and report the per-operation figures.
 */

#include <gtest/gtest.h>

#include <chrono>
#include <iomanip>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "cache/cache_key.h"
#include "cache/invalidation_manager.h"
#include "cache/query_cache.h"

namespace mygramdb::cache {
namespace {

template <typename Body>
double TimeMs(Body&& body) {
  const auto start = std::chrono::steady_clock::now();
  body();
  const auto end = std::chrono::steady_clock::now();
  return std::chrono::duration<double, std::milli>(end - start).count();
}

constexpr const char* kTable = "posts";

/// Ngrams unique to @p index, so no two entries share a reverse-index bucket.
std::vector<std::string> NgramsFor(size_t index) {
  const std::string tag = std::to_string(index);
  return {"q" + tag, "r" + tag, "s" + tag};
}

/**
 * @brief Register @p total entries, of which @p sensitive carry the given flag.
 *
 * The number of entries that a sweep must actually touch is held constant while
 * the cache around them grows, which is what separates "cost of the matches"
 * from "cost of the cache".
 */
void RegisterEntries(InvalidationManager& manager, size_t total, size_t sensitive, bool with_filters,
                     bool text_sensitive) {
  for (size_t i = 0; i < total; ++i) {
    CacheMetadata metadata;
    metadata.table = kTable;
    metadata.ngrams = NgramsFor(i);
    metadata.ngram_size = 3;
    metadata.kanji_ngram_size = 2;
    if (i < sensitive) {
      if (with_filters) {
        metadata.filters.push_back(query::FilterCondition{"status", query::FilterOp::EQ, "1"});
      }
      metadata.invalidate_on_any_text_change = text_sensitive;
    }
    manager.RegisterCacheEntry(CacheKeyGenerator::Generate("query" + std::to_string(i)), metadata);
  }
}

class QueryCacheBenchmarkTest : public ::testing::Test {};

/**
 * @brief A filter-column change costs what it matches, not what is cached.
 *
 * Any row whose filter columns change invalidates every cached query on that
 * table that used a filter. Deciding which those are by walking the table's
 * cache keys makes one row update cost a pass over the table's whole cache
 * footprint, on the replication thread.
 */
TEST_F(QueryCacheBenchmarkTest, FilterChangeSweepCostIsFlatAcrossCacheSize) {
  constexpr size_t kSweeps = 2000;
  constexpr size_t kMatching = 16;

  const auto per_sweep_us = [](size_t entries) {
    QueryCache cache(256UL * 1024 * 1024, 0.0, 0, false, 1, false);
    InvalidationManager manager(&cache);
    RegisterEntries(manager, entries, kMatching, /*with_filters=*/true, /*text_sensitive=*/false);

    size_t affected = 0;
    const double ms = TimeMs([&] {
      for (size_t i = 0; i < kSweeps; ++i) {
        // Identical old and new text: no ngram changed and no text-sensitive
        // entry qualifies, so only the filter sweep does any work.
        affected += manager.InvalidateAffectedEntries(kTable, "unchanged", "unchanged", 3, 2, true, true).size();
      }
    });
    EXPECT_EQ(affected, kSweeps * kMatching) << "the sweep matched a different set at " << entries << " entries";
    return (ms * 1000.0) / static_cast<double>(kSweeps);
  };

  const double small_us = per_sweep_us(10000);
  const double large_us = per_sweep_us(100000);

  std::cout << "\nFilter-change sweep: " << kMatching << " filtered entries per cache\n";
  std::cout << "  " << std::left << std::setw(38) << "per sweep us (10k -> 100k entries)" << std::right << std::fixed
            << std::setprecision(3) << std::setw(11) << small_us << "  ->" << std::setw(11) << large_us << std::endl;

  EXPECT_LT(large_us, small_us * 3.0)
      << "sweep cost grew with the cache, so the filtered entries are being found by walking the table";
}

/**
 * @brief A text change costs what it matches, not what is cached.
 *
 * Queries whose result set can move on any text change — NOT terms and the
 * text-sensitive fallbacks — cannot be narrowed by ngram, so they are swept on
 * every row event. That makes their sweep the one most exposed to cache size.
 */
TEST_F(QueryCacheBenchmarkTest, TextSensitiveSweepCostIsFlatAcrossCacheSize) {
  constexpr size_t kSweeps = 2000;
  constexpr size_t kMatching = 16;

  const auto per_sweep_us = [](size_t entries) {
    QueryCache cache(256UL * 1024 * 1024, 0.0, 0, false, 1, false);
    InvalidationManager manager(&cache);
    RegisterEntries(manager, entries, kMatching, /*with_filters=*/false, /*text_sensitive=*/true);

    size_t affected = 0;
    const double ms = TimeMs([&] {
      for (size_t i = 0; i < kSweeps; ++i) {
        // The changed text shares no ngram with any registered entry, so every
        // match below comes from the text-sensitive sweep alone.
        affected += manager.InvalidateAffectedEntries(kTable, "aaaa", "bbbb", 3, 2, true, false).size();
      }
    });
    EXPECT_EQ(affected, kSweeps * kMatching) << "the sweep matched a different set at " << entries << " entries";
    return (ms * 1000.0) / static_cast<double>(kSweeps);
  };

  const double small_us = per_sweep_us(10000);
  const double large_us = per_sweep_us(100000);

  std::cout << "\nText-change sweep: " << kMatching << " text-sensitive entries per cache\n";
  std::cout << "  " << std::left << std::setw(38) << "per sweep us (10k -> 100k entries)" << std::right << std::fixed
            << std::setprecision(3) << std::setw(11) << small_us << "  ->" << std::setw(11) << large_us << std::endl;

  EXPECT_LT(large_us, small_us * 3.0)
      << "sweep cost grew with the cache, so text-sensitive entries are being found by walking the table";
}

/**
 * @brief One maintenance tick holds the exclusive lock for a slice, not the cache.
 *
 * Periodic maintenance takes the write lock, so every query on the server waits
 * behind it. Walking the whole cache on each tick makes that wait grow with the
 * cache; walking a bounded slice and resuming from where it stopped keeps the
 * per-tick wait to a fraction of a full pass.
 */
TEST_F(QueryCacheBenchmarkTest, OneMaintenanceTickCostsAFractionOfAFullPass) {
  constexpr size_t kEntries = 200000;

  auto cache = std::make_unique<QueryCache>(1024UL * 1024 * 1024, 0.0, 0, false, 1, false);
  const std::vector<DocId> result{1, 2, 3, 4};
  for (size_t i = 0; i < kEntries; ++i) {
    CacheMetadata metadata;
    metadata.table = kTable;
    metadata.ngrams = NgramsFor(i);
    metadata.ngram_size = 3;
    metadata.kanji_ngram_size = 2;
    const auto key = CacheKeyGenerator::Generate("query" + std::to_string(i));
    metadata.key = key;
    ASSERT_TRUE(cache->Insert(key, result, metadata, 1.0)) << "entry " << i << " was not cached";
  }
  ASSERT_EQ(cache->GetStatistics().current_entries, kEntries);

  const size_t slice = QueryCache::RefreshSliceSizeForTesting(kEntries);
  const size_t ticks_per_pass = (kEntries + slice - 1) / slice;

  double worst_tick_ms = 0.0;
  const double full_pass_ms = TimeMs([&] {
    for (size_t i = 0; i < ticks_per_pass; ++i) {
      worst_tick_ms = std::max(worst_tick_ms, TimeMs([&] { cache->RefreshLRUForTesting(); }));
    }
  });

  std::cout << "\nPeriodic maintenance: " << kEntries << " entries, slice " << slice << " (" << ticks_per_pass
            << " ticks per pass)\n";
  std::cout << "  " << std::left << std::setw(38) << "ms (full pass -> worst single tick)" << std::right << std::fixed
            << std::setprecision(3) << std::setw(11) << full_pass_ms << "  ->" << std::setw(11) << worst_tick_ms
            << std::endl;

  EXPECT_EQ(cache->GetStatistics().current_entries, kEntries)
      << "maintenance dropped entries that were neither expired nor over budget";
  // A slice is an eighth of the cache, so a tick that costs anything close to a
  // full pass means the scan is not being bounded.
  EXPECT_LT(worst_tick_ms * 2.0, full_pass_ms) << "one maintenance tick cost about as much as a full pass";
}

}  // namespace
}  // namespace mygramdb::cache
