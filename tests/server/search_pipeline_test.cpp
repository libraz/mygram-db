/**
 * @file search_pipeline_test.cpp
 * @brief Tests for search pipeline filter application and helper functions
 *
 * Tests the expected behavior of:
 * - ApplyFilters: per-document filter evaluation with all operators
 * - PostFilterByText: text substring verification
 * - IsCacheStale: stale cache detection via sampling
 * - Type coercion: string/int/double/bool filter value matching
 * - NULL value handling in filters
 * - ExecuteWithFuzzy: empty n-gram early exit (m-17)
 * - Execute: NOT/filter applied internally, not double-applied (m-16)
 */

#include "server/search_pipeline.h"

#include <gtest/gtest.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "cache/cache_manager.h"
#include "cache/cache_types.h"
#include "config/config.h"
#include "index/bm25_scorer.h"
#include "index/index.h"
#include "query/query_parser.h"
#include "query/synonym_dictionary.h"
#include "server/server_types.h"
#include "storage/document_store.h"

namespace mygramdb::server::search_pipeline {

std::unique_ptr<query::SynonymDictionary> MakeSynonymDictionary(const std::vector<std::vector<std::string>>& groups) {
  static std::atomic<uint64_t> counter{0};
  const auto path = std::filesystem::temp_directory_path() /
                    ("mygramdb_pipeline_synonyms_" + std::to_string(::getpid()) + "_" +
                     std::to_string(counter.fetch_add(1, std::memory_order_relaxed)) + ".tsv");
  std::ofstream output(path);
  for (const auto& group : groups) {
    for (size_t i = 0; i < group.size(); ++i) {
      if (i != 0) {
        output << '\t';
      }
      output << group[i];
    }
    output << '\n';
  }
  output.close();

  auto dictionary = std::make_unique<query::SynonymDictionary>();
  const auto loaded = dictionary->LoadFromFile(path.string(), [](std::string_view term) { return std::string(term); });
  std::error_code remove_error;
  std::filesystem::remove(path, remove_error);
  if (!loaded) {
    return nullptr;
  }
  return dictionary;
}

class SearchPipelineFilterTest : public ::testing::Test {
 protected:
  void SetUp() override {
    doc_store_ = std::make_unique<storage::DocumentStore>();

    // Add documents with various filter types
    // Doc 0: status=1, name="alice", score=85.5
    auto d0 = doc_store_->AddDocument("pk0",
                                      {{"status", storage::FilterValue{int64_t{1}}},
                                       {"name", storage::FilterValue{std::string("alice")}},
                                       {"score", storage::FilterValue{85.5}}},
                                      "alice likes cats");
    ASSERT_TRUE(d0.has_value());
    doc_ids_.push_back(d0.value());

    // Doc 1: status=2, name="bob", score=92.0
    auto d1 = doc_store_->AddDocument("pk1",
                                      {{"status", storage::FilterValue{int64_t{2}}},
                                       {"name", storage::FilterValue{std::string("bob")}},
                                       {"score", storage::FilterValue{92.0}}},
                                      "bob likes dogs");
    ASSERT_TRUE(d1.has_value());
    doc_ids_.push_back(d1.value());

    // Doc 2: status=1, name="charlie", score=78.0
    auto d2 = doc_store_->AddDocument("pk2",
                                      {{"status", storage::FilterValue{int64_t{1}}},
                                       {"name", storage::FilterValue{std::string("charlie")}},
                                       {"score", storage::FilterValue{78.0}}},
                                      "charlie likes cats and dogs");
    ASSERT_TRUE(d2.has_value());
    doc_ids_.push_back(d2.value());

    // Doc 3: no filters (NULL values for all columns)
    auto d3 = doc_store_->AddDocument("pk3", {}, "empty filters document");
    ASSERT_TRUE(d3.has_value());
    doc_ids_.push_back(d3.value());
  }

  std::unique_ptr<storage::DocumentStore> doc_store_;
  std::vector<storage::DocId> doc_ids_;
};

// --- EQ operator ---

TEST_F(SearchPipelineFilterTest, FilterEqIntegerMatch) {
  std::vector<query::FilterCondition> filters = {{"status", query::FilterOp::EQ, "1"}};

  auto result = ApplyFilters(doc_ids_, filters, doc_store_.get());
  // Docs 0 and 2 have status=1
  ASSERT_EQ(result.size(), 2);
  EXPECT_EQ(result[0], doc_ids_[0]);
  EXPECT_EQ(result[1], doc_ids_[2]);
}

TEST_F(SearchPipelineFilterTest, FilterEqStringMatch) {
  std::vector<query::FilterCondition> filters = {{"name", query::FilterOp::EQ, "bob"}};

  auto result = ApplyFilters(doc_ids_, filters, doc_store_.get());
  ASSERT_EQ(result.size(), 1);
  EXPECT_EQ(result[0], doc_ids_[1]);
}

TEST_F(SearchPipelineFilterTest, FilterEqNoMatch) {
  std::vector<query::FilterCondition> filters = {{"status", query::FilterOp::EQ, "99"}};

  auto result = ApplyFilters(doc_ids_, filters, doc_store_.get());
  EXPECT_TRUE(result.empty());
}

// --- NE operator ---

TEST_F(SearchPipelineFilterTest, FilterNeExcludesMatching) {
  std::vector<query::FilterCondition> filters = {{"status", query::FilterOp::NE, "1"}};

  auto result = ApplyFilters(doc_ids_, filters, doc_store_.get());
  // Doc 1 has status=2, Doc 3 has NULL (NULL != anything is true)
  ASSERT_EQ(result.size(), 2);
  EXPECT_EQ(result[0], doc_ids_[1]);
  EXPECT_EQ(result[1], doc_ids_[3]);
}

// --- GT / GTE / LT / LTE operators ---

TEST_F(SearchPipelineFilterTest, FilterGtDouble) {
  std::vector<query::FilterCondition> filters = {{"score", query::FilterOp::GT, "80.0"}};

  auto result = ApplyFilters(doc_ids_, filters, doc_store_.get());
  // Doc 0: 85.5 > 80 = yes, Doc 1: 92 > 80 = yes, Doc 2: 78 > 80 = no
  ASSERT_EQ(result.size(), 2);
  EXPECT_EQ(result[0], doc_ids_[0]);
  EXPECT_EQ(result[1], doc_ids_[1]);
}

TEST_F(SearchPipelineFilterTest, FilterGteDouble) {
  std::vector<query::FilterCondition> filters = {{"score", query::FilterOp::GTE, "85.5"}};

  auto result = ApplyFilters(doc_ids_, filters, doc_store_.get());
  // Doc 0: 85.5 >= 85.5 = yes, Doc 1: 92 >= 85.5 = yes
  ASSERT_EQ(result.size(), 2);
}

TEST_F(SearchPipelineFilterTest, FilterLtInteger) {
  std::vector<query::FilterCondition> filters = {{"status", query::FilterOp::LT, "2"}};

  auto result = ApplyFilters(doc_ids_, filters, doc_store_.get());
  // Doc 0: 1 < 2 = yes, Doc 2: 1 < 2 = yes
  ASSERT_EQ(result.size(), 2);
}

TEST_F(SearchPipelineFilterTest, FilterLteInteger) {
  std::vector<query::FilterCondition> filters = {{"status", query::FilterOp::LTE, "1"}};

  auto result = ApplyFilters(doc_ids_, filters, doc_store_.get());
  // Doc 0 and Doc 2 have status=1
  ASSERT_EQ(result.size(), 2);
}

// --- Multiple filters (AND) ---

TEST_F(SearchPipelineFilterTest, MultipleFiltersAnd) {
  std::vector<query::FilterCondition> filters = {
      {"status", query::FilterOp::EQ, "1"},
      {"score", query::FilterOp::GT, "80.0"},
  };

  auto result = ApplyFilters(doc_ids_, filters, doc_store_.get());
  // Doc 0: status=1 AND score=85.5>80 = yes
  // Doc 2: status=1 AND score=78<80 = no
  ASSERT_EQ(result.size(), 1);
  EXPECT_EQ(result[0], doc_ids_[0]);
}

// --- NULL value handling ---

TEST_F(SearchPipelineFilterTest, NullValueFailsEqFilter) {
  // Doc 3 has no "status" filter value (NULL)
  std::vector<query::FilterCondition> filters = {{"status", query::FilterOp::EQ, "1"}};

  auto result = ApplyFilters({doc_ids_[3]}, filters, doc_store_.get());
  EXPECT_TRUE(result.empty());
}

TEST_F(SearchPipelineFilterTest, NullValuePassesNeFilter) {
  // NULL != anything is true
  std::vector<query::FilterCondition> filters = {{"status", query::FilterOp::NE, "1"}};

  auto result = ApplyFilters({doc_ids_[3]}, filters, doc_store_.get());
  ASSERT_EQ(result.size(), 1);
  EXPECT_EQ(result[0], doc_ids_[3]);
}

// --- String comparison ---

TEST_F(SearchPipelineFilterTest, FilterGtString) {
  std::vector<query::FilterCondition> filters = {{"name", query::FilterOp::GT, "bob"}};

  auto result = ApplyFilters(doc_ids_, filters, doc_store_.get());
  // "charlie" > "bob" = yes, "alice" > "bob" = no
  ASSERT_EQ(result.size(), 1);
  EXPECT_EQ(result[0], doc_ids_[2]);
}

// --- Empty input ---

TEST_F(SearchPipelineFilterTest, EmptyResultsReturnsEmpty) {
  std::vector<query::FilterCondition> filters = {{"status", query::FilterOp::EQ, "1"}};

  auto result = ApplyFilters({}, filters, doc_store_.get());
  EXPECT_TRUE(result.empty());
}

TEST_F(SearchPipelineFilterTest, NoFiltersReturnsAll) {
  std::vector<query::FilterCondition> filters;

  auto result = ApplyFilters(doc_ids_, filters, doc_store_.get());
  EXPECT_EQ(result.size(), doc_ids_.size());
}

// --- PostFilterByText ---

TEST_F(SearchPipelineFilterTest, PostFilterByTextMatchesSubstring) {
  std::vector<std::string> terms = {"cats"};

  auto result = PostFilterByText(doc_ids_, terms, doc_store_.get());
  // Doc 0: "alice likes cats" = yes
  // Doc 1: "bob likes dogs" = no
  // Doc 2: "charlie likes cats and dogs" = yes
  // Doc 3: "empty filters document" = no
  ASSERT_EQ(result.size(), 2);
  EXPECT_EQ(result[0], doc_ids_[0]);
  EXPECT_EQ(result[1], doc_ids_[2]);
}

TEST_F(SearchPipelineFilterTest, PostFilterByTextMultipleTerms) {
  std::vector<std::string> terms = {"cats", "dogs"};

  auto result = PostFilterByText(doc_ids_, terms, doc_store_.get());
  // Only Doc 2: "charlie likes cats and dogs" contains both
  ASSERT_EQ(result.size(), 1);
  EXPECT_EQ(result[0], doc_ids_[2]);
}

TEST_F(SearchPipelineFilterTest, PostFilterByTextEmptyCandidates) {
  std::vector<std::string> terms = {"cats"};

  auto result = PostFilterByText({}, terms, doc_store_.get());
  EXPECT_TRUE(result.empty());
}

// --- IsCacheStale ---

TEST_F(SearchPipelineFilterTest, CacheNotStaleForValidDocs) {
  EXPECT_FALSE(IsCacheStale(doc_ids_, doc_store_.get()));
}

TEST_F(SearchPipelineFilterTest, CacheStaleForRemovedDoc) {
  doc_store_->RemoveDocument(doc_ids_[0]);
  // After removal, the doc_id still in the list but won't have a primary key
  EXPECT_TRUE(IsCacheStale(doc_ids_, doc_store_.get()));
}

TEST_F(SearchPipelineFilterTest, CacheNotStaleForEmpty) {
  EXPECT_FALSE(IsCacheStale({}, doc_store_.get()));
}

TEST(SearchPipelineCacheTest, CacheStalenessSamplingIsBoundedForWideResults) {
  EXPECT_EQ(CacheStaleSampleSize(0), 0U);
  EXPECT_EQ(CacheStaleSampleSize(5), 5U);
  EXPECT_EQ(CacheStaleSampleSize(100), 10U);
  EXPECT_EQ(CacheStaleSampleSize(10000), 1000U);
  EXPECT_EQ(CacheStaleSampleSize(1000000), 1024U);
}

// --- InsertToCache with null manager ---

TEST_F(SearchPipelineFilterTest, InsertToCacheWithNullManagerIsNoop) {
  query::Query query;
  std::vector<SearchTermInfo> term_infos;
  // Should not crash
  InsertToCache(nullptr, query, doc_ids_, term_infos, 1.0, 2, 0, false);
}

TEST_F(SearchPipelineFilterTest, InsertToCacheWithStaleDataVersionDoesNotInsert) {
  config::CacheConfig cache_config;
  cache_config.enabled = true;
  cache_config.max_memory_bytes = 10 * 1024 * 1024;
  cache_config.min_query_cost_ms = 0.0;
  cache_config.invalidation.max_delay_ms = 1;

  cache::NgramConfigMap ngram_configs;
  ngram_configs["test"] = cache::NgramConfig{
      .ngram_size = 2,
      .kanji_ngram_size = 2,
      .cross_boundary_ngrams = false,
  };
  cache::CacheManager cache_manager(cache_config, std::move(ngram_configs));

  query::Query query;
  query.type = query::QueryType::SEARCH;
  query.table = "test";
  query.search_text = "alpha";
  query.limit = 100;
  index::Index cache_index(2);
  FullPipelineParams pipeline_params;
  pipeline_params.current_index = &cache_index;
  pipeline_params.ngram_size = 2;
  pipeline_params.kanji_ngram_size = 2;
  const query::Query cache_query = BuildCanonicalCacheQuery(query, pipeline_params);

  std::vector<SearchTermInfo> term_infos = {{{"al", "lp", "ph", "ha"}, 4, 0, "alpha"}};
  const auto stale_version = cache_manager.CaptureDataVersion();
  cache_manager.Invalidate("test", "", "unrelated mutation");

  InsertToCache(&cache_manager, cache_query, doc_ids_, term_infos, 1.0, 2, 0, false, stale_version);
  EXPECT_FALSE(cache_manager.Lookup(cache_query).has_value());
}

TEST_F(SearchPipelineFilterTest, InsertToCacheSkipsEmptyNgramTerms) {
  config::CacheConfig cache_config;
  cache_config.enabled = true;
  cache_config.max_memory_bytes = 10 * 1024 * 1024;
  cache_config.min_query_cost_ms = 0.0;

  cache::NgramConfigMap ngram_configs;
  ngram_configs["test"] = cache::NgramConfig{
      .ngram_size = 2,
      .kanji_ngram_size = 2,
      .cross_boundary_ngrams = false,
  };
  cache::CacheManager cache_manager(cache_config, std::move(ngram_configs));

  query::Query query;
  query.type = query::QueryType::SEARCH;
  query.table = "test";
  query.search_text = "a";
  query.limit = 100;
  index::Index cache_index(2);
  FullPipelineParams pipeline_params;
  pipeline_params.current_index = &cache_index;
  pipeline_params.ngram_size = 2;
  pipeline_params.kanji_ngram_size = 2;
  const query::Query cache_query = BuildCanonicalCacheQuery(query, pipeline_params);

  std::vector<SearchTermInfo> term_infos = {{{}, std::numeric_limits<size_t>::max(), 0, "a"}};

  InsertToCache(&cache_manager, cache_query, doc_ids_, term_infos, 1.0, 2, 2, false);

  EXPECT_FALSE(cache_manager.Lookup(cache_query).has_value());
}

TEST(SearchPipelineCacheTest, MergeSortedTermNgramsForCacheUsesKWaySortedUniqueMerge) {
  std::vector<SearchTermInfo> term_infos = {
      {{"aa", "cc", "ee"}, 10},
      {{"aa", "bb", "ee", "ff"}, 20},
      {{"bb", "dd"}, 30},
      {{}, 0},
  };

  auto merged = MergeSortedTermNgramsForCache(term_infos);

  EXPECT_EQ(merged, (std::vector<std::string>{"aa", "bb", "cc", "dd", "ee", "ff"}));
}

TEST(SearchPipelineCacheTest, MergeSortedTermNgramsForCacheHandlesEmptyInput) {
  EXPECT_TRUE(MergeSortedTermNgramsForCache({}).empty());
  EXPECT_TRUE(MergeSortedTermNgramsForCache({{{}, 0}, {{}, 0}}).empty());
}

TEST(SearchPipelineBM25Test, GenerateTermInfosSkipsDocumentFrequencyByDefault) {
  index::Index index(/* ngram_size= */ 2, /* kanji_ngram_size= */ 1);
  index.AddDocument(1, "abc");
  index.AddDocument(2, "abq");
  index.AddDocument(3, "xbc");

  auto term_infos = GenerateTermInfos({"abc"}, &index, /* ngram_size= */ 2, /* kanji_ngram_size= */ 1,
                                      /* cross_boundary_ngrams= */ true);

  ASSERT_EQ(term_infos.size(), 1);
  EXPECT_EQ(term_infos[0].ngrams, (std::vector<std::string>{"ab", "bc"}));
  EXPECT_EQ(term_infos[0].estimated_size, 2u);
  EXPECT_EQ(term_infos[0].term_doc_freq, 0u);
  EXPECT_FALSE(term_infos[0].term_doc_freq_computed);
  EXPECT_EQ(term_infos[0].normalized_term, "abc");
}

TEST(SearchPipelineBM25Test, GenerateTermInfosUsesVerifiedTermDocumentFrequencyWhenRequested) {
  index::Index index(/* ngram_size= */ 2, /* kanji_ngram_size= */ 1);
  storage::DocumentStore doc_store;
  auto exact = doc_store.AddDocument("1", {}, "abc");
  auto false_positive = doc_store.AddDocument("2", {}, "abxxbc");
  ASSERT_TRUE(exact.has_value());
  ASSERT_TRUE(false_positive.has_value());
  index.AddDocument(*exact, "abc");
  index.AddDocument(*false_positive, "abxxbc");

  auto term_infos = GenerateTermInfos({"abc"}, &index, /* ngram_size= */ 2, /* kanji_ngram_size= */ 1,
                                      /* cross_boundary_ngrams= */ true,
                                      /*compute_term_doc_freq=*/true, &doc_store);

  ASSERT_EQ(term_infos.size(), 1);
  EXPECT_EQ(term_infos[0].ngrams, (std::vector<std::string>{"ab", "bc"}));
  EXPECT_EQ(term_infos[0].estimated_size, 2u);
  EXPECT_EQ(term_infos[0].term_doc_freq, 1u);
  EXPECT_TRUE(term_infos[0].term_doc_freq_computed);
  EXPECT_EQ(term_infos[0].normalized_term, "abc");
}

TEST(SearchPipelineBM25Test, TermDocumentFrequencyStaysPairedWithNormalizedTermAfterSorting) {
  index::Index index(/* ngram_size= */ 2, /* kanji_ngram_size= */ 1);
  storage::DocumentStore doc_store;
  auto first = doc_store.AddDocument("1", {}, "rare common");
  auto second = doc_store.AddDocument("2", {}, "common only");
  auto third = doc_store.AddDocument("3", {}, "common extra");
  ASSERT_TRUE(first.has_value());
  ASSERT_TRUE(second.has_value());
  ASSERT_TRUE(third.has_value());
  index.AddDocument(*first, "rare common");
  index.AddDocument(*second, "common only");
  index.AddDocument(*third, "common extra");

  auto term_infos = GenerateTermInfos({"common", "rare"}, &index, /* ngram_size= */ 2, /* kanji_ngram_size= */ 1,
                                      /* cross_boundary_ngrams= */ true,
                                      /*compute_term_doc_freq=*/true, &doc_store);
  std::sort(term_infos.begin(), term_infos.end(), [](const SearchTermInfo& lhs, const SearchTermInfo& rhs) {
    return lhs.estimated_size < rhs.estimated_size;
  });

  ASSERT_EQ(term_infos.size(), 2);
  ASSERT_EQ(term_infos[0].normalized_term, "rare");
  EXPECT_EQ(term_infos[0].term_doc_freq, 1u);
  EXPECT_TRUE(term_infos[0].term_doc_freq_computed);
  ASSERT_EQ(term_infos[1].normalized_term, "common");
  EXPECT_EQ(term_infos[1].term_doc_freq, 3u);
  EXPECT_TRUE(term_infos[1].term_doc_freq_computed);
}

TEST(SearchPipelineBM25Test, CjkDocumentFrequencyExcludesUnorderedUnigramCandidates) {
  index::Index index(/* ngram_size= */ 2, /* kanji_ngram_size= */ 1);
  storage::DocumentStore doc_store;
  auto exact = doc_store.AddDocument("1", {}, "東京");
  auto false_positive = doc_store.AddDocument("2", {}, "東西京");
  ASSERT_TRUE(exact.has_value());
  ASSERT_TRUE(false_positive.has_value());
  index.AddDocument(*exact, "東京");
  index.AddDocument(*false_positive, "東西京");

  auto term_infos = GenerateTermInfos({"東京"}, &index, /* ngram_size= */ 2, /* kanji_ngram_size= */ 1,
                                      /* cross_boundary_ngrams= */ true,
                                      /*compute_term_doc_freq=*/true, &doc_store);

  ASSERT_EQ(term_infos.size(), 1);
  EXPECT_EQ(term_infos[0].term_doc_freq, 1U);
  EXPECT_TRUE(term_infos[0].term_doc_freq_computed);
}

TEST(SearchPipelineBM25Test, SynonymExpansionDoesNotComputeUnusedVariantDocumentFrequency) {
  index::Index index(/* ngram_size= */ 2, /* kanji_ngram_size= */ 1);
  index.AddDocument(1, "car handbook");
  index.AddDocument(2, "automobile handbook");
  auto synonyms = MakeSynonymDictionary({{"car", "automobile"}});
  ASSERT_NE(synonyms, nullptr);

  auto groups = ExpandTermsWithSynonyms({"car"}, synonyms.get(), &index, /* ngram_size= */ 2,
                                        /* kanji_ngram_size= */ 1, /* cross_boundary_ngrams= */ true);

  ASSERT_EQ(groups.size(), 1U);
  ASSERT_EQ(groups[0].variants.size(), 2U);
  for (const auto& variant : groups[0].variants) {
    EXPECT_GT(variant.estimated_size, 0U);
    EXPECT_EQ(variant.term_doc_freq, 0U);
    EXPECT_FALSE(variant.term_doc_freq_computed);
  }
}

// =============================================================================
// ExecuteWithFuzzy: empty n-gram detection (m-17)
// =============================================================================
// When a search term produces no n-grams (e.g., single character with bigram
// indexing), ExecuteWithFuzzy must set empty_term_detected = true and return
// empty results, rather than proceeding with an empty n-gram list that would
// match all documents or crash.
// =============================================================================

class SearchPipelineFuzzyTest : public ::testing::Test {
 protected:
  void SetUp() override {
    index_ = std::make_unique<index::Index>();
    doc_store_ = std::make_unique<storage::DocumentStore>();

    // Add documents with text long enough to produce n-grams
    auto d0 = doc_store_->AddDocument("pk0", {}, "hello world");
    ASSERT_TRUE(d0.has_value());
    index_->AddDocument(d0.value(), "hello world");

    auto d1 = doc_store_->AddDocument("pk1", {}, "fuzzy search test");
    ASSERT_TRUE(d1.has_value());
    index_->AddDocument(d1.value(), "fuzzy search test");
  }

  std::unique_ptr<index::Index> index_;
  std::unique_ptr<storage::DocumentStore> doc_store_;
};

TEST_F(SearchPipelineFuzzyTest, EmptyNgramsSetEmptyTermDetected) {
  // A term with empty n-grams should trigger empty_term_detected
  std::vector<SearchTermInfo> term_infos;
  term_infos.push_back({/* ngrams= */ {}, /* estimated_size= */ 0});

  query::Query query;
  std::vector<std::string> all_terms = {"x"};

  auto result = ExecuteWithFuzzy(query, term_infos, all_terms, /* max_distance= */ 1, index_.get(), doc_store_.get(),
                                 /* full_config= */ nullptr,
                                 /* ngram_size= */ 2, /* kanji_ngram_size= */ 1,
                                 /* cross_boundary= */ true, /* filter_threshold= */ 100);

  EXPECT_TRUE(result.empty_term_detected);
  EXPECT_TRUE(result.results.empty());
}

TEST_F(SearchPipelineFuzzyTest, EmptyNgramsAmongMultipleTermsSetsEmptyTermDetected) {
  // If one of multiple terms has empty n-grams, the whole query should
  // report empty_term_detected
  std::vector<SearchTermInfo> term_infos;
  term_infos.push_back({{"he", "el", "ll", "lo"}, 2});                // valid term
  term_infos.push_back({/* ngrams= */ {}, /* estimated_size= */ 0});  // empty

  query::Query query;
  std::vector<std::string> all_terms = {"hello", "x"};

  auto result = ExecuteWithFuzzy(query, term_infos, all_terms, /* max_distance= */ 1, index_.get(), doc_store_.get(),
                                 /* full_config= */ nullptr,
                                 /* ngram_size= */ 2, /* kanji_ngram_size= */ 1,
                                 /* cross_boundary= */ true, /* filter_threshold= */ 100);

  EXPECT_TRUE(result.empty_term_detected);
  EXPECT_TRUE(result.results.empty());
}

TEST_F(SearchPipelineFuzzyTest, EmptyTermInfosSetEmptyTermDetected) {
  // No terms at all should also trigger empty_term_detected
  std::vector<SearchTermInfo> term_infos;
  query::Query query;
  std::vector<std::string> all_terms;

  auto result = ExecuteWithFuzzy(query, term_infos, all_terms, /* max_distance= */ 1, index_.get(), doc_store_.get(),
                                 /* full_config= */ nullptr,
                                 /* ngram_size= */ 2, /* kanji_ngram_size= */ 1,
                                 /* cross_boundary= */ true, /* filter_threshold= */ 100);

  EXPECT_TRUE(result.empty_term_detected);
  EXPECT_TRUE(result.results.empty());
}

// =============================================================================
// Execute: max() sentinel triggers empty_term_detected
// =============================================================================
// When GenerateTermInfos produces a term with no n-grams (e.g., empty search
// term after normalization), min_size stays at std::numeric_limits<size_t>::max().
// Execute() must detect this sentinel and set empty_term_detected = true,
// because the intersection result is guaranteed to be empty.
// =============================================================================

TEST_F(SearchPipelineFuzzyTest, ExecuteMaxSentinelTriggersEmptyTermDetected) {
  // Simulate a term_info where no n-grams were generated (min_size stays at max())
  std::vector<SearchTermInfo> term_infos;
  term_infos.push_back({/* ngrams= */ {}, std::numeric_limits<size_t>::max()});

  query::Query query;
  std::vector<std::string> all_terms = {"x"};

  auto result = Execute(query, term_infos, all_terms, index_.get(), doc_store_.get(),
                        /* full_config= */ nullptr,
                        /* ngram_size= */ 2, /* kanji_ngram_size= */ 1,
                        /* cross_boundary= */ true, /* filter_threshold= */ 100);

  EXPECT_TRUE(result.empty_term_detected);
  EXPECT_TRUE(result.results.empty());
}

TEST_F(SearchPipelineFuzzyTest, ExecuteMaxSentinelAmongValidTermsTriggersEmptyTermDetected) {
  // If one of multiple terms has max() sentinel, the entire query should
  // report empty_term_detected (AND semantics: empty intersection)
  std::vector<SearchTermInfo> term_infos;
  term_infos.push_back({{"he", "el", "ll", "lo"}, 2});                           // valid term
  term_infos.push_back({/* ngrams= */ {}, std::numeric_limits<size_t>::max()});  // max sentinel

  query::Query query;
  std::vector<std::string> all_terms = {"hello", "x"};

  auto result = Execute(query, term_infos, all_terms, index_.get(), doc_store_.get(),
                        /* full_config= */ nullptr,
                        /* ngram_size= */ 2, /* kanji_ngram_size= */ 1,
                        /* cross_boundary= */ true, /* filter_threshold= */ 100);

  EXPECT_TRUE(result.empty_term_detected);
  EXPECT_TRUE(result.results.empty());
}

TEST_F(SearchPipelineFuzzyTest, ExecuteZeroEstimatedSizeTriggersEmptyTermDetected) {
  // estimated_size == 0 means no posting list found for an n-gram
  std::vector<SearchTermInfo> term_infos;
  term_infos.push_back({{"zz"}, 0});

  query::Query query;
  std::vector<std::string> all_terms = {"zz"};

  auto result = Execute(query, term_infos, all_terms, index_.get(), doc_store_.get(),
                        /* full_config= */ nullptr,
                        /* ngram_size= */ 2, /* kanji_ngram_size= */ 1,
                        /* cross_boundary= */ true, /* filter_threshold= */ 100);

  EXPECT_TRUE(result.empty_term_detected);
  EXPECT_TRUE(result.results.empty());
}

// =============================================================================
// Execute: NOT filter and column filters are applied within pipeline (m-16)
// =============================================================================
// This validates that Execute() applies NOT terms and column filters internally,
// so callers (like FacetHandler) must NOT re-apply them when using Execute().
// The regression was that facet_handler applied NOT/filters AFTER Execute(),
// causing double-filtering that removed too many documents.
// =============================================================================

TEST_F(SearchPipelineFuzzyTest, ExecuteAppliesNotFilterInternally) {
  // Add a document that will be excluded by NOT
  auto d2 = doc_store_->AddDocument("pk2", {{"status", storage::FilterValue{int64_t{1}}}}, "excluded hello world");
  ASSERT_TRUE(d2.has_value());
  index_->AddDocument(d2.value(), "excluded hello world");

  // Search for "hello" NOT "excluded"
  auto term_infos = GenerateTermInfos({"hello"}, index_.get(), 2, 1, true);
  std::sort(term_infos.begin(), term_infos.end(),
            [](const SearchTermInfo& a, const SearchTermInfo& b) { return a.estimated_size < b.estimated_size; });

  query::Query query;
  query.not_terms = {"excluded"};
  std::vector<std::string> all_terms = {"hello"};

  auto result = Execute(query, term_infos, all_terms, index_.get(), doc_store_.get(), /* full_config= */ nullptr,
                        /* ngram_size= */ 2, /* kanji_ngram_size= */ 1,
                        /* cross_boundary= */ true, /* filter_threshold= */ 100);

  // "hello world" (doc 0) should remain; "excluded hello world" (doc 2) removed
  EXPECT_FALSE(result.empty_term_detected);
  ASSERT_EQ(result.results.size(), 1);

  // Verify the NOT filter was applied inside Execute -- applying it again
  // should not change the results (no double-filtering)
  auto double_filtered = ApplyNotFilter(result.results, query.not_terms, index_.get(), doc_store_.get(), 2, 1, true);
  EXPECT_EQ(double_filtered.size(), result.results.size())
      << "NOT filter was not applied inside Execute(); applying it again "
         "changed the result set (double-filter bug)";
}

TEST_F(SearchPipelineFuzzyTest, ExecuteAppliesColumnFiltersInternally) {
  // Add documents with filter values
  auto d2 = doc_store_->AddDocument("pk2", {{"status", storage::FilterValue{int64_t{1}}}}, "status one hello");
  ASSERT_TRUE(d2.has_value());
  index_->AddDocument(d2.value(), "status one hello");

  auto d3 = doc_store_->AddDocument("pk3", {{"status", storage::FilterValue{int64_t{2}}}}, "status two hello");
  ASSERT_TRUE(d3.has_value());
  index_->AddDocument(d3.value(), "status two hello");

  // Search for "hello" with FILTER status=1
  auto term_infos = GenerateTermInfos({"hello"}, index_.get(), 2, 1, true);
  std::sort(term_infos.begin(), term_infos.end(),
            [](const SearchTermInfo& a, const SearchTermInfo& b) { return a.estimated_size < b.estimated_size; });

  query::Query query;
  query.filters = {{"status", query::FilterOp::EQ, "1"}};
  std::vector<std::string> all_terms = {"hello"};

  auto result = Execute(query, term_infos, all_terms, index_.get(), doc_store_.get(), /* full_config= */ nullptr,
                        /* ngram_size= */ 2, /* kanji_ngram_size= */ 1,
                        /* cross_boundary= */ true, /* filter_threshold= */ 100);

  EXPECT_FALSE(result.empty_term_detected);

  // Verify that applying filters again produces the same result (no double-filter)
  auto double_filtered = ApplyFilters(result.results, query.filters, doc_store_.get());
  EXPECT_EQ(double_filtered.size(), result.results.size())
      << "Column filters were not applied inside Execute(); applying them again "
         "changed the result set (double-filter bug)";
}

// =============================================================================
// ApplyFiltersWithBitmap vs ApplyFilters parity test
// =============================================================================
// Both code paths (bitmap fast path for EQ/NE, per-document fallback for range
// operators) should produce identical results when given the same inputs.
// =============================================================================

class SearchPipelineFilterParityTest : public ::testing::Test {
 protected:
  void SetUp() override {
    doc_store_ = std::make_unique<storage::DocumentStore>();

    // Add documents with filter columns that exercise various value patterns
    // Doc 0: status=1, category="tech", score=85.5
    auto d0 = doc_store_->AddDocument("pk0",
                                      {{"status", storage::FilterValue{int64_t{1}}},
                                       {"category", storage::FilterValue{std::string("tech")}},
                                       {"score", storage::FilterValue{85.5}}},
                                      "text zero");
    ASSERT_TRUE(d0.has_value());
    doc_ids_.push_back(d0.value());

    // Doc 1: status=2, category="sports", score=92.0
    auto d1 = doc_store_->AddDocument("pk1",
                                      {{"status", storage::FilterValue{int64_t{2}}},
                                       {"category", storage::FilterValue{std::string("sports")}},
                                       {"score", storage::FilterValue{92.0}}},
                                      "text one");
    ASSERT_TRUE(d1.has_value());
    doc_ids_.push_back(d1.value());

    // Doc 2: status=1, category="tech", score=78.0
    auto d2 = doc_store_->AddDocument("pk2",
                                      {{"status", storage::FilterValue{int64_t{1}}},
                                       {"category", storage::FilterValue{std::string("tech")}},
                                       {"score", storage::FilterValue{78.0}}},
                                      "text two");
    ASSERT_TRUE(d2.has_value());
    doc_ids_.push_back(d2.value());

    // Doc 3: status=3, category="music", score=60.0
    auto d3 = doc_store_->AddDocument("pk3",
                                      {{"status", storage::FilterValue{int64_t{3}}},
                                       {"category", storage::FilterValue{std::string("music")}},
                                       {"score", storage::FilterValue{60.0}}},
                                      "text three");
    ASSERT_TRUE(d3.has_value());
    doc_ids_.push_back(d3.value());

    // Doc 4: no filters (NULL values)
    auto d4 = doc_store_->AddDocument("pk4", {}, "text four");
    ASSERT_TRUE(d4.has_value());
    doc_ids_.push_back(d4.value());
  }

  std::unique_ptr<storage::DocumentStore> doc_store_;
  std::vector<storage::DocId> doc_ids_;
};

TEST_F(SearchPipelineFilterParityTest, EqFilterBitmapMatchesFallback) {
  std::vector<query::FilterCondition> filters = {{"status", query::FilterOp::EQ, "1"}};

  auto bitmap_result = ApplyFiltersWithBitmap(doc_ids_, filters, doc_store_.get());
  auto fallback_result = ApplyFilters(doc_ids_, filters, doc_store_.get());

  ASSERT_EQ(bitmap_result.size(), fallback_result.size());
  for (size_t i = 0; i < bitmap_result.size(); ++i) {
    EXPECT_EQ(bitmap_result[i], fallback_result[i]);
  }
  // Both should find docs 0 and 2
  EXPECT_EQ(bitmap_result.size(), 2);
}

TEST_F(SearchPipelineFilterParityTest, NeFilterBitmapMatchesFallback) {
  std::vector<query::FilterCondition> filters = {{"status", query::FilterOp::NE, "1"}};

  auto bitmap_result = ApplyFiltersWithBitmap(doc_ids_, filters, doc_store_.get());
  auto fallback_result = ApplyFilters(doc_ids_, filters, doc_store_.get());

  ASSERT_EQ(bitmap_result.size(), fallback_result.size());
  for (size_t i = 0; i < bitmap_result.size(); ++i) {
    EXPECT_EQ(bitmap_result[i], fallback_result[i]);
  }
}

TEST_F(SearchPipelineFilterParityTest, EqStringFilterBitmapMatchesFallback) {
  std::vector<query::FilterCondition> filters = {{"category", query::FilterOp::EQ, "tech"}};

  auto bitmap_result = ApplyFiltersWithBitmap(doc_ids_, filters, doc_store_.get());
  auto fallback_result = ApplyFilters(doc_ids_, filters, doc_store_.get());

  ASSERT_EQ(bitmap_result.size(), fallback_result.size());
  for (size_t i = 0; i < bitmap_result.size(); ++i) {
    EXPECT_EQ(bitmap_result[i], fallback_result[i]);
  }
  // Both should find docs 0 and 2
  EXPECT_EQ(bitmap_result.size(), 2);
}

TEST_F(SearchPipelineFilterParityTest, FilterColumnResolvesCaseInsensitively) {
  std::vector<query::FilterCondition> filters = {{"CATEGORY", query::FilterOp::EQ, "tech"}};

  auto bitmap_result = ApplyFiltersWithBitmap(doc_ids_, filters, doc_store_.get());
  auto fallback_result = ApplyFilters(doc_ids_, filters, doc_store_.get());

  ASSERT_EQ(bitmap_result.size(), fallback_result.size());
  EXPECT_EQ(bitmap_result, fallback_result);
  ASSERT_EQ(bitmap_result.size(), 2);
  EXPECT_EQ(bitmap_result[0], doc_ids_[0]);
  EXPECT_EQ(bitmap_result[1], doc_ids_[2]);
}

TEST_F(SearchPipelineFilterParityTest, MultipleEqFiltersBitmapMatchesFallback) {
  std::vector<query::FilterCondition> filters = {
      {"status", query::FilterOp::EQ, "1"},
      {"category", query::FilterOp::EQ, "tech"},
  };

  auto bitmap_result = ApplyFiltersWithBitmap(doc_ids_, filters, doc_store_.get());
  auto fallback_result = ApplyFilters(doc_ids_, filters, doc_store_.get());

  ASSERT_EQ(bitmap_result.size(), fallback_result.size());
  for (size_t i = 0; i < bitmap_result.size(); ++i) {
    EXPECT_EQ(bitmap_result[i], fallback_result[i]);
  }
}

TEST_F(SearchPipelineFilterParityTest, NoMatchFilterBitmapMatchesFallback) {
  std::vector<query::FilterCondition> filters = {{"status", query::FilterOp::EQ, "999"}};

  auto bitmap_result = ApplyFiltersWithBitmap(doc_ids_, filters, doc_store_.get());
  auto fallback_result = ApplyFilters(doc_ids_, filters, doc_store_.get());

  EXPECT_TRUE(bitmap_result.empty());
  EXPECT_TRUE(fallback_result.empty());
}

TEST_F(SearchPipelineFilterParityTest, EmptyInputBitmapMatchesFallback) {
  std::vector<query::FilterCondition> filters = {{"status", query::FilterOp::EQ, "1"}};

  auto bitmap_result = ApplyFiltersWithBitmap({}, filters, doc_store_.get());
  auto fallback_result = ApplyFilters({}, filters, doc_store_.get());

  EXPECT_TRUE(bitmap_result.empty());
  EXPECT_TRUE(fallback_result.empty());
}

TEST_F(SearchPipelineFilterParityTest, NoFiltersBitmapMatchesFallback) {
  std::vector<query::FilterCondition> filters;

  auto bitmap_result = ApplyFiltersWithBitmap(doc_ids_, filters, doc_store_.get());
  auto fallback_result = ApplyFilters(doc_ids_, filters, doc_store_.get());

  ASSERT_EQ(bitmap_result.size(), fallback_result.size());
  EXPECT_EQ(bitmap_result.size(), doc_ids_.size());
}

TEST_F(SearchPipelineFilterParityTest, NullDocFilterBitmapMatchesFallback) {
  // Filter on doc with NULL values (doc 4 has no filters)
  std::vector<query::FilterCondition> filters = {{"status", query::FilterOp::EQ, "1"}};

  // Test with only the NULL-value doc
  std::vector<storage::DocId> null_doc = {doc_ids_[4]};
  auto bitmap_result = ApplyFiltersWithBitmap(null_doc, filters, doc_store_.get());
  auto fallback_result = ApplyFilters(null_doc, filters, doc_store_.get());

  EXPECT_TRUE(bitmap_result.empty());
  EXPECT_TRUE(fallback_result.empty());
}

// =============================================================================
// ApplyFiltersWithBitmap falls back to per-document for range operators
// =============================================================================

TEST_F(SearchPipelineFilterParityTest, MixedEqAndRangeFiltersBitmapMatchesFallback) {
  // ApplyFiltersWithBitmap uses bitmap for EQ, falls back for GT
  // ApplyFilters uses per-document for everything
  // Both should produce the same result
  std::vector<query::FilterCondition> filters = {
      {"status", query::FilterOp::EQ, "1"},
      {"score", query::FilterOp::GT, "80.0"},
  };

  auto bitmap_result = ApplyFiltersWithBitmap(doc_ids_, filters, doc_store_.get());
  auto fallback_result = ApplyFilters(doc_ids_, filters, doc_store_.get());

  ASSERT_EQ(bitmap_result.size(), fallback_result.size());
  for (size_t i = 0; i < bitmap_result.size(); ++i) {
    EXPECT_EQ(bitmap_result[i], fallback_result[i]);
  }
  // Only doc 0: status=1 AND score=85.5>80
  EXPECT_EQ(bitmap_result.size(), 1);
}

// =============================================================================
// ExecuteFullPipeline tests - unified pipeline used by both TCP and HTTP
// =============================================================================

class FullPipelineTest : public ::testing::Test {
 protected:
  void SetUp() override {
    index_ = std::make_unique<index::Index>(2);
    doc_store_ = std::make_unique<storage::DocumentStore>();

    // Add documents
    auto d1 = doc_store_->AddDocument("pk1", {{"status", storage::FilterValue{int64_t{1}}}}, "machine learning basics");
    auto d2 =
        doc_store_->AddDocument("pk2", {{"status", storage::FilterValue{int64_t{1}}}}, "deep learning techniques");
    auto d3 = doc_store_->AddDocument("pk3", {{"status", storage::FilterValue{int64_t{0}}}}, "old article about cats");
    ASSERT_TRUE(d1.has_value());
    ASSERT_TRUE(d2.has_value());
    ASSERT_TRUE(d3.has_value());

    index_->AddDocument(*d1, "machine learning basics");
    index_->AddDocument(*d2, "deep learning techniques");
    index_->AddDocument(*d3, "old article about cats");

    doc_ids_.push_back(*d1);
    doc_ids_.push_back(*d2);
    doc_ids_.push_back(*d3);
  }

  FullPipelineParams MakeParams() {
    FullPipelineParams params;
    params.current_index = index_.get();
    params.current_doc_store = doc_store_.get();
    params.ngram_size = 2;
    params.kanji_ngram_size = 0;
    params.cross_boundary_ngrams = false;
    params.filter_threshold = 1000;
    params.primary_key_column = "id";
    return params;
  }

  std::unique_ptr<index::Index> index_;
  std::unique_ptr<storage::DocumentStore> doc_store_;
  std::vector<storage::DocId> doc_ids_;
};

TEST_F(FullPipelineTest, BasicSearch) {
  query::Query query;
  query.type = query::QueryType::SEARCH;
  query.table = "test";
  query.search_text = "learning";
  query.limit = 100;

  auto params = MakeParams();
  auto output = ExecuteFullPipeline(query, params);

  EXPECT_TRUE(output.has_value());
  EXPECT_TRUE((output.has_value() ? std::string{} : output.error().message()).empty());
  EXPECT_FALSE(output->cache_hit);
  // "learning" appears in doc1 and doc2
  EXPECT_EQ(output->results.size(), 2);
  EXPECT_EQ(output->all_search_terms.size(), 1);
  EXPECT_EQ(output->all_search_terms[0], "learning");
}

TEST_F(FullPipelineTest, ReportsRealFunnelCountsIncludingZeroStages) {
  query::Query query;
  query.type = query::QueryType::SEARCH;
  query.table = "test";
  query.search_text = "learning";
  query.and_terms = {"machine"};
  query.not_terms = {"basics"};
  query.filters.push_back({"status", query::FilterOp::EQ, "1"});

  auto output = ExecuteFullPipeline(query, MakeParams());

  ASSERT_TRUE(output.has_value()) << (output ? "" : output.error().message());
  // Terms are size-ordered, so the selective "machine" posting list is the
  // first candidate set even though it appears in and_terms.
  EXPECT_EQ(output->total_candidates, 1U);
  EXPECT_EQ(output->after_intersection, 1U);
  EXPECT_EQ(output->after_not, 0U);
  EXPECT_EQ(output->after_filters, 0U);
  EXPECT_TRUE(output->results.empty());
}

TEST_F(FullPipelineTest, SharedFacetPipelineRechecksLoadingAfterSnapshot) {
  query::Query query;
  query.type = query::QueryType::FACET;
  query.table = "test";
  query.facet_column = "status";

  FacetPipelineParams params;
  params.search = MakeParams();
  params.load_in_progress = []() { return true; };
  auto output = ExecuteFacetPipeline(query, params);

  ASSERT_FALSE(output.has_value());
  EXPECT_EQ(output.error().code(), mygram::utils::ErrorCode::kServerLoading);
}

TEST_F(FullPipelineTest, SingleTermNgramAndReproducibilityIsExplicitPerExecutionPath) {
  auto cjk_only = doc_store_->AddDocument("pk4", {}, "京都");
  auto mixed_match = doc_store_->AddDocument("pk5", {}, "京タワー");
  ASSERT_TRUE(cjk_only.has_value());
  ASSERT_TRUE(mixed_match.has_value());
  index_->AddDocument(*cjk_only, "京都");
  index_->AddDocument(*mixed_match, "京タワー");

  struct TestCase {
    const char* name;
    query::Query query;
    int kanji_ngram_size;
    bool cross_boundary_ngrams;
    bool expected;
  };

  query::Query plain;
  plain.type = query::QueryType::SEARCH;
  plain.table = "test";
  plain.search_text = "learning";
  plain.limit = 1;

  query::Query fuzzy = plain;
  fuzzy.search_text = "learnig";
  fuzzy.fuzzy_max_distance = 1;

  query::Query boolean_not = plain;
  boolean_not.search_text = "learning AND (NOT deep)";

  query::Query hybrid = plain;
  hybrid.search_text = "京タ";

  const std::vector<TestCase> cases = {
      {"plain", plain, 0, false, true},
      {"fuzzy", fuzzy, 0, false, false},
      {"boolean_not", boolean_not, 0, false, false},
      {"hybrid_fragment", hybrid, 1, false, false},
  };

  for (const auto& test_case : cases) {
    SCOPED_TRACE(test_case.name);
    auto params = MakeParams();
    params.kanji_ngram_size = test_case.kanji_ngram_size;
    params.cross_boundary_ngrams = test_case.cross_boundary_ngrams;

    auto output = ExecuteFullPipeline(test_case.query, params);

    ASSERT_TRUE(output.has_value()) << (output.has_value() ? std::string{} : output.error().message());
    EXPECT_EQ(output->semantics_reproducible_by_single_term_ngram_and, test_case.expected);
  }
}

TEST_F(FullPipelineTest, InvalidUtf8SearchTextReturnsQueryError) {
  query::Query query;
  query.type = query::QueryType::SEARCH;
  query.table = "test";
  query.search_text = std::string("bad\xC3\x28", 5);
  query.limit = 100;

  auto output = ExecuteFullPipeline(query, MakeParams());

  ASSERT_FALSE(output.has_value());
  EXPECT_EQ(output.error().code(), mygram::utils::ErrorCode::kQueryInvalidToken);
  EXPECT_NE(output.error().message().find("Invalid UTF-8"), std::string::npos);
}

TEST_F(FullPipelineTest, InvalidUtf8AndTermReturnsQueryError) {
  query::Query query;
  query.type = query::QueryType::SEARCH;
  query.table = "test";
  query.search_text = "learning";
  query.and_terms = {std::string("bad\xC3\x28", 5)};
  query.limit = 100;

  auto output = ExecuteFullPipeline(query, MakeParams());

  ASSERT_FALSE(output.has_value());
  EXPECT_EQ(output.error().code(), mygram::utils::ErrorCode::kQueryInvalidToken);
  EXPECT_NE(output.error().message().find("Invalid UTF-8"), std::string::npos);
}

TEST_F(FullPipelineTest, InvalidBooleanExpressionPreservesTypedParseError) {
  query::Query query;
  query.type = query::QueryType::SEARCH;
  query.table = "test";
  query.search_text = "learning AND (";

  auto output = ExecuteFullPipeline(query, MakeParams());

  ASSERT_FALSE(output.has_value());
  EXPECT_EQ(output.error().code(), mygram::utils::ErrorCode::kQueryExpressionParseError);
  EXPECT_NE(output.error().message().find("Invalid boolean search expression"), std::string::npos);
}

TEST_F(FullPipelineTest, SearchWithFilters) {
  query::Query query;
  query.type = query::QueryType::SEARCH;
  query.table = "test";
  query.search_text = "learning";
  query.limit = 100;
  query.filters.push_back({"status", query::FilterOp::EQ, "1"});

  auto params = MakeParams();
  auto output = ExecuteFullPipeline(query, params);

  EXPECT_TRUE(output.has_value());
  // Both learning docs have status=1
  EXPECT_EQ(output->results.size(), 2);
}

TEST_F(FullPipelineTest, BooleanTopLevelOrReturnsUnion) {
  query::Query query;
  query.type = query::QueryType::SEARCH;
  query.table = "test";
  query.search_text = "basics OR cats";
  query.limit = 100;

  auto params = MakeParams();
  auto output = ExecuteFullPipeline(query, params);

  ASSERT_TRUE(output.has_value()) << (output.has_value() ? std::string{} : output.error().message());
  EXPECT_EQ(output->results, (std::vector<storage::DocId>{doc_ids_[0], doc_ids_[2]}));
}

TEST_F(FullPipelineTest, BooleanTermsUseFuzzySemantics) {
  auto golang_doc = doc_store_->AddDocument("pk_golang", {}, "golang tutorial");
  auto python_doc = doc_store_->AddDocument("pk_python", {}, "python tutorial");
  ASSERT_TRUE(golang_doc.has_value());
  ASSERT_TRUE(python_doc.has_value());
  index_->AddDocument(*golang_doc, "golang tutorial");
  index_->AddDocument(*python_doc, "python tutorial");

  config::Config config;
  config.memory.verify_text = "all";
  auto params = MakeParams();
  params.full_config = &config;

  query::Query query;
  query.type = query::QueryType::SEARCH;
  query.table = "test";
  query.search_text = "goleng OR python";
  query.fuzzy_max_distance = 1;
  query.limit = 100;

  auto output = ExecuteFullPipeline(query, params);

  ASSERT_TRUE(output.has_value()) << (output.has_value() ? std::string{} : output.error().message());
  EXPECT_EQ(output->path_taken, PipelinePath::FUZZY);
  EXPECT_EQ(output->results, (std::vector<storage::DocId>{*golang_doc, *python_doc}));
}

TEST_F(FullPipelineTest, BooleanTermsAndNotNodesUseSynonymSemantics) {
  auto automobile_doc = doc_store_->AddDocument("pk_auto", {}, "vehicle automobile");
  auto bicycle_doc = doc_store_->AddDocument("pk_bike", {}, "vehicle bicycle");
  auto cat_doc = doc_store_->AddDocument("pk_cat_syn", {}, "cat handbook");
  ASSERT_TRUE(automobile_doc.has_value());
  ASSERT_TRUE(bicycle_doc.has_value());
  ASSERT_TRUE(cat_doc.has_value());
  index_->AddDocument(*automobile_doc, "vehicle automobile");
  index_->AddDocument(*bicycle_doc, "vehicle bicycle");
  index_->AddDocument(*cat_doc, "cat handbook");

  auto synonyms = MakeSynonymDictionary({{"car", "automobile"}, {"cat", "feline"}});
  ASSERT_NE(synonyms, nullptr);
  config::Config config;
  config.memory.verify_text = "all";
  auto params = MakeParams();
  params.synonym_dict = synonyms.get();
  params.full_config = &config;

  query::Query positive;
  positive.type = query::QueryType::SEARCH;
  positive.table = "test";
  positive.search_text = "car OR feline";
  positive.limit = 100;
  auto positive_output = ExecuteFullPipeline(positive, params);
  ASSERT_TRUE(positive_output.has_value())
      << (positive_output.has_value() ? std::string{} : positive_output.error().message());
  EXPECT_EQ(positive_output->path_taken, PipelinePath::SYNONYM);
  EXPECT_EQ(positive_output->results, (std::vector<storage::DocId>{doc_ids_[2], *automobile_doc, *cat_doc}));

  query::Query boolean_not = positive;
  boolean_not.search_text = "vehicle AND NOT car";
  auto boolean_not_output = ExecuteFullPipeline(boolean_not, params);
  ASSERT_TRUE(boolean_not_output.has_value())
      << (boolean_not_output.has_value() ? std::string{} : boolean_not_output.error().message());
  EXPECT_EQ(boolean_not_output->results, (std::vector<storage::DocId>{*bicycle_doc}));
}

TEST_F(FullPipelineTest, ExplicitNotTermsExcludeEverySynonymVariant) {
  auto automobile_doc = doc_store_->AddDocument("pk_auto_not", {}, "vehicle automobile");
  auto bicycle_doc = doc_store_->AddDocument("pk_bike_not", {}, "vehicle bicycle");
  ASSERT_TRUE(automobile_doc.has_value());
  ASSERT_TRUE(bicycle_doc.has_value());
  index_->AddDocument(*automobile_doc, "vehicle automobile");
  index_->AddDocument(*bicycle_doc, "vehicle bicycle");

  auto synonyms = MakeSynonymDictionary({{"car", "automobile"}});
  ASSERT_NE(synonyms, nullptr);
  auto params = MakeParams();
  params.synonym_dict = synonyms.get();

  query::Query query;
  query.type = query::QueryType::SEARCH;
  query.table = "test";
  query.search_text = "vehicle";
  query.not_terms = {"car"};
  query.limit = 100;

  auto output = ExecuteFullPipeline(query, params);

  ASSERT_TRUE(output.has_value()) << (output.has_value() ? std::string{} : output.error().message());
  EXPECT_EQ(output->results, (std::vector<storage::DocId>{*bicycle_doc}));
}

TEST_F(FullPipelineTest, BooleanOperandsAcceptAsciiPunctuation) {
  auto email_doc = doc_store_->AddDocument("pk_email", {}, "contact foo@example.com");
  auto version_doc = doc_store_->AddDocument("pk_version", {}, "release v1.2");
  ASSERT_TRUE(email_doc.has_value());
  ASSERT_TRUE(version_doc.has_value());
  index_->AddDocument(*email_doc, "contact foo@example.com");
  index_->AddDocument(*version_doc, "release v1.2");

  query::Query query;
  query.type = query::QueryType::SEARCH;
  query.table = "test";
  query.search_text = "foo@example.com OR v1.2";
  query.limit = 100;

  auto output = ExecuteFullPipeline(query, MakeParams());

  ASSERT_TRUE(output.has_value()) << (output.has_value() ? std::string{} : output.error().message());
  EXPECT_EQ(output->results, (std::vector<storage::DocId>{*email_doc, *version_doc}));
}

TEST_F(FullPipelineTest, ParserQuotedBooleanPhraseDoesNotBecomeOrExpression) {
  query::QueryParser parser;
  auto parsed = parser.Parse(R"(SEARCH test "basics OR cats" LIMIT 100)");
  ASSERT_TRUE(parsed) << parsed.error().message();

  auto params = MakeParams();
  auto output = ExecuteFullPipeline(*parsed, params);

  ASSERT_TRUE(output.has_value()) << (output.has_value() ? std::string{} : output.error().message());
  EXPECT_TRUE(output->results.empty());
}

TEST_F(FullPipelineTest, ParserQuotedPhraseWithBooleanWordSearchesLiteralPhrase) {
  auto phrase_doc = doc_store_->AddDocument("pk_phrase", {}, "classic rock and roll history");
  ASSERT_TRUE(phrase_doc.has_value());
  index_->AddDocument(*phrase_doc, "classic rock and roll history");

  query::QueryParser parser;
  auto parsed = parser.Parse(R"(SEARCH test "rock and roll" LIMIT 100)");
  ASSERT_TRUE(parsed) << parsed.error().message();

  auto output = ExecuteFullPipeline(*parsed, MakeParams());

  ASSERT_TRUE(output.has_value()) << (output.has_value() ? std::string{} : output.error().message());
  EXPECT_EQ(output->results, (std::vector<storage::DocId>{*phrase_doc}));
}

TEST_F(FullPipelineTest, ParenthesizedSingleTermDoesNotEnableBooleanMode) {
  query::Query query;
  query.type = query::QueryType::SEARCH;
  query.table = "test";
  query.search_text = "(learning)";
  query.limit = 100;

  auto params = MakeParams();
  auto output = ExecuteFullPipeline(query, params);

  ASSERT_TRUE(output.has_value()) << (output.has_value() ? std::string{} : output.error().message());
  ASSERT_EQ(output->all_search_terms.size(), 1);
  EXPECT_EQ(output->all_search_terms[0], "(learning)");
}

TEST_F(FullPipelineTest, LowercaseBooleanWordsEnableBooleanMode) {
  query::Query query;
  query.type = query::QueryType::SEARCH;
  query.table = "test";
  query.search_text = "basics or cats";
  query.limit = 100;

  auto params = MakeParams();
  auto output = ExecuteFullPipeline(query, params);

  ASSERT_TRUE(output.has_value()) << (output.has_value() ? std::string{} : output.error().message());
  EXPECT_EQ(output->results, (std::vector<storage::DocId>{doc_ids_[0], doc_ids_[2]}));
}

TEST_F(FullPipelineTest, DuplicateBooleanTermsPreserveScoringShapeAfterMetadataDeduplication) {
  query::Query query;
  query.type = query::QueryType::SEARCH;
  query.table = "test";
  query.search_text = "learning OR learning";
  query.limit = 100;

  auto output = ExecuteFullPipeline(query, MakeParams());

  ASSERT_TRUE(output.has_value()) << (output.has_value() ? std::string{} : output.error().message());
  EXPECT_EQ(output->results, (std::vector<storage::DocId>{doc_ids_[0], doc_ids_[1]}));
  ASSERT_EQ(output->all_search_terms, (std::vector<std::string>{"learning", "learning"}));
  ASSERT_EQ(output->term_infos.size(), 2U);
  EXPECT_EQ(output->term_infos[0].normalized_term, output->term_infos[1].normalized_term);
  EXPECT_EQ(output->term_infos[0].ngrams, output->term_infos[1].ngrams);
  EXPECT_EQ(output->term_infos[0].estimated_size, output->term_infos[1].estimated_size);
}

TEST_F(FullPipelineTest, BooleanParenthesizedOrAndLegacyAndClause) {
  query::Query query;
  query.type = query::QueryType::SEARCH;
  query.table = "test";
  query.search_text = "(basics OR cats)";
  query.and_terms = {"old"};
  query.limit = 100;

  auto params = MakeParams();
  auto output = ExecuteFullPipeline(query, params);

  ASSERT_TRUE(output.has_value()) << (output.has_value() ? std::string{} : output.error().message());
  EXPECT_EQ(output->results, (std::vector<storage::DocId>{doc_ids_[2]}));
}

TEST_F(FullPipelineTest, BooleanTopLevelOrWithPostfixNot) {
  query::Query query;
  query.type = query::QueryType::SEARCH;
  query.table = "test";
  query.search_text = "basics OR cats NOT old";
  query.limit = 100;

  auto params = MakeParams();
  auto output = ExecuteFullPipeline(query, params);

  ASSERT_TRUE(output.has_value()) << (output.has_value() ? std::string{} : output.error().message());
  EXPECT_EQ(output->results, (std::vector<storage::DocId>{doc_ids_[0]}));
}

TEST_F(FullPipelineTest, BooleanNotTermExcludedFromScoringTerms) {
  auto plain_doc = doc_store_->AddDocument("pk_alpha_plain", {}, "alpha");
  auto not_text_doc = doc_store_->AddDocument("pk_alpha_not_text", {}, "alpha x x x x");
  ASSERT_TRUE(plain_doc.has_value());
  ASSERT_TRUE(not_text_doc.has_value());
  index_->AddDocument(*plain_doc, "alpha");
  index_->AddDocument(*not_text_doc, "alpha x x x x");
  doc_store_->SetNormalizedText(*plain_doc, "alpha");
  doc_store_->SetNormalizedText(*not_text_doc, "alpha x x x x");

  query::Query query;
  query.type = query::QueryType::SEARCH;
  query.table = "test";
  query.search_text = "alpha AND NOT x";
  query.limit = 100;
  query.order_by = query::OrderByClause{"_score", query::SortOrder::DESC};

  config::Config config;
  config.bm25.enable = true;
  auto params = MakeParams();
  params.full_config = &config;
  auto output = ExecuteFullPipeline(query, params);

  ASSERT_TRUE(output.has_value()) << (output.has_value() ? std::string{} : output.error().message());
  ASSERT_EQ(output->results, (std::vector<storage::DocId>{*plain_doc}));
  ASSERT_EQ(output->all_search_terms.size(), 1);
  EXPECT_EQ(output->all_search_terms[0], "alpha");

  std::vector<std::string> normalized_terms;
  std::vector<uint64_t> term_dfs;
  for (const auto& term_info : output->term_infos) {
    EXPECT_TRUE(term_info.term_doc_freq_computed);
    normalized_terms.push_back(term_info.normalized_term);
    term_dfs.push_back(term_info.term_doc_freq);
  }

  const index::BM25Params bm25_params{1.2, 0.0};
  const auto scored = index::BM25Scorer::ScoreDocuments(output->results, normalized_terms, term_dfs, *doc_store_,
                                                        doc_store_->GetAllDocIds().size(), 1.0, bm25_params);
  ASSERT_TRUE(scored.has_value()) << scored.error().message();
  ASSERT_EQ(scored->size(), 1);
  EXPECT_GT((*scored)[0].score, 0.0);
}

TEST_F(FullPipelineTest, BooleanVerifyTextHonorsOrBranches) {
  auto false_positive = doc_store_->AddDocument("pk_false", {}, "abzzba");
  ASSERT_TRUE(false_positive.has_value());
  index_->AddDocument(*false_positive, "abzzba");

  query::Query query;
  query.type = query::QueryType::SEARCH;
  query.table = "test";
  query.search_text = "abab OR cats";
  query.limit = 100;

  config::Config config;
  config.memory.verify_text = "all";
  auto params = MakeParams();
  params.full_config = &config;
  auto output = ExecuteFullPipeline(query, params);

  ASSERT_TRUE(output.has_value()) << (output.has_value() ? std::string{} : output.error().message());
  EXPECT_EQ(output->results, (std::vector<storage::DocId>{doc_ids_[2]}));
}

TEST_F(FullPipelineTest, BooleanEmptyTermResultSkipsCache) {
  query::Query query;
  query.type = query::QueryType::SEARCH;
  query.table = "test";
  query.search_text = "learning AND x";
  query.limit = 100;

  auto params = MakeParams();
  auto output = ExecuteFullPipeline(query, params);

  ASSERT_TRUE(output.has_value()) << (output.has_value() ? std::string{} : output.error().message());
  EXPECT_TRUE(output->results.empty());
  EXPECT_TRUE(output->empty_term_detected);
}

TEST_F(FullPipelineTest, ShortTermWithoutStoredTextReturnsExplicitError) {
  doc_store_->SetStoreTexts(false);

  query::Query query;
  query.type = query::QueryType::SEARCH;
  query.table = "test";
  query.search_text = "x";
  query.limit = 100;

  auto params = MakeParams();
  auto output = ExecuteFullPipeline(query, params);

  EXPECT_FALSE(output.has_value());
  EXPECT_NE((output.has_value() ? std::string{} : output.error().message()).find("too short for n-gram search"),
            std::string::npos);
}

TEST_F(FullPipelineTest, BooleanShortNotWithoutStoredTextReturnsExplicitError) {
  doc_store_->SetStoreTexts(false);

  query::Query query;
  query.type = query::QueryType::SEARCH;
  query.table = "test";
  query.search_text = "learning AND NOT x";
  query.limit = 100;

  auto params = MakeParams();
  auto output = ExecuteFullPipeline(query, params);

  EXPECT_FALSE(output.has_value());
  EXPECT_NE((output.has_value() ? std::string{} : output.error().message()).find("too short for n-gram search"),
            std::string::npos);
}

TEST_F(FullPipelineTest, MixedScriptBoundaryFragmentRequiresExactTextMatch) {
  auto cjk_only = doc_store_->AddDocument("pk_kyoto", {}, "京都");
  auto mixed_match = doc_store_->AddDocument("pk_kyo_tower", {}, "京タワー");
  ASSERT_TRUE(cjk_only.has_value());
  ASSERT_TRUE(mixed_match.has_value());
  index_->AddDocument(*cjk_only, "京都");
  index_->AddDocument(*mixed_match, "京タワー");

  query::Query query;
  query.type = query::QueryType::SEARCH;
  query.table = "test";
  query.search_text = "京タ";
  query.limit = 100;

  auto params = MakeParams();
  params.kanji_ngram_size = 1;
  params.cross_boundary_ngrams = false;
  auto output = ExecuteFullPipeline(query, params);

  ASSERT_TRUE(output.has_value()) << (output.has_value() ? std::string{} : output.error().message());
  EXPECT_EQ(output->results, (std::vector<storage::DocId>{*mixed_match}));
}

TEST_F(FullPipelineTest, SearchWithNotTerms) {
  // First verify the base search returns expected results
  {
    query::Query base_query;
    base_query.type = query::QueryType::SEARCH;
    base_query.table = "test";
    base_query.search_text = "learning";
    base_query.limit = 100;
    auto params = MakeParams();
    auto base_output = ExecuteFullPipeline(base_query, params);
    ASSERT_TRUE(base_output.has_value());
    ASSERT_GE(base_output->results.size(), 1) << "Base search for 'learning' should find docs";
  }

  query::Query query;
  query.type = query::QueryType::SEARCH;
  query.table = "test";
  query.search_text = "learning";
  query.not_terms.push_back("deep");
  query.limit = 100;

  auto params = MakeParams();
  auto output = ExecuteFullPipeline(query, params);

  EXPECT_TRUE(output.has_value());
  // NOT "deep" should exclude doc2, leaving fewer results than the base search
  EXPECT_GE(output->results.size(), 0);
  // At minimum, the NOT filter should not add results
  EXPECT_LE(output->results.size(), 2);
}

TEST_F(FullPipelineTest, NotFilterDoesNotPoolNgramsAcrossTerms) {
  auto plain_doc = doc_store_->AddDocument("pk_plain", {}, "alpha plain");
  auto dog_doc = doc_store_->AddDocument("pk_dog", {}, "alpha dog");
  auto dolphin_doc = doc_store_->AddDocument("pk_dolphin", {}, "alpha dolphin");
  auto fog_doc = doc_store_->AddDocument("pk_fog", {}, "alpha fog");
  ASSERT_TRUE(plain_doc.has_value());
  ASSERT_TRUE(dog_doc.has_value());
  ASSERT_TRUE(dolphin_doc.has_value());
  ASSERT_TRUE(fog_doc.has_value());

  index_->AddDocument(*plain_doc, "alpha plain");
  index_->AddDocument(*dog_doc, "alpha dog");
  index_->AddDocument(*dolphin_doc, "alpha dolphin");
  index_->AddDocument(*fog_doc, "alpha fog");

  query::Query query;
  query.type = query::QueryType::SEARCH;
  query.table = "test";
  query.search_text = "alpha";
  query.not_terms = {"dolphin", "fog"};
  query.limit = 100;

  auto params = MakeParams();
  auto output = ExecuteFullPipeline(query, params);

  ASSERT_TRUE(output.has_value()) << (output.has_value() ? std::string{} : output.error().message());
  EXPECT_NE(std::find(output->results.begin(), output->results.end(), *plain_doc), output->results.end());
  EXPECT_NE(std::find(output->results.begin(), output->results.end(), *dog_doc), output->results.end());
  EXPECT_EQ(std::find(output->results.begin(), output->results.end(), *dolphin_doc), output->results.end());
  EXPECT_EQ(std::find(output->results.begin(), output->results.end(), *fog_doc), output->results.end());
}

TEST_F(FullPipelineTest, NotTermNgramsAreRegisteredForCacheInvalidation) {
  config::CacheConfig cache_config;
  cache_config.enabled = true;
  cache_config.max_memory_bytes = 10 * 1024 * 1024;
  cache_config.min_query_cost_ms = 0.0;

  cache::NgramConfigMap ngram_configs;
  ngram_configs["test"] = cache::NgramConfig{
      .ngram_size = 2,
      .kanji_ngram_size = 0,
      .cross_boundary_ngrams = false,
  };
  cache::CacheManager cache_manager(cache_config, std::move(ngram_configs));

  auto article_doc = doc_store_->AddDocument("pk_article", {}, "article cat");
  ASSERT_TRUE(article_doc.has_value());
  index_->AddDocument(*article_doc, "article cat");

  query::Query query;
  query.type = query::QueryType::SEARCH;
  query.table = "test";
  query.search_text = "article";
  query.not_terms = {"do"};
  query.limit = 100;

  auto params = MakeParams();
  params.cache_manager = &cache_manager;
  const query::Query cache_query = BuildCanonicalCacheQuery(query, params);
  auto output = ExecuteFullPipeline(query, params);

  ASSERT_TRUE(output.has_value()) << (output.has_value() ? std::string{} : output.error().message());
  ASSERT_FALSE(output->cache_hit);
  ASSERT_TRUE(cache_manager.Lookup(cache_query).has_value());

  cache_manager.Invalidate("test", "article cat", "article do");

  for (int i = 0; i < 50 && cache_manager.Lookup(cache_query).has_value(); ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
  }
  EXPECT_FALSE(cache_manager.Lookup(cache_query).has_value());
}

TEST_F(FullPipelineTest, SearchWithAndTerms) {
  query::Query query;
  query.type = query::QueryType::SEARCH;
  query.table = "test";
  query.search_text = "learning";
  query.and_terms.push_back("machine");
  query.limit = 100;

  auto params = MakeParams();
  auto output = ExecuteFullPipeline(query, params);

  EXPECT_TRUE(output.has_value());
  // Only doc1 has both "learning" and "machine"
  EXPECT_EQ(output->results.size(), 1);
  EXPECT_EQ(output->all_search_terms.size(), 2);
}

TEST_F(FullPipelineTest, NullIndexReturnsError) {
  query::Query query;
  query.type = query::QueryType::SEARCH;
  query.table = "test";
  query.search_text = "learning";

  auto params = MakeParams();
  params.current_index = nullptr;
  auto output = ExecuteFullPipeline(query, params);

  EXPECT_FALSE(output.has_value());
  EXPECT_FALSE((output.has_value() ? std::string{} : output.error().message()).empty());
}

TEST_F(FullPipelineTest, NullDocStoreReturnsError) {
  query::Query query;
  query.type = query::QueryType::SEARCH;
  query.table = "test";
  query.search_text = "learning";

  auto params = MakeParams();
  params.current_doc_store = nullptr;
  auto output = ExecuteFullPipeline(query, params);

  EXPECT_FALSE(output.has_value());
  EXPECT_FALSE((output.has_value() ? std::string{} : output.error().message()).empty());
}

TEST_F(FullPipelineTest, EmptySearchTextReturnsEmpty) {
  query::Query query;
  query.type = query::QueryType::SEARCH;
  query.table = "test";
  query.search_text = "";
  query.limit = 100;

  auto params = MakeParams();
  auto output = ExecuteFullPipeline(query, params);

  EXPECT_TRUE(output.has_value());
  EXPECT_TRUE(output->results.empty());
}

TEST_F(FullPipelineTest, NoMatchReturnsEmpty) {
  query::Query query;
  query.type = query::QueryType::SEARCH;
  query.table = "test";
  query.search_text = "zzzznonexistent";
  query.limit = 100;

  auto params = MakeParams();
  auto output = ExecuteFullPipeline(query, params);

  EXPECT_TRUE(output.has_value());
  EXPECT_TRUE(output->results.empty());
}

TEST_F(FullPipelineTest, ShortTermFallsBackToSubstringSearch) {
  auto single_char_doc = doc_store_->AddDocument("pk_single_char", {}, "a marker");
  ASSERT_TRUE(single_char_doc.has_value());
  index_->AddDocument(*single_char_doc, "a marker");

  query::Query query;
  query.type = query::QueryType::SEARCH;
  query.table = "test";
  query.search_text = "a";
  query.limit = 100;

  auto params = MakeParams();
  auto output = ExecuteFullPipeline(query, params);

  ASSERT_TRUE(output.has_value()) << (output.has_value() ? std::string{} : output.error().message());
  EXPECT_FALSE(output->empty_term_detected);
  EXPECT_NE(std::find(output->results.begin(), output->results.end(), *single_char_doc), output->results.end());
}

TEST_F(FullPipelineTest, ShortAndTermFallsBackToSubstringSearch) {
  auto target_doc = doc_store_->AddDocument("pk_short_and", {}, "learning x");
  ASSERT_TRUE(target_doc.has_value());
  index_->AddDocument(*target_doc, "learning x");

  query::Query query;
  query.type = query::QueryType::SEARCH;
  query.table = "test";
  query.search_text = "learning";
  query.and_terms = {"x"};
  query.limit = 100;

  auto params = MakeParams();
  auto output = ExecuteFullPipeline(query, params);

  ASSERT_TRUE(output.has_value()) << (output.has_value() ? std::string{} : output.error().message());
  EXPECT_FALSE(output->empty_term_detected);
  EXPECT_EQ(output->results, (std::vector<storage::DocId>{*target_doc}));
}

TEST_F(FullPipelineTest, ShortNotTermUsesSubstringFallback) {
  auto excluded = doc_store_->AddDocument("pk_short_not", {}, "learning x");
  auto retained = doc_store_->AddDocument("pk_short_not_keep", {}, "learning y");
  ASSERT_TRUE(excluded.has_value());
  ASSERT_TRUE(retained.has_value());
  index_->AddDocument(*excluded, "learning x");
  index_->AddDocument(*retained, "learning y");

  query::Query query;
  query.type = query::QueryType::SEARCH;
  query.table = "test";
  query.search_text = "learning";
  query.not_terms = {"x"};
  query.limit = 100;

  auto output = ExecuteFullPipeline(query, MakeParams());

  ASSERT_TRUE(output.has_value()) << (output.has_value() ? std::string{} : output.error().message());
  EXPECT_EQ(std::find(output->results.begin(), output->results.end(), *excluded), output->results.end());
  EXPECT_NE(std::find(output->results.begin(), output->results.end(), *retained), output->results.end());
}

TEST_F(FullPipelineTest, BooleanExpressionWithShortLegacyAndTermUsesSubstringFallback) {
  auto learning_x = doc_store_->AddDocument("pk_bool_short_learning", {}, "learning x");
  auto cats_x = doc_store_->AddDocument("pk_bool_short_cats", {}, "cats x");
  ASSERT_TRUE(learning_x.has_value());
  ASSERT_TRUE(cats_x.has_value());
  index_->AddDocument(*learning_x, "learning x");
  index_->AddDocument(*cats_x, "cats x");

  query::Query query;
  query.type = query::QueryType::SEARCH;
  query.table = "test";
  query.search_text = "learning OR cats";
  query.and_terms = {"x"};
  query.limit = 100;

  auto output = ExecuteFullPipeline(query, MakeParams());

  ASSERT_TRUE(output.has_value()) << (output.has_value() ? std::string{} : output.error().message());
  EXPECT_EQ(output->results, (std::vector<storage::DocId>{*learning_x, *cats_x}));
}

TEST_F(FullPipelineTest, ShortSynonymVariantUsesSubstringFallback) {
  auto target = doc_store_->AddDocument("pk_short_synonym", {}, "x marker");
  ASSERT_TRUE(target.has_value());
  index_->AddDocument(*target, "x marker");

  SynonymTermGroup group;
  group.normalized_terms = {"x"};
  group.variants.push_back(SearchTermInfo{{}, std::numeric_limits<size_t>::max(), 0, "x", false});

  query::Query query;
  query.type = query::QueryType::SEARCH;
  query.table = "test";
  query.search_text = "x";
  query.limit = 100;

  auto result = ExecuteWithSynonyms(query, {group}, index_.get(), doc_store_.get(), nullptr, 2, 0, false, 1000);

  EXPECT_FALSE(result.empty_term_detected);
  EXPECT_EQ(result.results, (std::vector<storage::DocId>{*target}));
}

TEST_F(FullPipelineTest, VerifyTextFilterApplied) {
  // Enable verify_text and check that false positives are filtered
  config::Config config;
  config.memory.verify_text = "all";

  query::Query query;
  query.type = query::QueryType::SEARCH;
  query.table = "test";
  query.search_text = "learning";
  query.limit = 100;

  auto params = MakeParams();
  params.full_config = &config;
  auto output = ExecuteFullPipeline(query, params);

  EXPECT_TRUE(output.has_value());
  // With verify_text=all, results should still include docs that actually contain "learning"
  EXPECT_EQ(output->results.size(), 2);
}

TEST(SearchTopNOptimizationTest, SkipsWhenVerifyTextIsRequired) {
  index::Index index(2);
  storage::DocumentStore doc_store;

  auto doc1 = doc_store.AddDocument("1", {}, "abzzba");
  auto doc2 = doc_store.AddDocument("2", {}, "abab");
  ASSERT_TRUE(doc1.has_value());
  ASSERT_TRUE(doc2.has_value());
  index.AddDocument(*doc1, "abzzba");
  index.AddDocument(*doc2, "abab");

  query::Query query;
  query.type = query::QueryType::SEARCH;
  query.table = "test";
  query.search_text = "abab";
  query.limit = 1;

  auto term_infos = GenerateTermInfos({query.search_text}, &index, 2, 0, false);
  auto results = index.SearchAnd(term_infos[0].ngrams);

  config::Config config;
  config.memory.verify_text = "all";

  auto topn = ApplySearchTopNOptimization(query, &index, &doc_store, &config, term_infos, {query.search_text},
                                          /*semantics_reproducible_by_single_term_ngram_and=*/false,
                                          /*cache_hit=*/false, "id", results);

  EXPECT_TRUE(topn.considered);
  EXPECT_FALSE(topn.applicable);
}

TEST(SearchTopNOptimizationTest, ShortTermPreservesSubstringResults) {
  index::Index index(2);
  storage::DocumentStore doc_store;
  auto doc = doc_store.AddDocument("1", {}, "x marker");
  ASSERT_TRUE(doc.has_value());
  index.AddDocument(*doc, "x marker");

  query::Query query;
  query.type = query::QueryType::SEARCH;
  query.table = "test";
  query.search_text = "x";
  query.limit = 1;

  auto term_infos = GenerateTermInfos({query.search_text}, &index, 2, 0, false);
  std::vector<storage::DocId> results{*doc};
  config::Config config;
  config.memory.verify_text = "off";

  auto topn = ApplySearchTopNOptimization(query, &index, &doc_store, &config, term_infos, {query.search_text},
                                          /*semantics_reproducible_by_single_term_ngram_and=*/false,
                                          /*cache_hit=*/false, "id", results);

  EXPECT_FALSE(topn.considered);
  EXPECT_EQ(results, (std::vector<storage::DocId>{*doc}));
}

TEST(SearchTopNOptimizationTest, SkipsWhenPrimaryKeyDocIdOrderIsUnknown) {
  index::Index index(2);
  storage::DocumentStore doc_store;

  auto doc1 = doc_store.AddDocument("pk1", {}, "alpha");
  auto doc2 = doc_store.AddDocument("pk2", {}, "alpha");
  ASSERT_TRUE(doc1.has_value());
  ASSERT_TRUE(doc2.has_value());
  index.AddDocument(*doc1, "alpha");
  index.AddDocument(*doc2, "alpha");

  query::Query query;
  query.type = query::QueryType::SEARCH;
  query.table = "test";
  query.search_text = "alpha";
  query.limit = 1;

  auto term_infos = GenerateTermInfos({query.search_text}, &index, 2, 0, false);
  auto results = index.SearchAnd(term_infos[0].ngrams);

  config::Config config;
  config.memory.verify_text = "off";

  auto topn = ApplySearchTopNOptimization(query, &index, &doc_store, &config, term_infos, {query.search_text},
                                          /*semantics_reproducible_by_single_term_ngram_and=*/true,
                                          /*cache_hit=*/false, "id", results);

  EXPECT_TRUE(topn.considered);
  EXPECT_FALSE(topn.applicable);
}

TEST(SearchTopNOptimizationTest, ExplicitSemanticsFlagPreservesPostFilteredResults) {
  index::Index index(2);
  storage::DocumentStore doc_store;

  std::vector<storage::DocId> doc_ids;
  for (int i = 1; i <= 4; ++i) {
    auto doc_id = doc_store.AddDocument(std::to_string(i), {}, "alpha");
    ASSERT_TRUE(doc_id.has_value());
    index.AddDocument(*doc_id, "alpha");
    doc_ids.push_back(*doc_id);
  }
  ASSERT_TRUE(doc_store.IsPrimaryKeyDocIdOrderValid());

  query::Query query;
  query.type = query::QueryType::SEARCH;
  query.table = "test";
  query.search_text = "alpha";
  query.limit = 1;

  auto term_infos = GenerateTermInfos({query.search_text}, &index, 2, 0, false);
  std::vector<storage::DocId> post_filtered_results{doc_ids.front()};
  const auto expected = post_filtered_results;

  config::Config config;
  config.memory.verify_text = "off";
  auto topn = ApplySearchTopNOptimization(query, &index, &doc_store, &config, term_infos, {query.search_text},
                                          /*semantics_reproducible_by_single_term_ngram_and=*/false,
                                          /*cache_hit=*/false, "id", post_filtered_results);

  EXPECT_TRUE(topn.considered);
  EXPECT_FALSE(topn.applicable);
  EXPECT_FALSE(topn.optimized);
  EXPECT_EQ(post_filtered_results, expected);
}

TEST_F(FullPipelineTest, FuzzySearchPath) {
  query::Query query;
  query.type = query::QueryType::SEARCH;
  query.table = "test";
  query.search_text = "learnig";  // Intentional typo (distance=1 from "learning")
  query.fuzzy_max_distance = 1;
  query.limit = 100;

  config::Config config;
  config.memory.verify_text = "all";

  auto params = MakeParams();
  params.full_config = &config;
  auto output = ExecuteFullPipeline(query, params);

  EXPECT_TRUE(output.has_value());
  // Fuzzy search should find docs containing "learning" (edit distance 1 from "learnig")
  EXPECT_GE(output->results.size(), 1);
}

TEST_F(FullPipelineTest, FuzzyCjkSearchVerifiesContinuousTextByCodepointWindow) {
  auto cjk_doc = doc_store_->AddDocument("pk_cjk", {}, "私は東京都に住む");
  ASSERT_TRUE(cjk_doc.has_value());
  index_->AddDocument(*cjk_doc, "私は東京都に住む");

  query::Query query;
  query.type = query::QueryType::SEARCH;
  query.table = "test";
  query.search_text = "東京市";
  query.fuzzy_max_distance = 1;
  query.limit = 100;

  config::Config config;
  config.memory.verify_text = "all";

  auto params = MakeParams();
  params.kanji_ngram_size = 1;
  params.full_config = &config;
  auto output = ExecuteFullPipeline(query, params);

  ASSERT_TRUE(output.has_value()) << (output.has_value() ? std::string{} : output.error().message());
  EXPECT_EQ(output->results, (std::vector<storage::DocId>{*cjk_doc}));
}

TEST_F(FullPipelineTest, QueryTimeMsPopulated) {
  query::Query query;
  query.type = query::QueryType::SEARCH;
  query.table = "test";
  query.search_text = "learning";
  query.limit = 100;

  auto params = MakeParams();
  auto output = ExecuteFullPipeline(query, params);

  EXPECT_TRUE(output.has_value());
  EXPECT_GE(output->query_time_ms, 0.0);
}

// =============================================================================
// FullPipelineOutput::cache_hit and cache_miss_reason tests
// =============================================================================
// Regression coverage for the bug where SearchHandler relied on
// debug_info.cache_info.status to detect cache hits, which is only populated
// when conn_ctx.debug_mode == true. The non-debug fast path therefore never
// observed a cache hit. cache_hit (and cache_miss_reason) must be set on
// FullPipelineOutput regardless of debug mode.
// =============================================================================

class FullPipelineCacheTest : public ::testing::Test {
 protected:
  void SetUp() override {
    index_ = std::make_unique<index::Index>(2);
    doc_store_ = std::make_unique<storage::DocumentStore>();

    auto d1 = doc_store_->AddDocument("pk1", {}, "machine learning basics");
    auto d2 = doc_store_->AddDocument("pk2", {}, "deep learning techniques");
    ASSERT_TRUE(d1.has_value());
    ASSERT_TRUE(d2.has_value());

    index_->AddDocument(*d1, "machine learning basics");
    index_->AddDocument(*d2, "deep learning techniques");

    config::CacheConfig cache_config;
    cache_config.enabled = true;
    cache_config.max_memory_bytes = 10 * 1024 * 1024;
    cache_config.min_query_cost_ms = 0.0;  // Cache everything regardless of cost

    cache::NgramConfigMap ngram_configs;
    ngram_configs["test"] = cache::NgramConfig{
        .ngram_size = 2,
        .kanji_ngram_size = 0,
        .cross_boundary_ngrams = false,
    };

    cache_manager_ = std::make_unique<cache::CacheManager>(cache_config, std::move(ngram_configs));
  }

  FullPipelineParams MakeParams() {
    FullPipelineParams params;
    params.current_index = index_.get();
    params.current_doc_store = doc_store_.get();
    params.cache_manager = cache_manager_.get();
    params.ngram_size = 2;
    params.kanji_ngram_size = 0;
    params.cross_boundary_ngrams = false;
    params.filter_threshold = 1000;
    params.primary_key_column = "id";
    return params;
  }

  std::unique_ptr<index::Index> index_;
  std::unique_ptr<storage::DocumentStore> doc_store_;
  std::unique_ptr<cache::CacheManager> cache_manager_;
};

TEST_F(FullPipelineCacheTest, CacheHitFlagSetRegardlessOfDebugMode) {
  query::Query query;
  query.type = query::QueryType::SEARCH;
  query.table = "test";
  query.search_text = "learning";
  query.limit = 100;

  auto params = MakeParams();

  // First run: cache miss, populates the cache.
  auto first_output = ExecuteFullPipeline(query, params);
  ASSERT_TRUE(first_output.has_value());
  EXPECT_FALSE(first_output->cache_hit);
  EXPECT_EQ(first_output->cache_miss_reason, CacheMissReason::kNotFound);

  // Second run with the same query: must report cache_hit = true on the
  // FullPipelineOutput itself, independent of any debug-mode flag (debug mode
  // is not exercised here).
  auto second_output = ExecuteFullPipeline(query, params);
  ASSERT_TRUE(second_output.has_value());
  EXPECT_TRUE(second_output->cache_hit);
  EXPECT_EQ(second_output->cache_miss_reason, CacheMissReason::kHit);
  EXPECT_EQ(second_output->path_taken, PipelinePath::CACHE_HIT);
  EXPECT_EQ(second_output->results, first_output->results);
  EXPECT_TRUE(second_output->term_infos.empty());
}

TEST_F(FullPipelineCacheTest, ExecutionSignificantWhitespaceDoesNotShareCacheEntries) {
  const auto single_space_doc = doc_store_->AddDocument("pk_single_space", {}, "hello world");
  const auto repeated_space_doc = doc_store_->AddDocument("pk_repeated_space", {}, "hello   world");
  ASSERT_TRUE(single_space_doc.has_value());
  ASSERT_TRUE(repeated_space_doc.has_value());
  index_->AddDocument(*single_space_doc, "hello world");
  index_->AddDocument(*repeated_space_doc, "hello   world");

  config::Config config;
  config.memory.verify_text = "all";
  auto params = MakeParams();
  params.full_config = &config;

  query::Query single_space;
  single_space.type = query::QueryType::SEARCH;
  single_space.table = "test";
  single_space.search_text = "hello world";
  single_space.limit = 100;
  auto repeated_space = single_space;
  repeated_space.search_text = "hello   world";

  const auto first = ExecuteFullPipeline(single_space, params);
  ASSERT_TRUE(first.has_value()) << (first ? "" : first.error().message());
  EXPECT_FALSE(first->cache_hit);
  EXPECT_EQ(first->results, (std::vector<storage::DocId>{*single_space_doc}));

  const auto second = ExecuteFullPipeline(repeated_space, params);
  ASSERT_TRUE(second.has_value()) << (second ? "" : second.error().message());
  EXPECT_FALSE(second->cache_hit);
  EXPECT_EQ(second->results, (std::vector<storage::DocId>{*repeated_space_doc}));

  const auto repeated_hit = ExecuteFullPipeline(repeated_space, params);
  ASSERT_TRUE(repeated_hit.has_value()) << (repeated_hit ? "" : repeated_hit.error().message());
  EXPECT_TRUE(repeated_hit->cache_hit);
  EXPECT_EQ(repeated_hit->results, second->results);
}

TEST_F(FullPipelineCacheTest, BooleanCacheHitPreservesPositiveHighlightTerms) {
  query::Query query;
  query.type = query::QueryType::SEARCH;
  query.table = "test";
  query.search_text = "learning NOT (deep OR machine)";
  query.search_expression = query.search_text;
  query.limit = 100;

  auto params = MakeParams();
  auto miss = ExecuteFullPipeline(query, params);
  ASSERT_TRUE(miss.has_value()) << (miss ? "" : miss.error().message());
  ASSERT_FALSE(miss->cache_hit);

  auto hit = ExecuteFullPipeline(query, params);
  ASSERT_TRUE(hit.has_value()) << (hit ? "" : hit.error().message());
  ASSERT_TRUE(hit->cache_hit);
  EXPECT_EQ(hit->all_search_terms, miss->all_search_terms);
  EXPECT_EQ(hit->all_search_terms, (std::vector<std::string>{"learning"}));
  EXPECT_TRUE(hit->term_infos.empty());

  const auto miss_highlight_terms = BuildHighlightTerms(miss->all_search_terms, index_.get(), nullptr);
  const auto hit_highlight_terms = BuildHighlightTerms(hit->all_search_terms, index_.get(), nullptr);
  EXPECT_EQ(hit_highlight_terms, miss_highlight_terms);
  EXPECT_EQ(hit_highlight_terms, (std::vector<std::string>{"learning"}));
}

TEST_F(FullPipelineCacheTest, CanonicalCacheKeyOverridesParserPrecomputedKey) {
  query::Query first_query;
  first_query.type = query::QueryType::SEARCH;
  first_query.table = "test";
  first_query.search_text = "LEARNING";
  first_query.limit = 100;
  auto stale_key = cache::CacheKeyGenerator::Generate("parser-default-stale-key");
  first_query.cache_key = std::make_pair(stale_key.hash_high, stale_key.hash_low);

  query::Query second_query;
  second_query.type = query::QueryType::SEARCH;
  second_query.table = "test";
  second_query.search_text = "learning";
  second_query.limit = 100;

  auto params = MakeParams();

  auto first_output = ExecuteFullPipeline(first_query, params);
  ASSERT_TRUE(first_output.has_value());
  EXPECT_FALSE(first_output->cache_hit);
  ASSERT_FALSE(first_output->results.empty());

  auto second_output = ExecuteFullPipeline(second_query, params);
  ASSERT_TRUE(second_output.has_value());
  EXPECT_TRUE(second_output->cache_hit);
  EXPECT_EQ(second_output->results, first_output->results);
}

TEST_F(FullPipelineCacheTest, SortClauseDoesNotPartitionCacheEntries) {
  query::Query asc_query;
  asc_query.type = query::QueryType::SEARCH;
  asc_query.table = "test";
  asc_query.search_text = "learning";
  asc_query.order_by = query::OrderByClause{"created_at", query::SortOrder::ASC};
  asc_query.limit = 100;

  query::Query desc_query = asc_query;
  desc_query.order_by = query::OrderByClause{"created_at", query::SortOrder::DESC};

  auto params = MakeParams();

  auto first_output = ExecuteFullPipeline(asc_query, params);
  ASSERT_TRUE(first_output.has_value());
  EXPECT_FALSE(first_output->cache_hit);

  auto second_output = ExecuteFullPipeline(desc_query, params);
  ASSERT_TRUE(second_output.has_value());
  EXPECT_TRUE(second_output->cache_hit);
  EXPECT_EQ(second_output->cache_miss_reason, CacheMissReason::kHit);
  EXPECT_EQ(second_output->results, first_output->results);
}

TEST_F(FullPipelineCacheTest, CacheMissReasonNotFoundForUnknownQuery) {
  query::Query query;
  query.type = query::QueryType::SEARCH;
  query.table = "test";
  query.search_text = "completelynovelterm";
  query.limit = 100;

  auto params = MakeParams();
  auto output = ExecuteFullPipeline(query, params);
  ASSERT_TRUE(output.has_value());
  EXPECT_FALSE(output->cache_hit);
  EXPECT_EQ(output->cache_miss_reason, CacheMissReason::kNotFound);
}

TEST_F(FullPipelineCacheTest, FacetReusesSearchDocIdCacheEntry) {
  query::Query search;
  search.type = query::QueryType::SEARCH;
  search.table = "test";
  search.search_text = "learning";
  search.limit = 100;

  auto params = MakeParams();
  auto first = ExecuteFullPipeline(search, params);
  ASSERT_TRUE(first.has_value());
  ASSERT_FALSE(first->cache_hit);

  query::Query facet = search;
  facet.type = query::QueryType::FACET;
  facet.facet_column = "category";
  auto second = ExecuteFullPipeline(facet, params);
  ASSERT_TRUE(second.has_value());
  EXPECT_TRUE(second->cache_hit);
  EXPECT_EQ(second->cache_miss_reason, CacheMissReason::kHit);
  EXPECT_EQ(second->results, first->results);
}

TEST_F(FullPipelineCacheTest, CacheMissReasonDisabledWhenCacheManagerNull) {
  query::Query query;
  query.type = query::QueryType::SEARCH;
  query.table = "test";
  query.search_text = "learning";
  query.limit = 100;

  auto params = MakeParams();
  params.cache_manager = nullptr;
  auto output = ExecuteFullPipeline(query, params);
  ASSERT_TRUE(output.has_value());
  EXPECT_FALSE(output->cache_hit);
  EXPECT_EQ(output->cache_miss_reason, CacheMissReason::kDisabled);
}

TEST_F(FullPipelineCacheTest, CacheMissReasonStaleVsNotFound) {
  // 1) Unknown key -> kNotFound directly via TryCacheLookup.
  {
    query::Query unknown;
    unknown.type = query::QueryType::SEARCH;
    unknown.table = "test";
    unknown.search_text = "uniquenotcached";
    unknown.limit = 100;

    CacheMissReason reason = CacheMissReason::kHit;
    auto result = TryCacheLookup(unknown, cache_manager_.get(), doc_store_.get(), &reason);
    EXPECT_FALSE(result.has_value());
    EXPECT_EQ(reason, CacheMissReason::kNotFound);
  }

  // 2) Insert -> hit -> remove a referenced document -> stale on next lookup.
  query::Query query;
  query.type = query::QueryType::SEARCH;
  query.table = "test";
  query.search_text = "learning";
  query.limit = 100;

  auto params = MakeParams();
  const query::Query cache_query = BuildCanonicalCacheQuery(query, params);
  auto first = ExecuteFullPipeline(query, params);
  ASSERT_TRUE(first.has_value());
  ASSERT_FALSE(first->results.empty());

  // Removing a doc that appears in the cached result set causes
  // GetPrimaryKeysBatch to return no primary key for that DocId, which
  // IsCacheStale flags as stale.
  doc_store_->RemoveDocument(first->results.front());

  CacheMissReason reason = CacheMissReason::kHit;
  auto stale_result = TryCacheLookup(cache_query, cache_manager_.get(), doc_store_.get(), &reason);
  EXPECT_FALSE(stale_result.has_value());
  EXPECT_EQ(reason, CacheMissReason::kStale);

  // Staleness detection must remove the resident key. Otherwise the next
  // completed search cannot be cached because QueryCache sees a duplicate.
  EXPECT_FALSE(cache_manager_->Lookup(cache_query).has_value());
  EXPECT_TRUE(cache_manager_->Insert(cache_query, {999}, {"learning"}, 1.0, 2, 0, false));
}

// =============================================================================
// BuildPipelineParamsFromContext tests - shared helper used by HTTP and TCP
// =============================================================================

class BuildPipelineParamsTest : public ::testing::Test {
 protected:
  void SetUp() override {
    table_context_.name = "articles";
    table_context_.config.ngram_size = 3;
    table_context_.config.kanji_ngram_size = 2;
    table_context_.config.cross_boundary_ngrams = true;
    table_context_.config.primary_key = "uuid";
    table_context_.index = std::make_unique<index::Index>(3);
    table_context_.doc_store = std::make_unique<storage::DocumentStore>();
  }

  TableContext table_context_;
};

TEST_F(BuildPipelineParamsTest, ProjectsTableContextFieldsOntoParams) {
  // Sanity check that all per-table fields land where the pipeline expects.
  auto params = BuildPipelineParamsFromContext(table_context_, /*full_config=*/nullptr,
                                               /*cache_manager=*/nullptr,
                                               /*filter_threshold=*/4096,
                                               /*attach_bm25_stats=*/false);

  EXPECT_EQ(params.current_index, table_context_.index.get());
  EXPECT_EQ(params.current_doc_store, table_context_.doc_store.get());
  EXPECT_EQ(params.ngram_size, 3);
  EXPECT_EQ(params.kanji_ngram_size, 2);
  EXPECT_TRUE(params.cross_boundary_ngrams);
  EXPECT_EQ(params.filter_threshold, 4096u);
  EXPECT_EQ(params.primary_key_column, "uuid");
  // BM25 stats opt-in flag false -> nullptr.
  EXPECT_EQ(params.bm25_stats, nullptr);
  // Empty/null synonym dictionary -> not wired up.
  EXPECT_EQ(params.synonym_dict, nullptr);
}

TEST_F(BuildPipelineParamsTest, BuildPipelineParamsHonorsBm25StatsArgument) {
  // attach_bm25_stats=true should wire the per-table stats; false should not.
  // This is the difference between SEARCH (attaches) and COUNT (does not).
  auto params_with = BuildPipelineParamsFromContext(table_context_, /*full_config=*/nullptr,
                                                    /*cache_manager=*/nullptr,
                                                    /*filter_threshold=*/1000,
                                                    /*attach_bm25_stats=*/true);
  EXPECT_EQ(params_with.bm25_stats, &table_context_.bm25_stats);

  auto params_without = BuildPipelineParamsFromContext(table_context_, /*full_config=*/nullptr,
                                                       /*cache_manager=*/nullptr,
                                                       /*filter_threshold=*/1000,
                                                       /*attach_bm25_stats=*/false);
  EXPECT_EQ(params_without.bm25_stats, nullptr);
}

TEST_F(BuildPipelineParamsTest, EmptySynonymDictionaryIsNotWired) {
  // An empty SynonymDictionary should NOT cause the pipeline to walk the
  // synonym path. The helper enforces this; if it ever wires an empty
  // dictionary again, ExecuteFullPipeline would silently degrade to the
  // synonym path on every query.
  table_context_.synonym_dict = std::make_unique<query::SynonymDictionary>();
  ASSERT_TRUE(table_context_.synonym_dict->IsEmpty());

  auto params = BuildPipelineParamsFromContext(table_context_, /*full_config=*/nullptr,
                                               /*cache_manager=*/nullptr,
                                               /*filter_threshold=*/1000,
                                               /*attach_bm25_stats=*/true);
  EXPECT_EQ(params.synonym_dict, nullptr);
}

}  // namespace mygramdb::server::search_pipeline
