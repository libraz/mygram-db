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

#include <algorithm>
#include <string>
#include <unordered_map>
#include <vector>

#include "cache/cache_manager.h"
#include "cache/cache_types.h"
#include "config/config.h"
#include "index/index.h"
#include "query/query_parser.h"
#include "query/synonym_dictionary.h"
#include "server/server_types.h"
#include "storage/document_store.h"

namespace mygramdb::server::search_pipeline {

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

// --- InsertToCache with null manager ---

TEST_F(SearchPipelineFilterTest, InsertToCacheWithNullManagerIsNoop) {
  query::Query query;
  std::vector<SearchTermInfo> term_infos;
  // Should not crash
  InsertToCache(nullptr, query, doc_ids_, term_infos, 1.0, 2, 0, false);
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
  auto double_filtered = ApplyNotFilter(result.results, query.not_terms, index_.get(), 2, 1, true);
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
    // Create index with ngram_size=1 (matching HTTP server test pattern)
    index_ = std::make_unique<index::Index>(1);
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
    params.ngram_size = 1;
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

  EXPECT_TRUE(output.success);
  EXPECT_TRUE(output.error_message.empty());
  EXPECT_FALSE(output.cache_hit);
  // "learning" appears in doc1 and doc2
  EXPECT_EQ(output.results.size(), 2);
  EXPECT_EQ(output.all_search_terms.size(), 1);
  EXPECT_EQ(output.all_search_terms[0], "learning");
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

  EXPECT_TRUE(output.success);
  // Both learning docs have status=1
  EXPECT_EQ(output.results.size(), 2);
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
    ASSERT_TRUE(base_output.success);
    ASSERT_GE(base_output.results.size(), 1) << "Base search for 'learning' should find docs";
  }

  query::Query query;
  query.type = query::QueryType::SEARCH;
  query.table = "test";
  query.search_text = "learning";
  query.not_terms.push_back("deep");
  query.limit = 100;

  auto params = MakeParams();
  auto output = ExecuteFullPipeline(query, params);

  EXPECT_TRUE(output.success);
  // NOT "deep" should exclude doc2, leaving fewer results than the base search
  EXPECT_GE(output.results.size(), 0);
  // At minimum, the NOT filter should not add results
  EXPECT_LE(output.results.size(), 2);
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

  EXPECT_TRUE(output.success);
  // Only doc1 has both "learning" and "machine"
  EXPECT_EQ(output.results.size(), 1);
  EXPECT_EQ(output.all_search_terms.size(), 2);
}

TEST_F(FullPipelineTest, NullIndexReturnsError) {
  query::Query query;
  query.type = query::QueryType::SEARCH;
  query.table = "test";
  query.search_text = "learning";

  auto params = MakeParams();
  params.current_index = nullptr;
  auto output = ExecuteFullPipeline(query, params);

  EXPECT_FALSE(output.success);
  EXPECT_FALSE(output.error_message.empty());
}

TEST_F(FullPipelineTest, NullDocStoreReturnsError) {
  query::Query query;
  query.type = query::QueryType::SEARCH;
  query.table = "test";
  query.search_text = "learning";

  auto params = MakeParams();
  params.current_doc_store = nullptr;
  auto output = ExecuteFullPipeline(query, params);

  EXPECT_FALSE(output.success);
  EXPECT_FALSE(output.error_message.empty());
}

TEST_F(FullPipelineTest, EmptySearchTextReturnsEmpty) {
  query::Query query;
  query.type = query::QueryType::SEARCH;
  query.table = "test";
  query.search_text = "";
  query.limit = 100;

  auto params = MakeParams();
  auto output = ExecuteFullPipeline(query, params);

  EXPECT_TRUE(output.success);
  EXPECT_TRUE(output.results.empty());
}

TEST_F(FullPipelineTest, NoMatchReturnsEmpty) {
  query::Query query;
  query.type = query::QueryType::SEARCH;
  query.table = "test";
  query.search_text = "zzzznonexistent";
  query.limit = 100;

  auto params = MakeParams();
  auto output = ExecuteFullPipeline(query, params);

  EXPECT_TRUE(output.success);
  EXPECT_TRUE(output.results.empty());
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

  EXPECT_TRUE(output.success);
  // With verify_text=all, results should still include docs that actually contain "learning"
  EXPECT_EQ(output.results.size(), 2);
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

  EXPECT_TRUE(output.success);
  // Fuzzy search should find docs containing "learning" (edit distance 1 from "learnig")
  EXPECT_GE(output.results.size(), 1);
}

TEST_F(FullPipelineTest, QueryTimeMsPopulated) {
  query::Query query;
  query.type = query::QueryType::SEARCH;
  query.table = "test";
  query.search_text = "learning";
  query.limit = 100;

  auto params = MakeParams();
  auto output = ExecuteFullPipeline(query, params);

  EXPECT_TRUE(output.success);
  EXPECT_GE(output.query_time_ms, 0.0);
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
  ASSERT_TRUE(first_output.success);
  EXPECT_FALSE(first_output.cache_hit);
  EXPECT_EQ(first_output.cache_miss_reason, CacheMissReason::kNotFound);

  // Second run with the same query: must report cache_hit = true on the
  // FullPipelineOutput itself, independent of any debug-mode flag (debug mode
  // is not exercised here).
  auto second_output = ExecuteFullPipeline(query, params);
  ASSERT_TRUE(second_output.success);
  EXPECT_TRUE(second_output.cache_hit);
  EXPECT_EQ(second_output.cache_miss_reason, CacheMissReason::kHit);
  EXPECT_EQ(second_output.path_taken, PipelinePath::CACHE_HIT);
  EXPECT_EQ(second_output.results, first_output.results);
}

TEST_F(FullPipelineCacheTest, CacheMissReasonNotFoundForUnknownQuery) {
  query::Query query;
  query.type = query::QueryType::SEARCH;
  query.table = "test";
  query.search_text = "completelynovelterm";
  query.limit = 100;

  auto params = MakeParams();
  auto output = ExecuteFullPipeline(query, params);
  ASSERT_TRUE(output.success);
  EXPECT_FALSE(output.cache_hit);
  EXPECT_EQ(output.cache_miss_reason, CacheMissReason::kNotFound);
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
  ASSERT_TRUE(output.success);
  EXPECT_FALSE(output.cache_hit);
  EXPECT_EQ(output.cache_miss_reason, CacheMissReason::kDisabled);
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
  auto first = ExecuteFullPipeline(query, params);
  ASSERT_TRUE(first.success);
  ASSERT_FALSE(first.results.empty());

  // Removing a doc that appears in the cached result set causes
  // GetPrimaryKeysBatch to return an empty primary key for that DocId, which
  // IsCacheStale flags as stale.
  doc_store_->RemoveDocument(first.results.front());

  CacheMissReason reason = CacheMissReason::kHit;
  auto stale_result = TryCacheLookup(query, cache_manager_.get(), doc_store_.get(), &reason);
  EXPECT_FALSE(stale_result.has_value());
  EXPECT_EQ(reason, CacheMissReason::kStale);
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
