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

#include "index/index.h"
#include "query/query_parser.h"
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

  auto result =
      ExecuteWithFuzzy(query, term_infos, all_terms, /* max_distance= */ 1,
                       index_.get(), doc_store_.get(), /* full_config= */ nullptr,
                       /* ngram_size= */ 2, /* kanji_ngram_size= */ 1,
                       /* cross_boundary= */ true, /* filter_threshold= */ 100);

  EXPECT_TRUE(result.empty_term_detected);
  EXPECT_TRUE(result.results.empty());
}

TEST_F(SearchPipelineFuzzyTest, EmptyNgramsAmongMultipleTermsSetsEmptyTermDetected) {
  // If one of multiple terms has empty n-grams, the whole query should
  // report empty_term_detected
  std::vector<SearchTermInfo> term_infos;
  term_infos.push_back({{"he", "el", "ll", "lo"}, 2});  // valid term
  term_infos.push_back({/* ngrams= */ {}, /* estimated_size= */ 0});  // empty

  query::Query query;
  std::vector<std::string> all_terms = {"hello", "x"};

  auto result =
      ExecuteWithFuzzy(query, term_infos, all_terms, /* max_distance= */ 1,
                       index_.get(), doc_store_.get(), /* full_config= */ nullptr,
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

  auto result =
      ExecuteWithFuzzy(query, term_infos, all_terms, /* max_distance= */ 1,
                       index_.get(), doc_store_.get(), /* full_config= */ nullptr,
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
  auto d2 = doc_store_->AddDocument("pk2",
                                    {{"status", storage::FilterValue{int64_t{1}}}},
                                    "excluded hello world");
  ASSERT_TRUE(d2.has_value());
  index_->AddDocument(d2.value(), "excluded hello world");

  // Search for "hello" NOT "excluded"
  auto term_infos = GenerateTermInfos({"hello"}, index_.get(), 2, 1, true);
  std::sort(term_infos.begin(), term_infos.end(),
            [](const SearchTermInfo& a, const SearchTermInfo& b) {
              return a.estimated_size < b.estimated_size;
            });

  query::Query query;
  query.not_terms = {"excluded"};
  std::vector<std::string> all_terms = {"hello"};

  auto result = Execute(query, term_infos, all_terms, index_.get(),
                        doc_store_.get(), /* full_config= */ nullptr,
                        /* ngram_size= */ 2, /* kanji_ngram_size= */ 1,
                        /* cross_boundary= */ true, /* filter_threshold= */ 100);

  // "hello world" (doc 0) should remain; "excluded hello world" (doc 2) removed
  EXPECT_FALSE(result.empty_term_detected);
  ASSERT_EQ(result.results.size(), 1);

  // Verify the NOT filter was applied inside Execute -- applying it again
  // should not change the results (no double-filtering)
  auto double_filtered = ApplyNotFilter(result.results, query.not_terms,
                                        index_.get(), 2, 1, true);
  EXPECT_EQ(double_filtered.size(), result.results.size())
      << "NOT filter was not applied inside Execute(); applying it again "
         "changed the result set (double-filter bug)";
}

TEST_F(SearchPipelineFuzzyTest, ExecuteAppliesColumnFiltersInternally) {
  // Add documents with filter values
  auto d2 = doc_store_->AddDocument("pk2",
                                    {{"status", storage::FilterValue{int64_t{1}}}},
                                    "status one hello");
  ASSERT_TRUE(d2.has_value());
  index_->AddDocument(d2.value(), "status one hello");

  auto d3 = doc_store_->AddDocument("pk3",
                                    {{"status", storage::FilterValue{int64_t{2}}}},
                                    "status two hello");
  ASSERT_TRUE(d3.has_value());
  index_->AddDocument(d3.value(), "status two hello");

  // Search for "hello" with FILTER status=1
  auto term_infos = GenerateTermInfos({"hello"}, index_.get(), 2, 1, true);
  std::sort(term_infos.begin(), term_infos.end(),
            [](const SearchTermInfo& a, const SearchTermInfo& b) {
              return a.estimated_size < b.estimated_size;
            });

  query::Query query;
  query.filters = {{"status", query::FilterOp::EQ, "1"}};
  std::vector<std::string> all_terms = {"hello"};

  auto result = Execute(query, term_infos, all_terms, index_.get(),
                        doc_store_.get(), /* full_config= */ nullptr,
                        /* ngram_size= */ 2, /* kanji_ngram_size= */ 1,
                        /* cross_boundary= */ true, /* filter_threshold= */ 100);

  EXPECT_FALSE(result.empty_term_detected);

  // Verify that applying filters again produces the same result (no double-filter)
  auto double_filtered = ApplyFilters(result.results, query.filters, doc_store_.get());
  EXPECT_EQ(double_filtered.size(), result.results.size())
      << "Column filters were not applied inside Execute(); applying them again "
         "changed the result set (double-filter bug)";
}

}  // namespace mygramdb::server::search_pipeline
