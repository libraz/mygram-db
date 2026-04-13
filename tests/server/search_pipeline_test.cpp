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
 */

#include "server/search_pipeline.h"

#include <gtest/gtest.h>

#include <string>
#include <unordered_map>
#include <vector>

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

}  // namespace mygramdb::server::search_pipeline
