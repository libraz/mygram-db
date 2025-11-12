/**
 * @file index_gettopn_test.cpp
 * @brief Test GetTopN() optimization for Index::SearchAnd()
 */

#include <gtest/gtest.h>

#include "index/index.h"

using namespace mygramdb::index;

class IndexGetTopNTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create index with default settings (bigram for ASCII, unigram for Kanji)
    index_ = std::make_unique<Index>();
  }

  std::unique_ptr<Index> index_;
};

/**
 * @brief Test GetTopN() with single term and reverse=true (optimized path)
 */
TEST_F(IndexGetTopNTest, SingleTermReverseOptimization) {
  // Add many documents
  for (DocId i = 1; i <= 10000; i++) {
    index_->AddDocument(i, "test");
  }

  // Search with limit and reverse (should use GetTopN optimization)
  auto results = index_->SearchAnd({"te"}, 100, true);

  // Should return exactly 100 results
  EXPECT_EQ(results.size(), 100);

  // Should return highest DocIds first (9901-10000 in descending order)
  EXPECT_EQ(results[0], 10000);
  EXPECT_EQ(results[1], 9999);
  EXPECT_EQ(results[99], 9901);

  // Verify all results are in descending order
  for (size_t i = 1; i < results.size(); i++) {
    EXPECT_GT(results[i - 1], results[i]);
  }
}

/**
 * @brief Test GetTopN() with single term and reverse=false (no optimization)
 */
TEST_F(IndexGetTopNTest, SingleTermForwardNoOptimization) {
  // Add documents
  for (DocId i = 1; i <= 1000; i++) {
    index_->AddDocument(i, "test");
  }

  // Search without reverse (standard path, no GetTopN optimization)
  // Note: Index layer does not apply limit/reverse - that's ResultSorter's job
  auto results = index_->SearchAnd({"te"}, 100, false);

  // Should return all 1000 results (limit not applied in Index layer)
  EXPECT_EQ(results.size(), 1000);

  // Should be in ascending order (natural order from posting list)
  EXPECT_EQ(results[0], 1);
  EXPECT_EQ(results[999], 1000);

  // Verify all results are in ascending order
  for (size_t i = 1; i < results.size(); i++) {
    EXPECT_LT(results[i - 1], results[i]);
  }
}

/**
 * @brief Test multi-term query (no GetTopN optimization, standard intersection)
 */
TEST_F(IndexGetTopNTest, MultiTermNoOptimization) {
  // Add documents with different terms
  for (DocId i = 1; i <= 5000; i++) {
    if (i % 2 == 0) {
      index_->AddDocument(i, "test data");  // Contains both "test" and "data"
    } else {
      index_->AddDocument(i, "test only");  // Contains only "test"
    }
  }

  // Multi-term search with limit and reverse
  // Should use standard intersection (not GetTopN optimization)
  // Note: Index layer does not apply limit/reverse for multi-term queries
  auto results = index_->SearchAnd({"te", "da"}, 100, true);

  // Should return all 2500 documents containing both terms (all even DocIds)
  EXPECT_EQ(results.size(), 2500);

  // All results should be even DocIds (those with both terms)
  for (auto doc_id : results) {
    EXPECT_EQ(doc_id % 2, 0);
  }
}

/**
 * @brief Test GetTopN() with limit larger than result set
 */
TEST_F(IndexGetTopNTest, LimitLargerThanResultSet) {
  // Add only 50 documents
  for (DocId i = 1; i <= 50; i++) {
    index_->AddDocument(i, "test");
  }

  // Search with limit=1000 (larger than 50)
  auto results = index_->SearchAnd({"te"}, 1000, true);

  // Should return all 50 documents
  EXPECT_EQ(results.size(), 50);

  // Should still be in descending order
  EXPECT_EQ(results[0], 50);
  EXPECT_EQ(results[49], 1);
}

/**
 * @brief Test GetTopN() with limit=0 (no limit, no optimization)
 */
TEST_F(IndexGetTopNTest, NoLimit) {
  // Add documents
  for (DocId i = 1; i <= 1000; i++) {
    index_->AddDocument(i, "test");
  }

  // Search with limit=0 (return all)
  // Note: limit=0 does not trigger optimization, returns natural order
  auto results = index_->SearchAnd({"te"}, 0, true);

  // Should return all 1000 documents
  EXPECT_EQ(results.size(), 1000);

  // Should be in natural ascending order (optimization not triggered with limit=0)
  EXPECT_EQ(results[0], 1);
  EXPECT_EQ(results[999], 1000);
}

/**
 * @brief Test GetTopN() with Japanese unigrams
 */
TEST_F(IndexGetTopNTest, JapaneseUnigrams) {
  // Add documents with Japanese text (unigrams)
  for (DocId i = 1; i <= 1000; i++) {
    index_->AddDocument(i, "漫画");  // "manga" in Japanese
  }

  // Search for "漫" (single character, unigram)
  auto results = index_->SearchAnd({"漫"}, 100, true);

  // Should return 100 results
  EXPECT_EQ(results.size(), 100);

  // Should be in descending order
  EXPECT_EQ(results[0], 1000);
  EXPECT_EQ(results[99], 901);
}

/**
 * @brief Test GetTopN() correctness vs standard path
 */
TEST_F(IndexGetTopNTest, CorrectnessVsStandardPath) {
  // Add documents
  for (DocId i = 1; i <= 5000; i++) {
    index_->AddDocument(i, "test");
  }

  // Get results using GetTopN optimization (single term + reverse + limit)
  auto optimized_results = index_->SearchAnd({"te"}, 100, true);

  // Get all results and manually take top 100
  auto all_results = index_->SearchAnd({"te"}, 0, false);
  std::reverse(all_results.begin(), all_results.end());
  std::vector<DocId> manual_results(all_results.begin(), all_results.begin() + 100);

  // Results should match exactly
  EXPECT_EQ(optimized_results.size(), manual_results.size());
  for (size_t i = 0; i < optimized_results.size(); i++) {
    EXPECT_EQ(optimized_results[i], manual_results[i]);
  }
}

/**
 * @brief Test GetTopN() performance characteristic
 */
TEST_F(IndexGetTopNTest, PerformanceCharacteristic) {
  // Add many documents to simulate real scenario
  constexpr int kTotalDocs = 100000;
  constexpr int kLimit = 100;

  for (DocId i = 1; i <= kTotalDocs; i++) {
    index_->AddDocument(i, "test");
  }

  // Measure GetTopN optimization (should be fast)
  auto start = std::chrono::high_resolution_clock::now();
  auto results = index_->SearchAnd({"te"}, kLimit, true);
  auto end = std::chrono::high_resolution_clock::now();
  auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

  // Verify correctness
  EXPECT_EQ(results.size(), kLimit);
  EXPECT_EQ(results[0], kTotalDocs);
  EXPECT_EQ(results[kLimit - 1], kTotalDocs - kLimit + 1);

  // Performance check: should complete in reasonable time
  // (This is not a strict test, just a sanity check)
  EXPECT_LT(duration_ms, 1000);  // Should be < 1 second
}
