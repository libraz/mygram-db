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
 * @brief Test multi-term query with batch block search (high selectivity)
 */
TEST_F(IndexGetTopNTest, MultiTermNoOptimization) {
  // Add dataset where batch block search will trigger
  // Selectivity = 500/1000 = 0.5 (threshold), min_size=500 (> 10K threshold)
  // Actually, this is too small - let's make it larger
  for (DocId i = 1; i <= 20000; i++) {
    if (i % 2 == 0) {
      index_->AddDocument(i, "test data");  // Contains both "test" and "data" (10000 docs)
    } else {
      index_->AddDocument(i, "test only");  // Contains only "test" (10000 docs)
    }
  }

  // Multi-term search with limit and reverse
  // Batch block search should apply (selectivity=100%, min_size=10000)
  auto results = index_->SearchAnd({"te", "da"}, 100, true);

  // Should return exactly 100 results
  EXPECT_EQ(results.size(), 100);

  // All results should be even DocIds (those with both terms)
  for (auto doc_id : results) {
    EXPECT_EQ(doc_id % 2, 0);
  }

  // Should be in descending order (highest DocIds first)
  EXPECT_EQ(results[0], 20000);   // Highest even DocID
  EXPECT_EQ(results[99], 19802);  // 100th highest even DocID
  for (size_t i = 1; i < results.size(); i++) {
    EXPECT_GT(results[i - 1], results[i]);
  }
}

/**
 * @brief Test batch block search with very high selectivity (CJK-like)
 */
TEST_F(IndexGetTopNTest, BatchBlockSearchHighSelectivity) {
  // Simulate CJK bigram scenario: both terms have nearly identical posting lists
  // This represents a word like "test" split into ["te", "es", "st"]
  // where all ngrams appear in the same 15000 documents
  for (DocId i = 1; i <= 15000; i++) {
    index_->AddDocument(i, "test");  // Both "te" and "st" appear
  }

  // Multi-term search: both ngrams have 15000 docs, selectivity=100%
  auto results = index_->SearchAnd({"te", "st"}, 100, true);

  // Should return exactly 100 results
  EXPECT_EQ(results.size(), 100);

  // Should be in descending order
  EXPECT_EQ(results[0], 15000);
  EXPECT_EQ(results[99], 14901);

  // Verify all in DESC order
  for (size_t i = 1; i < results.size(); i++) {
    EXPECT_GT(results[i - 1], results[i]);
  }
}

/**
 * @brief Test batch block search fallback when insufficient results
 */
TEST_F(IndexGetTopNTest, BatchBlockSearchInsufficientResults) {
  // Create scenario where first block doesn't have enough results
  // but total intersection is sufficient
  // Pattern: every 10th doc has both terms, so low local density
  for (DocId i = 1; i <= 50000; i++) {
    if (i % 10 == 0) {
      index_->AddDocument(i, "test data");  // 5000 docs with both terms
    } else if (i % 2 == 0) {
      index_->AddDocument(i, "test");  // 20000 docs with only "test"
    } else {
      index_->AddDocument(i, "data");  // 25000 docs with only "data"
    }
  }

  // This should fall back to standard path because distribution is sparse
  auto results = index_->SearchAnd({"te", "da"}, 100, true);

  // Should still return correct results
  EXPECT_EQ(results.size(), 100);

  // All should be multiples of 10
  for (auto doc_id : results) {
    EXPECT_EQ(doc_id % 10, 0);
  }

  // Should be in DESC order
  EXPECT_EQ(results[0], 50000);
  for (size_t i = 1; i < results.size(); i++) {
    EXPECT_GT(results[i - 1], results[i]);
  }
}

/**
 * @brief Test batch block search with low selectivity (should fallback)
 */
TEST_F(IndexGetTopNTest, BatchBlockSearchLowSelectivity) {
  // Create dataset with low selectivity (< 50% threshold)
  // selectivity = 4000/20000 = 0.2 (20%)
  for (DocId i = 1; i <= 20000; i++) {
    if (i <= 4000) {
      index_->AddDocument(i, "test data");  // Both terms
    } else if (i <= 10000) {
      index_->AddDocument(i, "data only");  // Only "data"
    } else {
      index_->AddDocument(i, "test only");  // Only "test"
    }
  }

  auto results = index_->SearchAnd({"te", "da"}, 100, true);
  EXPECT_EQ(results.size(), 100);
  EXPECT_EQ(results[0], 4000);
}

/**
 * @brief Test batch block search with very small limit
 */
TEST_F(IndexGetTopNTest, BatchBlockSearchSmallLimit) {
  for (DocId i = 1; i <= 15000; i++) {
    index_->AddDocument(i, "test");
  }

  auto results = index_->SearchAnd({"te", "st"}, 1, true);
  EXPECT_EQ(results.size(), 1);
  EXPECT_EQ(results[0], 15000);
}

/**
 * @brief Test batch block search with empty result
 */
TEST_F(IndexGetTopNTest, BatchBlockSearchEmptyResult) {
  for (DocId i = 1; i <= 10000; i++) {
    if (i % 2 == 0) {
      index_->AddDocument(i, "test");
    } else {
      index_->AddDocument(i, "data");
    }
  }

  auto results = index_->SearchAnd({"te", "da"}, 100, true);
  EXPECT_EQ(results.size(), 0);
}

/**
 * @brief Test batch block search with result smaller than limit
 */
TEST_F(IndexGetTopNTest, BatchBlockSearchPartialResults) {
  for (DocId i = 1; i <= 10000; i++) {
    if (i <= 50) {
      index_->AddDocument(i, "test data");  // 50 matches
    } else if (i % 2 == 0) {
      index_->AddDocument(i, "test");
    } else {
      index_->AddDocument(i, "data");
    }
  }

  auto results = index_->SearchAnd({"te", "da"}, 100, true);
  EXPECT_EQ(results.size(), 50);
  // Falls back to standard path, which returns ASC order
  EXPECT_EQ(results[0], 1);
  EXPECT_EQ(results[49], 50);
}

/**
 * @brief Test batch block search with small dataset (below threshold)
 */
TEST_F(IndexGetTopNTest, BatchBlockSearchSmallDataset) {
  for (DocId i = 1; i <= 5000; i++) {
    index_->AddDocument(i, "test");
  }

  auto results = index_->SearchAnd({"te", "st"}, 100, true);
  // Falls back to standard path (min_size < 10000), returns all in ASC order
  EXPECT_EQ(results.size(), 5000);
  EXPECT_EQ(results[0], 1);
  EXPECT_EQ(results[4999], 5000);
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
 * @brief Test GetTopN() with CJK unigrams
 */
TEST_F(IndexGetTopNTest, JapaneseUnigrams) {
  // Add documents with CJK text (unigrams)
  for (DocId i = 1; i <= 1000; i++) {
    index_->AddDocument(i, "\xE6\xBC\xAB\xE7\x94\xBB");  // UTF-8 encoded CJK characters
  }

  // Search for first character (single unigram)
  auto results = index_->SearchAnd({"\xE6\xBC\xAB"}, 100, true);

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
  // Reduced from 100K to 30K for faster test execution
  constexpr int kTotalDocs = 30000;
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

/**
 * @brief Test 3-way merge join (N-way algorithm)
 */
TEST_F(IndexGetTopNTest, ThreeWayMergeJoin) {
  // Add documents with 3 terms - ensure high selectivity to trigger merge join
  // All 3 terms appear in same 15000 documents (selectivity = 100%)
  for (DocId i = 1; i <= 15000; i++) {
    index_->AddDocument(i, "test data info");  // All 3 terms
  }

  // Search for all 3 terms - should trigger N-way merge join
  auto results = index_->SearchAnd({"te", "da", "in"}, 100, true);

  // Should return exactly 100 results
  EXPECT_EQ(results.size(), 100);

  // Should be in descending order
  EXPECT_EQ(results[0], 15000);
  EXPECT_EQ(results[99], 14901);
  for (size_t i = 1; i < results.size(); i++) {
    EXPECT_GT(results[i - 1], results[i]);
  }
}

/**
 * @brief Test 4-way merge join
 */
TEST_F(IndexGetTopNTest, FourWayMergeJoin) {
  // Add documents with 4 terms - ensure high selectivity
  // All 4 terms appear in same 15000 documents (selectivity = 100%)
  for (DocId i = 1; i <= 15000; i++) {
    index_->AddDocument(i, "test data info more");  // All 4 terms
  }

  // Search for all 4 terms
  auto results = index_->SearchAnd({"te", "da", "in", "mo"}, 100, true);

  EXPECT_EQ(results.size(), 100);
  EXPECT_EQ(results[0], 15000);
  EXPECT_EQ(results[99], 14901);
  for (size_t i = 1; i < results.size(); i++) {
    EXPECT_GT(results[i - 1], results[i]);
  }
}

/**
 * @brief Test merge join with unbalanced list sizes
 */
TEST_F(IndexGetTopNTest, MergeJoinUnbalancedLists) {
  // Create unbalanced scenario with high selectivity:
  // - "rare" appears in 10000 docs
  // - "test" appears in 10000 docs (same set)
  // - selectivity = 10000/10000 = 100% (> 50% threshold)
  for (DocId i = 1; i <= 20000; i++) {
    if (i <= 10000) {
      index_->AddDocument(i, "test rare");  // Both terms
    } else if (i % 2 == 0) {
      index_->AddDocument(i, "other");  // Neither term
    } else {
      index_->AddDocument(i, "another");  // Neither term
    }
  }

  auto results = index_->SearchAnd({"te", "ra"}, 100, true);

  // Should use merge join optimization
  EXPECT_EQ(results.size(), 100);

  // Should be in descending order
  EXPECT_EQ(results[0], 10000);
  EXPECT_EQ(results[99], 9901);
}

/**
 * @brief Test merge join with identical posting lists (100% overlap)
 */
TEST_F(IndexGetTopNTest, MergeJoinIdenticalLists) {
  // All documents contain both terms (perfect overlap)
  for (DocId i = 1; i <= 15000; i++) {
    index_->AddDocument(i, "abc");  // Contains both "ab" and "bc"
  }

  auto results = index_->SearchAnd({"ab", "bc"}, 100, true);

  EXPECT_EQ(results.size(), 100);
  EXPECT_EQ(results[0], 15000);
  EXPECT_EQ(results[99], 14901);
}

/**
 * @brief Test merge join with no overlap (empty intersection)
 */
TEST_F(IndexGetTopNTest, MergeJoinNoOverlap) {
  // Create two disjoint sets
  for (DocId i = 1; i <= 20000; i++) {
    if (i <= 10000) {
      index_->AddDocument(i, "alpha");
    } else {
      index_->AddDocument(i, "beta");
    }
  }

  auto results = index_->SearchAnd({"al", "be"}, 100, true);

  // No intersection
  EXPECT_EQ(results.size(), 0);
}

/**
 * @brief Test merge join with single match
 */
TEST_F(IndexGetTopNTest, MergeJoinSingleMatch) {
  // Only one document has both terms
  for (DocId i = 1; i <= 15000; i++) {
    if (i == 12345) {
      index_->AddDocument(i, "test data");
    } else if (i % 2 == 0) {
      index_->AddDocument(i, "test");
    } else {
      index_->AddDocument(i, "data");
    }
  }

  auto results = index_->SearchAnd({"te", "da"}, 100, true);

  EXPECT_EQ(results.size(), 1);
  EXPECT_EQ(results[0], 12345);
}

/**
 * @brief Test merge join with gaps in DocId sequence
 */
TEST_F(IndexGetTopNTest, MergeJoinWithGaps) {
  // Add documents with gaps in DocId sequence
  std::vector<DocId> doc_ids = {1, 5, 10, 100, 500, 1000, 5000, 10000, 15000, 20000};
  for (auto id : doc_ids) {
    index_->AddDocument(id, "test data");
  }

  // Add noise documents
  for (DocId i = 2; i <= 19999; i++) {
    if (std::find(doc_ids.begin(), doc_ids.end(), i) == doc_ids.end()) {
      if (i % 2 == 0) {
        index_->AddDocument(i, "test");
      } else {
        index_->AddDocument(i, "data");
      }
    }
  }

  auto results = index_->SearchAnd({"te", "da"}, 5, true);

  EXPECT_EQ(results.size(), 5);
  EXPECT_EQ(results[0], 20000);
  EXPECT_EQ(results[1], 15000);
  EXPECT_EQ(results[2], 10000);
  EXPECT_EQ(results[3], 5000);
  EXPECT_EQ(results[4], 1000);
}

/**
 * @brief Test merge join at selectivity threshold boundary (exactly 50%)
 */
TEST_F(IndexGetTopNTest, MergeJoinSelectivityThreshold) {
  // Create exactly 50% selectivity (at threshold)
  // min_size = 10000, max_size = 20000, selectivity = 0.5
  for (DocId i = 1; i <= 20000; i++) {
    if (i <= 10000) {
      index_->AddDocument(i, "test data");  // Both terms
    } else {
      index_->AddDocument(i, "test");  // Only test
    }
  }

  auto results = index_->SearchAnd({"te", "da"}, 100, true);

  // Should use merge join optimization (selectivity >= 0.5)
  EXPECT_EQ(results.size(), 100);
  EXPECT_EQ(results[0], 10000);
  EXPECT_EQ(results[99], 9901);
}

/**
 * @brief Test merge join at size threshold boundary (exactly 10000)
 */
TEST_F(IndexGetTopNTest, MergeJoinSizeThreshold) {
  // Create exactly 10000 documents (at threshold)
  for (DocId i = 1; i <= 10000; i++) {
    index_->AddDocument(i, "test");
  }

  auto results = index_->SearchAnd({"te", "st"}, 100, true);

  // Should use merge join (min_size >= 10000)
  EXPECT_EQ(results.size(), 100);
  EXPECT_EQ(results[0], 10000);
  EXPECT_EQ(results[99], 9901);
}
