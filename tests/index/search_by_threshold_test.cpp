/**
 * @file search_by_threshold_test.cpp
 * @brief Unit tests for Index::SearchByThreshold
 */

#include <gtest/gtest.h>

#include <algorithm>
#include <vector>

#include "index/index.h"

using namespace mygramdb::index;

/**
 * @brief Test fixture for SearchByThreshold tests
 *
 * Creates a bigram index with 3 documents:
 *   Doc 1: "hello" -> bigrams: "he", "el", "ll", "lo"
 *   Doc 2: "help"  -> bigrams: "he", "el", "lp"
 *   Doc 3: "world" -> bigrams: "wo", "or", "rl", "ld"
 */
class SearchByThresholdTest : public ::testing::Test {
 protected:
  void SetUp() override {
    index_.AddDocument(1, "hello");
    index_.AddDocument(2, "help");
    index_.AddDocument(3, "world");
  }

  Index index_{2};  // Bigram index
};

/**
 * @brief When threshold equals term count, only documents with all terms match
 *
 * "he","el","ll","lo" are all bigrams of "hello" (doc 1).
 * Doc 2 has "he","el" but not "ll" or "lo" (only 2 of 4).
 * Doc 3 has none of these bigrams.
 */
TEST_F(SearchByThresholdTest, ThresholdEqualsTermCount) {
  auto results = index_.SearchByThreshold({"he", "el", "ll", "lo"}, 4);
  ASSERT_EQ(results.size(), 1);
  EXPECT_EQ(results[0], 1);
}

/**
 * @brief When threshold is below term count, more documents can match
 *
 * Doc 1 has "he","el","ll","lo" (4 of 4 -> meets threshold 2).
 * Doc 2 has "he","el" (2 of 4 -> meets threshold 2).
 * Doc 3 has none (0 of 4 -> does not meet threshold).
 */
TEST_F(SearchByThresholdTest, ThresholdBelowTermCount) {
  auto results = index_.SearchByThreshold({"he", "el", "ll", "lo"}, 2);
  ASSERT_EQ(results.size(), 2);
  EXPECT_EQ(results[0], 1);
  EXPECT_EQ(results[1], 2);
}

/**
 * @brief Threshold of 1 acts like OR: any single match suffices
 *
 * "he" matches docs 1,2. "wo" matches doc 3. All 3 docs match at least 1.
 */
TEST_F(SearchByThresholdTest, ThresholdOne) {
  auto results = index_.SearchByThreshold({"he", "wo"}, 1);
  ASSERT_EQ(results.size(), 3);
  EXPECT_EQ(results[0], 1);
  EXPECT_EQ(results[1], 2);
  EXPECT_EQ(results[2], 3);
}

/**
 * @brief Missing n-grams do not prevent matches for other terms
 *
 * "zz" is not in any document. "he" and "el" are in docs 1 and 2.
 * Docs 1 and 2 each match 2 of the 3 terms (threshold=2), so they qualify.
 */
TEST_F(SearchByThresholdTest, MissingNgrams) {
  auto results = index_.SearchByThreshold({"he", "el", "zz"}, 2);
  ASSERT_EQ(results.size(), 2);
  EXPECT_EQ(results[0], 1);
  EXPECT_EQ(results[1], 2);
}

/**
 * @brief Empty terms always returns empty result
 */
TEST_F(SearchByThresholdTest, EmptyTerms) {
  auto results = index_.SearchByThreshold({}, 1);
  EXPECT_TRUE(results.empty());
}

/**
 * @brief Threshold of zero always returns empty result
 */
TEST_F(SearchByThresholdTest, ThresholdZero) {
  auto results = index_.SearchByThreshold({"he"}, 0);
  EXPECT_TRUE(results.empty());
}

/**
 * @brief All n-grams missing from the index returns empty
 */
TEST_F(SearchByThresholdTest, AllNgramsMissing) {
  auto results = index_.SearchByThreshold({"zz", "yy", "xx"}, 1);
  EXPECT_TRUE(results.empty());
}

/**
 * @brief Exact threshold boundary: only documents meeting exact count qualify
 *
 * "he","el","lp" are all bigrams of "help" (doc 2 has all 3).
 * Doc 1 has "he","el" but not "lp" (only 2 of 3).
 * Threshold=3 means only doc 2 qualifies.
 */
TEST_F(SearchByThresholdTest, ExactThresholdBoundary) {
  auto results = index_.SearchByThreshold({"he", "el", "lp"}, 3);
  ASSERT_EQ(results.size(), 1);
  EXPECT_EQ(results[0], 2);
}

/**
 * @brief Single term with threshold 1 returns all documents containing it
 */
TEST_F(SearchByThresholdTest, SingleTerm) {
  auto results = index_.SearchByThreshold({"he"}, 1);
  ASSERT_EQ(results.size(), 2);
  EXPECT_EQ(results[0], 1);
  EXPECT_EQ(results[1], 2);
}

/**
 * @brief Results are returned in ascending DocId order
 */
TEST_F(SearchByThresholdTest, ResultsAreSorted) {
  auto results = index_.SearchByThreshold({"he", "wo"}, 1);
  ASSERT_FALSE(results.empty());
  EXPECT_TRUE(std::is_sorted(results.begin(), results.end()));
}

/**
 * @brief SearchByThreshold with threshold==size delegates to SearchAnd
 *
 * Verify that SearchByThreshold(terms, terms.size()) produces the same
 * result set as SearchAnd(terms).
 */
TEST_F(SearchByThresholdTest, DelegatesToSearchAndWhenThresholdEqualsSize) {
  std::vector<std::string> terms = {"he", "el"};
  auto threshold_results = index_.SearchByThreshold(terms, terms.size());
  auto and_results = index_.SearchAnd(terms);
  EXPECT_EQ(threshold_results, and_results);
}
