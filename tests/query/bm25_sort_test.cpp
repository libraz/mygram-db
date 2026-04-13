/**
 * @file bm25_sort_test.cpp
 * @brief Unit tests for BM25 score-based sorting
 */

#include <gtest/gtest.h>

#include "query/query_parser.h"
#include "query/result_sorter.h"

using mygramdb::query::ResultSorter;
using mygramdb::query::SortOrder;
using mygramdb::storage::DocId;

TEST(BM25SortTest, SortByScoreDescending) {
  std::vector<DocId> results = {1, 2, 3, 4};
  std::vector<double> scores = {1.0, 3.0, 2.0, 4.0};

  auto sorted = ResultSorter::SortByScore(results, scores, SortOrder::DESC, 0, 0);
  ASSERT_EQ(sorted.size(), 4);
  EXPECT_EQ(sorted[0], 4);  // score 4.0
  EXPECT_EQ(sorted[1], 2);  // score 3.0
  EXPECT_EQ(sorted[2], 3);  // score 2.0
  EXPECT_EQ(sorted[3], 1);  // score 1.0
}

TEST(BM25SortTest, SortByScoreAscending) {
  std::vector<DocId> results = {1, 2, 3};
  std::vector<double> scores = {3.0, 1.0, 2.0};

  auto sorted = ResultSorter::SortByScore(results, scores, SortOrder::ASC, 0, 0);
  ASSERT_EQ(sorted.size(), 3);
  EXPECT_EQ(sorted[0], 2);  // score 1.0
  EXPECT_EQ(sorted[1], 3);  // score 2.0
  EXPECT_EQ(sorted[2], 1);  // score 3.0
}

TEST(BM25SortTest, SortByScoreWithLimit) {
  std::vector<DocId> results = {1, 2, 3, 4, 5};
  std::vector<double> scores = {1.0, 5.0, 3.0, 2.0, 4.0};

  auto sorted = ResultSorter::SortByScore(results, scores, SortOrder::DESC, 3, 0);
  ASSERT_EQ(sorted.size(), 3);
  EXPECT_EQ(sorted[0], 2);  // score 5.0
  EXPECT_EQ(sorted[1], 5);  // score 4.0
  EXPECT_EQ(sorted[2], 3);  // score 3.0
}

TEST(BM25SortTest, SortByScoreWithOffset) {
  std::vector<DocId> results = {1, 2, 3, 4};
  std::vector<double> scores = {1.0, 4.0, 3.0, 2.0};

  auto sorted = ResultSorter::SortByScore(results, scores, SortOrder::DESC, 2, 1);
  ASSERT_EQ(sorted.size(), 2);
  EXPECT_EQ(sorted[0], 3);  // score 3.0 (offset=1 skips score 4.0)
  EXPECT_EQ(sorted[1], 4);  // score 2.0
}

TEST(BM25SortTest, SortByScoreEmpty) {
  std::vector<DocId> results;
  std::vector<double> scores;

  auto sorted = ResultSorter::SortByScore(results, scores, SortOrder::DESC, 0, 0);
  EXPECT_TRUE(sorted.empty());
}

TEST(BM25SortTest, IsScoreSortTrue) {
  mygramdb::query::OrderByClause clause;
  clause.column = "_score";
  EXPECT_TRUE(clause.IsScoreSort());
}

TEST(BM25SortTest, IsScoreSortFalse) {
  mygramdb::query::OrderByClause clause;
  clause.column = "name";
  EXPECT_FALSE(clause.IsScoreSort());

  mygramdb::query::OrderByClause clause2;
  EXPECT_FALSE(clause2.IsScoreSort());  // empty = primary key
}
