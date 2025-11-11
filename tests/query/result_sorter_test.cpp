/**
 * @file result_sorter_test.cpp
 * @brief Test result sorting and pagination
 */

#include "query/result_sorter.h"

#include <gtest/gtest.h>

#include <algorithm>

#include "query/query_parser.h"
#include "storage/document_store.h"

using namespace mygramdb::query;
using namespace mygramdb::storage;

class ResultSorterTest : public ::testing::Test {
 protected:
  void SetUp() override { doc_store_.Clear(); }

  DocumentStore doc_store_;
};

// Test basic sorting by primary key (default DESC)
TEST_F(ResultSorterTest, SortByPrimaryKeyDesc) {
  // Add documents with numeric primary keys
  std::vector<DocId> doc_ids;
  doc_ids.push_back(doc_store_.AddDocument("100"));
  doc_ids.push_back(doc_store_.AddDocument("50"));
  doc_ids.push_back(doc_store_.AddDocument("200"));
  doc_ids.push_back(doc_store_.AddDocument("150"));

  // Create query with default ordering (primary key DESC)
  Query query;
  query.type = QueryType::SEARCH;
  query.table = "test";
  query.search_text = "test";
  query.limit = 10;
  query.offset = 0;
  // order_by not set = defaults to primary key DESC

  // Sort
  auto sorted = ResultSorter::SortAndPaginate(doc_ids, doc_store_, query);

  // Verify result size
  ASSERT_EQ(sorted.size(), 4);

  // Verify descending order: 200, 150, 100, 50
  auto pk1 = doc_store_.GetPrimaryKey(sorted[0]);
  auto pk2 = doc_store_.GetPrimaryKey(sorted[1]);
  auto pk3 = doc_store_.GetPrimaryKey(sorted[2]);
  auto pk4 = doc_store_.GetPrimaryKey(sorted[3]);

  EXPECT_EQ(pk1.value(), "200");
  EXPECT_EQ(pk2.value(), "150");
  EXPECT_EQ(pk3.value(), "100");
  EXPECT_EQ(pk4.value(), "50");
}

// Test sorting by primary key ASC
TEST_F(ResultSorterTest, SortByPrimaryKeyAsc) {
  // Add documents
  std::vector<DocId> doc_ids;
  doc_ids.push_back(doc_store_.AddDocument("100"));
  doc_ids.push_back(doc_store_.AddDocument("50"));
  doc_ids.push_back(doc_store_.AddDocument("200"));

  // Create query with ASC ordering
  Query query;
  query.type = QueryType::SEARCH;
  query.table = "test";
  query.search_text = "test";
  query.limit = 10;
  query.offset = 0;
  query.order_by = OrderByClause{"", SortOrder::ASC};  // Empty = primary key

  // Sort
  auto sorted = ResultSorter::SortAndPaginate(doc_ids, doc_store_, query);

  // Verify ascending order: 50, 100, 200
  ASSERT_EQ(sorted.size(), 3);

  auto pk1 = doc_store_.GetPrimaryKey(sorted[0]);
  auto pk2 = doc_store_.GetPrimaryKey(sorted[1]);
  auto pk3 = doc_store_.GetPrimaryKey(sorted[2]);

  EXPECT_EQ(pk1.value(), "50");
  EXPECT_EQ(pk2.value(), "100");
  EXPECT_EQ(pk3.value(), "200");
}

// Test sorting by filter column
TEST_F(ResultSorterTest, SortByFilterColumn) {
  // Add documents with filter column "score"
  std::vector<DocId> doc_ids;
  doc_ids.push_back(doc_store_.AddDocument("doc1", {{"score", int32_t(100)}}));
  doc_ids.push_back(doc_store_.AddDocument("doc2", {{"score", int32_t(50)}}));
  doc_ids.push_back(doc_store_.AddDocument("doc3", {{"score", int32_t(200)}}));

  // Sort by score DESC
  Query query;
  query.type = QueryType::SEARCH;
  query.table = "test";
  query.search_text = "test";
  query.limit = 10;
  query.offset = 0;
  query.order_by = OrderByClause{"score", SortOrder::DESC};

  auto sorted = ResultSorter::SortAndPaginate(doc_ids, doc_store_, query);

  // Verify descending order by score: 200, 100, 50
  ASSERT_EQ(sorted.size(), 3);

  auto score1 = doc_store_.GetFilterValue(sorted[0], "score");
  auto score2 = doc_store_.GetFilterValue(sorted[1], "score");
  auto score3 = doc_store_.GetFilterValue(sorted[2], "score");

  EXPECT_EQ(std::get<int32_t>(score1.value()), 200);
  EXPECT_EQ(std::get<int32_t>(score2.value()), 100);
  EXPECT_EQ(std::get<int32_t>(score3.value()), 50);
}

// Test LIMIT
TEST_F(ResultSorterTest, ApplyLimit) {
  // Add 10 documents
  std::vector<DocId> doc_ids;
  for (int i = 0; i < 10; i++) {
    doc_ids.push_back(doc_store_.AddDocument(std::to_string(i)));
  }

  // Query with LIMIT 5
  Query query;
  query.type = QueryType::SEARCH;
  query.table = "test";
  query.search_text = "test";
  query.limit = 5;
  query.offset = 0;

  auto sorted = ResultSorter::SortAndPaginate(doc_ids, doc_store_, query);

  // Should return only 5 results
  EXPECT_EQ(sorted.size(), 5);
}

// Test OFFSET
TEST_F(ResultSorterTest, ApplyOffset) {
  // Add documents
  std::vector<DocId> doc_ids;
  doc_ids.push_back(doc_store_.AddDocument("1"));
  doc_ids.push_back(doc_store_.AddDocument("2"));
  doc_ids.push_back(doc_store_.AddDocument("3"));
  doc_ids.push_back(doc_store_.AddDocument("4"));
  doc_ids.push_back(doc_store_.AddDocument("5"));

  // Query with OFFSET 2, LIMIT 2
  Query query;
  query.type = QueryType::SEARCH;
  query.table = "test";
  query.search_text = "test";
  query.limit = 2;
  query.offset = 2;
  query.order_by = OrderByClause{"", SortOrder::ASC};  // ASC for easier testing

  auto sorted = ResultSorter::SortAndPaginate(doc_ids, doc_store_, query);

  // Should return documents 3 and 4 (0-indexed: skip 0,1, return 2,3)
  ASSERT_EQ(sorted.size(), 2);

  auto pk1 = doc_store_.GetPrimaryKey(sorted[0]);
  auto pk2 = doc_store_.GetPrimaryKey(sorted[1]);

  EXPECT_EQ(pk1.value(), "3");
  EXPECT_EQ(pk2.value(), "4");
}

// Test invalid column (now treated as NULL, with warning)
TEST_F(ResultSorterTest, InvalidColumn) {
  // Add documents without "invalid_column"
  std::vector<DocId> doc_ids;
  doc_ids.push_back(doc_store_.AddDocument("doc1", {{"score", int32_t(100)}}));
  doc_ids.push_back(doc_store_.AddDocument("doc2", {{"score", int32_t(50)}}));

  // Try to sort by non-existent column
  Query query;
  query.type = QueryType::SEARCH;
  query.table = "test";
  query.search_text = "test";
  query.limit = 10;
  query.offset = 0;
  query.order_by = OrderByClause{"invalid_column", SortOrder::DESC};

  auto sorted = ResultSorter::SortAndPaginate(doc_ids, doc_store_, query);

  // Should still succeed (warning logged, but no error)
  // Documents without the column are treated as NULL and sorted accordingly
  EXPECT_EQ(sorted.size(), 2);
}

// Test empty results
TEST_F(ResultSorterTest, EmptyResults) {
  std::vector<DocId> doc_ids;  // Empty

  Query query;
  query.type = QueryType::SEARCH;
  query.table = "test";
  query.search_text = "test";
  query.limit = 10;
  query.offset = 0;

  auto sorted = ResultSorter::SortAndPaginate(doc_ids, doc_store_, query);

  // Should succeed with empty results
  EXPECT_TRUE(sorted.empty());
}

// Test partial_sort optimization with large dataset
TEST_F(ResultSorterTest, PartialSortOptimization) {
  // Add 1000 documents
  std::vector<DocId> doc_ids;
  for (int i = 0; i < 1000; i++) {
    doc_ids.push_back(doc_store_.AddDocument(std::to_string(i)));
  }

  // Query with small LIMIT (should trigger partial_sort)
  Query query;
  query.type = QueryType::SEARCH;
  query.table = "test";
  query.search_text = "test";
  query.limit = 10;
  query.offset = 0;
  query.order_by = OrderByClause{"", SortOrder::DESC};

  auto sorted = ResultSorter::SortAndPaginate(doc_ids, doc_store_, query);

  // Should return top 10 in descending order
  ASSERT_EQ(sorted.size(), 10);

  // Verify first result is 999 (highest)
  auto pk_first = doc_store_.GetPrimaryKey(sorted[0]);
  EXPECT_EQ(pk_first.value(), "999");
}

// Test string primary key sorting
TEST_F(ResultSorterTest, StringPrimaryKey) {
  // Add documents with string primary keys
  std::vector<DocId> doc_ids;
  doc_ids.push_back(doc_store_.AddDocument("charlie"));
  doc_ids.push_back(doc_store_.AddDocument("alice"));
  doc_ids.push_back(doc_store_.AddDocument("bob"));

  // Sort ASC
  Query query;
  query.type = QueryType::SEARCH;
  query.table = "test";
  query.search_text = "test";
  query.limit = 10;
  query.offset = 0;
  query.order_by = OrderByClause{"", SortOrder::ASC};

  auto sorted = ResultSorter::SortAndPaginate(doc_ids, doc_store_, query);

  // Verify alphabetical order
  ASSERT_EQ(sorted.size(), 3);

  auto pk1 = doc_store_.GetPrimaryKey(sorted[0]);
  auto pk2 = doc_store_.GetPrimaryKey(sorted[1]);
  auto pk3 = doc_store_.GetPrimaryKey(sorted[2]);

  EXPECT_EQ(pk1.value(), "alice");
  EXPECT_EQ(pk2.value(), "bob");
  EXPECT_EQ(pk3.value(), "charlie");
}

/**
 * @brief Test numeric primary key sorting vs string comparison
 *
 * This test ensures numeric primary keys are sorted numerically, not lexicographically.
 * String comparison would give: "1" < "10" < "2" < "20" < "3" (WRONG)
 * Numeric comparison gives:    1 < 2 < 3 < 10 < 20 (CORRECT)
 */
TEST_F(ResultSorterTest, NumericPrimaryKeySortingNotLexicographic) {
  // Add documents with numeric primary keys that differ in numeric vs lexicographic order
  std::vector<DocId> doc_ids;
  doc_ids.push_back(doc_store_.AddDocument("1"));
  doc_ids.push_back(doc_store_.AddDocument("10"));
  doc_ids.push_back(doc_store_.AddDocument("2"));
  doc_ids.push_back(doc_store_.AddDocument("20"));
  doc_ids.push_back(doc_store_.AddDocument("3"));

  // Test ASC: should be 1, 2, 3, 10, 20 (numeric order)
  Query query_asc;
  query_asc.type = QueryType::SEARCH;
  query_asc.table = "test";
  query_asc.search_text = "test";
  query_asc.limit = 10;
  query_asc.offset = 0;
  query_asc.order_by = OrderByClause{"", SortOrder::ASC};

  auto sorted_asc = ResultSorter::SortAndPaginate(doc_ids, doc_store_, query_asc);
  ASSERT_EQ(sorted_asc.size(), 5);

  // Verify numeric ascending order: 1, 2, 3, 10, 20
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted_asc[0]).value(), "1");
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted_asc[1]).value(), "2");
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted_asc[2]).value(), "3");
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted_asc[3]).value(), "10");
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted_asc[4]).value(), "20");

  // Test DESC: should be 20, 10, 3, 2, 1 (numeric order descending)
  Query query_desc;
  query_desc.type = QueryType::SEARCH;
  query_desc.table = "test";
  query_desc.search_text = "test";
  query_desc.limit = 10;
  query_desc.offset = 0;
  query_desc.order_by = OrderByClause{"", SortOrder::DESC};

  auto sorted_desc = ResultSorter::SortAndPaginate(doc_ids, doc_store_, query_desc);
  ASSERT_EQ(sorted_desc.size(), 5);

  // Verify numeric descending order: 20, 10, 3, 2, 1
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted_desc[0]).value(), "20");
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted_desc[1]).value(), "10");
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted_desc[2]).value(), "3");
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted_desc[3]).value(), "2");
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted_desc[4]).value(), "1");
}

/**
 * @brief Test mixed numeric and non-numeric primary keys
 *
 * When primary keys are mixed (some numeric, some non-numeric),
 * numeric ones should be sorted numerically, and non-numeric ones lexicographically.
 */
TEST_F(ResultSorterTest, MixedNumericAndNonNumericPrimaryKeys) {
  std::vector<DocId> doc_ids;
  doc_ids.push_back(doc_store_.AddDocument("1"));
  doc_ids.push_back(doc_store_.AddDocument("abc"));
  doc_ids.push_back(doc_store_.AddDocument("10"));
  doc_ids.push_back(doc_store_.AddDocument("2"));
  doc_ids.push_back(doc_store_.AddDocument("xyz"));

  Query query;
  query.type = QueryType::SEARCH;
  query.table = "test";
  query.search_text = "test";
  query.limit = 10;
  query.offset = 0;
  query.order_by = OrderByClause{"", SortOrder::ASC};

  auto sorted = ResultSorter::SortAndPaginate(doc_ids, doc_store_, query);
  ASSERT_EQ(sorted.size(), 5);

  // Numeric keys sorted numerically: 1, 2, 10
  // Non-numeric keys sorted lexicographically: abc, xyz
  // Combined: 1, 10, 2, abc, xyz (numeric first if implementation uses that, or mixed)
  // Actually, the implementation compares numeric with numeric, and non-numeric with non-numeric
  // So we need to check the actual behavior

  std::vector<std::string> result_pks;
  for (auto doc_id : sorted) {
    result_pks.push_back(doc_store_.GetPrimaryKey(doc_id).value());
  }

  // Verify that numeric keys are in numeric order relative to each other
  auto it_1 = std::find(result_pks.begin(), result_pks.end(), "1");
  auto it_2 = std::find(result_pks.begin(), result_pks.end(), "2");
  auto it_10 = std::find(result_pks.begin(), result_pks.end(), "10");

  ASSERT_NE(it_1, result_pks.end());
  ASSERT_NE(it_2, result_pks.end());
  ASSERT_NE(it_10, result_pks.end());

  // Numeric order: 1 < 2 < 10
  EXPECT_TRUE(it_1 < it_2);
  EXPECT_TRUE(it_2 < it_10);

  // Verify that non-numeric keys are in lexicographic order relative to each other
  auto it_abc = std::find(result_pks.begin(), result_pks.end(), "abc");
  auto it_xyz = std::find(result_pks.begin(), result_pks.end(), "xyz");

  ASSERT_NE(it_abc, result_pks.end());
  ASSERT_NE(it_xyz, result_pks.end());

  // Lexicographic order: abc < xyz
  EXPECT_TRUE(it_abc < it_xyz);
}
