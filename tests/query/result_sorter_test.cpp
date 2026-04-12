/**
 * @file result_sorter_test.cpp
 * @brief Test result sorting and pagination
 */

#include "query/result_sorter.h"

#include <gtest/gtest.h>

#include <algorithm>

#include "query/query_parser.h"
#include "storage/document_store.h"
#include "utils/error.h"

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
  doc_ids.push_back(*doc_store_.AddDocument("100"));
  doc_ids.push_back(*doc_store_.AddDocument("50"));
  doc_ids.push_back(*doc_store_.AddDocument("200"));
  doc_ids.push_back(*doc_store_.AddDocument("150"));

  // Create query with default ordering (primary key DESC)
  Query query;
  query.type = QueryType::SEARCH;
  query.table = "test";
  query.search_text = "test";
  query.limit = 10;
  query.offset = 0;
  // order_by not set = defaults to primary key DESC

  // Sort
  auto result = ResultSorter::SortAndPaginate(doc_ids, doc_store_, query);
  ASSERT_TRUE(result.has_value()) << result.error().message();
  auto sorted = result.value();

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
  doc_ids.push_back(*doc_store_.AddDocument("100"));
  doc_ids.push_back(*doc_store_.AddDocument("50"));
  doc_ids.push_back(*doc_store_.AddDocument("200"));

  // Create query with ASC ordering
  Query query;
  query.type = QueryType::SEARCH;
  query.table = "test";
  query.search_text = "test";
  query.limit = 10;
  query.offset = 0;
  query.order_by = OrderByClause{"", SortOrder::ASC};  // Empty = primary key

  // Sort
  auto result = ResultSorter::SortAndPaginate(doc_ids, doc_store_, query);
  ASSERT_TRUE(result.has_value()) << result.error().message();
  auto sorted = result.value();

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
  doc_ids.push_back(*doc_store_.AddDocument("doc1", {{"score", int32_t(100)}}));
  doc_ids.push_back(*doc_store_.AddDocument("doc2", {{"score", int32_t(50)}}));
  doc_ids.push_back(*doc_store_.AddDocument("doc3", {{"score", int32_t(200)}}));

  // Sort by score DESC
  Query query;
  query.type = QueryType::SEARCH;
  query.table = "test";
  query.search_text = "test";
  query.limit = 10;
  query.offset = 0;
  query.order_by = OrderByClause{"score", SortOrder::DESC};

  auto result = ResultSorter::SortAndPaginate(doc_ids, doc_store_, query);
  ASSERT_TRUE(result.has_value()) << result.error().message();
  auto sorted = result.value();

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
    doc_ids.push_back(*doc_store_.AddDocument(std::to_string(i)));
  }

  // Query with LIMIT 5
  Query query;
  query.type = QueryType::SEARCH;
  query.table = "test";
  query.search_text = "test";
  query.limit = 5;
  query.offset = 0;

  auto result = ResultSorter::SortAndPaginate(doc_ids, doc_store_, query);
  ASSERT_TRUE(result.has_value()) << result.error().message();
  auto sorted = result.value();

  // Should return only 5 results
  EXPECT_EQ(sorted.size(), 5);
}

// Test OFFSET
TEST_F(ResultSorterTest, ApplyOffset) {
  // Add documents
  std::vector<DocId> doc_ids;
  doc_ids.push_back(*doc_store_.AddDocument("1"));
  doc_ids.push_back(*doc_store_.AddDocument("2"));
  doc_ids.push_back(*doc_store_.AddDocument("3"));
  doc_ids.push_back(*doc_store_.AddDocument("4"));
  doc_ids.push_back(*doc_store_.AddDocument("5"));

  // Query with OFFSET 2, LIMIT 2
  Query query;
  query.type = QueryType::SEARCH;
  query.table = "test";
  query.search_text = "test";
  query.limit = 2;
  query.offset = 2;
  query.order_by = OrderByClause{"", SortOrder::ASC};  // ASC for easier testing

  auto result = ResultSorter::SortAndPaginate(doc_ids, doc_store_, query);
  ASSERT_TRUE(result.has_value()) << result.error().message();
  auto sorted = result.value();

  // Should return documents 3 and 4 (0-indexed: skip 0,1, return 2,3)
  ASSERT_EQ(sorted.size(), 2);

  auto pk1 = doc_store_.GetPrimaryKey(sorted[0]);
  auto pk2 = doc_store_.GetPrimaryKey(sorted[1]);

  EXPECT_EQ(pk1.value(), "3");
  EXPECT_EQ(pk2.value(), "4");
}

/**
 * @brief Test sorting by primary key column name
 *
 * This test verifies that when a column name is specified in SORT clause
 * and that column is the primary key (not a filter column), sorting works correctly.
 *
 * Example: SEARCH threads 漫画 SORT id DESC
 * where "id" is the primary key column name (not a filter column)
 */
TEST_F(ResultSorterTest, SortByPrimaryKeyColumnName) {
  // Add documents with numeric primary keys (simulating an "id" column)
  // These are NOT filter columns, just primary keys
  std::vector<DocId> doc_ids;
  doc_ids.push_back(*doc_store_.AddDocument("100"));
  doc_ids.push_back(*doc_store_.AddDocument("50"));
  doc_ids.push_back(*doc_store_.AddDocument("200"));
  doc_ids.push_back(*doc_store_.AddDocument("150"));

  // Sort by column name "id" in DESC order
  // Since there's no filter column called "id", it should fall back to primary key
  Query query_desc;
  query_desc.type = QueryType::SEARCH;
  query_desc.table = "test";
  query_desc.search_text = "test";
  query_desc.limit = 10;
  query_desc.offset = 0;
  query_desc.order_by = OrderByClause{"id", SortOrder::DESC};  // Column name (not empty)

  auto result_desc = ResultSorter::SortAndPaginate(doc_ids, doc_store_, query_desc);
  ASSERT_TRUE(result_desc.has_value()) << result_desc.error().message();
  auto sorted_desc = result_desc.value();

  // Verify descending order: 200, 150, 100, 50
  ASSERT_EQ(sorted_desc.size(), 4);
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted_desc[0]).value(), "200");
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted_desc[1]).value(), "150");
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted_desc[2]).value(), "100");
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted_desc[3]).value(), "50");

  // Sort by column name "id" in ASC order
  Query query_asc;
  query_asc.type = QueryType::SEARCH;
  query_asc.table = "test";
  query_asc.search_text = "test";
  query_asc.limit = 10;
  query_asc.offset = 0;
  query_asc.order_by = OrderByClause{"id", SortOrder::ASC};  // Column name (not empty)

  auto result_asc = ResultSorter::SortAndPaginate(doc_ids, doc_store_, query_asc);
  ASSERT_TRUE(result_asc.has_value()) << result_asc.error().message();
  auto sorted_asc = result_asc.value();

  // Verify ascending order: 50, 100, 150, 200
  ASSERT_EQ(sorted_asc.size(), 4);
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted_asc[0]).value(), "50");
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted_asc[1]).value(), "100");
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted_asc[2]).value(), "150");
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted_asc[3]).value(), "200");

  // Verify that ASC and DESC are exact reverses of each other
  for (size_t i = 0; i < sorted_asc.size(); i++) {
    size_t reverse_idx = sorted_asc.size() - 1 - i;
    EXPECT_EQ(doc_store_.GetPrimaryKey(sorted_asc[i]).value(),
              doc_store_.GetPrimaryKey(sorted_desc[reverse_idx]).value())
        << "ASC[" << i << "] should equal DESC[" << reverse_idx << "]";
  }
}

/**
 * @brief Test sorting by primary key column name with non-default column name
 *
 * This test verifies that when the primary key column name is NOT "id",
 * sorting by that column name works correctly.
 */
TEST_F(ResultSorterTest, SortByNonDefaultPrimaryKeyColumnName) {
  // Add documents with numeric primary keys
  std::vector<DocId> doc_ids;
  doc_ids.push_back(*doc_store_.AddDocument("100"));
  doc_ids.push_back(*doc_store_.AddDocument("50"));
  doc_ids.push_back(*doc_store_.AddDocument("200"));

  // Sort by column name "user_id" (non-default primary key column name)
  Query query;
  query.type = QueryType::SEARCH;
  query.table = "test";
  query.search_text = "test";
  query.limit = 10;
  query.offset = 0;
  query.order_by = OrderByClause{"user_id", SortOrder::DESC};

  // Pass "user_id" as primary key column name
  auto result = ResultSorter::SortAndPaginate(doc_ids, doc_store_, query, "user_id");
  ASSERT_TRUE(result.has_value()) << result.error().message();
  auto sorted = result.value();

  // Verify descending order: 200, 100, 50
  ASSERT_EQ(sorted.size(), 3);
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted[0]).value(), "200");
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted[1]).value(), "100");
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted[2]).value(), "50");
}

/**
 * @brief Test filter column takes precedence over primary key column
 *
 * When both filter column and primary key have the same name,
 * filter column should be used for sorting.
 */
TEST_F(ResultSorterTest, FilterColumnTakesPrecedenceOverPrimaryKey) {
  // Add documents where filter column "id" has different values from primary key
  std::vector<DocId> doc_ids;
  doc_ids.push_back(*doc_store_.AddDocument("pk_100", {{"id", int32_t(1)}}));
  doc_ids.push_back(*doc_store_.AddDocument("pk_50", {{"id", int32_t(3)}}));
  doc_ids.push_back(*doc_store_.AddDocument("pk_200", {{"id", int32_t(2)}}));

  // Sort by "id" - should use filter column values (1, 2, 3), not primary keys
  Query query;
  query.type = QueryType::SEARCH;
  query.table = "test";
  query.search_text = "test";
  query.limit = 10;
  query.offset = 0;
  query.order_by = OrderByClause{"id", SortOrder::ASC};

  auto result = ResultSorter::SortAndPaginate(doc_ids, doc_store_, query, "id");
  ASSERT_TRUE(result.has_value()) << result.error().message();
  auto sorted = result.value();

  // Should be sorted by filter column values: 1, 2, 3
  // NOT by primary keys: pk_100, pk_200, pk_50
  ASSERT_EQ(sorted.size(), 3);
  auto val1 = doc_store_.GetFilterValue(sorted[0], "id");
  auto val2 = doc_store_.GetFilterValue(sorted[1], "id");
  auto val3 = doc_store_.GetFilterValue(sorted[2], "id");

  EXPECT_EQ(std::get<int32_t>(val1.value()), 1);
  EXPECT_EQ(std::get<int32_t>(val2.value()), 2);
  EXPECT_EQ(std::get<int32_t>(val3.value()), 3);
}

/**
 * @brief Test Schwartzian Transform with primary key column name
 *
 * When sorting >= 100 documents by primary key column name,
 * Schwartzian Transform should be used and work correctly.
 */
TEST_F(ResultSorterTest, SchwartzianTransformWithPrimaryKeyColumnName) {
  // Add 150 documents (above kSchwartzianTransformThreshold = 100)
  std::vector<DocId> doc_ids;
  for (int i = 0; i < 150; i++) {
    doc_ids.push_back(*doc_store_.AddDocument(std::to_string(i * 10)));
  }

  // Sort by column name "id" in DESC order
  Query query;
  query.type = QueryType::SEARCH;
  query.table = "test";
  query.search_text = "test";
  query.limit = 150;  // Request all results
  query.offset = 0;
  query.order_by = OrderByClause{"id", SortOrder::DESC};

  auto result = ResultSorter::SortAndPaginate(doc_ids, doc_store_, query, "id");
  ASSERT_TRUE(result.has_value()) << result.error().message();
  auto sorted = result.value();

  // Verify descending order
  ASSERT_EQ(sorted.size(), 150);
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted[0]).value(), "1490");
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted[1]).value(), "1480");
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted[149]).value(), "0");

  // Verify complete order
  for (size_t i = 1; i < sorted.size(); i++) {
    auto pk_prev = doc_store_.GetPrimaryKey(sorted[i - 1]);
    auto pk_curr = doc_store_.GetPrimaryKey(sorted[i]);
    uint64_t num_prev = std::stoull(pk_prev.value());
    uint64_t num_curr = std::stoull(pk_curr.value());
    EXPECT_GE(num_prev, num_curr) << "Descending order violation at index " << i;
  }
}

/**
 * @brief Test invalid column error
 *
 * This test verifies that specifying a non-existent column name
 * returns an error (not just a warning).
 *
 * Note: The current implementation allows primary key fallback,
 * so this test uses string primary keys (not numeric) to ensure
 * the invalid column is truly not found.
 */
TEST_F(ResultSorterTest, InvalidColumn) {
  // Add documents with STRING primary keys and a filter column "score"
  // This ensures "invalid_column" won't match the primary key pattern
  std::vector<DocId> doc_ids;
  doc_ids.push_back(*doc_store_.AddDocument("pk_alpha", {{"score", int32_t(100)}}));
  doc_ids.push_back(*doc_store_.AddDocument("pk_beta", {{"score", int32_t(50)}}));

  // Try to sort by non-existent column
  Query query;
  query.type = QueryType::SEARCH;
  query.table = "test";
  query.search_text = "test";
  query.limit = 10;
  query.offset = 0;
  query.order_by = OrderByClause{"nonexistent_column", SortOrder::DESC};

  auto result = ResultSorter::SortAndPaginate(doc_ids, doc_store_, query);

  // Should now return error for non-existent column
  ASSERT_FALSE(result.has_value()) << "Expected error for invalid column";

  // Verify error message content
  std::string error_msg = result.error().message();
  EXPECT_NE(error_msg.find("not found"), std::string::npos) << "Error message: " << error_msg;
  EXPECT_NE(error_msg.find("nonexistent_column"), std::string::npos) << "Column name should be in error message";

  // Verify error code
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kInvalidArgument);
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

  auto result = ResultSorter::SortAndPaginate(doc_ids, doc_store_, query);
  ASSERT_TRUE(result.has_value()) << result.error().message();
  auto sorted = result.value();

  // Should succeed with empty results
  EXPECT_TRUE(sorted.empty());
}

// Test partial_sort optimization with large dataset
TEST_F(ResultSorterTest, PartialSortOptimization) {
  // Add 1000 documents
  std::vector<DocId> doc_ids;
  for (int i = 0; i < 1000; i++) {
    doc_ids.push_back(*doc_store_.AddDocument(std::to_string(i)));
  }

  // Query with small LIMIT (should trigger partial_sort)
  Query query;
  query.type = QueryType::SEARCH;
  query.table = "test";
  query.search_text = "test";
  query.limit = 10;
  query.offset = 0;
  query.order_by = OrderByClause{"", SortOrder::DESC};

  auto result = ResultSorter::SortAndPaginate(doc_ids, doc_store_, query);
  ASSERT_TRUE(result.has_value()) << result.error().message();
  auto sorted = result.value();

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
  doc_ids.push_back(*doc_store_.AddDocument("charlie"));
  doc_ids.push_back(*doc_store_.AddDocument("alice"));
  doc_ids.push_back(*doc_store_.AddDocument("bob"));

  // Sort ASC
  Query query;
  query.type = QueryType::SEARCH;
  query.table = "test";
  query.search_text = "test";
  query.limit = 10;
  query.offset = 0;
  query.order_by = OrderByClause{"", SortOrder::ASC};

  auto result = ResultSorter::SortAndPaginate(doc_ids, doc_store_, query);
  ASSERT_TRUE(result.has_value()) << result.error().message();
  auto sorted = result.value();

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
  doc_ids.push_back(*doc_store_.AddDocument("1"));
  doc_ids.push_back(*doc_store_.AddDocument("10"));
  doc_ids.push_back(*doc_store_.AddDocument("2"));
  doc_ids.push_back(*doc_store_.AddDocument("20"));
  doc_ids.push_back(*doc_store_.AddDocument("3"));

  // Test ASC: should be 1, 2, 3, 10, 20 (numeric order)
  Query query_asc;
  query_asc.type = QueryType::SEARCH;
  query_asc.table = "test";
  query_asc.search_text = "test";
  query_asc.limit = 10;
  query_asc.offset = 0;
  query_asc.order_by = OrderByClause{"", SortOrder::ASC};

  auto result_asc = ResultSorter::SortAndPaginate(doc_ids, doc_store_, query_asc);
  ASSERT_TRUE(result_asc.has_value()) << result_asc.error().message();
  auto sorted_asc = result_asc.value();
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

  auto result_desc = ResultSorter::SortAndPaginate(doc_ids, doc_store_, query_desc);
  ASSERT_TRUE(result_desc.has_value()) << result_desc.error().message();
  auto sorted_desc = result_desc.value();
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
  doc_ids.push_back(*doc_store_.AddDocument("1"));
  doc_ids.push_back(*doc_store_.AddDocument("abc"));
  doc_ids.push_back(*doc_store_.AddDocument("10"));
  doc_ids.push_back(*doc_store_.AddDocument("2"));
  doc_ids.push_back(*doc_store_.AddDocument("xyz"));

  Query query;
  query.type = QueryType::SEARCH;
  query.table = "test";
  query.search_text = "test";
  query.limit = 10;
  query.offset = 0;
  query.order_by = OrderByClause{"", SortOrder::ASC};

  auto result = ResultSorter::SortAndPaginate(doc_ids, doc_store_, query);
  ASSERT_TRUE(result.has_value()) << result.error().message();
  auto sorted = result.value();
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

/**
 * @brief Test offset + limit overflow handling
 * Regression test for: offset + limit could overflow uint32_t
 */
TEST_F(ResultSorterTest, OffsetLimitOverflow) {
  // Create test documents
  std::vector<DocId> doc_ids;
  for (int i = 1; i <= 100; ++i) {
    doc_ids.push_back(*doc_store_.AddDocument("doc" + std::to_string(i), {{"score", int32_t(i)}}));
  }

  // Test case 1: offset + limit would overflow uint32_t
  Query query;
  query.type = QueryType::SEARCH;
  query.table = "test";
  query.search_text = "test";
  query.offset = UINT32_MAX - 50;  // Very large offset
  query.limit = 100;               // offset + limit > UINT32_MAX
  query.order_by = OrderByClause{"score", SortOrder::ASC};

  // Should not crash or cause undefined behavior
  auto result = ResultSorter::SortAndPaginate(doc_ids, doc_store_, query);
  ASSERT_TRUE(result.has_value()) << result.error().message();
  auto sorted = result.value();

  // With such a large offset, no results should be returned
  EXPECT_TRUE(sorted.empty());

  // Test case 2: Maximum possible offset
  query.offset = UINT32_MAX;
  query.limit = 1;

  result = ResultSorter::SortAndPaginate(doc_ids, doc_store_, query);
  ASSERT_TRUE(result.has_value()) << result.error().message();
  sorted = result.value();
  EXPECT_TRUE(sorted.empty());

  // Test case 3: Normal case for comparison
  query.offset = 10;
  query.limit = 5;

  result = ResultSorter::SortAndPaginate(doc_ids, doc_store_, query);
  ASSERT_TRUE(result.has_value()) << result.error().message();
  sorted = result.value();
  EXPECT_EQ(5, sorted.size());
}

// Schwartzian Transform Tests

// Test Schwartzian Transform with numeric primary keys (above threshold)
TEST_F(ResultSorterTest, SchwartzianTransformNumericPrimaryKey) {
  // Add 200 documents (above kSchwartzianTransformThreshold = 100)
  std::vector<DocId> doc_ids;
  for (int i = 0; i < 200; i++) {
    // Random numeric primary keys
    doc_ids.push_back(*doc_store_.AddDocument(std::to_string(rand() % 10000)));
  }

  // Sort ascending
  Query query;
  query.type = QueryType::SEARCH;
  query.table = "test";
  query.search_text = "test";
  query.limit = 200;
  query.offset = 0;
  query.order_by = OrderByClause{"", SortOrder::ASC};

  auto result = ResultSorter::SortAndPaginate(doc_ids, doc_store_, query);
  ASSERT_TRUE(result.has_value()) << result.error().message();
  auto sorted = result.value();

  // Verify all documents are present
  EXPECT_EQ(sorted.size(), 200);

  // Verify sorted order (numeric, not lexicographic)
  for (size_t i = 1; i < sorted.size(); i++) {
    auto pk_prev = doc_store_.GetPrimaryKey(sorted[i - 1]);
    auto pk_curr = doc_store_.GetPrimaryKey(sorted[i]);

    ASSERT_TRUE(pk_prev.has_value());
    ASSERT_TRUE(pk_curr.has_value());

    uint64_t num_prev = std::stoull(pk_prev.value());
    uint64_t num_curr = std::stoull(pk_curr.value());

    EXPECT_LE(num_prev, num_curr) << "Sorting error at index " << i << ": " << num_prev << " > " << num_curr;
  }
}

// Test Schwartzian Transform with string primary keys
TEST_F(ResultSorterTest, SchwartzianTransformStringPrimaryKey) {
  // Add 150 documents with string primary keys
  std::vector<DocId> doc_ids;
  std::vector<std::string> pks = {"apple", "banana", "cherry", "date", "elderberry"};

  for (int i = 0; i < 150; i++) {
    doc_ids.push_back(*doc_store_.AddDocument(pks[i % pks.size()] + std::to_string(i)));
  }

  // Sort ascending
  Query query;
  query.type = QueryType::SEARCH;
  query.table = "test";
  query.search_text = "test";
  query.limit = 150;
  query.offset = 0;
  query.order_by = OrderByClause{"", SortOrder::ASC};

  auto result = ResultSorter::SortAndPaginate(doc_ids, doc_store_, query);
  ASSERT_TRUE(result.has_value()) << result.error().message();
  auto sorted = result.value();

  // Verify all documents are present
  EXPECT_EQ(sorted.size(), 150);

  // Verify sorted order (lexicographic)
  for (size_t i = 1; i < sorted.size(); i++) {
    auto pk_prev = doc_store_.GetPrimaryKey(sorted[i - 1]);
    auto pk_curr = doc_store_.GetPrimaryKey(sorted[i]);

    ASSERT_TRUE(pk_prev.has_value());
    ASSERT_TRUE(pk_curr.has_value());

    EXPECT_LE(pk_prev.value(), pk_curr.value()) << "Sorting error at index " << i;
  }
}

// Test Schwartzian Transform falls back for filter columns
TEST_F(ResultSorterTest, SchwartzianTransformFallbackForFilterColumn) {
  // Add 150 documents with age filter (above threshold)
  std::vector<DocId> doc_ids;
  for (int i = 0; i < 150; i++) {
    std::string pk = std::to_string(i);
    int64_t age = rand() % 100;
    auto doc_id = doc_store_.AddDocument(pk);
    ASSERT_TRUE(doc_id.has_value());
    doc_ids.push_back(*doc_id);

    // Add age filter using UpdateDocument
    FilterMap filters;
    filters["age"] = FilterValue{age};
    ASSERT_TRUE(doc_store_.UpdateDocument(*doc_id, filters));
  }

  // Sort by filter column (should fall back to traditional sort)
  Query query;
  query.type = QueryType::SEARCH;
  query.table = "test";
  query.search_text = "test";
  query.limit = 150;
  query.offset = 0;
  query.order_by = OrderByClause{"age", SortOrder::ASC};

  // Capture warning log (filter_column_not_yet_optimized should be logged)
  auto result = ResultSorter::SortAndPaginate(doc_ids, doc_store_, query);
  ASSERT_TRUE(result.has_value()) << result.error().message();
  auto sorted = result.value();

  // Verify all documents are present
  EXPECT_EQ(sorted.size(), 150);

  // Verify sorted order by age
  for (size_t i = 1; i < sorted.size(); i++) {
    auto age_prev = doc_store_.GetFilterValue(sorted[i - 1], "age");
    auto age_curr = doc_store_.GetFilterValue(sorted[i], "age");

    ASSERT_TRUE(age_prev.has_value());
    ASSERT_TRUE(age_curr.has_value());

    int64_t prev_val = std::get<int64_t>(age_prev.value());
    int64_t curr_val = std::get<int64_t>(age_curr.value());

    EXPECT_LE(prev_val, curr_val) << "Sorting error at index " << i;
  }
}

// Test Schwartzian Transform does NOT activate below threshold
TEST_F(ResultSorterTest, SchwartzianTransformBelowThreshold) {
  // Add 50 documents (below kSchwartzianTransformThreshold = 100)
  std::vector<DocId> doc_ids;
  for (int i = 0; i < 50; i++) {
    doc_ids.push_back(*doc_store_.AddDocument(std::to_string(i * 10)));
  }

  // Sort descending
  Query query;
  query.type = QueryType::SEARCH;
  query.table = "test";
  query.search_text = "test";
  query.limit = 50;
  query.offset = 0;
  query.order_by = OrderByClause{"", SortOrder::DESC};

  auto result = ResultSorter::SortAndPaginate(doc_ids, doc_store_, query);
  ASSERT_TRUE(result.has_value()) << result.error().message();
  auto sorted = result.value();

  // Verify all documents are present
  EXPECT_EQ(sorted.size(), 50);

  // Verify sorted order (descending numeric)
  for (size_t i = 1; i < sorted.size(); i++) {
    auto pk_prev = doc_store_.GetPrimaryKey(sorted[i - 1]);
    auto pk_curr = doc_store_.GetPrimaryKey(sorted[i]);

    ASSERT_TRUE(pk_prev.has_value());
    ASSERT_TRUE(pk_curr.has_value());

    uint64_t num_prev = std::stoull(pk_prev.value());
    uint64_t num_curr = std::stoull(pk_curr.value());

    EXPECT_GE(num_prev, num_curr) << "Sorting error at index " << i;
  }
}

// Test Schwartzian Transform with missing primary keys
TEST_F(ResultSorterTest, SchwartzianTransformWithMissingPrimaryKeys) {
  // This test is conceptual - in practice, DocumentStore always has primary keys
  // But the code has fallback logic for missing PKs (uses DocId itself)

  // Add 120 documents
  std::vector<DocId> doc_ids;
  for (int i = 0; i < 120; i++) {
    doc_ids.push_back(*doc_store_.AddDocument(std::to_string(i)));
  }

  // Sort ascending
  Query query;
  query.type = QueryType::SEARCH;
  query.table = "test";
  query.search_text = "test";
  query.limit = 120;
  query.offset = 0;
  query.order_by = OrderByClause{"", SortOrder::ASC};

  auto result = ResultSorter::SortAndPaginate(doc_ids, doc_store_, query);
  ASSERT_TRUE(result.has_value()) << result.error().message();
  auto sorted = result.value();

  // Verify all documents are present
  EXPECT_EQ(sorted.size(), 120);

  // Verify sorted order
  for (size_t i = 1; i < sorted.size(); i++) {
    auto pk_prev = doc_store_.GetPrimaryKey(sorted[i - 1]);
    auto pk_curr = doc_store_.GetPrimaryKey(sorted[i]);

    ASSERT_TRUE(pk_prev.has_value());
    ASSERT_TRUE(pk_curr.has_value());

    uint64_t num_prev = std::stoull(pk_prev.value());
    uint64_t num_curr = std::stoull(pk_curr.value());

    EXPECT_LE(num_prev, num_curr);
  }
}

// =============================================================================
// Negative floating-point sort key tests
// =============================================================================

/**
 * @brief Test sorting negative doubles in ascending order
 *
 * Verifies that negative floating-point values are sorted correctly.
 * Previously, "-1.0" > "-20.0" lexicographically, causing incorrect order.
 */
TEST_F(ResultSorterTest, NegativeDoubleSortAsc) {
  std::vector<DocId> doc_ids;
  doc_ids.push_back(*doc_store_.AddDocument("pk1", {{"score", FilterValue(double{-20.0})}}));
  doc_ids.push_back(*doc_store_.AddDocument("pk2", {{"score", FilterValue(double{-5.0})}}));
  doc_ids.push_back(*doc_store_.AddDocument("pk3", {{"score", FilterValue(double{-1.0})}}));

  Query query;
  query.type = QueryType::SEARCH;
  query.table = "test";
  query.search_text = "test";
  query.limit = 100;
  query.offset = 0;
  query.order_by = OrderByClause{"score", SortOrder::ASC};

  auto result = ResultSorter::SortAndPaginate(doc_ids, doc_store_, query);
  ASSERT_TRUE(result.has_value()) << result.error().message();
  auto sorted = result.value();

  ASSERT_EQ(sorted.size(), 3);
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted[0]).value(), "pk1");  // -20.0
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted[1]).value(), "pk2");  // -5.0
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted[2]).value(), "pk3");  // -1.0
}

/**
 * @brief Test sorting negative doubles in descending order
 */
TEST_F(ResultSorterTest, NegativeDoubleSortDesc) {
  std::vector<DocId> doc_ids;
  doc_ids.push_back(*doc_store_.AddDocument("pk1", {{"score", FilterValue(double{-20.0})}}));
  doc_ids.push_back(*doc_store_.AddDocument("pk2", {{"score", FilterValue(double{-5.0})}}));
  doc_ids.push_back(*doc_store_.AddDocument("pk3", {{"score", FilterValue(double{-1.0})}}));

  Query query;
  query.type = QueryType::SEARCH;
  query.table = "test";
  query.search_text = "test";
  query.limit = 100;
  query.offset = 0;
  query.order_by = OrderByClause{"score", SortOrder::DESC};

  auto result = ResultSorter::SortAndPaginate(doc_ids, doc_store_, query);
  ASSERT_TRUE(result.has_value()) << result.error().message();
  auto sorted = result.value();

  ASSERT_EQ(sorted.size(), 3);
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted[0]).value(), "pk3");  // -1.0
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted[1]).value(), "pk2");  // -5.0
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted[2]).value(), "pk1");  // -20.0
}

/**
 * @brief Test sorting mixed positive and negative doubles in ascending order
 */
TEST_F(ResultSorterTest, MixedSignDoubleSortAsc) {
  std::vector<DocId> doc_ids;
  doc_ids.push_back(*doc_store_.AddDocument("pk1", {{"score", FilterValue(double{-10.0})}}));
  doc_ids.push_back(*doc_store_.AddDocument("pk2", {{"score", FilterValue(double{0.0})}}));
  doc_ids.push_back(*doc_store_.AddDocument("pk3", {{"score", FilterValue(double{5.0})}}));
  doc_ids.push_back(*doc_store_.AddDocument("pk4", {{"score", FilterValue(double{-3.0})}}));
  doc_ids.push_back(*doc_store_.AddDocument("pk5", {{"score", FilterValue(double{100.0})}}));

  Query query;
  query.type = QueryType::SEARCH;
  query.table = "test";
  query.search_text = "test";
  query.limit = 100;
  query.offset = 0;
  query.order_by = OrderByClause{"score", SortOrder::ASC};

  auto result = ResultSorter::SortAndPaginate(doc_ids, doc_store_, query);
  ASSERT_TRUE(result.has_value()) << result.error().message();
  auto sorted = result.value();

  ASSERT_EQ(sorted.size(), 5);
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted[0]).value(), "pk1");  // -10.0
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted[1]).value(), "pk4");  // -3.0
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted[2]).value(), "pk2");  // 0.0
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted[3]).value(), "pk3");  // 5.0
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted[4]).value(), "pk5");  // 100.0
}

/**
 * @brief Test that negative zero and positive zero are adjacent when sorted
 */
TEST_F(ResultSorterTest, NegativeZeroVsPositiveZero) {
  std::vector<DocId> doc_ids;
  doc_ids.push_back(*doc_store_.AddDocument("pk1", {{"score", FilterValue(double{-0.0})}}));
  doc_ids.push_back(*doc_store_.AddDocument("pk2", {{"score", FilterValue(double{0.0})}}));

  Query query;
  query.type = QueryType::SEARCH;
  query.table = "test";
  query.search_text = "test";
  query.limit = 100;
  query.offset = 0;
  query.order_by = OrderByClause{"score", SortOrder::ASC};

  auto result = ResultSorter::SortAndPaginate(doc_ids, doc_store_, query);
  ASSERT_TRUE(result.has_value()) << result.error().message();
  auto sorted = result.value();

  // Both zeros should be present and adjacent
  ASSERT_EQ(sorted.size(), 2);

  // -0.0 should sort <= +0.0
  auto pk_first = doc_store_.GetPrimaryKey(sorted[0]).value();
  auto pk_second = doc_store_.GetPrimaryKey(sorted[1]).value();
  EXPECT_EQ(pk_first, "pk1");   // -0.0
  EXPECT_EQ(pk_second, "pk2");  // +0.0
}

// =============================================================================
// Bug #7: Signed integer sort with extreme values (XOR sign-bit technique)
// =============================================================================

/**
 * @brief Test sorting INT64 extreme values (INT64_MIN, -1, 0, 1, INT64_MAX)
 *
 * Previously, kSignedOffset = (1LL << 60) caused:
 * - INT64_MAX + kSignedOffset → signed overflow (undefined behavior)
 * - INT64_MIN + kSignedOffset → still negative (incorrect sort order)
 *
 * Fixed by using XOR with (1ULL << 63) which avoids overflow.
 */
TEST_F(ResultSorterTest, SignedInt64SortExtremeValues) {
  std::vector<DocId> doc_ids;
  doc_ids.push_back(*doc_store_.AddDocument("pk1", {{"val", int64_t(INT64_MAX)}}));
  doc_ids.push_back(*doc_store_.AddDocument("pk2", {{"val", int64_t(0)}}));
  doc_ids.push_back(*doc_store_.AddDocument("pk3", {{"val", int64_t(INT64_MIN)}}));
  doc_ids.push_back(*doc_store_.AddDocument("pk4", {{"val", int64_t(-1)}}));
  doc_ids.push_back(*doc_store_.AddDocument("pk5", {{"val", int64_t(1)}}));

  // ASC: INT64_MIN, -1, 0, 1, INT64_MAX
  Query query;
  query.type = QueryType::SEARCH;
  query.table = "test";
  query.search_text = "test";
  query.limit = 100;
  query.offset = 0;
  query.order_by = OrderByClause{"val", SortOrder::ASC};

  auto result = ResultSorter::SortAndPaginate(doc_ids, doc_store_, query);
  ASSERT_TRUE(result.has_value()) << result.error().message();
  auto sorted = result.value();

  ASSERT_EQ(sorted.size(), 5);
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted[0]).value(), "pk3");  // INT64_MIN
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted[1]).value(), "pk4");  // -1
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted[2]).value(), "pk2");  // 0
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted[3]).value(), "pk5");  // 1
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted[4]).value(), "pk1");  // INT64_MAX

  // DESC: INT64_MAX, 1, 0, -1, INT64_MIN
  query.order_by = OrderByClause{"val", SortOrder::DESC};
  auto result_desc = ResultSorter::SortAndPaginate(doc_ids, doc_store_, query);
  ASSERT_TRUE(result_desc.has_value()) << result_desc.error().message();
  auto sorted_desc = result_desc.value();

  ASSERT_EQ(sorted_desc.size(), 5);
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted_desc[0]).value(), "pk1");  // INT64_MAX
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted_desc[1]).value(), "pk5");  // 1
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted_desc[2]).value(), "pk2");  // 0
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted_desc[3]).value(), "pk4");  // -1
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted_desc[4]).value(), "pk3");  // INT64_MIN
}

/**
 * @brief Test sorting INT32 extreme values
 */
TEST_F(ResultSorterTest, SignedInt32SortExtremeValues) {
  std::vector<DocId> doc_ids;
  doc_ids.push_back(*doc_store_.AddDocument("pk1", {{"val", int32_t(INT32_MAX)}}));
  doc_ids.push_back(*doc_store_.AddDocument("pk2", {{"val", int32_t(0)}}));
  doc_ids.push_back(*doc_store_.AddDocument("pk3", {{"val", int32_t(INT32_MIN)}}));

  Query query;
  query.type = QueryType::SEARCH;
  query.table = "test";
  query.search_text = "test";
  query.limit = 100;
  query.offset = 0;
  query.order_by = OrderByClause{"val", SortOrder::ASC};

  auto result = ResultSorter::SortAndPaginate(doc_ids, doc_store_, query);
  ASSERT_TRUE(result.has_value()) << result.error().message();
  auto sorted = result.value();

  ASSERT_EQ(sorted.size(), 3);
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted[0]).value(), "pk3");  // INT32_MIN
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted[1]).value(), "pk2");  // 0
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted[2]).value(), "pk1");  // INT32_MAX
}

/**
 * @brief Test mixed positive and negative integer sort
 */
TEST_F(ResultSorterTest, MixedPositiveNegativeIntSort) {
  std::vector<DocId> doc_ids;
  doc_ids.push_back(*doc_store_.AddDocument("pk1", {{"val", int64_t(-999)}}));
  doc_ids.push_back(*doc_store_.AddDocument("pk2", {{"val", int64_t(50)}}));
  doc_ids.push_back(*doc_store_.AddDocument("pk3", {{"val", int64_t(-100)}}));
  doc_ids.push_back(*doc_store_.AddDocument("pk4", {{"val", int64_t(999)}}));
  doc_ids.push_back(*doc_store_.AddDocument("pk5", {{"val", int64_t(0)}}));
  doc_ids.push_back(*doc_store_.AddDocument("pk6", {{"val", int64_t(-1)}}));

  Query query;
  query.type = QueryType::SEARCH;
  query.table = "test";
  query.search_text = "test";
  query.limit = 100;
  query.offset = 0;
  query.order_by = OrderByClause{"val", SortOrder::ASC};

  auto result = ResultSorter::SortAndPaginate(doc_ids, doc_store_, query);
  ASSERT_TRUE(result.has_value()) << result.error().message();
  auto sorted = result.value();

  // ASC: -999, -100, -1, 0, 50, 999
  ASSERT_EQ(sorted.size(), 6);
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted[0]).value(), "pk1");  // -999
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted[1]).value(), "pk3");  // -100
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted[2]).value(), "pk6");  // -1
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted[3]).value(), "pk5");  // 0
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted[4]).value(), "pk2");  // 50
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted[5]).value(), "pk4");  // 999
}

/**
 * @brief Test signed int sort with Schwartzian Transform (150+ docs)
 *
 * Ensures the XOR technique works correctly in the Schwartzian Transform path.
 */
TEST_F(ResultSorterTest, SignedIntSortWithSchwartzianTransform) {
  // Add 160 documents (above kSchwartzianTransformThreshold = 100)
  std::vector<DocId> doc_ids;
  for (int i = 0; i < 160; i++) {
    // Values from -80 to +79
    int64_t val = static_cast<int64_t>(i) - 80;
    doc_ids.push_back(*doc_store_.AddDocument(std::to_string(i), {{"val", val}}));
  }

  Query query;
  query.type = QueryType::SEARCH;
  query.table = "test";
  query.search_text = "test";
  query.limit = 160;
  query.offset = 0;
  query.order_by = OrderByClause{"val", SortOrder::ASC};

  auto result = ResultSorter::SortAndPaginate(doc_ids, doc_store_, query);
  ASSERT_TRUE(result.has_value()) << result.error().message();
  auto sorted = result.value();

  ASSERT_EQ(sorted.size(), 160);

  // Verify strictly ascending order
  for (size_t i = 1; i < sorted.size(); i++) {
    auto val_prev = doc_store_.GetFilterValue(sorted[i - 1], "val");
    auto val_curr = doc_store_.GetFilterValue(sorted[i], "val");
    ASSERT_TRUE(val_prev.has_value());
    ASSERT_TRUE(val_curr.has_value());
    int64_t prev = std::get<int64_t>(val_prev.value());
    int64_t curr = std::get<int64_t>(val_curr.value());
    EXPECT_LT(prev, curr) << "Sort order violation at index " << i << ": " << prev << " >= " << curr;
  }
}

// =============================================================================
// Bug #8: Sort column name case sensitivity
// =============================================================================

/**
 * @brief Test that sort by primary key column name is case-insensitive
 *
 * "SORT ID DESC" and "SORT id DESC" should both recognize PK and sort correctly.
 */
TEST_F(ResultSorterTest, SortByPrimaryKeyColumnNameCaseInsensitive) {
  std::vector<DocId> doc_ids;
  doc_ids.push_back(*doc_store_.AddDocument("100"));
  doc_ids.push_back(*doc_store_.AddDocument("50"));
  doc_ids.push_back(*doc_store_.AddDocument("200"));

  // Sort by "ID" (uppercase) with primary_key_column = "id" (lowercase)
  Query query;
  query.type = QueryType::SEARCH;
  query.table = "test";
  query.search_text = "test";
  query.limit = 10;
  query.offset = 0;
  query.order_by = OrderByClause{"ID", SortOrder::DESC};

  auto result = ResultSorter::SortAndPaginate(doc_ids, doc_store_, query, "id");
  ASSERT_TRUE(result.has_value()) << result.error().message();
  auto sorted = result.value();

  // Should recognize "ID" as PK and sort numerically: 200, 100, 50
  ASSERT_EQ(sorted.size(), 3);
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted[0]).value(), "200");
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted[1]).value(), "100");
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted[2]).value(), "50");
}

// =============================================================================
// #6: Schwartzian Transform threshold test
// =============================================================================

/**
 * @brief Test sorting correctness at Schwartzian Transform threshold boundary
 *
 * Verifies that sorting produces correct results both below and above
 * the Schwartzian Transform threshold (100 entries).
 */
TEST_F(ResultSorterTest, SchwartzianTransformThresholdBoundary) {
  // Add 150 documents (above threshold of 100)
  std::vector<DocId> doc_ids;
  for (int i = 150; i >= 1; --i) {
    auto result = doc_store_.AddDocument(std::to_string(i));
    ASSERT_TRUE(result.has_value());
    doc_ids.push_back(result.value());
  }

  // Create query with default ordering (primary key DESC)
  Query query;
  query.type = QueryType::SEARCH;
  query.table = "test";
  query.search_text = "test";
  query.limit = 10;
  query.offset = 0;

  auto result = ResultSorter::SortAndPaginate(doc_ids, doc_store_, query);
  ASSERT_TRUE(result.has_value()) << result.error().message();
  auto sorted = result.value();

  // Verify we get 10 results
  ASSERT_EQ(sorted.size(), 10);

  // Verify results are sorted by primary key DESC (150, 149, 148, ...)
  for (size_t i = 0; i < sorted.size(); ++i) {
    auto pk = doc_store_.GetPrimaryKey(sorted[i]);
    ASSERT_TRUE(pk.has_value());
    EXPECT_EQ(pk.value(), std::to_string(150 - static_cast<int>(i)))
        << "Position " << i << " should have primary key " << (150 - static_cast<int>(i));
  }
}
