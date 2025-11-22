/**
 * @file result_sorter_asc_desc_test.cpp
 * @brief Test to reproduce ASC/DESC bug - same input should produce reverse order
 */

#include <gtest/gtest.h>

#include <algorithm>

#include "query/query_parser.h"
#include "query/result_sorter.h"
#include "storage/document_store.h"

using namespace mygramdb::query;
using namespace mygramdb::storage;

class ResultSorterAscDescTest : public ::testing::Test {
 protected:
  void SetUp() override { doc_store_.Clear(); }

  DocumentStore doc_store_;
};

/**
 * @brief Test that ASC and DESC produce EXACT reverse order for the same input
 *
 * This test reproduces the bug: "プライマリキーをasc/descで並び替えたときの順が変わらない"
 * (Primary key sorting with asc/desc doesn't change order)
 */
TEST_F(ResultSorterAscDescTest, SameInputReverseOrderNumeric) {
  // Add documents with numeric primary keys
  std::vector<DocId> doc_ids;
  doc_ids.push_back(*doc_store_.AddDocument("100"));
  doc_ids.push_back(*doc_store_.AddDocument("50"));
  doc_ids.push_back(*doc_store_.AddDocument("200"));
  doc_ids.push_back(*doc_store_.AddDocument("150"));
  doc_ids.push_back(*doc_store_.AddDocument("75"));

  // Make a copy of the input for both queries
  std::vector<DocId> input_asc = doc_ids;
  std::vector<DocId> input_desc = doc_ids;

  // Query 1: Sort ASC
  Query query_asc;
  query_asc.type = QueryType::SEARCH;
  query_asc.table = "test";
  query_asc.search_text = "test";
  query_asc.limit = 10;
  query_asc.offset = 0;
  query_asc.order_by = OrderByClause{"", SortOrder::ASC};

  auto result_asc = ResultSorter::SortAndPaginate(input_asc, doc_store_, query_asc);
  ASSERT_TRUE(result_asc.has_value()) << result_asc.error().message();
  auto sorted_asc = result_asc.value();

  // Query 2: Sort DESC
  Query query_desc;
  query_desc.type = QueryType::SEARCH;
  query_desc.table = "test";
  query_desc.search_text = "test";
  query_desc.limit = 10;
  query_desc.offset = 0;
  query_desc.order_by = OrderByClause{"", SortOrder::DESC};

  auto result_desc = ResultSorter::SortAndPaginate(input_desc, doc_store_, query_desc);
  ASSERT_TRUE(result_desc.has_value()) << result_desc.error().message();
  auto sorted_desc = result_desc.value();

  // Verify sizes
  ASSERT_EQ(sorted_asc.size(), 5);
  ASSERT_EQ(sorted_desc.size(), 5);

  // CRITICAL TEST: ASC and DESC should be EXACT reverse of each other
  for (size_t i = 0; i < sorted_asc.size(); i++) {
    size_t reverse_idx = sorted_asc.size() - 1 - i;

    auto pk_asc = doc_store_.GetPrimaryKey(sorted_asc[i]);
    auto pk_desc = doc_store_.GetPrimaryKey(sorted_desc[reverse_idx]);

    ASSERT_TRUE(pk_asc.has_value());
    ASSERT_TRUE(pk_desc.has_value());

    // ASC[i] should equal DESC[size-1-i]
    EXPECT_EQ(pk_asc.value(), pk_desc.value())
        << "ASC[" << i << "] = " << pk_asc.value() << " should equal DESC[" << reverse_idx << "] = " << pk_desc.value();
  }

  // Also verify the expected order
  // ASC: 50, 75, 100, 150, 200
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted_asc[0]).value(), "50");
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted_asc[1]).value(), "75");
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted_asc[2]).value(), "100");
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted_asc[3]).value(), "150");
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted_asc[4]).value(), "200");

  // DESC: 200, 150, 100, 75, 50
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted_desc[0]).value(), "200");
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted_desc[1]).value(), "150");
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted_desc[2]).value(), "100");
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted_desc[3]).value(), "75");
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted_desc[4]).value(), "50");
}

/**
 * @brief Test reverse order for string primary keys
 */
TEST_F(ResultSorterAscDescTest, SameInputReverseOrderString) {
  // Add documents with string primary keys
  std::vector<DocId> doc_ids;
  doc_ids.push_back(*doc_store_.AddDocument("charlie"));
  doc_ids.push_back(*doc_store_.AddDocument("alice"));
  doc_ids.push_back(*doc_store_.AddDocument("bob"));
  doc_ids.push_back(*doc_store_.AddDocument("eve"));
  doc_ids.push_back(*doc_store_.AddDocument("david"));

  std::vector<DocId> input_asc = doc_ids;
  std::vector<DocId> input_desc = doc_ids;

  // Sort ASC
  Query query_asc;
  query_asc.type = QueryType::SEARCH;
  query_asc.table = "test";
  query_asc.search_text = "test";
  query_asc.limit = 10;
  query_asc.offset = 0;
  query_asc.order_by = OrderByClause{"", SortOrder::ASC};

  auto result_asc = ResultSorter::SortAndPaginate(input_asc, doc_store_, query_asc);
  ASSERT_TRUE(result_asc.has_value()) << result_asc.error().message();
  auto sorted_asc = result_asc.value();

  // Sort DESC
  Query query_desc;
  query_desc.type = QueryType::SEARCH;
  query_desc.table = "test";
  query_desc.search_text = "test";
  query_desc.limit = 10;
  query_desc.offset = 0;
  query_desc.order_by = OrderByClause{"", SortOrder::DESC};

  auto result_desc = ResultSorter::SortAndPaginate(input_desc, doc_store_, query_desc);
  ASSERT_TRUE(result_desc.has_value()) << result_desc.error().message();
  auto sorted_desc = result_desc.value();

  // Verify reverse order
  ASSERT_EQ(sorted_asc.size(), 5);
  ASSERT_EQ(sorted_desc.size(), 5);

  for (size_t i = 0; i < sorted_asc.size(); i++) {
    size_t reverse_idx = sorted_asc.size() - 1 - i;
    EXPECT_EQ(doc_store_.GetPrimaryKey(sorted_asc[i]).value(),
              doc_store_.GetPrimaryKey(sorted_desc[reverse_idx]).value())
        << "Reverse order check failed at index " << i;
  }

  // Verify expected order
  // ASC: alice, bob, charlie, david, eve
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted_asc[0]).value(), "alice");
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted_asc[1]).value(), "bob");
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted_asc[2]).value(), "charlie");
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted_asc[3]).value(), "david");
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted_asc[4]).value(), "eve");

  // DESC: eve, david, charlie, bob, alice
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted_desc[0]).value(), "eve");
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted_desc[1]).value(), "david");
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted_desc[2]).value(), "charlie");
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted_desc[3]).value(), "bob");
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted_desc[4]).value(), "alice");
}

/**
 * @brief Test reverse order for filter column sorting
 */
TEST_F(ResultSorterAscDescTest, SameInputReverseOrderFilterColumn) {
  // Add documents with score filter
  std::vector<DocId> doc_ids;
  doc_ids.push_back(*doc_store_.AddDocument("doc1", {{"score", int32_t(100)}}));
  doc_ids.push_back(*doc_store_.AddDocument("doc2", {{"score", int32_t(50)}}));
  doc_ids.push_back(*doc_store_.AddDocument("doc3", {{"score", int32_t(200)}}));
  doc_ids.push_back(*doc_store_.AddDocument("doc4", {{"score", int32_t(150)}}));

  std::vector<DocId> input_asc = doc_ids;
  std::vector<DocId> input_desc = doc_ids;

  // Sort ASC
  Query query_asc;
  query_asc.type = QueryType::SEARCH;
  query_asc.table = "test";
  query_asc.search_text = "test";
  query_asc.limit = 10;
  query_asc.offset = 0;
  query_asc.order_by = OrderByClause{"score", SortOrder::ASC};

  auto result_asc = ResultSorter::SortAndPaginate(input_asc, doc_store_, query_asc);
  ASSERT_TRUE(result_asc.has_value()) << result_asc.error().message();
  auto sorted_asc = result_asc.value();

  // Sort DESC
  Query query_desc;
  query_desc.type = QueryType::SEARCH;
  query_desc.table = "test";
  query_desc.search_text = "test";
  query_desc.limit = 10;
  query_desc.offset = 0;
  query_desc.order_by = OrderByClause{"score", SortOrder::DESC};

  auto result_desc = ResultSorter::SortAndPaginate(input_desc, doc_store_, query_desc);
  ASSERT_TRUE(result_desc.has_value()) << result_desc.error().message();
  auto sorted_desc = result_desc.value();

  // Verify reverse order
  ASSERT_EQ(sorted_asc.size(), 4);
  ASSERT_EQ(sorted_desc.size(), 4);

  for (size_t i = 0; i < sorted_asc.size(); i++) {
    size_t reverse_idx = sorted_asc.size() - 1 - i;

    auto score_asc = doc_store_.GetFilterValue(sorted_asc[i], "score");
    auto score_desc = doc_store_.GetFilterValue(sorted_desc[reverse_idx], "score");

    ASSERT_TRUE(score_asc.has_value());
    ASSERT_TRUE(score_desc.has_value());

    EXPECT_EQ(std::get<int32_t>(score_asc.value()), std::get<int32_t>(score_desc.value()))
        << "Reverse order check failed at index " << i;
  }

  // Verify expected order
  // ASC: 50, 100, 150, 200
  EXPECT_EQ(std::get<int32_t>(doc_store_.GetFilterValue(sorted_asc[0], "score").value()), 50);
  EXPECT_EQ(std::get<int32_t>(doc_store_.GetFilterValue(sorted_asc[1], "score").value()), 100);
  EXPECT_EQ(std::get<int32_t>(doc_store_.GetFilterValue(sorted_asc[2], "score").value()), 150);
  EXPECT_EQ(std::get<int32_t>(doc_store_.GetFilterValue(sorted_asc[3], "score").value()), 200);

  // DESC: 200, 150, 100, 50
  EXPECT_EQ(std::get<int32_t>(doc_store_.GetFilterValue(sorted_desc[0], "score").value()), 200);
  EXPECT_EQ(std::get<int32_t>(doc_store_.GetFilterValue(sorted_desc[1], "score").value()), 150);
  EXPECT_EQ(std::get<int32_t>(doc_store_.GetFilterValue(sorted_desc[2], "score").value()), 100);
  EXPECT_EQ(std::get<int32_t>(doc_store_.GetFilterValue(sorted_desc[3], "score").value()), 50);
}

/**
 * @brief Test reverse order with Schwartzian Transform (>= 100 documents)
 * This triggers the Schwartzian Transform optimization path
 */
TEST_F(ResultSorterAscDescTest, SchwartzianTransformReverseOrder) {
  // Add 150 documents (above kSchwartzianTransformThreshold = 100)
  std::vector<DocId> doc_ids;
  for (int i = 0; i < 150; i++) {
    // Use values that are easy to verify: 0, 10, 20, ..., 1490
    doc_ids.push_back(*doc_store_.AddDocument(std::to_string(i * 10)));
  }

  std::vector<DocId> input_asc = doc_ids;
  std::vector<DocId> input_desc = doc_ids;

  // Sort ASC (should trigger Schwartzian Transform)
  Query query_asc;
  query_asc.type = QueryType::SEARCH;
  query_asc.table = "test";
  query_asc.search_text = "test";
  query_asc.limit = 150;  // Request all results, no partial_sort
  query_asc.offset = 0;
  query_asc.order_by = OrderByClause{"", SortOrder::ASC};

  auto result_asc = ResultSorter::SortAndPaginate(input_asc, doc_store_, query_asc);
  ASSERT_TRUE(result_asc.has_value()) << result_asc.error().message();
  auto sorted_asc = result_asc.value();

  // Sort DESC (should trigger Schwartzian Transform)
  Query query_desc;
  query_desc.type = QueryType::SEARCH;
  query_desc.table = "test";
  query_desc.search_text = "test";
  query_desc.limit = 150;  // Request all results, no partial_sort
  query_desc.offset = 0;
  query_desc.order_by = OrderByClause{"", SortOrder::DESC};

  auto result_desc = ResultSorter::SortAndPaginate(input_desc, doc_store_, query_desc);
  ASSERT_TRUE(result_desc.has_value()) << result_desc.error().message();
  auto sorted_desc = result_desc.value();

  // Verify sizes
  ASSERT_EQ(sorted_asc.size(), 150);
  ASSERT_EQ(sorted_desc.size(), 150);

  // CRITICAL: Verify exact reverse order
  for (size_t i = 0; i < sorted_asc.size(); i++) {
    size_t reverse_idx = sorted_asc.size() - 1 - i;

    auto pk_asc = doc_store_.GetPrimaryKey(sorted_asc[i]);
    auto pk_desc = doc_store_.GetPrimaryKey(sorted_desc[reverse_idx]);

    ASSERT_TRUE(pk_asc.has_value());
    ASSERT_TRUE(pk_desc.has_value());

    EXPECT_EQ(pk_asc.value(), pk_desc.value()) << "At index " << i << ": ASC=" << pk_asc.value()
                                               << " should equal DESC[" << reverse_idx << "]=" << pk_desc.value();
  }

  // Verify first and last elements
  // ASC: 0, 10, 20, ..., 1480, 1490
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted_asc[0]).value(), "0");
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted_asc[1]).value(), "10");
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted_asc[148]).value(), "1480");
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted_asc[149]).value(), "1490");

  // DESC: 1490, 1480, ..., 10, 0
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted_desc[0]).value(), "1490");
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted_desc[1]).value(), "1480");
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted_desc[148]).value(), "10");
  EXPECT_EQ(doc_store_.GetPrimaryKey(sorted_desc[149]).value(), "0");
}
