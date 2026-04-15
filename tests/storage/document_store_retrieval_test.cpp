/**
 * @file document_store_retrieval_test.cpp
 * @brief Regression tests for DocumentStore multi-column batch retrieval
 *
 * Verifies that GetFilterValuesBatchMultiColumn (transposed loop order)
 * produces the same results as single-column GetFilterValuesBatch.
 */

#include <gtest/gtest.h>

#include <optional>
#include <string>
#include <vector>

#include "storage/document_store.h"

using namespace mygramdb::storage;

/**
 * @brief Regression test: GetFilterValuesBatchMultiColumn loop order optimization
 *
 * Verifies that the transposed loop order (doc_ids outer, columns inner)
 * produces the same results as single-column retrieval.
 */
TEST(DocumentStoreRetrievalTest, MultiColumnBatchMatchesSingleColumn) {
  DocumentStore store;

  // Add documents with multiple filter columns
  FilterMap filters1 = {{"color", std::string("red")}, {"size", int64_t(10)}};
  FilterMap filters2 = {{"color", std::string("blue")}, {"size", int64_t(20)}};
  FilterMap filters3 = {{"color", std::string("green")}};  // missing "size"

  auto id1 = store.AddDocument("pk1", filters1, "text1");
  auto id2 = store.AddDocument("pk2", filters2, "text2");
  auto id3 = store.AddDocument("pk3", filters3, "text3");
  ASSERT_TRUE(id1.has_value() && id2.has_value() && id3.has_value());

  std::vector<DocId> doc_ids = {*id1, *id2, *id3};
  std::vector<std::string> columns = {"color", "size"};

  auto multi_results = store.GetFilterValuesBatchMultiColumn(doc_ids, columns);
  ASSERT_EQ(multi_results.size(), 2u);  // 2 columns

  // Column "color": all 3 docs have values — verify actual values, not just presence
  ASSERT_EQ(multi_results[0].size(), 3u);
  ASSERT_TRUE(multi_results[0][0].has_value());
  EXPECT_EQ(std::get<std::string>(multi_results[0][0].value()), "red");
  ASSERT_TRUE(multi_results[0][1].has_value());
  EXPECT_EQ(std::get<std::string>(multi_results[0][1].value()), "blue");
  ASSERT_TRUE(multi_results[0][2].has_value());
  EXPECT_EQ(std::get<std::string>(multi_results[0][2].value()), "green");

  // Column "size": doc3 is missing — verify actual values where present
  ASSERT_EQ(multi_results[1].size(), 3u);
  ASSERT_TRUE(multi_results[1][0].has_value());
  EXPECT_EQ(std::get<int64_t>(multi_results[1][0].value()), 10);
  ASSERT_TRUE(multi_results[1][1].has_value());
  EXPECT_EQ(std::get<int64_t>(multi_results[1][1].value()), 20);
  EXPECT_FALSE(multi_results[1][2].has_value());  // doc3 has no "size"

  // Verify single-column retrieval matches
  auto single_color = store.GetFilterValuesBatch(doc_ids, "color");
  auto single_size = store.GetFilterValuesBatch(doc_ids, "size");
  ASSERT_EQ(single_color.size(), multi_results[0].size());
  for (size_t i = 0; i < single_color.size(); ++i) {
    EXPECT_EQ(single_color[i].has_value(), multi_results[0][i].has_value());
    if (single_color[i].has_value() && multi_results[0][i].has_value()) {
      EXPECT_EQ(single_color[i].value(), multi_results[0][i].value());
    }
  }
  ASSERT_EQ(single_size.size(), multi_results[1].size());
  for (size_t i = 0; i < single_size.size(); ++i) {
    EXPECT_EQ(single_size[i].has_value(), multi_results[1][i].has_value());
    if (single_size[i].has_value() && multi_results[1][i].has_value()) {
      EXPECT_EQ(single_size[i].value(), multi_results[1][i].value());
    }
  }
}

/**
 * @brief Test multi-column retrieval with empty inputs
 */
TEST(DocumentStoreRetrievalTest, MultiColumnBatchEmptyInputs) {
  DocumentStore store;

  // Empty doc_ids
  std::vector<DocId> empty_ids;
  std::vector<std::string> columns = {"color"};
  auto result1 = store.GetFilterValuesBatchMultiColumn(empty_ids, columns);
  ASSERT_EQ(result1.size(), 1u);
  EXPECT_TRUE(result1[0].empty());

  // Empty columns
  FilterMap filters = {{"color", std::string("red")}};
  auto id = store.AddDocument("pk1", filters, "text1");
  ASSERT_TRUE(id.has_value());
  std::vector<DocId> doc_ids = {*id};
  std::vector<std::string> empty_cols;
  auto result2 = store.GetFilterValuesBatchMultiColumn(doc_ids, empty_cols);
  EXPECT_TRUE(result2.empty());
}

/**
 * @brief Test GetDocumentsBatch returns correct documents and nullopt for
 * missing IDs
 */
TEST(DocumentStoreRetrievalTest, GetDocumentsBatchReturnsCorrectDocuments) {
  DocumentStore store;

  // Add documents
  auto id1 = store.AddDocument("pk1", {{"color", std::string("red")}}, "text1");
  ASSERT_TRUE(id1.has_value());
  auto id2 = store.AddDocument("pk2", {{"color", std::string("blue")}}, "text2");
  ASSERT_TRUE(id2.has_value());
  auto id3 = store.AddDocument("pk3", {{"color", std::string("green")}}, "text3");
  ASSERT_TRUE(id3.has_value());

  // Batch get with a mix of valid and invalid IDs
  std::vector<DocId> ids = {*id1, *id3, 99999};  // 99999 doesn't exist
  auto results = store.GetDocumentsBatch(ids);

  ASSERT_EQ(results.size(), 3u);
  ASSERT_TRUE(results[0].has_value());
  EXPECT_EQ(results[0]->primary_key, "pk1");
  ASSERT_TRUE(results[1].has_value());
  EXPECT_EQ(results[1]->primary_key, "pk3");
  EXPECT_FALSE(results[2].has_value());  // 99999 not found
}

/**
 * @brief Test GetDocumentsBatch with empty input returns empty output
 */
TEST(DocumentStoreRetrievalTest, GetDocumentsBatchEmptyInput) {
  DocumentStore store;

  std::vector<DocId> empty_ids;
  auto results = store.GetDocumentsBatch(empty_ids);
  EXPECT_TRUE(results.empty());
}

/**
 * @brief Test GetDocumentsBatch includes filter data in returned documents
 */
TEST(DocumentStoreRetrievalTest, GetDocumentsBatchIncludesFilters) {
  DocumentStore store;

  FilterMap filters = {{"color", std::string("red")}, {"size", int64_t(42)}};
  auto id = store.AddDocument("pk1", filters, "text1");
  ASSERT_TRUE(id.has_value());

  std::vector<DocId> ids = {*id};
  auto results = store.GetDocumentsBatch(ids);

  ASSERT_EQ(results.size(), 1u);
  ASSERT_TRUE(results[0].has_value());
  EXPECT_EQ(results[0]->primary_key, "pk1");
  EXPECT_EQ(results[0]->doc_id, *id);

  // Verify filters are populated
  auto color_it = results[0]->filters.find("color");
  ASSERT_NE(color_it, results[0]->filters.end());
  EXPECT_EQ(std::get<std::string>(color_it->second), "red");

  auto size_it = results[0]->filters.find("size");
  ASSERT_NE(size_it, results[0]->filters.end());
  EXPECT_EQ(std::get<int64_t>(size_it->second), 42);
}

/**
 * @brief Test multi-column retrieval with non-existent doc IDs
 */
TEST(DocumentStoreRetrievalTest, MultiColumnBatchNonExistentDocIds) {
  DocumentStore store;

  FilterMap filters = {{"color", std::string("red")}};
  auto id = store.AddDocument("pk1", filters, "text1");
  ASSERT_TRUE(id.has_value());

  // Include a non-existent doc ID
  std::vector<DocId> doc_ids = {*id, 99999};
  std::vector<std::string> columns = {"color"};

  auto multi_results = store.GetFilterValuesBatchMultiColumn(doc_ids, columns);
  ASSERT_EQ(multi_results.size(), 1u);
  ASSERT_EQ(multi_results[0].size(), 2u);
  EXPECT_TRUE(multi_results[0][0].has_value());
  EXPECT_FALSE(multi_results[0][1].has_value());  // non-existent doc

  // Should match single-column retrieval
  auto single = store.GetFilterValuesBatch(doc_ids, "color");
  ASSERT_EQ(single.size(), 2u);
  EXPECT_EQ(single[0].has_value(), multi_results[0][0].has_value());
  EXPECT_EQ(single[1].has_value(), multi_results[0][1].has_value());
}
