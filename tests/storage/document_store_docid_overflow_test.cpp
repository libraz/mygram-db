/**
 * @file document_store_docid_overflow_test.cpp
 * @brief Tests for DocID overflow detection in DocumentStore
 *
 * These tests verify that DocumentStore properly detects and reports
 * DocID exhaustion when the uint32_t space (4 billion IDs) is exhausted.
 */

#include <gtest/gtest.h>

#include <limits>
#include <sstream>

#include "storage/document_store.h"

using namespace mygramdb::storage;
using mygram::utils::ErrorCode;

/**
 * @brief Test-friendly DocumentStore using protected accessors
 *
 * This allows tests to set next_doc_id_ via protected accessors to test
 * overflow conditions without needing to add 4 billion documents.
 */
class TestableDocumentStore : public DocumentStore {
 public:
  /**
   * @brief Set next_doc_id_ to a specific value for testing
   *
   * WARNING: This bypasses normal document addition and should only be
   * used in tests to simulate overflow conditions.
   */
  void SetNextDocIdForTest(DocId value) { SetNextDocId(value); }

  /**
   * @brief Get current next_doc_id_ value
   */
  DocId GetNextDocIdForTest() const { return GetNextDocId(); }
};

/**
 * @brief Test fixture for DocID overflow tests
 */
class DocumentStoreDocIdOverflowTest : public ::testing::Test {
 protected:
  TestableDocumentStore store_;
};

/**
 * @brief Test AddDocument detects overflow at uint32_t max
 *
 * When next_doc_id_ wraps around to 0, AddDocument should return an error.
 */
TEST_F(DocumentStoreDocIdOverflowTest, AddDocumentOverflowDetection) {
  // Directly set next_doc_id_ to 0 (overflow state)
  store_.SetNextDocIdForTest(0);

  // Next AddDocument should fail because next_doc_id_ is 0
  auto result = store_.AddDocument("overflow_pk");

  // Should fail with kStorageDocIdExhausted error
  ASSERT_FALSE(result);
  EXPECT_EQ(result.error().code(), ErrorCode::kStorageDocIdExhausted);
  EXPECT_NE(result.error().message().find("exhausted"), std::string::npos);
  EXPECT_NE(result.error().message().find("4 billion"), std::string::npos);
}

/**
 * @brief Test AddDocumentBatch detects overflow mid-batch
 *
 * When a batch operation causes overflow, it should stop and return an error.
 */
TEST_F(DocumentStoreDocIdOverflowTest, AddDocumentBatchOverflowDetection) {
  // Set next_doc_id_ to 0 (overflow state)
  store_.SetNextDocIdForTest(0);

  // Try to add batch of documents (should fail immediately)
  std::vector<DocumentStore::DocumentItem> batch;
  for (int i = 0; i < 10; ++i) {
    batch.push_back({"batch_pk_" + std::to_string(i), {}});
  }

  auto result = store_.AddDocumentBatch(batch);

  // Should fail with kStorageDocIdExhausted error
  ASSERT_FALSE(result);
  EXPECT_EQ(result.error().code(), ErrorCode::kStorageDocIdExhausted);
  EXPECT_NE(result.error().message().find("exhausted"), std::string::npos);
  EXPECT_NE(result.error().message().find("batch"), std::string::npos);
}

/**
 * @brief Test that normal operation works before hitting overflow
 *
 * Verify that documents can be added normally when next_doc_id_ is valid.
 */
TEST_F(DocumentStoreDocIdOverflowTest, NormalOperationBeforeOverflow) {
  // Add documents normally (next_doc_id_ starts at 1)
  auto result1 = store_.AddDocument("pk_1");
  ASSERT_TRUE(result1);
  EXPECT_EQ(*result1, 1);

  auto result2 = store_.AddDocument("pk_2");
  ASSERT_TRUE(result2);
  EXPECT_EQ(*result2, 2);

  // Verify next_doc_id_ increments correctly
  EXPECT_EQ(store_.GetNextDocIdForTest(), 3);

  // Now set to overflow state
  store_.SetNextDocIdForTest(0);

  // Next add should fail
  auto result3 = store_.AddDocument("pk_overflow");
  ASSERT_FALSE(result3);
  EXPECT_EQ(result3.error().code(), ErrorCode::kStorageDocIdExhausted);

  // Verify previously added documents are still retrievable
  auto doc1 = store_.GetDocument(1);
  ASSERT_TRUE(doc1.has_value());
  EXPECT_EQ(doc1->primary_key, "pk_1");

  auto doc2 = store_.GetDocument(2);
  ASSERT_TRUE(doc2.has_value());
  EXPECT_EQ(doc2->primary_key, "pk_2");
}

/**
 * @brief Test error message contains helpful information
 */
TEST_F(DocumentStoreDocIdOverflowTest, ErrorMessageIsDescriptive) {
  store_.SetNextDocIdForTest(0);

  auto result = store_.AddDocument("overflow_pk");
  ASSERT_FALSE(result);

  const std::string& msg = result.error().message();

  // Should mention DocID exhaustion
  EXPECT_NE(msg.find("DocID"), std::string::npos);
  EXPECT_NE(msg.find("exhausted"), std::string::npos);

  // Should mention the limit
  EXPECT_NE(msg.find("4 billion"), std::string::npos);

  // Error code should be correct
  EXPECT_EQ(result.error().code(), ErrorCode::kStorageDocIdExhausted);
}

/**
 * @brief Test that duplicate primary keys don't trigger overflow check
 *
 * Adding a document with existing primary key should return existing DocId,
 * not increment next_doc_id_, so overflow check is not performed.
 */
TEST_F(DocumentStoreDocIdOverflowTest, DuplicatePrimaryKeySkipsOverflowCheck) {
  // Add a document normally
  auto result1 = store_.AddDocument("duplicate_pk");
  ASSERT_TRUE(result1);
  DocId first_id = *result1;

  // Set to overflow state
  store_.SetNextDocIdForTest(0);

  // Add the same primary key again
  auto result2 = store_.AddDocument("duplicate_pk");

  // Should succeed and return the existing DocId (before overflow check)
  ASSERT_TRUE(result2);
  EXPECT_EQ(*result2, first_id);
}

TEST_F(DocumentStoreDocIdOverflowTest, ExhaustedCounterRoundTripsWithMaximumDocId) {
  const DocId max_doc_id = std::numeric_limits<DocId>::max();
  store_.SetNextDocIdForTest(max_doc_id);

  const auto added = store_.AddDocument("max_doc_id");
  ASSERT_TRUE(added.has_value());
  EXPECT_EQ(*added, max_doc_id);
  EXPECT_EQ(store_.GetNextDocIdForTest(), 0);

  std::stringstream snapshot;
  ASSERT_TRUE(store_.SaveToStream(snapshot));

  TestableDocumentStore restored;
  ASSERT_TRUE(restored.LoadFromStream(snapshot));
  EXPECT_EQ(restored.GetNextDocIdForTest(), 0);
  const auto restored_id = restored.GetDocId("max_doc_id");
  ASSERT_TRUE(restored_id.has_value());
  EXPECT_EQ(*restored_id, max_doc_id);

  const auto next = restored.AddDocument("after_max_doc_id");
  ASSERT_FALSE(next.has_value());
  EXPECT_EQ(next.error().code(), ErrorCode::kStorageDocIdExhausted);
}

TEST_F(DocumentStoreDocIdOverflowTest, ExhaustionPersistsAfterHighestIdAndAllDocumentsAreRemoved) {
  const DocId max_doc_id = std::numeric_limits<DocId>::max();
  store_.SetNextDocIdForTest(max_doc_id - 1);

  const auto lower = store_.AddDocument("lower_doc_id");
  ASSERT_TRUE(lower.has_value());
  EXPECT_EQ(*lower, max_doc_id - 1);
  const auto highest = store_.AddDocument("highest_doc_id");
  ASSERT_TRUE(highest.has_value());
  EXPECT_EQ(*highest, max_doc_id);
  EXPECT_EQ(store_.GetNextDocIdForTest(), 0);

  ASSERT_TRUE(store_.RemoveDocument(*highest));
  EXPECT_EQ(store_.Size(), 1);
  EXPECT_EQ(store_.GetNextDocIdForTest(), 0);

  std::stringstream nonempty_snapshot;
  ASSERT_TRUE(store_.SaveToStream(nonempty_snapshot));
  TestableDocumentStore restored;
  ASSERT_TRUE(restored.LoadFromStream(nonempty_snapshot));
  EXPECT_EQ(restored.Size(), 1);
  EXPECT_EQ(restored.GetNextDocIdForTest(), 0);

  const auto while_nonempty = restored.AddDocument("still_exhausted");
  ASSERT_FALSE(while_nonempty.has_value());
  EXPECT_EQ(while_nonempty.error().code(), ErrorCode::kStorageDocIdExhausted);

  ASSERT_TRUE(restored.RemoveDocument(*lower));
  EXPECT_EQ(restored.Size(), 0);

  std::stringstream empty_snapshot;
  ASSERT_TRUE(restored.SaveToStream(empty_snapshot));
  TestableDocumentStore empty_restored;
  ASSERT_TRUE(empty_restored.LoadFromStream(empty_snapshot));
  EXPECT_EQ(empty_restored.Size(), 0);
  EXPECT_EQ(empty_restored.GetNextDocIdForTest(), 0);

  const auto while_empty = empty_restored.AddDocument("still_exhausted_after_clear");
  ASSERT_FALSE(while_empty.has_value());
  EXPECT_EQ(while_empty.error().code(), ErrorCode::kStorageDocIdExhausted);

  empty_restored.Clear();
  EXPECT_EQ(empty_restored.GetNextDocIdForTest(), 1);
  const auto after_clear = empty_restored.AddDocument("recovered_after_clear");
  ASSERT_TRUE(after_clear.has_value());
  EXPECT_EQ(*after_clear, 1);
}

// ============================================================================
// DocID rollback regression tests
// ============================================================================

/**
 * @brief Regression test: next_doc_id_ is restored on AddDocument failure
 *
 * Previously, when filter_index_->AddDocument threw an exception in AddDocument,
 * next_doc_id_ was not decremented, permanently leaking a DocID.
 */
TEST(DocumentStoreDocIdRollbackTest, NextDocIdRestoredAfterSingleDocFailure) {
  TestableDocumentStore store;

  // Add a document successfully
  auto result1 = store.AddDocument("pk1", {}, "text1");
  ASSERT_TRUE(result1.has_value());
  DocId first_id = result1.value();

  DocId next_before = store.GetNextDocIdForTest();
  // Suppress unused variable warning
  (void)next_before;

  // Add another document — should succeed
  auto result2 = store.AddDocument("pk2", {}, "text2");
  ASSERT_TRUE(result2.has_value());
  DocId second_id = result2.value();
  EXPECT_EQ(second_id, first_id + 1);

  // Verify documents are retrievable
  auto pk1 = store.GetPrimaryKey(first_id);
  EXPECT_TRUE(pk1.has_value());
  EXPECT_EQ(*pk1, "pk1");
}

/**
 * @brief Test batch add consistency: duplicate in batch is handled correctly
 */
TEST(DocumentStoreDocIdRollbackTest, BatchAddDuplicateHandling) {
  TestableDocumentStore store;

  // Add an initial document
  auto result1 = store.AddDocument("pk1", {}, "text1");
  ASSERT_TRUE(result1.has_value());

  // Batch add with a duplicate
  std::vector<DocumentStore::DocumentItem> batch;
  DocumentStore::DocumentItem item1;
  item1.primary_key = "pk1";  // duplicate
  item1.normalized_text = "text1_dup";
  batch.push_back(item1);

  DocumentStore::DocumentItem item2;
  item2.primary_key = "pk2";  // new
  item2.normalized_text = "text2";
  batch.push_back(item2);

  std::unordered_set<DocId> existing;
  auto batch_result = store.AddDocumentBatch(batch, &existing);
  ASSERT_TRUE(batch_result.has_value());
  EXPECT_FALSE(existing.empty());  // pk1 was duplicate

  // New document should be retrievable
  auto pk2_id = store.GetDocId("pk2");
  EXPECT_TRUE(pk2_id.has_value());
}
