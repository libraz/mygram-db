/**
 * @file document_store_docid_overflow_test.cpp
 * @brief Tests for DocID overflow detection in DocumentStore
 *
 * These tests verify that DocumentStore properly detects and reports
 * DocID exhaustion when the uint32_t space (4 billion IDs) is exhausted.
 */

#include <gtest/gtest.h>

#include <limits>

#include "storage/document_store.h"

using namespace mygramdb::storage;
using mygram::utils::ErrorCode;

/**
 * @brief Test-friendly DocumentStore with public next_doc_id_ access
 *
 * This allows tests to directly set next_doc_id_ to test overflow conditions
 * without needing to add 4 billion documents.
 */
class TestableDocumentStore : public DocumentStore {
 public:
  /**
   * @brief Set next_doc_id_ to a specific value for testing
   *
   * WARNING: This bypasses normal document addition and should only be
   * used in tests to simulate overflow conditions.
   */
  void SetNextDocIdForTest(DocId value) { next_doc_id_ = value; }

  /**
   * @brief Get current next_doc_id_ value
   */
  DocId GetNextDocIdForTest() const { return next_doc_id_; }
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
