/**
 * @file sync_instance_preservation_test.cpp
 * @brief Tests that SYNC cleanup preserves Index/DocumentStore instance pointers
 *
 * Bug: Replication events not reflected in search after SYNC
 * Root cause: SYNC cleanup created new Index/DocumentStore instances, breaking
 * pointers that BinlogReader holds through TableContext.
 *
 * Fix: Use Clear() instead of make_unique to preserve instance pointers.
 */

#include <gtest/gtest.h>

#include <memory>

#include "index/index.h"
#include "storage/document_store.h"

namespace mygramdb::server {

/**
 * @brief Test that demonstrates the bug and verifies the fix
 *
 * This test simulates what happens when:
 * 1. TableContext is created with Index and DocumentStore
 * 2. Data is loaded (simulating SYNC)
 * 3. Cleanup happens (simulating SYNC cancel/failure)
 * 4. Replication tries to use the same pointers
 */
class InstancePreservationTest : public ::testing::Test {
 protected:
  void SetUp() override {
    index_ = std::make_unique<index::Index>();
    doc_store_ = std::make_unique<storage::DocumentStore>();
  }

  std::unique_ptr<index::Index> index_;
  std::unique_ptr<storage::DocumentStore> doc_store_;
};

/**
 * @brief Test that Clear() preserves instance pointer (the fix)
 */
TEST_F(InstancePreservationTest, ClearPreservesInstancePointer) {
  // Get raw pointers before (simulating what BinlogReader holds)
  index::Index* index_ptr_before = index_.get();
  storage::DocumentStore* doc_store_ptr_before = doc_store_.get();

  // Add some data (simulating SYNC load)
  auto doc_id = doc_store_->AddDocument("pk1", {});
  ASSERT_TRUE(doc_id.has_value());
  index_->AddDocument(*doc_id, "test document");

  // Verify data exists
  EXPECT_EQ(doc_store_->Size(), 1);
  EXPECT_GT(index_->TermCount(), 0);

  // Clear data (the FIX - preserves instance)
  index_->Clear();
  doc_store_->Clear();

  // Get raw pointers after
  index::Index* index_ptr_after = index_.get();
  storage::DocumentStore* doc_store_ptr_after = doc_store_.get();

  // CRITICAL: Pointers must be the same after Clear()
  EXPECT_EQ(index_ptr_before, index_ptr_after) << "Index pointer changed after Clear()!";
  EXPECT_EQ(doc_store_ptr_before, doc_store_ptr_after) << "DocumentStore pointer changed after Clear()!";

  // Verify data is cleared
  EXPECT_EQ(doc_store_->Size(), 0);
  EXPECT_EQ(index_->TermCount(), 0);

  // Verify we can still add new data
  auto new_doc_id = doc_store_->AddDocument("pk2", {});
  ASSERT_TRUE(new_doc_id.has_value());
  EXPECT_EQ(*new_doc_id, 1);  // doc_id should restart from 1 after Clear
  index_->AddDocument(*new_doc_id, "new document");
  EXPECT_EQ(doc_store_->Size(), 1);
}

/**
 * @brief Test that make_unique changes instance pointer (the old bug)
 */
TEST_F(InstancePreservationTest, MakeUniqueChangesInstancePointer) {
  // Get raw pointers before (simulating what BinlogReader holds)
  index::Index* index_ptr_before = index_.get();
  storage::DocumentStore* doc_store_ptr_before = doc_store_.get();

  // Add some data (simulating SYNC load)
  auto doc_id = doc_store_->AddDocument("pk1", {});
  ASSERT_TRUE(doc_id.has_value());
  index_->AddDocument(*doc_id, "test document");

  // Replace with new instances (the OLD BUG behavior)
  index_ = std::make_unique<index::Index>();
  doc_store_ = std::make_unique<storage::DocumentStore>();

  // Get raw pointers after
  index::Index* index_ptr_after = index_.get();
  storage::DocumentStore* doc_store_ptr_after = doc_store_.get();

  // These pointers are DIFFERENT - this demonstrates the bug
  EXPECT_NE(index_ptr_before, index_ptr_after) << "Index pointer should change with make_unique";
  EXPECT_NE(doc_store_ptr_before, doc_store_ptr_after) << "DocumentStore pointer should change with make_unique";

  // The old pointers are now dangling!
  // If BinlogReader was using index_ptr_before, it would crash or use stale data
}

/**
 * @brief Test that doc_id continues correctly after Clear()
 *
 * This verifies that Clear() resets the internal doc_id counter,
 * which is expected behavior when clearing all data.
 */
TEST_F(InstancePreservationTest, DocIdRestartsAfterClear) {
  // Add documents to get high doc_ids
  for (int i = 0; i < 100; i++) {
    auto doc_id = doc_store_->AddDocument("pk" + std::to_string(i), {});
    ASSERT_TRUE(doc_id.has_value());
    EXPECT_EQ(*doc_id, static_cast<storage::DocId>(i + 1));
  }

  EXPECT_EQ(doc_store_->Size(), 100);

  // Clear all data
  doc_store_->Clear();

  EXPECT_EQ(doc_store_->Size(), 0);

  // After Clear, doc_id should restart from 1
  auto new_doc_id = doc_store_->AddDocument("new_pk", {});
  ASSERT_TRUE(new_doc_id.has_value());
  EXPECT_EQ(*new_doc_id, 1) << "doc_id should restart from 1 after Clear()";
}

}  // namespace mygramdb::server
