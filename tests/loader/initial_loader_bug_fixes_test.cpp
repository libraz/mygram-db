/**
 * @file initial_loader_bug_fixes_test.cpp
 * @brief Tests for InitialLoader bug fixes
 *
 * Tests for:
 * - Bug #4: Last batch not indexed
 * - Bug #5: index_batch/doc_ids size mismatch
 * - Bug #35: GTID capture timing issue (requires MySQL integration test)
 *
 * Bug #35 Note:
 * The GTID capture timing fix ensures consistent GTID by:
 * 1. Capturing GTID before starting transaction
 * 2. Starting transaction with consistent snapshot
 * 3. Capturing GTID after transaction start
 * 4. If GTIDs differ, rollback and retry (max 3 times)
 * This prevents the scenario where another transaction commits between
 * snapshot creation and GTID capture, causing data/GTID mismatch.
 * Full verification requires MySQL integration testing.
 */

#ifdef USE_MYSQL

#include <gtest/gtest.h>

#include <vector>

#include "index/index.h"
#include "storage/document_store.h"

namespace mygramdb::loader {

/**
 * @brief Test fixture for batch processing logic
 *
 * These tests verify the batch processing logic used in InitialLoader
 * without requiring MySQL connection.
 */
class BatchProcessingTest : public ::testing::Test {
 protected:
  void SetUp() override {
    doc_store_ = std::make_unique<storage::DocumentStore>();
    index_ = std::make_unique<index::Index>();
  }

  std::unique_ptr<storage::DocumentStore> doc_store_;
  std::unique_ptr<index::Index> index_;
};

/**
 * @brief Test that final batch is properly indexed
 *
 * Bug #4: The last batch of documents should be indexed even when
 * it's smaller than the batch size.
 */
TEST_F(BatchProcessingTest, FinalBatchIsIndexed) {
  const size_t batch_size = 5;

  // Simulate batch processing like InitialLoader does
  std::vector<storage::DocumentStore::DocumentItem> doc_batch;
  std::vector<index::Index::DocumentItem> index_batch;

  // Add 7 items (batch_size=5, so first batch of 5, then final batch of 2)
  std::vector<std::pair<std::string, std::string>> test_data = {
      {"pk1", "text one"},   {"pk2", "text two"},   {"pk3", "text three"},
      {"pk4", "text four"},  {"pk5", "text five"},  {"pk6", "text six"},
      {"pk7", "text seven"},
  };

  size_t processed = 0;
  for (const auto& [pk, text] : test_data) {
    doc_batch.push_back({pk, {}});
    index_batch.push_back({0, text});

    // Process batch when full
    if (doc_batch.size() >= batch_size) {
      auto doc_ids_result = doc_store_->AddDocumentBatch(doc_batch);
      ASSERT_TRUE(doc_ids_result.has_value());
      auto doc_ids = *doc_ids_result;

      ASSERT_EQ(doc_ids.size(), index_batch.size())
          << "doc_ids and index_batch size mismatch in regular batch";

      for (size_t i = 0; i < doc_ids.size(); ++i) {
        index_batch[i].doc_id = doc_ids[i];
      }
      index_->AddDocumentBatch(index_batch);

      processed += doc_batch.size();
      doc_batch.clear();
      index_batch.clear();
    }
  }

  // Process final batch (Bug #4: this should work correctly)
  ASSERT_FALSE(doc_batch.empty()) << "Final batch should not be empty";
  ASSERT_EQ(doc_batch.size(), 2) << "Final batch should have 2 items";
  ASSERT_EQ(doc_batch.size(), index_batch.size())
      << "doc_batch and index_batch should have same size in final batch";

  auto doc_ids_result = doc_store_->AddDocumentBatch(doc_batch);
  ASSERT_TRUE(doc_ids_result.has_value());
  auto doc_ids = *doc_ids_result;

  ASSERT_EQ(doc_ids.size(), index_batch.size())
      << "doc_ids and index_batch size mismatch in final batch";

  for (size_t i = 0; i < doc_ids.size(); ++i) {
    index_batch[i].doc_id = doc_ids[i];
  }
  index_->AddDocumentBatch(index_batch);

  processed += doc_batch.size();

  // Verify all documents are stored and indexed
  EXPECT_EQ(processed, 7);
  EXPECT_EQ(doc_store_->Size(), 7);

  // Verify documents can be found via search
  // Index uses bigrams (2-gram) by default, so search for "te" which is in all "text X"
  auto results = index_->SearchAnd({"te"});  // All texts contain "te" bigram
  EXPECT_EQ(results.size(), 7) << "All 7 documents should be found via search";
}

/**
 * @brief Test batch processing with duplicates
 *
 * Bug #5: When duplicates exist, doc_ids may not match index_batch properly.
 */
TEST_F(BatchProcessingTest, DuplicatesHandledCorrectly) {
  std::vector<storage::DocumentStore::DocumentItem> doc_batch;
  std::vector<index::Index::DocumentItem> index_batch;

  // Add items with one duplicate
  doc_batch.push_back({"pk1", {}});
  index_batch.push_back({0, "first text"});

  doc_batch.push_back({"pk2", {}});
  index_batch.push_back({0, "second text"});

  doc_batch.push_back({"pk1", {}});  // Duplicate!
  index_batch.push_back({0, "third text"});

  doc_batch.push_back({"pk3", {}});
  index_batch.push_back({0, "fourth text"});

  auto doc_ids_result = doc_store_->AddDocumentBatch(doc_batch);
  ASSERT_TRUE(doc_ids_result.has_value());
  auto doc_ids = *doc_ids_result;

  // AddDocumentBatch returns same size (returns existing doc_id for duplicates)
  ASSERT_EQ(doc_ids.size(), doc_batch.size());

  // The issue: doc_ids[2] will be the same as doc_ids[0] (existing doc_id)
  // But index_batch[2] has different text ("third text")
  // This causes incorrect indexing if we blindly map them

  // Verify the duplicate behavior
  EXPECT_EQ(doc_ids[0], doc_ids[2]) << "Duplicate should return same doc_id";

  // To fix Bug #5: Need to skip duplicates when indexing
  // Current implementation would incorrectly index "third text" with doc_ids[0]
}

/**
 * @brief Test that size assertions catch mismatches
 */
TEST_F(BatchProcessingTest, SizeAssertionCatchesMismatch) {
  std::vector<storage::DocumentStore::DocumentItem> doc_batch;
  std::vector<index::Index::DocumentItem> index_batch;

  // Simulate a scenario where sizes could mismatch
  doc_batch.push_back({"pk1", {}});
  index_batch.push_back({0, "text1"});

  doc_batch.push_back({"pk2", {}});
  index_batch.push_back({0, "text2"});

  auto doc_ids_result = doc_store_->AddDocumentBatch(doc_batch);
  ASSERT_TRUE(doc_ids_result.has_value());
  auto doc_ids = *doc_ids_result;

  // Verify sizes match
  EXPECT_EQ(doc_ids.size(), doc_batch.size());
  EXPECT_EQ(doc_ids.size(), index_batch.size());
}

/**
 * @brief Test empty batch handling
 */
TEST_F(BatchProcessingTest, EmptyBatchHandledCorrectly) {
  std::vector<storage::DocumentStore::DocumentItem> doc_batch;
  std::vector<index::Index::DocumentItem> index_batch;

  // Both should be empty
  EXPECT_TRUE(doc_batch.empty());
  EXPECT_TRUE(index_batch.empty());

  // Empty batch should return empty result
  auto doc_ids_result = doc_store_->AddDocumentBatch(doc_batch);
  ASSERT_TRUE(doc_ids_result.has_value());
  EXPECT_TRUE(doc_ids_result->empty());
}

/**
 * @brief Test single item batch (edge case)
 */
TEST_F(BatchProcessingTest, SingleItemBatch) {
  std::vector<storage::DocumentStore::DocumentItem> doc_batch;
  std::vector<index::Index::DocumentItem> index_batch;

  doc_batch.push_back({"single_pk", {}});
  index_batch.push_back({0, "single text"});

  auto doc_ids_result = doc_store_->AddDocumentBatch(doc_batch);
  ASSERT_TRUE(doc_ids_result.has_value());
  auto doc_ids = *doc_ids_result;

  ASSERT_EQ(doc_ids.size(), 1);
  ASSERT_EQ(index_batch.size(), 1);

  index_batch[0].doc_id = doc_ids[0];
  index_->AddDocumentBatch(index_batch);

  // Verify document is indexed
  // Index uses bigrams, so search for "si" which is in "single"
  auto results = index_->SearchAnd({"si"});
  EXPECT_EQ(results.size(), 1);
}

}  // namespace mygramdb::loader

#endif  // USE_MYSQL
