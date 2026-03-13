/**
 * @file binlog_event_processor_test.cpp
 * @brief Tests for BinlogEventProcessor
 */

#ifdef USE_MYSQL

#include "mysql/binlog_event_processor.h"

#include <gtest/gtest.h>

#include "config/config.h"
#include "index/index.h"
#include "storage/document_store.h"

namespace mygramdb::mysql {

/**
 * @brief Test fixture for BinlogEventProcessor
 */
class BinlogEventProcessorTest : public ::testing::Test {
 protected:
  void SetUp() override {
    doc_store_ = std::make_unique<storage::DocumentStore>();
    index_ = std::make_unique<index::Index>();

    // Setup minimal table config
    table_config_.name = "test_table";
    table_config_.primary_key = "id";
    table_config_.text_source.column = "text";

    // Setup minimal mysql config
    mysql_config_.datetime_timezone = "UTC";
  }

  std::unique_ptr<storage::DocumentStore> doc_store_;
  std::unique_ptr<index::Index> index_;
  config::TableConfig table_config_;
  config::MysqlConfig mysql_config_;
};

/**
 * @brief Test that INSERT adds document to both store and index atomically
 */
TEST_F(BinlogEventProcessorTest, InsertIsAtomic) {
  BinlogEvent event;
  event.type = BinlogEventType::INSERT;
  event.primary_key = "pk1";
  event.text = "test document text";
  event.table_name = "test_table";

  bool result = BinlogEventProcessor::ProcessEvent(event, *index_, *doc_store_, table_config_, mysql_config_, nullptr);

  EXPECT_TRUE(result);

  // Verify document is in store
  auto doc_id_opt = doc_store_->GetDocId("pk1");
  ASSERT_TRUE(doc_id_opt.has_value());
  storage::DocId doc_id = *doc_id_opt;

  // Verify document is in index (search for bigram "te" from "test")
  auto results = index_->SearchAnd({"te"});
  ASSERT_EQ(results.size(), 1);
  EXPECT_EQ(results[0], doc_id);
}

/**
 * @brief Test that DELETE removes document from both store and index atomically
 */
TEST_F(BinlogEventProcessorTest, DeleteIsAtomic) {
  // First insert a document
  BinlogEvent insert_event;
  insert_event.type = BinlogEventType::INSERT;
  insert_event.primary_key = "pk1";
  insert_event.text = "test document text";
  insert_event.table_name = "test_table";

  bool insert_result =
      BinlogEventProcessor::ProcessEvent(insert_event, *index_, *doc_store_, table_config_, mysql_config_, nullptr);
  ASSERT_TRUE(insert_result);

  // Now delete the document
  BinlogEvent delete_event;
  delete_event.type = BinlogEventType::DELETE;
  delete_event.primary_key = "pk1";
  delete_event.text = "test document text";  // Needed for index removal
  delete_event.table_name = "test_table";

  bool delete_result =
      BinlogEventProcessor::ProcessEvent(delete_event, *index_, *doc_store_, table_config_, mysql_config_, nullptr);
  EXPECT_TRUE(delete_result);

  // Verify document is NOT in store
  auto doc_id_opt = doc_store_->GetDocId("pk1");
  EXPECT_FALSE(doc_id_opt.has_value());

  // Verify document is NOT in index
  auto results = index_->SearchAnd({"te"});
  EXPECT_EQ(results.size(), 0);
}

/**
 * @brief Test that UPDATE modifies both store and index atomically
 */
TEST_F(BinlogEventProcessorTest, UpdateIsAtomic) {
  // First insert a document
  BinlogEvent insert_event;
  insert_event.type = BinlogEventType::INSERT;
  insert_event.primary_key = "pk1";
  insert_event.text = "old document text";
  insert_event.table_name = "test_table";

  bool insert_result =
      BinlogEventProcessor::ProcessEvent(insert_event, *index_, *doc_store_, table_config_, mysql_config_, nullptr);
  ASSERT_TRUE(insert_result);

  // Now update the document
  BinlogEvent update_event;
  update_event.type = BinlogEventType::UPDATE;
  update_event.primary_key = "pk1";
  update_event.old_text = "old document text";
  update_event.text = "new document text";
  update_event.table_name = "test_table";

  bool update_result =
      BinlogEventProcessor::ProcessEvent(update_event, *index_, *doc_store_, table_config_, mysql_config_, nullptr);
  EXPECT_TRUE(update_result);

  // Verify document is still in store
  auto doc_id_opt = doc_store_->GetDocId("pk1");
  ASSERT_TRUE(doc_id_opt.has_value());
  storage::DocId doc_id = *doc_id_opt;

  // Verify old text is NOT in index (search for "ol" bigram)
  auto old_results = index_->SearchAnd({"ol"});
  EXPECT_EQ(old_results.size(), 0);

  // Verify new text IS in index (search for "ne" bigram)
  auto new_results = index_->SearchAnd({"ne"});
  ASSERT_EQ(new_results.size(), 1);
  EXPECT_EQ(new_results[0], doc_id);
}

/**
 * @brief Test that document store and index stay in sync
 *
 * Bug #6: If doc_store.AddDocument succeeds but index.AddDocument fails,
 * the document would be in the store but not searchable.
 */
TEST_F(BinlogEventProcessorTest, StoreAndIndexStayInSync) {
  // Add multiple documents
  for (int i = 1; i <= 5; ++i) {
    BinlogEvent event;
    event.type = BinlogEventType::INSERT;
    event.primary_key = "pk" + std::to_string(i);
    event.text = "document number " + std::to_string(i);
    event.table_name = "test_table";

    bool result =
        BinlogEventProcessor::ProcessEvent(event, *index_, *doc_store_, table_config_, mysql_config_, nullptr);
    ASSERT_TRUE(result);
  }

  // Verify all documents are in store and index
  EXPECT_EQ(doc_store_->Size(), 5);

  // Search for "do" bigram which should be in all "document" texts
  auto results = index_->SearchAnd({"do"});
  EXPECT_EQ(results.size(), 5);
}

/**
 * @brief Test that duplicate insert is handled correctly
 */
TEST_F(BinlogEventProcessorTest, DuplicateInsertHandled) {
  // First insert
  BinlogEvent event1;
  event1.type = BinlogEventType::INSERT;
  event1.primary_key = "pk1";
  event1.text = "first text";
  event1.table_name = "test_table";

  bool result1 =
      BinlogEventProcessor::ProcessEvent(event1, *index_, *doc_store_, table_config_, mysql_config_, nullptr);
  ASSERT_TRUE(result1);

  // Second insert with same primary key
  BinlogEvent event2;
  event2.type = BinlogEventType::INSERT;
  event2.primary_key = "pk1";
  event2.text = "second text";
  event2.table_name = "test_table";

  bool result2 =
      BinlogEventProcessor::ProcessEvent(event2, *index_, *doc_store_, table_config_, mysql_config_, nullptr);

  // The second insert should succeed (returns existing doc_id)
  EXPECT_TRUE(result2);

  // Only one document should be in store
  EXPECT_EQ(doc_store_->Size(), 1);
}

/**
 * @brief Test that DELETE of non-existent document is handled
 */
TEST_F(BinlogEventProcessorTest, DeleteNonExistentHandled) {
  BinlogEvent event;
  event.type = BinlogEventType::DELETE;
  event.primary_key = "non_existent";
  event.text = "some text";
  event.table_name = "test_table";

  bool result = BinlogEventProcessor::ProcessEvent(event, *index_, *doc_store_, table_config_, mysql_config_, nullptr);

  // Should succeed (nothing to delete)
  EXPECT_TRUE(result);
  EXPECT_EQ(doc_store_->Size(), 0);
}

/**
 * @brief Bug #34: Test that UPDATE uses Index::UpdateDocument for atomic text updates
 *
 * This test verifies that when both old_text and new_text are provided,
 * the update is performed atomically to prevent partial index states.
 */
TEST_F(BinlogEventProcessorTest, UpdateUsesAtomicIndexUpdate) {
  // First insert a document
  BinlogEvent insert_event;
  insert_event.type = BinlogEventType::INSERT;
  insert_event.primary_key = "pk1";
  insert_event.text = "original apple banana cherry";
  insert_event.table_name = "test_table";

  bool insert_result =
      BinlogEventProcessor::ProcessEvent(insert_event, *index_, *doc_store_, table_config_, mysql_config_, nullptr);
  ASSERT_TRUE(insert_result);

  // Verify initial state
  auto doc_id_opt = doc_store_->GetDocId("pk1");
  ASSERT_TRUE(doc_id_opt.has_value());
  storage::DocId doc_id = *doc_id_opt;

  // Search for "ap" (from "apple") - should find 1 document
  auto apple_results = index_->SearchAnd({"ap"});
  ASSERT_EQ(apple_results.size(), 1);
  EXPECT_EQ(apple_results[0], doc_id);

  // Now update with both old_text and new_text
  BinlogEvent update_event;
  update_event.type = BinlogEventType::UPDATE;
  update_event.primary_key = "pk1";
  update_event.old_text = "original apple banana cherry";
  update_event.text = "modified dragon elephant fig";
  update_event.table_name = "test_table";

  bool update_result =
      BinlogEventProcessor::ProcessEvent(update_event, *index_, *doc_store_, table_config_, mysql_config_, nullptr);
  EXPECT_TRUE(update_result);

  // Verify old text ngrams are removed
  auto old_results = index_->SearchAnd({"ap"});  // "apple" bigram
  EXPECT_EQ(old_results.size(), 0);

  // Verify new text ngrams are added
  auto new_results = index_->SearchAnd({"dr"});  // "dragon" bigram
  ASSERT_EQ(new_results.size(), 1);
  EXPECT_EQ(new_results[0], doc_id);

  // Verify document store still has the document
  EXPECT_TRUE(doc_store_->GetDocId("pk1").has_value());
}

/**
 * @brief Bug #34: Test that UPDATE handles doc_store.UpdateDocument return value
 *
 * When doc_store.UpdateDocument returns false (document was removed),
 * the processor should handle it gracefully.
 */
TEST_F(BinlogEventProcessorTest, UpdateHandlesStoreUpdateFailure) {
  // This test verifies that if a document is removed between GetDocId and UpdateDocument,
  // the system handles it gracefully. We can't easily simulate this race condition,
  // but we can verify that normal updates still work correctly.

  // First insert a document
  BinlogEvent insert_event;
  insert_event.type = BinlogEventType::INSERT;
  insert_event.primary_key = "pk1";
  insert_event.text = "test text";
  insert_event.table_name = "test_table";

  ASSERT_TRUE(
      BinlogEventProcessor::ProcessEvent(insert_event, *index_, *doc_store_, table_config_, mysql_config_, nullptr));

  // Update with only filter changes (no text change)
  BinlogEvent update_event;
  update_event.type = BinlogEventType::UPDATE;
  update_event.primary_key = "pk1";
  update_event.filters["status"] = static_cast<int32_t>(1);
  update_event.table_name = "test_table";

  bool update_result =
      BinlogEventProcessor::ProcessEvent(update_event, *index_, *doc_store_, table_config_, mysql_config_, nullptr);
  EXPECT_TRUE(update_result);

  // Verify document still exists
  EXPECT_TRUE(doc_store_->GetDocId("pk1").has_value());
}

/**
 * @brief Bug #34: Test that DELETE handles index removal errors gracefully
 *
 * DELETE should succeed even if the document was already partially removed
 * from the index.
 */
TEST_F(BinlogEventProcessorTest, DeleteWithEmptyText) {
  // First insert a document
  BinlogEvent insert_event;
  insert_event.type = BinlogEventType::INSERT;
  insert_event.primary_key = "pk1";
  insert_event.text = "test document";
  insert_event.table_name = "test_table";

  ASSERT_TRUE(
      BinlogEventProcessor::ProcessEvent(insert_event, *index_, *doc_store_, table_config_, mysql_config_, nullptr));

  // Delete with empty text (edge case - index removal skipped)
  BinlogEvent delete_event;
  delete_event.type = BinlogEventType::DELETE;
  delete_event.primary_key = "pk1";
  delete_event.text = "";  // Empty text means no index removal
  delete_event.table_name = "test_table";

  bool delete_result =
      BinlogEventProcessor::ProcessEvent(delete_event, *index_, *doc_store_, table_config_, mysql_config_, nullptr);
  EXPECT_TRUE(delete_result);

  // Document should be removed from store
  EXPECT_FALSE(doc_store_->GetDocId("pk1").has_value());

  // Note: Index may still have stale entries, but this is acceptable
  // for edge cases where text is not available in DELETE event
}

/**
 * @brief Bug #34: Test UPDATE transition (exists && !matches_required)
 *
 * When a document transitions out of required conditions, both index
 * and document store should be updated consistently.
 */
TEST_F(BinlogEventProcessorTest, UpdateTransitionOutOfRequired) {
  // Setup table config with required filter (proper RequiredFilterConfig format)
  config::RequiredFilterConfig required_filter;
  required_filter.name = "status";
  required_filter.type = "int";
  required_filter.op = "=";
  required_filter.value = "1";
  table_config_.required_filters.push_back(required_filter);

  // Insert a document that matches required filter
  BinlogEvent insert_event;
  insert_event.type = BinlogEventType::INSERT;
  insert_event.primary_key = "pk1";
  insert_event.text = "document text";
  insert_event.filters["status"] = static_cast<int32_t>(1);
  insert_event.table_name = "test_table";

  ASSERT_TRUE(
      BinlogEventProcessor::ProcessEvent(insert_event, *index_, *doc_store_, table_config_, mysql_config_, nullptr));

  // Verify document is indexed
  auto doc_id_opt = doc_store_->GetDocId("pk1");
  ASSERT_TRUE(doc_id_opt.has_value());
  storage::DocId doc_id = *doc_id_opt;

  auto results = index_->SearchAnd({"do"});  // "document" bigram
  ASSERT_EQ(results.size(), 1);

  // Update to not match required filter (status = 0)
  BinlogEvent update_event;
  update_event.type = BinlogEventType::UPDATE;
  update_event.primary_key = "pk1";
  update_event.text = "document text";                       // Same text for removal
  update_event.filters["status"] = static_cast<int32_t>(0);  // No longer matches required
  update_event.table_name = "test_table";

  bool update_result =
      BinlogEventProcessor::ProcessEvent(update_event, *index_, *doc_store_, table_config_, mysql_config_, nullptr);
  EXPECT_TRUE(update_result);

  // Document should be removed from both store and index
  EXPECT_FALSE(doc_store_->GetDocId("pk1").has_value());

  auto removed_results = index_->SearchAnd({"do"});
  EXPECT_EQ(removed_results.size(), 0);
}

/**
 * @brief Bug: UPDATE transition out of required filters must use old_text (before-image) for removal
 *
 * When text changes AND the document transitions out of required filters,
 * the index removal must use old_text (what was actually indexed), not event.text (after-image).
 */
TEST_F(BinlogEventProcessorTest, UpdateTransitionOutUsesOldTextForRemoval) {
  // Setup required filter (status = 1)
  config::RequiredFilterConfig required_filter;
  required_filter.name = "status";
  required_filter.type = "int";
  required_filter.op = "=";
  required_filter.value = "1";
  table_config_.required_filters.push_back(required_filter);

  // INSERT: "alpha beta" with status=1 (matches filter)
  BinlogEvent insert_event;
  insert_event.type = BinlogEventType::INSERT;
  insert_event.primary_key = "pk1";
  insert_event.text = "alpha beta";
  insert_event.filters["status"] = static_cast<int32_t>(1);
  insert_event.table_name = "test_table";

  ASSERT_TRUE(
      BinlogEventProcessor::ProcessEvent(insert_event, *index_, *doc_store_, table_config_, mysql_config_, nullptr));

  // Verify "al" (from "alpha") is indexed
  auto results_before = index_->SearchAnd({"al"});
  ASSERT_EQ(results_before.size(), 1);

  // UPDATE: text changes to "gamma delta", status=0 (transitions out of filter)
  BinlogEvent update_event;
  update_event.type = BinlogEventType::UPDATE;
  update_event.primary_key = "pk1";
  update_event.old_text = "alpha beta";     // before-image (what's in the index)
  update_event.text = "gamma delta";        // after-image (NOT in the index)
  update_event.filters["status"] = static_cast<int32_t>(0);
  update_event.table_name = "test_table";

  EXPECT_TRUE(
      BinlogEventProcessor::ProcessEvent(update_event, *index_, *doc_store_, table_config_, mysql_config_, nullptr));

  // Document removed from store
  EXPECT_FALSE(doc_store_->GetDocId("pk1").has_value());

  // Old text n-grams must be removed from index (bug: was using after-image "gamma delta")
  auto alpha_results = index_->SearchAnd({"al"});
  EXPECT_EQ(alpha_results.size(), 0);

  // After-image n-grams should not be in index either
  auto gamma_results = index_->SearchAnd({"ga"});
  EXPECT_EQ(gamma_results.size(), 0);
}

/**
 * @brief Bug: UPDATE transition out falls back to event.text when old_text is empty
 */
TEST_F(BinlogEventProcessorTest, UpdateTransitionOutFallsBackToTextWhenOldTextEmpty) {
  config::RequiredFilterConfig required_filter;
  required_filter.name = "status";
  required_filter.type = "int";
  required_filter.op = "=";
  required_filter.value = "1";
  table_config_.required_filters.push_back(required_filter);

  // INSERT: "alpha beta" with status=1
  BinlogEvent insert_event;
  insert_event.type = BinlogEventType::INSERT;
  insert_event.primary_key = "pk1";
  insert_event.text = "alpha beta";
  insert_event.filters["status"] = static_cast<int32_t>(1);
  insert_event.table_name = "test_table";

  ASSERT_TRUE(
      BinlogEventProcessor::ProcessEvent(insert_event, *index_, *doc_store_, table_config_, mysql_config_, nullptr));

  // UPDATE: old_text empty, text = "alpha beta" (same text), status=0
  BinlogEvent update_event;
  update_event.type = BinlogEventType::UPDATE;
  update_event.primary_key = "pk1";
  update_event.old_text = "";               // before-image unavailable
  update_event.text = "alpha beta";         // after-image happens to match
  update_event.filters["status"] = static_cast<int32_t>(0);
  update_event.table_name = "test_table";

  EXPECT_TRUE(
      BinlogEventProcessor::ProcessEvent(update_event, *index_, *doc_store_, table_config_, mysql_config_, nullptr));

  EXPECT_FALSE(doc_store_->GetDocId("pk1").has_value());
  auto results = index_->SearchAnd({"al"});
  EXPECT_EQ(results.size(), 0);
}

/**
 * @brief Bug: Duplicate INSERT must be idempotent (skip, not double-register n-grams)
 *
 * Without the fix, the second INSERT adds n-grams from "different text abc" to the index
 * under the same doc_id, causing index bloat and potential rollback accidents.
 */
TEST_F(BinlogEventProcessorTest, DuplicateInsertIsIdempotent) {
  // First INSERT
  BinlogEvent event1;
  event1.type = BinlogEventType::INSERT;
  event1.primary_key = "pk1";
  event1.text = "unique keyword xyz";
  event1.table_name = "test_table";

  ASSERT_TRUE(
      BinlogEventProcessor::ProcessEvent(event1, *index_, *doc_store_, table_config_, mysql_config_, nullptr));

  auto doc_id = *doc_store_->GetDocId("pk1");

  // Second INSERT with same PK but different text (replay scenario)
  BinlogEvent event2;
  event2.type = BinlogEventType::INSERT;
  event2.primary_key = "pk1";
  event2.text = "different text abc";
  event2.table_name = "test_table";

  bool result2 =
      BinlogEventProcessor::ProcessEvent(event2, *index_, *doc_store_, table_config_, mysql_config_, nullptr);

  // Should succeed (idempotent skip)
  EXPECT_TRUE(result2);

  // Only one document in store
  EXPECT_EQ(doc_store_->Size(), 1);

  // Original n-grams still present
  auto original_results = index_->SearchAnd({"xy"});
  ASSERT_EQ(original_results.size(), 1);
  EXPECT_EQ(original_results[0], doc_id);

  // Second text's n-grams must NOT be in the index
  auto duplicate_results = index_->SearchAnd({"ab"});
  EXPECT_EQ(duplicate_results.size(), 0);
}

}  // namespace mygramdb::mysql

#endif  // USE_MYSQL
