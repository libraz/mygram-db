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
#include "utils/string_utils.h"

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
  update_event.old_text = "alpha beta";  // before-image (what's in the index)
  update_event.text = "gamma delta";     // after-image (NOT in the index)
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
  update_event.old_text = "";        // before-image unavailable
  update_event.text = "alpha beta";  // after-image happens to match
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

  ASSERT_TRUE(BinlogEventProcessor::ProcessEvent(event1, *index_, *doc_store_, table_config_, mysql_config_, nullptr));

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

/**
 * @brief Bug: UPDATE on a removed document should not corrupt other documents
 *
 * When doc1 is removed from doc_store and an UPDATE arrives for doc1,
 * GetDocId returns nullopt so it falls into the !exists branch.
 * Verify doc2's index state is completely unaffected.
 */
TEST_F(BinlogEventProcessorTest, UpdateWithRemovedDocumentDoesNotCorruptIndex) {
  // INSERT doc1
  BinlogEvent insert1;
  insert1.type = BinlogEventType::INSERT;
  insert1.primary_key = "pk1";
  insert1.text = "alpha beta gamma";
  insert1.table_name = "test_table";

  ASSERT_TRUE(BinlogEventProcessor::ProcessEvent(insert1, *index_, *doc_store_, table_config_, mysql_config_, nullptr));

  // INSERT doc2
  BinlogEvent insert2;
  insert2.type = BinlogEventType::INSERT;
  insert2.primary_key = "pk2";
  insert2.text = "delta epsilon zeta";
  insert2.table_name = "test_table";

  ASSERT_TRUE(BinlogEventProcessor::ProcessEvent(insert2, *index_, *doc_store_, table_config_, mysql_config_, nullptr));

  auto doc2_id = *doc_store_->GetDocId("pk2");

  // Remove doc1 directly from doc_store (simulating concurrent removal)
  auto doc1_id = *doc_store_->GetDocId("pk1");
  doc_store_->RemoveDocument(doc1_id);

  // Send UPDATE for pk1 - since doc1 is gone from doc_store, exists=false
  BinlogEvent update_event;
  update_event.type = BinlogEventType::UPDATE;
  update_event.primary_key = "pk1";
  update_event.old_text = "alpha beta gamma";
  update_event.text = "new text here";
  update_event.table_name = "test_table";

  bool result =
      BinlogEventProcessor::ProcessEvent(update_event, *index_, *doc_store_, table_config_, mysql_config_, nullptr);

  // ProcessEvent should succeed (GTID advances)
  EXPECT_TRUE(result);

  // doc2 must still be intact in doc_store
  EXPECT_TRUE(doc_store_->GetDocId("pk2").has_value());

  // doc2's index entries must be unaffected
  auto delta_results = index_->SearchAnd({"de"});  // "delta" bigram
  ASSERT_EQ(delta_results.size(), 1);
  EXPECT_EQ(delta_results[0], doc2_id);
}

/**
 * @brief Bug: UPDATE with filter change and text change updates both correctly
 *
 * Tests the normal path through the exists && matches_required branch
 * with the new rollback/save-old-filters code to ensure it works end-to-end.
 */
TEST_F(BinlogEventProcessorTest, UpdateWithFilterChangeAndTextChange) {
  // Setup required filter so we can verify filter updates
  config::RequiredFilterConfig required_filter;
  required_filter.name = "status";
  required_filter.type = "int";
  required_filter.op = "=";
  required_filter.value = "1";
  table_config_.required_filters.push_back(required_filter);

  // INSERT doc with status=1, text="alpha beta"
  BinlogEvent insert_event;
  insert_event.type = BinlogEventType::INSERT;
  insert_event.primary_key = "pk1";
  insert_event.text = "alpha beta";
  insert_event.filters["status"] = static_cast<int32_t>(1);
  insert_event.filters["category"] = static_cast<int32_t>(10);
  insert_event.table_name = "test_table";

  ASSERT_TRUE(
      BinlogEventProcessor::ProcessEvent(insert_event, *index_, *doc_store_, table_config_, mysql_config_, nullptr));

  auto doc_id = *doc_store_->GetDocId("pk1");

  // Verify initial index state
  auto alpha_results = index_->SearchAnd({"al"});
  ASSERT_EQ(alpha_results.size(), 1);

  // UPDATE: change filters (status stays 1 so still matches) AND change text
  BinlogEvent update_event;
  update_event.type = BinlogEventType::UPDATE;
  update_event.primary_key = "pk1";
  update_event.old_text = "alpha beta";
  update_event.text = "gamma delta";
  update_event.filters["status"] = static_cast<int32_t>(1);
  update_event.filters["category"] = static_cast<int32_t>(20);  // Changed filter
  update_event.table_name = "test_table";

  bool update_result =
      BinlogEventProcessor::ProcessEvent(update_event, *index_, *doc_store_, table_config_, mysql_config_, nullptr);
  EXPECT_TRUE(update_result);

  // Verify text was updated in index
  auto old_results = index_->SearchAnd({"al"});  // "alpha" removed
  EXPECT_EQ(old_results.size(), 0);

  auto new_results = index_->SearchAnd({"ga"});  // "gamma" added
  ASSERT_EQ(new_results.size(), 1);
  EXPECT_EQ(new_results[0], doc_id);

  // Verify filters were updated in doc_store
  auto doc = doc_store_->GetDocument(doc_id);
  ASSERT_TRUE(doc.has_value());
  auto category_it = doc->filters.find("category");
  ASSERT_NE(category_it, doc->filters.end());
  EXPECT_EQ(std::get<int32_t>(category_it->second), 20);
}

/**
 * @brief Bug: UPDATE skips index when document was concurrently removed
 *
 * When GetDocId succeeds but UpdateDocument fails (race condition: doc removed
 * between the two calls), the processor should skip the index update and
 * return true so the GTID still advances.
 */
TEST_F(BinlogEventProcessorTest, UpdateSkipsIndexWhenDocumentRemoved) {
  // INSERT doc1 and doc2
  BinlogEvent insert1;
  insert1.type = BinlogEventType::INSERT;
  insert1.primary_key = "pk1";
  insert1.text = "alpha beta gamma";
  insert1.table_name = "test_table";

  ASSERT_TRUE(BinlogEventProcessor::ProcessEvent(insert1, *index_, *doc_store_, table_config_, mysql_config_, nullptr));

  BinlogEvent insert2;
  insert2.type = BinlogEventType::INSERT;
  insert2.primary_key = "pk2";
  insert2.text = "delta epsilon zeta";
  insert2.table_name = "test_table";

  ASSERT_TRUE(BinlogEventProcessor::ProcessEvent(insert2, *index_, *doc_store_, table_config_, mysql_config_, nullptr));

  auto doc1_id = *doc_store_->GetDocId("pk1");
  auto doc2_id = *doc_store_->GetDocId("pk2");

  // Manually remove doc1 from both index and doc_store
  std::string normalized = mygram::utils::NormalizeText("alpha beta gamma", index_->GetNormalizeNfkc(),
                                                        index_->GetNormalizeWidth(), index_->GetNormalizeLower());
  index_->RemoveDocument(doc1_id, normalized);
  doc_store_->RemoveDocument(doc1_id);

  // Send UPDATE for pk1 - doc is gone, so exists=false.
  // Since matches_required=true, the processor treats this as a new insert
  // (the !exists && matches_required branch), which is correct behavior.
  BinlogEvent update_event;
  update_event.type = BinlogEventType::UPDATE;
  update_event.primary_key = "pk1";
  update_event.old_text = "alpha beta gamma";
  update_event.text = "new text here";
  update_event.table_name = "test_table";

  bool result =
      BinlogEventProcessor::ProcessEvent(update_event, *index_, *doc_store_, table_config_, mysql_config_, nullptr);

  // Should return true (GTID advances)
  EXPECT_TRUE(result);

  // doc2 must be completely unaffected
  EXPECT_TRUE(doc_store_->GetDocId("pk2").has_value());

  auto delta_results = index_->SearchAnd({"de"});
  ASSERT_EQ(delta_results.size(), 1);
  EXPECT_EQ(delta_results[0], doc2_id);

  // pk1 gets re-added via the !exists && matches_required branch
  // with the new text from the UPDATE event
  EXPECT_TRUE(doc_store_->GetDocId("pk1").has_value());

  // The new text should be searchable
  auto new_results = index_->SearchAnd({"ne"});  // "new" bigram
  EXPECT_EQ(new_results.size(), 1);
}

/**
 * @brief Test that DELETE then re-INSERT with same PK maintains consistency
 *
 * After deleting a document and re-inserting with the same primary key,
 * old n-grams must be gone and new n-grams must be searchable.
 */
TEST_F(BinlogEventProcessorTest, DeleteThenReinsertMaintainsConsistency) {
  // INSERT pk1 with text "hello world"
  BinlogEvent insert_event;
  insert_event.type = BinlogEventType::INSERT;
  insert_event.primary_key = "pk1";
  insert_event.text = "hello world";
  insert_event.table_name = "test_table";

  ASSERT_TRUE(
      BinlogEventProcessor::ProcessEvent(insert_event, *index_, *doc_store_, table_config_, mysql_config_, nullptr));

  auto original_doc_id = *doc_store_->GetDocId("pk1");

  // DELETE pk1
  BinlogEvent delete_event;
  delete_event.type = BinlogEventType::DELETE;
  delete_event.primary_key = "pk1";
  delete_event.text = "hello world";
  delete_event.table_name = "test_table";

  ASSERT_TRUE(
      BinlogEventProcessor::ProcessEvent(delete_event, *index_, *doc_store_, table_config_, mysql_config_, nullptr));

  // Re-INSERT pk1 with different text
  BinlogEvent reinsert_event;
  reinsert_event.type = BinlogEventType::INSERT;
  reinsert_event.primary_key = "pk1";
  reinsert_event.text = "goodbye universe";
  reinsert_event.table_name = "test_table";

  ASSERT_TRUE(
      BinlogEventProcessor::ProcessEvent(reinsert_event, *index_, *doc_store_, table_config_, mysql_config_, nullptr));

  // pk1 should exist in doc_store (new DocId may differ from original)
  auto new_doc_id_opt = doc_store_->GetDocId("pk1");
  ASSERT_TRUE(new_doc_id_opt.has_value());
  storage::DocId new_doc_id = *new_doc_id_opt;

  // Old n-grams ("he" from "hello") must not be in index
  auto old_results = index_->SearchAnd({"he"});
  EXPECT_EQ(old_results.size(), 0);

  // New n-grams ("go" from "goodbye") must be in index
  auto new_results = index_->SearchAnd({"go"});
  ASSERT_EQ(new_results.size(), 1);
  EXPECT_EQ(new_results[0], new_doc_id);
}

/**
 * @brief Test that DELETE with correct text removes all n-grams from the index
 */
TEST_F(BinlogEventProcessorTest, DeleteWithCorrectTextRemovesAllNgrams) {
  // INSERT pk1 with text "apple banana cherry"
  BinlogEvent insert_event;
  insert_event.type = BinlogEventType::INSERT;
  insert_event.primary_key = "pk1";
  insert_event.text = "apple banana cherry";
  insert_event.table_name = "test_table";

  ASSERT_TRUE(
      BinlogEventProcessor::ProcessEvent(insert_event, *index_, *doc_store_, table_config_, mysql_config_, nullptr));

  // DELETE pk1 with correct text
  BinlogEvent delete_event;
  delete_event.type = BinlogEventType::DELETE;
  delete_event.primary_key = "pk1";
  delete_event.text = "apple banana cherry";
  delete_event.table_name = "test_table";

  ASSERT_TRUE(
      BinlogEventProcessor::ProcessEvent(delete_event, *index_, *doc_store_, table_config_, mysql_config_, nullptr));

  // doc_store should be empty
  EXPECT_EQ(doc_store_->Size(), 0);

  // All n-grams should be removed from index
  EXPECT_EQ(index_->SearchAnd({"ap"}).size(), 0);
  EXPECT_EQ(index_->SearchAnd({"ba"}).size(), 0);
  EXPECT_EQ(index_->SearchAnd({"ch"}).size(), 0);
}

/**
 * @brief Test that TRUNCATE TABLE DDL clears all data from store and index
 */
TEST_F(BinlogEventProcessorTest, TruncateClearsAllData) {
  // INSERT 3 documents
  for (const auto& [pk, text] :
       std::vector<std::pair<std::string, std::string>>{{"pk1", "alpha"}, {"pk2", "beta"}, {"pk3", "gamma"}}) {
    BinlogEvent event;
    event.type = BinlogEventType::INSERT;
    event.primary_key = pk;
    event.text = text;
    event.table_name = "test_table";

    ASSERT_TRUE(BinlogEventProcessor::ProcessEvent(event, *index_, *doc_store_, table_config_, mysql_config_, nullptr));
  }

  ASSERT_EQ(doc_store_->Size(), 3);

  // Process TRUNCATE DDL event
  BinlogEvent ddl_event;
  ddl_event.type = BinlogEventType::DDL;
  ddl_event.text = "TRUNCATE TABLE test_table";
  ddl_event.table_name = "test_table";

  bool result =
      BinlogEventProcessor::ProcessEvent(ddl_event, *index_, *doc_store_, table_config_, mysql_config_, nullptr);
  EXPECT_TRUE(result);

  // doc_store should be empty
  EXPECT_EQ(doc_store_->Size(), 0);

  // All n-grams should be removed from index
  EXPECT_EQ(index_->SearchAnd({"al"}).size(), 0);  // "alpha"
  EXPECT_EQ(index_->SearchAnd({"be"}).size(), 0);  // "beta"
  EXPECT_EQ(index_->SearchAnd({"ga"}).size(), 0);  // "gamma"
}

/**
 * @brief Test that DROP TABLE DDL clears all data from store and index
 */
TEST_F(BinlogEventProcessorTest, DropClearsAllData) {
  // INSERT 3 documents
  for (const auto& [pk, text] :
       std::vector<std::pair<std::string, std::string>>{{"pk1", "alpha"}, {"pk2", "beta"}, {"pk3", "gamma"}}) {
    BinlogEvent event;
    event.type = BinlogEventType::INSERT;
    event.primary_key = pk;
    event.text = text;
    event.table_name = "test_table";

    ASSERT_TRUE(BinlogEventProcessor::ProcessEvent(event, *index_, *doc_store_, table_config_, mysql_config_, nullptr));
  }

  ASSERT_EQ(doc_store_->Size(), 3);

  // Process DROP DDL event
  BinlogEvent ddl_event;
  ddl_event.type = BinlogEventType::DDL;
  ddl_event.text = "DROP TABLE test_table";
  ddl_event.table_name = "test_table";

  bool result =
      BinlogEventProcessor::ProcessEvent(ddl_event, *index_, *doc_store_, table_config_, mysql_config_, nullptr);
  EXPECT_TRUE(result);

  // doc_store should be empty
  EXPECT_EQ(doc_store_->Size(), 0);

  // All n-grams should be removed from index
  EXPECT_EQ(index_->SearchAnd({"al"}).size(), 0);  // "alpha"
  EXPECT_EQ(index_->SearchAnd({"be"}).size(), 0);  // "beta"
  EXPECT_EQ(index_->SearchAnd({"ga"}).size(), 0);  // "gamma"
}

/**
 * @brief Test that UPDATE with filter and text changes maintains consistency
 *
 * After updating both text and filters, old n-grams must be gone,
 * new n-grams must be searchable, and filters must reflect the new values.
 */
TEST_F(BinlogEventProcessorTest, UpdateFilterAndTextConsistency) {
  // Setup required filter
  config::RequiredFilterConfig required_filter;
  required_filter.name = "status";
  required_filter.type = "int";
  required_filter.op = "=";
  required_filter.value = "1";
  table_config_.required_filters.push_back(required_filter);

  // INSERT pk1 with text and filters
  BinlogEvent insert_event;
  insert_event.type = BinlogEventType::INSERT;
  insert_event.primary_key = "pk1";
  insert_event.text = "alpha beta";
  insert_event.filters["status"] = static_cast<int32_t>(1);
  insert_event.filters["category"] = static_cast<int32_t>(10);
  insert_event.table_name = "test_table";

  ASSERT_TRUE(
      BinlogEventProcessor::ProcessEvent(insert_event, *index_, *doc_store_, table_config_, mysql_config_, nullptr));

  auto doc_id = *doc_store_->GetDocId("pk1");

  // UPDATE pk1: change text and category filter
  BinlogEvent update_event;
  update_event.type = BinlogEventType::UPDATE;
  update_event.primary_key = "pk1";
  update_event.old_text = "alpha beta";
  update_event.text = "gamma delta";
  update_event.filters["status"] = static_cast<int32_t>(1);
  update_event.filters["category"] = static_cast<int32_t>(20);
  update_event.table_name = "test_table";

  bool update_result =
      BinlogEventProcessor::ProcessEvent(update_event, *index_, *doc_store_, table_config_, mysql_config_, nullptr);
  EXPECT_TRUE(update_result);

  // Old n-grams ("al" from "alpha") must be gone
  auto old_results = index_->SearchAnd({"al"});
  EXPECT_EQ(old_results.size(), 0);

  // New n-grams ("ga" from "gamma") must be present
  auto new_results = index_->SearchAnd({"ga"});
  ASSERT_EQ(new_results.size(), 1);
  EXPECT_EQ(new_results[0], doc_id);

  // Verify category filter is updated to 20
  auto doc = doc_store_->GetDocument(doc_id);
  ASSERT_TRUE(doc.has_value());
  auto category_it = doc->filters.find("category");
  ASSERT_NE(category_it, doc->filters.end());
  EXPECT_EQ(std::get<int32_t>(category_it->second), 20);
}

/**
 * @brief Test that filter-only UPDATE keeps text searchable
 *
 * When both old_text and text are empty (filter-only change),
 * the existing text n-grams must remain in the index.
 */
TEST_F(BinlogEventProcessorTest, UpdateFilterOnlyKeepsTextSearchable) {
  // Setup required filter
  config::RequiredFilterConfig required_filter;
  required_filter.name = "status";
  required_filter.type = "int";
  required_filter.op = "=";
  required_filter.value = "1";
  table_config_.required_filters.push_back(required_filter);

  // INSERT pk1 with text and filters
  BinlogEvent insert_event;
  insert_event.type = BinlogEventType::INSERT;
  insert_event.primary_key = "pk1";
  insert_event.text = "alpha beta";
  insert_event.filters["status"] = static_cast<int32_t>(1);
  insert_event.filters["category"] = static_cast<int32_t>(10);
  insert_event.table_name = "test_table";

  ASSERT_TRUE(
      BinlogEventProcessor::ProcessEvent(insert_event, *index_, *doc_store_, table_config_, mysql_config_, nullptr));

  auto doc_id = *doc_store_->GetDocId("pk1");

  // UPDATE pk1: filter-only change (empty old_text and text)
  BinlogEvent update_event;
  update_event.type = BinlogEventType::UPDATE;
  update_event.primary_key = "pk1";
  update_event.old_text = "";
  update_event.text = "";
  update_event.filters["status"] = static_cast<int32_t>(1);
  update_event.filters["category"] = static_cast<int32_t>(20);
  update_event.table_name = "test_table";

  bool update_result =
      BinlogEventProcessor::ProcessEvent(update_event, *index_, *doc_store_, table_config_, mysql_config_, nullptr);
  EXPECT_TRUE(update_result);

  // Text should still be searchable ("al" from "alpha")
  auto results = index_->SearchAnd({"al"});
  ASSERT_EQ(results.size(), 1);
  EXPECT_EQ(results[0], doc_id);

  // Verify category filter is updated to 20
  auto doc = doc_store_->GetDocument(doc_id);
  ASSERT_TRUE(doc.has_value());
  auto category_it = doc->filters.find("category");
  ASSERT_NE(category_it, doc->filters.end());
  EXPECT_EQ(std::get<int32_t>(category_it->second), 20);
}

/**
 * @brief Test that cache_manager parameter defaults to nullptr (backwards compatibility)
 */
TEST_F(BinlogEventProcessorTest, ProcessEventWithNullCacheManager) {
  BinlogEvent event = BinlogEvent::CreateInsert("test_table", "pk_cache1", "hello world");
  EXPECT_TRUE(BinlogEventProcessor::ProcessEvent(event, *index_, *doc_store_, table_config_, mysql_config_));
}

}  // namespace mygramdb::mysql

#endif  // USE_MYSQL
