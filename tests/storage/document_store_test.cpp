/**
 * @file document_store_test.cpp
 * @brief Unit tests for document store
 */

#include "storage/document_store.h"

#include <gtest/gtest.h>

#include <cstdio>
#include <sstream>
#include <thread>
#include <vector>

using namespace mygramdb::storage;

/**
 * @brief Test basic document addition
 */
TEST(DocumentStoreTest, AddDocument) {
  DocumentStore store;

  DocId doc_id = *store.AddDocument("pk1");
  EXPECT_EQ(doc_id, 1);
  EXPECT_EQ(store.Size(), 1);

  // Add another document
  DocId doc_id2 = *store.AddDocument("pk2");
  EXPECT_EQ(doc_id2, 2);
  EXPECT_EQ(store.Size(), 2);
}

/**
 * @brief Test document addition with filters
 */
TEST(DocumentStoreTest, AddDocumentWithFilters) {
  DocumentStore store;

  FilterMap filters;
  filters["status"] = static_cast<int64_t>(1);
  filters["category"] = static_cast<int64_t>(10);
  filters["score"] = 95.5;

  DocId doc_id = *store.AddDocument("pk1", filters);
  EXPECT_EQ(doc_id, 1);

  // Verify filters
  auto status = store.GetFilterValue(doc_id, "status");
  ASSERT_TRUE(status.has_value());
  EXPECT_EQ(std::get<int64_t>(status.value()), 1);

  auto category = store.GetFilterValue(doc_id, "category");
  ASSERT_TRUE(category.has_value());
  EXPECT_EQ(std::get<int64_t>(category.value()), 10);

  auto score = store.GetFilterValue(doc_id, "score");
  ASSERT_TRUE(score.has_value());
  EXPECT_DOUBLE_EQ(std::get<double>(score.value()), 95.5);
}

/**
 * @brief Test duplicate primary key
 */
TEST(DocumentStoreTest, DuplicatePrimaryKey) {
  DocumentStore store;

  DocId doc_id1 = *store.AddDocument("pk1");
  DocId doc_id2 = *store.AddDocument("pk1");  // Duplicate

  // Should return same DocID
  EXPECT_EQ(doc_id1, doc_id2);
  EXPECT_EQ(store.Size(), 1);
}

/**
 * @brief Test document retrieval
 */
TEST(DocumentStoreTest, GetDocument) {
  DocumentStore store;

  FilterMap filters;
  filters["status"] = static_cast<int64_t>(1);

  DocId doc_id = *store.AddDocument("pk1", filters);

  // Get document
  auto doc = store.GetDocument(doc_id);
  ASSERT_TRUE(doc.has_value());
  EXPECT_EQ(doc->doc_id, doc_id);
  EXPECT_EQ(doc->primary_key, "pk1");
  EXPECT_EQ(doc->filters.size(), 1);
  EXPECT_EQ(std::get<int64_t>(doc->filters["status"]), 1);
}

/**
 * @brief Test non-existent document
 */
TEST(DocumentStoreTest, GetNonExistentDocument) {
  DocumentStore store;

  auto doc = store.GetDocument(999);
  EXPECT_FALSE(doc.has_value());
}

/**
 * @brief Test DocID lookup
 */
TEST(DocumentStoreTest, GetDocId) {
  DocumentStore store;

  DocId doc_id = *store.AddDocument("pk1");

  auto found_id = store.GetDocId("pk1");
  ASSERT_TRUE(found_id.has_value());
  EXPECT_EQ(found_id.value(), doc_id);

  // Non-existent
  auto not_found = store.GetDocId("pk_not_exist");
  EXPECT_FALSE(not_found.has_value());
}

/**
 * @brief Test primary key lookup
 */
TEST(DocumentStoreTest, GetPrimaryKey) {
  DocumentStore store;

  DocId doc_id = *store.AddDocument("pk1");

  auto pk = store.GetPrimaryKey(doc_id);
  ASSERT_TRUE(pk.has_value());
  EXPECT_EQ(pk.value(), "pk1");

  // Non-existent
  auto not_found = store.GetPrimaryKey(999);
  EXPECT_FALSE(not_found.has_value());
}

/**
 * @brief Test document update
 */
TEST(DocumentStoreTest, UpdateDocument) {
  DocumentStore store;

  FilterMap filters1;
  filters1["status"] = static_cast<int64_t>(1);

  DocId doc_id = *store.AddDocument("pk1", filters1);

  // Update filters
  FilterMap filters2;
  filters2["status"] = static_cast<int64_t>(2);
  filters2["category"] = static_cast<int64_t>(10);

  bool updated = store.UpdateDocument(doc_id, filters2);
  EXPECT_TRUE(updated);

  // Verify updated filters
  auto doc = store.GetDocument(doc_id);
  ASSERT_TRUE(doc.has_value());
  EXPECT_EQ(doc->filters.size(), 2);
  EXPECT_EQ(std::get<int64_t>(doc->filters["status"]), 2);
  EXPECT_EQ(std::get<int64_t>(doc->filters["category"]), 10);
}

/**
 * @brief Test update non-existent document
 */
TEST(DocumentStoreTest, UpdateNonExistentDocument) {
  DocumentStore store;

  FilterMap filters;
  filters["status"] = static_cast<int64_t>(1);

  bool updated = store.UpdateDocument(999, filters);
  EXPECT_FALSE(updated);
}

/**
 * @brief Test document removal
 */
TEST(DocumentStoreTest, RemoveDocument) {
  DocumentStore store;

  DocId doc_id = *store.AddDocument("pk1");
  EXPECT_EQ(store.Size(), 1);

  bool removed = store.RemoveDocument(doc_id);
  EXPECT_TRUE(removed);
  EXPECT_EQ(store.Size(), 0);

  // Verify removal
  auto doc = store.GetDocument(doc_id);
  EXPECT_FALSE(doc.has_value());

  auto pk = store.GetPrimaryKey(doc_id);
  EXPECT_FALSE(pk.has_value());

  auto doc_id_lookup = store.GetDocId("pk1");
  EXPECT_FALSE(doc_id_lookup.has_value());
}

/**
 * @brief Test remove non-existent document
 */
TEST(DocumentStoreTest, RemoveNonExistentDocument) {
  DocumentStore store;

  bool removed = store.RemoveDocument(999);
  EXPECT_FALSE(removed);
}

/**
 * @brief Test filter by value (int)
 */
TEST(DocumentStoreTest, FilterByValueInt) {
  DocumentStore store;

  FilterMap filters1;
  filters1["status"] = static_cast<int64_t>(1);

  FilterMap filters2;
  filters2["status"] = static_cast<int64_t>(2);

  FilterMap filters3;
  filters3["status"] = static_cast<int64_t>(1);

  EXPECT_TRUE(store.AddDocument("pk1", filters1));
  EXPECT_TRUE(store.AddDocument("pk2", filters2));
  EXPECT_TRUE(store.AddDocument("pk3", filters3));

  // Filter by status=1
  auto results = store.FilterByValue("status", static_cast<int64_t>(1));
  EXPECT_EQ(results.size(), 2);
  EXPECT_EQ(results[0], 1);
  EXPECT_EQ(results[1], 3);

  // Filter by status=2
  results = store.FilterByValue("status", static_cast<int64_t>(2));
  EXPECT_EQ(results.size(), 1);
  EXPECT_EQ(results[0], 2);
}

/**
 * @brief Test filter by value (string)
 */
TEST(DocumentStoreTest, FilterByValueString) {
  DocumentStore store;

  FilterMap filters1;
  filters1["tag"] = std::string("important");

  FilterMap filters2;
  filters2["tag"] = std::string("normal");

  FilterMap filters3;
  filters3["tag"] = std::string("important");

  EXPECT_TRUE(store.AddDocument("pk1", filters1));
  EXPECT_TRUE(store.AddDocument("pk2", filters2));
  EXPECT_TRUE(store.AddDocument("pk3", filters3));

  // Filter by tag="important"
  auto results = store.FilterByValue("tag", std::string("important"));
  EXPECT_EQ(results.size(), 2);
  EXPECT_EQ(results[0], 1);
  EXPECT_EQ(results[1], 3);
}

/**
 * @brief Test filter by non-existent column
 */
TEST(DocumentStoreTest, FilterByNonExistentColumn) {
  DocumentStore store;

  FilterMap filters;
  filters["status"] = static_cast<int64_t>(1);

  EXPECT_TRUE(store.AddDocument("pk1", filters));

  auto results = store.FilterByValue("non_existent", static_cast<int64_t>(1));
  EXPECT_EQ(results.size(), 0);
}

/**
 * @brief Test memory usage
 */
TEST(DocumentStoreTest, MemoryUsage) {
  DocumentStore store;

  size_t initial = store.MemoryUsage();

  FilterMap filters;
  filters["status"] = static_cast<int64_t>(1);

  EXPECT_TRUE(store.AddDocument("pk1", filters));

  size_t after = store.MemoryUsage();
  EXPECT_GT(after, initial);
}

/**
 * @test MemoryUsage should not double-count slot overhead
 *
 * With N documents, slot overhead (sizeof(pair<DocId,string>) per capacity)
 * should be counted once via capacity-based accounting. The per-element loop
 * should only add the heap-allocated part (string::capacity()), not the
 * in-slot sizeof(DocId) + sizeof(string) again.
 */
TEST(DocumentStoreTest, MemoryUsageNoDoubleCount) {
  DocumentStore store;

  // Add enough documents to make the double-counting measurable
  constexpr int kNumDocs = 500;
  for (int i = 0; i < kNumDocs; ++i) {
    store.AddDocument("pk" + std::to_string(i));
  }

  size_t usage = store.MemoryUsage();

  // Sanity: usage should be positive and reasonable
  EXPECT_GT(usage, 0u);

  // Upper bound: each document has a short primary key (~4-6 chars).
  // With 500 docs, total memory should be well under 1 MB.
  // Before the fix, double-counting could inflate this by ~2x.
  // sizeof(DocId) + sizeof(string) ~ 40 bytes, counted twice per doc per map
  // means ~80 bytes * 500 * 2 maps = 80KB of over-counting.
  constexpr size_t kReasonableUpperBound = 512 * 1024;  // 512 KB
  EXPECT_LT(usage, kReasonableUpperBound)
      << "MemoryUsage() returned " << usage << " bytes for 500 small documents, which suggests double-counting";
}

/**
 * @brief Test clear
 */
TEST(DocumentStoreTest, Clear) {
  DocumentStore store;

  EXPECT_TRUE(store.AddDocument("pk1"));
  EXPECT_TRUE(store.AddDocument("pk2"));
  EXPECT_EQ(store.Size(), 2);

  store.Clear();
  EXPECT_EQ(store.Size(), 0);

  // Verify all data is cleared
  auto doc = store.GetDocument(1);
  EXPECT_FALSE(doc.has_value());

  auto pk = store.GetDocId("pk1");
  EXPECT_FALSE(pk.has_value());
}

/**
 * @brief Test large document set
 */
TEST(DocumentStoreTest, LargeDocumentSet) {
  DocumentStore store;

  // Add 10000 documents
  for (int i = 0; i < 10000; ++i) {
    std::string pk = "pk" + std::to_string(i);
    FilterMap filters;
    filters["status"] = static_cast<int64_t>(i % 10);

    DocId doc_id = *store.AddDocument(pk, filters);
    EXPECT_EQ(doc_id, static_cast<DocId>(i + 1));
  }

  EXPECT_EQ(store.Size(), 10000);

  // Verify lookup
  auto doc_id = store.GetDocId("pk5000");
  ASSERT_TRUE(doc_id.has_value());
  EXPECT_EQ(doc_id.value(), 5001);

  // Filter by status=5 (should have 1000 documents)
  auto results = store.FilterByValue("status", static_cast<int64_t>(5));
  EXPECT_EQ(results.size(), 1000);
}

/**
 * @brief Test concurrent read access (simulating 10000 parallel clients)
 *
 * Note: DocumentStore is designed for read-heavy workloads.
 * Writes are typically from a single binlog thread.
 */
TEST(DocumentStoreTest, ConcurrentReads) {
  DocumentStore store;

  // Add documents
  for (int i = 0; i < 1000; ++i) {
    std::string pk = "pk" + std::to_string(i);
    FilterMap filters;
    filters["status"] = static_cast<int64_t>(i % 10);
    EXPECT_TRUE(store.AddDocument(pk, filters));
  }

  // Simulate concurrent reads
  const int num_threads = 100;       // Simulate 100 concurrent threads
  const int reads_per_thread = 100;  // Each thread does 100 reads

  std::vector<std::thread> threads;
  std::atomic<int> success_count{0};

  for (int t = 0; t < num_threads; ++t) {
    threads.emplace_back([&store, &success_count]() {
      for (int i = 0; i < reads_per_thread; ++i) {
        // Random lookups
        DocId doc_id = (i % 1000) + 1;
        auto doc = store.GetDocument(doc_id);
        if (doc.has_value()) {
          success_count++;
        }

        // Filter lookups
        auto results = store.FilterByValue("status", static_cast<int64_t>(i % 10));
        if (!results.empty()) {
          success_count++;
        }
      }
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }

  // All reads should succeed
  EXPECT_EQ(success_count, num_threads * reads_per_thread * 2);
}

/**
 * @brief Test DocID auto-increment
 */
TEST(DocumentStoreTest, DocIdAutoIncrement) {
  DocumentStore store;

  DocId id1 = *store.AddDocument("pk1");
  DocId id2 = *store.AddDocument("pk2");
  DocId id3 = *store.AddDocument("pk3");

  EXPECT_EQ(id1, 1);
  EXPECT_EQ(id2, 2);
  EXPECT_EQ(id3, 3);

  // Remove middle document
  store.RemoveDocument(id2);

  // Next ID should still be 4 (not reusing removed IDs)
  DocId id4 = *store.AddDocument("pk4");
  EXPECT_EQ(id4, 4);
}

/**
 * @brief Test DocID sequential assignment and size tracking
 *
 * Verifies that the DocumentStore assigns monotonically increasing DocIDs
 * and correctly tracks the number of documents.
 */
TEST(DocumentStoreTest, DocIdSequentialAssignmentAndSize) {
  DocumentStore store;

  // This test requires access to internal state for setting next_doc_id_
  // We can't easily test the actual overflow without adding 4 billion documents,
  // so we verify the logic by checking error handling near the boundary.

  // Add a few documents normally
  DocId id1 = *store.AddDocument("pk1");
  EXPECT_EQ(id1, 1);

  // The overflow check should prevent assignment of UINT32_MAX
  // and should detect when next_doc_id_ == 0 or UINT32_MAX
  // This test documents the expected behavior:
  // 1. IDs 1 through UINT32_MAX-1 are valid
  // 2. UINT32_MAX is NOT assigned (reserved as sentinel)
  // 3. The check happens BEFORE increment to prevent wraparound

  // Since we can't manipulate internal state without making it public,
  // we document the boundary conditions here:
  // - If next_doc_id_ == UINT32_MAX, AddDocument should fail
  // - If next_doc_id_ == 0, AddDocument should fail
  // - This prevents any document from getting ID 0 or UINT32_MAX

  // Normal operation should continue to work
  DocId id2 = *store.AddDocument("pk2");
  EXPECT_EQ(id2, 2);
  DocId id3 = *store.AddDocument("pk3");
  EXPECT_EQ(id3, 3);

  EXPECT_EQ(store.Size(), 3);
}

/**
 * @brief Test mixed filter types
 */
TEST(DocumentStoreTest, MixedFilterTypes) {
  DocumentStore store;

  FilterMap filters;
  filters["status"] = static_cast<int64_t>(1);
  filters["tag"] = std::string("important");
  filters["score"] = 98.5;

  DocId doc_id = *store.AddDocument("pk1", filters);

  // Verify all types
  auto status = store.GetFilterValue(doc_id, "status");
  ASSERT_TRUE(status.has_value());
  EXPECT_EQ(std::get<int64_t>(status.value()), 1);

  auto tag = store.GetFilterValue(doc_id, "tag");
  ASSERT_TRUE(tag.has_value());
  EXPECT_EQ(std::get<std::string>(tag.value()), "important");

  auto score = store.GetFilterValue(doc_id, "score");
  ASSERT_TRUE(score.has_value());
  EXPECT_DOUBLE_EQ(std::get<double>(score.value()), 98.5);
}

/**
 * @brief Test batch document addition
 */
TEST(DocumentStoreTest, AddDocumentBatch) {
  DocumentStore store;

  // Prepare batch of documents
  std::vector<DocumentStore::DocumentItem> batch;
  batch.push_back({"pk1", {}});
  batch.push_back({"pk2", {{"status", static_cast<int32_t>(1)}}});
  batch.push_back({"pk3", {{"status", static_cast<int32_t>(2)}}});

  // Add batch
  std::vector<DocId> doc_ids = *store.AddDocumentBatch(batch);

  // Verify doc_ids were assigned sequentially
  EXPECT_EQ(doc_ids.size(), 3);
  EXPECT_EQ(doc_ids[0], 1);
  EXPECT_EQ(doc_ids[1], 2);
  EXPECT_EQ(doc_ids[2], 3);

  // Verify documents can be retrieved
  auto doc1 = store.GetDocument(doc_ids[0]);
  ASSERT_TRUE(doc1.has_value());
  EXPECT_EQ(doc1->primary_key, "pk1");

  auto doc2 = store.GetDocument(doc_ids[1]);
  ASSERT_TRUE(doc2.has_value());
  EXPECT_EQ(doc2->primary_key, "pk2");

  // Verify filter values
  auto status2 = store.GetFilterValue(doc_ids[1], "status");
  ASSERT_TRUE(status2.has_value());
  EXPECT_EQ(std::get<int32_t>(status2.value()), 1);
}

/**
 * @brief Test empty batch addition
 */
TEST(DocumentStoreTest, AddDocumentBatchEmpty) {
  DocumentStore store;

  std::vector<DocumentStore::DocumentItem> batch;
  std::vector<DocId> doc_ids = *store.AddDocumentBatch(batch);

  EXPECT_EQ(doc_ids.size(), 0);
  EXPECT_EQ(store.Size(), 0);
}

/**
 * @brief Test large batch addition
 */
TEST(DocumentStoreTest, AddDocumentBatchLarge) {
  DocumentStore store;

  // Create large batch (10000 documents)
  std::vector<DocumentStore::DocumentItem> batch;
  for (int i = 1; i <= 10000; ++i) {
    std::string pk = "pk" + std::to_string(i);
    batch.push_back({pk, {{"index", static_cast<int32_t>(i)}}});
  }

  std::vector<DocId> doc_ids = *store.AddDocumentBatch(batch);

  // Verify all documents were added
  EXPECT_EQ(doc_ids.size(), 10000);
  EXPECT_EQ(store.Size(), 10000);

  // Verify sequential doc_id assignment
  for (size_t i = 0; i < doc_ids.size(); ++i) {
    EXPECT_EQ(doc_ids[i], static_cast<DocId>(i + 1));
  }

  // Spot check a few documents
  auto doc1 = store.GetDocument(1);
  ASSERT_TRUE(doc1.has_value());
  EXPECT_EQ(doc1->primary_key, "pk1");

  auto doc5000 = store.GetDocument(5000);
  ASSERT_TRUE(doc5000.has_value());
  EXPECT_EQ(doc5000->primary_key, "pk5000");
}

/**
 * @brief Test batch addition with duplicate primary keys
 */
TEST(DocumentStoreTest, AddDocumentBatchDuplicates) {
  DocumentStore store;

  // Add initial document
  EXPECT_TRUE(store.AddDocument("pk1", {{"status", static_cast<int32_t>(1)}}));

  // Try to add batch with duplicate primary key
  std::vector<DocumentStore::DocumentItem> batch;
  batch.push_back({"pk1", {{"status", static_cast<int32_t>(2)}}});  // Duplicate
  batch.push_back({"pk2", {{"status", static_cast<int32_t>(3)}}});  // New

  std::vector<DocId> doc_ids = *store.AddDocumentBatch(batch);

  // Verify duplicate returns existing doc_id
  EXPECT_EQ(doc_ids[0], 1);  // Existing doc_id
  EXPECT_EQ(doc_ids[1], 2);  // New doc_id

  // Verify only 2 documents in store (not 3)
  EXPECT_EQ(store.Size(), 2);

  // Verify first document was not modified
  auto status1 = store.GetFilterValue(1, "status");
  ASSERT_TRUE(status1.has_value());
  EXPECT_EQ(std::get<int32_t>(status1.value()), 1);  // Original value
}

/**
 * @brief Test storing documents with 4-byte emoji characters
 */
TEST(DocumentStoreTest, EmojiInDocuments) {
  DocumentStore store;

  // Add document with emoji in primary key
  DocId doc_id1 = *store.AddDocument("😀_pk1", {});
  EXPECT_GT(doc_id1, 0);

  // Add document with emoji in filter value (string)
  FilterMap filters;
  filters["title"] = FilterValue("Tutorial😀🎉");
  filters["category"] = FilterValue("楽しい😀学習");
  DocId doc_id2 = *store.AddDocument("pk2", filters);
  EXPECT_GT(doc_id2, 0);

  // Verify retrieval
  auto doc1 = store.GetDocument(doc_id1);
  ASSERT_TRUE(doc1.has_value());
  EXPECT_EQ(doc1->primary_key, "😀_pk1");

  auto doc2 = store.GetDocument(doc_id2);
  ASSERT_TRUE(doc2.has_value());
  auto title = store.GetFilterValue(doc_id2, "title");
  ASSERT_TRUE(title.has_value());
  EXPECT_EQ(std::get<std::string>(title.value()), "Tutorial😀🎉");

  auto category = store.GetFilterValue(doc_id2, "category");
  ASSERT_TRUE(category.has_value());
  EXPECT_EQ(std::get<std::string>(category.value()), "楽しい😀学習");
}

/**
 * @brief Test GetDocId with emoji
 */
TEST(DocumentStoreTest, EmojiPrimaryKeyLookup) {
  DocumentStore store;

  // Add documents with emoji primary keys
  EXPECT_TRUE(store.AddDocument("😀", {}));
  EXPECT_TRUE(store.AddDocument("🎉", {}));
  EXPECT_TRUE(store.AddDocument("👍", {}));

  // Lookup by emoji
  auto doc_id1 = store.GetDocId("😀");
  ASSERT_TRUE(doc_id1.has_value());
  EXPECT_EQ(doc_id1.value(), 1);

  auto doc_id2 = store.GetDocId("🎉");
  ASSERT_TRUE(doc_id2.has_value());
  EXPECT_EQ(doc_id2.value(), 2);

  auto doc_id3 = store.GetDocId("👍");
  ASSERT_TRUE(doc_id3.has_value());
  EXPECT_EQ(doc_id3.value(), 3);

  // Non-existent emoji
  auto not_found = store.GetDocId("🚀");
  EXPECT_FALSE(not_found.has_value());
}

/**
 * @brief Test emoji in filter values
 */
TEST(DocumentStoreTest, EmojiFilterValues) {
  DocumentStore store;

  // Add document with various emoji filter values
  FilterMap filters;
  filters["mood"] = FilterValue("😀");
  filters["celebration"] = FilterValue("🎉");
  filters["rating"] = FilterValue("👍");
  filters["mixed"] = FilterValue("Hello😀World🎉");

  DocId doc_id = *store.AddDocument("pk1", filters);

  // Verify all emoji filter values
  auto mood = store.GetFilterValue(doc_id, "mood");
  ASSERT_TRUE(mood.has_value());
  EXPECT_EQ(std::get<std::string>(mood.value()), "😀");

  auto celebration = store.GetFilterValue(doc_id, "celebration");
  ASSERT_TRUE(celebration.has_value());
  EXPECT_EQ(std::get<std::string>(celebration.value()), "🎉");

  auto rating = store.GetFilterValue(doc_id, "rating");
  ASSERT_TRUE(rating.has_value());
  EXPECT_EQ(std::get<std::string>(rating.value()), "👍");

  auto mixed = store.GetFilterValue(doc_id, "mixed");
  ASSERT_TRUE(mixed.has_value());
  EXPECT_EQ(std::get<std::string>(mixed.value()), "Hello😀World🎉");
}

/**
 * @brief Test batch operations with emojis
 */
TEST(DocumentStoreTest, EmojiBatchOperations) {
  DocumentStore store;

  // Create batch with emoji data
  std::vector<DocumentStore::DocumentItem> batch;
  for (int i = 0; i < 100; ++i) {
    FilterMap filters;
    filters["emoji"] = FilterValue("😀");
    filters["number"] = FilterValue(i);
    batch.push_back({"emoji_pk_" + std::to_string(i), filters});
  }

  // Add batch
  std::vector<DocId> doc_ids = *store.AddDocumentBatch(batch);
  EXPECT_EQ(doc_ids.size(), 100);
  EXPECT_EQ(store.Size(), 100);

  // Verify emoji filter values
  for (size_t i = 0; i < doc_ids.size(); ++i) {
    auto emoji = store.GetFilterValue(doc_ids[i], "emoji");
    ASSERT_TRUE(emoji.has_value());
    EXPECT_EQ(std::get<std::string>(emoji.value()), "😀");
  }
}

/**
 * @brief Test complex emoji (with modifiers)
 */
TEST(DocumentStoreTest, ComplexEmoji) {
  DocumentStore store;

  // Emoji with skin tone modifier
  FilterMap filters;
  filters["thumbs"] = FilterValue("👍🏽");                       // Medium skin tone
  filters["family"] = FilterValue("👨‍👩‍👧‍👦");  // Family with ZWJ

  DocId doc_id = *store.AddDocument("complex", filters);

  // Verify retrieval
  auto thumbs = store.GetFilterValue(doc_id, "thumbs");
  ASSERT_TRUE(thumbs.has_value());
  EXPECT_EQ(std::get<std::string>(thumbs.value()), "👍🏽");

  auto family = store.GetFilterValue(doc_id, "family");
  ASSERT_TRUE(family.has_value());
  EXPECT_EQ(std::get<std::string>(family.value()), "👨‍👩‍👧‍👦");
}

/**
 * @brief Test concurrent writes from multiple threads
 *
 * DocumentStore should handle concurrent writes safely.
 * This test simulates multiple binlog threads or concurrent updates.
 */
TEST(DocumentStoreTest, ConcurrentWrites) {
  DocumentStore store;

  const int num_threads = 10;
  const int writes_per_thread = 100;

  std::vector<std::thread> threads;
  std::atomic<int> success_count{0};

  for (int t = 0; t < num_threads; ++t) {
    threads.emplace_back([&store, &success_count, t]() {
      for (int i = 0; i < writes_per_thread; ++i) {
        std::string pk = "pk_thread" + std::to_string(t) + "_doc" + std::to_string(i);
        FilterMap filters;
        filters["thread_id"] = static_cast<int64_t>(t);
        filters["doc_num"] = static_cast<int64_t>(i);

        DocId doc_id = *store.AddDocument(pk, filters);
        if (doc_id > 0) {
          success_count++;
        }
      }
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }

  // All writes should succeed
  EXPECT_EQ(success_count.load(), num_threads * writes_per_thread);
  EXPECT_EQ(store.Size(), static_cast<size_t>(num_threads * writes_per_thread));

  // Verify data integrity - spot check some documents
  auto doc_id = store.GetDocId("pk_thread0_doc0");
  ASSERT_TRUE(doc_id.has_value());

  auto thread_id = store.GetFilterValue(doc_id.value(), "thread_id");
  ASSERT_TRUE(thread_id.has_value());
  EXPECT_EQ(std::get<int64_t>(thread_id.value()), 0);
}

/**
 * @brief Test concurrent mixed read and write operations
 *
 * Simulates realistic workload with both reads and writes happening concurrently.
 */
TEST(DocumentStoreTest, ConcurrentReadWrite) {
  DocumentStore store;

  // Pre-populate store with some documents
  for (int i = 0; i < 100; ++i) {
    std::string pk = "initial_pk" + std::to_string(i);
    FilterMap filters;
    filters["status"] = static_cast<int64_t>(i % 10);
    EXPECT_TRUE(store.AddDocument(pk, filters));
  }

  const int num_reader_threads = 20;
  const int num_writer_threads = 5;
  const int operations_per_thread = 100;

  std::vector<std::thread> threads;
  std::atomic<int> read_success{0};
  std::atomic<int> write_success{0};

  // Launch reader threads
  for (int t = 0; t < num_reader_threads; ++t) {
    threads.emplace_back([&store, &read_success]() {
      for (int i = 0; i < operations_per_thread; ++i) {
        // Read operations
        DocId doc_id = (i % 100) + 1;
        auto doc = store.GetDocument(doc_id);
        if (doc.has_value()) {
          read_success++;
        }

        // Filter operations
        auto results = store.FilterByValue("status", static_cast<int64_t>(i % 10));
        if (!results.empty()) {
          read_success++;
        }
      }
    });
  }

  // Launch writer threads
  for (int t = 0; t < num_writer_threads; ++t) {
    threads.emplace_back([&store, &write_success, t]() {
      for (int i = 0; i < operations_per_thread; ++i) {
        std::string pk = "new_pk_thread" + std::to_string(t) + "_doc" + std::to_string(i);
        FilterMap filters;
        filters["status"] = static_cast<int64_t>(i % 10);
        filters["thread_id"] = static_cast<int64_t>(t);

        DocId doc_id = *store.AddDocument(pk, filters);
        if (doc_id > 0) {
          write_success++;
        }
      }
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }

  // All operations should succeed
  EXPECT_EQ(write_success.load(), num_writer_threads * operations_per_thread);
  // Reads should mostly succeed (some may fail due to concurrent modifications)
  EXPECT_GT(read_success.load(), 0);

  // Verify final size
  EXPECT_EQ(store.Size(), 100 + static_cast<size_t>(num_writer_threads * operations_per_thread));
}

/**
 * @brief Test concurrent updates to the same documents
 *
 * Multiple threads updating different fields of the same documents.
 */
TEST(DocumentStoreTest, ConcurrentUpdates) {
  DocumentStore store;

  // Add documents
  std::vector<DocId> doc_ids;
  for (int i = 0; i < 100; ++i) {
    std::string pk = "pk" + std::to_string(i);
    FilterMap filters;
    filters["value"] = static_cast<int64_t>(0);
    doc_ids.push_back(*store.AddDocument(pk, filters));
  }

  const int num_threads = 10;
  const int updates_per_thread = 50;

  std::vector<std::thread> threads;
  std::atomic<int> update_success{0};

  for (int t = 0; t < num_threads; ++t) {
    threads.emplace_back([&store, &doc_ids, &update_success, t]() {
      for (int i = 0; i < updates_per_thread; ++i) {
        DocId doc_id = doc_ids[i % doc_ids.size()];
        FilterMap filters;
        filters["thread_" + std::to_string(t)] = static_cast<int64_t>(i);

        bool updated = store.UpdateDocument(doc_id, filters);
        if (updated) {
          update_success++;
        }
      }
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }

  // All updates should succeed
  EXPECT_EQ(update_success.load(), num_threads * updates_per_thread);

  // Verify some documents have been updated
  for (auto doc_id : doc_ids) {
    auto doc = store.GetDocument(doc_id);
    ASSERT_TRUE(doc.has_value());
    // Document should have multiple filters from different threads
    EXPECT_GE(doc->filters.size(), 1);
  }
}

/**
 * @brief Test concurrent deletes
 *
 * Multiple threads deleting different documents concurrently.
 */
TEST(DocumentStoreTest, ConcurrentDeletes) {
  DocumentStore store;

  // Add documents
  std::vector<DocId> doc_ids;
  for (int i = 0; i < 1000; ++i) {
    std::string pk = "pk" + std::to_string(i);
    doc_ids.push_back(*store.AddDocument(pk));
  }

  const int num_threads = 10;
  const int deletes_per_thread = 100;

  std::vector<std::thread> threads;
  std::atomic<int> delete_success{0};

  for (int t = 0; t < num_threads; ++t) {
    threads.emplace_back([&store, &doc_ids, &delete_success, t]() {
      for (int i = 0; i < deletes_per_thread; ++i) {
        int doc_index = t * deletes_per_thread + i;
        DocId doc_id = doc_ids[doc_index];

        bool removed = store.RemoveDocument(doc_id);
        if (removed) {
          delete_success++;
        }
      }
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }

  // All deletes should succeed
  EXPECT_EQ(delete_success.load(), num_threads * deletes_per_thread);
  EXPECT_EQ(store.Size(), 1000 - static_cast<size_t>(num_threads * deletes_per_thread));

  // Verify deleted documents are gone
  for (int i = 0; i < num_threads * deletes_per_thread; ++i) {
    auto doc = store.GetDocument(doc_ids[i]);
    EXPECT_FALSE(doc.has_value());
  }
}

/**
 * @brief Test concurrent batch operations
 *
 * Multiple threads performing batch additions concurrently.
 */
TEST(DocumentStoreTest, ConcurrentBatchOperations) {
  DocumentStore store;

  const int num_threads = 10;
  const int batch_size = 100;

  std::vector<std::thread> threads;
  std::atomic<int> total_added{0};

  for (int t = 0; t < num_threads; ++t) {
    threads.emplace_back([&store, &total_added, t]() {
      std::vector<DocumentStore::DocumentItem> batch;
      for (int i = 0; i < 100; ++i) {
        std::string pk = "batch_thread" + std::to_string(t) + "_doc" + std::to_string(i);
        FilterMap filters;
        filters["batch_id"] = static_cast<int64_t>(t);
        batch.push_back({pk, filters});
      }

      std::vector<DocId> doc_ids = *store.AddDocumentBatch(batch);
      total_added += doc_ids.size();
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }

  // All documents should be added
  EXPECT_EQ(total_added.load(), num_threads * batch_size);
  EXPECT_EQ(store.Size(), static_cast<size_t>(num_threads * batch_size));
}

/**
 * @brief Test concurrent read-write with filter queries
 *
 * Realistic scenario: reads and writes with filter-based queries.
 */
TEST(DocumentStoreTest, ConcurrentFilterOperations) {
  DocumentStore store;

  // Pre-populate
  for (int i = 0; i < 500; ++i) {
    std::string pk = "pk" + std::to_string(i);
    FilterMap filters;
    filters["category"] = static_cast<int64_t>(i % 20);
    EXPECT_TRUE(store.AddDocument(pk, filters));
  }

  const int num_threads = 20;
  const int operations_per_thread = 100;

  std::vector<std::thread> threads;
  std::atomic<int> filter_queries{0};

  for (int t = 0; t < num_threads; ++t) {
    threads.emplace_back([&store, &filter_queries]() {
      for (int i = 0; i < operations_per_thread; ++i) {
        // Perform filter query
        auto results = store.FilterByValue("category", static_cast<int64_t>(i % 20));
        if (!results.empty()) {
          filter_queries++;
        }
      }
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }

  // Most filter queries should succeed
  EXPECT_GT(filter_queries.load(), num_threads * operations_per_thread * 0.9);
}

// =============================================================================
// DocumentStore filter map buckets not reclaimed
// =============================================================================
// After many insertions and deletions, the unordered_map bucket count doesn't
// shrink, causing memory waste. The Compact() method should reduce memory usage.
// =============================================================================

/**
 * @test Memory should be reclaimed after Compact()
 *
 * After adding and removing many documents, calling Compact() should
 * reduce memory usage by rehashing the internal hash maps.
 */
TEST(DocumentStoreTest, Bug20_CompactReducesMemoryUsage) {
  DocumentStore store;

  // Add many documents
  const int num_docs = 10000;
  for (int i = 0; i < num_docs; ++i) {
    std::string pk = "pk" + std::to_string(i);
    FilterMap filters;
    filters["category"] = static_cast<int64_t>(i % 100);
    filters["status"] = std::string("active");
    store.AddDocument(pk, filters);
  }

  // Record peak memory with all documents
  size_t memory_peak = store.MemoryUsage();

  // Remove most documents (keep only 100)
  for (int i = 100; i < num_docs; ++i) {
    store.RemoveDocument(static_cast<DocId>(i + 1));  // DocId starts at 1
  }

  // Verify only 100 documents remain
  EXPECT_EQ(100, store.Size());

  // Memory before compact: documents removed but hash map buckets not reclaimed
  size_t memory_before_compact = store.MemoryUsage();

  // Compact the store
  store.Compact();

  // Get memory usage after compact
  size_t memory_after_compact = store.MemoryUsage();

  // Compact() should rehash maps and reduce bucket overhead.
  // After compaction, memory should be less than before compaction.
  EXPECT_LT(memory_after_compact, memory_before_compact) << "Compact() should reduce memory usage. "
                                                         << "Before compact: " << memory_before_compact << " bytes, "
                                                         << "After compact: " << memory_after_compact << " bytes";

  // After removing 99% of documents and compacting, memory should be
  // significantly less than the peak with all 10000 documents.
  EXPECT_LT(memory_after_compact, memory_peak * 0.5) << "Memory after removing 99% of docs and compacting should be "
                                                     << "well below peak. Peak: " << memory_peak << " bytes, "
                                                     << "After compact: " << memory_after_compact << " bytes";
}

/**
 * @test Clear() should also reclaim memory
 */
TEST(DocumentStoreTest, Bug20_ClearReclaimsMemory) {
  DocumentStore store;

  // Add many documents
  for (int i = 0; i < 5000; ++i) {
    std::string pk = "pk" + std::to_string(i);
    FilterMap filters;
    filters["value"] = static_cast<int64_t>(i);
    store.AddDocument(pk, filters);
  }

  // Memory should be significant
  size_t memory_after_add = store.MemoryUsage();
  EXPECT_GT(memory_after_add, 0);

  // Clear all documents
  store.Clear();

  // Memory should be minimal after clear
  size_t memory_after_clear = store.MemoryUsage();

  // After fix, Clear() should release most memory
  EXPECT_LT(memory_after_clear, memory_after_add * 0.1) << "Clear() should release most memory";
}

/**
 * @test Stream error checking during LoadFromFile
 *
 * This test verifies that stream read operations during snapshot loading
 * have proper error checking to detect corrupted or truncated files.
 * The fix adds ifs.good() checks after ifs.read() operations.
 *
 * Note: Direct testing of stream errors requires creating corrupted files,
 * which is complex in unit tests. This test verifies basic load functionality
 * still works correctly after the fix.
 */
TEST(DocumentStoreTest, Bug36_LoadFromFileBasicValidation) {
  DocumentStore store;

  // Add documents with various filter types to exercise all read paths
  FilterMap filters1;
  filters1["status"] = static_cast<int32_t>(1);
  filters1["name"] = std::string("test document with long name");
  filters1["score"] = 95.5;
  store.AddDocument("pk1", filters1);

  FilterMap filters2;
  filters2["status"] = static_cast<int64_t>(2);
  filters2["is_active"] = true;
  store.AddDocument("pk2", filters2);

  // Save to file
  std::string filepath = "/tmp/bug36_test_snapshot.bin";
  auto save_result = store.SaveToFile(filepath);
  ASSERT_TRUE(save_result.has_value()) << "Save should succeed";

  // Load into a new store
  DocumentStore store2;
  auto load_result = store2.LoadFromFile(filepath);
  ASSERT_TRUE(load_result.has_value()) << "Load should succeed for valid file";

  // Verify loaded data
  EXPECT_EQ(store2.Size(), 2);
  EXPECT_TRUE(store2.GetDocId("pk1").has_value());
  EXPECT_TRUE(store2.GetDocId("pk2").has_value());

  // Verify filters are loaded correctly
  auto doc1 = store2.GetDocId("pk1");
  auto status = store2.GetFilterValue(*doc1, "status");
  ASSERT_TRUE(status.has_value());
  EXPECT_EQ(std::get<int32_t>(*status), 1);

  auto name = store2.GetFilterValue(*doc1, "name");
  ASSERT_TRUE(name.has_value());
  EXPECT_EQ(std::get<std::string>(*name), "test document with long name");

  // Clean up
  std::remove(filepath.c_str());
}

// =============================================================================
// GetNormalizedText / SetNormalizedText tests (verify_text feature)
// =============================================================================

/**
 * @brief Test that GetNormalizedText returns the text set via SetNormalizedText
 */
TEST(DocumentStoreTest, GetNormalizedText_ReturnsText) {
  DocumentStore store;

  DocId doc_id = *store.AddDocument("pk1");

  store.SetNormalizedText(doc_id, "hello world");

  auto text = store.GetNormalizedText(doc_id);
  ASSERT_TRUE(text.has_value());
  EXPECT_EQ(text.value(), "hello world");
}

/**
 * @brief Test that GetNormalizedText returns nullopt for a non-existent doc
 */
TEST(DocumentStoreTest, GetNormalizedText_ReturnsNulloptForMissingDoc) {
  DocumentStore store;

  auto text = store.GetNormalizedText(999);
  EXPECT_FALSE(text.has_value());
}

/**
 * @brief Test that GetNormalizedText returns an owned copy not invalidated by
 *        subsequent operations on the same doc
 */
TEST(DocumentStoreTest, GetNormalizedText_ReturnsOwnedString) {
  DocumentStore store;

  DocId doc_id = *store.AddDocument("pk1");

  store.SetNormalizedText(doc_id, "original text");

  // Get the text - should be an owned copy
  auto text = store.GetNormalizedText(doc_id);
  ASSERT_TRUE(text.has_value());
  EXPECT_EQ(text.value(), "original text");

  // Overwrite the normalized text for the same doc
  store.SetNormalizedText(doc_id, "replaced text");

  // The previously returned string should still hold the original value
  EXPECT_EQ(text.value(), "original text");

  // A new call should return the updated text
  auto text2 = store.GetNormalizedText(doc_id);
  ASSERT_TRUE(text2.has_value());
  EXPECT_EQ(text2.value(), "replaced text");
}

// =============================================================================
// doc_texts_ snapshot serialization tests (v2 format)
// =============================================================================

/**
 * @brief SaveToStream/LoadFromStream round-trip preserves doc_texts_
 */
TEST(DocumentStoreTest, StreamRoundTrip_PreservesDocTexts) {
  DocumentStore store;

  auto doc_id1 = *store.AddDocument("pk1", {{"status", static_cast<int32_t>(1)}});
  auto doc_id2 = *store.AddDocument("pk2", {{"status", static_cast<int32_t>(2)}});
  auto doc_id3 = *store.AddDocument("pk3");

  store.SetNormalizedText(doc_id1, "hello world");
  store.SetNormalizedText(doc_id2, "goodbye universe");
  // doc_id3: no normalized text set

  // Save to stream
  std::stringstream ss;
  auto save_result = store.SaveToStream(ss);
  ASSERT_TRUE(save_result.has_value()) << "SaveToStream should succeed";

  // Load into a new store
  DocumentStore store2;
  auto load_result = store2.LoadFromStream(ss);
  ASSERT_TRUE(load_result.has_value()) << "LoadFromStream should succeed";

  // Verify doc_texts_ are preserved
  auto text1 = store2.GetNormalizedText(doc_id1);
  ASSERT_TRUE(text1.has_value());
  EXPECT_EQ(*text1, "hello world");

  auto text2 = store2.GetNormalizedText(doc_id2);
  ASSERT_TRUE(text2.has_value());
  EXPECT_EQ(*text2, "goodbye universe");

  // doc_id3 should still have no text
  auto text3 = store2.GetNormalizedText(doc_id3);
  EXPECT_FALSE(text3.has_value());
}

/**
 * @brief SaveToFile/LoadFromFile preserves doc_texts_ (v2 format)
 *
 * SaveToFile writes v2 format which includes doc_texts_.
 * This test verifies that both documents and normalized texts are preserved.
 */
TEST(DocumentStoreTest, FileRoundTrip_PreservesDocTexts) {
  DocumentStore store;

  auto doc_id1 = *store.AddDocument("pk1");
  auto doc_id2 = *store.AddDocument("pk2");

  store.SetNormalizedText(doc_id1, "normalized text for doc1");
  store.SetNormalizedText(doc_id2, "日本語テキスト");

  std::string filepath = "/tmp/doc_texts_roundtrip_test.bin";
  auto save_result = store.SaveToFile(filepath);
  ASSERT_TRUE(save_result.has_value()) << "SaveToFile should succeed";

  DocumentStore store2;
  auto load_result = store2.LoadFromFile(filepath);
  ASSERT_TRUE(load_result.has_value()) << "LoadFromFile should succeed";

  // Documents themselves are preserved
  EXPECT_EQ(store2.Size(), 2);
  EXPECT_TRUE(store2.GetDocId("pk1").has_value());
  EXPECT_TRUE(store2.GetDocId("pk2").has_value());

  // doc_texts_ are preserved (SaveToFile uses v2 format)
  auto text1 = store2.GetNormalizedText(doc_id1);
  ASSERT_TRUE(text1.has_value()) << "v2 file format should serialize doc_texts_";
  EXPECT_EQ(*text1, "normalized text for doc1");

  auto text2 = store2.GetNormalizedText(doc_id2);
  ASSERT_TRUE(text2.has_value()) << "v2 file format should serialize doc_texts_";
  EXPECT_EQ(*text2, "日本語テキスト");

  std::remove(filepath.c_str());
}

/**
 * @brief v1 snapshot backward compatibility: doc_texts_ is empty after load
 *
 * Simulates loading a v1 snapshot by creating a stream with version=1 format
 * (without doc_texts_), verifying the new code handles it gracefully.
 */
TEST(DocumentStoreTest, LoadFromStream_V1BackwardCompatibility) {
  // Create a v1 snapshot: save with a store that has doc_texts_, then
  // manually create a v1-format stream without doc_texts_
  DocumentStore store;
  auto doc_id1 = *store.AddDocument("pk1");
  store.SetNormalizedText(doc_id1, "some text");

  // Save normally (v2 format)
  std::stringstream v2_stream;
  auto save_result = store.SaveToStream(v2_stream);
  ASSERT_TRUE(save_result.has_value());

  // Load it back to verify v2 works
  DocumentStore store_v2;
  auto load_v2 = store_v2.LoadFromStream(v2_stream);
  ASSERT_TRUE(load_v2.has_value());
  auto text_v2 = store_v2.GetNormalizedText(doc_id1);
  ASSERT_TRUE(text_v2.has_value());
  EXPECT_EQ(*text_v2, "some text");

  // Now create a v1-format stream manually
  // v1 format: [magic "MGDS"][version=1][next_doc_id][gtid_len=0][doc_count=1]
  //   per doc: [doc_id][pk_len][pk][filter_count=0]
  // (no doc_texts_ field)
  std::stringstream v1_stream;
  v1_stream.write("MGDS", 4);
  uint32_t v1_version = 1;
  v1_stream.write(reinterpret_cast<const char*>(&v1_version), sizeof(v1_version));
  uint32_t next_id = 2;  // next_doc_id after adding 1 doc
  v1_stream.write(reinterpret_cast<const char*>(&next_id), sizeof(next_id));
  uint32_t gtid_len = 0;
  v1_stream.write(reinterpret_cast<const char*>(&gtid_len), sizeof(gtid_len));
  uint64_t doc_count = 1;
  v1_stream.write(reinterpret_cast<const char*>(&doc_count), sizeof(doc_count));
  // Document: doc_id=1, pk="pk1", filter_count=0
  uint32_t did = 1;
  v1_stream.write(reinterpret_cast<const char*>(&did), sizeof(did));
  uint32_t pk_len = 3;
  v1_stream.write(reinterpret_cast<const char*>(&pk_len), sizeof(pk_len));
  v1_stream.write("pk1", 3);
  uint32_t filter_count = 0;
  v1_stream.write(reinterpret_cast<const char*>(&filter_count), sizeof(filter_count));

  // Load v1 stream
  DocumentStore store_v1;
  auto load_v1 = store_v1.LoadFromStream(v1_stream);
  ASSERT_TRUE(load_v1.has_value()) << "v1 snapshot should load successfully";

  // Document should exist but doc_texts_ should be empty
  EXPECT_EQ(store_v1.Size(), 1);
  EXPECT_TRUE(store_v1.GetDocId("pk1").has_value());
  auto text_v1 = store_v1.GetNormalizedText(static_cast<DocId>(1));
  EXPECT_FALSE(text_v1.has_value()) << "v1 snapshot should have no doc_texts_";
}

// =============================================================================
// UINT32_MAX wraparound tests for AddDocument and AddDocumentBatch
// =============================================================================

/**
 * @brief Test helper: DocumentStore subclass that uses protected accessors for testing
 *
 * The protected GetNextDocId()/SetNextDocId() accessors allow subclasses to set
 * the counter to values near UINT32_MAX without adding 4 billion documents.
 */
class TestableDocumentStore : public DocumentStore {
 public:
  void SetNextDocIdForTest(DocId id) { SetNextDocId(id); }
  DocId GetNextDocIdForTest() const { return GetNextDocId(); }
};

/**
 * @brief Test AddDocument UINT32_MAX boundary: assigns UINT32_MAX then errors
 *
 * When next_doc_id_ == UINT32_MAX, AddDocument should:
 * 1. Successfully assign UINT32_MAX as the DocId
 * 2. Set next_doc_id_ to 0 (sentinel)
 * 3. Return an error on the subsequent call
 */
TEST(DocumentStoreTest, AddDocument_Uint32MaxBoundary) {
  TestableDocumentStore store;

  // Position next_doc_id_ at UINT32_MAX
  store.SetNextDocIdForTest(UINT32_MAX);

  // Should succeed and assign UINT32_MAX
  auto result = store.AddDocument("pk_max");
  ASSERT_TRUE(result.has_value()) << "AddDocument should succeed at UINT32_MAX";
  EXPECT_EQ(*result, UINT32_MAX);

  // next_doc_id_ should now be 0 (sentinel)
  EXPECT_EQ(store.GetNextDocIdForTest(), 0);

  // Next call should fail with DocID exhaustion
  auto result2 = store.AddDocument("pk_overflow");
  EXPECT_FALSE(result2.has_value()) << "AddDocument should fail after UINT32_MAX is assigned";
}

/**
 * @brief Test AddDocument: next_doc_id_ == 0 returns error immediately
 */
TEST(DocumentStoreTest, AddDocument_ZeroSentinelReturnsError) {
  TestableDocumentStore store;

  // Set sentinel value
  store.SetNextDocIdForTest(0);

  auto result = store.AddDocument("pk_fail");
  EXPECT_FALSE(result.has_value()) << "AddDocument should fail when next_doc_id_ == 0";
}

/**
 * @brief Test AddDocumentBatch UINT32_MAX boundary: assigns UINT32_MAX then errors
 *
 * When next_doc_id_ == UINT32_MAX, AddDocumentBatch should:
 * 1. Successfully assign UINT32_MAX for the first new document
 * 2. Set next_doc_id_ to 0 (sentinel)
 * 3. Return an error for the second new document in the same batch
 */
TEST(DocumentStoreTest, AddDocumentBatch_Uint32MaxBoundary) {
  TestableDocumentStore store;

  // Position next_doc_id_ at UINT32_MAX
  store.SetNextDocIdForTest(UINT32_MAX);

  // Batch with two new documents - first should succeed, second should fail
  std::vector<DocumentStore::DocumentItem> batch;
  batch.push_back({"pk_max", {}, ""});
  batch.push_back({"pk_overflow", {}, ""});

  auto result = store.AddDocumentBatch(batch);
  EXPECT_FALSE(result.has_value()) << "AddDocumentBatch should fail when DocID space is exhausted mid-batch";
}

/**
 * @brief Test AddDocumentBatch: single document at UINT32_MAX succeeds
 */
TEST(DocumentStoreTest, AddDocumentBatch_Uint32MaxSingleDoc) {
  TestableDocumentStore store;

  // Position next_doc_id_ at UINT32_MAX
  store.SetNextDocIdForTest(UINT32_MAX);

  // Single document batch should succeed
  std::vector<DocumentStore::DocumentItem> batch;
  batch.push_back({"pk_max", {}, ""});

  auto result = store.AddDocumentBatch(batch);
  ASSERT_TRUE(result.has_value()) << "Single-doc batch at UINT32_MAX should succeed";
  EXPECT_EQ(result->size(), 1);
  EXPECT_EQ((*result)[0], UINT32_MAX);

  // Sentinel should be set
  EXPECT_EQ(store.GetNextDocIdForTest(), 0);

  // Next batch should fail
  std::vector<DocumentStore::DocumentItem> batch2;
  batch2.push_back({"pk_next", {}, ""});
  auto result2 = store.AddDocumentBatch(batch2);
  EXPECT_FALSE(result2.has_value()) << "Batch after UINT32_MAX should fail";
}

/**
 * @brief Test AddDocumentBatch: duplicate at UINT32_MAX does not consume the ID
 */
TEST(DocumentStoreTest, AddDocumentBatch_DuplicateAtUint32Max) {
  TestableDocumentStore store;

  // Add a document first via normal path
  store.SetNextDocIdForTest(1);
  auto pre = store.AddDocument("pk_existing");
  ASSERT_TRUE(pre.has_value());

  // Position next_doc_id_ at UINT32_MAX
  store.SetNextDocIdForTest(UINT32_MAX);

  // Batch: duplicate (should return existing ID) + new (should get UINT32_MAX)
  std::vector<DocumentStore::DocumentItem> batch;
  batch.push_back({"pk_existing", {}, ""});  // Duplicate - returns existing DocId
  batch.push_back({"pk_max", {}, ""});       // New - should get UINT32_MAX

  auto result = store.AddDocumentBatch(batch);
  ASSERT_TRUE(result.has_value()) << "Batch with duplicate + new at UINT32_MAX should succeed";
  EXPECT_EQ(result->size(), 2);
  EXPECT_EQ((*result)[0], *pre);        // Existing DocId for duplicate
  EXPECT_EQ((*result)[1], UINT32_MAX);  // Last valid DocId for new document

  // Sentinel should be set after using UINT32_MAX
  EXPECT_EQ(store.GetNextDocIdForTest(), 0);
}

// ============================================================
// SetStoreTexts / IsStoreTextsEnabled tests
// ============================================================

TEST(DocumentStoreTest, IsStoreTextsEnabledDefault) {
  DocumentStore store;
  EXPECT_TRUE(store.IsStoreTextsEnabled());
}

TEST(DocumentStoreTest, SetStoreTextsFalseDisablesTextStorage) {
  DocumentStore store;
  store.SetStoreTexts(false);
  EXPECT_FALSE(store.IsStoreTextsEnabled());

  // Add document with normalized text
  auto doc_id = store.AddDocument("pk1", {}, "hello world");
  ASSERT_TRUE(doc_id.has_value());

  // Text should NOT be stored
  auto text = store.GetNormalizedText(*doc_id);
  EXPECT_FALSE(text.has_value());
}

TEST(DocumentStoreTest, SetStoreTextsTrueEnablesTextStorage) {
  DocumentStore store;
  store.SetStoreTexts(true);

  auto doc_id = store.AddDocument("pk1", {}, "hello world");
  ASSERT_TRUE(doc_id.has_value());

  // Text should be stored
  auto text = store.GetNormalizedText(*doc_id);
  ASSERT_TRUE(text.has_value());
  EXPECT_EQ(text.value(), "hello world");
}

TEST(DocumentStoreTest, SetStoreTextsFalseBatchSkipsText) {
  DocumentStore store;
  store.SetStoreTexts(false);

  std::vector<DocumentStore::DocumentItem> batch;
  batch.push_back({"pk1", {}, "text1"});
  batch.push_back({"pk2", {}, "text2"});

  auto result = store.AddDocumentBatch(batch);
  ASSERT_TRUE(result.has_value());

  // No text should be stored
  EXPECT_FALSE(store.GetNormalizedText((*result)[0]).has_value());
  EXPECT_FALSE(store.GetNormalizedText((*result)[1]).has_value());
}

/**
 * @brief Test GetFilterValuesBatchMultiColumn retrieves multiple columns in one call
 */
TEST(DocumentStoreTest, GetFilterValuesBatchMultiColumn) {
  DocumentStore store;

  FilterMap filters1;
  filters1["status"] = int32_t{1};
  filters1["score"] = double{3.14};
  filters1["name"] = std::string("alice");

  FilterMap filters2;
  filters2["status"] = int32_t{2};
  filters2["score"] = double{2.71};
  // name deliberately missing for doc2

  auto id1 = store.AddDocument("pk1", filters1);
  auto id2 = store.AddDocument("pk2", filters2);
  ASSERT_TRUE(id1.has_value());
  ASSERT_TRUE(id2.has_value());

  std::vector<DocId> doc_ids = {*id1, *id2};
  std::vector<std::string> columns = {"status", "score", "name"};

  auto results = store.GetFilterValuesBatchMultiColumn(doc_ids, columns);
  ASSERT_EQ(results.size(), 3);  // 3 columns

  // Column 0: "status"
  ASSERT_EQ(results[0].size(), 2);
  ASSERT_TRUE(results[0][0].has_value());
  EXPECT_EQ(std::get<int32_t>(*results[0][0]), 1);
  ASSERT_TRUE(results[0][1].has_value());
  EXPECT_EQ(std::get<int32_t>(*results[0][1]), 2);

  // Column 1: "score"
  ASSERT_EQ(results[1].size(), 2);
  ASSERT_TRUE(results[1][0].has_value());
  EXPECT_DOUBLE_EQ(std::get<double>(*results[1][0]), 3.14);
  ASSERT_TRUE(results[1][1].has_value());
  EXPECT_DOUBLE_EQ(std::get<double>(*results[1][1]), 2.71);

  // Column 2: "name" (missing for doc2)
  ASSERT_EQ(results[2].size(), 2);
  ASSERT_TRUE(results[2][0].has_value());
  EXPECT_EQ(std::get<std::string>(*results[2][0]), "alice");
  EXPECT_FALSE(results[2][1].has_value());  // doc2 has no "name" filter
}

/**
 * @brief Test GetFilterValuesBatchMultiColumn with empty inputs
 */
TEST(DocumentStoreTest, GetFilterValuesBatchMultiColumnEmpty) {
  DocumentStore store;

  // Empty doc_ids
  auto result1 = store.GetFilterValuesBatchMultiColumn({}, {"col1"});
  ASSERT_EQ(result1.size(), 1);
  EXPECT_TRUE(result1[0].empty());

  // Empty columns
  auto result2 = store.GetFilterValuesBatchMultiColumn({1, 2}, {});
  EXPECT_TRUE(result2.empty());
}

/**
 * @brief Test GetFilterValuesBatchMultiColumn with non-existent doc_ids
 */
TEST(DocumentStoreTest, GetFilterValuesBatchMultiColumnMissingDocs) {
  DocumentStore store;

  FilterMap filters;
  filters["x"] = int32_t{10};
  auto id1 = store.AddDocument("pk1", filters);
  ASSERT_TRUE(id1.has_value());

  std::vector<DocId> doc_ids = {*id1, 999};  // 999 does not exist
  std::vector<std::string> columns = {"x"};

  auto results = store.GetFilterValuesBatchMultiColumn(doc_ids, columns);
  ASSERT_EQ(results.size(), 1);
  ASSERT_EQ(results[0].size(), 2);
  ASSERT_TRUE(results[0][0].has_value());
  EXPECT_EQ(std::get<int32_t>(*results[0][0]), 10);
  EXPECT_FALSE(results[0][1].has_value());  // doc 999 does not exist
}

// =============================================================================
// AddDocument consistency tests (exception safety)
// =============================================================================

/**
 * @brief Test that AddDocument maintains mapping consistency
 *
 * When a document is added, both pk->DocId and DocId->pk mappings must be
 * consistent. A duplicate primary key should return the existing DocId
 * without creating a new one.
 */
TEST(DocumentStoreTest, AddDocumentMaintainsConsistency) {
  DocumentStore store;
  FilterMap filters;

  auto result = store.AddDocument("pk1", filters, "text1");
  ASSERT_TRUE(result.has_value());
  DocId doc1 = *result;

  // Duplicate returns same DocId
  auto result2 = store.AddDocument("pk1", filters, "text1");
  ASSERT_TRUE(result2.has_value());
  EXPECT_EQ(*result2, doc1);

  // Different key gets different DocId
  auto result3 = store.AddDocument("pk2", filters, "text2");
  ASSERT_TRUE(result3.has_value());
  EXPECT_NE(*result3, doc1);

  // Verify bidirectional mapping consistency
  auto pk_for_doc1 = store.GetPrimaryKey(doc1);
  ASSERT_TRUE(pk_for_doc1.has_value());
  EXPECT_EQ(*pk_for_doc1, "pk1");

  auto docid_for_pk1 = store.GetDocId("pk1");
  ASSERT_TRUE(docid_for_pk1.has_value());
  EXPECT_EQ(*docid_for_pk1, doc1);

  auto pk_for_doc2 = store.GetPrimaryKey(*result3);
  ASSERT_TRUE(pk_for_doc2.has_value());
  EXPECT_EQ(*pk_for_doc2, "pk2");

  auto docid_for_pk2 = store.GetDocId("pk2");
  ASSERT_TRUE(docid_for_pk2.has_value());
  EXPECT_EQ(*docid_for_pk2, *result3);

  // Size should reflect only unique documents
  EXPECT_EQ(store.Size(), 2);
}

/**
 * @brief Test that AddDocument with normalized text stores text correctly
 *
 * Verifies the three-argument AddDocument overload stores the normalized
 * text and that it can be retrieved consistently.
 */
TEST(DocumentStoreTest, AddDocumentWithNormalizedTextConsistency) {
  DocumentStore store;
  FilterMap filters;
  filters["status"] = static_cast<int64_t>(1);

  auto result = store.AddDocument("pk1", filters, "normalized hello world");
  ASSERT_TRUE(result.has_value());
  DocId doc1 = *result;

  // Verify document, filters, and text are all retrievable
  auto doc = store.GetDocument(doc1);
  ASSERT_TRUE(doc.has_value());
  EXPECT_EQ(doc->primary_key, "pk1");

  auto text = store.GetNormalizedText(doc1);
  ASSERT_TRUE(text.has_value());
  EXPECT_EQ(*text, "normalized hello world");

  auto status = store.GetFilterValue(doc1, "status");
  ASSERT_TRUE(status.has_value());
  EXPECT_EQ(std::get<int64_t>(*status), 1);
}

TEST(DocumentStoreTest, PrimaryKeyDocIdOrderValidForMonotonicNumericKeys) {
  DocumentStore store;

  ASSERT_TRUE(store.AddDocument("1").has_value());
  ASSERT_TRUE(store.AddDocument("2").has_value());
  ASSERT_TRUE(store.AddDocument("10").has_value());

  EXPECT_TRUE(store.IsPrimaryKeyDocIdOrderValid());
}

TEST(DocumentStoreTest, PrimaryKeyDocIdOrderInvalidatedByNonNumericKey) {
  DocumentStore store;

  ASSERT_TRUE(store.AddDocument("1").has_value());
  ASSERT_TRUE(store.AddDocument("pk2").has_value());

  EXPECT_FALSE(store.IsPrimaryKeyDocIdOrderValid());
}

TEST(DocumentStoreTest, PrimaryKeyDocIdOrderInvalidatedByOutOfOrderNumericKey) {
  DocumentStore store;

  ASSERT_TRUE(store.AddDocument("10").has_value());
  ASSERT_TRUE(store.AddDocument("2").has_value());

  EXPECT_FALSE(store.IsPrimaryKeyDocIdOrderValid());
}

TEST(DocumentStoreTest, PrimaryKeyDocIdOrderInvalidatedByRemoveAndResetByClear) {
  DocumentStore store;

  auto doc_id = store.AddDocument("1");
  ASSERT_TRUE(doc_id.has_value());
  ASSERT_TRUE(store.AddDocument("2").has_value());

  EXPECT_TRUE(store.RemoveDocument(*doc_id));
  EXPECT_FALSE(store.IsPrimaryKeyDocIdOrderValid());

  store.Clear();
  EXPECT_TRUE(store.IsPrimaryKeyDocIdOrderValid());
  ASSERT_TRUE(store.AddDocument("1").has_value());
  EXPECT_TRUE(store.IsPrimaryKeyDocIdOrderValid());
}
