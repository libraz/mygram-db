/**
 * @file document_store_test.cpp
 * @brief Unit tests for document store
 */

#include "storage/document_store.h"
#include <gtest/gtest.h>
#include <thread>
#include <vector>

using namespace mygramdb::storage;

/**
 * @brief Test basic document addition
 */
TEST(DocumentStoreTest, AddDocument) {
  DocumentStore store;

  DocId doc_id = store.AddDocument("pk1");
  EXPECT_EQ(doc_id, 1);
  EXPECT_EQ(store.Size(), 1);

  // Add another document
  DocId doc_id2 = store.AddDocument("pk2");
  EXPECT_EQ(doc_id2, 2);
  EXPECT_EQ(store.Size(), 2);
}

/**
 * @brief Test document addition with filters
 */
TEST(DocumentStoreTest, AddDocumentWithFilters) {
  DocumentStore store;

  std::unordered_map<std::string, FilterValue> filters;
  filters["status"] = static_cast<int64_t>(1);
  filters["category"] = static_cast<int64_t>(10);
  filters["score"] = 95.5;

  DocId doc_id = store.AddDocument("pk1", filters);
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

  DocId doc_id1 = store.AddDocument("pk1");
  DocId doc_id2 = store.AddDocument("pk1");  // Duplicate

  // Should return same DocID
  EXPECT_EQ(doc_id1, doc_id2);
  EXPECT_EQ(store.Size(), 1);
}

/**
 * @brief Test document retrieval
 */
TEST(DocumentStoreTest, GetDocument) {
  DocumentStore store;

  std::unordered_map<std::string, FilterValue> filters;
  filters["status"] = static_cast<int64_t>(1);

  DocId doc_id = store.AddDocument("pk1", filters);

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

  DocId doc_id = store.AddDocument("pk1");

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

  DocId doc_id = store.AddDocument("pk1");

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

  std::unordered_map<std::string, FilterValue> filters1;
  filters1["status"] = static_cast<int64_t>(1);

  DocId doc_id = store.AddDocument("pk1", filters1);

  // Update filters
  std::unordered_map<std::string, FilterValue> filters2;
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

  std::unordered_map<std::string, FilterValue> filters;
  filters["status"] = static_cast<int64_t>(1);

  bool updated = store.UpdateDocument(999, filters);
  EXPECT_FALSE(updated);
}

/**
 * @brief Test document removal
 */
TEST(DocumentStoreTest, RemoveDocument) {
  DocumentStore store;

  DocId doc_id = store.AddDocument("pk1");
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

  std::unordered_map<std::string, FilterValue> filters1;
  filters1["status"] = static_cast<int64_t>(1);

  std::unordered_map<std::string, FilterValue> filters2;
  filters2["status"] = static_cast<int64_t>(2);

  std::unordered_map<std::string, FilterValue> filters3;
  filters3["status"] = static_cast<int64_t>(1);

  store.AddDocument("pk1", filters1);
  store.AddDocument("pk2", filters2);
  store.AddDocument("pk3", filters3);

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

  std::unordered_map<std::string, FilterValue> filters1;
  filters1["tag"] = std::string("important");

  std::unordered_map<std::string, FilterValue> filters2;
  filters2["tag"] = std::string("normal");

  std::unordered_map<std::string, FilterValue> filters3;
  filters3["tag"] = std::string("important");

  store.AddDocument("pk1", filters1);
  store.AddDocument("pk2", filters2);
  store.AddDocument("pk3", filters3);

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

  std::unordered_map<std::string, FilterValue> filters;
  filters["status"] = static_cast<int64_t>(1);

  store.AddDocument("pk1", filters);

  auto results = store.FilterByValue("non_existent", static_cast<int64_t>(1));
  EXPECT_EQ(results.size(), 0);
}

/**
 * @brief Test memory usage
 */
TEST(DocumentStoreTest, MemoryUsage) {
  DocumentStore store;

  size_t initial = store.MemoryUsage();

  std::unordered_map<std::string, FilterValue> filters;
  filters["status"] = static_cast<int64_t>(1);

  store.AddDocument("pk1", filters);

  size_t after = store.MemoryUsage();
  EXPECT_GT(after, initial);
}

/**
 * @brief Test clear
 */
TEST(DocumentStoreTest, Clear) {
  DocumentStore store;

  store.AddDocument("pk1");
  store.AddDocument("pk2");
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
    std::unordered_map<std::string, FilterValue> filters;
    filters["status"] = static_cast<int64_t>(i % 10);

    DocId doc_id = store.AddDocument(pk, filters);
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
    std::unordered_map<std::string, FilterValue> filters;
    filters["status"] = static_cast<int64_t>(i % 10);
    store.AddDocument(pk, filters);
  }

  // Simulate concurrent reads
  const int num_threads = 100;  // Simulate 100 concurrent threads
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

  DocId id1 = store.AddDocument("pk1");
  DocId id2 = store.AddDocument("pk2");
  DocId id3 = store.AddDocument("pk3");

  EXPECT_EQ(id1, 1);
  EXPECT_EQ(id2, 2);
  EXPECT_EQ(id3, 3);

  // Remove middle document
  store.RemoveDocument(id2);

  // Next ID should still be 4 (not reusing removed IDs)
  DocId id4 = store.AddDocument("pk4");
  EXPECT_EQ(id4, 4);
}

/**
 * @brief Test mixed filter types
 */
TEST(DocumentStoreTest, MixedFilterTypes) {
  DocumentStore store;

  std::unordered_map<std::string, FilterValue> filters;
  filters["status"] = static_cast<int64_t>(1);
  filters["tag"] = std::string("important");
  filters["score"] = 98.5;

  DocId doc_id = store.AddDocument("pk1", filters);

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
