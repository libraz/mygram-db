/**
 * @file document_store_stress_test.cpp
 * @brief Stress tests for DocumentStore (marked as SLOW for CI)
 *
 * These tests are designed to detect concurrency bugs like use-after-free
 * through high memory pressure and concurrent operations. They are excluded
 * from regular CI runs due to their longer execution time.
 */

#include <gtest/gtest.h>

#include <atomic>
#include <thread>
#include <vector>

#include "storage/document_store.h"

using namespace mygramdb::storage;

/**
 * @brief Stress test for RemoveDocument to detect use-after-free bugs
 *
 * This test targets the RemoveDocument function with high concurrency and memory
 * pressure. It was added to prevent regression of the use-after-free bug where
 * RemoveDocument held a reference to the primary key string after erasing the
 * map entry (the reference became dangling).
 *
 * The bug manifested as:
 * - const std::string& primary_key = pk_it->second;  // Reference to string
 * - doc_id_to_pk_.erase(doc_id);                     // Invalidates reference
 * - StructuredLog()...Field("primary_key", primary_key)  // Use after free!
 *
 * The fix was to copy the string before erasing:
 * - std::string primary_key = pk_it->second;  // Copy the string
 */
TEST(DocumentStoreStressTest, RemoveDocumentUseAfterFreeRegression) {
  constexpr int kIterations = 10;
  constexpr int kDocsPerIteration = 500;
  constexpr int kNumThreads = 8;

  for (int iter = 0; iter < kIterations; ++iter) {
    DocumentStore store;

    // Add documents with long primary keys to increase memory churn
    std::vector<DocId> doc_ids;
    doc_ids.reserve(kDocsPerIteration);

    for (int i = 0; i < kDocsPerIteration; ++i) {
      // Use longer primary keys to increase memory allocation/deallocation
      std::string pk = "primary_key_with_longer_content_for_memory_pressure_" + std::to_string(iter) + "_" +
                       std::to_string(i) + "_padding";

      std::unordered_map<std::string, FilterValue> filters;
      filters["iteration"] = static_cast<int64_t>(iter);
      filters["index"] = static_cast<int64_t>(i);

      auto result = store.AddDocument(pk, filters);
      ASSERT_TRUE(result.has_value()) << "Failed to add document " << i;
      doc_ids.push_back(*result);
    }

    ASSERT_EQ(store.Size(), kDocsPerIteration);

    // Concurrent deletion from multiple threads
    std::vector<std::thread> threads;
    std::atomic<int> delete_count{0};
    std::atomic<int> docs_per_thread = kDocsPerIteration / kNumThreads;

    for (int t = 0; t < kNumThreads; ++t) {
      threads.emplace_back([&store, &doc_ids, &delete_count, &docs_per_thread, t]() {
        int start = t * docs_per_thread;
        int end = (t == kNumThreads - 1) ? static_cast<int>(doc_ids.size()) : start + docs_per_thread;

        for (int i = start; i < end; ++i) {
          // RemoveDocument should not crash even with concurrent access
          // The bug was that primary_key reference became invalid after erase
          bool removed = store.RemoveDocument(doc_ids[i]);
          if (removed) {
            delete_count++;
          }
        }
      });
    }

    for (auto& thread : threads) {
      thread.join();
    }

    // All documents should be deleted
    EXPECT_EQ(delete_count.load(), kDocsPerIteration) << "Iteration " << iter << " failed";
    EXPECT_EQ(store.Size(), 0) << "Store not empty after iteration " << iter;

    // Verify all documents are gone
    for (const auto& doc_id : doc_ids) {
      auto doc = store.GetDocument(doc_id);
      EXPECT_FALSE(doc.has_value()) << "Document " << doc_id << " still exists";
    }
  }
}

/**
 * @brief Test concurrent add and remove operations with memory stress
 *
 * This test creates memory pressure by doing rapid add/remove cycles
 * across multiple threads, which increases the likelihood of detecting
 * use-after-free bugs due to memory reuse.
 */
TEST(DocumentStoreStressTest, ConcurrentAddRemoveMemoryStress) {
  DocumentStore store;

  constexpr int kNumThreads = 6;
  constexpr int kOperationsPerThread = 200;

  std::atomic<bool> stop{false};
  std::atomic<int> add_success{0};
  std::atomic<int> remove_success{0};
  std::vector<std::thread> threads;

  // Producer threads - add documents
  for (int t = 0; t < kNumThreads / 2; ++t) {
    threads.emplace_back([&store, &stop, &add_success, t]() {
      int counter = 0;
      while (!stop && counter < kOperationsPerThread) {
        std::string pk = "stress_add_thread_" + std::to_string(t) + "_doc_" + std::to_string(counter) +
                         "_with_extra_padding_for_memory_allocation";

        std::unordered_map<std::string, FilterValue> filters;
        filters["thread"] = static_cast<int64_t>(t);
        filters["counter"] = static_cast<int64_t>(counter);
        filters["description"] = std::string("Document created by thread ") + std::to_string(t);

        auto result = store.AddDocument(pk, filters);
        if (result.has_value()) {
          add_success++;
        }
        counter++;
      }
    });
  }

  // Consumer threads - remove documents (will remove whatever exists)
  for (int t = 0; t < kNumThreads / 2; ++t) {
    threads.emplace_back([&store, &stop, &remove_success]() {
      while (!stop) {
        // Get all doc IDs and try to remove some
        auto all_ids = store.GetAllDocIds();
        for (const auto& doc_id : all_ids) {
          if (stop)
            break;
          if (store.RemoveDocument(doc_id)) {
            remove_success++;
          }
        }
        // Small yield to allow other operations
        std::this_thread::yield();
      }
    });
  }

  // Let it run for a bit
  std::this_thread::sleep_for(std::chrono::milliseconds(300));
  stop = true;

  for (auto& thread : threads) {
    thread.join();
  }

  // Verify operations completed without crashes
  EXPECT_GT(add_success.load(), 0) << "No documents were added";
  // Note: remove_success may be 0 if all adds happened after removes finished
  // The main verification is that no crashes occurred

  // Final state verification
  size_t final_size = store.Size();
  EXPECT_GE(final_size, 0);
}
