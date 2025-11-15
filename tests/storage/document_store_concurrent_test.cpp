/**
 * @file document_store_concurrent_test.cpp
 * @brief Concurrent access tests for DocumentStore
 */

#include <gtest/gtest.h>

#include <atomic>
#include <thread>
#include <vector>

#include "storage/document_store.h"

using namespace mygramdb::storage;

/**
 * @brief Test concurrent reads
 */
TEST(DocumentStoreConcurrentTest, ConcurrentReads) {
  DocumentStore store;

  // Add some documents
  for (int i = 0; i < 100; i++) {
    store.AddDocument(std::to_string(i), {});
  }

  // Concurrent reads from multiple threads
  std::vector<std::thread> threads;
  std::atomic<int> success_count{0};

  for (int i = 0; i < 10; i++) {
    threads.emplace_back([&store, &success_count]() {
      for (int j = 0; j < 100; j++) {
        auto doc = store.GetDocument(j + 1);
        if (doc.has_value()) {
          success_count++;
        }
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  EXPECT_EQ(success_count, 1000);  // 10 threads * 100 reads
}

/**
 * @brief Test concurrent read and write
 * This test may expose data races if DocumentStore is not thread-safe
 */
TEST(DocumentStoreConcurrentTest, ConcurrentReadWrite) {
  DocumentStore store;

  // Add initial documents
  for (int i = 0; i < 50; i++) {
    store.AddDocument(std::to_string(i), {});
  }

  std::vector<std::thread> threads;
  std::atomic<bool> writer_done{false};

  // Writer thread - adds more documents
  threads.emplace_back([&store, &writer_done]() {
    for (int i = 50; i < 100; i++) {
      store.AddDocument(std::to_string(i), {});
      std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
    writer_done = true;
  });

  // Reader threads - read documents
  for (int i = 0; i < 5; i++) {
    threads.emplace_back([&store, &writer_done]() {
      while (!writer_done) {
        for (int j = 0; j < 50; j++) {
          auto doc = store.GetDocument(j + 1);
          // Just access, don't validate (document may or may not exist)
          (void)doc;
        }
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  // Verify final state
  EXPECT_EQ(store.Size(), 100);
}

/**
 * @brief Test concurrent LoadFromFile and reads
 * This test exposes the LoadFromFile race condition
 */
TEST(DocumentStoreConcurrentTest, ConcurrentLoadAndRead) {
  DocumentStore store1;

  // Create and save a snapshot
  std::unordered_map<std::string, FilterValue> filters;
  filters["status"] = static_cast<int32_t>(1);

  for (int i = 0; i < 100; i++) {
    store1.AddDocument(std::to_string(i), filters);
  }

  std::string test_file = "/tmp/test_concurrent_load";
  ASSERT_TRUE(store1.SaveToFile(test_file + ".docs"));

  DocumentStore store2;

  // Add some initial documents
  for (int i = 0; i < 50; i++) {
    store2.AddDocument(std::to_string(i), {});
  }

  std::vector<std::thread> threads;
  std::atomic<bool> load_done{false};

  // Thread that loads from file
  threads.emplace_back([&store2, &test_file, &load_done]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    [[maybe_unused]] auto result = store2.LoadFromFile(test_file + ".docs");
    load_done = true;
  });

  // Reader threads
  for (int i = 0; i < 3; i++) {
    threads.emplace_back([&store2, &load_done]() {
      for (int j = 0; j < 100; j++) {
        if (load_done)
          break;
        auto doc = store2.GetDocument(j + 1);
        (void)doc;
        std::this_thread::sleep_for(std::chrono::microseconds(100));
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  // Cleanup
  std::remove((test_file + ".docs").c_str());

  EXPECT_EQ(store2.Size(), 100);
}

/**
 * @brief Test concurrent Size() calls with Add/Remove operations
 *
 * This test is designed to detect data races in DocumentStore::Size().
 * Before the fix, Size() was called without acquiring a lock, which could
 * lead to data races when called concurrently with AddDocument/RemoveDocument.
 */
TEST(DocumentStoreConcurrentTest, ConcurrentSizeCalls) {
  DocumentStore store;

  // Add initial documents
  constexpr int kInitialDocs = 100;
  for (int i = 0; i < kInitialDocs; i++) {
    store.AddDocument(std::to_string(i), {});
  }

  std::atomic<bool> stop{false};
  std::atomic<int> size_calls{0};
  std::atomic<int> add_calls{0};
  std::atomic<int> remove_calls{0};

  std::vector<std::thread> threads;

  // Size reader threads - continuously call Size()
  for (int i = 0; i < 4; i++) {
    threads.emplace_back([&store, &stop, &size_calls]() {
      while (!stop) {
        size_t size = store.Size();
        size_calls++;
        // Size should always be non-negative and reasonable
        EXPECT_GE(size, 0);
        EXPECT_LT(size, 10000);  // Sanity check
        std::this_thread::yield();
      }
    });
  }

  // Writer threads - add documents
  for (int i = 0; i < 2; i++) {
    threads.emplace_back([&store, &stop, &add_calls, i]() {
      int doc_id = 1000 + i * 1000;
      while (!stop) {
        std::unordered_map<std::string, FilterValue> filters;
        filters["thread_id"] = static_cast<int32_t>(i);
        store.AddDocument("add_" + std::to_string(doc_id++), filters);
        add_calls++;
        std::this_thread::sleep_for(std::chrono::microseconds(100));
      }
    });
  }

  // Remover threads - remove documents
  for (int i = 0; i < 2; i++) {
    threads.emplace_back([&store, &stop, &remove_calls, i]() {
      int idx = i;
      while (!stop) {
        // Try to remove documents (may fail if already removed)
        DocId doc_id = static_cast<DocId>((idx % kInitialDocs) + 1);
        store.RemoveDocument(doc_id);
        remove_calls++;
        idx += 2;  // Each thread works on different documents
        std::this_thread::sleep_for(std::chrono::microseconds(150));
      }
    });
  }

  // Run for a short duration
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  stop = true;

  // Wait for all threads
  for (auto& t : threads) {
    t.join();
  }

  // Verify operations completed without crashes
  EXPECT_GT(size_calls.load(), 0);
  EXPECT_GT(add_calls.load(), 0);
  EXPECT_GT(remove_calls.load(), 0);

  // Final size should be consistent
  size_t final_size = store.Size();
  EXPECT_GE(final_size, 0);
  EXPECT_LT(final_size, 10000);
}
