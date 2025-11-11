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
    store2.LoadFromFile(test_file + ".docs");
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
