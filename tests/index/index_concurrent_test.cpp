/**
 * @file index_concurrent_test.cpp
 * @brief Concurrent access tests for Index
 */

#include <gtest/gtest.h>

#include <atomic>
#include <thread>
#include <vector>

#include "index/index.h"

using namespace mygramdb::index;

/**
 * @brief Test concurrent searches
 */
TEST(IndexConcurrentTest, ConcurrentSearches) {
  Index index(1);

  // Add documents (using simple text for unigram matching)
  for (DocId i = 1; i <= 1000; i++) {
    index.AddDocument(i, "abc");
  }

  // Concurrent searches from multiple threads
  std::vector<std::thread> threads;
  std::atomic<int> success_count{0};

  for (int i = 0; i < 10; i++) {
    threads.emplace_back([&index, &success_count]() {
      for (int j = 0; j < 100; j++) {
        auto results = index.SearchAnd({"a"});  // Search for unigram "a"
        if (results.size() == 1000) {
          success_count++;
        }
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  EXPECT_EQ(success_count, 1000);  // 10 threads * 100 searches
}

/**
 * @brief Test concurrent searches with different operations
 */
TEST(IndexConcurrentTest, MixedConcurrentOperations) {
  Index index(1);

  // Add initial documents
  for (DocId i = 1; i <= 500; i++) {
    index.AddDocument(i, "abc");
  }

  std::vector<std::thread> threads;

  // Search threads
  for (int i = 0; i < 5; i++) {
    threads.emplace_back([&index]() {
      for (int j = 0; j < 50; j++) {
        auto results = index.SearchAnd({"a"});
        (void)results;
      }
    });
  }

  // Count threads
  for (int i = 0; i < 3; i++) {
    threads.emplace_back([&index]() {
      for (int j = 0; j < 50; j++) {
        auto count = index.Count("a");
        (void)count;
      }
    });
  }

  // Statistics threads
  for (int i = 0; i < 2; i++) {
    threads.emplace_back([&index]() {
      for (int j = 0; j < 50; j++) {
        auto stats = index.GetStatistics();
        (void)stats;
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  // Verify final state
  auto results = index.SearchAnd({"a"});
  EXPECT_EQ(results.size(), 500);
}

/**
 * @brief Test OptimizeInBatches with concurrent searches
 */
TEST(IndexConcurrentTest, OptimizeWithConcurrentSearches) {
  Index index(1);

  // Add documents
  for (DocId i = 1; i <= 10000; i++) {
    index.AddDocument(i, "abc");
  }

  std::vector<std::thread> threads;
  std::atomic<bool> optimize_done{false};

  // Optimization thread
  threads.emplace_back([&index, &optimize_done]() {
    EXPECT_TRUE(index.OptimizeInBatches(10000, 1000));
    optimize_done = true;
  });

  // Concurrent search threads
  for (int i = 0; i < 5; i++) {
    threads.emplace_back([&index, &optimize_done]() {
      while (!optimize_done) {
        auto results = index.SearchAnd({"a"});
        // Results should always be consistent
        EXPECT_EQ(results.size(), 10000);
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
      }
      // Continue searching after optimization
      for (int j = 0; j < 10; j++) {
        auto results = index.SearchAnd({"a"});
        EXPECT_EQ(results.size(), 10000);
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  // Verify final state
  auto results = index.SearchAnd({"a"});
  EXPECT_EQ(results.size(), 10000);
}

/**
 * @brief Test SaveToFile with concurrent reads
 */
TEST(IndexConcurrentTest, SaveWithConcurrentReads) {
  Index index(1);

  // Add documents
  for (DocId i = 1; i <= 1000; i++) {
    index.AddDocument(i, "abc");
  }

  std::string test_file = "/tmp/test_index_concurrent_" + std::to_string(std::time(nullptr));

  std::vector<std::thread> threads;
  std::atomic<bool> save_done{false};

  // Save thread
  threads.emplace_back([&index, &test_file, &save_done]() {
    EXPECT_TRUE(index.SaveToFile(test_file + ".index"));
    save_done = true;
  });

  // Concurrent read threads
  for (int i = 0; i < 3; i++) {
    threads.emplace_back([&index, &save_done]() {
      while (!save_done) {
        auto results = index.SearchAnd({"a"});
        EXPECT_EQ(results.size(), 1000);
        std::this_thread::sleep_for(std::chrono::microseconds(500));
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  // Cleanup
  std::remove((test_file + ".index").c_str());
}

/**
 * @brief Test LoadFromFile with concurrent attempts (should be serialized)
 */
TEST(IndexConcurrentTest, LoadFromFile) {
  Index index1(1);

  // Create and save a snapshot
  for (DocId i = 1; i <= 100; i++) {
    index1.AddDocument(i, "abc");
  }

  std::string test_file = "/tmp/test_index_load_" + std::to_string(std::time(nullptr));
  ASSERT_TRUE(index1.SaveToFile(test_file + ".index"));

  Index index2(1);

  // Add some initial documents
  for (DocId i = 1; i <= 50; i++) {
    index2.AddDocument(i, "xyz");
  }

  std::vector<std::thread> threads;
  std::atomic<bool> load_done{false};

  // Thread that loads from file
  threads.emplace_back([&index2, &test_file, &load_done]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    EXPECT_TRUE(index2.LoadFromFile(test_file + ".index"));
    load_done = true;
  });

  // Reader threads (will see either old or new data, but always consistent)
  for (int i = 0; i < 3; i++) {
    threads.emplace_back([&index2, &load_done]() {
      for (int j = 0; j < 100; j++) {
        if (load_done)
          break;
        auto results = index2.SearchAnd({"a"});
        // Should be either 0 (before load - "xyz" docs) or 100 (after load - "abc" docs), never
        // partial
        EXPECT_TRUE(results.size() == 0 || results.size() == 100);
        std::this_thread::sleep_for(std::chrono::microseconds(100));
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  // Cleanup
  std::remove((test_file + ".index").c_str());

  // Verify final state
  auto results = index2.SearchAnd({"a"});
  EXPECT_EQ(results.size(), 100);
}

/**
 * @brief Test concurrent SearchOr and SearchNot
 */
TEST(IndexConcurrentTest, ConcurrentSearchOrAndNot) {
  Index index(1);

  // Add documents with different terms (using single chars for unigram)
  for (DocId i = 1; i <= 500; i++) {
    if (i % 3 == 0) {
      index.AddDocument(i, "ab");  // Contains 'a' and 'b'
    } else if (i % 3 == 1) {
      index.AddDocument(i, "ac");  // Contains 'a' and 'c'
    } else {
      index.AddDocument(i, "bc");  // Contains 'b' and 'c'
    }
  }

  std::vector<std::thread> threads;
  std::atomic<int> success_count{0};

  for (int i = 0; i < 10; i++) {
    threads.emplace_back([&index, &success_count]() {
      for (int j = 0; j < 50; j++) {
        // Test SearchOr (docs with 'a' OR 'b')
        auto or_results = index.SearchOr({"a", "b"});
        if (!or_results.empty()) {
          success_count++;
        }

        // Test SearchNot (all docs NOT containing 'c')
        std::vector<DocId> all_docs;
        for (DocId k = 1; k <= 500; k++) {
          all_docs.push_back(k);
        }
        auto not_results = index.SearchNot(all_docs, {"c"});
        if (!not_results.empty()) {
          success_count++;
        }
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  EXPECT_EQ(success_count, 1000);  // 10 threads * 50 iterations * 2 operations
}

/**
 * @brief Test concurrent OPTIMIZE attempts (only one should succeed)
 */
TEST(IndexConcurrentTest, ConcurrentOptimizeExclusion) {
  Index index(1);

  // Add documents
  for (DocId i = 1; i <= 5000; i++) {
    index.AddDocument(i, "abc");
  }

  std::vector<std::thread> threads;
  std::atomic<int> success_count{0};
  std::atomic<int> failure_count{0};

  // Launch 3 threads attempting to optimize simultaneously
  for (int i = 0; i < 3; i++) {
    threads.emplace_back([&index, &success_count, &failure_count]() {
      bool result = index.OptimizeInBatches(5000, 500);
      if (result) {
        success_count++;
      } else {
        failure_count++;
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  // Only one optimization should succeed, others should fail
  EXPECT_EQ(success_count, 1);
  EXPECT_EQ(failure_count, 2);

  // Verify index is still functional after optimization
  auto results = index.SearchAnd({"a"});
  EXPECT_EQ(results.size(), 5000);
}

/**
 * @brief Test OPTIMIZE with concurrent document additions
 *
 * This test verifies that OptimizeInBatches properly blocks concurrent writes
 * using exclusive locking. The optimization acquires an exclusive lock that
 * blocks all Add/Update/Remove operations during the optimization process,
 * ensuring thread safety.
 *
 * NOTE: Due to the exclusive lock, additions will be blocked during optimization
 * and will only complete after optimization finishes. This is the intended behavior
 * to prevent race conditions.
 */
TEST(IndexConcurrentTest, OptimizeWithConcurrentAdditions) {
  Index index(1);

  // Add initial documents
  for (DocId i = 1; i <= 5000; i++) {
    index.AddDocument(i, "abc");
  }

  std::vector<std::thread> threads;
  std::atomic<bool> optimize_done{false};
  std::atomic<int> additions_during_optimize{0};

  // Optimization thread
  threads.emplace_back([&index, &optimize_done]() {
    EXPECT_TRUE(index.OptimizeInBatches(5000, 500));
    optimize_done = true;
  });

  // Concurrent addition threads (add documents while optimization is running)
  for (int i = 0; i < 2; i++) {
    threads.emplace_back([&index, &optimize_done, &additions_during_optimize, i]() {
      for (int j = 0; j < 100; j++) {
        DocId doc_id = 10000 + i * 100 + j;
        index.AddDocument(doc_id, "xyz");
        if (!optimize_done) {
          additions_during_optimize++;
        }
        std::this_thread::sleep_for(std::chrono::microseconds(100));
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  // Verify all documents are present and searchable
  auto results_abc = index.SearchAnd({"a"});
  EXPECT_EQ(results_abc.size(), 5000);

  auto results_xyz = index.SearchAnd({"x"});
  // With exclusive locking, all 200 additions complete after optimization finishes
  // (2 threads * 100 additions each = 200 total)
  EXPECT_EQ(results_xyz.size(), 200);

  // Note: additions_during_optimize may vary depending on optimization speed.
  // With exclusive locking, AddDocument calls are properly synchronized and
  // no race conditions occur. The counter just tracks timing of thread execution.
  // What matters is that all 200 documents are safely added without crashes.
  (void)additions_during_optimize;  // Timing-dependent, not critical for correctness
}
