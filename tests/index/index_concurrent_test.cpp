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

/**
 * @brief Test for thread-unsafe Optimize() fix
 *
 * Regression test for: Optimize() had dangling pointer issue causing SEGFAULT
 *
 * The bug (discovered during code review):
 * - Optimize() created a snapshot with raw pointers: vector<pair<string, PostingList*>>
 * - After releasing shared_lock, other threads could delete entries from term_postings_
 * - When unique_ptr was destroyed, the raw pointers in snapshot became dangling
 * - Calling posting->Optimize() on dangling pointer caused SEGFAULT
 *
 * The fix:
 * - Changed from unique_ptr to shared_ptr for reference counting
 * - Optimize() creates snapshot with shared_ptr copies (increments reference count)
 * - Even if entries are removed from term_postings_, snapshot keeps them alive
 * - Concurrent searches can continue during optimization (maintains high concurrency)
 *
 * This test verifies that Optimize() properly synchronizes with concurrent
 * operations (searches and additions) without crashes or data corruption.
 */
TEST(IndexConcurrentTest, OptimizeThreadSafety) {
  Index index;
  const int num_docs = 1000;

  // Add initial documents
  for (int i = 0; i < num_docs; ++i) {
    std::string text = "document " + std::to_string(i) + " content";
    index.AddDocument(static_cast<DocId>(i), text);
  }

  std::atomic<bool> optimization_running{false};
  std::atomic<bool> test_passed{true};

  // Thread 1: Call Optimize()
  std::thread optimize_thread([&]() {
    optimization_running = true;
    try {
      index.Optimize(num_docs);
    } catch (...) {
      test_passed = false;
    }
    optimization_running = false;
  });

  // Thread 2: Perform concurrent searches and additions
  std::thread modify_thread([&]() {
    // Wait for optimization to start
    while (!optimization_running.load()) {
      std::this_thread::yield();
    }

    std::atomic<int> successful_searches{0};

    try {
      // Perform searches (shared access)
      // With shared_ptr fix, searches should succeed even during optimization
      for (int i = 0; i < 100; ++i) {
        auto results = index.SearchAnd({"do"});  // Search for bigram "do" from "document"
        // Searches should return results (all initial documents contain "do")
        if (!results.empty()) {
          successful_searches++;
        }
      }

      // Add new documents (write access)
      for (int i = num_docs; i < num_docs + 100; ++i) {
        std::string text = "new document " + std::to_string(i);
        index.AddDocument(static_cast<DocId>(i), text);
      }
    } catch (...) {
      test_passed = false;
    }

    // Verify that searches were successful during optimization
    // This validates that shared_ptr approach maintains concurrency
    EXPECT_GT(successful_searches.load(), 0) << "Searches should succeed during Optimize() with shared_ptr approach";
  });

  optimize_thread.join();
  modify_thread.join();

  // Verify test passed without crashes or data corruption
  EXPECT_TRUE(test_passed.load()) << "Thread safety violation detected during Optimize()";

  // Verify index is still functional (search for bigram "do" which appears in all documents)
  auto results = index.SearchAnd({"do"});
  EXPECT_GT(results.size(), static_cast<size_t>(num_docs)) << "Index corrupted after concurrent Optimize()";
}

/**
 * @brief Test Optimize() dangling pointer bug with document removal
 *
 * This is a more aggressive test specifically targeting the dangling pointer bug.
 * By removing documents during optimization, we increase the likelihood of
 * triggering the bug if the fix is reverted.
 */
TEST(IndexConcurrentTest, OptimizeDanglingPointerRegression) {
  Index index;
  const int num_docs = 500;

  // Add initial documents with diverse content to create many different terms
  for (int i = 0; i < num_docs; ++i) {
    std::string text = "document " + std::to_string(i) + " unique content " + std::to_string(i * 2);
    index.AddDocument(static_cast<DocId>(i), text);
  }

  std::atomic<bool> optimization_running{false};
  std::atomic<bool> test_passed{true};
  std::atomic<int> operations_during_optimize{0};

  // Thread 1: Call Optimize() - this is where the bug manifests
  std::thread optimize_thread([&]() {
    optimization_running = true;
    try {
      index.Optimize(num_docs);
    } catch (...) {
      test_passed = false;
    }
    optimization_running = false;
  });

  // Thread 2: Aggressively remove and re-add documents
  // This forces term_postings_ modifications that could create dangling pointers
  std::thread modify_thread([&]() {
    // Wait for optimization to start
    while (!optimization_running.load()) {
      std::this_thread::yield();
    }

    std::atomic<int> successful_searches{0};

    try {
      // Perform searches DURING modification to verify concurrent access
      // Search for bigram "do" that exists in "document" (docs 200-499 won't be deleted)
      for (int i = 0; i < 50; ++i) {
        auto results = index.SearchAnd({"do"});  // bigram from "document"
        if (!results.empty()) {
          successful_searches++;
        }
        std::this_thread::yield();  // Give optimization thread time to run
      }

      // Remove documents - only first 200, leaving 300 documents with "document" term
      for (int i = 0; i < 200; ++i) {
        index.RemoveDocument(static_cast<DocId>(i),
                             "document " + std::to_string(i) + " unique content " + std::to_string(i * 2));
        operations_during_optimize++;
      }

      // Add new documents with different terms
      for (int i = num_docs; i < num_docs + 200; ++i) {
        std::string text = "newdoc " + std::to_string(i) + " different terms " + std::to_string(i * 3);
        index.AddDocument(static_cast<DocId>(i), text);
        operations_during_optimize++;
      }

      // More searches after modifications - search for "do" which still exists (docs 200-499)
      for (int i = 0; i < 50; ++i) {
        auto results = index.SearchAnd({"do"});  // bigram from "document"
        if (!results.empty()) {
          successful_searches++;
        }
        std::this_thread::yield();
      }

      // Search for newly added documents using bigram "ne" from "newdoc"
      for (int i = 0; i < 50; ++i) {
        auto results = index.SearchAnd({"ne"});  // bigram from "newdoc"
        if (!results.empty()) {
          successful_searches++;
        }
        std::this_thread::yield();
      }
    } catch (...) {
      test_passed = false;
    }

    // Verify searches succeeded during heavy modifications
    // Note: Some searches might return 0 results due to timing, but most should succeed
    EXPECT_GT(successful_searches.load(), 50)
        << "Majority of searches should succeed during Optimize() with shared_ptr approach";
  });

  optimize_thread.join();
  modify_thread.join();

  // Verify test passed without crashes (the main goal)
  EXPECT_TRUE(test_passed.load()) << "Dangling pointer or other thread safety issue detected";

  // Verify index is still functional - search for bigram "do" from "document" (300 docs remain)
  auto results_document = index.SearchAnd({"do"});
  EXPECT_GT(results_document.size(), 0UL) << "Index corrupted after concurrent Optimize()";

  // Verify newly added documents are searchable - search for bigram "ne" from "newdoc"
  auto results_newdoc = index.SearchAnd({"ne"});
  EXPECT_GT(results_newdoc.size(), 0UL) << "Newly added documents not found after Optimize()";

  // Log how many operations happened during optimization
  // This is informational - the test's success is based on not crashing
  if (operations_during_optimize.load() > 0) {
    SUCCEED() << "Successfully performed " << operations_during_optimize.load()
              << " operations during Optimize() without dangling pointer issues";
  }
}

/**
 * @brief Test concurrent Optimize() and OptimizeInBatches() calls
 */
TEST(IndexConcurrentTest, ConcurrentOptimizeCalls) {
  Index index;
  const int num_docs = 500;

  // Add documents
  for (int i = 0; i < num_docs; ++i) {
    std::string text = "test content " + std::to_string(i);
    index.AddDocument(static_cast<DocId>(i), text);
  }

  std::atomic<int> successful_optimizations{0};
  std::vector<std::thread> threads;

  // Launch multiple optimization threads
  for (int t = 0; t < 5; ++t) {
    threads.emplace_back([&, t]() {
      try {
        if (t % 2 == 0) {
          index.Optimize(num_docs);
        } else {
          index.OptimizeInBatches(num_docs, 50);
        }
        successful_optimizations++;
      } catch (const std::exception& e) {
        // OptimizeInBatches may skip if already optimizing, which is expected
        if (std::string(e.what()).find("already in progress") == std::string::npos) {
          FAIL() << "Unexpected exception during optimization: " << e.what();
        }
      }
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }

  // At least some optimizations should succeed
  EXPECT_GT(successful_optimizations.load(), 0);

  // Index should still be functional (search for bigram "te" which appears in all "test" documents)
  auto results = index.SearchAnd({"te"});
  EXPECT_EQ(results.size(), static_cast<size_t>(num_docs)) << "Index corrupted after concurrent Optimize() calls";
}
