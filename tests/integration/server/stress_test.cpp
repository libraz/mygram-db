/**
 * @file stress_test.cpp
 * @brief Stress tests with large-scale data
 */

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

#include "index/index.h"
#include "storage/document_store.h"
#include "utils/string_utils.h"

using namespace mygramdb;

/**
 * @brief Test fixture for stress tests
 */
class StressTest : public ::testing::Test {
 protected:
  void SetUp() override {
    index_ = std::make_unique<index::Index>(3, 2);
    doc_store_ = std::make_unique<storage::DocumentStore>();
  }

  void TearDown() override {
    index_.reset();
    doc_store_.reset();
  }

  std::unique_ptr<index::Index> index_;
  std::unique_ptr<storage::DocumentStore> doc_store_;
};

/**
 * @brief Test with 100,000 documents
 * This test is disabled by default in CI due to time constraints
 */
TEST_F(StressTest, DISABLED_LargeScale100K) {
  const int num_docs = 100000;

  auto start = std::chrono::high_resolution_clock::now();

  // Add documents using batch operation for better performance
  std::vector<index::Index::DocumentItem> batch;
  batch.reserve(num_docs);

  for (int i = 0; i < num_docs; ++i) {
    std::string pk = "pk" + std::to_string(i);
    std::unordered_map<std::string, storage::FilterValue> filters;
    filters["category"] = static_cast<int64_t>(i % 100);

    auto doc_id = doc_store_->AddDocument(pk, filters);
    std::string text = "document " + std::to_string(i) + " content test data";
    batch.push_back({*doc_id, text});
  }

  // Add all documents in one batch operation
  index_->AddDocumentBatch(batch);

  auto end = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

  // Verify size
  EXPECT_EQ(doc_store_->Size(), num_docs);

  // Log performance
  std::cout << "Added " << num_docs << " documents in " << duration.count() << "ms" << std::endl;
  std::cout << "Average: " << (duration.count() * 1000.0 / num_docs) << " μs/doc" << std::endl;

  // DEBUG: Check index memory to verify documents were added
  std::cout << "Index memory usage: " << index_->MemoryUsage() << " bytes" << std::endl;

  // Search performance - need to n-gram化 the search terms first
  std::string search_term = "test";
  auto search_ngrams = utils::GenerateHybridNgrams(search_term, 3, 2);

  start = std::chrono::high_resolution_clock::now();
  auto results = index_->SearchAnd(search_ngrams);
  end = std::chrono::high_resolution_clock::now();
  duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

  EXPECT_GT(results.size(), 0) << "Search should find documents containing 'test'";
  std::cout << "Search found " << results.size() << " results in " << duration.count() << "ms" << std::endl;

  // Memory usage
  size_t index_memory = index_->MemoryUsage();
  size_t doc_store_memory = doc_store_->MemoryUsage();
  std::cout << "Index memory: " << (index_memory / 1024 / 1024) << " MB" << std::endl;
  std::cout << "DocumentStore memory: " << (doc_store_memory / 1024 / 1024) << " MB" << std::endl;

  // Memory should be reasonable (< 500MB for 100k docs)
  EXPECT_LT(index_memory + doc_store_memory, 500 * 1024 * 1024);
}

/**
 * @brief Test with 1 million documents (if resources allow)
 * This test is disabled by default due to resource requirements
 */
TEST_F(StressTest, DISABLED_LargeScale1M) {
  const int num_docs = 1000000;

  auto start = std::chrono::high_resolution_clock::now();

  // Add documents using batch operation for better performance
  std::vector<index::Index::DocumentItem> batch;
  batch.reserve(num_docs);

  for (int i = 0; i < num_docs; ++i) {
    std::string pk = "pk" + std::to_string(i);
    std::unordered_map<std::string, storage::FilterValue> filters;
    filters["category"] = static_cast<int64_t>(i % 1000);

    auto doc_id = doc_store_->AddDocument(pk, filters);
    std::string text = "document " + std::to_string(i) + " test";
    batch.push_back({*doc_id, text});

    if (i % 100000 == 0 && i > 0) {
      std::cout << "Progress: " << i << " / " << num_docs << std::endl;
    }
  }

  // Add all documents in one batch operation
  std::cout << "Adding all documents to index..." << std::endl;
  index_->AddDocumentBatch(batch);

  auto end = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::seconds>(end - start);

  EXPECT_EQ(doc_store_->Size(), num_docs);
  std::cout << "Added " << num_docs << " documents in " << duration.count() << "s" << std::endl;

  // Memory usage
  size_t total_memory = index_->MemoryUsage() + doc_store_->MemoryUsage();
  std::cout << "Total memory: " << (total_memory / 1024 / 1024) << " MB" << std::endl;
}

/**
 * @brief Test continuous add/remove operations
 */
TEST_F(StressTest, ContinuousAddRemove) {
  const int num_iterations = 10000;
  const int max_docs = 1000;

  for (int i = 0; i < num_iterations; ++i) {
    // Add document
    std::string pk = "pk" + std::to_string(i);
    auto doc_id = doc_store_->AddDocument(pk);
    std::string text = "document " + std::to_string(i);
    index_->AddDocument(*doc_id, text);

    // Remove old document
    if (i >= max_docs) {
      std::string old_pk = "pk" + std::to_string(i - max_docs);
      auto old_doc_id = doc_store_->GetDocId(old_pk);
      if (old_doc_id.has_value()) {
        std::string old_text = "document " + std::to_string(i - max_docs);
        index_->RemoveDocument(old_doc_id.value(), old_text);
        doc_store_->RemoveDocument(old_doc_id.value());
      }
    }
  }

  // Size should stabilize around max_docs
  EXPECT_LE(doc_store_->Size(), max_docs);
  EXPECT_GT(doc_store_->Size(), max_docs * 0.9);  // Allow some variance
}

/**
 * @brief Test memory leak with repeated add/remove
 */
TEST_F(StressTest, MemoryLeakTest) {
  size_t initial_memory = index_->MemoryUsage() + doc_store_->MemoryUsage();

  // Repeat add/remove cycles
  for (int cycle = 0; cycle < 10; ++cycle) {
    // Add 1000 documents
    for (int i = 0; i < 1000; ++i) {
      std::string pk = "cycle" + std::to_string(cycle) + "_pk" + std::to_string(i);
      auto doc_id = doc_store_->AddDocument(pk);
      std::string text = "test document " + std::to_string(i);
      index_->AddDocument(*doc_id, text);
    }

    // Remove all documents from this cycle
    for (int i = 0; i < 1000; ++i) {
      std::string pk = "cycle" + std::to_string(cycle) + "_pk" + std::to_string(i);
      auto doc_id = doc_store_->GetDocId(pk);
      if (doc_id.has_value()) {
        std::string text = "test document " + std::to_string(i);
        index_->RemoveDocument(doc_id.value(), text);
        doc_store_->RemoveDocument(doc_id.value());
      }
    }
  }

  size_t final_memory = index_->MemoryUsage() + doc_store_->MemoryUsage();

  // Memory should not grow significantly
  // Allow up to 2x growth (some fragmentation is expected)
  EXPECT_LT(final_memory, initial_memory * 2 + 10 * 1024 * 1024);  // +10MB allowance

  std::cout << "Initial memory: " << (initial_memory / 1024) << " KB" << std::endl;
  std::cout << "Final memory: " << (final_memory / 1024) << " KB" << std::endl;
}

/**
 * @brief Test high-frequency concurrent operations
 */
TEST_F(StressTest, HighFrequencyConcurrentOps) {
  // Pre-populate with 10000 documents
  for (int i = 0; i < 10000; ++i) {
    std::string pk = "pk" + std::to_string(i);
    auto doc_id = doc_store_->AddDocument(pk, {{"status", static_cast<int64_t>(i % 10)}});
    std::string text = "document " + std::to_string(i) + " test data";
    index_->AddDocument(*doc_id, text);
  }

  const int num_threads = 20;
  const int ops_per_thread = 1000;
  std::vector<std::thread> threads;
  std::atomic<int> search_count{0};
  std::atomic<int> filter_count{0};

  auto start = std::chrono::high_resolution_clock::now();

  for (int t = 0; t < num_threads; ++t) {
    threads.emplace_back([this, &search_count, &filter_count]() {
      for (int i = 0; i < ops_per_thread; ++i) {
        // Mix of operations
        if (i % 3 == 0) {
          // Search
          auto results = index_->SearchAnd({"document", "test"});
          if (!results.empty()) {
            search_count++;
          }
        } else if (i % 3 == 1) {
          // Filter query
          auto results = doc_store_->FilterByValue("status", static_cast<int64_t>(i % 10));
          if (!results.empty()) {
            filter_count++;
          }
        } else {
          // Document lookup
          auto doc = doc_store_->GetDocument(i % 10000 + 1);
          if (doc.has_value()) {
            search_count++;
          }
        }
      }
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }

  auto end = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

  int total_ops = num_threads * 1000;
  std::cout << "Executed " << total_ops << " operations in " << duration.count() << "ms" << std::endl;
  std::cout << "Throughput: " << (total_ops * 1000.0 / duration.count()) << " ops/sec" << std::endl;

  // Most operations should succeed (allow some variance in concurrent access)
  EXPECT_GT(search_count.load() + filter_count.load(), total_ops * 0.6);
}

/**
 * @brief Test search performance degradation with increasing data
 */
TEST_F(StressTest, SearchPerformanceDegradation) {
  std::vector<std::pair<int, double>> performance_data;

  // Test at different scales
  std::vector<int> scales = {1000, 5000, 10000, 50000, 100000};

  for (int scale : scales) {
    // Clear and rebuild
    index_ = std::make_unique<index::Index>(3, 2);
    doc_store_ = std::make_unique<storage::DocumentStore>();

    // Add documents using batch operation for better performance
    std::vector<index::Index::DocumentItem> batch;
    batch.reserve(scale);

    for (int i = 0; i < scale; ++i) {
      std::string pk = "pk" + std::to_string(i);
      auto doc_id = doc_store_->AddDocument(pk);
      std::string text = "document " + std::to_string(i) + " test search performance";
      batch.push_back({*doc_id, text});
    }

    // Add all documents in one batch operation
    index_->AddDocumentBatch(batch);

    // Measure search time - need to n-gram化 search terms first
    auto test_ngrams = utils::GenerateHybridNgrams("test", 3, 2);
    auto search_ngrams = utils::GenerateHybridNgrams("search", 3, 2);

    // Combine all ngrams from both terms for AND search
    std::vector<std::string> combined_ngrams;
    combined_ngrams.insert(combined_ngrams.end(), test_ngrams.begin(), test_ngrams.end());
    combined_ngrams.insert(combined_ngrams.end(), search_ngrams.begin(), search_ngrams.end());

    auto start = std::chrono::high_resolution_clock::now();
    size_t total_results = 0;
    for (int i = 0; i < 100; ++i) {  // 100 searches
      auto results = index_->SearchAnd(combined_ngrams);
      total_results += results.size();
    }
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);

    double avg_time = duration.count() / 100.0;  // μs per search
    performance_data.push_back({scale, avg_time});

    std::cout << "Scale: " << scale << " docs, Avg search time: " << avg_time
              << " μs, Avg results: " << (total_results / 100.0) << std::endl;
  }

  // Verify performance doesn't degrade exponentially
  // For n-gram search, near-linear degradation (O(n)) is expected and acceptable
  // Allow up to 2x scale_ratio (i.e., for 100x data, allow up to 200x time increase)
  if (performance_data.size() >= 2) {
    double first_time = performance_data[0].second;
    double last_time = performance_data.back().second;
    double first_scale = performance_data[0].first;
    double last_scale = performance_data.back().first;

    double scale_ratio = last_scale / first_scale;
    double time_ratio = last_time / first_time;

    std::cout << "Scale increased " << scale_ratio << "x, time increased " << time_ratio << "x" << std::endl;

    // Performance should not be worse than O(n^2) - allow up to 2x linear degradation
    EXPECT_LT(time_ratio, scale_ratio * 2.0) << "Performance degradation is worse than O(n)";
  }
}
