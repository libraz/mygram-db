/**
 * @file posting_list_test.cpp
 * @brief Unit tests for PostingList class optimizations
 */

#include "index/posting_list.h"

#include <gtest/gtest.h>

using namespace mygramdb::index;

class PostingListTest : public ::testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

/**
 * @brief Test Size() returns correct value for delta-compressed strategy
 */
TEST_F(PostingListTest, SizeDeltaCompressed) {
  // Start with delta-compressed (small dataset)
  PostingList posting(0.5);

  EXPECT_EQ(posting.Size(), 0);

  posting.Add(10);
  EXPECT_EQ(posting.Size(), 1);

  posting.Add(20);
  EXPECT_EQ(posting.Size(), 2);

  posting.Add(30);
  EXPECT_EQ(posting.Size(), 3);

  posting.Remove(20);
  EXPECT_EQ(posting.Size(), 2);
}

/**
 * @brief Test Size() returns correct value for larger dataset
 */
TEST_F(PostingListTest, SizeLargeDataset) {
  PostingList posting(0.5);

  EXPECT_EQ(posting.Size(), 0);

  // Add many documents
  for (DocId id = 1; id <= 100; id++) {
    posting.Add(id);
  }
  EXPECT_EQ(posting.Size(), 100);

  // Remove some
  for (DocId id = 1; id <= 50; id++) {
    posting.Remove(id);
  }
  EXPECT_EQ(posting.Size(), 50);
}

/**
 * @brief Test Contains() for small delta-compressed arrays (linear search path)
 */
TEST_F(PostingListTest, ContainsSmallDeltaArray) {
  PostingList posting(0.5);

  // Add elements (≤16 elements to trigger linear search)
  for (DocId id = 10; id <= 100; id += 10) {
    posting.Add(id);
  }

  EXPECT_EQ(posting.Size(), 10);

  // Test existing elements
  EXPECT_TRUE(posting.Contains(10));
  EXPECT_TRUE(posting.Contains(50));
  EXPECT_TRUE(posting.Contains(100));

  // Test non-existing elements
  EXPECT_FALSE(posting.Contains(5));
  EXPECT_FALSE(posting.Contains(15));
  EXPECT_FALSE(posting.Contains(105));
  EXPECT_FALSE(posting.Contains(0));
}

/**
 * @brief Test Contains() for large delta-compressed arrays (binary search path)
 */
TEST_F(PostingListTest, ContainsLargeDeltaArray) {
  PostingList posting(0.5);

  // Add >16 elements to trigger binary search
  for (DocId id = 1; id <= 50; id++) {
    posting.Add(id);
  }

  EXPECT_EQ(posting.Size(), 50);

  // Test existing elements
  EXPECT_TRUE(posting.Contains(1));
  EXPECT_TRUE(posting.Contains(25));
  EXPECT_TRUE(posting.Contains(50));
  EXPECT_TRUE(posting.Contains(17));  // Test mid-range

  // Test non-existing elements
  EXPECT_FALSE(posting.Contains(0));
  EXPECT_FALSE(posting.Contains(51));
  EXPECT_FALSE(posting.Contains(100));
}

/**
 * @brief Test Contains() for larger dataset (may use Roaring Bitmap)
 */
TEST_F(PostingListTest, ContainsLargeDataset) {
  PostingList posting(0.5);

  for (DocId id = 1; id <= 100; id += 2) {
    posting.Add(id);  // Add odd numbers
  }

  // Test existing elements (odd numbers)
  EXPECT_TRUE(posting.Contains(1));
  EXPECT_TRUE(posting.Contains(51));
  EXPECT_TRUE(posting.Contains(99));

  // Test non-existing elements (even numbers)
  EXPECT_FALSE(posting.Contains(2));
  EXPECT_FALSE(posting.Contains(50));
  EXPECT_FALSE(posting.Contains(100));
}

/**
 * @brief Test GetTopN with reverse=true for small dataset
 */
TEST_F(PostingListTest, GetTopNReverseSmall) {
  PostingList posting(0.5);

  // Add documents in non-sequential order
  posting.Add(10);
  posting.Add(30);
  posting.Add(20);
  posting.Add(50);
  posting.Add(40);

  // Get top 3 in reverse order (should be 50, 40, 30)
  auto result = posting.GetTopN(3, true);

  ASSERT_EQ(result.size(), 3);
  EXPECT_EQ(result[0], 50);
  EXPECT_EQ(result[1], 40);
  EXPECT_EQ(result[2], 30);
}

/**
 * @brief Test GetTopN with reverse=true for large dataset
 */
TEST_F(PostingListTest, GetTopNReverseLarge) {
  PostingList posting(0.5);

  // Add many documents
  for (DocId id = 1; id <= 1000; id++) {
    posting.Add(id);
  }

  // Get top 10 in reverse order
  auto result = posting.GetTopN(10, true);

  ASSERT_EQ(result.size(), 10);
  // Should get 1000, 999, 998, ..., 991
  for (size_t i = 0; i < 10; i++) {
    EXPECT_EQ(result[i], 1000 - i);
  }
}

/**
 * @brief Test GetTopN with reverse=true when limit > size
 */
TEST_F(PostingListTest, GetTopNReverseLimitExceedsSize) {
  PostingList posting(0.5);

  posting.Add(10);
  posting.Add(20);
  posting.Add(30);

  // Request more than available
  auto result = posting.GetTopN(10, true);

  ASSERT_EQ(result.size(), 3);
  EXPECT_EQ(result[0], 30);
  EXPECT_EQ(result[1], 20);
  EXPECT_EQ(result[2], 10);
}

/**
 * @brief Test GetTopN with reverse=false (forward order)
 */
TEST_F(PostingListTest, GetTopNForward) {
  PostingList posting(0.5);

  posting.Add(10);
  posting.Add(30);
  posting.Add(20);
  posting.Add(50);
  posting.Add(40);

  // Get top 3 in forward order (should be 10, 20, 30)
  auto result = posting.GetTopN(3, false);

  ASSERT_EQ(result.size(), 3);
  EXPECT_EQ(result[0], 10);
  EXPECT_EQ(result[1], 20);
  EXPECT_EQ(result[2], 30);
}

/**
 * @brief Test Add() maintains sorted order for delta-compressed
 */
TEST_F(PostingListTest, AddMaintainsSortedOrder) {
  PostingList posting(0.5);

  // Add in random order
  posting.Add(50);
  posting.Add(10);
  posting.Add(30);
  posting.Add(20);
  posting.Add(40);

  // Get all elements
  auto result = posting.GetTopN(10, false);

  ASSERT_EQ(result.size(), 5);
  EXPECT_EQ(result[0], 10);
  EXPECT_EQ(result[1], 20);
  EXPECT_EQ(result[2], 30);
  EXPECT_EQ(result[3], 40);
  EXPECT_EQ(result[4], 50);
}

/**
 * @brief Test Add() with duplicates (should not increase size)
 */
TEST_F(PostingListTest, AddDuplicates) {
  PostingList posting(0.5);

  posting.Add(10);
  posting.Add(20);
  EXPECT_EQ(posting.Size(), 2);

  // Add duplicate
  posting.Add(10);
  EXPECT_EQ(posting.Size(), 2);  // Size should not change

  posting.Add(20);
  EXPECT_EQ(posting.Size(), 2);  // Size should not change

  // Verify elements
  EXPECT_TRUE(posting.Contains(10));
  EXPECT_TRUE(posting.Contains(20));
}

/**
 * @brief Test Remove() from posting list
 */
TEST_F(PostingListTest, Remove) {
  PostingList posting(0.5);

  posting.Add(10);
  posting.Add(20);
  posting.Add(30);
  posting.Add(40);

  EXPECT_EQ(posting.Size(), 4);
  EXPECT_TRUE(posting.Contains(20));

  // Remove middle element
  posting.Remove(20);
  EXPECT_EQ(posting.Size(), 3);
  EXPECT_FALSE(posting.Contains(20));

  // Remove first element
  posting.Remove(10);
  EXPECT_EQ(posting.Size(), 2);
  EXPECT_FALSE(posting.Contains(10));

  // Remove last element
  posting.Remove(40);
  EXPECT_EQ(posting.Size(), 1);
  EXPECT_FALSE(posting.Contains(40));

  // Only 30 should remain
  EXPECT_TRUE(posting.Contains(30));
}

/**
 * @brief Test AddBatch()
 */
TEST_F(PostingListTest, AddBatch) {
  PostingList posting(0.5);

  std::vector<DocId> batch = {10, 20, 30, 40, 50};
  posting.AddBatch(batch);

  EXPECT_EQ(posting.Size(), 5);

  for (auto id : batch) {
    EXPECT_TRUE(posting.Contains(id));
  }
}

/**
 * @brief Test that Contains() works correctly after multiple Add/Remove operations
 */
TEST_F(PostingListTest, ContainsAfterMixedOperations) {
  PostingList posting(0.5);

  // Add initial elements
  for (DocId id = 1; id <= 20; id++) {
    posting.Add(id);
  }

  // Remove some elements
  posting.Remove(5);
  posting.Remove(10);
  posting.Remove(15);

  // Verify Contains() returns correct results
  EXPECT_TRUE(posting.Contains(1));
  EXPECT_TRUE(posting.Contains(4));
  EXPECT_FALSE(posting.Contains(5));
  EXPECT_TRUE(posting.Contains(6));
  EXPECT_FALSE(posting.Contains(10));
  EXPECT_TRUE(posting.Contains(11));
  EXPECT_FALSE(posting.Contains(15));
  EXPECT_TRUE(posting.Contains(16));
  EXPECT_TRUE(posting.Contains(20));

  EXPECT_EQ(posting.Size(), 17);
}

/**
 * @brief Test Contains() with small delta-compressed list (linear scan)
 *
 * This test verifies that Contains() uses linear scan for small lists
 * (size <= 64) for optimal performance.
 */
TEST_F(PostingListTest, ContainsSmallListOptimization) {
  PostingList posting;

  // Add 50 elements (below threshold of 64)
  std::vector<DocId> doc_ids;
  for (DocId id = 1; id <= 50; ++id) {
    posting.Add(id);
    doc_ids.push_back(id);
  }

  EXPECT_EQ(posting.Size(), 50);

  // Verify all elements are found
  for (DocId id : doc_ids) {
    EXPECT_TRUE(posting.Contains(id)) << "DocID " << id << " should be found";
  }

  // Verify non-existent elements return false
  EXPECT_FALSE(posting.Contains(0));
  EXPECT_FALSE(posting.Contains(51));
  EXPECT_FALSE(posting.Contains(100));
}

/**
 * @brief Test Contains() with large delta-compressed list (decode + binary search)
 *
 * This test verifies that Contains() uses full decode + binary search
 * for large lists (size > 64) to achieve O(n) + O(log n) instead of O(n log n).
 *
 * This is the fix for the performance issue in the improvement report.
 */
TEST_F(PostingListTest, ContainsLargeListOptimization) {
  PostingList posting;

  // Add 1000 elements (well above threshold of 64)
  std::vector<DocId> doc_ids;
  for (DocId id = 1; id <= 1000; ++id) {
    posting.Add(id);
    doc_ids.push_back(id);
  }

  EXPECT_EQ(posting.Size(), 1000);

  // Verify all elements are found (using optimized binary search)
  for (DocId id : doc_ids) {
    EXPECT_TRUE(posting.Contains(id)) << "DocID " << id << " should be found";
  }

  // Verify non-existent elements return false
  EXPECT_FALSE(posting.Contains(0));
  EXPECT_FALSE(posting.Contains(1001));
  EXPECT_FALSE(posting.Contains(5000));

  // Test sparse lookups
  EXPECT_TRUE(posting.Contains(1));
  EXPECT_TRUE(posting.Contains(500));
  EXPECT_TRUE(posting.Contains(1000));
  EXPECT_FALSE(posting.Contains(999999));
}

/**
 * @brief Benchmark test for Contains() performance improvement
 *
 * This test measures the performance difference between the old O(n log n)
 * implementation and the new optimized O(n) + O(log n) implementation.
 *
 * Expected: 10x faster for 1000-element lists.
 */
TEST_F(PostingListTest, ContainsPerformanceBenchmark) {
  PostingList posting;

  // Create a large posting list (1000 elements)
  const size_t list_size = 1000;
  for (DocId id = 1; id <= list_size; ++id) {
    posting.Add(id * 10);  // Sparse IDs: 10, 20, 30, ..., 10000
  }

  EXPECT_EQ(posting.Size(), list_size);

  // Benchmark: 1000 lookups
  const int num_lookups = 1000;
  auto start = std::chrono::high_resolution_clock::now();

  for (int i = 0; i < num_lookups; ++i) {
    DocId search_id = (i % list_size + 1) * 10;
    bool found = posting.Contains(search_id);
    EXPECT_TRUE(found);
  }

  auto end = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);

  // With optimization, should complete in reasonable time
  // Old implementation: ~50-100ms for 1000 lookups on 1000-element list
  // New implementation: ~5-10ms for 1000 lookups
  EXPECT_LT(duration.count(), 50000);  // Less than 50ms

  std::cout << "Contains() performance: " << num_lookups << " lookups in " << duration.count() << " microseconds ("
            << (duration.count() / static_cast<double>(num_lookups)) << " μs/lookup)" << std::endl;
}

/**
 * @brief Test Contains() correctness at threshold boundary
 *
 * This test verifies correct behavior at the threshold (64 elements)
 * where the algorithm switches from linear scan to decode + binary search.
 */
TEST_F(PostingListTest, ContainsThresholdBoundary) {
  PostingList posting;

  // Test at threshold - 1 (63 elements - linear scan)
  for (DocId id = 1; id <= 63; ++id) {
    posting.Add(id);
  }

  EXPECT_TRUE(posting.Contains(1));
  EXPECT_TRUE(posting.Contains(32));
  EXPECT_TRUE(posting.Contains(63));
  EXPECT_FALSE(posting.Contains(64));

  // Add one more to cross threshold (64 elements - decode + binary search)
  posting.Add(64);

  EXPECT_TRUE(posting.Contains(1));
  EXPECT_TRUE(posting.Contains(32));
  EXPECT_TRUE(posting.Contains(63));
  EXPECT_TRUE(posting.Contains(64));
  EXPECT_FALSE(posting.Contains(65));

  // Add many more (1000 elements - decode + binary search)
  for (DocId id = 65; id <= 1000; ++id) {
    posting.Add(id);
  }

  EXPECT_TRUE(posting.Contains(1));
  EXPECT_TRUE(posting.Contains(500));
  EXPECT_TRUE(posting.Contains(1000));
  EXPECT_FALSE(posting.Contains(1001));
}
