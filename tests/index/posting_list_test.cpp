/**
 * @file posting_list_test.cpp
 * @brief Unit tests for PostingList class optimizations
 */

#include "index/posting_list.h"

#include <gtest/gtest.h>

#include <chrono>
#include <iostream>

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
 * @brief Test Contains() with small delta-compressed list (streaming decode)
 *
 * This test verifies that Contains() uses streaming decode with early exit
 * for small lists.
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
 * @brief Test Contains() with large delta-compressed list (streaming decode)
 *
 * This test verifies that Contains() uses streaming decode with early exit
 * for large lists, avoiding O(n) memory allocation from full decode.
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
 * This test measures performance of the streaming decode with early exit
 * approach, which avoids O(n) memory allocation per call.
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
 * @brief Test Contains() correctness at various list sizes
 *
 * This test verifies correct behavior at various sizes to ensure
 * the unified streaming decode path works for all list sizes.
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

/**
 * @brief Test Optimize() correctly converts from Roaring to Delta without deadlock
 *
 * This is a regression test for a critical bug where ConvertToDelta() called
 * GetAll(), which tried to acquire a shared_lock on mutex_ while the caller
 * Optimize() already held a unique_lock. With std::shared_mutex, re-acquiring
 * a lock (even shared) on the same thread is undefined behavior and typically
 * causes a deadlock.
 *
 * The fix accesses the roaring bitmap directly instead of going through GetAll().
 */
TEST_F(PostingListTest, OptimizeRoaringToDeltaNoDeadlock) {
  // Use a very low threshold so that even a few docs trigger Roaring conversion
  PostingList posting(0.01);

  // Add enough documents to exceed the roaring threshold
  // With threshold 0.01 and total_docs=100, density=50/100=0.5 >> 0.01
  std::vector<DocId> doc_ids;
  for (DocId id = 1; id <= 50; ++id) {
    doc_ids.push_back(id);
  }
  posting.AddBatch(doc_ids);

  // First, convert to Roaring by calling Optimize with small total_docs
  // density = 50/100 = 0.5, threshold = 0.01, so 0.5 >= 0.01 triggers Roaring
  posting.Optimize(100);
  EXPECT_EQ(posting.GetStrategy(), PostingStrategy::kRoaringBitmap);

  // Now convert back to Delta by calling Optimize with very large total_docs
  // density = 50/100000 = 0.0005, hysteresis threshold = 0.01 * 0.5 = 0.005
  // 0.0005 < 0.005, so this should trigger ConvertToDelta()
  // Before the fix, this would deadlock due to recursive lock acquisition
  posting.Optimize(100000);
  EXPECT_EQ(posting.GetStrategy(), PostingStrategy::kDeltaCompressed);

  // Verify all data is preserved after the conversion
  EXPECT_EQ(posting.Size(), 50);
  for (DocId id = 1; id <= 50; ++id) {
    EXPECT_TRUE(posting.Contains(id)) << "DocID " << id << " should be preserved after Roaring->Delta conversion";
  }

  // Verify non-existent elements are still not found
  EXPECT_FALSE(posting.Contains(0));
  EXPECT_FALSE(posting.Contains(51));
  EXPECT_FALSE(posting.Contains(100));
}

/**
 * @brief Test Optimize() preserves data integrity through full round-trip conversion
 *
 * Verifies that data survives Delta -> Roaring -> Delta conversion cycle with
 * various doc ID patterns (sparse, dense, edge values).
 */
TEST_F(PostingListTest, OptimizeRoundTripPreservesData) {
  PostingList posting(0.01);

  // Add sparse doc IDs with gaps to test delta encoding correctness
  std::vector<DocId> original_ids = {1, 5, 10, 100, 500, 1000, 5000, 10000, 50000, 100000};
  posting.AddBatch(original_ids);

  EXPECT_EQ(posting.GetStrategy(), PostingStrategy::kDeltaCompressed);

  // Convert to Roaring (density = 10/20 = 0.5 >> 0.01)
  posting.Optimize(20);
  EXPECT_EQ(posting.GetStrategy(), PostingStrategy::kRoaringBitmap);

  // Verify data after Delta -> Roaring
  EXPECT_EQ(posting.Size(), original_ids.size());
  for (DocId id : original_ids) {
    EXPECT_TRUE(posting.Contains(id)) << "DocID " << id << " lost after Delta->Roaring";
  }

  // Convert back to Delta (density = 10/10000000 ≈ 0)
  posting.Optimize(10000000);
  EXPECT_EQ(posting.GetStrategy(), PostingStrategy::kDeltaCompressed);

  // Verify data after Roaring -> Delta
  EXPECT_EQ(posting.Size(), original_ids.size());
  auto all_docs = posting.GetAll();
  ASSERT_EQ(all_docs.size(), original_ids.size());
  for (size_t i = 0; i < original_ids.size(); ++i) {
    EXPECT_EQ(all_docs[i], original_ids[i]) << "DocID mismatch at index " << i << " after round-trip conversion";
  }
}

// =============================================================================
// Iterator consistency tests (forward and reverse)
// =============================================================================

/**
 * @brief Helper to create a PostingList in Roaring bitmap strategy
 *
 * Forces conversion to Roaring by using a very low threshold and calling
 * Optimize() with a small total_docs value to achieve high density.
 */
static PostingList CreateRoaringPostingList(const std::vector<DocId>& doc_ids) {
  PostingList posting(0.01);  // Very low threshold to force Roaring
  posting.AddBatch(doc_ids);
  posting.Optimize(static_cast<uint64_t>(doc_ids.size()));  // density = 1.0 >> 0.01
  return posting;
}

/**
 * @brief Test forward iteration produces correct sorted results (Roaring)
 */
TEST_F(PostingListTest, ForwardIterationRoaringCorrectness) {
  std::vector<DocId> doc_ids = {50, 10, 30, 20, 40};
  std::sort(doc_ids.begin(), doc_ids.end());
  auto posting = CreateRoaringPostingList(doc_ids);

  ASSERT_EQ(posting.GetStrategy(), PostingStrategy::kRoaringBitmap);

  auto result = posting.GetTopN(doc_ids.size(), false);
  ASSERT_EQ(result.size(), doc_ids.size());
  for (size_t i = 0; i < doc_ids.size(); ++i) {
    EXPECT_EQ(result[i], doc_ids[i]) << "Forward iteration mismatch at index " << i;
  }
}

/**
 * @brief Test reverse iteration produces correct reverse-sorted results (Roaring)
 */
TEST_F(PostingListTest, ReverseIterationRoaringCorrectness) {
  std::vector<DocId> doc_ids = {50, 10, 30, 20, 40};
  std::sort(doc_ids.begin(), doc_ids.end());
  auto posting = CreateRoaringPostingList(doc_ids);

  ASSERT_EQ(posting.GetStrategy(), PostingStrategy::kRoaringBitmap);

  auto result = posting.GetTopN(doc_ids.size(), true);
  ASSERT_EQ(result.size(), doc_ids.size());
  for (size_t i = 0; i < doc_ids.size(); ++i) {
    EXPECT_EQ(result[i], doc_ids[doc_ids.size() - 1 - i]) << "Reverse iteration mismatch at index " << i;
  }
}

/**
 * @brief Test forward and reverse iterations produce consistent data (Roaring)
 *
 * Verifies that sorting the reverse result gives the same sequence as the
 * forward result, ensuring both iterators traverse the same set of elements.
 */
TEST_F(PostingListTest, ForwardReverseConsistencyRoaring) {
  std::vector<DocId> doc_ids;
  for (DocId id = 1; id <= 200; ++id) {
    doc_ids.push_back(id * 3);  // 3, 6, 9, ..., 600
  }
  auto posting = CreateRoaringPostingList(doc_ids);

  ASSERT_EQ(posting.GetStrategy(), PostingStrategy::kRoaringBitmap);

  auto forward_result = posting.GetTopN(0, false);  // all, ascending
  auto reverse_result = posting.GetTopN(0, true);   // all, descending

  ASSERT_EQ(forward_result.size(), reverse_result.size());
  ASSERT_EQ(forward_result.size(), doc_ids.size());

  // Reverse the reverse_result and compare with forward_result
  std::vector<DocId> reversed_back(reverse_result.rbegin(), reverse_result.rend());
  EXPECT_EQ(forward_result, reversed_back);

  // Verify forward is sorted ascending
  EXPECT_TRUE(std::is_sorted(forward_result.begin(), forward_result.end()));

  // Verify reverse is sorted descending
  EXPECT_TRUE(std::is_sorted(reverse_result.begin(), reverse_result.end(), std::greater<DocId>()));
}

/**
 * @brief Test iteration on an empty Roaring bitmap
 */
TEST_F(PostingListTest, IterationEmptyRoaringBitmap) {
  // Create empty posting list and force Roaring strategy
  PostingList posting(0.01);
  // Add and remove to trigger Roaring conversion, then empty it
  std::vector<DocId> temp = {1, 2, 3};
  posting.AddBatch(temp);
  posting.Optimize(3);
  ASSERT_EQ(posting.GetStrategy(), PostingStrategy::kRoaringBitmap);

  posting.Remove(1);
  posting.Remove(2);
  posting.Remove(3);
  ASSERT_EQ(posting.Size(), 0);

  auto forward_result = posting.GetTopN(10, false);
  auto reverse_result = posting.GetTopN(10, true);

  EXPECT_TRUE(forward_result.empty());
  EXPECT_TRUE(reverse_result.empty());
}

/**
 * @brief Test iteration on a single-element Roaring bitmap
 */
TEST_F(PostingListTest, IterationSingleElementRoaring) {
  std::vector<DocId> doc_ids = {42};
  auto posting = CreateRoaringPostingList(doc_ids);

  ASSERT_EQ(posting.GetStrategy(), PostingStrategy::kRoaringBitmap);

  auto forward_result = posting.GetTopN(10, false);
  auto reverse_result = posting.GetTopN(10, true);

  ASSERT_EQ(forward_result.size(), 1);
  ASSERT_EQ(reverse_result.size(), 1);
  EXPECT_EQ(forward_result[0], 42);
  EXPECT_EQ(reverse_result[0], 42);
}

/**
 * @brief Test iteration on a large Roaring bitmap with partial retrieval
 *
 * Verifies that GetTopN with a limit smaller than the total count works
 * correctly for both forward and reverse iterators.
 */
TEST_F(PostingListTest, IterationLargeRoaringPartialRetrieval) {
  std::vector<DocId> doc_ids;
  for (DocId id = 1; id <= 10000; ++id) {
    doc_ids.push_back(id);
  }
  auto posting = CreateRoaringPostingList(doc_ids);

  ASSERT_EQ(posting.GetStrategy(), PostingStrategy::kRoaringBitmap);

  // Forward: first 5 elements
  auto forward_result = posting.GetTopN(5, false);
  ASSERT_EQ(forward_result.size(), 5);
  for (size_t i = 0; i < 5; ++i) {
    EXPECT_EQ(forward_result[i], i + 1);
  }

  // Reverse: last 5 elements (highest DocIds)
  auto reverse_result = posting.GetTopN(5, true);
  ASSERT_EQ(reverse_result.size(), 5);
  for (size_t i = 0; i < 5; ++i) {
    EXPECT_EQ(reverse_result[i], 10000 - i);
  }
}

// =============================================================================
// Contains() optimization tests for large delta-compressed lists
// =============================================================================

/**
 * @brief Test Contains() correctness on large sparse delta-compressed list
 *
 * Verifies that the streaming decode with early exit correctly handles
 * a large list with sparse (non-contiguous) doc IDs.
 */
TEST_F(PostingListTest, ContainsLargeSparseList) {
  PostingList posting;

  // Add 10000 sparse elements (every 3rd ID)
  std::vector<DocId> ids;
  for (DocId i = 1; i <= 10000; i += 3) {
    ids.push_back(i);
  }
  posting.AddBatch(ids);

  EXPECT_EQ(posting.GetStrategy(), PostingStrategy::kDeltaCompressed);

  // Test Contains for existing elements
  EXPECT_TRUE(posting.Contains(1));
  EXPECT_TRUE(posting.Contains(4));
  EXPECT_TRUE(posting.Contains(10000));

  // Test Contains for non-existing elements (gaps between sparse IDs)
  EXPECT_FALSE(posting.Contains(0));
  EXPECT_FALSE(posting.Contains(2));
  EXPECT_FALSE(posting.Contains(3));
  EXPECT_FALSE(posting.Contains(10001));
}

/**
 * @brief Benchmark Contains() on large delta-compressed list
 *
 * Measures that 1000 Contains() calls on a 10K-element delta list complete
 * within a reasonable time, validating the streaming decode optimization
 * avoids unnecessary memory allocation.
 */
TEST_F(PostingListTest, ContainsLargeListPerformanceNoAllocation) {
  PostingList posting;

  // Add 10000 elements (even numbers)
  std::vector<DocId> ids;
  ids.reserve(10000);
  for (DocId i = 1; i <= 10000; ++i) {
    ids.push_back(i * 2);
  }
  posting.AddBatch(ids);

  // Ensure still delta-compressed (not Roaring)
  EXPECT_EQ(posting.GetStrategy(), PostingStrategy::kDeltaCompressed);

  // Run 1000 Contains() calls and measure time
  auto start = std::chrono::steady_clock::now();
  size_t found = 0;
  for (int i = 0; i < 1000; ++i) {
    if (posting.Contains(static_cast<DocId>(i * 20 + 2))) {
      found++;
    }
  }
  auto end = std::chrono::steady_clock::now();
  auto duration_us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

  std::cout << "1000 Contains() on 10K delta list: " << duration_us << "us, found=" << found << std::endl;

  // Should complete in reasonable time (< 100ms)
  EXPECT_LT(duration_us, 100000);
}

// =============================================================================
// Delta encoding roundtrip tests (#3)
// =============================================================================

/**
 * @brief Test EncodeDelta/DecodeDelta roundtrip with sorted input
 */
TEST_F(PostingListTest, DeltaEncodingRoundtrip) {
  PostingList posting(0.5);

  // Add documents in order
  std::vector<DocId> doc_ids = {5, 10, 20, 50, 100, 500, 1000};
  for (auto id : doc_ids) {
    posting.Add(id);
  }

  // Verify all elements are retrievable
  auto all = posting.GetAll();
  ASSERT_EQ(all.size(), doc_ids.size());
  for (size_t i = 0; i < doc_ids.size(); ++i) {
    EXPECT_EQ(all[i], doc_ids[i]);
  }
}

/**
 * @brief Test delta encoding with empty input
 */
TEST_F(PostingListTest, DeltaEncodingEmpty) {
  PostingList posting(0.5);
  EXPECT_EQ(posting.Size(), 0);
  auto all = posting.GetAll();
  EXPECT_TRUE(all.empty());
}

/**
 * @brief Test delta encoding with single element
 */
TEST_F(PostingListTest, DeltaEncodingSingleElement) {
  PostingList posting(0.5);
  posting.Add(42);
  EXPECT_EQ(posting.Size(), 1);
  auto all = posting.GetAll();
  ASSERT_EQ(all.size(), 1);
  EXPECT_EQ(all[0], 42);
}

/**
 * @brief Test delta encoding with large DocId values
 */
TEST_F(PostingListTest, DeltaEncodingLargeDocIds) {
  PostingList posting(0.5);

  // Use large DocId values near uint32_t max
  std::vector<DocId> large_ids = {4000000000U, 4100000000U, 4200000000U, 4294967290U};
  for (auto id : large_ids) {
    posting.Add(id);
  }

  auto all = posting.GetAll();
  ASSERT_EQ(all.size(), large_ids.size());
  for (size_t i = 0; i < large_ids.size(); ++i) {
    EXPECT_EQ(all[i], large_ids[i]);
  }
}

/**
 * @brief Test delta encoding with consecutive DocIds
 */
TEST_F(PostingListTest, DeltaEncodingConsecutive) {
  PostingList posting(0.5);

  for (DocId id = 1; id <= 20; ++id) {
    posting.Add(id);
  }

  auto all = posting.GetAll();
  ASSERT_EQ(all.size(), 20);
  for (DocId id = 1; id <= 20; ++id) {
    EXPECT_EQ(all[id - 1], id);
  }
}

// =============================================================================
// ConvertToRoaring data preservation test (#5)
// =============================================================================

/**
 * @brief Test that ConvertToRoaring preserves all data
 */
TEST_F(PostingListTest, ConvertToRoaringPreservesData) {
  // Use very low threshold so Optimize triggers conversion
  PostingList posting(0.01);  // 1% threshold

  // Add enough documents to trigger roaring conversion
  std::vector<DocId> doc_ids;
  for (DocId id = 1; id <= 100; ++id) {
    doc_ids.push_back(id);
  }
  posting.AddBatch(doc_ids);

  // Force optimization with total_docs close to doc count (high density)
  posting.Optimize(100);  // density = 100/100 = 1.0 > threshold

  // Verify all data preserved after conversion
  EXPECT_EQ(posting.Size(), 100);
  for (DocId id = 1; id <= 100; ++id) {
    EXPECT_TRUE(posting.Contains(id)) << "DocId " << id << " missing after ConvertToRoaring";
  }

  auto all = posting.GetAll();
  ASSERT_EQ(all.size(), 100);
  for (DocId id = 1; id <= 100; ++id) {
    EXPECT_EQ(all[id - 1], id);
  }
}
