/**
 * @file posting_list_test.cpp
 * @brief Unit tests for PostingList class optimizations
 */

#include <gtest/gtest.h>

#include "index/posting_list.h"

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
