/**
 * @file roaring_bitmap_ptr_test.cpp
 * @brief Unit tests for RoaringBitmapPtr / MakeRoaringFromVector / MakeEmptyRoaring
 */

#include "utils/roaring_bitmap_ptr.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <utility>
#include <vector>

namespace {

using mygram::utils::MakeEmptyRoaring;
using mygram::utils::MakeRoaringFromVector;
using mygram::utils::RoaringBitmapPtr;

TEST(RoaringBitmapPtrTest, MakeEmptyReturnsValidEmptyBitmap) {
  RoaringBitmapPtr bm = MakeEmptyRoaring();
  ASSERT_NE(bm.get(), nullptr);
  EXPECT_EQ(roaring_bitmap_get_cardinality(bm.get()), 0u);
}

TEST(RoaringBitmapPtrTest, MakeFromVectorPopulatesBitmap) {
  std::vector<uint32_t> ids{3, 1, 4, 1, 5, 9, 2, 6};
  RoaringBitmapPtr bm = MakeRoaringFromVector(ids);
  ASSERT_NE(bm.get(), nullptr);

  // Cardinality reflects unique values (1, 2, 3, 4, 5, 6, 9 -> 7 entries).
  EXPECT_EQ(roaring_bitmap_get_cardinality(bm.get()), 7u);

  EXPECT_TRUE(roaring_bitmap_contains(bm.get(), 1));
  EXPECT_TRUE(roaring_bitmap_contains(bm.get(), 9));
  EXPECT_FALSE(roaring_bitmap_contains(bm.get(), 7));
}

TEST(RoaringBitmapPtrTest, MakeFromEmptyVectorReturnsEmptyBitmap) {
  std::vector<uint32_t> ids;
  RoaringBitmapPtr bm = MakeRoaringFromVector(ids);
  ASSERT_NE(bm.get(), nullptr);
  EXPECT_EQ(roaring_bitmap_get_cardinality(bm.get()), 0u);
}

TEST(RoaringBitmapPtrTest, DestructorFreesBitmap) {
  // Construct and let go out of scope; ASAN/LSAN catches a leak if the
  // deleter is not wired up correctly.
  for (int i = 0; i < 10; ++i) {
    RoaringBitmapPtr bm = MakeRoaringFromVector({1U, 2U, 3U});
    ASSERT_NE(bm.get(), nullptr);
  }
  SUCCEED();
}

TEST(RoaringBitmapPtrTest, MoveTransfersOwnership) {
  RoaringBitmapPtr first = MakeRoaringFromVector({10U, 20U, 30U});
  ASSERT_NE(first.get(), nullptr);
  roaring_bitmap_t* raw = first.get();

  RoaringBitmapPtr second = std::move(first);
  EXPECT_EQ(first.get(), nullptr);
  EXPECT_EQ(second.get(), raw);
  EXPECT_EQ(roaring_bitmap_get_cardinality(second.get()), 3u);
}

TEST(RoaringBitmapPtrTest, NullDeleterIsNoop) {
  // Default-constructed unique_ptr holds null and the deleter must accept it.
  RoaringBitmapPtr empty;
  EXPECT_EQ(empty.get(), nullptr);
  // Destruction here exercises the noexcept null-check in RoaringBitmapDeleter.
}

}  // namespace
