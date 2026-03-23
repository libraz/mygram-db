/**
 * @file filter_index_test.cpp
 * @brief Unit tests for FilterIndex bitmap-based filter acceleration
 */

#include "storage/filter_index.h"

#include <gtest/gtest.h>

#include <string>
#include <unordered_map>

using namespace mygramdb::storage;

class FilterIndexTest : public ::testing::Test {
 protected:
  FilterIndex index_;
};

TEST_F(FilterIndexTest, AddAndGetEqBitmap) {
  std::unordered_map<std::string, FilterValue> filters;
  filters["category"] = std::string("tech");

  index_.AddDocument(1, filters);
  index_.AddDocument(2, filters);

  std::string key = FilterIndex::SerializeFilterValue(FilterValue{std::string("tech")});
  const roaring_bitmap_t* bm = index_.GetEqBitmap("category", key);
  ASSERT_NE(bm, nullptr);
  EXPECT_EQ(roaring_bitmap_get_cardinality(bm), 2);
  EXPECT_TRUE(roaring_bitmap_contains(bm, 1));
  EXPECT_TRUE(roaring_bitmap_contains(bm, 2));
}

TEST_F(FilterIndexTest, DifferentValues) {
  index_.AddDocument(1, {{"category", FilterValue{std::string("tech")}}});
  index_.AddDocument(2, {{"category", FilterValue{std::string("news")}}});
  index_.AddDocument(3, {{"category", FilterValue{std::string("tech")}}});

  auto key_tech = FilterIndex::SerializeFilterValue(FilterValue{std::string("tech")});
  auto key_news = FilterIndex::SerializeFilterValue(FilterValue{std::string("news")});

  const roaring_bitmap_t* bm_tech = index_.GetEqBitmap("category", key_tech);
  const roaring_bitmap_t* bm_news = index_.GetEqBitmap("category", key_news);

  ASSERT_NE(bm_tech, nullptr);
  ASSERT_NE(bm_news, nullptr);
  EXPECT_EQ(roaring_bitmap_get_cardinality(bm_tech), 2);
  EXPECT_EQ(roaring_bitmap_get_cardinality(bm_news), 1);
}

TEST_F(FilterIndexTest, RemoveDocument) {
  index_.AddDocument(1, {{"category", FilterValue{std::string("tech")}}});
  index_.AddDocument(2, {{"category", FilterValue{std::string("tech")}}});

  index_.RemoveDocument(1, {{"category", FilterValue{std::string("tech")}}});

  auto key = FilterIndex::SerializeFilterValue(FilterValue{std::string("tech")});
  const roaring_bitmap_t* bm = index_.GetEqBitmap("category", key);
  ASSERT_NE(bm, nullptr);
  EXPECT_EQ(roaring_bitmap_get_cardinality(bm), 1);
  EXPECT_FALSE(roaring_bitmap_contains(bm, 1));
  EXPECT_TRUE(roaring_bitmap_contains(bm, 2));
}

TEST_F(FilterIndexTest, RemoveLastDocCleansUpBitmap) {
  index_.AddDocument(1, {{"category", FilterValue{std::string("tech")}}});
  index_.RemoveDocument(1, {{"category", FilterValue{std::string("tech")}}});

  auto key = FilterIndex::SerializeFilterValue(FilterValue{std::string("tech")});
  EXPECT_EQ(index_.GetEqBitmap("category", key), nullptr);
}

TEST_F(FilterIndexTest, UpdateDocument) {
  index_.AddDocument(1, {{"category", FilterValue{std::string("tech")}}});

  std::unordered_map<std::string, FilterValue> old_filters = {{"category", FilterValue{std::string("tech")}}};
  std::unordered_map<std::string, FilterValue> new_filters = {{"category", FilterValue{std::string("news")}}};

  index_.UpdateDocument(1, old_filters, new_filters);

  auto key_tech = FilterIndex::SerializeFilterValue(FilterValue{std::string("tech")});
  auto key_news = FilterIndex::SerializeFilterValue(FilterValue{std::string("news")});

  EXPECT_EQ(index_.GetEqBitmap("category", key_tech), nullptr);
  const roaring_bitmap_t* bm = index_.GetEqBitmap("category", key_news);
  ASSERT_NE(bm, nullptr);
  EXPECT_TRUE(roaring_bitmap_contains(bm, 1));
}

TEST_F(FilterIndexTest, NumericFilterValues) {
  index_.AddDocument(1, {{"status", FilterValue{static_cast<int64_t>(1)}}});
  index_.AddDocument(2, {{"status", FilterValue{static_cast<int64_t>(2)}}});
  index_.AddDocument(3, {{"status", FilterValue{static_cast<int64_t>(1)}}});

  auto key1 = FilterIndex::SerializeFilterValue(FilterValue{static_cast<int64_t>(1)});
  auto key2 = FilterIndex::SerializeFilterValue(FilterValue{static_cast<int64_t>(2)});

  const roaring_bitmap_t* bm1 = index_.GetEqBitmap("status", key1);
  ASSERT_NE(bm1, nullptr);
  EXPECT_EQ(roaring_bitmap_get_cardinality(bm1), 2);

  const roaring_bitmap_t* bm2 = index_.GetEqBitmap("status", key2);
  ASSERT_NE(bm2, nullptr);
  EXPECT_EQ(roaring_bitmap_get_cardinality(bm2), 1);
}

TEST_F(FilterIndexTest, NonexistentBitmapReturnsNull) {
  auto key = FilterIndex::SerializeFilterValue(FilterValue{std::string("nonexistent")});
  EXPECT_EQ(index_.GetEqBitmap("no_column", key), nullptr);
}

TEST_F(FilterIndexTest, NullValuesSkipped) {
  // monostate (NULL) should not create a bitmap entry
  index_.AddDocument(1, {{"category", FilterValue{std::monostate{}}}});

  auto key = FilterIndex::SerializeFilterValue(FilterValue{std::monostate{}});
  EXPECT_EQ(index_.GetEqBitmap("category", key), nullptr);
}

TEST_F(FilterIndexTest, ClearRemovesAll) {
  index_.AddDocument(1, {{"category", FilterValue{std::string("tech")}}});
  index_.AddDocument(2, {{"status", FilterValue{static_cast<int64_t>(1)}}});

  index_.Clear();

  auto key = FilterIndex::SerializeFilterValue(FilterValue{std::string("tech")});
  EXPECT_EQ(index_.GetEqBitmap("category", key), nullptr);
}

TEST_F(FilterIndexTest, MemoryUsageNonZero) {
  index_.AddDocument(1, {{"category", FilterValue{std::string("tech")}}});
  EXPECT_GT(index_.MemoryUsage(), 0);
}

TEST_F(FilterIndexTest, MultipleColumns) {
  std::unordered_map<std::string, FilterValue> filters;
  filters["category"] = std::string("tech");
  filters["status"] = static_cast<int64_t>(1);

  index_.AddDocument(1, filters);

  auto key_cat = FilterIndex::SerializeFilterValue(FilterValue{std::string("tech")});
  auto key_status = FilterIndex::SerializeFilterValue(FilterValue{static_cast<int64_t>(1)});

  EXPECT_NE(index_.GetEqBitmap("category", key_cat), nullptr);
  EXPECT_NE(index_.GetEqBitmap("status", key_status), nullptr);
}

TEST_F(FilterIndexTest, BoolFilterValue) {
  index_.AddDocument(1, {{"active", FilterValue{true}}});
  index_.AddDocument(2, {{"active", FilterValue{false}}});

  auto key_true = FilterIndex::SerializeFilterValue(FilterValue{true});
  auto key_false = FilterIndex::SerializeFilterValue(FilterValue{false});

  const roaring_bitmap_t* bm_true = index_.GetEqBitmap("active", key_true);
  const roaring_bitmap_t* bm_false = index_.GetEqBitmap("active", key_false);

  ASSERT_NE(bm_true, nullptr);
  ASSERT_NE(bm_false, nullptr);
  EXPECT_TRUE(roaring_bitmap_contains(bm_true, 1));
  EXPECT_TRUE(roaring_bitmap_contains(bm_false, 2));
}

TEST_F(FilterIndexTest, BitmapIntersection) {
  // Test that bitmaps can be intersected for multi-filter queries
  index_.AddDocument(
      1, {{"category", FilterValue{std::string("tech")}}, {"status", FilterValue{static_cast<int64_t>(1)}}});
  index_.AddDocument(
      2, {{"category", FilterValue{std::string("tech")}}, {"status", FilterValue{static_cast<int64_t>(2)}}});
  index_.AddDocument(
      3, {{"category", FilterValue{std::string("news")}}, {"status", FilterValue{static_cast<int64_t>(1)}}});

  auto key_tech = FilterIndex::SerializeFilterValue(FilterValue{std::string("tech")});
  auto key_status1 = FilterIndex::SerializeFilterValue(FilterValue{static_cast<int64_t>(1)});

  const roaring_bitmap_t* bm_tech = index_.GetEqBitmap("category", key_tech);
  const roaring_bitmap_t* bm_status1 = index_.GetEqBitmap("status", key_status1);

  ASSERT_NE(bm_tech, nullptr);
  ASSERT_NE(bm_status1, nullptr);

  // Manually intersect
  roaring_bitmap_t* intersection = roaring_bitmap_and(bm_tech, bm_status1);
  EXPECT_EQ(roaring_bitmap_get_cardinality(intersection), 1);
  EXPECT_TRUE(roaring_bitmap_contains(intersection, 1));
  roaring_bitmap_free(intersection);
}

TEST_F(FilterIndexTest, LargeScale) {
  // Add 10K documents with 5 category values
  constexpr uint32_t kDocCount = 10000;
  constexpr int kCategories = 5;
  for (uint32_t i = 1; i <= kDocCount; ++i) {
    std::string cat = "cat_" + std::to_string(i % kCategories);
    index_.AddDocument(i, {{"category", FilterValue{cat}}});
  }

  // Each category should have ~2000 docs
  auto key = FilterIndex::SerializeFilterValue(FilterValue{std::string("cat_0")});
  const roaring_bitmap_t* bm = index_.GetEqBitmap("category", key);
  ASSERT_NE(bm, nullptr);
  EXPECT_EQ(roaring_bitmap_get_cardinality(bm), kDocCount / kCategories);
}
