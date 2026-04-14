/**
 * @file filter_index_facet_test.cpp
 * @brief Unit tests for FilterIndex facet API (GetColumnValueCounts,
 *        GetColumnValueCountsFiltered, DeserializeToDisplayString)
 */

#include "storage/filter_index.h"

#include <gtest/gtest.h>
#include <roaring/roaring.h>

#include <algorithm>
#include <string>
#include <utility>
#include <variant>
#include <vector>

using namespace mygramdb::storage;

class FilterIndexFacetTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Doc 1: category="news", status="active"
    FilterMap f1;
    f1["category"] = std::string("news");
    f1["status"] = std::string("active");
    filter_index_.AddDocument(1, f1);

    // Doc 2: category="sports", status="active"
    FilterMap f2;
    f2["category"] = std::string("sports");
    f2["status"] = std::string("active");
    filter_index_.AddDocument(2, f2);

    // Doc 3: category="news", status="inactive"
    FilterMap f3;
    f3["category"] = std::string("news");
    f3["status"] = std::string("inactive");
    filter_index_.AddDocument(3, f3);

    // Doc 4: category="tech", status="active"
    FilterMap f4;
    f4["category"] = std::string("tech");
    f4["status"] = std::string("active");
    filter_index_.AddDocument(4, f4);

    // Doc 5: category="news", status="active"
    FilterMap f5;
    f5["category"] = std::string("news");
    f5["status"] = std::string("active");
    filter_index_.AddDocument(5, f5);
  }

  FilterIndex filter_index_;
};

// Helper: find count for a given deserialized display string in facet results
static uint64_t FindCount(const std::vector<std::pair<std::string, uint64_t>>& counts,
                          const std::string& display_value) {
  for (const auto& [serialized, count] : counts) {
    if (FilterIndex::DeserializeToDisplayString(serialized) == display_value) {
      return count;
    }
  }
  return 0;
}

// --- GetColumnValueCounts tests ---

TEST_F(FilterIndexFacetTest, GetColumnValueCounts_ReturnsAllValues) {
  auto counts = filter_index_.GetColumnValueCounts("category");
  ASSERT_EQ(counts.size(), 3);

  EXPECT_EQ(FindCount(counts, "news"), 3);
  EXPECT_EQ(FindCount(counts, "sports"), 1);
  EXPECT_EQ(FindCount(counts, "tech"), 1);
}

TEST_F(FilterIndexFacetTest, GetColumnValueCounts_SortedByCountDescending) {
  auto counts = filter_index_.GetColumnValueCounts("category");
  ASSERT_GE(counts.size(), 2);

  // First entry should have the highest count
  for (size_t i = 1; i < counts.size(); ++i) {
    EXPECT_GE(counts[i - 1].second, counts[i].second)
        << "Entry at index " << (i - 1) << " (count=" << counts[i - 1].second
        << ") should be >= entry at index " << i << " (count=" << counts[i].second << ")";
  }
}

TEST_F(FilterIndexFacetTest, GetColumnValueCounts_EmptyColumn) {
  auto counts = filter_index_.GetColumnValueCounts("nonexistent");
  EXPECT_TRUE(counts.empty());
}

// --- GetColumnValueCountsFiltered tests ---

TEST_F(FilterIndexFacetTest, GetColumnValueCountsFiltered_RestrictsToBitmap) {
  // Bitmap contains docs {1, 2, 3} — excludes doc 4 (tech) and doc 5 (news)
  roaring_bitmap_t* bm = roaring_bitmap_create();
  roaring_bitmap_add(bm, 1);
  roaring_bitmap_add(bm, 2);
  roaring_bitmap_add(bm, 3);

  auto counts = filter_index_.GetColumnValueCountsFiltered("category", bm);
  roaring_bitmap_free(bm);

  // news: docs 1,3 -> 2; sports: doc 2 -> 1; tech: excluded
  ASSERT_EQ(counts.size(), 2);
  EXPECT_EQ(FindCount(counts, "news"), 2);
  EXPECT_EQ(FindCount(counts, "sports"), 1);
  EXPECT_EQ(FindCount(counts, "tech"), 0);  // Not present at all
}

TEST_F(FilterIndexFacetTest, GetColumnValueCountsFiltered_ZeroCountExcluded) {
  // Bitmap contains only doc 4 (tech)
  roaring_bitmap_t* bm = roaring_bitmap_create();
  roaring_bitmap_add(bm, 4);

  auto counts = filter_index_.GetColumnValueCountsFiltered("category", bm);
  roaring_bitmap_free(bm);

  // Only tech should appear
  ASSERT_EQ(counts.size(), 1);
  EXPECT_EQ(FindCount(counts, "tech"), 1);
  EXPECT_EQ(FindCount(counts, "news"), 0);
  EXPECT_EQ(FindCount(counts, "sports"), 0);
}

TEST_F(FilterIndexFacetTest, GetColumnValueCountsFiltered_NullBitmapFallsBack) {
  auto counts_filtered = filter_index_.GetColumnValueCountsFiltered("category", nullptr);
  auto counts_all = filter_index_.GetColumnValueCounts("category");

  ASSERT_EQ(counts_filtered.size(), counts_all.size());

  // Both should have the same entries (sorted by count desc, so order matches)
  for (size_t i = 0; i < counts_all.size(); ++i) {
    EXPECT_EQ(counts_filtered[i].first, counts_all[i].first);
    EXPECT_EQ(counts_filtered[i].second, counts_all[i].second);
  }
}

// --- DeserializeToDisplayString tests ---

TEST_F(FilterIndexFacetTest, DeserializeToDisplayString_StringType) {
  std::string serialized = FilterIndex::SerializeFilterValue(FilterValue{std::string("hello")});
  std::string display = FilterIndex::DeserializeToDisplayString(serialized);
  EXPECT_EQ(display, "hello");
}

TEST_F(FilterIndexFacetTest, DeserializeToDisplayString_BoolType) {
  std::string ser_true = FilterIndex::SerializeFilterValue(FilterValue{true});
  std::string ser_false = FilterIndex::SerializeFilterValue(FilterValue{false});
  EXPECT_EQ(FilterIndex::DeserializeToDisplayString(ser_true), "true");
  EXPECT_EQ(FilterIndex::DeserializeToDisplayString(ser_false), "false");
}

TEST_F(FilterIndexFacetTest, DeserializeToDisplayString_Int32Type) {
  std::string serialized = FilterIndex::SerializeFilterValue(FilterValue{int32_t(42)});
  EXPECT_EQ(FilterIndex::DeserializeToDisplayString(serialized), "42");
}

TEST_F(FilterIndexFacetTest, DeserializeToDisplayString_Int64Type) {
  std::string serialized = FilterIndex::SerializeFilterValue(FilterValue{int64_t(-100)});
  EXPECT_EQ(FilterIndex::DeserializeToDisplayString(serialized), "-100");
}

TEST_F(FilterIndexFacetTest, DeserializeToDisplayString_Uint64Type) {
  std::string serialized = FilterIndex::SerializeFilterValue(FilterValue{uint64_t(12345)});
  EXPECT_EQ(FilterIndex::DeserializeToDisplayString(serialized), "12345");
}

TEST_F(FilterIndexFacetTest, DeserializeToDisplayString_DoubleType) {
  std::string serialized = FilterIndex::SerializeFilterValue(FilterValue{double(3.14)});
  std::string display = FilterIndex::DeserializeToDisplayString(serialized);
  EXPECT_TRUE(display.find("3.14") == 0)
      << "Expected display to start with '3.14', got: " << display;
}

TEST_F(FilterIndexFacetTest, DeserializeToDisplayString_NullType) {
  std::string serialized = FilterIndex::SerializeFilterValue(FilterValue{std::monostate{}});
  EXPECT_EQ(FilterIndex::DeserializeToDisplayString(serialized), "NULL");
}

TEST_F(FilterIndexFacetTest, DeserializeToDisplayString_EmptyString) {
  EXPECT_EQ(FilterIndex::DeserializeToDisplayString(""), "NULL");
}
