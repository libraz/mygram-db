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

// BUG-5: DeserializeToDisplayString for double is locale-independent
TEST_F(FilterIndexFacetTest, DeserializeDoubleLocaleIndependent) {
  // Verify that decimal point is always '.' not ','
  FilterValue double_val = 3.14;
  std::string serialized = FilterIndex::SerializeFilterValue(double_val);
  std::string display = FilterIndex::DeserializeToDisplayString(serialized);
  EXPECT_EQ(display.find(','), std::string::npos)
      << "Display should not contain ',' as decimal separator, got: " << display;
  EXPECT_NE(display.find('.'), std::string::npos)
      << "Display should contain '.' as decimal separator, got: " << display;
  // Check specific value
  EXPECT_EQ(display, "3.14");
}

TEST_F(FilterIndexFacetTest, DeserializeDoubleWholeNumber) {
  FilterValue double_val = 42.0;
  std::string serialized = FilterIndex::SerializeFilterValue(double_val);
  std::string display = FilterIndex::DeserializeToDisplayString(serialized);
  // to_chars produces "42" for 42.0 (minimal representation)
  EXPECT_EQ(display, "42");
}

// =============================================================================
// Facet + pre-filtered bitmap: no double-application of exclusion (m-16)
// =============================================================================
// When a search pipeline has already removed documents (via NOT or column
// filters), the result bitmap passed to GetColumnValueCountsFiltered should
// yield correct counts. Applying the exclusion again on the bitmap before
// passing it would incorrectly reduce counts further.
//
// This tests the correct pattern: exclude documents from the bitmap ONCE,
// then pass the bitmap to GetColumnValueCountsFiltered.
// =============================================================================

TEST_F(FilterIndexFacetTest, FilteredFacetAfterNotExclusion) {
  // Scenario: user searches with NOT status="inactive"
  // Docs 1,2,4,5 have status="active", doc 3 has status="inactive"
  // After NOT filter, the result set should be {1, 2, 4, 5}

  // Build result bitmap excluding doc 3 (simulating NOT filter applied once)
  roaring_bitmap_t* bm = roaring_bitmap_create();
  roaring_bitmap_add(bm, 1);
  roaring_bitmap_add(bm, 2);
  roaring_bitmap_add(bm, 4);
  roaring_bitmap_add(bm, 5);

  auto counts = filter_index_.GetColumnValueCountsFiltered("category", bm);
  roaring_bitmap_free(bm);

  // news: docs 1,5 -> 2; sports: doc 2 -> 1; tech: doc 4 -> 1
  ASSERT_EQ(counts.size(), 3);
  EXPECT_EQ(FindCount(counts, "news"), 2);
  EXPECT_EQ(FindCount(counts, "sports"), 1);
  EXPECT_EQ(FindCount(counts, "tech"), 1);
}

TEST_F(FilterIndexFacetTest, FilteredFacetAfterColumnFilter) {
  // Scenario: user filters by status="active"
  // Active docs: 1, 2, 4, 5
  // The pipeline has already filtered to these docs; facet on "category"

  roaring_bitmap_t* bm = roaring_bitmap_create();
  roaring_bitmap_add(bm, 1);
  roaring_bitmap_add(bm, 2);
  roaring_bitmap_add(bm, 4);
  roaring_bitmap_add(bm, 5);

  auto counts = filter_index_.GetColumnValueCountsFiltered("category", bm);
  roaring_bitmap_free(bm);

  // Same as above: news=2, sports=1, tech=1
  EXPECT_EQ(FindCount(counts, "news"), 2);
  EXPECT_EQ(FindCount(counts, "sports"), 1);
  EXPECT_EQ(FindCount(counts, "tech"), 1);
}

TEST_F(FilterIndexFacetTest, FilteredFacetIdempotentWithSameBitmap) {
  // Verify that calling GetColumnValueCountsFiltered twice with the same
  // bitmap produces identical results (no internal state mutation)
  roaring_bitmap_t* bm = roaring_bitmap_create();
  roaring_bitmap_add(bm, 1);
  roaring_bitmap_add(bm, 2);
  roaring_bitmap_add(bm, 3);

  auto counts1 = filter_index_.GetColumnValueCountsFiltered("category", bm);
  auto counts2 = filter_index_.GetColumnValueCountsFiltered("category", bm);
  roaring_bitmap_free(bm);

  ASSERT_EQ(counts1.size(), counts2.size());
  for (size_t i = 0; i < counts1.size(); ++i) {
    EXPECT_EQ(counts1[i].first, counts2[i].first);
    EXPECT_EQ(counts1[i].second, counts2[i].second);
  }
}
