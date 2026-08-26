/**
 * @file filter_index_test.cpp
 * @brief Unit tests for FilterIndex bitmap-based filter acceleration
 */

#include "storage/filter_index.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>

#include "utils/roaring_bitmap_ptr.h"

using namespace mygramdb::storage;

class FilterIndexTest : public ::testing::Test {
 protected:
  /// Add a document, failing the test if the index could not take it.
  void Add(DocId doc_id, const FilterMap& filters) { ASSERT_TRUE(index_.AddDocument(doc_id, filters)); }

  /// Replace a document's values, failing the test if the index could not take them.
  void Update(DocId doc_id, const FilterMap& old_filters, const FilterMap& new_filters) {
    ASSERT_TRUE(index_.UpdateDocument(doc_id, old_filters, new_filters));
  }

  /// Doc ids selected by (column, value); empty when the value is not indexed.
  std::vector<DocId> Selected(std::string_view column, const FilterValue& value) {
    std::vector<DocId> doc_ids;
    index_.CollectEqDocIds(column, FilterIndex::SerializeFilterValue(value), doc_ids);
    return doc_ids;
  }

  /// Whether (column, value) has an entry at all, regardless of what it selects.
  bool Indexed(std::string_view column, const FilterValue& value) {
    std::vector<DocId> doc_ids;
    return index_.CollectEqDocIds(column, FilterIndex::SerializeFilterValue(value), doc_ids);
  }

  FilterIndex index_;
};

TEST_F(FilterIndexTest, AddAndSelectByValue) {
  FilterMap filters;
  filters["category"] = std::string("tech");

  Add(1, filters);
  Add(2, filters);

  EXPECT_EQ(Selected("category", FilterValue{std::string("tech")}), (std::vector<DocId>{1, 2}));
}

TEST_F(FilterIndexTest, DifferentValues) {
  Add(1, {{"category", FilterValue{std::string("tech")}}});
  Add(2, {{"category", FilterValue{std::string("news")}}});
  Add(3, {{"category", FilterValue{std::string("tech")}}});

  EXPECT_EQ(Selected("category", FilterValue{std::string("tech")}), (std::vector<DocId>{1, 3}));
  EXPECT_EQ(Selected("category", FilterValue{std::string("news")}), (std::vector<DocId>{2}));
}

TEST_F(FilterIndexTest, RemoveDocument) {
  Add(1, {{"category", FilterValue{std::string("tech")}}});
  Add(2, {{"category", FilterValue{std::string("tech")}}});

  index_.RemoveDocument(1, {{"category", FilterValue{std::string("tech")}}});

  EXPECT_EQ(Selected("category", FilterValue{std::string("tech")}), (std::vector<DocId>{2}));
}

TEST_F(FilterIndexTest, RemoveLastDocCleansUpBitmap) {
  Add(1, {{"category", FilterValue{std::string("tech")}}});
  index_.RemoveDocument(1, {{"category", FilterValue{std::string("tech")}}});

  EXPECT_FALSE(Indexed("category", FilterValue{std::string("tech")}));
}

TEST_F(FilterIndexTest, UpdateDocument) {
  Add(1, {{"category", FilterValue{std::string("tech")}}});

  FilterMap old_filters = {{"category", FilterValue{std::string("tech")}}};
  FilterMap new_filters = {{"category", FilterValue{std::string("news")}}};

  Update(1, old_filters, new_filters);

  EXPECT_FALSE(Indexed("category", FilterValue{std::string("tech")}));
  EXPECT_EQ(Selected("category", FilterValue{std::string("news")}), (std::vector<DocId>{1}));
}

TEST_F(FilterIndexTest, NumericFilterValues) {
  Add(1, {{"status", FilterValue{static_cast<int64_t>(1)}}});
  Add(2, {{"status", FilterValue{static_cast<int64_t>(2)}}});
  Add(3, {{"status", FilterValue{static_cast<int64_t>(1)}}});

  EXPECT_EQ(Selected("status", FilterValue{static_cast<int64_t>(1)}), (std::vector<DocId>{1, 3}));
  EXPECT_EQ(Selected("status", FilterValue{static_cast<int64_t>(2)}), (std::vector<DocId>{2}));
}

TEST_F(FilterIndexTest, NonexistentValueIsReportedAsNotIndexed) {
  EXPECT_FALSE(Indexed("no_column", FilterValue{std::string("nonexistent")}));
}

TEST_F(FilterIndexTest, NullValuesSkipped) {
  // monostate (NULL) should not create a bitmap entry
  Add(1, {{"category", FilterValue{std::monostate{}}}});

  EXPECT_FALSE(Indexed("category", FilterValue{std::monostate{}}));
}

TEST_F(FilterIndexTest, ClearRemovesAll) {
  Add(1, {{"category", FilterValue{std::string("tech")}}});
  Add(2, {{"status", FilterValue{static_cast<int64_t>(1)}}});

  index_.Clear();

  EXPECT_FALSE(Indexed("category", FilterValue{std::string("tech")}));
}

TEST_F(FilterIndexTest, MemoryUsageNonZero) {
  Add(1, {{"category", FilterValue{std::string("tech")}}});
  EXPECT_GT(index_.MemoryUsage(), 0);
}

TEST_F(FilterIndexTest, MultipleColumns) {
  FilterMap filters;
  filters["category"] = std::string("tech");
  filters["status"] = static_cast<int64_t>(1);

  Add(1, filters);

  EXPECT_EQ(Selected("category", FilterValue{std::string("tech")}), (std::vector<DocId>{1}));
  EXPECT_EQ(Selected("status", FilterValue{static_cast<int64_t>(1)}), (std::vector<DocId>{1}));
}

TEST_F(FilterIndexTest, BoolFilterValue) {
  Add(1, {{"active", FilterValue{true}}});
  Add(2, {{"active", FilterValue{false}}});

  EXPECT_EQ(Selected("active", FilterValue{true}), (std::vector<DocId>{1}));
  EXPECT_EQ(Selected("active", FilterValue{false}), (std::vector<DocId>{2}));
}

TEST_F(FilterIndexTest, BitmapIntersection) {
  // Test that bitmaps can be intersected for multi-filter queries
  Add(1, {{"category", FilterValue{std::string("tech")}}, {"status", FilterValue{static_cast<int64_t>(1)}}});
  Add(2, {{"category", FilterValue{std::string("tech")}}, {"status", FilterValue{static_cast<int64_t>(2)}}});
  Add(3, {{"category", FilterValue{std::string("news")}}, {"status", FilterValue{static_cast<int64_t>(1)}}});

  auto tech = mygram::utils::MakeEmptyRoaring();
  auto status1 = mygram::utils::MakeEmptyRoaring();
  ASSERT_NE(tech, nullptr);
  ASSERT_NE(status1, nullptr);
  const auto key_tech = FilterIndex::SerializeFilterValue(FilterValue{std::string("tech")});
  const auto key_status1 = FilterIndex::SerializeFilterValue(FilterValue{static_cast<int64_t>(1)});
  ASSERT_TRUE(index_.OrEqBitmapInto("category", key_tech, tech.get()));
  ASSERT_TRUE(index_.OrEqBitmapInto("status", key_status1, status1.get()));

  roaring_bitmap_and_inplace(tech.get(), status1.get());
  EXPECT_EQ(roaring_bitmap_get_cardinality(tech.get()), 1);
  EXPECT_TRUE(roaring_bitmap_contains(tech.get(), 1));
}

TEST_F(FilterIndexTest, LargeScale) {
  // Add 10K documents with 5 category values
  constexpr uint32_t kDocCount = 10000;
  constexpr int kCategories = 5;
  for (uint32_t i = 1; i <= kDocCount; ++i) {
    std::string cat = "cat_" + std::to_string(i % kCategories);
    Add(i, {{"category", FilterValue{cat}}});
  }

  // Each category should have ~2000 docs
  EXPECT_EQ(Selected("category", FilterValue{std::string("cat_0")}).size(), kDocCount / kCategories);
}

TEST_F(FilterIndexTest, CollectedDocIdsAreASnapshotIndependentOfTheIndex) {
  // The collected ids are taken under the lock, so they stay valid and
  // unchanged after the source data is modified (use-after-free prevention)
  Add(1, {{"category", FilterValue{std::string("tech")}}});
  Add(2, {{"category", FilterValue{std::string("tech")}}});

  const std::vector<DocId> snapshot = Selected("category", FilterValue{std::string("tech")});
  EXPECT_EQ(snapshot, (std::vector<DocId>{1, 2}));

  // Modify the source data: remove both documents
  index_.RemoveDocument(1, {{"category", FilterValue{std::string("tech")}}});
  index_.RemoveDocument(2, {{"category", FilterValue{std::string("tech")}}});

  EXPECT_EQ(snapshot, (std::vector<DocId>{1, 2}));
  EXPECT_FALSE(Indexed("category", FilterValue{std::string("tech")}));
}

TEST_F(FilterIndexTest, OrEqBitmapIntoMergesWithoutReturningAnIntermediateCopy) {
  FilterMap first_filters;
  first_filters["category"] = std::string("news");
  Add(1, first_filters);
  FilterMap second_filters;
  second_filters["category"] = std::string("sports");
  Add(2, second_filters);

  auto destination = mygram::utils::MakeEmptyRoaring();
  ASSERT_NE(destination, nullptr);
  const auto news = FilterIndex::SerializeFilterValue(FilterValue{std::string("news")});
  const auto sports = FilterIndex::SerializeFilterValue(FilterValue{std::string("sports")});
  EXPECT_TRUE(index_.OrEqBitmapInto("category", news, destination.get()));
  EXPECT_TRUE(index_.OrEqBitmapInto("category", sports, destination.get()));
  EXPECT_FALSE(index_.OrEqBitmapInto("category", "missing", destination.get()));
  EXPECT_FALSE(index_.OrEqBitmapInto("category", news, nullptr));

  EXPECT_EQ(roaring_bitmap_get_cardinality(destination.get()), 2U);
  EXPECT_TRUE(roaring_bitmap_contains(destination.get(), 1));
  EXPECT_TRUE(roaring_bitmap_contains(destination.get(), 2));
}

TEST_F(FilterIndexTest, ResolveColumnNameUsesLiveColumnIndexIncludingNullValues) {
  Add(1, {{"createdAt", FilterValue{std::monostate{}}}});

  EXPECT_EQ(index_.ResolveColumnName("createdAt"), "createdAt");
  EXPECT_EQ(index_.ResolveColumnName("CREATEDAT"), "createdAt");
  EXPECT_TRUE(index_.HasColumn("createdAt"));

  index_.RemoveDocument(1, {{"createdAt", FilterValue{std::monostate{}}}});
  EXPECT_EQ(index_.ResolveColumnName("createdat"), std::nullopt);
  EXPECT_FALSE(index_.HasColumn("createdAt"));
}

TEST_F(FilterIndexTest, ResolveColumnNameRejectsAmbiguousCaseOnlyDuplicatesUnlessExact) {
  Add(1, {{"Status", FilterValue{std::string("ready")}}});
  Add(2, {{"STATUS", FilterValue{std::string("pending")}}});

  EXPECT_EQ(index_.ResolveColumnName("Status"), "Status");
  EXPECT_EQ(index_.ResolveColumnName("STATUS"), "STATUS");
  EXPECT_EQ(index_.ResolveColumnName("status"), std::nullopt);
}

TEST_F(FilterIndexTest, ConcurrentReadWriteSafety) {
  // Verify concurrent reads + AddDocument/RemoveDocument does not crash
  constexpr int kIterations = 5000;
  const auto key = FilterIndex::SerializeFilterValue(FilterValue{std::string("tech")});

  // Seed initial data
  for (uint32_t i = 1; i <= 100; ++i) {
    Add(i, {{"category", FilterValue{std::string("tech")}}});
  }

  std::atomic<bool> stop{false};
  std::atomic<int> writes_refused{0};

  // Writer thread: add and remove documents
  std::thread writer([&] {
    for (int i = 0; i < kIterations && !stop; ++i) {
      auto doc_id = static_cast<uint32_t>(1000 + i);
      if (!index_.AddDocument(doc_id, {{"category", FilterValue{std::string("tech")}}})) {
        writes_refused.fetch_add(1, std::memory_order_relaxed);
        continue;
      }
      index_.RemoveDocument(doc_id, {{"category", FilterValue{std::string("tech")}}});
    }
  });

  // Reader thread: collect doc ids and check every snapshot against the
  // invariant that the writer never touches the seeded documents. The ids are
  // materialized under the lock, so a snapshot may include at most the one
  // document the writer currently has in flight.
  std::atomic<int> snapshots_taken{0};
  std::atomic<int> snapshots_missing_seed{0};
  std::atomic<uint64_t> max_cardinality{0};
  std::atomic<uint64_t> min_cardinality{UINT64_MAX};
  std::thread reader([&] {
    std::vector<DocId> doc_ids;
    for (int i = 0; i < kIterations && !stop; ++i) {
      if (!index_.CollectEqDocIds("category", key, doc_ids)) {
        continue;
      }
      snapshots_taken.fetch_add(1, std::memory_order_relaxed);
      const auto cardinality = static_cast<uint64_t>(doc_ids.size());
      uint64_t previous_max = max_cardinality.load(std::memory_order_relaxed);
      while (cardinality > previous_max &&
             !max_cardinality.compare_exchange_weak(previous_max, cardinality, std::memory_order_relaxed)) {
      }
      uint64_t previous_min = min_cardinality.load(std::memory_order_relaxed);
      while (cardinality < previous_min &&
             !min_cardinality.compare_exchange_weak(previous_min, cardinality, std::memory_order_relaxed)) {
      }
      for (uint32_t doc_id = 1; doc_id <= 100; ++doc_id) {
        if (std::find(doc_ids.begin(), doc_ids.end(), doc_id) == doc_ids.end()) {
          snapshots_missing_seed.fetch_add(1, std::memory_order_relaxed);
          break;
        }
      }
    }
  });

  writer.join();
  reader.join();

  EXPECT_EQ(writes_refused.load(), 0) << "the index refused a write it had the memory for";
  EXPECT_GT(snapshots_taken.load(), 0) << "the reader never observed the bitmap";
  EXPECT_EQ(snapshots_missing_seed.load(), 0) << "a snapshot lost a document the writer never touched";
  EXPECT_GE(min_cardinality.load(), 100U) << "a snapshot was torn below the seeded document count";
  EXPECT_LE(max_cardinality.load(), 101U) << "a snapshot held more than the one in-flight writer document";

  // The writer removed every document it added, so the final state is the seed.
  const std::vector<DocId> final_ids = Selected("category", FilterValue{std::string("tech")});
  EXPECT_EQ(final_ids.size(), 100U);
  for (uint32_t doc_id = 1; doc_id <= 100; ++doc_id) {
    EXPECT_NE(std::find(final_ids.begin(), final_ids.end(), doc_id), final_ids.end())
        << "seeded document " << doc_id << " was lost";
  }
}
