/**
 * @file filter_index_allocation_failure_test.cpp
 * @brief What the filter index does when a bitmap allocation fails.
 *
 * A filter answers which documents a value selects, so an allocation failure
 * that goes unreported does not degrade the answer -- it changes it, and the
 * caller cannot tell the difference between "no document has this value" and
 * "the index could not record the document that does".
 *
 * Roaring bitmaps allocate through their own hook rather than operator new,
 * which lets this binary fail exactly the allocations the filter index makes
 * while leaving every other allocation in the process alone. The switch lives
 * here, not in the production code.
 */

#include <gtest/gtest.h>
#include <roaring/memory.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <new>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "storage/document_store.h"
#include "storage/filter_index.h"

namespace {

thread_local bool g_fail_cpp_allocations = false;

void* AllocateCppMemory(size_t size, size_t alignment) noexcept {
  if (g_fail_cpp_allocations) {
    return nullptr;
  }
  if (alignment <= alignof(std::max_align_t)) {
    return std::malloc(size == 0 ? 1 : size);
  }
  void* ptr = nullptr;
  return posix_memalign(&ptr, alignment, size == 0 ? 1 : size) == 0 ? ptr : nullptr;
}

}  // namespace

void* operator new(size_t size) {
  if (void* ptr = AllocateCppMemory(size, alignof(std::max_align_t)); ptr != nullptr) {
    return ptr;
  }
  throw std::bad_alloc();
}

void* operator new[](size_t size) {
  return ::operator new(size);
}

void* operator new(size_t size, const std::nothrow_t&) noexcept {
  return AllocateCppMemory(size, alignof(std::max_align_t));
}

void* operator new[](size_t size, const std::nothrow_t& tag) noexcept {
  return ::operator new(size, tag);
}

void* operator new(size_t size, std::align_val_t alignment) {
  if (void* ptr = AllocateCppMemory(size, static_cast<size_t>(alignment)); ptr != nullptr) {
    return ptr;
  }
  throw std::bad_alloc();
}

void* operator new[](size_t size, std::align_val_t alignment) {
  return ::operator new(size, alignment);
}

void* operator new(size_t size, std::align_val_t alignment, const std::nothrow_t&) noexcept {
  return AllocateCppMemory(size, static_cast<size_t>(alignment));
}

void* operator new[](size_t size, std::align_val_t alignment, const std::nothrow_t& tag) noexcept {
  return ::operator new(size, alignment, tag);
}

void operator delete(void* ptr) noexcept {
  std::free(ptr);
}
void operator delete[](void* ptr) noexcept {
  std::free(ptr);
}
void operator delete(void* ptr, size_t) noexcept {
  std::free(ptr);
}
void operator delete[](void* ptr, size_t) noexcept {
  std::free(ptr);
}
void operator delete(void* ptr, const std::nothrow_t&) noexcept {
  std::free(ptr);
}
void operator delete[](void* ptr, const std::nothrow_t&) noexcept {
  std::free(ptr);
}
void operator delete(void* ptr, std::align_val_t) noexcept {
  std::free(ptr);
}
void operator delete[](void* ptr, std::align_val_t) noexcept {
  std::free(ptr);
}
void operator delete(void* ptr, size_t, std::align_val_t) noexcept {
  std::free(ptr);
}
void operator delete[](void* ptr, size_t, std::align_val_t) noexcept {
  std::free(ptr);
}
void operator delete(void* ptr, std::align_val_t, const std::nothrow_t&) noexcept {
  std::free(ptr);
}
void operator delete[](void* ptr, std::align_val_t, const std::nothrow_t&) noexcept {
  std::free(ptr);
}

namespace mygramdb::storage {
namespace {

/// Roaring allocations still allowed before the failures begin.
/// A negative value disables injection.
std::atomic<int64_t> g_allowed_allocations{-1};

/// Allocations still to be failed once the allowance runs out.
/// A negative value fails every remaining allocation.
std::atomic<int64_t> g_remaining_failures{-1};

bool AllocationShouldFail() {
  int64_t allowed = g_allowed_allocations.load(std::memory_order_relaxed);
  if (allowed < 0) {
    return false;
  }
  if (allowed > 0) {
    g_allowed_allocations.fetch_sub(1, std::memory_order_relaxed);
    return false;
  }
  int64_t remaining = g_remaining_failures.load(std::memory_order_relaxed);
  if (remaining == 0) {
    return false;
  }
  if (remaining > 0) {
    g_remaining_failures.fetch_sub(1, std::memory_order_relaxed);
  }
  return true;
}

void* HookedMalloc(size_t size) {
  return AllocationShouldFail() ? nullptr : std::malloc(size);
}

void* HookedCalloc(size_t count, size_t size) {
  return AllocationShouldFail() ? nullptr : std::calloc(count, size);
}

void* HookedRealloc(void* ptr, size_t size) {
  return AllocationShouldFail() ? nullptr : std::realloc(ptr, size);
}

void HookedFree(void* ptr) {
  std::free(ptr);
}

void* HookedAlignedMalloc(size_t alignment, size_t size) {
  if (AllocationShouldFail()) {
    return nullptr;
  }
  void* ptr = nullptr;
  if (posix_memalign(&ptr, alignment, size) != 0) {
    return nullptr;
  }
  return ptr;
}

void HookedAlignedFree(void* ptr) {
  std::free(ptr);
}

void InstallRoaringAllocationHook() {
  roaring_memory_t hooks{};
  hooks.malloc = HookedMalloc;
  hooks.realloc = HookedRealloc;
  hooks.calloc = HookedCalloc;
  hooks.free = HookedFree;
  hooks.aligned_malloc = HookedAlignedMalloc;
  hooks.aligned_free = HookedAlignedFree;
  roaring_init_memory_hook(hooks);
}

/// Lets @p allowed roaring allocations through, then fails the next
/// @p failures of them (a negative count fails all of them) until the guard
/// goes out of scope.
class FailingRoaringAllocations {
 public:
  explicit FailingRoaringAllocations(int64_t allowed, int64_t failures = -1) {
    g_remaining_failures.store(failures, std::memory_order_relaxed);
    g_allowed_allocations.store(allowed, std::memory_order_relaxed);
  }
  ~FailingRoaringAllocations() { g_allowed_allocations.store(-1, std::memory_order_relaxed); }

  FailingRoaringAllocations(const FailingRoaringAllocations&) = delete;
  FailingRoaringAllocations& operator=(const FailingRoaringAllocations&) = delete;
  FailingRoaringAllocations(FailingRoaringAllocations&&) = delete;
  FailingRoaringAllocations& operator=(FailingRoaringAllocations&&) = delete;
};

class FilterIndexAllocationFailureTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() { InstallRoaringAllocationHook(); }

  /// Doc ids selected by (column, value); empty when the value is not indexed.
  static std::vector<DocId> Selected(const FilterIndex& index, std::string_view column, const FilterValue& value) {
    std::vector<DocId> doc_ids;
    index.CollectEqDocIds(column, FilterIndex::SerializeFilterValue(value), doc_ids);
    return doc_ids;
  }

  /// Whether (column, value) has an entry at all, regardless of what it selects.
  static bool Indexed(const FilterIndex& index, std::string_view column, const FilterValue& value) {
    std::vector<DocId> doc_ids;
    return index.CollectEqDocIds(column, FilterIndex::SerializeFilterValue(value), doc_ids);
  }
};

/**
 * @brief A document the index could not record must not read as "not present".
 *
 * This is the whole contract: whatever the index cannot do, the caller has to
 * be able to see. Dropping the document and returning normally makes the index
 * disagree with the data with nothing to show for it.
 */
TEST_F(FilterIndexAllocationFailureTest, AddDocumentReportsWhatItCouldNotIndex) {
  FilterIndex index;
  const FilterMap filters{{"category", FilterValue{std::string("tech")}}};

  Expected<void, Error> added;
  {
    FailingRoaringAllocations fail_next(0);
    added = index.AddDocument(1, filters);
  }

  ASSERT_FALSE(added.has_value()) << "the document was dropped from the index without the caller being told";
  EXPECT_EQ(added.error().code(), mygram::utils::ErrorCode::kStorageWriteError);
  EXPECT_FALSE(Indexed(index, "category", FilterValue{std::string("tech")}));
}

/**
 * @brief A document is indexed under all of its values or under none of them.
 *
 * Failing on one column while another one stands would leave the document
 * selectable by one filter and invisible to another, which is a state no caller
 * can reason about. Rolling back must also leave every other document alone.
 *
 * NULL values are counted as live columns but allocate nothing, so the one
 * value that does allocate is where the memory runs out, wherever the map
 * happens to visit it.
 */
TEST_F(FilterIndexAllocationFailureTest, AddDocumentRollsBackTheColumnsItAlreadyApplied) {
  FilterIndex index;
  const FilterMap filters{{"created_at", FilterValue{std::monostate{}}},
                          {"deleted_at", FilterValue{std::monostate{}}},
                          {"region", FilterValue{std::string("east")}}};

  Expected<void, Error> added;
  {
    FailingRoaringAllocations fail_new_values(0);
    added = index.AddDocument(1, filters);
  }

  ASSERT_FALSE(added.has_value());
  EXPECT_FALSE(Indexed(index, "region", FilterValue{std::string("east")}));
  EXPECT_FALSE(index.HasColumn("created_at")) << "a column the document was rolled out of is still counted";
  EXPECT_FALSE(index.HasColumn("deleted_at"));
  EXPECT_FALSE(index.HasColumn("region"));
  EXPECT_EQ(index.MemoryUsage(), 0U);
}

/**
 * @brief An update that cannot be applied leaves the previous values in place.
 *
 * The document keeps answering the filter it answered before. What must not
 * happen is the old values being taken away in exchange for nothing.
 */
TEST_F(FilterIndexAllocationFailureTest, UpdateDocumentKeepsThePreviousValuesWhenTheNewOnesFail) {
  FilterIndex index;
  const FilterMap old_filters{{"category", FilterValue{std::string("tech")}}};
  const FilterMap new_filters{{"category", FilterValue{std::string("news")}}};
  ASSERT_TRUE(index.AddDocument(1, old_filters));

  Expected<void, Error> updated;
  {
    FailingRoaringAllocations fail_all(0);
    updated = index.UpdateDocument(1, old_filters, new_filters);
  }

  ASSERT_FALSE(updated.has_value());
  EXPECT_FALSE(Indexed(index, "category", FilterValue{std::string("news")}));
  EXPECT_EQ(Selected(index, "category", FilterValue{std::string("tech")}), std::vector<DocId>{1})
      << "the document lost the values it had in exchange for values that were never applied";
}

TEST_F(FilterIndexAllocationFailureTest, UpdateIsAllOldOrAllNewAcrossAllocationFailurePoints) {
  const FilterMap old_filters{{"a", FilterValue{std::string("old-a")}},
                              {"b", FilterValue{std::string("old-b")}},
                              {"removed_null", FilterValue{std::monostate{}}},
                              {"stable", FilterValue{std::string("keep")}}};
  const FilterMap new_filters{{"a", FilterValue{std::string("new-a")}},
                              {"c", FilterValue{std::string("new-c")}},
                              {"added_null", FilterValue{std::monostate{}}},
                              {"stable", FilterValue{std::string("keep")}}};
  const FilterValue old_a{std::string("old-a")};
  const FilterValue new_a{std::string("new-a")};
  const FilterValue old_b{std::string("old-b")};
  const FilterValue new_c{std::string("new-c")};
  const FilterValue stable{std::string("keep")};

  bool observed_failure = false;
  bool observed_success = false;
  // Each new bitmap needs a bitmap object, its container index, and one array container.
  for (int64_t allowance = 0; allowance <= 8; ++allowance) {
    FilterIndex index;
    ASSERT_TRUE(index.AddDocument(1, old_filters));
    const size_t old_memory_usage = index.MemoryUsage();

    Expected<void, Error> updated;
    {
      FailingRoaringAllocations fail_after(allowance);
      updated = index.UpdateDocument(1, old_filters, new_filters);
    }

    if (updated) {
      observed_success = true;
      EXPECT_FALSE(Indexed(index, "a", old_a));
      EXPECT_EQ(Selected(index, "a", new_a), (std::vector<DocId>{1}));
      EXPECT_FALSE(index.HasColumn("b"));
      EXPECT_TRUE(index.HasColumn("c"));
      EXPECT_FALSE(index.HasColumn("removed_null"));
      EXPECT_TRUE(index.HasColumn("added_null"));
    } else {
      observed_failure = true;
      EXPECT_EQ(Selected(index, "a", old_a), (std::vector<DocId>{1}));
      EXPECT_FALSE(Indexed(index, "a", new_a));
      EXPECT_EQ(Selected(index, "b", old_b), (std::vector<DocId>{1}));
      EXPECT_FALSE(Indexed(index, "c", new_c));
      EXPECT_TRUE(index.HasColumn("removed_null"));
      EXPECT_FALSE(index.HasColumn("added_null"));
      EXPECT_EQ(index.MemoryUsage(), old_memory_usage);
    }
    EXPECT_EQ(Selected(index, "stable", stable), (std::vector<DocId>{1}));
    EXPECT_EQ(index.GetColumnValueCounts("a").size(), 1U);
    EXPECT_EQ(index.GetColumnValueCounts("stable").size(), 1U);
  }
  EXPECT_TRUE(observed_failure);
  EXPECT_TRUE(observed_success);
}

TEST_F(FilterIndexAllocationFailureTest, ExistingBitmapHighKeyGrowthIsPreparedBeforeRemoval) {
  const FilterMap old_target{{"category", FilterValue{std::string("old")}}};
  const FilterMap target{{"category", FilterValue{std::string("target")}}};

  for (int64_t allowance = 0; allowance <= 3; ++allowance) {
    FilterIndex index;
    ASSERT_TRUE(index.AddDocument(1, target));
    ASSERT_TRUE(index.AddDocument(70000, old_target));

    Expected<void, Error> updated;
    {
      FailingRoaringAllocations fail_after(allowance);
      updated = index.UpdateDocument(70000, old_target, target);
    }

    if (allowance < 3) {
      ASSERT_FALSE(updated);
      EXPECT_EQ(Selected(index, "category", FilterValue{std::string("old")}), (std::vector<DocId>{70000}));
      EXPECT_EQ(Selected(index, "category", FilterValue{std::string("target")}), (std::vector<DocId>{1}));
    } else {
      ASSERT_TRUE(updated);
      EXPECT_FALSE(Indexed(index, "category", FilterValue{std::string("old")}));
      EXPECT_EQ(Selected(index, "category", FilterValue{std::string("target")}), (std::vector<DocId>{1, 70000}));
    }
  }
}

TEST_F(FilterIndexAllocationFailureTest, ArrayGrowthFailureLeavesOldMembershipUntouched) {
  const FilterMap old_filters{{"category", FilterValue{std::string("old")}}};
  const FilterMap target{{"category", FilterValue{std::string("target")}}};

  for (int64_t allowance = 0; allowance <= 2; ++allowance) {
    FilterIndex index;
    for (DocId doc_id = 1; doc_id <= 4; ++doc_id) {
      ASSERT_TRUE(index.AddDocument(doc_id, target));
    }
    ASSERT_TRUE(index.AddDocument(5, old_filters));

    Expected<void, Error> updated;
    {
      FailingRoaringAllocations fail_after(allowance);
      updated = index.UpdateDocument(5, old_filters, target);
    }

    if (allowance < 2) {
      ASSERT_FALSE(updated);
      EXPECT_EQ(Selected(index, "category", FilterValue{std::string("old")}), (std::vector<DocId>{5}));
      EXPECT_EQ(Selected(index, "category", FilterValue{std::string("target")}), (std::vector<DocId>{1, 2, 3, 4}));
    } else {
      ASSERT_TRUE(updated);
      EXPECT_FALSE(Indexed(index, "category", FilterValue{std::string("old")}));
      EXPECT_EQ(Selected(index, "category", FilterValue{std::string("target")}), (std::vector<DocId>{1, 2, 3, 4, 5}));
    }
  }
}

TEST_F(FilterIndexAllocationFailureTest, SparseAddsStayInArrayContainerUntilCapacityIsExceeded) {
  const FilterMap filters{{"category", FilterValue{std::string("sparse")}}};
  FilterIndex index;
  ASSERT_TRUE(index.AddDocument(1, filters));
  ASSERT_TRUE(index.AddDocument(2, filters));
  const size_t memory_after_array_growth = index.MemoryUsage();

  // The first growth gives the array spare capacity; filling that capacity must not promote it.
  ASSERT_TRUE(index.AddDocument(3, filters));
  EXPECT_LT(index.MemoryUsage(), memory_after_array_growth + 4096U);
  for (DocId doc_id = 4; doc_id <= 16; ++doc_id) {
    ASSERT_TRUE(index.AddDocument(doc_id, filters));
  }

  EXPECT_EQ(Selected(index, "category", FilterValue{std::string("sparse")}).size(), 16U);
}

TEST_F(FilterIndexAllocationFailureTest, ArrayToBitsetPromotionFailureIsAtomic) {
  const FilterMap old_filters{{"category", FilterValue{std::string("old")}}};
  const FilterMap target{{"category", FilterValue{std::string("target")}}};

  for (int64_t allowance = 0; allowance <= 2; ++allowance) {
    FilterIndex index;
    for (DocId doc_id = 1; doc_id <= 4096; ++doc_id) {
      ASSERT_TRUE(index.AddDocument(doc_id, target));
    }
    ASSERT_TRUE(index.AddDocument(4097, old_filters));

    Expected<void, Error> updated;
    {
      FailingRoaringAllocations fail_after(allowance);
      updated = index.UpdateDocument(4097, old_filters, target);
    }

    if (allowance < 2) {
      ASSERT_FALSE(updated);
      EXPECT_EQ(Selected(index, "category", FilterValue{std::string("old")}), (std::vector<DocId>{4097}));
      EXPECT_EQ(Selected(index, "category", FilterValue{std::string("target")}).size(), 4096U);
    } else {
      ASSERT_TRUE(updated);
      EXPECT_FALSE(Indexed(index, "category", FilterValue{std::string("old")}));
      EXPECT_EQ(Selected(index, "category", FilterValue{std::string("target")}).size(), 4097U);
    }
  }
}

TEST_F(FilterIndexAllocationFailureTest, RemovalWorksWhenEveryRoaringAllocationFails) {
  const FilterMap filters{{"category", FilterValue{std::string("target")}}};
  FilterIndex index;
  for (DocId doc_id = 1; doc_id <= 4097; ++doc_id) {
    ASSERT_TRUE(index.AddDocument(doc_id, filters));
  }

  {
    FailingRoaringAllocations fail_all(0);
    index.RemoveDocument(1, filters);
    EXPECT_EQ(index.GetColumnValueCounts("category").front().second, 4096U);
    for (DocId doc_id = 2; doc_id <= 4097; ++doc_id) {
      index.RemoveDocument(doc_id, filters);
    }
  }

  EXPECT_FALSE(index.HasColumn("category"));
  EXPECT_FALSE(Indexed(index, "category", FilterValue{std::string("target")}));
}

TEST_F(FilterIndexAllocationFailureTest, RemovalNeedsNoCppAllocationAndSurvivesRoaringFailure) {
  const FilterMap filters{
      {"nullable", std::monostate{}},
      {"boolean", true},
      {"int8", int8_t{-7}},
      {"uint8", uint8_t{250}},
      {"int16", int16_t{-1024}},
      {"uint16", uint16_t{4096}},
      {"int32", int32_t{-100000}},
      {"uint32", uint32_t{4000000000U}},
      {"int64", int64_t{-1234567890}},
      {"uint64", uint64_t{12345678901ULL}},
      {"time", TimeValue{123456}},
      {"text", std::string(128, 'v')},
      {"double", 3.25},
      {"positive_zero", 0.0},
      {"negative_zero", -0.0},
      {"nan", std::numeric_limits<double>::quiet_NaN()},
  };

  EXPECT_EXIT(
      {
        FilterIndex index;
        if (!index.AddDocument(1, filters)) {
          std::_Exit(20);
        }

        {
          FailingRoaringAllocations fail_roaring(0);
          g_fail_cpp_allocations = true;
          index.RemoveDocument(1, filters);
          g_fail_cpp_allocations = false;
        }

        for (const auto& [column, value] : filters) {
          if (index.HasColumn(column)) {
            std::_Exit(21);
          }
          if (!std::holds_alternative<std::monostate>(value)) {
            const std::string serialized = FilterIndex::SerializeFilterValue(value);
            std::vector<DocId> selected;
            if (index.CollectEqDocIds(column, serialized, selected)) {
              std::_Exit(22);
            }
          }
        }
        std::_Exit(0);
      },
      ::testing::ExitedWithCode(0), "");
}

TEST_F(FilterIndexAllocationFailureTest, RemovalMatchesSerializedSignedZeroAndNaNKeys) {
  FilterIndex index;
  const FilterMap positive_zero{{"measure", FilterValue{0.0}}};
  const FilterMap negative_zero{{"measure", FilterValue{-0.0}}};
  const double nan = std::numeric_limits<double>::quiet_NaN();
  const FilterMap nan_filter{{"measure", FilterValue{nan}}};
  ASSERT_TRUE(index.AddDocument(1, positive_zero));
  ASSERT_TRUE(index.AddDocument(2, negative_zero));
  ASSERT_TRUE(index.AddDocument(3, nan_filter));

  index.RemoveDocument(2, negative_zero);
  index.RemoveDocument(3, nan_filter);

  EXPECT_EQ(Selected(index, "measure", FilterValue{0.0}), (std::vector<DocId>{1}));
  EXPECT_FALSE(Indexed(index, "measure", FilterValue{-0.0}));
  EXPECT_FALSE(Indexed(index, "measure", FilterValue{nan}));
}

TEST_F(FilterIndexAllocationFailureTest, SameSerializedValueIsANoOpAndSignedZeroRemainsDistinct) {
  const double nan = std::numeric_limits<double>::quiet_NaN();
  const FilterMap nan_filters{{"measure", FilterValue{nan}}};
  FilterIndex index;
  ASSERT_TRUE(index.AddDocument(1, nan_filters));
  {
    FailingRoaringAllocations fail_all(0);
    EXPECT_TRUE(index.UpdateDocument(1, nan_filters, nan_filters));
  }

  const FilterMap positive_zero{{"measure", FilterValue{0.0}}};
  const FilterMap negative_zero{{"measure", FilterValue{-0.0}}};
  ASSERT_TRUE(index.AddDocument(2, positive_zero));
  Expected<void, Error> updated;
  {
    FailingRoaringAllocations fail_all(0);
    updated = index.UpdateDocument(2, positive_zero, negative_zero);
  }
  ASSERT_FALSE(updated);
  EXPECT_EQ(Selected(index, "measure", FilterValue{0.0}), (std::vector<DocId>{2}));
  EXPECT_FALSE(Indexed(index, "measure", FilterValue{-0.0}));

  ASSERT_TRUE(index.UpdateDocument(2, positive_zero, negative_zero));
  EXPECT_FALSE(Indexed(index, "measure", FilterValue{0.0}));
  EXPECT_EQ(Selected(index, "measure", FilterValue{-0.0}), (std::vector<DocId>{2}));
}

/**
 * @brief The store and its filter index must not disagree about a document.
 *
 * A successful AddDocument means the document is searchable. If the filter
 * bitmap behind it could not be built, the store must not claim the document
 * is there.
 */
TEST_F(FilterIndexAllocationFailureTest, StoreDoesNotHoldADocumentItsFilterIndexIsMissing) {
  DocumentStore store;
  const FilterMap filters{{"category", FilterValue{std::string("tech")}}};

  Expected<DocId, Error> added = MakeUnexpected(MakeError(mygram::utils::ErrorCode::kUnknown, "unset", ""));
  {
    FailingRoaringAllocations fail_next(0);
    added = store.AddDocument("pk-1", filters, "tech", "tech");
  }

  ASSERT_FALSE(added.has_value()) << "the store reported the document as added while its filter selects nothing";
  EXPECT_FALSE(store.GetDocId("pk-1").has_value()) << "the store kept a document whose filters it failed to index";
  EXPECT_TRUE(store.FilterByValue("category", FilterValue{std::string("tech")}).empty());
}

TEST_F(FilterIndexAllocationFailureTest, StoreUpdateFailureRetainsFiltersPrimaryKeyAndText) {
  DocumentStore store;
  const FilterMap old_filters{{"category", FilterValue{std::string("old")}}};
  const FilterMap new_filters{{"category", FilterValue{std::string("new")}}};
  const auto added = store.AddDocument("pk-1", old_filters, "old normalized", "old original");
  ASSERT_TRUE(added);

  Expected<bool, Error> updated = false;
  {
    FailingRoaringAllocations fail_all(0);
    updated = store.UpdateDocument(*added, new_filters, "new normalized", "new original");
  }

  ASSERT_FALSE(updated);
  const auto document = store.GetDocument(*added);
  ASSERT_TRUE(document.has_value());
  EXPECT_EQ(document->filters, old_filters);
  EXPECT_EQ(store.GetDocId("pk-1"), *added);
  EXPECT_EQ(store.GetPrimaryKey(*added), std::optional<std::string>("pk-1"));
  EXPECT_EQ(store.GetNormalizedText(*added), std::optional<std::string>("old normalized"));
  EXPECT_EQ(store.GetOriginalText(*added), std::optional<std::string>("old original"));
  EXPECT_EQ(store.FilterByValue("category", FilterValue{std::string("old")}), (std::vector<DocId>{*added}));
  EXPECT_TRUE(store.FilterByValue("category", FilterValue{std::string("new")}).empty());
}

/**
 * @brief A batch stops at the document it cannot index rather than skipping it.
 *
 * The documents the batch did take stay indexed, and the one it could not take
 * is in neither the store nor the index.
 */
TEST_F(FilterIndexAllocationFailureTest, BatchAddStopsAtTheDocumentItCannotIndex) {
  DocumentStore store;
  std::vector<DocumentStore::DocumentItem> documents;
  // A NULL value needs no bitmap, so pk-1 goes in and pk-2 is the one that
  // runs out of memory.
  documents.push_back({"pk-1", {{"category", FilterValue{std::monostate{}}}}, "one", "one"});
  documents.push_back({"pk-2", {{"category", FilterValue{std::string("news")}}}, "two", "two"});

  Expected<std::vector<DocId>, Error> added =
      MakeUnexpected(MakeError(mygram::utils::ErrorCode::kUnknown, "unset", ""));
  {
    FailingRoaringAllocations fail_new_values(0);
    added = store.AddDocumentBatch(documents);
  }

  ASSERT_FALSE(added.has_value());
  EXPECT_FALSE(store.GetDocId("pk-2").has_value());
  EXPECT_TRUE(store.FilterByValue("category", FilterValue{std::string("news")}).empty());

  const auto pk1 = store.GetDocId("pk-1");
  ASSERT_TRUE(pk1.has_value()) << "a document the batch reported no problem with is gone";
  EXPECT_EQ(store.FilterByValue("category", FilterValue{std::monostate{}}), (std::vector<DocId>{*pk1}));
}

}  // namespace
}  // namespace mygramdb::storage
