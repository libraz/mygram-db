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
#include <cstdint>
#include <cstdlib>
#include <string>
#include <string_view>
#include <vector>

#include "storage/document_store.h"
#include "storage/filter_index.h"

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
    FailingRoaringAllocations fail_once(0, 1);
    updated = index.UpdateDocument(1, old_filters, new_filters);
  }

  ASSERT_FALSE(updated.has_value());
  EXPECT_FALSE(Indexed(index, "category", FilterValue{std::string("news")}));
  EXPECT_EQ(Selected(index, "category", FilterValue{std::string("tech")}), std::vector<DocId>{1})
      << "the document lost the values it had in exchange for values that were never applied";
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
