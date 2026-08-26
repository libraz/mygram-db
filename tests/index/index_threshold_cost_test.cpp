/**
 * @file index_threshold_cost_test.cpp
 * @brief How much a threshold search allocates relative to what it returns.
 *
 * A fuzzy term is answered by counting how many of its n-grams a document
 * carries, which means visiting several posting lists at once. Those lists are
 * as long as the corpus makes them, so the transient memory a single request
 * takes is a property worth pinning: it is what a concurrent burst of fuzzy
 * queries multiplies.
 *
 * Allocation volume is counted directly by routing operator new through a
 * counter, which is deterministic and machine-independent, and reported against
 * both the answer size and the size of the largest list involved.
 */

#include <gtest/gtest.h>
#include <roaring/memory.h>

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <new>
#include <string>
#include <vector>

#include "index/index.h"
#include "utils/string_utils.h"

namespace {

// Every allocation carries a header holding its size and, for the aligned
// forms, the base pointer, so a release can subtract exactly what it returns.
// Tracking live bytes rather than cumulative bytes is what distinguishes
// "materializes every list at once" from "visits every list in turn".
constexpr size_t kHeaderBytes = 32;

std::atomic<size_t> g_live_bytes{0};
std::atomic<size_t> g_peak_bytes{0};

void NoteAllocation(size_t bytes) {
  const size_t live = g_live_bytes.fetch_add(bytes, std::memory_order_relaxed) + bytes;
  size_t peak = g_peak_bytes.load(std::memory_order_relaxed);
  while (live > peak && !g_peak_bytes.compare_exchange_weak(peak, live, std::memory_order_relaxed)) {
  }
}

void NoteRelease(size_t bytes) {
  g_live_bytes.fetch_sub(bytes, std::memory_order_relaxed);
}

void* TrackedAllocate(size_t size) {
  auto* base = static_cast<unsigned char*>(std::malloc(size + kHeaderBytes));
  if (base == nullptr) {
    return nullptr;
  }
  auto* user = base + kHeaderBytes;
  std::memcpy(user - sizeof(size_t), &size, sizeof(size_t));
  NoteAllocation(size);
  return user;
}

void TrackedRelease(void* ptr) {
  if (ptr == nullptr) {
    return;
  }
  auto* user = static_cast<unsigned char*>(ptr);
  size_t size = 0;
  std::memcpy(&size, user - sizeof(size_t), sizeof(size_t));
  NoteRelease(size);
  std::free(user - kHeaderBytes);
}

size_t TrackedSizeOf(void* ptr) {
  size_t size = 0;
  std::memcpy(&size, static_cast<unsigned char*>(ptr) - sizeof(size_t), sizeof(size_t));
  return size;
}

}  // namespace

void* operator new(size_t size) {
  void* ptr = TrackedAllocate(size == 0 ? 1 : size);
  if (ptr == nullptr) {
    throw std::bad_alloc();
  }
  return ptr;
}

void* operator new[](size_t size) {
  return ::operator new(size);
}

void* operator new(size_t size, const std::nothrow_t&) noexcept {
  return TrackedAllocate(size == 0 ? 1 : size);
}

void* operator new[](size_t size, const std::nothrow_t& tag) noexcept {
  return ::operator new(size, tag);
}

void operator delete(void* ptr) noexcept {
  TrackedRelease(ptr);
}
void operator delete[](void* ptr) noexcept {
  TrackedRelease(ptr);
}
void operator delete(void* ptr, size_t) noexcept {
  TrackedRelease(ptr);
}
void operator delete[](void* ptr, size_t) noexcept {
  TrackedRelease(ptr);
}
void operator delete(void* ptr, const std::nothrow_t&) noexcept {
  TrackedRelease(ptr);
}
void operator delete[](void* ptr, const std::nothrow_t&) noexcept {
  TrackedRelease(ptr);
}

namespace {

// Roaring bitmaps allocate through their own hook rather than operator new, so
// both allocators must be accounted for or a change of representation would
// look like a saving.
void* CountedMalloc(size_t size) {
  return TrackedAllocate(size);
}

void* CountedCalloc(size_t count, size_t size) {
  void* ptr = TrackedAllocate(count * size);
  if (ptr != nullptr) {
    std::memset(ptr, 0, count * size);
  }
  return ptr;
}

void* CountedRealloc(void* ptr, size_t size) {
  if (ptr == nullptr) {
    return TrackedAllocate(size);
  }
  const size_t previous = TrackedSizeOf(ptr);
  void* fresh = TrackedAllocate(size);
  if (fresh == nullptr) {
    return nullptr;
  }
  std::memcpy(fresh, ptr, std::min(previous, size));
  TrackedRelease(ptr);
  return fresh;
}

void CountedFree(void* ptr) {
  TrackedRelease(ptr);
}

void* CountedAlignedMalloc(size_t alignment, size_t size) {
  const size_t padding = std::max(alignment, kHeaderBytes);
  void* base = nullptr;
  if (posix_memalign(&base, alignment, size + padding) != 0) {
    return nullptr;
  }
  auto* user = static_cast<unsigned char*>(base) + padding;
  std::memcpy(user - sizeof(size_t), &size, sizeof(size_t));
  std::memcpy(user - (2 * sizeof(size_t)), &base, sizeof(void*));
  NoteAllocation(size);
  return user;
}

void CountedAlignedFree(void* ptr) {
  if (ptr == nullptr) {
    return;
  }
  auto* user = static_cast<unsigned char*>(ptr);
  size_t size = 0;
  void* base = nullptr;
  std::memcpy(&size, user - sizeof(size_t), sizeof(size_t));
  std::memcpy(&base, user - (2 * sizeof(size_t)), sizeof(void*));
  NoteRelease(size);
  std::free(base);
}

void InstallRoaringAllocationCounter() {
  roaring_memory_t hooks{};
  hooks.malloc = CountedMalloc;
  hooks.realloc = CountedRealloc;
  hooks.calloc = CountedCalloc;
  hooks.free = CountedFree;
  hooks.aligned_malloc = CountedAlignedMalloc;
  hooks.aligned_free = CountedAlignedFree;
  roaring_init_memory_hook(hooks);
}

}  // namespace

namespace mygramdb::index {
namespace {

constexpr size_t kCorpusSize = 200000;
constexpr const char* kFuzzyTerm = "tokyo";
constexpr size_t kThreshold = 2;

}  // namespace

class IndexThresholdCostTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    InstallRoaringAllocationCounter();
    index_ = new Index(
        /*ngram_size=*/2, /*kanji_ngram_size=*/2,
        /*roaring_threshold=*/0.1, /*cross_boundary_ngrams=*/false,
        /*normalize_nfkc=*/true, /*normalize_width=*/"half", /*normalize_lower=*/true);

    // Every document carries the bigrams of the fuzzy term, so each posting
    // list involved is corpus-sized -- the shape a common term produces.
    for (size_t i = 0; i < kCorpusSize; ++i) {
      index_->AddDocument(static_cast<DocId>(i + 1), "tokyo record " + std::to_string(i));
    }
    ngrams_ = mygram::utils::GenerateNgrams(kFuzzyTerm, 2);
  }

  static void TearDownTestSuite() {
    delete index_;
    index_ = nullptr;
  }

  static Index* index_;
  static std::vector<std::string> ngrams_;
};

Index* IndexThresholdCostTest::index_ = nullptr;
std::vector<std::string> IndexThresholdCostTest::ngrams_ = {};

/**
 * @brief One threshold search must not allocate the whole candidate set at once.
 *
 * The answer is bounded by the corpus, and so is the longest single posting
 * list. What must not happen is every candidate list being resident
 * simultaneously, because that multiplies the request's footprint by the number
 * of n-grams the term expands to.
 */
TEST_F(IndexThresholdCostTest, ThresholdSearchDoesNotAllocateEveryPostingListAtOnce) {
  ASSERT_FALSE(ngrams_.empty());

  size_t total_postings = 0;
  size_t largest_list = 0;
  for (const auto& ngram : ngrams_) {
    const size_t size = static_cast<size_t>(index_->PostingSize(ngram));
    total_postings += size;
    largest_list = std::max(largest_list, size);
  }

  // Warm up so one-time lazy initialization is not attributed to the search.
  auto warm = index_->SearchByThreshold(ngrams_, kThreshold);
  ASSERT_FALSE(warm.empty());

  const size_t live_before = g_live_bytes.load(std::memory_order_relaxed);
  g_peak_bytes.store(live_before, std::memory_order_relaxed);
  auto results = index_->SearchByThreshold(ngrams_, kThreshold);
  const size_t allocated = g_peak_bytes.load(std::memory_order_relaxed) - live_before;

  const size_t result_bytes = results.size() * sizeof(DocId);
  std::cout << "\nThreshold search over " << kCorpusSize << " documents (" << ngrams_.size() << " n-grams, threshold "
            << kThreshold << ")\n";
  std::cout << "  " << std::left << std::setw(28) << "postings across n-grams" << std::right << std::setw(12)
            << total_postings << "\n";
  std::cout << "  " << std::left << std::setw(28) << "largest single list" << std::right << std::setw(12)
            << largest_list << "\n";
  std::cout << "  " << std::left << std::setw(28) << "documents returned" << std::right << std::setw(12)
            << results.size() << "\n";
  std::cout << "  " << std::left << std::setw(28) << "peak transient bytes" << std::right << std::setw(12) << allocated
            << "\n";
  std::cout << "  " << std::left << std::setw(28) << "x result size" << std::right << std::setw(12) << std::fixed
            << std::setprecision(2) << (static_cast<double>(allocated) / static_cast<double>(result_bytes)) << "\n";

  EXPECT_EQ(results.size(), kCorpusSize);

  // Room for the answer, one list at a time and the bookkeeping around them,
  // but not for every candidate list living at once.
  const size_t budget = result_bytes + (largest_list * sizeof(DocId) * 3);
  EXPECT_LT(allocated, budget);
}

}  // namespace mygramdb::index
