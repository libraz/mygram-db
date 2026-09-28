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

#include <algorithm>
#include <cstddef>
#include <iomanip>
#include <iostream>
#include <string>
#include <vector>

#include "counting_allocator.h"
#include "index/index.h"
#include "utils/string_utils.h"

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
