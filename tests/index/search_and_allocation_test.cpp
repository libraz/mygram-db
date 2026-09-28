/**
 * @file search_and_allocation_test.cpp
 * @brief An AND over terms of mixed posting strategies allocates by the smallest list.
 *
 * A rare n-gram stays a fixed-width delta list while a common one becomes a
 * Roaring bitmap, so a query mixing the two is ordinary traffic. Neither the
 * standard intersection nor PostingList::Intersect may copy the common list to
 * answer it.
 */

#include <gtest/gtest.h>

#include <cstddef>
#include <string>
#include <utility>
#include <vector>

#include "counting_allocator.h"
#include "index/index.h"
#include "index/posting_list.h"

namespace mygramdb::index {
namespace {

constexpr size_t kCommonDocs = 50000;
constexpr DocId kRareEvery = 5000;  // "qz" appears in every 5000th document

class SearchAndAllocationTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    InstallRoaringAllocationCounter();
    index_ = new Index(/*ngram_size=*/2);
    for (DocId doc_id = 1; doc_id <= kCommonDocs; ++doc_id) {
      index_->AddDocument(doc_id, doc_id % kRareEvery == 0 ? "ab qz" : "ab");
    }
  }

  static void TearDownTestSuite() {
    delete index_;
    index_ = nullptr;
  }

  /// Peak bytes live at once while @p run executes, above what was live before.
  template <typename Fn>
  static size_t PeakTransientBytes(Fn&& run) {
    const size_t live_before = g_live_bytes.load(std::memory_order_relaxed);
    g_peak_bytes.store(live_before, std::memory_order_relaxed);
    run();
    return g_peak_bytes.load(std::memory_order_relaxed) - live_before;
  }

  static Index* index_;
};

Index* SearchAndAllocationTest::index_ = nullptr;

std::vector<DocId> ExpectedRareDocs() {
  std::vector<DocId> expected;
  for (DocId doc_id = kRareEvery; doc_id <= kCommonDocs; doc_id += kRareEvery) {
    expected.push_back(doc_id);
  }
  return expected;
}

TEST_F(SearchAndAllocationTest, MixedStrategyAndNeverCopiesTheLargeList) {
  ASSERT_EQ(index_->PostingSize("ab"), kCommonDocs);
  ASSERT_EQ(index_->PostingSize("qz"), kCommonDocs / kRareEvery);
  const size_t large_list_bytes = kCommonDocs * sizeof(DocId);

  // Both term orders, both through the standard path (limit 0) and the
  // top-N path that chains PostingList::Intersect.
  for (const auto& terms : {std::vector<std::string>{"ab", "qz"}, std::vector<std::string>{"qz", "ab"}}) {
    std::vector<DocId> all;
    std::vector<DocId> top;
    (void)index_->SearchAnd(terms);  // warm up one-time initialization
    const size_t standard = PeakTransientBytes([&] { all = index_->SearchAnd(terms); });
    const size_t top_n = PeakTransientBytes([&] { top = index_->SearchAnd(terms, 3, /*reverse=*/true); });

    EXPECT_EQ(all, ExpectedRareDocs());
    EXPECT_EQ(top, (std::vector<DocId>{50000, 45000, 40000}));
    EXPECT_LT(standard, large_list_bytes / 8) << terms[0] << "," << terms[1];
    EXPECT_LT(top_n, large_list_bytes / 8) << terms[0] << "," << terms[1];
  }
}

TEST(PostingListIntersectTest, MixedStrategyIntersectMatchesAndProbesTheLargerSide) {
  PostingList rare;
  PostingList common(0.01);
  for (DocId doc_id = 1; doc_id <= 10000; ++doc_id) {
    common.Add(doc_id);
  }
  common.Optimize(10000);
  ASSERT_EQ(common.GetStrategy(), PostingStrategy::kRoaringBitmap);
  for (DocId doc_id : {3U, 700U, 9999U, 20000U}) {
    rare.Add(doc_id);
  }
  ASSERT_EQ(rare.GetStrategy(), PostingStrategy::kFixedWidthDelta);

  const std::vector<DocId> expected = {3, 700, 9999};
  for (const auto& [lhs, rhs] : {std::pair<PostingList*, PostingList*>{&rare, &common}, {&common, &rare}}) {
    const size_t live_before = g_live_bytes.load(std::memory_order_relaxed);
    g_peak_bytes.store(live_before, std::memory_order_relaxed);
    auto intersected = lhs->Intersect(*rhs);
    const size_t transient = g_peak_bytes.load(std::memory_order_relaxed) - live_before;
    EXPECT_EQ(intersected->GetAll(), expected);
    EXPECT_LT(transient, 10000 * sizeof(DocId) / 8) << "the Roaring side must be probed, not copied";
  }

  PostingList other_delta;
  for (DocId doc_id : {1U, 700U, 20000U, 30000U}) {
    other_delta.Add(doc_id);
  }
  EXPECT_EQ(rare.Intersect(other_delta)->GetAll(), (std::vector<DocId>{700, 20000}));
}

}  // namespace
}  // namespace mygramdb::index
