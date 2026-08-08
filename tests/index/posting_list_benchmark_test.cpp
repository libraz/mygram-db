/**
 * @file posting_list_benchmark_test.cpp
 * @brief Comparative timings for the posting-list operations that carry search cost.
 *
 * Each case runs the production path and a straightforward baseline for the
 * same operation over identical data in the same process, so the two figures
 * are directly comparable on whatever machine the suite runs on. Timings are
 * reported rather than gated: the assertions require the two paths to agree and
 * require the separation to stay in the order of magnitude the production path
 * is chosen for, not a specific duration.
 */

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <string>
#include <vector>

#include "index/posting_list.h"

using mygramdb::DocId;
using mygramdb::index::PostingList;
using mygramdb::index::PostingStrategy;

namespace {

/// Wall-clock milliseconds spent in @p body.
template <typename Body>
double TimeMs(Body&& body) {
  const auto start = std::chrono::steady_clock::now();
  body();
  const auto end = std::chrono::steady_clock::now();
  return std::chrono::duration<double, std::milli>(end - start).count();
}

void Report(const std::string& label, double baseline_ms, double production_ms) {
  std::cout << "  " << std::left << std::setw(46) << label << std::right << std::fixed << std::setprecision(2)
            << std::setw(10) << baseline_ms << " ms  ->" << std::setw(10) << production_ms << " ms  ("
            << std::setprecision(1) << (production_ms > 0.0 ? baseline_ms / production_ms : 0.0) << "x)" << std::endl;
}

/// DocIds spread evenly over @p span, which keeps the list sparse enough that
/// the strategy choice is the variable under test rather than the density.
std::vector<DocId> SpreadDocIds(size_t count, DocId span) {
  std::vector<DocId> ids;
  ids.reserve(count);
  const DocId step = span / static_cast<DocId>(count);
  for (size_t i = 0; i < count; ++i) {
    ids.push_back(static_cast<DocId>(i + 1) * step);
  }
  return ids;
}

std::vector<DocId> RetainByContains(const PostingList& posting, const std::vector<DocId>& candidates) {
  std::vector<DocId> kept;
  kept.reserve(candidates.size());
  for (DocId candidate : candidates) {
    if (posting.Contains(candidate)) {
      kept.push_back(candidate);
    }
  }
  return kept;
}

class PostingListBenchmarkTest : public ::testing::Test {};

/**
 * @brief Candidate filtering: per-candidate probing against a single walk.
 *
 * Filtering N candidates against a posting list is the cost centre of every
 * substring and verification path. Probing candidate by candidate takes one
 * lock per candidate and, for the fixed-width delta strategy, rescans the
 * encoded array each time; RetainPresent walks both sequences once under one
 * lock. The delta row is where the difference is structural (O(candidates x
 * postings) against O(candidates + postings)); the Roaring row is included so
 * the comparison is not read as a strategy artefact.
 */
TEST_F(PostingListBenchmarkTest, CandidateFilteringCostAcrossStrategies) {
  // The largest list the fixed-width delta strategy is allowed to hold: past
  // this the list is promoted to a Roaring bitmap on the way in, so a longer
  // delta list is not reachable and would not be a fair fixture.
  constexpr size_t kPostingCount = 4096;
  constexpr size_t kCandidateCount = 20000;
  constexpr DocId kSpan = 4000000;

  const std::vector<DocId> postings = SpreadDocIds(kPostingCount, kSpan);

  PostingList delta_list(0.1);
  for (DocId id : postings) {
    delta_list.Add(id);
  }
  ASSERT_EQ(delta_list.GetStrategy(), PostingStrategy::kFixedWidthDelta);

  PostingList roaring_list(0.1);
  for (DocId id : postings) {
    roaring_list.Add(id);
  }
  roaring_list.Optimize(kPostingCount * 2);
  ASSERT_EQ(roaring_list.GetStrategy(), PostingStrategy::kRoaringBitmap);

  // Half the candidates are members and half are not, so neither path wins by
  // short-circuiting on a lopsided hit rate.
  std::vector<DocId> candidates;
  candidates.reserve(kCandidateCount);
  for (size_t i = 0; i < kCandidateCount; ++i) {
    const DocId member = postings[(i * 7) % postings.size()];
    candidates.push_back((i % 2 == 0) ? member : member + 1);
  }
  std::sort(candidates.begin(), candidates.end());
  candidates.erase(std::unique(candidates.begin(), candidates.end()), candidates.end());

  std::cout << "\nCandidate filtering: " << candidates.size() << " candidates against " << kPostingCount
            << " postings\n";
  std::cout << "  " << std::left << std::setw(46) << "case" << std::right << std::setw(13) << "Contains() loop"
            << "   " << std::setw(13) << "RetainPresent" << std::endl;

  std::vector<DocId> delta_probe;
  std::vector<DocId> delta_retain;
  const double delta_probe_ms = TimeMs([&] { delta_probe = RetainByContains(delta_list, candidates); });
  const double delta_retain_ms = TimeMs([&] { delta_retain = delta_list.RetainPresent(candidates); });
  Report("fixed-width delta", delta_probe_ms, delta_retain_ms);

  std::vector<DocId> roaring_probe;
  std::vector<DocId> roaring_retain;
  const double roaring_probe_ms = TimeMs([&] { roaring_probe = RetainByContains(roaring_list, candidates); });
  const double roaring_retain_ms = TimeMs([&] { roaring_retain = roaring_list.RetainPresent(candidates); });
  Report("roaring bitmap", roaring_probe_ms, roaring_retain_ms);

  EXPECT_EQ(delta_probe, delta_retain) << "RetainPresent disagrees with Contains() on the delta strategy";
  EXPECT_EQ(roaring_probe, roaring_retain) << "RetainPresent disagrees with Contains() on the roaring strategy";
  EXPECT_EQ(delta_retain, roaring_retain) << "the two strategies disagree about which candidates are present";

  // The delta separation is a complexity difference, so it holds with a wide
  // margin even on a loaded machine. A regression that reintroduces the rescan
  // collapses it.
  EXPECT_GT(delta_probe_ms, delta_retain_ms * 5.0)
      << "single-walk filtering lost its advantage over per-candidate probing on the delta strategy";
}

/**
 * @brief Membership probing at the delta strategy's upper bound, by strategy.
 *
 * A Roaring bitmap answers membership without walking its contents; the
 * fixed-width delta form has to decode the array up to the probed DocId, so its
 * probe cost grows with the list. That is why the delta form is capped: this
 * measures the cost at the cap and confirms that a list one entry past it is
 * held as a bitmap however sparse it is.
 */
TEST_F(PostingListBenchmarkTest, MembershipProbeCostByStrategy) {
  constexpr size_t kPostingCount = 4096;
  constexpr size_t kProbeCount = 5000;
  constexpr DocId kSpan = 4000000;

  const std::vector<DocId> postings = SpreadDocIds(kPostingCount, kSpan);

  PostingList delta_list(0.1);
  PostingList roaring_list(0.1);
  for (DocId id : postings) {
    delta_list.Add(id);
    roaring_list.Add(id);
  }
  roaring_list.Optimize(kPostingCount * 2);
  ASSERT_EQ(delta_list.GetStrategy(), PostingStrategy::kFixedWidthDelta);
  ASSERT_EQ(roaring_list.GetStrategy(), PostingStrategy::kRoaringBitmap);

  size_t delta_hits = 0;
  size_t roaring_hits = 0;
  const double delta_ms = TimeMs([&] {
    for (size_t i = 0; i < kProbeCount; ++i) {
      if (delta_list.Contains(postings[(i * 13) % postings.size()])) {
        ++delta_hits;
      }
    }
  });
  const double roaring_ms = TimeMs([&] {
    for (size_t i = 0; i < kProbeCount; ++i) {
      if (roaring_list.Contains(postings[(i * 13) % postings.size()])) {
        ++roaring_hits;
      }
    }
  });

  std::cout << "\nMembership probe: " << kProbeCount << " Contains() calls on " << kPostingCount << " postings\n";
  Report("Contains() by strategy (delta -> roaring)", delta_ms, roaring_ms);

  EXPECT_EQ(delta_hits, roaring_hits) << "the two strategies disagree about membership";
  EXPECT_GT(delta_ms, roaring_ms) << "probing a capped delta list is no cheaper in a roaring bitmap";

  // One entry past the cap the list is a bitmap on arrival, and stays one even
  // when its density is far below the threshold that would otherwise pick the
  // delta form. Without that floor the probe cost above would keep growing.
  PostingList past_cap(0.1);
  for (DocId id : SpreadDocIds(kPostingCount + 1, kSpan)) {
    past_cap.Add(id);
  }
  EXPECT_EQ(past_cap.GetStrategy(), PostingStrategy::kRoaringBitmap) << "a list past the delta cap was left as deltas";
  past_cap.Optimize(kSpan * 1000);
  EXPECT_EQ(past_cap.GetStrategy(), PostingStrategy::kRoaringBitmap)
      << "a large sparse list was demoted to deltas, putting probe cost back on the list length";
}

/**
 * @brief Intersection: native bitmap AND against materialise-then-intersect.
 *
 * Executing an AND by pulling every posting into a vector per term costs a full
 * materialisation of the widest term before any narrowing happens. Intersecting
 * the lists directly narrows first and materialises only the result.
 */
TEST_F(PostingListBenchmarkTest, IntersectionCostAgainstMaterializedVectors) {
  constexpr size_t kWideCount = 200000;
  constexpr size_t kNarrowCount = 2000;
  constexpr DocId kSpan = 4000000;

  PostingList wide(0.05);
  for (DocId id : SpreadDocIds(kWideCount, kSpan)) {
    wide.Add(id);
  }
  wide.Optimize(kWideCount * 2);
  ASSERT_EQ(wide.GetStrategy(), PostingStrategy::kRoaringBitmap);

  PostingList narrow(0.05);
  for (DocId id : SpreadDocIds(kNarrowCount, kSpan)) {
    narrow.Add(id);
  }
  narrow.Optimize(kNarrowCount * 2);
  ASSERT_EQ(narrow.GetStrategy(), PostingStrategy::kRoaringBitmap);

  std::vector<DocId> materialized;
  const double materialize_ms = TimeMs([&] {
    const std::vector<DocId> left = wide.GetAll();
    const std::vector<DocId> right = narrow.GetAll();
    materialized.clear();
    std::set_intersection(left.begin(), left.end(), right.begin(), right.end(), std::back_inserter(materialized));
  });

  std::vector<DocId> intersected;
  const double intersect_ms = TimeMs([&] {
    const auto result = wide.Intersect(narrow);
    intersected = result->GetAll();
  });

  std::cout << "\nIntersection: " << kWideCount << " x " << kNarrowCount << " postings\n";
  Report("GetAll() + set_intersection -> Intersect()", materialize_ms, intersect_ms);

  EXPECT_EQ(materialized, intersected) << "the native intersection disagrees with the materialized one";
  EXPECT_FALSE(intersected.empty()) << "the fixture produced no overlap, so neither path was exercised";
  EXPECT_LT(intersect_ms, materialize_ms) << "intersecting directly is no cheaper than materializing both sides";
}

/**
 * @brief Point-mutation cost must not scale with the list it mutates.
 *
 * Single-document Add and Remove are on the row-event path, so a per-mutation
 * cost proportional to the posting list turns a busy term into a replication
 * bottleneck. This measures the per-operation cost at two cardinalities an
 * order of magnitude apart and requires it to stay flat.
 */
TEST_F(PostingListBenchmarkTest, PointMutationCostIsFlatAcrossCardinality) {
  constexpr size_t kMutations = 20000;

  const auto per_mutation_us = [](size_t cardinality) {
    PostingList posting(0.05);
    for (DocId id = 1; id <= static_cast<DocId>(cardinality); ++id) {
      posting.Add(id);
    }
    posting.Optimize(cardinality * 2);
    EXPECT_EQ(posting.GetStrategy(), PostingStrategy::kRoaringBitmap);

    // Mutate above the populated range so the set neither grows without bound
    // nor collapses: each Add is paired with the Remove of the previous one.
    const DocId scratch_base = static_cast<DocId>(cardinality) + 1000;
    const double ms = TimeMs([&] {
      for (size_t i = 0; i < kMutations; ++i) {
        posting.Add(scratch_base + static_cast<DocId>(i));
        posting.Remove(scratch_base + static_cast<DocId>(i));
      }
    });
    EXPECT_EQ(posting.Size(), cardinality) << "the paired mutations did not leave the list unchanged";
    return (ms * 1000.0) / static_cast<double>(kMutations);
  };

  const double small_us = per_mutation_us(100000);
  const double large_us = per_mutation_us(1000000);

  std::cout << "\nPoint mutation: " << kMutations << " paired Add/Remove per cardinality\n";
  Report("per Add+Remove us (100k -> 1M postings)", small_us, large_us);

  EXPECT_LT(large_us, small_us * 3.0)
      << "per-mutation cost grew with cardinality, so a full-container walk is back on the mutation path";
}

/**
 * @brief Deleting most of a delta list gives the memory back.
 *
 * A posting list that keeps its peak allocation after the documents are gone
 * holds that memory for the process lifetime, which on a table that churns is
 * indistinguishable from a leak in the reported figures.
 */
TEST_F(PostingListBenchmarkTest, DeltaMemoryIsReclaimedAfterBulkRemoval) {
  constexpr size_t kInitial = 4096;
  constexpr size_t kRemaining = 50;

  PostingList posting(0.9);
  for (DocId id = 1; id <= static_cast<DocId>(kInitial); ++id) {
    posting.Add(id);
  }
  ASSERT_EQ(posting.GetStrategy(), PostingStrategy::kFixedWidthDelta);
  const size_t peak_bytes = posting.MemoryUsage();

  for (DocId id = static_cast<DocId>(kRemaining) + 1; id <= static_cast<DocId>(kInitial); ++id) {
    posting.Remove(id);
  }
  const size_t settled_bytes = posting.MemoryUsage();

  std::cout << "\nDelta memory after removing " << (kInitial - kRemaining) << " of " << kInitial << " postings\n";
  std::cout << "  " << std::left << std::setw(46) << "MemoryUsage() bytes" << std::right << std::setw(10) << peak_bytes
            << "     ->" << std::setw(10) << settled_bytes << std::endl;

  ASSERT_EQ(posting.Size(), kRemaining);
  EXPECT_LT(settled_bytes, peak_bytes / 4) << "the removed postings' allocation was not reclaimed";
  EXPECT_EQ(posting.MemoryUsageApprox(), settled_bytes) << "the lock-free estimate drifted from the exact figure";
}

}  // namespace
