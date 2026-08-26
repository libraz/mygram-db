/**
 * @file search_and_concurrent_mutation_test.cpp
 * @brief SearchAnd over Roaring posting lists while those lists are mutated
 */

#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
#include <string>
#include <thread>
#include <vector>

#include "index/index.h"

using namespace mygramdb::index;

namespace {

// A multi-term query orders the posting lists by size before intersecting them.
// The term count is large enough that std::sort takes its partitioning path
// instead of a fixed-size sorting network, so one query performs a few hundred
// comparisons over the live lists. A query of this length is what a long CJK
// phrase produces when each ideograph is its own term.
constexpr size_t kTermCount = 48;

// Documents shared by every term; they are the entire intersection result.
constexpr uint32_t kSharedDocCount = 200;

// Toggled DocIds live above the shared range, and each one belongs to a single
// term, so toggling them changes posting list sizes without ever changing the
// intersection.
constexpr uint32_t kToggledDocIdBase = 1000000;

constexpr int kMutatorThreads = 16;
constexpr int kSearchThreads = 4;
constexpr int kSearchIterations = 3000;

/**
 * @brief UTF-8 encode one CJK ideograph, used as a single-character term
 * @param offset Offset from the start of the CJK Unified Ideographs block
 * @return The encoded character
 */
std::string CjkTerm(size_t offset) {
  const uint32_t codepoint = 0x4E00U + static_cast<uint32_t>(offset);
  std::string term;
  term.push_back(static_cast<char>(0xE0U | (codepoint >> 12U)));
  term.push_back(static_cast<char>(0x80U | ((codepoint >> 6U) & 0x3FU)));
  term.push_back(static_cast<char>(0x80U | (codepoint & 0x3FU)));
  return term;
}

}  // namespace

/**
 * @brief Concurrent mutation must not corrupt the Roaring intersection path
 *
 * Every term resolves to a Roaring posting list, which selects the
 * bitmap-native intersection in SearchAnd. That path orders the lists by size
 * first, and the sizes move under it while binlog apply runs, so the ordering
 * step must not observe a size changing part-way through.
 */
TEST(SearchAndConcurrentMutationTest, IntersectionStaysCompleteWhilePostingSizesChange) {
  // A zero density threshold makes Optimize() move every posting list to the
  // Roaring strategy regardless of how few documents each holds.
  Index index(2, 1, 0.0, false);

  std::vector<std::string> terms;
  terms.reserve(kTermCount);
  for (size_t i = 0; i < kTermCount; ++i) {
    terms.push_back(CjkTerm(i));
  }

  std::string shared_text;
  for (const auto& term : terms) {
    shared_text += term;
  }

  for (uint32_t doc_id = 1; doc_id <= kSharedDocCount; ++doc_id) {
    ASSERT_TRUE(index.AddDocument(doc_id, shared_text));
  }
  index.Optimize(kSharedDocCount);

  const auto stats = index.GetStatistics();
  ASSERT_EQ(stats.total_terms, kTermCount);
  ASSERT_EQ(stats.roaring_bitmap_lists, kTermCount) << "Every term must be Roaring-backed for this query path";

  std::atomic<bool> stop{false};
  std::vector<std::thread> mutators;
  mutators.reserve(kMutatorThreads);
  for (int worker = 0; worker < kMutatorThreads; ++worker) {
    mutators.emplace_back([&, worker]() {
      while (!stop.load(std::memory_order_relaxed)) {
        for (size_t i = static_cast<size_t>(worker); i < kTermCount; i += static_cast<size_t>(kMutatorThreads)) {
          const uint32_t toggled = kToggledDocIdBase + static_cast<uint32_t>(i);
          index.AddDocument(toggled, terms[i]);
          index.RemoveDocument(toggled, terms[i]);
        }
      }
    });
  }

  std::atomic<int> incomplete_results{0};
  std::vector<std::thread> searchers;
  searchers.reserve(kSearchThreads);
  for (int worker = 0; worker < kSearchThreads; ++worker) {
    searchers.emplace_back([&]() {
      for (int iteration = 0; iteration < kSearchIterations; ++iteration) {
        const auto results = index.SearchAnd(terms);
        if (results.size() != kSharedDocCount) {
          incomplete_results.fetch_add(1, std::memory_order_relaxed);
        }
      }
    });
  }

  for (auto& searcher : searchers) {
    searcher.join();
  }
  stop.store(true, std::memory_order_relaxed);
  for (auto& mutator : mutators) {
    mutator.join();
  }

  EXPECT_EQ(incomplete_results.load(), 0) << "Every search must return the documents shared by all terms";
}
