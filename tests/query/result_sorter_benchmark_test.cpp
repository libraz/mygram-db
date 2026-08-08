/**
 * @file result_sorter_benchmark_test.cpp
 * @brief Cost of ordering a wide result set down to a page.
 *
 * A paginated query orders every matching document but returns only a page of
 * them. Two paths can do that: precomputing sort keys in bounded batches and
 * keeping only the requested top-k, or comparing through the document store on
 * every comparison. Which one wins depends on what a sort key costs to build,
 * so both are measured over the same result sets, ordered by primary key and by
 * a filter column, at widths spanning the batch bound. Timings are reported;
 * the assertions cover the two paths producing the same page.
 */

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "query/query_parser.h"
#include "query/result_sorter.h"
#include "storage/document_store.h"

namespace mygramdb::query {
namespace {

template <typename Body>
double TimeMs(Body&& body) {
  const auto start = std::chrono::steady_clock::now();
  body();
  const auto end = std::chrono::steady_clock::now();
  return std::chrono::duration<double, std::milli>(end - start).count();
}

// The last width crosses the batch bound the production path uses, so the
// multi-batch behaviour is covered without paying for a much larger fixture.
constexpr std::array<size_t, 3> kProbeSizes = {100000, 1000000, 2000000};
constexpr uint32_t kPageSize = 100;
constexpr const char* kSortFilterColumn = "author_id";

class ResultSorterBenchmarkTest : public ::testing::Test {};

/**
 * @brief Ordering a wide match set down to one page, both paths, by width.
 *
 * Ordering by primary key reads a value the store already holds, so a
 * precomputed key buys little. Ordering by a filter column costs a store lookup
 * per key, which a comparator pays once per comparison and a precomputed key
 * pays once per document. The two rows separate those cases.
 */
TEST_F(ResultSorterBenchmarkTest, PaginationCostOverAWideMatchSet) {
  std::cout << "\nPagination: top " << kPageSize << " of N matches\n";
  std::cout << "  " << std::left << std::setw(12) << "matches" << std::setw(16) << "sort by" << std::right
            << std::setw(16) << "comparator ms" << std::setw(18) << "batched top-k ms" << std::endl;

  for (size_t matches_count : kProbeSizes) {
    storage::DocumentStore doc_store;
    std::vector<storage::DocumentStore::DocumentItem> items;
    items.reserve(matches_count);
    for (size_t i = 0; i < matches_count; ++i) {
      storage::FilterMap filters;
      filters.emplace(kSortFilterColumn, static_cast<int64_t>((i * 104729) % matches_count));
      // Keys are interleaved rather than ascending so neither path can benefit
      // from the input already being in order.
      items.push_back({std::to_string((i * 7919) % matches_count), std::move(filters), "", ""});
    }
    const auto added = doc_store.AddDocumentBatch(items);
    ASSERT_TRUE(added.has_value());
    const std::vector<DocId>& matches = added.value();
    ASSERT_EQ(matches.size(), matches_count);

    const auto measure = [&](const char* label, std::optional<OrderByClause> order_by) {
      Query query;
      query.type = QueryType::SEARCH;
      query.table = "bench";
      query.search_text = "bench";
      query.limit = kPageSize;
      query.offset = 0;
      query.order_by = std::move(order_by);

      std::vector<DocId> batched_input = matches;
      std::vector<DocId> batched_page;
      const double batched_ms = TimeMs([&] {
        auto result = ResultSorter::SortAndPaginate(batched_input, doc_store, query);
        ASSERT_TRUE(result.has_value());
        batched_page = result.value();
      });

      ResultSorter::ForceSchwartzianPartialFailureForTesting(true);
      std::vector<DocId> comparator_input = matches;
      std::vector<DocId> comparator_page;
      const double comparator_ms = TimeMs([&] {
        auto result = ResultSorter::SortAndPaginate(comparator_input, doc_store, query);
        ASSERT_TRUE(result.has_value());
        comparator_page = result.value();
      });
      ResultSorter::ForceSchwartzianPartialFailureForTesting(false);

      std::cout << "  " << std::left << std::setw(12) << matches_count << std::setw(16) << label << std::right
                << std::fixed << std::setprecision(2) << std::setw(16) << comparator_ms << std::setw(18) << batched_ms
                << std::endl;

      EXPECT_EQ(batched_page.size(), kPageSize) << label << " at " << matches_count << " matches";
      EXPECT_EQ(batched_page, comparator_page) << "the two ordering paths returned different pages sorting by " << label
                                               << " at " << matches_count << " matches";
    };

    measure("primary key", std::nullopt);
    measure("filter column", OrderByClause{kSortFilterColumn, SortOrder::DESC});
  }
}

/**
 * @brief The same two paths while replication is writing to the same store.
 *
 * A single-threaded reading is not enough to choose between them. The
 * comparator reaches into the document store on every comparison, which is
 * roughly 2*N*log(top_k) shared-lock acquisitions; the batched path takes one
 * per batch. A server that applies binlog rows while it serves searches has a
 * writer contending for that same lock, so this measures both the ordering cost
 * and what the writer got done underneath it.
 */
TEST_F(ResultSorterBenchmarkTest, PaginationCostWhileTheStoreIsBeingWritten) {
  // An order of magnitude below the widths measured without a writer. The
  // comparator's cost here is dominated by lock acquisitions rather than by the
  // width, so the separation shows at this size and the case stays inside the
  // load tier's time budget.
  constexpr size_t kMatchesCount = 50000;

  storage::DocumentStore doc_store;
  std::vector<storage::DocumentStore::DocumentItem> items;
  items.reserve(kMatchesCount);
  for (size_t i = 0; i < kMatchesCount; ++i) {
    storage::FilterMap filters;
    filters.emplace(kSortFilterColumn, static_cast<int64_t>((i * 104729) % kMatchesCount));
    items.push_back({std::to_string((i * 7919) % kMatchesCount), std::move(filters), "", ""});
  }
  const auto added = doc_store.AddDocumentBatch(items);
  ASSERT_TRUE(added.has_value());
  const std::vector<DocId>& matches = added.value();

  std::cout << "\nPagination under a concurrent writer: top " << kPageSize << " of " << kMatchesCount << " matches\n";
  std::cout << "  " << std::left << std::setw(16) << "sort by" << std::setw(14) << "path" << std::right << std::setw(12)
            << "sort ms" << std::setw(16) << "writes done" << std::endl;

  const auto measure = [&](const char* label, const char* path, std::optional<OrderByClause> order_by,
                           bool force_comparator) {
    Query query;
    query.type = QueryType::SEARCH;
    query.table = "bench";
    query.search_text = "bench";
    query.limit = kPageSize;
    query.offset = 0;
    query.order_by = std::move(order_by);

    std::atomic<bool> stop{false};
    std::atomic<uint64_t> writes{0};
    std::atomic<uint64_t> next_key{kMatchesCount};
    std::thread writer([&]() {
      while (!stop.load(std::memory_order_acquire)) {
        const auto key = next_key.fetch_add(1, std::memory_order_relaxed);
        if (doc_store.AddDocument(std::to_string(key)).has_value()) {
          writes.fetch_add(1, std::memory_order_relaxed);
        }
      }
    });

    ResultSorter::ForceSchwartzianPartialFailureForTesting(force_comparator);
    std::vector<DocId> input = matches;
    std::vector<DocId> page;
    const double sort_ms = TimeMs([&] {
      auto result = ResultSorter::SortAndPaginate(input, doc_store, query);
      ASSERT_TRUE(result.has_value());
      page = result.value();
    });
    ResultSorter::ForceSchwartzianPartialFailureForTesting(false);

    stop.store(true, std::memory_order_release);
    writer.join();

    std::cout << "  " << std::left << std::setw(16) << label << std::setw(14) << path << std::right << std::fixed
              << std::setprecision(2) << std::setw(12) << sort_ms << std::setw(16)
              << writes.load(std::memory_order_relaxed) << std::endl;

    EXPECT_EQ(page.size(), kPageSize) << label << " via " << path;
    return std::make_pair(page, sort_ms);
  };

  // Whichever path is quicker on an idle store, the one that reaches into the
  // store per comparison is the one a writer can hurt, so the ordering below is
  // the property the strategy choice rests on. The measured separation is an
  // order of magnitude or more, so this is not a close call being pinned.
  const auto expect_batched_wins = [](const char* label, double batched_ms, double comparator_ms) {
    EXPECT_LT(batched_ms, comparator_ms) << "sorting by " << label
                                         << " through the store per comparison was not the slower path under a writer; "
                                         << "the strategy choice in SortAndPaginate assumes it is";
  };

  // Documents added by the writer are outside the match set being ordered, so
  // both paths must still return the same page despite the concurrent writes.
  const auto batched_by_key = measure("primary key", "batched", std::nullopt, false);
  const auto comparator_by_key = measure("primary key", "comparator", std::nullopt, true);
  EXPECT_EQ(batched_by_key.first, comparator_by_key.first)
      << "the two paths disagreed on the page while the store was written";
  expect_batched_wins("primary key", batched_by_key.second, comparator_by_key.second);

  const auto batched_by_filter =
      measure("filter column", "batched", OrderByClause{kSortFilterColumn, SortOrder::DESC}, false);
  const auto comparator_by_filter =
      measure("filter column", "comparator", OrderByClause{kSortFilterColumn, SortOrder::DESC}, true);
  EXPECT_EQ(batched_by_filter.first, comparator_by_filter.first)
      << "the two paths disagreed on the page while the store was written";
  expect_batched_wins("filter column", batched_by_filter.second, comparator_by_filter.second);
}

}  // namespace
}  // namespace mygramdb::query
