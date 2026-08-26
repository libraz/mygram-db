/**
 * @file search_handler_test.cpp
 * @brief Unit tests for search handler logic
 *
 * Tests for:
 * - SEARCH GetTopN optimization selection for empty, small and large result sets
 * - Result count reported for pagination when the optimization does not apply
 * - Cached result staleness detection and sample sizing
 * - Floating-point and out-of-range filter value comparison
 * - Configurable FilterByNgrams threshold
 * - N-gram deduplication and NOT-term exclusion
 * - PostFilterByText fail-open behavior
 */

#include "server/handlers/search_handler.h"

#include <gtest/gtest.h>

#include <cmath>
#include <limits>
#include <string>
#include <vector>

#include "config/config.h"
#include "index/index.h"
#include "query/query_parser.h"
#include "server/search_pipeline.h"
#include "storage/document_store.h"

namespace mygramdb {
namespace server {
namespace {

using search_pipeline::ApplySearchTopNOptimization;
using search_pipeline::GenerateTermInfos;

constexpr int kNgramSize = 2;

/// @brief Index and document store seeded with documents sharing one text body.
class SearchCorpus {
 public:
  SearchCorpus(size_t document_count, const std::string& text) : index_(kNgramSize) {
    for (size_t i = 1; i <= document_count; ++i) {
      Add(std::to_string(i), text);
    }
  }

  storage::DocId Add(const std::string& primary_key, const std::string& text) {
    auto doc_id = doc_store_.AddDocument(primary_key, {}, text);
    EXPECT_TRUE(doc_id.has_value());
    index_.AddDocument(*doc_id, text);
    doc_ids_.push_back(*doc_id);
    return *doc_id;
  }

  index::Index* index() { return &index_; }
  storage::DocumentStore* doc_store() { return &doc_store_; }
  const std::vector<storage::DocId>& doc_ids() const { return doc_ids_; }

 private:
  index::Index index_;
  storage::DocumentStore doc_store_;
  std::vector<storage::DocId> doc_ids_;
};

/// @brief Query shape the GetTopN optimization accepts: single term, no NOT, no filters.
query::Query MakeTopNQuery(const std::string& search_text, uint32_t limit, uint32_t offset = 0) {
  query::Query query;
  query.type = query::QueryType::SEARCH;
  query.table = "test";
  query.search_text = search_text;
  query.limit = limit;
  query.offset = offset;
  return query;
}

config::Config MakeConfigWithoutVerifyText() {
  config::Config config;
  config.memory.verify_text = "off";
  return config;
}

}  // namespace

// =============================================================================
// SEARCH GetTopN optimization
// =============================================================================

/**
 * @brief An empty result set must not reach the reuse ratio division
 *
 * The optimization divides the requested page size by the full result count.
 * With no results that division would produce infinity and select the reuse
 * path, so the empty case has to be answered before it.
 */
TEST(SearchHandlerTopNTest, EmptyResultSetIsReportedWithoutSelectingReuse) {
  SearchCorpus corpus(0, "");
  // Every n-gram of "abcd" ("ab", "bc", "cd") has a non-empty posting list here,
  // so the term survives the empty-posting-list early exit, yet the three lists
  // have no document in common.
  corpus.Add("1", "abcx");
  corpus.Add("2", "xbcd");
  ASSERT_TRUE(corpus.doc_store()->IsPrimaryKeyDocIdOrderValid());

  const auto query = MakeTopNQuery("abcd", /*limit=*/10);
  auto term_infos = GenerateTermInfos({query.search_text}, corpus.index(), kNgramSize, 0, false);
  ASSERT_EQ(term_infos.size(), 1U);
  ASSERT_EQ(term_infos[0].ngrams.size(), 3U);
  ASSERT_GT(term_infos[0].estimated_size, 0U);

  auto results = corpus.index()->SearchAnd(term_infos[0].ngrams);
  ASSERT_TRUE(results.empty());

  const config::Config config = MakeConfigWithoutVerifyText();
  auto topn =
      ApplySearchTopNOptimization(query, corpus.index(), corpus.doc_store(), &config, term_infos, {query.search_text},
                                  /*semantics_reproducible_by_single_term_ngram_and=*/true,
                                  /*cache_hit=*/false, "id", results);

  EXPECT_TRUE(topn.considered);
  EXPECT_TRUE(topn.applicable);
  EXPECT_TRUE(topn.no_results);
  EXPECT_FALSE(topn.reused_existing);
  EXPECT_FALSE(topn.optimized);
  EXPECT_EQ(topn.total_results, 0U);
  EXPECT_TRUE(results.empty());
}

/**
 * @brief A result set smaller than twice the page is reused instead of re-fetched
 */
TEST(SearchHandlerTopNTest, SmallResultSetReusesTheExistingResults) {
  SearchCorpus corpus(4, "alpha");
  ASSERT_TRUE(corpus.doc_store()->IsPrimaryKeyDocIdOrderValid());

  const auto query = MakeTopNQuery("alpha", /*limit=*/10);
  auto term_infos = GenerateTermInfos({query.search_text}, corpus.index(), kNgramSize, 0, false);
  auto results = corpus.index()->SearchAnd(term_infos[0].ngrams);
  ASSERT_EQ(results.size(), 4U);
  const auto results_before = results;

  const config::Config config = MakeConfigWithoutVerifyText();
  auto topn =
      ApplySearchTopNOptimization(query, corpus.index(), corpus.doc_store(), &config, term_infos, {query.search_text},
                                  /*semantics_reproducible_by_single_term_ngram_and=*/true,
                                  /*cache_hit=*/false, "id", results);

  EXPECT_TRUE(topn.applicable);
  EXPECT_TRUE(topn.reused_existing);
  EXPECT_FALSE(topn.optimized);
  EXPECT_FALSE(topn.no_results);
  EXPECT_EQ(topn.total_results, 4U);
  EXPECT_EQ(results, results_before) << "reuse must leave the fetched results untouched";
}

/**
 * @brief A large result set is replaced by a descending top-N fetch
 *
 * total_results keeps the pre-truncation count so pagination metadata still
 * reports the full match count after the result vector has been shortened.
 */
TEST(SearchHandlerTopNTest, LargeResultSetIsReplacedByDescendingTopN) {
  constexpr size_t kDocumentCount = 40;
  SearchCorpus corpus(kDocumentCount, "alpha");
  ASSERT_TRUE(corpus.doc_store()->IsPrimaryKeyDocIdOrderValid());

  const auto query = MakeTopNQuery("alpha", /*limit=*/10);
  auto term_infos = GenerateTermInfos({query.search_text}, corpus.index(), kNgramSize, 0, false);
  auto results = corpus.index()->SearchAnd(term_infos[0].ngrams);
  ASSERT_EQ(results.size(), kDocumentCount);

  const config::Config config = MakeConfigWithoutVerifyText();
  auto topn =
      ApplySearchTopNOptimization(query, corpus.index(), corpus.doc_store(), &config, term_infos, {query.search_text},
                                  /*semantics_reproducible_by_single_term_ngram_and=*/true,
                                  /*cache_hit=*/false, "id", results);

  EXPECT_TRUE(topn.applicable);
  EXPECT_TRUE(topn.optimized);
  EXPECT_FALSE(topn.reused_existing);
  EXPECT_TRUE(topn.reverse);
  EXPECT_EQ(topn.total_results, kDocumentCount) << "total_results must survive the truncation";

  const auto& doc_ids = corpus.doc_ids();
  std::vector<storage::DocId> expected;
  expected.reserve(query.limit);
  for (size_t i = 0; i < query.limit; ++i) {
    expected.push_back(doc_ids[doc_ids.size() - 1 - i]);
  }
  EXPECT_EQ(results, expected) << "the ten highest document ids, highest first";
}

/**
 * @brief Offset and limit decide between reuse, top-N fetch and no optimization
 */
TEST(SearchHandlerTopNTest, OffsetAndLimitSelectTheOptimizationPath) {
  constexpr size_t kDocumentCount = 40;
  SearchCorpus corpus(kDocumentCount, "alpha");

  struct TestCase {
    uint32_t offset;
    uint32_t limit;
    bool expected_applicable;
    bool expected_optimized;
    bool expected_reused;
  };
  const std::vector<TestCase> test_cases = {
      {0, 10, true, true, false},       // 10/40 = 0.25 -> re-fetch the top 10
      {0, 20, true, true, false},       // 20/40 = 0.50 -> not above the threshold
      {0, 30, true, false, true},       // 30/40 = 0.75 -> reuse what was fetched
      {5, 20, true, false, true},       // 25/40 = 0.625 -> reuse what was fetched
      {10001, 10, false, false, false}  // beyond the offset cap -> no optimization
  };

  const config::Config config = MakeConfigWithoutVerifyText();
  for (const auto& test_case : test_cases) {
    const auto query = MakeTopNQuery("alpha", test_case.limit, test_case.offset);
    auto term_infos = GenerateTermInfos({query.search_text}, corpus.index(), kNgramSize, 0, false);
    auto results = corpus.index()->SearchAnd(term_infos[0].ngrams);
    ASSERT_EQ(results.size(), kDocumentCount);

    auto topn =
        ApplySearchTopNOptimization(query, corpus.index(), corpus.doc_store(), &config, term_infos, {query.search_text},
                                    /*semantics_reproducible_by_single_term_ngram_and=*/true,
                                    /*cache_hit=*/false, "id", results);

    const std::string context =
        "offset=" + std::to_string(test_case.offset) + " limit=" + std::to_string(test_case.limit);
    EXPECT_TRUE(topn.considered) << context;
    EXPECT_EQ(topn.applicable, test_case.expected_applicable) << context;
    EXPECT_EQ(topn.optimized, test_case.expected_optimized) << context;
    EXPECT_EQ(topn.reused_existing, test_case.expected_reused) << context;
    if (test_case.expected_optimized) {
      EXPECT_EQ(results.size(), test_case.offset + test_case.limit) << context;
    } else {
      EXPECT_EQ(results.size(), kDocumentCount) << context;
    }
  }
}

/**
 * @brief Column filters disable the optimization so the filtered count is kept
 *
 * The handler only overwrites its result count with total_results when the
 * optimization applies. A filtered query must therefore report the number of
 * documents left after filtering, not the number matched before it.
 */
TEST(SearchHandlerTopNTest, FilteredQueryKeepsThePostFilteredResultCount) {
  SearchCorpus corpus(40, "alpha");

  auto query = MakeTopNQuery("alpha", /*limit=*/10);
  query.filters.push_back({"status", query::FilterOp::EQ, "1"});

  auto term_infos = GenerateTermInfos({query.search_text}, corpus.index(), kNgramSize, 0, false);
  // Stand in for the result vector the pipeline hands over after filtering.
  std::vector<storage::DocId> results(corpus.doc_ids().begin(), corpus.doc_ids().begin() + 5);
  const auto results_before = results;

  const config::Config config = MakeConfigWithoutVerifyText();
  auto topn =
      ApplySearchTopNOptimization(query, corpus.index(), corpus.doc_store(), &config, term_infos, {query.search_text},
                                  /*semantics_reproducible_by_single_term_ngram_and=*/true,
                                  /*cache_hit=*/false, "id", results);

  EXPECT_TRUE(topn.considered);
  EXPECT_FALSE(topn.applicable) << "a filtered query must not take the top-N path";
  EXPECT_EQ(topn.total_results, 0U);
  EXPECT_EQ(results, results_before) << "the filtered result set must survive unchanged";
}

/**
 * @brief NOT terms disable the optimization the same way column filters do
 */
TEST(SearchHandlerTopNTest, NotTermsKeepThePostFilteredResultCount) {
  SearchCorpus corpus(40, "alpha");

  auto query = MakeTopNQuery("alpha", /*limit=*/10);
  query.not_terms.push_back("beta");

  auto term_infos = GenerateTermInfos({query.search_text}, corpus.index(), kNgramSize, 0, false);
  std::vector<storage::DocId> results(corpus.doc_ids().begin(), corpus.doc_ids().begin() + 7);
  const auto results_before = results;

  const config::Config config = MakeConfigWithoutVerifyText();
  auto topn =
      ApplySearchTopNOptimization(query, corpus.index(), corpus.doc_store(), &config, term_infos, {query.search_text},
                                  /*semantics_reproducible_by_single_term_ngram_and=*/true,
                                  /*cache_hit=*/false, "id", results);

  EXPECT_TRUE(topn.considered);
  EXPECT_FALSE(topn.applicable);
  EXPECT_EQ(topn.total_results, 0U);
  EXPECT_EQ(results, results_before);
}

// =============================================================================
// Cached result staleness
// =============================================================================

/**
 * @brief A removed document makes a cached result set stale
 *
 * Cached entries hold DocIds. Once a document is gone its DocId no longer
 * resolves to a primary key, and the entry must not be served.
 */
TEST(SearchHandlerCacheStalenessTest, RemovedDocumentMakesCachedResultsStale) {
  storage::DocumentStore doc_store;
  std::vector<storage::DocId> cached_results;
  for (int i = 1; i <= 5; ++i) {
    auto doc_id = doc_store.AddDocument("pk" + std::to_string(i), {}, "text");
    ASSERT_TRUE(doc_id.has_value());
    cached_results.push_back(*doc_id);
  }

  EXPECT_FALSE(search_pipeline::IsCacheStale(cached_results, &doc_store));

  ASSERT_TRUE(doc_store.RemoveDocument(cached_results[2]));
  EXPECT_TRUE(search_pipeline::IsCacheStale(cached_results, &doc_store));
}

/**
 * @brief Staleness sampling covers a result set far larger than the sample size
 *
 * Only a bounded sample of a wide result set is validated, so the sampled
 * positions have to be spread across the whole vector.
 */
TEST(SearchHandlerCacheStalenessTest, StalenessIsDetectedInWideResultSets) {
  constexpr size_t kResultCount = 5000;
  storage::DocumentStore doc_store;
  std::vector<storage::DocId> cached_results;
  cached_results.reserve(kResultCount);
  for (size_t i = 1; i <= kResultCount; ++i) {
    auto doc_id = doc_store.AddDocument("pk" + std::to_string(i), {}, "text");
    ASSERT_TRUE(doc_id.has_value());
    cached_results.push_back(*doc_id);
  }
  ASSERT_FALSE(search_pipeline::IsCacheStale(cached_results, &doc_store));

  // The sample walks the vector in fixed steps from index 0, so the first
  // element is always inspected.
  ASSERT_TRUE(doc_store.RemoveDocument(cached_results.front()));
  EXPECT_TRUE(search_pipeline::IsCacheStale(cached_results, &doc_store));
}

/**
 * @brief Sample size grows with the result count and is capped
 */
TEST(SearchHandlerCacheStalenessTest, SampleSizeIsBoundedAndProportional) {
  EXPECT_EQ(search_pipeline::CacheStaleSampleSize(0), 0U);
  EXPECT_EQ(search_pipeline::CacheStaleSampleSize(5), 5U) << "below the minimum, check everything";
  EXPECT_EQ(search_pipeline::CacheStaleSampleSize(50), 10U) << "at least ten samples";
  EXPECT_EQ(search_pipeline::CacheStaleSampleSize(1000), 100U) << "one tenth of the results";
  EXPECT_EQ(search_pipeline::CacheStaleSampleSize(100000), 1024U) << "capped, not one tenth";
}

// =============================================================================
// Filter value comparison
// =============================================================================

namespace {

/// @brief Documents whose only filter column holds notable double values.
class DoubleFilterCorpus {
 public:
  DoubleFilterCorpus() {
    sum_of_tenths_ = Add(0.1 + 0.2);
    three_tenths_ = Add(0.3);
    not_a_number_ = Add(std::numeric_limits<double>::quiet_NaN());
    large_ = Add(1e15);
    large_plus_one_ = Add(1e15 + 1.0);
  }

  std::vector<storage::DocId> Filter(query::FilterOp op, const std::string& value) {
    const std::vector<query::FilterCondition> filters = {{"score", op, value}};
    return search_pipeline::ApplyFilters(doc_ids_, filters, &doc_store_);
  }

  storage::DocId sum_of_tenths() const { return sum_of_tenths_; }
  storage::DocId three_tenths() const { return three_tenths_; }
  storage::DocId not_a_number() const { return not_a_number_; }
  storage::DocId large() const { return large_; }
  storage::DocId large_plus_one() const { return large_plus_one_; }
  const std::vector<storage::DocId>& doc_ids() const { return doc_ids_; }

 private:
  storage::DocId Add(double score) {
    auto doc_id =
        doc_store_.AddDocument("pk" + std::to_string(doc_ids_.size() + 1), {{"score", storage::FilterValue{score}}});
    EXPECT_TRUE(doc_id.has_value());
    doc_ids_.push_back(*doc_id);
    return *doc_id;
  }

  storage::DocumentStore doc_store_;
  std::vector<storage::DocId> doc_ids_;
  storage::DocId sum_of_tenths_ = 0;
  storage::DocId three_tenths_ = 0;
  storage::DocId not_a_number_ = 0;
  storage::DocId large_ = 0;
  storage::DocId large_plus_one_ = 0;
};

}  // namespace

/**
 * @brief Double equality selects the exact stored value, not a neighbouring one
 *
 * 0.1 + 0.2 and 0.3 are different doubles, and each equality filter selects
 * only the document holding its own value.
 */
TEST(SearchHandlerFilterValueTest, DoubleEqualityDistinguishesAdjacentValues) {
  DoubleFilterCorpus corpus;

  auto exactly_three_tenths = corpus.Filter(query::FilterOp::EQ, "0.3");
  ASSERT_EQ(exactly_three_tenths.size(), 1U);
  EXPECT_EQ(exactly_three_tenths[0], corpus.three_tenths());

  auto sum_of_tenths = corpus.Filter(query::FilterOp::EQ, "0.30000000000000004");
  ASSERT_EQ(sum_of_tenths.size(), 1U);
  EXPECT_EQ(sum_of_tenths[0], corpus.sum_of_tenths());
}

/**
 * @brief NaN matches no ordering or equality filter, and every inequality filter
 */
TEST(SearchHandlerFilterValueTest, NotANumberNeverMatchesAComparison) {
  DoubleFilterCorpus corpus;

  for (const auto op :
       {query::FilterOp::EQ, query::FilterOp::GT, query::FilterOp::GTE, query::FilterOp::LT, query::FilterOp::LTE}) {
    auto matched = corpus.Filter(op, "1.0");
    EXPECT_EQ(std::count(matched.begin(), matched.end(), corpus.not_a_number()), 0)
        << "NaN matched operator " << static_cast<int>(op);
  }

  auto not_equal = corpus.Filter(query::FilterOp::NE, "1.0");
  EXPECT_EQ(std::count(not_equal.begin(), not_equal.end(), corpus.not_a_number()), 1);
}

/**
 * @brief Ordering filters keep their resolution at large magnitudes
 */
TEST(SearchHandlerFilterValueTest, OrderingFiltersSeparateLargeNeighbouringDoubles) {
  DoubleFilterCorpus corpus;

  auto above = corpus.Filter(query::FilterOp::GT, "1000000000000000");
  ASSERT_EQ(above.size(), 1U);
  EXPECT_EQ(above[0], corpus.large_plus_one()) << "a difference of one at 1e15 is not absorbed";

  auto at_or_above = corpus.Filter(query::FilterOp::GTE, "1000000000000000");
  ASSERT_EQ(at_or_above.size(), 2U);
  EXPECT_EQ(at_or_above[0], corpus.large());
  EXPECT_EQ(at_or_above[1], corpus.large_plus_one());
}

/**
 * @brief A filter value that is not a number matches no numeric document
 */
TEST(SearchHandlerFilterValueTest, NonNumericFilterValueMatchesNoNumericColumn) {
  storage::DocumentStore doc_store;
  auto doc_id = doc_store.AddDocument("pk1", {{"status", storage::FilterValue{int64_t{1}}}});
  ASSERT_TRUE(doc_id.has_value());
  const std::vector<storage::DocId> candidates = {*doc_id};

  for (const std::string& value : {std::string("abc"), std::string(), std::string("1x")}) {
    const std::vector<query::FilterCondition> filters = {{"status", query::FilterOp::EQ, value}};
    EXPECT_TRUE(search_pipeline::ApplyFilters(candidates, filters, &doc_store).empty())
        << "value \"" << value << "\" must not match an integer column";
  }
}

/**
 * @brief The largest unsigned value round-trips through filter parsing
 */
TEST(SearchHandlerFilterValueTest, UnsignedColumnMatchesItsMaximumValue) {
  storage::DocumentStore doc_store;
  auto doc_id = doc_store.AddDocument("pk1", {{"counter", storage::FilterValue{std::numeric_limits<uint64_t>::max()}}});
  ASSERT_TRUE(doc_id.has_value());
  const std::vector<storage::DocId> candidates = {*doc_id};

  const std::vector<query::FilterCondition> match = {{"counter", query::FilterOp::EQ, "18446744073709551615"}};
  EXPECT_EQ(search_pipeline::ApplyFilters(candidates, match, &doc_store), candidates);

  const std::vector<query::FilterCondition> no_match = {{"counter", query::FilterOp::EQ, "18446744073709551614"}};
  EXPECT_TRUE(search_pipeline::ApplyFilters(candidates, no_match, &doc_store).empty());
}

// =============================================================================
// Configurable FilterByNgrams threshold
// =============================================================================

TEST(SearchHandlerTest, FilterThresholdConfigurable) {
  // Default threshold
  EXPECT_EQ(SearchHandler::GetFilterThreshold(), 1000);

  // Set custom threshold
  SearchHandler::SetFilterThreshold(500);
  EXPECT_EQ(SearchHandler::GetFilterThreshold(), 500);

  // Set another threshold
  SearchHandler::SetFilterThreshold(2000);
  EXPECT_EQ(SearchHandler::GetFilterThreshold(), 2000);

  // Restore default
  SearchHandler::SetFilterThreshold(1000);
  EXPECT_EQ(SearchHandler::GetFilterThreshold(), 1000);
}

/**
 * @brief Both sides of the threshold produce the same intersection
 *
 * The threshold only picks between filtering the current candidates and
 * intersecting a freshly searched posting list, so the answer must not depend
 * on it.
 */
TEST(SearchHandlerTest, FilterThresholdDoesNotChangeTheResultSet) {
  SearchCorpus corpus(0, "");
  const auto both = corpus.Add("1", "alpha beta");
  corpus.Add("2", "alpha gamma");
  const auto both_again = corpus.Add("3", "beta alpha");
  corpus.Add("4", "gamma delta");

  query::Query query;
  query.type = query::QueryType::SEARCH;
  query.table = "test";
  query.search_text = "alpha";
  query.and_terms.push_back("beta");
  query.limit = 100;

  const std::vector<std::string> all_terms = {"alpha", "beta"};
  auto term_infos = GenerateTermInfos(all_terms, corpus.index(), kNgramSize, 0, false);
  const config::Config config = MakeConfigWithoutVerifyText();

  auto filtered = search_pipeline::Execute(query, term_infos, all_terms, corpus.index(), corpus.doc_store(), &config,
                                           kNgramSize, 0, false, /*filter_threshold=*/1000);
  auto intersected = search_pipeline::Execute(query, term_infos, all_terms, corpus.index(), corpus.doc_store(), &config,
                                              kNgramSize, 0, false, /*filter_threshold=*/0);

  const std::vector<storage::DocId> expected = {both, both_again};
  EXPECT_EQ(filtered.results, expected) << "candidate filtering path";
  EXPECT_EQ(intersected.results, expected) << "full intersection path";
}

// =============================================================================
// N-gram generation and NOT-term exclusion
// =============================================================================

/**
 * @brief Repeated n-grams within a term are collapsed to one lookup each
 */
TEST(SearchHandlerTest, GeneratedNgramsAreSortedAndUnique) {
  SearchCorpus corpus(0, "");
  corpus.Add("1", "abab");

  auto term_infos = GenerateTermInfos({"abab"}, corpus.index(), kNgramSize, 0, false);
  ASSERT_EQ(term_infos.size(), 1U);
  // "abab" yields "ab", "ba", "ab"; the repeat is dropped.
  EXPECT_EQ(term_infos[0].ngrams, (std::vector<std::string>{"ab", "ba"}));
}

/**
 * @brief Overlapping NOT terms exclude exactly the documents they match
 */
TEST(SearchHandlerTest, OverlappingNotTermsExcludeOnlyTheirOwnMatches) {
  SearchCorpus corpus(0, "");
  const auto plain = corpus.Add("1", "alpha zzz");
  corpus.Add("2", "alpha abc");
  corpus.Add("3", "alpha abcd");

  auto term_infos = GenerateTermInfos({"alpha"}, corpus.index(), kNgramSize, 0, false);
  auto candidates = corpus.index()->SearchAnd(term_infos[0].ngrams);
  ASSERT_EQ(candidates.size(), 3U);

  auto remaining = search_pipeline::ApplyNotFilter(candidates, {"abc", "abcd"}, corpus.index(), corpus.doc_store(),
                                                   kNgramSize, 0, false);
  EXPECT_EQ(remaining, (std::vector<storage::DocId>{plain}));
}

// =============================================================================
// PostFilterByText: nullopt text includes document (fail-open behavior)
// =============================================================================

/**
 * @brief PostFilterByText includes documents when normalized text is unavailable
 *
 * After snapshot restore, doc_texts_ may be empty. PostFilterByText should
 * include such documents rather than dropping them (false positive > false negative).
 */
TEST(SearchHandlerTest, PostFilterByText_NulloptTextIncludesDocument) {
  storage::DocumentStore doc_store;
  // Add document but do NOT set normalized text
  auto doc_id = doc_store.AddDocument("pk1", {{"content", storage::FilterValue("hello world")}});
  ASSERT_TRUE(doc_id.has_value());

  // GetNormalizedText should return nullopt (no text stored)
  EXPECT_FALSE(doc_store.GetNormalizedText(*doc_id).has_value());

  std::vector<storage::DocId> candidates = {*doc_id};
  std::vector<std::string> terms = {"hello"};
  auto result = SearchHandler::PostFilterByText(candidates, terms, &doc_store);

  // Document should be INCLUDED (not filtered out) when text is unavailable
  ASSERT_EQ(result.size(), 1);
  EXPECT_EQ(result[0], *doc_id);
}

/**
 * @brief PostFilterByText correctly filters documents with stored normalized text
 */
TEST(SearchHandlerTest, PostFilterByText_WithTextFiltersCorrectly) {
  storage::DocumentStore doc_store;
  auto doc_id1 = doc_store.AddDocument("pk1", {{"content", storage::FilterValue("hello world")}});
  auto doc_id2 = doc_store.AddDocument("pk2", {{"content", storage::FilterValue("goodbye world")}});
  ASSERT_TRUE(doc_id1.has_value());
  ASSERT_TRUE(doc_id2.has_value());

  doc_store.SetNormalizedText(*doc_id1, "hello world");
  doc_store.SetNormalizedText(*doc_id2, "goodbye world");

  std::vector<storage::DocId> candidates = {*doc_id1, *doc_id2};
  std::vector<std::string> terms = {"hello"};
  auto result = SearchHandler::PostFilterByText(candidates, terms, &doc_store);

  // Only doc_id1 contains "hello"
  ASSERT_EQ(result.size(), 1);
  EXPECT_EQ(result[0], *doc_id1);
}

/**
 * @brief PostFilterByText with mixed text and nullopt uses fail-open for nullopt
 *
 * Documents with stored text are filtered normally, while documents without
 * stored text (nullopt) are included to avoid false negatives.
 */
TEST(SearchHandlerTest, PostFilterByText_MixedTextAndNullopt) {
  storage::DocumentStore doc_store;
  auto doc_id1 = doc_store.AddDocument("pk1", {{"content", storage::FilterValue("hello world")}});
  auto doc_id2 = doc_store.AddDocument("pk2", {{"content", storage::FilterValue("goodbye world")}});
  auto doc_id3 = doc_store.AddDocument("pk3", {{"content", storage::FilterValue("test data")}});
  ASSERT_TRUE(doc_id1.has_value());
  ASSERT_TRUE(doc_id2.has_value());
  ASSERT_TRUE(doc_id3.has_value());

  // Only set text for doc1 and doc2, not doc3
  doc_store.SetNormalizedText(*doc_id1, "hello world");
  doc_store.SetNormalizedText(*doc_id2, "goodbye world");

  std::vector<storage::DocId> candidates = {*doc_id1, *doc_id2, *doc_id3};
  std::vector<std::string> terms = {"hello"};
  auto result = SearchHandler::PostFilterByText(candidates, terms, &doc_store);

  // doc_id1: has text, contains "hello" -> included
  // doc_id2: has text, doesn't contain "hello" -> excluded
  // doc_id3: no text (nullopt) -> included (fail-open)
  ASSERT_EQ(result.size(), 2);
  EXPECT_EQ(result[0], *doc_id1);
  EXPECT_EQ(result[1], *doc_id3);
}

}  // namespace server
}  // namespace mygramdb
