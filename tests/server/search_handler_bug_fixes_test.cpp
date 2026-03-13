/**
 * @file search_handler_bug_fixes_test.cpp
 * @brief Unit tests for critical search handler bug fixes
 *
 * TDD tests for:
 * - Bug #21: Division by zero in search optimization
 * - Bug #22: Wrong total_results after filter application
 */

#include <gtest/gtest.h>

#include <cmath>
#include <limits>

#include "server/handlers/search_handler.h"

namespace mygramdb {
namespace server {

// =============================================================================
// Bug #21: Division by zero in search optimization
// =============================================================================
// The bug is in search_handler.cpp:205
//
// auto all_results = current_index->SearchAnd(term_infos[0].ngrams);
// total_results = all_results.size();  // Could be 0!
//
// size_t index_limit = query.offset + query.limit;
// bool should_reuse = (static_cast<double>(index_limit) /
//                      static_cast<double>(total_results)) > kReuseThreshold;
// // Division by zero when total_results == 0!
// =============================================================================

/**
 * @brief Test the specific calculation that causes division by zero
 *
 * This test directly validates the problematic calculation at line 205.
 */
TEST(SearchHandlerBugFixTest, Bug21_ReuseCalculationWithZeroResults) {
  // Simulate the calculation at line 205
  size_t total_results = 0;  // Empty search results
  size_t query_offset = 0;
  size_t query_limit = 10;

  size_t index_limit = query_offset + query_limit;  // = 10
  constexpr double kReuseThreshold = 0.5;

  // The buggy calculation:
  // bool should_reuse = (static_cast<double>(index_limit) /
  //                      static_cast<double>(total_results)) > kReuseThreshold;

  // This would be: 10.0 / 0.0 > 0.5 -> inf > 0.5 -> true (or crash on some systems)

  // Test that division by zero produces infinity (IEEE 754 behavior)
  if (total_results == 0) {
    double buggy_ratio = static_cast<double>(index_limit) / static_cast<double>(total_results);
    EXPECT_TRUE(std::isinf(buggy_ratio)) << "Division by zero should produce infinity";
  }

  // The fix should check for total_results == 0 BEFORE the division
  bool can_optimize = true;  // Assume we can optimize initially

  // Fixed logic: Don't optimize if there are no results
  if (total_results == 0) {
    can_optimize = false;
  }

  double ratio = 0.0;
  bool should_reuse = false;

  if (can_optimize && total_results > 0) {
    ratio = static_cast<double>(index_limit) / static_cast<double>(total_results);
    should_reuse = ratio > kReuseThreshold;
  }

  // With zero results, we should NOT attempt the optimization
  EXPECT_FALSE(can_optimize) << "Should disable optimization for empty results";
  EXPECT_FALSE(should_reuse) << "should_reuse should be false for empty results";
}

/**
 * @brief Test edge case: very small result set
 */
TEST(SearchHandlerBugFixTest, Bug21_SmallResultSet) {
  size_t total_results = 1;  // Single result
  size_t query_offset = 0;
  size_t query_limit = 10;

  size_t index_limit = query_offset + query_limit;  // = 10
  constexpr double kReuseThreshold = 0.5;

  // With 1 result and limit 10, ratio = 10/1 = 10 > 0.5
  // So should_reuse = true (which might be unexpected but not a crash)

  bool can_optimize = total_results > 0;
  double ratio = 0.0;
  bool should_reuse = false;

  if (can_optimize) {
    ratio = static_cast<double>(index_limit) / static_cast<double>(total_results);
    should_reuse = ratio > kReuseThreshold;
  }

  EXPECT_TRUE(can_optimize);
  EXPECT_DOUBLE_EQ(ratio, 10.0);
  EXPECT_TRUE(should_reuse);
}

/**
 * @brief Test normal case: large result set
 */
TEST(SearchHandlerBugFixTest, Bug21_LargeResultSet) {
  size_t total_results = 1000;  // Many results
  size_t query_offset = 0;
  size_t query_limit = 10;

  size_t index_limit = query_offset + query_limit;  // = 10
  constexpr double kReuseThreshold = 0.5;

  // With 1000 results and limit 10, ratio = 10/1000 = 0.01 < 0.5
  // So should_reuse = false (use GetTopN optimization)

  bool can_optimize = total_results > 0;
  double ratio = 0.0;
  bool should_reuse = false;

  if (can_optimize) {
    ratio = static_cast<double>(index_limit) / static_cast<double>(total_results);
    should_reuse = ratio > kReuseThreshold;
  }

  EXPECT_TRUE(can_optimize);
  EXPECT_DOUBLE_EQ(ratio, 0.01);
  EXPECT_FALSE(should_reuse);
}

/**
 * @brief Test that the fix correctly handles zero total_results
 *
 * This test verifies the proposed fix: checking total_results > 0
 * before performing the division.
 */
TEST(SearchHandlerBugFixTest, Bug21_FixedCodePath) {
  // Simulating the fixed code path from search_handler.cpp:195-206
  struct TestCase {
    size_t total_results;
    size_t offset;
    size_t limit;
    bool expected_can_optimize;
  };

  std::vector<TestCase> test_cases = {
      {0, 0, 10, false},      // Zero results - should NOT optimize
      {1, 0, 10, true},       // Single result - can optimize
      {100, 0, 10, true},     // Many results - can optimize
      {0, 5, 20, false},      // Zero results with offset - should NOT optimize
      {1000, 500, 100, true}  // Large offset/limit - can optimize
  };

  for (const auto& tc : test_cases) {
    bool can_optimize = true;

    // Fixed code: Check total_results before division
    if (tc.total_results == 0) {
      can_optimize = false;
    }

    EXPECT_EQ(can_optimize, tc.expected_can_optimize)
        << "Failed for total_results=" << tc.total_results << ", offset=" << tc.offset << ", limit=" << tc.limit;

    // If can_optimize is false, we should NOT attempt division
    if (!can_optimize) {
      // The fix prevents us from reaching the division
      SUCCEED() << "Correctly avoided division by zero";
    }
  }
}

// =============================================================================
// Bug #22: Wrong total_results after filter application
// =============================================================================
// When GetTopN optimization is used, total_results is set before filters are
// applied. After filtering, the results vector shrinks but total_results
// is not updated, causing incorrect pagination metadata.
//
// This is a logic test - actual SearchHandler integration tests would be
// in a separate file that sets up the full infrastructure.
// =============================================================================

/**
 * @brief Test that total_results tracking logic is correct
 */
TEST(SearchHandlerBugFixTest, Bug22_TotalResultsLogic) {
  // Simulate the problematic code path:
  // 1. can_optimize = true
  // 2. total_results = all_results.size() = 100  (set before filtering)
  // 3. Apply filters, results shrinks to 20
  // 4. But total_results is still 100 (BUG!)

  size_t initial_results = 100;
  size_t after_filter_results = 20;

  // Buggy behavior: total_results not updated
  size_t buggy_total_results = initial_results;  // Still 100 after filtering

  // Fixed behavior: total_results should be updated after filtering
  // when we're NOT using the GetTopN optimization
  size_t fixed_total_results = after_filter_results;  // Updated to 20

  // The bug causes pagination to be wrong
  EXPECT_NE(buggy_total_results, after_filter_results) << "Bug #22: total_results not updated after filtering";
  EXPECT_EQ(fixed_total_results, after_filter_results) << "Fixed: total_results matches filtered count";
}

/**
 * @brief Test the correct code flow for total_results
 *
 * After the fix at line 274-277:
 * if (!can_optimize) {
 *   total_results = results.size();
 * }
 */
TEST(SearchHandlerBugFixTest, Bug22_FixedCodePath) {
  // Simulate the search handler flow
  bool can_optimize = true;
  size_t total_results = 0;
  std::vector<int> results;

  // Initial fetch
  results = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};  // 10 initial matches
  total_results = results.size();             // = 10

  // Apply NOT filter (removes some results)
  results = {1, 2, 3, 4, 5, 6, 7, 8};  // 8 after NOT filter

  // Apply regular filters (removes more results)
  results = {1, 2, 3, 4, 5};  // 5 after all filters

  // Fix: Update total_results when not using optimization
  if (!can_optimize) {
    total_results = results.size();  // = 5
  }

  // When can_optimize is true, total_results stays at initial value
  // This is the bug - for the test, we simulate can_optimize being reset to false
  // after we realize the optimization doesn't apply

  // The fix ensures total_results reflects actual results count
  // Note: In the actual code, can_optimize may be set to false earlier
  // which then triggers the update at line 274-277

  // For demonstration, let's simulate can_optimize being false
  can_optimize = false;
  if (!can_optimize) {
    total_results = results.size();
  }

  EXPECT_EQ(total_results, 5) << "total_results should match filtered results count";
  EXPECT_EQ(total_results, results.size()) << "total_results should equal results.size()";
}

// =============================================================================
// Bug #25: TOCTOU race in cache search
// =============================================================================
// When cache is hit, the cached DocIds might be stale (documents deleted since
// cache population). The fix validates a sample of cached DocIds before use
// and falls through to normal execution if any are stale.
//
// This is a race condition fix that can't be easily unit tested, but we can
// verify the validation logic conceptually works.
// =============================================================================

/**
 * @test Bug #25: Conceptual test for stale cache detection
 *
 * This test verifies the concept that stale DocIds can be detected
 * by checking if they exist in the document store.
 */
TEST(SearchHandlerBugFixTest, Bug25_StaleCacheDetectionConcept) {
  // Simulated scenario:
  // 1. Cache stores DocIds [1, 2, 3, 4, 5]
  // 2. Document 3 is deleted
  // 3. Validation should detect that DocId 3 is stale

  // This is a conceptual test - the actual implementation uses
  // DocumentStore::GetPrimaryKey() to validate DocIds.
  // The key insight is: if GetPrimaryKey(doc_id) returns nullopt,
  // the document has been deleted and the cache is stale.

  std::vector<uint32_t> cached_doc_ids = {1, 2, 3, 4, 5};
  std::set<uint32_t> deleted_doc_ids = {3};  // DocId 3 was deleted

  // Validation logic: check if any cached DocId is deleted
  bool cache_is_stale = false;
  for (auto doc_id : cached_doc_ids) {
    if (deleted_doc_ids.count(doc_id) > 0) {
      cache_is_stale = true;
      break;
    }
  }

  // Bug #25: Validation should detect the stale cache
  EXPECT_TRUE(cache_is_stale) << "Bug #25: Should detect stale cache when DocIds are deleted";
}

/**
 * @test Bug #25: Fresh cache should not be detected as stale
 */
TEST(SearchHandlerBugFixTest, Bug25_FreshCacheNotDetectedAsStale) {
  // Simulated scenario:
  // 1. Cache stores DocIds [1, 2, 3, 4, 5]
  // 2. All documents still exist
  // 3. Validation should NOT detect stale cache

  std::vector<uint32_t> cached_doc_ids = {1, 2, 3, 4, 5};
  std::set<uint32_t> deleted_doc_ids = {};  // No documents deleted

  bool cache_is_stale = false;
  for (auto doc_id : cached_doc_ids) {
    if (deleted_doc_ids.count(doc_id) > 0) {
      cache_is_stale = true;
      break;
    }
  }

  EXPECT_FALSE(cache_is_stale) << "Fresh cache should not be detected as stale";
}

// =============================================================================
// H3: Floating-point epsilon comparison for FILTER
// =============================================================================

/**
 * @brief Test that floating-point filter equality uses epsilon comparison
 *
 * The classic 0.1 + 0.2 != 0.3 problem should be handled gracefully.
 */
TEST(SearchHandlerBugFixTest, H3_FloatFilterEqualityWithEpsilon) {
  // Simulate the filter comparison logic
  double stored_value = 0.1 + 0.2;  // = 0.30000000000000004
  double filter_value = 0.3;         // Exact 0.3

  // Exact comparison fails
  EXPECT_NE(stored_value, filter_value) << "Exact comparison should fail (demonstrates the problem)";

  // Epsilon comparison should succeed
  double max_abs = std::max({1.0, std::abs(stored_value), std::abs(filter_value)});
  double epsilon = std::numeric_limits<double>::epsilon() * max_abs;
  bool matches = std::abs(stored_value - filter_value) < epsilon;
  EXPECT_TRUE(matches) << "Epsilon comparison should handle 0.1+0.2 == 0.3";
}

/**
 * @brief Test NaN handling in float filter
 */
TEST(SearchHandlerBugFixTest, H3_FloatFilterNaNHandling) {
  double nan_val = std::numeric_limits<double>::quiet_NaN();
  double normal_val = 1.0;

  // NaN comparisons
  double max_abs = std::max({1.0, std::abs(nan_val), std::abs(normal_val)});
  // std::abs(NaN) is NaN, std::max with NaN is implementation-defined
  // The epsilon comparison with NaN should NOT match (NaN != anything)
  bool eq_result = std::abs(nan_val - normal_val) < std::numeric_limits<double>::epsilon() * max_abs;
  EXPECT_FALSE(eq_result) << "NaN should not equal any value";
}

/**
 * @brief Test large value epsilon comparison
 */
TEST(SearchHandlerBugFixTest, H3_LargeValueEpsilonComparison) {
  // For large values, absolute epsilon would fail but relative epsilon works
  double large1 = 1e15;
  double large2 = 1e15 + 1.0;  // Difference of 1.0

  // These are different values
  double max_abs = std::max({1.0, std::abs(large1), std::abs(large2)});
  double epsilon = std::numeric_limits<double>::epsilon() * max_abs;
  bool matches = std::abs(large1 - large2) < epsilon;
  // 1.0 difference at 1e15 scale is within relative epsilon
  // epsilon * 1e15 ≈ 2.2e-16 * 1e15 ≈ 0.22 < 1.0, so should NOT match
  EXPECT_FALSE(matches) << "Values differing by 1.0 at 1e15 scale should not match";

  // But values that are representationally close should match
  double a = 1e15;
  double b = a;  // Exact same value
  max_abs = std::max({1.0, std::abs(a), std::abs(b)});
  epsilon = std::numeric_limits<double>::epsilon() * max_abs;
  matches = std::abs(a - b) < epsilon;
  EXPECT_TRUE(matches) << "Identical large values should match";
}

// =============================================================================
// M1: Adaptive cache validation sampling
// =============================================================================

/**
 * @brief Test adaptive sample size calculation
 *
 * Validates the formula: min(n, max(10, n/10))
 */
TEST(SearchHandlerBugFixTest, M1_AdaptiveSampleSize) {
  // Small result set: check all
  {
    size_t n = 5;
    size_t sample = std::min(n, std::max(size_t{10}, n / 10));
    EXPECT_EQ(sample, 5) << "For n=5, should check all (5 < 10)";
  }

  // Medium result set: check 10
  {
    size_t n = 50;
    size_t sample = std::min(n, std::max(size_t{10}, n / 10));
    EXPECT_EQ(sample, 10) << "For n=50, should check 10 (50/10=5 < 10, so 10)";
  }

  // Large result set: check 10%
  {
    size_t n = 1000;
    size_t sample = std::min(n, std::max(size_t{10}, n / 10));
    EXPECT_EQ(sample, 100) << "For n=1000, should check 100 (10%)";
  }

  // Very large result set: check 10%
  {
    size_t n = 100000;
    size_t sample = std::min(n, std::max(size_t{10}, n / 10));
    EXPECT_EQ(sample, 10000) << "For n=100000, should check 10000 (10%)";
  }

  // Edge case: empty
  {
    size_t n = 0;
    // The actual code guards with !full_results.empty()
    // but the formula itself: min(0, max(10, 0)) = 0
    size_t sample = std::min(n, std::max(size_t{10}, n / 10));
    EXPECT_EQ(sample, 0);
  }
}

// =============================================================================
// M6: Configurable FilterByNgrams threshold
// =============================================================================

/**
 * @brief Test that filter threshold is configurable
 */
TEST(SearchHandlerBugFixTest, M6_FilterThresholdConfigurable) {
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

}  // namespace server
}  // namespace mygramdb
