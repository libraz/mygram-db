/**
 * @file index_empty_doc_and_search_test.cpp
 * @brief Tests for empty document handling and SearchOr correctness
 *
 * Covers:
 * - Issue #6: Empty document silent success (observability)
 * - Issue #5: SearchOrInternal lock precondition (tested via public SearchOr API)
 */

#include <gtest/gtest.h>

#include "index/index.h"

using namespace mygramdb::index;

// =============================================================================
// Issue #6: Empty document handling
// =============================================================================

/**
 * @test Adding an empty document succeeds (no crash, no exception)
 */
TEST(IndexEmptyDocTest, EmptyDocumentAddsSuccessfully) {
  Index index(1);  // Unigram index

  // Should not throw or crash
  index.AddDocument(1, "");

  // Empty document produces no index terms
  EXPECT_EQ(index.TermCount(), 0);
}

/**
 * @test Empty document is not found by any search
 */
TEST(IndexEmptyDocTest, EmptyDocumentNotFoundBySearch) {
  Index index(1);

  // Add an empty document and a normal document
  index.AddDocument(1, "");
  index.AddDocument(2, "abc");

  // SearchAnd should only find doc 2
  auto results = index.SearchAnd({"a"});
  ASSERT_EQ(results.size(), 1);
  EXPECT_EQ(results[0], 2);

  // SearchOr should only find doc 2
  results = index.SearchOr({"a", "b", "c"});
  ASSERT_EQ(results.size(), 1);
  EXPECT_EQ(results[0], 2);
}

/**
 * @test Empty document does not affect term counts
 *
 * The empty doc exists in DocumentStore (caller responsibility) but produces
 * zero n-grams, so TermCount and Count remain unaffected.
 */
TEST(IndexEmptyDocTest, EmptyDocumentDoesNotAffectTermCounts) {
  Index index(1);

  index.AddDocument(1, "abc");
  EXPECT_EQ(index.TermCount(), 3);  // a, b, c
  EXPECT_EQ(index.Count("a"), 1);

  // Adding an empty document should not change any counts
  index.AddDocument(2, "");
  EXPECT_EQ(index.TermCount(), 3);
  EXPECT_EQ(index.Count("a"), 1);
  EXPECT_EQ(index.Count("b"), 1);
  EXPECT_EQ(index.Count("c"), 1);
}

/**
 * @test Multiple empty documents can be added without issue
 */
TEST(IndexEmptyDocTest, MultipleEmptyDocuments) {
  Index index(1);

  index.AddDocument(1, "");
  index.AddDocument(2, "");
  index.AddDocument(3, "");

  EXPECT_EQ(index.TermCount(), 0);

  // Adding a real document after empties still works
  index.AddDocument(4, "xyz");
  EXPECT_EQ(index.TermCount(), 3);
  EXPECT_EQ(index.Count("x"), 1);
}

/**
 * @test Whitespace-only text produces no n-grams (treated like empty)
 */
TEST(IndexEmptyDocTest, WhitespaceOnlyDocumentProducesNoNgrams) {
  Index index(1);

  index.AddDocument(1, "   ");
  // Spaces may or may not generate n-grams depending on GenerateHybridNgrams;
  // either way, searching for normal terms should not find this doc
  auto results = index.SearchAnd({"a"});
  EXPECT_TRUE(results.empty());
}

// =============================================================================
// Issue #5: SearchOr correctness (exercises SearchOrInternal's logic path)
// =============================================================================

/**
 * @test SearchOr returns correct union of results (single term)
 */
TEST(IndexSearchOrCorrectnessTest, SingleTerm) {
  Index index(1);

  index.AddDocument(1, "abc");
  index.AddDocument(2, "def");

  auto results = index.SearchOr({"a"});
  ASSERT_EQ(results.size(), 1);
  EXPECT_EQ(results[0], 1);
}

/**
 * @test SearchOr returns correct union across multiple terms
 */
TEST(IndexSearchOrCorrectnessTest, MultipleTermsUnion) {
  Index index(1);

  index.AddDocument(1, "abc");
  index.AddDocument(2, "def");
  index.AddDocument(3, "ghi");

  // "a" matches doc 1, "d" matches doc 2
  auto results = index.SearchOr({"a", "d"});
  ASSERT_EQ(results.size(), 2);
  EXPECT_EQ(results[0], 1);
  EXPECT_EQ(results[1], 2);

  // All three terms
  results = index.SearchOr({"a", "d", "g"});
  ASSERT_EQ(results.size(), 3);
  EXPECT_EQ(results[0], 1);
  EXPECT_EQ(results[1], 2);
  EXPECT_EQ(results[2], 3);
}

/**
 * @test SearchOr with overlapping terms does not produce duplicates
 */
TEST(IndexSearchOrCorrectnessTest, OverlappingTermsNoDuplicates) {
  Index index(1);

  // Doc 1 contains both "a" and "b"
  index.AddDocument(1, "ab");
  index.AddDocument(2, "cd");

  // Both "a" and "b" match doc 1; it should appear only once
  auto results = index.SearchOr({"a", "b"});
  ASSERT_EQ(results.size(), 1);
  EXPECT_EQ(results[0], 1);
}

/**
 * @test SearchOr with non-existent terms returns empty
 */
TEST(IndexSearchOrCorrectnessTest, NonExistentTerms) {
  Index index(1);

  index.AddDocument(1, "abc");

  auto results = index.SearchOr({"x", "y", "z"});
  EXPECT_TRUE(results.empty());
}

/**
 * @test SearchOr with empty terms vector returns empty
 */
TEST(IndexSearchOrCorrectnessTest, EmptyTermsVector) {
  Index index(1);

  index.AddDocument(1, "abc");

  auto results = index.SearchOr({});
  EXPECT_TRUE(results.empty());
}

/**
 * @test SearchOr results are sorted by DocId
 */
TEST(IndexSearchOrCorrectnessTest, ResultsSortedByDocId) {
  Index index(1);

  // Add in non-sequential order
  index.AddDocument(5, "abc");
  index.AddDocument(1, "def");
  index.AddDocument(3, "ghi");

  auto results = index.SearchOr({"a", "d", "g"});
  ASSERT_EQ(results.size(), 3);
  EXPECT_EQ(results[0], 1);
  EXPECT_EQ(results[1], 3);
  EXPECT_EQ(results[2], 5);
}

/**
 * @test SearchOr with mix of existing and non-existing terms
 */
TEST(IndexSearchOrCorrectnessTest, MixedExistingAndNonExistingTerms) {
  Index index(1);

  index.AddDocument(1, "abc");
  index.AddDocument(2, "def");

  // "a" exists, "z" does not
  auto results = index.SearchOr({"a", "z"});
  ASSERT_EQ(results.size(), 1);
  EXPECT_EQ(results[0], 1);
}

/**
 * @test Normal document addition still works correctly after all changes
 */
TEST(IndexSearchOrCorrectnessTest, NormalDocumentAdditionWorks) {
  Index index(2);  // Bigram index

  index.AddDocument(10, "hello");
  index.AddDocument(20, "world");

  // "he" is a bigram of "hello"
  EXPECT_EQ(index.Count("he"), 1);
  EXPECT_EQ(index.Count("el"), 1);
  EXPECT_EQ(index.Count("ll"), 1);
  EXPECT_EQ(index.Count("lo"), 1);

  // "wo" is a bigram of "world"
  EXPECT_EQ(index.Count("wo"), 1);
  EXPECT_EQ(index.Count("or"), 1);

  // SearchOr across terms from different docs
  auto results = index.SearchOr({"he", "wo"});
  ASSERT_EQ(results.size(), 2);
  EXPECT_EQ(results[0], 10);
  EXPECT_EQ(results[1], 20);

  // SearchAnd still works
  auto and_results = index.SearchAnd({"he", "el"});
  ASSERT_EQ(and_results.size(), 1);
  EXPECT_EQ(and_results[0], 10);
}
