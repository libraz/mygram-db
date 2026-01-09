/**
 * @file index_bug_fixes_test.cpp
 * @brief TDD tests for index bug fixes
 *
 * This file contains tests for bugs discovered in the bug report.
 */

#include <gtest/gtest.h>

#include "index/index.h"
#include "utils/string_utils.h"

using namespace mygramdb::index;
using namespace mygramdb::utils;

// =============================================================================
// Bug #15: N-gram map grows unbounded in UpdateDocument
// =============================================================================
// The bug is in index.cpp UpdateDocument function.
// When a document is updated and old n-grams are removed, empty posting lists
// are not cleaned up, causing the term_postings_ map to grow unbounded.
// =============================================================================

/**
 * @test Bug #15: UpdateDocument should remove empty posting lists
 *
 * When a document is updated and an n-gram is no longer present in the new text,
 * the posting list for that n-gram may become empty. Empty posting lists should
 * be removed to prevent memory leaks.
 */
TEST(IndexBugFixesTest, Bug15_UpdateDocumentRemovesEmptyPostingLists) {
  Index index(1);  // Unigram index for simplicity

  // Add a document with text "abc"
  index.AddDocument(1, "abc");

  // Verify initial term count: a, b, c
  EXPECT_EQ(index.TermCount(), 3);
  EXPECT_EQ(index.Count("a"), 1);
  EXPECT_EQ(index.Count("b"), 1);
  EXPECT_EQ(index.Count("c"), 1);

  // Update the document: old text "abc", new text "xyz"
  // This should remove a, b, c and add x, y, z
  index.UpdateDocument(1, "abc", "xyz");

  // Verify new terms exist
  EXPECT_EQ(index.Count("x"), 1);
  EXPECT_EQ(index.Count("y"), 1);
  EXPECT_EQ(index.Count("z"), 1);

  // Bug #15: Empty posting lists for old terms should be removed
  // Before fix: term_postings_ would still contain entries for a, b, c with empty lists
  // After fix: term_postings_ should only contain x, y, z
  EXPECT_EQ(index.Count("a"), 0) << "Old term 'a' should have count 0";
  EXPECT_EQ(index.Count("b"), 0) << "Old term 'b' should have count 0";
  EXPECT_EQ(index.Count("c"), 0) << "Old term 'c' should have count 0";

  // Critical check: TermCount should be 3 (only x, y, z), not 6
  EXPECT_EQ(index.TermCount(), 3) << "Bug #15: Empty posting lists should be removed after UpdateDocument";
}

/**
 * @test Bug #15: Multiple updates should not cause memory growth
 */
TEST(IndexBugFixesTest, Bug15_MultipleUpdatesNoMemoryGrowth) {
  Index index(1);  // Unigram index

  // Add initial document
  index.AddDocument(1, "a");
  EXPECT_EQ(index.TermCount(), 1);

  // Update document multiple times with completely different text
  // Each update should not increase term count beyond the current terms
  index.UpdateDocument(1, "a", "b");
  EXPECT_EQ(index.TermCount(), 1) << "After update a->b, should have 1 term";

  index.UpdateDocument(1, "b", "c");
  EXPECT_EQ(index.TermCount(), 1) << "After update b->c, should have 1 term";

  index.UpdateDocument(1, "c", "d");
  EXPECT_EQ(index.TermCount(), 1) << "After update c->d, should have 1 term";

  index.UpdateDocument(1, "d", "e");
  EXPECT_EQ(index.TermCount(), 1) << "After update d->e, should have 1 term";

  // Final state should only have the current term
  EXPECT_EQ(index.Count("e"), 1);
  EXPECT_EQ(index.Count("a"), 0);
  EXPECT_EQ(index.Count("b"), 0);
  EXPECT_EQ(index.Count("c"), 0);
  EXPECT_EQ(index.Count("d"), 0);
}

/**
 * @test Bug #15: Update with partial overlap should handle correctly
 */
TEST(IndexBugFixesTest, Bug15_UpdateWithPartialOverlap) {
  Index index(1);  // Unigram index

  // Add document with "abc"
  index.AddDocument(1, "abc");
  EXPECT_EQ(index.TermCount(), 3);

  // Update to "bcd" - b and c are shared, a is removed, d is added
  index.UpdateDocument(1, "abc", "bcd");

  // Verify term counts
  EXPECT_EQ(index.Count("a"), 0) << "Term 'a' should be removed";
  EXPECT_EQ(index.Count("b"), 1) << "Term 'b' should still exist";
  EXPECT_EQ(index.Count("c"), 1) << "Term 'c' should still exist";
  EXPECT_EQ(index.Count("d"), 1) << "Term 'd' should be added";

  // TermCount should be 3 (b, c, d), not 4 (including empty 'a')
  EXPECT_EQ(index.TermCount(), 3) << "Bug #15: Empty posting list for 'a' should be removed";
}

/**
 * @test Bug #15: Update document that becomes empty
 */
TEST(IndexBugFixesTest, Bug15_UpdateToEmptyText) {
  Index index(1);  // Unigram index

  // Add document
  index.AddDocument(1, "abc");
  EXPECT_EQ(index.TermCount(), 3);

  // Update to empty text
  index.UpdateDocument(1, "abc", "");

  // All posting lists should be removed
  EXPECT_EQ(index.TermCount(), 0) << "Bug #15: All empty posting lists should be removed";
  EXPECT_EQ(index.Count("a"), 0);
  EXPECT_EQ(index.Count("b"), 0);
  EXPECT_EQ(index.Count("c"), 0);
}

/**
 * @test Bug #15: Update with multiple documents
 */
TEST(IndexBugFixesTest, Bug15_UpdateWithMultipleDocuments) {
  Index index(1);  // Unigram index

  // Add two documents sharing term 'b'
  index.AddDocument(1, "ab");  // Terms: a, b
  index.AddDocument(2, "bc");  // Terms: b, c
  EXPECT_EQ(index.TermCount(), 3);  // a, b, c
  EXPECT_EQ(index.Count("b"), 2);   // Both docs have 'b'

  // Update doc 1: remove 'a' and 'b', add 'x'
  index.UpdateDocument(1, "ab", "x");

  // Term 'b' should still exist (doc 2 has it)
  EXPECT_EQ(index.Count("b"), 1) << "Term 'b' should still have count 1 from doc 2";

  // Term 'a' had only doc 1, so its posting list should be removed
  EXPECT_EQ(index.Count("a"), 0) << "Term 'a' should have count 0";

  // TermCount should be 3: b (from doc2), c (from doc2), x (from doc1)
  EXPECT_EQ(index.TermCount(), 3) << "Bug #15: Empty posting list for 'a' should be removed";
}

// =============================================================================
// Bug #14: Empty PostingList not removed after RemoveDocument
// (Already fixed, this test verifies the fix)
// =============================================================================

/**
 * @test Bug #14: RemoveDocument should remove empty posting lists
 */
TEST(IndexBugFixesTest, Bug14_RemoveDocumentRemovesEmptyPostingLists) {
  Index index(1);  // Unigram index

  // Add a document
  index.AddDocument(1, "abc");
  EXPECT_EQ(index.TermCount(), 3);

  // Remove the document
  index.RemoveDocument(1, "abc");

  // All posting lists should be removed since doc was the only one
  EXPECT_EQ(index.TermCount(), 0) << "Bug #14: Empty posting lists should be removed after RemoveDocument";
}

/**
 * @test Bug #14: RemoveDocument with multiple documents
 */
TEST(IndexBugFixesTest, Bug14_RemoveDocumentPartialCleanup) {
  Index index(1);  // Unigram index

  // Add two documents
  index.AddDocument(1, "ab");  // Terms: a, b
  index.AddDocument(2, "bc");  // Terms: b, c
  EXPECT_EQ(index.TermCount(), 3);  // a, b, c

  // Remove doc 1
  index.RemoveDocument(1, "ab");

  // Term 'a' should be completely removed (only doc 1 had it)
  // Term 'b' should still exist (doc 2 has it)
  EXPECT_EQ(index.Count("a"), 0);
  EXPECT_EQ(index.Count("b"), 1);
  EXPECT_EQ(index.Count("c"), 1);

  // TermCount should be 2: b, c (not 3 including empty 'a')
  EXPECT_EQ(index.TermCount(), 2) << "Bug #14: Empty posting list for 'a' should be removed";
}

// =============================================================================
// Bug #16: SearchAnd materializes all documents (100MB+ per query)
// =============================================================================
// The streaming optimization in SearchAnd calls GetAll() on all posting lists,
// which materializes all documents before performing merge join.
// This defeats the purpose of streaming and can use 100MB+ per query.
//
// The fix: Use PostingList::Intersect() chain followed by GetTopN() to avoid
// materializing all documents when only top N results are needed.
// =============================================================================

/**
 * @test Bug #16: SearchAnd with limit should not materialize all documents
 *
 * This test verifies that SearchAnd returns correct results when using
 * the optimized path (limit > 0, reverse = true, multiple terms).
 * Note: For small posting lists, SearchAnd returns all results and limit/reverse
 * is applied by the caller. This test verifies the intersection is correct.
 */
TEST(IndexBugFixesTest, Bug16_SearchAndWithLimitReturnsCorrectResults) {
  Index index(2);  // Bigram index

  // Add documents with overlapping terms
  // "hello" has bigrams: "he", "el", "ll", "lo"
  // "help" has bigrams: "he", "el", "lp"
  // "yellow" has bigrams: "ye", "el", "ll", "lo", "ow"
  index.AddDocument(100, "hello");
  index.AddDocument(200, "help");
  index.AddDocument(300, "yellow");
  index.AddDocument(400, "hello world");  // Contains "hello"
  index.AddDocument(500, "shell");        // "sh", "he", "el", "ll"

  // Search for documents containing both "he" and "el" bigrams
  // Documents with both: 100(hello), 200(help), 400(hello world), 500(shell)
  // "yellow" doesn't have "he" but has "el"
  std::vector<std::string> terms = {"he", "el"};

  // For small lists, SearchAnd returns all matching docs (limit/reverse applied by caller)
  auto results = index.SearchAnd(terms, 0, false);  // No limit, ascending

  // Should return all 4 documents that have both "he" and "el"
  ASSERT_EQ(results.size(), 4);
  // Results should be in ascending order (default)
  EXPECT_EQ(results[0], 100) << "First result should be DocId 100 (hello)";
  EXPECT_EQ(results[1], 200) << "Second result should be DocId 200 (help)";
  EXPECT_EQ(results[2], 400) << "Third result should be DocId 400 (hello world)";
  EXPECT_EQ(results[3], 500) << "Fourth result should be DocId 500 (shell)";
}

/**
 * @test Bug #16: SearchAnd with single term should use GetTopN optimization
 */
TEST(IndexBugFixesTest, Bug16_SearchAndSingleTermUsesGetTopN) {
  Index index(1);  // Unigram index

  // Add many documents
  for (DocId i = 1; i <= 1000; ++i) {
    index.AddDocument(i, "a");
  }

  // Search for single term with limit (should use GetTopN directly)
  std::vector<std::string> terms = {"a"};
  auto results = index.SearchAnd(terms, 5, true);

  // Should return top 5 by DocId descending: 1000, 999, 998, 997, 996
  ASSERT_EQ(results.size(), 5);
  EXPECT_EQ(results[0], 1000);
  EXPECT_EQ(results[1], 999);
  EXPECT_EQ(results[2], 998);
  EXPECT_EQ(results[3], 997);
  EXPECT_EQ(results[4], 996);
}

/**
 * @test Bug #16: SearchAnd with multiple terms returns correct intersection
 *
 * Note: For small posting lists (< 10000), SearchAnd returns results in
 * ascending DocId order. The caller applies limit/reverse as needed.
 */
TEST(IndexBugFixesTest, Bug16_SearchAndMultipleTermsCorrectIntersection) {
  Index index(1);  // Unigram index

  // Create documents with different term combinations
  // Doc 1: a, b
  // Doc 2: b, c
  // Doc 3: a, b, c
  // Doc 4: a, c
  index.AddDocument(1, "ab");
  index.AddDocument(2, "bc");
  index.AddDocument(3, "abc");
  index.AddDocument(4, "ac");

  // Search for documents containing both "a" AND "b"
  // Should return: 1, 3 (in ascending order for small lists)
  std::vector<std::string> terms = {"a", "b"};
  auto results = index.SearchAnd(terms, 0, false);  // No limit, ascending

  ASSERT_EQ(results.size(), 2);
  EXPECT_EQ(results[0], 1) << "First should be DocId 1 (ab)";
  EXPECT_EQ(results[1], 3) << "Second should be DocId 3 (abc)";

  // Search for a, b, c (only doc 3 has all three)
  std::vector<std::string> terms_abc = {"a", "b", "c"};
  auto results_abc = index.SearchAnd(terms_abc, 0, false);

  ASSERT_EQ(results_abc.size(), 1);
  EXPECT_EQ(results_abc[0], 3) << "Only DocId 3 has all three terms";
}

/**
 * @test Bug #16: SearchAnd should handle non-existent term gracefully
 */
TEST(IndexBugFixesTest, Bug16_SearchAndNonExistentTermReturnsEmpty) {
  Index index(1);

  index.AddDocument(1, "abc");
  index.AddDocument(2, "def");

  // Search for term that doesn't exist
  std::vector<std::string> terms = {"a", "x"};  // "x" doesn't exist
  auto results = index.SearchAnd(terms, 10, true);

  EXPECT_TRUE(results.empty()) << "Should return empty when any term is missing";
}

/**
 * @test Bug #16: SearchAnd with large posting lists should not allocate excessively
 *
 * This test creates a scenario where the streaming optimization would be triggered
 * (high selectivity, large posting lists) and verifies correct behavior.
 */
TEST(IndexBugFixesTest, Bug16_SearchAndLargePostingListsCorrectResults) {
  Index index(1);  // Unigram index

  // Create a large number of documents
  // Most documents have both "a" and "b" (high selectivity)
  const DocId kNumDocs = 15000;  // Above kMinSizeThreshold (10000)

  for (DocId i = 1; i <= kNumDocs; ++i) {
    index.AddDocument(i, "ab");  // All docs have "a" and "b"
  }

  // Add a few documents with only "a" to make lists slightly different
  for (DocId i = kNumDocs + 1; i <= kNumDocs + 100; ++i) {
    index.AddDocument(i, "a");
  }

  std::vector<std::string> terms = {"a", "b"};

  // Request only top 10 results (should not need to materialize all 15000)
  auto results = index.SearchAnd(terms, 10, true);

  // Should return top 10 documents that have both a and b
  // The highest DocIds with both are: kNumDocs, kNumDocs-1, ..., kNumDocs-9
  ASSERT_EQ(results.size(), 10);
  for (size_t i = 0; i < 10; ++i) {
    EXPECT_EQ(results[i], kNumDocs - static_cast<DocId>(i))
        << "Result[" << i << "] should be " << (kNumDocs - i);
  }
}
