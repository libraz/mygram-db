/**
 * @file index_basic_test.cpp
 * @brief Unit tests for n-gram inverted index - Basic operations
 */

#include <gtest/gtest.h>

#include <unordered_set>

#include "index/index.h"
#include "utils/string_utils.h"

using namespace mygramdb::index;
using namespace mygram::utils;

/**
 * @brief Test basic document addition
 */
TEST(IndexTest, AddDocument) {
  Index index(1);  // Unigram index

  // Add document with normalized text
  std::string text = NormalizeText("abc", true, "keep", false);
  index.AddDocument(1, text);

  // Verify term count
  EXPECT_EQ(index.TermCount(), 3);  // a, b, c

  // Verify each term exists
  EXPECT_EQ(index.Count("a"), 1);
  EXPECT_EQ(index.Count("b"), 1);
  EXPECT_EQ(index.Count("c"), 1);
}

/**
 * @brief Test Japanese document addition
 */
TEST(IndexTest, AddDocumentJapanese) {
  Index index(1);  // Unigram index

  // Add Japanese document
  std::string text = NormalizeText("ライブ", true, "keep", false);
  index.AddDocument(1, text);

  // Verify term count (ラ, イ, ブ)
  EXPECT_EQ(index.TermCount(), 3);

  // Generate expected terms
  auto terms = GenerateNgrams(text, 1);
  for (const auto& term : terms) {
    EXPECT_EQ(index.Count(term), 1);
  }
}

/**
 * @brief Test multiple documents
 */
TEST(IndexTest, AddMultipleDocuments) {
  Index index(1);

  // Add multiple documents
  index.AddDocument(1, "abc");
  index.AddDocument(2, "bcd");
  index.AddDocument(3, "cde");

  // Verify term counts
  EXPECT_EQ(index.Count("a"), 1);  // Only in doc 1
  EXPECT_EQ(index.Count("b"), 2);  // In docs 1, 2
  EXPECT_EQ(index.Count("c"), 3);  // In docs 1, 2, 3
  EXPECT_EQ(index.Count("d"), 2);  // In docs 2, 3
  EXPECT_EQ(index.Count("e"), 1);  // Only in doc 3
}

/**
 * @brief Test duplicate terms in same document
 */
TEST(IndexTest, DuplicateTermsInDocument) {
  Index index(1);

  // Add document with duplicate characters
  index.AddDocument(1, "aaa");

  // Should only count once per document
  EXPECT_EQ(index.Count("a"), 1);
  EXPECT_EQ(index.TermCount(), 1);
}

/**
 * @brief Test document removal
 */
TEST(IndexTest, RemoveDocument) {
  Index index(1);

  // Add documents
  index.AddDocument(1, "abc");
  index.AddDocument(2, "bcd");

  EXPECT_EQ(index.Count("a"), 1);
  EXPECT_EQ(index.Count("b"), 2);
  EXPECT_EQ(index.Count("c"), 2);

  // Remove document 1
  index.RemoveDocument(1, "abc");

  EXPECT_EQ(index.Count("a"), 0);
  EXPECT_EQ(index.Count("b"), 1);
  EXPECT_EQ(index.Count("c"), 1);
  EXPECT_EQ(index.Count("d"), 1);
}

/**
 * @brief Test document update
 */
TEST(IndexTest, UpdateDocument) {
  Index index(1);

  // Add document
  index.AddDocument(1, "abc");

  EXPECT_EQ(index.Count("a"), 1);
  EXPECT_EQ(index.Count("b"), 1);
  EXPECT_EQ(index.Count("c"), 1);
  EXPECT_EQ(index.Count("d"), 0);

  // Update document
  index.UpdateDocument(1, "abc", "bcd");

  EXPECT_EQ(index.Count("a"), 0);  // Removed
  EXPECT_EQ(index.Count("b"), 1);  // Kept
  EXPECT_EQ(index.Count("c"), 1);  // Kept
  EXPECT_EQ(index.Count("d"), 1);  // Added
}

/**
 * @brief Ensure UpdateDocument keeps SearchAnd(limit, reverse) results in sync
 */
TEST(IndexTest, UpdateDocumentMaintainsTopNOrdering) {
  Index index(1);

  constexpr DocId kBaseDocs = 512;
  constexpr size_t kTopCount = 3;

  // All initial documents contain the term "a"
  for (DocId doc_id = 1; doc_id <= kBaseDocs; ++doc_id) {
    index.AddDocument(doc_id, "aaaa");
  }

  // Extra document intentionally lacks the term so it is not part of the posting list yet
  const DocId extra_doc = kBaseDocs + 1;
  index.AddDocument(extra_doc, "zzzz");

  auto expect_top_docs = [&](const std::vector<DocId>& expected) {
    auto results = index.SearchAnd({"a"}, kTopCount, true);
    ASSERT_EQ(results.size(), kTopCount);
    for (size_t i = 0; i < expected.size(); ++i) {
      EXPECT_EQ(results[i], expected[i]);
    }
  };

  expect_top_docs({kBaseDocs, kBaseDocs - 1, kBaseDocs - 2});
  EXPECT_EQ(index.Count("a"), kBaseDocs);

  // Remove highest doc_id from the posting list via update
  index.UpdateDocument(kBaseDocs, "aaaa", "zzzz");
  expect_top_docs({kBaseDocs - 1, kBaseDocs - 2, kBaseDocs - 3});
  EXPECT_EQ(index.Count("a"), kBaseDocs - 1);

  // Add the extra document into the posting list via update and ensure it becomes the new top result
  index.UpdateDocument(extra_doc, "zzzz", "aaaa");
  expect_top_docs({extra_doc, kBaseDocs - 1, kBaseDocs - 2});
  EXPECT_EQ(index.Count("a"), kBaseDocs);
}

/**
 * @brief Test index optimization
 */
TEST(IndexTest, Optimize) {
  Index index(1);

  // Add many documents to trigger optimization
  for (int i = 1; i <= 100; ++i) {
    index.AddDocument(i, "abc");
  }

  // Optimize (should convert to Roaring bitmap for high-density term "a", "b", "c")
  index.Optimize(100);

  // Memory usage might increase or decrease depending on density
  // Just verify it completes without error
  size_t after_usage = index.MemoryUsage();
  EXPECT_GT(after_usage, 0);
}

/**
 * @brief Test large document set
 */
TEST(IndexTest, LargeDocumentSet) {
  Index index(1);

  // Add 1000 documents
  for (int i = 1; i <= 1000; ++i) {
    std::string text = "doc" + std::to_string(i % 10);
    index.AddDocument(i, text);
  }

  // Verify search works correctly
  auto results = index.SearchAnd({"d"});
  EXPECT_EQ(results.size(), 1000);  // All documents contain "d"

  results = index.SearchAnd({"0"});
  EXPECT_EQ(results.size(), 100);  // Only doc0, doc10, doc20, ...
}

/**
 * @brief Test document ID ordering
 */
TEST(IndexTest, DocumentIdOrdering) {
  Index index(1);

  // Add documents in non-sequential order
  index.AddDocument(3, "abc");
  index.AddDocument(1, "abc");
  index.AddDocument(2, "abc");

  // Results should be sorted
  auto results = index.SearchAnd({"a"});
  EXPECT_EQ(results.size(), 3);
  EXPECT_EQ(results[0], 1);
  EXPECT_EQ(results[1], 2);
  EXPECT_EQ(results[2], 3);
}

// =============================================================================
// UpdateDocument empty posting list cleanup
// =============================================================================

/**
 * @test UpdateDocument should remove empty posting lists
 *
 * When a document is updated and an n-gram is no longer present in the new text,
 * the posting list for that n-gram may become empty. Empty posting lists should
 * be removed to prevent memory leaks.
 */
TEST(IndexTest, UpdateDocumentRemovesEmptyPostingLists) {
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

  // Empty posting lists for old terms should be removed
  // Before fix: term_postings_ would still contain entries for a, b, c with empty lists
  // After fix: term_postings_ should only contain x, y, z
  EXPECT_EQ(index.Count("a"), 0) << "Old term 'a' should have count 0";
  EXPECT_EQ(index.Count("b"), 0) << "Old term 'b' should have count 0";
  EXPECT_EQ(index.Count("c"), 0) << "Old term 'c' should have count 0";

  // Critical check: TermCount should be 3 (only x, y, z), not 6
  EXPECT_EQ(index.TermCount(), 3) << "Empty posting lists should be removed after UpdateDocument";
}

/**
 * @test Multiple updates should not cause posting list growth
 */
TEST(IndexTest, MultipleUpdatesNoPostingListGrowth) {
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
 * @test Update with partial overlap should clean up removed terms
 */
TEST(IndexTest, UpdateDocumentPartialOverlapCleansUp) {
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
  EXPECT_EQ(index.TermCount(), 3) << "Empty posting list for 'a' should be removed";
}

/**
 * @test Update document to empty text should remove all postings
 */
TEST(IndexTest, UpdateDocumentToEmptyRemovesAllPostings) {
  Index index(1);  // Unigram index

  // Add document
  index.AddDocument(1, "abc");
  EXPECT_EQ(index.TermCount(), 3);

  // Update to empty text
  index.UpdateDocument(1, "abc", "");

  // All posting lists should be removed
  EXPECT_EQ(index.TermCount(), 0) << "All empty posting lists should be removed";
  EXPECT_EQ(index.Count("a"), 0);
  EXPECT_EQ(index.Count("b"), 0);
  EXPECT_EQ(index.Count("c"), 0);
}

/**
 * @test Update with multiple documents does partial cleanup
 */
TEST(IndexTest, UpdateDocumentMultiDocsPartialCleanup) {
  Index index(1);  // Unigram index

  // Add two documents sharing term 'b'
  index.AddDocument(1, "ab");       // Terms: a, b
  index.AddDocument(2, "bc");       // Terms: b, c
  EXPECT_EQ(index.TermCount(), 3);  // a, b, c
  EXPECT_EQ(index.Count("b"), 2);   // Both docs have 'b'

  // Update doc 1: remove 'a' and 'b', add 'x'
  index.UpdateDocument(1, "ab", "x");

  // Term 'b' should still exist (doc 2 has it)
  EXPECT_EQ(index.Count("b"), 1) << "Term 'b' should still have count 1 from doc 2";

  // Term 'a' had only doc 1, so its posting list should be removed
  EXPECT_EQ(index.Count("a"), 0) << "Term 'a' should have count 0";

  // TermCount should be 3: b (from doc2), c (from doc2), x (from doc1)
  EXPECT_EQ(index.TermCount(), 3) << "Empty posting list for 'a' should be removed";
}

// =============================================================================
// RemoveDocument empty posting list cleanup
// =============================================================================

/**
 * @test RemoveDocument should remove empty posting lists
 */
TEST(IndexTest, RemoveDocumentRemovesEmptyPostingLists) {
  Index index(1);  // Unigram index

  // Add a document
  index.AddDocument(1, "abc");
  EXPECT_EQ(index.TermCount(), 3);

  // Remove the document
  index.RemoveDocument(1, "abc");

  // All posting lists should be removed since doc was the only one
  EXPECT_EQ(index.TermCount(), 0) << "Empty posting lists should be removed after RemoveDocument";
}

/**
 * @test RemoveDocument with multiple documents does partial cleanup
 */
TEST(IndexTest, RemoveDocumentPartialCleanup) {
  Index index(1);  // Unigram index

  // Add two documents
  index.AddDocument(1, "ab");       // Terms: a, b
  index.AddDocument(2, "bc");       // Terms: b, c
  EXPECT_EQ(index.TermCount(), 3);  // a, b, c

  // Remove doc 1
  index.RemoveDocument(1, "ab");

  // Term 'a' should be completely removed (only doc 1 had it)
  // Term 'b' should still exist (doc 2 has it)
  EXPECT_EQ(index.Count("a"), 0);
  EXPECT_EQ(index.Count("b"), 1);
  EXPECT_EQ(index.Count("c"), 1);

  // TermCount should be 2: b, c (not 3 including empty 'a')
  EXPECT_EQ(index.TermCount(), 2) << "Empty posting list for 'a' should be removed";
}

// =============================================================================
// EstimatePostingSize
// =============================================================================

TEST(IndexTest, EstimatePostingSize) {
  Index index(2);
  index.AddDocument(1, "hello world");
  index.AddDocument(2, "hello there");

  // "he" appears in both documents
  EXPECT_EQ(index.EstimatePostingSize("he"), 2u);
  // Non-existent term
  EXPECT_EQ(index.EstimatePostingSize("zz"), 0u);
}
