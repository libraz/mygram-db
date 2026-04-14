/**
 * @file index_search_test.cpp
 * @brief Unit tests for n-gram inverted index - Search operations
 */

#include <gtest/gtest.h>

#include <unordered_set>

#include "index/index.h"
#include "utils/string_utils.h"

using namespace mygramdb::index;
using namespace mygram::utils;

TEST(IndexTest, SearchAndSingleTerm) {
  Index index(1);

  index.AddDocument(1, "abc");
  index.AddDocument(2, "bcd");
  index.AddDocument(3, "cde");

  auto results = index.SearchAnd({"b"});
  EXPECT_EQ(results.size(), 2);
  EXPECT_EQ(results[0], 1);
  EXPECT_EQ(results[1], 2);
}

/**
 * @brief Test AND search with multiple terms
 */
TEST(IndexTest, SearchAndMultipleTerms) {
  Index index(1);

  index.AddDocument(1, "abc");
  index.AddDocument(2, "bcd");
  index.AddDocument(3, "cde");

  // Search for documents containing both "b" AND "c"
  auto results = index.SearchAnd({"b", "c"});
  EXPECT_EQ(results.size(), 2);
  EXPECT_EQ(results[0], 1);
  EXPECT_EQ(results[1], 2);

  // Search for documents containing "c" AND "d"
  results = index.SearchAnd({"c", "d"});
  EXPECT_EQ(results.size(), 2);
  EXPECT_EQ(results[0], 2);
  EXPECT_EQ(results[1], 3);
}

/**
 * @brief Test AND search with no matches
 */
TEST(IndexTest, SearchAndNoMatch) {
  Index index(1);

  index.AddDocument(1, "abc");
  index.AddDocument(2, "def");

  // No document contains both "a" AND "d"
  auto results = index.SearchAnd({"a", "d"});
  EXPECT_EQ(results.size(), 0);
}

/**
 * @brief Test AND search with non-existent term
 */
TEST(IndexTest, SearchAndNonExistentTerm) {
  Index index(1);

  index.AddDocument(1, "abc");

  auto results = index.SearchAnd({"z"});
  EXPECT_EQ(results.size(), 0);
}

/**
 * @brief Test OR search with single term
 */
TEST(IndexTest, SearchOrSingleTerm) {
  Index index(1);

  index.AddDocument(1, "abc");
  index.AddDocument(2, "def");

  auto results = index.SearchOr({"a"});
  EXPECT_EQ(results.size(), 1);
  EXPECT_EQ(results[0], 1);
}

/**
 * @brief Test OR search with multiple terms
 */
TEST(IndexTest, SearchOrMultipleTerms) {
  Index index(1);

  index.AddDocument(1, "abc");
  index.AddDocument(2, "def");
  index.AddDocument(3, "ghi");

  // Search for documents containing "a" OR "d"
  auto results = index.SearchOr({"a", "d"});
  EXPECT_EQ(results.size(), 2);
  EXPECT_EQ(results[0], 1);
  EXPECT_EQ(results[1], 2);

  // Search for documents containing "a" OR "d" OR "g"
  results = index.SearchOr({"a", "d", "g"});
  EXPECT_EQ(results.size(), 3);
  EXPECT_EQ(results[0], 1);
  EXPECT_EQ(results[1], 2);
  EXPECT_EQ(results[2], 3);
}

/**
 * @brief Test OR search with non-existent terms
 */
TEST(IndexTest, SearchOrNonExistentTerm) {
  Index index(1);

  index.AddDocument(1, "abc");

  auto results = index.SearchOr({"z"});
  EXPECT_EQ(results.size(), 0);

  results = index.SearchOr({"a", "z"});
  EXPECT_EQ(results.size(), 1);
  EXPECT_EQ(results[0], 1);
}

/**
 * @brief Stress test OR search with large posting lists and overlapping terms
 */
TEST(IndexTest, SearchOrLargeDataset) {
  Index index(1);

  constexpr DocId kDocs = 10000;
  std::vector<DocId> expected;
  expected.reserve(kDocs);

  for (DocId doc_id = 1; doc_id <= kDocs; ++doc_id) {
    bool has_a = false;
    bool has_b = false;
    std::string text;

    if (doc_id % 10 == 0) {
      text = "ab";  // Contains both
      has_a = true;
      has_b = true;
    } else if (doc_id % 2 == 0) {
      text = "aaaa";  // Only 'a'
      has_a = true;
    } else if (doc_id % 3 == 0) {
      text = "bbbb";  // Only 'b'
      has_b = true;
    } else {
      text = "cccc";  // Neither term
    }

    index.AddDocument(doc_id, text);
    if (has_a || has_b) {
      expected.push_back(doc_id);
    }
  }

  auto results = index.SearchOr({"a", "b"});
  ASSERT_EQ(results.size(), expected.size());
  for (size_t i = 0; i < expected.size(); ++i) {
    EXPECT_EQ(results[i], expected[i]) << "Mismatch at position " << i;
  }
}

/**
 * @brief Test NOT search with single term
 */
TEST(IndexTest, SearchNotSingleTerm) {
  Index index(1);

  index.AddDocument(1, "abc");
  index.AddDocument(2, "def");
  index.AddDocument(3, "ghi");

  std::vector<DocId> all_docs = {1, 2, 3};

  // Exclude documents containing "a"
  auto results = index.SearchNot(all_docs, {"a"});
  EXPECT_EQ(results.size(), 2);
  EXPECT_EQ(results[0], 2);
  EXPECT_EQ(results[1], 3);
}

/**
 * @brief Test NOT search with multiple terms
 */
TEST(IndexTest, SearchNotMultipleTerms) {
  Index index(1);

  index.AddDocument(1, "abc");
  index.AddDocument(2, "def");
  index.AddDocument(3, "ghi");
  index.AddDocument(4, "jkl");

  std::vector<DocId> all_docs = {1, 2, 3, 4};

  // Exclude documents containing "a" OR "d"
  auto results = index.SearchNot(all_docs, {"a", "d"});
  EXPECT_EQ(results.size(), 2);
  EXPECT_EQ(results[0], 3);
  EXPECT_EQ(results[1], 4);

  // Exclude documents containing "a" OR "d" OR "g"
  results = index.SearchNot(all_docs, {"a", "d", "g"});
  EXPECT_EQ(results.size(), 1);
  EXPECT_EQ(results[0], 4);
}

/**
 * @brief Test NOT search with non-existent term
 */
TEST(IndexTest, SearchNotNonExistentTerm) {
  Index index(1);

  index.AddDocument(1, "abc");
  index.AddDocument(2, "def");

  std::vector<DocId> all_docs = {1, 2};

  // Exclude non-existent term should return all documents
  auto results = index.SearchNot(all_docs, {"z"});
  EXPECT_EQ(results.size(), 2);
  EXPECT_EQ(results[0], 1);
  EXPECT_EQ(results[1], 2);
}

/**
 * @brief Test NOT search with empty terms
 */
TEST(IndexTest, SearchNotEmptyTerms) {
  Index index(1);

  index.AddDocument(1, "abc");
  index.AddDocument(2, "def");

  std::vector<DocId> all_docs = {1, 2};

  // Empty NOT terms should return all documents
  auto results = index.SearchNot(all_docs, {});
  EXPECT_EQ(results.size(), 2);
  EXPECT_EQ(results[0], 1);
  EXPECT_EQ(results[1], 2);
}

/**
 * @brief Test NOT search excluding all documents
 */
TEST(IndexTest, SearchNotExcludeAll) {
  Index index(1);

  index.AddDocument(1, "abc");
  index.AddDocument(2, "abc");
  index.AddDocument(3, "abc");

  std::vector<DocId> all_docs = {1, 2, 3};

  // Exclude all documents containing "a"
  auto results = index.SearchNot(all_docs, {"a"});
  EXPECT_EQ(results.size(), 0);
}

/**
 * @brief Test NOT search with empty document set
 */
TEST(IndexTest, SearchNotEmptyDocSet) {
  Index index(1);

  index.AddDocument(1, "abc");

  std::vector<DocId> all_docs = {};

  // Empty document set should return empty
  auto results = index.SearchNot(all_docs, {"a"});
  EXPECT_EQ(results.size(), 0);
}

/**
 * @brief Stress test NOT search against large document sets with overlapping exclusions
 */
TEST(IndexTest, SearchNotLargeDataset) {
  Index index(1);

  constexpr DocId kDocs = 9000;
  std::vector<DocId> all_docs;
  std::vector<DocId> expected;
  all_docs.reserve(kDocs);
  expected.reserve(kDocs);

  for (DocId doc_id = 1; doc_id <= kDocs; ++doc_id) {
    all_docs.push_back(doc_id);
    bool excluded = false;
    std::string text;

    if (doc_id % 35 == 0) {
      text = "xy";  // Contains both excluded terms
      excluded = true;
    } else if (doc_id % 7 == 0) {
      text = "xxx";  // Contains 'x'
      excluded = true;
    } else if (doc_id % 5 == 0) {
      text = "yyy";  // Contains 'y'
      excluded = true;
    } else {
      text = "zzz";  // Neither term
    }

    index.AddDocument(doc_id, text);
    if (!excluded) {
      expected.push_back(doc_id);
    }
  }

  auto results = index.SearchNot(all_docs, {"x", "y"});
  ASSERT_EQ(results.size(), expected.size());
  for (size_t i = 0; i < expected.size(); ++i) {
    EXPECT_EQ(results[i], expected[i]) << "DocId mismatch at " << i;
  }
}

/**
 * @brief Test Japanese text search with normalization
 */
TEST(IndexTest, SearchJapanese) {
  Index index(1);

  // Add Japanese documents with normalization
  std::string text1 = NormalizeText("ライブ", true, "keep", false);
  std::string text2 = NormalizeText("ライブラリ", true, "keep", false);
  std::string text3 = NormalizeText("プログラム", true, "keep", false);

  index.AddDocument(1, text1);
  index.AddDocument(2, text2);
  index.AddDocument(3, text3);

  // Generate search terms
  auto terms1 = GenerateNgrams(text1, 1);

  // Search for "ライブ" (should match docs 1 and 2)
  auto results = index.SearchAnd(terms1);
  EXPECT_EQ(results.size(), 2);
  EXPECT_EQ(results[0], 1);
  EXPECT_EQ(results[1], 2);
}

/**
 * @brief Test half-width katakana normalization in search
 */
TEST(IndexTest, SearchHalfWidthKatakana) {
  Index index(1);

  // Add document with full-width katakana (normalized)
  std::string text1 = NormalizeText("ライブ", true, "keep", false);
  index.AddDocument(1, text1);

  // Search with half-width katakana (should be normalized to full-width)
  std::string search_text = NormalizeText("ﾗｲﾌﾞ", true, "keep", false);
  auto search_terms = GenerateNgrams(search_text, 1);

  // Should find document 1 because both normalize to "ライブ"
  auto results = index.SearchAnd(search_terms);
  EXPECT_EQ(results.size(), 1);
  EXPECT_EQ(results[0], 1);
}

/**
 * @brief Test bigram index
 */
TEST(IndexTest, BigramIndex) {
  Index index(2);  // Bigram index

  index.AddDocument(1, "abc");
  index.AddDocument(2, "bcd");

  // Should have bigrams: "ab", "bc" (doc 1), "bc", "cd" (doc 2)
  EXPECT_EQ(index.TermCount(), 3);  // ab, bc, cd (bc is shared)

  EXPECT_EQ(index.Count("ab"), 1);
  EXPECT_EQ(index.Count("bc"), 2);
  EXPECT_EQ(index.Count("cd"), 1);
}

/**
 * @brief Test bigram search
 */
TEST(IndexTest, BigramSearch) {
  Index index(2);

  index.AddDocument(1, "abcd");
  index.AddDocument(2, "bcde");
  index.AddDocument(3, "cdef");

  // Search for documents containing "bc"
  auto results = index.SearchAnd({"bc"});
  EXPECT_EQ(results.size(), 2);
  EXPECT_EQ(results[0], 1);
  EXPECT_EQ(results[1], 2);

  // Search for documents containing "bc" AND "cd"
  results = index.SearchAnd({"bc", "cd"});
  EXPECT_EQ(results.size(), 2);
  EXPECT_EQ(results[0], 1);
  EXPECT_EQ(results[1], 2);
}

/**
 * @brief Test empty search
 */
TEST(IndexTest, EmptySearch) {
  Index index(1);

  index.AddDocument(1, "abc");

  auto results = index.SearchAnd({});
  EXPECT_EQ(results.size(), 0);

  results = index.SearchOr({});
  EXPECT_EQ(results.size(), 0);
}

// =============================================================================
// Duplicate n-grams produce correct results
// =============================================================================

TEST(IndexSearchTest, DuplicateNgramsProduceCorrectResults) {
  Index index(2);
  index.AddDocument(1, "hello");
  // Same n-grams duplicated should still produce correct results
  auto results = index.SearchAnd({"he", "he", "el"});
  EXPECT_EQ(results.size(), 1u);
  EXPECT_EQ(results[0], 1u);
}

// =============================================================================
// SearchAnd optimization (Bug #16)
// =============================================================================

/**
 * @test Bug #16: SearchAnd with limit should not materialize all documents
 *
 * This test verifies that SearchAnd returns correct results when using
 * the optimized path (limit > 0, reverse = true, multiple terms).
 * Note: For small posting lists, SearchAnd returns all results and limit/reverse
 * is applied by the caller. This test verifies the intersection is correct.
 */
TEST(IndexTest, SearchAndWithLimitCorrectResults) {
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
TEST(IndexTest, SearchAndSingleTermGetTopN) {
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
TEST(IndexTest, SearchAndMultipleTermsIntersection) {
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
TEST(IndexTest, SearchAndNonExistentTermEmpty) {
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
TEST(IndexTest, SearchAndLargePostingListsTopN) {
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
    EXPECT_EQ(results[i], kNumDocs - static_cast<DocId>(i)) << "Result[" << i << "] should be " << (kNumDocs - i);
  }
}

/**
 * @test M-1: SearchAnd standard path truncates results to limit
 *
 * Verifies that the standard (non-streaming) intersection path applies the
 * limit parameter, returning at most `limit` elements.
 */
TEST(IndexTest, SearchAndStandardPathLimitTruncation) {
  Index index(1);  // Unigram index

  // Add 50 documents that all share terms "a" and "b"
  for (DocId i = 1; i <= 50; ++i) {
    index.AddDocument(i, "ab");
  }

  // Without limit: should return all 50
  auto all_results = index.SearchAnd({"a", "b"}, 0, false);
  ASSERT_EQ(all_results.size(), 50);

  // With limit=10, reverse=false: should return first 10 (ascending DocIDs)
  auto limited = index.SearchAnd({"a", "b"}, 10, false);
  ASSERT_LE(limited.size(), 10u);
  EXPECT_EQ(limited.size(), 10u);
  for (size_t i = 0; i < limited.size(); ++i) {
    EXPECT_EQ(limited[i], static_cast<DocId>(i + 1));
  }

  // With limit=10, reverse=true: should return last 10 (highest DocIDs)
  auto limited_rev = index.SearchAnd({"a", "b"}, 10, true);
  ASSERT_LE(limited_rev.size(), 10u);
  EXPECT_EQ(limited_rev.size(), 10u);
  for (size_t i = 0; i < limited_rev.size(); ++i) {
    EXPECT_EQ(limited_rev[i], static_cast<DocId>(41 + i));
  }

  // With limit larger than result set: should return all
  auto large_limit = index.SearchAnd({"a", "b"}, 100, false);
  EXPECT_EQ(large_limit.size(), 50u);
}
