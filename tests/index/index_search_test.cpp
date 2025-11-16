/**
 * @file index_search_test.cpp
 * @brief Unit tests for n-gram inverted index - Search operations
 */

#include <gtest/gtest.h>

#include <unordered_set>

#include "index/index.h"
#include "utils/string_utils.h"

using namespace mygramdb::index;
using namespace mygramdb::utils;

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
