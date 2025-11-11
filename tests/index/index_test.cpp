/**
 * @file index_test.cpp
 * @brief Unit tests for n-gram inverted index
 */

#include "index/index.h"
#include "utils/string_utils.h"
#include <gtest/gtest.h>
#include <unordered_set>

using namespace mygramdb::index;
using namespace mygramdb::utils;

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
 * @brief Test AND search with single term
 */
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

/**
 * @brief Test memory usage
 */
TEST(IndexTest, MemoryUsage) {
  Index index(1);

  size_t initial_usage = index.MemoryUsage();
  EXPECT_GE(initial_usage, 0);

  // Add documents
  index.AddDocument(1, "abc");
  index.AddDocument(2, "def");

  size_t after_usage = index.MemoryUsage();
  EXPECT_GT(after_usage, initial_usage);
}

/**
 * @brief Test optimize
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

/**
 * @brief Test batch document addition
 */
TEST(IndexTest, AddDocumentBatch) {
  Index index(1);  // Unigram index

  // Prepare batch of documents
  std::vector<Index::DocumentItem> batch;
  batch.push_back({1, NormalizeText("abc", true, "keep", false)});
  batch.push_back({2, NormalizeText("bcd", true, "keep", false)});
  batch.push_back({3, NormalizeText("def", true, "keep", false)});

  // Add batch
  index.AddDocumentBatch(batch);

  // Verify terms were added correctly
  EXPECT_EQ(index.Count("a"), 1);  // doc 1
  EXPECT_EQ(index.Count("b"), 2);  // doc 1, 2
  EXPECT_EQ(index.Count("c"), 2);  // doc 1, 2
  EXPECT_EQ(index.Count("d"), 2);  // doc 2, 3
  EXPECT_EQ(index.Count("e"), 1);  // doc 3
  EXPECT_EQ(index.Count("f"), 1);  // doc 3

  // Verify search works correctly
  auto results_b = index.SearchAnd({"b"});
  EXPECT_EQ(results_b.size(), 2);
  EXPECT_EQ(results_b[0], 1);
  EXPECT_EQ(results_b[1], 2);
}

/**
 * @brief Test empty batch addition
 */
TEST(IndexTest, AddDocumentBatchEmpty) {
  Index index(1);

  std::vector<Index::DocumentItem> batch;
  index.AddDocumentBatch(batch);  // Should not crash

  EXPECT_EQ(index.TermCount(), 0);
}

/**
 * @brief Test large batch addition
 */
TEST(IndexTest, AddDocumentBatchLarge) {
  Index index(2);  // Bigram index

  // Create large batch (1000 documents)
  std::vector<Index::DocumentItem> batch;
  for (DocId i = 1; i <= 1000; ++i) {
    std::string text = "document" + std::to_string(i);
    batch.push_back({i, NormalizeText(text, true, "keep", false)});
  }

  index.AddDocumentBatch(batch);

  // Verify all documents were added
  auto results = index.SearchAnd({"do"});  // "do" is common to all
  EXPECT_EQ(results.size(), 1000);

  // Verify sorted order
  for (size_t i = 0; i < results.size(); ++i) {
    EXPECT_EQ(results[i], static_cast<DocId>(i + 1));
  }
}

/**
 * @brief Test batch addition preserves search correctness
 */
TEST(IndexTest, AddDocumentBatchSearchCorrectness) {
  Index index_single(2);  // Single document addition
  Index index_batch(2);   // Batch addition

  // Add same documents using both methods
  std::vector<Index::DocumentItem> batch;
  for (DocId i = 1; i <= 100; ++i) {
    std::string text = NormalizeText("テスト" + std::to_string(i), true, "keep", false);
    index_single.AddDocument(i, text);
    batch.push_back({i, text});
  }
  index_batch.AddDocumentBatch(batch);

  // Verify both produce same search results
  auto results_single = index_single.SearchAnd({"テ"});
  auto results_batch = index_batch.SearchAnd({"テ"});

  EXPECT_EQ(results_single.size(), results_batch.size());
  for (size_t i = 0; i < results_single.size(); ++i) {
    EXPECT_EQ(results_single[i], results_batch[i]);
  }
}

/**
 * @brief Test internal index structure integrity after batch addition
 */
TEST(IndexTest, AddDocumentBatchStructureIntegrity) {
  Index index(2);  // Bigram index

  // Add batch with overlapping terms
  std::vector<Index::DocumentItem> batch;
  batch.push_back({1, NormalizeText("abcdef", true, "keep", false)});
  batch.push_back({2, NormalizeText("bcdefg", true, "keep", false)});
  batch.push_back({3, NormalizeText("cdefgh", true, "keep", false)});
  batch.push_back({4, NormalizeText("abcxyz", true, "keep", false)});

  index.AddDocumentBatch(batch);

  // Verify posting lists are sorted and have no duplicates
  // Test term "ab" (should have docs 1, 4)
  auto results_ab = index.SearchAnd({"ab"});
  EXPECT_EQ(results_ab.size(), 2);
  EXPECT_EQ(results_ab[0], 1);
  EXPECT_EQ(results_ab[1], 4);

  // Verify sorted order
  for (size_t i = 1; i < results_ab.size(); ++i) {
    EXPECT_LT(results_ab[i-1], results_ab[i]) << "Results must be sorted";
  }

  // Test term "cd" (should have docs 1, 2, 3)
  auto results_cd = index.SearchAnd({"cd"});
  EXPECT_EQ(results_cd.size(), 3);
  EXPECT_EQ(results_cd[0], 1);
  EXPECT_EQ(results_cd[1], 2);
  EXPECT_EQ(results_cd[2], 3);

  // Verify no duplicates
  std::unordered_set<DocId> unique_docs(results_cd.begin(), results_cd.end());
  EXPECT_EQ(unique_docs.size(), results_cd.size()) << "No duplicates allowed";
}

/**
 * @brief Test batch addition with many documents containing same terms
 */
TEST(IndexTest, AddDocumentBatchManyOverlappingTerms) {
  Index index(1);  // Unigram

  // Create 100 documents all containing "test"
  std::vector<Index::DocumentItem> batch;
  for (DocId i = 1; i <= 100; ++i) {
    std::string text = "test" + std::to_string(i);
    batch.push_back({i, NormalizeText(text, true, "keep", false)});
  }

  index.AddDocumentBatch(batch);

  // Verify "t" appears in all 100 documents
  auto results_t = index.SearchAnd({"t"});
  EXPECT_EQ(results_t.size(), 100);

  // Verify sorted and no duplicates
  for (size_t i = 1; i < results_t.size(); ++i) {
    EXPECT_LT(results_t[i-1], results_t[i]) << "Results must be strictly increasing";
  }

  // Verify all doc_ids are present
  for (DocId i = 1; i <= 100; ++i) {
    EXPECT_NE(std::find(results_t.begin(), results_t.end(), i), results_t.end())
        << "DocId " << i << " should be in results";
  }
}

/**
 * @brief Test batch addition with identical documents
 */
TEST(IndexTest, AddDocumentBatchIdenticalDocuments) {
  Index index(2, 2);  // Both ASCII and Kanji use bigram

  // Add multiple documents with exact same text
  std::vector<Index::DocumentItem> batch;
  std::string text = NormalizeText("同じテキスト", true, "keep", false);
  for (DocId i = 1; i <= 50; ++i) {
    batch.push_back({i, text});
  }

  index.AddDocumentBatch(batch);

  // Get n-grams using hybrid mode (both 2)
  auto ngrams = GenerateHybridNgrams(text, 2, 2);
  ASSERT_FALSE(ngrams.empty());

  // Verify first n-gram has all 50 documents
  auto results = index.SearchAnd({ngrams[0]});
  EXPECT_EQ(results.size(), 50);

  // Verify sorted
  for (size_t i = 1; i < results.size(); ++i) {
    EXPECT_EQ(results[i], static_cast<DocId>(i + 1));
  }

  // Verify no duplicates
  std::unordered_set<DocId> unique_docs(results.begin(), results.end());
  EXPECT_EQ(unique_docs.size(), results.size());
}

/**
 * @brief Test batch vs single addition produces identical internal structure
 */
TEST(IndexTest, AddDocumentBatchVsSingleIdenticalStructure) {
  Index index_single(2);
  Index index_batch(2);

  // Prepare test data with complex overlapping terms
  std::vector<std::string> texts = {
      NormalizeText("データベース", true, "keep", false),
      NormalizeText("データ構造", true, "keep", false),
      NormalizeText("構造化データ", true, "keep", false),
      NormalizeText("データベース設計", true, "keep", false)
  };

  // Add to single index one by one
  for (DocId i = 0; i < texts.size(); ++i) {
    index_single.AddDocument(i + 1, texts[i]);
  }

  // Add to batch index at once
  std::vector<Index::DocumentItem> batch;
  for (DocId i = 0; i < texts.size(); ++i) {
    batch.push_back({i + 1, texts[i]});
  }
  index_batch.AddDocumentBatch(batch);

  // Verify identical term count
  EXPECT_EQ(index_single.TermCount(), index_batch.TermCount());

  // For each text, generate n-grams and verify identical results
  for (const auto& text : texts) {
    auto ngrams = GenerateNgrams(text, 2);
    for (const auto& ngram : ngrams) {
      auto results_single = index_single.SearchAnd({ngram});
      auto results_batch = index_batch.SearchAnd({ngram});

      EXPECT_EQ(results_single.size(), results_batch.size())
          << "Term '" << ngram << "' has different result count";

      for (size_t i = 0; i < results_single.size(); ++i) {
        EXPECT_EQ(results_single[i], results_batch[i])
            << "Term '" << ngram << "' has different doc_id at position " << i;
      }
    }
  }
}
