/**
 * @file index_batch_test.cpp
 * @brief Unit tests for n-gram inverted index - Batch operations
 */

#include <gtest/gtest.h>

#include <unordered_set>

#include "index/index.h"
#include "utils/string_utils.h"

using namespace mygramdb::index;
using namespace mygramdb::utils;

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
    EXPECT_LT(results_ab[i - 1], results_ab[i]) << "Results must be sorted";
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
    EXPECT_LT(results_t[i - 1], results_t[i]) << "Results must be strictly increasing";
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
      NormalizeText("データベース", true, "keep", false), NormalizeText("データ構造", true, "keep", false),
      NormalizeText("構造化データ", true, "keep", false), NormalizeText("データベース設計", true, "keep", false)};

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

      EXPECT_EQ(results_single.size(), results_batch.size()) << "Term '" << ngram << "' has different result count";

      for (size_t i = 0; i < results_single.size(); ++i) {
        EXPECT_EQ(results_single[i], results_batch[i])
            << "Term '" << ngram << "' has different doc_id at position " << i;
      }
    }
  }
}

/**
 * @brief Test indexing documents with 4-byte emoji characters
 */
