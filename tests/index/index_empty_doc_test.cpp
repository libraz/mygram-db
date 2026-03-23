/**
 * @file index_empty_doc_test.cpp
 * @brief Tests for empty document handling in Index::AddDocument
 */

#include <gtest/gtest.h>

#include "index/index.h"

namespace mygramdb::index {

/**
 * @brief Test that single-character document with bigram returns false
 */
TEST(IndexEmptyDocTest, SingleCharDocumentWithBigramReturnsFalse) {
  Index index(2, 1);  // ngram_size=2, kanji_ngram_size=1

  // Single ASCII character can't form a bigram
  bool result = index.AddDocument(1, "a");
  EXPECT_FALSE(result) << "Single char with ngram_size=2 should return false";
}

/**
 * @brief Test that single whitespace character with bigram returns false
 */
TEST(IndexEmptyDocTest, SingleWhitespaceWithBigramReturnsFalse) {
  Index index(2, 1);

  // Single space can't form a bigram
  bool result = index.AddDocument(2, " ");
  EXPECT_FALSE(result) << "Single whitespace with ngram_size=2 should return false";
}

/**
 * @brief Test that empty string returns false
 */
TEST(IndexEmptyDocTest, EmptyStringReturnsFalse) {
  Index index(2, 1);

  bool result = index.AddDocument(3, "");
  EXPECT_FALSE(result) << "Empty string should return false";
}

/**
 * @brief Test that normal document returns true
 */
TEST(IndexEmptyDocTest, NormalDocumentReturnsTrue) {
  Index index(2, 1);

  bool result = index.AddDocument(4, "hello world");
  EXPECT_TRUE(result) << "Normal text should return true";
}

/**
 * @brief Test that single CJK character with unigram returns true
 */
TEST(IndexEmptyDocTest, SingleCJKCharWithUnigramReturnsTrue) {
  Index index(2, 1);  // kanji_ngram_size=1

  // Single Kanji character can form a unigram
  bool result = index.AddDocument(5, "\xe6\x9d\xb1");  // U+6771 東
  EXPECT_TRUE(result) << "Single CJK char with kanji_ngram_size=1 should return true";
}

}  // namespace mygramdb::index
