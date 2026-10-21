/**
 * @file index_empty_doc_test.cpp
 * @brief Tests for empty document handling in Index::AddDocument
 */

#include <gtest/gtest.h>

#include <sstream>
#include <vector>

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

/**
 * @brief A document that never gained postings is purged without visiting every term
 */
TEST(IndexEmptyDocTest, PurgeOfDocumentWithoutPostingsSkipsTermScan) {
  Index index(2, 1);
  ASSERT_TRUE(index.AddDocument(1, "alpha"));
  EXPECT_FALSE(index.AddDocument(2, ""));
  index.AddDocumentBatch({{3, "bravo"}, {4, ""}});
  index.UpdateDocument(5, "", "");

  EXPECT_TRUE(index.MayHavePostings(1));
  EXPECT_FALSE(index.MayHavePostings(2));
  EXPECT_TRUE(index.MayHavePostings(3));
  EXPECT_FALSE(index.MayHavePostings(4));
  EXPECT_FALSE(index.MayHavePostings(5));

  index.UpdateDocument(5, "", "charlie");
  EXPECT_TRUE(index.MayHavePostings(5));

  ASSERT_TRUE(index.AddDocument(6, "delta"));
  index.UpdateDocument(6, "delta", "");
  EXPECT_FALSE(index.MayHavePostings(6)) << "text cleared by update leaves no postings";
  index.RemoveDocument(5, "charlie");
  EXPECT_FALSE(index.MayHavePostings(5));

#ifdef MYGRAMDB_INDEX_TEST_HOOKS
  const uint64_t scans_before = index.PurgeScanCountForTesting();
  index.PurgeDocument(2);
  index.PurgeDocument(4);
  EXPECT_EQ(index.PurgeScanCountForTesting(), scans_before);

  index.PurgeDocument(1);
  EXPECT_EQ(index.PurgeScanCountForTesting(), scans_before + 1);
#else
  index.PurgeDocument(1);
#endif
  EXPECT_TRUE(index.SearchOr({"al", "lp", "ph", "ha"}).empty());
  EXPECT_FALSE(index.MayHavePostings(1));
  EXPECT_EQ(index.SearchAnd({"br", "ra"}), std::vector<DocId>{3});
}

/**
 * @brief Loaded and swapped-in postings carry which documents hold postings
 */
TEST(IndexEmptyDocTest, LoadedPostingsDeriveWhichDocumentsHavePostings) {
  std::stringstream dump;
  {
    Index source(2, 1);
    ASSERT_TRUE(source.AddDocument(1, "alpha"));
    EXPECT_FALSE(source.AddDocument(2, ""));
    ASSERT_TRUE(source.SaveToStream(dump).has_value());
  }

  Index live(2, 1);
  ASSERT_TRUE(live.AddDocument(7, "zulu"));
  ASSERT_TRUE(live.MayHavePostings(7));
  ASSERT_TRUE(live.LoadFromStream(dump).has_value());
  EXPECT_TRUE(live.MayHavePostings(1));
  EXPECT_FALSE(live.MayHavePostings(2));
  EXPECT_FALSE(live.MayHavePostings(7)) << "postings replaced by the load must not keep their documents";

  Index candidate(2, 1);
  ASSERT_TRUE(candidate.AddDocument(9, "yankee"));
  live.ReplaceWithLoaded(candidate);
  EXPECT_TRUE(live.MayHavePostings(9));
  EXPECT_FALSE(live.MayHavePostings(1));
  EXPECT_TRUE(candidate.MayHavePostings(1)) << "the swapped-out state must stay usable for rollback";

  live.Clear();
  EXPECT_FALSE(live.MayHavePostings(9));
}

}  // namespace mygramdb::index
