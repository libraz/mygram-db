/**
 * @file index_advanced_test.cpp
 * @brief Unit tests for n-gram inverted index - Advanced features
 */

#include <gtest/gtest.h>

#include <unordered_set>

#include "index/index.h"
#include "utils/string_utils.h"

using namespace mygramdb::index;
using namespace mygramdb::utils;

/** @brief Test memory usage
 */
TEST(IndexTest, MemoryUsage) {
  Index index(1);

  size_t initial_usage = index.MemoryUsage();
  // Initial usage may be zero if no memory is allocated yet
  EXPECT_GE(initial_usage, 0);

  // Add documents
  index.AddDocument(1, "abc");
  index.AddDocument(2, "def");

  size_t after_two_docs = index.MemoryUsage();
  // After adding documents, memory should increase
  EXPECT_GT(after_two_docs, initial_usage);

  // Add more documents and verify memory growth
  for (int i = 3; i <= 100; ++i) {
    index.AddDocument(i, "test document " + std::to_string(i));
  }

  size_t after_hundred_docs = index.MemoryUsage();
  EXPECT_GT(after_hundred_docs, after_two_docs);

  // Memory usage should be reasonable (not more than 100MB for 100 small documents)
  EXPECT_LT(after_hundred_docs, 100 * 1024 * 1024);

  // Remove some documents and verify memory decreases or stays reasonable
  for (int i = 1; i <= 50; ++i) {
    std::string text = (i == 1 || i == 2) ? (i == 1 ? "abc" : "def") : "test document " + std::to_string(i);
    index.RemoveDocument(i, text);
  }

  size_t after_removal = index.MemoryUsage();
  // After removal, memory might not decrease immediately (depending on implementation)
  // but it should not increase
  EXPECT_LE(after_removal, after_hundred_docs);
}

/**
 * @brief Test emoji indexing with various emoji characters
 */
TEST(IndexTest, EmojiIndexing) {
  Index index(1);  // Unigram

  // Add documents with emojis
  index.AddDocument(1, "Hello😀World");
  index.AddDocument(2, "😀🎉👍");
  index.AddDocument(3, "楽しい😀チュートリアル");

  // Search for emoji (should find all 3 documents containing this emoji)
  auto results = index.SearchAnd({"😀"});
  EXPECT_EQ(results.size(), 3);

  // Search for different emoji (should find only doc 2)
  results = index.SearchAnd({"🎉"});
  EXPECT_EQ(results.size(), 1);
  EXPECT_EQ(results[0], 2);

  // Search for emoji AND another character
  results = index.SearchAnd({"😀", "W"});
  EXPECT_EQ(results.size(), 1);
  EXPECT_EQ(results[0], 1);
}

/**
 * @brief Test emoji n-gram generation
 */
TEST(IndexTest, EmojiNgrams) {
  Index index(2);  // Bigram

  // Add document with pure emoji sequence
  std::string text = "😀🎉👍";
  index.AddDocument(1, text);

  // Should generate bigrams: "😀🎉", "🎉👍"
  auto results = index.SearchAnd({"😀🎉"});
  EXPECT_EQ(results.size(), 1);

  results = index.SearchAnd({"🎉👍"});
  EXPECT_EQ(results.size(), 1);

  // Add another document to test multiple matches
  index.AddDocument(2, "🎉👍😎");

  // Should find both documents containing "🎉👍"
  results = index.SearchAnd({"🎉👍"});
  EXPECT_EQ(results.size(), 2);
}

/**
 * @brief Test AND search with emojis
 */
TEST(IndexTest, EmojiAndSearch) {
  Index index(1);  // Unigram

  index.AddDocument(1, "😀A");
  index.AddDocument(2, "😀🎉");
  index.AddDocument(3, "A🎉");

  // Both "😀" AND "A"
  auto results = index.SearchAnd({"😀", "A"});
  EXPECT_EQ(results.size(), 1);
  EXPECT_EQ(results[0], 1);

  // Both "😀" AND "🎉"
  results = index.SearchAnd({"😀", "🎉"});
  EXPECT_EQ(results.size(), 1);
  EXPECT_EQ(results[0], 2);

  // Both "A" AND "🎉"
  results = index.SearchAnd({"A", "🎉"});
  EXPECT_EQ(results.size(), 1);
  EXPECT_EQ(results[0], 3);
}

/**
 * @brief Test OR search with emojis
 */
TEST(IndexTest, EmojiOrSearch) {
  Index index(1);  // Unigram

  index.AddDocument(1, "😀A");
  index.AddDocument(2, "🎉B");
  index.AddDocument(3, "👍C");

  // "😀" OR "🎉"
  auto results = index.SearchOr({"😀", "🎉"});
  EXPECT_EQ(results.size(), 2);

  // "😀" OR "🎉" OR "👍"
  results = index.SearchOr({"😀", "🎉", "👍"});
  EXPECT_EQ(results.size(), 3);
}

/**
 * @brief Test NOT search with emojis
 */
TEST(IndexTest, EmojiNotSearch) {
  Index index(1);  // Unigram

  index.AddDocument(1, "😀X");
  index.AddDocument(2, "🎉X");
  index.AddDocument(3, "X");

  // Get all documents with "X"
  auto all_x = index.SearchAnd({"X"});
  EXPECT_EQ(all_x.size(), 3);

  // "X" NOT "😀"
  auto results = index.SearchNot(all_x, {"😀"});
  EXPECT_EQ(results.size(), 2);
  EXPECT_TRUE(std::find(results.begin(), results.end(), 2) != results.end());
  EXPECT_TRUE(std::find(results.begin(), results.end(), 3) != results.end());
}

/**
 * @brief Test complex emoji (skin tone, ZWJ sequences)
 */
TEST(IndexTest, ComplexEmoji) {
  Index index(1);  // Unigram

  // Emoji with skin tone modifier: 👍🏽 (thumbs up + medium skin tone)
  index.AddDocument(1, "👍🏽Y");
  index.AddDocument(2, "👍Z");  // Without skin tone

  // Search for the base emoji
  auto results = index.SearchAnd({"👍"});
  EXPECT_GE(results.size(), 1);  // Should find at least the plain thumbs up

  // Search for the skin tone modifier
  results = index.SearchAnd({"🏽"});
  EXPECT_GE(results.size(), 1);  // Should find document with skin tone

  // Search for common character
  results = index.SearchAnd({"Y"});
  EXPECT_EQ(results.size(), 1);
  EXPECT_EQ(results[0], 1);
}

/**
 * @brief Test stream-based serialization with basic data
 */
TEST(IndexTest, StreamSerializationBasic) {
  Index index1(2);  // Bigram index

  // Add some documents
  std::string text1 = NormalizeText("hello world", true, "keep", false);
  std::string text2 = NormalizeText("world peace", true, "keep", false);
  std::string text3 = NormalizeText("hello peace", true, "keep", false);

  index1.AddDocument(1, text1);
  index1.AddDocument(2, text2);
  index1.AddDocument(3, text3);

  // Serialize to stringstream
  std::stringstream stream;
  ASSERT_TRUE(index1.SaveToStream(stream));

  // Deserialize from stringstream
  Index index2(2);
  ASSERT_TRUE(index2.LoadFromStream(stream));

  // Verify term count
  EXPECT_EQ(index1.TermCount(), index2.TermCount());

  // Verify search results are identical
  auto results1 = index1.SearchAnd({"he", "ll"});
  auto results2 = index2.SearchAnd({"he", "ll"});
  EXPECT_EQ(results1, results2);

  results1 = index1.SearchAnd({"wo", "rl"});
  results2 = index2.SearchAnd({"wo", "rl"});
  EXPECT_EQ(results1, results2);
}

/**
 * @brief Test stream-based serialization with Japanese text
 */
TEST(IndexTest, StreamSerializationJapanese) {
  Index index1(2, 1);  // Bigram for ASCII, Unigram for Kanji

  // Add Japanese documents
  std::string text1 = NormalizeText("東京タワー", true, "keep", false);
  std::string text2 = NormalizeText("大阪城", true, "keep", false);
  std::string text3 = NormalizeText("京都タワー", true, "keep", false);

  index1.AddDocument(1, text1);
  index1.AddDocument(2, text2);
  index1.AddDocument(3, text3);

  // Serialize to stringstream
  std::stringstream stream;
  ASSERT_TRUE(index1.SaveToStream(stream));

  // Deserialize from stringstream
  Index index2(2, 1);
  ASSERT_TRUE(index2.LoadFromStream(stream));

  // Verify term count
  EXPECT_EQ(index1.TermCount(), index2.TermCount());

  // Verify search results
  auto results1 = index1.SearchAnd({"京"});
  auto results2 = index2.SearchAnd({"京"});
  EXPECT_EQ(results1.size(), results2.size());
  EXPECT_EQ(results1, results2);
}

/**
 * @brief Test stream-based serialization with large dataset
 */
TEST(IndexTest, StreamSerializationLargeDataset) {
  Index index1(2);

  // Add 1000 documents
  for (DocId i = 1; i <= 1000; ++i) {
    std::string text = NormalizeText("document " + std::to_string(i), true, "keep", false);
    index1.AddDocument(i, text);
  }

  // Serialize to stringstream
  std::stringstream stream;
  ASSERT_TRUE(index1.SaveToStream(stream));

  // Deserialize from stringstream
  Index index2(2);
  ASSERT_TRUE(index2.LoadFromStream(stream));

  // Verify term count
  EXPECT_EQ(index1.TermCount(), index2.TermCount());

  // Verify search results
  auto results1 = index1.SearchAnd({"do", "cu"});
  auto results2 = index2.SearchAnd({"do", "cu"});
  EXPECT_EQ(results1.size(), 1000);
  EXPECT_EQ(results2.size(), 1000);
  EXPECT_EQ(results1, results2);
}

/**
 * @brief Test stream-based serialization with emoji
 */
TEST(IndexTest, StreamSerializationEmoji) {
  Index index1(1);  // Unigram

  // Add documents with emojis
  index1.AddDocument(1, "Hello😀World");
  index1.AddDocument(2, "😀🎉👍");
  index1.AddDocument(3, "楽しい😀チュートリアル");

  // Serialize to stringstream
  std::stringstream stream;
  ASSERT_TRUE(index1.SaveToStream(stream));

  // Deserialize from stringstream
  Index index2(1);
  ASSERT_TRUE(index2.LoadFromStream(stream));

  // Verify term count
  EXPECT_EQ(index1.TermCount(), index2.TermCount());

  // Verify emoji search works
  auto results1 = index1.SearchAnd({"😀"});
  auto results2 = index2.SearchAnd({"😀"});
  EXPECT_EQ(results1.size(), 3);
  EXPECT_EQ(results2.size(), 3);
  EXPECT_EQ(results1, results2);
}

/**
 * @brief Test stream-based serialization preserves n-gram configuration
 */
TEST(IndexTest, StreamSerializationNgramConfig) {
  Index index1(3, 2);  // Trigram for ASCII, Bigram for Kanji

  // Add mixed content
  std::string text = NormalizeText("abc日本語xyz", true, "keep", false);
  index1.AddDocument(1, text);

  // Serialize to stringstream
  std::stringstream stream;
  ASSERT_TRUE(index1.SaveToStream(stream));

  // Deserialize from stringstream
  Index index2(3, 2);
  ASSERT_TRUE(index2.LoadFromStream(stream));

  // Verify n-gram configuration is preserved
  EXPECT_EQ(index1.GetNgramSize(), index2.GetNgramSize());
  EXPECT_EQ(index1.GetKanjiNgramSize(), index2.GetKanjiNgramSize());

  // Verify term count
  EXPECT_EQ(index1.TermCount(), index2.TermCount());
}
