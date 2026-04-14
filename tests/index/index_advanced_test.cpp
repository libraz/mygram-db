/**
 * @file index_advanced_test.cpp
 * @brief Unit tests for n-gram inverted index - Advanced features
 */

#include <gtest/gtest.h>

#include <cstdio>
#include <fstream>
#include <unordered_set>
#include <vector>

#include "index/index.h"
#include "utils/string_utils.h"

using namespace mygramdb::index;
using namespace mygram::utils;

/** @brief Test memory usage
 */
TEST(IndexTest, MemoryUsage) {
  Index index(1);

  // Add documents
  index.AddDocument(1, "abc");
  index.AddDocument(2, "def");

  size_t after_two_docs = index.MemoryUsage();
  // After adding documents, memory usage should be positive
  EXPECT_GT(after_two_docs, 0u);

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
  ASSERT_TRUE(index1.SaveToStream(stream).has_value());

  // Deserialize from stringstream
  Index index2(2);
  ASSERT_TRUE(index2.LoadFromStream(stream).has_value());

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
  ASSERT_TRUE(index1.SaveToStream(stream).has_value());

  // Deserialize from stringstream
  Index index2(2, 1);
  ASSERT_TRUE(index2.LoadFromStream(stream).has_value());

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
  ASSERT_TRUE(index1.SaveToStream(stream).has_value());

  // Deserialize from stringstream
  Index index2(2);
  ASSERT_TRUE(index2.LoadFromStream(stream).has_value());

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
  ASSERT_TRUE(index1.SaveToStream(stream).has_value());

  // Deserialize from stringstream
  Index index2(1);
  ASSERT_TRUE(index2.LoadFromStream(stream).has_value());

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
  ASSERT_TRUE(index1.SaveToStream(stream).has_value());

  // Deserialize from stringstream
  Index index2(3, 2);
  ASSERT_TRUE(index2.LoadFromStream(stream).has_value());

  // Verify n-gram configuration is preserved
  EXPECT_EQ(index1.GetNgramSize(), index2.GetNgramSize());
  EXPECT_EQ(index1.GetKanjiNgramSize(), index2.GetKanjiNgramSize());

  // Verify term count
  EXPECT_EQ(index1.TermCount(), index2.TermCount());
}

// ============================================================================
// CRC32 Checksum Tests (V2 format)
// ============================================================================

/**
 * @brief Test corrupted serialized data is detected by CRC32 checksum
 */
TEST(IndexSerializationTest, CorruptedDataDetected) {
  Index index(2, 1);
  index.AddDocument(1, "hello world");
  index.AddDocument(2, "test document");

  // Serialize to file
  std::string filepath = "/tmp/test_index_corruption.bin";
  ASSERT_TRUE(index.SaveToFile(filepath).has_value());

  // Read file, corrupt 1 byte in the middle, write back
  std::ifstream ifs(filepath, std::ios::binary);
  std::vector<uint8_t> data((std::istreambuf_iterator<char>(ifs)), std::istreambuf_iterator<char>());
  ifs.close();

  ASSERT_GT(data.size(), 10U);
  data[data.size() / 2] ^= 0xFF;  // Flip bits in middle byte

  std::ofstream ofs(filepath, std::ios::binary);
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) - Required for binary I/O
  ofs.write(reinterpret_cast<const char*>(data.data()), static_cast<std::streamsize>(data.size()));
  ofs.close();

  // Load should fail due to checksum mismatch
  Index index2(2, 1);
  EXPECT_FALSE(index2.LoadFromFile(filepath).has_value());

  // Cleanup
  std::remove(filepath.c_str());
}

/**
 * @brief Test valid data passes CRC32 checksum verification
 */
TEST(IndexSerializationTest, ValidDataPassesChecksum) {
  Index index(2, 1);
  index.AddDocument(1, "hello world");
  index.AddDocument(2, "test document");
  index.AddDocument(3, "another test");

  std::string filepath = "/tmp/test_index_valid.bin";
  ASSERT_TRUE(index.SaveToFile(filepath).has_value());

  Index index2(2, 1);
  EXPECT_TRUE(index2.LoadFromFile(filepath).has_value());

  // Verify data integrity
  auto results = index2.SearchAnd({"he", "ll"});
  EXPECT_EQ(results.size(), 1U);
  EXPECT_EQ(results[0], 1U);

  // Cleanup
  std::remove(filepath.c_str());
}

/**
 * @brief Test stream-based serialization with CRC32 checksum
 */
TEST(IndexSerializationTest, StreamRoundtripWithChecksum) {
  Index index1(2, 1);
  index1.AddDocument(1, "hello world");
  index1.AddDocument(2, "test document");

  // Serialize to stringstream
  std::stringstream stream;
  ASSERT_TRUE(index1.SaveToStream(stream).has_value());

  // Deserialize from stringstream
  Index index2(2, 1);
  ASSERT_TRUE(index2.LoadFromStream(stream).has_value());

  // Verify term count and search results match
  EXPECT_EQ(index1.TermCount(), index2.TermCount());

  auto results1 = index1.SearchAnd({"he", "ll"});
  auto results2 = index2.SearchAnd({"he", "ll"});
  EXPECT_EQ(results1, results2);
}

/**
 * @brief Test corrupted stream data is detected by CRC32 checksum
 */
TEST(IndexSerializationTest, CorruptedStreamDetected) {
  Index index(2, 1);
  index.AddDocument(1, "hello world");
  index.AddDocument(2, "test document");

  // Serialize to stringstream
  std::stringstream stream;
  ASSERT_TRUE(index.SaveToStream(stream).has_value());

  // Corrupt a byte in the middle of the stream data
  std::string data = stream.str();
  ASSERT_GT(data.size(), 10U);
  data[data.size() / 2] ^= 0xFF;

  std::istringstream corrupted_stream(data, std::ios::binary);

  // Load should fail due to checksum mismatch
  Index index2(2, 1);
  EXPECT_FALSE(index2.LoadFromStream(corrupted_stream).has_value());
}

/**
 * @brief Test truncated data is detected
 */
TEST(IndexSerializationTest, TruncatedDataDetected) {
  Index index(2, 1);
  index.AddDocument(1, "hello world");

  // Serialize to stringstream
  std::stringstream stream;
  ASSERT_TRUE(index.SaveToStream(stream).has_value());

  // Truncate the data (remove last 10 bytes including CRC32)
  std::string data = stream.str();
  ASSERT_GT(data.size(), 10U);
  data.resize(data.size() - 10);

  std::istringstream truncated_stream(data, std::ios::binary);

  // Load should fail
  Index index2(2, 1);
  EXPECT_FALSE(index2.LoadFromStream(truncated_stream).has_value());
}

/**
 * @brief Test SaveToFile with non-writable path returns error with appropriate code
 */
TEST(IndexSerializationTest, SaveToFileInvalidPathReturnsError) {
  Index index(2, 1);
  index.AddDocument(1, "hello world");

  auto result = index.SaveToFile("/nonexistent/directory/test.bin");
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kStorageWriteError);
}

/**
 * @brief Test LoadFromFile with non-existent path returns error with appropriate code
 */
TEST(IndexSerializationTest, LoadFromFileNonExistentReturnsError) {
  Index index(2, 1);

  auto result = index.LoadFromFile("/nonexistent/path/missing.bin");
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kStorageFileNotFound);
}

/**
 * @brief Test LoadFromStream with corrupted data returns error with CRC mismatch code
 */
TEST(IndexSerializationTest, CorruptedStreamReturnsErrorCode) {
  Index index(2, 1);
  index.AddDocument(1, "hello world");

  std::stringstream stream;
  ASSERT_TRUE(index.SaveToStream(stream).has_value());

  // Corrupt a byte in the middle
  std::string data = stream.str();
  ASSERT_GT(data.size(), 10U);
  data[data.size() / 2] ^= 0xFF;

  std::istringstream corrupted_stream(data, std::ios::binary);

  Index index2(2, 1);
  auto result = index2.LoadFromStream(corrupted_stream);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kStorageCRCMismatch);
}

/**
 * @brief Test LoadFromStream with invalid magic returns StorageInvalidFormat error
 */
TEST(IndexSerializationTest, InvalidMagicReturnsError) {
  std::string bad_data = "BADXsomegarbage1234567890";
  std::istringstream stream(bad_data, std::ios::binary);

  Index index(2, 1);
  auto result = index.LoadFromStream(stream);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kStorageInvalidFormat);
}

/**
 * @brief Test LoadFromStream with too-short data returns StorageInvalidFormat error
 */
TEST(IndexSerializationTest, TooShortDataReturnsError) {
  std::string short_data = "MGIX";  // Only magic, no version/ngram/term_count
  std::istringstream stream(short_data, std::ios::binary);

  Index index(2, 1);
  auto result = index.LoadFromStream(stream);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kStorageInvalidFormat);
}

/**
 * @brief Test LoadFromStream with unsupported version returns kStorageVersionMismatch
 *
 * Validates that the error code is kStorageVersionMismatch (not a generic error)
 * when the index data has a version number that is not recognized.
 */
TEST(IndexSerializationTest, UnsupportedVersionReturnsVersionMismatch) {
  // Build a minimal valid-looking index data with version=99
  // Layout: magic(4) + version(4) + ngram_size(4) + term_count(8) = 20 bytes minimum
  std::string data(20, '\0');
  // Magic: "MGIX"
  std::memcpy(data.data(), "MGIX", 4);
  // Version: 99 (unsupported) in little-endian
  uint32_t version = 99;
  std::memcpy(data.data() + 4, &version, sizeof(version));
  // ngram_size: 2
  uint32_t ngram_size = 2;
  std::memcpy(data.data() + 8, &ngram_size, sizeof(ngram_size));
  // term_count: 0
  uint64_t term_count = 0;
  std::memcpy(data.data() + 12, &term_count, sizeof(term_count));

  std::istringstream stream(data, std::ios::binary);

  Index index(2, 1);
  auto result = index.LoadFromStream(stream);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kStorageVersionMismatch)
      << "Unsupported version should return kStorageVersionMismatch, got: " << result.error().message();
  // Context should include the version number
  EXPECT_FALSE(result.error().context().empty()) << "Error context should include the unsupported version number";
}
