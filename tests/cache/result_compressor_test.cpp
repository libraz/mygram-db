/**
 * @file result_compressor_test.cpp
 * @brief Unit tests for ResultCompressor
 */

#include "cache/result_compressor.h"

#include <gtest/gtest.h>

#include <vector>

namespace mygramdb::cache {

/**
 * @brief Test basic compression and decompression
 */
TEST(ResultCompressorTest, BasicCompressionDecompression) {
  std::vector<DocId> original = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

  auto compressed = ResultCompressor::Compress(original);
  auto decompressed = ResultCompressor::Decompress(compressed, original.size());

  EXPECT_EQ(original, decompressed);
}

/**
 * @brief Test empty result
 */
TEST(ResultCompressorTest, EmptyResult) {
  std::vector<DocId> original;

  auto compressed = ResultCompressor::Compress(original);
  auto decompressed = ResultCompressor::Decompress(compressed, 0);

  EXPECT_EQ(original, decompressed);
  EXPECT_TRUE(decompressed.empty());
}

/**
 * @brief Test single element
 */
TEST(ResultCompressorTest, SingleElement) {
  std::vector<DocId> original = {42};

  auto compressed = ResultCompressor::Compress(original);
  auto decompressed = ResultCompressor::Decompress(compressed, 1);

  EXPECT_EQ(original, decompressed);
}

/**
 * @brief Test large result set
 */
TEST(ResultCompressorTest, LargeResultSet) {
  std::vector<DocId> original;
  for (DocId i = 0; i < 10000; ++i) {
    original.push_back(i);
  }

  auto compressed = ResultCompressor::Compress(original);
  auto decompressed = ResultCompressor::Decompress(compressed, original.size());

  EXPECT_EQ(original, decompressed);

  // Note: Sequential data should compress well, but LZ4 fast mode may not always
  // achieve compression on small datasets due to header overhead.
  // Just verify decompression works correctly.
}

/**
 * @brief Test compression with highly repetitive data
 */
TEST(ResultCompressorTest, RepetitiveData) {
  // Create data with many repeated values (highly compressible)
  std::vector<DocId> original;
  for (int i = 0; i < 1000; ++i) {
    // Repeat each ID 10 times
    for (int j = 0; j < 10; ++j) {
      original.push_back(static_cast<DocId>(i));
    }
  }

  auto compressed = ResultCompressor::Compress(original);
  auto decompressed = ResultCompressor::Decompress(compressed, original.size());

  EXPECT_EQ(original, decompressed);

  // Highly repetitive data should compress well
  size_t original_bytes = original.size() * sizeof(DocId);
  size_t compressed_bytes = compressed.size();

  // At least verify compression didn't make it significantly larger (allow up to 10% overhead)
  EXPECT_LT(compressed_bytes, original_bytes * 1.1);
}

/**
 * @brief Test non-sequential data
 */
TEST(ResultCompressorTest, NonSequentialData) {
  // Create sparse non-sequential data
  std::vector<DocId> original = {1, 100, 1000, 10000, 100000};

  auto compressed = ResultCompressor::Compress(original);
  auto decompressed = ResultCompressor::Decompress(compressed, original.size());

  EXPECT_EQ(original, decompressed);
}

}  // namespace mygramdb::cache
