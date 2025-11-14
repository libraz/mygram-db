/**
 * @file result_compressor.h
 * @brief LZ4 compression for cached search results
 */

#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

namespace mygramdb::cache {

// Use DocId from storage namespace
using DocId = uint32_t;

/**
 * @brief Compress and decompress search results using LZ4
 *
 * LZ4 provides fast compression (500+ MB/s) and very fast decompression (2+ GB/s),
 * making it ideal for query cache where latency is critical.
 * Typical compression ratio: 2-3x for search results.
 */
class ResultCompressor {
 public:
  /**
   * @brief Compress vector of document IDs
   * @param result Vector of document IDs to compress
   * @return Compressed data
   * @throws std::runtime_error if compression fails
   */
  static std::vector<uint8_t> Compress(const std::vector<DocId>& result);

  /**
   * @brief Decompress to vector of document IDs
   * @param compressed Compressed data
   * @param original_size Original uncompressed size in bytes
   * @return Decompressed vector of document IDs
   * @throws std::runtime_error if decompression fails or size mismatch
   */
  static std::vector<DocId> Decompress(const std::vector<uint8_t>& compressed, size_t original_size);
};

}  // namespace mygramdb::cache
