/**
 * @file result_compressor.cpp
 * @brief LZ4 compression implementation
 */

#include "cache/result_compressor.h"

#include <lz4.h>

#include <stdexcept>

namespace mygramdb::cache {

std::vector<uint8_t> ResultCompressor::Compress(const std::vector<DocId>& result) {
  if (result.empty()) {
    return {};
  }

  const size_t src_size = result.size() * sizeof(DocId);
  // reinterpret_cast required for LZ4 C API (expects char*)
  const auto* src_data =
      reinterpret_cast<const char*>(result.data());  // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)

  // Calculate maximum compressed size
  const int max_dst_size = LZ4_compressBound(static_cast<int>(src_size));
  if (max_dst_size <= 0) {
    throw std::runtime_error("LZ4_compressBound failed");
  }

  std::vector<uint8_t> compressed(static_cast<size_t>(max_dst_size));

  // Compress with default compression level (fast)
  // reinterpret_cast required for LZ4 C API (expects char*)
  const int compressed_size = LZ4_compress_default(
      src_data, reinterpret_cast<char*>(compressed.data()),  // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
      static_cast<int>(src_size), max_dst_size);

  if (compressed_size <= 0) {
    throw std::runtime_error("LZ4 compression failed");
  }

  // Resize to actual compressed size
  compressed.resize(static_cast<size_t>(compressed_size));
  return compressed;
}

std::vector<DocId> ResultCompressor::Decompress(const std::vector<uint8_t>& compressed, size_t original_size) {
  if (compressed.empty() || original_size == 0) {
    return {};
  }

  // original_size is the number of DocId elements, not bytes
  const size_t original_bytes = original_size * sizeof(DocId);

  // Allocate output buffer
  std::vector<DocId> result(original_size);

  // Decompress
  // reinterpret_cast required for LZ4 C API (expects char*)
  const int decompressed_size = LZ4_decompress_safe(
      reinterpret_cast<const char*>(compressed.data()),  // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
      reinterpret_cast<char*>(result.data()),            // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
      static_cast<int>(compressed.size()), static_cast<int>(original_bytes));

  if (decompressed_size < 0) {
    throw std::runtime_error("LZ4 decompression failed");
  }

  if (static_cast<size_t>(decompressed_size) != original_bytes) {
    throw std::runtime_error("LZ4 decompression size mismatch: expected " + std::to_string(original_bytes) +
                             " bytes, got " + std::to_string(decompressed_size) + " bytes");
  }

  return result;
}

}  // namespace mygramdb::cache
