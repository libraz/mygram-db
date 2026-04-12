/**
 * @file result_compressor.cpp
 * @brief LZ4 compression implementation
 */

#include "cache/result_compressor.h"

#include <lz4.h>

#include <climits>
#include <string>

using mygram::utils::ErrorCode;
using mygram::utils::MakeError;
using mygram::utils::MakeUnexpected;

namespace mygramdb::cache {

mygram::utils::Expected<std::vector<uint8_t>, mygram::utils::Error> ResultCompressor::Compress(
    const std::vector<DocId>& result) {
  if (result.empty()) {
    return {};
  }

  const size_t src_size = result.size() * sizeof(DocId);

  // Guard against size_t -> int overflow (LZ4 APIs take int)
  if (src_size > static_cast<size_t>(INT_MAX)) {
    return MakeUnexpected(
        MakeError(ErrorCode::kCacheCompressionFailed,
                  "Input size exceeds INT_MAX (" + std::to_string(src_size) + " bytes); cannot pass to LZ4"));
  }

  // reinterpret_cast required for LZ4 C API (expects char*)
  const auto* src_data =
      reinterpret_cast<const char*>(result.data());  // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)

  // Calculate maximum compressed size
  const int max_dst_size = LZ4_compressBound(static_cast<int>(src_size));
  if (max_dst_size <= 0) {
    return MakeUnexpected(MakeError(ErrorCode::kCacheCompressionFailed, "LZ4_compressBound failed"));
  }

  std::vector<uint8_t> compressed(static_cast<size_t>(max_dst_size));

  // Compress with default compression level (fast)
  // reinterpret_cast required for LZ4 C API (expects char*)
  const int compressed_size = LZ4_compress_default(
      src_data, reinterpret_cast<char*>(compressed.data()),  // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
      static_cast<int>(src_size), max_dst_size);

  if (compressed_size <= 0) {
    return MakeUnexpected(MakeError(ErrorCode::kCacheCompressionFailed, "LZ4 compression failed"));
  }

  // Resize to actual compressed size
  compressed.resize(static_cast<size_t>(compressed_size));
  return compressed;
}

mygram::utils::Expected<std::vector<DocId>, mygram::utils::Error> ResultCompressor::Decompress(
    const std::vector<uint8_t>& compressed, size_t original_size) {
  if (compressed.empty() || original_size == 0) {
    return {};
  }

  // original_size is the number of DocId elements, not bytes
  const size_t original_bytes = original_size * sizeof(DocId);

  // Guard against size_t -> int overflow (LZ4 APIs take int)
  if (compressed.size() > static_cast<size_t>(INT_MAX)) {
    return MakeUnexpected(MakeError(
        ErrorCode::kCacheDecompressionFailed,
        "Compressed size exceeds INT_MAX (" + std::to_string(compressed.size()) + " bytes); cannot pass to LZ4"));
  }
  if (original_bytes > static_cast<size_t>(INT_MAX)) {
    return MakeUnexpected(
        MakeError(ErrorCode::kCacheDecompressionFailed,
                  "Original size exceeds INT_MAX (" + std::to_string(original_bytes) + " bytes); cannot pass to LZ4"));
  }

  // Allocate output buffer
  std::vector<DocId> result(original_size);

  // Decompress
  // reinterpret_cast required for LZ4 C API (expects char*)
  const int decompressed_size = LZ4_decompress_safe(
      reinterpret_cast<const char*>(compressed.data()),  // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
      reinterpret_cast<char*>(result.data()),            // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
      static_cast<int>(compressed.size()), static_cast<int>(original_bytes));

  if (decompressed_size < 0) {
    return MakeUnexpected(MakeError(ErrorCode::kCacheDecompressionFailed, "LZ4 decompression failed"));
  }

  if (static_cast<size_t>(decompressed_size) != original_bytes) {
    return MakeUnexpected(MakeError(ErrorCode::kCacheDecompressionFailed,
                                    "LZ4 decompression size mismatch: expected " + std::to_string(original_bytes) +
                                        " bytes, got " + std::to_string(decompressed_size) + " bytes"));
  }

  return result;
}

}  // namespace mygramdb::cache
