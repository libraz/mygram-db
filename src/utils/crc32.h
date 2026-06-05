#ifndef MYGRAMDB_UTILS_CRC32_H_
#define MYGRAMDB_UTILS_CRC32_H_

#include <zlib.h>

#include <cstddef>
#include <cstdint>
#include <limits>
#include <string>

#include "utils/namespace_compat.h"

namespace mygramdb::utils {

inline uint32_t UpdateCRC32(uint32_t crc, const void* data, size_t length);

/// @brief Compute CRC32 checksum using zlib.
/// @param data Pointer to the data buffer
/// @param length Size of the data in bytes
/// @return CRC32 checksum value
inline uint32_t ComputeCRC32(const void* data, size_t length) {
  return UpdateCRC32(0, data, length);
}

/// @brief Compute CRC32 checksum for a string.
/// @param str Input string
/// @return CRC32 checksum value
inline uint32_t ComputeCRC32(const std::string& str) {
  return ComputeCRC32(str.data(), str.size());
}

/// @brief Incrementally update a running CRC32 checksum.
/// @param crc Previous CRC32 value (use 0 for initial call)
/// @param data Pointer to the data buffer
/// @param length Size of the data in bytes
/// @return Updated CRC32 checksum value
inline uint32_t UpdateCRC32(uint32_t crc, const void* data, size_t length) {
  const auto* bytes = reinterpret_cast<const Bytef*>(data);  // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
  uLong running_crc = static_cast<uLong>(crc);
  size_t remaining = length;
  constexpr size_t kMaxZlibChunk = static_cast<size_t>(std::numeric_limits<uInt>::max());

  while (remaining > 0) {
    const size_t chunk_size = remaining > kMaxZlibChunk ? kMaxZlibChunk : remaining;
    running_crc = crc32(running_crc, bytes, static_cast<uInt>(chunk_size));
    bytes += chunk_size;
    remaining -= chunk_size;
  }

  return static_cast<uint32_t>(running_crc);
}

}  // namespace mygramdb::utils

#endif  // MYGRAMDB_UTILS_CRC32_H_
