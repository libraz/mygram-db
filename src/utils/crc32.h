#ifndef MYGRAMDB_UTILS_CRC32_H_
#define MYGRAMDB_UTILS_CRC32_H_

#include <zlib.h>

#include <cstddef>
#include <cstdint>
#include <string>

namespace mygram::utils {

/// @brief Compute CRC32 checksum using zlib.
/// @param data Pointer to the data buffer
/// @param length Size of the data in bytes
/// @return CRC32 checksum value
inline uint32_t ComputeCRC32(const void* data, size_t length) {
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
  return static_cast<uint32_t>(crc32(0L, reinterpret_cast<const Bytef*>(data), static_cast<uInt>(length)));
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
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
  return static_cast<uint32_t>(
      crc32(static_cast<uLong>(crc), reinterpret_cast<const Bytef*>(data), static_cast<uInt>(length)));
}

}  // namespace mygram::utils

#endif  // MYGRAMDB_UTILS_CRC32_H_
