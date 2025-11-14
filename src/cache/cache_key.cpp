/**
 * @file cache_key.cpp
 * @brief Cache key generation implementation
 */

#include "cache/cache_key.h"

#include <iomanip>
#include <sstream>

#include "cache/md5.h"

namespace mygramdb::cache {

std::string CacheKey::ToString() const {
  constexpr int kHexWidth = 16;
  std::ostringstream oss;
  oss << std::hex << std::setfill('0');
  oss << std::setw(kHexWidth) << hash_high;
  oss << std::setw(kHexWidth) << hash_low;
  return oss.str();
}

CacheKey CacheKeyGenerator::Generate(const std::string& normalized_query) {
  constexpr int kMD5DigestSize = 16;
  constexpr int kHalfDigestSize = 8;

  uint8_t digest[kMD5DigestSize];       // NOLINT(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
  MD5::Hash(normalized_query, digest);  // NOLINT(cppcoreguidelines-pro-bounds-array-to-pointer-decay)

  // Extract two 64-bit values from MD5 digest (128 bits total)
  uint64_t hash_high = 0;
  uint64_t hash_low = 0;

  // High 64 bits (bytes 0-7)
  for (int i = 0; i < kHalfDigestSize; ++i) {
    hash_high =
        (hash_high << kHalfDigestSize) | digest[i];  // NOLINT(cppcoreguidelines-pro-bounds-constant-array-index)
  }

  // Low 64 bits (bytes 8-15)
  for (int i = kHalfDigestSize; i < kMD5DigestSize; ++i) {
    hash_low = (hash_low << kHalfDigestSize) | digest[i];  // NOLINT(cppcoreguidelines-pro-bounds-constant-array-index)
  }

  return {hash_high, hash_low};
}

}  // namespace mygramdb::cache
