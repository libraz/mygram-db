/**
 * @file cache_key.h
 * @brief Cache key generation using MD5 hashing
 */

#pragma once

#include <cstdint>
#include <functional>
#include <string>

namespace mygramdb::cache {

/**
 * @brief Cache key based on MD5 hash
 *
 * Uses MD5 hash (128 bits) as cache key for fast lookup and good distribution.
 * MD5 is suitable for cache keys as we don't need cryptographic security,
 * just fast computation and low collision probability.
 */
struct CacheKey {
  uint64_t hash_high;  ///< Upper 64 bits of MD5
  uint64_t hash_low;   ///< Lower 64 bits of MD5

  /**
   * @brief Default constructor
   */
  CacheKey() : hash_high(0), hash_low(0) {}

  /**
   * @brief Constructor from hash values
   */
  CacheKey(uint64_t high, uint64_t low) : hash_high(high), hash_low(low) {}

  /**
   * @brief Equality comparison
   */
  bool operator==(const CacheKey& other) const { return hash_high == other.hash_high && hash_low == other.hash_low; }

  /**
   * @brief Inequality comparison
   */
  bool operator!=(const CacheKey& other) const { return !(*this == other); }

  /**
   * @brief Less-than comparison (for use in std::map)
   */
  bool operator<(const CacheKey& other) const {
    if (hash_high != other.hash_high) {
      return hash_high < other.hash_high;
    }
    return hash_low < other.hash_low;
  }

  /**
   * @brief Convert to hex string for debugging
   * @return 32-character hex string
   */
  [[nodiscard]] std::string ToString() const;
};

/**
 * @brief Generate cache key from normalized query string
 */
class CacheKeyGenerator {
 public:
  /**
   * @brief Generate cache key using MD5 hash
   * @param normalized_query Normalized query string
   * @return Cache key (MD5 hash split into two 64-bit integers)
   */
  static CacheKey Generate(const std::string& normalized_query);
};

}  // namespace mygramdb::cache

// Hash function for CacheKey (for use in std::unordered_map)
namespace std {
template <>
struct hash<mygramdb::cache::CacheKey> {
  size_t operator()(const mygramdb::cache::CacheKey& key) const noexcept {
    // XOR the two halves for hash combination
    return static_cast<size_t>(key.hash_high ^ key.hash_low);
  }
};
}  // namespace std
