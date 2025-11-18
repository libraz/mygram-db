/**
 * @file md5.h
 * @brief Simple MD5 implementation for cache key generation
 *
 * Based on RFC 1321 - The MD5 Message-Digest Algorithm
 * This is a simple, standalone implementation without external dependencies.
 *
 * NOLINTBEGIN - Low-level cryptographic implementation following RFC 1321
 */

#pragma once

#include <cstdint>
#include <cstring>
#include <string>

namespace mygramdb::cache {

/**
 * @brief MD5 hasher
 */
class MD5 {
 public:
  MD5();

  /**
   * @brief Update hash with data
   */
  void Update(const uint8_t* data, size_t len);

  /**
   * @brief Update hash with string
   */
  void Update(const std::string& str) { Update(reinterpret_cast<const uint8_t*>(str.data()), str.size()); }

  /**
   * @brief Finalize and get digest
   * @param digest Output buffer (must be 16 bytes)
   */
  void Finalize(uint8_t digest[16]);

  /**
   * @brief Convenience method to hash a string
   */
  static void Hash(const std::string& input, uint8_t digest[16]);

 private:
  void Transform(const uint8_t block[64]);

  uint32_t state_[4];   // A, B, C, D
  uint32_t count_[2];   // Number of bits, modulo 2^64 (lsb first)
  uint8_t buffer_[64];  // Input buffer
};

}  // namespace mygramdb::cache

// NOLINTEND
