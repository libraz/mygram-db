/**
 * @file md5_test.cpp
 * @brief Unit tests for MD5 implementation
 */

#include "query/md5.h"

#include <gtest/gtest.h>

#include <cstring>
#include <string>

namespace mygramdb::cache {

/**
 * @brief Test MD5 against known test vectors from RFC 1321
 */
TEST(MD5Test, RFC1321TestVectors) {
  // Test vector 1: empty string
  {
    uint8_t digest[16];
    MD5::Hash("", digest);
    const uint8_t expected[16] = {0xd4, 0x1d, 0x8c, 0xd9, 0x8f, 0x00, 0xb2, 0x04,
                                  0xe9, 0x80, 0x09, 0x98, 0xec, 0xf8, 0x42, 0x7e};
    EXPECT_EQ(0, std::memcmp(digest, expected, 16));
  }

  // Test vector 2: "a"
  {
    uint8_t digest[16];
    MD5::Hash("a", digest);
    const uint8_t expected[16] = {0x0c, 0xc1, 0x75, 0xb9, 0xc0, 0xf1, 0xb6, 0xa8,
                                  0x31, 0xc3, 0x99, 0xe2, 0x69, 0x77, 0x26, 0x61};
    EXPECT_EQ(0, std::memcmp(digest, expected, 16));
  }

  // Test vector 3: "abc"
  {
    uint8_t digest[16];
    MD5::Hash("abc", digest);
    const uint8_t expected[16] = {0x90, 0x01, 0x50, 0x98, 0x3c, 0xd2, 0x4f, 0xb0,
                                  0xd6, 0x96, 0x3f, 0x7d, 0x28, 0xe1, 0x7f, 0x72};
    EXPECT_EQ(0, std::memcmp(digest, expected, 16));
  }

  // Test vector 4: "message digest"
  {
    uint8_t digest[16];
    MD5::Hash("message digest", digest);
    const uint8_t expected[16] = {0xf9, 0x6b, 0x69, 0x7d, 0x7c, 0xb7, 0x93, 0x8d,
                                  0x52, 0x5a, 0x2f, 0x31, 0xaa, 0xf1, 0x61, 0xd0};
    EXPECT_EQ(0, std::memcmp(digest, expected, 16));
  }

  // Test vector 5: "abcdefghijklmnopqrstuvwxyz"
  {
    uint8_t digest[16];
    MD5::Hash("abcdefghijklmnopqrstuvwxyz", digest);
    const uint8_t expected[16] = {0xc3, 0xfc, 0xd3, 0xd7, 0x61, 0x92, 0xe4, 0x00,
                                  0x7d, 0xfb, 0x49, 0x6c, 0xca, 0x67, 0xe1, 0x3b};
    EXPECT_EQ(0, std::memcmp(digest, expected, 16));
  }
}

/**
 * @brief Test MD5 incremental API
 */
TEST(MD5Test, IncrementalUpdate) {
  // Hash "abc" in one call
  uint8_t digest1[16];
  MD5::Hash("abc", digest1);

  // Hash "abc" incrementally
  MD5 md5;
  md5.Update("a");
  md5.Update("b");
  md5.Update("c");
  uint8_t digest2[16];
  md5.Finalize(digest2);

  EXPECT_EQ(0, std::memcmp(digest1, digest2, 16));
}

/**
 * @brief Test MD5 with long input
 */
TEST(MD5Test, LongInput) {
  std::string input(1000, 'x');
  uint8_t digest[16];
  MD5::Hash(input, digest);

  // Verify it produces some non-zero digest
  bool all_zero = true;
  for (int i = 0; i < 16; ++i) {
    if (digest[i] != 0) {
      all_zero = false;
      break;
    }
  }
  EXPECT_FALSE(all_zero);
}

}  // namespace mygramdb::cache
