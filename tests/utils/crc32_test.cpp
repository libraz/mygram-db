/**
 * @file crc32_test.cpp
 * @brief Unit tests for CRC32 computation utilities
 */

#include "utils/crc32.h"

#include <gtest/gtest.h>

#include <string>

using namespace mygram::utils;

TEST(CRC32Test, EmptyData) {
  uint32_t crc = ComputeCRC32("", 0);
  // CRC32 of empty data is 0
  EXPECT_EQ(crc, 0U);
}

TEST(CRC32Test, EmptyString) {
  std::string empty;
  uint32_t crc = ComputeCRC32(empty);
  EXPECT_EQ(crc, 0U);
}

TEST(CRC32Test, KnownValue) {
  // Well-known CRC32 of "hello" is 0x3610A686
  std::string data = "hello";
  uint32_t crc = ComputeCRC32(data);
  EXPECT_EQ(crc, 0x3610A686U);
}

TEST(CRC32Test, DifferentDataDifferentCRC) {
  uint32_t crc1 = ComputeCRC32(std::string("abc"));
  uint32_t crc2 = ComputeCRC32(std::string("def"));
  EXPECT_NE(crc1, crc2);
}

TEST(CRC32Test, ConsistencyCheck) {
  std::string data = "The quick brown fox jumps over the lazy dog";
  uint32_t crc1 = ComputeCRC32(data);
  uint32_t crc2 = ComputeCRC32(data);
  EXPECT_EQ(crc1, crc2);
}

TEST(CRC32Test, PointerOverloadMatchesStringOverload) {
  std::string data = "test data";
  uint32_t crc_ptr = ComputeCRC32(data.data(), data.size());
  uint32_t crc_str = ComputeCRC32(data);
  EXPECT_EQ(crc_ptr, crc_str);
}
