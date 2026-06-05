/**
 * @file endian_utils_test.cpp
 * @brief Unit tests for endian conversion utilities
 */

#include "utils/endian_utils.h"

#include <gtest/gtest.h>

#include <cstdint>

using namespace mygramdb::utils;

TEST(EndianUtilsTest, RoundTripUint16) {
  uint16_t value = 0x1234;
  uint16_t le = ToLittleEndian(value);
  uint16_t native = FromLittleEndian(le);
  EXPECT_EQ(native, value);
}

TEST(EndianUtilsTest, RoundTripUint32) {
  uint32_t value = 0x12345678;
  uint32_t le = ToLittleEndian(value);
  uint32_t native = FromLittleEndian(le);
  EXPECT_EQ(native, value);
}

TEST(EndianUtilsTest, RoundTripUint64) {
  uint64_t value = 0x123456789ABCDEF0ULL;
  uint64_t le = ToLittleEndian(value);
  uint64_t native = FromLittleEndian(le);
  EXPECT_EQ(native, value);
}

TEST(EndianUtilsTest, RoundTripDouble) {
  double value = 3.14159265358979;
  double le = ToLittleEndianDouble(value);
  double native = FromLittleEndianDouble(le);
  EXPECT_DOUBLE_EQ(native, value);
}

TEST(EndianUtilsTest, RoundTripDoubleNegative) {
  double value = -123.456;
  double le = ToLittleEndianDouble(value);
  double native = FromLittleEndianDouble(le);
  EXPECT_DOUBLE_EQ(native, value);
}

TEST(EndianUtilsTest, RoundTripZero) {
  EXPECT_EQ(FromLittleEndian(ToLittleEndian(uint16_t{0})), uint16_t{0});
  EXPECT_EQ(FromLittleEndian(ToLittleEndian(uint32_t{0})), uint32_t{0});
  EXPECT_EQ(FromLittleEndian(ToLittleEndian(uint64_t{0})), uint64_t{0});
  EXPECT_DOUBLE_EQ(FromLittleEndianDouble(ToLittleEndianDouble(0.0)), 0.0);
}

TEST(EndianUtilsTest, RoundTripMaxValues) {
  EXPECT_EQ(FromLittleEndian(ToLittleEndian(UINT16_MAX)), UINT16_MAX);
  EXPECT_EQ(FromLittleEndian(ToLittleEndian(UINT32_MAX)), UINT32_MAX);
  EXPECT_EQ(FromLittleEndian(ToLittleEndian(UINT64_MAX)), UINT64_MAX);
}

TEST(EndianUtilsTest, ByteSwapKnownValues) {
  // On a little-endian system, ToLittleEndian should be identity
  // On a big-endian system, bytes should be reversed
  // Either way, round-trip must be identity
  uint32_t value = 0x01020304;
  uint32_t le = ToLittleEndian(value);
  EXPECT_EQ(FromLittleEndian(le), value);
}

TEST(EndianUtilsTest, RoundTripUint8) {
  // Single-byte values should always be identity
  uint8_t value = 0xAB;
  EXPECT_EQ(ToLittleEndian(value), value);
  EXPECT_EQ(FromLittleEndian(value), value);
}

TEST(EndianUtilsTest, IsLittleEndianConsistent) {
  // Just verify the function is callable and returns consistent results
  bool result1 = detail::IsLittleEndian();
  bool result2 = detail::IsLittleEndian();
  EXPECT_EQ(result1, result2);
}
