/**
 * @file endian_serialization_test.cpp
 * @brief Tests for endian-aware binary serialization (BUG-0076)
 *
 * Verifies that dump files use consistent little-endian format
 * for cross-platform compatibility.
 */

#include <gtest/gtest.h>

#include <cstring>
#include <sstream>

#include "utils/endian_utils.h"

using namespace mygram::utils;

/**
 * @test Verify IsLittleEndian detection works correctly
 */
TEST(EndianUtilsTest, IsLittleEndianDetection) {
  // This test verifies that the endian detection is consistent
  constexpr bool is_le = detail::IsLittleEndian();

  // On x86/x86_64 and ARM (little-endian mode), this should be true
  // On big-endian systems (PowerPC, SPARC), this should be false
  // We can't assert a specific value, but we can verify it's computed correctly
  // by checking that the conversion works correctly

  uint32_t native = 0x01020304;
  uint32_t converted = ToLittleEndian(native);

  // After conversion to little-endian, the bytes should be in LE order
  unsigned char bytes[4];
  std::memcpy(bytes, &converted, 4);

  if (is_le) {
    // On little-endian, value should be unchanged
    EXPECT_EQ(converted, native);
    EXPECT_EQ(bytes[0], 0x04);  // LSB first
    EXPECT_EQ(bytes[1], 0x03);
    EXPECT_EQ(bytes[2], 0x02);
    EXPECT_EQ(bytes[3], 0x01);  // MSB last
  } else {
    // On big-endian, value should be byte-swapped
    EXPECT_NE(converted, native);
    EXPECT_EQ(bytes[0], 0x04);
    EXPECT_EQ(bytes[1], 0x03);
    EXPECT_EQ(bytes[2], 0x02);
    EXPECT_EQ(bytes[3], 0x01);
  }
}

/**
 * @test Verify 16-bit conversion produces correct little-endian bytes
 */
TEST(EndianUtilsTest, ToLittleEndian16) {
  uint16_t value = 0x1234;
  uint16_t le_value = ToLittleEndian(value);

  unsigned char bytes[2];
  std::memcpy(bytes, &le_value, 2);

  // Little-endian: LSB first
  EXPECT_EQ(bytes[0], 0x34);
  EXPECT_EQ(bytes[1], 0x12);
}

/**
 * @test Verify 32-bit conversion produces correct little-endian bytes
 */
TEST(EndianUtilsTest, ToLittleEndian32) {
  uint32_t value = 0x12345678;
  uint32_t le_value = ToLittleEndian(value);

  unsigned char bytes[4];
  std::memcpy(bytes, &le_value, 4);

  // Little-endian: LSB first
  EXPECT_EQ(bytes[0], 0x78);
  EXPECT_EQ(bytes[1], 0x56);
  EXPECT_EQ(bytes[2], 0x34);
  EXPECT_EQ(bytes[3], 0x12);
}

/**
 * @test Verify 64-bit conversion produces correct little-endian bytes
 */
TEST(EndianUtilsTest, ToLittleEndian64) {
  uint64_t value = 0x123456789ABCDEF0ULL;
  uint64_t le_value = ToLittleEndian(value);

  unsigned char bytes[8];
  std::memcpy(bytes, &le_value, 8);

  // Little-endian: LSB first
  EXPECT_EQ(bytes[0], 0xF0);
  EXPECT_EQ(bytes[1], 0xDE);
  EXPECT_EQ(bytes[2], 0xBC);
  EXPECT_EQ(bytes[3], 0x9A);
  EXPECT_EQ(bytes[4], 0x78);
  EXPECT_EQ(bytes[5], 0x56);
  EXPECT_EQ(bytes[6], 0x34);
  EXPECT_EQ(bytes[7], 0x12);
}

/**
 * @test Verify signed integer conversion works correctly
 */
TEST(EndianUtilsTest, SignedIntegerConversion) {
  int32_t negative = -12345;
  int32_t le_value = ToLittleEndian(negative);
  int32_t restored = FromLittleEndian(le_value);
  EXPECT_EQ(restored, negative);

  int64_t negative64 = -9876543210LL;
  int64_t le_value64 = ToLittleEndian(negative64);
  int64_t restored64 = FromLittleEndian(le_value64);
  EXPECT_EQ(restored64, negative64);
}

/**
 * @test Verify roundtrip conversion preserves values
 */
TEST(EndianUtilsTest, RoundtripConversion) {
  // 8-bit (no conversion needed)
  uint8_t u8 = 0xAB;
  EXPECT_EQ(FromLittleEndian(ToLittleEndian(u8)), u8);

  // 16-bit
  uint16_t u16 = 0x1234;
  EXPECT_EQ(FromLittleEndian(ToLittleEndian(u16)), u16);

  int16_t i16 = -12345;
  EXPECT_EQ(FromLittleEndian(ToLittleEndian(i16)), i16);

  // 32-bit
  uint32_t u32 = 0x12345678;
  EXPECT_EQ(FromLittleEndian(ToLittleEndian(u32)), u32);

  int32_t i32 = -123456789;
  EXPECT_EQ(FromLittleEndian(ToLittleEndian(i32)), i32);

  // 64-bit
  uint64_t u64 = 0x123456789ABCDEF0ULL;
  EXPECT_EQ(FromLittleEndian(ToLittleEndian(u64)), u64);

  int64_t i64 = -9876543210123456789LL;
  EXPECT_EQ(FromLittleEndian(ToLittleEndian(i64)), i64);
}

/**
 * @test Verify double conversion roundtrip
 */
TEST(EndianUtilsTest, DoubleConversion) {
  double values[] = {0.0, 1.0, -1.0, 3.14159265358979, -2.71828, 1.0e100, -1.0e-100};

  for (double value : values) {
    double le_value = ToLittleEndianDouble(value);
    double restored = FromLittleEndianDouble(le_value);
    EXPECT_DOUBLE_EQ(restored, value);
  }
}

/**
 * @test Verify bool conversion (should be 1 byte, no conversion)
 */
TEST(EndianUtilsTest, BoolConversion) {
  bool t = true;
  bool f = false;

  EXPECT_EQ(FromLittleEndian(ToLittleEndian(t)), t);
  EXPECT_EQ(FromLittleEndian(ToLittleEndian(f)), f);
}

/**
 * @test Verify stream serialization with little-endian format
 *
 * This simulates how dump files should serialize data.
 */
TEST(EndianUtilsTest, StreamSerialization) {
  std::stringstream ss;

  // Write values in little-endian format
  uint32_t write_u32 = 0x12345678;
  uint64_t write_u64 = 0xFEDCBA9876543210ULL;
  double write_double = 3.14159265358979;

  uint32_t le_u32 = ToLittleEndian(write_u32);
  uint64_t le_u64 = ToLittleEndian(write_u64);
  double le_double = ToLittleEndianDouble(write_double);

  ss.write(reinterpret_cast<const char*>(&le_u32), sizeof(le_u32));
  ss.write(reinterpret_cast<const char*>(&le_u64), sizeof(le_u64));
  ss.write(reinterpret_cast<const char*>(&le_double), sizeof(le_double));

  // Read back values
  ss.seekg(0);

  uint32_t read_le_u32;
  uint64_t read_le_u64;
  double read_le_double;

  ss.read(reinterpret_cast<char*>(&read_le_u32), sizeof(read_le_u32));
  ss.read(reinterpret_cast<char*>(&read_le_u64), sizeof(read_le_u64));
  ss.read(reinterpret_cast<char*>(&read_le_double), sizeof(read_le_double));

  uint32_t read_u32 = FromLittleEndian(read_le_u32);
  uint64_t read_u64 = FromLittleEndian(read_le_u64);
  double read_double = FromLittleEndianDouble(read_le_double);

  EXPECT_EQ(read_u32, write_u32);
  EXPECT_EQ(read_u64, write_u64);
  EXPECT_DOUBLE_EQ(read_double, write_double);
}

/**
 * @test Verify byte representation matches expected little-endian layout
 *
 * This test explicitly checks that the serialized bytes are in little-endian order,
 * which is what dump files expect according to the documentation.
 */
TEST(EndianUtilsTest, ByteLayoutVerification) {
  std::stringstream ss;

  // Write 0x04030201 in little-endian format
  // Expected byte order in file: 01 02 03 04
  uint32_t value = 0x04030201;
  uint32_t le_value = ToLittleEndian(value);
  ss.write(reinterpret_cast<const char*>(&le_value), sizeof(le_value));

  // Verify byte order
  std::string data = ss.str();
  ASSERT_EQ(data.size(), 4);

  EXPECT_EQ(static_cast<uint8_t>(data[0]), 0x01);
  EXPECT_EQ(static_cast<uint8_t>(data[1]), 0x02);
  EXPECT_EQ(static_cast<uint8_t>(data[2]), 0x03);
  EXPECT_EQ(static_cast<uint8_t>(data[3]), 0x04);
}

/**
 * @test Verify reading little-endian bytes produces correct native value
 */
TEST(EndianUtilsTest, ReadLittleEndianBytes) {
  // Simulate reading from a file created on a little-endian system
  unsigned char bytes[] = {0x78, 0x56, 0x34, 0x12};  // Little-endian 0x12345678

  uint32_t le_value;
  std::memcpy(&le_value, bytes, 4);

  uint32_t native_value = FromLittleEndian(le_value);

  // Should read as 0x12345678 on any platform
  EXPECT_EQ(native_value, 0x12345678U);
}

/**
 * @test Verify edge cases
 */
TEST(EndianUtilsTest, EdgeCases) {
  // Zero
  EXPECT_EQ(FromLittleEndian(ToLittleEndian(uint32_t{0})), 0U);
  EXPECT_EQ(FromLittleEndian(ToLittleEndian(uint64_t{0})), 0ULL);

  // Max values
  EXPECT_EQ(FromLittleEndian(ToLittleEndian(UINT16_MAX)), UINT16_MAX);
  EXPECT_EQ(FromLittleEndian(ToLittleEndian(UINT32_MAX)), UINT32_MAX);
  EXPECT_EQ(FromLittleEndian(ToLittleEndian(UINT64_MAX)), UINT64_MAX);

  // Min values (signed)
  EXPECT_EQ(FromLittleEndian(ToLittleEndian(INT16_MIN)), INT16_MIN);
  EXPECT_EQ(FromLittleEndian(ToLittleEndian(INT32_MIN)), INT32_MIN);
  EXPECT_EQ(FromLittleEndian(ToLittleEndian(INT64_MIN)), INT64_MIN);
}
