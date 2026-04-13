/**
 * @file rows_parser_enum_set_test.cpp
 * @brief Unit tests for ENUM and SET column type decoding in DecodeFieldValue
 */

#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <vector>

#include "mysql/rows_parser_internal.h"

#ifdef USE_MYSQL

using mygramdb::mysql::internal::DecodeFieldValue;

// MYSQL_TYPE_ENUM = 247
// MYSQL_TYPE_SET = 248

class EnumSetDecodeTest : public ::testing::Test {};

// ============================================================================
// ENUM (type 247) tests
// ============================================================================

TEST_F(EnumSetDecodeTest, Enum1ByteValue) {
  // 1-byte ENUM with metadata high byte = 1
  uint16_t metadata = (1 << 8);  // enum_size = 1
  unsigned char data[] = {42};
  const unsigned char* end = data + sizeof(data);

  auto result = DecodeFieldValue(247, data, metadata, false, end, false);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*result, "42");
}

TEST_F(EnumSetDecodeTest, Enum1ByteZero) {
  uint16_t metadata = (1 << 8);
  unsigned char data[] = {0};
  const unsigned char* end = data + sizeof(data);

  auto result = DecodeFieldValue(247, data, metadata, false, end, false);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*result, "0");
}

TEST_F(EnumSetDecodeTest, Enum1ByteMax) {
  uint16_t metadata = (1 << 8);
  unsigned char data[] = {255};
  const unsigned char* end = data + sizeof(data);

  auto result = DecodeFieldValue(247, data, metadata, false, end, false);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*result, "255");
}

TEST_F(EnumSetDecodeTest, Enum2ByteValue) {
  // 2-byte ENUM with metadata high byte = 2
  uint16_t metadata = (2 << 8);
  // Little-endian: 0x0301 = 1 | (3 << 8) = 769
  unsigned char data[] = {0x01, 0x03};
  const unsigned char* end = data + sizeof(data);

  auto result = DecodeFieldValue(247, data, metadata, false, end, false);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*result, "769");
}

TEST_F(EnumSetDecodeTest, Enum2ByteZero) {
  uint16_t metadata = (2 << 8);
  unsigned char data[] = {0x00, 0x00};
  const unsigned char* end = data + sizeof(data);

  auto result = DecodeFieldValue(247, data, metadata, false, end, false);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*result, "0");
}

TEST_F(EnumSetDecodeTest, EnumDefaultTo1ByteWhenMetadataZero) {
  // When metadata high byte is 0, default to 1-byte enum
  uint16_t metadata = 0;
  unsigned char data[] = {5};
  const unsigned char* end = data + sizeof(data);

  auto result = DecodeFieldValue(247, data, metadata, false, end, false);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*result, "5");
}

TEST_F(EnumSetDecodeTest, Enum1ByteTruncated) {
  uint16_t metadata = (1 << 8);
  unsigned char data[] = {42};
  const unsigned char* end = data;  // No data available

  auto result = DecodeFieldValue(247, data, metadata, false, end, false);
  EXPECT_FALSE(result.has_value());
}

TEST_F(EnumSetDecodeTest, Enum2ByteTruncated) {
  uint16_t metadata = (2 << 8);
  unsigned char data[] = {0x01};
  const unsigned char* end = data + 1;  // Only 1 byte, need 2

  auto result = DecodeFieldValue(247, data, metadata, false, end, false);
  EXPECT_FALSE(result.has_value());
}

TEST_F(EnumSetDecodeTest, EnumNull) {
  uint16_t metadata = (1 << 8);
  unsigned char data[] = {42};
  const unsigned char* end = data + sizeof(data);

  auto result = DecodeFieldValue(247, data, metadata, true, end, false);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*result, "");
}

// ============================================================================
// SET (type 248) tests
// ============================================================================

TEST_F(EnumSetDecodeTest, Set1ByteValue) {
  uint16_t metadata = (1 << 8);   // set_size = 1
  unsigned char data[] = {0x05};  // bits 0 and 2 set
  const unsigned char* end = data + sizeof(data);

  auto result = DecodeFieldValue(248, data, metadata, false, end, false);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*result, "5");
}

TEST_F(EnumSetDecodeTest, Set2ByteValue) {
  uint16_t metadata = (2 << 8);
  // Little-endian: 0x0301 = 1 | (3 << 8) = 769
  unsigned char data[] = {0x01, 0x03};
  const unsigned char* end = data + sizeof(data);

  auto result = DecodeFieldValue(248, data, metadata, false, end, false);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*result, "769");
}

TEST_F(EnumSetDecodeTest, Set4ByteValue) {
  uint16_t metadata = (4 << 8);
  unsigned char data[] = {0x0F, 0x00, 0x00, 0x01};
  const unsigned char* end = data + sizeof(data);

  // 0x0F | (0 << 8) | (0 << 16) | (1 << 24) = 16777231
  auto result = DecodeFieldValue(248, data, metadata, false, end, false);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*result, "16777231");
}

TEST_F(EnumSetDecodeTest, Set8ByteValue) {
  uint16_t metadata = (8 << 8);
  unsigned char data[] = {0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80};
  const unsigned char* end = data + sizeof(data);

  // 1 | (0x80 << 56) = 9223372036854775809
  uint64_t expected = 1ULL | (static_cast<uint64_t>(0x80) << 56);
  auto result = DecodeFieldValue(248, data, metadata, false, end, false);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*result, std::to_string(expected));
}

TEST_F(EnumSetDecodeTest, SetDefaultTo1ByteWhenMetadataZero) {
  uint16_t metadata = 0;
  unsigned char data[] = {7};
  const unsigned char* end = data + sizeof(data);

  auto result = DecodeFieldValue(248, data, metadata, false, end, false);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*result, "7");
}

TEST_F(EnumSetDecodeTest, Set1ByteTruncated) {
  uint16_t metadata = (1 << 8);
  unsigned char data[] = {0x05};
  const unsigned char* end = data;  // No data available

  auto result = DecodeFieldValue(248, data, metadata, false, end, false);
  EXPECT_FALSE(result.has_value());
}

TEST_F(EnumSetDecodeTest, Set4ByteTruncated) {
  uint16_t metadata = (4 << 8);
  unsigned char data[] = {0x01, 0x02};
  const unsigned char* end = data + 2;  // Only 2 bytes, need 4

  auto result = DecodeFieldValue(248, data, metadata, false, end, false);
  EXPECT_FALSE(result.has_value());
}

TEST_F(EnumSetDecodeTest, SetNull) {
  uint16_t metadata = (2 << 8);
  unsigned char data[] = {0xFF, 0xFF};
  const unsigned char* end = data + sizeof(data);

  auto result = DecodeFieldValue(248, data, metadata, true, end, false);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*result, "");
}

#else

TEST(EnumSetDecodeSkipped, MysqlNotEnabled) {
  GTEST_SKIP() << "MySQL support not enabled";
}

#endif  // USE_MYSQL
