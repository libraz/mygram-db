/**
 * @file rows_parser_enum_set_test.cpp
 * @brief Unit tests for ENUM and SET column type decoding in DecodeFieldValue
 */

#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <vector>

#include "mysql/binlog_util.h"
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
  uint16_t metadata = 1;  // enum_size = 1
  unsigned char data[] = {42};
  const unsigned char* end = data + sizeof(data);

  auto result = DecodeFieldValue(247, data, metadata, false, end, false);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*result, "42");
}

TEST_F(EnumSetDecodeTest, Enum1ByteZero) {
  uint16_t metadata = 1;
  unsigned char data[] = {0};
  const unsigned char* end = data + sizeof(data);

  auto result = DecodeFieldValue(247, data, metadata, false, end, false);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*result, "0");
}

TEST_F(EnumSetDecodeTest, Enum1ByteMax) {
  uint16_t metadata = 1;
  unsigned char data[] = {255};
  const unsigned char* end = data + sizeof(data);

  auto result = DecodeFieldValue(247, data, metadata, false, end, false);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*result, "255");
}

TEST_F(EnumSetDecodeTest, Enum2ByteValue) {
  uint16_t metadata = 2;
  // Little-endian: 0x0301 = 1 | (3 << 8) = 769
  unsigned char data[] = {0x01, 0x03};
  const unsigned char* end = data + sizeof(data);

  auto result = DecodeFieldValue(247, data, metadata, false, end, false);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*result, "769");
}

TEST_F(EnumSetDecodeTest, Enum2ByteZero) {
  uint16_t metadata = 2;
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
  uint16_t metadata = 1;
  unsigned char data[] = {42};
  const unsigned char* end = data;  // No data available

  auto result = DecodeFieldValue(247, data, metadata, false, end, false);
  EXPECT_FALSE(result.has_value());
}

TEST_F(EnumSetDecodeTest, Enum2ByteTruncated) {
  uint16_t metadata = 2;
  unsigned char data[] = {0x01};
  const unsigned char* end = data + 1;  // Only 1 byte, need 2

  auto result = DecodeFieldValue(247, data, metadata, false, end, false);
  EXPECT_FALSE(result.has_value());
}

TEST_F(EnumSetDecodeTest, EnumNull) {
  uint16_t metadata = 1;
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
  uint16_t metadata = 1;          // set_size = 1
  unsigned char data[] = {0x05};  // bits 0 and 2 set
  const unsigned char* end = data + sizeof(data);

  auto result = DecodeFieldValue(248, data, metadata, false, end, false);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*result, "5");
}

TEST_F(EnumSetDecodeTest, Set2ByteValue) {
  uint16_t metadata = 2;
  // Little-endian: 0x0301 = 1 | (3 << 8) = 769
  unsigned char data[] = {0x01, 0x03};
  const unsigned char* end = data + sizeof(data);

  auto result = DecodeFieldValue(248, data, metadata, false, end, false);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*result, "769");
}

TEST_F(EnumSetDecodeTest, Set4ByteValue) {
  uint16_t metadata = 4;
  unsigned char data[] = {0x0F, 0x00, 0x00, 0x01};
  const unsigned char* end = data + sizeof(data);

  // 0x0F | (0 << 8) | (0 << 16) | (1 << 24) = 16777231
  auto result = DecodeFieldValue(248, data, metadata, false, end, false);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*result, "16777231");
}

TEST_F(EnumSetDecodeTest, Set8ByteValue) {
  uint16_t metadata = 8;
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
  uint16_t metadata = 1;
  unsigned char data[] = {0x05};
  const unsigned char* end = data;  // No data available

  auto result = DecodeFieldValue(248, data, metadata, false, end, false);
  EXPECT_FALSE(result.has_value());
}

TEST_F(EnumSetDecodeTest, Set4ByteTruncated) {
  uint16_t metadata = 4;
  unsigned char data[] = {0x01, 0x02};
  const unsigned char* end = data + 2;  // Only 2 bytes, need 4

  auto result = DecodeFieldValue(248, data, metadata, false, end, false);
  EXPECT_FALSE(result.has_value());
}

TEST_F(EnumSetDecodeTest, SetNull) {
  uint16_t metadata = 2;
  unsigned char data[] = {0xFF, 0xFF};
  const unsigned char* end = data + sizeof(data);

  auto result = DecodeFieldValue(248, data, metadata, true, end, false);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*result, "");
}

TEST_F(EnumSetDecodeTest, StringEncodedEnumReadsConfiguredPackLength) {
  uint16_t metadata = static_cast<uint16_t>((247 << 8) | 2);
  unsigned char data[] = {0x01, 0x03};
  const unsigned char* end = data + sizeof(data);

  auto result = DecodeFieldValue(254, data, metadata, false, end, false);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*result, "769");
  EXPECT_EQ(mygramdb::mysql::binlog_util::calc_field_size(254, data, metadata), 2U);
}

TEST_F(EnumSetDecodeTest, StringEncodedSetReadsEightBytePackLength) {
  uint16_t metadata = static_cast<uint16_t>((248 << 8) | 8);
  unsigned char data[] = {0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80};
  const unsigned char* end = data + sizeof(data);
  uint64_t expected = 1ULL | (static_cast<uint64_t>(0x80) << 56);

  auto result = DecodeFieldValue(254, data, metadata, false, end, false);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*result, std::to_string(expected));
  EXPECT_EQ(mygramdb::mysql::binlog_util::calc_field_size(254, data, metadata), 8U);
}

TEST_F(EnumSetDecodeTest, StringEncodedEnumReportsTruncatedPackLength) {
  uint16_t metadata = static_cast<uint16_t>((247 << 8) | 2);
  unsigned char data[] = {0x01};
  const unsigned char* end = data + sizeof(data);

  auto result = DecodeFieldValue(254, data, metadata, false, end, false);
  EXPECT_FALSE(result.has_value());
}

TEST_F(EnumSetDecodeTest, TinyMediumAndLongBlobTypesFallbackToBlobDecoder) {
  {
    unsigned char data[] = {3, 'a', 'b', 'c'};
    const unsigned char* end = data + sizeof(data);
    auto result = DecodeFieldValue(249, data, 0, false, end, false);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(*result, "abc");
    EXPECT_EQ(mygramdb::mysql::binlog_util::calc_field_size(249, data, 0), 4U);
  }
  {
    unsigned char data[] = {3, 0, 0, 'd', 'e', 'f'};
    const unsigned char* end = data + sizeof(data);
    auto result = DecodeFieldValue(250, data, 0, false, end, false);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(*result, "def");
    EXPECT_EQ(mygramdb::mysql::binlog_util::calc_field_size(250, data, 0), 6U);
  }
  {
    unsigned char data[] = {3, 0, 0, 0, 'g', 'h', 'i'};
    const unsigned char* end = data + sizeof(data);
    auto result = DecodeFieldValue(251, data, 0, false, end, false);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(*result, "ghi");
    EXPECT_EQ(mygramdb::mysql::binlog_util::calc_field_size(251, data, 0), 7U);
  }
}

TEST_F(EnumSetDecodeTest, OldDatetimeFormatsAsComparableTimestampString) {
  const uint64_t encoded = 20240605123456ULL;
  unsigned char data[] = {
      static_cast<unsigned char>(encoded & 0xFF),         static_cast<unsigned char>((encoded >> 8) & 0xFF),
      static_cast<unsigned char>((encoded >> 16) & 0xFF), static_cast<unsigned char>((encoded >> 24) & 0xFF),
      static_cast<unsigned char>((encoded >> 32) & 0xFF), static_cast<unsigned char>((encoded >> 40) & 0xFF),
      static_cast<unsigned char>((encoded >> 48) & 0xFF), static_cast<unsigned char>((encoded >> 56) & 0xFF)};
  const unsigned char* end = data + sizeof(data);

  auto result = DecodeFieldValue(12, data, 0, false, end, false);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*result, "2024-06-05 12:34:56");
}

TEST_F(EnumSetDecodeTest, OldDatetimeReportsTruncatedPayload) {
  unsigned char data[] = {0x01, 0x02, 0x03, 0x04};
  const unsigned char* end = data + sizeof(data);

  auto result = DecodeFieldValue(12, data, 0, false, end, false);
  EXPECT_FALSE(result.has_value());
}

TEST_F(EnumSetDecodeTest, GeometryReportsTruncatedLengthPrefix) {
  unsigned char data[] = {0x03, 0x00};
  const unsigned char* end = data + sizeof(data);

  auto result = DecodeFieldValue(255, data, 4, false, end, false);
  EXPECT_FALSE(result.has_value());
}

TEST_F(EnumSetDecodeTest, GeometryReportsTruncatedPayload) {
  unsigned char data[] = {0x04, 0x00, 0x00, 0x00, 0x01, 0x02};
  const unsigned char* end = data + sizeof(data);

  auto result = DecodeFieldValue(255, data, 4, false, end, false);
  EXPECT_FALSE(result.has_value());
}

#else

TEST(EnumSetDecodeSkipped, MysqlNotEnabled) {
  GTEST_SKIP() << "MySQL support not enabled";
}

#endif  // USE_MYSQL
