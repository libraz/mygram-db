/**
 * @file rows_parser_json_decode_test.cpp
 * @brief Unit tests for JSON column type decoding in DecodeFieldValue
 */

#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <vector>

#include "mysql/rows_parser_internal.h"
#include "utils/error.h"

#ifdef USE_MYSQL

using mygramdb::mysql::internal::DecodeFieldValue;

// MYSQL_TYPE_JSON = 245

class JsonDecodeTest : public ::testing::Test {};

namespace {

std::vector<unsigned char> LengthPrefixed(uint32_t width, const std::vector<unsigned char>& binary_json) {
  std::vector<unsigned char> data;
  for (uint32_t index = 0; index < width; ++index) {
    data.push_back(static_cast<unsigned char>((binary_json.size() >> (index * 8)) & 0xFFU));
  }
  data.insert(data.end(), binary_json.begin(), binary_json.end());
  return data;
}

const std::vector<unsigned char> kTrue = {0x04, 0x01};

}  // namespace

// ============================================================================
// JSON with valid metadata (1, 2, 3, 4) tests
// ============================================================================

TEST_F(JsonDecodeTest, JsonMetadata1ByteLength) {
  constexpr uint16_t metadata = 1;
  auto data = LengthPrefixed(metadata, kTrue);
  const unsigned char* end = data.data() + data.size();

  auto result = DecodeFieldValue(245, data.data(), metadata, false, end, false);
  ASSERT_TRUE(result.has_value()) << "Error: " << result.error().message();
  EXPECT_EQ(*result, "true");
}

TEST_F(JsonDecodeTest, JsonMetadata2ByteLength) {
  constexpr uint16_t metadata = 2;
  auto data = LengthPrefixed(metadata, kTrue);
  const unsigned char* end = data.data() + data.size();

  auto result = DecodeFieldValue(245, data.data(), metadata, false, end, false);
  ASSERT_TRUE(result.has_value()) << "Error: " << result.error().message();
  EXPECT_EQ(*result, "true");
}

TEST_F(JsonDecodeTest, JsonMetadata3ByteLength) {
  constexpr uint16_t metadata = 3;
  auto data = LengthPrefixed(metadata, kTrue);
  const unsigned char* end = data.data() + data.size();

  auto result = DecodeFieldValue(245, data.data(), metadata, false, end, false);
  ASSERT_TRUE(result.has_value()) << "Error: " << result.error().message();
  EXPECT_EQ(*result, "true");
}

TEST_F(JsonDecodeTest, JsonMetadata4ByteLength) {
  constexpr uint16_t metadata = 4;
  auto data = LengthPrefixed(metadata, kTrue);
  const unsigned char* end = data.data() + data.size();

  auto result = DecodeFieldValue(245, data.data(), metadata, false, end, false);
  ASSERT_TRUE(result.has_value()) << "Error: " << result.error().message();
  EXPECT_EQ(*result, "true");
}

TEST_F(JsonDecodeTest, JsonDefaultMetadata0Uses4Bytes) {
  constexpr uint16_t metadata = 0;
  auto data = LengthPrefixed(4, kTrue);
  const unsigned char* end = data.data() + data.size();

  auto result = DecodeFieldValue(245, data.data(), metadata, false, end, false);
  ASSERT_TRUE(result.has_value()) << "Error: " << result.error().message();
  EXPECT_EQ(*result, "true");
}

TEST_F(JsonDecodeTest, JsonEmptyPayloadIsRejected) {
  constexpr uint16_t metadata = 1;
  std::vector<unsigned char> data = {0};
  const unsigned char* end = data.data() + data.size();

  auto result = DecodeFieldValue(245, data.data(), metadata, false, end, false);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kMySQLFieldTruncated);
}

TEST_F(JsonDecodeTest, DecodesSmallObject) {
  const std::vector<unsigned char> object = {
      0x00,              // small object
      0x01, 0x00,        // element count
      0x10, 0x00,        // encoded byte size
      0x0B, 0x00,        // key offset
      0x01, 0x00,        // key length
      0x07, 0x0C, 0x00,  // int32 value at offset 12
      'a',  0x01, 0x00, 0x00, 0x00,
  };
  auto data = LengthPrefixed(1, object);

  auto result = DecodeFieldValue(245, data.data(), 1, false, data.data() + data.size(), false);

  ASSERT_TRUE(result.has_value()) << result.error().message();
  EXPECT_EQ(*result, R"({"a":1})");
}

TEST_F(JsonDecodeTest, DecodesSmallArrayWithInlineAndStringValues) {
  const std::vector<unsigned char> array = {
      0x02,              // small array
      0x03, 0x00,        // element count
      0x0F, 0x00,        // encoded byte size
      0x04, 0x01, 0x00,  // true
      0x05, 0xFE, 0xFF,  // int16 -2
      0x0C, 0x0D, 0x00,  // string at offset 13
      0x01, 'x',
  };
  auto data = LengthPrefixed(1, array);

  auto result = DecodeFieldValue(245, data.data(), 1, false, data.data() + data.size(), false);

  ASSERT_TRUE(result.has_value()) << result.error().message();
  EXPECT_EQ(*result, R"([true,-2,"x"])");
}

TEST_F(JsonDecodeTest, RejectsTextJsonInsteadOfReturningBinaryGarbage) {
  const std::vector<unsigned char> text = {'{', '"', 'a', '"', ':', '1', '}'};
  auto data = LengthPrefixed(1, text);

  auto result = DecodeFieldValue(245, data.data(), 1, false, data.data() + data.size(), false);

  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kMySQLInvalidMetadata);
}

TEST_F(JsonDecodeTest, JsonNullField) {
  // NULL JSON field returns empty string
  uint16_t metadata = 4;
  unsigned char data[] = {0};
  const unsigned char* end = data + sizeof(data);

  auto result = DecodeFieldValue(245, data, metadata, true, end, false);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*result, "");
}

// ============================================================================
// JSON with invalid metadata tests
// ============================================================================

TEST_F(JsonDecodeTest, JsonInvalidMetadata5ReturnsError) {
  uint16_t metadata = 5;
  unsigned char data[] = {0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
  const unsigned char* end = data + sizeof(data);

  auto result = DecodeFieldValue(245, data, metadata, false, end, false);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kMySQLInvalidMetadata);
}

TEST_F(JsonDecodeTest, JsonInvalidMetadata255ReturnsError) {
  uint16_t metadata = 255;
  unsigned char data[] = {0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
  const unsigned char* end = data + sizeof(data);

  auto result = DecodeFieldValue(245, data, metadata, false, end, false);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kMySQLInvalidMetadata);
}

TEST_F(JsonDecodeTest, JsonInvalidMetadata6ReturnsError) {
  uint16_t metadata = 6;
  unsigned char data[] = {0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
  const unsigned char* end = data + sizeof(data);

  auto result = DecodeFieldValue(245, data, metadata, false, end, false);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kMySQLInvalidMetadata);
}

// ============================================================================
// JSON truncation tests
// ============================================================================

TEST_F(JsonDecodeTest, JsonTruncatedLengthPrefix) {
  // metadata=4 but only 2 bytes available
  uint16_t metadata = 4;
  unsigned char data[] = {0x05, 0x00};
  const unsigned char* end = data + sizeof(data);

  auto result = DecodeFieldValue(245, data, metadata, false, end, false);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kMySQLFieldTruncated);
}

TEST_F(JsonDecodeTest, JsonLengthExceedsBounds) {
  // metadata=1, length says 10 but only 3 bytes of payload
  uint16_t metadata = 1;
  unsigned char data[] = {10, 'a', 'b', 'c'};
  const unsigned char* end = data + sizeof(data);

  auto result = DecodeFieldValue(245, data, metadata, false, end, false);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kMySQLFieldTruncated);
}

#endif  // USE_MYSQL
