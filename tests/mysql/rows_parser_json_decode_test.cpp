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

// ============================================================================
// JSON with valid metadata (1, 2, 3, 4) tests
// ============================================================================

TEST_F(JsonDecodeTest, JsonMetadata1ByteLength) {
  // metadata=1: 1-byte length prefix
  uint16_t metadata = 1;
  // JSON payload: {"a":1} (7 bytes)
  std::string json_str = R"({"a":1})";
  std::vector<unsigned char> data;
  data.push_back(static_cast<unsigned char>(json_str.size()));  // 1-byte length
  data.insert(data.end(), json_str.begin(), json_str.end());
  const unsigned char* end = data.data() + data.size();

  auto result = DecodeFieldValue(245, data.data(), metadata, false, end, false);
  ASSERT_TRUE(result.has_value()) << "Error: " << result.error().message();
  EXPECT_EQ(*result, json_str);
}

TEST_F(JsonDecodeTest, JsonMetadata2ByteLength) {
  // metadata=2: 2-byte length prefix (little-endian)
  uint16_t metadata = 2;
  std::string json_str = R"({"key":"value"})";
  std::vector<unsigned char> data;
  uint16_t len = static_cast<uint16_t>(json_str.size());
  data.push_back(len & 0xFF);
  data.push_back((len >> 8) & 0xFF);
  data.insert(data.end(), json_str.begin(), json_str.end());
  const unsigned char* end = data.data() + data.size();

  auto result = DecodeFieldValue(245, data.data(), metadata, false, end, false);
  ASSERT_TRUE(result.has_value()) << "Error: " << result.error().message();
  EXPECT_EQ(*result, json_str);
}

TEST_F(JsonDecodeTest, JsonMetadata3ByteLength) {
  // metadata=3: 3-byte length prefix (little-endian)
  uint16_t metadata = 3;
  std::string json_str = R"([1,2,3])";
  std::vector<unsigned char> data;
  uint32_t len = static_cast<uint32_t>(json_str.size());
  data.push_back(len & 0xFF);
  data.push_back((len >> 8) & 0xFF);
  data.push_back((len >> 16) & 0xFF);
  data.insert(data.end(), json_str.begin(), json_str.end());
  const unsigned char* end = data.data() + data.size();

  auto result = DecodeFieldValue(245, data.data(), metadata, false, end, false);
  ASSERT_TRUE(result.has_value()) << "Error: " << result.error().message();
  EXPECT_EQ(*result, json_str);
}

TEST_F(JsonDecodeTest, JsonMetadata4ByteLength) {
  // metadata=4: 4-byte length prefix (little-endian)
  uint16_t metadata = 4;
  std::string json_str = R"({"nested":{"x":true}})";
  std::vector<unsigned char> data;
  uint32_t len = static_cast<uint32_t>(json_str.size());
  data.push_back(len & 0xFF);
  data.push_back((len >> 8) & 0xFF);
  data.push_back((len >> 16) & 0xFF);
  data.push_back((len >> 24) & 0xFF);
  data.insert(data.end(), json_str.begin(), json_str.end());
  const unsigned char* end = data.data() + data.size();

  auto result = DecodeFieldValue(245, data.data(), metadata, false, end, false);
  ASSERT_TRUE(result.has_value()) << "Error: " << result.error().message();
  EXPECT_EQ(*result, json_str);
}

TEST_F(JsonDecodeTest, JsonDefaultMetadata0Uses4Bytes) {
  // metadata=0 should default to 4-byte length prefix
  uint16_t metadata = 0;
  std::string json_str = R"({"default":true})";
  std::vector<unsigned char> data;
  uint32_t len = static_cast<uint32_t>(json_str.size());
  data.push_back(len & 0xFF);
  data.push_back((len >> 8) & 0xFF);
  data.push_back((len >> 16) & 0xFF);
  data.push_back((len >> 24) & 0xFF);
  data.insert(data.end(), json_str.begin(), json_str.end());
  const unsigned char* end = data.data() + data.size();

  auto result = DecodeFieldValue(245, data.data(), metadata, false, end, false);
  ASSERT_TRUE(result.has_value()) << "Error: " << result.error().message();
  EXPECT_EQ(*result, json_str);
}

TEST_F(JsonDecodeTest, JsonEmptyPayload) {
  // Zero-length JSON with metadata=1
  uint16_t metadata = 1;
  std::vector<unsigned char> data;
  data.push_back(0);  // length = 0
  const unsigned char* end = data.data() + data.size();

  auto result = DecodeFieldValue(245, data.data(), metadata, false, end, false);
  ASSERT_TRUE(result.has_value()) << "Error: " << result.error().message();
  EXPECT_EQ(*result, "");
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
