/**
 * @file rows_parser_json_decode_test.cpp
 * @brief Unit tests for JSON column type decoding in DecodeFieldValue
 */

#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <limits>
#include <string>
#include <vector>

#include "mysql/binary_json.h"
#include "mysql/rows_parser_internal.h"
#include "utils/error.h"

#ifdef USE_MYSQL

using mygramdb::mysql::DecodeBinaryJson;
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

void AppendLittleEndian(std::vector<unsigned char>& output, uint64_t value, size_t width) {
  for (size_t index = 0; index < width; ++index) {
    output.push_back(static_cast<unsigned char>((value >> (index * 8)) & 0xFFU));
  }
}

std::vector<unsigned char> Signed64(int64_t value) {
  std::vector<unsigned char> result{0x09};
  uint64_t bits = 0;
  std::memcpy(&bits, &value, sizeof(bits));
  AppendLittleEndian(result, bits, sizeof(bits));
  return result;
}

std::vector<unsigned char> Double(double value) {
  std::vector<unsigned char> result{0x0B};
  uint64_t bits = 0;
  std::memcpy(&bits, &value, sizeof(bits));
  AppendLittleEndian(result, bits, sizeof(bits));
  return result;
}

std::vector<unsigned char> WrapSmallArray(const std::vector<unsigned char>& value) {
  constexpr size_t kHeaderSize = 4;
  constexpr size_t kValueEntrySize = 3;
  const size_t value_offset = kHeaderSize + kValueEntrySize;
  const size_t encoded_size = value_offset + value.size() - 1;

  std::vector<unsigned char> result{0x02};
  AppendLittleEndian(result, 1, 2);
  AppendLittleEndian(result, encoded_size, 2);
  result.push_back(value.front());
  AppendLittleEndian(result, value_offset, 2);
  result.insert(result.end(), value.begin() + 1, value.end());
  return result;
}

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

TEST_F(JsonDecodeTest, DecodesLargeObjectAndLargeArrayContainers) {
  const std::vector<unsigned char> object = {
      0x01,                    // large object
      0x01, 0x00, 0x00, 0x00,  // element count
      0x14, 0x00, 0x00, 0x00,  // encoded byte size
      0x13, 0x00, 0x00, 0x00,  // key offset
      0x01, 0x00,              // key length
      0x08,                    // inline uint32
      0xFF, 0xFF, 0xFF, 0xFF,  // UINT32_MAX
      'n',
  };
  auto object_result = DecodeBinaryJson(object.data(), object.size());
  ASSERT_TRUE(object_result.has_value()) << object_result.error().message();
  EXPECT_EQ(*object_result, R"({"n":4294967295})");

  const std::vector<unsigned char> array = {
      0x03,                          // large array
      0x02, 0x00, 0x00, 0x00,        // element count
      0x12, 0x00, 0x00, 0x00,        // encoded byte size
      0x07, 0x01, 0x00, 0x00, 0x00,  // inline int32 1
      0x07, 0xFE, 0xFF, 0xFF, 0xFF,  // inline int32 -2
  };
  auto array_result = DecodeBinaryJson(array.data(), array.size());
  ASSERT_TRUE(array_result.has_value()) << array_result.error().message();
  EXPECT_EQ(*array_result, "[1,-2]");
}

TEST_F(JsonDecodeTest, DecodesInt64AndDoubleScalarsWithoutPrecisionLoss) {
  const auto minimum = Signed64(std::numeric_limits<int64_t>::min());
  auto integer_result = DecodeBinaryJson(minimum.data(), minimum.size());
  ASSERT_TRUE(integer_result.has_value()) << integer_result.error().message();
  EXPECT_EQ(*integer_result, "-9223372036854775808");

  const auto precise_double = Double(1.2345678901234567);
  auto double_result = DecodeBinaryJson(precise_double.data(), precise_double.size());
  ASSERT_TRUE(double_result.has_value()) << double_result.error().message();
  EXPECT_EQ(*double_result, "1.2345678901234567");

  const auto non_finite = Double(std::numeric_limits<double>::infinity());
  auto non_finite_result = DecodeBinaryJson(non_finite.data(), non_finite.size());
  ASSERT_FALSE(non_finite_result.has_value());
  EXPECT_EQ(non_finite_result.error().code(), mygram::utils::ErrorCode::kMySQLInvalidMetadata);
}

TEST_F(JsonDecodeTest, EnforcesNestingDepthLimit) {
  std::vector<unsigned char> at_limit = {0x0C, 0x01, 'x'};
  for (size_t depth = 0; depth < 100; ++depth) {
    at_limit = WrapSmallArray(at_limit);
  }
  auto accepted = DecodeBinaryJson(at_limit.data(), at_limit.size());
  ASSERT_TRUE(accepted.has_value()) << accepted.error().message();

  const auto beyond_limit = WrapSmallArray(at_limit);
  auto rejected = DecodeBinaryJson(beyond_limit.data(), beyond_limit.size());
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().code(), mygram::utils::ErrorCode::kMySQLInvalidMetadata);
  EXPECT_NE(rejected.error().message().find("nesting is too deep"), std::string::npos);
}

TEST_F(JsonDecodeTest, RejectsContainerAboveElementLimitBeforeEntryTraversal) {
  std::vector<unsigned char> array{0x03};
  AppendLittleEndian(array, 1'000'001, 4);
  AppendLittleEndian(array, 8, 4);

  auto result = DecodeBinaryJson(array.data(), array.size());

  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kMySQLInvalidMetadata);
  EXPECT_NE(result.error().message().find("container size"), std::string::npos);
}

TEST_F(JsonDecodeTest, EnforcesDecodedOutputByteLimitBeforeAppending) {
  const std::vector<unsigned char> string_value = {0x0C, 0x04, 'a', 'b', 'c', 'd'};

  auto accepted = DecodeBinaryJson(string_value.data(), string_value.size(), 6);
  ASSERT_TRUE(accepted.has_value()) << accepted.error().message();
  EXPECT_EQ(*accepted, R"("abcd")");

  auto rejected = DecodeBinaryJson(string_value.data(), string_value.size(), 5);
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().code(), mygram::utils::ErrorCode::kMySQLInvalidMetadata);
  EXPECT_NE(rejected.error().message().find("output exceeds"), std::string::npos);
}

TEST_F(JsonDecodeTest, RejectsContainerOffsetCycleBeforeDepthExpansion) {
  const std::vector<unsigned char> cyclic_array = {
      0x02,        // small array
      0x01, 0x00,  // element count
      0x07, 0x00,  // encoded byte size
      0x02,        // nested small array
      0x00, 0x00,  // value offset points back to this container's header
  };

  auto result = DecodeBinaryJson(cyclic_array.data(), cyclic_array.size());

  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kMySQLInvalidMetadata);
  EXPECT_NE(result.error().message().find("cycle detected"), std::string::npos);
}

TEST_F(JsonDecodeTest, OpaqueJsonValueIsSafelyPreservedAsPlaceholder) {
  // Opaque JSON envelope: tag, MySQL field type (NEWDECIMAL), payload length
  // (zero). The placeholder keeps the overall JSON valid so a deterministic
  // unsupported scalar cannot make binlog replication retry the same GTID.
  const std::vector<unsigned char> opaque = {0x0F, 0xF6, 0x00};
  auto data = LengthPrefixed(1, opaque);

  auto result = DecodeFieldValue(245, data.data(), 1, false, data.data() + data.size(), false);

  ASSERT_TRUE(result.has_value()) << result.error().message();
  EXPECT_EQ(*result, R"("__mygramdb_opaque_json_type_246__")");
}

TEST_F(JsonDecodeTest, TruncatedOpaqueJsonValueIsRejected) {
  const std::vector<unsigned char> opaque = {0x0F, 0xF6, 0x02, 0x12};
  auto data = LengthPrefixed(1, opaque);

  auto result = DecodeFieldValue(245, data.data(), 1, false, data.data() + data.size(), false);

  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kMySQLFieldTruncated);
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
