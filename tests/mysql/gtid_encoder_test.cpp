/**
 * @file gtid_encoder_test.cpp
 * @brief Unit tests for GtidEncoder class
 *
 * Tests MySQL GTID set encoding to binary format used by COM_BINLOG_DUMP_GTID.
 */

#include "mysql/gtid_encoder.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <stdexcept>
#include <vector>

using namespace mygramdb::mysql;

namespace {

/**
 * @brief Helper to extract a little-endian int64 from binary data
 */
int64_t ReadInt64LE(const std::vector<uint8_t>& data, size_t offset) {
  int64_t result = 0;
  for (size_t i = 0; i < 8; ++i) {
    result |= static_cast<int64_t>(data[offset + i]) << (i * 8);
  }
  return result;
}

/**
 * @brief Helper to extract UUID bytes from binary data
 */
std::vector<uint8_t> ExtractUuid(const std::vector<uint8_t>& data, size_t offset) {
  return std::vector<uint8_t>(data.begin() + offset, data.begin() + offset + 16);
}

}  // namespace

class GtidEncoderTest : public ::testing::Test {};

// ===========================================================================
// Empty GTID set tests
// ===========================================================================

TEST_F(GtidEncoderTest, EmptyGtidSetReturnsZeroSids) {
  auto result = GtidEncoder::Encode("");
  ASSERT_EQ(result.size(), 8u);  // Only n_sids field

  int64_t n_sids = ReadInt64LE(result, 0);
  EXPECT_EQ(n_sids, 0);
}

// ===========================================================================
// Single UUID with single interval tests
// ===========================================================================

TEST_F(GtidEncoderTest, SingleUuidSingleInterval) {
  // Standard UUID format with single interval "1-10"
  std::string gtid = "61d5b289-bccc-11f0-b921-cabbb4ee51f6:1-10";
  auto result = GtidEncoder::Encode(gtid);

  // Expected size: 8 (n_sids) + 16 (uuid) + 8 (n_intervals) + 16 (interval) = 48
  ASSERT_EQ(result.size(), 48u);

  // Check n_sids = 1
  int64_t n_sids = ReadInt64LE(result, 0);
  EXPECT_EQ(n_sids, 1);

  // Check UUID bytes (manually calculated from "61d5b289-bccc-11f0-b921-cabbb4ee51f6")
  std::vector<uint8_t> expected_uuid = {0x61, 0xd5, 0xb2, 0x89, 0xbc, 0xcc, 0x11, 0xf0,
                                        0xb9, 0x21, 0xca, 0xbb, 0xb4, 0xee, 0x51, 0xf6};
  auto uuid = ExtractUuid(result, 8);
  EXPECT_EQ(uuid, expected_uuid);

  // Check n_intervals = 1
  int64_t n_intervals = ReadInt64LE(result, 24);
  EXPECT_EQ(n_intervals, 1);

  // Check interval: start=1, end=11 (exclusive)
  int64_t interval_start = ReadInt64LE(result, 32);
  int64_t interval_end = ReadInt64LE(result, 40);
  EXPECT_EQ(interval_start, 1);
  EXPECT_EQ(interval_end, 11);  // 10 + 1 = 11 (exclusive)
}

TEST_F(GtidEncoderTest, SingleUuidSingleTransaction) {
  // Single transaction number "5" means interval [5, 6)
  std::string gtid = "00000000-0000-0000-0000-000000000001:5";
  auto result = GtidEncoder::Encode(gtid);

  ASSERT_EQ(result.size(), 48u);

  int64_t n_sids = ReadInt64LE(result, 0);
  EXPECT_EQ(n_sids, 1);

  int64_t n_intervals = ReadInt64LE(result, 24);
  EXPECT_EQ(n_intervals, 1);

  int64_t interval_start = ReadInt64LE(result, 32);
  int64_t interval_end = ReadInt64LE(result, 40);
  EXPECT_EQ(interval_start, 5);
  EXPECT_EQ(interval_end, 6);  // 5 + 1 = 6 (exclusive for single transaction)
}

// ===========================================================================
// Single UUID with multiple intervals tests
// ===========================================================================

TEST_F(GtidEncoderTest, SingleUuidMultipleIntervals) {
  // Multiple intervals: "1-3:5-7:9"
  std::string gtid = "00000000-0000-0000-0000-000000000001:1-3:5-7:9";
  auto result = GtidEncoder::Encode(gtid);

  // Expected size: 8 + 16 + 8 + (16 * 3) = 80
  ASSERT_EQ(result.size(), 80u);

  int64_t n_sids = ReadInt64LE(result, 0);
  EXPECT_EQ(n_sids, 1);

  int64_t n_intervals = ReadInt64LE(result, 24);
  EXPECT_EQ(n_intervals, 3);

  // First interval: 1-3 -> [1, 4)
  EXPECT_EQ(ReadInt64LE(result, 32), 1);
  EXPECT_EQ(ReadInt64LE(result, 40), 4);

  // Second interval: 5-7 -> [5, 8)
  EXPECT_EQ(ReadInt64LE(result, 48), 5);
  EXPECT_EQ(ReadInt64LE(result, 56), 8);

  // Third interval: 9 -> [9, 10)
  EXPECT_EQ(ReadInt64LE(result, 64), 9);
  EXPECT_EQ(ReadInt64LE(result, 72), 10);
}

// ===========================================================================
// Multiple UUIDs tests
// ===========================================================================

TEST_F(GtidEncoderTest, MultipleUuidsSeparatedByComma) {
  std::string gtid =
      "00000000-0000-0000-0000-000000000001:1-3,"
      "00000000-0000-0000-0000-000000000002:5-7";
  auto result = GtidEncoder::Encode(gtid);

  // Expected size: 8 + (16 + 8 + 16) * 2 = 8 + 80 = 88
  ASSERT_EQ(result.size(), 88u);

  int64_t n_sids = ReadInt64LE(result, 0);
  EXPECT_EQ(n_sids, 2);

  // First UUID
  auto uuid1 = ExtractUuid(result, 8);
  std::vector<uint8_t> expected_uuid1 = {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                                         0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01};
  EXPECT_EQ(uuid1, expected_uuid1);

  int64_t n_intervals1 = ReadInt64LE(result, 24);
  EXPECT_EQ(n_intervals1, 1);
  EXPECT_EQ(ReadInt64LE(result, 32), 1);  // start
  EXPECT_EQ(ReadInt64LE(result, 40), 4);  // end

  // Second UUID
  auto uuid2 = ExtractUuid(result, 48);
  std::vector<uint8_t> expected_uuid2 = {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                                         0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02};
  EXPECT_EQ(uuid2, expected_uuid2);

  int64_t n_intervals2 = ReadInt64LE(result, 64);
  EXPECT_EQ(n_intervals2, 1);
  EXPECT_EQ(ReadInt64LE(result, 72), 5);  // start
  EXPECT_EQ(ReadInt64LE(result, 80), 8);  // end
}

// ===========================================================================
// Whitespace handling tests
// ===========================================================================

TEST_F(GtidEncoderTest, WhitespaceAroundGtidIsTrimmed) {
  std::string gtid = "  00000000-0000-0000-0000-000000000001:1-3  ";
  auto result = GtidEncoder::Encode(gtid);

  ASSERT_EQ(result.size(), 48u);
  int64_t n_sids = ReadInt64LE(result, 0);
  EXPECT_EQ(n_sids, 1);
}

TEST_F(GtidEncoderTest, WhitespaceAroundCommaIsTrimmed) {
  std::string gtid =
      "00000000-0000-0000-0000-000000000001:1 , "
      "00000000-0000-0000-0000-000000000002:2";
  auto result = GtidEncoder::Encode(gtid);

  int64_t n_sids = ReadInt64LE(result, 0);
  EXPECT_EQ(n_sids, 2);
}

TEST_F(GtidEncoderTest, WhitespaceInIntervalIsTrimmed) {
  std::string gtid = "00000000-0000-0000-0000-000000000001: 1 - 3 ";
  // The interval " 1 - 3 " gets trimmed to "1 - 3"
  // ParseInterval trims and finds dash at position where '1' ends
  // Result: start=1, end=4 (exclusive, so represents transactions 1,2,3)

  auto result = GtidEncoder::Encode(gtid);

  int64_t interval_start = ReadInt64LE(result, 32);
  int64_t interval_end = ReadInt64LE(result, 40);
  EXPECT_EQ(interval_start, 1);
  EXPECT_EQ(interval_end, 4);  // 3 + 1 = 4 (exclusive)
}

// ===========================================================================
// Error cases - Invalid UUID
// ===========================================================================

TEST_F(GtidEncoderTest, InvalidUuidLengthTooShort) {
  std::string gtid = "00000000-0000-0000-0000-00000001:1-3";  // UUID too short
  EXPECT_THROW(GtidEncoder::Encode(gtid), std::invalid_argument);
}

TEST_F(GtidEncoderTest, InvalidUuidLengthTooLong) {
  std::string gtid = "00000000-0000-0000-0000-0000000000001234:1-3";  // UUID too long
  EXPECT_THROW(GtidEncoder::Encode(gtid), std::invalid_argument);
}

TEST_F(GtidEncoderTest, InvalidUuidNonHexCharacter) {
  std::string gtid = "0000000g-0000-0000-0000-000000000001:1-3";  // 'g' is not hex
  EXPECT_THROW(GtidEncoder::Encode(gtid), std::invalid_argument);
}

TEST_F(GtidEncoderTest, InvalidUuidMissingDashes) {
  // UUID without dashes has wrong length
  std::string gtid = "00000000000000000000000000000001:1-3";
  EXPECT_THROW(GtidEncoder::Encode(gtid), std::invalid_argument);
}

TEST_F(GtidEncoderTest, InvalidUuidExtraDashes) {
  std::string gtid = "0000-0000-0000-0000-0000-000000000001:1-3";  // Extra dash
  EXPECT_THROW(GtidEncoder::Encode(gtid), std::invalid_argument);
}

// ===========================================================================
// Error cases - Missing colon
// ===========================================================================

TEST_F(GtidEncoderTest, MissingColonBetweenUuidAndInterval) {
  std::string gtid = "00000000-0000-0000-0000-0000000000011-3";  // No colon
  EXPECT_THROW(GtidEncoder::Encode(gtid), std::invalid_argument);
}

// ===========================================================================
// Error cases - Invalid intervals
// ===========================================================================

TEST_F(GtidEncoderTest, InvalidIntervalStartZero) {
  // GTID transaction numbers start at 1, not 0
  std::string gtid = "00000000-0000-0000-0000-000000000001:0-3";
  EXPECT_THROW(GtidEncoder::Encode(gtid), std::invalid_argument);
}

TEST_F(GtidEncoderTest, InvalidIntervalStartNegative) {
  std::string gtid = "00000000-0000-0000-0000-000000000001:-1-3";
  EXPECT_THROW(GtidEncoder::Encode(gtid), std::invalid_argument);
}

TEST_F(GtidEncoderTest, InvalidIntervalEndBeforeStart) {
  std::string gtid = "00000000-0000-0000-0000-000000000001:5-3";  // 5 > 3
  EXPECT_THROW(GtidEncoder::Encode(gtid), std::invalid_argument);
}

TEST_F(GtidEncoderTest, InvalidIntervalStartEqualsEnd) {
  // "3-3" means transaction 3 only, which should be valid
  // But the code checks interval.end <= interval.start after converting to exclusive
  // So "3-3" -> start=3, end=4, which is valid
  std::string gtid = "00000000-0000-0000-0000-000000000001:3-3";
  auto result = GtidEncoder::Encode(gtid);
  EXPECT_EQ(ReadInt64LE(result, 32), 3);  // start
  EXPECT_EQ(ReadInt64LE(result, 40), 4);  // end (exclusive)
}

TEST_F(GtidEncoderTest, InvalidIntervalEmptyString) {
  std::string gtid = "00000000-0000-0000-0000-000000000001:";  // Empty interval
  // Empty interval is invalid - a UUID entry MUST have at least one interval
  // Bug #57 fix: throw exception for empty intervals
  EXPECT_THROW(GtidEncoder::Encode(gtid), std::invalid_argument);
}

// ===========================================================================
// Edge cases
// ===========================================================================

TEST_F(GtidEncoderTest, LargeTransactionNumber) {
  // Test with a large transaction number
  std::string gtid = "00000000-0000-0000-0000-000000000001:1000000000-1000000010";
  auto result = GtidEncoder::Encode(gtid);

  EXPECT_EQ(ReadInt64LE(result, 32), 1000000000);
  EXPECT_EQ(ReadInt64LE(result, 40), 1000000011);
}

TEST_F(GtidEncoderTest, AllZerosUuid) {
  std::string gtid = "00000000-0000-0000-0000-000000000000:1-3";
  auto result = GtidEncoder::Encode(gtid);

  auto uuid = ExtractUuid(result, 8);
  std::vector<uint8_t> expected_uuid(16, 0x00);
  EXPECT_EQ(uuid, expected_uuid);
}

TEST_F(GtidEncoderTest, AllFsUuid) {
  std::string gtid = "ffffffff-ffff-ffff-ffff-ffffffffffff:1-3";
  auto result = GtidEncoder::Encode(gtid);

  auto uuid = ExtractUuid(result, 8);
  std::vector<uint8_t> expected_uuid(16, 0xff);
  EXPECT_EQ(uuid, expected_uuid);
}

TEST_F(GtidEncoderTest, MixedCaseUuid) {
  // UUID parsing should handle mixed case
  std::string gtid = "AbCdEf01-2345-6789-aBcD-ef0123456789:1-3";
  auto result = GtidEncoder::Encode(gtid);

  auto uuid = ExtractUuid(result, 8);
  std::vector<uint8_t> expected_uuid = {0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89,
                                        0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89};
  EXPECT_EQ(uuid, expected_uuid);
}

// ===========================================================================
// Real-world GTID format tests
// ===========================================================================

TEST_F(GtidEncoderTest, RealWorldGtidFormat) {
  // Typical GTID set from MySQL replication
  std::string gtid = "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-77";
  auto result = GtidEncoder::Encode(gtid);

  ASSERT_EQ(result.size(), 48u);

  int64_t n_sids = ReadInt64LE(result, 0);
  EXPECT_EQ(n_sids, 1);

  int64_t interval_start = ReadInt64LE(result, 32);
  int64_t interval_end = ReadInt64LE(result, 40);
  EXPECT_EQ(interval_start, 1);
  EXPECT_EQ(interval_end, 78);  // 77 + 1
}

TEST_F(GtidEncoderTest, MultiServerGtidSet) {
  // Multiple MySQL servers contributing to GTID set
  std::string gtid =
      "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-77,"
      "a5c42c6e-7234-4e4e-9234-1234567890ab:1-50:100-150";
  auto result = GtidEncoder::Encode(gtid);

  int64_t n_sids = ReadInt64LE(result, 0);
  EXPECT_EQ(n_sids, 2);

  // First SID has 1 interval
  int64_t n_intervals1 = ReadInt64LE(result, 24);
  EXPECT_EQ(n_intervals1, 1);

  // Second SID has 2 intervals
  // Offset: 8 (n_sids) + 16 (uuid1) + 8 (n_intervals1) + 16 (interval1) = 48
  // + 16 (uuid2) = 64
  int64_t n_intervals2 = ReadInt64LE(result, 64);
  EXPECT_EQ(n_intervals2, 2);
}
