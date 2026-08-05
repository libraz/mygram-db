/**
 * @file binlog_crc32_test.cpp
 * @brief Unit tests for binlog CRC32 checksum verification
 */

#include <gtest/gtest.h>

#include <vector>

#include "mysql/binlog_checksum.h"
#include "mysql/binlog_event_types.h"
#include "utils/constants.h"
#include "utils/crc32.h"

// NOLINTBEGIN(readability-magic-numbers)

namespace {

/// @brief Build a minimal binlog event header (19 bytes) with given type.
std::vector<uint8_t> BuildMinimalHeader(uint8_t event_type) {
  std::vector<uint8_t> buf(mygram::constants::kBinlogEventHeaderLen, 0);
  buf[4] = event_type;  // event_type offset
  return buf;
}

/// @brief Append correct CRC32 checksum to buffer.
void AppendValidCRC(std::vector<uint8_t>& buf) {
  uint32_t crc = mygram::utils::ComputeCRC32(buf.data(), buf.size());
  buf.push_back(static_cast<uint8_t>(crc & 0xFF));
  buf.push_back(static_cast<uint8_t>((crc >> 8) & 0xFF));
  buf.push_back(static_cast<uint8_t>((crc >> 16) & 0xFF));
  buf.push_back(static_cast<uint8_t>((crc >> 24) & 0xFF));
}

}  // namespace

/// Verify CRC32 matches zlib crc32 used by MySQL.
TEST(BinlogCRC32Test, ComputeCRC32MatchesZlib) {
  // "hello" -> known CRC32 value from zlib
  const std::string data = "hello";
  uint32_t crc = mygram::utils::ComputeCRC32(data);
  EXPECT_EQ(crc, 0x3610A686) << "CRC32 of 'hello' should match zlib reference value";
}

/// Verify that a correctly checksummed event passes verification.
TEST(BinlogCRC32Test, ValidChecksumPasses) {
  // Build a header + some payload
  auto buf = BuildMinimalHeader(0x21);  // GTID_LOG_EVENT = 33
  // Add some payload bytes
  for (int i = 0; i < 25; ++i) {
    buf.push_back(static_cast<uint8_t>(i));
  }
  // Append valid CRC
  AppendValidCRC(buf);

  auto result = mygramdb::mysql::VerifyBinlogEventChecksum(buf.data(), buf.size());
  EXPECT_TRUE(result.has_value()) << result.error().message();
}

/// Verify that a corrupted event fails verification.
TEST(BinlogCRC32Test, CorruptedDataFailsVerification) {
  auto buf = BuildMinimalHeader(0x21);
  for (int i = 0; i < 25; ++i) {
    buf.push_back(static_cast<uint8_t>(i));
  }
  AppendValidCRC(buf);

  // Corrupt one byte in the payload
  buf[20] ^= 0xFF;

  auto result = mygramdb::mysql::VerifyBinlogEventChecksum(buf.data(), buf.size());
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kMySQLBinlogChecksumMismatch);
}

/// Verify that a corrupted checksum fails verification.
TEST(BinlogCRC32Test, CorruptedChecksumFailsVerification) {
  auto buf = BuildMinimalHeader(0x21);
  for (int i = 0; i < 25; ++i) {
    buf.push_back(static_cast<uint8_t>(i));
  }
  AppendValidCRC(buf);

  // Corrupt the checksum itself
  buf[buf.size() - 1] ^= 0xFF;

  auto result = mygramdb::mysql::VerifyBinlogEventChecksum(buf.data(), buf.size());
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kMySQLBinlogChecksumMismatch);
}

/// Verify minimum event size handling (header + checksum only, no payload).
TEST(BinlogCRC32Test, MinimalEventWithChecksum) {
  auto buf = BuildMinimalHeader(0x21);
  AppendValidCRC(buf);

  EXPECT_EQ(buf.size(), mygram::constants::kBinlogEventHeaderLen + mygram::constants::kBinlogChecksumSize);

  auto result = mygramdb::mysql::VerifyBinlogEventChecksum(buf.data(), buf.size());
  EXPECT_TRUE(result.has_value()) << result.error().message();
}

/// Verify that events too small for checksum are handled gracefully.
TEST(BinlogCRC32Test, EventTooSmallForChecksum) {
  // An event smaller than header + checksum is rejected by the production verifier.
  std::vector<uint8_t> buf(10, 0);
  auto result = mygramdb::mysql::VerifyBinlogEventChecksum(buf.data(), buf.size());
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kMySQLFieldTruncated);
}

/// Verify CRC32 of empty data.
TEST(BinlogCRC32Test, EmptyDataCRC) {
  uint32_t crc = mygram::utils::ComputeCRC32(nullptr, 0);
  EXPECT_EQ(crc, 0U) << "CRC32 of empty data should be 0";
}

/// Verify CRC computation is consistent across multiple calls.
TEST(BinlogCRC32Test, CRCConsistency) {
  auto buf = BuildMinimalHeader(0x1E);  // WRITE_ROWS_EVENT
  for (int i = 0; i < 100; ++i) {
    buf.push_back(static_cast<uint8_t>(i & 0xFF));
  }

  uint32_t crc1 = mygram::utils::ComputeCRC32(buf.data(), buf.size());
  uint32_t crc2 = mygram::utils::ComputeCRC32(buf.data(), buf.size());
  EXPECT_EQ(crc1, crc2) << "CRC32 should be deterministic";
}

// NOLINTEND(readability-magic-numbers)
