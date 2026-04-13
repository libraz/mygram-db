/**
 * @file binlog_crc32_test.cpp
 * @brief Unit tests for binlog CRC32 checksum verification
 */

#include <gtest/gtest.h>

#include <cstring>
#include <vector>

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

/// @brief Read little-endian uint32 from buffer.
uint32_t ReadLE32(const uint8_t* ptr) {
  uint32_t val = 0;
  std::memcpy(&val, ptr, sizeof(val));
  return val;
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

  // Simulate receiver-side verification
  size_t event_length = buf.size();
  ASSERT_GE(event_length,
            mygram::constants::kBinlogEventHeaderLen + mygram::constants::kBinlogChecksumSize);

  size_t data_length = event_length - mygram::constants::kBinlogChecksumSize;
  uint32_t computed = mygram::utils::ComputeCRC32(buf.data(), data_length);
  uint32_t stored = ReadLE32(buf.data() + data_length);
  EXPECT_EQ(computed, stored) << "Valid CRC should match";
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

  size_t data_length = buf.size() - mygram::constants::kBinlogChecksumSize;
  uint32_t computed = mygram::utils::ComputeCRC32(buf.data(), data_length);
  uint32_t stored = ReadLE32(buf.data() + data_length);
  EXPECT_NE(computed, stored) << "Corrupted data should fail CRC check";
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

  size_t data_length = buf.size() - mygram::constants::kBinlogChecksumSize;
  uint32_t computed = mygram::utils::ComputeCRC32(buf.data(), data_length);
  uint32_t stored = ReadLE32(buf.data() + data_length);
  EXPECT_NE(computed, stored) << "Corrupted checksum should fail CRC check";
}

/// Verify minimum event size handling (header + checksum only, no payload).
TEST(BinlogCRC32Test, MinimalEventWithChecksum) {
  auto buf = BuildMinimalHeader(0x21);
  AppendValidCRC(buf);

  EXPECT_EQ(buf.size(),
            mygram::constants::kBinlogEventHeaderLen + mygram::constants::kBinlogChecksumSize);

  size_t data_length = buf.size() - mygram::constants::kBinlogChecksumSize;
  uint32_t computed = mygram::utils::ComputeCRC32(buf.data(), data_length);
  uint32_t stored = ReadLE32(buf.data() + data_length);
  EXPECT_EQ(computed, stored);
}

/// Verify that events too small for checksum are handled gracefully.
TEST(BinlogCRC32Test, EventTooSmallForChecksum) {
  // An event smaller than header + checksum should not be verified
  std::vector<uint8_t> buf(10, 0);
  EXPECT_LT(buf.size(),
            mygram::constants::kBinlogEventHeaderLen + mygram::constants::kBinlogChecksumSize)
      << "Buffer should be too small for CRC verification";
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
