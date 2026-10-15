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

/// @brief Set the event length field of a frame to its actual size.
void PatchEventLength(std::vector<uint8_t>& buf) {
  for (size_t i = 0; i < 4; ++i) {
    buf[9 + i] = static_cast<uint8_t>((buf.size() >> (8 * i)) & 0xFF);
  }
}

/// @brief A FORMAT_DESCRIPTION_EVENT declaring @p algorithm; the trailer is a valid CRC32 only when @p valid_crc.
std::vector<uint8_t> BuildFormatDescription(const std::string& server_version, uint8_t algorithm, bool valid_crc) {
  auto buf = BuildMinimalHeader(15);
  buf.push_back(4);  // binlog version
  buf.push_back(0);
  std::string version = server_version;
  version.resize(50, '\0');
  buf.insert(buf.end(), version.begin(), version.end());
  buf.insert(buf.end(), {0, 0, 0, 0});  // created
  buf.push_back(19);                    // common header length
  buf.insert(buf.end(), 40, 0x1F);      // post-header lengths
  buf.push_back(algorithm);
  buf.insert(buf.end(), {0xDE, 0xAD, 0xBE, 0xEF});
  PatchEventLength(buf);
  if (valid_crc) {
    const uint32_t crc = mygram::utils::ComputeCRC32(buf.data(), buf.size() - 4);
    for (size_t i = 0; i < 4; ++i) {
      buf[buf.size() - 4 + i] = static_cast<uint8_t>((crc >> (8 * i)) & 0xFF);
    }
  }
  return buf;
}

/// @brief An event of @p type with a payload, framed with a valid CRC32 or none.
std::vector<uint8_t> BuildFrame(uint8_t type, bool with_crc) {
  auto buf = BuildMinimalHeader(type);
  for (int i = 0; i < 10; ++i) {
    buf.push_back(static_cast<uint8_t>(0x40 + i));
  }
  if (with_crc) {
    buf.insert(buf.end(), 4, 0);
  }
  PatchEventLength(buf);
  if (with_crc) {
    const uint32_t crc = mygram::utils::ComputeCRC32(buf.data(), buf.size() - 4);
    for (size_t i = 0; i < 4; ++i) {
      buf[buf.size() - 4 + i] = static_cast<uint8_t>((crc >> (8 * i)) & 0xFF);
    }
  }
  return buf;
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

using mygramdb::mysql::BinlogChecksumAlgorithm;
using mygramdb::mysql::BinlogChecksumTracker;

/// A file written with CRC32 is verified frame by frame, and a mismatch still fails closed.
TEST(BinlogChecksumTrackerTest, Crc32FileIsVerified) {
  BinlogChecksumTracker tracker;
  auto fde = BuildFormatDescription("8.4.11", 1, true);
  ASSERT_TRUE(tracker.Admit(fde.data(), fde.size()).has_value());
  EXPECT_EQ(tracker.algorithm(), BinlogChecksumAlgorithm::kCrc32);

  auto event = BuildFrame(0x21, true);
  auto admitted = tracker.Admit(event.data(), event.size());
  ASSERT_TRUE(admitted.has_value()) << admitted.error().message();
  EXPECT_EQ(admitted->data, event.data());
  EXPECT_EQ(admitted->length, event.size());

  event[25] ^= 0xFF;
  auto corrupted = tracker.Admit(event.data(), event.size());
  ASSERT_FALSE(corrupted.has_value());
  EXPECT_EQ(corrupted.error().code(), mygram::utils::ErrorCode::kMySQLBinlogChecksumMismatch);

  auto bad_fde = BuildFormatDescription("8.4.11", 1, false);
  auto bad = tracker.Admit(bad_fde.data(), bad_fde.size());
  ASSERT_FALSE(bad.has_value());
  EXPECT_EQ(bad.error().code(), mygram::utils::ErrorCode::kMySQLBinlogChecksumMismatch);
}

/// A file written with binlog_checksum=NONE carries no trailer; frames reach the parsers with a zeroed one.
TEST(BinlogChecksumTrackerTest, ChecksumlessFileIsPresentedWithAnEmptyTrailer) {
  BinlogChecksumTracker tracker;
  // The source does not recompute the format description's trailer for such a file.
  auto fde = BuildFormatDescription("8.4.11", 0, false);
  ASSERT_TRUE(tracker.Admit(fde.data(), fde.size()).has_value());
  EXPECT_EQ(tracker.algorithm(), BinlogChecksumAlgorithm::kOff);

  auto event = BuildFrame(0x21, false);
  const size_t original = event.size();
  auto admitted = tracker.Admit(event.data(), event.size());
  ASSERT_TRUE(admitted.has_value()) << admitted.error().message();
  ASSERT_EQ(admitted->length, original + 4);
  for (size_t i = 0; i < original; ++i) {
    if (i < 9 || i >= 13) {
      EXPECT_EQ(admitted->data[i], event[i]) << i;
    }
  }
  for (size_t i = original; i < admitted->length; ++i) {
    EXPECT_EQ(admitted->data[i], 0);
  }
  uint32_t declared = 0;
  for (size_t i = 0; i < 4; ++i) {
    declared |= static_cast<uint32_t>(admitted->data[9 + i]) << (8 * i);
  }
  EXPECT_EQ(declared, original + 4);
}

/// Each file brings its own algorithm; the heartbeat or fake ROTATE between files uses the one last in effect.
TEST(BinlogChecksumTrackerTest, AlgorithmFollowsEachFilesFormatDescription) {
  BinlogChecksumTracker tracker;
  EXPECT_EQ(tracker.algorithm(), BinlogChecksumAlgorithm::kCrc32);

  // Fake ROTATE before the first format description is sent with the algorithm the session requested.
  auto fake_rotate = BuildFrame(4, true);
  EXPECT_TRUE(tracker.Admit(fake_rotate.data(), fake_rotate.size()).has_value());

  auto first = BuildFormatDescription("8.0.30", 0, false);
  ASSERT_TRUE(tracker.Admit(first.data(), first.size()).has_value());
  auto heartbeat = BuildFrame(0x1B, false);
  EXPECT_TRUE(tracker.Admit(heartbeat.data(), heartbeat.size()).has_value());
  // The artificial ROTATE that precedes the next file still follows the previous file.
  auto next_fake_rotate = BuildFrame(4, false);
  EXPECT_TRUE(tracker.Admit(next_fake_rotate.data(), next_fake_rotate.size()).has_value());

  auto second = BuildFormatDescription("8.4.11", 1, true);
  ASSERT_TRUE(tracker.Admit(second.data(), second.size()).has_value());
  EXPECT_EQ(tracker.algorithm(), BinlogChecksumAlgorithm::kCrc32);
  auto checked = BuildFrame(0x21, true);
  checked[30] ^= 0x01;
  EXPECT_FALSE(tracker.Admit(checked.data(), checked.size()).has_value());

  tracker.Reset();
  EXPECT_EQ(tracker.algorithm(), BinlogChecksumAlgorithm::kCrc32);
}

/// A server that predates checksums writes none, and an unknown algorithm byte is not guessed at.
TEST(BinlogChecksumTrackerTest, OldServersAndUnknownAlgorithmsAreHandledExplicitly) {
  for (const char* version : {"5.5.62-log", "5.6.0", "5.1.73", "4.1.22"}) {
    BinlogChecksumTracker tracker;
    auto fde = BuildFormatDescription(version, 1, false);
    ASSERT_TRUE(tracker.Admit(fde.data(), fde.size()).has_value()) << version;
    EXPECT_EQ(tracker.algorithm(), BinlogChecksumAlgorithm::kUndefined) << version;
  }
  for (const char* version :
       {"5.6.1", "5.7.44-log", "8.4.11", "10.11.6-MariaDB-log", "5.5.5-10.6.12-MariaDB", "5.3.0-MariaDB"}) {
    BinlogChecksumTracker tracker;
    auto fde = BuildFormatDescription(version, 1, true);
    ASSERT_TRUE(tracker.Admit(fde.data(), fde.size()).has_value()) << version;
    EXPECT_EQ(tracker.algorithm(), BinlogChecksumAlgorithm::kCrc32) << version;
  }

  BinlogChecksumTracker tracker;
  auto unknown = BuildFormatDescription("8.4.11", 7, true);
  auto result = tracker.Admit(unknown.data(), unknown.size());
  ASSERT_FALSE(result.has_value());
  EXPECT_NE(result.error().message().find("algorithm 7"), std::string::npos) << result.error().message();

  auto truncated = BuildFormatDescription("8.4.11", 1, true);
  truncated.resize(60);
  EXPECT_FALSE(tracker.Admit(truncated.data(), truncated.size()).has_value());
}

// NOLINTEND(readability-magic-numbers)
