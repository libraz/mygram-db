/**
 * @file dump_format_v1_test.cpp
 * @brief Unit tests for dump format V1 header serialization
 */

#include "storage/dump_format_v1.h"

#include <gtest/gtest.h>

#include <cstring>
#include <filesystem>
#include <fstream>
#include <sstream>

#include "storage/dump_format.h"
#include "utils/binary_io.h"

using namespace mygramdb::storage::dump_v1;
using mygram::utils::WriteBinary;

/**
 * @brief Test that header_size is non-zero after write
 */
TEST(DumpFormatV1Test, HeaderSizeNonZeroAfterWrite) {
  // Create a header with a GTID
  HeaderV1 header;
  header.header_size = 0;
  header.flags = 0;
  header.dump_timestamp = 1234567890;
  header.total_file_size = 0;
  header.file_crc32 = 0;
  header.gtid = "3E11FA47-71CA-11E1-9E33-C80AA9429562:42";

  // Calculate expected header size:
  // header_size(4) + flags(4) + dump_timestamp(8) + total_file_size(8) +
  // file_crc32(4) + gtid_length_prefix(4) + gtid_data(N)
  uint32_t expected_size = static_cast<uint32_t>(4 + 4 + 8 + 8 + 4 + 4 + header.gtid.size());
  header.header_size = expected_size;

  // Write header to stream
  std::ostringstream oss;
  auto write_result = WriteHeaderV1(oss, header);
  ASSERT_TRUE(write_result.has_value()) << "WriteHeaderV1 failed: " << write_result.error().message();

  // Read header back
  std::istringstream iss(oss.str());
  HeaderV1 read_header;
  auto read_result = ReadHeaderV1(iss, read_header);
  ASSERT_TRUE(read_result.has_value()) << "ReadHeaderV1 failed: " << read_result.error().message();

  // Verify header_size is non-zero and matches expected value
  EXPECT_GT(read_header.header_size, 0u) << "header_size should be non-zero after write";
  EXPECT_EQ(read_header.header_size, expected_size) << "header_size should match calculated value";

  // Verify other fields round-trip correctly
  EXPECT_EQ(read_header.flags, header.flags);
  EXPECT_EQ(read_header.dump_timestamp, header.dump_timestamp);
  EXPECT_EQ(read_header.gtid, header.gtid);
}

/**
 * @brief Test header_size with empty GTID
 */
TEST(DumpFormatV1Test, HeaderSizeWithEmptyGtid) {
  HeaderV1 header;
  header.gtid = "";
  // Expected: 4 + 4 + 8 + 8 + 4 + 4 + 0 = 32
  uint32_t expected_size = 32;
  header.header_size = expected_size;

  std::ostringstream oss;
  auto write_result = WriteHeaderV1(oss, header);
  ASSERT_TRUE(write_result.has_value());

  std::istringstream iss(oss.str());
  HeaderV1 read_header;
  auto read_result = ReadHeaderV1(iss, read_header);
  ASSERT_TRUE(read_result.has_value());

  EXPECT_EQ(read_header.header_size, expected_size);
}

// ============================================================================
// Config section bounds check (#6)
// ============================================================================

/**
 * @brief Test that ReadHeaderV1 fails on truncated stream
 */
TEST(DumpFormatV1Test, ReadHeaderV1FailsOnTruncatedStream) {
  // Write only partial header data (just header_size field)
  std::ostringstream oss;
  uint32_t partial = 32;
  WriteBinary(oss, partial);
  // No more data - stream is truncated

  std::istringstream iss(oss.str());
  HeaderV1 header;
  auto result = ReadHeaderV1(iss, header);
  EXPECT_FALSE(result.has_value()) << "ReadHeaderV1 should fail on truncated stream";
}

// ============================================================================
// VerifyDumpIntegrity with corrupted header (#27)
// ============================================================================

namespace {

/// Create a temporary file path for testing
std::string TestTempFilePath(const std::string& name) {
  auto dir = std::filesystem::temp_directory_path() / "mygramdb_v1_test";
  std::filesystem::create_directories(dir);
  return (dir / (name + ".dmp")).string();
}

/// Clean up a test file
void CleanupTestFile(const std::string& path) {
  std::filesystem::remove(path);
}

}  // namespace

/**
 * @brief Test that VerifyDumpIntegrity propagates ReadHeaderV1 error details
 */
TEST(DumpFormatV1Test, VerifyDumpIntegrityCorruptedHeader) {
  auto filepath = TestTempFilePath("corrupted_header");
  CleanupTestFile(filepath);

  // Write a file with valid magic and version but corrupted/truncated V1 header
  {
    std::ofstream ofs(filepath, std::ios::binary);
    ASSERT_TRUE(ofs.good());
    // Write valid magic number
    ofs.write(mygramdb::storage::dump_format::kMagicNumber.data(), 4);
    // Write valid version (V1 = 1)
    uint32_t version = 1;
    WriteBinary(ofs, version);
    // Write truncated header - just a header_size field with no following data
    uint32_t header_size = 100;  // Claims 100 bytes but file ends here
    WriteBinary(ofs, header_size);
  }

  mygramdb::storage::dump_format::IntegrityError error;
  auto result = VerifyDumpIntegrity(filepath, error);
  EXPECT_FALSE(result.has_value()) << "Should fail with corrupted header";
  EXPECT_EQ(error.type, mygramdb::storage::dump_format::CRCErrorType::FileCRC);
  // Error message should contain details from ReadHeaderV1, not just a generic message
  EXPECT_TRUE(error.message.find("Failed to read V1 header:") != std::string::npos)
      << "Error message should propagate ReadHeaderV1 details, got: " << error.message;

  CleanupTestFile(filepath);
}

/**
 * @brief Test that VerifyDumpIntegrity fails on non-existent file
 */
TEST(DumpFormatV1Test, VerifyDumpIntegrityNonExistentFile) {
  mygramdb::storage::dump_format::IntegrityError error;
  auto result = VerifyDumpIntegrity("/tmp/nonexistent_file_12345.dmp", error);
  EXPECT_FALSE(result.has_value());
  EXPECT_EQ(error.type, mygramdb::storage::dump_format::CRCErrorType::FileCRC);
}

/**
 * @brief Test that VerifyDumpIntegrity fails on invalid magic number
 */
TEST(DumpFormatV1Test, VerifyDumpIntegrityInvalidMagic) {
  auto filepath = TestTempFilePath("invalid_magic");
  CleanupTestFile(filepath);

  {
    std::ofstream ofs(filepath, std::ios::binary);
    ASSERT_TRUE(ofs.good());
    ofs.write("XXXX", 4);  // Invalid magic
    uint32_t version = 1;
    WriteBinary(ofs, version);
  }

  mygramdb::storage::dump_format::IntegrityError error;
  auto result = VerifyDumpIntegrity(filepath, error);
  EXPECT_FALSE(result.has_value());
  EXPECT_EQ(error.type, mygramdb::storage::dump_format::CRCErrorType::FileCRC);

  CleanupTestFile(filepath);
}
