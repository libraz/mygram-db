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
#include <unordered_map>

#include "storage/dump_format.h"
#include "utils/binary_io.h"
#include "utils/error.h"

using namespace mygramdb::storage::dump_v1;
using mygram::utils::WriteBinary;
using mygramdb::config::Config;
using mygramdb::config::TableConfig;

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

TEST(DumpFormatV1Test, ConfigRoundTripPreservesPerTableDatabase) {
  Config write_config;
  write_config.mysql.database = "default_db";

  TableConfig live;
  live.name = "articles";
  live.database = "live_db";
  live.text_source.column = "body";
  write_config.tables.push_back(live);

  TableConfig archive = live;
  archive.database = "archive_db";
  write_config.tables.push_back(archive);

  std::ostringstream output;
  auto write_result = SerializeConfig(output, write_config);
  ASSERT_TRUE(write_result.has_value()) << write_result.error().message();

  Config read_config;
  std::istringstream input(output.str());
  auto read_result = DeserializeConfig(input, read_config);
  ASSERT_TRUE(read_result.has_value()) << read_result.error().message();

  ASSERT_EQ(read_config.tables.size(), 2u);
  EXPECT_EQ(read_config.tables[0].name, "articles");
  EXPECT_EQ(read_config.tables[0].database, "live_db");
  EXPECT_EQ(read_config.tables[1].name, "articles");
  EXPECT_EQ(read_config.tables[1].database, "archive_db");
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

void WriteMinimalDumpV1(const std::string& filepath) {
  mygramdb::config::Config config;
  config.mysql.database = "testdb";
  std::unordered_map<std::string, std::pair<mygramdb::index::Index*, mygramdb::storage::DocumentStore*>> contexts;

  auto result = WriteDumpV1(filepath, "GTID:1", config, contexts);
  ASSERT_TRUE(result.has_value()) << result.error().message();
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

TEST(DumpFormatV1Test, RejectsZeroTotalFileSizeHeader) {
  auto filepath = TestTempFilePath("zero_total_file_size");
  CleanupTestFile(filepath);
  WriteMinimalDumpV1(filepath);

  {
    std::fstream fs(filepath, std::ios::in | std::ios::out | std::ios::binary);
    ASSERT_TRUE(fs.good());
    fs.seekp(kHeaderTotalFileSizeOffset);
    uint64_t zero = 0;
    ASSERT_TRUE(WriteBinary(fs, zero));
  }

  mygramdb::storage::dump_format::IntegrityError verify_error;
  auto verify_result = VerifyDumpIntegrity(filepath, verify_error);
  EXPECT_FALSE(verify_result.has_value());
  EXPECT_EQ(verify_error.type, mygramdb::storage::dump_format::CRCErrorType::FileCRC);
  EXPECT_NE(verify_error.message.find("total_file_size is zero"), std::string::npos);

  std::string gtid = "unchanged";
  mygramdb::config::Config config;
  mygramdb::storage::dump_format::IntegrityError read_error;
  std::unordered_map<std::string, std::pair<mygramdb::index::Index*, mygramdb::storage::DocumentStore*>> contexts;
  auto read_result = ReadDumpV1(filepath, gtid, config, contexts, nullptr, nullptr, &read_error);
  EXPECT_FALSE(read_result.has_value());
  EXPECT_EQ(read_error.type, mygramdb::storage::dump_format::CRCErrorType::FileCRC);
  EXPECT_NE(read_error.message.find("total_file_size is zero"), std::string::npos);
  EXPECT_EQ(gtid, "unchanged");

  CleanupTestFile(filepath);
}

TEST(DumpFormatV1Test, RejectsZeroFileCrcHeader) {
  auto filepath = TestTempFilePath("zero_file_crc");
  CleanupTestFile(filepath);
  WriteMinimalDumpV1(filepath);

  {
    std::fstream fs(filepath, std::ios::in | std::ios::out | std::ios::binary);
    ASSERT_TRUE(fs.good());
    fs.seekp(kHeaderFileCRC32Offset);
    uint32_t zero = 0;
    ASSERT_TRUE(WriteBinary(fs, zero));
  }

  mygramdb::storage::dump_format::IntegrityError verify_error;
  auto verify_result = VerifyDumpIntegrity(filepath, verify_error);
  EXPECT_FALSE(verify_result.has_value());
  EXPECT_EQ(verify_error.type, mygramdb::storage::dump_format::CRCErrorType::FileCRC);
  EXPECT_NE(verify_error.message.find("file_crc32 is zero"), std::string::npos);

  std::string gtid = "unchanged";
  mygramdb::config::Config config;
  mygramdb::storage::dump_format::IntegrityError read_error;
  std::unordered_map<std::string, std::pair<mygramdb::index::Index*, mygramdb::storage::DocumentStore*>> contexts;
  auto read_result = ReadDumpV1(filepath, gtid, config, contexts, nullptr, nullptr, &read_error);
  EXPECT_FALSE(read_result.has_value());
  EXPECT_EQ(read_error.type, mygramdb::storage::dump_format::CRCErrorType::FileCRC);
  EXPECT_NE(read_error.message.find("file_crc32 is zero"), std::string::npos);
  EXPECT_EQ(gtid, "unchanged");

  CleanupTestFile(filepath);
}

// ============================================================================
// ReadDumpV1 error code validation
// ============================================================================

/**
 * @brief Test that ReadDumpV1 returns kStorageVersionMismatch for version too new
 *
 * Validates that the error propagation fix correctly returns a
 * kStorageVersionMismatch error code when the dump file has a version
 * number higher than the maximum supported version.
 */
TEST(DumpFormatV1Test, ReadDumpVersionTooNewReturnsVersionMismatch) {
  auto filepath = TestTempFilePath("version_too_new");
  CleanupTestFile(filepath);

  {
    std::ofstream ofs(filepath, std::ios::binary);
    ASSERT_TRUE(ofs.good());
    // Write valid magic number
    ofs.write(mygramdb::storage::dump_format::kMagicNumber.data(), 4);
    // Write version 999 (far beyond max supported)
    uint32_t version = 999;
    WriteBinary(ofs, version);
  }

  std::string gtid;
  mygramdb::config::Config config;
  std::unordered_map<std::string, std::pair<mygramdb::index::Index*, mygramdb::storage::DocumentStore*>> contexts;

  auto result = ReadDumpV1(filepath, gtid, config, contexts, nullptr, nullptr, nullptr);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kStorageVersionMismatch);
  // Error message should indicate version is too new
  EXPECT_NE(result.error().message().find("too new"), std::string::npos)
      << "Error message should mention 'too new', got: " << result.error().message();
  // Context should contain the filepath
  EXPECT_FALSE(result.error().context().empty());

  CleanupTestFile(filepath);
}

/**
 * @brief Test that ReadDumpV1 returns kStorageDumpReadError for invalid magic
 *
 * Validates that reading a file with wrong magic bytes returns the correct
 * error code with filepath context.
 */
TEST(DumpFormatV1Test, ReadDumpInvalidMagicReturnsDumpReadError) {
  auto filepath = TestTempFilePath("bad_magic");
  CleanupTestFile(filepath);

  {
    std::ofstream ofs(filepath, std::ios::binary);
    ASSERT_TRUE(ofs.good());
    ofs.write("XXXX", 4);  // Invalid magic
    uint32_t version = 1;
    WriteBinary(ofs, version);
  }

  std::string gtid;
  mygramdb::config::Config config;
  std::unordered_map<std::string, std::pair<mygramdb::index::Index*, mygramdb::storage::DocumentStore*>> contexts;

  auto result = ReadDumpV1(filepath, gtid, config, contexts, nullptr, nullptr, nullptr);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kStorageDumpReadError);
  // Error message should mention magic number
  EXPECT_NE(result.error().message().find("magic"), std::string::npos)
      << "Error message should mention 'magic', got: " << result.error().message();
  // Context should include the filepath
  EXPECT_FALSE(result.error().context().empty());

  CleanupTestFile(filepath);
}

/**
 * @brief Test that ReadDumpV1 returns kStorageDumpReadError for non-existent file
 */
TEST(DumpFormatV1Test, ReadDumpNonExistentFileReturnsReadError) {
  std::string filepath = "/tmp/mygramdb_nonexistent_dump_test_file.dmp";
  std::filesystem::remove(filepath);  // Ensure it doesn't exist

  std::string gtid;
  mygramdb::config::Config config;
  std::unordered_map<std::string, std::pair<mygramdb::index::Index*, mygramdb::storage::DocumentStore*>> contexts;

  auto result = ReadDumpV1(filepath, gtid, config, contexts, nullptr, nullptr, nullptr);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kStorageDumpReadError);
  EXPECT_FALSE(result.error().context().empty());
}

/**
 * @brief Test that ReadDumpV1 returns kStorageDumpReadError for truncated file
 *
 * A file that has only the magic number but no version should fail.
 */
TEST(DumpFormatV1Test, ReadDumpTruncatedFileReturnsReadError) {
  auto filepath = TestTempFilePath("truncated");
  CleanupTestFile(filepath);

  {
    std::ofstream ofs(filepath, std::ios::binary);
    ASSERT_TRUE(ofs.good());
    // Write only magic, no version
    ofs.write(mygramdb::storage::dump_format::kMagicNumber.data(), 4);
  }

  std::string gtid;
  mygramdb::config::Config config;
  std::unordered_map<std::string, std::pair<mygramdb::index::Index*, mygramdb::storage::DocumentStore*>> contexts;

  auto result = ReadDumpV1(filepath, gtid, config, contexts, nullptr, nullptr, nullptr);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kStorageDumpReadError);

  CleanupTestFile(filepath);
}

TEST(DumpFormatV1Test, ReadDumpDoesNotPublishGtidOnCrcFailure) {
  auto filepath = TestTempFilePath("crc_failure_gtid");
  CleanupTestFile(filepath);
  WriteMinimalDumpV1(filepath);

  {
    std::fstream fs(filepath, std::ios::in | std::ios::out | std::ios::binary);
    ASSERT_TRUE(fs.good());
    fs.seekg(0, std::ios::end);
    const auto file_size = fs.tellg();
    ASSERT_GT(file_size, std::streampos{kHeaderFileCRC32Offset + static_cast<std::streamoff>(sizeof(uint32_t))});
    fs.seekg(file_size - std::streamoff{1});
    char byte = '\0';
    fs.read(&byte, 1);
    ASSERT_TRUE(fs.good());
    fs.seekp(file_size - std::streamoff{1});
    byte ^= 0x01;
    fs.write(&byte, 1);
    ASSERT_TRUE(fs.good());
  }

  std::string gtid = "unchanged";
  mygramdb::config::Config config;
  std::unordered_map<std::string, std::pair<mygramdb::index::Index*, mygramdb::storage::DocumentStore*>> contexts;
  auto result = ReadDumpV1(filepath, gtid, config, contexts, nullptr, nullptr, nullptr);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kStorageDumpReadError);
  EXPECT_EQ(gtid, "unchanged");

  CleanupTestFile(filepath);
}

TEST(DumpFormatV1Test, ReadDumpRejectsTruncatedDeclaredConfigSection) {
  auto filepath = TestTempFilePath("truncated_config_section");
  CleanupTestFile(filepath);

  {
    std::ofstream ofs(filepath, std::ios::binary);
    ASSERT_TRUE(ofs.good());
    ofs.write(mygramdb::storage::dump_format::kMagicNumber.data(), 4);
    uint32_t version = static_cast<uint32_t>(mygramdb::storage::dump_format::FormatVersion::V1);
    WriteBinary(ofs, version);

    HeaderV1 header;
    header.header_size = 32;
    header.flags = 0;
    header.dump_timestamp = 1;
    header.total_file_size = 0;
    header.file_crc32 = 0;
    header.gtid = "GTID:truncated-config";
    ASSERT_TRUE(WriteHeaderV1(ofs, header).has_value());

    uint32_t config_len = 4;
    WriteBinary(ofs, config_len);
    ofs.write("xx", 2);
  }

  std::string gtid = "unchanged";
  mygramdb::config::Config config;
  std::unordered_map<std::string, std::pair<mygramdb::index::Index*, mygramdb::storage::DocumentStore*>> contexts;
  auto result = ReadDumpV1(filepath, gtid, config, contexts, nullptr, nullptr, nullptr);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kStorageDumpReadError);
  EXPECT_EQ(gtid, "unchanged");

  CleanupTestFile(filepath);
}
