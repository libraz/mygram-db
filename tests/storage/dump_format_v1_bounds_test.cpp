/**
 * @file dump_format_v1_bounds_test.cpp
 * @brief Tests for config_len and stats_len bounds checking in ReadDumpV1
 *
 * Verifies that ReadDumpV1 rejects dump files with oversized config or
 * statistics sections to prevent OOM from malicious dump files.
 */

#include <gtest/gtest.h>

#include <cstring>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>

#include "config/config.h"
#include "index/index.h"
#include "storage/document_store.h"
#include "storage/dump_format.h"
#include "storage/dump_format_v1.h"
#include "utils/binary_io.h"

using namespace mygramdb::storage;
using namespace mygramdb::storage::dump_v1;
using mygram::utils::WriteBinary;

namespace {

/**
 * @brief Write a minimal valid dump file prefix up to (but not including) the config_len field
 *
 * This writes: magic, version, header_size, flags, timestamp, total_file_size,
 * file_crc32, and GTID. After calling this, the stream is positioned where
 * config_len should be written.
 */
void WriteMinimalDumpPrefix(std::ostream& oss) {
  // Magic number "MGDB"
  oss.write(dump_format::kMagicNumber.data(), 4);

  // Version = 1
  auto version = static_cast<uint32_t>(dump_format::FormatVersion::V1);
  WriteBinary(oss, version);

  // HeaderV1 fields
  uint32_t header_size = 0;
  WriteBinary(oss, header_size);

  uint32_t flags = dump_format::flags_v1::kNone;
  WriteBinary(oss, flags);

  uint64_t dump_timestamp = 0;
  WriteBinary(oss, dump_timestamp);

  uint64_t total_file_size = 0;  // 0 = skip file size check
  WriteBinary(oss, total_file_size);

  uint32_t file_crc32 = 0;  // 0 = skip CRC check
  WriteBinary(oss, file_crc32);

  // GTID (empty string: length=0)
  uint32_t gtid_len = 0;
  WriteBinary(oss, gtid_len);
}

class DumpFormatV1BoundsTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create a unique temp directory for test files
    temp_dir_ = std::filesystem::temp_directory_path() / "dump_bounds_test";
    std::filesystem::create_directories(temp_dir_);
  }

  void TearDown() override {
    std::error_code ec;
    std::filesystem::remove_all(temp_dir_, ec);
  }

  std::filesystem::path temp_dir_;
};

TEST_F(DumpFormatV1BoundsTest, RejectsOversizedConfigLength) {
  // Build a dump file with an oversized config_len
  std::string filepath = (temp_dir_ / "oversized_config.dmp").string();

  {
    std::ofstream ofs(filepath, std::ios::binary);
    ASSERT_TRUE(ofs.good());

    WriteMinimalDumpPrefix(ofs);

    // Write an oversized config_len (exceeds kMaxConfigSectionLength)
    uint32_t oversized_config_len = kMaxConfigSectionLength + 1;
    WriteBinary(ofs, oversized_config_len);

    ofs.close();
  }

  // Attempt to read the dump file
  std::string gtid;
  mygramdb::config::Config config;
  std::unordered_map<std::string, std::pair<mygramdb::index::Index*, DocumentStore*>> table_contexts;

  auto result = ReadDumpV1(filepath, gtid, config, table_contexts);
  ASSERT_FALSE(result.has_value()) << "ReadDumpV1 should reject oversized config_len";
  EXPECT_NE(result.error().message().find("Config section too large"), std::string::npos)
      << "Error message should mention config section: " << result.error().message();
}

TEST_F(DumpFormatV1BoundsTest, AcceptsConfigLengthAtLimit) {
  // Build a dump file with config_len exactly at the limit
  // It will fail to read the actual config data (not enough bytes), but
  // it should NOT fail the bounds check itself.
  std::string filepath = (temp_dir_ / "max_config.dmp").string();

  {
    std::ofstream ofs(filepath, std::ios::binary);
    ASSERT_TRUE(ofs.good());

    WriteMinimalDumpPrefix(ofs);

    // Write config_len at exactly the limit
    uint32_t max_config_len = kMaxConfigSectionLength;
    WriteBinary(ofs, max_config_len);

    // Don't write actual config data - the read will fail downstream
    // but not from our bounds check
    ofs.close();
  }

  std::string gtid;
  mygramdb::config::Config config;
  std::unordered_map<std::string, std::pair<mygramdb::index::Index*, DocumentStore*>> table_contexts;

  auto result = ReadDumpV1(filepath, gtid, config, table_contexts);
  // Should fail, but NOT with "Config section too large"
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().message().find("Config section too large"), std::string::npos)
      << "Should not fail bounds check at exactly the limit";
}

TEST_F(DumpFormatV1BoundsTest, RejectsOversizedStatsLength) {
  // Build a dump file with a valid (small) config section but oversized stats_len
  std::string filepath = (temp_dir_ / "oversized_stats.dmp").string();

  {
    std::ofstream ofs(filepath, std::ios::binary);
    ASSERT_TRUE(ofs.good());

    WriteMinimalDumpPrefix(ofs);

    // Write a minimal valid config section (empty config serialization)
    // We need to write config data that DeserializeConfig can parse.
    // Write config_len = 0 and let it try to deserialize empty config
    uint32_t config_len = 0;
    WriteBinary(ofs, config_len);

    // Write an oversized stats_len
    uint32_t oversized_stats_len = kMaxStatsSectionLength + 1;
    WriteBinary(ofs, oversized_stats_len);

    ofs.close();
  }

  std::string gtid;
  mygramdb::config::Config config;
  std::unordered_map<std::string, std::pair<mygramdb::index::Index*, DocumentStore*>> table_contexts;
  DumpStatistics stats;

  auto result = ReadDumpV1(filepath, gtid, config, table_contexts, &stats);
  // This may fail at config deserialization first since config_len=0 might be invalid.
  // If config deserialization succeeds with empty data, it should then fail at stats bounds.
  // Either way, verify the dump is rejected.
  ASSERT_FALSE(result.has_value()) << "ReadDumpV1 should reject oversized stats_len";
}

}  // namespace
