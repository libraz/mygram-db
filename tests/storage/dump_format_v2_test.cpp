/**
 * @file dump_format_v2_test.cpp
 * @brief Unit tests for dump format V2 (section envelope format)
 */

#include "storage/dump_format_v2.h"

#include <gtest/gtest.h>
#include <zlib.h>

#include <cstring>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <utility>
#include <vector>

#include "storage/dump_format_v1.h"
#include "utils/binary_io.h"

using namespace mygramdb::storage;
using namespace mygramdb::storage::dump_v2;
using mygram::utils::ReadBinary;
using mygram::utils::WriteBinary;
using mygramdb::config::Config;
using mygramdb::config::TableConfig;
using mygramdb::index::Index;

namespace {

/// Helper: create a minimal Config for testing
Config MakeTestConfig() {
  Config cfg;
  cfg.mysql.host = "127.0.0.1";
  cfg.mysql.port = 3306;
  cfg.mysql.database = "testdb";
  cfg.mysql.use_gtid = true;
  cfg.mysql.binlog_format = "ROW";
  cfg.mysql.binlog_row_image = "FULL";
  cfg.mysql.connect_timeout_ms = 5000;
  cfg.mysql.read_timeout_ms = 30000;
  cfg.mysql.write_timeout_ms = 30000;

  TableConfig table;
  table.name = "articles";
  table.primary_key = "id";
  table.text_source.column = "body";
  table.ngram_size = 2;
  table.kanji_ngram_size = 1;
  table.cross_boundary_ngrams = true;
  table.posting.block_size = 128;
  table.posting.freq_bits = 0;
  table.posting.use_roaring = "auto";
  cfg.tables.push_back(table);

  cfg.build.mode = "full";
  cfg.build.batch_size = 1000;
  cfg.build.parallelism = 4;
  cfg.build.throttle_ms = 0;

  cfg.replication.enable = true;
  cfg.replication.server_id = 12345;
  cfg.replication.start_from = "";
  cfg.replication.queue_size = 10000;
  cfg.replication.reconnect_backoff_min_ms = 1000;
  cfg.replication.reconnect_backoff_max_ms = 60000;

  cfg.memory.hard_limit_mb = 1024;
  cfg.memory.soft_target_mb = 768;
  cfg.memory.arena_chunk_mb = 64;
  cfg.memory.roaring_threshold = 256;
  cfg.memory.minute_epoch = false;
  cfg.memory.normalize.nfkc = true;
  cfg.memory.normalize.width = "full";
  cfg.memory.normalize.lower = true;

  cfg.dump.dir = "/tmp/mygramdb_test_dumps";
  cfg.dump.interval_sec = 3600;
  cfg.dump.retain = 3;

  cfg.api.tcp.bind = "0.0.0.0";
  cfg.api.tcp.port = 11211;
  cfg.api.http.enable = true;
  cfg.api.http.bind = "0.0.0.0";
  cfg.api.http.port = 8080;
  cfg.api.default_limit = 100;
  cfg.api.max_query_length = 4096;

  cfg.logging.level = "info";
  cfg.logging.format = "json";

  return cfg;
}

/// Test directory under temp (avoids /tmp symlink issue on macOS)
const std::filesystem::path kTestDir = std::filesystem::temp_directory_path() / "mygramdb_v2_test";

/// Helper: create a temporary file path (ensures directory exists)
std::string TempFilePath(const std::string& suffix) {
  std::filesystem::create_directories(kTestDir);
  return (kTestDir / (suffix + ".dmp")).string();
}

/// Helper: cleanup temp file
void CleanupFile(const std::string& path) {
  std::filesystem::remove(path);
}

/// RAII guard that cleans up a temp file on construction and destruction.
/// Ensures cleanup even if the test fails mid-way.
struct ScopedCleanup {
  std::string path;
  explicit ScopedCleanup(const std::string& p) : path(p) { CleanupFile(p); }
  ~ScopedCleanup() { CleanupFile(path); }
  ScopedCleanup(const ScopedCleanup&) = delete;
  ScopedCleanup& operator=(const ScopedCleanup&) = delete;
};

}  // namespace

// ============================================================================
// Header V2 Round-Trip
// ============================================================================

TEST(DumpFormatV2Test, HeaderV2RoundTrip) {
  HeaderV2 header;
  header.header_size = 42;
  header.flags = dump_format::flags_v2::kWithStatistics | dump_format::flags_v2::kWithCRC;
  header.dump_timestamp = 1700000000;
  header.total_file_size = 123456;
  header.file_crc32 = 0xDEADBEEF;
  header.section_count = 3;
  header.gtid = "3E11FA47-71CA-11E1-9E33-C80AA9429562:42";

  std::ostringstream oss;
  auto write_result = WriteHeaderV2(oss, header);
  ASSERT_TRUE(write_result.has_value()) << write_result.error().message();

  std::istringstream iss(oss.str());
  HeaderV2 read_header;
  auto read_result = ReadHeaderV2(iss, read_header);
  ASSERT_TRUE(read_result.has_value()) << read_result.error().message();

  EXPECT_EQ(read_header.header_size, header.header_size);
  EXPECT_EQ(read_header.flags, header.flags);
  EXPECT_EQ(read_header.dump_timestamp, header.dump_timestamp);
  EXPECT_EQ(read_header.total_file_size, header.total_file_size);
  EXPECT_EQ(read_header.file_crc32, header.file_crc32);
  EXPECT_EQ(read_header.section_count, header.section_count);
  EXPECT_EQ(read_header.gtid, header.gtid);
}

TEST(DumpFormatV2Test, HeaderV2EmptyGtid) {
  HeaderV2 header;
  header.gtid = "";
  header.section_count = 1;

  std::ostringstream oss;
  ASSERT_TRUE(WriteHeaderV2(oss, header).has_value());

  std::istringstream iss(oss.str());
  HeaderV2 read_header;
  ASSERT_TRUE(ReadHeaderV2(iss, read_header).has_value());
  EXPECT_EQ(read_header.gtid, "");
  EXPECT_EQ(read_header.section_count, 1u);
}

// ============================================================================
// Section Envelope Round-Trip
// ============================================================================

TEST(DumpFormatV2Test, SectionEnvelopeRoundTrip) {
  std::string data = "Hello, section data!";

  std::ostringstream oss;
  auto write_result = WriteSectionEnvelope(oss, dump_format::SectionType::kConfig, data);
  ASSERT_TRUE(write_result.has_value()) << write_result.error().message();

  std::istringstream iss(oss.str());
  dump_format::SectionEnvelope envelope;
  auto read_result = ReadSectionEnvelope(iss, envelope);
  ASSERT_TRUE(read_result.has_value()) << read_result.error().message();

  EXPECT_EQ(envelope.type, dump_format::SectionType::kConfig);
  EXPECT_EQ(envelope.data_length, data.size());

  // Read actual data and verify CRC
  std::string read_data(envelope.data_length, '\0');
  iss.read(read_data.data(), static_cast<std::streamsize>(envelope.data_length));
  EXPECT_EQ(read_data, data);

  // Verify CRC
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
  uint32_t expected_crc =
      static_cast<uint32_t>(crc32(0, reinterpret_cast<const Bytef*>(data.data()), static_cast<uInt>(data.size())));
  EXPECT_EQ(envelope.crc32, expected_crc);
}

TEST(DumpFormatV2Test, SectionEnvelopeEmptyData) {
  std::string data;

  std::ostringstream oss;
  ASSERT_TRUE(WriteSectionEnvelope(oss, dump_format::SectionType::kStatistics, data).has_value());

  std::istringstream iss(oss.str());
  dump_format::SectionEnvelope envelope;
  ASSERT_TRUE(ReadSectionEnvelope(iss, envelope).has_value());

  EXPECT_EQ(envelope.type, dump_format::SectionType::kStatistics);
  EXPECT_EQ(envelope.data_length, 0u);
}

TEST(DumpFormatV2Test, SectionEnvelopeSize) {
  // Verify envelope is exactly 16 bytes
  std::string data = "test";
  std::ostringstream oss;
  ASSERT_TRUE(WriteSectionEnvelope(oss, dump_format::SectionType::kConfig, data).has_value());

  // Expected size: type(4) + crc32(4) + data_length(8) + data(4) = 20
  EXPECT_EQ(oss.str().size(), dump_format::kSectionEnvelopeSize + data.size());
}

// ============================================================================
// Full V2 Dump Write + Read
// ============================================================================

TEST(DumpFormatV2Test, FullDumpRoundTrip) {
  auto filepath = TempFilePath("full_roundtrip");
  ScopedCleanup cleanup(filepath);

  Config write_config = MakeTestConfig();
  std::string write_gtid = "3E11FA47-71CA-11E1-9E33-C80AA9429562:100";

  // Create empty index and docstore
  Index write_index;
  DocumentStore write_doc_store;

  std::unordered_map<std::string, std::pair<Index*, DocumentStore*>> write_contexts;
  write_contexts["articles"] = {&write_index, &write_doc_store};

  // Write V2 dump
  auto write_result = WriteDumpV2(filepath, write_gtid, write_config, write_contexts);
  ASSERT_TRUE(write_result.has_value()) << write_result.error().message();

  // Verify file exists
  ASSERT_TRUE(std::filesystem::exists(filepath));

  // Read V2 dump
  std::string read_gtid;
  Config read_config;
  Index read_index;
  DocumentStore read_doc_store;

  std::unordered_map<std::string, std::pair<Index*, DocumentStore*>> read_contexts;
  read_contexts["articles"] = {&read_index, &read_doc_store};

  auto read_result = ReadDumpV2(filepath, read_gtid, read_config, read_contexts);
  ASSERT_TRUE(read_result.has_value()) << read_result.error().message();

  // Verify round-trip
  EXPECT_EQ(read_gtid, write_gtid);
  EXPECT_EQ(read_config.mysql.host, write_config.mysql.host);
  EXPECT_EQ(read_config.mysql.port, write_config.mysql.port);
  EXPECT_EQ(read_config.mysql.database, write_config.mysql.database);
  EXPECT_EQ(read_config.tables.size(), 1u);
  EXPECT_EQ(read_config.tables[0].name, "articles");
  EXPECT_EQ(read_config.api.max_query_length, write_config.api.max_query_length);
}

TEST(DumpFormatV2Test, FullDumpWithStatistics) {
  auto filepath = TempFilePath("with_stats");
  ScopedCleanup cleanup(filepath);

  Config cfg = MakeTestConfig();
  std::string gtid = "GTID:42";
  DumpStatistics stats;
  stats.total_documents = 1000;
  stats.total_terms = 5000;
  stats.total_index_bytes = 1024 * 1024;
  stats.total_docstore_bytes = 2 * 1024 * 1024;
  stats.dump_time_ms = 150;

  Index idx;
  DocumentStore ds;
  std::unordered_map<std::string, std::pair<Index*, DocumentStore*>> contexts;
  contexts["articles"] = {&idx, &ds};

  auto write_result = WriteDumpV2(filepath, gtid, cfg, contexts, &stats);
  ASSERT_TRUE(write_result.has_value()) << write_result.error().message();

  // Read back with stats
  std::string read_gtid;
  Config read_config;
  DumpStatistics read_stats;
  Index read_idx;
  DocumentStore read_ds;
  std::unordered_map<std::string, std::pair<Index*, DocumentStore*>> read_contexts;
  read_contexts["articles"] = {&read_idx, &read_ds};

  auto read_result = ReadDumpV2(filepath, read_gtid, read_config, read_contexts, &read_stats);
  ASSERT_TRUE(read_result.has_value()) << read_result.error().message();

  EXPECT_EQ(read_stats.total_documents, 1000u);
  EXPECT_EQ(read_stats.total_terms, 5000u);
  EXPECT_EQ(read_stats.total_index_bytes, 1024u * 1024);
  EXPECT_EQ(read_stats.total_docstore_bytes, 2u * 1024 * 1024);
  EXPECT_EQ(read_stats.dump_time_ms, 150u);
}

TEST(DumpFormatV2Test, DumpWithoutStatistics) {
  auto filepath = TempFilePath("no_stats");
  ScopedCleanup cleanup(filepath);

  Config cfg = MakeTestConfig();
  Index idx;
  DocumentStore ds;
  std::unordered_map<std::string, std::pair<Index*, DocumentStore*>> contexts;
  contexts["articles"] = {&idx, &ds};

  auto write_result = WriteDumpV2(filepath, "GTID:1", cfg, contexts);
  ASSERT_TRUE(write_result.has_value()) << write_result.error().message();

  // Read back — stats should be untouched
  std::string read_gtid;
  Config read_config;
  DumpStatistics read_stats;
  read_stats.total_documents = 999;  // Should remain unchanged
  Index read_idx;
  DocumentStore read_ds;
  std::unordered_map<std::string, std::pair<Index*, DocumentStore*>> read_contexts;
  read_contexts["articles"] = {&read_idx, &read_ds};

  auto read_result = ReadDumpV2(filepath, read_gtid, read_config, read_contexts, &read_stats);
  ASSERT_TRUE(read_result.has_value()) << read_result.error().message();

  // Stats should not have been overwritten (no stats section in dump)
  EXPECT_EQ(read_stats.total_documents, 999u);
}

// ============================================================================
// Version Dispatch (V1 Backward Compatibility)
// ============================================================================

TEST(DumpFormatV2Test, DispatchReadsV1File) {
  auto filepath = TempFilePath("v1_compat");
  auto cleanup = [&]() { CleanupFile(filepath); };
  cleanup();

  // Write a V1 dump
  Config cfg = MakeTestConfig();
  std::string gtid = "V1_GTID:42";
  Index idx;
  DocumentStore ds;
  std::unordered_map<std::string, std::pair<Index*, DocumentStore*>> contexts;
  contexts["articles"] = {&idx, &ds};

  auto write_result = dump_v1::WriteDumpV1(filepath, gtid, cfg, contexts);
  ASSERT_TRUE(write_result.has_value()) << write_result.error().message();

  // Read through dispatch (should detect V1 and use V1 reader)
  std::string read_gtid;
  Config read_config;
  Index read_idx;
  DocumentStore read_ds;
  std::unordered_map<std::string, std::pair<Index*, DocumentStore*>> read_contexts;
  read_contexts["articles"] = {&read_idx, &read_ds};

  auto read_result = ReadDump(filepath, read_gtid, read_config, read_contexts);
  ASSERT_TRUE(read_result.has_value()) << read_result.error().message();

  EXPECT_EQ(read_gtid, gtid);
  EXPECT_EQ(read_config.mysql.host, cfg.mysql.host);

  cleanup();
}

TEST(DumpFormatV2Test, DispatchReadsV2File) {
  auto filepath = TempFilePath("v2_dispatch");
  auto cleanup = [&]() { CleanupFile(filepath); };
  cleanup();

  Config cfg = MakeTestConfig();
  std::string gtid = "V2_GTID:100";
  Index idx;
  DocumentStore ds;
  std::unordered_map<std::string, std::pair<Index*, DocumentStore*>> contexts;
  contexts["articles"] = {&idx, &ds};

  auto write_result = WriteDump(filepath, gtid, cfg, contexts);
  ASSERT_TRUE(write_result.has_value()) << write_result.error().message();

  std::string read_gtid;
  Config read_config;
  Index read_idx;
  DocumentStore read_ds;
  std::unordered_map<std::string, std::pair<Index*, DocumentStore*>> read_contexts;
  read_contexts["articles"] = {&read_idx, &read_ds};

  auto read_result = ReadDump(filepath, read_gtid, read_config, read_contexts);
  ASSERT_TRUE(read_result.has_value()) << read_result.error().message();

  EXPECT_EQ(read_gtid, gtid);

  cleanup();
}

TEST(DumpFormatV2Test, WriteDumpReportsTableProgressAsTablesBegin) {
  auto filepath = TempFilePath("progress_callback");
  ScopedCleanup cleanup(filepath);

  Config cfg = MakeTestConfig();
  Index articles_idx;
  DocumentStore articles_ds;
  Index comments_idx;
  DocumentStore comments_ds;
  std::unordered_map<std::string, std::pair<Index*, DocumentStore*>> contexts;
  contexts["articles"] = {&articles_idx, &articles_ds};
  contexts["comments"] = {&comments_idx, &comments_ds};

  std::vector<std::pair<std::string, size_t>> progress_calls;
  auto result = WriteDump(filepath, "GTID:progress", cfg, contexts, nullptr, nullptr,
                          [&](const std::string& table_name, size_t tables_processed) {
                            progress_calls.emplace_back(table_name, tables_processed);
                          });

  ASSERT_TRUE(result.has_value()) << result.error().message();
  ASSERT_EQ(progress_calls.size(), contexts.size());

  std::unordered_map<std::string, size_t> processed_by_table;
  for (const auto& [table_name, tables_processed] : progress_calls) {
    processed_by_table[table_name] = tables_processed;
  }

  ASSERT_NE(processed_by_table.find("articles"), processed_by_table.end());
  ASSERT_NE(processed_by_table.find("comments"), processed_by_table.end());
  EXPECT_NE(processed_by_table["articles"], processed_by_table["comments"]);
  EXPECT_LT(processed_by_table["articles"], contexts.size());
  EXPECT_LT(processed_by_table["comments"], contexts.size());
}

// ============================================================================
// Integrity Verification
// ============================================================================

TEST(DumpFormatV2Test, IntegrityVerificationPasses) {
  auto filepath = TempFilePath("integrity_ok");
  ScopedCleanup cleanup(filepath);

  Config cfg = MakeTestConfig();
  Index idx;
  DocumentStore ds;
  std::unordered_map<std::string, std::pair<Index*, DocumentStore*>> contexts;
  contexts["articles"] = {&idx, &ds};

  ASSERT_TRUE(WriteDump(filepath, "GTID:1", cfg, contexts).has_value());

  dump_format::IntegrityError error;
  auto result = VerifyDumpIntegrity(filepath, error);
  ASSERT_TRUE(result.has_value()) << error.message;
  EXPECT_EQ(error.type, dump_format::CRCErrorType::None);
}

TEST(DumpFormatV2Test, IntegrityVerificationV1) {
  auto filepath = TempFilePath("integrity_v1");
  auto cleanup = [&]() { CleanupFile(filepath); };
  cleanup();

  Config cfg = MakeTestConfig();
  Index idx;
  DocumentStore ds;
  std::unordered_map<std::string, std::pair<Index*, DocumentStore*>> contexts;
  contexts["articles"] = {&idx, &ds};

  ASSERT_TRUE(dump_v1::WriteDumpV1(filepath, "GTID:1", cfg, contexts).has_value());

  dump_format::IntegrityError error;
  auto result = VerifyDumpIntegrity(filepath, error);
  ASSERT_TRUE(result.has_value()) << error.message;
  EXPECT_EQ(error.type, dump_format::CRCErrorType::None);
}

TEST(DumpFormatV2Test, FileCRCCorruptionDetected) {
  auto filepath = TempFilePath("crc_corrupt");
  auto cleanup = [&]() { CleanupFile(filepath); };
  cleanup();

  Config cfg = MakeTestConfig();
  Index idx;
  DocumentStore ds;
  std::unordered_map<std::string, std::pair<Index*, DocumentStore*>> contexts;
  contexts["articles"] = {&idx, &ds};

  ASSERT_TRUE(WriteDump(filepath, "GTID:1", cfg, contexts).has_value());

  // Corrupt a byte in the middle of the file
  {
    std::fstream fs(filepath, std::ios::in | std::ios::out | std::ios::binary);
    ASSERT_TRUE(fs.good());
    fs.seekg(0, std::ios::end);
    auto size = fs.tellg();
    // Corrupt byte at file midpoint (avoiding header CRC field)
    auto corrupt_pos = size / 2;
    fs.seekp(corrupt_pos);
    char byte = 0;
    fs.read(&byte, 1);
    byte ^= 0xFF;  // Flip all bits
    fs.seekp(corrupt_pos);
    fs.write(&byte, 1);
  }

  dump_format::IntegrityError error;
  auto result = VerifyDumpIntegrity(filepath, error);
  EXPECT_FALSE(result.has_value());
  EXPECT_EQ(error.type, dump_format::CRCErrorType::FileCRC);

  cleanup();
}

TEST(DumpFormatV2Test, SectionCRCCorruptionDetected) {
  auto filepath = TempFilePath("section_crc_corrupt");
  auto cleanup = [&]() { CleanupFile(filepath); };
  cleanup();

  Config cfg = MakeTestConfig();
  Index idx;
  DocumentStore ds;
  std::unordered_map<std::string, std::pair<Index*, DocumentStore*>> contexts;
  contexts["articles"] = {&idx, &ds};

  ASSERT_TRUE(WriteDumpV2(filepath, "GTID:1", cfg, contexts).has_value());

  // Read the file, find the first section envelope, corrupt its CRC,
  // then recalculate file-level CRC to bypass file-level check
  {
    std::fstream fs(filepath, std::ios::in | std::ios::out | std::ios::binary);
    ASSERT_TRUE(fs.good());

    // Skip to after header to find first section envelope
    // magic(4) + version(4) = 8, then read V2 header
    fs.seekg(8);
    HeaderV2 header;
    ASSERT_TRUE(ReadHeaderV2(fs, header).has_value());

    // Now at start of first section envelope
    auto section_start = fs.tellg();
    // Section envelope: type(4) + crc32(4) + data_length(8)
    // Corrupt the CRC field (offset +4 from section start)
    auto crc_pos = static_cast<std::streamoff>(section_start) + 4;
    fs.seekg(crc_pos);
    uint32_t original_crc = 0;
    ReadBinary(fs, original_crc);
    uint32_t bad_crc = original_crc ^ 0xFFFFFFFF;
    fs.seekp(crc_pos);
    WriteBinary(fs, bad_crc);
    fs.close();

    // Now recalculate file-level CRC so file-level check passes
    std::ifstream ifs(filepath, std::ios::binary | std::ios::ate);
    auto file_size = static_cast<uint64_t>(ifs.tellg());
    ifs.seekg(0);

    // Read entire file, zero out file CRC field, compute CRC
    std::vector<char> file_data(file_size);
    ifs.read(file_data.data(), static_cast<std::streamsize>(file_size));
    ifs.close();

    // Zero out file CRC field at kV2HeaderFileCRC32Offset
    auto file_crc_offset = static_cast<size_t>(kV2HeaderFileCRC32Offset);
    std::memset(&file_data[file_crc_offset], 0, 4);

    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    uint32_t new_file_crc =
        static_cast<uint32_t>(crc32(0, reinterpret_cast<const Bytef*>(file_data.data()), static_cast<uInt>(file_size)));

    // Write new file CRC
    std::fstream fs2(filepath, std::ios::in | std::ios::out | std::ios::binary);
    fs2.seekp(kV2HeaderFileCRC32Offset);
    WriteBinary(fs2, new_file_crc);
    fs2.close();
  }

  // Now try to read — should fail at section CRC level
  std::string read_gtid;
  Config read_config;
  Index read_idx;
  DocumentStore read_ds;
  std::unordered_map<std::string, std::pair<Index*, DocumentStore*>> read_contexts;
  read_contexts["articles"] = {&read_idx, &read_ds};
  dump_format::IntegrityError error;

  auto read_result = ReadDumpV2(filepath, read_gtid, read_config, read_contexts, nullptr, nullptr, &error);
  EXPECT_FALSE(read_result.has_value());
  EXPECT_EQ(error.type, dump_format::CRCErrorType::SectionCRC);

  cleanup();
}

// ============================================================================
// DumpInfo
// ============================================================================

TEST(DumpFormatV2Test, GetDumpInfoV2) {
  auto filepath = TempFilePath("info_v2");
  auto cleanup = [&]() { CleanupFile(filepath); };
  cleanup();

  Config cfg = MakeTestConfig();
  DumpStatistics stats;
  stats.total_documents = 500;
  Index idx;
  DocumentStore ds;
  std::unordered_map<std::string, std::pair<Index*, DocumentStore*>> contexts;
  contexts["articles"] = {&idx, &ds};

  ASSERT_TRUE(WriteDumpV2(filepath, "GTID:50", cfg, contexts, &stats).has_value());

  DumpV2Info info;
  auto result = GetDumpInfo(filepath, info);
  ASSERT_TRUE(result.has_value()) << result.error().message();

  EXPECT_EQ(info.version, 2u);
  EXPECT_EQ(info.gtid, "GTID:50");
  EXPECT_EQ(info.table_count, 1u);
  EXPECT_TRUE(info.has_statistics);
  EXPECT_GT(info.file_size, 0u);
  EXPECT_GT(info.timestamp, 0u);
  // Config + Statistics + 1 TableData = 3 sections
  EXPECT_EQ(info.section_count, 3u);
  EXPECT_EQ(info.section_types.size(), 3u);
  EXPECT_EQ(info.section_types[0], dump_format::SectionType::kConfig);
  EXPECT_EQ(info.section_types[1], dump_format::SectionType::kStatistics);
  EXPECT_EQ(info.section_types[2], dump_format::SectionType::kTableData);

  cleanup();
}

TEST(DumpFormatV2Test, GetDumpInfoV1) {
  auto filepath = TempFilePath("info_v1");
  auto cleanup = [&]() { CleanupFile(filepath); };
  cleanup();

  Config cfg = MakeTestConfig();
  Index idx;
  DocumentStore ds;
  std::unordered_map<std::string, std::pair<Index*, DocumentStore*>> contexts;
  contexts["articles"] = {&idx, &ds};

  ASSERT_TRUE(dump_v1::WriteDumpV1(filepath, "V1_GTID", cfg, contexts).has_value());

  DumpV2Info info;
  auto result = GetDumpInfo(filepath, info);
  ASSERT_TRUE(result.has_value()) << result.error().message();

  EXPECT_EQ(info.version, 1u);
  EXPECT_EQ(info.gtid, "V1_GTID");
  EXPECT_EQ(info.table_count, 1u);
  EXPECT_EQ(info.section_count, 0u);  // V1 has no sections
  EXPECT_TRUE(info.section_types.empty());

  cleanup();
}

// ============================================================================
// Unknown Section Skipping (Forward Compatibility)
// ============================================================================

TEST(DumpFormatV2Test, UnknownSectionSkipped) {
  auto filepath = TempFilePath("unknown_section");
  auto cleanup = [&]() { CleanupFile(filepath); };
  cleanup();

  // Write a V2 dump normally
  Config cfg = MakeTestConfig();
  Index idx;
  DocumentStore ds;
  std::unordered_map<std::string, std::pair<Index*, DocumentStore*>> contexts;
  contexts["articles"] = {&idx, &ds};

  ASSERT_TRUE(WriteDumpV2(filepath, "GTID:1", cfg, contexts).has_value());

  // Read the file, insert a fake section between Config and TableData,
  // update section_count, recalculate file CRC
  std::vector<char> file_data;
  {
    std::ifstream ifs(filepath, std::ios::binary | std::ios::ate);
    auto size = static_cast<size_t>(ifs.tellg());
    file_data.resize(size);
    ifs.seekg(0);
    ifs.read(file_data.data(), static_cast<std::streamsize>(size));
  }

  // Find the position after the first section (Config)
  // Skip: magic(4) + version(4) + header
  size_t pos = 8;  // After fixed header

  // Read header_size to skip header
  uint32_t header_size = 0;
  std::memcpy(&header_size, &file_data[pos], 4);

  // The V2 header starts at pos=8, and its total byte size includes header_size field (4)
  // but header_size value = 4+4+8+8+4+4+4+gtid.size()
  // We need to skip the entire header. Let's parse it:
  // header_size(4) + flags(4) + timestamp(8) + file_size(8) + crc(4) + section_count(4) + gtid_len(4) + gtid(N)
  size_t header_start = pos;
  pos += 4;  // header_size
  pos += 4;  // flags
  pos += 8;  // timestamp
  pos += 8;  // file_size
  pos += 4;  // crc
  pos += 4;  // section_count
  uint32_t gtid_len = 0;
  std::memcpy(&gtid_len, &file_data[pos], 4);
  pos += 4 + gtid_len;

  // Now at start of first section. Read it to find where it ends.
  size_t first_section_start = pos;
  // type(4) + crc(4) + data_length(8)
  uint64_t first_data_length = 0;
  std::memcpy(&first_data_length, &file_data[pos + 8], 8);
  size_t first_section_end = pos + 16 + static_cast<size_t>(first_data_length);

  // Create a fake unknown section (type=99)
  std::string fake_payload = "fake unknown section data";
  std::ostringstream fake_section_stream;
  uint32_t fake_type = 99;
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
  uint32_t fake_crc = static_cast<uint32_t>(
      crc32(0, reinterpret_cast<const Bytef*>(fake_payload.data()), static_cast<uInt>(fake_payload.size())));
  auto fake_len = static_cast<uint64_t>(fake_payload.size());
  WriteBinary(fake_section_stream, fake_type);
  WriteBinary(fake_section_stream, fake_crc);
  WriteBinary(fake_section_stream, fake_len);
  fake_section_stream.write(fake_payload.data(), static_cast<std::streamsize>(fake_payload.size()));
  std::string fake_section = fake_section_stream.str();

  // Insert fake section after first section
  std::vector<char> new_file_data;
  new_file_data.insert(new_file_data.end(), file_data.begin(),
                       file_data.begin() + static_cast<long>(first_section_end));
  new_file_data.insert(new_file_data.end(), fake_section.begin(), fake_section.end());
  new_file_data.insert(new_file_data.end(), file_data.begin() + static_cast<long>(first_section_end), file_data.end());

  // Update section_count (+1)
  size_t section_count_offset = static_cast<size_t>(kV2HeaderSectionCountOffset);
  uint32_t old_section_count = 0;
  std::memcpy(&old_section_count, &new_file_data[section_count_offset], 4);
  uint32_t new_section_count = old_section_count + 1;
  std::memcpy(&new_file_data[section_count_offset], &new_section_count, 4);

  // Update total_file_size
  auto new_total_size = static_cast<uint64_t>(new_file_data.size());
  size_t file_size_offset = static_cast<size_t>(kV2HeaderTotalFileSizeOffset);
  std::memcpy(&new_file_data[file_size_offset], &new_total_size, 8);

  // Recalculate file CRC (zero out CRC field, compute, write back)
  size_t file_crc_offset = static_cast<size_t>(kV2HeaderFileCRC32Offset);
  std::memset(&new_file_data[file_crc_offset], 0, 4);
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
  uint32_t new_crc = static_cast<uint32_t>(
      crc32(0, reinterpret_cast<const Bytef*>(new_file_data.data()), static_cast<uInt>(new_file_data.size())));
  std::memcpy(&new_file_data[file_crc_offset], &new_crc, 4);

  // Write modified file
  {
    std::ofstream ofs(filepath, std::ios::binary | std::ios::trunc);
    ofs.write(new_file_data.data(), static_cast<std::streamsize>(new_file_data.size()));
  }

  // Read through dispatch — should succeed, skipping the unknown section
  std::string read_gtid;
  Config read_config;
  Index read_idx;
  DocumentStore read_ds;
  std::unordered_map<std::string, std::pair<Index*, DocumentStore*>> read_contexts;
  read_contexts["articles"] = {&read_idx, &read_ds};

  auto read_result = ReadDump(filepath, read_gtid, read_config, read_contexts);
  ASSERT_TRUE(read_result.has_value()) << read_result.error().message();
  EXPECT_EQ(read_gtid, "GTID:1");

  // DumpInfo should show the unknown section
  DumpV2Info info;
  ASSERT_TRUE(GetDumpInfo(filepath, info).has_value());
  EXPECT_EQ(info.section_count, new_section_count);
  // Should contain Config, unknown(99), TableData
  ASSERT_EQ(info.section_types.size(), 3u);
  EXPECT_EQ(static_cast<uint32_t>(info.section_types[1]), 99u);

  cleanup();
}

// ============================================================================
// Invalid Magic Number
// ============================================================================

TEST(DumpFormatV2Test, InvalidMagicNumber) {
  auto filepath = TempFilePath("bad_magic");
  auto cleanup = [&]() { CleanupFile(filepath); };
  cleanup();

  // Write garbage
  {
    std::ofstream ofs(filepath, std::ios::binary);
    ofs.write("XXXX", 4);
    uint32_t version = 2;
    WriteBinary(ofs, version);
  }

  std::string gtid;
  Config cfg;
  std::unordered_map<std::string, std::pair<Index*, DocumentStore*>> contexts;
  auto result = ReadDump(filepath, gtid, cfg, contexts);
  EXPECT_FALSE(result.has_value());

  cleanup();
}

TEST(DumpFormatV2Test, UnsupportedVersion) {
  auto filepath = TempFilePath("bad_version");
  auto cleanup = [&]() { CleanupFile(filepath); };
  cleanup();

  {
    std::ofstream ofs(filepath, std::ios::binary);
    ofs.write(dump_format::kMagicNumber.data(), 4);
    uint32_t version = 99;
    WriteBinary(ofs, version);
  }

  std::string gtid;
  Config cfg;
  std::unordered_map<std::string, std::pair<Index*, DocumentStore*>> contexts;
  auto result = ReadDump(filepath, gtid, cfg, contexts);
  EXPECT_FALSE(result.has_value());

  cleanup();
}
