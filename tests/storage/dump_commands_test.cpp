/**
 * @file dump_commands_test.cpp
 * @brief Test DUMP SAVE/LOAD/VERIFY/INFO commands with Version 1 format
 */

#include <gtest/gtest.h>
#include <spdlog/spdlog.h>

#include <cstdio>
#include <filesystem>
#include <fstream>

#include "config/config.h"
#include "index/index.h"
#include "storage/document_store.h"
#include "storage/dump_format_v1.h"

using namespace mygramdb;

class DumpCommandsTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Set logging level
    spdlog::set_level(spdlog::level::info);

    // Create test data
    config_.mysql.host = "127.0.0.1";
    config_.mysql.port = 3306;
    config_.mysql.database = "test";

    config::TableConfig table1;
    table1.name = "table1";
    table1.primary_key = "id";
    table1.text_source.column = "text";
    table1.ngram_size = 2;
    config_.tables.push_back(table1);

    config::TableConfig table2;
    table2.name = "table2";
    table2.primary_key = "id";
    table2.text_source.column = "content";
    table2.ngram_size = 3;
    config_.tables.push_back(table2);

    // Create indexes and document stores
    index1_ = std::make_unique<index::Index>(2);
    doc_store1_ = std::make_unique<storage::DocumentStore>();

    index2_ = std::make_unique<index::Index>(3);
    doc_store2_ = std::make_unique<storage::DocumentStore>();

    // Add test data to table1
    storage::DocumentStore::DocumentItem doc1;
    doc1.primary_key = "1";
    doc1.filters["status"] = 1;
    doc_store1_->AddDocument(doc1.primary_key, doc1.filters);
    index1_->AddDocument(1, "hello world");

    storage::DocumentStore::DocumentItem doc2;
    doc2.primary_key = "2";
    doc2.filters["status"] = 2;
    doc_store1_->AddDocument(doc2.primary_key, doc2.filters);
    index1_->AddDocument(2, "test data");

    // Add test data to table2
    storage::DocumentStore::DocumentItem doc3;
    doc3.primary_key = "100";
    doc3.filters["category"] = std::string("news");
    doc_store2_->AddDocument(doc3.primary_key, doc3.filters);
    index2_->AddDocument(1, "breaking news");

    test_gtid_ = "00000000-0000-0000-0000-000000000000:1-100";
    test_filepath_ = "/tmp/mygramdb_dump_test.dmp";
  }

  void TearDown() override {
    // Clean up test file
    std::remove(test_filepath_.c_str());
  }

  config::Config config_;
  std::unique_ptr<index::Index> index1_;
  std::unique_ptr<storage::DocumentStore> doc_store1_;
  std::unique_ptr<index::Index> index2_;
  std::unique_ptr<storage::DocumentStore> doc_store2_;
  std::string test_gtid_;
  std::string test_filepath_;
};

// Test basic DUMP SAVE and LOAD
TEST_F(DumpCommandsTest, BasicSaveAndLoad) {
  // Prepare table contexts
  std::unordered_map<std::string, std::pair<index::Index*, storage::DocumentStore*>> contexts;
  contexts["table1"] = std::make_pair(index1_.get(), doc_store1_.get());
  contexts["table2"] = std::make_pair(index2_.get(), doc_store2_.get());

  // Save snapshot
  bool save_success = storage::dump_v1::WriteDumpV1(test_filepath_, test_gtid_, config_, contexts, nullptr, nullptr);
  ASSERT_TRUE(save_success) << "Failed to save snapshot";

  // Verify file exists
  std::ifstream file_check(test_filepath_, std::ios::binary);
  ASSERT_TRUE(file_check.good()) << "Snapshot file was not created";
  file_check.close();

  // Create new empty structures for loading
  auto loaded_index1 = std::make_unique<index::Index>(2);
  auto loaded_doc_store1 = std::make_unique<storage::DocumentStore>();
  auto loaded_index2 = std::make_unique<index::Index>(3);
  auto loaded_doc_store2 = std::make_unique<storage::DocumentStore>();

  std::unordered_map<std::string, std::pair<index::Index*, storage::DocumentStore*>> load_contexts;
  load_contexts["table1"] = std::make_pair(loaded_index1.get(), loaded_doc_store1.get());
  load_contexts["table2"] = std::make_pair(loaded_index2.get(), loaded_doc_store2.get());

  // Load snapshot
  std::string loaded_gtid;
  config::Config loaded_config;
  bool load_success = storage::dump_v1::ReadDumpV1(test_filepath_, loaded_gtid, loaded_config, load_contexts, nullptr,
                                                   nullptr, nullptr);
  ASSERT_TRUE(load_success) << "Failed to load snapshot";

  // Verify GTID
  EXPECT_EQ(test_gtid_, loaded_gtid) << "GTID mismatch";

  // Verify table1 data
  auto doc1_id = loaded_doc_store1->GetDocId("1");
  ASSERT_TRUE(doc1_id.has_value()) << "Document 1 not found";
  EXPECT_EQ(1u, doc1_id.value());

  auto doc2_id = loaded_doc_store1->GetDocId("2");
  ASSERT_TRUE(doc2_id.has_value()) << "Document 2 not found";
  EXPECT_EQ(2u, doc2_id.value());

  // Verify table2 data
  auto doc3_id = loaded_doc_store2->GetDocId("100");
  ASSERT_TRUE(doc3_id.has_value()) << "Document 100 not found";
  EXPECT_EQ(1u, doc3_id.value());

  // Verify document count
  EXPECT_EQ(2u, loaded_doc_store1->Size()) << "Table1 document count mismatch";
  EXPECT_EQ(1u, loaded_doc_store2->Size()) << "Table2 document count mismatch";
}

// Test DUMP SAVE with statistics
TEST_F(DumpCommandsTest, SaveWithStatistics) {
  std::unordered_map<std::string, std::pair<index::Index*, storage::DocumentStore*>> contexts;
  contexts["table1"] = std::make_pair(index1_.get(), doc_store1_.get());

  // Prepare statistics
  storage::DumpStatistics stats;
  stats.total_documents = 2;
  stats.total_terms = 10;
  stats.total_index_bytes = 1024;
  stats.total_docstore_bytes = 512;
  stats.dump_time_ms = 100;

  storage::TableStatistics table1_stats;
  table1_stats.document_count = 2;
  table1_stats.term_count = 10;
  table1_stats.index_bytes = 1024;
  table1_stats.docstore_bytes = 512;
  table1_stats.next_doc_id = 3;

  std::unordered_map<std::string, storage::TableStatistics> table_stats;
  table_stats["table1"] = table1_stats;

  // Save with statistics
  bool save_success =
      storage::dump_v1::WriteDumpV1(test_filepath_, test_gtid_, config_, contexts, &stats, &table_stats);
  ASSERT_TRUE(save_success) << "Failed to save snapshot with statistics";

  // Load and verify statistics are preserved
  auto loaded_index1 = std::make_unique<index::Index>(2);
  auto loaded_doc_store1 = std::make_unique<storage::DocumentStore>();

  std::unordered_map<std::string, std::pair<index::Index*, storage::DocumentStore*>> load_contexts;
  load_contexts["table1"] = std::make_pair(loaded_index1.get(), loaded_doc_store1.get());

  storage::DumpStatistics loaded_stats;
  std::unordered_map<std::string, storage::TableStatistics> loaded_table_stats;

  std::string loaded_gtid;
  config::Config loaded_config;
  bool load_success = storage::dump_v1::ReadDumpV1(test_filepath_, loaded_gtid, loaded_config, load_contexts,
                                                   &loaded_stats, &loaded_table_stats, nullptr);
  ASSERT_TRUE(load_success) << "Failed to load snapshot";

  // Verify statistics
  EXPECT_EQ(stats.total_documents, loaded_stats.total_documents);
  EXPECT_EQ(stats.total_terms, loaded_stats.total_terms);
  EXPECT_EQ(stats.total_index_bytes, loaded_stats.total_index_bytes);
  EXPECT_EQ(stats.total_docstore_bytes, loaded_stats.total_docstore_bytes);

  ASSERT_TRUE(loaded_table_stats.count("table1") > 0) << "Table1 statistics not found";
  EXPECT_EQ(table1_stats.document_count, loaded_table_stats["table1"].document_count);
  EXPECT_EQ(table1_stats.term_count, loaded_table_stats["table1"].term_count);
}

// Test DUMP VERIFY
TEST_F(DumpCommandsTest, VerifySnapshot) {
  std::unordered_map<std::string, std::pair<index::Index*, storage::DocumentStore*>> contexts;
  contexts["table1"] = std::make_pair(index1_.get(), doc_store1_.get());

  // Save snapshot
  bool save_success = storage::dump_v1::WriteDumpV1(test_filepath_, test_gtid_, config_, contexts, nullptr, nullptr);
  ASSERT_TRUE(save_success) << "Failed to save snapshot";

  // Verify snapshot
  storage::dump_format::IntegrityError error;
  bool verify_success = storage::dump_v1::VerifyDumpIntegrity(test_filepath_, error);
  EXPECT_TRUE(verify_success) << "Snapshot verification failed: " << error.message;
  EXPECT_EQ(storage::dump_format::CRCErrorType::None, error.type);
}

// Test DUMP VERIFY with corrupted file
TEST_F(DumpCommandsTest, VerifyCorruptedSnapshot) {
  // Create a corrupted file
  std::ofstream corrupt_file(test_filepath_, std::ios::binary);
  corrupt_file << "INVALID_DATA";
  corrupt_file.close();

  // Verify should fail
  storage::dump_format::IntegrityError error;
  bool verify_success = storage::dump_v1::VerifyDumpIntegrity(test_filepath_, error);
  EXPECT_FALSE(verify_success) << "Verification should fail for corrupted file";
  EXPECT_NE(storage::dump_format::CRCErrorType::None, error.type);
}

// Test DUMP INFO
TEST_F(DumpCommandsTest, GetDumpInfo) {
  std::unordered_map<std::string, std::pair<index::Index*, storage::DocumentStore*>> contexts;
  contexts["table1"] = std::make_pair(index1_.get(), doc_store1_.get());
  contexts["table2"] = std::make_pair(index2_.get(), doc_store2_.get());

  // Save snapshot
  bool save_success = storage::dump_v1::WriteDumpV1(test_filepath_, test_gtid_, config_, contexts, nullptr, nullptr);
  ASSERT_TRUE(save_success) << "Failed to save snapshot";

  // Get snapshot info
  storage::dump_v1::DumpInfo info;
  bool info_success = storage::dump_v1::GetDumpInfo(test_filepath_, info);
  ASSERT_TRUE(info_success) << "Failed to get snapshot info";

  // Verify info
  EXPECT_EQ(1u, info.version) << "Version mismatch";
  EXPECT_EQ(test_gtid_, info.gtid) << "GTID mismatch";
  EXPECT_EQ(2u, info.table_count) << "Table count mismatch";
  EXPECT_FALSE(info.has_statistics) << "Should not have statistics";
  EXPECT_GT(info.file_size, 0u) << "File size should be positive";
  EXPECT_GT(info.timestamp, 0u) << "Timestamp should be positive";
}

// Test version compatibility
TEST_F(DumpCommandsTest, VersionCompatibility) {
  // Create a file with unsupported version
  std::ofstream file(test_filepath_, std::ios::binary);

  // Write magic number
  file.write("MGDB", 4);

  // Write future version (999)
  uint32_t future_version = 999;
  file.write(reinterpret_cast<const char*>(&future_version), sizeof(future_version));
  file.close();

  // Verify should fail
  storage::dump_format::IntegrityError error;
  bool verify_success = storage::dump_v1::VerifyDumpIntegrity(test_filepath_, error);
  EXPECT_FALSE(verify_success) << "Should reject future version";

  // Get info should also fail
  storage::dump_v1::DumpInfo info;
  bool info_success = storage::dump_v1::GetDumpInfo(test_filepath_, info);
  EXPECT_FALSE(info_success) << "Should reject future version";
}

// Test CRC field corruption detection
TEST_F(DumpCommandsTest, DetectCRCCorruption) {
  std::unordered_map<std::string, std::pair<index::Index*, storage::DocumentStore*>> contexts;
  contexts["table1"] = std::make_pair(index1_.get(), doc_store1_.get());

  // Save snapshot
  bool save_success = storage::dump_v1::WriteDumpV1(test_filepath_, test_gtid_, config_, contexts, nullptr, nullptr);
  ASSERT_TRUE(save_success) << "Failed to save snapshot";

  // Manually corrupt the CRC field (at offset 32: magic(4) + version(4) + header_size(4) + flags(4) + timestamp(8) +
  // total_file_size(8))
  std::fstream file(test_filepath_, std::ios::in | std::ios::out | std::ios::binary);
  ASSERT_TRUE(file.is_open()) << "Failed to open file for corruption";

  const std::streamoff crc_offset = 4 + 4 + 4 + 4 + 8 + 8;
  file.seekp(crc_offset);
  uint32_t corrupted_crc = 0xDEADBEEF;  // Write invalid CRC
  file.write(reinterpret_cast<const char*>(&corrupted_crc), sizeof(corrupted_crc));
  file.close();

  // Verify should fail with CRC mismatch
  storage::dump_format::IntegrityError error;
  bool verify_success = storage::dump_v1::VerifyDumpIntegrity(test_filepath_, error);
  EXPECT_FALSE(verify_success) << "Should detect CRC corruption";
  EXPECT_EQ(storage::dump_format::CRCErrorType::FileCRC, error.type);
  EXPECT_EQ("CRC32 checksum mismatch", error.message);

  // Load should also fail
  auto loaded_index1 = std::make_unique<index::Index>(2);
  auto loaded_doc_store1 = std::make_unique<storage::DocumentStore>();
  std::unordered_map<std::string, std::pair<index::Index*, storage::DocumentStore*>> load_contexts;
  load_contexts["table1"] = std::make_pair(loaded_index1.get(), loaded_doc_store1.get());

  std::string loaded_gtid;
  config::Config loaded_config;
  storage::dump_format::IntegrityError load_error;
  bool load_success = storage::dump_v1::ReadDumpV1(test_filepath_, loaded_gtid, loaded_config, load_contexts, nullptr,
                                                   nullptr, &load_error);
  EXPECT_FALSE(load_success) << "Load should fail with corrupted CRC";
  EXPECT_EQ(storage::dump_format::CRCErrorType::FileCRC, load_error.type);
}

// Test file truncation detection
TEST_F(DumpCommandsTest, DetectFileTruncation) {
  std::unordered_map<std::string, std::pair<index::Index*, storage::DocumentStore*>> contexts;
  contexts["table1"] = std::make_pair(index1_.get(), doc_store1_.get());

  // Save snapshot
  bool save_success = storage::dump_v1::WriteDumpV1(test_filepath_, test_gtid_, config_, contexts, nullptr, nullptr);
  ASSERT_TRUE(save_success) << "Failed to save snapshot";

  // Get original file size
  std::ifstream check_file(test_filepath_, std::ios::binary | std::ios::ate);
  auto original_size = check_file.tellg();
  check_file.close();
  ASSERT_GT(original_size, 100) << "File too small for truncation test";

  // Truncate the file (remove last 100 bytes)
  std::filesystem::resize_file(test_filepath_, static_cast<std::size_t>(original_size) - 100);

  // Verify should fail with file size mismatch
  storage::dump_format::IntegrityError error;
  bool verify_success = storage::dump_v1::VerifyDumpIntegrity(test_filepath_, error);
  EXPECT_FALSE(verify_success) << "Should detect file truncation";
  EXPECT_EQ(storage::dump_format::CRCErrorType::FileCRC, error.type);
  EXPECT_NE(std::string::npos, error.message.find("File size mismatch")) << "Error message should mention size";

  // Load should also fail
  auto loaded_index1 = std::make_unique<index::Index>(2);
  auto loaded_doc_store1 = std::make_unique<storage::DocumentStore>();
  std::unordered_map<std::string, std::pair<index::Index*, storage::DocumentStore*>> load_contexts;
  load_contexts["table1"] = std::make_pair(loaded_index1.get(), loaded_doc_store1.get());

  std::string loaded_gtid;
  config::Config loaded_config;
  storage::dump_format::IntegrityError load_error;
  bool load_success = storage::dump_v1::ReadDumpV1(test_filepath_, loaded_gtid, loaded_config, load_contexts, nullptr,
                                                   nullptr, &load_error);
  EXPECT_FALSE(load_success) << "Load should fail with truncated file";
}

// Test data corruption detection (corrupt data in the middle of file)
TEST_F(DumpCommandsTest, DetectDataCorruption) {
  std::unordered_map<std::string, std::pair<index::Index*, storage::DocumentStore*>> contexts;
  contexts["table1"] = std::make_pair(index1_.get(), doc_store1_.get());

  // Save snapshot
  bool save_success = storage::dump_v1::WriteDumpV1(test_filepath_, test_gtid_, config_, contexts, nullptr, nullptr);
  ASSERT_TRUE(save_success) << "Failed to save snapshot";

  // Get file size
  std::ifstream check_file(test_filepath_, std::ios::binary | std::ios::ate);
  auto file_size = check_file.tellg();
  check_file.close();
  ASSERT_GT(file_size, 200) << "File too small for corruption test";

  // Corrupt data in the middle of the file (offset 100)
  std::fstream file(test_filepath_, std::ios::in | std::ios::out | std::ios::binary);
  ASSERT_TRUE(file.is_open()) << "Failed to open file for corruption";

  file.seekp(100);
  const char corrupted_data[] = "CORRUPTED_DATA_HERE";
  file.write(corrupted_data, sizeof(corrupted_data));
  file.close();

  // Verify should fail with CRC mismatch
  storage::dump_format::IntegrityError error;
  bool verify_success = storage::dump_v1::VerifyDumpIntegrity(test_filepath_, error);
  EXPECT_FALSE(verify_success) << "Should detect data corruption";
  EXPECT_EQ(storage::dump_format::CRCErrorType::FileCRC, error.type);
  EXPECT_EQ("CRC32 checksum mismatch", error.message);

  // Load should also fail
  auto loaded_index1 = std::make_unique<index::Index>(2);
  auto loaded_doc_store1 = std::make_unique<storage::DocumentStore>();
  std::unordered_map<std::string, std::pair<index::Index*, storage::DocumentStore*>> load_contexts;
  load_contexts["table1"] = std::make_pair(loaded_index1.get(), loaded_doc_store1.get());

  std::string loaded_gtid;
  config::Config loaded_config;
  storage::dump_format::IntegrityError load_error;
  bool load_success = storage::dump_v1::ReadDumpV1(test_filepath_, loaded_gtid, loaded_config, load_contexts, nullptr,
                                                   nullptr, &load_error);
  EXPECT_FALSE(load_success) << "Load should fail with corrupted data";
  EXPECT_EQ(storage::dump_format::CRCErrorType::FileCRC, load_error.type);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  spdlog::set_level(spdlog::level::warn);  // Reduce noise in tests
  return RUN_ALL_TESTS();
}
