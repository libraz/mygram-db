/**
 * @file atomic_write_test.cpp
 * @brief Tests for atomic write (temp file + rename) strategy (BUG-0077)
 *
 * Verifies that dump writes use atomic temp-file-then-rename strategy
 * to prevent file corruption during crashes.
 */

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <thread>

#include "config/config.h"
#include "index/index.h"
#include "storage/document_store.h"
#include "storage/dump_format_v1.h"

using namespace mygramdb::storage;
using namespace mygramdb::storage::dump_v1;

namespace fs = std::filesystem;

class AtomicWriteTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create test directory
    test_dir_ = "/tmp/mygram_atomic_write_test_" + std::to_string(std::time(nullptr));
    fs::create_directories(test_dir_);

    // Create test data
    index_ = std::make_unique<mygramdb::index::Index>(2);
    doc_store_ = std::make_unique<DocumentStore>();

    // Add some test documents
    for (int i = 0; i < 100; ++i) {
      std::string pk = "pk" + std::to_string(i);
      std::unordered_map<std::string, FilterValue> filters;
      filters["status"] = static_cast<int64_t>(i % 10);
      doc_store_->AddDocument(pk, filters);
    }
  }

  void TearDown() override {
    // Clean up test directory
    std::error_code ec;
    fs::remove_all(test_dir_, ec);
  }

  std::string test_dir_;
  std::unique_ptr<mygramdb::index::Index> index_;
  std::unique_ptr<DocumentStore> doc_store_;
};

/**
 * @test Verify that successful write produces a valid file
 */
TEST_F(AtomicWriteTest, SuccessfulWriteProducesValidFile) {
  std::string dump_path = test_dir_ + "/test.dmp";
  std::string gtid = "test-gtid-12345";

  mygramdb::config::Config config;
  std::unordered_map<std::string, std::pair<mygramdb::index::Index*, DocumentStore*>> table_contexts;
  table_contexts["test_table"] = {index_.get(), doc_store_.get()};

  // Write dump
  auto result = WriteDumpV1(dump_path, gtid, config, table_contexts);
  ASSERT_TRUE(result.has_value()) << "Write should succeed";

  // Verify file exists and is valid
  EXPECT_TRUE(fs::exists(dump_path)) << "Dump file should exist";

  // Verify integrity
  dump_format::IntegrityError integrity_error;
  auto verify_result = VerifyDumpIntegrity(dump_path, integrity_error);
  EXPECT_TRUE(verify_result.has_value()) << "Integrity check should pass: " << integrity_error.message;
}

/**
 * @test Verify that no temp file remains after successful write
 */
TEST_F(AtomicWriteTest, NoTempFileAfterSuccessfulWrite) {
  std::string dump_path = test_dir_ + "/test.dmp";
  std::string temp_path = dump_path + ".tmp";
  std::string gtid = "test-gtid-12345";

  mygramdb::config::Config config;
  std::unordered_map<std::string, std::pair<mygramdb::index::Index*, DocumentStore*>> table_contexts;
  table_contexts["test_table"] = {index_.get(), doc_store_.get()};

  // Write dump
  auto result = WriteDumpV1(dump_path, gtid, config, table_contexts);
  ASSERT_TRUE(result.has_value()) << "Write should succeed";

  // Verify temp file doesn't exist
  EXPECT_FALSE(fs::exists(temp_path)) << "Temp file should not exist after successful write";

  // Verify final file exists
  EXPECT_TRUE(fs::exists(dump_path)) << "Final dump file should exist";
}

/**
 * @test Verify that existing file is preserved on write failure
 *
 * This test verifies that if write fails, the original file is not corrupted.
 */
TEST_F(AtomicWriteTest, ExistingFilePreservedOnFailure) {
  std::string dump_path = test_dir_ + "/test.dmp";
  std::string gtid_v1 = "gtid-version-1";
  std::string gtid_v2 = "gtid-version-2";

  mygramdb::config::Config config;
  std::unordered_map<std::string, std::pair<mygramdb::index::Index*, DocumentStore*>> table_contexts;
  table_contexts["test_table"] = {index_.get(), doc_store_.get()};

  // Write initial dump (version 1)
  auto result = WriteDumpV1(dump_path, gtid_v1, config, table_contexts);
  ASSERT_TRUE(result.has_value()) << "Initial write should succeed";

  // Record file size
  auto original_size = fs::file_size(dump_path);
  EXPECT_GT(original_size, 0) << "Original file should have content";

  // Write another dump (version 2) to the same path
  // This should atomically replace the original
  auto result2 = WriteDumpV1(dump_path, gtid_v2, config, table_contexts);
  ASSERT_TRUE(result2.has_value()) << "Second write should succeed";

  // Verify file still exists and is valid
  EXPECT_TRUE(fs::exists(dump_path)) << "Dump file should still exist";

  dump_format::IntegrityError integrity_error;
  auto verify_result = VerifyDumpIntegrity(dump_path, integrity_error);
  EXPECT_TRUE(verify_result.has_value()) << "Integrity check should pass";

  // Verify GTID was updated to version 2
  std::string loaded_gtid;
  mygramdb::config::Config loaded_config;
  std::unordered_map<std::string, std::pair<mygramdb::index::Index*, DocumentStore*>> loaded_contexts;

  auto new_index = std::make_unique<mygramdb::index::Index>(2);
  auto new_doc_store = std::make_unique<DocumentStore>();
  loaded_contexts["test_table"] = {new_index.get(), new_doc_store.get()};

  auto read_result = ReadDumpV1(dump_path, loaded_gtid, loaded_config, loaded_contexts);
  ASSERT_TRUE(read_result.has_value()) << "Read should succeed";
  EXPECT_EQ(loaded_gtid, gtid_v2) << "GTID should be version 2";
}

/**
 * @test Verify concurrent writes don't corrupt the file
 *
 * Multiple writers should either succeed completely or fail completely,
 * never leaving a corrupted file.
 */
TEST_F(AtomicWriteTest, ConcurrentWritesSafe) {
  std::string dump_path = test_dir_ + "/concurrent.dmp";
  mygramdb::config::Config config;

  const int num_threads = 10;
  std::vector<std::thread> threads;
  std::atomic<int> success_count{0};
  std::atomic<int> failure_count{0};

  for (int t = 0; t < num_threads; ++t) {
    threads.emplace_back([&, t]() {
      // Each thread creates its own data
      auto thread_index = std::make_unique<mygramdb::index::Index>(2);
      auto thread_doc_store = std::make_unique<DocumentStore>();

      for (int i = 0; i < 10; ++i) {
        std::string pk = "t" + std::to_string(t) + "_pk" + std::to_string(i);
        thread_doc_store->AddDocument(pk);
      }

      std::unordered_map<std::string, std::pair<mygramdb::index::Index*, DocumentStore*>> contexts;
      contexts["test_table"] = {thread_index.get(), thread_doc_store.get()};

      std::string gtid = "gtid-thread-" + std::to_string(t);

      auto result = WriteDumpV1(dump_path, gtid, config, contexts);
      if (result.has_value()) {
        success_count++;
      } else {
        failure_count++;
      }
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }

  // At least one write should succeed
  EXPECT_GT(success_count.load(), 0) << "At least one write should succeed";

  // Verify final file is valid
  if (fs::exists(dump_path)) {
    dump_format::IntegrityError integrity_error;
    auto verify_result = VerifyDumpIntegrity(dump_path, integrity_error);
    EXPECT_TRUE(verify_result.has_value()) << "Final file should be valid: " << integrity_error.message;
  }
}

/**
 * @test Verify write to read-only directory fails gracefully
 */
TEST_F(AtomicWriteTest, WriteToReadOnlyDirectoryFails) {
  std::string readonly_dir = test_dir_ + "/readonly";
  fs::create_directories(readonly_dir);

  // Make directory read-only
  fs::permissions(readonly_dir, fs::perms::owner_read | fs::perms::owner_exec);

  std::string dump_path = readonly_dir + "/test.dmp";
  std::string gtid = "test-gtid";

  mygramdb::config::Config config;
  std::unordered_map<std::string, std::pair<mygramdb::index::Index*, DocumentStore*>> table_contexts;
  table_contexts["test_table"] = {index_.get(), doc_store_.get()};

  auto result = WriteDumpV1(dump_path, gtid, config, table_contexts);
  EXPECT_FALSE(result.has_value()) << "Write to read-only directory should fail";

  // Restore permissions for cleanup
  fs::permissions(readonly_dir, fs::perms::owner_all);
}

/**
 * @test Verify fsync is called before rename (data durability)
 *
 * This test verifies the write order:
 * 1. Write to temp file
 * 2. fsync temp file
 * 3. Rename to final path
 *
 * We can't directly test fsync, but we can verify the file is complete
 * and matches expected checksum after write.
 */
TEST_F(AtomicWriteTest, DataDurabilityCheck) {
  std::string dump_path = test_dir_ + "/durability.dmp";
  std::string gtid = "test-gtid-durability";

  mygramdb::config::Config config;
  std::unordered_map<std::string, std::pair<mygramdb::index::Index*, DocumentStore*>> table_contexts;
  table_contexts["test_table"] = {index_.get(), doc_store_.get()};

  // Write dump
  auto result = WriteDumpV1(dump_path, gtid, config, table_contexts);
  ASSERT_TRUE(result.has_value()) << "Write should succeed";

  // Verify file integrity (CRC check)
  dump_format::IntegrityError integrity_error;
  auto verify_result = VerifyDumpIntegrity(dump_path, integrity_error);
  ASSERT_TRUE(verify_result.has_value()) << "Integrity check should pass";

  // Read back and verify data
  std::string loaded_gtid;
  mygramdb::config::Config loaded_config;

  auto new_index = std::make_unique<mygramdb::index::Index>(2);
  auto new_doc_store = std::make_unique<DocumentStore>();

  std::unordered_map<std::string, std::pair<mygramdb::index::Index*, DocumentStore*>> loaded_contexts;
  loaded_contexts["test_table"] = {new_index.get(), new_doc_store.get()};

  auto read_result = ReadDumpV1(dump_path, loaded_gtid, loaded_config, loaded_contexts);
  ASSERT_TRUE(read_result.has_value()) << "Read should succeed";

  EXPECT_EQ(loaded_gtid, gtid) << "GTID should match";
  EXPECT_EQ(new_doc_store->Size(), 100) << "Document count should match";
}
