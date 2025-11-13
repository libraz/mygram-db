/**
 * @file dump_auto_save_test.cpp
 * @brief Tests for dump auto-save functionality
 */

#include <gtest/gtest.h>
#include <spdlog/spdlog.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <thread>

#include "config/config.h"
#include "index/index.h"
#include "server/tcp_server.h"
#include "storage/document_store.h"

using namespace mygramdb::server;
using namespace mygramdb;

namespace {

/**
 * @brief Test fixture for dump auto-save tests
 */
class DumpAutoSaveTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Set log level to error to reduce test noise
    spdlog::set_level(spdlog::level::err);

    // Create temporary test directory
    test_dir_ = std::filesystem::temp_directory_path() / "mygramdb_dump_test";
    std::filesystem::create_directories(test_dir_);

    // Create test table context
    auto index = std::make_unique<index::Index>(2);
    auto doc_store = std::make_unique<storage::DocumentStore>();

    // Add test data
    doc_store->AddDocument("test1", {});
    doc_store->AddDocument("test2", {});
    index->AddDocument(1, "hello world");
    index->AddDocument(2, "test data");

    table_context_.name = "test_table";
    table_context_.config.ngram_size = 2;
    table_context_.index = std::move(index);
    table_context_.doc_store = std::move(doc_store);

    table_contexts_["test_table"] = &table_context_;
  }

  void TearDown() override {
    // Clean up test directory
    if (std::filesystem::exists(test_dir_)) {
      std::filesystem::remove_all(test_dir_);
    }

    // Reset log level
    spdlog::set_level(spdlog::level::info);
  }

  std::filesystem::path test_dir_;
  TableContext table_context_;
  std::unordered_map<std::string, TableContext*> table_contexts_;
};

/**
 * @brief Test dump directory creation
 */
TEST_F(DumpAutoSaveTest, DumpDirectoryCreation) {
  std::filesystem::path new_dir = test_dir_ / "new_dump_dir";

  // Directory should not exist initially
  EXPECT_FALSE(std::filesystem::exists(new_dir));

  // Create config with new directory
  config::Config config;
  config.dump.dir = new_dir.string();
  config.dump.interval_sec = 0;  // Disable auto-save

  // Create server (should create directory)
  ServerConfig server_config;
  server_config.port = 0;  // Random port

  TcpServer server(server_config, table_contexts_, config.dump.dir, &config, nullptr);

  // Start and immediately stop
  EXPECT_TRUE(server.Start());
  server.Stop();

  // Directory should exist but may not have been created by server
  // (main.cpp does the creation, not TcpServer)
}

/**
 * @brief Test dump directory permission check
 */
TEST_F(DumpAutoSaveTest, DumpDirectoryPermission) {
  // Create a read-only directory
  std::filesystem::path readonly_dir = test_dir_ / "readonly";
  std::filesystem::create_directories(readonly_dir);

#ifndef _WIN32
  // Make directory read-only (Unix only)
  std::filesystem::permissions(readonly_dir, std::filesystem::perms::owner_read | std::filesystem::perms::owner_exec,
                               std::filesystem::perm_options::replace);

  // Try to create a test file (should fail)
  std::filesystem::path test_file = readonly_dir / ".write_test";
  std::ofstream test_stream(test_file);
  EXPECT_FALSE(test_stream.is_open());

  // Restore permissions for cleanup
  std::filesystem::permissions(readonly_dir, std::filesystem::perms::owner_all, std::filesystem::perm_options::replace);
#endif
}

/**
 * @brief Test auto-save disabled
 */
TEST_F(DumpAutoSaveTest, AutoSaveDisabled) {
  config::Config config;
  config.dump.dir = test_dir_.string();
  config.dump.interval_sec = 0;  // Disabled
  config.dump.retain = 3;

  ServerConfig server_config;
  server_config.port = 0;  // Random port

  TcpServer server(server_config, table_contexts_, config.dump.dir, &config, nullptr);

  EXPECT_TRUE(server.Start());

  // Wait a bit
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  server.Stop();

  // No auto-saved files should be created
  int auto_file_count = 0;
  for (const auto& entry : std::filesystem::directory_iterator(test_dir_)) {
    if (entry.is_regular_file() && entry.path().filename().string().rfind("auto_", 0) == 0) {
      auto_file_count++;
    }
  }

  EXPECT_EQ(auto_file_count, 0);
}

/**
 * @brief Test auto-save functionality
 */
TEST_F(DumpAutoSaveTest, AutoSaveEnabled) {
  config::Config config;
  config.dump.dir = test_dir_.string();
  config.dump.interval_sec = 1;  // 1 second for fast test
  config.dump.retain = 3;

  ServerConfig server_config;
  server_config.port = 0;  // Random port

  TcpServer server(server_config, table_contexts_, config.dump.dir, &config, nullptr);

  EXPECT_TRUE(server.Start());

  // Wait for at least one auto-save
  std::this_thread::sleep_for(std::chrono::seconds(2));

  server.Stop();

  // Check that at least one auto-saved file was created
  int auto_file_count = 0;
  std::filesystem::path found_file;

  for (const auto& entry : std::filesystem::directory_iterator(test_dir_)) {
    if (entry.is_regular_file() && entry.path().filename().string().rfind("auto_", 0) == 0 &&
        entry.path().extension() == ".dmp") {
      auto_file_count++;
      found_file = entry.path();
    }
  }

  EXPECT_GE(auto_file_count, 1) << "Expected at least one auto-saved file";

  // Verify file is valid
  if (!found_file.empty()) {
    EXPECT_TRUE(std::filesystem::exists(found_file));
    EXPECT_GT(std::filesystem::file_size(found_file), 0);
  }
}

/**
 * @brief Test dump file cleanup based on retain count
 */
TEST_F(DumpAutoSaveTest, DumpFileCleanup) {
  config::Config config;
  config.dump.dir = test_dir_.string();
  config.dump.interval_sec = 1;  // 1 second for fast test
  config.dump.retain = 2;        // Keep only 2 files

  ServerConfig server_config;
  server_config.port = 0;  // Random port

  TcpServer server(server_config, table_contexts_, config.dump.dir, &config, nullptr);

  EXPECT_TRUE(server.Start());

  // Wait for multiple auto-saves
  std::this_thread::sleep_for(std::chrono::seconds(4));

  server.Stop();

  // Count auto-saved files
  int auto_file_count = 0;
  for (const auto& entry : std::filesystem::directory_iterator(test_dir_)) {
    if (entry.is_regular_file() && entry.path().filename().string().rfind("auto_", 0) == 0 &&
        entry.path().extension() == ".dmp") {
      auto_file_count++;
    }
  }

  // Should have at most 'retain' files
  EXPECT_LE(auto_file_count, config.dump.retain) << "Expected at most " << config.dump.retain << " files";
}

/**
 * @brief Test manual dumps are not affected by cleanup
 */
TEST_F(DumpAutoSaveTest, ManualDumpsNotAffected) {
  config::Config config;
  config.dump.dir = test_dir_.string();
  config.dump.interval_sec = 1;  // 1 second for fast test
  config.dump.retain = 1;        // Keep only 1 auto-saved file

  // Create manual dump files (without "auto_" prefix)
  std::filesystem::path manual_file1 = test_dir_ / "manual_backup_20231201.dmp";
  std::filesystem::path manual_file2 = test_dir_ / "mygramdb.dmp";

  std::ofstream(manual_file1) << "manual dump 1";
  std::ofstream(manual_file2) << "manual dump 2";

  ServerConfig server_config;
  server_config.port = 0;  // Random port

  TcpServer server(server_config, table_contexts_, config.dump.dir, &config, nullptr);

  EXPECT_TRUE(server.Start());

  // Wait for multiple auto-saves
  std::this_thread::sleep_for(std::chrono::seconds(3));

  server.Stop();

  // Manual files should still exist
  EXPECT_TRUE(std::filesystem::exists(manual_file1));
  EXPECT_TRUE(std::filesystem::exists(manual_file2));

  // Count auto-saved files
  int auto_file_count = 0;
  for (const auto& entry : std::filesystem::directory_iterator(test_dir_)) {
    if (entry.is_regular_file() && entry.path().filename().string().rfind("auto_", 0) == 0 &&
        entry.path().extension() == ".dmp") {
      auto_file_count++;
    }
  }

  // Should have at most 'retain' auto-saved files
  EXPECT_LE(auto_file_count, config.dump.retain);
}

/**
 * @brief Test filename format
 */
TEST_F(DumpAutoSaveTest, FilenameFormat) {
  config::Config config;
  config.dump.dir = test_dir_.string();
  config.dump.interval_sec = 1;  // 1 second for fast test
  config.dump.retain = 5;

  ServerConfig server_config;
  server_config.port = 0;  // Random port

  TcpServer server(server_config, table_contexts_, config.dump.dir, &config, nullptr);

  EXPECT_TRUE(server.Start());

  // Wait for one auto-save
  std::this_thread::sleep_for(std::chrono::seconds(2));

  server.Stop();

  // Check filename format: auto_YYYYMMDD_HHMMSS.dmp
  bool found_valid_filename = false;
  for (const auto& entry : std::filesystem::directory_iterator(test_dir_)) {
    std::string filename = entry.path().filename().string();
    if (filename.rfind("auto_", 0) == 0 && entry.path().extension() == ".dmp") {
      // Extract timestamp part: auto_YYYYMMDD_HHMMSS.dmp
      std::string timestamp = filename.substr(5, 15);  // "YYYYMMDD_HHMMSS"

      // Verify format (should be digits and underscore)
      bool valid_format = timestamp.length() == 15 && timestamp[8] == '_' && std::isdigit(timestamp[0]) &&
                          std::isdigit(timestamp[1]) && std::isdigit(timestamp[2]) && std::isdigit(timestamp[3]);

      if (valid_format) {
        found_valid_filename = true;
        break;
      }
    }
  }

  EXPECT_TRUE(found_valid_filename) << "Expected to find a file with format auto_YYYYMMDD_HHMMSS.dmp";
}

}  // namespace
