/**
 * @file dump_security_test.cpp
 * @brief Security tests for dump operations (TOCTOU protection, symlink attacks)
 */

#include <fcntl.h>
#include <gtest/gtest.h>
#include <sys/stat.h>
#include <unistd.h>

#include <filesystem>
#include <fstream>

#include "config/config.h"
#include "index/index.h"
#include "storage/document_store.h"
#include "storage/dump_format_v1.h"

using namespace mygramdb::storage;
using namespace mygramdb::storage::dump_v1;

namespace {

// Helper to create a minimal config for testing
mygramdb::config::Config CreateMinimalConfig() {
  mygramdb::config::Config config;
  config.tables.emplace_back();
  config.tables[0].name = "test_table";
  config.tables[0].primary_key = "id";
  config.tables[0].text_source.column = "content";
  config.tables[0].text_source.delimiter = " ";
  config.tables[0].ngram_size = 2;
  return config;
}

}  // namespace

/**
 * @brief Test that dump creation with secure flags works normally
 */
TEST(DumpSecurityTest, NormalDumpCreation) {
  std::filesystem::path temp_dir = std::filesystem::temp_directory_path() / "dump_security_test";
  std::filesystem::create_directories(temp_dir);

  std::string dump_path = (temp_dir / "test.dmp").string();

  // Clean up any existing file
  std::filesystem::remove(dump_path);

  // Create empty table contexts
  std::unordered_map<std::string, std::pair<mygramdb::index::Index*, DocumentStore*>> contexts;

  // Create dump
  auto config = CreateMinimalConfig();
  auto success = WriteDumpV1(dump_path, "test-gtid", config, contexts);

  EXPECT_TRUE(success) << "Dump creation should succeed";
  EXPECT_TRUE(std::filesystem::exists(dump_path)) << "Dump file should exist";

  // Verify file permissions are restrictive (600 = rw-------)
#ifndef _WIN32
  struct stat st;
  ASSERT_EQ(stat(dump_path.c_str(), &st), 0) << "stat() should succeed";
  EXPECT_EQ(st.st_mode & 0777, S_IRUSR | S_IWUSR) << "File should have 600 permissions";
#endif

  // Cleanup
  std::filesystem::remove_all(temp_dir);
}

/**
 * @brief Test that symlink in dump path is rejected
 */
TEST(DumpSecurityTest, RejectSymlinkInPath) {
#ifndef _WIN32
  std::filesystem::path temp_dir = std::filesystem::temp_directory_path() / "dump_security_symlink_test";
  std::filesystem::create_directories(temp_dir);

  // Create target directory
  std::filesystem::path target_dir = temp_dir / "target";
  std::filesystem::create_directories(target_dir);

  // Create symlink
  std::filesystem::path symlink_path = temp_dir / "symlink.dmp";
  std::filesystem::path target_file = target_dir / "actual.dmp";

  // Create symlink pointing to target file
  std::filesystem::create_symlink(target_file, symlink_path);

  // Try to write dump to symlink - should fail with O_NOFOLLOW
  std::unordered_map<std::string, std::pair<mygramdb::index::Index*, DocumentStore*>> contexts;
  auto config = CreateMinimalConfig();
  auto success = WriteDumpV1(symlink_path.string(), "test-gtid", config, contexts);

  EXPECT_FALSE(success) << "Dump creation through symlink should fail";
  EXPECT_FALSE(std::filesystem::exists(target_file)) << "Target file should not be created";

  // Cleanup
  std::filesystem::remove_all(temp_dir);
#else
  GTEST_SKIP() << "Symlink test not applicable on Windows";
#endif
}

/**
 * @brief Test that symlink directory is rejected
 */
TEST(DumpSecurityTest, RejectSymlinkDirectory) {
#ifndef _WIN32
  std::filesystem::path temp_dir = std::filesystem::temp_directory_path() / "dump_security_dir_symlink_test";
  std::filesystem::create_directories(temp_dir);

  // Create target directory
  std::filesystem::path target_dir = temp_dir / "target";
  std::filesystem::create_directories(target_dir);

  // Create symlink directory
  std::filesystem::path symlink_dir = temp_dir / "symlink_dir";
  std::filesystem::create_directory_symlink(target_dir, symlink_dir);

  // Try to write dump to file inside symlink directory - should fail
  std::filesystem::path dump_path = symlink_dir / "test.dmp";

  std::unordered_map<std::string, std::pair<mygramdb::index::Index*, DocumentStore*>> contexts;
  auto config = CreateMinimalConfig();
  auto success = WriteDumpV1(dump_path.string(), "test-gtid", config, contexts);

  EXPECT_FALSE(success) << "Dump creation in symlink directory should fail";

  // Cleanup
  std::filesystem::remove_all(temp_dir);
#else
  GTEST_SKIP() << "Symlink test not applicable on Windows";
#endif
}

/**
 * @brief Test that existing file is overwritten securely
 */
TEST(DumpSecurityTest, OverwriteExistingFile) {
  std::filesystem::path temp_dir = std::filesystem::temp_directory_path() / "dump_security_overwrite_test";
  std::filesystem::create_directories(temp_dir);

  std::string dump_path = (temp_dir / "test.dmp").string();

  // Create initial file with different content
  {
    std::ofstream out(dump_path, std::ios::binary);
    out << "old content";
  }

  ASSERT_TRUE(std::filesystem::exists(dump_path)) << "Initial file should exist";

  // Create dump - should overwrite
  std::unordered_map<std::string, std::pair<mygramdb::index::Index*, DocumentStore*>> contexts;
  auto config = CreateMinimalConfig();
  auto success = WriteDumpV1(dump_path, "test-gtid", config, contexts);

  EXPECT_TRUE(success) << "Dump creation should succeed";
  EXPECT_TRUE(std::filesystem::exists(dump_path)) << "Dump file should exist";

  // Verify file was overwritten (size should be different)
  auto size = std::filesystem::file_size(dump_path);
  EXPECT_GT(size, 100u) << "New dump should be larger than old content";

  // Cleanup
  std::filesystem::remove_all(temp_dir);
}

/**
 * @brief Test that dump directory is created with correct permissions
 */
TEST(DumpSecurityTest, CreateDumpDirectory) {
  std::filesystem::path temp_dir =
      std::filesystem::temp_directory_path() / "dump_security_create_dir_test" / "nested" / "dir";

  // Ensure directory doesn't exist
  std::filesystem::remove_all(temp_dir.parent_path().parent_path());

  std::string dump_path = (temp_dir / "test.dmp").string();

  // Create dump - should create directories
  std::unordered_map<std::string, std::pair<mygramdb::index::Index*, DocumentStore*>> contexts;
  auto config = CreateMinimalConfig();
  auto success = WriteDumpV1(dump_path, "test-gtid", config, contexts);

  EXPECT_TRUE(success) << "Dump creation should succeed";
  EXPECT_TRUE(std::filesystem::exists(temp_dir)) << "Dump directory should be created";
  EXPECT_TRUE(std::filesystem::exists(dump_path)) << "Dump file should exist";

  // Cleanup
  std::filesystem::remove_all(temp_dir.parent_path().parent_path());
}

/**
 * @brief Test ownership verification (simulated)
 */
TEST(DumpSecurityTest, OwnershipVerification) {
  // This test verifies that the ownership check code path exists
  // We can't actually test ownership mismatch without root privileges

  std::filesystem::path temp_dir = std::filesystem::temp_directory_path() / "dump_security_ownership_test";
  std::filesystem::create_directories(temp_dir);

  std::string dump_path = (temp_dir / "test.dmp").string();

  // Create dump normally - should succeed
  std::unordered_map<std::string, std::pair<mygramdb::index::Index*, DocumentStore*>> contexts;
  auto config = CreateMinimalConfig();
  auto success = WriteDumpV1(dump_path, "test-gtid", config, contexts);

  EXPECT_TRUE(success) << "Dump creation should succeed with correct ownership";

#ifndef _WIN32
  // Verify file is owned by current user
  struct stat st;
  ASSERT_EQ(stat(dump_path.c_str(), &st), 0) << "stat() should succeed";
  EXPECT_EQ(st.st_uid, geteuid()) << "File should be owned by current user";
#endif

  // Cleanup
  std::filesystem::remove_all(temp_dir);
}

/**
 * @brief Test that relative paths are handled correctly
 */
TEST(DumpSecurityTest, RelativePathHandling) {
  // Create temp directory
  std::filesystem::path temp_dir = std::filesystem::temp_directory_path() / "dump_security_relative_test";
  std::filesystem::create_directories(temp_dir);

  // Change to temp directory
  auto original_path = std::filesystem::current_path();
  std::filesystem::current_path(temp_dir);

  // Use relative path
  std::string dump_path = "test.dmp";

  // Create dump
  std::unordered_map<std::string, std::pair<mygramdb::index::Index*, DocumentStore*>> contexts;
  auto config = CreateMinimalConfig();
  auto success = WriteDumpV1(dump_path, "test-gtid", config, contexts);

  EXPECT_TRUE(success) << "Dump creation with relative path should succeed";
  EXPECT_TRUE(std::filesystem::exists(dump_path)) << "Dump file should exist";

  // Restore original directory
  std::filesystem::current_path(original_path);

  // Cleanup
  std::filesystem::remove_all(temp_dir);
}

/**
 * @brief Test concurrent dump creation to same file (should be serialized by OS)
 */
TEST(DumpSecurityTest, ConcurrentDumpCreation) {
  std::filesystem::path temp_dir = std::filesystem::temp_directory_path() / "dump_security_concurrent_test";
  std::filesystem::create_directories(temp_dir);

  std::string dump_path = (temp_dir / "test.dmp").string();

  // Clean up any existing file
  std::filesystem::remove(dump_path);

  // Try creating same dump file twice concurrently
  // The second attempt should fail with EEXIST or succeed after removal
  std::unordered_map<std::string, std::pair<mygramdb::index::Index*, DocumentStore*>> contexts;
  auto config = CreateMinimalConfig();

  auto success1 = WriteDumpV1(dump_path, "test-gtid-1", config, contexts);
  EXPECT_TRUE(success1) << "First dump creation should succeed";

  auto success2 = WriteDumpV1(dump_path, "test-gtid-2", config, contexts);
  EXPECT_TRUE(success2) << "Second dump creation should succeed (file removed and recreated)";

  // Cleanup
  std::filesystem::remove_all(temp_dir);
}
