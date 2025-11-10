/**
 * @file gtid_state_test.cpp
 * @brief Unit tests for GTID state file
 */

#include <gtest/gtest.h>
#include "storage/gtid_state.h"
#include <filesystem>
#include <fstream>

#ifdef USE_MYSQL

using namespace mygramdb::storage;

class GTIDStateFileTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create a temporary test directory
    test_dir_ = std::filesystem::temp_directory_path() / "mygramdb_test_gtid";
    std::filesystem::create_directories(test_dir_);
    test_file_ = test_dir_ / "gtid_state.txt";
  }

  void TearDown() override {
    // Clean up test files
    if (std::filesystem::exists(test_dir_)) {
      std::filesystem::remove_all(test_dir_);
    }
  }

  std::filesystem::path test_dir_;
  std::filesystem::path test_file_;
};

TEST_F(GTIDStateFileTest, WriteAndRead) {
  GTIDStateFile state_file(test_file_.string());

  // Write GTID
  std::string gtid = "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5";
  EXPECT_TRUE(state_file.Write(gtid));

  // Read GTID
  auto read_gtid = state_file.Read();
  ASSERT_TRUE(read_gtid.has_value());
  EXPECT_EQ(gtid, read_gtid.value());
}

TEST_F(GTIDStateFileTest, WriteEmptyGTID) {
  GTIDStateFile state_file(test_file_.string());

  // Writing empty GTID should fail
  EXPECT_FALSE(state_file.Write(""));
}

TEST_F(GTIDStateFileTest, ReadNonExistentFile) {
  GTIDStateFile state_file(test_file_.string());

  // Reading non-existent file should return nullopt
  auto read_gtid = state_file.Read();
  EXPECT_FALSE(read_gtid.has_value());
}

TEST_F(GTIDStateFileTest, ReadEmptyFile) {
  // Create empty file
  std::ofstream empty_file(test_file_);
  empty_file.close();

  GTIDStateFile state_file(test_file_.string());

  // Reading empty file should return nullopt
  auto read_gtid = state_file.Read();
  EXPECT_FALSE(read_gtid.has_value());
}

TEST_F(GTIDStateFileTest, OverwriteExisting) {
  GTIDStateFile state_file(test_file_.string());

  // Write first GTID
  std::string gtid1 = "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5";
  EXPECT_TRUE(state_file.Write(gtid1));

  // Overwrite with second GTID
  std::string gtid2 = "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-10";
  EXPECT_TRUE(state_file.Write(gtid2));

  // Read should return second GTID
  auto read_gtid = state_file.Read();
  ASSERT_TRUE(read_gtid.has_value());
  EXPECT_EQ(gtid2, read_gtid.value());
}

TEST_F(GTIDStateFileTest, AtomicWrite) {
  GTIDStateFile state_file(test_file_.string());

  // Write GTID
  std::string gtid = "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5";
  EXPECT_TRUE(state_file.Write(gtid));

  // Verify temporary file is cleaned up
  std::filesystem::path temp_file = test_file_.string() + ".tmp";
  EXPECT_FALSE(std::filesystem::exists(temp_file));

  // Verify actual file exists
  EXPECT_TRUE(std::filesystem::exists(test_file_));
}

TEST_F(GTIDStateFileTest, ExistsCheck) {
  GTIDStateFile state_file(test_file_.string());

  // File should not exist initially
  EXPECT_FALSE(state_file.Exists());

  // Write GTID
  std::string gtid = "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5";
  EXPECT_TRUE(state_file.Write(gtid));

  // File should exist now
  EXPECT_TRUE(state_file.Exists());
}

TEST_F(GTIDStateFileTest, DeleteFile) {
  GTIDStateFile state_file(test_file_.string());

  // Write GTID
  std::string gtid = "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5";
  EXPECT_TRUE(state_file.Write(gtid));

  // Delete file
  EXPECT_TRUE(state_file.Delete());

  // File should not exist
  EXPECT_FALSE(state_file.Exists());

  // Deleting non-existent file should succeed
  EXPECT_TRUE(state_file.Delete());
}

TEST_F(GTIDStateFileTest, DeleteNonExistentFile) {
  GTIDStateFile state_file(test_file_.string());

  // Deleting non-existent file should succeed
  EXPECT_TRUE(state_file.Delete());
}

TEST_F(GTIDStateFileTest, WriteWithWhitespace) {
  GTIDStateFile state_file(test_file_.string());

  // Write GTID with leading/trailing whitespace
  std::string gtid_with_space = "  3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5  ";
  EXPECT_TRUE(state_file.Write(gtid_with_space));

  // Read should return trimmed GTID
  auto read_gtid = state_file.Read();
  ASSERT_TRUE(read_gtid.has_value());
  // Note: Write doesn't trim, but Read does
  EXPECT_EQ("3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5", read_gtid.value());
}

TEST_F(GTIDStateFileTest, CreateParentDirectory) {
  // Use nested directory that doesn't exist
  std::filesystem::path nested_file = test_dir_ / "subdir" / "nested" / "gtid_state.txt";

  GTIDStateFile state_file(nested_file.string());

  // Write should create parent directories
  std::string gtid = "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5";
  EXPECT_TRUE(state_file.Write(gtid));

  // Verify file exists
  EXPECT_TRUE(std::filesystem::exists(nested_file));

  // Read should work
  auto read_gtid = state_file.Read();
  ASSERT_TRUE(read_gtid.has_value());
  EXPECT_EQ(gtid, read_gtid.value());
}

TEST_F(GTIDStateFileTest, MultipleGTIDRanges) {
  GTIDStateFile state_file(test_file_.string());

  // Write GTID with multiple ranges (MySQL GTID set format)
  std::string gtid = "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5:10-20";
  EXPECT_TRUE(state_file.Write(gtid));

  // Read GTID
  auto read_gtid = state_file.Read();
  ASSERT_TRUE(read_gtid.has_value());
  EXPECT_EQ(gtid, read_gtid.value());
}

TEST_F(GTIDStateFileTest, MultipleServerUUIDs) {
  GTIDStateFile state_file(test_file_.string());

  // Write GTID with multiple server UUIDs (comma-separated)
  std::string gtid = "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5,"
                     "4E11FA47-71CA-11E1-9E33-C80AA9429563:1-3";
  EXPECT_TRUE(state_file.Write(gtid));

  // Read GTID
  auto read_gtid = state_file.Read();
  ASSERT_TRUE(read_gtid.has_value());
  EXPECT_EQ(gtid, read_gtid.value());
}

#endif  // USE_MYSQL
