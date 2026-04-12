/**
 * @file atomic_file_writer_test.cpp
 * @brief Tests for AtomicFileWriter: atomic write via temp-file + fsync + rename
 *
 * Tests the expected behavior:
 * - Write to temp, commit atomically renames to final path
 * - Rollback removes temp file without touching final path
 * - Destructor cleans up uncommitted temp file (RAII)
 * - Double commit returns error
 * - Commit on nonexistent temp file (no write) handles gracefully
 * - Unique suffix mode generates distinct temp paths
 * - Overwriting an existing file works
 */

#include "utils/atomic_file_writer.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <string>

namespace mygram::utils {

class AtomicFileWriterTest : public ::testing::Test {
 protected:
  void SetUp() override {
    test_dir_ = std::filesystem::temp_directory_path() / ("afw_test_" + std::to_string(getpid()));
    std::filesystem::create_directories(test_dir_);
  }

  void TearDown() override {
    std::error_code ec;
    std::filesystem::remove_all(test_dir_, ec);
  }

  std::string TestPath(const std::string& name) { return (test_dir_ / name).string(); }

  std::string ReadFile(const std::string& path) {
    std::ifstream ifs(path);
    return {std::istreambuf_iterator<char>(ifs), std::istreambuf_iterator<char>()};
  }

  void WriteToFile(const std::string& path, const std::string& content) {
    std::ofstream ofs(path);
    ofs << content;
  }

  std::filesystem::path test_dir_;
};

TEST_F(AtomicFileWriterTest, CommitRenamesTempToFinal) {
  std::string final_path = TestPath("output.dat");

  AtomicFileWriter writer(final_path);

  // Write content to temp file
  WriteToFile(writer.GetTempPath(), "hello world");
  EXPECT_TRUE(std::filesystem::exists(writer.GetTempPath()));
  EXPECT_FALSE(std::filesystem::exists(final_path));

  auto result = writer.Commit();
  ASSERT_TRUE(result.has_value()) << result.error().to_string();

  // After commit: final file exists, temp file removed
  EXPECT_TRUE(std::filesystem::exists(final_path));
  EXPECT_FALSE(std::filesystem::exists(writer.GetTempPath()));
  EXPECT_EQ(ReadFile(final_path), "hello world");
}

TEST_F(AtomicFileWriterTest, RollbackRemovesTempFile) {
  std::string final_path = TestPath("output.dat");

  AtomicFileWriter writer(final_path);
  WriteToFile(writer.GetTempPath(), "temporary data");

  writer.Rollback();

  EXPECT_FALSE(std::filesystem::exists(writer.GetTempPath()));
  EXPECT_FALSE(std::filesystem::exists(final_path));
}

TEST_F(AtomicFileWriterTest, DestructorCleansUpUncommitted) {
  std::string final_path = TestPath("output.dat");
  std::string temp_path;

  {
    AtomicFileWriter writer(final_path);
    temp_path = writer.GetTempPath();
    WriteToFile(temp_path, "will be cleaned up");
    EXPECT_TRUE(std::filesystem::exists(temp_path));
    // Destructor runs here without Commit
  }

  // Destructor should have cleaned up the temp file
  EXPECT_FALSE(std::filesystem::exists(temp_path));
  EXPECT_FALSE(std::filesystem::exists(final_path));
}

TEST_F(AtomicFileWriterTest, DoubleCommitReturnsError) {
  std::string final_path = TestPath("output.dat");

  AtomicFileWriter writer(final_path);
  WriteToFile(writer.GetTempPath(), "data");

  auto first = writer.Commit();
  ASSERT_TRUE(first.has_value());

  auto second = writer.Commit();
  ASSERT_FALSE(second.has_value());
  EXPECT_EQ(second.error().code(), ErrorCode::kStorageWriteError);
}

TEST_F(AtomicFileWriterTest, OverwriteExistingFile) {
  std::string final_path = TestPath("existing.dat");
  WriteToFile(final_path, "old content");

  AtomicFileWriter writer(final_path);
  WriteToFile(writer.GetTempPath(), "new content");

  auto result = writer.Commit();
  ASSERT_TRUE(result.has_value()) << result.error().to_string();

  EXPECT_EQ(ReadFile(final_path), "new content");
}

TEST_F(AtomicFileWriterTest, DefaultSuffixIsTmp) {
  std::string final_path = TestPath("output.dat");
  AtomicFileWriter writer(final_path);

  EXPECT_EQ(writer.GetTempPath(), final_path + ".tmp");
}

TEST_F(AtomicFileWriterTest, UniqueSuffixGeneratesDistinctPath) {
  std::string final_path = TestPath("output.dat");
  AtomicFileWriter writer1(final_path, true);
  AtomicFileWriter writer2(final_path, true);

  // Unique suffixes should be different from each other and from the default
  EXPECT_NE(writer1.GetTempPath(), final_path + ".tmp");
  EXPECT_NE(writer1.GetTempPath(), writer2.GetTempPath());

  // Both should start with the final path
  EXPECT_EQ(writer1.GetTempPath().substr(0, final_path.size()), final_path);
  EXPECT_EQ(writer2.GetTempPath().substr(0, final_path.size()), final_path);
}

TEST_F(AtomicFileWriterTest, CommitWithNoTempFileWritten) {
  std::string final_path = TestPath("output.dat");

  AtomicFileWriter writer(final_path);
  // Don't write anything to temp file

  auto result = writer.Commit();
  // Commit should fail because temp file does not exist
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), ErrorCode::kIOError);
}

TEST_F(AtomicFileWriterTest, EmptyContentWrite) {
  std::string final_path = TestPath("empty.dat");

  AtomicFileWriter writer(final_path);
  WriteToFile(writer.GetTempPath(), "");

  auto result = writer.Commit();
  ASSERT_TRUE(result.has_value()) << result.error().to_string();

  EXPECT_TRUE(std::filesystem::exists(final_path));
  EXPECT_EQ(ReadFile(final_path), "");
}

TEST_F(AtomicFileWriterTest, LargeContentWrite) {
  std::string final_path = TestPath("large.dat");

  AtomicFileWriter writer(final_path);
  std::string large_content(1024 * 1024, 'X');  // 1MB
  WriteToFile(writer.GetTempPath(), large_content);

  auto result = writer.Commit();
  ASSERT_TRUE(result.has_value()) << result.error().to_string();

  EXPECT_EQ(ReadFile(final_path), large_content);
}

}  // namespace mygram::utils
