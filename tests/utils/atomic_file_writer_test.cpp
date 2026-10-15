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

#include <cerrno>
#include <chrono>
#include <cstdint>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#ifndef _WIN32
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

namespace mygramdb::utils {

#ifndef _WIN32
namespace {

struct AbandonedDumpTemp {
  std::string temp_basename;
  pid_t owner_pid = 0;
};

bool WriteAllForTest(int file_desc, const void* buffer, size_t size) {
  const auto* bytes = static_cast<const char*>(buffer);
  size_t written = 0;
  while (written < size) {
    const ssize_t result = write(file_desc, bytes + written, size - written);
    if (result < 0) {
      if (errno == EINTR) {
        continue;
      }
      return false;
    }
    if (result == 0) {
      return false;
    }
    written += static_cast<size_t>(result);
  }
  return true;
}

bool ReadAllForTest(int file_desc, void* buffer, size_t size) {
  auto* bytes = static_cast<char*>(buffer);
  size_t read_size = 0;
  while (read_size < size) {
    const ssize_t result = read(file_desc, bytes + read_size, size - read_size);
    if (result < 0) {
      if (errno == EINTR) {
        continue;
      }
      return false;
    }
    if (result == 0) {
      return false;
    }
    read_size += static_cast<size_t>(result);
  }
  return true;
}

std::optional<AbandonedDumpTemp> CreateAbandonedDumpTemp(const std::string& final_path) {
  int communication_pipe[2] = {-1, -1};
  if (pipe(communication_pipe) != 0) {
    return std::nullopt;
  }
  const pid_t child_pid = fork();
  if (child_pid < 0) {
    close(communication_pipe[0]);
    close(communication_pipe[1]);
    return std::nullopt;
  }
  if (child_pid == 0) {
    close(communication_pipe[0]);
    AtomicFileWriter writer(final_path, true);
    const int temp_fd = open(writer.GetTempPath().c_str(), O_RDWR | O_CREAT | O_EXCL | O_NOFOLLOW, S_IRUSR | S_IWUSR);
    if (temp_fd < 0 || !writer.TrackDumpTempFile(temp_fd)) {
      _exit(10);
    }
    const std::string temp_basename = std::filesystem::path(writer.GetTempPath()).filename().string();
    const uint32_t basename_size = static_cast<uint32_t>(temp_basename.size());
    if (!WriteAllForTest(communication_pipe[1], &basename_size, sizeof(basename_size)) ||
        !WriteAllForTest(communication_pipe[1], temp_basename.data(), temp_basename.size()) ||
        !WriteAllForTest(temp_fd, "incomplete dump", 15)) {
      _exit(11);
    }
    _exit(0);  // Simulate process death without running AtomicFileWriter::~AtomicFileWriter.
  }

  close(communication_pipe[1]);
  uint32_t basename_size = 0;
  std::string temp_basename;
  const bool received = ReadAllForTest(communication_pipe[0], &basename_size, sizeof(basename_size)) &&
                        basename_size > 0 && basename_size < 4096;
  if (received) {
    temp_basename.resize(basename_size);
  }
  const bool got_name = received && ReadAllForTest(communication_pipe[0], temp_basename.data(), temp_basename.size());
  close(communication_pipe[0]);
  int child_status = 0;
  const bool waited = waitpid(child_pid, &child_status, 0) == child_pid;
  if (!got_name || !waited || !WIFEXITED(child_status) || WEXITSTATUS(child_status) != 0) {
    return std::nullopt;
  }
  return AbandonedDumpTemp{std::move(temp_basename), child_pid};
}

std::string MakeOwnerMarkerContents(pid_t owner_pid, std::string_view temp_basename, uint64_t device, uint64_t inode) {
  return "MYGRAMDB_DUMP_TEMP_OWNER\n1\n" + std::to_string(static_cast<uint64_t>(owner_pid)) + "\n" +
         std::to_string(device) + "\n" + std::to_string(inode) + "\n" + std::to_string(temp_basename.size()) + "\n" +
         std::string(temp_basename);
}

bool MakeOld(const std::filesystem::path& path) {
  const auto old_time = std::time(nullptr) - 3600;
  const struct timespec times[2] = {{old_time, 0}, {old_time, 0}};
  return utimensat(AT_FDCWD, path.c_str(), times, 0) == 0;
}

}  // namespace
#endif

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

  std::vector<std::filesystem::path> FindOwnerMarkers() const {
    std::vector<std::filesystem::path> markers;
    std::error_code ec;
    for (std::filesystem::directory_iterator it(test_dir_, ec), end; !ec && it != end; it.increment(ec)) {
      const std::string filename = it->path().filename().string();
      if (filename.compare(0, kDumpTempOwnerMarkerPrefix.size(), kDumpTempOwnerMarkerPrefix) == 0) {
        markers.push_back(it->path());
      }
    }
    return markers;
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
  EXPECT_EQ(result.error().code(), ErrorCode::kStorageFileNotFound);
}

#ifndef _WIN32
TEST_F(AtomicFileWriterTest, ParentDirectoryOpenFailureIsReported) {
  const std::string missing_parent_path = TestPath("missing/output.dat");
  auto result = SyncParentDirectory(missing_parent_path);

  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), ErrorCode::kStorageWriteError);
  EXPECT_NE(result.error().message().find("open output directory"), std::string::npos);
}
#endif

#ifndef _WIN32
TEST_F(AtomicFileWriterTest, CommitRejectsSymlinkTempFile) {
  std::string final_path = TestPath("output.dat");
  std::string target_path = TestPath("target.dat");
  WriteToFile(target_path, "target data");

  AtomicFileWriter writer(final_path);
  std::filesystem::create_symlink(target_path, writer.GetTempPath());

  auto result = writer.Commit();

  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), ErrorCode::kStorageWriteError);
  EXPECT_FALSE(std::filesystem::exists(final_path));
  EXPECT_EQ(ReadFile(target_path), "target data");
  EXPECT_FALSE(std::filesystem::exists(writer.GetTempPath()));
}
#endif

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

TEST_F(AtomicFileWriterTest, RollbackCleansUpTempAndLeavesCommittedFalse) {
  std::string final_path = TestPath("output.dat");

  AtomicFileWriter writer(final_path);
  WriteToFile(writer.GetTempPath(), "temporary data");
  EXPECT_TRUE(std::filesystem::exists(writer.GetTempPath()));

  writer.Rollback();

  // Temp file should be removed
  EXPECT_FALSE(std::filesystem::exists(writer.GetTempPath()));
  // Final file should not exist
  EXPECT_FALSE(std::filesystem::exists(final_path));

  // Commit after rollback should fail (temp file no longer exists)
  auto result = writer.Commit();
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), ErrorCode::kStorageFileNotFound);
}

TEST_F(AtomicFileWriterTest, DoubleRollbackIsSafe) {
  std::string final_path = TestPath("output.dat");

  AtomicFileWriter writer(final_path);
  WriteToFile(writer.GetTempPath(), "temporary data");

  writer.Rollback();
  // Second rollback should be a no-op (no crash)
  writer.Rollback();

  EXPECT_FALSE(std::filesystem::exists(writer.GetTempPath()));
  EXPECT_FALSE(std::filesystem::exists(final_path));
}

TEST_F(AtomicFileWriterTest, DestructorAfterRollbackDoesNotDoubleRemove) {
  std::string final_path = TestPath("output.dat");
  std::string temp_path;

  {
    AtomicFileWriter writer(final_path);
    temp_path = writer.GetTempPath();
    WriteToFile(temp_path, "temporary data");
    writer.Rollback();
    EXPECT_FALSE(std::filesystem::exists(temp_path));
    // Destructor should not attempt cleanup again
  }

  EXPECT_FALSE(std::filesystem::exists(temp_path));
  EXPECT_FALSE(std::filesystem::exists(final_path));
}

#ifndef _WIN32
TEST_F(AtomicFileWriterTest, TrackedDumpCommitAndRollbackRemoveOwnerMarker) {
  const std::string committed_path = TestPath("tracked_commit.dat");
  {
    AtomicFileWriter writer(committed_path, true);
    const int fd = open(writer.GetTempPath().c_str(), O_RDWR | O_CREAT | O_EXCL | O_NOFOLLOW, S_IRUSR | S_IWUSR);
    ASSERT_GE(fd, 0);
    auto tracked = writer.TrackDumpTempFile(fd);
    ASSERT_TRUE(tracked.has_value()) << tracked.error().message();
    const auto owner_markers = FindOwnerMarkers();
    ASSERT_EQ(owner_markers.size(), 1U);
    struct stat marker_stat {};
    ASSERT_EQ(stat(owner_markers.front().c_str(), &marker_stat), 0);
    EXPECT_EQ(marker_stat.st_mode & (S_IRWXU | S_IRWXG | S_IRWXO), S_IRUSR | S_IWUSR);
    ASSERT_TRUE(WriteAllForTest(fd, "committed", 9));
    ASSERT_EQ(close(fd), 0);

    auto result = writer.Commit();
    ASSERT_TRUE(result.has_value()) << result.error().message();
  }
  EXPECT_EQ(ReadFile(committed_path), "committed");
  EXPECT_TRUE(FindOwnerMarkers().empty());

  const std::string rolled_back_path = TestPath("tracked_rollback.dat");
  {
    AtomicFileWriter writer(rolled_back_path, true);
    const int fd = open(writer.GetTempPath().c_str(), O_RDWR | O_CREAT | O_EXCL | O_NOFOLLOW, S_IRUSR | S_IWUSR);
    ASSERT_GE(fd, 0);
    auto tracked = writer.TrackDumpTempFile(fd);
    ASSERT_TRUE(tracked.has_value()) << tracked.error().message();
    ASSERT_TRUE(WriteAllForTest(fd, "discard", 7));
    ASSERT_EQ(close(fd), 0);
    writer.Rollback();
    EXPECT_FALSE(std::filesystem::exists(writer.GetTempPath()));
  }
  EXPECT_FALSE(std::filesystem::exists(rolled_back_path));
  EXPECT_TRUE(FindOwnerMarkers().empty());
}

TEST_F(AtomicFileWriterTest, GenericWriterDoesNotCreateDumpOwnerMarker) {
  const std::string final_path = TestPath("generic_writer.dat");
  {
    AtomicFileWriter writer(final_path, true);
    WriteToFile(writer.GetTempPath(), "generic temp");
    EXPECT_TRUE(FindOwnerMarkers().empty());
  }
  EXPECT_TRUE(FindOwnerMarkers().empty());
}

TEST_F(AtomicFileWriterTest, RemovesOnlyOldOrphanNamedByDeadOwnerMarker) {
  const std::string final_path = TestPath("orphaned_dump.dat");
  const auto abandoned = CreateAbandonedDumpTemp(final_path);
  ASSERT_TRUE(abandoned.has_value());
  const auto temp_path = test_dir_ / abandoned->temp_basename;
  ASSERT_TRUE(std::filesystem::exists(temp_path));
  auto markers = FindOwnerMarkers();
  ASSERT_EQ(markers.size(), 1U);
  struct stat marker_stat {};
  ASSERT_EQ(stat(markers.front().c_str(), &marker_stat), 0);
  EXPECT_EQ(marker_stat.st_mode & (S_IRWXU | S_IRWXG | S_IRWXO), S_IRUSR | S_IWUSR);

  const std::string unmarked_legacy = TestPath("backup.tmp");
  const std::string unmarked_unique = TestPath("backup.tmp.123.456");
  WriteToFile(unmarked_legacy, "keep legacy temp");
  WriteToFile(unmarked_unique, "keep unrelated temp");

  auto young_result = RemoveOrphanDumpTempFile(test_dir_, markers.front().filename().string(), std::chrono::hours(1));
  ASSERT_TRUE(young_result.has_value()) << young_result.error().message();
  EXPECT_FALSE(*young_result);
  EXPECT_TRUE(std::filesystem::exists(temp_path));
  EXPECT_TRUE(std::filesystem::exists(markers.front()));

  ASSERT_TRUE(MakeOld(temp_path));
  ASSERT_TRUE(MakeOld(markers.front()));
  auto cleanup_result =
      RemoveOrphanDumpTempFile(test_dir_, markers.front().filename().string(), std::chrono::seconds(0));
  ASSERT_TRUE(cleanup_result.has_value()) << cleanup_result.error().message();
  EXPECT_TRUE(*cleanup_result);
  EXPECT_FALSE(std::filesystem::exists(temp_path));
  EXPECT_FALSE(std::filesystem::exists(markers.front()));
  EXPECT_TRUE(std::filesystem::exists(unmarked_legacy));
  EXPECT_TRUE(std::filesystem::exists(unmarked_unique));

  auto legacy_result = RemoveOrphanDumpTempFile(test_dir_, "backup.tmp", std::chrono::seconds(0));
  auto unrelated_result = RemoveOrphanDumpTempFile(test_dir_, "backup.tmp.123.456", std::chrono::seconds(0));
  ASSERT_TRUE(legacy_result.has_value());
  ASSERT_TRUE(unrelated_result.has_value());
  EXPECT_FALSE(*legacy_result);
  EXPECT_FALSE(*unrelated_result);
  EXPECT_TRUE(std::filesystem::exists(unmarked_legacy));
  EXPECT_TRUE(std::filesystem::exists(unmarked_unique));
}

TEST_F(AtomicFileWriterTest, MissingTempRemovesOnlyItsOldDeadOwnerMarker) {
  const std::string final_path = TestPath("missing_temp_dump.dat");
  const auto abandoned = CreateAbandonedDumpTemp(final_path);
  ASSERT_TRUE(abandoned.has_value());
  const auto temp_path = test_dir_ / abandoned->temp_basename;
  const auto markers = FindOwnerMarkers();
  ASSERT_EQ(markers.size(), 1U);
  ASSERT_TRUE(std::filesystem::remove(temp_path));
  ASSERT_TRUE(MakeOld(markers.front()));
  const std::string unrelated_temp = TestPath("backup.tmp.123.456");
  WriteToFile(unrelated_temp, "keep unrelated temp");

  auto result = RemoveOrphanDumpTempFile(test_dir_, markers.front().filename().string(), std::chrono::seconds(0));
  ASSERT_TRUE(result.has_value()) << result.error().message();
  EXPECT_TRUE(*result);
  EXPECT_FALSE(std::filesystem::exists(markers.front()));
  EXPECT_TRUE(std::filesystem::exists(unrelated_temp));
}

TEST_F(AtomicFileWriterTest, LiveOwnerAndUnsafeMarkersAreRetained) {
  const std::string final_path = TestPath("live_dump.dat");
  AtomicFileWriter writer(final_path, true);
  const int fd = open(writer.GetTempPath().c_str(), O_RDWR | O_CREAT | O_EXCL | O_NOFOLLOW, S_IRUSR | S_IWUSR);
  ASSERT_GE(fd, 0);
  auto tracked = writer.TrackDumpTempFile(fd);
  ASSERT_TRUE(tracked.has_value()) << tracked.error().message();
  ASSERT_TRUE(WriteAllForTest(fd, "partial", 7));
  ASSERT_EQ(close(fd), 0);
  const auto markers = FindOwnerMarkers();
  ASSERT_EQ(markers.size(), 1U);
  ASSERT_TRUE(MakeOld(writer.GetTempPath()));
  ASSERT_TRUE(MakeOld(markers.front()));

  auto live_result = RemoveOrphanDumpTempFile(test_dir_, markers.front().filename().string(), std::chrono::seconds(0));
  ASSERT_TRUE(live_result.has_value()) << live_result.error().message();
  EXPECT_FALSE(*live_result);
  EXPECT_TRUE(std::filesystem::exists(writer.GetTempPath()));
  EXPECT_TRUE(std::filesystem::exists(markers.front()));
  writer.Rollback();
  EXPECT_FALSE(std::filesystem::exists(writer.GetTempPath()));
  EXPECT_TRUE(FindOwnerMarkers().empty());
}

TEST_F(AtomicFileWriterTest, MalformedDeadOwnerMarkerFieldsAreRetained) {
  const auto abandoned = CreateAbandonedDumpTemp(TestPath("malformed_marker_dump.dat"));
  ASSERT_TRUE(abandoned.has_value());
  const auto temp_path = test_dir_ / abandoned->temp_basename;
  const auto markers = FindOwnerMarkers();
  ASSERT_EQ(markers.size(), 1U);
  const auto marker_path = markers.front();

  struct stat temp_stat {};
  ASSERT_EQ(stat(temp_path.c_str(), &temp_stat), 0);
  struct stat marker_stat {};
  ASSERT_EQ(stat(marker_path.c_str(), &marker_stat), 0);
  EXPECT_EQ(marker_stat.st_mode & (S_IRWXU | S_IRWXG | S_IRWXO), S_IRUSR | S_IWUSR);

  const std::string valid_contents = ReadFile(marker_path.string());
  ASSERT_FALSE(valid_contents.empty());
  std::string bad_magic_contents = valid_contents;
  ASSERT_GE(bad_magic_contents.size(), 8U);
  bad_magic_contents.replace(0, 8, "INVALID!");
  WriteToFile(marker_path.string(), bad_magic_contents);
  ASSERT_EQ(ReadFile(marker_path.string()), bad_magic_contents);
  ASSERT_TRUE(MakeOld(marker_path));

  auto bad_magic_result = RemoveOrphanDumpTempFile(test_dir_, marker_path.filename().string(), std::chrono::seconds(0));
  ASSERT_TRUE(bad_magic_result.has_value()) << bad_magic_result.error().message();
  EXPECT_FALSE(*bad_magic_result);
  EXPECT_TRUE(std::filesystem::exists(temp_path));
  EXPECT_EQ(ReadFile(temp_path.string()), "incomplete dump");
  EXPECT_TRUE(std::filesystem::exists(marker_path));

  const std::string traversal_basename = "../" + test_dir_.filename().string() + "/untrusted.tmp";
  const auto traversal_target = test_dir_ / "untrusted.tmp";
  WriteToFile(traversal_target.string(), "keep traversal target");
  const std::string traversal_contents =
      MakeOwnerMarkerContents(abandoned->owner_pid, traversal_basename, static_cast<uint64_t>(temp_stat.st_dev),
                              static_cast<uint64_t>(temp_stat.st_ino));
  WriteToFile(marker_path.string(), traversal_contents);
  ASSERT_EQ(ReadFile(marker_path.string()), traversal_contents);
  ASSERT_EQ(stat(marker_path.c_str(), &marker_stat), 0);
  EXPECT_EQ(marker_stat.st_mode & (S_IRWXU | S_IRWXG | S_IRWXO), S_IRUSR | S_IWUSR);
  ASSERT_TRUE(MakeOld(marker_path));

  auto traversal_result = RemoveOrphanDumpTempFile(test_dir_, marker_path.filename().string(), std::chrono::seconds(0));
  ASSERT_TRUE(traversal_result.has_value()) << traversal_result.error().message();
  EXPECT_FALSE(*traversal_result);
  EXPECT_TRUE(std::filesystem::exists(temp_path));
  EXPECT_EQ(ReadFile(temp_path.string()), "incomplete dump");
  EXPECT_TRUE(std::filesystem::exists(marker_path));
  EXPECT_EQ(ReadFile(traversal_target.string()), "keep traversal target");

  auto unsafe_basename_result = RemoveOrphanDumpTempFile(test_dir_, "../untrusted.tmp", std::chrono::seconds(0));
  ASSERT_TRUE(unsafe_basename_result.has_value());
  EXPECT_FALSE(*unsafe_basename_result);
}

TEST_F(AtomicFileWriterTest, SymlinkOwnerMarkerIsNotFollowed) {
  const std::string final_path = TestPath("symlink_marker_dump.dat");
  const auto abandoned = CreateAbandonedDumpTemp(final_path);
  ASSERT_TRUE(abandoned.has_value());
  const auto temp_path = test_dir_ / abandoned->temp_basename;
  const auto markers = FindOwnerMarkers();
  ASSERT_EQ(markers.size(), 1U);
  const auto real_marker_path = test_dir_ / "marker_contents.dat";
  std::error_code ec;
  std::filesystem::rename(markers.front(), real_marker_path, ec);
  ASSERT_FALSE(ec) << ec.message();
  std::filesystem::create_symlink(real_marker_path, markers.front(), ec);
  ASSERT_FALSE(ec) << ec.message();

  ASSERT_TRUE(MakeOld(temp_path));
  ASSERT_TRUE(MakeOld(real_marker_path));
  auto result = RemoveOrphanDumpTempFile(test_dir_, markers.front().filename().string(), std::chrono::seconds(0));
  ASSERT_TRUE(result.has_value()) << result.error().message();
  EXPECT_FALSE(*result);
  EXPECT_TRUE(std::filesystem::is_symlink(markers.front()));
  EXPECT_TRUE(std::filesystem::exists(temp_path));
}

TEST_F(AtomicFileWriterTest, TempSymlinkAndInodeReplacementAreNeverDeleted) {
  {
    const std::string final_path = TestPath("symlink_temp_dump.dat");
    const auto abandoned = CreateAbandonedDumpTemp(final_path);
    ASSERT_TRUE(abandoned.has_value());
    const auto temp_path = test_dir_ / abandoned->temp_basename;
    const auto markers = FindOwnerMarkers();
    ASSERT_EQ(markers.size(), 1U);
    const auto target_path = test_dir_ / "symlink_target.dat";
    WriteToFile(target_path.string(), "preserve target");
    ASSERT_EQ(std::filesystem::remove(temp_path), true);
    ASSERT_NO_THROW(std::filesystem::create_symlink(target_path, temp_path));
    ASSERT_TRUE(MakeOld(markers.front()));

    auto result = RemoveOrphanDumpTempFile(test_dir_, markers.front().filename().string(), std::chrono::seconds(0));
    ASSERT_TRUE(result.has_value()) << result.error().message();
    EXPECT_FALSE(*result);
    EXPECT_TRUE(std::filesystem::is_symlink(temp_path));
    EXPECT_EQ(ReadFile(target_path.string()), "preserve target");
    std::filesystem::remove(temp_path);
    std::filesystem::remove(markers.front());
  }

  {
    const std::string final_path = TestPath("replacement_dump.dat");
    const auto abandoned = CreateAbandonedDumpTemp(final_path);
    ASSERT_TRUE(abandoned.has_value());
    const auto temp_path = test_dir_ / abandoned->temp_basename;
    const auto markers = FindOwnerMarkers();
    ASSERT_EQ(markers.size(), 1U);
    ASSERT_TRUE(std::filesystem::remove(temp_path));
    WriteToFile(temp_path.string(), "completed replacement payload");
    ASSERT_TRUE(MakeOld(temp_path));
    ASSERT_TRUE(MakeOld(markers.front()));

    auto result = RemoveOrphanDumpTempFile(test_dir_, markers.front().filename().string(), std::chrono::seconds(0));
    ASSERT_TRUE(result.has_value()) << result.error().message();
    EXPECT_FALSE(*result);
    EXPECT_EQ(ReadFile(temp_path.string()), "completed replacement payload");
    EXPECT_TRUE(std::filesystem::exists(markers.front()));
  }
}
#endif

}  // namespace mygramdb::utils
