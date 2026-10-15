/**
 * @file atomic_file_writer.cpp
 * @brief Implementation of AtomicFileWriter for durable atomic file writes
 */

#include "utils/atomic_file_writer.h"

#include <signal.h>

#include <array>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <limits>
#include <optional>
#include <random>
#include <sstream>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>

#ifndef _WIN32
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#endif

#include "utils/fd_guard.h"
#include "utils/structured_log.h"

namespace mygramdb::utils {

#ifndef _WIN32
#ifndef O_NOFOLLOW
#define O_NOFOLLOW 0
#endif
#ifndef O_CLOEXEC
#define O_CLOEXEC 0
#endif
#endif

namespace {

#ifndef _WIN32

constexpr char kDumpTempOwnerMagic[] = "MYGRAMDB_DUMP_TEMP_OWNER";
constexpr char kDumpTempOwnerVersion[] = "1";
constexpr size_t kMaxDumpTempOwnerMarkerSize = 1024;

using mygramdb::utils::Error;
using mygramdb::utils::ErrorCode;
using mygramdb::utils::Expected;
using mygramdb::utils::MakeError;
using mygramdb::utils::MakeUnexpected;

auto MakeFileError(const std::string& operation, const std::string& path, int error_number) {
  return MakeUnexpected(MakeError(ErrorCode::kStorageWriteError,
                                  operation + ": " + std::error_code(error_number, std::generic_category()).message(),
                                  path));
}

bool IsSafeSingleBasename(std::string_view basename) {
  return !basename.empty() && basename != "." && basename != ".." && basename.find('/') == std::string_view::npos &&
         basename.find('\0') == std::string_view::npos && !std::filesystem::path(std::string(basename)).is_absolute();
}

bool WriteAll(int file_desc, std::string_view contents) {
  size_t written = 0;
  while (written < contents.size()) {
    const ssize_t result = write(file_desc, contents.data() + written, contents.size() - written);
    if (result < 0) {
      if (errno == EINTR) {
        continue;
      }
      return false;
    }
    if (result == 0) {
      errno = EIO;
      return false;
    }
    written += static_cast<size_t>(result);
  }
  return true;
}

bool ReadBounded(int file_desc, std::string& contents) {
  std::array<char, 256> buffer{};
  contents.clear();
  for (;;) {
    const ssize_t result = read(file_desc, buffer.data(), buffer.size());
    if (result < 0) {
      if (errno == EINTR) {
        continue;
      }
      return false;
    }
    if (result == 0) {
      return true;
    }
    if (contents.size() + static_cast<size_t>(result) > kMaxDumpTempOwnerMarkerSize) {
      return false;
    }
    contents.append(buffer.data(), static_cast<size_t>(result));
  }
}

bool ReadLine(std::string_view input, size_t& offset, std::string_view& line) {
  const size_t newline = input.find('\n', offset);
  if (newline == std::string_view::npos) {
    return false;
  }
  line = input.substr(offset, newline - offset);
  offset = newline + 1;
  return true;
}

bool ParseUnsigned(std::string_view text, uint64_t& value) {
  if (text.empty()) {
    return false;
  }
  const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), value);
  return error == std::errc{} && end == text.data() + text.size();
}

struct DumpTempOwnerRecord {
  pid_t process_id = 0;
  std::string temp_basename;
  uint64_t device = 0;
  uint64_t inode = 0;
};

bool ParseOwnerMarker(std::string_view marker_basename, std::string_view contents, DumpTempOwnerRecord& record) {
  const std::string_view prefix = mygramdb::utils::kDumpTempOwnerMarkerPrefix;
  if (marker_basename.compare(0, prefix.size(), prefix) != 0) {
    return false;
  }

  const std::string_view suffix = marker_basename.substr(prefix.size());
  const size_t separator = suffix.find('.');
  if (separator == std::string_view::npos || separator == 0 || separator + 1 == suffix.size() ||
      suffix.find('.', separator + 1) != std::string_view::npos) {
    return false;
  }
  uint64_t filename_pid = 0;
  uint64_t nonce = 0;
  if (!ParseUnsigned(suffix.substr(0, separator), filename_pid) || filename_pid == 0 ||
      filename_pid > static_cast<uint64_t>(std::numeric_limits<pid_t>::max()) ||
      !ParseUnsigned(suffix.substr(separator + 1), nonce)) {
    return false;
  }
  (void)nonce;

  size_t offset = 0;
  std::string_view line;
  uint64_t process_id = 0;
  uint64_t device = 0;
  uint64_t inode = 0;
  uint64_t basename_size = 0;
  if (!ReadLine(contents, offset, line) || line != kDumpTempOwnerMagic || !ReadLine(contents, offset, line) ||
      line != kDumpTempOwnerVersion || !ReadLine(contents, offset, line) || !ParseUnsigned(line, process_id) ||
      process_id != filename_pid || process_id == 0 ||
      process_id > static_cast<uint64_t>(std::numeric_limits<pid_t>::max()) || !ReadLine(contents, offset, line) ||
      !ParseUnsigned(line, device) || !ReadLine(contents, offset, line) || !ParseUnsigned(line, inode) ||
      !ReadLine(contents, offset, line) || !ParseUnsigned(line, basename_size) ||
      basename_size != contents.size() - offset) {
    return false;
  }

  const std::string_view temp_basename = contents.substr(offset, static_cast<size_t>(basename_size));
  if (!IsSafeSingleBasename(temp_basename)) {
    return false;
  }
  record.process_id = static_cast<pid_t>(process_id);
  record.temp_basename.assign(temp_basename);
  record.device = device;
  record.inode = inode;
  return true;
}

std::string SerializeOwnerMarker(pid_t process_id, std::string_view temp_basename, uint64_t device, uint64_t inode) {
  std::string contents;
  contents.reserve(sizeof(kDumpTempOwnerMagic) + sizeof(kDumpTempOwnerVersion) + temp_basename.size() + 80);
  contents.append(kDumpTempOwnerMagic);
  contents.push_back('\n');
  contents.append(kDumpTempOwnerVersion);
  contents.push_back('\n');
  contents.append(std::to_string(static_cast<uint64_t>(process_id)));
  contents.push_back('\n');
  contents.append(std::to_string(device));
  contents.push_back('\n');
  contents.append(std::to_string(inode));
  contents.push_back('\n');
  contents.append(std::to_string(temp_basename.size()));
  contents.push_back('\n');
  contents.append(temp_basename);
  return contents;
}

bool IsOldEnough(const struct stat& file_stat, std::chrono::seconds min_age,
                 std::chrono::system_clock::time_point now) {
  const auto modified = std::chrono::system_clock::from_time_t(file_stat.st_mtime);
  return now >= modified && now - modified >= min_age;
}

int OpenDirectory(const std::filesystem::path& directory) {
  const std::string directory_name = directory.empty() ? "." : directory.string();
  return open(directory_name.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
}

Expected<bool, Error> RemoveMarkerAt(int directory_fd, const std::string& marker_basename, uint64_t device,
                                     uint64_t inode) {
  struct stat marker_stat {};
  if (fstatat(directory_fd, marker_basename.c_str(), &marker_stat, AT_SYMLINK_NOFOLLOW) != 0) {
    if (errno == ENOENT) {
      return true;
    }
    return MakeFileError("Failed to inspect dump owner marker", marker_basename, errno);
  }
  if (!S_ISREG(marker_stat.st_mode) || static_cast<uint64_t>(marker_stat.st_dev) != device ||
      static_cast<uint64_t>(marker_stat.st_ino) != inode) {
    return false;
  }
  if (unlinkat(directory_fd, marker_basename.c_str(), 0) != 0 && errno != ENOENT) {
    return MakeFileError("Failed to remove dump owner marker", marker_basename, errno);
  }
  return true;
}

#endif  // !_WIN32

}  // namespace

Expected<void, Error> SyncParentDirectory(const std::string& filepath) {
#ifndef _WIN32
  const auto parent_dir = std::filesystem::path(filepath).parent_path();
  const std::string directory = parent_dir.empty() ? "." : parent_dir.string();
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg) - open() requires varargs
  const int dir_file_desc = open(directory.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
  if (dir_file_desc < 0) {
    const int saved_errno = errno;
    StructuredLog()
        .Event("storage_error")
        .Field("operation", "open_output_directory")
        .Field("filepath", directory)
        .Field("errno", static_cast<int64_t>(saved_errno))
        .Error();
    return MakeUnexpected(MakeError(ErrorCode::kStorageWriteError, "Failed to open output directory", filepath));
  }
  if (fsync(dir_file_desc) != 0) {
    const int saved_errno = errno;
    close(dir_file_desc);
    StructuredLog()
        .Event("storage_error")
        .Field("operation", "fsync_directory")
        .Field("filepath", directory)
        .Field("errno", static_cast<int64_t>(saved_errno))
        .Error();
    return MakeUnexpected(MakeError(ErrorCode::kStorageWriteError, "Failed to fsync output directory", filepath));
  }
  close(dir_file_desc);
#else
  (void)filepath;
#endif
  return {};
}

Expected<bool, Error> RemoveOrphanDumpTempFile(const std::filesystem::path& directory,
                                               const std::string& marker_basename, std::chrono::seconds min_age) {
#ifndef _WIN32
  if (min_age.count() < 0) {
    return MakeUnexpected(MakeError(ErrorCode::kInvalidArgument, "Dump temp minimum age cannot be negative"));
  }
  if (!IsSafeSingleBasename(marker_basename) ||
      marker_basename.compare(0, kDumpTempOwnerMarkerPrefix.size(), kDumpTempOwnerMarkerPrefix) != 0) {
    return false;
  }

  const int directory_fd = OpenDirectory(directory);
  if (directory_fd < 0) {
    return MakeFileError("Failed to open dump directory", directory.string(), errno);
  }
  FDGuard directory_guard(directory_fd);

  const int marker_fd = openat(directory_fd, marker_basename.c_str(), O_RDONLY | O_NONBLOCK | O_NOFOLLOW | O_CLOEXEC);
  if (marker_fd < 0) {
    if (errno == ENOENT || errno == ELOOP) {
      return false;
    }
    return MakeFileError("Failed to open dump owner marker", marker_basename, errno);
  }
  FDGuard marker_guard(marker_fd);

  struct stat marker_stat {};
  if (fstat(marker_fd, &marker_stat) != 0) {
    return MakeFileError("Failed to inspect dump owner marker", marker_basename, errno);
  }
  if (!S_ISREG(marker_stat.st_mode) || marker_stat.st_uid != geteuid() ||
      (marker_stat.st_mode & (S_IRWXU | S_IRWXG | S_IRWXO)) != (S_IRUSR | S_IWUSR) || marker_stat.st_size < 0 ||
      static_cast<uint64_t>(marker_stat.st_size) > kMaxDumpTempOwnerMarkerSize) {
    return false;
  }

  std::string marker_contents;
  if (!ReadBounded(marker_fd, marker_contents)) {
    return false;
  }
  DumpTempOwnerRecord record;
  if (!ParseOwnerMarker(marker_basename, marker_contents, record)) {
    return false;
  }

  const auto now = std::chrono::system_clock::now();
  if (!IsOldEnough(marker_stat, min_age, now)) {
    return false;
  }

  if (kill(record.process_id, 0) == 0) {
    return false;
  }
  if (errno == EPERM) {
    return false;
  }
  if (errno != ESRCH) {
    return MakeFileError("Failed to check dump temp owner process", marker_basename, errno);
  }

  struct stat temp_entry_stat {};
  if (fstatat(directory_fd, record.temp_basename.c_str(), &temp_entry_stat, AT_SYMLINK_NOFOLLOW) != 0) {
    if (errno != ENOENT) {
      if (errno == ELOOP) {
        return false;
      }
      return MakeFileError("Failed to inspect dump temp file", record.temp_basename, errno);
    }
    auto marker_removed = RemoveMarkerAt(directory_fd, marker_basename, static_cast<uint64_t>(marker_stat.st_dev),
                                         static_cast<uint64_t>(marker_stat.st_ino));
    if (!marker_removed) {
      return MakeUnexpected(std::move(marker_removed).error());
    }
    if (!marker_removed.value()) {
      return false;
    }
    if (fsync(directory_fd) != 0) {
      return MakeFileError("Failed to sync dump directory after marker cleanup", directory.string(), errno);
    }
    return true;
  }
  if (!S_ISREG(temp_entry_stat.st_mode)) {
    return false;
  }

  const int temp_fd =
      openat(directory_fd, record.temp_basename.c_str(), O_RDONLY | O_NOFOLLOW | O_CLOEXEC | O_NONBLOCK);
  if (temp_fd < 0) {
    if (errno == ENOENT || errno == ELOOP) {
      return false;
    }
    return MakeFileError("Failed to open dump temp file", record.temp_basename, errno);
  }
  FDGuard temp_guard(temp_fd);

  struct stat opened_temp_stat {};
  if (fstat(temp_fd, &opened_temp_stat) != 0) {
    return MakeFileError("Failed to inspect dump temp file", record.temp_basename, errno);
  }
  if (!S_ISREG(opened_temp_stat.st_mode) || opened_temp_stat.st_dev != temp_entry_stat.st_dev ||
      opened_temp_stat.st_uid != geteuid() || opened_temp_stat.st_ino != temp_entry_stat.st_ino ||
      static_cast<uint64_t>(opened_temp_stat.st_dev) != record.device ||
      static_cast<uint64_t>(opened_temp_stat.st_ino) != record.inode || !IsOldEnough(opened_temp_stat, min_age, now)) {
    return false;
  }

  struct stat current_temp_stat {};
  if (fstatat(directory_fd, record.temp_basename.c_str(), &current_temp_stat, AT_SYMLINK_NOFOLLOW) != 0) {
    if (errno == ENOENT) {
      auto marker_removed = RemoveMarkerAt(directory_fd, marker_basename, static_cast<uint64_t>(marker_stat.st_dev),
                                           static_cast<uint64_t>(marker_stat.st_ino));
      if (!marker_removed) {
        return MakeUnexpected(std::move(marker_removed).error());
      }
      if (!marker_removed.value()) {
        return false;
      }
      if (fsync(directory_fd) != 0) {
        return MakeFileError("Failed to sync dump directory after marker cleanup", directory.string(), errno);
      }
      return true;
    }
    return MakeFileError("Failed to recheck dump temp file", record.temp_basename, errno);
  }
  if (current_temp_stat.st_dev != opened_temp_stat.st_dev || current_temp_stat.st_ino != opened_temp_stat.st_ino ||
      !S_ISREG(current_temp_stat.st_mode)) {
    return false;
  }
  struct stat current_marker_stat {};
  if (fstatat(directory_fd, marker_basename.c_str(), &current_marker_stat, AT_SYMLINK_NOFOLLOW) != 0) {
    if (errno == ENOENT) {
      return false;
    }
    return MakeFileError("Failed to recheck dump owner marker", marker_basename, errno);
  }
  if (!S_ISREG(current_marker_stat.st_mode) || current_marker_stat.st_dev != marker_stat.st_dev ||
      current_marker_stat.st_ino != marker_stat.st_ino || current_marker_stat.st_size != marker_stat.st_size ||
      current_marker_stat.st_mtime != marker_stat.st_mtime) {
    return false;
  }
  if (unlinkat(directory_fd, record.temp_basename.c_str(), 0) != 0 && errno != ENOENT) {
    return MakeFileError("Failed to remove orphan dump temp file", record.temp_basename, errno);
  }

  auto marker_removed = RemoveMarkerAt(directory_fd, marker_basename, static_cast<uint64_t>(marker_stat.st_dev),
                                       static_cast<uint64_t>(marker_stat.st_ino));
  if (!marker_removed) {
    return MakeUnexpected(std::move(marker_removed).error());
  }
  if (!marker_removed.value()) {
    return false;
  }
  if (fsync(directory_fd) != 0) {
    return MakeFileError("Failed to sync dump directory after orphan cleanup", directory.string(), errno);
  }
  return true;
#else
  (void)directory;
  (void)marker_basename;
  (void)min_age;
  return false;
#endif
}

Expected<void, Error> AtomicFileWriter::TrackDumpTempFile(int already_open_fd) {
#ifndef _WIN32
  if (dump_temp_tracked_ || committed_ || rolled_back_) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageWriteError, "Dump temp ownership is already finalized"));
  }
  if (already_open_fd < 0) {
    return MakeUnexpected(MakeError(ErrorCode::kInvalidArgument, "Invalid open dump temp file descriptor"));
  }

  const std::filesystem::path temp_path(temp_filepath_);
  const std::string temp_basename = temp_path.filename().string();
  if (!IsSafeSingleBasename(temp_basename)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageWriteError, "Unsafe dump temp file name", temp_filepath_));
  }
  const std::filesystem::path directory =
      temp_path.parent_path().empty() ? std::filesystem::path(".") : temp_path.parent_path();
  const int directory_fd = OpenDirectory(directory);
  if (directory_fd < 0) {
    return MakeFileError("Failed to open dump directory", directory.string(), errno);
  }
  FDGuard directory_guard(directory_fd);

  struct stat opened_temp_stat {};
  if (fstat(already_open_fd, &opened_temp_stat) != 0) {
    return MakeFileError("Failed to inspect open dump temp file", temp_filepath_, errno);
  }
  if (!S_ISREG(opened_temp_stat.st_mode) || opened_temp_stat.st_uid != geteuid()) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageWriteError,
                                    "Dump temp file is not a regular file owned by this user", temp_filepath_));
  }
  struct stat temp_path_stat {};
  if (fstatat(directory_fd, temp_basename.c_str(), &temp_path_stat, AT_SYMLINK_NOFOLLOW) != 0) {
    return MakeFileError("Failed to verify dump temp path", temp_filepath_, errno);
  }
  if (!S_ISREG(temp_path_stat.st_mode) || temp_path_stat.st_dev != opened_temp_stat.st_dev ||
      temp_path_stat.st_ino != opened_temp_stat.st_ino) {
    return MakeUnexpected(
        MakeError(ErrorCode::kStorageWriteError, "Dump temp path does not match open file", temp_filepath_));
  }

  const pid_t owner_pid = getpid();
  static thread_local std::mt19937_64 rng(std::random_device{}());
  std::uniform_int_distribution<uint64_t> nonce_distribution;
  int marker_fd = -1;
  std::string marker_basename;
  for (size_t attempt = 0; attempt < 16; ++attempt) {
    marker_basename = std::string(kDumpTempOwnerMarkerPrefix) + std::to_string(static_cast<uint64_t>(owner_pid)) + "." +
                      std::to_string(nonce_distribution(rng));
    marker_fd = openat(directory_fd, marker_basename.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC,
                       S_IRUSR | S_IWUSR);
    if (marker_fd >= 0 || errno != EEXIST) {
      break;
    }
  }
  if (marker_fd < 0) {
    return MakeFileError("Failed to create dump owner marker", marker_basename, errno);
  }
  FDGuard marker_guard(marker_fd);

  struct stat marker_stat {};
  if (fstat(marker_fd, &marker_stat) != 0) {
    const int saved_errno = errno;
    unlinkat(directory_fd, marker_basename.c_str(), 0);
    return MakeFileError("Failed to verify dump owner marker", marker_basename, saved_errno);
  }
  if (!S_ISREG(marker_stat.st_mode)) {
    unlinkat(directory_fd, marker_basename.c_str(), 0);
    return MakeUnexpected(
        MakeError(ErrorCode::kStorageWriteError, "Dump owner marker is not regular", marker_basename));
  }
  if (fchmod(marker_fd, S_IRUSR | S_IWUSR) != 0) {
    const int saved_errno = errno;
    unlinkat(directory_fd, marker_basename.c_str(), 0);
    return MakeFileError("Failed to secure dump owner marker", marker_basename, saved_errno);
  }
  const std::string marker_contents =
      SerializeOwnerMarker(owner_pid, temp_basename, static_cast<uint64_t>(opened_temp_stat.st_dev),
                           static_cast<uint64_t>(opened_temp_stat.st_ino));
  if (marker_contents.size() > kMaxDumpTempOwnerMarkerSize || !WriteAll(marker_fd, marker_contents)) {
    const int saved_errno = marker_contents.size() > kMaxDumpTempOwnerMarkerSize ? EFBIG : (errno == 0 ? EIO : errno);
    unlinkat(directory_fd, marker_basename.c_str(), 0);
    return MakeFileError("Failed to write dump owner marker", marker_basename, saved_errno);
  }
  if (fsync(marker_fd) != 0) {
    const int saved_errno = errno;
    unlinkat(directory_fd, marker_basename.c_str(), 0);
    return MakeFileError("Failed to sync dump owner marker", marker_basename, saved_errno);
  }
  marker_guard.Release();
  if (close(marker_fd) != 0) {
    const int saved_errno = errno;
    unlinkat(directory_fd, marker_basename.c_str(), 0);
    return MakeFileError("Failed to close dump owner marker", marker_basename, saved_errno);
  }
  if (fsync(directory_fd) != 0) {
    const int saved_errno = errno;
    unlinkat(directory_fd, marker_basename.c_str(), 0);
    return MakeFileError("Failed to sync dump directory after marker creation", directory.string(), saved_errno);
  }

  const std::filesystem::path marker_path = directory / marker_basename;
  owner_marker_filepath_ = marker_path.string();
  tracked_temp_device_ = static_cast<uint64_t>(opened_temp_stat.st_dev);
  tracked_temp_inode_ = static_cast<uint64_t>(opened_temp_stat.st_ino);
  owner_marker_device_ = static_cast<uint64_t>(marker_stat.st_dev);
  owner_marker_inode_ = static_cast<uint64_t>(marker_stat.st_ino);
  dump_temp_tracked_ = true;
  return {};
#else
  (void)already_open_fd;
  return MakeUnexpected(MakeError(ErrorCode::kNotImplemented, "Dump temp ownership markers are unavailable"));
#endif
}

AtomicFileWriter::AtomicFileWriter(std::string filepath, bool unique_suffix) : filepath_(std::move(filepath)) {
  if (unique_suffix) {
    static thread_local std::mt19937 rng(std::random_device{}());
    std::uniform_int_distribution<uint32_t> dist(0, UINT32_MAX);
    std::ostringstream oss;
    oss << filepath_ << ".tmp." << getpid() << "." << dist(rng);
    temp_filepath_ = oss.str();
  } else {
    temp_filepath_ = filepath_ + ".tmp";
  }
}

AtomicFileWriter::~AtomicFileWriter() {
  if (!committed_ && !rolled_back_) {
    Rollback();
  }
}

Expected<void, Error> AtomicFileWriter::Commit() {
  if (committed_) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageWriteError, "Already committed"));
  }

  if (!std::filesystem::exists(temp_filepath_)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageFileNotFound, "Temp file does not exist"));
  }

#ifndef _WIN32
  // Fsync the temp file to ensure data is durable before rename
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg) - open() requires varargs
  int file_desc = open(temp_filepath_.c_str(), O_RDWR | O_NOFOLLOW | O_CLOEXEC);
  if (file_desc < 0) {
    StructuredLog()
        .Event("storage_error")
        .Field("operation", "open_temp_file")
        .Field("filepath", temp_filepath_)
        .Field("errno", static_cast<int64_t>(errno))
        .Error();
    Rollback();
    return MakeUnexpected(MakeError(ErrorCode::kStorageWriteError, "Failed to safely open temp file", temp_filepath_));
  }

  struct stat file_stat {};
  if (fstat(file_desc, &file_stat) != 0 || !S_ISREG(file_stat.st_mode)) {
    const int saved_errno = errno;
    close(file_desc);
    StructuredLog()
        .Event("storage_error")
        .Field("operation", "validate_temp_file")
        .Field("filepath", temp_filepath_)
        .Field("errno", static_cast<int64_t>(saved_errno))
        .Error();
    Rollback();
    return MakeUnexpected(MakeError(ErrorCode::kStorageWriteError, "Temp file is not a regular file", temp_filepath_));
  }

  if (dump_temp_tracked_ && (static_cast<uint64_t>(file_stat.st_dev) != tracked_temp_device_ ||
                             static_cast<uint64_t>(file_stat.st_ino) != tracked_temp_inode_)) {
    close(file_desc);
    return MakeUnexpected(
        MakeError(ErrorCode::kStorageWriteError, "Temp file no longer matches its dump owner marker", temp_filepath_));
  }

  if (fsync(file_desc) != 0) {
    const int saved_errno = errno;
    close(file_desc);
    StructuredLog()
        .Event("storage_error")
        .Field("operation", "fsync_temp_file")
        .Field("filepath", temp_filepath_)
        .Field("errno", static_cast<int64_t>(saved_errno))
        .Error();
    Rollback();
    return MakeUnexpected(MakeError(ErrorCode::kStorageWriteError, "Failed to fsync temp file", temp_filepath_));
  }
  close(file_desc);
#endif

  // Atomic rename: temp file -> final path
  std::error_code rename_error;
  std::filesystem::rename(temp_filepath_, filepath_, rename_error);
  if (rename_error) {
    StructuredLog()
        .Event("storage_error")
        .Field("operation", "atomic_rename")
        .Field("filepath", filepath_)
        .Field("error", rename_error.message())
        .Error();
    Rollback();
    return MakeUnexpected(
        MakeError(ErrorCode::kStorageWriteError, "Failed to rename temp file: " + rename_error.message(), filepath_));
  }

  committed_ = true;

  std::optional<Error> marker_cleanup_error;
  if (dump_temp_tracked_) {
    const std::filesystem::path marker_path(owner_marker_filepath_);
    const std::filesystem::path parent =
        marker_path.parent_path().empty() ? std::filesystem::path(".") : marker_path.parent_path();
    const int directory_fd = OpenDirectory(parent);
    if (directory_fd < 0) {
      marker_cleanup_error =
          MakeError(ErrorCode::kStorageWriteError, "Failed to open dump directory for marker cleanup", parent.string());
    } else {
      FDGuard directory_guard(directory_fd);
      auto marker_removed =
          RemoveMarkerAt(directory_fd, marker_path.filename().string(), owner_marker_device_, owner_marker_inode_);
      if (!marker_removed) {
        marker_cleanup_error = std::move(marker_removed).error();
      } else if (!marker_removed.value()) {
        marker_cleanup_error = MakeError(ErrorCode::kStorageWriteError,
                                         "Dump owner marker changed before commit cleanup", owner_marker_filepath_);
      }
    }
  }

  if (auto sync_result = SyncParentDirectory(filepath_); !sync_result) {
    return sync_result;
  }
  if (marker_cleanup_error.has_value()) {
    return MakeUnexpected(std::move(*marker_cleanup_error));
  }

  return {};
}

void AtomicFileWriter::Rollback() {
  if (!committed_ && !rolled_back_) {
    if (!dump_temp_tracked_) {
      std::error_code ec;
      std::filesystem::remove(temp_filepath_, ec);
    } else {
#ifndef _WIN32
      const std::filesystem::path temp_path(temp_filepath_);
      const std::filesystem::path directory =
          temp_path.parent_path().empty() ? std::filesystem::path(".") : temp_path.parent_path();
      const std::string temp_basename = temp_path.filename().string();
      const std::filesystem::path marker_path(owner_marker_filepath_);
      const int directory_fd = OpenDirectory(directory);
      if (directory_fd >= 0) {
        FDGuard directory_guard(directory_fd);
        struct stat temp_stat {};
        bool temp_removed = false;
        if (fstatat(directory_fd, temp_basename.c_str(), &temp_stat, AT_SYMLINK_NOFOLLOW) != 0) {
          temp_removed = errno == ENOENT;
        } else if (S_ISREG(temp_stat.st_mode) && static_cast<uint64_t>(temp_stat.st_dev) == tracked_temp_device_ &&
                   static_cast<uint64_t>(temp_stat.st_ino) == tracked_temp_inode_) {
          struct stat current_temp_stat {};
          if (fstatat(directory_fd, temp_basename.c_str(), &current_temp_stat, AT_SYMLINK_NOFOLLOW) == 0 &&
              current_temp_stat.st_dev == temp_stat.st_dev && current_temp_stat.st_ino == temp_stat.st_ino &&
              S_ISREG(current_temp_stat.st_mode)) {
            temp_removed = unlinkat(directory_fd, temp_basename.c_str(), 0) == 0 || errno == ENOENT;
          }
        }
        if (temp_removed) {
          auto marker_removed =
              RemoveMarkerAt(directory_fd, marker_path.filename().string(), owner_marker_device_, owner_marker_inode_);
          if (marker_removed && marker_removed.value()) {
            (void)fsync(directory_fd);
          }
        }
      }
#endif
    }
    rolled_back_ = true;
  }
}

}  // namespace mygramdb::utils
