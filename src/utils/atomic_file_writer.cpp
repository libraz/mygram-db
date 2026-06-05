/**
 * @file atomic_file_writer.cpp
 * @brief Implementation of AtomicFileWriter for durable atomic file writes
 */

#include "utils/atomic_file_writer.h"

#include <cerrno>
#include <cstdint>
#include <filesystem>
#include <random>
#include <sstream>
#include <string>

#ifndef _WIN32
#include <fcntl.h>
#include <sys/types.h>
#include <unistd.h>
#endif

#include "utils/structured_log.h"

namespace mygramdb::utils {

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
  int file_desc = open(temp_filepath_.c_str(), O_RDWR);
  if (file_desc >= 0) {
    if (fsync(file_desc) != 0) {
      StructuredLog()
          .Event("storage_warning")
          .Field("operation", "fsync_temp_file")
          .Field("filepath", temp_filepath_)
          .Field("errno", static_cast<int64_t>(errno))
          .Warn();
    }
    close(file_desc);
  }
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

#ifndef _WIN32
  // Sync the directory to ensure the rename is durable
  {
    auto parent_dir = std::filesystem::path(filepath_).parent_path();
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg) - open() requires varargs
    int dir_file_desc = open(parent_dir.empty() ? "." : parent_dir.c_str(), O_RDONLY | O_DIRECTORY);
    if (dir_file_desc >= 0) {
      if (fsync(dir_file_desc) != 0) {
        StructuredLog()
            .Event("storage_warning")
            .Field("operation", "fsync_directory")
            .Field("filepath", parent_dir.empty() ? "." : parent_dir.string())
            .Field("errno", static_cast<int64_t>(errno))
            .Warn();
      }
      close(dir_file_desc);
    }
  }
#endif

  return {};
}

void AtomicFileWriter::Rollback() {
  if (!committed_ && !rolled_back_) {
    std::error_code ec;
    std::filesystem::remove(temp_filepath_, ec);
    rolled_back_ = true;
  }
}

}  // namespace mygramdb::utils
