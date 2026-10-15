/**
 * @file atomic_file_writer.h
 * @brief RAII helper for atomic file writes via temp-file + fsync + rename
 */

#ifndef MYGRAMDB_UTILS_ATOMIC_FILE_WRITER_H_
#define MYGRAMDB_UTILS_ATOMIC_FILE_WRITER_H_

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <string>
#include <string_view>

#include "utils/error.h"
#include "utils/expected.h"
#include "utils/namespace_compat.h"

namespace mygramdb::utils {

/// Prefix used to discover dump temp ownership markers during startup cleanup.
inline constexpr std::string_view kDumpTempOwnerMarkerPrefix = ".mygramdb-dump-owner.";

/**
 * @brief Remove one abandoned, validated dump temp and its owner marker.
 *
 * Only markers created by AtomicFileWriter::TrackDumpTempFile are accepted.
 * The temp file is removed only when its directory entry still identifies the
 * recorded regular file, the marker and temp are at least min_age old, and the
 * recorded owner process no longer exists.
 *
 * @return true if cleanup removed an artifact, false if it was retained or
 *         already absent, or an error if a filesystem operation failed.
 */
Expected<bool, Error> RemoveOrphanDumpTempFile(const std::filesystem::path& directory,
                                               const std::string& marker_basename, std::chrono::seconds min_age);

/**
 * @brief Fsync the parent directory of a committed path.
 *
 * Exposed separately so callers and tests can verify that inability to open
 * the directory is reported instead of silently weakening durability.
 */
Expected<void, Error> SyncParentDirectory(const std::string& filepath);

/**
 * @brief RAII helper for atomic file writes via temp-file + fsync + rename.
 *
 * Usage:
 * @code
 *   AtomicFileWriter writer(final_path);
 *   const auto& temp = writer.GetTempPath();
 *   // ... write to temp ...
 *   writer.Commit();  // fsync + rename + dir fsync
 *   // destructor cleans up temp file on failure
 * @endcode
 */
class AtomicFileWriter {
 public:
  /**
   * @brief Construct an atomic file writer.
   * @param filepath Final destination path
   * @param unique_suffix Use PID+random suffix for concurrent safety (default: false, uses ".tmp")
   */
  explicit AtomicFileWriter(std::string filepath, bool unique_suffix = false);

  ~AtomicFileWriter();

  // Non-copyable, non-movable
  AtomicFileWriter(const AtomicFileWriter&) = delete;
  AtomicFileWriter& operator=(const AtomicFileWriter&) = delete;
  AtomicFileWriter(AtomicFileWriter&&) = delete;
  AtomicFileWriter& operator=(AtomicFileWriter&&) = delete;

  /// @brief Get the temp file path to write to.
  /// @return Temp file path
  [[nodiscard]] const std::string& GetTempPath() const { return temp_filepath_; }

  /**
   * @brief Record ownership of an already-open dump temp file for crash cleanup.
   *
   * The descriptor must refer to the regular temp file created by this writer.
   * A durable owner marker is created before the caller writes the dump bytes.
   */
  Expected<void, Error> TrackDumpTempFile(int already_open_fd);

  /// @brief Fsync the temp file, atomically rename to final path, fsync directory.
  /// @return Success or error
  ///
  /// @note The caller MUST close (or flush+sync) their write stream before calling Commit().
  /// Commit() opens the temp file read-only for fsync, which only guarantees durability
  /// if all prior writes have been flushed to the OS by the caller.
  Expected<void, Error> Commit();

  /// @brief Remove the temp file without renaming.
  void Rollback();

 private:
  std::string filepath_;
  std::string temp_filepath_;
  std::string owner_marker_filepath_;
  uint64_t tracked_temp_device_ = 0;
  uint64_t tracked_temp_inode_ = 0;
  uint64_t owner_marker_device_ = 0;
  uint64_t owner_marker_inode_ = 0;
  bool dump_temp_tracked_ = false;
  bool committed_ = false;
  bool rolled_back_ = false;
};

}  // namespace mygramdb::utils

#endif  // MYGRAMDB_UTILS_ATOMIC_FILE_WRITER_H_
