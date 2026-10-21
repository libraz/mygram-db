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
 * Only markers created by AtomicFileWriter::CreateDumpTempFile are accepted.
 * The temp file is removed only when its directory entry still identifies the
 * recorded regular file (or, for a marker written before the temp existed, a
 * regular file of this user under the recorded name), the marker and temp are
 * at least min_age old, and no live writer owns it: the recorded process no
 * longer exists, or it is this process and none of its writers holds the temp.
 *
 * @return true if cleanup removed an artifact, false if it was retained or
 *         already absent, or an error if a filesystem operation failed.
 */
Expected<bool, Error> RemoveOrphanDumpTempFile(const std::filesystem::path& directory,
                                               const std::string& marker_basename, std::chrono::seconds min_age);

/**
 * @brief Reclaim abandoned dump temps under directory and its subdirectories.
 *
 * Applies RemoveOrphanDumpTempFile to every owner marker found. Files without
 * an owner marker are never removed. Failures are logged, not returned.
 */
void SweepOrphanDumpTempFiles(const std::filesystem::path& directory, std::chrono::seconds min_age);

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
   * @brief Create the dump temp file together with its durable owner marker.
   *
   * The marker naming the temp is synced before the temp is created, so a
   * crash at any point leaves a temp the startup sweep can classify.
   *
   * @return Descriptor of the new regular temp file, owned by the caller.
   */
  Expected<int, Error> CreateDumpTempFile();

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
  std::string live_temp_basename_;
  bool dump_temp_tracked_ = false;
  bool committed_ = false;
  bool rolled_back_ = false;
};

}  // namespace mygramdb::utils

#endif  // MYGRAMDB_UTILS_ATOMIC_FILE_WRITER_H_
