/**
 * @file atomic_file_writer.h
 * @brief RAII helper for atomic file writes via temp-file + fsync + rename
 */

#ifndef MYGRAMDB_UTILS_ATOMIC_FILE_WRITER_H_
#define MYGRAMDB_UTILS_ATOMIC_FILE_WRITER_H_

#include <string>

#include "utils/error.h"
#include "utils/expected.h"

namespace mygram::utils {

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

  /// @brief Fsync the temp file, atomically rename to final path, fsync directory.
  /// @return Success or error
  Expected<void, Error> Commit();

  /// @brief Remove the temp file without renaming.
  void Rollback();

 private:
  std::string filepath_;
  std::string temp_filepath_;
  bool committed_ = false;
};

}  // namespace mygram::utils

#endif  // MYGRAMDB_UTILS_ATOMIC_FILE_WRITER_H_
