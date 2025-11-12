/**
 * @file gtid_state.h
 * @brief GTID state persistence for binlog replication
 */

#pragma once

#ifdef USE_MYSQL

#include <optional>
#include <string>

namespace mygramdb::storage {

/**
 * @brief GTID state file manager
 *
 * Persists the current GTID position to a file for crash recovery.
 * Uses atomic write (write to temp file, then rename) to ensure
 * file integrity.
 */
class GTIDStateFile {
 public:
  /**
   * @brief Constructor
   *
   * @param file_path Path to the state file
   */
  explicit GTIDStateFile(std::string file_path);

  /**
   * @brief Read GTID from state file
   *
   * @return GTID string if file exists and is valid, nullopt otherwise
   */
  [[nodiscard]] std::optional<std::string> Read() const;

  /**
   * @brief Write GTID to state file (atomic)
   *
   * Writes to a temporary file first, then atomically renames it
   * to the actual state file path.
   *
   * @param gtid GTID string to write
   * @return true if write was successful, false otherwise
   */
  bool Write(const std::string& gtid);

  /**
   * @brief Check if state file exists
   *
   * @return true if file exists, false otherwise
   */
  [[nodiscard]] bool Exists() const;

  /**
   * @brief Delete state file
   *
   * @return true if deletion was successful, false otherwise
   */
  bool Delete();

 private:
  std::string file_path_;
};

}  // namespace mygramdb::storage

#endif  // USE_MYSQL
