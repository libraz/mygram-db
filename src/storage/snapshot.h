/**
 * @file snapshot.h
 * @brief Snapshot save/restore functionality
 */

#pragma once

#include <string>

namespace mygramdb {
namespace storage {

/**
 * @brief Snapshot manager
 *
 * Handles saving and loading index snapshots to/from disk
 */
class SnapshotManager {
 public:
  explicit SnapshotManager(const std::string& snapshot_dir);
  ~SnapshotManager() = default;

  /**
   * @brief Save snapshot to disk
   *
   * @param name Snapshot name
   * @return true if successful
   */
  bool Save(const std::string& name);

  /**
   * @brief Load snapshot from disk
   *
   * @param name Snapshot name
   * @return true if successful
   */
  bool Load(const std::string& name);

 private:
  std::string snapshot_dir_;
};

}  // namespace storage
}  // namespace mygramdb
