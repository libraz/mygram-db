/**
 * @file dump_manager.h
 * @brief Dump save/restore functionality
 */

#pragma once

#include <string>

namespace mygramdb::storage {

/**
 * @brief Dump manager
 *
 * Handles saving and loading index dumps to/from disk
 */
class DumpManager {
 public:
  explicit DumpManager(std::string dump_dir);

  /**
   * @brief Save dump to disk
   *
   * @note Placeholder stub. Currently returns false and logs "not implemented".
   *       Future feature: mmap-based dump save for faster recovery.
   *
   * @param name Dump name
   * @return true if successful (currently always false)
   */
  bool Save(const std::string& name);

  /**
   * @brief Load dump from disk
   *
   * @note Placeholder stub. Currently returns false and logs "not implemented".
   *       Future feature: mmap-based dump load for faster startup.
   *
   * @param name Dump name
   * @return true if successful (currently always false)
   */
  bool Load(const std::string& name);

 private:
  std::string dump_dir_;
};

}  // namespace mygramdb::storage
