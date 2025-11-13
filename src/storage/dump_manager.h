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
   * @param name Dump name
   * @return true if successful
   */
  bool Save(const std::string& name);

  /**
   * @brief Load dump from disk
   *
   * @param name Dump name
   * @return true if successful
   */
  bool Load(const std::string& name);

 private:
  std::string dump_dir_;
};

}  // namespace mygramdb::storage
