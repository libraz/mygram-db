/**
 * @file gtid_state.cpp
 * @brief GTID state persistence for binlog replication
 */

#include "storage/gtid_state.h"

#include <spdlog/spdlog.h>

#include <cstdio>  // for std::rename
#include <filesystem>
#include <fstream>
#include <utility>

#ifdef USE_MYSQL

namespace mygramdb {
namespace storage {

GTIDStateFile::GTIDStateFile(std::string file_path) : file_path_(std::move(file_path)) {}

std::optional<std::string> GTIDStateFile::Read() const {
  if (!Exists()) {
    spdlog::debug("GTID state file does not exist: {}", file_path_);
    return std::nullopt;
  }

  std::ifstream file(file_path_);
  if (!file.is_open()) {
    spdlog::error("Failed to open GTID state file for reading: {}", file_path_);
    return std::nullopt;
  }

  std::string gtid;
  if (!std::getline(file, gtid)) {
    spdlog::error("Failed to read GTID from state file: {}", file_path_);
    return std::nullopt;
  }

  // Trim whitespace
  gtid.erase(0, gtid.find_first_not_of(" \t\r\n"));
  gtid.erase(gtid.find_last_not_of(" \t\r\n") + 1);

  if (gtid.empty()) {
    spdlog::warn("GTID state file is empty: {}", file_path_);
    return std::nullopt;
  }

  spdlog::info("Read GTID from state file: {}", gtid);
  return gtid;
}

bool GTIDStateFile::Write(const std::string& gtid) {
  if (gtid.empty()) {
    spdlog::error("Cannot write empty GTID to state file");
    return false;
  }

  // Write to temporary file first for atomic operation
  std::string temp_path = file_path_ + ".tmp";

  try {
    // Ensure parent directory exists
    std::filesystem::path file_path_obj(file_path_);
    if (file_path_obj.has_parent_path()) {
      std::filesystem::create_directories(file_path_obj.parent_path());
    }

    // Write to temporary file
    std::ofstream temp_file(temp_path, std::ios::trunc);
    if (!temp_file.is_open()) {
      spdlog::error("Failed to open temporary GTID state file for writing: {}", temp_path);
      return false;
    }

    temp_file << gtid << '\n';
    temp_file.flush();

    if (!temp_file.good()) {
      spdlog::error("Failed to write GTID to temporary file: {}", temp_path);
      temp_file.close();
      std::filesystem::remove(temp_path);
      return false;
    }

    temp_file.close();

    // Atomic rename
    if (std::rename(temp_path.c_str(), file_path_.c_str()) != 0) {
      spdlog::error("Failed to rename temporary GTID state file: {} -> {}",
                   temp_path, file_path_);
      std::filesystem::remove(temp_path);
      return false;
    }

    spdlog::debug("Wrote GTID to state file: {}", gtid);
    return true;

  } catch (const std::exception& e) {
    spdlog::error("Exception while writing GTID state file: {}", e.what());
    // Clean up temporary file if it exists
    try {
      if (std::filesystem::exists(temp_path)) {
        std::filesystem::remove(temp_path);
      }
    } catch (...) {
      // Ignore cleanup errors
    }
    return false;
  }
}

bool GTIDStateFile::Exists() const {
  try {
    return std::filesystem::exists(file_path_);
  } catch (const std::exception& e) {
    spdlog::error("Exception while checking GTID state file existence: {}", e.what());
    return false;
  }
}

bool GTIDStateFile::Delete() {
  try {
    if (!Exists()) {
      return true;  // Already deleted
    }

    if (std::filesystem::remove(file_path_)) {
      spdlog::info("Deleted GTID state file: {}", file_path_);
      return true;
    }
    spdlog::error("Failed to delete GTID state file: {}", file_path_);
    return false;

  } catch (const std::exception& e) {
    spdlog::error("Exception while deleting GTID state file: {}", e.what());
    return false;
  }
}

}  // namespace storage
}  // namespace mygramdb

#endif  // USE_MYSQL
