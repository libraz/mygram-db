/**
 * @file dump_manager.cpp
 * @brief Dump implementation
 */

#include "storage/dump_manager.h"

#include <spdlog/spdlog.h>

#include <utility>

namespace mygramdb::storage {

DumpManager::DumpManager(std::string dump_dir) : dump_dir_(std::move(dump_dir)) {
  spdlog::info("Dump manager initialized: {}", dump_dir_);
}

bool DumpManager::Save(const std::string& name) {
  // Future feature: mmap-based dump save for faster recovery
  // Currently using InitialLoader for incremental dump building
  spdlog::info("Saving dump: {} to {} (not yet implemented)", name, dump_dir_);
  return false;
}

bool DumpManager::Load(const std::string& name) {
  // Future feature: mmap-based dump load for faster startup
  // Currently using InitialLoader for dump management
  spdlog::info("Loading dump: {} from {} (not yet implemented)", name, dump_dir_);
  return false;
}

}  // namespace mygramdb::storage
