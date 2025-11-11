/**
 * @file snapshot.cpp
 * @brief Snapshot implementation
 */

#include "storage/snapshot.h"

#include <spdlog/spdlog.h>

#include <utility>

namespace mygramdb {
namespace storage {

SnapshotManager::SnapshotManager(std::string snapshot_dir)
    : snapshot_dir_(std::move(snapshot_dir)) {
  spdlog::info("Snapshot manager initialized: {}", snapshot_dir_);
}

bool SnapshotManager::Save(const std::string& name) {
  // Future feature: mmap-based snapshot save for faster recovery
  // Currently using SnapshotBuilder for incremental snapshot building
  spdlog::info("Saving snapshot: {} to {} (not yet implemented)", name, snapshot_dir_);
  return false;
}

bool SnapshotManager::Load(const std::string& name) {
  // Future feature: mmap-based snapshot load for faster startup
  // Currently using SnapshotBuilder for snapshot management
  spdlog::info("Loading snapshot: {} from {} (not yet implemented)", name, snapshot_dir_);
  return false;
}

}  // namespace storage
}  // namespace mygramdb
