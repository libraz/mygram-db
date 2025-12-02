/**
 * @file dump_manager.cpp
 * @brief Dump implementation
 */

#include "storage/dump_manager.h"

#include <spdlog/spdlog.h>

#include <utility>

#include "utils/structured_log.h"

namespace mygramdb::storage {

DumpManager::DumpManager(std::string dump_dir) : dump_dir_(std::move(dump_dir)) {
  mygram::utils::StructuredLog().Event("dump_manager_initialized").Field("path", dump_dir_).Info();
}

bool DumpManager::Save(const std::string& name) {
  // Future feature: mmap-based dump save for faster recovery
  // Currently using InitialLoader for incremental dump building
  mygram::utils::StructuredLog().Event("dump_save_not_implemented").Field("name", name).Field("path", dump_dir_).Info();
  return false;
}

bool DumpManager::Load(const std::string& name) {
  // Future feature: mmap-based dump load for faster startup
  // Currently using InitialLoader for dump management
  mygram::utils::StructuredLog().Event("dump_load_not_implemented").Field("name", name).Field("path", dump_dir_).Info();
  return false;
}

}  // namespace mygramdb::storage
