/**
 * @file table_catalog.cpp
 * @brief Implementation of TableCatalog
 */

#include "server/table_catalog.h"

#include <spdlog/spdlog.h>

#include "utils/structured_log.h"

namespace mygramdb::server {

TableCatalog::TableCatalog(std::unordered_map<std::string, TableContext*> tables) : tables_(std::move(tables)) {
  mygram::utils::StructuredLog()
      .Event("table_catalog_initialized")
      .Field("table_count", static_cast<uint64_t>(tables_.size()))
      .Debug();
}

TableContext* TableCatalog::GetTable(const std::string& name) {
  std::shared_lock lock(mutex_);
  auto iter = tables_.find(name);
  return iter != tables_.end() ? iter->second : nullptr;
}

bool TableCatalog::TableExists(const std::string& name) const {
  std::shared_lock lock(mutex_);
  return tables_.find(name) != tables_.end();
}

std::vector<std::string> TableCatalog::GetTableNames() const {
  std::shared_lock lock(mutex_);
  std::vector<std::string> names;
  names.reserve(tables_.size());
  for (const auto& [name, _] : tables_) {
    names.push_back(name);
  }
  return names;
}

std::unordered_map<std::string, std::pair<index::Index*, storage::DocumentStore*>> TableCatalog::GetDumpableContexts()
    const {
  std::shared_lock lock(mutex_);
  std::unordered_map<std::string, std::pair<index::Index*, storage::DocumentStore*>> result;
  for (const auto& [table_name, table_ctx] : tables_) {
    result[table_name] = {table_ctx->index.get(), table_ctx->doc_store.get()};
  }
  return result;
}

void TableCatalog::SetReadOnly(bool read_only) {
  read_only_ = read_only;
  mygram::utils::StructuredLog().Event("table_catalog_read_only_changed").Field("read_only", read_only).Info();
}

void TableCatalog::SetLoading(bool loading) {
  loading_ = loading;
  mygram::utils::StructuredLog().Event("table_catalog_loading_changed").Field("loading", loading).Info();
}

}  // namespace mygramdb::server
