/**
 * @file table_catalog.cpp
 * @brief Implementation of TableCatalog
 */

#include "server/table_catalog.h"

#include <spdlog/spdlog.h>

namespace mygramdb::server {

TableCatalog::TableCatalog(std::unordered_map<std::string, TableContext*> tables) : tables_(std::move(tables)) {
  spdlog::info("TableCatalog initialized with {} tables", tables_.size());
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
  spdlog::info("TableCatalog: read_only={}", read_only);
}

void TableCatalog::SetLoading(bool loading) {
  loading_ = loading;
  spdlog::info("TableCatalog: loading={}", loading);
}

}  // namespace mygramdb::server
