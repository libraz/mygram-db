/**
 * @file table_catalog.cpp
 * @brief Implementation of TableCatalog
 */

#include "server/table_catalog.h"

#include "utils/structured_log.h"

namespace mygramdb::server {

std::optional<std::string> ResolveTableKey(const std::unordered_map<std::string, TableContext*>& tables,
                                           std::string_view name) {
  if (name.find('.') != std::string_view::npos) {
    // Exact match covers already-qualified references and literal table names
    // that contain dots.
    if (tables.find(std::string(name)) != tables.end()) {
      return std::string(name);
    }
    return std::nullopt;
  }

  // Bare name: prefer a unique canonical key of the form "<db>.<name>" even
  // if a legacy bare alias exists in the map. Cache invalidation and binlog
  // events use qualified names, so returning the qualified key keeps query
  // metadata byte-identical with mutation events.
  std::optional<std::string> resolved;
  for (const auto& [key, _] : tables) {
    const auto separator = key.find('.');
    if (separator == std::string::npos) {
      continue;
    }
    const std::string_view table_part(key.data() + separator + 1, key.size() - separator - 1);
    if (table_part == name) {
      if (resolved.has_value()) {
        return std::nullopt;  // Ambiguous: same bare name in multiple databases.
      }
      resolved = key;
    }
  }
  if (resolved.has_value()) {
    return resolved;
  }

  // Legacy fallback for tests and single-table callers that only provide a
  // bare key.
  if (tables.find(std::string(name)) != tables.end()) {
    return std::string(name);
  }
  return std::nullopt;
}

TableCatalog::TableCatalog(std::unordered_map<std::string, TableContext*> tables) : tables_(std::move(tables)) {
  mygram::utils::StructuredLog()
      .Event("table_catalog_initialized")
      .Field("table_count", static_cast<uint64_t>(tables_.size()))
      .Debug();
}

// All accessors below are lock-free: tables_ is immutable post-construction.
// See TableCatalog class-level Doxygen.

TableContext* TableCatalog::GetTable(const std::string& name) {
  auto resolved = ResolveTableKey(tables_, name);
  if (!resolved.has_value()) {
    return nullptr;
  }
  auto iter = tables_.find(*resolved);
  return iter != tables_.end() ? iter->second : nullptr;
}

const TableContext* TableCatalog::GetTable(const std::string& name) const {
  auto resolved = ResolveTableKey(tables_, name);
  if (!resolved.has_value()) {
    return nullptr;
  }
  auto iter = tables_.find(*resolved);
  return iter != tables_.end() ? iter->second : nullptr;
}

bool TableCatalog::TableExists(const std::string& name) const {
  return ResolveTableKey(tables_, name).has_value();
}

std::optional<std::string> TableCatalog::ResolveName(const std::string& name) const {
  return ResolveTableKey(tables_, name);
}

std::vector<std::string> TableCatalog::GetTableNames() const {
  std::vector<std::string> names;
  names.reserve(tables_.size());
  for (const auto& [name, _] : tables_) {
    names.push_back(name);
  }
  return names;
}

std::unordered_map<std::string, std::pair<index::Index*, storage::DocumentStore*>> TableCatalog::GetDumpableContexts()
    const {
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
