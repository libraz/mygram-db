/**
 * @file table_metadata.cpp
 * @brief Table metadata cache implementation
 */

#include "mysql/table_metadata.h"

#ifdef USE_MYSQL

#include <algorithm>

namespace mygramdb::mysql {

void TableMetadata::RebuildColumnOrdinals() {
  column_ordinals.clear();
  column_ordinals.reserve(columns.size());
  for (size_t index = 0; index < columns.size(); ++index) {
    column_ordinals.emplace(columns[index].name, index);
  }
}

std::optional<size_t> TableMetadata::FindColumnOrdinal(std::string_view name) const {
  if (column_ordinals.size() == columns.size()) {
    const auto iterator = column_ordinals.find(std::string(name));
    if (iterator != column_ordinals.end()) {
      return iterator->second;
    }
    return std::nullopt;
  }
  for (size_t index = 0; index < columns.size(); ++index) {
    if (columns[index].name == name) {
      return index;
    }
  }
  return std::nullopt;
}

TableMetadataCache::AddResult TableMetadataCache::AddOrUpdate(uint64_t table_id, const TableMetadata& metadata) {
  auto iterator = cache_.find(table_id);
  if (iterator == cache_.end()) {
    // TABLE_MAP ids are connection/session scoped and may change after a
    // reconnect or table reopen. Keep only the newest mapping for a logical
    // table so stale ids cannot accumulate for the lifetime of the process.
    auto same_table = std::find_if(cache_.begin(), cache_.end(), [&metadata](const auto& entry) {
      return entry.second.database_name == metadata.database_name && entry.second.table_name == metadata.table_name;
    });
    if (same_table != cache_.end()) {
      const bool schema_changed = !SchemaEquals(same_table->second, metadata);
      cache_.erase(same_table);
      cache_[table_id] = metadata;
      return schema_changed ? AddResult::kSchemaChanged : AddResult::kUpdated;
    }
    cache_[table_id] = metadata;
    return AddResult::kAdded;
  }

  // Check if schema changed
  if (!SchemaEquals(iterator->second, metadata)) {
    cache_[table_id] = metadata;
    return AddResult::kSchemaChanged;
  }

  // Update without schema change (might update other fields)
  cache_[table_id] = metadata;
  return AddResult::kUpdated;
}

void TableMetadataCache::Add(uint64_t table_id, const TableMetadata& metadata) {
  AddOrUpdate(table_id, metadata);
}

const TableMetadata* TableMetadataCache::Get(uint64_t table_id) const {
  auto iterator = cache_.find(table_id);
  if (iterator != cache_.end()) {
    return &iterator->second;
  }
  return nullptr;
}

void TableMetadataCache::Remove(uint64_t table_id) {
  cache_.erase(table_id);
}

void TableMetadataCache::Clear() {
  cache_.clear();
}

bool TableMetadataCache::Contains(uint64_t table_id) const {
  return cache_.find(table_id) != cache_.end();
}

bool TableMetadataCache::SchemaEquals(const TableMetadata& lhs, const TableMetadata& rhs) {
  // Different column count means schema changed
  if (lhs.columns.size() != rhs.columns.size()) {
    return false;
  }

  // Check each column
  for (size_t idx = 0; idx < lhs.columns.size(); ++idx) {
    const auto& col_lhs = lhs.columns[idx];
    const auto& col_rhs = rhs.columns[idx];

    // Check type
    if (col_lhs.type != col_rhs.type) {
      return false;
    }

    // Check metadata (affects parsing)
    if (col_lhs.metadata != col_rhs.metadata) {
      return false;
    }

    // Signedness decides how the row decoder reads an integer's bytes, and a
    // TABLE_MAP carries the same type code either way. An ALTER that only
    // changes it must still be reported as a schema change.
    if (col_lhs.is_unsigned != col_rhs.is_unsigned) {
      return false;
    }

    // Check name (might change after ALTER TABLE)
    if (col_lhs.name != col_rhs.name) {
      return false;
    }

    if (col_lhs.enum_set_values != col_rhs.enum_set_values) {
      return false;
    }
  }

  return true;
}

}  // namespace mygramdb::mysql

#endif  // USE_MYSQL
