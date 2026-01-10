/**
 * @file table_metadata.cpp
 * @brief Table metadata cache implementation
 */

#include "mysql/table_metadata.h"

#ifdef USE_MYSQL

namespace mygramdb::mysql {

TableMetadataCache::AddResult TableMetadataCache::AddOrUpdate(uint64_t table_id, const TableMetadata& metadata) {
  auto iterator = cache_.find(table_id);
  if (iterator == cache_.end()) {
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

    // Check name (might change after ALTER TABLE)
    if (col_lhs.name != col_rhs.name) {
      return false;
    }
  }

  return true;
}

}  // namespace mygramdb::mysql

#endif  // USE_MYSQL
