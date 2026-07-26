/**
 * @file table_metadata.cpp
 * @brief Table metadata cache implementation
 */

#include "mysql/table_metadata.h"

#ifdef USE_MYSQL

#include <cctype>

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

std::vector<std::string> ParseEnumSetColumnValues(const std::string& column_type) {
  size_t prefix_length = 0;
  if (column_type.size() >= 5) {
    std::string prefix = column_type.substr(0, 5);
    for (char& chr : prefix) {
      chr = static_cast<char>(std::tolower(static_cast<unsigned char>(chr)));
    }
    if (prefix == "enum(") {
      prefix_length = 5;
    }
  }
  if (prefix_length == 0 && column_type.size() >= 4) {
    std::string prefix = column_type.substr(0, 4);
    for (char& chr : prefix) {
      chr = static_cast<char>(std::tolower(static_cast<unsigned char>(chr)));
    }
    if (prefix == "set(") {
      prefix_length = 4;
    }
  }
  if (prefix_length == 0 || column_type.back() != ')') {
    return {};
  }

  std::vector<std::string> values;
  size_t pos = prefix_length;
  const size_t end = column_type.size() - 1;
  while (pos < end) {
    if (column_type[pos] != '\'') {
      return {};
    }
    ++pos;
    std::string value;
    bool closed = false;
    while (pos < end) {
      const char chr = column_type[pos++];
      if (chr == '\\') {
        if (pos >= end) {
          return {};
        }
        value.push_back(column_type[pos++]);
      } else if (chr == '\'') {
        if (pos < end && column_type[pos] == '\'') {
          value.push_back('\'');
          ++pos;
        } else {
          closed = true;
          break;
        }
      } else {
        value.push_back(chr);
      }
    }
    if (!closed) {
      return {};
    }
    values.push_back(std::move(value));
    if (pos == end) {
      break;
    }
    if (column_type[pos] != ',') {
      return {};
    }
    ++pos;
  }
  return values;
}

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

    if (col_lhs.enum_set_values != col_rhs.enum_set_values) {
      return false;
    }
  }

  return true;
}

}  // namespace mygramdb::mysql

#endif  // USE_MYSQL
