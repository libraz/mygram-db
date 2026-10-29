/**
 * @file table_metadata.h
 * @brief Table metadata from TABLE_MAP events
 */

#pragma once

#ifdef USE_MYSQL

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "mybinlog/event_types.h"

namespace mygramdb::mysql {

/// Column type code as a TABLE_MAP carries it.
using ColumnType = mybinlog::ColumnType;

/**
 * @brief Column metadata
 */
struct ColumnMetadata {
  ColumnType type = ColumnType::kLong;
  std::string name;       // "col_<index>" unless the TABLE_MAP carried COLUMN_NAME
  uint16_t metadata = 0;  // Type-specific metadata
  bool is_nullable = false;
  bool is_unsigned = false;
  std::vector<std::string> enum_set_values;  // ENUM labels or SET members in declaration order
};

/**
 * @brief Table metadata extracted from TABLE_MAP event
 */
struct TableMetadata {
  uint64_t table_id = 0;
  std::string database_name;
  std::string table_name;
  std::vector<ColumnMetadata> columns;
  std::unordered_map<std::string, size_t> column_ordinals;

  // What the TABLE_MAP optional metadata block supplied (binlog_row_metadata=FULL carries all three).
  bool has_column_names = false;       ///< COLUMN_NAME gave every column its real name
  bool signedness_known = false;       ///< Every numeric column has its UNSIGNED flag from SIGNEDNESS
  bool enum_set_labels_known = false;  ///< Every ENUM and SET column has its labels

  // Bitmap indicating which columns are used
  std::vector<uint8_t> columns_before_image;  // For UPDATE: old values
  std::vector<uint8_t> columns_after_image;   // For INSERT/UPDATE: new values

  /// Whether names, signedness and ENUM/SET labels all came from the binlog rather than a schema query.
  [[nodiscard]] bool HasFullRowMetadata() const {
    return has_column_names && signedness_known && enum_set_labels_known;
  }

  void RebuildColumnOrdinals();
  [[nodiscard]] std::optional<size_t> FindColumnOrdinal(std::string_view name) const;
};

/**
 * @brief Table metadata cache
 *
 * Stores metadata for tables seen in TABLE_MAP events
 */
class TableMetadataCache {
 public:
  /**
   * @brief Result of adding or updating table metadata
   */
  enum class AddResult : std::uint8_t {
    kAdded,         // New entry added
    kUpdated,       // Existing entry updated (no schema change)
    kSchemaChanged  // Existing entry updated (schema changed)
  };

  /**
   * @brief Add or update table metadata with schema change detection
   *
   * @param table_id Table ID from TABLE_MAP event
   * @param metadata New metadata
   * @return AddResult indicating what happened
   */
  AddResult AddOrUpdate(uint64_t table_id, const TableMetadata& metadata);

  /**
   * @brief Add table metadata (legacy, calls AddOrUpdate internally)
   */
  void Add(uint64_t table_id, const TableMetadata& metadata);

  /**
   * @brief Get table metadata by ID
   */
  [[nodiscard]] const TableMetadata* Get(uint64_t table_id) const;

  /**
   * @brief Remove table metadata
   */
  void Remove(uint64_t table_id);

  /**
   * @brief Clear all metadata
   */
  void Clear();

  /**
   * @brief Check if a table exists in cache
   */
  [[nodiscard]] bool Contains(uint64_t table_id) const;

  /**
   * @brief Number of cached table identities.
   *
   * A new TABLE_MAP id for the same database/table replaces the previous id,
   * so production cache growth is bounded by the configured table set.
   */
  [[nodiscard]] size_t Size() const { return cache_.size(); }

 private:
  /**
   * @brief Compare two metadata entries for schema equality
   *
   * Checks column count, types, and names for differences
   */
  [[nodiscard]] static bool SchemaEquals(const TableMetadata& lhs, const TableMetadata& rhs);

  std::map<uint64_t, TableMetadata> cache_;
};

}  // namespace mygramdb::mysql

#endif  // USE_MYSQL
