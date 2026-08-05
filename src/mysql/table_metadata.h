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

namespace mygramdb::mysql {

/**
 * @brief MySQL column types (subset relevant for text search)
 *
 * Based on enum_field_types from MySQL source
 */
enum class ColumnType : uint8_t {
  TINY = 1,           // TINYINT
  SHORT = 2,          // SMALLINT
  LONG = 3,           // INT
  FLOAT = 4,          // FLOAT
  DOUBLE = 5,         // DOUBLE
  TIMESTAMP = 7,      // TIMESTAMP
  LONGLONG = 8,       // BIGINT
  INT24 = 9,          // MEDIUMINT
  DATE = 10,          // DATE
  TIME = 11,          // TIME
  DATETIME = 12,      // DATETIME
  YEAR = 13,          // YEAR
  NEWDATE = 14,       // Internal
  VARCHAR = 15,       // VARCHAR
  BIT = 16,           // BIT
  TIMESTAMP2 = 17,    // TIMESTAMP with fractional seconds
  DATETIME2 = 18,     // DATETIME with fractional seconds
  TIME2 = 19,         // TIME with fractional seconds
  VECTOR = 242,       // VECTOR (MySQL 9.0+)
  JSON = 245,         // JSON
  NEWDECIMAL = 246,   // DECIMAL
  ENUM = 247,         // ENUM
  SET = 248,          // SET
  TINY_BLOB = 249,    // TINYBLOB/TINYTEXT
  MEDIUM_BLOB = 250,  // MEDIUMBLOB/MEDIUMTEXT
  LONG_BLOB = 251,    // LONGBLOB/LONGTEXT
  BLOB = 252,         // BLOB/TEXT
  VAR_STRING = 253,   // VARCHAR/VARBINARY
  STRING = 254,       // CHAR/BINARY
  GEOMETRY = 255      // Spatial types
};

/**
 * @brief Column metadata
 */
struct ColumnMetadata {
  ColumnType type = ColumnType::LONG;
  std::string name;       // May not be available from binlog
  uint16_t metadata = 0;  // Type-specific metadata
  bool is_nullable = false;
  bool is_unsigned = false;
  std::vector<std::string> enum_set_values;  // ENUM labels or SET members in declaration order
};

/**
 * @brief Parse ENUM/SET member labels from a SHOW COLUMNS type definition.
 *
 * @param column_type MySQL type string such as enum('draft','published')
 * @return Labels in declaration order, or an empty vector for non-ENUM/SET or malformed input
 */
[[nodiscard]] std::vector<std::string> ParseEnumSetColumnValues(const std::string& column_type);

/**
 * @brief Table metadata extracted from TABLE_MAP event
 */
struct TableMetadata {
  uint64_t table_id = 0;
  std::string database_name;
  std::string table_name;
  std::vector<ColumnMetadata> columns;
  std::unordered_map<std::string, size_t> column_ordinals;

  // Bitmap indicating which columns are used
  std::vector<uint8_t> columns_before_image;  // For UPDATE: old values
  std::vector<uint8_t> columns_after_image;   // For INSERT/UPDATE: new values

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
