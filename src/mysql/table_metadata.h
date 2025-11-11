/**
 * @file table_metadata.h
 * @brief Table metadata from TABLE_MAP events
 */

#pragma once

#ifdef USE_MYSQL

#include <cstdint>
#include <map>
#include <string>
#include <vector>

namespace mygramdb {
namespace mysql {

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
  ColumnType type;
  std::string name;   // May not be available from binlog
  uint16_t metadata;  // Type-specific metadata
  bool is_nullable;
  bool is_unsigned;
};

/**
 * @brief Table metadata extracted from TABLE_MAP event
 */
struct TableMetadata {
  uint64_t table_id;
  std::string database_name;
  std::string table_name;
  std::vector<ColumnMetadata> columns;

  // Bitmap indicating which columns are used
  std::vector<uint8_t> columns_before_image;  // For UPDATE: old values
  std::vector<uint8_t> columns_after_image;   // For INSERT/UPDATE: new values
};

/**
 * @brief Table metadata cache
 *
 * Stores metadata for tables seen in TABLE_MAP events
 */
class TableMetadataCache {
 public:
  /**
   * @brief Add table metadata
   */
  void Add(uint64_t table_id, const TableMetadata& metadata);

  /**
   * @brief Get table metadata by ID
   */
  const TableMetadata* Get(uint64_t table_id) const;

  /**
   * @brief Remove table metadata
   */
  void Remove(uint64_t table_id);

  /**
   * @brief Clear all metadata
   */
  void Clear();

 private:
  std::map<uint64_t, TableMetadata> cache_;
};

}  // namespace mysql
}  // namespace mygramdb

#endif  // USE_MYSQL
