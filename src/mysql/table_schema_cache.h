/**
 * @file table_schema_cache.h
 * @brief Reader-thread owner of TABLE_MAP metadata and SHOW COLUMNS definitions
 *
 * A rows event is decoded against two caches that have to move together: the
 * TABLE_MAP metadata for its table id, and the column names, signedness and
 * ENUM/SET labels a TABLE_MAP does not carry. Both are written and invalidated
 * here only, by the binlog reader thread, so a table id never outlives the
 * stream connection that assigned it and a column list never outlives the DDL
 * that redefined it. The class is not thread-safe by design.
 */

#pragma once

#ifdef USE_MYSQL

#include <cstdint>
#include <map>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

#include "mysql/table_metadata.h"

namespace mygramdb::mysql {

class TableSchemaCache {
 public:
  /**
   * @brief Column attributes read from SHOW COLUMNS
   */
  struct ColumnDefinition {
    std::string name;
    bool is_unsigned = false;
    std::vector<std::string> enum_set_values;
  };

  /// Metadata of every table id mapped on the current stream.
  [[nodiscard]] const TableMetadataCache& table_maps() const { return table_maps_; }

  /// Forget everything. Table ids are scoped to one stream connection.
  void Reset();

  /**
   * @brief Record a TABLE_MAP whose rows will be decoded
   * @return How the map compares with the table's previous one
   */
  TableMetadataCache::AddResult RecordTableMap(const TableMetadata& metadata);

  /// Record a TABLE_MAP whose rows a SYNC snapshot already covers; its rows are skipped.
  void RecordSuppressedTableMap(uint64_t table_id);

  /// Record a TABLE_MAP for a table that is not replicated; its rows are ignored.
  void RecordUnmonitoredTableMap(uint64_t table_id);

  [[nodiscard]] bool IsSuppressed(uint64_t table_id) const;

  /// Cached SHOW COLUMNS result for a table, or nullptr.
  [[nodiscard]] const std::vector<ColumnDefinition>* FindColumns(const std::string& database,
                                                                 const std::string& table) const;

  void StoreColumns(const std::string& database, const std::string& table, std::vector<ColumnDefinition> columns);

  /**
   * @brief Drop a table's cached column definitions
   *
   * An empty @p database drops the table in every database, matching a table
   * configuration that does not name one.
   */
  void InvalidateColumns(std::string_view database, std::string_view table);

 private:
  TableMetadataCache table_maps_;
  std::unordered_set<uint64_t> suppressed_table_ids_;
  std::map<std::pair<std::string, std::string>, std::vector<ColumnDefinition>> columns_;
};

}  // namespace mygramdb::mysql

#endif  // USE_MYSQL
