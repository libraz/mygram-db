/**
 * @file table_schema_cache.cpp
 * @brief Reader-thread owner of TABLE_MAP metadata and SHOW COLUMNS definitions
 */

#include "mysql/table_schema_cache.h"

#ifdef USE_MYSQL

namespace mygramdb::mysql {

void TableSchemaCache::Reset() {
  table_maps_.Clear();
  suppressed_table_ids_.clear();
  columns_.clear();
}

TableMetadataCache::AddResult TableSchemaCache::RecordTableMap(const TableMetadata& metadata) {
  suppressed_table_ids_.erase(metadata.table_id);
  return table_maps_.AddOrUpdate(metadata.table_id, metadata);
}

void TableSchemaCache::RecordSuppressedTableMap(uint64_t table_id) {
  table_maps_.Remove(table_id);
  suppressed_table_ids_.insert(table_id);
}

void TableSchemaCache::RecordUnmonitoredTableMap(uint64_t table_id) {
  // A server reuses table ids, so the id may still map to a replicated table
  // seen earlier; its rows must not decode against that table's schema.
  table_maps_.Remove(table_id);
  suppressed_table_ids_.erase(table_id);
}

bool TableSchemaCache::IsSuppressed(uint64_t table_id) const {
  return suppressed_table_ids_.find(table_id) != suppressed_table_ids_.end();
}

const std::vector<TableSchemaCache::ColumnDefinition>* TableSchemaCache::FindColumns(const std::string& database,
                                                                                     const std::string& table) const {
  auto iter = columns_.find({database, table});
  return iter == columns_.end() ? nullptr : &iter->second;
}

void TableSchemaCache::StoreColumns(const std::string& database, const std::string& table,
                                    std::vector<ColumnDefinition> columns) {
  columns_[{database, table}] = std::move(columns);
}

void TableSchemaCache::InvalidateColumns(std::string_view database, std::string_view table) {
  for (auto iter = columns_.begin(); iter != columns_.end();) {
    const auto& [cached_database, cached_table] = iter->first;
    if (cached_table == table && (database.empty() || cached_database == database)) {
      iter = columns_.erase(iter);
    } else {
      ++iter;
    }
  }
}

}  // namespace mygramdb::mysql

#endif  // USE_MYSQL
