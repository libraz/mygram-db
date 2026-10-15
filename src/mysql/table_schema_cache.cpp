/**
 * @file table_schema_cache.cpp
 * @brief Reader-thread owner of TABLE_MAP metadata
 */

#include "mysql/table_schema_cache.h"

#ifdef USE_MYSQL

namespace mygramdb::mysql {

void TableSchemaCache::Reset() {
  table_maps_.Clear();
  suppressed_table_ids_.clear();
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

}  // namespace mygramdb::mysql

#endif  // USE_MYSQL
