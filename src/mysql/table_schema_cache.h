/**
 * @file table_schema_cache.h
 * @brief Reader-thread owner of TABLE_MAP metadata
 *
 * A rows event is decoded against the TABLE_MAP metadata for its table id. It
 * is written here only, by the binlog reader thread, so a table id never
 * outlives the stream connection that assigned it. The class is not
 * thread-safe by design.
 */

#pragma once

#ifdef USE_MYSQL

#include <cstdint>
#include <unordered_set>

#include "mysql/table_metadata.h"

namespace mygramdb::mysql {

class TableSchemaCache {
 public:
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

 private:
  TableMetadataCache table_maps_;
  std::unordered_set<uint64_t> suppressed_table_ids_;
};

}  // namespace mygramdb::mysql

#endif  // USE_MYSQL
