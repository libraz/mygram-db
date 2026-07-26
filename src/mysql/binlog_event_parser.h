/**
 * @file binlog_event_parser.h
 * @brief MySQL binlog event parsing utilities
 */

#pragma once

#ifdef USE_MYSQL

#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "mysql/binlog_reader.h"
#include "mysql/rows_parser.h"
#include "mysql/table_metadata.h"

namespace mygramdb::mysql {

/**
 * @brief Binlog event parser
 *
 * Parses raw MySQL binlog event buffers into structured BinlogEvent objects
 */
class BinlogEventParser {
 public:
  enum class QueryTransactionBoundary : uint8_t { kNone, kBegin, kEnd, kUnsupportedXa };

  /**
   * @brief Parse binlog event buffer and create BinlogEvent(s)
   * @param buffer Raw binlog event data
   * @param length Length of the buffer
   * @param current_gtid Current GTID for this event
   * @param table_metadata_cache Cache for table metadata
   * @param table_contexts Map of table name to TableContext (multi-table mode)
   * @param table_config Single table config (single-table mode, can be nullptr)
   * @param multi_table_mode Whether operating in multi-table mode
   * @param datetime_timezone Timezone offset for DATETIME interpretation (e.g., "+09:00")
   * @return Vector of BinlogEvents (empty if no events parsed, multiple for batch operations)
   */
  static std::vector<BinlogEvent> ParseBinlogEvent(
      const unsigned char* buffer, unsigned long length, const std::string& current_gtid,
      TableMetadataCache& table_metadata_cache,
      const std::unordered_map<std::string, server::TableContext*>& table_contexts,
      const config::TableConfig* table_config, bool multi_table_mode, const std::string& datetime_timezone = "+00:00");

  /**
   * @brief Extract GTID from GTID_LOG_EVENT
   * @param buffer Event buffer
   * @param length Buffer length
   * @return GTID string if found
   */
  static std::optional<std::string> ExtractGTID(const unsigned char* buffer, unsigned long length);

  /**
   * @brief Extract tagged GTID from GTID_TAGGED_LOG_EVENT (MySQL 8.4+)
   *
   * MySQL 8.4 uses a serialization framework format for tagged GTIDs.
   * Returns GTID in "UUID:TAG:GNO" or "UUID:GNO" format.
   *
   * @param buffer Event buffer
   * @param length Buffer length
   * @return GTID string if found
   */
  static std::optional<std::string> ExtractTaggedGTID(const unsigned char* buffer, unsigned long length);

  /**
   * @brief Parse TABLE_MAP_EVENT
   * @param buffer Event buffer (post-header)
   * @param length Buffer length
   * @return TableMetadata if successfully parsed
   */
  static std::optional<TableMetadata> ParseTableMapEvent(const unsigned char* buffer, unsigned long length);

  /**
   * @brief Extract SQL query string from QUERY_EVENT
   * @param buffer Event buffer
   * @param length Buffer length
   * @return Query string if successfully extracted
   */
  static std::optional<std::string> ExtractQueryString(const unsigned char* buffer, unsigned long length);

  /**
   * @brief Classify QUERY_EVENT transaction boundaries.
   *
   * The reader uses this to distinguish a standalone statement (whose GTID
   * completes at the QUERY_EVENT) from statements inside an explicit
   * transaction (whose GTID completes at XID/COMMIT).
   */
  static QueryTransactionBoundary ClassifyQueryTransactionBoundary(std::string_view query);

  /**
   * @brief Whether a MariaDB GTID event starts a transaction that needs a later COMMIT/XID.
   *
   * MariaDB sets FL_STANDALONE when the GTID event itself represents a
   * statement with no terminating COMMIT event.
   */
  static bool IsMariaDBGtidTransactionOpen(uint8_t flags);

  /**
   * @brief Whether a row event is the final event for its SQL statement.
   *
   * MariaDB standalone non-transactional groups have no XID/COMMIT event, so
   * the reader uses the row-event STMT_END_F flag as their commit boundary.
   */
  static bool IsRowsStatementEnd(const unsigned char* buffer, unsigned long length);

  /**
   * @brief Whether an ignored standalone QUERY_EVENT is proven not to mutate table rows.
   *
   * Statement-based DML and unknown statements must never advance the applied
   * GTID because the row materializer has not applied their effects. This is
   * intentionally an allowlist of administrative statements and supported
   * non-target table DDL emitted while the server remains in ROW mode.
   */
  static bool IsSafeIgnoredQuery(std::string_view query);

  /**
   * @brief Check if DDL affects target table
   * @param query SQL query string
   * @param table_name Target table name
   * @return true if DDL affects the table
   */
  static bool IsTableAffectingDDL(const std::string& query, const std::string& table_name);
};

}  // namespace mygramdb::mysql

#endif  // USE_MYSQL
