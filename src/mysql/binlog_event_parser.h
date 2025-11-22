/**
 * @file binlog_event_parser.h
 * @brief MySQL binlog event parsing utilities
 */

#pragma once

#ifdef USE_MYSQL

#include <optional>
#include <string>

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
  /**
   * @brief Parse binlog event buffer and create BinlogEvent
   * @param buffer Raw binlog event data
   * @param length Length of the buffer
   * @param current_gtid Current GTID for this event
   * @param table_metadata_cache Cache for table metadata
   * @param table_contexts Map of table name to TableContext (multi-table mode)
   * @param table_config Single table config (single-table mode, can be nullptr)
   * @param multi_table_mode Whether operating in multi-table mode
   * @param datetime_timezone Timezone offset for DATETIME interpretation (e.g., "+09:00")
   * @return BinlogEvent if successfully parsed, nullopt otherwise
   */
  static std::optional<BinlogEvent> ParseBinlogEvent(
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
   * @brief Check if DDL affects target table
   * @param query SQL query string
   * @param table_name Target table name
   * @return true if DDL affects the table
   */
  static bool IsTableAffectingDDL(const std::string& query, const std::string& table_name);
};

}  // namespace mygramdb::mysql

#endif  // USE_MYSQL
