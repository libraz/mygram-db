/**
 * @file rows_parser.h
 * @brief Parser for MySQL ROWS events (WRITE/UPDATE/DELETE)
 */

#pragma once

#ifdef USE_MYSQL

#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "config/config.h"
#include "mysql/binlog_event_types.h"
#include "mysql/table_metadata.h"
#include "storage/document_store.h"
#include "utils/error.h"
#include "utils/expected.h"

namespace mygramdb::mysql {

/**
 * @brief Parsed row data from ROWS event
 */
struct RowData {
  std::string primary_key;
  std::string text;  // Extracted text for full-text search
  const TableMetadata* table_metadata = nullptr;
  std::vector<std::string> column_values;                // Values indexed by TableMetadata::columns ordinal
  std::vector<bool> column_values_present;               // True when the row image included the ordinal
  std::vector<bool> column_values_null;                  // True when the row image included an explicit NULL
  std::unordered_map<std::string, std::string> columns;  // All column values as strings
  std::unordered_set<std::string> null_columns;          // Explicit NULLs for map-backed/manual RowData

  [[nodiscard]] const std::string* FindColumnValue(const std::string& column_name) const {
    if (table_metadata != nullptr && column_values.size() == table_metadata->columns.size() &&
        column_values_present.size() == table_metadata->columns.size()) {
      for (size_t i = 0; i < table_metadata->columns.size(); ++i) {
        if (column_values_present[i] && table_metadata->columns[i].name == column_name) {
          return &column_values[i];
        }
      }
    }
    auto it = columns.find(column_name);
    return it == columns.end() ? nullptr : &it->second;
  }

  [[nodiscard]] bool IsColumnNull(const std::string& column_name) const {
    if (table_metadata != nullptr && column_values_present.size() == table_metadata->columns.size() &&
        column_values_null.size() == table_metadata->columns.size()) {
      for (size_t i = 0; i < table_metadata->columns.size(); ++i) {
        if (column_values_present[i] && table_metadata->columns[i].name == column_name) {
          return column_values_null[i];
        }
      }
    }
    return null_columns.find(column_name) != null_columns.end();
  }

  [[nodiscard]] std::string GetColumnValue(const std::string& column_name) const {
    const std::string* value = FindColumnValue(column_name);
    return value == nullptr ? std::string{} : *value;
  }
};

/**
 * @brief Parse WRITE_ROWS event (INSERT)
 *
 * @param buffer Event buffer (after header)
 * @param length Buffer length
 * @param table_metadata Table metadata from TABLE_MAP
 * @param pk_column_name Primary key column name
 * @param text_column_name Text column name for search
 * @return Expected<vector<RowData>, Error> - parsed rows or error with code from 2000-2999
 */
mygram::utils::Expected<std::vector<RowData>, mygram::utils::Error> ParseWriteRowsEvent(
    const unsigned char* buffer, unsigned long length, const TableMetadata* table_metadata,
    const std::string& pk_column_name, const std::string& text_column_name, MySQLBinlogEventType event_type);

/**
 * @brief Parse UPDATE_ROWS event
 *
 * @param buffer Event buffer (after header)
 * @param length Buffer length
 * @param table_metadata Table metadata from TABLE_MAP
 * @param pk_column_name Primary key column name
 * @param text_column_name Text column name for search
 * @return Expected<vector<pair<RowData, RowData>>, Error> - parsed row pairs or error
 */
mygram::utils::Expected<std::vector<std::pair<RowData, RowData>>, mygram::utils::Error> ParseUpdateRowsEvent(
    const unsigned char* buffer, unsigned long length, const TableMetadata* table_metadata,
    const std::string& pk_column_name, const std::string& text_column_name, MySQLBinlogEventType event_type);

/**
 * @brief Parse DELETE_ROWS event
 *
 * @param buffer Event buffer (after header)
 * @param length Buffer length
 * @param table_metadata Table metadata from TABLE_MAP
 * @param pk_column_name Primary key column name
 * @param text_column_name Text column name for search
 * @return Expected<vector<RowData>, Error> - parsed rows or error with code from 2000-2999
 */
mygram::utils::Expected<std::vector<RowData>, mygram::utils::Error> ParseDeleteRowsEvent(
    const unsigned char* buffer, unsigned long length, const TableMetadata* table_metadata,
    const std::string& pk_column_name, const std::string& text_column_name, MySQLBinlogEventType event_type);

/**
 * @brief Extract filter values from row data
 *
 * Converts string column values from RowData.columns to typed FilterValue
 * based on the filter configuration.
 *
 * @param row_data Parsed row data with all columns as strings
 * @param filter_configs Vector of filter configurations
 * @param datetime_timezone Timezone offset for DATETIME interpretation (e.g., "+09:00")
 * @return Map of filter name to typed FilterValue
 */
storage::FilterMap ExtractFilters(const RowData& row_data, const std::vector<config::FilterConfig>& filter_configs,
                                  const std::string& datetime_timezone = "+00:00");

}  // namespace mygramdb::mysql

#endif  // USE_MYSQL
