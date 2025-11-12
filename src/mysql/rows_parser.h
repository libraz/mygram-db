/**
 * @file rows_parser.h
 * @brief Parser for MySQL ROWS events (WRITE/UPDATE/DELETE)
 */

#pragma once

#ifdef USE_MYSQL

#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "config/config.h"
#include "mysql/table_metadata.h"
#include "storage/document_store.h"

namespace mygramdb::mysql {

/**
 * @brief Parsed row data from ROWS event
 */
struct RowData {
  std::string primary_key;
  std::string text;                                      // Extracted text for full-text search
  std::unordered_map<std::string, std::string> columns;  // All column values as strings
};

/**
 * @brief Parse WRITE_ROWS event (INSERT)
 *
 * @param buffer Event buffer (after header)
 * @param length Buffer length
 * @param table_metadata Table metadata from TABLE_MAP
 * @param pk_column_name Primary key column name
 * @param text_column_name Text column name for search
 * @return Vector of rows if parsed successfully
 */
std::optional<std::vector<RowData>> ParseWriteRowsEvent(const unsigned char* buffer, unsigned long length,
                                                        const TableMetadata* table_metadata,
                                                        const std::string& pk_column_name,
                                                        const std::string& text_column_name);

/**
 * @brief Parse UPDATE_ROWS event
 *
 * @param buffer Event buffer (after header)
 * @param length Buffer length
 * @param table_metadata Table metadata from TABLE_MAP
 * @param pk_column_name Primary key column name
 * @param text_column_name Text column name for search
 * @return Vector of (old_row, new_row) pairs if parsed successfully
 */
std::optional<std::vector<std::pair<RowData, RowData>>> ParseUpdateRowsEvent(const unsigned char* buffer,
                                                                             unsigned long length,
                                                                             const TableMetadata* table_metadata,
                                                                             const std::string& pk_column_name,
                                                                             const std::string& text_column_name);

/**
 * @brief Parse DELETE_ROWS event
 *
 * @param buffer Event buffer (after header)
 * @param length Buffer length
 * @param table_metadata Table metadata from TABLE_MAP
 * @param pk_column_name Primary key column name
 * @param text_column_name Text column name for search
 * @return Vector of rows if parsed successfully
 */
std::optional<std::vector<RowData>> ParseDeleteRowsEvent(const unsigned char* buffer, unsigned long length,
                                                         const TableMetadata* table_metadata,
                                                         const std::string& pk_column_name,
                                                         const std::string& text_column_name);

/**
 * @brief Extract filter values from row data
 *
 * Converts string column values from RowData.columns to typed FilterValue
 * based on the filter configuration.
 *
 * @param row_data Parsed row data with all columns as strings
 * @param filter_configs Vector of filter configurations
 * @return Map of filter name to typed FilterValue
 */
std::unordered_map<std::string, storage::FilterValue> ExtractFilters(
    const RowData& row_data, const std::vector<config::FilterConfig>& filter_configs);

}  // namespace mygramdb::mysql

#endif  // USE_MYSQL
