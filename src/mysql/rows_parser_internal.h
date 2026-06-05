/**
 * @file rows_parser_internal.h
 * @brief Internal declarations for rows_parser split translation units
 *
 * This header is not part of the public API. It provides shared declarations
 * between rows_parser.cpp, rows_parser_field_decoder.cpp, and
 * rows_parser_filter.cpp.
 */

#pragma once

#ifdef USE_MYSQL

#include <cstdint>
#include <string>

#include "mysql/binlog_event_types.h"
#include "mysql/rows_parser.h"
#include "mysql/table_metadata.h"
#include "utils/error.h"
#include "utils/expected.h"

namespace mygramdb::mysql::internal {

/**
 * @brief Parsed header data from a ROWS event (WRITE/UPDATE/DELETE)
 *
 * Contains all fields extracted from the common post-header, var_header,
 * column_count, first column bitmap, and PK/text column index lookup.
 * For UPDATE events, the caller must parse the second (after-image) bitmap
 * starting at row_data_ptr.
 */
struct RowsEventHeader {
  const unsigned char* row_data_ptr;     ///< Pointer to where row data (or second bitmap) starts
  const unsigned char* end;              ///< Pointer to end of parseable data (before CRC32)
  uint64_t column_count;                 ///< Number of columns in the event
  const unsigned char* columns_present;  ///< First column bitmap (before-image for UPDATE)
  size_t bitmap_size;                    ///< Size of each column bitmap in bytes
  int pk_col_idx;                        ///< Index of primary key column, or -1 if not found
  int text_col_idx;                      ///< Index of text column, or -1 if not found
};

/**
 * @brief Parse the common header of a ROWS event (post-header, var_header,
 *        column_count, first column bitmap, PK/text column indices).
 *
 * This handles the identical prologue shared by WRITE, UPDATE, and DELETE
 * row events. For UPDATE events the returned row_data_ptr points to the
 * start of the after-image column bitmap (which the caller must parse).
 * For WRITE/DELETE events row_data_ptr points to the first row.
 *
 * @param buffer       Raw event buffer (after OK byte, includes event header)
 * @param length       Total buffer length
 * @param event_type   Binlog event type (determines V1 vs V2 format)
 * @param table_metadata Table metadata from the preceding TABLE_MAP event
 * @param pk_column_name  Primary key column name to look up
 * @param text_column_name Text column name to look up
 * @param event_type_label Short label for log messages (e.g. "write_rows")
 * @return Parsed header on success, Error on any parse error
 */
mygram::utils::Expected<RowsEventHeader, mygram::utils::Error> ParseRowsEventHeader(
    const unsigned char* buffer, unsigned long length, MySQLBinlogEventType event_type,
    const TableMetadata* table_metadata, const std::string& pk_column_name, const std::string& text_column_name,
    const char* event_type_label);

/**
 * @brief Convert fractional seconds to microseconds based on precision metadata.
 * @param frac Raw fractional value from binlog
 * @param precision Metadata precision (1-6)
 * @return Microseconds value
 */
uint32_t FractionalToMicroseconds(int32_t frac, uint8_t precision);

/**
 * @brief Decode a single field value as string
 *
 * @param col_type Column type
 * @param data Pointer to field data
 * @param metadata Type metadata
 * @param is_null Whether the field is NULL
 * @param end Pointer to end of buffer
 * @param is_unsigned Whether the field is unsigned
 * @return String representation of the value
 */
mygram::utils::Expected<std::string, mygram::utils::Error> DecodeFieldValue(uint8_t col_type, const unsigned char* data,
                                                                            uint16_t metadata, bool is_null,
                                                                            const unsigned char* end,
                                                                            bool is_unsigned = false);

/**
 * @brief Result of parsing a single row from a ROWS event
 */
struct SingleRowResult {
  RowData row;                    ///< Parsed row data
  const unsigned char* next_ptr;  ///< Pointer past this row's data
};

/**
 * @brief Parse a single row from a ROWS event buffer
 *
 * Handles NULL bitmap reading, per-column type dispatch, field decoding,
 * PK/text extraction, and pointer advancement. Used by ParseWriteRowsEvent,
 * ParseUpdateRowsEvent, and ParseDeleteRowsEvent to avoid code duplication.
 *
 * @param ptr Pointer to the start of the row (null bitmap)
 * @param end Pointer to end of parseable data
 * @param meta Table metadata
 * @param columns_present Column presence bitmap
 * @param null_bitmap_size Size of the per-row null bitmap in bytes
 * @param column_count Number of columns in the event
 * @param pk_col_idx Index of primary key column, or -1 if not found
 * @param text_col_idx Index of text column, or -1 if not found
 * @param event_type_label Short label for log/error messages (e.g. "write_rows")
 * @param image_label Image label for log/error messages (e.g. "before", "after", or "")
 * @return Parsed row and pointer past the row data, or error
 */
mygram::utils::Expected<SingleRowResult, mygram::utils::Error> ParseSingleRow(
    const unsigned char* ptr, const unsigned char* end, const TableMetadata* meta, const unsigned char* columns_present,
    size_t null_bitmap_size, uint64_t column_count, int pk_col_idx, int text_col_idx, const char* event_type_label,
    const char* image_label);

}  // namespace mygramdb::mysql::internal

#endif  // USE_MYSQL
