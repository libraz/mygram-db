/**
 * @file rows_parser.cpp
 * @brief Parser for MySQL ROWS events (WRITE/UPDATE/DELETE)
 *
 * Implementation based on MySQL 8.4.7 source code:
 * - libs/mysql/binlog/event/rows_event.h (event format documentation)
 * - libs/mysql/binlog/event/binary_log_funcs.cpp (field size calculation)
 * - mysys/my_time.cc (DATETIME2 format: my_datetime_packed_from_binary, TIME_from_longlong_datetime_packed)
 *
 * Binary format for WRITE_ROWS event:
 * 1. Common event header (19 bytes) - already skipped by caller
 * 2. Post-header:
 *    - table_id (6 bytes)
 *    - flags (2 bytes)
 * 3. Body:
 *    - width (packed integer) - number of columns
 *    - columns_present bitmap - which columns are in the event
 *    - extra_row_info (optional)
 *    - For each row:
 *      - NULL bitmap - which fields are NULL
 *      - Row data - values for non-NULL fields
 *
 * Note: This file contains low-level MySQL binlog binary protocol parsing.
 * Modern C++ guidelines are relaxed for protocol compatibility.
 */

#include "mysql/rows_parser.h"

#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <sstream>

#include "mysql/binlog_util.h"
#include "mysql/rows_parser_internal.h"
#include "utils/constants.h"
#include "utils/structured_log.h"

#ifdef USE_MYSQL

// NOLINTBEGIN(cppcoreguidelines-pro-*,cppcoreguidelines-avoid-*,readability-magic-numbers,readability-function-cognitive-complexity,readability-else-after-return)

namespace mygramdb::mysql {

// ---------------------------------------------------------------------------
// Helper: determine whether an event_type is V2 (>= 30)
// ---------------------------------------------------------------------------
namespace {

bool IsV2Event(MySQLBinlogEventType event_type) {
  return event_type == MySQLBinlogEventType::WRITE_ROWS_EVENT ||
         event_type == MySQLBinlogEventType::UPDATE_ROWS_EVENT || event_type == MySQLBinlogEventType::DELETE_ROWS_EVENT;
}

bool AllColumnsPresent(const unsigned char* columns_present, uint64_t column_count) {
  if (columns_present == nullptr) {
    return false;
  }
  for (uint64_t col_idx = 0; col_idx < column_count; ++col_idx) {
    if (!binlog_util::bitmap_is_set(columns_present, col_idx)) {
      return false;
    }
  }
  return true;
}

}  // namespace

// ---------------------------------------------------------------------------
// ParseRowsEventHeader -- shared prologue for all three row-event parsers
// ---------------------------------------------------------------------------

mygram::utils::Expected<internal::RowsEventHeader, mygram::utils::Error> internal::ParseRowsEventHeader(
    const unsigned char* buffer, unsigned long length, MySQLBinlogEventType event_type,
    const TableMetadata* table_metadata, const std::string& pk_column_name, const std::string& text_column_name,
    const char* event_type_label) {
  using mygram::utils::ErrorCode;
  using mygram::utils::MakeError;
  using mygram::utils::MakeUnexpected;
  // Event size is at bytes [9-12] of event data (little-endian)
  // (see mysql-8.4.7/libs/mysql/binlog/event/binlog_event.h: LOG_EVENT_HEADER_LEN)
  uint32_t event_size = static_cast<uint32_t>(buffer[9]) | (static_cast<uint32_t>(buffer[10]) << 8) |
                        (static_cast<uint32_t>(buffer[11]) << 16) | (static_cast<uint32_t>(buffer[12]) << 24);

  // Validate event_size before computing end pointer to prevent underflow/OOB
  if (event_size < 4 || event_size > length) {
    return MakeUnexpected(
        MakeError(ErrorCode::kMySQLBinlogError, std::string(event_type_label) + " event: invalid event_size"));
  }

  const unsigned char* ptr = buffer + mygram::constants::kBinlogEventHeaderLen;  // Skip standard header
  // Event size (from header) includes header + data + 4-byte CRC32 checksum.
  // CRC32 is verified by the binlog reader before dispatch.
  // Subtract checksum size to get the actual data boundary.
  const unsigned char* end = buffer + event_size - mygram::constants::kBinlogChecksumSize;

  // Parse post-header: table_id (6 bytes) + flags (2 bytes)
  if (ptr + 8 > end) {
    mygram::utils::StructuredLog()
        .Event("mysql_binlog_error")
        .Field("type", std::string(event_type_label) + "_too_short")
        .Field("section", "post-header")
        .Error();
    return MakeUnexpected(
        MakeError(ErrorCode::kMySQLBinlogError, std::string(event_type_label) + " event too short for post-header"));
  }

  ptr += 6;  // table_id (already known from TABLE_MAP)
  ptr += 2;  // flags

  // V2 Rows Events (type >= 30): always have var_header_len (uint16_t, minimum value 2)
  // V1 Rows Events (type 23-25): no var_header_len
  // (see mysql-8.4.7/libs/mysql/binlog/event/rows_event.h)
  if (IsV2Event(event_type)) {
    if (ptr + 2 > end) {
      mygram::utils::StructuredLog()
          .Event("mysql_binlog_error")
          .Field("type", std::string(event_type_label) + "_too_short")
          .Field("section", "var_header_len")
          .Error();
      return MakeUnexpected(MakeError(ErrorCode::kMySQLBinlogError,
                                      std::string(event_type_label) + " event too short for var_header_len"));
    }
    uint16_t var_header_len = binlog_util::uint2korr(ptr);
    if (var_header_len < 2 || ptr + var_header_len > end) {
      mygram::utils::StructuredLog()
          .Event("mysql_binlog_error")
          .Field("type", "invalid_var_header_len")
          .Field("event_type", event_type_label)
          .Field("var_header_len", static_cast<uint64_t>(var_header_len))
          .Error();
      return MakeUnexpected(
          MakeError(ErrorCode::kMySQLBinlogError, std::string(event_type_label) + " event: invalid var_header_len"));
    }
    ptr += var_header_len;  // Skip entire var_header (len field + extra data)
  }

  // Parse body -- column_count (packed integer)
  if (ptr >= end) {
    mygram::utils::StructuredLog()
        .Event("mysql_binlog_error")
        .Field("type", std::string(event_type_label) + "_too_short")
        .Field("section", "width")
        .Error();
    return MakeUnexpected(
        MakeError(ErrorCode::kMySQLBinlogError, std::string(event_type_label) + " event too short for column_count"));
  }
  uint64_t column_count = 0;
  if (!binlog_util::read_packed_integer(&ptr, end, &column_count)) {
    mygram::utils::StructuredLog()
        .Event("mysql_binlog_error")
        .Field("type", std::string(event_type_label) + "_too_short")
        .Field("section", "column_count")
        .Error();
    return MakeUnexpected(
        MakeError(ErrorCode::kMySQLBinlogError, std::string(event_type_label) + " event too short for column_count"));
  }

  if (column_count != table_metadata->columns.size()) {
    mygram::utils::StructuredLog()
        .Event("mysql_binlog_error")
        .Field("type", "column_count_mismatch")
        .Field("event_type", event_type_label)
        .Field("event_columns", column_count)
        .Field("table_columns", static_cast<uint64_t>(table_metadata->columns.size()))
        .Error();
    return MakeUnexpected(
        MakeError(ErrorCode::kMySQLBinlogError, std::string(event_type_label) + " event: column_count mismatch"));
  }

  // columns_present bitmap (first bitmap -- "before image" for UPDATE, only bitmap for WRITE/DELETE)
  size_t bitmap_size = binlog_util::bitmap_bytes(column_count);
  if (ptr + bitmap_size > end) {
    mygram::utils::StructuredLog()
        .Event("mysql_binlog_error")
        .Field("type", std::string(event_type_label) + "_too_short")
        .Field("section", "columns_present bitmap")
        .Error();
    return MakeUnexpected(MakeError(ErrorCode::kMySQLBinlogError,
                                    std::string(event_type_label) + " event too short for columns_present bitmap"));
  }
  const unsigned char* columns_present = ptr;
  ptr += bitmap_size;
  if (!AllColumnsPresent(columns_present, column_count)) {
    mygram::utils::StructuredLog()
        .Event("mysql_binlog_error")
        .Field("type", "partial_columns_present_bitmap")
        .Field("event_type", event_type_label)
        .Field("required", "binlog_row_image=FULL")
        .Error();
    return MakeUnexpected(MakeError(
        ErrorCode::kMySQLBinlogError,
        std::string(event_type_label) + " event: partial columns_present bitmap requires binlog_row_image=FULL"));
  }

  // Find PK and text column indices (one-time lookup)
  int pk_col_idx = -1;
  int text_col_idx = -1;
  for (size_t i = 0; i < column_count; i++) {
    if (table_metadata->columns[i].name == pk_column_name) {
      pk_col_idx = static_cast<int>(i);
    }
    if (table_metadata->columns[i].name == text_column_name) {
      text_col_idx = static_cast<int>(i);
    }
  }

  return RowsEventHeader{
      ptr,              // row_data_ptr (or second-bitmap start for UPDATE)
      end,              // end
      column_count,     // column_count
      columns_present,  // columns_present
      bitmap_size,      // bitmap_size
      pk_col_idx,       // pk_col_idx
      text_col_idx,     // text_col_idx
  };
}

// ---------------------------------------------------------------------------
// ParseSingleRow -- shared per-row column decode loop
// ---------------------------------------------------------------------------

mygram::utils::Expected<internal::SingleRowResult, mygram::utils::Error> internal::ParseSingleRow(
    const unsigned char* ptr, const unsigned char* end, const TableMetadata* meta, const unsigned char* columns_present,
    size_t null_bitmap_size, uint64_t column_count, int pk_col_idx, int text_col_idx, const char* event_type_label,
    const char* image_label) {
  using mygram::utils::ErrorCode;
  using mygram::utils::MakeError;
  using mygram::utils::MakeUnexpected;

  if (!AllColumnsPresent(columns_present, column_count)) {
    std::string msg = std::string(event_type_label) +
                      " event: partial columns_present bitmap requires "
                      "binlog_row_image=FULL";
    if (image_label[0] != '\0') {
      msg += std::string(" (") + image_label + " image)";
    }
    return MakeUnexpected(MakeError(ErrorCode::kMySQLBinlogError, msg));
  }

  // NULL bitmap for this row
  if (ptr + null_bitmap_size > end) {
    return MakeUnexpected(
        MakeError(ErrorCode::kMySQLFieldTruncated, std::string(event_type_label) + " row truncated at null bitmap"));
  }
  const unsigned char* null_bitmap = ptr;
  ptr += null_bitmap_size;

  // Debug: Show bitmaps
  if (mygram::utils::IsDebugLogEnabled()) {
    std::string col_bitmap_str;
    std::string null_bitmap_str;
    for (uint64_t i = 0; i < column_count; i++) {
      col_bitmap_str += binlog_util::bitmap_is_set(columns_present, i) ? "1" : "0";
      null_bitmap_str += binlog_util::bitmap_is_set(null_bitmap, i) ? "N" : ".";
    }
    std::string action = "image_bitmaps";
    if (image_label[0] != '\0') {
      action = std::string(image_label) + "_image_bitmaps";
    }
    mygram::utils::StructuredLog()
        .Event("binlog_debug")
        .Field("action", action)
        .Field("columns_bitmap", col_bitmap_str)
        .Field("null_bitmap", null_bitmap_str)
        .Debug();
  }

  RowData row;
  row.table_metadata = meta;
  row.column_values.resize(meta->columns.size());
  row.column_values_present.assign(meta->columns.size(), false);
  row.column_values_null.assign(meta->columns.size(), false);

  // Parse each column value
  for (uint64_t col_idx = 0; col_idx < column_count; col_idx++) {
    // Check if column is present in this event
    if (!binlog_util::bitmap_is_set(columns_present, col_idx)) {
      if (mygram::utils::IsDebugLogEnabled()) {
        mygram::utils::StructuredLog()
            .Event("binlog_debug")
            .Field("action", "column_not_in_bitmap")
            .Field("col_idx", col_idx)
            .Field("col_name", col_idx < meta->columns.size() ? meta->columns[col_idx].name : "?")
            .Debug();
      }
      continue;
    }

    const auto& col_meta = meta->columns[col_idx];
    bool is_null = binlog_util::bitmap_is_set(null_bitmap, col_idx);

    if (mygram::utils::IsDebugLogEnabled()) {
      mygram::utils::StructuredLog()
          .Event("binlog_debug")
          .Field("action", "parsing_column")
          .Field("col_idx", col_idx)
          .Field("col_name", col_meta.name)
          .Field("col_type", static_cast<int64_t>(col_meta.type))
          .Field("remaining_bytes", static_cast<int64_t>(end - ptr))
          .Field("is_null", is_null)
          .Debug();
    }

    if (!is_null && ptr >= end) {
      std::string msg = std::string(event_type_label) + " event truncated at column " + std::to_string(col_idx);
      if (image_label[0] != '\0') {
        msg += std::string(" (") + image_label + " image)";
      }
      mygram::utils::StructuredLog()
          .Event("mysql_binlog_error")
          .Field("type", std::string(event_type_label) + "_truncated")
          .Field("column_index", col_idx)
          .Error();
      return MakeUnexpected(MakeError(ErrorCode::kMySQLFieldTruncated, msg));
    }

    // Decode field value
    auto value_result = DecodeFieldValue(static_cast<uint8_t>(col_meta.type), ptr, col_meta.metadata, is_null, end,
                                         col_meta.is_unsigned);
    if (!value_result) {
      mygram::utils::StructuredLog()
          .Event("mysql_binlog_error")
          .Field("type", "field_decode_error")
          .Field("event_type", event_type_label)
          .Field("column_index", col_idx)
          .Field("error", value_result.error().message())
          .Error();
      return MakeUnexpected(value_result.error());
    }
    std::string value = *value_result;

    // Check pointer validity after decode
    if (ptr > end) {
      std::string msg =
          std::string(event_type_label) + " exceeded buffer after decode at column " + std::to_string(col_idx);
      if (image_label[0] != '\0') {
        msg += std::string(" (") + image_label + " image)";
      }
      return MakeUnexpected(MakeError(ErrorCode::kMySQLFieldTruncated, msg));
    }

    row.column_values[col_idx] = std::move(value);
    row.column_values_present[col_idx] = true;
    row.column_values_null[col_idx] = is_null;
    if (is_null) {
      row.null_columns.insert(col_meta.name);
    }
    const std::string& stored_value = row.column_values[col_idx];

    // Check if this is the primary key or text column (using cached indices)
    if (static_cast<int>(col_idx) == pk_col_idx) {
      row.primary_key = stored_value;
      if (mygram::utils::IsDebugLogEnabled()) {
        mygram::utils::StructuredLog()
            .Event("binlog_debug")
            .Field("action", "set_pk")
            .Field("value", stored_value)
            .Debug();
      }
    }
    if (static_cast<int>(col_idx) == text_col_idx) {
      row.text = stored_value;
    }

    // Advance pointer by field size (if not NULL)
    if (!is_null) {
      uint32_t field_size = binlog_util::calc_field_size(static_cast<uint8_t>(col_meta.type), ptr, col_meta.metadata);
      if (field_size == 0) {
        mygram::utils::StructuredLog()
            .Event("mysql_binlog_warning")
            .Field("type", "unsupported_column_type")
            .Field("event_type", event_type_label)
            .Field("column_type", static_cast<int64_t>(col_meta.type))
            .Field("column_name", col_meta.name)
            .Warn();
        std::string msg = std::string("Unsupported column type in ") + event_type_label + " event";
        if (image_label[0] != '\0') {
          msg += std::string(" ") + image_label + " image";
        }
        return MakeUnexpected(MakeError(ErrorCode::kMySQLUnsupportedType, msg, col_meta.name));
      }
      if (ptr + field_size > end) {
        mygram::utils::StructuredLog()
            .Event("mysql_binlog_error")
            .Field("type", "field_size_exceeds_buffer")
            .Field("event_type", event_type_label)
            .Field("field_size", static_cast<uint64_t>(field_size))
            .Field("remaining_bytes", static_cast<int64_t>(end - ptr))
            .Error();
        std::string msg = std::string("Field size exceeds buffer in ") + event_type_label + " event";
        if (image_label[0] != '\0') {
          msg += std::string(" ") + image_label + " image";
        }
        return MakeUnexpected(MakeError(ErrorCode::kMySQLFieldTruncated, msg, col_meta.name));
      }
      if (mygram::utils::IsDebugLogEnabled()) {
        mygram::utils::StructuredLog()
            .Event("binlog_debug")
            .Field("action", "decoded_value")
            .Field("value_preview", stored_value.size() > 50 ? stored_value.substr(0, 50) + "..." : stored_value)
            .Field("field_size", static_cast<uint64_t>(field_size))
            .Debug();
      }
      ptr += field_size;
    } else {
      if (mygram::utils::IsDebugLogEnabled()) {
        mygram::utils::StructuredLog().Event("binlog_debug").Field("action", "column_is_null").Debug();
      }
    }
  }

  return SingleRowResult{std::move(row), ptr};
}

// ---------------------------------------------------------------------------
// ParseWriteRowsEvent
// ---------------------------------------------------------------------------

mygram::utils::Expected<std::vector<RowData>, mygram::utils::Error> ParseWriteRowsEvent(
    const unsigned char* buffer, unsigned long length, const TableMetadata* table_metadata,
    const std::string& pk_column_name, const std::string& text_column_name, MySQLBinlogEventType event_type) {
  using mygram::utils::ErrorCode;
  using mygram::utils::MakeError;
  using mygram::utils::MakeUnexpected;

  if ((buffer == nullptr) || (table_metadata == nullptr)) {
    return MakeUnexpected(
        MakeError(ErrorCode::kMySQLBinlogError, "Null buffer or table_metadata in ParseWriteRowsEvent"));
  }

  try {
    auto header = internal::ParseRowsEventHeader(buffer, length, event_type, table_metadata, pk_column_name,
                                                 text_column_name, "write_rows");
    if (!header) {
      return MakeUnexpected(header.error());
    }

    const unsigned char* ptr = header->row_data_ptr;
    const unsigned char* end = header->end;

    // Parse rows
    std::vector<RowData> rows;
    size_t estimated_rows = (end - ptr) / mygram::constants::kEstimatedBytesPerBinlogRow;
    if (estimated_rows > 0 && estimated_rows < mygram::constants::kMaxPreReserveRows) {
      rows.reserve(estimated_rows);
    }

    while (ptr < end) {
      auto result =
          internal::ParseSingleRow(ptr, end, table_metadata, header->columns_present, header->bitmap_size,
                                   header->column_count, header->pk_col_idx, header->text_col_idx, "WRITE_ROWS", "");
      if (!result) {
        // Truncation at null bitmap boundary means we reached end of rows
        if (result.error().code() == ErrorCode::kMySQLFieldTruncated && ptr + header->bitmap_size > end) {
          break;
        }
        return MakeUnexpected(result.error());
      }
      ptr = result->next_ptr;
      rows.push_back(std::move(result->row));
    }

    mygram::utils::StructuredLog()
        .Event("binlog_debug")
        .Field("action", "parsed_write_rows")
        .Field("rows", static_cast<uint64_t>(rows.size()))
        .Field("database", table_metadata->database_name)
        .Field("table", table_metadata->table_name)
        .Debug();

    return rows;

  } catch (const std::exception& e) {
    mygram::utils::StructuredLog()
        .Event("mysql_binlog_error")
        .Field("type", "parse_exception")
        .Field("event_type", "write_rows")
        .Field("error", e.what())
        .Error();
    return MakeUnexpected(
        MakeError(ErrorCode::kMySQLBinlogError, std::string("Exception parsing WRITE_ROWS event: ") + e.what()));
  }
}

// ---------------------------------------------------------------------------
// ParseUpdateRowsEvent
// ---------------------------------------------------------------------------

mygram::utils::Expected<std::vector<std::pair<RowData, RowData>>, mygram::utils::Error> ParseUpdateRowsEvent(
    const unsigned char* buffer, unsigned long length, const TableMetadata* table_metadata,
    const std::string& pk_column_name, const std::string& text_column_name, MySQLBinlogEventType event_type) {
  using mygram::utils::ErrorCode;
  using mygram::utils::MakeError;
  using mygram::utils::MakeUnexpected;

  if ((buffer == nullptr) || (table_metadata == nullptr)) {
    return MakeUnexpected(
        MakeError(ErrorCode::kMySQLBinlogError, "Null buffer or table_metadata in ParseUpdateRowsEvent"));
  }

  try {
    auto header = internal::ParseRowsEventHeader(buffer, length, event_type, table_metadata, pk_column_name,
                                                 text_column_name, "update_rows");
    if (!header) {
      return MakeUnexpected(header.error());
    }

    const unsigned char* ptr = header->row_data_ptr;
    const unsigned char* end = header->end;
    const size_t bitmap_size = header->bitmap_size;

    // UPDATE events have a second bitmap: columns_after_image
    if (ptr + bitmap_size > end) {
      mygram::utils::StructuredLog()
          .Event("mysql_binlog_error")
          .Field("type", "update_rows_too_short")
          .Field("section", "columns_after_image bitmap")
          .Error();
      return MakeUnexpected(
          MakeError(ErrorCode::kMySQLBinlogError, "UPDATE_ROWS event too short for columns_after_image bitmap"));
    }
    const unsigned char* columns_after = ptr;
    ptr += bitmap_size;
    if (!AllColumnsPresent(columns_after, header->column_count)) {
      mygram::utils::StructuredLog()
          .Event("mysql_binlog_error")
          .Field("type", "partial_columns_after_image_bitmap")
          .Field("event_type", "update_rows")
          .Field("required", "binlog_row_image=FULL")
          .Error();
      return MakeUnexpected(MakeError(ErrorCode::kMySQLBinlogError,
                                      "UPDATE_ROWS event: partial columns_after_image bitmap requires "
                                      "binlog_row_image=FULL"));
    }

    // Parse rows (each row has before and after images)
    std::vector<std::pair<RowData, RowData>> row_pairs;
    size_t estimated_pairs = (end - ptr) / (2 * mygram::constants::kEstimatedBytesPerBinlogRow);
    if (estimated_pairs > 0 && estimated_pairs < mygram::constants::kMaxPreReserveRows) {
      row_pairs.reserve(estimated_pairs);
    }

    mygram::utils::StructuredLog()
        .Event("binlog_debug")
        .Field("action", "starting_row_parsing")
        .Field("ptr_offset", static_cast<int64_t>(ptr - buffer))
        .Field("end_offset", static_cast<int64_t>(end - buffer))
        .Field("available_bytes", static_cast<int64_t>(end - ptr))
        .Debug();

    while (ptr < end) {
      if (mygram::utils::IsDebugLogEnabled()) {
        mygram::utils::StructuredLog()
            .Event("binlog_debug")
            .Field("action", "row_start")
            .Field("ptr_offset", static_cast<int64_t>(ptr - buffer))
            .Field("null_bitmap_size", static_cast<uint64_t>(header->bitmap_size))
            .Debug();
      }

      // Parse before image
      auto before_result = internal::ParseSingleRow(ptr, end, table_metadata, header->columns_present,
                                                    header->bitmap_size, header->column_count, header->pk_col_idx,
                                                    header->text_col_idx, "UPDATE_ROWS", "before");
      if (!before_result) {
        // Truncation in UPDATE is treated as end-of-rows (lenient)
        if (before_result.error().code() == ErrorCode::kMySQLFieldTruncated) {
          if (mygram::utils::IsDebugLogEnabled()) {
            mygram::utils::StructuredLog()
                .Event("binlog_debug")
                .Field("action", "parse_ended_early_before_image")
                .Field("error", before_result.error().message())
                .Debug();
          }
          break;
        }
        return MakeUnexpected(before_result.error());
      }
      ptr = before_result->next_ptr;

      // Parse after image
      auto after_result =
          internal::ParseSingleRow(ptr, end, table_metadata, columns_after, header->bitmap_size, header->column_count,
                                   header->pk_col_idx, header->text_col_idx, "UPDATE_ROWS", "after");
      if (!after_result) {
        // Truncation in UPDATE is treated as end-of-rows (lenient)
        if (after_result.error().code() == ErrorCode::kMySQLFieldTruncated) {
          if (mygram::utils::IsDebugLogEnabled()) {
            mygram::utils::StructuredLog()
                .Event("binlog_debug")
                .Field("action", "parse_ended_early_after_image")
                .Field("error", after_result.error().message())
                .Debug();
          }
          break;
        }
        return MakeUnexpected(after_result.error());
      }
      ptr = after_result->next_ptr;

      row_pairs.emplace_back(std::move(before_result->row), std::move(after_result->row));
    }

    mygram::utils::StructuredLog()
        .Event("binlog_debug")
        .Field("action", "parsed_update_rows")
        .Field("row_pairs", static_cast<uint64_t>(row_pairs.size()))
        .Field("database", table_metadata->database_name)
        .Field("table", table_metadata->table_name)
        .Debug();

    return row_pairs;

  } catch (const std::exception& e) {
    mygram::utils::StructuredLog()
        .Event("mysql_binlog_error")
        .Field("type", "parse_exception")
        .Field("event_type", "update_rows")
        .Field("error", e.what())
        .Error();
    return MakeUnexpected(
        MakeError(ErrorCode::kMySQLBinlogError, std::string("Exception parsing UPDATE_ROWS event: ") + e.what()));
  }
}

// ---------------------------------------------------------------------------
// ParseDeleteRowsEvent
// ---------------------------------------------------------------------------

mygram::utils::Expected<std::vector<RowData>, mygram::utils::Error> ParseDeleteRowsEvent(
    const unsigned char* buffer, unsigned long length, const TableMetadata* table_metadata,
    const std::string& pk_column_name, const std::string& text_column_name, MySQLBinlogEventType event_type) {
  using mygram::utils::ErrorCode;
  using mygram::utils::MakeError;
  using mygram::utils::MakeUnexpected;

  if ((buffer == nullptr) || (table_metadata == nullptr)) {
    return MakeUnexpected(
        MakeError(ErrorCode::kMySQLBinlogError, "Null buffer or table_metadata in ParseDeleteRowsEvent"));
  }

  try {
    auto header = internal::ParseRowsEventHeader(buffer, length, event_type, table_metadata, pk_column_name,
                                                 text_column_name, "delete_rows");
    if (!header) {
      return MakeUnexpected(header.error());
    }

    const unsigned char* ptr = header->row_data_ptr;
    const unsigned char* end = header->end;

    // Parse rows
    std::vector<RowData> rows;
    size_t estimated_rows = (end - ptr) / mygram::constants::kEstimatedBytesPerBinlogRow;
    if (estimated_rows > 0 && estimated_rows < mygram::constants::kMaxPreReserveRows) {
      rows.reserve(estimated_rows);
    }

    while (ptr < end) {
      auto result =
          internal::ParseSingleRow(ptr, end, table_metadata, header->columns_present, header->bitmap_size,
                                   header->column_count, header->pk_col_idx, header->text_col_idx, "DELETE_ROWS", "");
      if (!result) {
        // Truncation at null bitmap boundary means we reached end of rows
        if (result.error().code() == ErrorCode::kMySQLFieldTruncated && ptr + header->bitmap_size > end) {
          break;
        }
        return MakeUnexpected(result.error());
      }
      ptr = result->next_ptr;
      rows.push_back(std::move(result->row));
    }

    mygram::utils::StructuredLog()
        .Event("binlog_debug")
        .Field("action", "parsed_delete_rows")
        .Field("rows", static_cast<uint64_t>(rows.size()))
        .Field("database", table_metadata->database_name)
        .Field("table", table_metadata->table_name)
        .Debug();

    return rows;

  } catch (const std::exception& e) {
    mygram::utils::StructuredLog()
        .Event("mysql_binlog_error")
        .Field("type", "parse_exception")
        .Field("event_type", "delete_rows")
        .Field("error", e.what())
        .Error();
    return MakeUnexpected(
        MakeError(ErrorCode::kMySQLBinlogError, std::string("Exception parsing DELETE_ROWS event: ") + e.what()));
  }
}

}  // namespace mygramdb::mysql

// NOLINTEND(cppcoreguidelines-pro-*,cppcoreguidelines-avoid-*,readability-magic-numbers,readability-function-cognitive-complexity,readability-else-after-return)

#endif  // USE_MYSQL
