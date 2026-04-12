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

#include <spdlog/spdlog.h>

#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <sstream>

#include "mysql/binlog_util.h"
#include "mysql/rows_parser_internal.h"
#include "utils/structured_log.h"

#ifdef USE_MYSQL

// NOLINTBEGIN(cppcoreguidelines-pro-*,cppcoreguidelines-avoid-*,readability-magic-numbers,readability-function-cognitive-complexity,readability-else-after-return)

namespace mygramdb::mysql {

std::optional<std::vector<RowData>> ParseWriteRowsEvent(const unsigned char* buffer, unsigned long length,
                                                        const TableMetadata* table_metadata,
                                                        const std::string& pk_column_name,
                                                        const std::string& text_column_name,
                                                        MySQLBinlogEventType event_type) {
  if ((buffer == nullptr) || (table_metadata == nullptr)) {
    return std::nullopt;
  }

  try {
    // binlog_reader already skipped OK byte, buffer points to event data
    // Event size is at bytes [9-12] of event data (little-endian)
    // (see mysql-8.4.7/libs/mysql/binlog/event/binlog_event.h: LOG_EVENT_HEADER_LEN)
    uint32_t event_size = static_cast<uint32_t>(buffer[9]) | (static_cast<uint32_t>(buffer[10]) << 8) |
                          (static_cast<uint32_t>(buffer[11]) << 16) | (static_cast<uint32_t>(buffer[12]) << 24);

    // Validate event_size before computing end pointer to prevent underflow/OOB
    if (event_size < 4 || event_size > length) {
      return std::nullopt;
    }

    const unsigned char* ptr = buffer + 19;  // Skip standard header (LOG_EVENT_HEADER_LEN = 19)

    // IMPORTANT: Event size includes header + data + 4-byte CRC32 checksum at the end.
    // Even when checksums are disabled via SET @source_binlog_checksum='NONE', MySQL still
    // includes 4 bytes at the end of each event for checksum space.
    // (see mysql-8.4.7/libs/mysql/binlog/event/binlog_event.h: BINLOG_CHECKSUM_LEN = 4)
    // We must exclude these 4 bytes when calculating the end of parseable data.
    const unsigned char* end = buffer + event_size - 4;  // Exclude 4-byte checksum

    // Parse post-header
    if (ptr + 8 > end) {
      mygram::utils::StructuredLog()
          .Event("mysql_binlog_error")
          .Field("type", "write_rows_too_short")
          .Field("section", "post-header")
          .Error();
      return std::nullopt;
    }

    // table_id (6 bytes) - already known from TABLE_MAP
    ptr += 6;

    // flags (2 bytes) - reserved for future use in write rows
    ptr += 2;

    // V2 Rows Events (type >= 30): always have var_header_len (uint16_t, minimum value 2)
    // V1 Rows Events (type 23-25): no var_header_len
    // (see mysql-8.4.7/libs/mysql/binlog/event/rows_event.h)
    bool is_v2 = (event_type == MySQLBinlogEventType::WRITE_ROWS_EVENT);
    if (is_v2) {
      if (ptr + 2 > end) {
        mygram::utils::StructuredLog()
            .Event("mysql_binlog_error")
            .Field("type", "write_rows_too_short")
            .Field("section", "var_header_len")
            .Error();
        return std::nullopt;
      }
      uint16_t var_header_len = binlog_util::uint2korr(ptr);
      if (var_header_len < 2 || ptr + var_header_len > end) {
        mygram::utils::StructuredLog()
            .Event("mysql_binlog_error")
            .Field("type", "invalid_var_header_len")
            .Field("event_type", "write_rows")
            .Field("var_header_len", static_cast<uint64_t>(var_header_len))
            .Error();
        return std::nullopt;
      }
      ptr += var_header_len;  // Skip entire var_header (len field + extra data)
    }

    // Parse body
    // width (packed integer) - number of columns
    if (ptr >= end) {
      mygram::utils::StructuredLog()
          .Event("mysql_binlog_error")
          .Field("type", "write_rows_too_short")
          .Field("section", "width")
          .Error();
      return std::nullopt;
    }
    uint64_t column_count = binlog_util::read_packed_integer(&ptr);

    if (column_count != table_metadata->columns.size()) {
      mygram::utils::StructuredLog()
          .Event("mysql_binlog_error")
          .Field("type", "column_count_mismatch")
          .Field("event_type", "write_rows")
          .Field("event_columns", column_count)
          .Field("table_columns", static_cast<uint64_t>(table_metadata->columns.size()))
          .Error();
      return std::nullopt;
    }

    // columns_present bitmap - which columns are in the event
    size_t bitmap_size = binlog_util::bitmap_bytes(column_count);
    if (ptr + bitmap_size > end) {
      mygram::utils::StructuredLog()
          .Event("mysql_binlog_error")
          .Field("type", "write_rows_too_short")
          .Field("section", "columns_present bitmap")
          .Error();
      return std::nullopt;
    }
    const unsigned char* columns_present = ptr;
    ptr += bitmap_size;

    // Pre-calculate bitmap sizes for better cache locality
    const size_t kNullBitmapSize = binlog_util::bitmap_bytes(column_count);

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

    // Parse rows
    std::vector<RowData> rows;
    // Reserve space for estimated rows (avg 100 bytes per row)
    size_t estimated_rows = (end - ptr) / 100;
    if (estimated_rows > 0 && estimated_rows < 10000) {
      rows.reserve(estimated_rows);
    }

    while (ptr < end) {
      RowData row;

      // NULL bitmap for this row
      if (ptr + kNullBitmapSize > end) {
        break;  // End of rows
      }
      const unsigned char* null_bitmap = ptr;
      ptr += kNullBitmapSize;

      // Parse each column value
      for (size_t col_idx = 0; col_idx < column_count; col_idx++) {
        // Check if column is present in this event
        if (!binlog_util::bitmap_is_set(columns_present, col_idx)) {
          continue;
        }

        const auto& col_meta = table_metadata->columns[col_idx];
        bool is_null = binlog_util::bitmap_is_set(null_bitmap, col_idx);

        if (ptr > end) {
          mygram::utils::StructuredLog()
              .Event("mysql_binlog_error")
              .Field("type", "write_rows_truncated")
              .Field("column_index", static_cast<uint64_t>(col_idx))
              .Error();
          return std::nullopt;
        }

        // Decode field value
        std::string value = internal::DecodeFieldValue(static_cast<uint8_t>(col_meta.type), ptr, col_meta.metadata,
                                                       is_null, end, col_meta.is_unsigned);

        // Store in row data
        row.columns[col_meta.name] = value;

        // Check if this is the primary key or text column (using cached indices)
        if (static_cast<int>(col_idx) == pk_col_idx) {
          row.primary_key = value;
        }
        if (static_cast<int>(col_idx) == text_col_idx) {
          row.text = value;
        }

        // Advance pointer by field size (if not NULL)
        if (!is_null) {
          uint32_t field_size =
              binlog_util::calc_field_size(static_cast<uint8_t>(col_meta.type), ptr, col_meta.metadata);
          if (field_size == 0) {
            mygram::utils::StructuredLog()
                .Event("mysql_binlog_warning")
                .Field("type", "unsupported_column_type")
                .Field("event_type", "write_rows")
                .Field("column_type", static_cast<int64_t>(col_meta.type))
                .Field("column_name", col_meta.name)
                .Warn();
            return std::nullopt;
          }
          ptr += field_size;
        }
      }

      rows.push_back(std::move(row));
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
    return std::nullopt;
  }
}

std::optional<std::vector<std::pair<RowData, RowData>>> ParseUpdateRowsEvent(
    const unsigned char* buffer, unsigned long length, const TableMetadata* table_metadata,
    const std::string& pk_column_name, const std::string& text_column_name, MySQLBinlogEventType event_type) {
  if ((buffer == nullptr) || (table_metadata == nullptr)) {
    return std::nullopt;
  }

  try {
    // binlog_reader already skipped OK byte, buffer points to event data.
    // Event size is at bytes [9-12] of event data (little-endian)
    // (see mysql-8.4.7/libs/mysql/binlog/event/binlog_event.h: LOG_EVENT_HEADER_LEN)
    uint32_t event_size = static_cast<uint32_t>(buffer[9]) | (static_cast<uint32_t>(buffer[10]) << 8) |
                          (static_cast<uint32_t>(buffer[11]) << 16) | (static_cast<uint32_t>(buffer[12]) << 24);

    // Validate event_size before computing end pointer to prevent underflow/OOB
    if (event_size < 4 || event_size > length) {
      return std::nullopt;
    }

    const unsigned char* ptr = buffer + 19;  // Skip standard header (LOG_EVENT_HEADER_LEN)
    // Event size includes header + data + 4-byte checksum (even when checksums are disabled)
    // (see mysql-8.4.7/libs/mysql/binlog/event/binlog_event.h: BINLOG_CHECKSUM_LEN = 4)
    const unsigned char* end = buffer + event_size - 4;  // Exclude 4-byte checksum

    mygram::utils::StructuredLog()
        .Event("binlog_debug")
        .Field("action", "update_rows_buffer")
        .Field("length_param", static_cast<uint64_t>(length))
        .Field("event_size", static_cast<uint64_t>(event_size))
        .Debug();

    // Parse post-header (same as WRITE_ROWS)
    if (ptr + 8 > end) {
      mygram::utils::StructuredLog()
          .Event("mysql_binlog_error")
          .Field("type", "update_rows_too_short")
          .Field("section", "post-header")
          .Error();
      return std::nullopt;
    }

    ptr += 6;  // table_id
    uint16_t flags = binlog_util::uint2korr(ptr);
    ptr += 2;  // flags

    mygram::utils::StructuredLog()
        .Event("binlog_debug")
        .Field("action", "update_rows_flags")
        .Field("flags", static_cast<uint64_t>(flags))
        .Field("ptr_offset", static_cast<int64_t>(ptr - buffer))
        .Debug();

    // V2 Rows Events (type >= 30): always have var_header_len (uint16_t, minimum value 2)
    // V1 Rows Events (type 23-25): no var_header_len
    // (see mysql-8.4.7/libs/mysql/binlog/event/rows_event.h)
    bool is_v2 = (event_type == MySQLBinlogEventType::UPDATE_ROWS_EVENT);
    if (is_v2) {
      if (ptr + 2 > end) {
        mygram::utils::StructuredLog()
            .Event("mysql_binlog_error")
            .Field("type", "update_rows_too_short")
            .Field("section", "var_header_len")
            .Error();
        return std::nullopt;
      }
      uint16_t var_header_len = binlog_util::uint2korr(ptr);
      if (var_header_len < 2 || ptr + var_header_len > end) {
        mygram::utils::StructuredLog()
            .Event("mysql_binlog_error")
            .Field("type", "invalid_var_header_len")
            .Field("event_type", "update_rows")
            .Field("var_header_len", static_cast<uint64_t>(var_header_len))
            .Error();
        return std::nullopt;
      }
      ptr += var_header_len;  // Skip entire var_header (len field + extra data)
    }

    // Parse body
    if (ptr >= end) {
      mygram::utils::StructuredLog()
          .Event("mysql_binlog_error")
          .Field("type", "update_rows_too_short")
          .Field("section", "width")
          .Error();
      return std::nullopt;
    }

    const unsigned char* column_count_ptr = ptr;  // Save position before read
    uint64_t column_count = binlog_util::read_packed_integer(&ptr);

    mygram::utils::StructuredLog()
        .Event("binlog_debug")
        .Field("action", "column_count_parsed")
        .Field("column_count", column_count)
        .Field("ptr_moved", static_cast<int64_t>(ptr - column_count_ptr))
        .Field("first_byte", static_cast<uint64_t>(*column_count_ptr))
        .Debug();

    if (column_count != table_metadata->columns.size()) {
      mygram::utils::StructuredLog()
          .Event("mysql_binlog_error")
          .Field("type", "column_count_mismatch")
          .Field("event_type", "update_rows")
          .Field("event_columns", column_count)
          .Field("table_columns", static_cast<uint64_t>(table_metadata->columns.size()))
          .Error();
      return std::nullopt;
    }

    // columns_before_image bitmap - which columns are in the before image
    size_t bitmap_size = binlog_util::bitmap_bytes(column_count);
    if (ptr + bitmap_size > end) {
      mygram::utils::StructuredLog()
          .Event("mysql_binlog_error")
          .Field("type", "update_rows_too_short")
          .Field("section", "columns_before_image bitmap")
          .Error();
      return std::nullopt;
    }
    const unsigned char* columns_before = ptr;
    ptr += bitmap_size;

    // columns_after_image bitmap - which columns are in the after image
    if (ptr + bitmap_size > end) {
      mygram::utils::StructuredLog()
          .Event("mysql_binlog_error")
          .Field("type", "update_rows_too_short")
          .Field("section", "columns_after_image bitmap")
          .Error();
      return std::nullopt;
    }
    const unsigned char* columns_after = ptr;
    ptr += bitmap_size;

    // NOTE: extra_row_info was already handled in the post-header section (lines 443-489)
    // No need to skip it again here

    // Pre-calculate bitmap size
    const size_t kNullBitmapSize = binlog_util::bitmap_bytes(column_count);

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

    // Parse rows (each row has before and after images)
    std::vector<std::pair<RowData, RowData>> row_pairs;
    // Reserve space for estimated row pairs (each pair is ~200 bytes)
    size_t estimated_pairs = (end - ptr) / 200;
    if (estimated_pairs > 0 && estimated_pairs < 10000) {
      row_pairs.reserve(estimated_pairs);
    }

    mygram::utils::StructuredLog()
        .Event("binlog_debug")
        .Field("action", "starting_row_parsing")
        .Field("ptr_offset", static_cast<int64_t>(ptr - buffer))
        .Field("end_offset", static_cast<int64_t>(end - buffer))
        .Field("available_bytes", static_cast<int64_t>(end - ptr))
        .Debug();

    bool parse_ended_early = false;

    while (ptr < end && !parse_ended_early) {
      RowData before_row;
      RowData after_row;

      mygram::utils::StructuredLog()
          .Event("binlog_debug")
          .Field("action", "row_start")
          .Field("ptr_offset", static_cast<int64_t>(ptr - buffer))
          .Field("null_bitmap_size", static_cast<uint64_t>(kNullBitmapSize))
          .Debug();

      // Parse before image
      if (ptr + kNullBitmapSize > end) {
        mygram::utils::StructuredLog()
            .Event("binlog_debug")
            .Field("action", "insufficient_space_for_null_bitmap")
            .Debug();
        break;
      }
      const unsigned char* null_bitmap_before = ptr;
      ptr += kNullBitmapSize;

      // Debug: Show which columns are in columns_before bitmap and null_bitmap_before
      std::string col_bitmap_str;
      std::string null_bitmap_str;
      for (size_t i = 0; i < column_count; i++) {
        col_bitmap_str += binlog_util::bitmap_is_set(columns_before, i) ? "1" : "0";
        null_bitmap_str += binlog_util::bitmap_is_set(null_bitmap_before, i) ? "N" : ".";
      }
      mygram::utils::StructuredLog()
          .Event("binlog_debug")
          .Field("action", "before_image_bitmaps")
          .Field("columns_bitmap", col_bitmap_str)
          .Field("null_bitmap", null_bitmap_str)
          .Debug();

      for (size_t col_idx = 0; col_idx < column_count; col_idx++) {
        if (!binlog_util::bitmap_is_set(columns_before, col_idx)) {
          mygram::utils::StructuredLog()
              .Event("binlog_debug")
              .Field("action", "column_not_in_bitmap")
              .Field("col_idx", static_cast<uint64_t>(col_idx))
              .Field("col_name", col_idx < table_metadata->columns.size() ? table_metadata->columns[col_idx].name : "?")
              .Debug();
          continue;
        }

        const auto& col_meta = table_metadata->columns[col_idx];
        bool is_null = binlog_util::bitmap_is_set(null_bitmap_before, col_idx);

        mygram::utils::StructuredLog()
            .Event("binlog_debug")
            .Field("action", "parsing_column")
            .Field("col_idx", static_cast<uint64_t>(col_idx))
            .Field("col_name", col_meta.name)
            .Field("col_type", static_cast<int64_t>(col_meta.type))
            .Field("ptr_offset", static_cast<int64_t>(ptr - buffer))
            .Field("remaining_bytes", static_cast<int64_t>(end - ptr))
            .Field("is_null", is_null)
            .Debug();

        // Check if we have data remaining before attempting to decode
        // NULL columns consume no buffer space, so ptr >= end is OK for them
        if (!is_null && ptr >= end) {
          mygram::utils::StructuredLog()
              .Event("binlog_debug")
              .Field("action", "reached_end_before_image")
              .Field("col_idx", static_cast<uint64_t>(col_idx))
              .Debug();
          parse_ended_early = true;
          break;
        }

        std::string value = internal::DecodeFieldValue(static_cast<uint8_t>(col_meta.type), ptr, col_meta.metadata,
                                                       is_null, end, col_meta.is_unsigned);

        // Check for truncation marker
        if (value == "[TRUNCATED]") {
          mygram::utils::StructuredLog()
              .Event("mysql_binlog_error")
              .Field("type", "field_truncation")
              .Field("event_type", "update_rows")
              .Field("image", "before")
              .Field("column_index", static_cast<uint64_t>(col_idx))
              .Error();
          return std::nullopt;
        }

        // Check again after decode, as DecodeFieldValue advances ptr
        if (ptr > end) {
          mygram::utils::StructuredLog()
              .Event("binlog_debug")
              .Field("action", "exceeded_end_after_decode")
              .Field("col_idx", static_cast<uint64_t>(col_idx))
              .Debug();
          parse_ended_early = true;
          break;
        }

        before_row.columns[col_meta.name] = value;

        // Check using cached indices
        if (static_cast<int>(col_idx) == pk_col_idx) {
          before_row.primary_key = value;
          mygram::utils::StructuredLog().Event("binlog_debug").Field("action", "set_pk").Field("value", value).Debug();
        }
        if (static_cast<int>(col_idx) == text_col_idx) {
          before_row.text = value;
        }

        if (!is_null) {
          uint32_t field_size =
              binlog_util::calc_field_size(static_cast<uint8_t>(col_meta.type), ptr, col_meta.metadata);
          if (field_size == 0) {
            mygram::utils::StructuredLog()
                .Event("mysql_binlog_warning")
                .Field("type", "unsupported_column_type")
                .Field("event_type", "update_rows")
                .Field("image", "before")
                .Field("column_type", static_cast<int64_t>(col_meta.type))
                .Field("column_name", col_meta.name)
                .Warn();
            return std::nullopt;
          }
          mygram::utils::StructuredLog()
              .Event("binlog_debug")
              .Field("action", "decoded_value")
              .Field("value_preview", value.size() > 50 ? value.substr(0, 50) + "..." : value)
              .Field("field_size", static_cast<uint64_t>(field_size))
              .Field("new_ptr_offset", static_cast<int64_t>((ptr + field_size) - buffer))
              .Debug();
          ptr += field_size;
        } else {
          mygram::utils::StructuredLog().Event("binlog_debug").Field("action", "column_is_null").Debug();
        }
      }

      if (parse_ended_early) {
        break;
      }

      // Parse after image
      if (ptr + kNullBitmapSize > end) {
        mygram::utils::StructuredLog()
            .Event("binlog_debug")
            .Field("action", "insufficient_space_for_after_null_bitmap")
            .Debug();
        break;
      }
      const unsigned char* null_bitmap_after = ptr;
      ptr += kNullBitmapSize;

      // Debug: Show which columns are in columns_after bitmap and null_bitmap_after
      std::string col_bitmap_after_str;
      std::string null_bitmap_after_str;
      for (size_t i = 0; i < column_count; i++) {
        col_bitmap_after_str += binlog_util::bitmap_is_set(columns_after, i) ? "1" : "0";
        null_bitmap_after_str += binlog_util::bitmap_is_set(null_bitmap_after, i) ? "N" : ".";
      }
      mygram::utils::StructuredLog()
          .Event("binlog_debug")
          .Field("action", "after_image_bitmaps")
          .Field("columns_bitmap", col_bitmap_after_str)
          .Field("null_bitmap", null_bitmap_after_str)
          .Debug();

      for (size_t col_idx = 0; col_idx < column_count; col_idx++) {
        if (!binlog_util::bitmap_is_set(columns_after, col_idx)) {
          mygram::utils::StructuredLog()
              .Event("binlog_debug")
              .Field("action", "column_not_in_after_bitmap")
              .Field("col_idx", static_cast<uint64_t>(col_idx))
              .Field("col_name", col_idx < table_metadata->columns.size() ? table_metadata->columns[col_idx].name : "?")
              .Debug();
          continue;
        }

        const auto& col_meta = table_metadata->columns[col_idx];
        bool is_null = binlog_util::bitmap_is_set(null_bitmap_after, col_idx);

        mygram::utils::StructuredLog()
            .Event("binlog_debug")
            .Field("action", "parsing_column")
            .Field("col_idx", static_cast<uint64_t>(col_idx))
            .Field("col_name", col_meta.name)
            .Field("col_type", static_cast<int64_t>(col_meta.type))
            .Field("ptr_offset", static_cast<int64_t>(ptr - buffer))
            .Field("remaining_bytes", static_cast<int64_t>(end - ptr))
            .Field("is_null", is_null)
            .Debug();

        // Check if we have data remaining before attempting to decode
        // NULL columns consume no buffer space, so ptr >= end is OK for them
        if (!is_null && ptr >= end) {
          mygram::utils::StructuredLog()
              .Event("binlog_debug")
              .Field("action", "reached_end_after_image")
              .Field("col_idx", static_cast<uint64_t>(col_idx))
              .Debug();
          parse_ended_early = true;
          break;
        }

        std::string value = internal::DecodeFieldValue(static_cast<uint8_t>(col_meta.type), ptr, col_meta.metadata,
                                                       is_null, end, col_meta.is_unsigned);

        // Check for truncation marker
        if (value == "[TRUNCATED]") {
          mygram::utils::StructuredLog()
              .Event("mysql_binlog_error")
              .Field("type", "field_truncation")
              .Field("event_type", "update_rows")
              .Field("image", "after")
              .Field("column_index", static_cast<uint64_t>(col_idx))
              .Error();
          return std::nullopt;
        }

        // Check again after decode
        if (ptr > end) {
          mygram::utils::StructuredLog()
              .Event("binlog_debug")
              .Field("action", "exceeded_end_after_decode_after_image")
              .Field("col_idx", static_cast<uint64_t>(col_idx))
              .Debug();
          parse_ended_early = true;
          break;
        }

        after_row.columns[col_meta.name] = value;

        // Check using cached indices
        if (static_cast<int>(col_idx) == pk_col_idx) {
          after_row.primary_key = value;
          mygram::utils::StructuredLog().Event("binlog_debug").Field("action", "set_pk").Field("value", value).Debug();
        }
        if (static_cast<int>(col_idx) == text_col_idx) {
          after_row.text = value;
        }

        if (!is_null) {
          uint32_t field_size =
              binlog_util::calc_field_size(static_cast<uint8_t>(col_meta.type), ptr, col_meta.metadata);
          if (field_size == 0) {
            mygram::utils::StructuredLog()
                .Event("mysql_binlog_warning")
                .Field("type", "unsupported_column_type")
                .Field("event_type", "update_rows")
                .Field("image", "after")
                .Field("column_type", static_cast<int64_t>(col_meta.type))
                .Field("column_name", col_meta.name)
                .Warn();
            return std::nullopt;
          }
          mygram::utils::StructuredLog()
              .Event("binlog_debug")
              .Field("action", "decoded_value")
              .Field("value_preview", value.size() > 50 ? value.substr(0, 50) + "..." : value)
              .Field("field_size", static_cast<uint64_t>(field_size))
              .Field("new_ptr_offset", static_cast<int64_t>((ptr + field_size) - buffer))
              .Debug();
          ptr += field_size;
        } else {
          mygram::utils::StructuredLog().Event("binlog_debug").Field("action", "column_is_null").Debug();
        }
      }

      if (!parse_ended_early) {
        row_pairs.emplace_back(std::move(before_row), std::move(after_row));
      }
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
    return std::nullopt;
  }
}

std::optional<std::vector<RowData>> ParseDeleteRowsEvent(const unsigned char* buffer, unsigned long length,
                                                         const TableMetadata* table_metadata,
                                                         const std::string& pk_column_name,
                                                         const std::string& text_column_name,
                                                         MySQLBinlogEventType event_type) {
  if ((buffer == nullptr) || (table_metadata == nullptr)) {
    return std::nullopt;
  }

  try {
    // binlog_reader already skipped OK byte, buffer points to event data
    // Event size is at bytes [9-12] of event data (little-endian)
    uint32_t event_size = static_cast<uint32_t>(buffer[9]) | (static_cast<uint32_t>(buffer[10]) << 8) |
                          (static_cast<uint32_t>(buffer[11]) << 16) | (static_cast<uint32_t>(buffer[12]) << 24);

    // Validate event_size before computing end pointer to prevent underflow/OOB
    if (event_size < 4 || event_size > length) {
      return std::nullopt;
    }

    const unsigned char* ptr = buffer + 19;  // Skip standard header (LOG_EVENT_HEADER_LEN)
    // Event size includes header + data + 4-byte checksum (even when checksums are disabled)
    const unsigned char* end = buffer + event_size - 4;  // Exclude 4-byte checksum

    // Parse post-header
    if (ptr + 8 > end) {
      mygram::utils::StructuredLog()
          .Event("mysql_binlog_error")
          .Field("type", "delete_rows_too_short")
          .Field("section", "post-header")
          .Error();
      return std::nullopt;
    }

    ptr += 6;  // table_id
    ptr += 2;  // flags - reserved for future use in delete rows

    // V2 Rows Events (type >= 30): always have var_header_len (uint16_t, minimum value 2)
    // V1 Rows Events (type 23-25): no var_header_len
    // (see mysql-8.4.7/libs/mysql/binlog/event/rows_event.h)
    bool is_v2 = (event_type == MySQLBinlogEventType::DELETE_ROWS_EVENT);
    if (is_v2) {
      if (ptr + 2 > end) {
        mygram::utils::StructuredLog()
            .Event("mysql_binlog_error")
            .Field("type", "delete_rows_too_short")
            .Field("section", "var_header_len")
            .Error();
        return std::nullopt;
      }
      uint16_t var_header_len = binlog_util::uint2korr(ptr);
      if (var_header_len < 2 || ptr + var_header_len > end) {
        mygram::utils::StructuredLog()
            .Event("mysql_binlog_error")
            .Field("type", "invalid_var_header_len")
            .Field("event_type", "delete_rows")
            .Field("var_header_len", static_cast<uint64_t>(var_header_len))
            .Error();
        return std::nullopt;
      }
      ptr += var_header_len;  // Skip entire var_header (len field + extra data)
    }

    // Parse body
    if (ptr >= end) {
      mygram::utils::StructuredLog()
          .Event("mysql_binlog_error")
          .Field("type", "delete_rows_too_short")
          .Field("section", "width")
          .Error();
      return std::nullopt;
    }
    uint64_t column_count = binlog_util::read_packed_integer(&ptr);

    if (column_count != table_metadata->columns.size()) {
      mygram::utils::StructuredLog()
          .Event("mysql_binlog_error")
          .Field("type", "column_count_mismatch")
          .Field("event_type", "delete_rows")
          .Field("event_columns", column_count)
          .Field("table_columns", static_cast<uint64_t>(table_metadata->columns.size()))
          .Error();
      return std::nullopt;
    }

    // columns_present bitmap - which columns are in the event (before image only)
    size_t bitmap_size = binlog_util::bitmap_bytes(column_count);
    if (ptr + bitmap_size > end) {
      mygram::utils::StructuredLog()
          .Event("mysql_binlog_error")
          .Field("type", "delete_rows_too_short")
          .Field("section", "columns_present bitmap")
          .Error();
      return std::nullopt;
    }
    const unsigned char* columns_present = ptr;
    ptr += bitmap_size;

    // Pre-calculate bitmap size
    const size_t kNullBitmapSize = binlog_util::bitmap_bytes(column_count);

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

    // Parse rows
    std::vector<RowData> rows;
    // Reserve space for estimated rows
    size_t estimated_rows = (end - ptr) / 100;
    if (estimated_rows > 0 && estimated_rows < 10000) {
      rows.reserve(estimated_rows);
    }

    while (ptr < end) {
      RowData row;

      // NULL bitmap for this row
      if (ptr + kNullBitmapSize > end) {
        break;
      }
      const unsigned char* null_bitmap = ptr;
      ptr += kNullBitmapSize;

      // Parse each column value
      for (size_t col_idx = 0; col_idx < column_count; col_idx++) {
        if (!binlog_util::bitmap_is_set(columns_present, col_idx)) {
          continue;
        }

        const auto& col_meta = table_metadata->columns[col_idx];
        bool is_null = binlog_util::bitmap_is_set(null_bitmap, col_idx);

        if (ptr > end) {
          mygram::utils::StructuredLog()
              .Event("mysql_binlog_error")
              .Field("type", "delete_rows_truncated")
              .Field("column_index", static_cast<uint64_t>(col_idx))
              .Error();
          return std::nullopt;
        }

        std::string value = internal::DecodeFieldValue(static_cast<uint8_t>(col_meta.type), ptr, col_meta.metadata,
                                                       is_null, end, col_meta.is_unsigned);

        // Check for truncation marker
        if (value == "[TRUNCATED]") {
          mygram::utils::StructuredLog()
              .Event("mysql_binlog_error")
              .Field("type", "field_truncation")
              .Field("event_type", "delete_rows")
              .Field("column_index", static_cast<uint64_t>(col_idx))
              .Error();
          return std::nullopt;
        }

        row.columns[col_meta.name] = value;

        // Check using cached indices
        if (static_cast<int>(col_idx) == pk_col_idx) {
          row.primary_key = value;
        }
        if (static_cast<int>(col_idx) == text_col_idx) {
          row.text = value;
        }

        if (!is_null) {
          uint32_t field_size =
              binlog_util::calc_field_size(static_cast<uint8_t>(col_meta.type), ptr, col_meta.metadata);
          if (field_size == 0) {
            mygram::utils::StructuredLog()
                .Event("mysql_binlog_warning")
                .Field("type", "unsupported_column_type")
                .Field("event_type", "delete_rows")
                .Field("column_type", static_cast<int64_t>(col_meta.type))
                .Field("column_name", col_meta.name)
                .Warn();
            return std::nullopt;
          }
          ptr += field_size;
        }
      }

      rows.push_back(std::move(row));
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
    return std::nullopt;
  }
}

}  // namespace mygramdb::mysql

// NOLINTEND(cppcoreguidelines-pro-*,cppcoreguidelines-avoid-*,readability-magic-numbers,readability-function-cognitive-complexity,readability-else-after-return)

#endif  // USE_MYSQL
