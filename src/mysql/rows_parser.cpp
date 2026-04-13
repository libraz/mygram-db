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

}  // namespace

// ---------------------------------------------------------------------------
// ParseRowsEventHeader -- shared prologue for all three row-event parsers
// ---------------------------------------------------------------------------

std::optional<internal::RowsEventHeader> internal::ParseRowsEventHeader(
    const unsigned char* buffer, unsigned long length, MySQLBinlogEventType event_type,
    const TableMetadata* table_metadata, const std::string& pk_column_name, const std::string& text_column_name,
    const char* event_type_label) {
  // Event size is at bytes [9-12] of event data (little-endian)
  // (see mysql-8.4.7/libs/mysql/binlog/event/binlog_event.h: LOG_EVENT_HEADER_LEN)
  uint32_t event_size = static_cast<uint32_t>(buffer[9]) | (static_cast<uint32_t>(buffer[10]) << 8) |
                        (static_cast<uint32_t>(buffer[11]) << 16) | (static_cast<uint32_t>(buffer[12]) << 24);

  // Validate event_size before computing end pointer to prevent underflow/OOB
  if (event_size < 4 || event_size > length) {
    return std::nullopt;
  }

  const unsigned char* ptr = buffer + mygram::constants::kBinlogEventHeaderLen;  // Skip standard header
  // IMPORTANT: Event size includes header + data + 4-byte CRC32 checksum at the end.
  // Even when checksums are disabled via SET @source_binlog_checksum='NONE', MySQL still
  // includes 4 bytes at the end of each event for checksum space.
  // (see mysql-8.4.7/libs/mysql/binlog/event/binlog_event.h: BINLOG_CHECKSUM_LEN = 4)
  const unsigned char* end = buffer + event_size - 4;  // Exclude 4-byte checksum

  // Parse post-header: table_id (6 bytes) + flags (2 bytes)
  if (ptr + 8 > end) {
    mygram::utils::StructuredLog()
        .Event("mysql_binlog_error")
        .Field("type", std::string(event_type_label) + "_too_short")
        .Field("section", "post-header")
        .Error();
    return std::nullopt;
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
      return std::nullopt;
    }
    uint16_t var_header_len = binlog_util::uint2korr(ptr);
    if (var_header_len < 2 || ptr + var_header_len > end) {
      mygram::utils::StructuredLog()
          .Event("mysql_binlog_error")
          .Field("type", "invalid_var_header_len")
          .Field("event_type", event_type_label)
          .Field("var_header_len", static_cast<uint64_t>(var_header_len))
          .Error();
      return std::nullopt;
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
    return std::nullopt;
  }
  uint64_t column_count = binlog_util::read_packed_integer(&ptr);

  if (column_count != table_metadata->columns.size()) {
    mygram::utils::StructuredLog()
        .Event("mysql_binlog_error")
        .Field("type", "column_count_mismatch")
        .Field("event_type", event_type_label)
        .Field("event_columns", column_count)
        .Field("table_columns", static_cast<uint64_t>(table_metadata->columns.size()))
        .Error();
    return std::nullopt;
  }

  // columns_present bitmap (first bitmap -- "before image" for UPDATE, only bitmap for WRITE/DELETE)
  size_t bitmap_size = binlog_util::bitmap_bytes(column_count);
  if (ptr + bitmap_size > end) {
    mygram::utils::StructuredLog()
        .Event("mysql_binlog_error")
        .Field("type", std::string(event_type_label) + "_too_short")
        .Field("section", "columns_present bitmap")
        .Error();
    return std::nullopt;
  }
  const unsigned char* columns_present = ptr;
  ptr += bitmap_size;

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
      ptr,                                      // row_data_ptr (or second-bitmap start for UPDATE)
      end,                                      // end
      column_count,                             // column_count
      columns_present,                          // columns_present
      bitmap_size,                              // bitmap_size
      binlog_util::bitmap_bytes(column_count),  // null_bitmap_size
      pk_col_idx,                               // pk_col_idx
      text_col_idx,                             // text_col_idx
  };
}

// ---------------------------------------------------------------------------
// ParseWriteRowsEvent
// ---------------------------------------------------------------------------

std::optional<std::vector<RowData>> ParseWriteRowsEvent(const unsigned char* buffer, unsigned long length,
                                                        const TableMetadata* table_metadata,
                                                        const std::string& pk_column_name,
                                                        const std::string& text_column_name,
                                                        MySQLBinlogEventType event_type) {
  if ((buffer == nullptr) || (table_metadata == nullptr)) {
    return std::nullopt;
  }

  try {
    auto header = internal::ParseRowsEventHeader(buffer, length, event_type, table_metadata, pk_column_name,
                                                 text_column_name, "write_rows");
    if (!header) {
      return std::nullopt;
    }

    const unsigned char* ptr = header->row_data_ptr;
    const unsigned char* end = header->end;
    const uint64_t column_count = header->column_count;
    const unsigned char* columns_present = header->columns_present;
    const size_t kNullBitmapSize = header->null_bitmap_size;
    const int pk_col_idx = header->pk_col_idx;
    const int text_col_idx = header->text_col_idx;

    // Parse rows
    std::vector<RowData> rows;
    // Reserve space for estimated rows
    size_t estimated_rows = (end - ptr) / mygram::constants::kEstimatedBytesPerBinlogRow;
    if (estimated_rows > 0 && estimated_rows < mygram::constants::kMaxPreReserveRows) {
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

        if (ptr >= end) {
          mygram::utils::StructuredLog()
              .Event("mysql_binlog_error")
              .Field("type", "write_rows_truncated")
              .Field("column_index", static_cast<uint64_t>(col_idx))
              .Error();
          return std::nullopt;
        }

        // Decode field value
        auto value_result = internal::DecodeFieldValue(static_cast<uint8_t>(col_meta.type), ptr, col_meta.metadata,
                                                       is_null, end, col_meta.is_unsigned);
        if (!value_result) {
          mygram::utils::StructuredLog()
              .Event("mysql_binlog_error")
              .Field("type", "field_decode_error")
              .Field("event_type", "write_rows")
              .Field("column_index", static_cast<uint64_t>(col_idx))
              .Field("error", value_result.error().message())
              .Error();
          return std::nullopt;
        }
        std::string value = *value_result;

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
          if (ptr + field_size > end) {
            mygram::utils::StructuredLog()
                .Event("mysql_binlog_error")
                .Field("type", "field_size_exceeds_buffer")
                .Field("event_type", "write_rows")
                .Field("field_size", static_cast<uint64_t>(field_size))
                .Field("remaining_bytes", static_cast<int64_t>(end - ptr))
                .Error();
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

// ---------------------------------------------------------------------------
// ParseUpdateRowsEvent
// ---------------------------------------------------------------------------

std::optional<std::vector<std::pair<RowData, RowData>>> ParseUpdateRowsEvent(
    const unsigned char* buffer, unsigned long length, const TableMetadata* table_metadata,
    const std::string& pk_column_name, const std::string& text_column_name, MySQLBinlogEventType event_type) {
  if ((buffer == nullptr) || (table_metadata == nullptr)) {
    return std::nullopt;
  }

  try {
    auto header = internal::ParseRowsEventHeader(buffer, length, event_type, table_metadata, pk_column_name,
                                                 text_column_name, "update_rows");
    if (!header) {
      return std::nullopt;
    }

    const unsigned char* ptr = header->row_data_ptr;
    const unsigned char* end = header->end;
    const uint64_t column_count = header->column_count;
    const unsigned char* columns_before = header->columns_present;
    const size_t bitmap_size = header->bitmap_size;
    const size_t kNullBitmapSize = header->null_bitmap_size;
    const int pk_col_idx = header->pk_col_idx;
    const int text_col_idx = header->text_col_idx;

    // UPDATE events have a second bitmap: columns_after_image
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

    // Parse rows (each row has before and after images)
    std::vector<std::pair<RowData, RowData>> row_pairs;
    // Reserve space for estimated row pairs (each pair is ~2x a single row)
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

    bool parse_ended_early = false;

    while (ptr < end && !parse_ended_early) {
      RowData before_row;
      RowData after_row;

      if (spdlog::should_log(spdlog::level::debug)) {
        mygram::utils::StructuredLog()
            .Event("binlog_debug")
            .Field("action", "row_start")
            .Field("ptr_offset", static_cast<int64_t>(ptr - buffer))
            .Field("null_bitmap_size", static_cast<uint64_t>(kNullBitmapSize))
            .Debug();
      }

      // Parse before image
      if (ptr + kNullBitmapSize > end) {
        if (spdlog::should_log(spdlog::level::debug)) {
          mygram::utils::StructuredLog()
              .Event("binlog_debug")
              .Field("action", "insufficient_space_for_null_bitmap")
              .Debug();
        }
        break;
      }
      const unsigned char* null_bitmap_before = ptr;
      ptr += kNullBitmapSize;

      // Debug: Show which columns are in columns_before bitmap and null_bitmap_before
      if (spdlog::should_log(spdlog::level::debug)) {
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
      }

      for (size_t col_idx = 0; col_idx < column_count; col_idx++) {
        if (!binlog_util::bitmap_is_set(columns_before, col_idx)) {
          if (spdlog::should_log(spdlog::level::debug)) {
            mygram::utils::StructuredLog()
                .Event("binlog_debug")
                .Field("action", "column_not_in_bitmap")
                .Field("col_idx", static_cast<uint64_t>(col_idx))
                .Field("col_name",
                       col_idx < table_metadata->columns.size() ? table_metadata->columns[col_idx].name : "?")
                .Debug();
          }
          continue;
        }

        const auto& col_meta = table_metadata->columns[col_idx];
        bool is_null = binlog_util::bitmap_is_set(null_bitmap_before, col_idx);

        if (spdlog::should_log(spdlog::level::debug)) {
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
        }

        // Check if we have data remaining before attempting to decode
        // NULL columns consume no buffer space, so ptr >= end is OK for them
        if (!is_null && ptr >= end) {
          if (spdlog::should_log(spdlog::level::debug)) {
            mygram::utils::StructuredLog()
                .Event("binlog_debug")
                .Field("action", "reached_end_before_image")
                .Field("col_idx", static_cast<uint64_t>(col_idx))
                .Debug();
          }
          parse_ended_early = true;
          break;
        }

        auto value_result = internal::DecodeFieldValue(static_cast<uint8_t>(col_meta.type), ptr, col_meta.metadata,
                                                       is_null, end, col_meta.is_unsigned);
        if (!value_result) {
          mygram::utils::StructuredLog()
              .Event("mysql_binlog_error")
              .Field("type", "field_decode_error")
              .Field("event_type", "update_rows")
              .Field("image", "before")
              .Field("column_index", static_cast<uint64_t>(col_idx))
              .Field("error", value_result.error().message())
              .Error();
          return std::nullopt;
        }
        std::string value = *value_result;

        // Check again after decode, as DecodeFieldValue advances ptr
        if (ptr > end) {
          if (spdlog::should_log(spdlog::level::debug)) {
            mygram::utils::StructuredLog()
                .Event("binlog_debug")
                .Field("action", "exceeded_end_after_decode")
                .Field("col_idx", static_cast<uint64_t>(col_idx))
                .Debug();
          }
          parse_ended_early = true;
          break;
        }

        before_row.columns[col_meta.name] = value;

        // Check using cached indices
        if (static_cast<int>(col_idx) == pk_col_idx) {
          before_row.primary_key = value;
          if (spdlog::should_log(spdlog::level::debug)) {
            mygram::utils::StructuredLog()
                .Event("binlog_debug")
                .Field("action", "set_pk")
                .Field("value", value)
                .Debug();
          }
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
          if (ptr + field_size > end) {
            mygram::utils::StructuredLog()
                .Event("mysql_binlog_error")
                .Field("type", "field_size_exceeds_buffer")
                .Field("event_type", "update_rows")
                .Field("image", "before")
                .Field("field_size", static_cast<uint64_t>(field_size))
                .Field("remaining_bytes", static_cast<int64_t>(end - ptr))
                .Error();
            return std::nullopt;
          }
          if (spdlog::should_log(spdlog::level::debug)) {
            mygram::utils::StructuredLog()
                .Event("binlog_debug")
                .Field("action", "decoded_value")
                .Field("value_preview", value.size() > 50 ? value.substr(0, 50) + "..." : value)
                .Field("field_size", static_cast<uint64_t>(field_size))
                .Field("new_ptr_offset", static_cast<int64_t>((ptr + field_size) - buffer))
                .Debug();
          }
          ptr += field_size;
        } else {
          if (spdlog::should_log(spdlog::level::debug)) {
            mygram::utils::StructuredLog().Event("binlog_debug").Field("action", "column_is_null").Debug();
          }
        }
      }

      if (parse_ended_early) {
        break;
      }

      // Parse after image
      if (ptr + kNullBitmapSize > end) {
        if (spdlog::should_log(spdlog::level::debug)) {
          mygram::utils::StructuredLog()
              .Event("binlog_debug")
              .Field("action", "insufficient_space_for_after_null_bitmap")
              .Debug();
        }
        break;
      }
      const unsigned char* null_bitmap_after = ptr;
      ptr += kNullBitmapSize;

      // Debug: Show which columns are in columns_after bitmap and null_bitmap_after
      if (spdlog::should_log(spdlog::level::debug)) {
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
      }

      for (size_t col_idx = 0; col_idx < column_count; col_idx++) {
        if (!binlog_util::bitmap_is_set(columns_after, col_idx)) {
          if (spdlog::should_log(spdlog::level::debug)) {
            mygram::utils::StructuredLog()
                .Event("binlog_debug")
                .Field("action", "column_not_in_after_bitmap")
                .Field("col_idx", static_cast<uint64_t>(col_idx))
                .Field("col_name",
                       col_idx < table_metadata->columns.size() ? table_metadata->columns[col_idx].name : "?")
                .Debug();
          }
          continue;
        }

        const auto& col_meta = table_metadata->columns[col_idx];
        bool is_null = binlog_util::bitmap_is_set(null_bitmap_after, col_idx);

        if (spdlog::should_log(spdlog::level::debug)) {
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
        }

        // Check if we have data remaining before attempting to decode
        // NULL columns consume no buffer space, so ptr >= end is OK for them
        if (!is_null && ptr >= end) {
          if (spdlog::should_log(spdlog::level::debug)) {
            mygram::utils::StructuredLog()
                .Event("binlog_debug")
                .Field("action", "reached_end_after_image")
                .Field("col_idx", static_cast<uint64_t>(col_idx))
                .Debug();
          }
          parse_ended_early = true;
          break;
        }

        auto value_result = internal::DecodeFieldValue(static_cast<uint8_t>(col_meta.type), ptr, col_meta.metadata,
                                                       is_null, end, col_meta.is_unsigned);
        if (!value_result) {
          mygram::utils::StructuredLog()
              .Event("mysql_binlog_error")
              .Field("type", "field_decode_error")
              .Field("event_type", "update_rows")
              .Field("image", "after")
              .Field("column_index", static_cast<uint64_t>(col_idx))
              .Field("error", value_result.error().message())
              .Error();
          return std::nullopt;
        }
        std::string value = *value_result;

        // Check again after decode
        if (ptr > end) {
          if (spdlog::should_log(spdlog::level::debug)) {
            mygram::utils::StructuredLog()
                .Event("binlog_debug")
                .Field("action", "exceeded_end_after_decode_after_image")
                .Field("col_idx", static_cast<uint64_t>(col_idx))
                .Debug();
          }
          parse_ended_early = true;
          break;
        }

        after_row.columns[col_meta.name] = value;

        // Check using cached indices
        if (static_cast<int>(col_idx) == pk_col_idx) {
          after_row.primary_key = value;
          if (spdlog::should_log(spdlog::level::debug)) {
            mygram::utils::StructuredLog()
                .Event("binlog_debug")
                .Field("action", "set_pk")
                .Field("value", value)
                .Debug();
          }
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
          if (ptr + field_size > end) {
            mygram::utils::StructuredLog()
                .Event("mysql_binlog_error")
                .Field("type", "field_size_exceeds_buffer")
                .Field("event_type", "update_rows")
                .Field("image", "after")
                .Field("field_size", static_cast<uint64_t>(field_size))
                .Field("remaining_bytes", static_cast<int64_t>(end - ptr))
                .Error();
            return std::nullopt;
          }
          if (spdlog::should_log(spdlog::level::debug)) {
            mygram::utils::StructuredLog()
                .Event("binlog_debug")
                .Field("action", "decoded_value")
                .Field("value_preview", value.size() > 50 ? value.substr(0, 50) + "..." : value)
                .Field("field_size", static_cast<uint64_t>(field_size))
                .Field("new_ptr_offset", static_cast<int64_t>((ptr + field_size) - buffer))
                .Debug();
          }
          ptr += field_size;
        } else {
          if (spdlog::should_log(spdlog::level::debug)) {
            mygram::utils::StructuredLog().Event("binlog_debug").Field("action", "column_is_null").Debug();
          }
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

// ---------------------------------------------------------------------------
// ParseDeleteRowsEvent
// ---------------------------------------------------------------------------

std::optional<std::vector<RowData>> ParseDeleteRowsEvent(const unsigned char* buffer, unsigned long length,
                                                         const TableMetadata* table_metadata,
                                                         const std::string& pk_column_name,
                                                         const std::string& text_column_name,
                                                         MySQLBinlogEventType event_type) {
  if ((buffer == nullptr) || (table_metadata == nullptr)) {
    return std::nullopt;
  }

  try {
    auto header = internal::ParseRowsEventHeader(buffer, length, event_type, table_metadata, pk_column_name,
                                                 text_column_name, "delete_rows");
    if (!header) {
      return std::nullopt;
    }

    const unsigned char* ptr = header->row_data_ptr;
    const unsigned char* end = header->end;
    const uint64_t column_count = header->column_count;
    const unsigned char* columns_present = header->columns_present;
    const size_t kNullBitmapSize = header->null_bitmap_size;
    const int pk_col_idx = header->pk_col_idx;
    const int text_col_idx = header->text_col_idx;

    // Parse rows
    std::vector<RowData> rows;
    // Reserve space for estimated rows
    size_t estimated_rows = (end - ptr) / mygram::constants::kEstimatedBytesPerBinlogRow;
    if (estimated_rows > 0 && estimated_rows < mygram::constants::kMaxPreReserveRows) {
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

        if (ptr >= end) {
          mygram::utils::StructuredLog()
              .Event("mysql_binlog_error")
              .Field("type", "delete_rows_truncated")
              .Field("column_index", static_cast<uint64_t>(col_idx))
              .Error();
          return std::nullopt;
        }

        auto value_result = internal::DecodeFieldValue(static_cast<uint8_t>(col_meta.type), ptr, col_meta.metadata,
                                                       is_null, end, col_meta.is_unsigned);
        if (!value_result) {
          mygram::utils::StructuredLog()
              .Event("mysql_binlog_error")
              .Field("type", "field_decode_error")
              .Field("event_type", "delete_rows")
              .Field("column_index", static_cast<uint64_t>(col_idx))
              .Field("error", value_result.error().message())
              .Error();
          return std::nullopt;
        }
        std::string value = *value_result;

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
          if (ptr + field_size > end) {
            mygram::utils::StructuredLog()
                .Event("mysql_binlog_error")
                .Field("type", "field_size_exceeds_buffer")
                .Field("event_type", "delete_rows")
                .Field("field_size", static_cast<uint64_t>(field_size))
                .Field("remaining_bytes", static_cast<int64_t>(end - ptr))
                .Error();
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
