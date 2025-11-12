/**
 * @file rows_parser.cpp
 * @brief Parser for MySQL ROWS events (WRITE/UPDATE/DELETE)
 *
 * Implementation based on MySQL 8.4.7 source code:
 * - libs/mysql/binlog/event/rows_event.h (event format documentation)
 * - libs/mysql/binlog/event/binary_log_funcs.cpp (field size calculation)
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

#include <cstring>
#include <iomanip>
#include <sstream>

#include "mysql/binlog_util.h"

#ifdef USE_MYSQL

// NOLINTBEGIN(cppcoreguidelines-pro-*,cppcoreguidelines-avoid-*,readability-magic-numbers,readability-function-cognitive-complexity,readability-else-after-return)

namespace mygramdb::mysql {

/**
 * @brief Decode a single field value as string
 *
 * @param col_type Column type
 * @param data Pointer to field data
 * @param metadata Type metadata
 * @param is_null Whether the field is NULL
 * @return String representation of the value
 */
static std::string DecodeFieldValue(uint8_t col_type, const unsigned char* data, uint16_t metadata, bool is_null) {
  if (is_null) {
    return "";  // NULL values represented as empty string
  }

  switch (col_type) {
    // Integer types
    case 1: {  // MYSQL_TYPE_TINY
      auto val = static_cast<int8_t>(*data);
      return std::to_string(val);
    }
    case 2: {  // MYSQL_TYPE_SHORT
      auto val = static_cast<int16_t>(binlog_util::uint2korr(data));
      return std::to_string(val);
    }
    case 3: {  // MYSQL_TYPE_LONG
      auto val = static_cast<int32_t>(binlog_util::uint4korr(data));
      return std::to_string(val);
    }
    case 8: {  // MYSQL_TYPE_LONGLONG
      auto val = static_cast<int64_t>(binlog_util::uint8korr(data));
      return std::to_string(val);
    }
    case 9: {  // MYSQL_TYPE_INT24
      // 3-byte signed integer
      uint32_t val = binlog_util::uint3korr(data);
      if ((val & 0x800000) != 0U) {
        val |= 0xFF000000;  // Sign extend
      }
      return std::to_string(static_cast<int32_t>(val));
    }

    // String types
    case 15: {  // MYSQL_TYPE_VARCHAR
      uint32_t str_len = 0;
      const unsigned char* str_data = nullptr;
      if (metadata > 255) {
        str_len = binlog_util::uint2korr(data);
        str_data = data + 2;
      } else {
        str_len = *data;
        str_data = data + 1;
      }
      return {reinterpret_cast<const char*>(str_data), str_len};
    }

    case 252: {  // MYSQL_TYPE_BLOB (includes TEXT, MEDIUMTEXT, LONGTEXT)
      uint32_t blob_len = 0;
      const unsigned char* blob_data = nullptr;
      switch (metadata) {
        case 1:  // TINYBLOB/TINYTEXT
          blob_len = *data;
          blob_data = data + 1;
          break;
        case 2:  // BLOB/TEXT
          blob_len = binlog_util::uint2korr(data);
          blob_data = data + 2;
          break;
        case 3:  // MEDIUMBLOB/MEDIUMTEXT
          blob_len = binlog_util::uint3korr(data);
          blob_data = data + 3;
          break;
        case 4:  // LONGBLOB/LONGTEXT
          blob_len = binlog_util::uint4korr(data);
          blob_data = data + 4;
          break;
      }
      return {reinterpret_cast<const char*>(blob_data), blob_len};
    }

    case 254: {  // MYSQL_TYPE_STRING (CHAR)
      unsigned char type = metadata >> 8;
      if (type == 0xf7 || type == 0xf8) {  // ENUM or SET
        // For now, return the numeric value
        return std::to_string(*data);
      }
      uint32_t max_len = (((metadata >> 4) & 0x300) ^ 0x300) + (metadata & 0xff);
      uint32_t str_len = 0;
      const unsigned char* str_data = nullptr;
      if (max_len > 255) {
        str_len = binlog_util::uint2korr(data);
        str_data = data + 2;
      } else {
        str_len = *data;
        str_data = data + 1;
      }
      return {reinterpret_cast<const char*>(str_data), str_len};
    }

    // JSON type
    case 245: {  // MYSQL_TYPE_JSON
      uint32_t json_len = 0;
      const unsigned char* json_data = nullptr;
      // JSON typically uses 4 bytes for length, but check metadata
      uint8_t len_bytes = (metadata > 0) ? metadata : 4;
      switch (len_bytes) {
        case 1:
          json_len = *data;
          json_data = data + 1;
          break;
        case 2:
          json_len = binlog_util::uint2korr(data);
          json_data = data + 2;
          break;
        case 3:
          json_len = binlog_util::uint3korr(data);
          json_data = data + 3;
          break;
        case 4:
          json_len = binlog_util::uint4korr(data);
          json_data = data + 4;
          break;
      }
      return {reinterpret_cast<const char*>(json_data), json_len};
    }

    // Date/Time types (simple representation as strings)
    case 10: {  // MYSQL_TYPE_DATE (3 bytes)
      // Format: 3 bytes, little-endian
      // Bit layout: | year (14 bits) | month (4 bits) | day (5 bits) |
      uint32_t val = binlog_util::uint3korr(data);
      unsigned int day = val & 0x1F;
      unsigned int month = (val >> 5) & 0x0F;
      unsigned int year = (val >> 9);
      std::ostringstream oss;
      oss << std::setfill('0') << std::setw(4) << year << '-' << std::setw(2) << month << '-' << std::setw(2) << day;
      return oss.str();
    }

    case 7:     // MYSQL_TYPE_TIMESTAMP (4 bytes)
    case 17: {  // MYSQL_TYPE_TIMESTAMP2
      // Unix timestamp (seconds since 1970-01-01)
      uint32_t timestamp = binlog_util::uint4korr(data);
      return std::to_string(timestamp);
    }

    case 12: {  // MYSQL_TYPE_DATETIME (8 bytes, old format)
      // 8 bytes, packed format
      uint64_t val = binlog_util::uint8korr(data);
      return std::to_string(val);  // Simplified - return as number
    }

    case 18: {  // MYSQL_TYPE_DATETIME2 (5+ bytes, new format)
      // Format: 5 bytes for datetime + fractional seconds bytes
      // Bit layout is complex, packed in big-endian

      // Read 5 bytes in big-endian
      uint64_t packed = 0;
      for (int i = 0; i < 5; i++) {
        packed = (packed << 8) | data[i];
      }

      // Extract datetime parts
      // Format: YYYYMMDDhhmmss packed in 40 bits
      uint64_t ymd = (packed >> 17) & 0x3FFFF;  // 18 bits for date
      uint64_t hms = packed & 0x1FFFF;          // 17 bits for time

      unsigned int year = ymd >> 9;
      unsigned int month = (ymd >> 5) & 0x0F;
      unsigned int day = ymd & 0x1F;

      unsigned int hour = (hms >> 12) & 0x1F;
      unsigned int minute = (hms >> 6) & 0x3F;
      unsigned int second = hms & 0x3F;

      std::ostringstream oss;
      oss << std::setfill('0') << std::setw(4) << year << '-' << std::setw(2) << month << '-' << std::setw(2) << day
          << ' ' << std::setw(2) << hour << ':' << std::setw(2) << minute << ':' << std::setw(2) << second;

      // Process fractional seconds if present
      if (metadata > 0) {
        int frac_bytes = (metadata + 1) / 2;
        uint32_t frac = 0;
        for (int i = 0; i < frac_bytes; i++) {
          frac = (frac << 8) | data[5 + i];
        }

        // Convert to microseconds based on precision
        uint32_t usec = 0;
        switch (metadata) {
          case 1:
            usec = frac * 100000;
            break;
          case 2:
            usec = frac * 10000;
            break;
          case 3:
            usec = frac * 1000;
            break;
          case 4:
            usec = frac * 100;
            break;
          case 5:
            usec = frac * 10;
            break;
          case 6:
            usec = frac;
            break;
        }

        oss << '.' << std::setw(6) << usec;
      }

      return oss.str();
    }

    case 246: {  // MYSQL_TYPE_NEWDECIMAL
      // metadata: (precision << 8) | scale
      uint8_t precision = metadata >> 8;
      uint8_t scale = metadata & 0xFF;
      return binlog_util::decode_decimal(data, precision, scale);
    }

    default:
      return "[UNSUPPORTED_TYPE:" + std::to_string(col_type) + "]";
  }
}

std::optional<std::vector<RowData>> ParseWriteRowsEvent(const unsigned char* buffer, unsigned long length,
                                                        const TableMetadata* table_metadata,
                                                        const std::string& pk_column_name,
                                                        const std::string& text_column_name) {
  if ((buffer == nullptr) || (table_metadata == nullptr)) {
    return std::nullopt;
  }

  try {
    const unsigned char* ptr = buffer + 19;  // Skip common header
    const unsigned char* end = buffer + length;

    // Parse post-header
    if (ptr + 8 > end) {
      spdlog::error("WRITE_ROWS event too short for post-header");
      return std::nullopt;
    }

    // table_id (6 bytes) - already known from TABLE_MAP
    ptr += 6;

    // flags (2 bytes)
    uint16_t flags = binlog_util::uint2korr(ptr);
    ptr += 2;

    // MySQL 8.0 ROWS_EVENT_V2: skip extra_row_info if present
    if ((flags & 0x0001) != 0) {  // ROWS_EVENT_V2 with extra info
      if (ptr >= end) {
        spdlog::error("WRITE_ROWS event too short for extra_row_info_length");
        return std::nullopt;
      }
      uint64_t extra_info_len = binlog_util::read_packed_integer(&ptr);
      ptr += extra_info_len;  // Skip the extra row info data
    }

    // Parse body
    // width (packed integer) - number of columns
    if (ptr >= end) {
      spdlog::error("WRITE_ROWS event too short for width");
      return std::nullopt;
    }
    uint64_t column_count = binlog_util::read_packed_integer(&ptr);

    if (column_count != table_metadata->columns.size()) {
      spdlog::error("Column count mismatch: event has {}, table has {}", column_count, table_metadata->columns.size());
      return std::nullopt;
    }

    // columns_present bitmap - which columns are in the event
    size_t bitmap_size = binlog_util::bitmap_bytes(column_count);
    if (ptr + bitmap_size > end) {
      spdlog::error("WRITE_ROWS event too short for columns_present bitmap");
      return std::nullopt;
    }
    const unsigned char* columns_present = ptr;
    ptr += bitmap_size;

    // Parse extra_row_info if present
    size_t extra_info_size = binlog_util::skip_extra_row_info(&ptr, end, flags);
    if (extra_info_size > 0) {
      spdlog::debug("Skipped {} bytes of extra_row_info", extra_info_size);
    }

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
          spdlog::error("WRITE_ROWS event truncated while parsing column {}", col_idx);
          return std::nullopt;
        }

        // Decode field value
        std::string value = DecodeFieldValue(static_cast<uint8_t>(col_meta.type), ptr, col_meta.metadata, is_null);

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
            spdlog::warn("Unsupported column type {} for column {}", static_cast<int>(col_meta.type), col_meta.name);
            return std::nullopt;
          }
          ptr += field_size;
        }
      }

      rows.push_back(std::move(row));
    }

    spdlog::debug("Parsed {} rows from WRITE_ROWS event for table {}.{}", rows.size(), table_metadata->database_name,
                  table_metadata->table_name);

    return rows;

  } catch (const std::exception& e) {
    spdlog::error("Exception while parsing WRITE_ROWS event: {}", e.what());
    return std::nullopt;
  }
}

std::optional<std::vector<std::pair<RowData, RowData>>> ParseUpdateRowsEvent(const unsigned char* buffer,
                                                                             unsigned long length,
                                                                             const TableMetadata* table_metadata,
                                                                             const std::string& pk_column_name,
                                                                             const std::string& text_column_name) {
  if ((buffer == nullptr) || (table_metadata == nullptr)) {
    return std::nullopt;
  }

  try {
    // Read event_size from binlog header (bytes 9-12, little-endian)
    uint32_t event_size = buffer[9] | (buffer[10] << 8) | (buffer[11] << 16) | (buffer[12] << 24);

    const unsigned char* ptr = buffer + 19;          // Skip common header
    const unsigned char* end = buffer + event_size;  // Use event_size from header, not length param

    spdlog::debug("UPDATE_ROWS buffer: length_param={}, event_size_from_header={}, using event_size", length,
                  event_size);

    // Parse post-header (same as WRITE_ROWS)
    if (ptr + 8 > end) {
      spdlog::error("UPDATE_ROWS event too short for post-header");
      return std::nullopt;
    }

    ptr += 6;  // table_id
    uint16_t flags = binlog_util::uint2korr(ptr);
    ptr += 2;  // flags

    // MySQL 8.0 ROWS_EVENT_V2: skip extra_row_info if present
    // The flags field indicates if extra info exists
    if ((flags & 0x0001) != 0) {  // ROWS_EVENT_V2 with extra info
      // Read extra_row_info_length (packed integer)
      if (ptr >= end) {
        spdlog::error("UPDATE_ROWS event too short for extra_row_info_length");
        return std::nullopt;
      }

      // NOLINTBEGIN(modernize-avoid-c-arrays,cppcoreguidelines-avoid-c-arrays)
      // Reason: C-style arrays are used with snprintf() for debug hex formatting
      // Debug: show bytes before reading
      char pre_hex[31];
      for (int i = 0; i < std::min(10, static_cast<int>(end - ptr)); i++) {
        snprintf(&pre_hex[i * 3], 4, "%02x ", ptr[i]);
      }
      spdlog::debug("Before extra_row_info_len read: {}", pre_hex);

      const unsigned char* ptr_before = ptr;
      uint64_t extra_info_len = binlog_util::read_packed_integer(&ptr);
      auto len_bytes = static_cast<int>(ptr - ptr_before);

      spdlog::debug("Extra_row_info_len: {} bytes, packed_int used {} bytes", extra_info_len, len_bytes);

      // In MySQL 8.0, extra_row_info_len is the TOTAL length including the length field itself
      // So we need to skip (extra_info_len - len_bytes) more bytes
      auto skip_bytes = static_cast<int>(extra_info_len) - len_bytes;
      if (skip_bytes < 0) {
        spdlog::error("Invalid extra_row_info_len: {} (packed int used {} bytes)", extra_info_len, len_bytes);
        return std::nullopt;
      }

      if (ptr + skip_bytes > end) {
        spdlog::error("UPDATE_ROWS event too short for extra_row_info data");
        return std::nullopt;
      }

      // Debug: show the extra bytes we're skipping
      if (skip_bytes > 0) {
        char skip_hex[31];
        for (int i = 0; i < std::min(10, skip_bytes); i++) {
          snprintf(&skip_hex[i * 3], 4, "%02x ", ptr[i]);
        }
        spdlog::debug("Skipping {} extra_row_info bytes: {}", skip_bytes, skip_hex);
      }

      ptr += skip_bytes;  // Skip the extra row info data
      spdlog::debug("After extra_row_info skip, now at offset {}", ptr - buffer);
    }

    // Parse body
    if (ptr >= end) {
      spdlog::error("UPDATE_ROWS event too short for width");
      return std::nullopt;
    }

    // Debug: log the bytes we're about to read
    char debug_hex[31];
    for (int i = 0; i < std::min(10, static_cast<int>(end - ptr)); i++) {
      snprintf(&debug_hex[i * 3], 4, "%02x ", ptr[i]);
    }
    spdlog::debug("UPDATE_ROWS body start hex: {}", debug_hex);
    // NOLINTEND(modernize-avoid-c-arrays,cppcoreguidelines-avoid-c-arrays)

    const unsigned char* column_count_ptr = ptr;  // Save position before read
    uint64_t column_count = binlog_util::read_packed_integer(&ptr);

    spdlog::debug("Column count parsed: {} (ptr moved {} bytes from 0x{:02x})", column_count, ptr - column_count_ptr,
                  *column_count_ptr);

    if (column_count != table_metadata->columns.size()) {
      spdlog::error("Column count mismatch: event has {}, table has {}", column_count, table_metadata->columns.size());
      return std::nullopt;
    }

    // columns_before_image bitmap - which columns are in the before image
    size_t bitmap_size = binlog_util::bitmap_bytes(column_count);
    if (ptr + bitmap_size > end) {
      spdlog::error("UPDATE_ROWS event too short for columns_before_image bitmap");
      return std::nullopt;
    }
    const unsigned char* columns_before = ptr;
    ptr += bitmap_size;

    // columns_after_image bitmap - which columns are in the after image
    if (ptr + bitmap_size > end) {
      spdlog::error("UPDATE_ROWS event too short for columns_after_image bitmap");
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

    spdlog::debug("Starting row parsing loop: ptr offset={}, end offset={}, available={} bytes", ptr - buffer,
                  end - buffer, end - ptr);

    while (ptr < end) {
      RowData before_row;
      RowData after_row;

      spdlog::debug("Row start: ptr offset={}, kNullBitmapSize={}", ptr - buffer, kNullBitmapSize);

      // Parse before image
      if (ptr + kNullBitmapSize > end) {
        spdlog::debug("Not enough space for NULL bitmap, breaking loop");
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
      spdlog::debug("Before image - columns_bitmap: {} (1=included)", col_bitmap_str);
      spdlog::debug("Before image - null_bitmap:    {} (N=null, .=not null)", null_bitmap_str);

      for (size_t col_idx = 0; col_idx < column_count; col_idx++) {
        if (!binlog_util::bitmap_is_set(columns_before, col_idx)) {
          spdlog::debug("  Col {} ({}) - not in bitmap, skipping", col_idx,
                        col_idx < table_metadata->columns.size() ? table_metadata->columns[col_idx].name : "?");
          continue;
        }

        const auto& col_meta = table_metadata->columns[col_idx];
        bool is_null = binlog_util::bitmap_is_set(null_bitmap_before, col_idx);

        spdlog::debug("  Col {} ({}, type={}) - ptr_offset={}, end_offset={}, remaining={} bytes, is_null={}", col_idx,
                      col_meta.name, static_cast<int>(col_meta.type), ptr - buffer, end - buffer, end - ptr, is_null);

        // Check if we have data remaining before attempting to decode
        // If not, this means we've reached the end (e.g., checksum/padding bytes)
        if (ptr >= end) {
          spdlog::debug("Reached end of event data while parsing before image at column {}, breaking", col_idx);
          goto end_of_rows;  // Exit both loops cleanly
        }

        std::string value = DecodeFieldValue(static_cast<uint8_t>(col_meta.type), ptr, col_meta.metadata, is_null);

        // Check again after decode, as DecodeFieldValue advances ptr
        if (ptr > end) {
          spdlog::debug("Exceeded end after decoding column {}, breaking", col_idx);
          goto end_of_rows;  // Exit both loops cleanly
        }

        before_row.columns[col_meta.name] = value;

        // Check using cached indices
        if (static_cast<int>(col_idx) == pk_col_idx) {
          before_row.primary_key = value;
          spdlog::debug("    -> Set PK = '{}'", value);
        }
        if (static_cast<int>(col_idx) == text_col_idx) {
          before_row.text = value;
        }

        if (!is_null) {
          uint32_t field_size =
              binlog_util::calc_field_size(static_cast<uint8_t>(col_meta.type), ptr, col_meta.metadata);
          if (field_size == 0) {
            spdlog::warn("Unsupported column type {} for column {}", static_cast<int>(col_meta.type), col_meta.name);
            return std::nullopt;
          }
          spdlog::debug("    -> Decoded value='{}', field_size={}, advancing ptr to offset={}",
                        value.size() > 50 ? value.substr(0, 50) + "..." : value, field_size,
                        (ptr + field_size) - buffer);
          ptr += field_size;
        } else {
          spdlog::debug("    -> Column is NULL, not advancing ptr");
        }
      }

      // Parse after image
      if (ptr + kNullBitmapSize > end) {
        spdlog::debug("Not enough space for after image NULL bitmap, breaking loop");
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
      spdlog::debug("After image - columns_bitmap: {} (1=included)", col_bitmap_after_str);
      spdlog::debug("After image - null_bitmap:    {} (N=null, .=not null)", null_bitmap_after_str);

      for (size_t col_idx = 0; col_idx < column_count; col_idx++) {
        if (!binlog_util::bitmap_is_set(columns_after, col_idx)) {
          spdlog::debug("  Col {} ({}) - not in after bitmap, skipping", col_idx,
                        col_idx < table_metadata->columns.size() ? table_metadata->columns[col_idx].name : "?");
          continue;
        }

        const auto& col_meta = table_metadata->columns[col_idx];
        bool is_null = binlog_util::bitmap_is_set(null_bitmap_after, col_idx);

        spdlog::debug("  Col {} ({}, type={}) - ptr_offset={}, end_offset={}, remaining={} bytes, is_null={}", col_idx,
                      col_meta.name, static_cast<int>(col_meta.type), ptr - buffer, end - buffer, end - ptr, is_null);

        // Check if we have data remaining before attempting to decode
        if (ptr >= end) {
          spdlog::debug("Reached end of event data while parsing after image at column {}, breaking", col_idx);
          goto end_of_rows;
        }

        std::string value = DecodeFieldValue(static_cast<uint8_t>(col_meta.type), ptr, col_meta.metadata, is_null);

        // Check again after decode
        if (ptr > end) {
          spdlog::debug("Exceeded end after decoding after image column {}, breaking", col_idx);
          goto end_of_rows;
        }

        after_row.columns[col_meta.name] = value;

        // Check using cached indices
        if (static_cast<int>(col_idx) == pk_col_idx) {
          after_row.primary_key = value;
          spdlog::debug("    -> Set PK = '{}'", value);
        }
        if (static_cast<int>(col_idx) == text_col_idx) {
          after_row.text = value;
        }

        if (!is_null) {
          uint32_t field_size =
              binlog_util::calc_field_size(static_cast<uint8_t>(col_meta.type), ptr, col_meta.metadata);
          if (field_size == 0) {
            spdlog::warn("Unsupported column type {} for column {}", static_cast<int>(col_meta.type), col_meta.name);
            return std::nullopt;
          }
          spdlog::debug("    -> Decoded value='{}', field_size={}, advancing ptr to offset={}",
                        value.size() > 50 ? value.substr(0, 50) + "..." : value, field_size,
                        (ptr + field_size) - buffer);
          ptr += field_size;
        } else {
          spdlog::debug("    -> Column is NULL, not advancing ptr");
        }
      }

      row_pairs.emplace_back(std::move(before_row), std::move(after_row));
    }

  end_of_rows:  // Label for graceful early exit from nested loops
    spdlog::debug("Parsed {} row pairs from UPDATE_ROWS event for table {}.{}", row_pairs.size(),
                  table_metadata->database_name, table_metadata->table_name);

    return row_pairs;

  } catch (const std::exception& e) {
    spdlog::error("Exception while parsing UPDATE_ROWS event: {}", e.what());
    return std::nullopt;
  }
}

std::optional<std::vector<RowData>> ParseDeleteRowsEvent(const unsigned char* buffer, unsigned long length,
                                                         const TableMetadata* table_metadata,
                                                         const std::string& pk_column_name,
                                                         const std::string& text_column_name) {
  if ((buffer == nullptr) || (table_metadata == nullptr)) {
    return std::nullopt;
  }

  try {
    const unsigned char* ptr = buffer + 19;  // Skip common header
    const unsigned char* end = buffer + length;

    // Parse post-header
    if (ptr + 8 > end) {
      spdlog::error("DELETE_ROWS event too short for post-header");
      return std::nullopt;
    }

    ptr += 6;  // table_id
    uint16_t flags = binlog_util::uint2korr(ptr);
    ptr += 2;  // flags

    // MySQL 8.0 ROWS_EVENT_V2: skip extra_row_info if present
    if ((flags & 0x0001) != 0) {  // ROWS_EVENT_V2 with extra info
      if (ptr >= end) {
        spdlog::error("DELETE_ROWS event too short for extra_row_info_length");
        return std::nullopt;
      }
      uint64_t extra_info_len = binlog_util::read_packed_integer(&ptr);
      ptr += extra_info_len;  // Skip the extra row info data
    }

    // Parse body
    if (ptr >= end) {
      spdlog::error("DELETE_ROWS event too short for width");
      return std::nullopt;
    }
    uint64_t column_count = binlog_util::read_packed_integer(&ptr);

    if (column_count != table_metadata->columns.size()) {
      spdlog::error("Column count mismatch: event has {}, table has {}", column_count, table_metadata->columns.size());
      return std::nullopt;
    }

    // columns_present bitmap - which columns are in the event (before image only)
    size_t bitmap_size = binlog_util::bitmap_bytes(column_count);
    if (ptr + bitmap_size > end) {
      spdlog::error("DELETE_ROWS event too short for columns_present bitmap");
      return std::nullopt;
    }
    const unsigned char* columns_present = ptr;
    ptr += bitmap_size;

    // Parse extra_row_info if present
    size_t extra_info_size = binlog_util::skip_extra_row_info(&ptr, end, flags);
    if (extra_info_size > 0) {
      spdlog::debug("Skipped {} bytes of extra_row_info in DELETE_ROWS", extra_info_size);
    }

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
          spdlog::error("DELETE_ROWS event truncated while parsing column {}", col_idx);
          return std::nullopt;
        }

        std::string value = DecodeFieldValue(static_cast<uint8_t>(col_meta.type), ptr, col_meta.metadata, is_null);

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
            spdlog::warn("Unsupported column type {} for column {}", static_cast<int>(col_meta.type), col_meta.name);
            return std::nullopt;
          }
          ptr += field_size;
        }
      }

      rows.push_back(std::move(row));
    }

    spdlog::debug("Parsed {} rows from DELETE_ROWS event for table {}.{}", rows.size(), table_metadata->database_name,
                  table_metadata->table_name);

    return rows;

  } catch (const std::exception& e) {
    spdlog::error("Exception while parsing DELETE_ROWS event: {}", e.what());
    return std::nullopt;
  }
}

std::unordered_map<std::string, storage::FilterValue> ExtractFilters(
    const RowData& row_data, const std::vector<config::FilterConfig>& filter_configs) {
  std::unordered_map<std::string, storage::FilterValue> filters;

  for (const auto& filter_config : filter_configs) {
    // Check if column exists in row data
    auto iterator = row_data.columns.find(filter_config.name);
    if (iterator == row_data.columns.end()) {
      spdlog::warn("Filter column '{}' not found in row data", filter_config.name);
      continue;
    }

    const std::string& value_str = iterator->second;

    // Skip empty values (NULL)
    if (value_str.empty()) {
      continue;
    }

    try {
      // Convert string to appropriate type based on filter config
      if (filter_config.type == "tinyint") {
        filters[filter_config.name] = static_cast<int8_t>(std::stoi(value_str));
      } else if (filter_config.type == "tinyint_unsigned") {
        filters[filter_config.name] = static_cast<uint8_t>(std::stoul(value_str));
      } else if (filter_config.type == "smallint") {
        filters[filter_config.name] = static_cast<int16_t>(std::stoi(value_str));
      } else if (filter_config.type == "smallint_unsigned") {
        filters[filter_config.name] = static_cast<uint16_t>(std::stoul(value_str));
      } else if (filter_config.type == "int" || filter_config.type == "mediumint") {
        filters[filter_config.name] = static_cast<int32_t>(std::stoi(value_str));
      } else if (filter_config.type == "int_unsigned" || filter_config.type == "mediumint_unsigned") {
        filters[filter_config.name] = static_cast<uint32_t>(std::stoul(value_str));
      } else if (filter_config.type == "bigint") {
        filters[filter_config.name] = static_cast<int64_t>(std::stoll(value_str));
      } else if (filter_config.type == "float" || filter_config.type == "double") {
        filters[filter_config.name] = std::stod(value_str);
      } else if (filter_config.type == "string" || filter_config.type == "varchar" || filter_config.type == "text" ||
                 filter_config.type == "datetime" || filter_config.type == "date" ||
                 filter_config.type == "timestamp") {
        filters[filter_config.name] = value_str;
      } else if (filter_config.type == "boolean") {
        // Boolean: "1"/"true" = true, "0"/"false" = false
        filters[filter_config.name] = (value_str == "1" || value_str == "true");
      } else {
        spdlog::warn("Unknown filter type '{}' for column '{}'", filter_config.type, filter_config.name);
      }
    } catch (const std::exception& e) {
      spdlog::error("Failed to convert filter value '{}' for column '{}': {}", value_str, filter_config.name, e.what());
    }
  }

  return filters;
}

}  // namespace mygramdb::mysql

// NOLINTEND(cppcoreguidelines-pro-*,cppcoreguidelines-avoid-*,readability-magic-numbers,readability-function-cognitive-complexity,readability-else-after-return)

#endif  // USE_MYSQL
