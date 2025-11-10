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
 */

#include "mysql/rows_parser.h"
#include "mysql/binlog_util.h"
#include <spdlog/spdlog.h>
#include <cstring>

#ifdef USE_MYSQL

namespace mygramdb {
namespace mysql {

/**
 * @brief Decode a single field value as string
 *
 * @param col_type Column type
 * @param data Pointer to field data
 * @param metadata Type metadata
 * @param is_null Whether the field is NULL
 * @return String representation of the value
 */
static std::string DecodeFieldValue(uint8_t col_type, const unsigned char* data,
                                     uint16_t metadata, bool is_null) {
  if (is_null) {
    return "";  // NULL values represented as empty string
  }

  switch (col_type) {
    // Integer types
    case 1: {  // MYSQL_TYPE_TINY
      int8_t val = static_cast<int8_t>(*data);
      return std::to_string(val);
    }
    case 2: {  // MYSQL_TYPE_SHORT
      int16_t val = static_cast<int16_t>(binlog_util::uint2korr(data));
      return std::to_string(val);
    }
    case 3: {  // MYSQL_TYPE_LONG
      int32_t val = static_cast<int32_t>(binlog_util::uint4korr(data));
      return std::to_string(val);
    }
    case 8: {  // MYSQL_TYPE_LONGLONG
      int64_t val = static_cast<int64_t>(binlog_util::uint8korr(data));
      return std::to_string(val);
    }
    case 9: {  // MYSQL_TYPE_INT24
      // 3-byte signed integer
      uint32_t val = binlog_util::uint3korr(data);
      if (val & 0x800000) {
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
      return std::string(reinterpret_cast<const char*>(str_data), str_len);
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
      return std::string(reinterpret_cast<const char*>(blob_data), blob_len);
    }

    case 254: {  // MYSQL_TYPE_STRING (CHAR)
      unsigned char type = metadata >> 8;
      if (type == 0xf7 || type == 0xf8) {  // ENUM or SET
        // For now, return the numeric value
        return std::to_string(*data);
      } else {
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
        return std::string(reinterpret_cast<const char*>(str_data), str_len);
      }
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
      return std::string(reinterpret_cast<const char*>(json_data), json_len);
    }

    // Date/Time types (simple representation as strings)
    case 10: {  // MYSQL_TYPE_DATE (3 bytes)
      // Format: 3 bytes, little-endian
      // Bit layout: | year (14 bits) | month (4 bits) | day (5 bits) |
      uint32_t val = binlog_util::uint3korr(data);
      unsigned int day = val & 0x1F;
      unsigned int month = (val >> 5) & 0x0F;
      unsigned int year = (val >> 9);
      char date_str[16];
      snprintf(date_str, sizeof(date_str), "%04u-%02u-%02u", year, month, day);
      return std::string(date_str);
    }

    case 7:  // MYSQL_TYPE_TIMESTAMP (4 bytes)
    case 17: { // MYSQL_TYPE_TIMESTAMP2
      // Unix timestamp (seconds since 1970-01-01)
      uint32_t timestamp = binlog_util::uint4korr(data);
      return std::to_string(timestamp);
    }

    case 12: { // MYSQL_TYPE_DATETIME (8 bytes, old format)
      // 8 bytes, packed format
      uint64_t val = binlog_util::uint8korr(data);
      return std::to_string(val);  // Simplified - return as number
    }

    case 18: { // MYSQL_TYPE_DATETIME2 (5+ bytes, new format)
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
      uint64_t hms = packed & 0x1FFFF;           // 17 bits for time

      unsigned int year = ymd >> 9;
      unsigned int month = (ymd >> 5) & 0x0F;
      unsigned int day = ymd & 0x1F;

      unsigned int hour = (hms >> 12) & 0x1F;
      unsigned int minute = (hms >> 6) & 0x3F;
      unsigned int second = hms & 0x3F;

      char datetime_str[32];

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
          case 1: usec = frac * 100000; break;
          case 2: usec = frac * 10000; break;
          case 3: usec = frac * 1000; break;
          case 4: usec = frac * 100; break;
          case 5: usec = frac * 10; break;
          case 6: usec = frac; break;
        }

        snprintf(datetime_str, sizeof(datetime_str),
                "%04u-%02u-%02u %02u:%02u:%02u.%06u",
                year, month, day, hour, minute, second, usec);
      } else {
        snprintf(datetime_str, sizeof(datetime_str),
                "%04u-%02u-%02u %02u:%02u:%02u",
                year, month, day, hour, minute, second);
      }

      return std::string(datetime_str);
    }

    case 246: { // MYSQL_TYPE_NEWDECIMAL
      // metadata: (precision << 8) | scale
      uint8_t precision = metadata >> 8;
      uint8_t scale = metadata & 0xFF;
      return binlog_util::decode_decimal(data, precision, scale);
    }

    default:
      return "[UNSUPPORTED_TYPE:" + std::to_string(col_type) + "]";
  }
}

std::optional<std::vector<RowData>> ParseWriteRowsEvent(
    const unsigned char* buffer,
    unsigned long length,
    const TableMetadata* table_metadata,
    const std::string& pk_column_name,
    const std::string& text_column_name) {
  if (!buffer || !table_metadata) {
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

    // Parse body
    // width (packed integer) - number of columns
    if (ptr >= end) {
      spdlog::error("WRITE_ROWS event too short for width");
      return std::nullopt;
    }
    uint64_t column_count = binlog_util::read_packed_integer(&ptr);

    if (column_count != table_metadata->columns.size()) {
      spdlog::error("Column count mismatch: event has {}, table has {}",
                   column_count, table_metadata->columns.size());
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
    const size_t null_bitmap_size = binlog_util::bitmap_bytes(column_count);

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
      if (ptr + null_bitmap_size > end) {
        break;  // End of rows
      }
      const unsigned char* null_bitmap = ptr;
      ptr += null_bitmap_size;

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
        std::string value = DecodeFieldValue(
            static_cast<uint8_t>(col_meta.type), ptr, col_meta.metadata, is_null);

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
          uint32_t field_size = binlog_util::calc_field_size(
              static_cast<uint8_t>(col_meta.type), ptr, col_meta.metadata);
          if (field_size == 0) {
            spdlog::warn("Unsupported column type {} for column {}",
                        static_cast<int>(col_meta.type), col_meta.name);
            return std::nullopt;
          }
          ptr += field_size;
        }
      }

      rows.push_back(std::move(row));
    }

    spdlog::debug("Parsed {} rows from WRITE_ROWS event for table {}.{}",
                 rows.size(), table_metadata->database_name, table_metadata->table_name);

    return rows;

  } catch (const std::exception& e) {
    spdlog::error("Exception while parsing WRITE_ROWS event: {}", e.what());
    return std::nullopt;
  }
}

std::optional<std::vector<std::pair<RowData, RowData>>> ParseUpdateRowsEvent(
    const unsigned char* buffer,
    unsigned long length,
    const TableMetadata* table_metadata,
    const std::string& pk_column_name,
    const std::string& text_column_name) {
  if (!buffer || !table_metadata) {
    return std::nullopt;
  }

  try {
    const unsigned char* ptr = buffer + 19;  // Skip common header
    const unsigned char* end = buffer + length;

    // Parse post-header (same as WRITE_ROWS)
    if (ptr + 8 > end) {
      spdlog::error("UPDATE_ROWS event too short for post-header");
      return std::nullopt;
    }

    ptr += 6;  // table_id
    uint16_t flags = binlog_util::uint2korr(ptr);
    ptr += 2;  // flags

    // Parse body
    if (ptr >= end) {
      spdlog::error("UPDATE_ROWS event too short for width");
      return std::nullopt;
    }
    uint64_t column_count = binlog_util::read_packed_integer(&ptr);

    if (column_count != table_metadata->columns.size()) {
      spdlog::error("Column count mismatch: event has {}, table has {}",
                   column_count, table_metadata->columns.size());
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

    // Parse extra_row_info if present
    size_t extra_info_size = binlog_util::skip_extra_row_info(&ptr, end, flags);
    if (extra_info_size > 0) {
      spdlog::debug("Skipped {} bytes of extra_row_info in UPDATE_ROWS", extra_info_size);
    }

    // Pre-calculate bitmap size
    const size_t null_bitmap_size = binlog_util::bitmap_bytes(column_count);

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

    while (ptr < end) {
      RowData before_row;
      RowData after_row;

      // Parse before image
      if (ptr + null_bitmap_size > end) {
        break;
      }
      const unsigned char* null_bitmap_before = ptr;
      ptr += null_bitmap_size;

      for (size_t col_idx = 0; col_idx < column_count; col_idx++) {
        if (!binlog_util::bitmap_is_set(columns_before, col_idx)) {
          continue;
        }

        const auto& col_meta = table_metadata->columns[col_idx];
        bool is_null = binlog_util::bitmap_is_set(null_bitmap_before, col_idx);

        if (ptr > end) {
          spdlog::error("UPDATE_ROWS event truncated while parsing before image column {}", col_idx);
          return std::nullopt;
        }

        std::string value = DecodeFieldValue(
            static_cast<uint8_t>(col_meta.type), ptr, col_meta.metadata, is_null);

        before_row.columns[col_meta.name] = value;

        // Check using cached indices
        if (static_cast<int>(col_idx) == pk_col_idx) {
          before_row.primary_key = value;
        }
        if (static_cast<int>(col_idx) == text_col_idx) {
          before_row.text = value;
        }

        if (!is_null) {
          uint32_t field_size = binlog_util::calc_field_size(
              static_cast<uint8_t>(col_meta.type), ptr, col_meta.metadata);
          if (field_size == 0) {
            spdlog::warn("Unsupported column type {} for column {}",
                        static_cast<int>(col_meta.type), col_meta.name);
            return std::nullopt;
          }
          ptr += field_size;
        }
      }

      // Parse after image
      if (ptr + null_bitmap_size > end) {
        break;
      }
      const unsigned char* null_bitmap_after = ptr;
      ptr += null_bitmap_size;

      for (size_t col_idx = 0; col_idx < column_count; col_idx++) {
        if (!binlog_util::bitmap_is_set(columns_after, col_idx)) {
          continue;
        }

        const auto& col_meta = table_metadata->columns[col_idx];
        bool is_null = binlog_util::bitmap_is_set(null_bitmap_after, col_idx);

        if (ptr > end) {
          spdlog::error("UPDATE_ROWS event truncated while parsing after image column {}", col_idx);
          return std::nullopt;
        }

        std::string value = DecodeFieldValue(
            static_cast<uint8_t>(col_meta.type), ptr, col_meta.metadata, is_null);

        after_row.columns[col_meta.name] = value;

        // Check using cached indices
        if (static_cast<int>(col_idx) == pk_col_idx) {
          after_row.primary_key = value;
        }
        if (static_cast<int>(col_idx) == text_col_idx) {
          after_row.text = value;
        }

        if (!is_null) {
          uint32_t field_size = binlog_util::calc_field_size(
              static_cast<uint8_t>(col_meta.type), ptr, col_meta.metadata);
          if (field_size == 0) {
            spdlog::warn("Unsupported column type {} for column {}",
                        static_cast<int>(col_meta.type), col_meta.name);
            return std::nullopt;
          }
          ptr += field_size;
        }
      }

      row_pairs.push_back({std::move(before_row), std::move(after_row)});
    }

    spdlog::debug("Parsed {} row pairs from UPDATE_ROWS event for table {}.{}",
                 row_pairs.size(), table_metadata->database_name, table_metadata->table_name);

    return row_pairs;

  } catch (const std::exception& e) {
    spdlog::error("Exception while parsing UPDATE_ROWS event: {}", e.what());
    return std::nullopt;
  }
}

std::optional<std::vector<RowData>> ParseDeleteRowsEvent(
    const unsigned char* buffer,
    unsigned long length,
    const TableMetadata* table_metadata,
    const std::string& pk_column_name,
    const std::string& text_column_name) {
  if (!buffer || !table_metadata) {
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

    // Parse body
    if (ptr >= end) {
      spdlog::error("DELETE_ROWS event too short for width");
      return std::nullopt;
    }
    uint64_t column_count = binlog_util::read_packed_integer(&ptr);

    if (column_count != table_metadata->columns.size()) {
      spdlog::error("Column count mismatch: event has {}, table has {}",
                   column_count, table_metadata->columns.size());
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
    const size_t null_bitmap_size = binlog_util::bitmap_bytes(column_count);

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
      if (ptr + null_bitmap_size > end) {
        break;
      }
      const unsigned char* null_bitmap = ptr;
      ptr += null_bitmap_size;

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

        std::string value = DecodeFieldValue(
            static_cast<uint8_t>(col_meta.type), ptr, col_meta.metadata, is_null);

        row.columns[col_meta.name] = value;

        // Check using cached indices
        if (static_cast<int>(col_idx) == pk_col_idx) {
          row.primary_key = value;
        }
        if (static_cast<int>(col_idx) == text_col_idx) {
          row.text = value;
        }

        if (!is_null) {
          uint32_t field_size = binlog_util::calc_field_size(
              static_cast<uint8_t>(col_meta.type), ptr, col_meta.metadata);
          if (field_size == 0) {
            spdlog::warn("Unsupported column type {} for column {}",
                        static_cast<int>(col_meta.type), col_meta.name);
            return std::nullopt;
          }
          ptr += field_size;
        }
      }

      rows.push_back(std::move(row));
    }

    spdlog::debug("Parsed {} rows from DELETE_ROWS event for table {}.{}",
                 rows.size(), table_metadata->database_name, table_metadata->table_name);

    return rows;

  } catch (const std::exception& e) {
    spdlog::error("Exception while parsing DELETE_ROWS event: {}", e.what());
    return std::nullopt;
  }
}

std::unordered_map<std::string, storage::FilterValue> ExtractFilters(
    const RowData& row_data,
    const std::vector<config::FilterConfig>& filter_configs) {
  std::unordered_map<std::string, storage::FilterValue> filters;

  for (const auto& filter_config : filter_configs) {
    // Check if column exists in row data
    auto it = row_data.columns.find(filter_config.name);
    if (it == row_data.columns.end()) {
      spdlog::warn("Filter column '{}' not found in row data", filter_config.name);
      continue;
    }

    const std::string& value_str = it->second;

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
      } else if (filter_config.type == "string" ||
                 filter_config.type == "varchar" ||
                 filter_config.type == "text" ||
                 filter_config.type == "datetime" ||
                 filter_config.type == "date" ||
                 filter_config.type == "timestamp") {
        filters[filter_config.name] = value_str;
      } else if (filter_config.type == "boolean") {
        // Boolean: "1"/"true" = true, "0"/"false" = false
        filters[filter_config.name] = (value_str == "1" || value_str == "true");
      } else {
        spdlog::warn("Unknown filter type '{}' for column '{}'",
                    filter_config.type, filter_config.name);
      }
    } catch (const std::exception& e) {
      spdlog::error("Failed to convert filter value '{}' for column '{}': {}",
                   value_str, filter_config.name, e.what());
    }
  }

  return filters;
}

}  // namespace mysql
}  // namespace mygramdb

#endif  // USE_MYSQL
