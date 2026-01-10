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
#include "utils/datetime_converter.h"
#include "utils/string_utils.h"
#include "utils/structured_log.h"

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
static std::string DecodeFieldValue(uint8_t col_type, const unsigned char* data, uint16_t metadata, bool is_null,
                                    const unsigned char* end, bool is_unsigned = false) {
  constexpr uint32_t kMaxFieldLength = 256 * 1024 * 1024;  // 256MB max for any field
  if (is_null) {
    return "";  // NULL values represented as empty string
  }

  switch (col_type) {
    // Integer types - handle UNSIGNED vs SIGNED correctly
    case 1: {  // MYSQL_TYPE_TINY
      if (is_unsigned) {
        auto val = static_cast<uint8_t>(*data);
        return std::to_string(val);
      }
      auto val = static_cast<int8_t>(*data);
      return std::to_string(val);
    }
    case 2: {  // MYSQL_TYPE_SHORT
      if (is_unsigned) {
        auto val = binlog_util::uint2korr(data);
        return std::to_string(val);
      }
      auto val = static_cast<int16_t>(binlog_util::uint2korr(data));
      return std::to_string(val);
    }
    case 3: {  // MYSQL_TYPE_LONG
      if (is_unsigned) {
        auto val = binlog_util::uint4korr(data);
        return std::to_string(val);
      }
      auto val = static_cast<int32_t>(binlog_util::uint4korr(data));
      return std::to_string(val);
    }
    case 8: {  // MYSQL_TYPE_LONGLONG
      if (is_unsigned) {
        auto val = binlog_util::uint8korr(data);
        return std::to_string(val);
      }
      auto val = static_cast<int64_t>(binlog_util::uint8korr(data));
      return std::to_string(val);
    }
    case 9: {  // MYSQL_TYPE_INT24
      uint32_t val = binlog_util::uint3korr(data);
      if (!is_unsigned && (val & 0x800000) != 0U) {
        val |= 0xFF000000;  // Sign extend for signed values only
      }
      if (is_unsigned) {
        return std::to_string(val);
      }
      return std::to_string(static_cast<int32_t>(val));
    }

    // Floating point types
    case 4: {  // MYSQL_TYPE_FLOAT
      if (data + 4 > end) {
        return "[TRUNCATED]";
      }
      float val = 0;
      memcpy(&val, data, sizeof(float));
      return std::to_string(val);
    }
    case 5: {  // MYSQL_TYPE_DOUBLE
      if (data + 8 > end) {
        return "[TRUNCATED]";
      }
      double val = 0;
      memcpy(&val, data, sizeof(double));
      return std::to_string(val);
    }

    // YEAR type
    case 13: {  // MYSQL_TYPE_YEAR
      // 1 byte: 0 means 0000, 1-255 means year 1901-2155 (value + 1900)
      uint8_t year_byte = *data;
      if (year_byte == 0) {
        return "0000";
      }
      int year = static_cast<int>(year_byte) + 1900;
      return std::to_string(year);
    }

    // BIT type
    case 16: {  // MYSQL_TYPE_BIT
      // metadata: (bytes << 8) | bits
      // Total bytes = bytes + (bits > 0 ? 1 : 0)
      unsigned int full_bytes = (metadata >> 8) & 0xFF;
      unsigned int extra_bits = metadata & 0xFF;
      unsigned int total_bytes = full_bytes + (extra_bits > 0 ? 1 : 0);
      if (data + total_bytes > end) {
        return "[TRUNCATED]";
      }
      // Read bytes as big-endian unsigned integer
      uint64_t val = 0;
      for (unsigned int i = 0; i < total_bytes; i++) {
        val = (val << 8) | data[i];
      }
      return std::to_string(val);
    }

    // String types
    case 15: {  // MYSQL_TYPE_VARCHAR
      uint32_t str_len = 0;
      const unsigned char* str_data = nullptr;
      if (metadata > 255) {
        if (data + 2 > end) {
          return "[TRUNCATED]";
        }
        str_len = binlog_util::uint2korr(data);
        str_data = data + 2;
      } else {
        if (data + 1 > end) {
          return "[TRUNCATED]";
        }
        str_len = *data;
        str_data = data + 1;
      }
      if (str_len > kMaxFieldLength || str_data + str_len > end) {
        mygram::utils::StructuredLog()
            .Event("mysql_binlog_error")
            .Field("type", "varchar_length_exceeds_bounds")
            .Field("length", static_cast<uint64_t>(str_len))
            .Error();
        return "[TRUNCATED]";
      }
      return mygramdb::utils::SanitizeUtf8({reinterpret_cast<const char*>(str_data), str_len});
    }

    case 252: {  // MYSQL_TYPE_BLOB (includes TEXT, MEDIUMTEXT, LONGTEXT)
      uint32_t blob_len = 0;
      const unsigned char* blob_data = nullptr;
      switch (metadata) {
        case 1:  // TINYBLOB/TINYTEXT
          if (data + 1 > end) {
            return "[TRUNCATED]";
          }
          blob_len = *data;
          blob_data = data + 1;
          break;
        case 2:  // BLOB/TEXT
          if (data + 2 > end) {
            return "[TRUNCATED]";
          }
          blob_len = binlog_util::uint2korr(data);
          blob_data = data + 2;
          break;
        case 3:  // MEDIUMBLOB/MEDIUMTEXT
          if (data + 3 > end) {
            return "[TRUNCATED]";
          }
          blob_len = binlog_util::uint3korr(data);
          blob_data = data + 3;
          break;
        case 4:  // LONGBLOB/LONGTEXT
          if (data + 4 > end) {
            return "[TRUNCATED]";
          }
          blob_len = binlog_util::uint4korr(data);
          blob_data = data + 4;
          break;
        default:
          // Invalid metadata - log warning and return error marker
          mygram::utils::StructuredLog()
              .Event("mysql_binlog_error")
              .Field("type", "invalid_blob_metadata")
              .Field("metadata", static_cast<int64_t>(metadata))
              .Error();
          return "[INVALID_BLOB_METADATA]";
      }
      if (blob_len > kMaxFieldLength || blob_data + blob_len > end) {
        mygram::utils::StructuredLog()
            .Event("mysql_binlog_error")
            .Field("type", "blob_length_exceeds_bounds")
            .Field("length", static_cast<uint64_t>(blob_len))
            .Error();
        return "[TRUNCATED]";
      }
      return mygramdb::utils::SanitizeUtf8({reinterpret_cast<const char*>(blob_data), blob_len});
    }

    case 254: {  // MYSQL_TYPE_STRING (CHAR)
      unsigned char type = metadata >> 8;
      if (type == 0xf7 || type == 0xf8) {  // ENUM or SET
        if (data + 1 > end) {
          return "[TRUNCATED]";
        }
        return std::to_string(*data);
      }
      uint32_t max_len = (((metadata >> 4) & 0x300) ^ 0x300) + (metadata & 0xff);
      uint32_t str_len = 0;
      const unsigned char* str_data = nullptr;
      if (max_len > 255) {
        if (data + 2 > end) {
          return "[TRUNCATED]";
        }
        str_len = binlog_util::uint2korr(data);
        str_data = data + 2;
      } else {
        if (data + 1 > end) {
          return "[TRUNCATED]";
        }
        str_len = *data;
        str_data = data + 1;
      }
      if (str_len > kMaxFieldLength || str_data + str_len > end) {
        mygram::utils::StructuredLog()
            .Event("mysql_binlog_error")
            .Field("type", "string_length_exceeds_bounds")
            .Field("length", static_cast<uint64_t>(str_len))
            .Error();
        return "[TRUNCATED]";
      }
      return mygramdb::utils::SanitizeUtf8({reinterpret_cast<const char*>(str_data), str_len});
    }

    // JSON type
    case 245: {  // MYSQL_TYPE_JSON
      uint32_t json_len = 0;
      const unsigned char* json_data = nullptr;
      // JSON typically uses 4 bytes for length, but check metadata
      uint8_t len_bytes = (metadata > 0) ? metadata : 4;
      switch (len_bytes) {
        case 1:
          if (data + 1 > end) {
            return "[TRUNCATED]";
          }
          json_len = *data;
          json_data = data + 1;
          break;
        case 2:
          if (data + 2 > end) {
            return "[TRUNCATED]";
          }
          json_len = binlog_util::uint2korr(data);
          json_data = data + 2;
          break;
        case 3:
          if (data + 3 > end) {
            return "[TRUNCATED]";
          }
          json_len = binlog_util::uint3korr(data);
          json_data = data + 3;
          break;
        case 4:
          if (data + 4 > end) {
            return "[TRUNCATED]";
          }
          json_len = binlog_util::uint4korr(data);
          json_data = data + 4;
          break;
      }
      if (json_len > kMaxFieldLength || json_data + json_len > end) {
        mygram::utils::StructuredLog()
            .Event("mysql_binlog_error")
            .Field("type", "json_length_exceeds_bounds")
            .Field("length", static_cast<uint64_t>(json_len))
            .Error();
        return "[TRUNCATED]";
      }
      return mygramdb::utils::SanitizeUtf8({reinterpret_cast<const char*>(json_data), json_len});
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

    case 7: {  // MYSQL_TYPE_TIMESTAMP (4 bytes)
      // Unix timestamp (seconds since 1970-01-01), no fractional seconds
      uint32_t timestamp = binlog_util::uint4korr(data);
      return std::to_string(timestamp);
    }

    case 17: {  // MYSQL_TYPE_TIMESTAMP2 (4+ bytes)
      // MySQL TIMESTAMP2 format (see mysql-source/mysys/my_time.cc):
      // - 4 bytes for seconds (big-endian)
      // - Additional bytes for fractional seconds based on precision (metadata)
      //
      // Note: TIMESTAMP2 stores seconds in big-endian unlike TIMESTAMP

      // Read 4 bytes in big-endian
      uint32_t timestamp = (static_cast<uint32_t>(data[0]) << 24) | (static_cast<uint32_t>(data[1]) << 16) |
                           (static_cast<uint32_t>(data[2]) << 8) | static_cast<uint32_t>(data[3]);

      // Process fractional seconds if present
      if (metadata > 0) {
        int frac_bytes = (metadata + 1) / 2;
        int32_t frac = 0;
        for (int i = 0; i < frac_bytes; i++) {
          frac = (frac << 8) | data[4 + i];
        }

        // Convert to microseconds based on precision
        uint32_t usec = 0;
        switch (metadata) {
          case 1:
            usec = static_cast<uint32_t>(std::abs(frac)) * 100000;
            break;
          case 2:
            usec = static_cast<uint32_t>(std::abs(frac)) * 10000;
            break;
          case 3:
            usec = static_cast<uint32_t>(std::abs(frac)) * 1000;
            break;
          case 4:
            usec = static_cast<uint32_t>(std::abs(frac)) * 100;
            break;
          case 5:
            usec = static_cast<uint32_t>(std::abs(frac)) * 10;
            break;
          case 6:
            usec = static_cast<uint32_t>(std::abs(frac));
            break;
        }

        std::ostringstream oss;
        oss << timestamp << '.' << std::setfill('0') << std::setw(6) << usec;
        return oss.str();
      }

      return std::to_string(timestamp);
    }

    case 12: {  // MYSQL_TYPE_DATETIME (8 bytes, old format)
      // 8 bytes, packed format
      uint64_t val = binlog_util::uint8korr(data);
      return std::to_string(val);  // Simplified - return as number
    }

    case 18: {  // MYSQL_TYPE_DATETIME2 (5+ bytes, new format)
      // MySQL DATETIME2 format (see mysql-source/mysys/my_time.cc):
      // - 5 bytes base datetime stored as unsigned with DATETIMEF_INT_OFS offset
      // - Additional bytes for fractional seconds based on precision (metadata)
      //
      // The 5 bytes are read as big-endian unsigned, then subtract DATETIMEF_INT_OFS
      // to get the signed packed datetime value.
      //
      // Packed format (40-bit signed integer):
      //   ymdhms = (year * 13 + month) << 22 | day << 17 | hour << 12 | minute << 6 | second

      // Read 5 bytes in big-endian (unsigned)
      uint64_t packed = 0;
      for (int i = 0; i < 5; i++) {
        packed = (packed << 8) | data[i];
      }

      // Subtract the offset to get signed packed value
      // DATETIMEF_INT_OFS = 0x8000000000LL (for binary key compatibility)
      constexpr int64_t kDatetimeIntOfs = 0x8000000000LL;
      int64_t intpart = static_cast<int64_t>(packed) - kDatetimeIntOfs;

      // Handle negative values (dates before year 0) - just use absolute value
      if (intpart < 0) {
        intpart = -intpart;
      }

      // Extract datetime parts per MySQL format:
      // ymd = intpart >> 17
      // ym = ymd >> 5
      // day = ymd % 32 (i.e., ymd & 0x1F)
      // month = ym % 13
      // year = ym / 13
      // hms = intpart % (1 << 17) (i.e., intpart & 0x1FFFF)
      // second = hms % 64 (i.e., hms & 0x3F)
      // minute = (hms >> 6) % 64 (i.e., (hms >> 6) & 0x3F)
      // hour = hms >> 12

      int64_t ymd = intpart >> 17;
      int64_t hms = intpart & 0x1FFFF;
      int64_t year_month = ymd >> 5;

      unsigned int day = ymd & 0x1F;
      unsigned int month = year_month % 13;
      auto year = static_cast<unsigned int>(year_month / 13);

      unsigned int second = hms & 0x3F;
      unsigned int minute = (hms >> 6) & 0x3F;
      auto hour = static_cast<unsigned int>(hms >> 12);

      std::ostringstream oss;
      oss << std::setfill('0') << std::setw(4) << year << '-' << std::setw(2) << month << '-' << std::setw(2) << day
          << ' ' << std::setw(2) << hour << ':' << std::setw(2) << minute << ':' << std::setw(2) << second;

      // Process fractional seconds if present
      if (metadata > 0) {
        int frac_bytes = (metadata + 1) / 2;
        int32_t frac = 0;
        for (int i = 0; i < frac_bytes; i++) {
          frac = (frac << 8) | data[5 + i];
        }

        // Handle signed fractional values (for precision 1-2, it's a signed byte)
        // For precision 3-4, it's signed 2 bytes; for 5-6, signed 3 bytes
        // The fractional part can be negative for dates before epoch

        // Convert to microseconds based on precision
        uint32_t usec = 0;
        switch (metadata) {
          case 1:
            usec = static_cast<uint32_t>(std::abs(frac)) * 100000;
            break;
          case 2:
            usec = static_cast<uint32_t>(std::abs(frac)) * 10000;
            break;
          case 3:
            usec = static_cast<uint32_t>(std::abs(frac)) * 1000;
            break;
          case 4:
            usec = static_cast<uint32_t>(std::abs(frac)) * 100;
            break;
          case 5:
            usec = static_cast<uint32_t>(std::abs(frac)) * 10;
            break;
          case 6:
            usec = static_cast<uint32_t>(std::abs(frac));
            break;
        }

        oss << '.' << std::setw(6) << usec;
      }

      return oss.str();
    }

    case 11: {  // MYSQL_TYPE_TIME (3 bytes, old format)
      // Old TIME format: 3 bytes, stored as HHMMSS
      uint32_t val = binlog_util::uint3korr(data);
      unsigned int second = val % 100;
      unsigned int minute = (val / 100) % 100;
      unsigned int hour = val / 10000;

      std::ostringstream oss;
      oss << std::setfill('0') << std::setw(2) << hour << ':' << std::setw(2) << minute << ':' << std::setw(2)
          << second;
      return oss.str();
    }

    case 19: {  // MYSQL_TYPE_TIME2 (3+ bytes, new format)
      // MySQL TIME2 format (see mysql-source/mysys/my_time.cc):
      // - 3 bytes base time stored as unsigned with TIMEF_INT_OFS offset
      // - Additional bytes for fractional seconds based on precision (metadata)
      //
      // Packed format (24-bit signed integer after offset subtraction):
      //   hms = hour << 12 | minute << 6 | second

      // Read 3 bytes in big-endian (unsigned)
      uint32_t packed = (static_cast<uint32_t>(data[0]) << 16) | (static_cast<uint32_t>(data[1]) << 8) |
                        static_cast<uint32_t>(data[2]);

      // Subtract the offset to get signed packed value
      // TIMEF_INT_OFS = 0x800000LL (for binary key compatibility)
      constexpr int32_t kTimefIntOfs = 0x800000;
      int32_t intpart = static_cast<int32_t>(packed) - kTimefIntOfs;

      // Handle negative time values
      bool negative = intpart < 0;
      if (negative) {
        intpart = -intpart;
      }

      // Extract time parts per MySQL format:
      // hour = hms >> 12 (10 bits)
      // minute = (hms >> 6) & 0x3F (6 bits)
      // second = hms & 0x3F (6 bits)
      unsigned int hour = (intpart >> 12) & 0x3FF;
      unsigned int minute = (intpart >> 6) & 0x3F;
      unsigned int second = intpart & 0x3F;

      std::ostringstream oss;
      if (negative) {
        oss << '-';
      }
      oss << std::setfill('0') << std::setw(2) << hour << ':' << std::setw(2) << minute << ':' << std::setw(2)
          << second;

      // Process fractional seconds if present
      if (metadata > 0) {
        int frac_bytes = (metadata + 1) / 2;
        int32_t frac = 0;
        for (int i = 0; i < frac_bytes; i++) {
          frac = (frac << 8) | data[3 + i];
        }

        // Handle signed fractional values
        uint32_t usec = 0;
        switch (metadata) {
          case 1:
            usec = static_cast<uint32_t>(std::abs(frac)) * 100000;
            break;
          case 2:
            usec = static_cast<uint32_t>(std::abs(frac)) * 10000;
            break;
          case 3:
            usec = static_cast<uint32_t>(std::abs(frac)) * 1000;
            break;
          case 4:
            usec = static_cast<uint32_t>(std::abs(frac)) * 100;
            break;
          case 5:
            usec = static_cast<uint32_t>(std::abs(frac)) * 10;
            break;
          case 6:
            usec = static_cast<uint32_t>(std::abs(frac));
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

    case 255: {  // MYSQL_TYPE_GEOMETRY
      // GEOMETRY is stored like BLOB: length prefix (1-4 bytes based on metadata) + WKB data
      // metadata indicates the number of bytes used for length prefix (1, 2, 3, or 4)
      size_t geo_len = 0;
      const unsigned char* geo_data = nullptr;

      switch (metadata) {
        case 1:
          geo_len = data[0];
          geo_data = data + 1;
          break;
        case 2:
          geo_len = data[0] | (data[1] << 8);
          geo_data = data + 2;
          break;
        case 3:
          geo_len = data[0] | (data[1] << 8) | (data[2] << 16);
          geo_data = data + 3;
          break;
        case 4:
          geo_len = data[0] | (data[1] << 8) | (data[2] << 16) | (data[3] << 24);
          geo_data = data + 4;
          break;
        default:
          return "[GEOMETRY:INVALID_METADATA]";
      }

      // Return WKB data as hex string
      std::ostringstream oss;
      oss << std::hex << std::setfill('0');
      for (size_t i = 0; i < geo_len && geo_data != nullptr; ++i) {
        oss << std::setw(2) << static_cast<int>(geo_data[i]);
      }
      return oss.str();
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
    // binlog_reader already skipped OK byte, buffer points to event data
    // Event size is at bytes [9-12] of event data (little-endian)
    // (see mysql-8.4.7/libs/mysql/binlog/event/binlog_event.h: LOG_EVENT_HEADER_LEN)
    uint32_t event_size = buffer[9] | (buffer[10] << 8) | (buffer[11] << 16) | (buffer[12] << 24);

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

    // flags (2 bytes)
    uint16_t flags = binlog_util::uint2korr(ptr);
    ptr += 2;

    // MySQL 8.0 ROWS_EVENT_V2: skip extra_row_info if present
    // (see mysql-8.4.7/libs/mysql/binlog/event/rows_event.h: EXTRA_ROW_INFO_LEN_OFFSET)
    if ((flags & 0x0001) != 0) {  // ROWS_EVENT_V2 with extra info
      if (ptr >= end) {
        mygram::utils::StructuredLog()
            .Event("mysql_binlog_error")
            .Field("type", "write_rows_too_short")
            .Field("section", "extra_row_info_length")
            .Error();
        return std::nullopt;
      }
      const unsigned char* ptr_before = ptr;
      uint64_t extra_info_len = binlog_util::read_packed_integer(&ptr);
      auto len_bytes = static_cast<int>(ptr - ptr_before);

      // IMPORTANT: extra_info_len is the TOTAL length INCLUDING the packed integer itself.
      // MySQL format: [packed_int_len_byte(s)][extra_row_info_data]
      // If packed_int is 1 byte (value=2), then total=2 means: 1 byte for packed_int + 1 byte data.
      // So we skip (extra_info_len - len_bytes) more bytes.
      // (see mysql-8.4.7/libs/mysql/binlog/event/rows_event.h: EXTRA_ROW_INFO_LEN_OFFSET)
      auto skip_bytes = static_cast<int>(extra_info_len) - len_bytes;
      if (skip_bytes < 0 || ptr + skip_bytes > end) {
        mygram::utils::StructuredLog()
            .Event("mysql_binlog_error")
            .Field("type", "invalid_extra_row_info")
            .Field("event_type", "write_rows")
            .Error();
        return std::nullopt;
      }
      ptr += skip_bytes;
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

    // Parse extra_row_info if present
    size_t extra_info_size = binlog_util::skip_extra_row_info(&ptr, end, flags);
    if (extra_info_size > 0) {
      mygram::utils::StructuredLog()
          .Event("binlog_debug")
          .Field("action", "skipped_extra_row_info")
          .Field("bytes", static_cast<uint64_t>(extra_info_size))
          .Debug();
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
          mygram::utils::StructuredLog()
              .Event("mysql_binlog_error")
              .Field("type", "write_rows_truncated")
              .Field("column_index", static_cast<uint64_t>(col_idx))
              .Error();
          return std::nullopt;
        }

        // Decode field value
        std::string value = DecodeFieldValue(static_cast<uint8_t>(col_meta.type), ptr, col_meta.metadata, is_null, end,
                                             col_meta.is_unsigned);

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

std::optional<std::vector<std::pair<RowData, RowData>>> ParseUpdateRowsEvent(const unsigned char* buffer,
                                                                             unsigned long length,
                                                                             const TableMetadata* table_metadata,
                                                                             const std::string& pk_column_name,
                                                                             const std::string& text_column_name) {
  if ((buffer == nullptr) || (table_metadata == nullptr)) {
    return std::nullopt;
  }

  try {
    // binlog_reader already skipped OK byte, buffer points to event data.
    // Event size is at bytes [9-12] of event data (little-endian)
    // (see mysql-8.4.7/libs/mysql/binlog/event/binlog_event.h: LOG_EVENT_HEADER_LEN)
    uint32_t event_size = buffer[9] | (buffer[10] << 8) | (buffer[11] << 16) | (buffer[12] << 24);

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

    // MySQL 8.0 ROWS_EVENT_V2: skip extra_row_info if present
    // (see mysql-8.4.7/libs/mysql/binlog/event/rows_event.h: EXTRA_ROW_INFO_LEN_OFFSET)
    // The flags field indicates if extra info exists
    if ((flags & 0x0001) != 0) {  // ROWS_EVENT_V2 with extra info
      // Read extra_row_info_length (packed integer)
      if (ptr >= end) {
        mygram::utils::StructuredLog()
            .Event("mysql_binlog_error")
            .Field("type", "update_rows_too_short")
            .Field("section", "extra_row_info_length")
            .Error();
        return std::nullopt;
      }

      // NOLINTBEGIN(modernize-avoid-c-arrays,cppcoreguidelines-avoid-c-arrays)
      // Reason: C-style arrays are used with snprintf() for debug hex formatting
      // Debug: show bytes before reading
      char pre_hex[31];
      for (int i = 0; i < std::min(10, static_cast<int>(end - ptr)); i++) {
        snprintf(&pre_hex[i * 3], 4, "%02x ", ptr[i]);
      }
      mygram::utils::StructuredLog()
          .Event("binlog_debug")
          .Field("action", "before_extra_row_info_read")
          .Field("hex", std::string(pre_hex))
          .Debug();

      const unsigned char* ptr_before = ptr;
      uint64_t extra_info_len = binlog_util::read_packed_integer(&ptr);
      auto len_bytes = static_cast<int>(ptr - ptr_before);

      mygram::utils::StructuredLog()
          .Event("binlog_debug")
          .Field("action", "extra_row_info_len")
          .Field("total_bytes", extra_info_len)
          .Field("packed_int_bytes", static_cast<int64_t>(len_bytes))
          .Debug();

      // IMPORTANT: extra_row_info_len is the TOTAL length INCLUDING the packed integer itself.
      // MySQL format: [packed_int_len_byte(s)][extra_row_info_data]
      // If packed_int is 1 byte (value=2), then total=2 means: 1 byte for packed_int + 1 byte data.
      // So we skip (extra_info_len - len_bytes) more bytes.
      // (see mysql-8.4.7/libs/mysql/binlog/event/rows_event.h: EXTRA_ROW_INFO_LEN_OFFSET)
      auto skip_bytes = static_cast<int>(extra_info_len) - len_bytes;
      if (skip_bytes < 0) {
        mygram::utils::StructuredLog()
            .Event("mysql_binlog_error")
            .Field("type", "invalid_extra_row_info_len")
            .Field("extra_info_len", extra_info_len)
            .Field("packed_int_bytes", static_cast<uint64_t>(len_bytes))
            .Error();
        return std::nullopt;
      }

      if (ptr + skip_bytes > end) {
        mygram::utils::StructuredLog()
            .Event("mysql_binlog_error")
            .Field("type", "update_rows_too_short")
            .Field("section", "extra_row_info data")
            .Error();
        return std::nullopt;
      }

      // Debug: show the extra bytes we're skipping
      if (skip_bytes > 0) {
        char skip_hex[31];
        for (int i = 0; i < std::min(10, skip_bytes); i++) {
          snprintf(&skip_hex[i * 3], 4, "%02x ", ptr[i]);
        }
        mygram::utils::StructuredLog()
            .Event("binlog_debug")
            .Field("action", "skipping_extra_row_info")
            .Field("bytes", static_cast<int64_t>(skip_bytes))
            .Field("hex", std::string(skip_hex))
            .Debug();
      }

      ptr += skip_bytes;  // Skip the extra row info data
      mygram::utils::StructuredLog()
          .Event("binlog_debug")
          .Field("action", "after_extra_row_info_skip")
          .Field("offset", static_cast<int64_t>(ptr - buffer))
          .Debug();
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

    // Debug: log the bytes we're about to read
    char debug_hex[31];
    for (int i = 0; i < std::min(10, static_cast<int>(end - ptr)); i++) {
      snprintf(&debug_hex[i * 3], 4, "%02x ", ptr[i]);
    }
    mygram::utils::StructuredLog()
        .Event("binlog_debug")
        .Field("action", "update_rows_body_start")
        .Field("hex", std::string(debug_hex))
        .Debug();
    // NOLINTEND(modernize-avoid-c-arrays,cppcoreguidelines-avoid-c-arrays)

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

    while (ptr < end) {
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
        // If not, this means we've reached the end (e.g., checksum/padding bytes)
        if (ptr >= end) {
          mygram::utils::StructuredLog()
              .Event("binlog_debug")
              .Field("action", "reached_end_before_image")
              .Field("col_idx", static_cast<uint64_t>(col_idx))
              .Debug();
          goto end_of_rows;  // Exit both loops cleanly
        }

        std::string value = DecodeFieldValue(static_cast<uint8_t>(col_meta.type), ptr, col_meta.metadata, is_null, end,
                                             col_meta.is_unsigned);

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
          goto end_of_rows;  // Exit both loops cleanly
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
        if (ptr >= end) {
          mygram::utils::StructuredLog()
              .Event("binlog_debug")
              .Field("action", "reached_end_after_image")
              .Field("col_idx", static_cast<uint64_t>(col_idx))
              .Debug();
          goto end_of_rows;
        }

        std::string value = DecodeFieldValue(static_cast<uint8_t>(col_meta.type), ptr, col_meta.metadata, is_null, end,
                                             col_meta.is_unsigned);

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
          goto end_of_rows;
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

      row_pairs.emplace_back(std::move(before_row), std::move(after_row));
    }

  end_of_rows:  // Label for graceful early exit from nested loops
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
                                                         const std::string& text_column_name) {
  if ((buffer == nullptr) || (table_metadata == nullptr)) {
    return std::nullopt;
  }

  try {
    // binlog_reader already skipped OK byte, buffer points to event data
    // Event size is at bytes [9-12] of event data (little-endian)
    uint32_t event_size = buffer[9] | (buffer[10] << 8) | (buffer[11] << 16) | (buffer[12] << 24);

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
    uint16_t flags = binlog_util::uint2korr(ptr);
    ptr += 2;  // flags

    // MySQL 8.0 ROWS_EVENT_V2: skip extra_row_info if present
    // (see mysql-8.4.7/libs/mysql/binlog/event/rows_event.h: EXTRA_ROW_INFO_LEN_OFFSET)
    if ((flags & 0x0001) != 0) {  // ROWS_EVENT_V2 with extra info
      if (ptr >= end) {
        mygram::utils::StructuredLog()
            .Event("mysql_binlog_error")
            .Field("type", "delete_rows_too_short")
            .Field("section", "extra_row_info_length")
            .Error();
        return std::nullopt;
      }
      const unsigned char* ptr_before = ptr;
      uint64_t extra_info_len = binlog_util::read_packed_integer(&ptr);
      auto len_bytes = static_cast<int>(ptr - ptr_before);

      // IMPORTANT: extra_info_len is the TOTAL length INCLUDING the packed integer itself.
      // MySQL format: [packed_int_len_byte(s)][extra_row_info_data]
      // If packed_int is 1 byte (value=2), then total=2 means: 1 byte for packed_int + 1 byte data.
      // So we skip (extra_info_len - len_bytes) more bytes.
      // (see mysql-8.4.7/libs/mysql/binlog/event/rows_event.h: EXTRA_ROW_INFO_LEN_OFFSET)
      auto skip_bytes = static_cast<int>(extra_info_len) - len_bytes;
      if (skip_bytes < 0 || ptr + skip_bytes > end) {
        mygram::utils::StructuredLog()
            .Event("mysql_binlog_error")
            .Field("type", "invalid_extra_row_info")
            .Field("event_type", "delete_rows")
            .Error();
        return std::nullopt;
      }
      ptr += skip_bytes;
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

    // Parse extra_row_info if present
    size_t extra_info_size = binlog_util::skip_extra_row_info(&ptr, end, flags);
    if (extra_info_size > 0) {
      mygram::utils::StructuredLog()
          .Event("binlog_debug")
          .Field("action", "skipped_extra_row_info_delete")
          .Field("bytes", static_cast<uint64_t>(extra_info_size))
          .Debug();
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
          mygram::utils::StructuredLog()
              .Event("mysql_binlog_error")
              .Field("type", "delete_rows_truncated")
              .Field("column_index", static_cast<uint64_t>(col_idx))
              .Error();
          return std::nullopt;
        }

        std::string value = DecodeFieldValue(static_cast<uint8_t>(col_meta.type), ptr, col_meta.metadata, is_null, end,
                                             col_meta.is_unsigned);

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

std::unordered_map<std::string, storage::FilterValue> ExtractFilters(
    const RowData& row_data, const std::vector<config::FilterConfig>& filter_configs,
    const std::string& datetime_timezone) {
  std::unordered_map<std::string, storage::FilterValue> filters;

  for (const auto& filter_config : filter_configs) {
    // Check if column exists in row data
    auto iterator = row_data.columns.find(filter_config.name);
    if (iterator == row_data.columns.end()) {
      mygram::utils::StructuredLog()
          .Event("mysql_binlog_warning")
          .Field("type", "filter_column_not_found")
          .Field("column_name", filter_config.name)
          .Warn();
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
      } else if (filter_config.type == "datetime" || filter_config.type == "date") {
        // DATETIME/DATE: Convert to epoch seconds using timezone
        auto epoch_opt = mygramdb::utils::ParseDatetimeValue(value_str, datetime_timezone);
        if (epoch_opt) {
          filters[filter_config.name] = *epoch_opt;
        } else {
          mygram::utils::StructuredLog()
              .Event("mysql_binlog_warning")
              .Field("type", "datetime_conversion_failed")
              .Field("value", value_str)
              .Field("column_name", filter_config.name)
              .Field("timezone", datetime_timezone)
              .Warn();
        }
      } else if (filter_config.type == "timestamp") {
        // TIMESTAMP: Already in epoch seconds (UTC), no timezone conversion needed
        try {
          filters[filter_config.name] = static_cast<uint64_t>(std::stoull(value_str));
        } catch (const std::exception& e) {
          mygram::utils::StructuredLog()
              .Event("mysql_binlog_error")
              .Field("type", "timestamp_conversion_failed")
              .Field("value", value_str)
              .Field("column_name", filter_config.name)
              .Field("error", e.what())
              .Error();
        }
      } else if (filter_config.type == "time") {
        // TIME: Convert to seconds since midnight using DateTimeProcessor
        // Create a temporary MysqlConfig to use DateTimeProcessor
        config::MysqlConfig temp_config;
        temp_config.datetime_timezone = datetime_timezone;
        auto processor_result = temp_config.CreateDateTimeProcessor();
        if (!processor_result) {
          mygram::utils::StructuredLog()
              .Event("mysql_binlog_error")
              .Field("type", "datetime_processor_creation_failed")
              .Field("column_name", filter_config.name)
              .Field("error", processor_result.error().message())
              .Error();
        } else {
          auto seconds_result = processor_result->TimeToSeconds(value_str);
          if (seconds_result) {
            filters[filter_config.name] = storage::TimeValue{*seconds_result};
          } else {
            mygram::utils::StructuredLog()
                .Event("mysql_binlog_warning")
                .Field("type", "time_conversion_failed")
                .Field("value", value_str)
                .Field("column_name", filter_config.name)
                .Field("error", seconds_result.error().message())
                .Warn();
          }
        }
      } else if (filter_config.type == "string" || filter_config.type == "varchar" || filter_config.type == "text") {
        filters[filter_config.name] = value_str;
      } else if (filter_config.type == "boolean") {
        // Boolean: "1"/"true" = true, "0"/"false" = false
        filters[filter_config.name] = (value_str == "1" || value_str == "true");
      } else {
        mygram::utils::StructuredLog()
            .Event("mysql_binlog_warning")
            .Field("type", "unknown_filter_type")
            .Field("filter_type", filter_config.type)
            .Field("column_name", filter_config.name)
            .Warn();
      }
    } catch (const std::exception& e) {
      mygram::utils::StructuredLog()
          .Event("mysql_binlog_error")
          .Field("type", "filter_conversion_failed")
          .Field("value", value_str)
          .Field("column_name", filter_config.name)
          .Field("error", e.what())
          .Error();
    }
  }

  return filters;
}

}  // namespace mygramdb::mysql

// NOLINTEND(cppcoreguidelines-pro-*,cppcoreguidelines-avoid-*,readability-magic-numbers,readability-function-cognitive-complexity,readability-else-after-return)

#endif  // USE_MYSQL
