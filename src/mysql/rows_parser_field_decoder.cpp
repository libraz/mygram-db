/**
 * @file rows_parser_field_decoder.cpp
 * @brief Field value decoder for MySQL binlog ROWS events
 *
 * Contains DecodeFieldValue and FractionalToMicroseconds, extracted from
 * rows_parser.cpp for translation unit splitting.
 */

#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <sstream>

#include "mysql/binlog_util.h"
#include "mysql/rows_parser_internal.h"
#include "utils/error.h"
#include "utils/expected.h"
#include "utils/string_utils.h"
#include "utils/structured_log.h"

#ifdef USE_MYSQL

// NOLINTBEGIN(cppcoreguidelines-pro-*,cppcoreguidelines-avoid-*,readability-magic-numbers,readability-function-cognitive-complexity,readability-else-after-return)

namespace mygramdb::mysql::internal {

using mygram::utils::Error;
using mygram::utils::ErrorCode;
using mygram::utils::Expected;
using mygram::utils::MakeError;
using mygram::utils::MakeUnexpected;

uint32_t FractionalToMicroseconds(int32_t frac, uint8_t precision) {
  static constexpr uint32_t kMultipliers[] = {0, 100000, 10000, 1000, 100, 10, 1};
  if (precision == 0 || precision > 6) {
    return 0;
  }
  return static_cast<uint32_t>(std::abs(frac)) * kMultipliers[precision];
}

Expected<std::string, Error> DecodeFieldValue(uint8_t col_type, const unsigned char* data, uint16_t metadata,
                                              bool is_null, const unsigned char* end, bool is_unsigned) {
  constexpr uint32_t kMaxFieldLength = 256 * 1024 * 1024;  // 256MB max for any field
  if (is_null) {
    return std::string{};  // NULL values represented as empty string
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
        return MakeUnexpected(MakeError(ErrorCode::kMySQLFieldTruncated, "Field data truncated"));
      }
      float val = 0;
      memcpy(&val, data, sizeof(float));
      return std::to_string(val);
    }
    case 5: {  // MYSQL_TYPE_DOUBLE
      if (data + 8 > end) {
        return MakeUnexpected(MakeError(ErrorCode::kMySQLFieldTruncated, "Field data truncated"));
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
        return std::string{"0000"};
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
        return MakeUnexpected(MakeError(ErrorCode::kMySQLFieldTruncated, "Field data truncated"));
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
          return MakeUnexpected(MakeError(ErrorCode::kMySQLFieldTruncated, "Field data truncated"));
        }
        str_len = binlog_util::uint2korr(data);
        str_data = data + 2;
      } else {
        if (data + 1 > end) {
          return MakeUnexpected(MakeError(ErrorCode::kMySQLFieldTruncated, "Field data truncated"));
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
        return MakeUnexpected(MakeError(ErrorCode::kMySQLFieldTruncated, "Field data truncated"));
      }
      return mygram::utils::SanitizeUtf8({reinterpret_cast<const char*>(str_data), str_len});
    }

    case 252: {  // MYSQL_TYPE_BLOB (includes TEXT, MEDIUMTEXT, LONGTEXT)
      uint32_t blob_len = 0;
      const unsigned char* blob_data = nullptr;
      switch (metadata) {
        case 1:  // TINYBLOB/TINYTEXT
          if (data + 1 > end) {
            return MakeUnexpected(MakeError(ErrorCode::kMySQLFieldTruncated, "Field data truncated"));
          }
          blob_len = *data;
          blob_data = data + 1;
          break;
        case 2:  // BLOB/TEXT
          if (data + 2 > end) {
            return MakeUnexpected(MakeError(ErrorCode::kMySQLFieldTruncated, "Field data truncated"));
          }
          blob_len = binlog_util::uint2korr(data);
          blob_data = data + 2;
          break;
        case 3:  // MEDIUMBLOB/MEDIUMTEXT
          if (data + 3 > end) {
            return MakeUnexpected(MakeError(ErrorCode::kMySQLFieldTruncated, "Field data truncated"));
          }
          blob_len = binlog_util::uint3korr(data);
          blob_data = data + 3;
          break;
        case 4:  // LONGBLOB/LONGTEXT
          if (data + 4 > end) {
            return MakeUnexpected(MakeError(ErrorCode::kMySQLFieldTruncated, "Field data truncated"));
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
          return MakeUnexpected(MakeError(ErrorCode::kMySQLInvalidMetadata, "Invalid BLOB metadata"));
      }
      if (blob_len > kMaxFieldLength || blob_data + blob_len > end) {
        mygram::utils::StructuredLog()
            .Event("mysql_binlog_error")
            .Field("type", "blob_length_exceeds_bounds")
            .Field("length", static_cast<uint64_t>(blob_len))
            .Error();
        return MakeUnexpected(MakeError(ErrorCode::kMySQLFieldTruncated, "Field data truncated"));
      }
      return mygram::utils::SanitizeUtf8({reinterpret_cast<const char*>(blob_data), blob_len});
    }

    case 254: {  // MYSQL_TYPE_STRING (CHAR)
      unsigned char type = metadata >> 8;
      if (type == 0xf7 || type == 0xf8) {  // ENUM or SET
        if (data + 1 > end) {
          return MakeUnexpected(MakeError(ErrorCode::kMySQLFieldTruncated, "Field data truncated"));
        }
        return std::to_string(*data);
      }
      uint32_t max_len = (((metadata >> 4) & 0x300) ^ 0x300) + (metadata & 0xff);
      uint32_t str_len = 0;
      const unsigned char* str_data = nullptr;
      if (max_len > 255) {
        if (data + 2 > end) {
          return MakeUnexpected(MakeError(ErrorCode::kMySQLFieldTruncated, "Field data truncated"));
        }
        str_len = binlog_util::uint2korr(data);
        str_data = data + 2;
      } else {
        if (data + 1 > end) {
          return MakeUnexpected(MakeError(ErrorCode::kMySQLFieldTruncated, "Field data truncated"));
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
        return MakeUnexpected(MakeError(ErrorCode::kMySQLFieldTruncated, "Field data truncated"));
      }
      return mygram::utils::SanitizeUtf8({reinterpret_cast<const char*>(str_data), str_len});
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
            return MakeUnexpected(MakeError(ErrorCode::kMySQLFieldTruncated, "Field data truncated"));
          }
          json_len = *data;
          json_data = data + 1;
          break;
        case 2:
          if (data + 2 > end) {
            return MakeUnexpected(MakeError(ErrorCode::kMySQLFieldTruncated, "Field data truncated"));
          }
          json_len = binlog_util::uint2korr(data);
          json_data = data + 2;
          break;
        case 3:
          if (data + 3 > end) {
            return MakeUnexpected(MakeError(ErrorCode::kMySQLFieldTruncated, "Field data truncated"));
          }
          json_len = binlog_util::uint3korr(data);
          json_data = data + 3;
          break;
        case 4:
          if (data + 4 > end) {
            return MakeUnexpected(MakeError(ErrorCode::kMySQLFieldTruncated, "Field data truncated"));
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
        return MakeUnexpected(MakeError(ErrorCode::kMySQLFieldTruncated, "Field data truncated"));
      }
      return mygram::utils::SanitizeUtf8({reinterpret_cast<const char*>(json_data), json_len});
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

        uint32_t usec = FractionalToMicroseconds(frac, metadata);

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

        uint32_t usec = FractionalToMicroseconds(frac, metadata);

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

        uint32_t usec = FractionalToMicroseconds(frac, metadata);

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

    case 242: {  // MYSQL_TYPE_VECTOR (same encoding as BLOB, binary float data)
      uint32_t vec_len = 0;
      const unsigned char* vec_data = nullptr;
      switch (metadata) {
        case 1:
          if (data + 1 > end) {
            return MakeUnexpected(MakeError(ErrorCode::kMySQLFieldTruncated, "Field data truncated"));
          }
          vec_len = *data;
          vec_data = data + 1;
          break;
        case 2:
          if (data + 2 > end) {
            return MakeUnexpected(MakeError(ErrorCode::kMySQLFieldTruncated, "Field data truncated"));
          }
          vec_len = binlog_util::uint2korr(data);
          vec_data = data + 2;
          break;
        case 3:
          if (data + 3 > end) {
            return MakeUnexpected(MakeError(ErrorCode::kMySQLFieldTruncated, "Field data truncated"));
          }
          vec_len = binlog_util::uint3korr(data);
          vec_data = data + 3;
          break;
        case 4:
          if (data + 4 > end) {
            return MakeUnexpected(MakeError(ErrorCode::kMySQLFieldTruncated, "Field data truncated"));
          }
          vec_len = binlog_util::uint4korr(data);
          vec_data = data + 4;
          break;
        default:
          return MakeUnexpected(MakeError(ErrorCode::kMySQLInvalidMetadata, "Invalid VECTOR metadata"));
      }
      if (vec_len > kMaxFieldLength || vec_data + vec_len > end) {
        mygram::utils::StructuredLog()
            .Event("mysql_binlog_error")
            .Field("type", "vector_length_exceeds_bounds")
            .Field("length", static_cast<uint64_t>(vec_len))
            .Error();
        return MakeUnexpected(MakeError(ErrorCode::kMySQLFieldTruncated, "Field data truncated"));
      }
      // VECTOR contains binary float data; return as hex string
      std::ostringstream oss;
      oss << std::hex << std::setfill('0');
      for (uint32_t i = 0; i < vec_len && vec_data != nullptr; ++i) {
        oss << std::setw(2) << static_cast<int>(vec_data[i]);
      }
      return oss.str();
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
          geo_len = static_cast<uint32_t>(data[0]) | (static_cast<uint32_t>(data[1]) << 8);
          geo_data = data + 2;
          break;
        case 3:
          geo_len = static_cast<uint32_t>(data[0]) | (static_cast<uint32_t>(data[1]) << 8) |
                    (static_cast<uint32_t>(data[2]) << 16);
          geo_data = data + 3;
          break;
        case 4:
          geo_len = static_cast<uint32_t>(data[0]) | (static_cast<uint32_t>(data[1]) << 8) |
                    (static_cast<uint32_t>(data[2]) << 16) | (static_cast<uint32_t>(data[3]) << 24);
          geo_data = data + 4;
          break;
        default:
          return MakeUnexpected(MakeError(ErrorCode::kMySQLInvalidMetadata, "Invalid GEOMETRY metadata"));
      }

      // Return WKB data as hex string
      std::ostringstream oss;
      oss << std::hex << std::setfill('0');
      for (size_t i = 0; i < geo_len && geo_data != nullptr; ++i) {
        oss << std::setw(2) << static_cast<int>(geo_data[i]);
      }
      return oss.str();
    }

    case 247: {  // MYSQL_TYPE_ENUM
      // ENUM values are stored as 1 or 2 byte integers
      // The metadata byte tells us the size
      uint8_t enum_size = (metadata >> 8) & 0xFF;  // high byte of metadata
      if (enum_size == 0) {
        enum_size = 1;  // default to 1 byte if metadata not available
      }
      if (enum_size == 1) {
        if (data + 1 > end)
          return MakeUnexpected(MakeError(ErrorCode::kMySQLFieldTruncated, "Field data truncated"));
        uint8_t val = *data;
        return std::to_string(val);
      } else {
        if (data + 2 > end)
          return MakeUnexpected(MakeError(ErrorCode::kMySQLFieldTruncated, "Field data truncated"));
        uint16_t val = data[0] | (static_cast<uint16_t>(data[1]) << 8);
        return std::to_string(val);
      }
    }
    case 248: {  // MYSQL_TYPE_SET
      // SET values are stored as 1-8 byte bitmask
      uint8_t set_size = (metadata >> 8) & 0xFF;
      if (set_size == 0) {
        set_size = 1;
      }
      if (set_size > 8)
        set_size = 8;
      if (data + set_size > end)
        return MakeUnexpected(MakeError(ErrorCode::kMySQLFieldTruncated, "Field data truncated"));
      uint64_t val = 0;
      for (uint8_t i = 0; i < set_size; i++) {
        val |= static_cast<uint64_t>(data[i]) << (i * 8);
      }
      return std::to_string(val);
    }

    default:
      return MakeUnexpected(
          MakeError(ErrorCode::kMySQLUnsupportedType, "Unsupported column type: " + std::to_string(col_type)));
  }
}

}  // namespace mygramdb::mysql::internal

// NOLINTEND(cppcoreguidelines-pro-*,cppcoreguidelines-avoid-*,readability-magic-numbers,readability-function-cognitive-complexity,readability-else-after-return)

#endif  // USE_MYSQL
