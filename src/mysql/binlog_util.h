/**
 * @file binlog_util.h
 * @brief Utilities for parsing MySQL binlog binary format
 *
 * Note: This file contains low-level binary protocol parsing functions that
 * must match MySQL's wire format exactly. Modern C++ guidelines are relaxed:
 * - Pointer arithmetic is required for binary parsing
 * - Magic numbers represent MySQL protocol constants
 * - C-style arrays and casts are necessary for protocol compatibility
 */

#pragma once

#ifdef USE_MYSQL

#include <cstdint>
#include <string>
#include <vector>

// NOLINTBEGIN(cppcoreguidelines-pro-bounds-*,cppcoreguidelines-avoid-*,cppcoreguidelines-pro-type-vararg,readability-magic-numbers,readability-function-cognitive-complexity,readability-else-after-return,readability-redundant-casting,readability-math-missing-parentheses,readability-implicit-bool-conversion,modernize-avoid-c-arrays)

namespace mygramdb::mysql::binlog_util {

/**
 * @brief Read 2 bytes in little-endian format
 */
inline uint16_t uint2korr(const unsigned char* ptr) {
  return (uint16_t)(ptr[0]) | ((uint16_t)(ptr[1]) << 8);
}

/**
 * @brief Read 3 bytes in little-endian format
 */
inline uint32_t uint3korr(const unsigned char* ptr) {
  return (uint32_t)(ptr[0]) | ((uint32_t)(ptr[1]) << 8) | ((uint32_t)(ptr[2]) << 16);
}

/**
 * @brief Read 4 bytes in little-endian format
 */
inline uint32_t uint4korr(const unsigned char* ptr) {
  return (uint32_t)(ptr[0]) | ((uint32_t)(ptr[1]) << 8) | ((uint32_t)(ptr[2]) << 16) | ((uint32_t)(ptr[3]) << 24);
}

/**
 * @brief Read 8 bytes in little-endian format
 */
inline uint64_t uint8korr(const unsigned char* ptr) {
  return (uint64_t)(ptr[0]) | ((uint64_t)(ptr[1]) << 8) | ((uint64_t)(ptr[2]) << 16) | ((uint64_t)(ptr[3]) << 24) |
         ((uint64_t)(ptr[4]) << 32) | ((uint64_t)(ptr[5]) << 40) | ((uint64_t)(ptr[6]) << 48) |
         ((uint64_t)(ptr[7]) << 56);
}

/**
 * @brief Read packed integer (length-encoded integer)
 *
 * Based on MySQL's net_field_length_ll
 *
 * Format:
 * - If first byte < 251: value is 1 byte
 * - If first byte == 252: value is next 2 bytes
 * - If first byte == 253: value is next 3 bytes
 * - If first byte == 254: value is next 8 bytes
 * - If first byte == 251: NULL (returns 0)
 */
inline uint64_t read_packed_integer(const unsigned char** ptr) {
  const unsigned char* pos = *ptr;

  if (*pos < 251) {
    (*ptr)++;
    return (uint64_t)*pos;
  }

  if (*pos == 251) {
    // NULL value
    (*ptr)++;
    return 0;
  }

  if (*pos == 252) {
    (*ptr) += 3;
    return (uint64_t)uint2korr(pos + 1);
  }

  if (*pos == 253) {
    (*ptr) += 4;
    return (uint64_t)uint3korr(pos + 1);
  }

  // Must be 254
  (*ptr) += 9;
  return (uint64_t)uint8korr(pos + 1);
}

/**
 * @brief Calculate number of bytes needed for bitmap
 */
inline size_t bitmap_bytes(size_t bit_count) {
  return (bit_count + 7) / 8;
}

/**
 * @brief Check if bit is set in bitmap
 */
inline bool bitmap_is_set(const unsigned char* bitmap, size_t bit_index) {
  return (bitmap[bit_index / 8] & (1 << (bit_index % 8))) != 0;
}

/**
 * @brief Decode MySQL DECIMAL/NEWDECIMAL binary format
 *
 * Based on MySQL's bin2decimal() function from strings/decimal.c
 *
 * MySQL DECIMAL binary format:
 * - Sign bit is stored in MSB of first byte (0x80)
 * - For storage: positive values have MSB set, negative values have MSB clear
 * - Positive encoding: XOR first byte with 0x80
 * - Negative encoding: XOR all bytes with 0xFF, then XOR first byte with 0x80
 *
 * Decoding:
 * - Check MSB: if set (>= 0x80) -> positive, if clear (< 0x80) -> negative
 * - For positive: XOR first byte with 0x80 to restore
 * - For negative: XOR all bytes with 0xFF to restore (includes undoing the 0x80)
 *
 * @param data Pointer to binary decimal data
 * @param precision Total number of digits
 * @param scale Number of digits after decimal point
 * @return String representation of the decimal value
 */
inline std::string decode_decimal(const unsigned char* data, uint8_t precision, uint8_t scale) {
  if (precision == 0) {
    return "0";
  }

  int intg = precision - scale;  // Integer part digits
  int intg0 = intg / 9;          // Full 4-byte groups in integer part
  int intg_rem = intg % 9;       // Remaining digits in integer part
  int frac0 = scale / 9;         // Full 4-byte groups in fractional part
  int frac_rem = scale % 9;      // Remaining digits in fractional part

  // Digits per byte mapping: how many bytes to store n digits (1-9)
  static const int dig2bytes[10] = {0, 1, 1, 2, 2, 3, 3, 4, 4, 4};

  // Calculate total size
  int total_size = dig2bytes[intg_rem] + intg0 * 4 + frac0 * 4 + dig2bytes[frac_rem];
  if (total_size == 0) {
    return "0";
  }

  // Make a copy and apply sign-based transformation
  std::vector<unsigned char> buf(data, data + total_size);

  // Check sign bit (MSB of first byte): 0x80 set = positive, 0x80 clear = negative
  bool is_negative = (buf[0] & 0x80) == 0;

  // Apply transformation to restore original values
  // MySQL encoding:
  // - Positive: XOR first byte with 0x80
  // - Negative: XOR all bytes with 0xFF, then XOR first byte with 0x80
  // To decode, we reverse this:
  // - Positive: XOR first byte with 0x80
  // - Negative: XOR first byte with 0x80, then XOR all bytes with 0xFF
  buf[0] ^= 0x80;  // Reverse sign bit toggle for both positive and negative
  if (is_negative) {
    // For negative: also XOR all bytes with 0xFF to get original magnitude
    for (auto& b : buf) {
      b ^= 0xFF;
    }
  }

  const unsigned char* ptr = buf.data();
  std::string result;

  // Process integer remainder (leading digits that don't fill a 4-byte group)
  if (intg_rem > 0) {
    int bytes = dig2bytes[intg_rem];
    int32_t val = 0;
    for (int i = 0; i < bytes; i++) {
      val = (val << 8) | *ptr++;
    }
    result += std::to_string(val);
  }

  // Process full 4-byte groups in integer part
  for (int i = 0; i < intg0; i++) {
    int32_t val = 0;
    for (int j = 0; j < 4; j++) {
      val = (val << 8) | *ptr++;
    }
    if (result.empty()) {
      result += std::to_string(val);
    } else {
      char buf_fmt[16];
      snprintf(buf_fmt, sizeof(buf_fmt), "%09d", val);
      result += buf_fmt;
    }
  }

  if (result.empty()) {
    result = "0";
  }

  // Add decimal point and fractional part
  if (scale > 0) {
    result += ".";

    // Process full 4-byte groups in fractional part
    for (int i = 0; i < frac0; i++) {
      int32_t val = 0;
      for (int j = 0; j < 4; j++) {
        val = (val << 8) | *ptr++;
      }
      char buf_fmt[16];
      snprintf(buf_fmt, sizeof(buf_fmt), "%09d", val);
      result += buf_fmt;
    }

    // Process fractional remainder
    if (frac_rem > 0) {
      int bytes = dig2bytes[frac_rem];
      int32_t val = 0;
      for (int i = 0; i < bytes; i++) {
        val = (val << 8) | *ptr++;
      }
      char buf_fmt[16];
      snprintf(buf_fmt, sizeof(buf_fmt), "%0*d", frac_rem, val);
      result += buf_fmt;
    }
  }

  // Add negative sign if needed
  if (is_negative) {
    result = "-" + result;
  }

  return result;
}

/**
 * @brief Calculate field size in bytes for a given MySQL column type
 *
 * Based on calc_field_size() from MySQL source:
 * libs/mysql/binlog/event/binary_log_funcs.cpp
 *
 * @param col_type MySQL column type
 * @param master_data Pointer to the field data
 * @param metadata Type-specific metadata
 * @return Size of the field in bytes
 */
inline uint32_t calc_field_size(uint8_t col_type, const unsigned char* master_data, uint16_t metadata) {
  switch (col_type) {
    // Fixed-size integer types
    case 1:  // MYSQL_TYPE_TINY
      return 1;
    case 2:  // MYSQL_TYPE_SHORT
      return 2;
    case 3:  // MYSQL_TYPE_LONG
      return 4;
    case 4:  // MYSQL_TYPE_FLOAT
      return 4;
    case 5:  // MYSQL_TYPE_DOUBLE
      return 8;
    case 8:  // MYSQL_TYPE_LONGLONG
      return 8;
    case 9:  // MYSQL_TYPE_INT24
      return 3;
    case 13:  // MYSQL_TYPE_YEAR
      return 1;

    // VARCHAR
    case 15: {  // MYSQL_TYPE_VARCHAR
      uint32_t length = metadata > 255 ? 2 : 1;
      if (length == 1) {
        length += static_cast<uint32_t>(*master_data);
      } else {
        length += uint2korr(master_data);
      }
      return length;
    }

    // BLOB/TEXT types
    case 252: {  // MYSQL_TYPE_BLOB (includes TEXT)
      // metadata indicates the number of length bytes (1, 2, 3, or 4)
      uint32_t blob_len = 0;
      switch (metadata) {
        case 1:
          blob_len = *master_data;
          break;
        case 2:
          blob_len = uint2korr(master_data);
          break;
        case 3:
          blob_len = uint3korr(master_data);
          break;
        case 4:
          blob_len = uint4korr(master_data);
          break;
        default:
          // Invalid metadata - return 0 to indicate error
          return 0;
      }
      return metadata + blob_len;  // length bytes + actual data
    }

    // STRING (CHAR)
    case 254: {  // MYSQL_TYPE_STRING
      unsigned char type = metadata >> 8;
      if (type == 0xf7 || type == 0xf8) {  // ENUM or SET
        return metadata & 0xff;
      } else {
        // Fixed-length or variable-length string
        uint32_t max_len = (((metadata >> 4) & 0x300) ^ 0x300) + (metadata & 0xff);
        uint32_t length = max_len > 255 ? 2 : 1;
        if (length == 1) {
          length += *master_data;
        } else {
          length += uint2korr(master_data);
        }
        return length;
      }
    }

    // NULL type
    case 6:  // MYSQL_TYPE_NULL
      return 0;

    // Date/Time types
    case 10:  // MYSQL_TYPE_DATE
      return 3;
    case 11:  // MYSQL_TYPE_TIME
      return 3;
    case 12:  // MYSQL_TYPE_DATETIME
      return 8;
    case 7:  // MYSQL_TYPE_TIMESTAMP
      return 4;

    // Date/Time types with fractional seconds
    case 19: {  // MYSQL_TYPE_TIME2
      // metadata is fractional seconds precision (0-6)
      return 3 + (metadata + 1) / 2;
    }
    case 17: {  // MYSQL_TYPE_TIMESTAMP2
      // metadata is fractional seconds precision (0-6)
      return 4 + (metadata + 1) / 2;
    }
    case 18: {  // MYSQL_TYPE_DATETIME2
      // metadata is fractional seconds precision (0-6)
      return 5 + (metadata + 1) / 2;
    }

    // DECIMAL types
    case 246: {  // MYSQL_TYPE_NEWDECIMAL
      // metadata: (precision << 8) | scale
      uint8_t precision = metadata >> 8;
      uint8_t scale = metadata & 0xFF;
      // Decimal binary size calculation
      // Based on MySQL's decimal_bin_size() function
      int intg = precision - scale;  // Integer part digits
      int intg0 = intg / 9;          // Full 4-byte groups
      int intg_rem = intg % 9;       // Remaining digits
      int frac0 = scale / 9;         // Full 4-byte groups in fractional part
      int frac_rem = scale % 9;      // Remaining digits in fractional part

      // Bytes needed for remaining digits
      static const int dig2bytes[10] = {0, 1, 1, 2, 2, 3, 3, 4, 4, 4};

      return intg0 * 4 + dig2bytes[intg_rem] + frac0 * 4 + dig2bytes[frac_rem];
    }

    // JSON type
    case 245: {  // MYSQL_TYPE_JSON
      // JSON is stored like BLOB with metadata indicating length bytes
      uint32_t json_len = 0;
      switch (metadata) {
        case 1:
          json_len = *master_data;
          break;
        case 2:
          json_len = uint2korr(master_data);
          break;
        case 3:
          json_len = uint3korr(master_data);
          break;
        case 4:
          json_len = uint4korr(master_data);
          break;
        default:
          // JSON typically uses 4 bytes for length
          json_len = uint4korr(master_data);
          metadata = 4;
          break;
      }
      return metadata + json_len;
    }

    // BIT type
    case 16: {  // MYSQL_TYPE_BIT
      // metadata: (bytes << 8) | bits
      unsigned int bytes = (metadata >> 8) & 0xFF;
      unsigned int bits = metadata & 0xFF;
      return bytes + (bits > 0 ? 1 : 0);
    }

    // GEOMETRY type
    case 255: {  // MYSQL_TYPE_GEOMETRY
      // GEOMETRY is stored like BLOB: length prefix (1-4 bytes based on metadata) + WKB data
      // metadata indicates the number of bytes used for length prefix
      uint32_t geo_len = 0;
      switch (metadata) {
        case 1:
          geo_len = *master_data;
          break;
        case 2:
          geo_len = uint2korr(master_data);
          break;
        case 3:
          geo_len = uint3korr(master_data);
          break;
        case 4:
          geo_len = uint4korr(master_data);
          break;
        default:
          // Invalid metadata - return 0 to indicate error
          return 0;
      }
      return metadata + geo_len;  // length bytes + WKB data
    }

    // For unsupported types, return 0 (will need to be handled specially)
    default:
      return 0;
  }
}

/**
 * @brief ROWS event flags (from post-header)
 */
constexpr uint16_t ROWS_EVENT_END_OF_STATEMENT = 0x0001;
constexpr uint16_t ROWS_EVENT_EXTRA_DATA_PRESENT = 0x0002;

/**
 * @brief Extra row info type codes
 */
enum class ExtraRowInfoType : uint8_t {
  NDB = 0,       // MySQL Cluster (NDB) info
  PART = 1,      // Partition info
  JSON_DIFF = 2  // JSON partial update diff
};

/**
 * @brief Parse extra_row_info section
 *
 * Format: [length: 2 bytes][data: length-2 bytes]
 * Data contains TLV (Type-Length-Value) encoded sections
 *
 * @param ptr Pointer to extra_row_info start (will be updated)
 * @param end Pointer to buffer end
 * @param flags ROWS event flags
 * @return Size of extra_row_info section (including length field)
 */
inline size_t skip_extra_row_info(const unsigned char** ptr, const unsigned char* end, uint16_t flags) {
  // Check if extra_row_info is present
  if (!(flags & ROWS_EVENT_EXTRA_DATA_PRESENT)) {
    return 0;  // No extra data
  }

  // Read length field (2 bytes)
  if (*ptr + 2 > end) {
    return 0;  // Invalid
  }

  uint16_t extra_data_len = uint2korr(*ptr);
  *ptr += 2;

  // extra_data_len includes the 2-byte length field itself
  if (extra_data_len < 2 || *ptr + (extra_data_len - 2) > end) {
    return 0;  // Invalid length
  }

  // Skip the extra data (we don't parse it for now)
  *ptr += (extra_data_len - 2);

  return extra_data_len;
}

}  // namespace mygramdb::mysql::binlog_util

// NOLINTEND(cppcoreguidelines-pro-bounds-*,cppcoreguidelines-avoid-*,cppcoreguidelines-pro-type-vararg,readability-magic-numbers,readability-function-cognitive-complexity,readability-else-after-return,readability-redundant-casting,readability-math-missing-parentheses,readability-implicit-bool-conversion,modernize-avoid-c-arrays)

#endif  // USE_MYSQL
