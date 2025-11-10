/**
 * @file binlog_util.h
 * @brief Utilities for parsing MySQL binlog binary format
 */

#pragma once

#ifdef USE_MYSQL

#include <cstdint>

namespace mygramdb {
namespace mysql {
namespace binlog_util {

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
  return (uint32_t)(ptr[0]) | ((uint32_t)(ptr[1]) << 8) |
         ((uint32_t)(ptr[2]) << 16) | ((uint32_t)(ptr[3]) << 24);
}

/**
 * @brief Read 8 bytes in little-endian format
 */
inline uint64_t uint8korr(const unsigned char* ptr) {
  return (uint64_t)(ptr[0]) | ((uint64_t)(ptr[1]) << 8) |
         ((uint64_t)(ptr[2]) << 16) | ((uint64_t)(ptr[3]) << 24) |
         ((uint64_t)(ptr[4]) << 32) | ((uint64_t)(ptr[5]) << 40) |
         ((uint64_t)(ptr[6]) << 48) | ((uint64_t)(ptr[7]) << 56);
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
 * Based on MySQL's decimal2string() function
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

  // Digits per byte mapping
  static const int dig2bytes[10] = {0, 1, 1, 2, 2, 3, 3, 4, 4, 4};

  const unsigned char* ptr = data;
  std::string result;

  // Check sign bit (MSB of first byte)
  bool is_negative = ((*ptr & 0x80) == 0);

  // Process integer part
  if (intg_rem > 0) {
    int bytes = dig2bytes[intg_rem];
    int32_t val = 0;
    for (int i = 0; i < bytes; i++) {
      val = (val << 8) | (is_negative ? ~(*ptr) : *ptr);
      ptr++;
    }
    if (intg_rem > 0) {
      result += std::to_string(val);
    }
  }

  for (int i = 0; i < intg0; i++) {
    int32_t val = 0;
    for (int j = 0; j < 4; j++) {
      val = (val << 8) | (is_negative ? ~(*ptr) : *ptr);
      ptr++;
    }
    if (result.empty()) {
      result += std::to_string(val);
    } else {
      char buf[16];
      snprintf(buf, sizeof(buf), "%09d", val);
      result += buf;
    }
  }

  if (result.empty()) {
    result = "0";
  }

  // Add decimal point and fractional part
  if (scale > 0) {
    result += ".";

    for (int i = 0; i < frac0; i++) {
      int32_t val = 0;
      for (int j = 0; j < 4; j++) {
        val = (val << 8) | (is_negative ? ~(*ptr) : *ptr);
        ptr++;
      }
      char buf[16];
      snprintf(buf, sizeof(buf), "%09d", val);
      result += buf;
    }

    if (frac_rem > 0) {
      int bytes = dig2bytes[frac_rem];
      int32_t val = 0;
      for (int i = 0; i < bytes; i++) {
        val = (val << 8) | (is_negative ? ~(*ptr) : *ptr);
        ptr++;
      }
      char buf[16];
      snprintf(buf, sizeof(buf), "%0*d", frac_rem, val);
      result += buf;
    }
  }

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
inline uint32_t calc_field_size(uint8_t col_type, const unsigned char* master_data,
                                uint16_t metadata) {
  switch (col_type) {
    // Fixed-size integer types
    case 1:  // MYSQL_TYPE_TINY
      return 1;
    case 2:  // MYSQL_TYPE_SHORT
      return 2;
    case 3:  // MYSQL_TYPE_LONG
      return 4;
    case 8:  // MYSQL_TYPE_LONGLONG
      return 8;
    case 9:  // MYSQL_TYPE_INT24
      return 3;
    case 13: // MYSQL_TYPE_YEAR
      return 1;

    // VARCHAR
    case 15: { // MYSQL_TYPE_VARCHAR
      uint32_t length = metadata > 255 ? 2 : 1;
      if (length == 1) {
        length += static_cast<uint32_t>(*master_data);
      } else {
        length += uint2korr(master_data);
      }
      return length;
    }

    // BLOB/TEXT types
    case 252: { // MYSQL_TYPE_BLOB (includes TEXT)
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
      }
      return metadata + blob_len;  // length bytes + actual data
    }

    // STRING (CHAR)
    case 254: { // MYSQL_TYPE_STRING
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
    case 10: // MYSQL_TYPE_DATE
      return 3;
    case 11: // MYSQL_TYPE_TIME
      return 3;
    case 12: // MYSQL_TYPE_DATETIME
      return 8;
    case 7:  // MYSQL_TYPE_TIMESTAMP
      return 4;

    // Date/Time types with fractional seconds
    case 19: { // MYSQL_TYPE_TIME2
      // metadata is fractional seconds precision (0-6)
      return 3 + (metadata + 1) / 2;
    }
    case 17: { // MYSQL_TYPE_TIMESTAMP2
      // metadata is fractional seconds precision (0-6)
      return 4 + (metadata + 1) / 2;
    }
    case 18: { // MYSQL_TYPE_DATETIME2
      // metadata is fractional seconds precision (0-6)
      return 5 + (metadata + 1) / 2;
    }

    // DECIMAL types
    case 246: { // MYSQL_TYPE_NEWDECIMAL
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
    case 245: { // MYSQL_TYPE_JSON
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
    case 16: { // MYSQL_TYPE_BIT
      // metadata: (bytes << 8) | bits
      unsigned int bytes = (metadata >> 8) & 0xFF;
      unsigned int bits = metadata & 0xFF;
      return bytes + (bits > 0 ? 1 : 0);
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
inline size_t skip_extra_row_info(const unsigned char** ptr, const unsigned char* end,
                                  uint16_t flags) {
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

}  // namespace binlog_util
}  // namespace mysql
}  // namespace mygramdb

#endif  // USE_MYSQL
