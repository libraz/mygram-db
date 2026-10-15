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

#include <array>
#include <cstdint>
#include <limits>
#include <optional>
#include <string>

#include "utils/checked_math.h"
#include "utils/error.h"
#include "utils/expected.h"

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
inline bool read_packed_integer(const unsigned char** ptr, const unsigned char* end, uint64_t* value) {
  if (ptr == nullptr || *ptr == nullptr || value == nullptr || *ptr >= end) {
    return false;
  }

  const unsigned char* pos = *ptr;
  if (*pos < 251) {
    *value = static_cast<uint64_t>(*pos);
    *ptr = pos + 1;
    return true;
  }

  if (*pos == 251) {
    *value = 0;
    *ptr = pos + 1;
    return true;
  }

  if (*pos == 252) {
    if (end - pos < 3) {
      return false;
    }
    *value = static_cast<uint64_t>(uint2korr(pos + 1));
    *ptr = pos + 3;
    return true;
  }

  if (*pos == 253) {
    if (end - pos < 4) {
      return false;
    }
    *value = static_cast<uint64_t>(uint3korr(pos + 1));
    *ptr = pos + 4;
    return true;
  }

  if (end - pos < 9) {
    return false;
  }
  *value = static_cast<uint64_t>(uint8korr(pos + 1));
  *ptr = pos + 9;
  return true;
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

/// Bytes needed to hold 0-9 leftover decimal digits (MySQL's dig2bytes table).
inline constexpr int kDecimalDigitsToBytes[10] = {0, 1, 1, 2, 2, 3, 3, 4, 4, 4};

/**
 * @brief Encoded byte length of a DECIMAL/NEWDECIMAL value
 *
 * Based on MySQL's decimal_bin_size() function from strings/decimal.c.
 *
 * Precision and scale arrive verbatim from a TABLE_MAP event, so pairs MySQL
 * would never emit (scale above precision, precision past the protocol maximum)
 * have to be rejected instead of sized. This is the single size rule for the
 * type: every caller that needs the length of a DECIMAL field goes through here.
 *
 * @param precision Total number of digits
 * @param scale Number of digits after the decimal point
 * @return Encoded size in bytes, or std::nullopt if the pair is not valid
 */
inline std::optional<uint32_t> decimal_binary_size(uint8_t precision, uint8_t scale) {
  constexpr uint8_t kMaxDecimalPrecision = 65;
  constexpr uint8_t kMaxDecimalScale = 38;  // MariaDB DECIMAL_MAX_SCALE; MySQL stops at 30
  if (precision == 0 || precision > kMaxDecimalPrecision || scale > precision || scale > kMaxDecimalScale) {
    return std::nullopt;
  }

  int intg = precision - scale;  // Integer part digits
  int intg0 = intg / 9;          // Full 4-byte groups in integer part
  int intg_rem = intg % 9;       // Remaining digits in integer part
  int frac0 = scale / 9;         // Full 4-byte groups in fractional part
  int frac_rem = scale % 9;      // Remaining digits in fractional part

  return static_cast<uint32_t>(intg0 * 4 + kDecimalDigitsToBytes[intg_rem] + frac0 * 4 +
                               kDecimalDigitsToBytes[frac_rem]);
}

/// Largest value a group of 0-9 decimal digits can hold (10^digits - 1).
inline constexpr uint32_t kDecimalGroupMax[10] = {0, 9, 99, 999, 9999, 99999, 999999, 9999999, 99999999, 999999999};

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
 * @return String representation of the decimal value, or kMySQLInvalidMetadata
 *         when the metadata is invalid or a digit group exceeds its digit count
 */
inline mygram::utils::Expected<std::string, mygram::utils::Error> decode_decimal(const unsigned char* data,
                                                                                 uint8_t precision, uint8_t scale) {
  using mygram::utils::ErrorCode;
  using mygram::utils::MakeError;
  using mygram::utils::MakeUnexpected;

  auto encoded_size = decimal_binary_size(precision, scale);
  if (!encoded_size) {
    return MakeUnexpected(MakeError(ErrorCode::kMySQLInvalidMetadata, "Invalid DECIMAL metadata"));
  }

  int intg = precision - scale;  // Integer part digits
  int intg0 = intg / 9;          // Full 4-byte groups in integer part
  int intg_rem = intg % 9;       // Remaining digits in integer part
  int frac0 = scale / 9;         // Full 4-byte groups in fractional part
  int frac_rem = scale % 9;      // Remaining digits in fractional part

  auto total_size = static_cast<int>(*encoded_size);

  // MySQL DECIMAL precision is capped at 65 digits; the encoded byte length is
  // smaller than that, but 65 keeps this stack buffer aligned with the protocol
  // limit and avoids per-row heap allocation.
  constexpr size_t kMaxDecimalBufferBytes = 65;
  if (static_cast<size_t>(total_size) > kMaxDecimalBufferBytes) {
    return MakeUnexpected(MakeError(ErrorCode::kMySQLInvalidMetadata, "Invalid DECIMAL metadata"));
  }

  // Make a copy and apply sign-based transformation
  std::array<unsigned char, kMaxDecimalBufferBytes> buf{};
  for (int i = 0; i < total_size; ++i) {
    buf[static_cast<size_t>(i)] = data[i];
  }

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
    for (int i = 0; i < total_size; ++i) {
      buf[static_cast<size_t>(i)] ^= 0xFF;
    }
  }

  const unsigned char* ptr = buf.data();
  std::string result;
  bool group_in_range = true;

  // Reads one big-endian group holding `digits` decimal digits; a value past
  // 10^digits - 1 is a corrupt packet, not a number.
  auto read_group = [&ptr, &group_in_range](int digits) {
    uint32_t val = 0;
    for (int i = 0; i < kDecimalDigitsToBytes[digits]; i++) {
      if (!mygram::utils::CheckedShift<uint32_t>(val, 8, *ptr++, kDecimalGroupMax[digits])) {
        group_in_range = false;
      }
    }
    return val;
  };

  // Process integer remainder (leading digits that don't fill a 4-byte group)
  if (intg_rem > 0) {
    result += std::to_string(read_group(intg_rem));
  }

  // Process full 4-byte groups in integer part
  for (int i = 0; i < intg0; i++) {
    uint32_t val = read_group(9);
    if (result.empty()) {
      result += std::to_string(val);
    } else {
      char buf_fmt[16];
      snprintf(buf_fmt, sizeof(buf_fmt), "%09u", val);
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
      char buf_fmt[16];
      snprintf(buf_fmt, sizeof(buf_fmt), "%09u", read_group(9));
      result += buf_fmt;
    }

    // Process fractional remainder
    if (frac_rem > 0) {
      char buf_fmt[16];
      snprintf(buf_fmt, sizeof(buf_fmt), "%0*u", frac_rem, read_group(frac_rem));
      result += buf_fmt;
    }
  }

  if (!group_in_range) {
    return MakeUnexpected(MakeError(ErrorCode::kMySQLInvalidMetadata, "DECIMAL digit group out of range"));
  }

  // Add negative sign if needed
  if (is_negative) {
    result = "-" + result;
  }

  return result;
}

/// Returned by calc_field_size when a field's length prefix runs past the buffer; it exceeds any remaining length.
inline constexpr uint64_t kFieldSizeTruncated = std::numeric_limits<uint64_t>::max();

/**
 * @brief Size of a field made of a little-endian length prefix followed by that many bytes
 *
 * The size is returned in 64 bits so that a declared 4-byte length near
 * UINT32_MAX plus its prefix cannot wrap into a small value that passes the
 * caller's bounds check.
 *
 * @return Prefix plus payload bytes, or kFieldSizeTruncated when the prefix does not fit before @p end
 */
inline uint64_t length_prefixed_field_size(const unsigned char* data, const unsigned char* end,
                                           unsigned int prefix_bytes) {
  if (data == nullptr || end == nullptr || data > end || static_cast<size_t>(end - data) < prefix_bytes) {
    return kFieldSizeTruncated;
  }
  uint32_t length = 0;
  switch (prefix_bytes) {
    case 1:
      length = *data;
      break;
    case 2:
      length = uint2korr(data);
      break;
    case 3:
      length = uint3korr(data);
      break;
    default:
      length = uint4korr(data);
      break;
  }
  return uint64_t{prefix_bytes} + length;
}

/**
 * @brief Calculate field size in bytes for a given MySQL column type
 *
 * Based on calc_field_size() from MySQL source:
 * libs/mysql/binlog/event/binary_log_funcs.cpp
 *
 * Only the length prefix of a variable-length field is read, and only when it
 * lies before @p end.
 *
 * @param col_type MySQL column type
 * @param master_data Pointer to the field data
 * @param metadata Type-specific metadata
 * @param end Pointer one past the last readable byte of the row image
 * @return Size of the field in bytes, 0 for an unsupported type or invalid
 *         metadata, or kFieldSizeTruncated when the length prefix is cut off
 */
inline uint64_t calc_field_size(uint8_t col_type, const unsigned char* master_data, uint16_t metadata,
                                const unsigned char* end) {
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
    case 15:     // MYSQL_TYPE_VARCHAR
    case 141: {  // MariaDB MYSQL_TYPE_VARCHAR_COMPRESSED (same framing as VARCHAR)
      return length_prefixed_field_size(master_data, end, metadata > 255 ? 2 : 1);
    }

    // BLOB/TEXT types
    case 140:    // MariaDB MYSQL_TYPE_BLOB_COMPRESSED (same framing as BLOB)
    case 242:    // MYSQL_TYPE_VECTOR (same encoding as BLOB)
    case 249:    // MYSQL_TYPE_TINY_BLOB
    case 250:    // MYSQL_TYPE_MEDIUM_BLOB
    case 251:    // MYSQL_TYPE_LONG_BLOB
    case 252: {  // MYSQL_TYPE_BLOB (includes TEXT)
      // metadata indicates the number of length bytes (1, 2, 3, or 4)
      uint16_t blob_len_bytes = metadata;
      if (blob_len_bytes == 0) {
        if (col_type == 249) {
          blob_len_bytes = 1;
        } else if (col_type == 250) {
          blob_len_bytes = 3;
        } else if (col_type == 251) {
          blob_len_bytes = 4;
        }
      }
      if (blob_len_bytes < 1 || blob_len_bytes > 4) {
        return 0;
      }
      return length_prefixed_field_size(master_data, end, blob_len_bytes);
    }

    // STRING (CHAR)
    case 254: {  // MYSQL_TYPE_STRING
      unsigned char type = metadata >> 8;
      if (type == 0xf7 || type == 0xf8) {  // ENUM or SET
        uint32_t pack_length = metadata & 0xff;
        return pack_length == 0 ? 1 : pack_length;
      } else {
        // Fixed-length or variable-length string
        uint32_t max_len = (((metadata >> 4) & 0x300) ^ 0x300) + (metadata & 0xff);
        return length_prefixed_field_size(master_data, end, max_len > 255 ? 2 : 1);
      }
    }

    case 247:  // MYSQL_TYPE_ENUM
    case 248:  // MYSQL_TYPE_SET
      return metadata == 0 ? 1 : (metadata & 0xff);

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
      // metadata: (precision << 8) | scale. A pair MySQL would never emit
      // yields 0, which the caller reports as an unsupported column type.
      auto precision = static_cast<uint8_t>(metadata >> 8);
      auto scale = static_cast<uint8_t>(metadata & 0xFF);
      return decimal_binary_size(precision, scale).value_or(0);
    }

    // JSON type
    case 245: {  // MYSQL_TYPE_JSON
      // JSON is stored like BLOB; metadata is the number of length bytes
      if (metadata < 1 || metadata > 4) {
        return 0;
      }
      return length_prefixed_field_size(master_data, end, metadata);
    }

    // BIT type
    case 16: {  // MYSQL_TYPE_BIT
      // metadata: (bytes << 8) | bits; a column holds at most 64 bits
      unsigned int bytes = (metadata >> 8) & 0xFF;
      unsigned int bits = metadata & 0xFF;
      const unsigned int total_bytes = bytes + (bits > 0 ? 1 : 0);
      return total_bytes > 8 ? 0 : total_bytes;
    }

    // GEOMETRY type
    case 255: {  // MYSQL_TYPE_GEOMETRY
      // GEOMETRY is stored like BLOB: length prefix (1-4 bytes based on metadata) + WKB data
      if (metadata < 1 || metadata > 4) {
        return 0;
      }
      return length_prefixed_field_size(master_data, end, metadata);
    }

    // For unsupported types, return 0 (will need to be handled specially)
    default:
      return 0;
  }
}

}  // namespace mygramdb::mysql::binlog_util

// NOLINTEND(cppcoreguidelines-pro-bounds-*,cppcoreguidelines-avoid-*,cppcoreguidelines-pro-type-vararg,readability-magic-numbers,readability-function-cognitive-complexity,readability-else-after-return,readability-redundant-casting,readability-math-missing-parentheses,readability-implicit-bool-conversion,modernize-avoid-c-arrays)

#endif  // USE_MYSQL
