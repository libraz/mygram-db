/**
 * @file rows_parser_internal.h
 * @brief Internal declarations for rows_parser split translation units
 *
 * This header is not part of the public API. It provides shared declarations
 * between rows_parser.cpp, rows_parser_field_decoder.cpp, and
 * rows_parser_filter.cpp.
 */

#pragma once

#ifdef USE_MYSQL

#include <cstdint>
#include <string>

namespace mygramdb::mysql::internal {

/**
 * @brief Convert fractional seconds to microseconds based on precision metadata.
 * @param frac Raw fractional value from binlog
 * @param precision Metadata precision (1-6)
 * @return Microseconds value
 */
uint32_t FractionalToMicroseconds(int32_t frac, uint8_t precision);

/**
 * @brief Decode a single field value as string
 *
 * @param col_type Column type
 * @param data Pointer to field data
 * @param metadata Type metadata
 * @param is_null Whether the field is NULL
 * @param end Pointer to end of buffer
 * @param is_unsigned Whether the field is unsigned
 * @return String representation of the value
 */
std::string DecodeFieldValue(uint8_t col_type, const unsigned char* data, uint16_t metadata, bool is_null,
                             const unsigned char* end, bool is_unsigned = false);

}  // namespace mygramdb::mysql::internal

#endif  // USE_MYSQL
