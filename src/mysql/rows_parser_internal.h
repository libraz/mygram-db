/**
 * @file rows_parser_internal.h
 * @brief Internal declarations for rows_parser split translation units
 *
 * This header is not part of the public API. It exposes the single-value path
 * of the row decoder so that value rendering can be checked one column at a
 * time against the snapshot text.
 */

#pragma once

#ifdef USE_MYSQL

#include <cstdint>
#include <string>
#include <vector>

#include "utils/error.h"
#include "utils/expected.h"

namespace mygramdb::mysql::internal {

/**
 * @brief Decode one column value of a row image into the text a RowData carries.
 *
 * The value takes the path a retained column of a ROWS event takes: the column
 * type table, libmybinlog's decoder and CanonicalizeBinlogValue().
 *
 * @param col_type Column type code as a TABLE_MAP carries it
 * @param data First byte of the value as the row image stores it
 * @param metadata Type metadata as a TABLE_MAP carries it
 * @param is_null Whether the row's NULL bitmap marks the value NULL; @p data is then not read
 * @param end End of the readable bytes; any after the value are ignored
 * @param is_unsigned Whether the column is UNSIGNED
 * @param enum_set_values ENUM labels or SET members; nullptr renders the ordinal or bitmask as a number
 * @return The canonical text (empty for NULL), or the error the row decoder raises for it
 */
mygram::utils::Expected<std::string, mygram::utils::Error> DecodeColumnValue(
    uint8_t col_type, const unsigned char* data, uint16_t metadata, bool is_null, const unsigned char* end,
    bool is_unsigned = false, const std::vector<std::string>* enum_set_values = nullptr);

}  // namespace mygramdb::mysql::internal

#endif  // USE_MYSQL
