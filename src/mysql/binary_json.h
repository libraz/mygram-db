/**
 * @file binary_json.h
 * @brief Decoder for MySQL's binary JSON representation.
 */

#pragma once

#ifdef USE_MYSQL

#include <cstddef>
#include <string>

#include "utils/error.h"
#include "utils/expected.h"

namespace mygramdb::mysql {

/**
 * Decode one MySQL binary-JSON value to canonical JSON text.
 *
 * The input starts with MySQL's one-byte JSON type tag. Invalid, truncated, or
 * unsupported values are rejected instead of being exposed as binary text.
 */
mygram::utils::Expected<std::string, mygram::utils::Error> DecodeBinaryJson(const unsigned char* data, size_t size);

}  // namespace mygramdb::mysql

#endif  // USE_MYSQL
