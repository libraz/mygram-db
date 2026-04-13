/**
 * @file dump_format_v1_internal.h
 * @brief Internal helper functions for dump_format_v1 serialization
 *
 * Shared inline helpers used by multiple dump_format_v1_*.cpp translation units.
 * Not part of the public API.
 */

#pragma once

#include <cstdint>
#include <iostream>
#include <string>

#include "storage/dump_format_v1.h"
#include "utils/binary_io.h"
#include "utils/structured_log.h"

namespace mygramdb::storage::dump_v1::internal {

using mygram::utils::ReadBinary;
using mygram::utils::StructuredLog;
using mygram::utils::WriteBinary;

/**
 * @brief Write string to stream (length-prefixed)
 */
inline bool WriteString(std::ostream& output_stream, const std::string& str) {
  auto len = static_cast<uint32_t>(str.size());
  if (!WriteBinary(output_stream, len)) {
    return false;
  }
  if (len > 0) {
    output_stream.write(str.data(), len);
  }
  return output_stream.good();
}

/**
 * @brief Read string from stream (length-prefixed) with field-specific size limit
 * @param input_stream Input stream to read from
 * @param str Output string
 * @param max_length Maximum allowed string length for this field type
 * @return true if read succeeded and length is within limit, false otherwise
 */
inline bool ReadString(std::istream& input_stream, std::string& str, uint32_t max_length = kMaxGeneralStringLength) {
  uint32_t len = 0;
  if (!ReadBinary(input_stream, len)) {
    return false;
  }
  if (len > max_length) {
    StructuredLog()
        .Event("storage_validation_error")
        .Field("type", "string_length_exceeded")
        .Field("length", static_cast<uint64_t>(len))
        .Field("max_length", static_cast<uint64_t>(max_length))
        .Error();
    return false;
  }
  if (len > 0) {
    str.resize(len);
    input_stream.read(str.data(), len);
  } else {
    str.clear();
  }
  return input_stream.good();
}

}  // namespace mygramdb::storage::dump_v1::internal
