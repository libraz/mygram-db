/**
 * @file binary_io.h
 * @brief Binary I/O utilities for stream-based serialization
 *
 * Provides endian-aware binary read/write functions for std::iostream.
 * All multi-byte integers are stored in little-endian format for
 * cross-platform compatibility.
 *
 * This header consolidates duplicate WriteBinary/ReadBinary implementations
 * from document_store.cpp and dump_format_v1.cpp (REF-0006).
 */

#pragma once

#include <cstdint>
#include <istream>
#include <ostream>
#include <string>
#include <type_traits>

#include "utils/endian_utils.h"

namespace mygram::utils {

/**
 * @brief Write binary data to stream in little-endian format
 *
 * All multi-byte integers are stored in little-endian format for
 * cross-platform compatibility.
 *
 * @tparam T Type of data to write (must be trivially copyable)
 * @param output_stream Output stream to write to
 * @param value Value to write
 * @return true if write succeeded, false otherwise
 */
template <typename T>
inline bool WriteBinary(std::ostream& output_stream, const T& value) {
  static_assert(std::is_trivially_copyable_v<T>, "WriteBinary requires trivially copyable type");

  if constexpr (std::is_same_v<T, double>) {
    double le_value = ToLittleEndianDouble(value);
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    output_stream.write(reinterpret_cast<const char*>(&le_value), sizeof(T));
  } else if constexpr (std::is_integral_v<T>) {
    T le_value = ToLittleEndian(value);
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    output_stream.write(reinterpret_cast<const char*>(&le_value), sizeof(T));
  } else {
    // For non-integral types (e.g., structs), write as-is
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    output_stream.write(reinterpret_cast<const char*>(&value), sizeof(T));
  }
  return output_stream.good();
}

/**
 * @brief Read binary data from stream in little-endian format
 *
 * All multi-byte integers are stored in little-endian format for
 * cross-platform compatibility.
 *
 * @tparam T Type of data to read (must be trivially copyable)
 * @param input_stream Input stream to read from
 * @param value Reference to store the read value
 * @return true if read succeeded, false otherwise
 */
template <typename T>
inline bool ReadBinary(std::istream& input_stream, T& value) {
  static_assert(std::is_trivially_copyable_v<T>, "ReadBinary requires trivially copyable type");

  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
  input_stream.read(reinterpret_cast<char*>(&value), sizeof(T));
  if (!input_stream.good()) {
    return false;
  }

  if constexpr (std::is_same_v<T, double>) {
    value = FromLittleEndianDouble(value);
  } else if constexpr (std::is_integral_v<T>) {
    value = FromLittleEndian(value);
  }
  // For non-integral types (e.g., structs), keep as-is

  return true;
}

/**
 * @brief Write a string to stream (length-prefixed with uint32_t)
 *
 * Format: [length: uint32_t][data: char[length]]
 *
 * @param output_stream Output stream to write to
 * @param str String to write
 * @return true if write succeeded, false otherwise
 */
inline bool WriteString(std::ostream& output_stream, const std::string& str) {
  auto len = static_cast<uint32_t>(str.size());
  if (!WriteBinary(output_stream, len)) {
    return false;
  }
  if (len > 0) {
    output_stream.write(str.data(), static_cast<std::streamsize>(len));
  }
  return output_stream.good();
}

/**
 * @brief Read a length-prefixed string from stream
 *
 * Format: [length: uint32_t][data: char[length]]
 *
 * @param input_stream Input stream to read from
 * @param str Reference to store the read string
 * @return true if read succeeded, false otherwise
 */
inline bool ReadString(std::istream& input_stream, std::string& str) {
  uint32_t len = 0;
  if (!ReadBinary(input_stream, len)) {
    return false;
  }
  if (len > 0) {
    str.resize(len);
    input_stream.read(str.data(), static_cast<std::streamsize>(len));
    return input_stream.good();
  }
  str.clear();
  return true;
}

}  // namespace mygram::utils
