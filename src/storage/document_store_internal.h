/**
 * @file document_store_internal.h
 * @brief Internal helper functions for document store serialization
 *
 * Shared inline helpers used by multiple document_store_*.cpp translation units.
 * Not part of the public API.
 */

#pragma once

#include <cstdint>
#include <iostream>

#include "utils/binary_io.h"

namespace mygramdb::storage::internal {

// FilterValue type indices for serialization
// These map to std::variant<std::monostate, bool, int8_t, uint8_t, int16_t, uint16_t, int32_t,
// uint32_t, int64_t, uint64_t, TimeValue, std::string, double>
inline constexpr uint8_t kTypeIndexMonostate = 0;
inline constexpr uint8_t kTypeIndexBool = 1;
inline constexpr uint8_t kTypeIndexInt8 = 2;
inline constexpr uint8_t kTypeIndexUInt8 = 3;
inline constexpr uint8_t kTypeIndexInt16 = 4;
inline constexpr uint8_t kTypeIndexUInt16 = 5;
inline constexpr uint8_t kTypeIndexInt32 = 6;
inline constexpr uint8_t kTypeIndexUInt32 = 7;
inline constexpr uint8_t kTypeIndexInt64 = 8;
inline constexpr uint8_t kTypeIndexUInt64 = 9;
inline constexpr uint8_t kTypeIndexTimeValue = 10;
inline constexpr uint8_t kTypeIndexString = 11;
inline constexpr uint8_t kTypeIndexDouble = 12;

/**
 * @brief Void-returning wrapper for WriteBinary (maintains API compatibility)
 *
 * Delegates to mygram::utils::WriteBinary from binary_io.h.
 * This wrapper maintains the original void return type for compatibility
 * with existing code that doesn't check return values.
 *
 * @tparam T Type of data to write
 * @param output_stream Output stream
 * @param data Reference to data to write
 */
template <typename T>
inline void WriteBinary(std::ostream& output_stream, const T& data) {
  (void)mygram::utils::WriteBinary(output_stream, data);
}

/**
 * @brief Void-returning wrapper for ReadBinary (maintains API compatibility)
 *
 * Delegates to mygram::utils::ReadBinary from binary_io.h.
 * This wrapper maintains the original void return type for compatibility
 * with existing code that doesn't check return values.
 *
 * @tparam T Type of data to read
 * @param input_stream Input stream
 * @param data Reference to data to read into
 */
template <typename T>
inline void ReadBinary(std::istream& input_stream, T& data) {
  (void)mygram::utils::ReadBinary(input_stream, data);
}

}  // namespace mygramdb::storage::internal
