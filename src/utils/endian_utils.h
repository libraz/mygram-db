/**
 * @file endian_utils.h
 * @brief Endian-aware binary I/O utilities
 *
 * Provides portable little-endian read/write functions for binary serialization.
 * All dump files use little-endian format for cross-platform compatibility.
 */

#pragma once

#include <cstdint>
#include <cstring>
#include <type_traits>

namespace mygram::utils {

namespace detail {

/**
 * @brief Check if the host system is little-endian at compile time
 *
 * This uses a union trick that works in C++17.
 * The result is computed at compile time via constexpr.
 */
constexpr bool IsLittleEndian() {
  // Use a known pattern that differs between big and little endian
  constexpr uint32_t test_value = 0x01020304;
  constexpr uint8_t first_byte = static_cast<uint8_t>(test_value & 0xFF);
  return first_byte == 0x04;  // Little-endian stores LSB first
}

/**
 * @brief Swap bytes for 16-bit integer
 */
inline uint16_t ByteSwap16(uint16_t value) {
  return static_cast<uint16_t>((value >> 8) | (value << 8));
}

/**
 * @brief Swap bytes for 32-bit integer
 */
inline uint32_t ByteSwap32(uint32_t value) {
  return ((value & 0xFF000000U) >> 24) | ((value & 0x00FF0000U) >> 8) | ((value & 0x0000FF00U) << 8) |
         ((value & 0x000000FFU) << 24);
}

/**
 * @brief Swap bytes for 64-bit integer
 */
inline uint64_t ByteSwap64(uint64_t value) {
  return ((value & 0xFF00000000000000ULL) >> 56) | ((value & 0x00FF000000000000ULL) >> 40) |
         ((value & 0x0000FF0000000000ULL) >> 24) | ((value & 0x000000FF00000000ULL) >> 8) |
         ((value & 0x00000000FF000000ULL) << 8) | ((value & 0x0000000000FF0000ULL) << 24) |
         ((value & 0x000000000000FF00ULL) << 40) | ((value & 0x00000000000000FFULL) << 56);
}

}  // namespace detail

/**
 * @brief Convert native value to little-endian for storage
 */
template <typename T>
inline T ToLittleEndian(T value) {
  static_assert(std::is_integral_v<T> || std::is_same_v<T, bool>, "ToLittleEndian requires integral type");

  if constexpr (detail::IsLittleEndian() || sizeof(T) == 1) {
    return value;
  } else {
    if constexpr (sizeof(T) == 2) {
      uint16_t swapped = detail::ByteSwap16(static_cast<uint16_t>(value));
      T result;
      std::memcpy(&result, &swapped, sizeof(T));
      return result;
    } else if constexpr (sizeof(T) == 4) {
      uint32_t swapped = detail::ByteSwap32(static_cast<uint32_t>(value));
      T result;
      std::memcpy(&result, &swapped, sizeof(T));
      return result;
    } else if constexpr (sizeof(T) == 8) {
      uint64_t swapped = detail::ByteSwap64(static_cast<uint64_t>(value));
      T result;
      std::memcpy(&result, &swapped, sizeof(T));
      return result;
    } else {
      static_assert(sizeof(T) <= 8, "Unsupported type size for endian conversion");
      return value;
    }
  }
}

/**
 * @brief Convert little-endian value from storage to native
 */
template <typename T>
inline T FromLittleEndian(T value) {
  // Little-endian to native is the same operation as native to little-endian
  return ToLittleEndian(value);
}

/**
 * @brief Convert double to little-endian for storage
 *
 * Doubles are stored as their binary representation in little-endian byte order.
 */
inline double ToLittleEndianDouble(double value) {
  if constexpr (detail::IsLittleEndian()) {
    return value;
  } else {
    uint64_t bits;
    std::memcpy(&bits, &value, sizeof(double));
    bits = detail::ByteSwap64(bits);
    double result;
    std::memcpy(&result, &bits, sizeof(double));
    return result;
  }
}

/**
 * @brief Convert little-endian double from storage to native
 */
inline double FromLittleEndianDouble(double value) {
  return ToLittleEndianDouble(value);
}

}  // namespace mygram::utils
