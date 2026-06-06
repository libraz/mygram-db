/**
 * @file numeric_parse.h
 * @brief Type-safe numeric parsing utilities using std::from_chars
 *
 * Provides a unified ParseNumeric<T> template for all integer types and
 * a specialization for double that wraps std::stod (since std::from_chars
 * for floating-point is not reliably available on Apple Clang / older compilers).
 */

#pragma once

#include <algorithm>
#include <cctype>
#include <charconv>
#include <cmath>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>

#include "utils/namespace_compat.h"

namespace mygramdb::utils {

/**
 * @brief Parse a numeric value from a string using std::from_chars
 *
 * For integer types, uses std::from_chars (no-throw, no-allocation).
 * For double, falls back to std::stod wrapped in exception handling
 * because std::from_chars for floating-point is not available on all
 * C++17 compilers (notably Apple Clang).
 *
 * @tparam T Numeric type (int8_t, uint8_t, int16_t, uint16_t, int32_t,
 *           uint32_t, int64_t, uint64_t, double)
 * @param str Input string to parse
 * @return Parsed value, or std::nullopt on failure
 */
template <typename T>
std::optional<T> ParseNumeric(std::string_view str, int base = 10) {
  static_assert(std::is_arithmetic_v<T>, "ParseNumeric requires an arithmetic type");

  if constexpr (std::is_floating_point_v<T>) {
    // std::from_chars for floating-point is not reliably available in C++17
    // (Apple Clang does not support it). Use std::stod with exception handling.
    if (str.empty() || str.front() == '+') {
      return std::nullopt;
    }
    if (std::any_of(str.begin(), str.end(), [](unsigned char ch) { return std::isspace(ch) != 0; })) {
      return std::nullopt;
    }

    try {
      std::string tmp(str);
      size_t pos = 0;
      double val = std::stod(tmp, &pos);
      if (pos != tmp.size()) {
        return std::nullopt;  // Trailing characters
      }
      if (!std::isfinite(val)) {
        return std::nullopt;
      }
      return static_cast<T>(val);
    } catch (...) {
      return std::nullopt;
    }
  } else {
    T val{};
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
    auto [ptr, ec] = std::from_chars(str.data(), str.data() + str.size(), val, base);
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
    if (ec != std::errc() || ptr != str.data() + str.size()) {
      return std::nullopt;
    }
    return val;
  }
}

}  // namespace mygramdb::utils
