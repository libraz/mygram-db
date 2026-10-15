/**
 * @file numeric_parse.h
 * @brief Type-safe numeric parsing utilities using std::from_chars
 *
 * Provides a unified ParseNumeric<T> template for integer and floating-point
 * types using std::from_chars.
 */

#pragma once

#include <charconv>
#include <cmath>
#include <optional>
#include <string_view>
#include <system_error>
#include <type_traits>

#include "utils/namespace_compat.h"

namespace mygramdb::utils {

/**
 * @brief Parse a numeric value from a string using std::from_chars
 *
 * Uses std::from_chars without allocation or locale dependence. Floating-point
 * input follows the general decimal format, including decimal and scientific
 * notation; hexadecimal notation, whitespace, and a leading '+' are rejected.
 * Long double input is parsed as double before conversion, because some standard
 * libraries lack a long-double std::from_chars overload.
 *
 * @tparam T Numeric type (int8_t, uint8_t, int16_t, uint16_t, int32_t,
 *           uint32_t, int64_t, uint64_t, float, double, long double)
 * @param str Input string to parse
 * @return Parsed value, or std::nullopt on failure
 */
template <typename T>
std::optional<T> ParseNumeric(std::string_view str, int base = 10) {
  static_assert(std::is_arithmetic_v<T>, "ParseNumeric requires an arithmetic type");

  if constexpr (std::is_floating_point_v<T>) {
    if (str.empty() || str.front() == '+') {
      return std::nullopt;
    }
    using ParseType = std::conditional_t<std::is_same_v<T, long double>, double, T>;
    ParseType parsed_value{};
    const char* const end = str.data() + str.size();
    const auto [ptr, ec] = std::from_chars(str.data(), end, parsed_value, std::chars_format::general);
    if (ec != std::errc() || ptr != end || !std::isfinite(parsed_value)) {
      return std::nullopt;
    }
    const T value = static_cast<T>(parsed_value);
    if (!std::isfinite(value)) {
      return std::nullopt;
    }
    return value;
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
