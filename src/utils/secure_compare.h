/**
 * @file secure_compare.h
 * @brief Constant-time comparisons for secret byte strings
 */

#pragma once

#include <algorithm>
#include <cstddef>
#include <string_view>

namespace mygramdb::utils {

/**
 * @brief Compare byte strings without stopping at the first differing byte
 *
 * The loop scans the longer input, and the length difference is folded into
 * the accumulator. This preserves the same work for equal and unequal token
 * contents, including strings that contain embedded NUL bytes.
 */
inline bool ConstantTimeEqual(std::string_view lhs, std::string_view rhs) {
  std::size_t difference = lhs.size() ^ rhs.size();
  const std::size_t length = std::max(lhs.size(), rhs.size());
  for (std::size_t i = 0; i < length; ++i) {
    const unsigned char left = i < lhs.size() ? static_cast<unsigned char>(lhs[i]) : 0;
    const unsigned char right = i < rhs.size() ? static_cast<unsigned char>(rhs[i]) : 0;
    difference |= static_cast<std::size_t>(left ^ right);
  }
  return difference == 0;
}

}  // namespace mygramdb::utils
