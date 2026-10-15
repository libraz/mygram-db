/**
 * @file checked_math.h
 * @brief Range-checked integer accumulation for values decoded from untrusted input
 *
 * Appending decoded bits uses unsigned arithmetic and checks the bound before
 * updating the accumulator. An out-of-range value is reported without wrapping.
 */

#pragma once

#include <limits>
#include <type_traits>

#include "utils/namespace_compat.h"

namespace mygramdb::utils {

/**
 * @brief Compute (acc << bits) | low, rejecting a result above @p max
 *
 * @param acc Accumulator; updated only on success
 * @param bits Shift width; must be below the width of @p UInt
 * @param low Bits appended below the shifted accumulator; must fit in @p bits
 * @param max Largest value the accumulator may hold
 * @return true if the result is within [0, max], false otherwise
 */
template <typename UInt>
[[nodiscard]] constexpr bool CheckedShift(UInt& acc, unsigned bits, UInt low, UInt max) noexcept {
  static_assert(std::is_unsigned_v<UInt>, "CheckedShift works on unsigned arithmetic only");
  if (bits == 0 || bits >= std::numeric_limits<UInt>::digits || (low >> bits) != 0 || acc > (max >> bits)) {
    return false;
  }
  const UInt next = static_cast<UInt>(acc << bits) | low;
  if (next > max) {
    return false;
  }
  acc = next;
  return true;
}

}  // namespace mygramdb::utils
