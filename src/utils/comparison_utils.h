/**
 * @file comparison_utils.h
 * @brief Generic comparison helper to eliminate operator dispatch duplication
 */

#pragma once

#include <cmath>
#include <limits>
#include <string_view>

namespace mygram::utils {

/**
 * @brief Compare two values using a string operator ("=", "!=", "<", ">", "<=", ">=")
 *
 * Works with any type that supports the six comparison operators.
 *
 * @tparam T Value type (must support ==, !=, <, >, <=, >=)
 * @param lhs Left-hand side value
 * @param rhs Right-hand side value
 * @param op Operator string
 * @return true if the comparison holds, false otherwise (including unknown operators)
 */
template <typename T>
bool CompareValues(const T& lhs, const T& rhs, std::string_view op) {
  if (op == "=")
    return lhs == rhs;
  if (op == "!=")
    return lhs != rhs;
  if (op == "<")
    return lhs < rhs;
  if (op == ">")
    return lhs > rhs;
  if (op == "<=")
    return lhs <= rhs;
  if (op == ">=")
    return lhs >= rhs;
  return false;
}

/**
 * @brief Compare two double values with epsilon-based equality
 *
 * Uses a fixed epsilon for EQ/NE comparisons. Other operators use direct comparison.
 *
 * @param lhs Left-hand side value
 * @param rhs Right-hand side value
 * @param op Operator string
 * @param epsilon Tolerance for equality comparison (default: 1e-9)
 * @return true if the comparison holds, false otherwise
 */
inline bool CompareDoubleValues(double lhs, double rhs, std::string_view op, double epsilon = 1e-9) {
  if (op == "=")
    return std::abs(lhs - rhs) < epsilon;
  if (op == "!=")
    return std::abs(lhs - rhs) >= epsilon;
  if (op == "<")
    return lhs < rhs;
  if (op == ">")
    return lhs > rhs;
  if (op == "<=")
    return lhs <= rhs;
  if (op == ">=")
    return lhs >= rhs;
  return false;
}

/**
 * @brief Compare two double values with relative-epsilon equality
 *
 * Uses machine epsilon scaled by the larger magnitude for EQ/NE comparisons.
 * This is more robust than fixed epsilon for values of varying magnitude.
 *
 * @param lhs Left-hand side value
 * @param rhs Right-hand side value
 * @param op Operator string
 * @return true if the comparison holds, false otherwise
 */
inline bool CompareDoubleValuesRelative(double lhs, double rhs, std::string_view op) {
  if (op == "=") {
    double max_abs = std::max({1.0, std::abs(lhs), std::abs(rhs)});
    return std::abs(lhs - rhs) < std::numeric_limits<double>::epsilon() * max_abs;
  }
  if (op == "!=") {
    double max_abs = std::max({1.0, std::abs(lhs), std::abs(rhs)});
    return std::abs(lhs - rhs) >= std::numeric_limits<double>::epsilon() * max_abs;
  }
  if (op == "<")
    return lhs < rhs;
  if (op == ">")
    return lhs > rhs;
  if (op == "<=")
    return lhs <= rhs;
  if (op == ">=")
    return lhs >= rhs;
  return false;
}

}  // namespace mygram::utils
