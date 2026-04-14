/**
 * @file query_parser_internal.h
 * @brief Internal utilities shared across query_parser translation units
 */

#pragma once

#include <algorithm>
#include <cctype>
#include <string>
#include <string_view>

namespace mygramdb::query::internal {

/// Maximum LIMIT value (1000)
constexpr uint32_t kMaxLimit = 1000;

/**
 * @brief Convert string to lowercase
 *
 * Used to normalize column names (MySQL column names are case-insensitive).
 */
inline std::string ToLower(std::string_view str) {
  std::string result(str);
  std::transform(result.begin(), result.end(), result.begin(),
                 [](unsigned char character) { return std::tolower(character); });
  return result;
}

/**
 * @brief Case-insensitive string comparison (optimized, no allocations)
 * @param lhs First string
 * @param rhs Second string
 * @return true if strings are equal ignoring case
 */
inline bool EqualsIgnoreCase(std::string_view lhs, std::string_view rhs) {
  if (lhs.size() != rhs.size()) {
    return false;
  }
  return std::equal(lhs.begin(), lhs.end(), rhs.begin(), [](unsigned char lhs_char, unsigned char rhs_char) {
    return std::tolower(lhs_char) == std::tolower(rhs_char);
  });
}

/**
 * @brief Check if token is a query clause keyword
 * @param token Token to check (should be uppercase)
 * @return true if token is a clause keyword (AND, OR, NOT, FILTER, SORT, LIMIT, OFFSET)
 */
inline bool IsClauseKeyword(const std::string& token) {
  return token == "AND" || token == "OR" || token == "NOT" || token == "FILTER" || token == "SORT" ||
         token == "LIMIT" || token == "OFFSET" || token == "HIGHLIGHT" || token == "FUZZY" || token == "FACET";
}

}  // namespace mygramdb::query::internal
