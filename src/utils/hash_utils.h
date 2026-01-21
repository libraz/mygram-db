/**
 * @file hash_utils.h
 * @brief Hash utilities for heterogeneous lookup
 *
 * Provides transparent hash and equality functors that enable
 * heterogeneous lookup in hash containers (e.g., absl::flat_hash_map).
 * This allows lookups with std::string_view without creating temporary
 * std::string objects, improving performance in hot paths.
 */

#pragma once

#include <cstddef>
#include <functional>
#include <string>
#include <string_view>

namespace mygram::utils {

/**
 * @brief Transparent hash for heterogeneous lookup with string_view
 *
 * Enables O(1) lookup without creating temporary std::string objects.
 * Works with absl::flat_hash_map when is_transparent is defined.
 *
 * Usage:
 *   absl::flat_hash_map<std::string, Value, TransparentStringHash, TransparentStringEqual> map;
 *   map.find(std::string_view("key"));  // No temporary string created
 */
struct TransparentStringHash {
  using is_transparent = void;

  size_t operator()(std::string_view str_view) const { return std::hash<std::string_view>{}(str_view); }
  size_t operator()(const std::string& str) const { return std::hash<std::string_view>{}(str); }
  size_t operator()(const char* str) const { return std::hash<std::string_view>{}(str); }
};

/**
 * @brief Transparent equality for heterogeneous lookup with string_view
 *
 * Enables comparison between different string types without conversion.
 * Works with absl::flat_hash_map when is_transparent is defined.
 */
struct TransparentStringEqual {
  using is_transparent = void;

  bool operator()(std::string_view lhs, std::string_view rhs) const { return lhs == rhs; }
};

}  // namespace mygram::utils
