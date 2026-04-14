/**
 * @file cache_types.h
 * @brief Cache layer type definitions
 */

#pragma once

#include <string>
#include <unordered_map>

namespace mygramdb::cache {

/**
 * @brief Per-table N-gram configuration for cache invalidation
 *
 * This lightweight struct decouples the cache layer from server::TableContext,
 * carrying only the N-gram settings needed for invalidation and cache key
 * consistency.
 */
struct NgramConfig {
  int ngram_size = 2;                 ///< N-gram size for ASCII/alphanumeric characters
  int kanji_ngram_size = 1;           ///< N-gram size for CJK characters
  bool cross_boundary_ngrams = true;  ///< Generate N-grams spanning CJK/non-CJK boundaries
};

/// Map of table name to its N-gram configuration
using NgramConfigMap = std::unordered_map<std::string, NgramConfig>;

}  // namespace mygramdb::cache
