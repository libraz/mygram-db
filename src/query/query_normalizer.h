/**
 * @file query_normalizer.h
 * @brief Query normalization for cache key generation
 */

#pragma once

#include <functional>
#include <string>
#include <string_view>

#include "query/query_parser.h"

namespace mygramdb::cache {

/**
 * @brief Normalize queries for cache key generation
 *
 * Normalizes queries to maximize cache hit rate while maintaining correctness.
 * Multiple queries with the same semantic meaning will produce the same
 * normalized form.
 *
 * Normalization rules:
 * 1. Whitespace: Normalize to single spaces
 * 2. Keywords: Convert to uppercase (SEARCH, FILTER, etc.)
 * 3. Search text: Normalize whitespace, then apply the optional index text normalizer
 * 4. Clause order: Canonicalize to fixed order
 * 5. Filter order: Sort alphabetically by column name
 * 6. Presentation clauses: Exclude LIMIT/OFFSET/SORT from the key
 *
 * Note: LIMIT, OFFSET, and SORT are intentionally excluded from the normalized
 * form. The cache stores full unsorted results, and presentation clauses are
 * applied when retrieving from cache. This allows a single cache entry to serve
 * pagination and ordering variants for the same query.
 */
class QueryNormalizer {
 public:
  using TextNormalizer = std::function<std::string(std::string_view)>;

  /**
   * @brief Normalize query for cache key generation
   * @param query Parsed query object
   * @param primary_key_column Name of the primary key column for the table
   * @param text_normalizer Optional index-compatible normalizer for search, AND, and NOT terms
   * @return Normalized query string
   */
  static std::string Normalize(const query::Query& query, const std::string& primary_key_column = "id",
                               const TextNormalizer& text_normalizer = nullptr);

 private:
  /**
   * @brief Normalize search text whitespace and apply optional index text normalization
   */
  static std::string NormalizeSearchText(const std::string& text, const TextNormalizer& text_normalizer);

  /**
   * @brief Normalize AND terms
   */
  static std::string NormalizeAndTerms(const std::vector<std::string>& and_terms,
                                       const TextNormalizer& text_normalizer);

  /**
   * @brief Normalize NOT terms
   */
  static std::string NormalizeNotTerms(const std::vector<std::string>& not_terms,
                                       const TextNormalizer& text_normalizer);

  /**
   * @brief Normalize filter conditions
   * Sorts filters alphabetically by column name for consistency
   */
  static std::string NormalizeFilters(const std::vector<query::FilterCondition>& filters);
};

}  // namespace mygramdb::cache
