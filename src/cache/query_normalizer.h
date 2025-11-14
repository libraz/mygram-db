/**
 * @file query_normalizer.h
 * @brief Query normalization for cache key generation
 */

#pragma once

#include <string>

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
 * 2. Keywords: Convert to uppercase (SEARCH, FILTER, SORT, LIMIT, etc.)
 * 3. Search text: Keep as-is (case-sensitive)
 * 4. Clause order: Canonicalize to fixed order
 * 5. Filter order: Sort alphabetically by column name
 * 6. Default values: Include explicit defaults (SORT id DESC, LIMIT 100, OFFSET 0)
 */
class QueryNormalizer {
 public:
  /**
   * @brief Normalize query for cache key generation
   * @param query Parsed query object
   * @return Normalized query string
   */
  static std::string Normalize(const query::Query& query);

 private:
  /**
   * @brief Normalize search text (keep as-is, no transformations)
   */
  static std::string NormalizeSearchText(const std::string& text);

  /**
   * @brief Normalize AND terms
   */
  static std::string NormalizeAndTerms(const std::vector<std::string>& and_terms);

  /**
   * @brief Normalize NOT terms
   */
  static std::string NormalizeNotTerms(const std::vector<std::string>& not_terms);

  /**
   * @brief Normalize filter conditions
   * Sorts filters alphabetically by column name for consistency
   */
  static std::string NormalizeFilters(const std::vector<query::FilterCondition>& filters);

  /**
   * @brief Normalize SORT clause
   */
  static std::string NormalizeSortClause(const std::optional<query::OrderByClause>& sort, const std::string& table);

  /**
   * @brief Convert filter operator to string
   */
  static std::string FilterOpToString(query::FilterOp filter_op);
};

}  // namespace mygramdb::cache
