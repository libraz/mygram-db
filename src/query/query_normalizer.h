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
 * Serializes queries to an injective, versioned cache-key representation.
 * Multiple queries with the same semantic meaning will produce the same
 * representation, while structural boundaries remain unambiguous.
 *
 * Normalization rules:
 * 1. Whitespace: Normalize to single spaces
 * 2. Structure: Encode every field with a type tag and byte length
 * 3. Table identity: Preserve the exact canonical table key supplied by the catalog
 * 4. Search text: Normalize whitespace, then apply the optional index text normalizer
 * 5. Clause order: Canonicalize to fixed order
 * 6. Filter order: Sort by the complete (column, operator, value) tuple
 * 7. Presentation clauses: Exclude LIMIT/OFFSET/SORT from the key
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
   * @param text_normalizer Optional index-compatible normalizer for search, AND, and NOT terms
   * The returned string is binary and may contain NUL bytes. It is intended
   * only as input to CacheKeyGenerator.
   * @return Versioned canonical query serialization
   */
  static std::string Normalize(const query::Query& query, const TextNormalizer& text_normalizer = nullptr);

 private:
  /**
   * @brief Normalize search text whitespace and apply optional index text normalization
   */
  static std::string NormalizeSearchText(const std::string& text, const TextNormalizer& text_normalizer);
};

}  // namespace mygramdb::cache
