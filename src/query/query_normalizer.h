/**
 * @file query_normalizer.h
 * @brief Query normalization for cache key generation
 */

#pragma once

#include <cstdint>
#include <functional>
#include <string>
#include <string_view>

#include "query/query_parser.h"

namespace mygramdb::cache {

enum class CacheExecutionMode : uint8_t { kRegular = 0, kBooleanAst = 1, kFuzzy = 2, kSynonym = 3 };

struct CacheSemanticContext {
  CacheExecutionMode execution_mode = CacheExecutionMode::kRegular;
  std::string verification_policy;
  uint64_t synonym_revision = 0;
};

/**
 * @brief Normalize queries for cache key generation
 *
 * Serializes queries to an injective, versioned cache-key representation.
 * Multiple queries with the same semantic meaning will produce the same
 * representation, while structural boundaries remain unambiguous.
 *
 * Normalization rules:
 * 1. Whitespace: Preserve execution-significant multiplicity and boundaries
 * 2. Structure: Encode every field with a type tag and byte length
 * 3. Table identity: Preserve the exact canonical table key supplied by the catalog
 * 4. Search text: Apply only the same optional text normalizer used by the index
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
  static std::string Normalize(const query::Query& query, const TextNormalizer& text_normalizer = nullptr,
                               const CacheSemanticContext& semantic_context = {});

 private:
  /**
   * @brief Apply optional index text normalization without changing query whitespace semantics
   */
  static std::string NormalizeSearchText(const std::string& text, const TextNormalizer& text_normalizer);
};

}  // namespace mygramdb::cache
