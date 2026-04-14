/**
 * @file highlighter.h
 * @brief Search result snippet generation with keyword highlighting
 */

#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "query/query_parser.h"

namespace mygramdb::query {

/**
 * @brief A highlighted snippet fragment with metadata
 */
struct SnippetFragment {
  std::string text;   ///< Fragment text with highlight tags inserted
  uint32_t start_cp;  ///< Start position in code points (for merging)
  uint32_t end_cp;    ///< End position in code points (for merging)
};

/**
 * @brief Highlighted snippet result for a single document
 */
struct HighlightResult {
  std::string snippet;  ///< Final snippet with tags and ellipsis separators
};

/**
 * @brief Search result highlighter
 *
 * Generates snippets from normalized document text with search terms
 * wrapped in configurable highlight tags. Supports:
 * - Multiple search terms
 * - Context windows around matches
 * - Overlapping window merging
 * - Ellipsis joining of non-adjacent fragments
 */
class Highlighter {
 public:
  /**
   * @brief Generate a highlighted snippet for a document
   *
   * @param normalized_text Document's normalized text
   * @param search_terms Normalized search terms to highlight
   * @param options Highlight configuration (tags, snippet length, max fragments)
   * @return Highlighted snippet result
   */
  static HighlightResult Generate(std::string_view normalized_text, const std::vector<std::string>& search_terms,
                                  const HighlightOptions& options);

  /**
   * @brief Find all non-overlapping match positions (in code points) for terms in text
   *
   * @param normalized_text Text to search
   * @param search_terms Terms to find
   * @return Vector of (start_cp, end_cp) pairs sorted by start position
   */
  static std::vector<std::pair<uint32_t, uint32_t>> FindMatchPositions(std::string_view normalized_text,
                                                                       const std::vector<std::string>& search_terms);
};

}  // namespace mygramdb::query
