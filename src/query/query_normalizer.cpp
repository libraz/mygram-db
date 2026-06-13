/**
 * @file query_normalizer.cpp
 * @brief Query normalization implementation
 */

#include "query/query_normalizer.h"

#include <cctype>
#include <string>

#include "utils/string_utils.h"

namespace mygramdb::cache {

std::string QueryNormalizer::Normalize(const query::Query& query, const TextNormalizer& text_normalizer) {
  std::string result;
  result.reserve(128);  // Pre-allocate to reduce reallocations

  // Start with command type
  switch (query.type) {
    case query::QueryType::SEARCH:
      result += 'S';
      break;
    case query::QueryType::COUNT:
      result += 'C';
      break;
    default:
      // Only SEARCH and COUNT queries are cacheable
      return "";
  }

  // Add table name (lowercase for case-insensitive consistency)
  std::string lowercase_table = mygram::utils::ToLower(query.table);
  result += ' ';
  result += lowercase_table;

  // Add main search text
  if (!query.search_text.empty()) {
    result += ' ';
    result += NormalizeSearchText(query.search_text, text_normalizer);
  }

  // Add AND terms
  if (!query.and_terms.empty()) {
    result += ' ';
    result.append(NormalizeAndTerms(query.and_terms, text_normalizer));
  }

  // Add NOT terms
  if (!query.not_terms.empty()) {
    result += ' ';
    result.append(NormalizeNotTerms(query.not_terms, text_normalizer));
  }

  // Add filters (sorted for consistency)
  if (!query.filters.empty()) {
    result += ' ';
    result.append(NormalizeFilters(query.filters));
  }

  if (query.fuzzy_max_distance.has_value()) {
    result += " FUZZY ";
    result += std::to_string(*query.fuzzy_max_distance);
  }

  // Note: LIMIT, OFFSET, and SORT are intentionally excluded from cache key.
  // The cache stores full unsorted results, and presentation concerns are
  // applied by the request handler after lookup.

  return result;
}

std::string QueryNormalizer::NormalizeSearchText(const std::string& text, const TextNormalizer& text_normalizer) {
  // Normalize whitespace: collapse multiple spaces (including Unicode spaces) to single space
  std::string normalized;
  normalized.reserve(text.size());

  bool prev_was_space = false;
  for (size_t i = 0; i < text.size(); ++i) {
    size_t ws_len = 0;
    bool is_space = mygram::utils::IsUnicodeWhitespace(text, i, ws_len);

    if (is_space) {
      // Skip extra bytes of multi-byte whitespace character (loop will ++i)
      i += ws_len - 1;
      if (!prev_was_space && !normalized.empty()) {
        normalized += ' ';
        prev_was_space = true;
      }
    } else {
      normalized += text[i];
      prev_was_space = false;
    }
  }

  // Remove trailing space if any
  if (!normalized.empty() && normalized.back() == ' ') {
    normalized.pop_back();
  }

  if (text_normalizer) {
    return text_normalizer(normalized);
  }

  return normalized;
}

std::string QueryNormalizer::NormalizeAndTerms(const std::vector<std::string>& and_terms,
                                               const TextNormalizer& text_normalizer) {
  // Sort AND terms for consistent cache key
  std::vector<std::string> sorted_terms;
  sorted_terms.reserve(and_terms.size());
  for (const auto& term : and_terms) {
    sorted_terms.push_back(NormalizeSearchText(term, text_normalizer));
  }
  std::sort(sorted_terms.begin(), sorted_terms.end());

  std::string result;
  for (size_t i = 0; i < sorted_terms.size(); ++i) {
    if (i > 0) {
      result += ' ';
    }
    result.append("AND ");
    result += sorted_terms[i];
  }
  return result;
}

std::string QueryNormalizer::NormalizeNotTerms(const std::vector<std::string>& not_terms,
                                               const TextNormalizer& text_normalizer) {
  // Sort NOT terms for consistent cache key
  std::vector<std::string> sorted_terms;
  sorted_terms.reserve(not_terms.size());
  for (const auto& term : not_terms) {
    sorted_terms.push_back(NormalizeSearchText(term, text_normalizer));
  }
  std::sort(sorted_terms.begin(), sorted_terms.end());

  std::string result;
  for (size_t i = 0; i < sorted_terms.size(); ++i) {
    if (i > 0) {
      result += ' ';
    }
    result.append("NOT ");
    result += sorted_terms[i];
  }
  return result;
}

std::string QueryNormalizer::NormalizeFilters(const std::vector<query::FilterCondition>& filters) {
  // Sort filters by column name for consistent cache key
  std::vector<query::FilterCondition> sorted_filters = filters;
  std::sort(
      sorted_filters.begin(), sorted_filters.end(),
      [](const query::FilterCondition& lhs, const query::FilterCondition& rhs) { return lhs.column < rhs.column; });

  std::string result;
  for (size_t i = 0; i < sorted_filters.size(); ++i) {
    if (i > 0) {
      result += ' ';
    }
    result.append("FILTER ");
    result += sorted_filters[i].column;
    result += ' ';
    result.append(query::FilterOpToString(sorted_filters[i].op));
    result += ' ';
    result += sorted_filters[i].value;
  }
  return result;
}

}  // namespace mygramdb::cache
