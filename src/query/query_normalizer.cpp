/**
 * @file query_normalizer.cpp
 * @brief Query normalization implementation
 */

#include "query/query_normalizer.h"

#include <algorithm>
#include <cctype>
#include <string>

#include "utils/string_utils.h"

namespace mygramdb::cache {

std::string QueryNormalizer::Normalize(const query::Query& query, const std::string& primary_key_column) {
  std::string result;
  result.reserve(128);  // Pre-allocate to reduce reallocations

  // Start with command type
  switch (query.type) {
    case query::QueryType::SEARCH:
    case query::QueryType::COUNT:
      result += 'Q';  // Unified prefix: both SEARCH and COUNT cache full results
      break;
    default:
      // Only SEARCH and COUNT queries are cacheable
      return "";
  }

  // Add table name (lowercase for case-insensitive consistency)
  std::string lowercase_table = query.table;
  std::transform(lowercase_table.begin(), lowercase_table.end(), lowercase_table.begin(),
                 [](unsigned char chr) { return std::tolower(chr); });
  result += ' ';
  result += lowercase_table;

  // Add main search text
  if (!query.search_text.empty()) {
    result += ' ';
    result += NormalizeSearchText(query.search_text);
  }

  // Add AND terms
  if (!query.and_terms.empty()) {
    result += ' ';
    result.append(NormalizeAndTerms(query.and_terms));
  }

  // Add NOT terms
  if (!query.not_terms.empty()) {
    result += ' ';
    result.append(NormalizeNotTerms(query.not_terms));
  }

  // Add filters (sorted for consistency)
  if (!query.filters.empty()) {
    result += ' ';
    result.append(NormalizeFilters(query.filters));
  }

  // Add SORT clause (with default if not specified)
  result += ' ';
  result.append(NormalizeSortClause(query.order_by, query.table, primary_key_column));

  // Note: LIMIT and OFFSET are intentionally excluded from cache key.
  // The cache stores full results (before pagination), and LIMIT/OFFSET
  // are applied when retrieving from cache. This allows a single cache
  // entry to serve all pagination requests for the same query.

  return result;
}

std::string QueryNormalizer::NormalizeSearchText(const std::string& text) {
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

  return normalized;
}

std::string QueryNormalizer::NormalizeAndTerms(const std::vector<std::string>& and_terms) {
  // Sort AND terms for consistent cache key
  std::vector<std::string> sorted_terms = and_terms;
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

std::string QueryNormalizer::NormalizeNotTerms(const std::vector<std::string>& not_terms) {
  // Sort NOT terms for consistent cache key
  std::vector<std::string> sorted_terms = not_terms;
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
    result.append(FilterOpToString(sorted_filters[i].op));
    result += ' ';
    result += sorted_filters[i].value;
  }
  return result;
}

std::string QueryNormalizer::NormalizeSortClause(const std::optional<query::OrderByClause>& sort,
                                                 const std::string& /* table */,
                                                 const std::string& primary_key_column) {
  std::string result("SORT ");

  if (sort.has_value()) {
    // Normalize PK column name to canonical placeholder (case-insensitive)
    std::string sort_col_lower = sort->column;
    std::transform(sort_col_lower.begin(), sort_col_lower.end(), sort_col_lower.begin(),
                   [](unsigned char chr) { return std::tolower(chr); });
    std::string pk_col_lower = primary_key_column;
    std::transform(pk_col_lower.begin(), pk_col_lower.end(), pk_col_lower.begin(),
                   [](unsigned char chr) { return std::tolower(chr); });
    if (sort->column.empty() || sort_col_lower == pk_col_lower) {
      result.append("__pk__");
    } else {
      result += sort->column;
    }
    result += ' ';
    result.append(sort->order == query::SortOrder::ASC ? "ASC" : "DESC");
  } else {
    // Default: primary key DESC
    result.append("__pk__ DESC");
  }

  return result;
}

// NOTE: A separate FilterOpToString exists in search_pipeline.cpp for runtime
// filter comparison, returning string_view with "" as default. This version
// returns "=" as default for cache-key normalization. Intentionally separate.
// [AUDIT:FilterOpToString-separation]
std::string QueryNormalizer::FilterOpToString(query::FilterOp filter_op) {
  switch (filter_op) {
    case query::FilterOp::EQ:
      return "=";
    case query::FilterOp::NE:
      return "!=";
    case query::FilterOp::GT:
      return ">";
    case query::FilterOp::GTE:
      return ">=";
    case query::FilterOp::LT:
      return "<";
    case query::FilterOp::LTE:
      return "<=";
    default:
      return "=";
  }
}

}  // namespace mygramdb::cache
