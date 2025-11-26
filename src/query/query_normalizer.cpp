/**
 * @file query_normalizer.cpp
 * @brief Query normalization implementation
 */

#include "query/query_normalizer.h"

#include <algorithm>
#include <cctype>
#include <sstream>

namespace mygramdb::cache {

std::string QueryNormalizer::Normalize(const query::Query& query) {
  std::ostringstream oss;

  // Start with command type
  switch (query.type) {
    case query::QueryType::SEARCH:
      oss << "SEARCH";
      break;
    case query::QueryType::COUNT:
      oss << "COUNT";
      break;
    default:
      // Only SEARCH and COUNT queries are cacheable
      return "";
  }

  // Add table name (lowercase for case-insensitive consistency)
  std::string lowercase_table = query.table;
  std::transform(lowercase_table.begin(), lowercase_table.end(), lowercase_table.begin(),
                 [](unsigned char chr) { return std::tolower(chr); });
  oss << " " << lowercase_table;

  // Add main search text
  if (!query.search_text.empty()) {
    oss << " " << NormalizeSearchText(query.search_text);
  }

  // Add AND terms
  if (!query.and_terms.empty()) {
    oss << " " << NormalizeAndTerms(query.and_terms);
  }

  // Add NOT terms
  if (!query.not_terms.empty()) {
    oss << " " << NormalizeNotTerms(query.not_terms);
  }

  // Add filters (sorted for consistency)
  if (!query.filters.empty()) {
    oss << " " << NormalizeFilters(query.filters);
  }

  // Add SORT clause (with default if not specified)
  oss << " " << NormalizeSortClause(query.order_by, query.table);

  // Note: LIMIT and OFFSET are intentionally excluded from cache key.
  // The cache stores full results (before pagination), and LIMIT/OFFSET
  // are applied when retrieving from cache. This allows a single cache
  // entry to serve all pagination requests for the same query.

  return oss.str();
}

std::string QueryNormalizer::NormalizeSearchText(const std::string& text) {
  // Normalize whitespace: collapse multiple spaces (including full-width) to single space
  std::string normalized;
  normalized.reserve(text.size());

  // UTF-8 full-width space (U+3000) byte sequence constants
  constexpr unsigned char kFullWidthSpaceByte1 = 0xE3;
  constexpr unsigned char kFullWidthSpaceByte2 = 0x80;
  constexpr unsigned char kFullWidthSpaceByte3 = 0x80;

  bool prev_was_space = false;
  for (size_t i = 0; i < text.size(); ++i) {
    bool is_space = false;

    // Check for ASCII whitespace (space, tab, newline, etc.)
    if (std::isspace(static_cast<unsigned char>(text[i])) != 0) {
      is_space = true;
    }
    // Check for UTF-8 full-width space (U+3000 = 0xE3 0x80 0x80)
    else if (i + 2 < text.size() && static_cast<unsigned char>(text[i]) == kFullWidthSpaceByte1 &&
             static_cast<unsigned char>(text[i + 1]) == kFullWidthSpaceByte2 &&
             static_cast<unsigned char>(text[i + 2]) == kFullWidthSpaceByte3) {
      is_space = true;
      i += 2;  // Skip the next 2 bytes of the 3-byte UTF-8 sequence
    }

    if (is_space) {
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

  std::ostringstream oss;
  for (size_t i = 0; i < sorted_terms.size(); ++i) {
    if (i > 0) {
      oss << " ";
    }
    oss << "AND " << sorted_terms[i];
  }
  return oss.str();
}

std::string QueryNormalizer::NormalizeNotTerms(const std::vector<std::string>& not_terms) {
  // Sort NOT terms for consistent cache key
  std::vector<std::string> sorted_terms = not_terms;
  std::sort(sorted_terms.begin(), sorted_terms.end());

  std::ostringstream oss;
  for (size_t i = 0; i < sorted_terms.size(); ++i) {
    if (i > 0) {
      oss << " ";
    }
    oss << "NOT " << sorted_terms[i];
  }
  return oss.str();
}

std::string QueryNormalizer::NormalizeFilters(const std::vector<query::FilterCondition>& filters) {
  // Sort filters by column name for consistent cache key
  std::vector<query::FilterCondition> sorted_filters = filters;
  std::sort(
      sorted_filters.begin(), sorted_filters.end(),
      [](const query::FilterCondition& lhs, const query::FilterCondition& rhs) { return lhs.column < rhs.column; });

  std::ostringstream oss;
  for (size_t i = 0; i < sorted_filters.size(); ++i) {
    if (i > 0) {
      oss << " ";
    }
    oss << "FILTER " << sorted_filters[i].column << " " << FilterOpToString(sorted_filters[i].op) << " "
        << sorted_filters[i].value;
  }
  return oss.str();
}

std::string QueryNormalizer::NormalizeSortClause(const std::optional<query::OrderByClause>& sort,
                                                 const std::string& /* table */) {
  std::ostringstream oss;
  oss << "SORT ";

  if (sort.has_value()) {
    // Use specified sort column
    if (sort->column.empty()) {
      oss << "id";  // Primary key default
    } else {
      oss << sort->column;
    }
    oss << " " << (sort->order == query::SortOrder::ASC ? "ASC" : "DESC");
  } else {
    // Default: primary key DESC
    oss << "id DESC";
  }

  return oss.str();
}

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
