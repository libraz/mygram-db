/**
 * @file query_normalizer.cpp
 * @brief Query normalization implementation
 */

#include "query/query_normalizer.h"

#include <algorithm>
#include <cctype>
#include <string>

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

  // UTF-8 byte sequence constants for Unicode spaces
  // NOLINTBEGIN(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
  constexpr unsigned char kNoBreakSpaceByte1 = 0xC2;  // U+00A0
  constexpr unsigned char kNoBreakSpaceByte2 = 0xA0;
  constexpr unsigned char kUnicodeSpace3Byte1 = 0xE2;   // U+2000-U+200B, U+202F, U+205F
  constexpr unsigned char kFullWidthSpaceByte1 = 0xE3;  // U+3000
  constexpr unsigned char kFullWidthSpaceByte2 = 0x80;
  constexpr unsigned char kFullWidthSpaceByte3 = 0x80;
  // NOLINTEND(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)

  bool prev_was_space = false;
  for (size_t i = 0; i < text.size(); ++i) {
    bool is_space = false;
    auto byte = static_cast<unsigned char>(text[i]);

    // Check for ASCII whitespace (space, tab, newline, etc.)
    if (std::isspace(byte) != 0) {
      is_space = true;
    }
    // Check for UTF-8 No-Break Space U+00A0 (0xC2 0xA0)
    else if (byte == kNoBreakSpaceByte1 && i + 1 < text.size() &&
             static_cast<unsigned char>(text[i + 1]) == kNoBreakSpaceByte2) {
      is_space = true;
      i += 1;  // Skip 1 extra byte (2-byte sequence)
    }
    // Check for 3-byte Unicode spaces starting with 0xE2
    // NOLINTBEGIN(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
    else if (byte == kUnicodeSpace3Byte1 && i + 2 < text.size()) {
      auto byte2 = static_cast<unsigned char>(text[i + 1]);
      auto byte3 = static_cast<unsigned char>(text[i + 2]);
      // U+2000-U+200B: 0xE2 0x80 0x80-0x8B
      if (byte2 == 0x80 && byte3 >= 0x80 && byte3 <= 0x8B) {
        is_space = true;
        i += 2;
      }
      // U+2028 (Line Separator): 0xE2 0x80 0xA8
      // U+2029 (Paragraph Separator): 0xE2 0x80 0xA9
      else if (byte2 == 0x80 && (byte3 == 0xA8 || byte3 == 0xA9)) {
        is_space = true;
        i += 2;
      }
      // U+202F (Narrow No-Break Space): 0xE2 0x80 0xAF
      else if (byte2 == 0x80 && byte3 == 0xAF) {
        is_space = true;
        i += 2;
      }
      // U+205F (Medium Mathematical Space): 0xE2 0x81 0x9F
      else if (byte2 == 0x81 && byte3 == 0x9F) {
        is_space = true;
        i += 2;
      }
    }
    // NOLINTEND(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
    // Check for UTF-8 Ogham Space Mark U+1680 (0xE1 0x9A 0x80)
    // NOLINTBEGIN(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
    else if (byte == 0xE1 && i + 2 < text.size() && static_cast<unsigned char>(text[i + 1]) == 0x9A &&
             static_cast<unsigned char>(text[i + 2]) == 0x80) {
      is_space = true;
      i += 2;
    }
    // NOLINTEND(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
    // Check for UTF-8 full-width space U+3000 (0xE3 0x80 0x80)
    else if (byte == kFullWidthSpaceByte1 && i + 2 < text.size() &&
             static_cast<unsigned char>(text[i + 1]) == kFullWidthSpaceByte2 &&
             static_cast<unsigned char>(text[i + 2]) == kFullWidthSpaceByte3) {
      is_space = true;
      i += 2;  // Skip 2 extra bytes (3-byte sequence)
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
