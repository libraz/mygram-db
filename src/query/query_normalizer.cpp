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
  // Normalize whitespace: collapse multiple spaces (including Unicode spaces) to single space
  std::string normalized;
  normalized.reserve(text.size());

  // UTF-8 byte sequence constants for Unicode spaces
  // NOLINTBEGIN(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
  constexpr unsigned char kNoBreakSpaceByte1 = 0xC2;  // U+00A0
  constexpr unsigned char kNoBreakSpaceByte2 = 0xA0;
  constexpr unsigned char kUnicodeSpace3Byte1 = 0xE2;  // U+2000-U+200B, U+202F, U+205F
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
    else if (byte == 0xE1 && i + 2 < text.size() &&
             static_cast<unsigned char>(text[i + 1]) == 0x9A &&
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
