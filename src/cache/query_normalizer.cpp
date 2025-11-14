/**
 * @file query_normalizer.cpp
 * @brief Query normalization implementation
 */

#include "cache/query_normalizer.h"

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

  // Add table name
  oss << " " << query.table;

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

  // Add LIMIT - normalize default limits to a standard value
  if (query.limit_explicit) {
    // Explicit limit: use as-is
    oss << " LIMIT " << query.limit;
  } else {
    // Default limit: normalize to 100 (standard default)
    oss << " LIMIT 100";
  }

  // Add OFFSET (always include for consistency)
  oss << " OFFSET " << query.offset;

  return oss.str();
}

std::string QueryNormalizer::NormalizeSearchText(const std::string& text) {
  // Normalize whitespace: collapse multiple spaces to single space
  std::string normalized;
  normalized.reserve(text.size());

  bool prev_was_space = false;
  for (char current_char : text) {
    if (std::isspace(static_cast<unsigned char>(current_char)) != 0) {
      if (!prev_was_space && !normalized.empty()) {
        normalized += ' ';
        prev_was_space = true;
      }
    } else {
      normalized += current_char;
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
  std::ostringstream oss;
  for (size_t i = 0; i < and_terms.size(); ++i) {
    if (i > 0) {
      oss << " ";
    }
    oss << "AND " << and_terms[i];
  }
  return oss.str();
}

std::string QueryNormalizer::NormalizeNotTerms(const std::vector<std::string>& not_terms) {
  std::ostringstream oss;
  for (size_t i = 0; i < not_terms.size(); ++i) {
    if (i > 0) {
      oss << " ";
    }
    oss << "NOT " << not_terms[i];
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
