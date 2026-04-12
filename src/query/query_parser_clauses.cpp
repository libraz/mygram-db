/**
 * @file query_parser_clauses.cpp
 * @brief Query parser clause implementations (AND, NOT, FILTER, LIMIT, OFFSET, SORT)
 */

#include <array>
#include <optional>
#include <string>
#include <vector>

#include "query/query_parser.h"
#include "query/query_parser_internal.h"
#include "utils/string_utils.h"

namespace mygramdb::query {

using internal::ToLower;
using mygram::utils::ToUpper;

bool QueryParser::ParseAnd(const std::vector<std::string>& tokens, size_t& pos, Query& query) {
  // AND <term>
  pos++;  // Skip "AND"

  if (pos >= tokens.size()) {
    SetError("AND requires a term");
    return false;
  }

  query.and_terms.push_back(tokens[pos++]);
  return true;
}

bool QueryParser::ParseNot(const std::vector<std::string>& tokens, size_t& pos, Query& query) {
  // NOT <term>
  pos++;  // Skip "NOT"

  if (pos >= tokens.size()) {
    SetError("NOT requires a term");
    return false;
  }

  query.not_terms.push_back(tokens[pos++]);
  return true;
}

bool QueryParser::ParseFilters(const std::vector<std::string>& tokens, size_t& pos, Query& query) {
  // FILTER <col> <op> <value>
  pos++;  // Skip "FILTER"

  FilterCondition filter;
  if (!ParseFilterArguments(tokens, pos, filter)) {
    return false;
  }

  if (filter.column.size() > kMaxFilterColumnNameLength) {
    SetError("FILTER column name exceeds maximum length (" + std::to_string(kMaxFilterColumnNameLength) + ")");
    return false;
  }
  if (filter.value.size() > kMaxFilterValueLength) {
    SetError("FILTER value exceeds maximum length (" + std::to_string(kMaxFilterValueLength) + ")");
    return false;
  }

  query.filters.push_back(filter);
  return true;
}

bool QueryParser::ParseFilterArguments(const std::vector<std::string>& tokens, size_t& pos, FilterCondition& filter) {
  if (pos >= tokens.size()) {
    SetError("FILTER requires column, operator, and value");
    return false;
  }

  const auto parse_compound_token = [&](const std::string& token) -> bool {
    std::string column_part;
    std::string op_part;
    std::string value_part;

    static const std::array<std::string, 6> kOperators = {">=", "<=", "!=", "=", ">", "<"};

    for (const auto& op_symbol : kOperators) {
      auto operator_pos = token.find(op_symbol);
      if (operator_pos != std::string::npos) {
        column_part = token.substr(0, operator_pos);
        value_part = token.substr(operator_pos + op_symbol.size());
        op_part = op_symbol;
        break;
      }
    }

    if (op_part.empty() || column_part.empty()) {
      return false;
    }

    auto filter_op = ParseFilterOp(op_part);
    if (!filter_op.has_value()) {
      return false;
    }

    filter.column = ToLower(column_part);
    filter.op = filter_op.value();

    if (!value_part.empty()) {
      filter.value = value_part;
      ++pos;
      return true;
    }

    if (pos + 1 >= tokens.size()) {
      SetError("FILTER requires column, operator, and value");
      return false;
    }

    filter.value = tokens[pos + 1];
    pos += 2;
    return true;
  };

  if (parse_compound_token(tokens[pos])) {
    return true;
  }

  // Fallback to standard "col op value" tokens
  // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
  if (pos + 2 >= tokens.size()) {
    SetError("FILTER requires column, operator, and value");
    return false;
  }

  filter.column = ToLower(tokens[pos++]);

  auto filter_op = ParseFilterOp(tokens[pos++]);
  if (!filter_op.has_value()) {
    SetError("Invalid filter operator: " + tokens[pos - 1]);
    return false;
  }
  filter.op = filter_op.value();

  filter.value = tokens[pos++];

  return true;
}

bool QueryParser::ParseLimit(const std::vector<std::string>& tokens, size_t& pos, Query& query) {
  // LIMIT <n> or LIMIT <offset>,<count>
  pos++;  // Skip "LIMIT"

  if (pos >= tokens.size()) {
    SetError("LIMIT requires a number or offset,count");
    return false;
  }

  const std::string& limit_str = tokens[pos++];

  // Check for comma-separated format: LIMIT offset,count
  size_t comma_pos = limit_str.find(',');
  if (comma_pos != std::string::npos) {
    // Parse LIMIT offset,count
    std::string offset_str = limit_str.substr(0, comma_pos);
    std::string count_str = limit_str.substr(comma_pos + 1);

    try {
      int offset = std::stoi(offset_str);
      int count = std::stoi(count_str);

      if (offset < 0) {
        SetError("LIMIT offset must be non-negative");
        return false;
      }
      if (count <= 0) {
        SetError("LIMIT count must be positive");
        return false;
      }

      query.offset = static_cast<uint32_t>(offset);
      query.limit = static_cast<uint32_t>(count);
      query.offset_explicit = true;
      query.limit_explicit = true;
    } catch (const std::exception& e) {
      SetError("Invalid LIMIT offset,count format: " + limit_str);
      return false;
    }
  } else {
    // Parse LIMIT <n>
    try {
      int limit = std::stoi(limit_str);
      if (limit <= 0) {
        SetError("LIMIT must be positive");
        return false;
      }
      query.limit = static_cast<uint32_t>(limit);
      query.limit_explicit = true;  // Mark as explicitly specified
    } catch (const std::exception& e) {
      SetError("Invalid LIMIT value: " + limit_str);
      return false;
    }
  }

  return true;
}

bool QueryParser::ParseOffset(const std::vector<std::string>& tokens, size_t& pos, Query& query) {
  // OFFSET <n>
  pos++;  // Skip "OFFSET"

  if (pos >= tokens.size()) {
    SetError("OFFSET requires a number");
    return false;
  }

  try {
    int offset = std::stoi(tokens[pos++]);
    if (offset < 0) {
      SetError("OFFSET must be non-negative");
      return false;
    }
    query.offset = static_cast<uint32_t>(offset);
    query.offset_explicit = true;  // Mark as explicitly specified
  } catch (const std::exception& e) {
    SetError("Invalid OFFSET value: " + tokens[pos - 1]);
    return false;
  }

  return true;
}

bool QueryParser::ParseSort(const std::vector<std::string>& tokens, size_t& pos, Query& query) {
  // SORT <column> [ASC|DESC]
  // SORT ASC/DESC (shorthand for primary key)
  pos++;  // Skip "SORT"

  if (pos >= tokens.size()) {
    SetError("SORT requires a column name or ASC/DESC");
    return false;
  }

  OrderByClause order_by;
  std::string next_token = ToUpper(tokens[pos]);

  // Check for shorthand: SORT ASC/DESC (primary key ordering)
  if (next_token == "ASC" || next_token == "DESC") {
    // Shorthand for primary key ordering
    order_by.column = "";  // Empty = primary key
    order_by.order = (next_token == "ASC") ? SortOrder::ASC : SortOrder::DESC;
    pos++;
    query.order_by = order_by;
    return true;
  }

  // Normal case: SORT <column> [ASC|DESC]
  // Lowercase for case-insensitive matching (MySQL column names are case-insensitive)
  order_by.column = ToLower(tokens[pos++]);

  // Check for comma in column name (multi-column sort attempt)
  if (order_by.column.find(',') != std::string::npos) {
    SetError("Multiple column sorting is not supported. Sort by a single column only.");
    return false;
  }

  // Check for ASC/DESC (optional, default is DESC)
  if (pos < tokens.size()) {
    std::string order_str = ToUpper(tokens[pos]);
    if (order_str == "ASC") {
      order_by.order = SortOrder::ASC;
      pos++;
    } else if (order_str == "DESC") {
      order_by.order = SortOrder::DESC;
      pos++;
    }
    // If not ASC or DESC, leave it for next clause to handle
  }

  // Check for multiple columns: SORT col1 ASC col2 DESC
  // After consuming column and optional ASC/DESC, if next token looks like a column name
  // (not a known keyword), it's likely a multi-column sort attempt
  if (pos < tokens.size()) {
    std::string peek_token = ToUpper(tokens[pos]);

    // Lambda to check if token is a known keyword
    auto is_known_keyword = [&peek_token]() -> bool {
      return peek_token == "LIMIT" || peek_token == "OFFSET" || peek_token == "FILTER" || peek_token == "AND" ||
             peek_token == "NOT";
    };

    // If next token is not a known keyword, it might be a second column name
    if (!is_known_keyword()) {
      SetError(
          "Multiple column sorting is not supported. Hint: Sort by a single column only. Use application-level "
          "sorting for complex requirements.");
      return false;
    }
  }

  query.order_by = order_by;
  return true;
}

std::optional<FilterOp> QueryParser::ParseFilterOp(std::string_view op_str) {
  std::string normalized_op = ToUpper(op_str);

  if (normalized_op == "=" || normalized_op == "==" || normalized_op == "EQ") {
    return FilterOp::EQ;
  }
  if (normalized_op == "!=" || normalized_op == "<>" || normalized_op == "NE") {
    return FilterOp::NE;
  }
  if (normalized_op == ">" || normalized_op == "GT") {
    return FilterOp::GT;
  }
  // UTF-8 ≥ (U+2265): \xe2\x89\xa5
  if (normalized_op == ">=" || normalized_op == "\xe2\x89\xa5" || normalized_op == "GTE") {
    return FilterOp::GTE;
  }
  if (normalized_op == "<" || normalized_op == "LT") {
    return FilterOp::LT;
  }
  // UTF-8 ≤ (U+2264): \xe2\x89\xa4
  if (normalized_op == "<=" || normalized_op == "\xe2\x89\xa4" || normalized_op == "LTE") {
    return FilterOp::LTE;
  }

  return std::nullopt;
}

}  // namespace mygramdb::query
