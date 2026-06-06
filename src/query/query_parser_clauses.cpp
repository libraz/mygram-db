/**
 * @file query_parser_clauses.cpp
 * @brief Query parser clause implementations (AND, NOT, FILTER, LIMIT, OFFSET, SORT)
 */

#include <array>
#include <charconv>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "query/query_parser.h"
#include "query/query_parser_internal.h"
#include "utils/error.h"
#include "utils/expected.h"

namespace mygramdb::query {

using internal::EqualsIgnoreCase;
using internal::ToLower;
using mygram::utils::ErrorCode;
using mygram::utils::MakeError;
using mygram::utils::MakeUnexpected;

namespace {

bool StartsWithFilterOperatorChar(const std::string& value) {
  if (value.empty()) {
    return false;
  }
  const char first = value.front();
  return first == '=' || first == '<' || first == '>' || first == '!';
}

mygram::utils::Expected<void, mygram::utils::Error> ValidateFilterValue(const std::string& value) {
  if (StartsWithFilterOperatorChar(value)) {
    return MakeUnexpected(
        MakeError(ErrorCode::kQueryInvalidFilter, "FILTER value must not start with an operator character"));
  }
  return {};
}

}  // namespace

mygram::utils::Expected<void, mygram::utils::Error> QueryParser::ParseAnd(const std::vector<std::string>& tokens,
                                                                          size_t& pos, Query& query) {
  // AND <term>
  pos++;  // Skip "AND"

  if (pos >= tokens.size()) {
    return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, "AND requires a term"));
  }

  query.and_terms.push_back(tokens[pos++]);
  return {};
}

mygram::utils::Expected<void, mygram::utils::Error> QueryParser::ParseNot(const std::vector<std::string>& tokens,
                                                                          size_t& pos, Query& query) {
  // NOT <term>
  pos++;  // Skip "NOT"

  if (pos >= tokens.size()) {
    return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, "NOT requires a term"));
  }

  query.not_terms.push_back(tokens[pos++]);
  return {};
}

mygram::utils::Expected<void, mygram::utils::Error> QueryParser::ParseFilters(const std::vector<std::string>& tokens,
                                                                              size_t& pos, Query& query) {
  // FILTER <col> <op> <value>
  pos++;  // Skip "FILTER"

  FilterCondition filter;
  auto result = ParseFilterArguments(tokens, pos, filter);
  if (!result) {
    return result;
  }

  if (filter.column.size() > kMaxFilterColumnNameLength) {
    return MakeUnexpected(
        MakeError(ErrorCode::kQueryInvalidFilter,
                  "FILTER column name exceeds maximum length (" + std::to_string(kMaxFilterColumnNameLength) + ")"));
  }
  if (filter.value.size() > kMaxFilterValueLength) {
    return MakeUnexpected(MakeError(ErrorCode::kQueryInvalidFilter, "FILTER value exceeds maximum length (" +
                                                                        std::to_string(kMaxFilterValueLength) + ")"));
  }

  query.filters.push_back(filter);
  return {};
}

mygram::utils::Expected<void, mygram::utils::Error> QueryParser::ParseFilterArguments(
    const std::vector<std::string>& tokens, size_t& pos, FilterCondition& filter) {
  if (pos >= tokens.size()) {
    return MakeUnexpected(MakeError(ErrorCode::kQueryInvalidFilter, "FILTER requires column, operator, and value"));
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
      if (auto result = ValidateFilterValue(filter.value); !result) {
        return false;
      }
      ++pos;
      return true;
    }

    if (pos + 1 >= tokens.size()) {
      // Signal that we need value but it's missing — handled by caller
      return false;
    }

    filter.value = tokens[pos + 1];
    if (StartsWithFilterOperatorChar(filter.value)) {
      return false;
    }
    pos += 2;
    return true;
  };

  if (parse_compound_token(tokens[pos])) {
    return {};
  }

  // Fallback to standard "col op value" tokens
  // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
  if (pos + 2 >= tokens.size()) {
    return MakeUnexpected(MakeError(ErrorCode::kQueryInvalidFilter, "FILTER requires column, operator, and value"));
  }

  filter.column = ToLower(tokens[pos++]);

  auto filter_op = ParseFilterOp(tokens[pos++]);
  if (!filter_op.has_value()) {
    return MakeUnexpected(MakeError(ErrorCode::kQueryInvalidFilter, "Invalid filter operator: " + tokens[pos - 1]));
  }
  filter.op = filter_op.value();

  filter.value = tokens[pos++];
  if (auto result = ValidateFilterValue(filter.value); !result) {
    return result;
  }

  return {};
}

mygram::utils::Expected<void, mygram::utils::Error> QueryParser::ParseLimit(const std::vector<std::string>& tokens,
                                                                            size_t& pos, Query& query) {
  // LIMIT <n> or LIMIT <offset>,<count>
  pos++;  // Skip "LIMIT"

  if (pos >= tokens.size()) {
    return MakeUnexpected(MakeError(ErrorCode::kQueryInvalidLimit, "LIMIT requires a number or offset,count"));
  }

  const std::string& limit_str = tokens[pos++];

  // Check for comma-separated format: LIMIT offset,count
  size_t comma_pos = limit_str.find(',');
  if (comma_pos != std::string::npos) {
    if (query.offset_explicit) {
      return MakeUnexpected(
          MakeError(ErrorCode::kQueryInvalidOffset, "OFFSET specified more than once (LIMIT offset,count + OFFSET)"));
    }

    // Parse LIMIT offset,count
    std::string offset_str = limit_str.substr(0, comma_pos);
    std::string count_str = limit_str.substr(comma_pos + 1);

    // Reject negative values
    if (!offset_str.empty() && offset_str[0] == '-') {
      return MakeUnexpected(MakeError(ErrorCode::kQueryInvalidLimit, "LIMIT offset must be non-negative"));
    }
    if (!count_str.empty() && count_str[0] == '-') {
      return MakeUnexpected(MakeError(ErrorCode::kQueryInvalidLimit, "LIMIT count must be positive"));
    }

    uint32_t offset_val = 0;
    auto [ptr1, ec1] = std::from_chars(offset_str.data(), offset_str.data() + offset_str.size(), offset_val);
    if (ec1 != std::errc() || ptr1 != offset_str.data() + offset_str.size()) {
      if (ec1 == std::errc::result_out_of_range) {
        return MakeUnexpected(MakeError(ErrorCode::kQueryInvalidLimit, "LIMIT offset value too large"));
      }
      return MakeUnexpected(
          MakeError(ErrorCode::kQueryInvalidLimit, "Invalid LIMIT offset,count format: " + limit_str));
    }

    uint32_t count_val = 0;
    auto [ptr2, ec2] = std::from_chars(count_str.data(), count_str.data() + count_str.size(), count_val);
    if (ec2 != std::errc() || ptr2 != count_str.data() + count_str.size()) {
      if (ec2 == std::errc::result_out_of_range) {
        return MakeUnexpected(MakeError(ErrorCode::kQueryInvalidLimit, "LIMIT count value too large"));
      }
      return MakeUnexpected(
          MakeError(ErrorCode::kQueryInvalidLimit, "Invalid LIMIT offset,count format: " + limit_str));
    }

    if (count_val == 0) {
      return MakeUnexpected(MakeError(ErrorCode::kQueryInvalidLimit, "LIMIT count must be positive"));
    }

    query.offset = offset_val;
    query.limit = count_val;
    query.offset_explicit = true;
    query.limit_explicit = true;
  } else {
    // Parse LIMIT <n>
    // Reject negative values
    if (!limit_str.empty() && limit_str[0] == '-') {
      return MakeUnexpected(MakeError(ErrorCode::kQueryInvalidLimit, "LIMIT must be positive"));
    }

    uint32_t limit_val = 0;
    auto [ptr, ec] = std::from_chars(limit_str.data(), limit_str.data() + limit_str.size(), limit_val);
    if (ec != std::errc() || ptr != limit_str.data() + limit_str.size()) {
      if (ec == std::errc::result_out_of_range) {
        return MakeUnexpected(MakeError(ErrorCode::kQueryInvalidLimit, "LIMIT value out of range: " + limit_str));
      }
      return MakeUnexpected(MakeError(ErrorCode::kQueryInvalidLimit, "Invalid LIMIT value: " + limit_str));
    }

    if (limit_val == 0) {
      return MakeUnexpected(MakeError(ErrorCode::kQueryInvalidLimit, "LIMIT must be positive"));
    }

    query.limit = limit_val;
    query.limit_explicit = true;  // Mark as explicitly specified
  }

  return {};
}

mygram::utils::Expected<void, mygram::utils::Error> QueryParser::ParseOffset(const std::vector<std::string>& tokens,
                                                                             size_t& pos, Query& query) {
  // OFFSET <n>
  pos++;  // Skip "OFFSET"

  if (pos >= tokens.size()) {
    return MakeUnexpected(MakeError(ErrorCode::kQueryInvalidOffset, "OFFSET requires a number"));
  }

  const std::string& offset_str = tokens[pos++];

  if (query.offset_explicit) {
    return MakeUnexpected(MakeError(ErrorCode::kQueryInvalidOffset, "OFFSET specified more than once"));
  }

  // Reject negative values
  if (!offset_str.empty() && offset_str[0] == '-') {
    return MakeUnexpected(MakeError(ErrorCode::kQueryInvalidOffset, "OFFSET must be non-negative"));
  }

  uint32_t offset_val = 0;
  auto [ptr, ec] = std::from_chars(offset_str.data(), offset_str.data() + offset_str.size(), offset_val);
  if (ec != std::errc() || ptr != offset_str.data() + offset_str.size()) {
    if (ec == std::errc::result_out_of_range) {
      return MakeUnexpected(MakeError(ErrorCode::kQueryInvalidOffset, "OFFSET value out of range: " + offset_str));
    }
    return MakeUnexpected(MakeError(ErrorCode::kQueryInvalidOffset, "Invalid OFFSET value: " + offset_str));
  }

  query.offset = offset_val;
  query.offset_explicit = true;  // Mark as explicitly specified

  return {};
}

mygram::utils::Expected<void, mygram::utils::Error> QueryParser::ParseSort(const std::vector<std::string>& tokens,
                                                                           size_t& pos, Query& query) {
  // SORT <column> [ASC|DESC]
  // SORT ASC/DESC (shorthand for primary key)
  pos++;  // Skip "SORT"

  if (pos >= tokens.size()) {
    return MakeUnexpected(MakeError(ErrorCode::kQueryInvalidSort, "SORT requires a column name or ASC/DESC"));
  }

  OrderByClause order_by;
  const auto& next_token = tokens[pos];

  // Check for shorthand: SORT ASC/DESC (primary key ordering)
  if (EqualsIgnoreCase(next_token, "ASC") || EqualsIgnoreCase(next_token, "DESC")) {
    // Shorthand for primary key ordering
    order_by.column = "";  // Empty = primary key
    order_by.order = EqualsIgnoreCase(next_token, "ASC") ? SortOrder::ASC : SortOrder::DESC;
    pos++;
    query.order_by = order_by;
    return {};
  }

  // Normal case: SORT <column> [ASC|DESC]
  // Lowercase for case-insensitive matching (MySQL column names are case-insensitive)
  order_by.column = ToLower(tokens[pos++]);

  // Check for comma in column name (multi-column sort attempt)
  if (order_by.column.find(',') != std::string::npos) {
    return MakeUnexpected(MakeError(ErrorCode::kQueryInvalidSort,
                                    "Multiple column sorting is not supported. Sort by a single column only."));
  }

  // Check for ASC/DESC (optional, default is DESC)
  if (pos < tokens.size()) {
    if (EqualsIgnoreCase(tokens[pos], "ASC")) {
      order_by.order = SortOrder::ASC;
      pos++;
    } else if (EqualsIgnoreCase(tokens[pos], "DESC")) {
      order_by.order = SortOrder::DESC;
      pos++;
    }
    // If not ASC or DESC, leave it for next clause to handle
  }

  // Check for multiple columns: SORT col1 ASC col2 DESC
  // After consuming column and optional ASC/DESC, if next token looks like a column name
  // (not a known keyword), it's likely a multi-column sort attempt
  if (pos < tokens.size()) {
    const auto& peek_token = tokens[pos];
    std::string peek_upper = peek_token;
    std::transform(peek_upper.begin(), peek_upper.end(), peek_upper.begin(),
                   [](unsigned char chr) { return std::toupper(chr); });

    // If next token is not a known keyword, it might be a second column name
    if (!internal::IsClauseKeyword(peek_upper)) {
      return MakeUnexpected(MakeError(
          ErrorCode::kQueryInvalidSort,
          "Multiple column sorting is not supported. Hint: Sort by a single column only. Use application-level "
          "sorting for complex requirements."));
    }
  }

  query.order_by = order_by;
  return {};
}

mygram::utils::Expected<void, mygram::utils::Error> QueryParser::ParseHighlight(const std::vector<std::string>& tokens,
                                                                                size_t& pos, Query& query) {
  // HIGHLIGHT [TAG <open> <close>] [SNIPPET_LEN <n>] [MAX_FRAGMENTS <n>]
  pos++;  // Skip "HIGHLIGHT"

  query::HighlightOptions opts;

  while (pos < tokens.size()) {
    const auto& keyword = tokens[pos];

    if (EqualsIgnoreCase(keyword, "TAG")) {
      // TAG <open> <close>
      // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
      if (pos + 2 >= tokens.size()) {
        return MakeUnexpected(
            MakeError(ErrorCode::kQuerySyntaxError, "HIGHLIGHT TAG requires open and close tag arguments"));
      }
      pos++;
      opts.open_tag = tokens[pos++];
      opts.close_tag = tokens[pos++];
    } else if (EqualsIgnoreCase(keyword, "SNIPPET_LEN")) {
      // SNIPPET_LEN <n>
      if (pos + 1 >= tokens.size()) {
        return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, "HIGHLIGHT SNIPPET_LEN requires a number"));
      }
      pos++;
      const std::string& val_str = tokens[pos++];
      uint32_t val = 0;
      auto [ptr, ec] = std::from_chars(val_str.data(), val_str.data() + val_str.size(), val);
      if (ec != std::errc() || ptr != val_str.data() + val_str.size()) {
        if (ec == std::errc::result_out_of_range) {
          return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, "HIGHLIGHT SNIPPET_LEN value out of range"));
        }
        return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, "Invalid HIGHLIGHT SNIPPET_LEN value"));
      }
      if (val == 0 || val > 10000) {
        return MakeUnexpected(
            MakeError(ErrorCode::kQuerySyntaxError, "HIGHLIGHT SNIPPET_LEN must be between 1 and 10000"));
      }
      opts.snippet_length = val;
    } else if (EqualsIgnoreCase(keyword, "MAX_FRAGMENTS")) {
      // MAX_FRAGMENTS <n>
      if (pos + 1 >= tokens.size()) {
        return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, "HIGHLIGHT MAX_FRAGMENTS requires a number"));
      }
      pos++;
      const std::string& val_str = tokens[pos++];
      uint32_t val = 0;
      auto [ptr, ec] = std::from_chars(val_str.data(), val_str.data() + val_str.size(), val);
      if (ec != std::errc() || ptr != val_str.data() + val_str.size()) {
        if (ec == std::errc::result_out_of_range) {
          return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, "HIGHLIGHT MAX_FRAGMENTS value out of range"));
        }
        return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, "Invalid HIGHLIGHT MAX_FRAGMENTS value"));
      }
      if (val == 0 || val > 100) {
        return MakeUnexpected(
            MakeError(ErrorCode::kQuerySyntaxError, "HIGHLIGHT MAX_FRAGMENTS must be between 1 and 100"));
      }
      opts.max_fragments = val;
    } else {
      // Not a HIGHLIGHT sub-option, stop consuming
      break;
    }
  }

  query.highlight = opts;
  return {};
}

mygram::utils::Expected<void, mygram::utils::Error> QueryParser::ParseFuzzy(const std::vector<std::string>& tokens,
                                                                            size_t& pos, Query& query) {
  // FUZZY [distance]
  pos++;                      // Skip "FUZZY" (consistent with other ParseX methods)
  uint32_t max_distance = 1;  // Default

  // Check if next token is a number (optional distance parameter)
  if (pos < tokens.size() && !internal::IsClauseKeyword(tokens[pos])) {
    uint32_t val = 0;
    const auto& token = tokens[pos];
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
    auto [ptr, ec] = std::from_chars(token.data(), token.data() + token.size(), val);
    if (ec == std::errc() && ptr == token.data() + token.size()) {
      // Successfully parsed as number — validate range
      if (val < 1 || val > 2) {
        return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, "FUZZY distance must be 1 or 2, got: " + token));
      }
      max_distance = val;
      ++pos;
    }
    // If not a valid number, don't consume — leave for next clause to handle
  }

  query.fuzzy_max_distance = max_distance;
  return {};
}

std::optional<FilterOp> QueryParser::ParseFilterOp(std::string_view op_str) {
  if (op_str == "=" || op_str == "==" || EqualsIgnoreCase(op_str, "EQ")) {
    return FilterOp::EQ;
  }
  if (op_str == "!=" || op_str == "<>" || EqualsIgnoreCase(op_str, "NE")) {
    return FilterOp::NE;
  }
  if (op_str == ">" || EqualsIgnoreCase(op_str, "GT")) {
    return FilterOp::GT;
  }
  // UTF-8 ≥ (U+2265): \xe2\x89\xa5
  if (op_str == ">=" || op_str == "\xe2\x89\xa5" || EqualsIgnoreCase(op_str, "GTE")) {
    return FilterOp::GTE;
  }
  if (op_str == "<" || EqualsIgnoreCase(op_str, "LT")) {
    return FilterOp::LT;
  }
  // UTF-8 ≤ (U+2264): \xe2\x89\xa4
  if (op_str == "<=" || op_str == "\xe2\x89\xa4" || EqualsIgnoreCase(op_str, "LTE")) {
    return FilterOp::LTE;
  }

  return std::nullopt;
}

}  // namespace mygramdb::query
