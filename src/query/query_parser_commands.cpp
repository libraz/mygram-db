/**
 * @file query_parser_commands.cpp
 * @brief Query parser command implementations (SEARCH, COUNT, GET)
 */

#include <spdlog/spdlog.h>

#include <string>
#include <vector>

#include "query/query_parser.h"
#include "query/query_parser_internal.h"
#include "utils/string_utils.h"

namespace mygramdb::query {

using mygram::utils::ErrorCode;
using mygram::utils::MakeError;
using mygram::utils::MakeUnexpected;

using internal::EqualsIgnoreCase;
using internal::IsClauseKeyword;
using internal::kMaxLimit;

constexpr size_t kMaxTermCount = 64;
using mygram::utils::ToUpper;

static bool IsNonExpressionClauseKeyword(const std::string& token) {
  return token == "FILTER" || token == "SORT" || token == "LIMIT" || token == "OFFSET" || token == "HIGHLIGHT" ||
         token == "FUZZY" || token == "FACET";
}

/**
 * @brief Extract search text tokens from a command's token list
 *
 * Shared logic for ParseSearch, ParseCount, and ParseFacet:
 * 1. Multi-table comma detection (error if found)
 * 2. Parentheses balance check
 * 3. Search token accumulation with IsClauseKeyword stop, ORDER deprecation detection
 * 4. Space-aware token joining (handling parentheses)
 * 5. Empty search text check
 *
 * @param tokens Full token list
 * @param start_pos Position to start consuming search text (after command + table)
 * @param[out] query Query object to populate search_text
 * @param command_name Name of the command for error messages (e.g., "SEARCH", "COUNT")
 * @param[out] error_msg Output error message on failure
 * @return Position after search text extraction on success, or empty on error
 */
static mygram::utils::Expected<size_t, mygram::utils::Error> ParseSearchTextTokens(
    const std::vector<std::string>& tokens, size_t start_pos, Query& query, const std::string& command_name,
    std::string& error_msg) {
  // Check for comma-separated table names (SQL-style multi-table syntax)
  if (query.table.find(',') != std::string::npos || (tokens.size() > start_pos && tokens[start_pos] == ",")) {
    error_msg =
        "Multiple tables not supported. Hint: MygramDB searches a single table at a time. Use separate queries "
        "for multiple tables.";
    query.type = QueryType::UNKNOWN;
    return MakeUnexpected(MakeError(mygram::utils::ErrorCode::kQuerySyntaxError, error_msg));
  }

  // First pass: check parentheses balance across ALL tokens
  int total_paren_depth = 0;
  for (size_t i = start_pos; i < tokens.size(); ++i) {
    auto [open, close] = detail::CountParensInToken(tokens[i]);
    total_paren_depth += open - close;

    // Check for unmatched closing parentheses
    if (total_paren_depth < 0) {
      error_msg = "Unmatched closing parenthesis";
      query.type = QueryType::UNKNOWN;
      return MakeUnexpected(MakeError(mygram::utils::ErrorCode::kQuerySyntaxError, error_msg));
    }
  }

  // Check for unclosed parentheses
  if (total_paren_depth > 0) {
    error_msg = "Unclosed parenthesis";
    query.type = QueryType::UNKNOWN;
    return MakeUnexpected(MakeError(mygram::utils::ErrorCode::kQuerySyntaxError, error_msg));
  }

  // Top-level OR is part of the boolean expression syntax handled by QueryASTParser.
  // Preserve legacy AND/NOT clauses unless the expression contains top-level OR.
  bool has_top_level_or = false;
  {
    int scan_paren_depth = 0;
    for (size_t i = start_pos; i < tokens.size(); ++i) {
      const std::string upper = ToUpper(tokens[i]);
      auto [open, close] = detail::CountParensInToken(tokens[i]);
      scan_paren_depth += open - close;
      if (scan_paren_depth == 0 && IsNonExpressionClauseKeyword(upper)) {
        break;
      }
      if (scan_paren_depth == 0 && EqualsIgnoreCase(tokens[i], "ORDER")) {
        break;
      }
      if (scan_paren_depth == 0 && upper == "OR") {
        has_top_level_or = true;
        break;
      }
    }
  }

  // Extract search text: consume tokens until we hit a command clause keyword.
  // Handle parentheses by tracking nesting level - but respect quoted strings.
  size_t pos = start_pos;
  std::vector<std::string> search_tokens;
  int paren_depth = 0;

  while (pos < tokens.size()) {
    const std::string& token = tokens[pos];

    // Track parentheses depth (respecting quotes)
    auto [open, close] = detail::CountParensInToken(token);
    paren_depth += open - close;

    const std::string upper_token = ToUpper(token);

    // Check if this is a clause keyword (only when not inside parentheses).
    if (paren_depth == 0 &&
        (IsNonExpressionClauseKeyword(upper_token) || (!has_top_level_or && IsClauseKeyword(upper_token)))) {
      break;  // Stop consuming search text
    }

    // Special check for deprecated ORDER keyword - provide helpful error
    if (paren_depth == 0 && EqualsIgnoreCase(token, "ORDER")) {
      error_msg = "ORDER BY is not supported. Use SORT instead.";
      query.type = QueryType::UNKNOWN;
      return MakeUnexpected(MakeError(mygram::utils::ErrorCode::kQuerySyntaxError, error_msg));
    }

    search_tokens.push_back(token);
    pos++;
  }

  if (search_tokens.empty()) {
    error_msg = command_name + " requires search text";
    return MakeUnexpected(MakeError(mygram::utils::ErrorCode::kQuerySyntaxError, error_msg));
  }

  // Join search tokens with spaces to form complete search expression
  query.search_text = search_tokens[0];
  // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
  for (size_t i = 1; i < search_tokens.size(); ++i) {  // 1: Start from second token
    const std::string& token = search_tokens[i];
    // Don't add space before closing parentheses or after opening parentheses
    // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
    bool prev_ends_with_open_paren =
        !search_tokens[i - 1].empty() && search_tokens[i - 1].back() == '(';  // 1: Check previous token
    bool current_starts_with_close_paren = !token.empty() && token[0] == ')';

    if (!prev_ends_with_open_paren && !current_starts_with_close_paren) {
      query.search_text += " ";
    }
    query.search_text += token;
  }

  // Check for empty search text (e.g., empty quoted strings)
  // After tokenization, empty quoted strings become empty string tokens
  bool is_empty = true;
  for (const auto& token : search_tokens) {
    if (!token.empty()) {
      is_empty = false;
      break;
    }
  }

  if (is_empty) {
    error_msg = command_name + " requires non-empty search text";
    query.type = QueryType::UNKNOWN;
    return MakeUnexpected(MakeError(mygram::utils::ErrorCode::kQuerySyntaxError, error_msg));
  }

  return pos;
}

mygram::utils::Expected<Query, mygram::utils::Error> QueryParser::ParseSearch(const std::vector<std::string>& tokens) {
  Query query;
  query.type = QueryType::SEARCH;

  // SEARCH <table> <text> [FILTER ...] [LIMIT n] [OFFSET n]
  // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
  if (tokens.size() < 3) {  // 3: SEARCH + table + text (minimum)
    SetError("SEARCH requires at least table and search text");
    return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, error_));
  }

  query.table = tokens[1];

  // Extract search text using shared helper
  // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
  auto search_result = ParseSearchTextTokens(tokens, 2, query, "SEARCH", error_);  // 2: after SEARCH + table
  if (!search_result.has_value()) {
    SetError(error_);
    return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, error_));
  }
  size_t pos = search_result.value();

  // Parse optional clauses
  while (pos < tokens.size()) {
    if (EqualsIgnoreCase(tokens[pos], "AND")) {
      auto result = ParseAnd(tokens, pos, query);
      if (!result) {
        query.type = QueryType::UNKNOWN;
        return MakeUnexpected(result.error());
      }
    } else if (EqualsIgnoreCase(tokens[pos], "NOT")) {
      auto result = ParseNot(tokens, pos, query);
      if (!result) {
        query.type = QueryType::UNKNOWN;
        return MakeUnexpected(result.error());
      }
    } else if (EqualsIgnoreCase(tokens[pos], "FILTER")) {
      auto result = ParseFilters(tokens, pos, query);
      if (!result) {
        query.type = QueryType::UNKNOWN;
        return MakeUnexpected(result.error());
      }
    } else if (EqualsIgnoreCase(tokens[pos], "ORDER")) {
      // ORDER BY is deprecated, guide users to use SORT
      SetError("ORDER BY is not supported. Use SORT instead. Example: SEARCH table text SORT column DESC");
      query.type = QueryType::UNKNOWN;
      return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, error_));
    } else if (EqualsIgnoreCase(tokens[pos], "SORT")) {
      auto result = ParseSort(tokens, pos, query);
      if (!result) {
        query.type = QueryType::UNKNOWN;
        return MakeUnexpected(result.error());
      }
    } else if (EqualsIgnoreCase(tokens[pos], "LIMIT")) {
      auto result = ParseLimit(tokens, pos, query);
      if (!result) {
        query.type = QueryType::UNKNOWN;
        return MakeUnexpected(result.error());
      }
    } else if (EqualsIgnoreCase(tokens[pos], "OFFSET")) {
      auto result = ParseOffset(tokens, pos, query);
      if (!result) {
        query.type = QueryType::UNKNOWN;
        return MakeUnexpected(result.error());
      }
    } else if (EqualsIgnoreCase(tokens[pos], "HIGHLIGHT")) {
      auto result = ParseHighlight(tokens, pos, query);
      if (!result) {
        query.type = QueryType::UNKNOWN;
        return MakeUnexpected(result.error());
      }
    } else if (EqualsIgnoreCase(tokens[pos], "FUZZY")) {
      auto result = ParseFuzzy(tokens, pos, query);
      if (!result) {
        query.type = QueryType::UNKNOWN;
        return MakeUnexpected(result.error());
      }
    } else {
      SetError("Unknown keyword: " + tokens[pos]);
      query.type = QueryType::UNKNOWN;
      return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, error_));
    }
  }

  // Validate term counts
  if (query.and_terms.size() > kMaxTermCount) {
    SetError("Too many AND terms (max " + std::to_string(kMaxTermCount) + ")");
    return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, error_));
  }
  if (query.not_terms.size() > kMaxTermCount) {
    SetError("Too many NOT terms (max " + std::to_string(kMaxTermCount) + ")");
    return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, error_));
  }
  if (query.filters.size() > kMaxTermCount) {
    SetError("Too many FILTER conditions (max " + std::to_string(kMaxTermCount) + ")");
    return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, error_));
  }

  // Validate limit
  if (query.limit > kMaxLimit) {
    SetError("LIMIT exceeds maximum of " + std::to_string(kMaxLimit));
    query.type = QueryType::UNKNOWN;
    return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, error_));
  }

  {
    auto result = ValidateQueryLength(query);
    if (!result) {
      query.type = QueryType::UNKNOWN;
      return MakeUnexpected(result.error());
    }
  }

  return query;
}

mygram::utils::Expected<Query, mygram::utils::Error> QueryParser::ParseCount(const std::vector<std::string>& tokens) {
  Query query;
  query.type = QueryType::COUNT;

  // COUNT <table> <text> [FILTER ...]
  // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
  if (tokens.size() < 3) {  // 3: COUNT + table + text (minimum)
    SetError("COUNT requires at least table and search text");
    return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, error_));
  }

  query.table = tokens[1];

  // Extract search text using shared helper
  // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
  auto search_result = ParseSearchTextTokens(tokens, 2, query, "COUNT", error_);  // 2: after COUNT + table
  if (!search_result.has_value()) {
    SetError(error_);
    return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, error_));
  }
  size_t pos = search_result.value();

  // Parse optional clauses
  while (pos < tokens.size()) {
    if (EqualsIgnoreCase(tokens[pos], "AND")) {
      auto result = ParseAnd(tokens, pos, query);
      if (!result) {
        query.type = QueryType::UNKNOWN;
        return MakeUnexpected(result.error());
      }
    } else if (EqualsIgnoreCase(tokens[pos], "NOT")) {
      auto result = ParseNot(tokens, pos, query);
      if (!result) {
        query.type = QueryType::UNKNOWN;
        return MakeUnexpected(result.error());
      }
    } else if (EqualsIgnoreCase(tokens[pos], "FILTER")) {
      auto result = ParseFilters(tokens, pos, query);
      if (!result) {
        query.type = QueryType::UNKNOWN;
        return MakeUnexpected(result.error());
      }
    } else if (EqualsIgnoreCase(tokens[pos], "ORDER")) {
      SetError("ORDER BY is not supported. Use SORT instead (note: COUNT does not support sorting).");
      query.type = QueryType::UNKNOWN;
      return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, error_));
    } else if (EqualsIgnoreCase(tokens[pos], "SORT")) {
      SetError("COUNT does not support SORT clause. Use SEARCH if you need sorted results.");
      query.type = QueryType::UNKNOWN;
      return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, error_));
    } else {
      SetError("COUNT only supports AND, NOT and FILTER clauses");
      query.type = QueryType::UNKNOWN;
      return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, error_));
    }
  }

  // Validate term counts
  if (query.and_terms.size() > kMaxTermCount) {
    SetError("Too many AND terms (max " + std::to_string(kMaxTermCount) + ")");
    return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, error_));
  }
  if (query.not_terms.size() > kMaxTermCount) {
    SetError("Too many NOT terms (max " + std::to_string(kMaxTermCount) + ")");
    return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, error_));
  }
  if (query.filters.size() > kMaxTermCount) {
    SetError("Too many FILTER conditions (max " + std::to_string(kMaxTermCount) + ")");
    return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, error_));
  }

  {
    auto result = ValidateQueryLength(query);
    if (!result) {
      query.type = QueryType::UNKNOWN;
      return MakeUnexpected(result.error());
    }
  }

  return query;
}

mygram::utils::Expected<Query, mygram::utils::Error> QueryParser::ParseGet(const std::vector<std::string>& tokens) {
  using mygram::utils::ErrorCode;
  using mygram::utils::MakeError;
  using mygram::utils::MakeUnexpected;

  Query query;
  query.type = QueryType::GET;

  // GET <table> <primary_key>
  // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
  if (tokens.size() != 3) {  // 3: GET + table + primary_key (exact)
    SetError("GET requires table and primary_key");
    return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, error_));
  }

  query.table = tokens[1];
  query.primary_key = tokens[2];

  return query;
}

mygram::utils::Expected<Query, mygram::utils::Error> QueryParser::ParseFacet(const std::vector<std::string>& tokens,
                                                                             size_t& pos) {
  Query query;
  query.type = QueryType::FACET;

  // Require table name
  if (pos >= tokens.size()) {
    SetError("FACET requires table name");
    return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, error_));
  }
  query.table = tokens[pos++];

  // Require column name
  if (pos >= tokens.size()) {
    SetError("FACET requires column name");
    return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, error_));
  }
  query.facet_column = tokens[pos++];

  if (query.facet_column.size() > kMaxFilterColumnNameLength) {
    SetError("FACET column name exceeds maximum length (" + std::to_string(kMaxFilterColumnNameLength) + ")");
    return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, error_));
  }

  // Optional: search text (accumulate multiple tokens until a clause keyword)
  {
    std::vector<std::string> search_tokens;
    while (pos < tokens.size()) {
      std::string upper = ToUpper(tokens[pos]);
      if (IsClauseKeyword(upper)) {
        break;
      }
      if (EqualsIgnoreCase(tokens[pos], "ORDER")) {
        break;
      }
      search_tokens.push_back(tokens[pos]);
      pos++;
    }
    if (!search_tokens.empty()) {
      query.search_text = search_tokens[0];
      for (size_t i = 1; i < search_tokens.size(); ++i) {
        const std::string& token = search_tokens[i];
        bool prev_ends_with_open_paren = !search_tokens[i - 1].empty() && search_tokens[i - 1].back() == '(';
        bool current_starts_with_close_paren = !token.empty() && token[0] == ')';
        if (!prev_ends_with_open_paren && !current_starts_with_close_paren) {
          query.search_text += " ";
        }
        query.search_text += token;
      }
    }
  }

  // Parse optional clauses (AND, NOT, FILTER, LIMIT, OFFSET)
  while (pos < tokens.size()) {
    if (EqualsIgnoreCase(tokens[pos], "AND")) {
      auto result = ParseAnd(tokens, pos, query);
      if (!result) {
        query.type = QueryType::UNKNOWN;
        return MakeUnexpected(result.error());
      }
    } else if (EqualsIgnoreCase(tokens[pos], "NOT")) {
      auto result = ParseNot(tokens, pos, query);
      if (!result) {
        query.type = QueryType::UNKNOWN;
        return MakeUnexpected(result.error());
      }
    } else if (EqualsIgnoreCase(tokens[pos], "FILTER")) {
      auto result = ParseFilters(tokens, pos, query);
      if (!result) {
        query.type = QueryType::UNKNOWN;
        return MakeUnexpected(result.error());
      }
    } else if (EqualsIgnoreCase(tokens[pos], "LIMIT")) {
      auto result = ParseLimit(tokens, pos, query);
      if (!result) {
        query.type = QueryType::UNKNOWN;
        return MakeUnexpected(result.error());
      }
    } else if (EqualsIgnoreCase(tokens[pos], "OFFSET")) {
      auto result = ParseOffset(tokens, pos, query);
      if (!result) {
        query.type = QueryType::UNKNOWN;
        return MakeUnexpected(result.error());
      }
    } else {
      SetError("FACET: Unknown clause: " + tokens[pos]);
      query.type = QueryType::UNKNOWN;
      return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, error_));
    }
  }

  // Validate term counts
  if (query.and_terms.size() > kMaxTermCount) {
    SetError("Too many AND terms (max " + std::to_string(kMaxTermCount) + ")");
    return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, error_));
  }
  if (query.not_terms.size() > kMaxTermCount) {
    SetError("Too many NOT terms (max " + std::to_string(kMaxTermCount) + ")");
    return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, error_));
  }
  if (query.filters.size() > kMaxTermCount) {
    SetError("Too many FILTER conditions (max " + std::to_string(kMaxTermCount) + ")");
    return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, error_));
  }

  {
    auto result = ValidateQueryLength(query);
    if (!result) {
      query.type = QueryType::UNKNOWN;
      return MakeUnexpected(result.error());
    }
  }

  return query;
}

}  // namespace mygramdb::query
