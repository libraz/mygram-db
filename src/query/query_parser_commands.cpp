/**
 * @file query_parser_commands.cpp
 * @brief Query parser command implementations (SEARCH, COUNT, GET)
 */

#include <spdlog/spdlog.h>

#include <string>
#include <vector>

#include "query/cache_key.h"
#include "query/query_normalizer.h"
#include "query/query_parser.h"
#include "query/query_parser_internal.h"
#include "utils/string_utils.h"

namespace mygramdb::query {

using mygram::utils::ErrorCode;
using mygram::utils::MakeError;
using mygram::utils::MakeUnexpected;

using internal::IsClauseKeyword;
using internal::kMaxLimit;

constexpr size_t kMaxTermCount = 64;
using mygram::utils::ToUpper;

mygram::utils::Expected<Query, mygram::utils::Error> QueryParser::ParseSearch(const std::vector<std::string>& tokens) {
  using mygram::utils::ErrorCode;
  using mygram::utils::MakeError;
  using mygram::utils::MakeUnexpected;

  Query query;
  query.type = QueryType::SEARCH;

  // SEARCH <table> <text> [FILTER ...] [LIMIT n] [OFFSET n]
  // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
  if (tokens.size() < 3) {  // 3: SEARCH + table + text (minimum)
    SetError("SEARCH requires at least table and search text");
    return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, error_));
  }

  query.table = tokens[1];

  // Check for comma-separated table names (SQL-style multi-table syntax)
  if (query.table.find(',') != std::string::npos || (tokens.size() > 2 && tokens[2] == ",")) {
    SetError(
        "Multiple tables not supported. Hint: MygramDB searches a single table at a time. Use separate queries "
        "for multiple tables.");
    query.type = QueryType::UNKNOWN;
    return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, error_));
  }

  // First pass: check parentheses balance across ALL tokens
  int total_paren_depth = 0;
  // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
  for (size_t i = 2; i < tokens.size(); ++i) {  // 2: Skip SEARCH and table name
    auto [open, close] = detail::CountParensInToken(tokens[i]);
    total_paren_depth += open - close;

    // Check for unmatched closing parentheses
    if (total_paren_depth < 0) {
      SetError("Unmatched closing parenthesis");
      query.type = QueryType::UNKNOWN;
      return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, error_));
    }
  }

  // Check for unclosed parentheses
  if (total_paren_depth > 0) {
    SetError("Unclosed parenthesis");
    query.type = QueryType::UNKNOWN;
    return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, error_));
  }

  // Extract search text: consume tokens until we hit a keyword (AND, OR, NOT, FILTER, ORDER, LIMIT,
  // OFFSET) Handle parentheses by tracking nesting level - but respect quoted strings
  // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
  size_t pos = 2;  // 2: Start after SEARCH and table name
  std::vector<std::string> search_tokens;
  int paren_depth = 0;

  while (pos < tokens.size()) {
    const std::string& token = tokens[pos];
    std::string upper_token = ToUpper(token);

    // Track parentheses depth (respecting quotes)
    auto [open, close] = detail::CountParensInToken(token);
    paren_depth += open - close;

    // Check if this is a keyword (only when not inside parentheses)
    if (paren_depth == 0 && IsClauseKeyword(upper_token)) {
      break;  // Stop consuming search text
    }

    // Special check for deprecated ORDER keyword - provide helpful error
    if (paren_depth == 0 && upper_token == "ORDER") {
      SetError("ORDER BY is not supported. Use SORT instead. Example: SEARCH table text SORT column DESC");
      query.type = QueryType::UNKNOWN;
      return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, error_));
    }

    search_tokens.push_back(token);
    pos++;
  }

  if (search_tokens.empty()) {
    SetError("SEARCH requires search text");
    return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, error_));
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
    SetError("SEARCH requires non-empty search text");
    query.type = QueryType::UNKNOWN;
    return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, error_));
  }

  // Parse optional clauses
  while (pos < tokens.size()) {
    std::string keyword = ToUpper(tokens[pos]);

    if (keyword == "AND") {
      if (!ParseAnd(tokens, pos, query)) {
        query.type = QueryType::UNKNOWN;
        return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, error_));
      }
    } else if (keyword == "NOT") {
      if (!ParseNot(tokens, pos, query)) {
        query.type = QueryType::UNKNOWN;
        return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, error_));
      }
    } else if (keyword == "FILTER") {
      if (!ParseFilters(tokens, pos, query)) {
        query.type = QueryType::UNKNOWN;
        return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, error_));
      }
    } else if (keyword == "ORDER") {
      // ORDER BY is deprecated, guide users to use SORT
      SetError("ORDER BY is not supported. Use SORT instead. Example: SEARCH table text SORT column DESC");
      query.type = QueryType::UNKNOWN;
      return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, error_));
    } else if (keyword == "SORT") {
      if (!ParseSort(tokens, pos, query)) {
        query.type = QueryType::UNKNOWN;
        return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, error_));
      }
    } else if (keyword == "LIMIT") {
      if (!ParseLimit(tokens, pos, query)) {
        query.type = QueryType::UNKNOWN;
        return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, error_));
      }
    } else if (keyword == "OFFSET") {
      if (!ParseOffset(tokens, pos, query)) {
        query.type = QueryType::UNKNOWN;
        return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, error_));
      }
    } else {
      SetError("Unknown keyword: " + keyword);
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

  if (!ValidateQueryLength(query)) {
    query.type = QueryType::UNKNOWN;
    return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, error_));
  }

  // Precompute cache key for performance optimization
  // This avoids recomputing normalization and MD5 hash on every cache lookup
  const std::string normalized = cache::QueryNormalizer::Normalize(query);
  if (!normalized.empty()) {
    const cache::CacheKey key = cache::CacheKeyGenerator::Generate(normalized);
    query.cache_key = std::make_pair(key.hash_high, key.hash_low);
  }

  return query;
}

mygram::utils::Expected<Query, mygram::utils::Error> QueryParser::ParseCount(const std::vector<std::string>& tokens) {
  using mygram::utils::ErrorCode;
  using mygram::utils::MakeError;
  using mygram::utils::MakeUnexpected;

  Query query;
  query.type = QueryType::COUNT;

  // COUNT <table> <text> [FILTER ...]
  // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
  if (tokens.size() < 3) {  // 3: COUNT + table + text (minimum)
    SetError("COUNT requires at least table and search text");
    return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, error_));
  }

  query.table = tokens[1];

  // Check for comma-separated table names (SQL-style multi-table syntax)
  if (query.table.find(',') != std::string::npos || (tokens.size() > 2 && tokens[2] == ",")) {
    SetError(
        "Multiple tables not supported. Hint: MygramDB searches a single table at a time. Use separate queries "
        "for multiple tables.");
    query.type = QueryType::UNKNOWN;
    return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, error_));
  }

  // First pass: check parentheses balance across ALL tokens
  int total_paren_depth = 0;
  // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
  for (size_t i = 2; i < tokens.size(); ++i) {  // 2: Skip COUNT and table name
    auto [open, close] = detail::CountParensInToken(tokens[i]);
    total_paren_depth += open - close;

    // Check for unmatched closing parentheses
    if (total_paren_depth < 0) {
      SetError("Unmatched closing parenthesis");
      query.type = QueryType::UNKNOWN;
      return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, error_));
    }
  }

  // Check for unclosed parentheses
  if (total_paren_depth > 0) {
    SetError("Unclosed parenthesis");
    query.type = QueryType::UNKNOWN;
    return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, error_));
  }

  // Extract search text: consume tokens until we hit a keyword
  // Handle parentheses by tracking nesting level - but respect quoted strings
  // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
  size_t pos = 2;  // 2: Start after COUNT and table name
  std::vector<std::string> search_tokens;
  int paren_depth = 0;

  while (pos < tokens.size()) {
    const std::string& token = tokens[pos];
    std::string upper_token = ToUpper(token);

    // Track parentheses depth (respecting quotes)
    auto [open, close] = detail::CountParensInToken(token);
    paren_depth += open - close;

    // Check if this is a keyword (only when not inside parentheses)
    // Include LIMIT/OFFSET so they stop token consumption and get rejected below
    if (paren_depth == 0 && IsClauseKeyword(upper_token)) {
      break;  // Stop consuming search text
    }

    // Special check for deprecated ORDER keyword - provide helpful error
    if (paren_depth == 0 && upper_token == "ORDER") {
      SetError("ORDER BY is not supported. Use SORT instead.");
      query.type = QueryType::UNKNOWN;
      return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, error_));
    }

    search_tokens.push_back(token);
    pos++;
  }

  if (search_tokens.empty()) {
    SetError("COUNT requires search text");
    return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, error_));
  }

  // Join search tokens with spaces
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
    SetError("COUNT requires non-empty search text");
    query.type = QueryType::UNKNOWN;
    return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, error_));
  }

  // Parse optional clauses
  while (pos < tokens.size()) {
    std::string keyword = ToUpper(tokens[pos]);

    if (keyword == "AND") {
      if (!ParseAnd(tokens, pos, query)) {
        query.type = QueryType::UNKNOWN;
        return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, error_));
      }
    } else if (keyword == "NOT") {
      if (!ParseNot(tokens, pos, query)) {
        query.type = QueryType::UNKNOWN;
        return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, error_));
      }
    } else if (keyword == "FILTER") {
      if (!ParseFilters(tokens, pos, query)) {
        query.type = QueryType::UNKNOWN;
        return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, error_));
      }
    } else if (keyword == "ORDER") {
      SetError("ORDER BY is not supported. Use SORT instead (note: COUNT does not support sorting).");
      query.type = QueryType::UNKNOWN;
      return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, error_));
    } else if (keyword == "SORT") {
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

  if (!ValidateQueryLength(query)) {
    query.type = QueryType::UNKNOWN;
    return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, error_));
  }

  // Precompute cache key for performance optimization
  // This avoids recomputing normalization and MD5 hash on every cache lookup
  const std::string normalized = cache::QueryNormalizer::Normalize(query);
  if (!normalized.empty()) {
    const cache::CacheKey key = cache::CacheKeyGenerator::Generate(normalized);
    query.cache_key = std::make_pair(key.hash_high, key.hash_low);
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

}  // namespace mygramdb::query
