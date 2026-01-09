/**
 * @file query_parser.cpp
 * @brief Query parser implementation
 */

#include "query/query_parser.h"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <array>
#include <cctype>
#include <sstream>

#include "query/cache_key.h"
#include "query/query_normalizer.h"

namespace mygramdb::query {

namespace {

// Maximum LIMIT value (1000)
constexpr uint32_t kMaxLimit = 1000;

size_t CalculateQueryExpressionLength(const Query& query) {
  size_t length = query.search_text.size();

  auto accumulate_terms = [&length](const std::vector<std::string>& terms) {
    for (const auto& term : terms) {
      length += term.size();
    }
  };

  accumulate_terms(query.and_terms);
  accumulate_terms(query.not_terms);

  for (const auto& filter : query.filters) {
    length += filter.column.size();
    length += filter.value.size();
  }

  if (query.order_by.has_value()) {
    length += query.order_by->column.size();
  }

  return length;
}

/**
 * @brief Convert string to uppercase
 * @deprecated Use EqualsIgnoreCase for comparisons to avoid unnecessary allocations
 */
std::string ToUpper(std::string_view str) {
  std::string result(str);
  std::transform(result.begin(), result.end(), result.begin(),
                 [](unsigned char character) { return std::toupper(character); });
  return result;
}

/**
 * @brief Case-insensitive string comparison (optimized, no allocations)
 * @param lhs First string
 * @param rhs Second string
 * @return true if strings are equal ignoring case
 */
bool EqualsIgnoreCase(std::string_view lhs, std::string_view rhs) {
  if (lhs.size() != rhs.size()) {
    return false;
  }
  return std::equal(lhs.begin(), lhs.end(), rhs.begin(), [](unsigned char lhs_char, unsigned char rhs_char) {
    return std::tolower(lhs_char) == std::tolower(rhs_char);
  });
}

/**
 * @brief Check if token is a query clause keyword
 * @param token Token to check (should be uppercase)
 * @return true if token is a clause keyword (AND, OR, NOT, FILTER, SORT, LIMIT, OFFSET)
 */
bool IsClauseKeyword(const std::string& token) {
  return token == "AND" || token == "OR" || token == "NOT" || token == "FILTER" || token == "SORT" ||
         token == "LIMIT" || token == "OFFSET";
}

}  // namespace

bool Query::IsValid() const {
  if (type == QueryType::UNKNOWN) {
    return false;
  }

  // Check if this query type requires a table name
  auto requires_table = [this]() -> bool {
    switch (type) {
      // Commands that do NOT require a table
      case QueryType::INFO:
      case QueryType::SAVE:
      case QueryType::LOAD:
      case QueryType::DUMP_SAVE:
      case QueryType::DUMP_LOAD:
      case QueryType::DUMP_VERIFY:
      case QueryType::DUMP_INFO:
      case QueryType::DUMP_STATUS:
      case QueryType::REPLICATION_STATUS:
      case QueryType::REPLICATION_STOP:
      case QueryType::REPLICATION_START:
      case QueryType::SYNC_STATUS:
      case QueryType::SYNC_STOP:
      case QueryType::CONFIG_HELP:
      case QueryType::CONFIG_SHOW:
      case QueryType::CONFIG_VERIFY:
      case QueryType::OPTIMIZE:
      case QueryType::DEBUG_ON:
      case QueryType::DEBUG_OFF:
      case QueryType::CACHE_CLEAR:
      case QueryType::CACHE_STATS:
      case QueryType::CACHE_ENABLE:
      case QueryType::CACHE_DISABLE:
      case QueryType::SET:
      case QueryType::SHOW_VARIABLES:
        return false;
      // All other commands require a table
      default:
        return true;
    }
  };

  if (requires_table() && table.empty()) {
    return false;
  }

  if (type == QueryType::SEARCH || type == QueryType::COUNT) {
    if (search_text.empty()) {
      return false;
    }
  }

  if (type == QueryType::GET) {
    if (primary_key.empty()) {
      return false;
    }
  }

  if (type == QueryType::SEARCH) {
    if (limit == 0 || limit > kMaxLimit) {
      return false;
    }
  }

  return true;
}

mygram::utils::Expected<Query, mygram::utils::Error> QueryParser::Parse(std::string_view query_str) {
  using mygram::utils::Error;
  using mygram::utils::ErrorCode;
  using mygram::utils::MakeError;
  using mygram::utils::MakeUnexpected;

  error_.clear();

  auto tokens = Tokenize(query_str);
  if (tokens.empty()) {
    // If error is already set by Tokenize (e.g., unclosed quote), keep it
    if (error_.empty()) {
      SetError("Empty query");
    }
    return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, error_));
  }

  // Use string_view for zero-copy comparison
  std::string_view command = tokens[0];

  if (EqualsIgnoreCase(command, "SEARCH")) {
    return ParseSearch(tokens);
  }
  if (EqualsIgnoreCase(command, "COUNT")) {
    return ParseCount(tokens);
  }
  if (EqualsIgnoreCase(command, "GET")) {
    return ParseGet(tokens);
  }
  if (EqualsIgnoreCase(command, "INFO")) {
    Query query;
    query.type = QueryType::INFO;
    query.table = "";  // INFO doesn't need a table
    return query;
  }
  if (EqualsIgnoreCase(command, "SAVE")) {
    Query query;
    query.type = QueryType::SAVE;
    query.table = "";  // SAVE doesn't need a table
    // Optional filepath argument
    // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
    if (tokens.size() > 1) {  // 1: Check for optional filepath after SAVE
      query.filepath = tokens[1];
    }
    return query;
  }
  if (EqualsIgnoreCase(command, "LOAD")) {
    Query query;
    query.type = QueryType::LOAD;
    query.table = "";  // LOAD doesn't need a table
    // Optional filepath argument
    // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
    if (tokens.size() > 1) {  // 1: Check for optional filepath after LOAD
      query.filepath = tokens[1];
    }
    return query;
  }
  if (EqualsIgnoreCase(command, "DUMP")) {
    // DUMP SAVE | LOAD | VERIFY | INFO
    // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
    if (tokens.size() < 2) {  // 2: DUMP + subcommand
      SetError("DUMP requires a subcommand (SAVE, LOAD, VERIFY, INFO, STATUS)");
      return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, error_));
    }

    std::string subcommand = ToUpper(tokens[1]);
    Query query;
    query.table = "";  // DUMP doesn't need a table

    if (subcommand == "SAVE") {
      query.type = QueryType::DUMP_SAVE;
      // DUMP SAVE [filepath] [--with-stats]
      // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
      for (size_t i = 2; i < tokens.size(); ++i) {  // 2: Start after DUMP SAVE
        const std::string& token = tokens[i];
        if (token == "--with-stats") {
          query.dump_with_stats = true;
        } else if (token[0] != '-') {
          // Filepath (not a flag)
          query.filepath = token;
        } else {
          SetError("Unknown DUMP SAVE flag: " + token);
          return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, error_));
        }
      }
    } else if (subcommand == "LOAD") {
      query.type = QueryType::DUMP_LOAD;
      // DUMP LOAD filepath
      // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
      if (tokens.size() > 2) {  // 2: DUMP LOAD + filepath
        query.filepath = tokens[2];
      } else {
        SetError("DUMP LOAD requires a filepath");
        return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, error_));
      }
    } else if (subcommand == "VERIFY") {
      query.type = QueryType::DUMP_VERIFY;
      // DUMP VERIFY filepath
      // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
      if (tokens.size() > 2) {  // 2: DUMP VERIFY + filepath
        query.filepath = tokens[2];
      } else {
        SetError("DUMP VERIFY requires a filepath");
        return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, error_));
      }
    } else if (subcommand == "INFO") {
      query.type = QueryType::DUMP_INFO;
      // DUMP INFO filepath
      // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
      if (tokens.size() > 2) {  // 2: DUMP INFO + filepath
        query.filepath = tokens[2];
      } else {
        SetError("DUMP INFO requires a filepath");
        return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, error_));
      }
    } else if (subcommand == "STATUS") {
      query.type = QueryType::DUMP_STATUS;
      // DUMP STATUS - no arguments required
    } else {
      SetError("Unknown DUMP subcommand: " + subcommand);
      return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, error_));
    }

    return query;
  }
  if (command == "CONFIG") {
    Query query;
    query.table = "";  // CONFIG doesn't need a table

    // CONFIG HELP [path] | CONFIG SHOW [path] | CONFIG VERIFY <filepath>
    // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
    if (tokens.size() > 1) {  // 1: CONFIG + subcommand
      std::string subcommand = ToUpper(tokens[1]);
      if (subcommand == "HELP") {
        query.type = QueryType::CONFIG_HELP;
        // Optional path argument
        // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
        if (tokens.size() > 2) {  // 2: CONFIG HELP + path
          query.filepath = tokens[2];
        }
      } else if (subcommand == "SHOW") {
        query.type = QueryType::CONFIG_SHOW;
        // Optional path argument
        // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
        if (tokens.size() > 2) {  // 2: CONFIG SHOW + path
          query.filepath = tokens[2];
        }
      } else if (subcommand == "VERIFY") {
        query.type = QueryType::CONFIG_VERIFY;
        // Required filepath argument
        // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
        if (tokens.size() > 2) {  // 2: CONFIG VERIFY + filepath
          query.filepath = tokens[2];
        } else {
          SetError("CONFIG VERIFY requires a filepath");
          return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, error_));
        }
      } else {
        SetError("Unknown CONFIG subcommand: " + subcommand + " (expected HELP, SHOW, or VERIFY)");
        return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, error_));
      }
    } else {
      // CONFIG without subcommand defaults to CONFIG SHOW
      query.type = QueryType::CONFIG_SHOW;
    }

    return query;
  }
  if (command == "REPLICATION") {
    // REPLICATION STATUS | STOP | START
    // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
    if (tokens.size() < 2) {  // 2: REPLICATION + subcommand
      SetError("REPLICATION requires a subcommand (STATUS, STOP, START)");
      return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, error_));
    }

    std::string subcommand = ToUpper(tokens[1]);
    Query query;
    query.table = "";  // REPLICATION doesn't need a table

    if (subcommand == "STATUS") {
      query.type = QueryType::REPLICATION_STATUS;
    } else if (subcommand == "STOP") {
      query.type = QueryType::REPLICATION_STOP;
    } else if (subcommand == "START") {
      query.type = QueryType::REPLICATION_START;
    } else {
      SetError("Unknown REPLICATION subcommand: " + subcommand);
      return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, error_));
    }

    return query;
  }
  if (command == "SYNC") {
    // SYNC [table] | SYNC STATUS | SYNC STOP [table]
    Query query;

    // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
    if (tokens.size() > 1) {  // 1: SYNC + subcommand or table
      std::string second_token = ToUpper(tokens[1]);
      if (second_token == "STATUS") {
        query.type = QueryType::SYNC_STATUS;
        query.table = "";  // SYNC STATUS doesn't need a table
      } else if (second_token == "STOP") {
        query.type = QueryType::SYNC_STOP;
        // SYNC STOP [table] - optional table name
        // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
        if (tokens.size() > 2) {    // 2: SYNC STOP <table>
          query.table = tokens[2];  // Keep original case for table name
        } else {
          query.table = "";  // Empty means stop all
        }
      } else {
        // SYNC <table>
        query.type = QueryType::SYNC;
        query.table = tokens[1];  // Keep original case for table name
      }
    } else {
      // SYNC without arguments (sync all tables or error)
      SetError("SYNC requires a table name or STATUS/STOP subcommand");
      return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, error_));
    }

    return query;
  }
  if (command == "OPTIMIZE") {
    // OPTIMIZE [table] - optimize index posting lists
    Query query;
    query.type = QueryType::OPTIMIZE;
    // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
    if (tokens.size() > 1) {  // 1: OPTIMIZE + table
      query.table = tokens[1];
    }
    // If no table specified, query.table remains empty (handler will use default or error)
    return query;
  }
  if (EqualsIgnoreCase(command, "DEBUG")) {
    // DEBUG ON | OFF
    // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
    if (tokens.size() < 2) {  // 2: DEBUG + mode (ON/OFF)
      SetError("DEBUG requires ON or OFF");
      return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, error_));
    }

    std::string_view mode = tokens[1];
    Query query;
    query.table = "";  // DEBUG doesn't need a table

    if (EqualsIgnoreCase(mode, "ON")) {
      query.type = QueryType::DEBUG_ON;
    } else if (EqualsIgnoreCase(mode, "OFF")) {
      query.type = QueryType::DEBUG_OFF;
    } else {
      SetError("DEBUG requires ON or OFF, got: " + std::string(mode));
      return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, error_));
    }

    return query;
  }
  if (command == "CACHE") {
    // CACHE CLEAR [table] | STATS | ENABLE | DISABLE
    // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
    if (tokens.size() < 2) {  // 2: CACHE + subcommand
      SetError("CACHE requires a subcommand (CLEAR, STATS, ENABLE, DISABLE)");
      return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, error_));
    }

    std::string subcommand = ToUpper(tokens[1]);
    Query query;
    query.table = "";  // CACHE doesn't need a table by default

    if (subcommand == "CLEAR") {
      query.type = QueryType::CACHE_CLEAR;
      // CACHE CLEAR [table] - optional table name
      // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
      if (tokens.size() > 2) {  // 2: CACHE CLEAR + table
        query.table = tokens[2];
      }
    } else if (subcommand == "STATS") {
      query.type = QueryType::CACHE_STATS;
    } else if (subcommand == "ENABLE") {
      query.type = QueryType::CACHE_ENABLE;
    } else if (subcommand == "DISABLE") {
      query.type = QueryType::CACHE_DISABLE;
    } else {
      SetError("Unknown CACHE subcommand: " + subcommand);
      return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, error_));
    }

    return query;
  }

  // SET variable = value [, variable2 = value2 ...]
  if (EqualsIgnoreCase(command, "SET")) {
    Query query;
    query.type = QueryType::SET;

    // Parse variable assignments: variable = value [, variable2 = value2 ...]
    size_t pos = 1;
    while (pos < tokens.size()) {
      // Expect: variable_name = value
      if (pos + 2 >= tokens.size()) {
        SetError("SET: Expected variable = value");
        return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, error_));
      }

      std::string variable_name = tokens[pos];
      std::string equals_sign = tokens[pos + 1];
      std::string value = tokens[pos + 2];

      if (equals_sign != "=") {
        SetError("SET: Expected '=' after variable name");
        return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, error_));
      }

      // Handle comma at end of value (e.g., "value1," -> "value1" with more to come)
      bool has_trailing_comma = !value.empty() && value.back() == ',';
      if (has_trailing_comma) {
        value.pop_back();  // Remove trailing comma
      }

      query.variable_assignments.emplace_back(variable_name, value);
      pos += 3;

      // Check for more assignments
      if (has_trailing_comma) {
        // We already consumed the comma attached to the value
        // Continue to next assignment
        continue;
      }

      // Check for comma as separate token (more assignments)
      if (pos < tokens.size()) {
        if (tokens[pos] == ",") {
          pos++;  // Skip comma token
        } else {
          SetError("SET: Expected ',' or end of query");
          return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, error_));
        }
      }
    }

    if (query.variable_assignments.empty()) {
      SetError("SET: No variable assignments found");
      return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, error_));
    }

    return query;
  }

  // SHOW VARIABLES [LIKE 'pattern']
  if (EqualsIgnoreCase(command, "SHOW")) {
    if (tokens.size() < 2) {
      SetError("SHOW: Expected subcommand");
      return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, error_));
    }

    std::string subcommand = ToUpper(tokens[1]);
    if (subcommand == "VARIABLES") {
      Query query;
      query.type = QueryType::SHOW_VARIABLES;

      // Check for LIKE clause
      if (tokens.size() >= 4 && ToUpper(tokens[2]) == "LIKE") {
        query.variable_like_pattern = tokens[3];
      }

      return query;
    }

    SetError("SHOW: Unknown subcommand: " + subcommand);
    return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, error_));
  }

  SetError("Unknown command: " + std::string(command));
  return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, error_));
}

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
    return query;
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

  // Helper lambda to count parentheses in a token, respecting quotes
  auto count_parens = [](const std::string& token) -> std::pair<int, int> {
    int open = 0;
    int close = 0;
    bool in_quote = false;
    char quote_char = '\0';

    for (size_t i = 0; i < token.size(); ++i) {
      char chr = token[i];

      // Handle quote state
      if ((chr == '"' || chr == '\'') && (i == 0 || token[i - 1] != '\\')) {
        if (!in_quote) {
          in_quote = true;
          quote_char = chr;
        } else if (chr == quote_char) {
          in_quote = false;
          quote_char = '\0';
        }
      }

      // Count parentheses only when not inside quotes
      if (!in_quote) {
        if (chr == '(') {
          open++;
        }
        if (chr == ')') {
          close++;
        }
      }
    }

    return {open, close};
  };

  // First pass: check parentheses balance across ALL tokens
  int total_paren_depth = 0;
  // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
  for (size_t i = 2; i < tokens.size(); ++i) {  // 2: Skip SEARCH and table name
    auto [open, close] = count_parens(tokens[i]);
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
    auto [open, close] = count_parens(token);
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
    return query;
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
    return query;
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

  // Helper lambda to count parentheses in a token, respecting quotes
  auto count_parens = [](const std::string& token) -> std::pair<int, int> {
    int open = 0;
    int close = 0;
    bool in_quote = false;
    char quote_char = '\0';

    for (size_t i = 0; i < token.size(); ++i) {
      char chr = token[i];

      // Handle quote state
      if ((chr == '"' || chr == '\'') && (i == 0 || token[i - 1] != '\\')) {
        if (!in_quote) {
          in_quote = true;
          quote_char = chr;
        } else if (chr == quote_char) {
          in_quote = false;
          quote_char = '\0';
        }
      }

      // Count parentheses only when not inside quotes
      if (!in_quote) {
        if (chr == '(') {
          open++;
        }
        if (chr == ')') {
          close++;
        }
      }
    }

    return {open, close};
  };

  // First pass: check parentheses balance across ALL tokens
  int total_paren_depth = 0;
  // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
  for (size_t i = 2; i < tokens.size(); ++i) {  // 2: Skip COUNT and table name
    auto [open, close] = count_parens(tokens[i]);
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
    auto [open, close] = count_parens(token);
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
    return query;
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
    return query;
  }

  query.table = tokens[1];
  query.primary_key = tokens[2];

  return query;
}

void QueryParser::SetMaxQueryLength(size_t max_length) {
  max_query_length_ = max_length;
}

bool QueryParser::ValidateQueryLength(const Query& query) {
  if (max_query_length_ == 0) {
    return true;
  }

  const size_t expression_length = CalculateQueryExpressionLength(query);
  if (expression_length > max_query_length_) {
    SetError("Query expression length (" + std::to_string(expression_length) + ") exceeds maximum allowed length of " +
             std::to_string(max_query_length_) +
             " characters. Increase api.max_query_length to permit longer queries.");
    return false;
  }

  return true;
}

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

    filter.column = column_part;
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

  filter.column = tokens[pos++];

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
  order_by.column = tokens[pos++];

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

std::vector<std::string> QueryParser::Tokenize(std::string_view str) {
  std::vector<std::string> tokens;
  std::string token;
  char quote_char = '\0';  // '\0' = not in quotes, '"' or '\'' = in quotes
  bool escape_next = false;

  for (size_t i = 0; i < str.length(); ++i) {
    char character = str[i];

    if (escape_next) {
      // Handle escape sequences
      switch (character) {
        case 'n':
          token += '\n';
          break;
        case 't':
          token += '\t';
          break;
        case 'r':
          token += '\r';
          break;
        case '\\':
          token += '\\';
          break;
        case '"':
          token += '"';
          break;
        case '\'':
          token += '\'';
          break;
        default:
          token += character;
          break;  // Unknown escape, keep as-is
      }
      escape_next = false;
      continue;
    }

    if (character == '\\') {
      escape_next = true;
      continue;
    }

    if (quote_char == '\0') {
      // Not currently in quotes
      if (character == '"' || character == '\'') {
        // Start of quoted string - save any pending token first
        if (!token.empty()) {
          tokens.push_back(token);
          token.clear();
        }
        quote_char = character;
        continue;
      }

      // Outside quotes, split on whitespace
      if (std::isspace(character) != 0) {
        if (!token.empty()) {
          tokens.push_back(token);
          token.clear();
        }
      } else {
        token += character;
      }
    } else {
      // Inside quotes
      if (character == quote_char) {
        // End of quoted string - always add token, even if empty
        // Empty quoted strings are significant (e.g., to detect errors)
        tokens.push_back(token);
        token.clear();
        quote_char = '\0';
        continue;
      }

      // Inside quotes, add everything including spaces
      token += character;
    }
  }

  // Check for unterminated escape sequence at EOF
  if (escape_next) {
    SetError("Unterminated escape sequence at end of input");
    return {};  // Return empty vector on error
  }

  // Check for unclosed quotes
  if (quote_char != '\0') {
    SetError(std::string("Unclosed quote: ") + quote_char);
    return {};  // Return empty vector on error
  }

  // Add final token if any
  if (!token.empty()) {
    tokens.push_back(token);
  }

  return tokens;
}

std::optional<FilterOp> QueryParser::ParseFilterOp(std::string_view op_str) {
  std::string normalized_op = ToUpper(op_str);

  if (normalized_op == "=" || normalized_op == "EQ") {
    return FilterOp::EQ;
  }
  if (normalized_op == "!=" || normalized_op == "NE") {
    return FilterOp::NE;
  }
  if (normalized_op == ">" || normalized_op == "GT") {
    return FilterOp::GT;
  }
  if (normalized_op == ">=" || normalized_op == "GTE") {
    return FilterOp::GTE;
  }
  if (normalized_op == "<" || normalized_op == "LT") {
    return FilterOp::LT;
  }
  if (normalized_op == "<=" || normalized_op == "LTE") {
    return FilterOp::LTE;
  }

  return std::nullopt;
}

}  // namespace mygramdb::query
