/**
 * @file query_parser.cpp
 * @brief Query parser implementation
 */

#include "query/query_parser.h"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <cctype>
#include <sstream>

namespace mygramdb {
namespace query {

namespace {

// Maximum LIMIT value (1000)
constexpr uint32_t kMaxLimit = 1000;

/**
 * @brief Convert string to uppercase
 */
std::string ToUpper(const std::string& str) {
  std::string result = str;
  std::transform(result.begin(), result.end(), result.begin(),
                 [](unsigned char character) { return std::toupper(character); });
  return result;
}

}  // namespace

bool Query::IsValid() const {
  if (type == QueryType::UNKNOWN) {
    return false;
  }

  // INFO, SAVE, LOAD, REPLICATION_*, CONFIG, OPTIMIZE, DEBUG_* commands don't require a table
  if (type != QueryType::INFO && type != QueryType::SAVE && type != QueryType::LOAD &&
      type != QueryType::REPLICATION_STATUS && type != QueryType::REPLICATION_STOP &&
      type != QueryType::REPLICATION_START && type != QueryType::CONFIG &&
      type != QueryType::OPTIMIZE && type != QueryType::DEBUG_ON && type != QueryType::DEBUG_OFF &&
      table.empty()) {
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

Query QueryParser::Parse(const std::string& query_str) {
  error_.clear();

  auto tokens = Tokenize(query_str);
  if (tokens.empty()) {
    // If error is already set by Tokenize (e.g., unclosed quote), keep it
    if (error_.empty()) {
      SetError("Empty query");
    }
    return Query{};
  }

  std::string command = ToUpper(tokens[0]);

  if (command == "SEARCH") {
    return ParseSearch(tokens);
  }
  if (command == "COUNT") {
    return ParseCount(tokens);
  }
  if (command == "GET") {
    return ParseGet(tokens);
  }
  if (command == "INFO") {
    Query query;
    query.type = QueryType::INFO;
    query.table = "";  // INFO doesn't need a table
    return query;
  }
  if (command == "SAVE") {
    Query query;
    query.type = QueryType::SAVE;
    query.table = "";  // SAVE doesn't need a table
    // Optional filepath argument
    if (tokens.size() > 1) {
      query.filepath = tokens[1];
    }
    return query;
  }
  if (command == "LOAD") {
    Query query;
    query.type = QueryType::LOAD;
    query.table = "";  // LOAD doesn't need a table
    // Optional filepath argument
    if (tokens.size() > 1) {
      query.filepath = tokens[1];
    }
    return query;
  }
  if (command == "REPLICATION") {
    // REPLICATION STATUS | STOP | START
    if (tokens.size() < 2) {
      SetError("REPLICATION requires a subcommand (STATUS, STOP, START)");
      return Query{};
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
      return Query{};
    }

    return query;
  }
  if (command == "CONFIG") {
    // CONFIG - show current configuration
    Query query;
    query.type = QueryType::CONFIG;
    query.table = "";  // CONFIG doesn't need a table
    return query;
  }
  if (command == "OPTIMIZE") {
    // OPTIMIZE - optimize index posting lists
    Query query;
    query.type = QueryType::OPTIMIZE;
    query.table = "";  // OPTIMIZE doesn't need a table
    return query;
  }
  if (command == "DEBUG") {
    // DEBUG ON | OFF
    if (tokens.size() < 2) {
      SetError("DEBUG requires ON or OFF");
      return Query{};
    }

    std::string mode = ToUpper(tokens[1]);
    Query query;
    query.table = "";  // DEBUG doesn't need a table

    if (mode == "ON") {
      query.type = QueryType::DEBUG_ON;
    } else if (mode == "OFF") {
      query.type = QueryType::DEBUG_OFF;
    } else {
      SetError("DEBUG requires ON or OFF, got: " + mode);
      return Query{};
    }

    return query;
  }

  SetError("Unknown command: " + command);
  return Query{};
}

Query QueryParser::ParseSearch(const std::vector<std::string>& tokens) {
  Query query;
  query.type = QueryType::SEARCH;

  // SEARCH <table> <text> [FILTER ...] [LIMIT n] [OFFSET n]
  if (tokens.size() < 3) {
    SetError("SEARCH requires at least table and search text");
    return query;
  }

  query.table = tokens[1];

  // Helper lambda to count parentheses in a token, respecting quotes
  auto count_parens = [](const std::string& token) -> std::pair<int, int> {
    int open = 0, close = 0;
    bool in_quote = false;
    char quote_char = '\0';

    for (size_t i = 0; i < token.size(); ++i) {
      char c = token[i];

      // Handle quote state
      if ((c == '"' || c == '\'') && (i == 0 || token[i - 1] != '\\')) {
        if (!in_quote) {
          in_quote = true;
          quote_char = c;
        } else if (c == quote_char) {
          in_quote = false;
          quote_char = '\0';
        }
      }

      // Count parentheses only when not inside quotes
      if (!in_quote) {
        if (c == '(')
          open++;
        if (c == ')')
          close++;
      }
    }

    return {open, close};
  };

  // First pass: check parentheses balance across ALL tokens
  int total_paren_depth = 0;
  for (size_t i = 2; i < tokens.size(); ++i) {
    auto [open, close] = count_parens(tokens[i]);
    total_paren_depth += open - close;

    // Check for unmatched closing parentheses
    if (total_paren_depth < 0) {
      SetError("Unmatched closing parenthesis");
      query.type = QueryType::UNKNOWN;
      return query;
    }
  }

  // Check for unclosed parentheses
  if (total_paren_depth > 0) {
    SetError("Unclosed parenthesis");
    query.type = QueryType::UNKNOWN;
    return query;
  }

  // Extract search text: consume tokens until we hit a keyword (AND, OR, NOT, FILTER, ORDER, LIMIT,
  // OFFSET) Handle parentheses by tracking nesting level - but respect quoted strings
  size_t pos = 2;
  std::vector<std::string> search_tokens;
  int paren_depth = 0;

  while (pos < tokens.size()) {
    const std::string& token = tokens[pos];
    std::string upper_token = ToUpper(token);

    // Track parentheses depth (respecting quotes)
    auto [open, close] = count_parens(token);
    paren_depth += open - close;

    // Check if this is a keyword (only when not inside parentheses)
    if (paren_depth == 0 && (upper_token == "AND" || upper_token == "OR" || upper_token == "NOT" ||
                             upper_token == "FILTER" || upper_token == "ORDER" ||
                             upper_token == "LIMIT" || upper_token == "OFFSET")) {
      break;  // Stop consuming search text
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
  for (size_t i = 1; i < search_tokens.size(); ++i) {
    const std::string& token = search_tokens[i];
    // Don't add space before closing parentheses or after opening parentheses
    bool prev_ends_with_open_paren =
        !search_tokens[i - 1].empty() && search_tokens[i - 1].back() == '(';
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
    return query;
  }

  // Parse optional clauses
  while (pos < tokens.size()) {
    std::string keyword = ToUpper(tokens[pos]);

    if (keyword == "AND") {
      if (!ParseAnd(tokens, pos, query)) {
        query.type = QueryType::UNKNOWN;
        return query;
      }
    } else if (keyword == "NOT") {
      if (!ParseNot(tokens, pos, query)) {
        query.type = QueryType::UNKNOWN;
        return query;
      }
    } else if (keyword == "FILTER") {
      if (!ParseFilters(tokens, pos, query)) {
        query.type = QueryType::UNKNOWN;
        return query;
      }
    } else if (keyword == "ORDER") {
      if (!ParseOrderBy(tokens, pos, query)) {
        query.type = QueryType::UNKNOWN;
        return query;
      }
    } else if (keyword == "LIMIT") {
      if (!ParseLimit(tokens, pos, query)) {
        query.type = QueryType::UNKNOWN;
        return query;
      }
    } else if (keyword == "OFFSET") {
      if (!ParseOffset(tokens, pos, query)) {
        query.type = QueryType::UNKNOWN;
        return query;
      }
    } else {
      SetError("Unknown keyword: " + keyword);
      query.type = QueryType::UNKNOWN;
      return query;
    }
  }

  // Validate limit
  if (query.limit > kMaxLimit) {
    SetError("LIMIT exceeds maximum of " + std::to_string(kMaxLimit));
    query.type = QueryType::UNKNOWN;
    return query;
  }

  return query;
}

Query QueryParser::ParseCount(const std::vector<std::string>& tokens) {
  Query query;
  query.type = QueryType::COUNT;

  // COUNT <table> <text> [FILTER ...]
  if (tokens.size() < 3) {
    SetError("COUNT requires at least table and search text");
    return query;
  }

  query.table = tokens[1];

  // Helper lambda to count parentheses in a token, respecting quotes
  auto count_parens = [](const std::string& token) -> std::pair<int, int> {
    int open = 0, close = 0;
    bool in_quote = false;
    char quote_char = '\0';

    for (size_t i = 0; i < token.size(); ++i) {
      char c = token[i];

      // Handle quote state
      if ((c == '"' || c == '\'') && (i == 0 || token[i - 1] != '\\')) {
        if (!in_quote) {
          in_quote = true;
          quote_char = c;
        } else if (c == quote_char) {
          in_quote = false;
          quote_char = '\0';
        }
      }

      // Count parentheses only when not inside quotes
      if (!in_quote) {
        if (c == '(')
          open++;
        if (c == ')')
          close++;
      }
    }

    return {open, close};
  };

  // First pass: check parentheses balance across ALL tokens
  int total_paren_depth = 0;
  for (size_t i = 2; i < tokens.size(); ++i) {
    auto [open, close] = count_parens(tokens[i]);
    total_paren_depth += open - close;

    // Check for unmatched closing parentheses
    if (total_paren_depth < 0) {
      SetError("Unmatched closing parenthesis");
      query.type = QueryType::UNKNOWN;
      return query;
    }
  }

  // Check for unclosed parentheses
  if (total_paren_depth > 0) {
    SetError("Unclosed parenthesis");
    query.type = QueryType::UNKNOWN;
    return query;
  }

  // Extract search text: consume tokens until we hit a keyword
  // Handle parentheses by tracking nesting level - but respect quoted strings
  size_t pos = 2;
  std::vector<std::string> search_tokens;
  int paren_depth = 0;

  while (pos < tokens.size()) {
    const std::string& token = tokens[pos];
    std::string upper_token = ToUpper(token);

    // Track parentheses depth (respecting quotes)
    auto [open, close] = count_parens(token);
    paren_depth += open - close;

    // Check if this is a keyword (only when not inside parentheses)
    // Include LIMIT/OFFSET/ORDER so they stop token consumption and get rejected below
    if (paren_depth == 0 && (upper_token == "AND" || upper_token == "OR" || upper_token == "NOT" ||
                             upper_token == "FILTER" || upper_token == "LIMIT" ||
                             upper_token == "OFFSET" || upper_token == "ORDER")) {
      break;  // Stop consuming search text
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
  for (size_t i = 1; i < search_tokens.size(); ++i) {
    const std::string& token = search_tokens[i];
    // Don't add space before closing parentheses or after opening parentheses
    bool prev_ends_with_open_paren =
        !search_tokens[i - 1].empty() && search_tokens[i - 1].back() == '(';
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
    return query;
  }

  // Parse optional clauses
  while (pos < tokens.size()) {
    std::string keyword = ToUpper(tokens[pos]);

    if (keyword == "AND") {
      if (!ParseAnd(tokens, pos, query)) {
        query.type = QueryType::UNKNOWN;
        return query;
      }
    } else if (keyword == "NOT") {
      if (!ParseNot(tokens, pos, query)) {
        query.type = QueryType::UNKNOWN;
        return query;
      }
    } else if (keyword == "FILTER") {
      if (!ParseFilters(tokens, pos, query)) {
        query.type = QueryType::UNKNOWN;
        return query;
      }
    } else {
      SetError("COUNT only supports AND, NOT and FILTER clauses");
      query.type = QueryType::UNKNOWN;
      return query;
    }
  }

  return query;
}

Query QueryParser::ParseGet(const std::vector<std::string>& tokens) {
  Query query;
  query.type = QueryType::GET;

  // GET <table> <primary_key>
  if (tokens.size() != 3) {
    SetError("GET requires table and primary_key");
    return query;
  }

  query.table = tokens[1];
  query.primary_key = tokens[2];

  return query;
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

  if (pos + 2 >= tokens.size()) {
    SetError("FILTER requires column, operator, and value");
    return false;
  }

  FilterCondition filter;
  filter.column = tokens[pos++];

  auto filter_op = ParseFilterOp(tokens[pos++]);
  if (!filter_op.has_value()) {
    SetError("Invalid filter operator: " + tokens[pos - 1]);
    return false;
  }
  filter.op = filter_op.value();

  filter.value = tokens[pos++];

  query.filters.push_back(filter);
  return true;
}

bool QueryParser::ParseLimit(const std::vector<std::string>& tokens, size_t& pos, Query& query) {
  // LIMIT <n>
  pos++;  // Skip "LIMIT"

  if (pos >= tokens.size()) {
    SetError("LIMIT requires a number");
    return false;
  }

  try {
    int limit = std::stoi(tokens[pos++]);
    if (limit <= 0) {
      SetError("LIMIT must be positive");
      return false;
    }
    query.limit = static_cast<uint32_t>(limit);
  } catch (const std::exception& e) {
    SetError("Invalid LIMIT value: " + tokens[pos - 1]);
    return false;
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
  } catch (const std::exception& e) {
    SetError("Invalid OFFSET value: " + tokens[pos - 1]);
    return false;
  }

  return true;
}

bool QueryParser::ParseOrderBy(const std::vector<std::string>& tokens, size_t& pos, Query& query) {
  // ORDER BY <column> [ASC|DESC]
  // ORDER BY ASC/DESC (shorthand for primary key)
  // ORDER ASC/DESC (shorthand, BY is optional)
  pos++;  // Skip "ORDER"

  if (pos >= tokens.size()) {
    SetError("ORDER requires BY or ASC/DESC");
    return false;
  }

  OrderByClause order_by;
  std::string next_token = ToUpper(tokens[pos]);

  // Check for shorthand: ORDER ASC/DESC (without BY)
  if (next_token == "ASC" || next_token == "DESC") {
    // Shorthand for primary key ordering
    order_by.column = "";  // Empty = primary key
    order_by.order = (next_token == "ASC") ? SortOrder::ASC : SortOrder::DESC;
    pos++;
    query.order_by = order_by;
    return true;
  }

  // Expect BY keyword
  if (next_token != "BY") {
    SetError("Expected BY or ASC/DESC after ORDER, got: " + next_token);
    return false;
  }
  pos++;  // Skip "BY"

  if (pos >= tokens.size()) {
    SetError("ORDER BY requires a column name or ASC/DESC");
    return false;
  }

  // Check if next token is ASC/DESC (shorthand for primary key)
  next_token = ToUpper(tokens[pos]);
  if (next_token == "ASC" || next_token == "DESC") {
    // ORDER BY ASC/DESC (shorthand for primary key)
    order_by.column = "";  // Empty = primary key
    order_by.order = (next_token == "ASC") ? SortOrder::ASC : SortOrder::DESC;
    pos++;
    query.order_by = order_by;
    return true;
  }

  // Normal case: ORDER BY <column> [ASC|DESC]
  order_by.column = tokens[pos++];

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

  query.order_by = order_by;
  return true;
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity)
std::vector<std::string> QueryParser::Tokenize(const std::string& str) {
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

std::optional<FilterOp> QueryParser::ParseFilterOp(const std::string& op_str) {
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

}  // namespace query
}  // namespace mygramdb
