/**
 * @file query_parser.cpp
 * @brief Query parser implementation - main dispatcher, tokenizer, and validation
 */

#include "query/query_parser.h"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <cctype>
#include <sstream>

#include "query/query_parser_internal.h"
#include "utils/string_utils.h"

namespace mygramdb::query {

namespace {

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

using internal::EqualsIgnoreCase;
using mygram::utils::ToUpper;

}  // namespace

std::pair<int, int> detail::CountParensInToken(const std::string& token) {
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
}

bool Query::IsValid() const {
  if (type == QueryType::UNKNOWN) {
    return false;
  }

  // Check if this query type requires a table name
  // Commands that do NOT require a table are listed; all others require one
  bool requires_table = true;
  switch (type) {
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
      requires_table = false;
      break;
    default:
      break;
  }

  if (requires_table && table.empty()) {
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
    if (limit == 0 || limit > internal::kMaxLimit) {
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
        if (token.empty()) {
          continue;  // Skip empty tokens (e.g., from empty quoted strings)
        }
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

      // Outside quotes, split on whitespace (ASCII and Unicode spaces)
      // NOLINTBEGIN(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
      bool is_whitespace = (std::isspace(character) != 0);
      auto ubyte = static_cast<unsigned char>(character);
      size_t extra_bytes = 0;
      if (!is_whitespace && i + 1 < str.length()) {
        auto next1 = static_cast<unsigned char>(str[i + 1]);
        // U+00A0 (No-Break Space): 0xC2 0xA0
        if (ubyte == 0xC2 && next1 == 0xA0) {
          is_whitespace = true;
          extra_bytes = 1;
        }
        // 3-byte Unicode spaces
        else if (i + 2 < str.length()) {
          auto next2 = static_cast<unsigned char>(str[i + 2]);
          // U+1680 (Ogham Space Mark): 0xE1 0x9A 0x80
          if (ubyte == 0xE1 && next1 == 0x9A && next2 == 0x80) {
            is_whitespace = true;
            extra_bytes = 2;
          }
          // U+2000-U+200B: 0xE2 0x80 0x80-0x8B
          else if (ubyte == 0xE2 && next1 == 0x80 && next2 >= 0x80 && next2 <= 0x8B) {
            is_whitespace = true;
            extra_bytes = 2;
          }
          // U+2028/U+2029: 0xE2 0x80 0xA8/0xA9
          else if (ubyte == 0xE2 && next1 == 0x80 && (next2 == 0xA8 || next2 == 0xA9)) {
            is_whitespace = true;
            extra_bytes = 2;
          }
          // U+202F (Narrow No-Break Space): 0xE2 0x80 0xAF
          else if (ubyte == 0xE2 && next1 == 0x80 && next2 == 0xAF) {
            is_whitespace = true;
            extra_bytes = 2;
          }
          // U+205F (Medium Mathematical Space): 0xE2 0x81 0x9F
          else if (ubyte == 0xE2 && next1 == 0x81 && next2 == 0x9F) {
            is_whitespace = true;
            extra_bytes = 2;
          }
          // U+3000 (Ideographic Space): 0xE3 0x80 0x80
          else if (ubyte == 0xE3 && next1 == 0x80 && next2 == 0x80) {
            is_whitespace = true;
            extra_bytes = 2;
          }
        }
      }
      // NOLINTEND(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
      if (is_whitespace) {
        if (!token.empty()) {
          tokens.push_back(token);
          token.clear();
        }
        i += extra_bytes;  // Skip extra bytes of multi-byte space
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

}  // namespace mygramdb::query
