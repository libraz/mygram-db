/**
 * @file sql_utils.cpp
 * @brief SQL string utility functions for parsing and normalization
 */

#include "utils/sql_utils.h"

#include <cctype>

namespace mygramdb::utils {

std::string StripSQLComments(const std::string& query) {
  std::string result;
  result.reserve(query.length());

  size_t pos = 0;
  while (pos < query.length()) {
    // Check for block comment start
    if (pos + 1 < query.length() && query[pos] == '/' && query[pos + 1] == '*') {
      // Skip until end of block comment
      pos += 2;
      bool found_end = false;
      while (pos + 1 < query.length()) {
        if (query[pos] == '*' && query[pos + 1] == '/') {
          pos += 2;
          found_end = true;
          break;
        }
        pos++;
      }
      // If unterminated, skip remaining characters
      if (!found_end) {
        pos = query.length();
      }
      // Add a space to preserve word boundaries
      if (!result.empty() && result.back() != ' ') {
        result += ' ';
      }
      continue;
    }

    // Check for line comment start
    if (pos + 1 < query.length() && query[pos] == '-' && query[pos + 1] == '-') {
      // Skip until end of line
      pos += 2;
      while (pos < query.length() && query[pos] != '\n' && query[pos] != '\r') {
        pos++;
      }
      // Skip the newline if present
      if (pos < query.length()) {
        pos++;
      }
      continue;
    }

    result += query[pos];
    pos++;
  }

  return result;
}

std::string NormalizeWhitespace(const std::string& str) {
  std::string result;
  result.reserve(str.length());

  bool prev_was_space = false;
  for (char cur_char : str) {
    bool is_space = std::isspace(static_cast<unsigned char>(cur_char)) != 0;
    if (is_space) {
      if (!prev_was_space) {
        result += ' ';
        prev_was_space = true;
      }
    } else {
      result += cur_char;
      prev_was_space = false;
    }
  }

  return result;
}

bool SkipWhitespace(const std::string& str, size_t& pos) {
  while (pos < str.length() && std::isspace(static_cast<unsigned char>(str[pos])) != 0) {
    ++pos;
  }
  return pos < str.length();
}

bool MatchKeyword(const std::string& str, size_t& pos, const std::string& keyword) {
  // Check if there's enough space for the keyword
  if (pos + keyword.length() > str.length()) {
    return false;
  }

  // Check if keyword matches
  if (str.compare(pos, keyword.length(), keyword) != 0) {
    return false;
  }

  // Check that keyword is followed by whitespace, backtick, or end of string
  size_t next_pos = pos + keyword.length();
  if (next_pos < str.length()) {
    char next_char = str[next_pos];
    if (std::isspace(static_cast<unsigned char>(next_char)) == 0 && next_char != '`') {
      return false;
    }
  }

  pos = next_pos;
  return true;
}

bool MatchTableName(const std::string& str, size_t& pos, const std::string& table_name) {
  size_t saved_pos = pos;

  // Skip optional backtick
  bool has_backtick = false;
  if (pos < str.length() && str[pos] == '`') {
    has_backtick = true;
    ++pos;
  }

  // Match table name
  if (pos + table_name.length() > str.length()) {
    pos = saved_pos;
    return false;
  }

  if (str.compare(pos, table_name.length(), table_name) != 0) {
    pos = saved_pos;
    return false;
  }

  pos += table_name.length();

  // Skip optional closing backtick
  if (has_backtick && pos < str.length() && str[pos] == '`') {
    ++pos;
  }

  // Ensure the match is a complete word (not a prefix of a longer identifier)
  // After the table name (and optional backtick), the next character must be:
  // - End of string
  // - Whitespace
  // - Semicolon
  // - Not an identifier character (alphanumeric or underscore)
  if (pos < str.length()) {
    char next_char = str[pos];
    if (std::isalnum(static_cast<unsigned char>(next_char)) != 0 || next_char == '_') {
      pos = saved_pos;
      return false;  // Table name is a prefix of a longer identifier
    }
  }

  return true;
}

}  // namespace mygramdb::utils
