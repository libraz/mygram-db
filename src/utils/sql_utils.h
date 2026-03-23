/**
 * @file sql_utils.h
 * @brief SQL string utility functions for parsing and normalization
 */

#pragma once

#include <string>

namespace mygramdb::utils {

/// @brief Remove SQL comments (/* ... */ and -- ...) from a query string.
/// @param sql SQL query string
/// @return Query with comments stripped
std::string StripSQLComments(const std::string& sql);

/// @brief Collapse consecutive whitespace characters into single spaces.
/// @param sql Input string
/// @return Normalized string
std::string NormalizeWhitespace(const std::string& sql);

/// @brief Advance position past whitespace characters.
/// @param str Input string
/// @param pos Starting position (updated to position after whitespace)
/// @return true if position is still valid after skipping
bool SkipWhitespace(const std::string& str, size_t& pos);

/// @brief Case-insensitive keyword match at the given position.
/// @param str Input string (should be uppercase)
/// @param pos Starting position (updated to position after keyword if matched)
/// @param keyword Keyword to match (should be uppercase)
/// @return true if keyword matches and is followed by whitespace or backtick
bool MatchKeyword(const std::string& str, size_t& pos, const std::string& keyword);

/// @brief Match a table name (with optional backtick quoting) at the given
/// position.
/// @param str Input string (should be uppercase)
/// @param pos Starting position (updated to position after table name if
/// matched)
/// @param table_name Table name to match (should be uppercase)
/// @return true if table_name matches at pos
bool MatchTableName(const std::string& str, size_t& pos, const std::string& table_name);

}  // namespace mygramdb::utils
