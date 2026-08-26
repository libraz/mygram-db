/**
 * @file sql_utils.h
 * @brief SQL string utility functions for parsing and normalization
 */

#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

#include "utils/error.h"
#include "utils/expected.h"
#include "utils/namespace_compat.h"

namespace mygramdb::utils {

/**
 * @brief Backtick-quote a MySQL identifier.
 *
 * Embedded backticks are escaped by doubling them. MySQL permits a broad
 * range of characters in quoted identifiers, so only values that cannot form
 * a valid identifier string (empty or containing NUL) are rejected.
 *
 * @param identifier Identifier to quote.
 * @return Quoted identifier, or kInvalidArgument for an invalid identifier.
 */
Expected<std::string, Error> QuoteSQLIdentifier(std::string_view identifier);

/**
 * @brief Backtick-quote a possibly database-qualified MySQL identifier.
 *
 * When database is empty, only identifier is quoted. Otherwise both
 * components are quoted independently and joined with a period.
 *
 * @param database Optional database/schema name.
 * @param identifier Table or other identifier within the database.
 * @return Quoted qualified identifier, or kInvalidArgument if either supplied
 *         component is empty or contains NUL.
 */
Expected<std::string, Error> QuoteQualifiedSQLIdentifier(std::string_view database, std::string_view identifier);

/**
 * @brief Encode a UTF-8 string as a MySQL string expression.
 *
 * Hex encoding avoids backslash and quote interpretation entirely, so the
 * expression has identical semantics with and without NO_BACKSLASH_ESCAPES.
 *
 * @param value UTF-8 string value.
 * @return `_utf8mb4 X'...'` character-set-introduced hex literal.
 */
std::string EncodeMySQLStringLiteral(std::string_view value);

/**
 * @brief Render seconds since midnight in MySQL's TIME text format.
 *
 * MySQL reads a bare number compared against a TIME column as packed HHMMSS
 * rather than as seconds, so a filter value held as seconds has to be written
 * back as a clock string before it becomes a comparison.
 *
 * @param seconds Signed seconds since midnight.
 * @return "[-]HH:MM:SS", or std::nullopt outside MySQL's TIME range of
 *         +/-838:59:59, which no TIME column can hold.
 */
std::optional<std::string> FormatMySQLTimeLiteral(int64_t seconds);

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
