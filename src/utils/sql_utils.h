/**
 * @file sql_utils.h
 * @brief SQL identifier quoting and MySQL literal encoding
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

/**
 * @brief Render an instant in MySQL's DATETIME text format.
 *
 * The wall clock is taken in the supplied offset, so a literal built from an
 * instant reads back as the same instant when the server interprets it with
 * the offset the column's values were converted with.
 *
 * @param epoch_seconds Signed seconds since the Unix epoch (UTC).
 * @param timezone_offset_seconds Offset the wall clock is expressed in.
 * @return "YYYY-MM-DD HH:MM:SS", or std::nullopt outside MySQL's DATETIME year
 *         range of 1000-9999, which no DATETIME or DATE column can hold.
 */
std::optional<std::string> FormatMySQLDateTimeLiteral(int64_t epoch_seconds, int32_t timezone_offset_seconds);

/**
 * @brief Render a double as a MySQL approximate-value literal.
 *
 * The digit count round-trips an IEEE double exactly, and the exponent marker
 * keeps MySQL reading the literal as an approximate value rather than as an
 * exact DECIMAL, so the server compares in the same domain the caller parsed
 * the value in.
 *
 * @param value Finite double value.
 * @return Literal text in exponent form, such as "1.10000000000000009e+00".
 */
std::string FormatMySQLDoubleLiteral(double value);

}  // namespace mygramdb::utils
