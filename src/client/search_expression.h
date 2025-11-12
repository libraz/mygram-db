/**
 * @file search_expression.h
 * @brief Web-style search expression parser (+/- syntax)
 *
 * Converts web-style search expressions into MygramDB query format.
 *
 * Syntax:
 * - `+term` - Required term (must appear)
 * - `-term` - Excluded term (must not appear)
 * - `term1 term2` - Multiple terms (implicit AND)
 * - `"phrase"` - Quoted phrase (exact match with spaces)
 * - `(expr)` - Grouping
 * - `OR` - Logical OR between terms
 *
 * Examples:
 * - `golang tutorial` → `golang AND tutorial` (implicit AND)
 * - `"machine learning" tutorial` → `"machine learning" AND tutorial` (phrase search)
 * - `golang -old` → `golang AND NOT old`
 * - `python OR ruby` → `(python OR ruby)`
 * - `golang +(tutorial OR guide)` → `golang AND (tutorial OR guide)`
 * - `機械学習　チュートリアル` → `機械学習 AND チュートリアル` (full-width space)
 */

#pragma once

#include <optional>
#include <string>
#include <variant>
#include <vector>

namespace mygramdb::client {

/**
 * @brief Parsed search expression components
 */
struct SearchExpression {
  std::vector<std::string> required_terms;  // Terms with + prefix (AND)
  std::vector<std::string> excluded_terms;  // Terms with - prefix (NOT)
  std::vector<std::string> optional_terms;  // Terms without prefix
  std::string raw_expression;               // Original expression for OR/grouping

  /**
   * @brief Check if expression has OR operators or grouping
   */
  [[nodiscard]] bool HasComplexExpression() const;

  /**
   * @brief Convert to query string for QueryASTParser
   *
   * Generates proper boolean query string:
   * - Required terms: joined with AND
   * - Excluded terms: prefixed with NOT
   * - Optional terms: joined with OR (if no required terms)
   *
   * @return Query string compatible with QueryASTParser
   */
  [[nodiscard]] std::string ToQueryString() const;
};

/**
 * @brief Parse web-style search expression
 *
 * Converts expressions like "+golang -old (tutorial OR guide)" into
 * structured format that can be converted to QueryAST.
 *
 * Operator precedence (highest to lowest):
 * 1. Parentheses `()`
 * 2. Prefix operators `+` and `-`
 * 3. Implicit AND (space between required terms)
 * 4. `OR` operator
 *
 * Examples:
 * ```cpp
 * auto expr = ParseSearchExpression("+golang tutorial");
 * // expr.required_terms = ["golang"]
 * // expr.optional_terms = ["tutorial"]
 *
 * auto expr = ParseSearchExpression("+golang +(tutorial OR guide) -old");
 * // expr.required_terms = ["golang"]
 * // expr.excluded_terms = ["old"]
 * // expr.raw_expression = "+(tutorial OR guide)"
 * ```
 *
 * @param expression Web-style search expression
 * @return Parsed expression components, or error message
 */
std::variant<SearchExpression, std::string> ParseSearchExpression(const std::string& expression);

/**
 * @brief Convert search expression directly to QueryAST-compatible string
 *
 * This is a convenience function that combines ParseSearchExpression
 * and ToQueryString() in one call.
 *
 * Examples:
 * - `+golang tutorial` → `golang AND (tutorial)`
 * - `+golang -old` → `golang AND NOT old`
 * - `python OR ruby` → `python OR ruby`
 * - `+golang +(tutorial OR guide)` → `golang AND (tutorial OR guide)`
 *
 * @param expression Web-style search expression
 * @return QueryAST-compatible query string, or error message
 */
std::variant<std::string, std::string> ConvertSearchExpression(const std::string& expression);

/**
 * @brief Simplify search expression to basic terms (for backward compatibility)
 *
 * For clients that don't support QueryAST, this extracts simple term lists.
 * Complex expressions with OR/grouping will lose semantic meaning.
 *
 * @param expression Web-style search expression
 * @param main_term Output: first required term (or first term if none required)
 * @param and_terms Output: additional required terms
 * @param not_terms Output: excluded terms
 * @return true on success, false on error (check error message)
 */
bool SimplifySearchExpression(const std::string& expression, std::string& main_term,
                              std::vector<std::string>& and_terms, std::vector<std::string>& not_terms);

}  // namespace mygramdb::client
