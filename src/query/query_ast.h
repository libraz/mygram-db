/**
 * @file query_ast.h
 * @brief Abstract Syntax Tree for boolean query expressions
 */

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace mygramdb {

// Forward declarations
namespace index {
class Index;
using DocId = uint32_t;
}  // namespace index

namespace storage {
class DocumentStore;
}  // namespace storage

namespace query {

/**
 * @brief AST node type
 */
enum class NodeType {
  AND,  // Logical AND (intersection)
  OR,   // Logical OR (union)
  NOT,  // Logical NOT (complement)
  TERM  // Search term (leaf node)
};

/**
 * @brief Query AST node
 *
 * Represents a node in the boolean query expression tree.
 * Operator precedence: NOT > AND > OR
 *
 * Grammar (BNF):
 *   query     → or_expr
 *   or_expr   → and_expr (OR and_expr)*
 *   and_expr  → not_expr (AND not_expr)*
 *   not_expr  → NOT not_expr | primary
 *   primary   → TERM | '(' or_expr ')'
 */
struct QueryNode {
  NodeType type;
  std::string term;  // For TERM type only
  std::vector<std::unique_ptr<QueryNode>> children;

  /**
   * @brief Construct term node
   */
  explicit QueryNode(std::string term_value) : type(NodeType::TERM), term(std::move(term_value)) {}

  /**
   * @brief Construct operator node
   */
  explicit QueryNode(NodeType node_type) : type(node_type) {}

  /**
   * @brief Get string representation for debugging
   */
  [[nodiscard]] std::string ToString() const;

  /**
   * @brief Evaluate AST node and return matching document IDs
   *
   * @param index The inverted index to search
   * @param doc_store Document store for NOT operations (to get all docs)
   * @return Vector of document IDs matching the query
   */
  [[nodiscard]] std::vector<index::DocId> Evaluate(const index::Index& index,
                                                   const storage::DocumentStore& doc_store) const;
};

/**
 * @brief Token type for lexical analysis
 */
enum class TokenType {
  AND,     // AND keyword
  OR,      // OR keyword
  NOT,     // NOT keyword
  LPAREN,  // Left parenthesis '('
  RPAREN,  // Right parenthesis ')'
  TERM,    // Search term (quoted or unquoted)
  END      // End of input
};

/**
 * @brief Token for lexical analysis
 */
struct Token {
  TokenType type;
  std::string value;  // For TERM type

  Token(TokenType token_type, std::string token_value = "")
      : type(token_type), value(std::move(token_value)) {}
};

/**
 * @brief Tokenizer for query expressions
 *
 * Converts input string into sequence of tokens.
 * Handles quoted strings, parentheses, and boolean operators.
 */
class Tokenizer {
 public:
  explicit Tokenizer(std::string input);

  /**
   * @brief Get all tokens from input
   * @return Vector of tokens, or empty on error
   */
  [[nodiscard]] std::vector<Token> Tokenize();

  /**
   * @brief Get last error message
   */
  [[nodiscard]] const std::string& GetError() const { return error_; }

 private:
  std::string input_;
  size_t pos_ = 0;
  std::string error_;

  /**
   * @brief Skip whitespace characters
   */
  void SkipWhitespace();

  /**
   * @brief Read quoted string
   */
  std::string ReadQuotedString(char quote_char);

  /**
   * @brief Read unquoted term
   */
  std::string ReadTerm();

  /**
   * @brief Check if character is valid in unquoted term
   */
  static bool IsTermChar(char character);

  /**
   * @brief Set error message
   */
  void SetError(const std::string& msg) { error_ = msg; }
};

/**
 * @brief Recursive descent parser for boolean query expressions
 *
 * Builds AST from token sequence with proper operator precedence:
 * - OR has lowest precedence
 * - AND has medium precedence
 * - NOT has highest precedence
 * - Parentheses override precedence
 */
class QueryASTParser {
 public:
  /**
   * @brief Parse query string into AST
   * @param query_str Input query string
   * @return Root node of AST, or nullptr on error
   */
  [[nodiscard]] std::unique_ptr<QueryNode> Parse(const std::string& query_str);

  /**
   * @brief Get last error message
   */
  [[nodiscard]] const std::string& GetError() const { return error_; }

 private:
  std::vector<Token> tokens_;
  size_t pos_ = 0;
  std::string error_;

  /**
   * @brief Get current token
   */
  [[nodiscard]] const Token& CurrentToken() const;

  /**
   * @brief Advance to next token
   */
  void Advance();

  /**
   * @brief Check if current token matches type
   */
  [[nodiscard]] bool Match(TokenType type) const;

  /**
   * @brief Consume token if it matches type
   * @return true if consumed
   */
  bool Consume(TokenType type);

  /**
   * @brief Expect token of given type, or set error
   * @return true if matched
   */
  bool Expect(TokenType type, const std::string& error_msg);

  /**
   * @brief Parse OR expression (lowest precedence)
   */
  std::unique_ptr<QueryNode> ParseOrExpr();

  /**
   * @brief Parse AND expression (medium precedence)
   */
  std::unique_ptr<QueryNode> ParseAndExpr();

  /**
   * @brief Parse NOT expression (highest precedence)
   */
  std::unique_ptr<QueryNode> ParseNotExpr();

  /**
   * @brief Parse primary expression (term or parenthesized expression)
   */
  std::unique_ptr<QueryNode> ParsePrimary();

  /**
   * @brief Set error message
   */
  void SetError(const std::string& msg) { error_ = msg; }
};

}  // namespace query
}  // namespace mygramdb
