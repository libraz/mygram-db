/**
 * @file search_expression.cpp
 * @brief Web-style search expression parser implementation
 */

#include "search_expression.h"

#include <algorithm>
#include <cctype>
#include <sstream>

namespace mygramdb {
namespace client {

namespace {

/**
 * @brief Check if character sequence is full-width space (U+3000)
 */
inline bool IsFullWidthSpace(const std::string& str, size_t pos) {
  if (pos + 2 >= str.size()) {
    return false;
  }
  // Full-width space in UTF-8: 0xE3 0x80 0x80
  return static_cast<unsigned char>(str[pos]) == 0xE3 &&
         static_cast<unsigned char>(str[pos + 1]) == 0x80 &&
         static_cast<unsigned char>(str[pos + 2]) == 0x80;
}

/**
 * @brief Token types for lexical analysis
 */
enum class TokenType {
  kTerm,        // Regular term
  kQuotedTerm,  // "quoted phrase"
  kPlus,        // + prefix
  kMinus,       // - prefix
  kOr,          // OR operator
  kLParen,      // (
  kRParen,      // )
  kEnd          // End of input
};

/**
 * @brief Token structure
 */
struct Token {
  TokenType type;
  std::string value;

  Token(TokenType t, std::string v = "") : type(t), value(std::move(v)) {}
};

/**
 * @brief Simple tokenizer for search expressions
 */
class Tokenizer {
 public:
  explicit Tokenizer(const std::string& input) : input_(input), pos_(0) {}

  /**
   * @brief Get next token
   */
  Token Next() {
    SkipWhitespace();

    if (pos_ >= input_.size()) {
      return Token(TokenType::kEnd);
    }

    char ch = input_[pos_];

    // Quoted string
    if (ch == '"') {
      std::string quoted = ReadQuotedString();
      return Token(TokenType::kQuotedTerm, quoted);
    }

    // Single-character tokens
    if (ch == '+') {
      ++pos_;
      return Token(TokenType::kPlus);
    }
    if (ch == '-') {
      ++pos_;
      return Token(TokenType::kMinus);
    }
    if (ch == '(') {
      ++pos_;
      return Token(TokenType::kLParen);
    }
    if (ch == ')') {
      ++pos_;
      return Token(TokenType::kRParen);
    }

    // Check for OR operator
    if (pos_ + 2 <= input_.size()) {
      std::string maybe_or = input_.substr(pos_, 2);
      if (maybe_or == "OR") {
        // Make sure it's a whole word (not part of another word)
        bool is_whole_word = true;
        if (pos_ > 0 && std::isalnum(static_cast<unsigned char>(input_[pos_ - 1]))) {
          is_whole_word = false;
        }
        if (pos_ + 2 < input_.size() &&
            std::isalnum(static_cast<unsigned char>(input_[pos_ + 2]))) {
          is_whole_word = false;
        }
        if (is_whole_word) {
          pos_ += 2;
          return Token(TokenType::kOr, "OR");
        }
      }
    }

    // Term (everything else)
    return Token(TokenType::kTerm, ReadTerm());
  }

  [[nodiscard]] size_t GetPosition() const { return pos_; }

  void SetPosition(size_t pos) { pos_ = pos; }

 private:
  void SkipWhitespace() {
    while (pos_ < input_.size()) {
      // Check for full-width space (3 bytes)
      if (IsFullWidthSpace(input_, pos_)) {
        pos_ += 3;
        continue;
      }
      // Check for ASCII whitespace
      if (std::isspace(static_cast<unsigned char>(input_[pos_]))) {
        ++pos_;
        continue;
      }
      break;
    }
  }

  std::string ReadTerm() {
    std::string term;
    while (pos_ < input_.size()) {
      // Stop at full-width space
      if (IsFullWidthSpace(input_, pos_)) {
        break;
      }
      char ch = input_[pos_];
      // Stop at whitespace or special characters
      if (std::isspace(static_cast<unsigned char>(ch)) || ch == '+' || ch == '-' || ch == '(' ||
          ch == ')' || ch == '"') {
        break;
      }
      term += ch;
      ++pos_;
    }
    return term;
  }

  std::string ReadQuotedString() {
    if (pos_ >= input_.size() || input_[pos_] != '"') {
      return "";
    }
    ++pos_;  // Skip opening quote

    std::string term;
    while (pos_ < input_.size()) {
      char ch = input_[pos_];
      if (ch == '"') {
        ++pos_;  // Skip closing quote
        return term;
      }
      if (ch == '\\' && pos_ + 1 < input_.size()) {
        // Handle escaped characters
        ++pos_;
        term += input_[pos_];
        ++pos_;
      } else {
        term += ch;
        ++pos_;
      }
    }
    // Unclosed quote - return what we have
    return term;
  }

  const std::string& input_;
  size_t pos_;
};

/**
 * @brief Recursive descent parser for search expressions
 */
class Parser {
 public:
  explicit Parser(const std::string& input)
      : tokenizer_(input), current_(TokenType::kEnd), last_pos_(0) {
    Advance();
  }

  /**
   * @brief Parse the expression
   */
  std::variant<SearchExpression, std::string> Parse() {
    SearchExpression expr;

    while (current_.type != TokenType::kEnd) {
      // Handle prefix operators
      if (current_.type == TokenType::kPlus) {
        Advance();
        if (auto term = ParsePrefixedTerm()) {
          expr.required_terms.push_back(*term);
        } else {
          return "Expected term after '+'";
        }
      } else if (current_.type == TokenType::kMinus) {
        Advance();
        if (auto term = ParsePrefixedTerm()) {
          expr.excluded_terms.push_back(*term);
        } else {
          return "Expected term after '-'";
        }
      } else if (current_.type == TokenType::kLParen) {
        // Parenthesized expression - capture as raw
        std::string paren_expr = CaptureParenExpression();
        if (paren_expr.empty()) {
          return "Unbalanced parentheses";
        }
        if (!expr.raw_expression.empty()) {
          expr.raw_expression += " ";
        }
        expr.raw_expression += paren_expr;
      } else if (current_.type == TokenType::kTerm || current_.type == TokenType::kQuotedTerm) {
        // Check if this starts an OR expression
        if (LooksLikeOrExpression()) {
          std::string or_expr = CaptureOrExpression();
          if (!expr.raw_expression.empty()) {
            expr.raw_expression += " ";
          }
          expr.raw_expression += or_expr;
        } else {
          // Regular term (implicit AND) - add quotes if it was a quoted term
          std::string term = current_.value;
          if (current_.type == TokenType::kQuotedTerm) {
            term = "\"" + term + "\"";
          }
          expr.required_terms.push_back(term);
          Advance();
        }
      } else if (current_.type == TokenType::kOr) {
        return "Unexpected 'OR' operator";
      } else if (current_.type == TokenType::kRParen) {
        return "Unexpected ')'";
      } else {
        Advance();
      }
    }

    return expr;
  }

 private:
  void Advance() {
    last_pos_ = tokenizer_.GetPosition();
    current_ = tokenizer_.Next();
  }

  std::optional<std::string> ParsePrefixedTerm() {
    if (current_.type == TokenType::kLParen) {
      // Parenthesized expression after + or -
      std::string expr = CaptureParenExpression();
      return expr.empty() ? std::nullopt : std::optional<std::string>(expr);
    }
    if (current_.type == TokenType::kTerm) {
      std::string term = current_.value;
      Advance();
      return term;
    }
    if (current_.type == TokenType::kQuotedTerm) {
      std::string term = "\"" + current_.value + "\"";
      Advance();
      return term;
    }
    return std::nullopt;
  }

  bool LooksLikeOrExpression() {
    // Save current state
    size_t saved_pos = tokenizer_.GetPosition();
    Token saved_current = current_;

    // Look ahead for OR
    Advance();  // Skip current term
    bool has_or = (current_.type == TokenType::kOr);

    // Restore state
    tokenizer_.SetPosition(saved_pos);
    current_ = saved_current;

    return has_or;
  }

  std::string CaptureOrExpression() {
    std::ostringstream oss;

    // Capture first term
    if (current_.type == TokenType::kQuotedTerm) {
      oss << "\"" << current_.value << "\"";
    } else {
      oss << current_.value;
    }
    Advance();

    // Capture OR chain
    while (current_.type == TokenType::kOr) {
      oss << " OR ";
      Advance();
      if (current_.type == TokenType::kTerm) {
        oss << current_.value;
        Advance();
      } else if (current_.type == TokenType::kQuotedTerm) {
        oss << "\"" << current_.value << "\"";
        Advance();
      } else if (current_.type == TokenType::kLParen) {
        std::string paren = CaptureParenExpression();
        if (paren.empty()) {
          return "";  // Error
        }
        oss << paren;
      } else {
        return "";  // Error: expected term after OR
      }
    }

    return oss.str();
  }

  std::string CaptureParenExpression() {
    if (current_.type != TokenType::kLParen) {
      return "";
    }

    std::ostringstream oss;
    int depth = 0;

    do {
      if (current_.type == TokenType::kLParen) {
        ++depth;
        oss << "(";
      } else if (current_.type == TokenType::kRParen) {
        --depth;
        oss << ")";
      } else if (current_.type == TokenType::kTerm) {
        oss << current_.value;
      } else if (current_.type == TokenType::kQuotedTerm) {
        oss << "\"" << current_.value << "\"";
      } else if (current_.type == TokenType::kOr) {
        oss << " OR ";
      } else if (current_.type == TokenType::kPlus) {
        oss << "+";
      } else if (current_.type == TokenType::kMinus) {
        oss << "-";
      } else if (current_.type == TokenType::kEnd) {
        return "";  // Unbalanced
      }

      if (depth > 0) {
        Advance();
      }
    } while (depth > 0);

    Advance();  // Skip closing paren
    return oss.str();
  }

  Tokenizer tokenizer_;
  Token current_;
  size_t last_pos_;
};

}  // namespace

bool SearchExpression::HasComplexExpression() const {
  if (!raw_expression.empty()) {
    return true;
  }

  // Check if any term contains OR or parentheses
  auto has_or_or_parens = [](const std::string& term) {
    return term.find("OR") != std::string::npos || term.find('(') != std::string::npos ||
           term.find(')') != std::string::npos;
  };

  for (const auto& term : required_terms) {
    if (has_or_or_parens(term)) {
      return true;
    }
  }

  for (const auto& term : excluded_terms) {
    if (has_or_or_parens(term)) {
      return true;
    }
  }

  for (const auto& term : optional_terms) {
    if (has_or_or_parens(term)) {
      return true;
    }
  }

  return false;
}

std::string SearchExpression::ToQueryString() const {
  std::ostringstream oss;

  // Build required terms (AND) - includes all non-prefixed terms
  if (!required_terms.empty()) {
    for (size_t i = 0; i < required_terms.size(); ++i) {
      if (i > 0) {
        oss << " AND ";
      }
      oss << required_terms[i];
    }
  }

  // Add excluded terms (NOT)
  for (const auto& term : excluded_terms) {
    if (!oss.str().empty()) {
      oss << " AND ";
    }
    oss << "NOT " << term;
  }

  // Add complex expression (raw) - for OR/parentheses
  if (!raw_expression.empty()) {
    if (!oss.str().empty()) {
      oss << " AND ";
    }
    oss << "(" << raw_expression << ")";
  }

  // Note: optional_terms is no longer used (kept for backward compatibility)
  // All terms are now treated as required (implicit AND)

  return oss.str();
}

std::variant<SearchExpression, std::string> ParseSearchExpression(const std::string& expression) {
  if (expression.empty()) {
    return "Empty search expression";
  }

  Parser parser(expression);
  return parser.Parse();
}

std::variant<std::string, std::string> ConvertSearchExpression(const std::string& expression) {
  auto result = ParseSearchExpression(expression);
  if (auto* err = std::get_if<std::string>(&result)) {
    // Return error (index 1)
    return std::variant<std::string, std::string>(std::in_place_index<1>, *err);
  }
  auto expr = std::get<SearchExpression>(result);
  // Return success (index 0)
  return std::variant<std::string, std::string>(std::in_place_index<0>, expr.ToQueryString());
}

bool SimplifySearchExpression(const std::string& expression, std::string& main_term,
                              std::vector<std::string>& and_terms,
                              std::vector<std::string>& not_terms) {
  auto result = ParseSearchExpression(expression);
  if (std::get_if<std::string>(&result)) {
    return false;
  }

  auto expr = std::get<SearchExpression>(result);

  // Extract main term - all terms are now in required_terms
  if (!expr.required_terms.empty()) {
    main_term = expr.required_terms[0];
    and_terms.assign(expr.required_terms.begin() + 1, expr.required_terms.end());
  } else {
    return false;  // No terms found
  }

  not_terms = expr.excluded_terms;
  return true;
}

}  // namespace client
}  // namespace mygramdb
