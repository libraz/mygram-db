/**
 * @file search_expression.cpp
 * @brief Web-style search expression parser implementation
 */

#include "client/search_expression.h"

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <optional>
#include <sstream>

#include "utils/error.h"
#include "utils/expected.h"

using namespace mygram::utils;

namespace mygramdb::client {

namespace {

// UTF-8 byte sequence for full-width space (U+3000)
constexpr unsigned char kFullWidthSpaceByte1 = 0xE3;
constexpr unsigned char kFullWidthSpaceByte2 = 0x80;
constexpr unsigned char kFullWidthSpaceByte3 = 0x80;

/**
 * @brief Check if character sequence is full-width space (U+3000)
 */
inline bool IsFullWidthSpace(const std::string& str, size_t pos) {
  if (pos + 2 >= str.size()) {
    return false;
  }
  // Full-width space in UTF-8: 0xE3 0x80 0x80
  return static_cast<unsigned char>(str[pos]) == kFullWidthSpaceByte1 &&
         static_cast<unsigned char>(str[pos + 1]) == kFullWidthSpaceByte2 &&
         static_cast<unsigned char>(str[pos + 2]) == kFullWidthSpaceByte3;
}

/**
 * @brief Token types for lexical analysis
 */
enum class TokenType : uint8_t {
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

  Token(TokenType token_type, std::string token_value = "") : type(token_type), value(std::move(token_value)) {}
};

/**
 * @brief Simple tokenizer for search expressions
 */
class Tokenizer {
 public:
  explicit Tokenizer(const std::string& input) : input_(input) {}

  /**
   * @brief Get next token
   */
  Token Next() {
    SkipWhitespace();

    if (pos_ >= input_.size()) {
      return {TokenType::kEnd};
    }

    char current_char = input_[pos_];

    // Quoted string
    if (current_char == '"') {
      std::string quoted = ReadQuotedString();
      return {TokenType::kQuotedTerm, quoted};
    }

    // Single-character tokens
    if (current_char == '+') {
      ++pos_;
      return {TokenType::kPlus};
    }
    if (current_char == '-') {
      ++pos_;
      return {TokenType::kMinus};
    }
    if (current_char == '(') {
      ++pos_;
      return {TokenType::kLParen};
    }
    if (current_char == ')') {
      ++pos_;
      return {TokenType::kRParen};
    }

    // Check for OR operator
    if (pos_ + 2 <= input_.size()) {
      std::string maybe_or = input_.substr(pos_, 2);
      if (maybe_or == "OR") {
        // Make sure it's a whole word (not part of another word)
        bool is_whole_word = true;
        if (pos_ > 0 && std::isalnum(static_cast<unsigned char>(input_[pos_ - 1])) != 0) {
          is_whole_word = false;
        }
        if (pos_ + 2 < input_.size() && std::isalnum(static_cast<unsigned char>(input_[pos_ + 2])) != 0) {
          is_whole_word = false;
        }
        if (is_whole_word) {
          pos_ += 2;
          return {TokenType::kOr, "OR"};
        }
      }
    }

    // Term (everything else)
    return {TokenType::kTerm, ReadTerm()};
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
      if (std::isspace(static_cast<unsigned char>(input_[pos_])) != 0) {
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
      char current_char = input_[pos_];
      // Stop at whitespace or special characters
      if (std::isspace(static_cast<unsigned char>(current_char)) != 0 || current_char == '+' || current_char == '-' ||
          current_char == '(' || current_char == ')' || current_char == '"') {
        break;
      }
      term += current_char;
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
      char current_char = input_[pos_];
      if (current_char == '"') {
        ++pos_;  // Skip closing quote
        return term;
      }
      if (current_char == '\\' && pos_ + 1 < input_.size()) {
        // Handle escaped characters
        ++pos_;
        term += input_[pos_];
        ++pos_;
      } else {
        term += current_char;
        ++pos_;
      }
    }
    // Unclosed quote - return what we have
    return term;
  }

  const std::string& input_;  // NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members) - Necessary for parser
  size_t pos_ = 0;
};

/**
 * @brief Recursive descent parser for search expressions
 */
class Parser {
 public:
  explicit Parser(const std::string& input) : tokenizer_(input), current_(TokenType::kEnd) { Advance(); }

  /**
   * @brief Parse the expression
   */
  Expected<SearchExpression, Error> Parse() {
    SearchExpression expr;

    while (current_.type != TokenType::kEnd) {
      // Handle prefix operators
      if (current_.type == TokenType::kPlus) {
        Advance();
        if (auto term = ParsePrefixedTerm()) {
          expr.required_terms.push_back(*term);
        } else {
          return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, "Expected term after '+'"));
        }
      } else if (current_.type == TokenType::kMinus) {
        Advance();
        if (auto term = ParsePrefixedTerm()) {
          expr.excluded_terms.push_back(*term);
        } else {
          return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, "Expected term after '-'"));
        }
      } else if (current_.type == TokenType::kLParen) {
        // Parenthesized expression - capture as raw
        std::string paren_expr = CaptureParenExpression();
        if (paren_expr.empty()) {
          return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, "Unbalanced parentheses"));
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
            term.insert(0, "\"");
            term += "\"";
          }
          expr.required_terms.push_back(term);
          Advance();
        }
      } else if (current_.type == TokenType::kOr) {
        return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, "Unexpected 'OR' operator"));
      } else if (current_.type == TokenType::kRParen) {
        return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, "Unexpected ')'"));
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

    // NOLINTNEXTLINE(cppcoreguidelines-avoid-do-while) - do-while is appropriate for paren matching
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
  size_t last_pos_ = 0;
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

  return std::any_of(required_terms.begin(), required_terms.end(), has_or_or_parens) ||
         std::any_of(excluded_terms.begin(), excluded_terms.end(), has_or_or_parens) ||
         std::any_of(optional_terms.begin(), optional_terms.end(), has_or_or_parens);
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

Expected<SearchExpression, Error> ParseSearchExpression(const std::string& expression) {
  if (expression.empty()) {
    return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, "Empty search expression"));
  }

  Parser parser(expression);
  return parser.Parse();
}

Expected<std::string, Error> ConvertSearchExpression(const std::string& expression) {
  auto result = ParseSearchExpression(expression);
  if (!result) {
    return MakeUnexpected(result.error());
  }
  return result->ToQueryString();
}

bool SimplifySearchExpression(const std::string& expression, std::string& main_term,
                              std::vector<std::string>& and_terms, std::vector<std::string>& not_terms) {
  auto result = ParseSearchExpression(expression);
  if (!result) {
    return false;
  }

  auto& expr = *result;

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

}  // namespace mygramdb::client
