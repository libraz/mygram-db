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

#include "client/wire_quoting.h"
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
 * @brief Check whether entry @p index of a term list is a formatted group
 *
 * @p is_group runs parallel to the term list (SearchExpression::
 * required_is_group / excluded_is_group); a missing flag means plain text.
 */
inline bool IsGroupEntry(const std::vector<bool>& is_group, size_t index) {
  return index < is_group.size() && is_group[index];
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

    // Check for OR operator. The server recognizes it case-insensitively
    // (query_parser_commands.cpp: ToUpper(tokens[i])=="OR"; query_ast.cpp:
    // upper_term=="OR"), so "or"/"Or" must be caught here too -- otherwise
    // the client treats it as a plain term while the server re-tokenizes
    // the same bytes as the OR operator, splitting "tom AND or AND jerry"
    // into a boolean expression the AST parser cannot parse.
    if (pos_ + 2 <= input_.size() && mygram::utils::EqualsAsciiCaseInsensitive(input_.substr(pos_, 2), "OR") &&
        IsOperatorBoundaryBefore(pos_) && IsOperatorBoundaryAfter(pos_ + 2)) {
      pos_ += 2;
      return {TokenType::kOr, "OR"};
    }

    // Term (everything else)
    return {TokenType::kTerm, ReadTerm()};
  }

  [[nodiscard]] size_t GetPosition() const { return pos_; }

  void SetPosition(size_t pos) { pos_ = pos; }

 private:
  /**
   * @brief Whether an operator word may end at @p pos: end of input, ASCII or
   *        U+3000 whitespace, a parenthesis or a double quote
   *
   * Anything else continues the word, so "OR-tools" and "OR回路" stay terms.
   */
  [[nodiscard]] bool IsOperatorBoundaryAfter(size_t pos) const {
    if (pos >= input_.size()) {
      return true;
    }
    const char current_char = input_[pos];
    return std::isspace(static_cast<unsigned char>(current_char)) != 0 || current_char == '(' || current_char == ')' ||
           current_char == '"' || IsFullWidthSpace(input_, pos);
  }

  /**
   * @brief Whether an operator word may start at @p pos (same delimiters as IsOperatorBoundaryAfter)
   */
  [[nodiscard]] bool IsOperatorBoundaryBefore(size_t pos) const {
    constexpr size_t kFullWidthSpaceBytes = 3;
    if (pos == 0) {
      return true;
    }
    const char previous_char = input_[pos - 1];
    return std::isspace(static_cast<unsigned char>(previous_char)) != 0 || previous_char == '(' ||
           previous_char == ')' || previous_char == '"' ||
           (pos >= kFullWidthSpaceBytes && IsFullWidthSpace(input_, pos - kFullWidthSpaceBytes));
  }

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
      // '+' and '-' are unary operators only when Next() sees them at a token
      // boundary. Once a term has started they are ordinary text, as in
      // "COVID-19", "e-mail", and "C++".
      if (std::isspace(static_cast<unsigned char>(current_char)) != 0 || current_char == '(' || current_char == ')' ||
          current_char == '"') {
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
        if (auto operand = ParsePrefixedTerm()) {
          expr.required_terms.push_back(operand->text);
          expr.required_is_group.push_back(operand->is_group);
        } else {
          return MakeUnexpected(MakeError(ErrorCode::kClientExpressionParseError, "Expected term after '+'"));
        }
      } else if (current_.type == TokenType::kMinus) {
        Advance();
        if (auto operand = ParsePrefixedTerm()) {
          expr.excluded_terms.push_back(operand->text);
          expr.excluded_is_group.push_back(operand->is_group);
        } else {
          return MakeUnexpected(MakeError(ErrorCode::kClientExpressionParseError, "Expected term after '-'"));
        }
      } else if (current_.type == TokenType::kLParen) {
        // Parenthesized expression - capture as raw
        std::string paren_expr = CaptureParenExpression();
        if (paren_expr.empty()) {
          return MakeUnexpected(MakeError(ErrorCode::kClientExpressionParseError, "Unbalanced parentheses"));
        }
        if (!expr.raw_expression.empty()) {
          expr.raw_expression += " ";
        }
        expr.raw_expression += paren_expr;
      } else if (current_.type == TokenType::kTerm || current_.type == TokenType::kQuotedTerm) {
        // Check if this starts an OR expression
        if (LooksLikeOrExpression()) {
          auto or_expr = CaptureOrExpression();
          if (!or_expr) {
            return MakeUnexpected(or_expr.error());
          }
          if (!expr.raw_expression.empty()) {
            expr.raw_expression += " ";
          }
          expr.raw_expression += *or_expr;
        } else {
          // Regular term (implicit AND). Stored as plain text regardless of
          // whether it was quoted -- ToQueryString/SimplifySearchExpression
          // re-apply wire quoting exactly once, at whichever destination the
          // term is serialized onto (see wire_quoting.h).
          expr.required_terms.push_back(current_.value);
          expr.required_is_group.push_back(false);
          Advance();
        }
      } else if (current_.type == TokenType::kOr) {
        return MakeUnexpected(MakeError(ErrorCode::kClientExpressionParseError, "Unexpected 'OR' operator"));
      } else if (current_.type == TokenType::kRParen) {
        return MakeUnexpected(MakeError(ErrorCode::kClientExpressionParseError, "Unexpected ')'"));
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

  /**
   * @brief Operand of a unary +/-: plain term text, or a formatted group
   */
  struct PrefixedOperand {
    std::string text;
    bool is_group = false;
  };

  std::optional<PrefixedOperand> ParsePrefixedTerm() {
    if (current_.type == TokenType::kLParen) {
      // Parenthesized expression after + or -
      std::string expr = CaptureParenExpression();
      return expr.empty() ? std::nullopt : std::optional<PrefixedOperand>(PrefixedOperand{std::move(expr), true});
    }
    if (current_.type == TokenType::kTerm || current_.type == TokenType::kQuotedTerm) {
      PrefixedOperand operand{current_.value, false};
      Advance();
      return operand;
    }
    return std::nullopt;
  }

  bool LooksLikeOrExpression() {
    // Save current state
    size_t saved_pos = tokenizer_.GetPosition();
    Token saved_current = current_;
    auto saved_last_pos = last_pos_;

    // Look ahead for OR
    Advance();  // Skip current term
    bool has_or = (current_.type == TokenType::kOr);

    // Restore state
    tokenizer_.SetPosition(saved_pos);
    current_ = saved_current;
    last_pos_ = saved_last_pos;

    return has_or;
  }

  Expected<std::string, Error> CaptureOrExpression() {
    std::ostringstream oss;

    // Capture first term. Every term (quoted or not) goes through the same
    // wire-quoting decision as ToQueryString's required/excluded terms, so a
    // plain word that collides with a reserved keyword (e.g. "order") is
    // quoted here too instead of reaching the AST parser as a bare operator.
    oss << detail::QuoteWireToken(current_.value);
    Advance();

    // Capture OR chain
    while (current_.type == TokenType::kOr) {
      oss << " OR ";
      Advance();
      if (current_.type == TokenType::kTerm || current_.type == TokenType::kQuotedTerm) {
        oss << detail::QuoteWireToken(current_.value);
        Advance();
      } else if (current_.type == TokenType::kLParen) {
        std::string paren = CaptureParenExpression();
        if (paren.empty()) {
          return MakeUnexpected(MakeError(ErrorCode::kClientExpressionParseError, "Unbalanced parentheses after 'OR'"));
        }
        oss << paren;
      } else {
        return MakeUnexpected(MakeError(ErrorCode::kClientExpressionParseError, "Expected term after 'OR'"));
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
    // Tracks the last *content* token (term/quoted-term/paren/OR) so the
    // spacing decision below is unaffected by a +/- prefix in between --
    // otherwise "tutorial -video" would see previous_type==kMinus and skip
    // the space it needs before the "NOT" it is about to emit.
    TokenType previous_type = TokenType::kEnd;
    // A bare '-' has no keyword the AST parser recognizes; the server only
    // understands the literal word NOT (query_ast.cpp). Defer emission to
    // the very next token: a term or a nested group takes the NOT, anything
    // else drops it, so it never leaks onto a later term.
    bool pending_not = false;

    // NOLINTNEXTLINE(cppcoreguidelines-avoid-do-while) - do-while is appropriate for paren matching
    do {
      if (current_.type == TokenType::kLParen) {
        if (previous_type == TokenType::kTerm || previous_type == TokenType::kQuotedTerm ||
            previous_type == TokenType::kRParen) {
          oss << " ";
        }
        if (pending_not) {
          oss << "NOT ";
          pending_not = false;
        }
        ++depth;
        oss << "(";
        previous_type = TokenType::kLParen;
      } else if (current_.type == TokenType::kRParen) {
        pending_not = false;
        --depth;
        oss << ")";
        previous_type = TokenType::kRParen;
      } else if (current_.type == TokenType::kTerm || current_.type == TokenType::kQuotedTerm) {
        if (previous_type == TokenType::kTerm || previous_type == TokenType::kQuotedTerm ||
            previous_type == TokenType::kRParen) {
          oss << " ";
        }
        if (pending_not) {
          oss << "NOT ";
          pending_not = false;
        }
        oss << detail::QuoteWireToken(current_.value);
        previous_type = current_.type;
      } else if (current_.type == TokenType::kOr) {
        pending_not = false;
        oss << " OR ";
        previous_type = TokenType::kOr;
      } else if (current_.type == TokenType::kPlus) {
        // No operator meaning inside a group -- adjacent terms are AND'ed
        // by adjacency already, so '+' is dropped rather than glued onto
        // the next term as a raw character the AST tokenizer rejects.
        pending_not = false;
      } else if (current_.type == TokenType::kMinus) {
        pending_not = true;
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
  return std::find(required_is_group.begin(), required_is_group.end(), true) != required_is_group.end() ||
         std::find(excluded_is_group.begin(), excluded_is_group.end(), true) != excluded_is_group.end();
}

std::string SearchExpression::ToQueryString() const {
  std::ostringstream oss;

  // required_terms/excluded_terms mix plain term text with entries that are
  // already a fully-formatted parenthesized sub-expression (flagged in
  // required_is_group/excluded_is_group); only the former still needs the
  // shared wire-quoting decision applied here.
  const auto emit_term = [](const std::string& term, bool is_group) {
    return is_group ? term : detail::QuoteWireToken(term);
  };

  // Build required terms (AND) - includes all non-prefixed terms
  if (!required_terms.empty()) {
    for (size_t i = 0; i < required_terms.size(); ++i) {
      if (i > 0) {
        oss << " AND ";
      }
      oss << emit_term(required_terms[i], IsGroupEntry(required_is_group, i));
    }
  }

  // Add excluded terms (NOT)
  for (size_t i = 0; i < excluded_terms.size(); ++i) {
    if (!oss.str().empty()) {
      oss << " AND ";
    }
    oss << "NOT " << emit_term(excluded_terms[i], IsGroupEntry(excluded_is_group, i));
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
    return MakeUnexpected(MakeError(ErrorCode::kClientExpressionParseError, "Empty search expression"));
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

Expected<SimplifiedExpression, Error> SimplifySearchExpression(const std::string& expression) {
  auto result = ParseSearchExpression(expression);
  if (!result) {
    return MakeUnexpected(result.error());
  }

  auto& expr = *result;

  // The legacy structured result has no way to represent an OR or
  // parenthesized sub-expression: every consumer of main_term/and_terms/
  // not_terms (Search(), mygramclient_search_advanced) re-applies wire
  // quoting to each field, which would wrap an already-formatted "(a OR b)"
  // as one opaque literal-phrase term instead of parsing it as boolean OR.
  // Refuse rather than silently changing the expression's meaning.
  if (expr.HasComplexExpression()) {
    return MakeUnexpected(MakeError(ErrorCode::kClientExpressionParseError,
                                    "Expression cannot be represented by the simplified client API"));
  }
  if (expr.required_terms.empty()) {
    return MakeUnexpected(MakeError(ErrorCode::kClientExpressionParseError, "No search terms found"));
  }

  SimplifiedExpression simplified;
  // Required terms (from + prefix or implicit AND) take priority; the first
  // becomes main_term and the rest are AND terms. Each is plain text (see
  // Parser::Parse), so the wire quoting Search()/mygramclient_search_advanced
  // apply downstream runs exactly once.
  simplified.main_term = expr.required_terms[0];
  simplified.and_terms.assign(expr.required_terms.begin() + 1, expr.required_terms.end());
  simplified.not_terms = expr.excluded_terms;
  return simplified;
}

}  // namespace mygramdb::client
