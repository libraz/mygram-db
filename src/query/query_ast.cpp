/**
 * @file query_ast.cpp
 * @brief AST implementation for boolean query expressions
 */

#include "query/query_ast.h"

#include <algorithm>
#include <cctype>
#include <set>
#include <sstream>

#include "index/index.h"
#include "storage/document_store.h"
#include "utils/string_utils.h"

namespace mygramdb {
namespace query {

namespace {

/**
 * @brief Convert string to uppercase
 */
std::string ToUpper(const std::string& str) {
  std::string result = str;
  for (auto& character : result) {
    character = static_cast<char>(std::toupper(static_cast<unsigned char>(character)));
  }
  return result;
}

}  // namespace

// ============================================================================
// QueryNode
// ============================================================================

std::string QueryNode::ToString() const {
  std::ostringstream oss;

  switch (type) {
    case NodeType::TERM:
      oss << "TERM(\"" << term << "\")";
      break;

    case NodeType::NOT:
      oss << "NOT(";
      if (!children.empty()) {
        oss << children[0]->ToString();
      }
      oss << ")";
      break;

    case NodeType::AND:
      oss << "AND(";
      for (size_t i = 0; i < children.size(); ++i) {
        if (i > 0) {
          oss << ", ";
        }
        oss << children[i]->ToString();
      }
      oss << ")";
      break;

    case NodeType::OR:
      oss << "OR(";
      for (size_t i = 0; i < children.size(); ++i) {
        if (i > 0) {
          oss << ", ";
        }
        oss << children[i]->ToString();
      }
      oss << ")";
      break;
  }

  return oss.str();
}

std::vector<index::DocId> QueryNode::Evaluate(const index::Index& index,
                                              const storage::DocumentStore& doc_store) const {
  using index::DocId;

  switch (type) {
    case NodeType::TERM: {
      // Normalize search term before searching
      std::string normalized_term = utils::NormalizeText(term);

      // Generate n-grams from the normalized term
      std::vector<std::string> ngrams;
      int ngram_size = index.GetNgramSize();
      int kanji_ngram_size = index.GetKanjiNgramSize();

      if (ngram_size == 0) {
        // Hybrid mode
        ngrams = utils::GenerateHybridNgrams(normalized_term, ngram_size, kanji_ngram_size);
      } else {
        // Regular n-gram mode
        ngrams = utils::GenerateNgrams(normalized_term, ngram_size);
      }

      // If no n-grams generated (e.g., 1-char term with ngram_size=2), return empty
      if (ngrams.empty()) {
        return {};
      }

      // Search using generated n-grams
      return index.SearchAnd(ngrams);
    }

    case NodeType::AND: {
      // Collect terms from all children and perform AND search
      std::vector<std::string> terms;
      std::vector<DocId> current_result;
      bool first = true;

      for (const auto& child : children) {
        auto child_result = child->Evaluate(index, doc_store);

        if (first) {
          current_result = std::move(child_result);
          first = false;
        } else {
          // Intersect with previous results
          std::vector<DocId> intersection;
          std::set_intersection(current_result.begin(), current_result.end(), child_result.begin(),
                                child_result.end(), std::back_inserter(intersection));
          current_result = std::move(intersection);
        }

        // Early termination if result is empty
        if (current_result.empty()) {
          break;
        }
      }

      return current_result;
    }

    case NodeType::OR: {
      // Union of all children's results
      std::set<DocId> result_set;

      for (const auto& child : children) {
        auto child_result = child->Evaluate(index, doc_store);
        result_set.insert(child_result.begin(), child_result.end());
      }

      std::vector<DocId> result(result_set.begin(), result_set.end());
      std::sort(result.begin(), result.end());
      return result;
    }

    case NodeType::NOT: {
      if (children.empty()) {
        return {};
      }

      // Get all document IDs
      auto all_docs = doc_store.GetAllDocIds();

      // Get documents matching the child expression
      auto exclude_docs = children[0]->Evaluate(index, doc_store);

      // Return complement (all_docs - exclude_docs)
      std::vector<DocId> result;
      std::set_difference(all_docs.begin(), all_docs.end(), exclude_docs.begin(),
                          exclude_docs.end(), std::back_inserter(result));

      return result;
    }
  }

  return {};
}

// ============================================================================
// Tokenizer
// ============================================================================

Tokenizer::Tokenizer(std::string input) : input_(std::move(input)) {}

void Tokenizer::SkipWhitespace() {
  while (pos_ < input_.size() && (std::isspace(static_cast<unsigned char>(input_[pos_])) != 0)) {
    pos_++;
  }
}

bool Tokenizer::IsTermChar(char character) {
  // Allow alphanumeric, underscore, and non-ASCII characters
  return std::isalnum(static_cast<unsigned char>(character)) != 0 || character == '_' ||
         static_cast<unsigned char>(character) > 127;
}

std::string Tokenizer::ReadQuotedString(char quote_char) {
  std::string result;
  pos_++;  // Skip opening quote

  bool escape_next = false;
  while (pos_ < input_.size()) {
    char character = input_[pos_];

    if (escape_next) {
      // Handle escape sequences
      switch (character) {
        case 'n':
          result += '\n';
          break;
        case 't':
          result += '\t';
          break;
        case 'r':
          result += '\r';
          break;
        case '\\':
          result += '\\';
          break;
        case '"':
          result += '"';
          break;
        case '\'':
          result += '\'';
          break;
        default:
          result += character;
          break;
      }
      escape_next = false;
      pos_++;
      continue;
    }

    if (character == '\\') {
      escape_next = true;
      pos_++;
      continue;
    }

    if (character == quote_char) {
      pos_++;  // Skip closing quote
      return result;
    }

    result += character;
    pos_++;
  }

  // Unclosed quote
  SetError(std::string("Unclosed quote: ") + quote_char);
  return "";
}

std::string Tokenizer::ReadTerm() {
  std::string result;
  while (pos_ < input_.size() && IsTermChar(input_[pos_])) {
    result += input_[pos_++];
  }
  return result;
}

std::vector<Token> Tokenizer::Tokenize() {
  std::vector<Token> tokens;
  error_.clear();

  while (pos_ < input_.size()) {
    SkipWhitespace();

    if (pos_ >= input_.size()) {
      break;
    }

    char character = input_[pos_];

    // Parentheses
    if (character == '(') {
      tokens.emplace_back(TokenType::LPAREN);
      pos_++;
      continue;
    }

    if (character == ')') {
      tokens.emplace_back(TokenType::RPAREN);
      pos_++;
      continue;
    }

    // Quoted string
    if (character == '"' || character == '\'') {
      std::string term = ReadQuotedString(character);
      if (!error_.empty()) {
        return {};  // Error already set
      }
      tokens.emplace_back(TokenType::TERM, term);
      continue;
    }

    // Unquoted term or keyword
    if (IsTermChar(character)) {
      std::string term = ReadTerm();
      std::string upper_term = ToUpper(term);

      if (upper_term == "AND") {
        tokens.emplace_back(TokenType::AND);
      } else if (upper_term == "OR") {
        tokens.emplace_back(TokenType::OR);
      } else if (upper_term == "NOT") {
        tokens.emplace_back(TokenType::NOT);
      } else {
        tokens.emplace_back(TokenType::TERM, term);
      }
      continue;
    }

    // Unknown character
    SetError(std::string("Unexpected character: '") + character + "'");
    return {};
  }

  tokens.emplace_back(TokenType::END);
  return tokens;
}

// ============================================================================
// QueryASTParser
// ============================================================================

std::unique_ptr<QueryNode> QueryASTParser::Parse(const std::string& query_str) {
  error_.clear();
  pos_ = 0;

  // Tokenize input
  Tokenizer tokenizer(query_str);
  tokens_ = tokenizer.Tokenize();

  if (!tokenizer.GetError().empty()) {
    error_ = tokenizer.GetError();
    return nullptr;
  }

  if (tokens_.empty() || tokens_[0].type == TokenType::END) {
    SetError("Empty query");
    return nullptr;
  }

  // Parse expression
  auto root = ParseOrExpr();

  if (!error_.empty()) {
    return nullptr;
  }

  // Check for trailing tokens
  if (!Match(TokenType::END)) {
    SetError("Unexpected token after expression");
    return nullptr;
  }

  return root;
}

const Token& QueryASTParser::CurrentToken() const {
  if (pos_ < tokens_.size()) {
    return tokens_[pos_];
  }
  static const Token kEndToken(TokenType::END);
  return kEndToken;
}

void QueryASTParser::Advance() {
  if (pos_ < tokens_.size()) {
    pos_++;
  }
}

bool QueryASTParser::Match(TokenType type) const {
  return CurrentToken().type == type;
}

bool QueryASTParser::Consume(TokenType type) {
  if (Match(type)) {
    Advance();
    return true;
  }
  return false;
}

bool QueryASTParser::Expect(TokenType type, const std::string& error_msg) {
  if (Match(type)) {
    Advance();
    return true;
  }
  SetError(error_msg);
  return false;
}

std::unique_ptr<QueryNode> QueryASTParser::ParseOrExpr() {
  // or_expr → and_expr (OR and_expr)*
  auto left = ParseAndExpr();
  if (!left || !error_.empty()) {
    return nullptr;
  }

  while (Match(TokenType::OR)) {
    Advance();

    auto right = ParseAndExpr();
    if (!right || !error_.empty()) {
      return nullptr;
    }

    auto or_node = std::make_unique<QueryNode>(NodeType::OR);
    or_node->children.push_back(std::move(left));
    or_node->children.push_back(std::move(right));
    left = std::move(or_node);
  }

  return left;
}

std::unique_ptr<QueryNode> QueryASTParser::ParseAndExpr() {
  // and_expr → not_expr (AND not_expr)*
  auto left = ParseNotExpr();
  if (!left || !error_.empty()) {
    return nullptr;
  }

  while (Match(TokenType::AND)) {
    Advance();

    auto right = ParseNotExpr();
    if (!right || !error_.empty()) {
      return nullptr;
    }

    auto and_node = std::make_unique<QueryNode>(NodeType::AND);
    and_node->children.push_back(std::move(left));
    and_node->children.push_back(std::move(right));
    left = std::move(and_node);
  }

  return left;
}

std::unique_ptr<QueryNode> QueryASTParser::ParseNotExpr() {
  // not_expr → NOT not_expr | primary
  if (Match(TokenType::NOT)) {
    Advance();

    auto child = ParseNotExpr();
    if (!child || !error_.empty()) {
      return nullptr;
    }

    auto not_node = std::make_unique<QueryNode>(NodeType::NOT);
    not_node->children.push_back(std::move(child));
    return not_node;
  }

  return ParsePrimary();
}

std::unique_ptr<QueryNode> QueryASTParser::ParsePrimary() {
  // primary → TERM | '(' or_expr ')'

  if (Match(TokenType::TERM)) {
    std::string term = CurrentToken().value;
    Advance();
    return std::make_unique<QueryNode>(term);
  }

  if (Match(TokenType::LPAREN)) {
    Advance();

    auto expr = ParseOrExpr();
    if (!expr || !error_.empty()) {
      return nullptr;
    }

    if (!Expect(TokenType::RPAREN, "Expected closing parenthesis ')'")) {
      return nullptr;
    }

    return expr;
  }

  SetError("Expected term or opening parenthesis '('");
  return nullptr;
}

}  // namespace query
}  // namespace mygramdb
