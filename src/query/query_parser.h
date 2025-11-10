/**
 * @file query_parser.h
 * @brief Query parser for text protocol commands
 */

#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include <optional>
#include <unordered_map>

namespace mygramdb {
namespace query {

/**
 * @brief Query command type
 */
enum class QueryType {
  SEARCH,  // Search with limit/offset
  COUNT,   // Get total count
  GET,     // Get document by primary key
  UNKNOWN
};

/**
 * @brief Filter operator
 */
enum class FilterOp {
  EQ,   // Equal
  NE,   // Not equal
  GT,   // Greater than
  GTE,  // Greater than or equal
  LT,   // Less than
  LTE   // Less than or equal
};

/**
 * @brief Filter condition
 */
struct FilterCondition {
  std::string column;
  FilterOp op;
  std::string value;
};

/**
 * @brief Parsed query
 */
struct Query {
  QueryType type = QueryType::UNKNOWN;
  std::string table;
  std::string search_text;
  std::vector<std::string> not_terms;  // Terms to exclude (NOT search)
  std::vector<FilterCondition> filters;
  uint32_t limit = 100;    // Default limit
  uint32_t offset = 0;     // Default offset
  std::string primary_key; // For GET command

  /**
   * @brief Check if query is valid
   */
  bool IsValid() const;
};

/**
 * @brief Query parser
 *
 * Parses text protocol commands:
 * - SEARCH <table> <text> [NOT <term>] [FILTER <col> <op> <value>] [LIMIT <n>] [OFFSET <n>]
 * - COUNT <table> <text> [NOT <term>] [FILTER <col> <op> <value>]
 * - GET <table> <primary_key>
 */
class QueryParser {
 public:
  QueryParser() = default;
  ~QueryParser() = default;

  /**
   * @brief Parse query string
   *
   * @param query_str Query string
   * @return Parsed query
   */
  Query Parse(const std::string& query_str);

  /**
   * @brief Get last error message
   */
  const std::string& GetError() const { return error_; }

 private:
  std::string error_;

  /**
   * @brief Parse SEARCH command
   */
  Query ParseSearch(const std::vector<std::string>& tokens);

  /**
   * @brief Parse COUNT command
   */
  Query ParseCount(const std::vector<std::string>& tokens);

  /**
   * @brief Parse GET command
   */
  Query ParseGet(const std::vector<std::string>& tokens);

  /**
   * @brief Parse NOT clause
   */
  bool ParseNot(const std::vector<std::string>& tokens, size_t& pos, Query& query);

  /**
   * @brief Parse filter conditions
   */
  bool ParseFilters(const std::vector<std::string>& tokens, size_t& pos, Query& query);

  /**
   * @brief Parse LIMIT clause
   */
  bool ParseLimit(const std::vector<std::string>& tokens, size_t& pos, Query& query);

  /**
   * @brief Parse OFFSET clause
   */
  bool ParseOffset(const std::vector<std::string>& tokens, size_t& pos, Query& query);

  /**
   * @brief Tokenize query string
   */
  std::vector<std::string> Tokenize(const std::string& str);

  /**
   * @brief Parse filter operator
   */
  std::optional<FilterOp> ParseFilterOp(const std::string& op_str);

  /**
   * @brief Set error message
   */
  void SetError(const std::string& msg) { error_ = msg; }
};

}  // namespace query
}  // namespace mygramdb
