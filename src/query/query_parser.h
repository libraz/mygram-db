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
  SEARCH,              // Search with limit/offset
  COUNT,               // Get total count
  GET,                 // Get document by primary key
  INFO,                // Get server info (memcached-style)
  SAVE,                // Save snapshot to disk
  LOAD,                // Load snapshot from disk
  REPLICATION_STATUS,  // Get replication status
  REPLICATION_STOP,    // Stop replication
  REPLICATION_START,   // Start replication
  CONFIG,              // Get current configuration
  OPTIMIZE,            // Optimize index (convert to Roaring bitmaps)
  DEBUG_ON,            // Enable debug mode for this connection
  DEBUG_OFF,           // Disable debug mode for this connection
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
 * @brief Debug information for query execution
 */
struct DebugInfo {
  double query_time_ms = 0.0;                    // Total query execution time
  double parse_time_ms = 0.0;                    // Query parsing time
  double index_time_ms = 0.0;                    // Index search time
  double filter_time_ms = 0.0;                   // Filter application time
  std::vector<std::string> search_terms;         // Search terms used
  std::vector<std::string> ngrams_used;          // N-grams generated
  std::vector<size_t> posting_list_sizes;        // Size of each posting list
  size_t total_candidates = 0;                   // Total candidates before filtering
  size_t after_intersection = 0;                 // Results after term intersection
  size_t after_not = 0;                          // Results after NOT filtering
  size_t after_filters = 0;                      // Results after filter conditions
  size_t final_results = 0;                      // Final result count
  std::string optimization_used;                 // Optimization strategy used
};

/**
 * @brief Parsed query
 */
struct Query {
  QueryType type = QueryType::UNKNOWN;
  std::string table;
  std::string search_text;
  std::vector<std::string> and_terms;  // Additional terms for AND search
  std::vector<std::string> not_terms;  // Terms to exclude (NOT search)
  std::vector<FilterCondition> filters;
  uint32_t limit = 100;    // Default limit
  uint32_t offset = 0;     // Default offset
  std::string primary_key; // For GET command
  std::string filepath;    // For SAVE/LOAD command (optional)

  /**
   * @brief Check if query is valid
   */
  bool IsValid() const;
};

/**
 * @brief Query parser
 *
 * Parses text protocol commands:
 * - SEARCH <table> <text> [AND <term>] [NOT <term>] [FILTER <col> <op> <value>] [LIMIT <n>] [OFFSET <n>]
 * - COUNT <table> <text> [AND <term>] [NOT <term>] [FILTER <col> <op> <value>]
 * - GET <table> <primary_key>
 * - INFO
 * - SAVE [filename]
 * - LOAD [filename]
 * - REPLICATION STATUS
 * - REPLICATION STOP
 * - REPLICATION START
 *
 * Notes:
 * - Use quotes for phrases: SEARCH threads "hello world" will search for the exact phrase
 * - AND operator: SEARCH threads term1 AND term2 AND term3
 * - NOT operator: SEARCH threads term1 NOT excluded
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
   * @brief Parse AND clause
   */
  bool ParseAnd(const std::vector<std::string>& tokens, size_t& pos, Query& query);

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
  static std::optional<FilterOp> ParseFilterOp(const std::string& op_str);

  /**
   * @brief Set error message
   */
  void SetError(const std::string& msg) { error_ = msg; }
};

}  // namespace query
}  // namespace mygramdb
