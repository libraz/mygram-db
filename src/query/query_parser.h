/**
 * @file query_parser.h
 * @brief Query parser for text protocol commands
 */

#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "config/config.h"
#include "utils/error.h"
#include "utils/expected.h"

namespace mygramdb::query {

/**
 * @brief Query command type
 */
enum class QueryType : uint8_t {
  SEARCH,  // Search with limit/offset
  COUNT,   // Get total count
  GET,     // Get document by primary key
  INFO,    // Get server info (memcached-style)

  // Dump commands (hierarchical)
  DUMP_SAVE,    // DUMP SAVE filepath [--with-stats]
  DUMP_LOAD,    // DUMP LOAD filepath
  DUMP_VERIFY,  // DUMP VERIFY filepath
  DUMP_INFO,    // DUMP INFO filepath

  // Legacy dump commands (for backward compatibility)
  SAVE,  // SAVE filepath (deprecated, use DUMP SAVE)
  LOAD,  // LOAD filepath (deprecated, use DUMP LOAD)

  // Replication commands
  REPLICATION_STATUS,  // REPLICATION STATUS
  REPLICATION_STOP,    // REPLICATION STOP
  REPLICATION_START,   // REPLICATION START

  // Snapshot synchronization commands
  SYNC,         // SYNC [table]
  SYNC_STATUS,  // SYNC STATUS

  // Config commands
  CONFIG_HELP,    // CONFIG HELP [path]
  CONFIG_SHOW,    // CONFIG SHOW [path]
  CONFIG_VERIFY,  // CONFIG VERIFY <filepath>

  // Server commands
  OPTIMIZE,   // OPTIMIZE [table]
  DEBUG_ON,   // DEBUG ON
  DEBUG_OFF,  // DEBUG OFF

  // Cache commands
  CACHE_CLEAR,    // CACHE CLEAR [table]
  CACHE_STATS,    // CACHE STATS
  CACHE_ENABLE,   // CACHE ENABLE
  CACHE_DISABLE,  // CACHE DISABLE

  UNKNOWN
};

/**
 * @brief Filter operator
 */
enum class FilterOp : uint8_t {
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
  FilterOp op = FilterOp::EQ;
  std::string value;
};

/**
 * @brief Sort order for SORT clause
 */
enum class SortOrder : uint8_t {
  ASC,  // Ascending
  DESC  // Descending (default)
};

/**
 * @brief SORT clause specification (formerly ORDER BY)
 */
struct OrderByClause {
  std::string column;                 // Column name (empty = primary key)
  SortOrder order = SortOrder::DESC;  // Default: descending

  /**
   * @brief Check if ordering by primary key
   */
  [[nodiscard]] bool IsPrimaryKey() const { return column.empty(); }
};

/**
 * @brief Cache debug information
 */
struct CacheDebugInfo {
  enum class Status : std::uint8_t { HIT, MISS_NOT_FOUND, MISS_INVALIDATED, MISS_DISABLED };

  Status status = Status::MISS_DISABLED;
  double cache_age_ms = 0.0;    // Age of cached result (HIT only)
  double cache_saved_ms = 0.0;  // Time saved by cache (HIT only)
  double query_cost_ms = 0.0;   // Actual query execution time (MISS only)
  std::string cache_key;        // Cache key used (optional, for debugging)
};

/**
 * @brief Debug information for query execution
 */
struct DebugInfo {
  double query_time_ms = 0.0;              // Total query execution time
  double parse_time_ms = 0.0;              // Query parsing time
  double index_time_ms = 0.0;              // Index search time
  double filter_time_ms = 0.0;             // Filter application time
  std::vector<std::string> search_terms;   // Search terms used
  std::vector<std::string> ngrams_used;    // N-grams generated
  std::vector<size_t> posting_list_sizes;  // Size of each posting list
  size_t total_candidates = 0;             // Total candidates before filtering
  size_t after_intersection = 0;           // Results after term intersection
  size_t after_not = 0;                    // Results after NOT filtering
  size_t after_filters = 0;                // Results after filter conditions
  size_t final_results = 0;                // Final result count
  std::string optimization_used;           // Optimization strategy used
  std::string order_by_applied;            // ORDER BY applied (e.g., "id DESC")
  uint32_t limit_applied = 0;              // LIMIT value applied
  uint32_t offset_applied = 0;             // OFFSET value applied
  bool limit_explicit = false;             // True if LIMIT was explicitly specified
  bool offset_explicit = false;            // True if OFFSET was explicitly specified
  CacheDebugInfo cache_info;               // Cache debug information
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
  std::optional<OrderByClause> order_by;  // SORT clause (default: primary key DESC)
  // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
  uint32_t limit = 100;          // Initial value (overridden by config.api.default_limit if not explicit)
  uint32_t offset = 0;           // Default offset
  bool limit_explicit = false;   // True if LIMIT was explicitly specified by user
  bool offset_explicit = false;  // True if OFFSET was explicitly specified by user
  std::string primary_key;       // For GET command
  std::string filepath;          // For DUMP SAVE/LOAD/VERIFY/INFO commands (optional)

  // DUMP command options
  bool dump_with_stats = false;  // --with-stats flag for DUMP SAVE

  // Cache optimization: precomputed cache key (set by QueryParser)
  // This avoids recomputing normalization and MD5 hash on every cache lookup
  std::optional<std::pair<uint64_t, uint64_t>> cache_key;

  /**
   * @brief Check if query is valid
   */
  [[nodiscard]] bool IsValid() const;
};

/**
 * @brief Query parser
 *
 * Parses text protocol commands:
 * - SEARCH <table> <text> [AND <term>] [NOT <term>] [FILTER <col> <op> <value>] [SORT <col>
 * ASC|DESC] [LIMIT <n>|<offset>,<count>] [OFFSET <n>]
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
 * - SORT: SEARCH threads golang SORT created_at DESC LIMIT 10
 * - LIMIT formats: LIMIT 100 or LIMIT 10,100 (offset,count)
 * - Default order: primary key DESC (if SORT not specified)
 */
class QueryParser {
 public:
  QueryParser() = default;
  ~QueryParser() = default;

  // Copyable and movable
  QueryParser(const QueryParser&) = default;
  QueryParser& operator=(const QueryParser&) = default;
  QueryParser(QueryParser&&) noexcept = default;
  QueryParser& operator=(QueryParser&&) noexcept = default;

  /**
   * @brief Parse query string
   *
   * @param query_str Query string
   * @return Expected<Query, Error> - Parsed query or error
   */
  mygram::utils::Expected<Query, mygram::utils::Error> Parse(std::string_view query_str);

  /**
   * @brief Get last error message
   */
  [[nodiscard]] const std::string& GetError() const { return error_; }

  /**
   * @brief Configure maximum allowed query expression length (0 = unlimited)
   */
  void SetMaxQueryLength(size_t max_length);

  /**
   * @brief Get configured maximum query expression length
   */
  [[nodiscard]] size_t GetMaxQueryLength() const { return max_query_length_; }

 private:
  std::string error_;
  size_t max_query_length_ = config::defaults::kDefaultQueryLengthLimit;  // Default upper bound

  /**
   * @brief Parse SEARCH command
   */
  mygram::utils::Expected<Query, mygram::utils::Error> ParseSearch(const std::vector<std::string>& tokens);

  /**
   * @brief Parse COUNT command
   */
  mygram::utils::Expected<Query, mygram::utils::Error> ParseCount(const std::vector<std::string>& tokens);

  /**
   * @brief Parse GET command
   */
  mygram::utils::Expected<Query, mygram::utils::Error> ParseGet(const std::vector<std::string>& tokens);

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
  bool ParseFilterArguments(const std::vector<std::string>& tokens, size_t& pos, FilterCondition& filter);

  /**
   * @brief Parse LIMIT clause
   */
  bool ParseLimit(const std::vector<std::string>& tokens, size_t& pos, Query& query);

  /**
   * @brief Parse OFFSET clause
   */
  bool ParseOffset(const std::vector<std::string>& tokens, size_t& pos, Query& query);

  /**
   * @brief Parse SORT clause
   */
  bool ParseSort(const std::vector<std::string>& tokens, size_t& pos, Query& query);

  /**
   * @brief Tokenize query string
   */
  std::vector<std::string> Tokenize(std::string_view str);

  /**
   * @brief Parse filter operator
   */
  static std::optional<FilterOp> ParseFilterOp(std::string_view op_str);

  /**
   * @brief Set error message
   */
  void SetError(std::string_view msg) { error_ = std::string(msg); }

  /**
   * @brief Validate query length against configured limit
   */
  bool ValidateQueryLength(const Query& query);
};

}  // namespace mygramdb::query
