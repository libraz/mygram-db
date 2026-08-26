/**
 * @file mygramclient.h
 * @brief C++ Client library for MygramDB
 *
 * This library provides a high-level C++ interface for connecting to and
 * querying MygramDB servers. It supports all MygramDB protocol commands
 * including SEARCH, COUNT, GET, INFO, and replication control.
 */

#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "utils/error.h"
#include "utils/expected.h"

namespace mygramdb::client {

/**
 * @brief Parse a TCP ERROR response, including its optional numeric code.
 *
 * New servers emit `ERROR <code> <message>`. Responses from older servers
 * that omit the numeric token are mapped to kClientServerError.
 */
[[nodiscard]] std::optional<mygram::utils::Error> ParseServerErrorResponse(std::string_view response);

/**
 * @brief Search result document
 */
struct SearchResult {
  std::string primary_key;  // Document primary key
  std::string snippet;      // Highlight snippet (empty for non-highlight searches)

  SearchResult() = default;
  explicit SearchResult(std::string primary_key_value) : primary_key(std::move(primary_key_value)) {}
  SearchResult(std::string primary_key_value, std::string snippet_value)
      : primary_key(std::move(primary_key_value)), snippet(std::move(snippet_value)) {}
};

/**
 * @brief Document with filter fields
 */
struct Document {
  std::string primary_key;                                  // Document primary key
  std::vector<std::pair<std::string, std::string>> fields;  // Filter fields (key=value)

  Document() = default;
  explicit Document(std::string primary_key_value) : primary_key(std::move(primary_key_value)) {}
};

/**
 * @brief Query debug information
 */
struct DebugInfo {
  double query_time_ms = 0.0;       // Total query execution time (ms)
  double index_time_ms = 0.0;       // Index search time (ms)
  double filter_time_ms = 0.0;      // Filter processing time (ms)
  uint32_t terms = 0;               // Number of search terms
  uint32_t ngrams = 0;              // Number of n-grams generated
  uint64_t candidates = 0;          // Initial candidate count
  uint64_t after_intersection = 0;  // After AND intersection
  uint64_t after_not = 0;           // After NOT filtering
  uint64_t after_filters = 0;       // After FILTER conditions
  uint64_t final = 0;               // Final result count
  std::string optimization;         // Optimization strategy used
};

/**
 * @brief Search query response with results and metadata
 */
struct SearchResponse {
  std::vector<SearchResult> results;  // Search results
  uint64_t total_count = 0;           // Total matching documents (may exceed results.size())
  std::optional<DebugInfo> debug;     // Debug info (if debug mode enabled)
};

enum class FilterOp : uint8_t {
  kEqual = 0,
  kNotEqual,
  kGreaterThan,
  kGreaterThanOrEqual,
  kLessThan,
  kLessThanOrEqual,
};

struct FilterCondition {
  std::string key;
  FilterOp op = FilterOp::kEqual;
  std::string value;
};

struct HighlightOptions {
  std::string open_tag;
  std::string close_tag;
  uint32_t snippet_length = 0;  // 0 = server default
  uint32_t max_fragments = 0;   // 0 = server default
};

enum class QueryMode : uint8_t {
  kLiteral = 0,
  kBoolean,
};

struct SearchOptions {
  QueryMode query_mode = QueryMode::kLiteral;
  uint32_t limit = 0;
  uint32_t offset = 0;
  std::vector<std::string> and_terms;
  std::vector<std::string> not_terms;
  std::vector<FilterCondition> filters;
  std::string sort_column;
  bool sort_desc = true;
  std::optional<uint32_t> fuzzy_distance;
  std::optional<HighlightOptions> highlight;
};

/**
 * @brief Count query response
 */
struct CountResponse {
  uint64_t count = 0;              // Total matching documents
  std::optional<DebugInfo> debug;  // Debug info (if debug mode enabled)
};

/**
 * @brief Single facet value/count pair
 */
struct FacetValue {
  std::string value;   // Facet display value
  uint64_t count = 0;  // Number of matching documents for the value
};

/**
 * @brief Facet query response
 */
struct FacetResponse {
  std::vector<FacetValue> facets;  // Facet value counts in this page
  uint64_t total_count = 0;        // Total distinct values before OFFSET/LIMIT
};

/**
 * @brief Server information
 */
struct ServerInfo {
  std::string version;
  uint64_t uptime_seconds = 0;
  uint64_t total_requests = 0;
  uint64_t active_connections = 0;  ///< From "connected_clients" in INFO response
  uint64_t index_size_bytes = 0;    ///< Total memory bytes (from "used_memory_bytes")
  uint64_t doc_count = 0;
  std::vector<std::string> tables;  ///< List of table names
};

/**
 * @brief Parsed CACHE STATS response.
 */
struct CacheStatistics {
  bool enabled = false;
  uint64_t total_queries = 0;
  uint64_t cache_hits = 0;
  uint64_t cache_misses = 0;
  double hit_rate = 0.0;
  uint64_t current_entries = 0;
  uint64_t current_memory_bytes = 0;
  uint64_t invalidation_index_memory_bytes = 0;
  uint64_t invalidation_queue_memory_bytes = 0;
  uint64_t accounted_memory_bytes = 0;
  uint64_t evictions = 0;
  uint64_t ttl_expirations = 0;
  uint64_t rejection_count = 0;
  uint64_t rejection_oversize = 0;
  uint64_t rejection_memory_budget = 0;
  uint64_t rejection_duplicate = 0;
  uint64_t stale_entry_removals = 0;
  uint64_t decompression_failures = 0;
  uint64_t stale_lru_entries = 0;
  uint64_t invalidations_immediate = 0;
  uint64_t invalidations_deferred = 0;
  uint64_t invalidations_batches = 0;
  std::optional<double> avg_cache_hit_time_ms;
  std::optional<double> avg_cache_miss_time_ms;
  double total_time_saved_ms = 0.0;
};

/**
 * @brief Replication status
 *
 * Reflects the contents of the server's "OK REPLICATION" multi-line response.
 * Fields not emitted by the server (e.g. when replication is not configured)
 * remain at their default values.
 */
struct ReplicationStatus {
  bool running = false;           ///< True when status == "running"
  std::string gtid;               ///< Current GTID position (from current_gtid)
  std::string status_str;         ///< Raw status string ("running", "stopped", "not_configured", ...)
  uint64_t processed_events = 0;  ///< Number of binlog events processed since start
  uint64_t queue_size = 0;        ///< Pending events waiting to be applied (0 when not running)
};

/**
 * @brief Client configuration
 */
// NOLINTBEGIN(readability-magic-numbers,cppcoreguidelines-avoid-magic-numbers) - Default MygramDB
// client settings
struct ClientConfig {
  std::string host = "127.0.0.1";                           // Server hostname
  uint16_t port = 11016;                                    // Default port for MygramDB protocol
  uint32_t timeout_ms = 5000;                               // Ordinary request timeout (0 = default)
  uint32_t connect_timeout_ms = 0;                          // Connect timeout (0 = timeout_ms)
  uint32_t dump_save_timeout_ms = 300000;                   // Max wait for async DUMP SAVE completion (0 = timeout_ms)
  uint32_t dump_load_timeout_ms = 300000;                   // DUMP LOAD request timeout (0 = timeout_ms)
  uint32_t dump_verify_timeout_ms = 300000;                 // DUMP VERIFY request timeout (0 = timeout_ms)
  uint32_t optimize_timeout_ms = 300000;                    // OPTIMIZE request timeout (0 = timeout_ms)
  uint32_t recv_buffer_size = 64U * 1024U;                  // Default buffer size (64KB; 0 = default, max 16 MiB)
  uint64_t max_response_bytes = 64ULL * 1024ULL * 1024ULL;  // Maximum response frame size (0 = default 64 MiB)
  std::string unix_socket_path;                             // Unix socket path (empty = use TCP)
};
// NOLINTEND(readability-magic-numbers,cppcoreguidelines-avoid-magic-numbers)

/**
 * @brief MygramDB client
 *
 * This class maintains a single TCP connection to a MygramDB server.
 *
 * Thread-safety: concurrent calls into a single instance from multiple
 * threads are serialized internally by an internal mutex around
 * SendCommand(). The wire protocol is synchronous request/response on a
 * single socket, so commands necessarily run sequentially. Throughput is
 * single-command-at-a-time per instance — for higher throughput, use
 * multiple `MygramClient` instances (e.g. one per worker thread).
 *
 * Connect(), Disconnect(), IsConnected(), and all commands are serialized on
 * the same internal mutex. Disconnect() waits for an in-flight command to
 * finish; it does not cancel that command. Object lifetime operations
 * (destruction and move) must not race with any member call.
 *
 * Example usage:
 * @code
 *   ClientConfig config;
 *   config.host = "localhost";
 *   config.port = 11016;
 *
 *   MygramClient client(config);
 *   auto conn_result = client.Connect();
 *   if (!conn_result) {
 *     std::cerr << "Connection failed: " << conn_result.error().message() << std::endl;
 *     return;
 *   }
 *
 *   auto result = client.Search("articles", "hello world", 100);
 *   if (!result) {
 *     std::cerr << "Search failed: " << result.error().message() << std::endl;
 *   } else {
 *     auto& resp = *result;
 *     std::cout << "Found " << resp.total_count << " results\n";
 *   }
 * @endcode
 */
class MygramClient {
 public:
  /**
   * @brief Construct client with configuration
   * @param config Client configuration
   */
  explicit MygramClient(ClientConfig config);

  /**
   * @brief Destructor - automatically disconnects
   */
  ~MygramClient();

  // Non-copyable
  MygramClient(const MygramClient&) = delete;
  MygramClient& operator=(const MygramClient&) = delete;

  // Movable
  MygramClient(MygramClient&&) noexcept;
  MygramClient& operator=(MygramClient&&) noexcept;

  /**
   * @brief Connect to MygramDB server
   * @return Expected<void, Error> - success or error
   */
  mygram::utils::Expected<void, mygram::utils::Error> Connect();

  /**
   * @brief Disconnect from server
   */
  void Disconnect();

  /**
   * @brief Check if connected to server
   * @return true if connected
   */
  [[nodiscard]] bool IsConnected() const;

  /**
   * @brief Search for documents
   *
   * @param table Table name
   * @param query Search query text
   * @param limit Maximum number of results to return (0 = server api.default_limit)
   * @param offset Result offset for pagination (default: 0)
   * @param and_terms Additional required terms
   * @param not_terms Excluded terms
   * @param filters Filter conditions (key=value pairs)
   * @param sort_column Column name for SORT clause (empty for primary key)
   * @param sort_desc Sort descending (default: true = descending)
   * @return Expected<SearchResponse, Error>
   */
  mygram::utils::Expected<SearchResponse, mygram::utils::Error> Search(
      const std::string& table, const std::string& query, uint32_t limit = 0, uint32_t offset = 0,
      const std::vector<std::string>& and_terms = {}, const std::vector<std::string>& not_terms = {},
      const std::vector<std::pair<std::string, std::string>>& filters = {}, const std::string& sort_column = "",
      bool sort_desc = true) const;

  /**
   * @brief Search with typed FILTER operators, FUZZY, and HIGHLIGHT options.
   */
  mygram::utils::Expected<SearchResponse, mygram::utils::Error> Search(const std::string& table,
                                                                       const std::string& query,
                                                                       const SearchOptions& options) const;

  /**
   * @brief Search for documents and return highlighted snippets
   *
   * The returned SearchResult::snippet field contains the server-generated
   * highlight snippet. It is empty when the server returns no snippet.
   */
  mygram::utils::Expected<SearchResponse, mygram::utils::Error> SearchWithHighlights(
      const std::string& table, const std::string& query, uint32_t limit = 0, uint32_t offset = 0,
      const std::vector<std::string>& and_terms = {}, const std::vector<std::string>& not_terms = {},
      const std::vector<std::pair<std::string, std::string>>& filters = {}, const std::string& sort_column = "",
      bool sort_desc = true) const;

  /**
   * @brief Search using a pre-built QueryAST expression without decomposing it
   *
   * Use this with ConvertSearchExpression() output when boolean OR/grouping
   * semantics must be preserved. The expression is sent verbatim (unquoted) so
   * the server tokenizes it and its AST parser can interpret the AND/OR/grouping
   * operators; the caller is responsible for any quoting of literal phrases.
   */
  mygram::utils::Expected<SearchResponse, mygram::utils::Error> SearchRaw(const std::string& table,
                                                                          const std::string& raw_query,
                                                                          uint32_t limit = 0,
                                                                          uint32_t offset = 0) const;

  /**
   * @brief SearchRaw variant that returns highlighted snippets
   */
  mygram::utils::Expected<SearchResponse, mygram::utils::Error> SearchRawWithHighlights(const std::string& table,
                                                                                        const std::string& raw_query,
                                                                                        uint32_t limit = 0,
                                                                                        uint32_t offset = 0) const;

  /**
   * @brief Count matching documents
   *
   * @param table Table name
   * @param query Search query text
   * @param and_terms Additional required terms
   * @param not_terms Excluded terms
   * @param filters Filter conditions (key=value pairs)
   * @return Expected<CountResponse, Error>
   */
  mygram::utils::Expected<CountResponse, mygram::utils::Error> Count(
      const std::string& table, const std::string& query, const std::vector<std::string>& and_terms = {},
      const std::vector<std::string>& not_terms = {},
      const std::vector<std::pair<std::string, std::string>>& filters = {}) const;

  /**
   * @brief Count matching documents by facet column value
   *
   * @param table Table name
   * @param column Facet column name
   * @param query Optional search query text
   * @param limit Maximum number of facet values to return (0 applies the server default limit)
   * @param and_terms Additional required terms
   * @param not_terms Excluded terms
   * @param filters Filter conditions (key=value pairs)
   * @param offset Number of facet values to skip before applying limit
   * @return Expected<FacetResponse, Error>
   */
  mygram::utils::Expected<FacetResponse, mygram::utils::Error> Facet(
      const std::string& table, const std::string& column, const std::string& query = "", uint32_t limit = 0,
      const std::vector<std::string>& and_terms = {}, const std::vector<std::string>& not_terms = {},
      const std::vector<std::pair<std::string, std::string>>& filters = {}, uint32_t offset = 0) const;

  /**
   * @brief Get document by primary key
   *
   * @param table Table name
   * @param primary_key Primary key value
   * @return Expected<Document, Error>
   */
  mygram::utils::Expected<Document, mygram::utils::Error> Get(const std::string& table,
                                                              const std::string& primary_key) const;

  /**
   * @brief Get server information
   * @return Expected<ServerInfo, Error>
   */
  mygram::utils::Expected<ServerInfo, mygram::utils::Error> Info() const;

  /**
   * @brief Get server configuration
   * @return Expected<std::string, Error>
   */
  mygram::utils::Expected<std::string, mygram::utils::Error> GetConfig() const;

  mygram::utils::Expected<void, mygram::utils::Error> SetVariable(const std::string& name,
                                                                  const std::string& value) const;
  mygram::utils::Expected<std::string, mygram::utils::Error> ShowVariables(const std::string& like_pattern = "") const;
  mygram::utils::Expected<void, mygram::utils::Error> CacheClear(const std::string& table = "") const;
  /** Raw protocol response retained for source and ABI compatibility. */
  mygram::utils::Expected<std::string, mygram::utils::Error> CacheStats() const;
  /** Parsed CACHE STATS response. */
  mygram::utils::Expected<CacheStatistics, mygram::utils::Error> GetCacheStatistics() const;
  mygram::utils::Expected<void, mygram::utils::Error> CacheEnable() const;
  mygram::utils::Expected<void, mygram::utils::Error> CacheDisable() const;
  mygram::utils::Expected<std::string, mygram::utils::Error> Optimize(const std::string& table = "") const;
  mygram::utils::Expected<std::string, mygram::utils::Error> Sync(const std::string& table) const;
  mygram::utils::Expected<std::string, mygram::utils::Error> SyncStatus() const;
  mygram::utils::Expected<std::string, mygram::utils::Error> SyncStop(const std::string& table = "") const;
  mygram::utils::Expected<std::string, mygram::utils::Error> DumpInfo(const std::string& filepath) const;
  mygram::utils::Expected<std::string, mygram::utils::Error> DumpStatus() const;
  mygram::utils::Expected<std::string, mygram::utils::Error> DumpVerify(const std::string& filepath) const;

  /**
   * @brief Save snapshot to disk
   * @param filepath Optional filepath (empty for default)
   * @return Expected<std::string, Error> - saved filepath or error
   */
  mygram::utils::Expected<std::string, mygram::utils::Error> Save(const std::string& filepath = "") const;

  /**
   * @brief Load snapshot from disk
   * @param filepath Snapshot filepath
   * @return Expected<std::string, Error> - loaded filepath or error
   */
  mygram::utils::Expected<std::string, mygram::utils::Error> Load(const std::string& filepath) const;

  /**
   * @brief Get replication status
   * @return Expected<ReplicationStatus, Error>
   */
  mygram::utils::Expected<ReplicationStatus, mygram::utils::Error> GetReplicationStatus() const;

  /**
   * @brief Stop replication
   * @return Expected<void, Error>
   */
  mygram::utils::Expected<void, mygram::utils::Error> StopReplication() const;

  /**
   * @brief Start replication
   * @return Expected<void, Error>
   */
  mygram::utils::Expected<void, mygram::utils::Error> StartReplication() const;

  /**
   * @brief Enable debug mode for this connection
   * @return Expected<void, Error>
   */
  mygram::utils::Expected<void, mygram::utils::Error> EnableDebug() const;

  /**
   * @brief Disable debug mode for this connection
   * @return Expected<void, Error>
   */
  mygram::utils::Expected<void, mygram::utils::Error> DisableDebug() const;

  /**
   * @brief Send raw command to server
   *
   * This is a low-level interface for sending custom commands.
   * Most users should use the higher-level methods instead.
   *
   * @param command Command string (without \r\n terminator)
   * @return Expected<std::string, Error>
   */
  mygram::utils::Expected<std::string, mygram::utils::Error> SendCommand(const std::string& command) const;

 private:
  class Impl;  // Forward declaration for PIMPL
  mutable std::unique_ptr<Impl> impl_;
};

}  // namespace mygramdb::client
