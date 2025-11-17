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
#include <vector>

#include "utils/error.h"
#include "utils/expected.h"

namespace mygramdb::client {

/**
 * @brief Search result document
 */
struct SearchResult {
  std::string primary_key;  // Document primary key

  SearchResult() = default;
  explicit SearchResult(std::string primary_key_value) : primary_key(std::move(primary_key_value)) {}
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

/**
 * @brief Count query response
 */
struct CountResponse {
  uint64_t count = 0;              // Total matching documents
  std::optional<DebugInfo> debug;  // Debug info (if debug mode enabled)
};

/**
 * @brief Server information
 */
struct ServerInfo {
  std::string version;
  uint64_t uptime_seconds = 0;
  uint64_t total_requests = 0;
  uint64_t active_connections = 0;
  uint64_t index_size_bytes = 0;
  uint64_t doc_count = 0;
  std::vector<std::string> tables;  // List of table names
};

/**
 * @brief Replication status
 */
struct ReplicationStatus {
  bool running = false;    // Whether replication is active
  std::string gtid;        // Current GTID position
  std::string status_str;  // Raw status string
};

/**
 * @brief Client configuration
 */
// NOLINTBEGIN(readability-magic-numbers,cppcoreguidelines-avoid-magic-numbers) - Default MygramDB
// client settings
struct ClientConfig {
  std::string host = "127.0.0.1";     // Server hostname
  uint16_t port = 11016;              // Default port for MygramDB protocol
  uint32_t timeout_ms = 5000;         // Default timeout in milliseconds
  uint32_t recv_buffer_size = 65536;  // Default buffer size (64KB)
};
// NOLINTEND(readability-magic-numbers,cppcoreguidelines-avoid-magic-numbers)

/**
 * @brief MygramDB client
 *
 * This class provides a thread-safe client for MygramDB. Each instance
 * maintains a single TCP connection to the server.
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
   * @param limit Maximum number of results to return (default: 100)
   * @param offset Result offset for pagination (default: 0)
   * @param and_terms Additional required terms
   * @param not_terms Excluded terms
   * @param filters Filter conditions (key=value pairs)
   * @param sort_column Column name for SORT clause (empty for primary key)
   * @param sort_desc Sort descending (default: true = descending)
   * @return Expected<SearchResponse, Error>
   */
  mygram::utils::Expected<SearchResponse, mygram::utils::Error> Search(
      const std::string& table, const std::string& query,
      uint32_t limit = 1000,  // NOLINT(readability-magic-numbers,cppcoreguidelines-avoid-magic-numbers)
                              // - Default result limit
      uint32_t offset = 0, const std::vector<std::string>& and_terms = {},
      const std::vector<std::string>& not_terms = {},
      const std::vector<std::pair<std::string, std::string>>& filters = {}, const std::string& sort_column = "",
      bool sort_desc = true) const;

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
