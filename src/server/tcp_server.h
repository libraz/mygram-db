/**
 * @file tcp_server.h
 * @brief Simple TCP server for text protocol
 */

#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "config/config.h"
#include "index/index.h"
#include "query/query_parser.h"
#include "server/server_stats.h"
#include "server/thread_pool.h"
#include "storage/document_store.h"

#ifdef USE_MYSQL
namespace mygramdb::mysql {
class BinlogReader;
}  // namespace mygramdb::mysql
#endif

namespace mygramdb::server {

constexpr uint16_t kDefaultPort = 11016;       // memcached default port
constexpr int kDefaultMaxConnections = 10000;  // Maximum concurrent connections
constexpr int kDefaultRecvBufferSize = 4096;   // Receive buffer size
constexpr int kDefaultSendBufferSize = 65536;  // Send buffer size

/**
 * @brief TCP server configuration
 */
struct ServerConfig {
  std::string host = "0.0.0.0";
  uint16_t port = kDefaultPort;
  int max_connections = kDefaultMaxConnections;
  int worker_threads = 0;  // Number of worker threads (0 = CPU count)
  int recv_buffer_size = kDefaultRecvBufferSize;
  int send_buffer_size = kDefaultSendBufferSize;
};

/**
 * @brief Per-connection context
 */
struct ConnectionContext {
  int client_fd = -1;
  bool debug_mode = false;  // Debug mode flag
};

/**
 * @brief Table context managing resources for a single table
 */
struct TableContext {
  std::string name;
  config::TableConfig config;
  std::unique_ptr<index::Index> index;
  std::unique_ptr<storage::DocumentStore> doc_store;
  // Note: BinlogReader is shared across all tables (single GTID stream)
};

/**
 * @brief Simple TCP server for text protocol
 *
 * Text protocol format:
 * Request: <COMMAND> <args...>\r\n
 * Response: <STATUS> <data>\r\n
 */
class TcpServer {
 public:
  /**
   * @brief Construct TCP server
   * @param config Server configuration
   * @param table_contexts Map of table name to TableContext pointer
   * @param snapshot_dir Snapshot directory path
   * @param full_config Full application configuration (for CONFIG command)
   * @param binlog_reader Optional BinlogReader for replication status
   */
  TcpServer(ServerConfig config, std::unordered_map<std::string, TableContext*> table_contexts,
            std::string snapshot_dir = "./snapshots", const config::Config* full_config = nullptr,
#ifdef USE_MYSQL
            mysql::BinlogReader* binlog_reader = nullptr
#else
            void* binlog_reader = nullptr
#endif
  );

  ~TcpServer();

  // Non-copyable and non-movable
  TcpServer(const TcpServer&) = delete;
  TcpServer& operator=(const TcpServer&) = delete;
  TcpServer(TcpServer&&) = delete;
  TcpServer& operator=(TcpServer&&) = delete;

  /**
   * @brief Start server
   * @return true if started successfully
   */
  bool Start();

  /**
   * @brief Stop server
   */
  void Stop();

  /**
   * @brief Check if server is running
   */
  bool IsRunning() const { return running_; }

  /**
   * @brief Get server port
   */
  uint16_t GetPort() const { return actual_port_; }

  /**
   * @brief Get active connection count
   */
  size_t GetConnectionCount() const { return stats_.GetActiveConnections(); }

  /**
   * @brief Get total requests handled
   */
  uint64_t GetTotalRequests() const { return stats_.GetTotalRequests(); }

  /**
   * @brief Get last error message
   */
  const std::string& GetLastError() const { return last_error_; }

  /**
   * @brief Get server start time (Unix timestamp)
   */
  uint64_t GetStartTime() const { return stats_.GetStartTime(); }

  /**
   * @brief Get server statistics
   */
  const ServerStats& GetStats() const { return stats_; }

  /**
   * @brief Get mutable server statistics pointer (for binlog reader)
   */
  ServerStats* GetMutableStats() { return &stats_; }

 private:
  ServerConfig config_;
  std::unordered_map<std::string, TableContext*> table_contexts_;
  query::QueryParser query_parser_;

  std::atomic<bool> running_{false};
  std::atomic<bool> should_stop_{false};

  // Statistics
  ServerStats stats_;

  int server_fd_ = -1;
  uint16_t actual_port_ = 0;
  std::unique_ptr<std::thread> accept_thread_;
  std::unique_ptr<ThreadPool> thread_pool_;
  std::set<int> connection_fds_;  // Active connection file descriptors
  mutable std::mutex connections_mutex_;
  std::unordered_map<int, ConnectionContext> connection_contexts_;  // Connection contexts
  mutable std::mutex contexts_mutex_;

  std::string last_error_;
  std::string snapshot_dir_;            // Snapshot directory
  std::atomic<bool> read_only_{false};  // Read-only mode flag
  std::atomic<bool> loading_{false};    // Loading mode flag (blocks queries during LOAD)
  const config::Config* full_config_;   // Full configuration for CONFIG command

#ifdef USE_MYSQL
  mysql::BinlogReader* binlog_reader_;
#else
  void* binlog_reader_;
#endif

  /**
   * @brief Accept thread function
   */
  void AcceptThreadFunc();

  /**
   * @brief Handle client connection
   */
  void HandleClient(int client_fd);

  /**
   * @brief Process single request
   * @param request Request string
   * @param ctx Connection context (for debug mode, etc.)
   * @return Response string
   */
  std::string ProcessRequest(const std::string& request, ConnectionContext& ctx);

  /**
   * @brief Format SEARCH response
   * @param results Search results (already sorted and paginated)
   * @param total_results Total number of results before pagination
   * @param doc_store Document store for retrieving primary keys
   * @param debug_info Optional debug information
   * @return Formatted response
   */
  std::string FormatSearchResponse(const std::vector<index::DocId>& results, size_t total_results,
                                   storage::DocumentStore* doc_store, const query::DebugInfo* debug_info = nullptr);

  /**
   * @brief Format COUNT response
   * @param count Result count
   * @param debug_info Optional debug information
   * @return Formatted response
   */
  static std::string FormatCountResponse(uint64_t count, const query::DebugInfo* debug_info = nullptr);

  /**
   * @brief Format GET response
   */
  static std::string FormatGetResponse(const std::optional<storage::Document>& doc);

  /**
   * @brief Format INFO response
   */
  std::string FormatInfoResponse();

  /**
   * @brief Format SAVE response
   */
  static std::string FormatSaveResponse(const std::string& filepath);

  /**
   * @brief Format LOAD response
   */
  static std::string FormatLoadResponse(const std::string& filepath);

  /**
   * @brief Format REPLICATION STATUS response
   */
  std::string FormatReplicationStatusResponse();

  /**
   * @brief Format REPLICATION STOP response
   */
  static std::string FormatReplicationStopResponse();

  /**
   * @brief Format REPLICATION START response
   */
  static std::string FormatReplicationStartResponse();

  /**
   * @brief Format CONFIG response
   */
  std::string FormatConfigResponse();

  /**
   * @brief Format error response
   */
  static std::string FormatError(const std::string& message);

  /**
   * @brief Set socket options
   */
  bool SetSocketOptions(int socket_fd);

  /**
   * @brief Remove connection from active list
   */
  void RemoveConnection(int socket_fd);
};

}  // namespace mygramdb::server
