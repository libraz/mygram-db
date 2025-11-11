/**
 * @file tcp_server.h
 * @brief Simple TCP server for text protocol
 */

#pragma once

#include "query/query_parser.h"
#include "index/index.h"
#include "storage/document_store.h"
#include "config/config.h"
#include "server/thread_pool.h"
#include "server/server_stats.h"
#include <string>
#include <thread>
#include <atomic>
#include <memory>
#include <vector>
#include <mutex>
#include <set>

#ifdef USE_MYSQL
namespace mygramdb {
namespace mysql {
class BinlogReader;
}
}
#endif

namespace mygramdb {
namespace server {

/**
 * @brief TCP server configuration
 */
struct ServerConfig {
  std::string host = "0.0.0.0";
  uint16_t port = 11211;  // memcached default port
  int max_connections = 10000;  // Maximum concurrent connections
  int worker_threads = 0;  // Number of worker threads (0 = CPU count)
  int recv_buffer_size = 4096;
  int send_buffer_size = 65536;
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
   * @param index N-gram index reference
   * @param doc_store Document store reference
   * @param ngram_size N-gram size (0 for hybrid mode)
   * @param snapshot_dir Snapshot directory path
   * @param full_config Full application configuration (for CONFIG command)
   * @param binlog_reader Optional binlog reader pointer (for stopping/starting replication)
   */
  TcpServer(ServerConfig config, index::Index& index, storage::DocumentStore& doc_store,
            int ngram_size = 1, std::string snapshot_dir = "./snapshots",
            const config::Config* full_config = nullptr,
#ifdef USE_MYSQL
            mysql::BinlogReader* binlog_reader = nullptr
#else
            void* binlog_reader = nullptr
#endif
  );

  ~TcpServer();

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

 private:
  ServerConfig config_;
  index::Index& index_;
  storage::DocumentStore& doc_store_;
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

  std::string last_error_;
  int ngram_size_;  // N-gram size (0 for hybrid mode)
  std::string snapshot_dir_;  // Snapshot directory
  std::atomic<bool> read_only_{false};  // Read-only mode flag
  const config::Config* full_config_;  // Full configuration for CONFIG command

#ifdef USE_MYSQL
  mysql::BinlogReader* binlog_reader_;  // Optional binlog reader for replication control
#else
  void* binlog_reader_;  // Placeholder when MySQL not compiled
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
   */
  std::string ProcessRequest(const std::string& request);

  /**
   * @brief Format SEARCH response
   */
  std::string FormatSearchResponse(const std::vector<index::DocId>& results,
                                   uint32_t limit, uint32_t offset);

  /**
   * @brief Format COUNT response
   */
  static std::string FormatCountResponse(uint64_t count);

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

}  // namespace server
}  // namespace mygramdb
