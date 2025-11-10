/**
 * @file tcp_server.h
 * @brief Simple TCP server for text protocol
 */

#pragma once

#include "query/query_parser.h"
#include "index/index.h"
#include "storage/document_store.h"
#include <string>
#include <thread>
#include <atomic>
#include <memory>
#include <vector>
#include <mutex>

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
  int max_connections = 1000;
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
   * @param binlog_reader Optional binlog reader pointer (for stopping/starting replication)
   */
  TcpServer(const ServerConfig& config,
            index::Index& index,
            storage::DocumentStore& doc_store,
            int ngram_size = 1,
            const std::string& snapshot_dir = "./snapshots",
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
  size_t GetConnectionCount() const;

  /**
   * @brief Get total requests handled
   */
  uint64_t GetTotalRequests() const { return total_requests_; }

  /**
   * @brief Get last error message
   */
  const std::string& GetLastError() const { return last_error_; }

  /**
   * @brief Get server start time (Unix timestamp)
   */
  uint64_t GetStartTime() const { return start_time_; }

 private:
  ServerConfig config_;
  index::Index& index_;
  storage::DocumentStore& doc_store_;
  query::QueryParser query_parser_;

  std::atomic<bool> running_{false};
  std::atomic<bool> should_stop_{false};
  std::atomic<uint64_t> total_requests_{0};
  uint64_t start_time_ = 0;  // Server start time (Unix timestamp)

  int server_fd_ = -1;
  uint16_t actual_port_ = 0;
  std::unique_ptr<std::thread> accept_thread_;
  std::vector<std::unique_ptr<std::thread>> worker_threads_;
  std::vector<int> active_connections_;
  mutable std::mutex connections_mutex_;

  std::string last_error_;
  int ngram_size_;  // N-gram size (0 for hybrid mode)
  std::string snapshot_dir_;  // Snapshot directory
  std::atomic<bool> read_only_{false};  // Read-only mode flag

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
  std::string FormatCountResponse(uint64_t count);

  /**
   * @brief Format GET response
   */
  std::string FormatGetResponse(const std::optional<storage::Document>& doc);

  /**
   * @brief Format INFO response
   */
  std::string FormatInfoResponse();

  /**
   * @brief Format SAVE response
   */
  std::string FormatSaveResponse(const std::string& filepath);

  /**
   * @brief Format LOAD response
   */
  std::string FormatLoadResponse(const std::string& filepath);

  /**
   * @brief Format REPLICATION STATUS response
   */
  std::string FormatReplicationStatusResponse();

  /**
   * @brief Format REPLICATION STOP response
   */
  std::string FormatReplicationStopResponse();

  /**
   * @brief Format REPLICATION START response
   */
  std::string FormatReplicationStartResponse();

  /**
   * @brief Format error response
   */
  std::string FormatError(const std::string& message);

  /**
   * @brief Set socket options
   */
  bool SetSocketOptions(int fd);

  /**
   * @brief Remove connection from active list
   */
  void RemoveConnection(int fd);
};

}  // namespace server
}  // namespace mygramdb
