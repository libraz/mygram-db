/**
 * @file http_server.h
 * @brief HTTP server for JSON API
 */

#pragma once

// Fix for httplib missing NI_MAXHOST on some platforms
#ifndef NI_MAXHOST
#define NI_MAXHOST 1025
#endif

#include "query/query_parser.h"
#include "index/index.h"
#include "storage/document_store.h"
#include "config/config.h"
#include <httplib.h>
#include <nlohmann/json.hpp>
#include <string>
#include <atomic>
#include <memory>
#include <thread>

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
 * @brief HTTP server configuration
 */
struct HttpServerConfig {
  std::string bind = "127.0.0.1";
  int port = 8080;
  int read_timeout_sec = 5;
  int write_timeout_sec = 5;
  bool enable_cors = true;
};

/**
 * @brief HTTP server for JSON API
 *
 * Provides RESTful JSON API:
 * - POST /{table}/search - Full-text search
 * - GET /{table}/:id - Get document by ID
 * - GET /info - Server information
 * - GET /health - Health check
 */
class HttpServer {
 public:
  /**
   * @brief Construct HTTP server
   * @param config Server configuration
   * @param index N-gram index reference
   * @param doc_store Document store reference
   * @param ngram_size N-gram size (0 for hybrid mode)
   * @param full_config Full application configuration
   * @param binlog_reader Optional binlog reader pointer
   */
  HttpServer(HttpServerConfig config, index::Index& index, storage::DocumentStore& doc_store,
             int ngram_size = 1,
             const config::Config* full_config = nullptr,
#ifdef USE_MYSQL
             mysql::BinlogReader* binlog_reader = nullptr
#else
             void* binlog_reader = nullptr
#endif
  );

  ~HttpServer();

  /**
   * @brief Start server (non-blocking)
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
  int GetPort() const { return config_.port; }

  /**
   * @brief Get total requests handled
   */
  uint64_t GetTotalRequests() const { return total_requests_; }

  /**
   * @brief Get last error message
   */
  const std::string& GetLastError() const { return last_error_; }

 private:
  HttpServerConfig config_;
  index::Index& index_;
  storage::DocumentStore& doc_store_;
  query::QueryParser query_parser_;

  std::atomic<bool> running_{false};
  std::atomic<uint64_t> total_requests_{0};
  uint64_t start_time_ = 0;  // Server start time (Unix timestamp)

  std::unique_ptr<httplib::Server> server_;
  std::unique_ptr<std::thread> server_thread_;

  std::string last_error_;
  int ngram_size_;
  const config::Config* full_config_;

#ifdef USE_MYSQL
  mysql::BinlogReader* binlog_reader_;
#else
  void* binlog_reader_;
#endif

  /**
   * @brief Setup routes
   */
  void SetupRoutes();

  /**
   * @brief Handle POST /{table}/search
   */
  void HandleSearch(const httplib::Request& req, httplib::Response& res);

  /**
   * @brief Handle GET /{table}/:id
   */
  void HandleGet(const httplib::Request& req, httplib::Response& res);

  /**
   * @brief Handle GET /info
   */
  void HandleInfo(const httplib::Request& req, httplib::Response& res);

  /**
   * @brief Handle GET /health
   */
  void HandleHealth(const httplib::Request& req, httplib::Response& res);

  /**
   * @brief Handle GET /config
   */
  void HandleConfig(const httplib::Request& req, httplib::Response& res);

  /**
   * @brief Handle GET /replication/status
   */
  void HandleReplicationStatus(const httplib::Request& req, httplib::Response& res);

  /**
   * @brief Send JSON response
   */
  void SendJson(httplib::Response& res, int status_code, const nlohmann::json& body);

  /**
   * @brief Send error response
   */
  void SendError(httplib::Response& res, int status_code, const std::string& message);

  /**
   * @brief CORS middleware
   */
  void SetupCors();
};

}  // namespace server
}  // namespace mygramdb
