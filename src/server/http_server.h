/**
 * @file http_server.h
 * @brief HTTP server for JSON API
 */

#pragma once

// Fix for httplib missing NI_MAXHOST on some platforms
#ifndef NI_MAXHOST
// NOLINTNEXTLINE(cppcoreguidelines-macro-usage) - Required for compatibility with httplib C API
#define NI_MAXHOST 1025
#endif

#include <httplib.h>

#include <atomic>
#include <memory>
#include <nlohmann/json.hpp>
#include <string>
#include <thread>
#include <vector>

#include "config/config.h"
#include "index/index.h"
#include "query/query_parser.h"
#include "server/server_stats.h"
#include "storage/document_store.h"
#include "utils/error.h"
#include "utils/expected.h"
#include "utils/network_utils.h"

#ifdef USE_MYSQL
namespace mygramdb::mysql {
class BinlogReader;
}  // namespace mygramdb::mysql
#endif

namespace mygramdb::cache {
class CacheManager;
}  // namespace mygramdb::cache

namespace mygramdb::server {

// HTTP server configuration defaults
namespace defaults {
constexpr int kHttpPort = 8080;
constexpr int kHttpTimeoutSec = 5;
}  // namespace defaults

/**
 * @brief HTTP server configuration
 */
struct HttpServerConfig {
  std::string bind = "127.0.0.1";
  int port = defaults::kHttpPort;
  int read_timeout_sec = defaults::kHttpTimeoutSec;
  int write_timeout_sec = defaults::kHttpTimeoutSec;
  bool enable_cors = false;
  std::string cors_allow_origin;
  std::vector<std::string> allow_cidrs;
};

// Forward declaration for TableContext
struct TableContext;

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
   * @param table_contexts Map of table name to TableContext pointer
   * @param full_config Full application configuration
   * @param binlog_reader Optional BinlogReader for replication status
   * @param cache_manager Optional CacheManager for cache statistics
   * @param loading Reference to loading flag (shared with TcpServer)
   * @param tcp_stats Optional pointer to TCP server's ServerStats (for /info and /metrics)
   */
  HttpServer(HttpServerConfig config, std::unordered_map<std::string, TableContext*> table_contexts,
             const config::Config* full_config = nullptr,
#ifdef USE_MYSQL
             mysql::BinlogReader* binlog_reader = nullptr,
#else
             void* binlog_reader = nullptr,
#endif
             cache::CacheManager* cache_manager = nullptr, std::atomic<bool>* loading = nullptr,
             ServerStats* tcp_stats = nullptr);

  ~HttpServer();

  // Non-copyable and non-movable (manages server thread)
  HttpServer(const HttpServer&) = delete;
  HttpServer& operator=(const HttpServer&) = delete;
  HttpServer(HttpServer&&) = delete;
  HttpServer& operator=(HttpServer&&) = delete;

  /**
   * @brief Start server (non-blocking)
   * @return Expected<void, Error> - Success or error details
   */
  mygram::utils::Expected<void, mygram::utils::Error> Start();

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
  uint64_t GetTotalRequests() const { return stats_.GetTotalRequests(); }

  /**
   * @brief Get server statistics
   */
  const ServerStats& GetStats() const { return stats_; }

 private:
  HttpServerConfig config_;
  std::unordered_map<std::string, TableContext*> table_contexts_;
  size_t max_query_length_{0};  // Configured max query length limit

  std::atomic<bool> running_{false};

  // Statistics
  ServerStats stats_;

  std::unique_ptr<httplib::Server> server_;
  std::unique_ptr<std::thread> server_thread_;

  const config::Config* full_config_;

#ifdef USE_MYSQL
  mysql::BinlogReader* binlog_reader_;
#else
  void* binlog_reader_;
#endif

  cache::CacheManager* cache_manager_;
  std::vector<utils::CIDR> parsed_allow_cidrs_;
  std::atomic<bool>* loading_;  // Shared loading flag (owned by TcpServer)
  ServerStats* tcp_stats_;      // Pointer to TCP server's statistics (for /info and /metrics)

  /**
   * @brief Setup routes
   */
  void SetupRoutes();

  /**
   * @brief Setup CIDR-based access control
   */
  void SetupAccessControl();

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
   * @brief Handle GET /health (legacy endpoint)
   */
  void HandleHealth(const httplib::Request& req, httplib::Response& res);

  /**
   * @brief Handle GET /health/live (liveness probe)
   */
  void HandleHealthLive(const httplib::Request& req, httplib::Response& res);

  /**
   * @brief Handle GET /health/ready (readiness probe)
   */
  void HandleHealthReady(const httplib::Request& req, httplib::Response& res);

  /**
   * @brief Handle GET /health/detail (detailed health status)
   */
  void HandleHealthDetail(const httplib::Request& req, httplib::Response& res);

  /**
   * @brief Handle GET /config
   */
  void HandleConfig(const httplib::Request& req, httplib::Response& res);

  /**
   * @brief Handle GET /replication/status
   */
  void HandleReplicationStatus(const httplib::Request& req, httplib::Response& res);

  /**
   * @brief Handle GET /metrics (Prometheus format)
   */
  void HandleMetrics(const httplib::Request& req, httplib::Response& res);

  /**
   * @brief Send JSON response
   */
  static void SendJson(httplib::Response& res, int status_code, const nlohmann::json& body);

  /**
   * @brief Send error response
   */
  static void SendError(httplib::Response& res, int status_code, const std::string& message);

  /**
   * @brief CORS middleware
   */
  void SetupCors();
};

}  // namespace mygramdb::server
