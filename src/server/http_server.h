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
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "config/config.h"
#include "index/index.h"
#include "query/query_parser.h"
#include "server/rate_limiter.h"
#include "server/server_stats.h"
#include "storage/document_store.h"
#include "utils/constants.h"
#include "utils/error.h"
#include "utils/expected.h"
#include "utils/network_utils.h"

namespace mygramdb::mysql {
class IBinlogReader;
}  // namespace mygramdb::mysql

namespace mygramdb::cache {
class CacheManager;
}  // namespace mygramdb::cache

namespace mygramdb::server {

// HTTP server configuration defaults
namespace defaults {
constexpr size_t kHttpDefaultMaxBodyBytes = 16 * mygram::constants::kBytesPerMegabyte;  // 16 MiB
}  // namespace defaults

/**
 * @brief HTTP server configuration
 */
struct HttpServerConfig {
  std::string bind = "127.0.0.1";
  int port = config::defaults::kHttpPort;
  int read_timeout_sec = config::defaults::kHttpTimeoutSec;
  int write_timeout_sec = config::defaults::kHttpTimeoutSec;
  bool enable_cors = false;
  std::string cors_allow_origin;
  std::vector<std::string> allow_cidrs;

  /**
   * @brief Maximum HTTP request body size in bytes.
   *
   * cpp-httplib rejects bodies larger than this with HTTP 413 (Payload Too
   * Large) before invoking any handler. Default: 16 MiB. Source of truth is
   * `config::ApiConfig::http::max_body_bytes`.
   */
  size_t max_body_bytes = defaults::kHttpDefaultMaxBodyBytes;

  /**
   * @brief Create HttpServerConfig from application Config
   *
   * @param cfg Application configuration
   * @return Populated HttpServerConfig
   */
  static HttpServerConfig FromConfig(const config::Config& cfg) {
    HttpServerConfig hc;
    hc.bind = cfg.api.http.bind;
    hc.port = cfg.api.http.port;
    hc.enable_cors = cfg.api.http.enable_cors;
    hc.cors_allow_origin = cfg.api.http.cors_allow_origin;
    hc.allow_cidrs = cfg.network.allow_cidrs;
    if (cfg.api.http.max_body_bytes > 0) {
      hc.max_body_bytes = static_cast<size_t>(cfg.api.http.max_body_bytes);
    }
    // 0 / negative timeouts are nonsensical (cpp-httplib would never time out
    // or behave undefined). Treat them as "not configured" and keep the
    // struct default so an operator can opt out by simply omitting the key.
    if (cfg.api.http.read_timeout_sec > 0) {
      hc.read_timeout_sec = cfg.api.http.read_timeout_sec;
    }
    if (cfg.api.http.write_timeout_sec > 0) {
      hc.write_timeout_sec = cfg.api.http.write_timeout_sec;
    }
    return hc;
  }
};

// Forward declaration for TableContext
struct TableContext;

/**
 * @brief HTTP server for JSON API
 *
 * Provides RESTful JSON API:
 * - POST /{table}/search - Full-text search
 * - GET /{table}/:primary_key - Get document by primary key
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
   * @param rate_limiter Optional shared rate limiter. When non-null, this
   *                     instance MUST be the same one used by TcpServer so a
   *                     single client's quota applies across protocols. When
   *                     null, HttpServer creates its own from `full_config`
   *                     for backward compatibility with tests.
   */
  HttpServer(HttpServerConfig config, std::unordered_map<std::string, TableContext*> table_contexts,
             const config::Config* full_config = nullptr, mysql::IBinlogReader* binlog_reader = nullptr,
             cache::CacheManager* cache_manager = nullptr, std::atomic<bool>* loading = nullptr,
             ServerStats* tcp_stats = nullptr, std::shared_ptr<RateLimiter> rate_limiter = nullptr);

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
   * @brief Update API query limits after runtime SET.
   */
  void UpdateApiConfig(int default_limit, int max_query_length);

  /**
   * @brief Get total requests handled.
   *
   * Reads through the *effective* stats source: when a `tcp_stats` pointer was
   * supplied at construction (the embedded-server case driven by
   * ServerLifecycleManager), the count reflects unified TCP+HTTP traffic on
   * that shared instance. Otherwise the local `stats_` is used (the
   * standalone HttpServer mode used by unit tests).
   *
   * Reconciles review finding L-6: previously `stats_` was always exposed
   * even when it was dead (because `RecordRequest()` had already routed
   * counter increments to the shared tcp_stats instance), causing /info to
   * appear to "lose" requests when the dead `stats_` was queried directly.
   */
  uint64_t GetTotalRequests() const { return GetEffectiveStats().GetTotalRequests(); }

  /**
   * @brief Get server statistics (effective source).
   *
   * Returns the same instance that `RecordRequest()` increments — the
   * injected `tcp_stats_` when present, otherwise the local `stats_`.
   * Callers that need to assert on the standalone HTTP-only counters can
   * still inject a private ServerStats and inspect it directly; this
   * accessor never returns the dead local instance when a shared one is
   * configured.
   */
  const ServerStats& GetStats() const { return GetEffectiveStats(); }

 private:
  HttpServerConfig config_;
  // Uses std::unordered_map (not absl::flat_hash_map) to match the
  // std::unordered_map<> parameter type used by ResponseFormatter and
  // StatisticsService. The HTTP handler's table lookup frequency is low
  // enough that the difference is negligible (reviewed: D3 false positive).
  std::unordered_map<std::string, TableContext*> table_contexts_;
  std::atomic<int> default_limit_{config::defaults::kDefaultLimit};
  std::atomic<size_t> max_query_length_{0};  // Configured max query length limit

  // running_ is the canonical lifecycle gate for HttpServer.
  //
  // Contract:
  //   - Start() acquires the gate via compare_exchange_strong(false -> true).
  //     Only the thread that succeeds at this CAS owns the right to spawn the
  //     server thread. All competing concurrent Start() calls observe the gate
  //     as already taken and return ErrorCode::kNetworkAlreadyRunning.
  //   - Stop() releases the gate via compare_exchange_strong(true -> false).
  //     Only the thread that succeeds at this CAS owns the right to stop the
  //     httplib server and join the worker thread. Subsequent Stop() calls
  //     short-circuit.
  //   - Internal failure paths (bind failure inside the worker thread, listen
  //     loop returning false) call store(false) only because they are reached
  //     while the gate is already held by the corresponding Start() invocation.
  //     They therefore "release" the gate the same way Stop() does, which is
  //     safe because the parent Start() has not yet returned success and
  //     concurrent Stop() callers either observe running_=false (and bail) or
  //     race the join via Stop()'s own CAS.
  std::atomic<bool> running_{false};
  std::chrono::steady_clock::time_point start_time_{std::chrono::steady_clock::now()};

  // Statistics
  ServerStats stats_;

  std::unique_ptr<httplib::Server> server_;
  std::unique_ptr<std::thread> server_thread_;

  const config::Config* full_config_;

  mysql::IBinlogReader* binlog_reader_;

  cache::CacheManager* cache_manager_;
  // Rate limiter is held as shared_ptr so the same instance can be co-owned
  // by TcpServer and HttpServer. A single client's quota MUST apply across
  // protocols; two independent limiters give the client effectively 2x the
  // configured limit. When the parent (ServerLifecycleManager / TcpServer)
  // does not provide one, HttpServer falls back to constructing its own from
  // `full_config_->api.rate_limiting`.
  std::shared_ptr<RateLimiter> rate_limiter_;
  std::vector<mygram::utils::CIDR> parsed_allow_cidrs_;
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
   * @brief Result of a successful PrepareHttpSearchQuery call.
   *
   * Holds non-owning pointers and a parsed Query that callers use to drive
   * the unified search pipeline. The pointer fields outlive the request so
   * long as `table_contexts_` is not mutated mid-request.
   */
  struct PreparedHttpQuery {
    TableContext* table_ctx = nullptr;
    nlohmann::json body;
    query::Query query;
  };

  /**
   * @brief Resolution result for a request-bound table context.
   *
   * Carries the (already-validated) `TableContext*` and an HTTP status code
   * describing the failure mode. On success the pointer is non-null and
   * `status` is 200; on failure the pointer is null, `status` carries the
   * intended HTTP code, and `message` is the user-facing reason.
   */
  struct TableContextLookup {
    TableContext* table_ctx = nullptr;  ///< Non-null on success.
    int status = 0;                     ///< HTTP status (0 means uninitialized).
    std::string message;                ///< Error message for failures.
  };

  /**
   * @brief Validate a URL-bound table name and resolve its TableContext.
   *
   * Centralises the trio of checks that HandleSearch / HandleCount / HandleGet
   * each performed inline:
   *   1. `IsValidTableName` — reject names that would break the parser
   *      grammar (uniform across all endpoints).
   *   2. `table_contexts_.find` — ensure the table exists.
   *   3. Non-null `index` and `doc_store` — defensive against partially
   *      initialised contexts.
   *
   * @param table_name URL-extracted table name (raw match[1]).
   * @return `TableContextLookup` whose `table_ctx` is non-null on success and
   *         whose `status` / `message` describe the error otherwise.
   */
  TableContextLookup ResolveHttpTableContext(const std::string& table_name);

  /**
   * @brief Run the shared HandleSearch/HandleCount preamble.
   *
   * Validates the URL table name, looks up the table context, parses the
   * JSON body, validates the `q` field (presence, type, control characters,
   * reserved clause keywords), invokes QueryParser, and parses the optional
   * `filters` JSON object into the resulting query. On failure, sends an
   * appropriately-coded HTTP error response and returns std::nullopt; the
   * caller should return immediately. On success, the caller continues with
   * pipeline parameter construction and execution.
   *
   * @param req         HTTP request (path matches and JSON body).
   * @param res         HTTP response (populated on error).
   * @param command     Parser command verb ("SEARCH" or "COUNT").
   * @param apply_pagination If true, propagate `limit`/`offset` from the JSON
   *                    body into the parser query string and apply the
   *                    configured default limit when none was supplied.
   * @return Prepared query on success, std::nullopt on failure (response
   *         already populated).
   */
  std::optional<PreparedHttpQuery> PrepareHttpSearchQuery(const httplib::Request& req, httplib::Response& res,
                                                          const std::string& command, bool apply_pagination);

  /**
   * @brief Handle POST /{table}/search
   */
  void HandleSearch(const httplib::Request& req, httplib::Response& res);

  /**
   * @brief Handle POST /{table}/count
   */
  void HandleCount(const httplib::Request& req, httplib::Response& res);

  /**
   * @brief Handle GET /{table}/:primary_key
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
   * @brief Increment the request counter on the effective stats instance.
   *
   * When a TCP server's ServerStats has been provided (`tcp_stats_`), the
   * counter is incremented there so /info and /metrics report unified
   * cross-protocol totals. Otherwise the HTTP-only `stats_` is used.
   */
  void RecordRequest() { (tcp_stats_ != nullptr ? tcp_stats_ : &stats_)->IncrementRequests(); }

  /**
   * @brief Increment a command counter on the effective stats instance.
   */
  void RecordCommand(query::QueryType type) { GetEffectiveStats().IncrementCommand(type); }

  /**
   * @brief Resolve the effective stats source (shared TCP stats or local).
   *
   * Centralises the "tcp_stats_ if non-null else stats_" choice that
   * RecordRequest() and the public GetStats()/GetTotalRequests() accessors
   * make. Keeps the three call sites in lockstep so an /info request never
   * observes a stats source different from the one RecordRequest() updated.
   */
  const ServerStats& GetEffectiveStats() const { return tcp_stats_ != nullptr ? *tcp_stats_ : stats_; }
  ServerStats& GetEffectiveStats() { return tcp_stats_ != nullptr ? *tcp_stats_ : stats_; }

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
