/**
 * @file http_server.cpp
 * @brief HTTP server implementation (lifecycle and routing)
 */

#include "server/http_server.h"

#include <spdlog/spdlog.h>

#include <chrono>
#include <future>
#include <nlohmann/json.hpp>

#include "utils/network_utils.h"
#include "utils/structured_log.h"

// Fix for httplib missing NI_MAXHOST on some platforms
#ifndef NI_MAXHOST
#define NI_MAXHOST 1025
#endif

using json = nlohmann::json;

namespace mygramdb::server {

namespace {
// HTTP status codes used by lifecycle/routing code
constexpr int kHttpNoContent = 204;
constexpr int kHttpForbidden = 403;
constexpr int kHttpTooManyRequests = 429;
}  // namespace

HttpServer::HttpServer(HttpServerConfig config, std::unordered_map<std::string, TableContext*> table_contexts,
                       const config::Config* full_config,
#ifdef USE_MYSQL
                       mysql::BinlogReader* binlog_reader,
#else
                       void* binlog_reader,
#endif
                       cache::CacheManager* cache_manager, std::atomic<bool>* loading, ServerStats* tcp_stats)
    : config_(std::move(config)),
      table_contexts_(std::move(table_contexts)),
      full_config_(full_config),
      binlog_reader_(binlog_reader),
      cache_manager_(cache_manager),
      loading_(loading),
      tcp_stats_(tcp_stats) {
  parsed_allow_cidrs_ = mygram::utils::ParseAllowCidrs(config_.allow_cidrs);

  if (full_config_ != nullptr) {
    const auto configured_limit = full_config_->api.max_query_length;
    max_query_length_ = configured_limit <= 0 ? 0 : static_cast<size_t>(configured_limit);

    // Initialize rate limiter (if configured)
    if (full_config_->api.rate_limiting.enable) {
      rate_limiter_ = std::make_unique<RateLimiter>(static_cast<size_t>(full_config_->api.rate_limiting.capacity),
                                                    static_cast<size_t>(full_config_->api.rate_limiting.refill_rate),
                                                    static_cast<size_t>(full_config_->api.rate_limiting.max_clients));
      mygram::utils::StructuredLog()
          .Event("http_rate_limiter_initialized")
          .Field("capacity", static_cast<uint64_t>(full_config_->api.rate_limiting.capacity))
          .Field("refill_rate", static_cast<uint64_t>(full_config_->api.rate_limiting.refill_rate))
          .Field("max_clients", static_cast<uint64_t>(full_config_->api.rate_limiting.max_clients))
          .Info();
    }
  }

  server_ = std::make_unique<httplib::Server>();

  // Set timeouts
  server_->set_read_timeout(config_.read_timeout_sec, 0);
  server_->set_write_timeout(config_.write_timeout_sec, 0);

  // Setup network ACL before registering routes
  SetupAccessControl();

  // Setup routes
  SetupRoutes();

  // Setup CORS if enabled
  if (config_.enable_cors) {
    SetupCors();
  }
}

HttpServer::~HttpServer() {
  Stop();
}

void HttpServer::SetupRoutes() {
  // POST /{table}/search - Full-text search
  // Route pattern: match any non-slash characters to support table names with dashes, dots, or unicode
  server_->Post(R"(/([^/]+)/search)",
                [this](const httplib::Request& req, httplib::Response& res) { HandleSearch(req, res); });

  // POST /{table}/count - Count matching documents
  server_->Post(R"(/([^/]+)/count)",
                [this](const httplib::Request& req, httplib::Response& res) { HandleCount(req, res); });

  // GET /{table}/:id - Get document by ID
  // Route pattern: match any non-slash characters for table name, digits for ID
  server_->Get(R"(/([^/]+)/(\d+))",
               [this](const httplib::Request& req, httplib::Response& res) { HandleGet(req, res); });

  // GET /info - Server information
  server_->Get("/info", [this](const httplib::Request& req, httplib::Response& res) { HandleInfo(req, res); });

  // GET /health - Health check
  // Health check endpoints
  server_->Get("/health", [this](const httplib::Request& req, httplib::Response& res) { HandleHealth(req, res); });
  server_->Get("/health/live",
               [this](const httplib::Request& req, httplib::Response& res) { HandleHealthLive(req, res); });
  server_->Get("/health/ready",
               [this](const httplib::Request& req, httplib::Response& res) { HandleHealthReady(req, res); });
  server_->Get("/health/detail",
               [this](const httplib::Request& req, httplib::Response& res) { HandleHealthDetail(req, res); });

  // GET /config - Configuration
  server_->Get("/config", [this](const httplib::Request& req, httplib::Response& res) { HandleConfig(req, res); });

  // GET /replication/status - Replication status
  server_->Get("/replication/status",
               [this](const httplib::Request& req, httplib::Response& res) { HandleReplicationStatus(req, res); });

  // GET /metrics - Prometheus metrics
  server_->Get("/metrics", [this](const httplib::Request& req, httplib::Response& res) { HandleMetrics(req, res); });
}

void HttpServer::SetupAccessControl() {
  server_->set_pre_routing_handler([this](const httplib::Request& req, httplib::Response& res) {
    const std::string& client_ip = req.remote_addr.empty() ? "unknown" : req.remote_addr;

    // Health check endpoints bypass CIDR and rate limit restrictions
    // (required for Docker HEALTHCHECK, load balancers, and orchestrator probes)
    if (req.path == "/health" || req.path == "/health/live" || req.path == "/health/ready" ||
        req.path == "/health/detail") {
      return httplib::Server::HandlerResponse::Unhandled;
    }

    // Check CIDR-based access control first
    if (!mygram::utils::IsIPAllowed(req.remote_addr, parsed_allow_cidrs_)) {
      stats_.IncrementRequests();
      mygram::utils::StructuredLog()
          .Event("server_warning")
          .Field("type", "http_request_rejected_acl")
          .Field("remote_addr", client_ip)
          .Warn();
      SendError(res, kHttpForbidden, "Access denied by network.allow_cidrs");
      return httplib::Server::HandlerResponse::Handled;
    }

    // Check rate limit (if enabled)
    if (rate_limiter_ && !rate_limiter_->AllowRequest(client_ip)) {
      stats_.IncrementRequests();
      mygram::utils::StructuredLog()
          .Event("server_warning")
          .Field("type", "http_rate_limit_exceeded")
          .Field("client_ip", client_ip)
          .Warn();
      SendError(res, kHttpTooManyRequests, "Rate limit exceeded");
      return httplib::Server::HandlerResponse::Handled;
    }

    return httplib::Server::HandlerResponse::Unhandled;
  });
}

void HttpServer::SetupCors() {
  const std::string allow_origin = config_.cors_allow_origin.empty() ? "null" : config_.cors_allow_origin;

  // CORS preflight
  server_->Options(".*", [allow_origin](const httplib::Request& /*req*/, httplib::Response& res) {
    res.set_header("Access-Control-Allow-Origin", allow_origin);
    res.set_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
    res.set_header("Access-Control-Allow-Headers", "Content-Type");
    res.status = kHttpNoContent;
  });

  // Add CORS headers to all responses
  server_->set_post_routing_handler([allow_origin](const httplib::Request& /*req*/, httplib::Response& res) {
    res.set_header("Access-Control-Allow-Origin", allow_origin);
  });
}

mygram::utils::Expected<void, mygram::utils::Error> HttpServer::Start() {
  using mygram::utils::ErrorCode;
  using mygram::utils::MakeError;
  using mygram::utils::MakeUnexpected;

  if (running_) {
    auto error = MakeError(ErrorCode::kNetworkAlreadyRunning, "Server already running");
    mygram::utils::StructuredLog()
        .Event("server_error")
        .Field("operation", "http_server_start")
        .Field("error", error.to_string())
        .Error();
    return MakeUnexpected(error);
  }

  // Set running flag before starting thread to avoid race condition
  running_ = true;

  // Use promise/future to synchronize with the server thread instead of sleeping.
  // httplib supports bind_to_port() + listen_after_bind() which lets us
  // deterministically know when the port is bound before proceeding.
  auto startup_promise = std::make_shared<std::promise<std::string>>();
  auto startup_future = startup_promise->get_future();

  // Start server in separate thread, capturing error by value via shared_ptr
  server_thread_ = std::make_unique<std::thread>([this, startup_promise]() {
    mygram::utils::StructuredLog()
        .Event("http_server_starting")
        .Field("bind", config_.bind)
        .Field("port", static_cast<uint64_t>(config_.port))
        .Info();

    // Bind to the port first (non-blocking)
    if (!server_->bind_to_port(config_.bind, config_.port)) {
      std::string error_msg = "Failed to bind to " + config_.bind + ":" + std::to_string(config_.port);
      mygram::utils::StructuredLog()
          .Event("server_error")
          .Field("operation", "http_server_listen")
          .Field("bind", config_.bind)
          .Field("port", static_cast<uint64_t>(config_.port))
          .Field("error", error_msg)
          .Error();
      running_ = false;
      startup_promise->set_value(error_msg);
      return;
    }

    // Signal that bind succeeded - server is ready to accept connections
    startup_promise->set_value("");

    // Start listening (blocks until server is stopped)
    if (!server_->listen_after_bind()) {
      running_ = false;
    }
  });

  // Wait for the server thread to signal bind result (with timeout)
  constexpr int kStartupTimeoutSec = 5;
  auto wait_status = startup_future.wait_for(std::chrono::seconds(kStartupTimeoutSec));

  if (wait_status == std::future_status::timeout) {
    // Timeout waiting for server to start
    running_ = false;
    server_->stop();
    if (server_thread_ && server_thread_->joinable()) {
      server_thread_->join();
    }
    auto error = MakeError(ErrorCode::kNetworkBindFailed, "HTTP server startup timed out");
    return MakeUnexpected(error);
  }

  std::string error_msg = startup_future.get();
  if (!error_msg.empty()) {
    if (server_thread_ && server_thread_->joinable()) {
      server_thread_->join();
    }
    auto error = MakeError(ErrorCode::kNetworkBindFailed, error_msg);
    return MakeUnexpected(error);
  }

  mygram::utils::StructuredLog()
      .Event("http_server_started")
      .Field("bind", config_.bind)
      .Field("port", static_cast<uint64_t>(config_.port))
      .Info();
  return {};
}

void HttpServer::Stop() {
  if (!running_) {
    return;
  }

  mygram::utils::StructuredLog().Event("http_server_stopping").Info();
  running_ = false;

  if (server_) {
    server_->stop();
  }

  if (server_thread_ && server_thread_->joinable()) {
    server_thread_->join();
  }

  mygram::utils::StructuredLog().Event("http_server_stopped").Info();
}

}  // namespace mygramdb::server
