/**
 * @file http_server.cpp
 * @brief HTTP server implementation
 */

#include "server/http_server.h"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <chrono>
#include <future>
#include <nlohmann/json.hpp>
#include <sstream>
#include <type_traits>
#include <variant>

#include "cache/cache_manager.h"
#include "query/query_parser.h"
#include "query/result_sorter.h"
#include "server/handlers/search_handler.h"
#include "server/response_formatter.h"
#include "server/search_pipeline.h"
#include "server/statistics_service.h"
#include "server/tcp_server.h"  // For TableContext definition
#include "storage/document_store.h"
#include "utils/memory_utils.h"
#include "utils/network_utils.h"
#include "utils/string_utils.h"
#include "utils/structured_log.h"
#include "version.h"

// Fix for httplib missing NI_MAXHOST on some platforms
#ifndef NI_MAXHOST
#define NI_MAXHOST 1025
#endif

#include "mysql/binlog_reader_interface.h"

using json = nlohmann::json;

namespace mygramdb::server {

namespace {
// HTTP status codes
constexpr int kHttpOk = 200;
constexpr int kHttpNoContent = 204;
constexpr int kHttpBadRequest = 400;
constexpr int kHttpForbidden = 403;
constexpr int kHttpNotFound = 404;
constexpr int kHttpTooManyRequests = 429;
constexpr int kHttpInternalServerError = 500;
constexpr int kHttpServiceUnavailable = 503;

// Server startup timeout (seconds)
constexpr int kStartupTimeoutSec = 5;

std::vector<mygram::utils::CIDR> ParseAllowCidrs(const std::vector<std::string>& allow_cidrs) {
  std::vector<mygram::utils::CIDR> parsed;
  parsed.reserve(allow_cidrs.size());

  for (const auto& cidr_str : allow_cidrs) {
    auto cidr = mygram::utils::CIDR::Parse(cidr_str);
    if (!cidr) {
      mygram::utils::StructuredLog()
          .Event("server_warning")
          .Field("type", "invalid_cidr_entry")
          .Field("cidr", cidr_str)
          .Warn();
      continue;
    }
    parsed.push_back(*cidr);
  }

  return parsed;
}

json FilterValueToJson(const storage::FilterValue& value) {
  json serialized = nullptr;
  std::visit(
      [&](const auto& arg) {
        using T = std::decay_t<decltype(arg)>;
        if constexpr (std::is_same_v<T, std::monostate>) {
          serialized = nullptr;
        } else if constexpr (std::is_same_v<T, storage::TimeValue>) {
          // TimeValue: serialize as seconds
          serialized = arg.seconds;
        } else {
          serialized = arg;
        }
      },
      value);
  return serialized;
}
/**
 * @brief Parse filter conditions from a JSON "filters" object into a query
 *
 * Supports two formats:
 * - Format 1: {"col": "value"} or {"col": 123} - defaults to EQ operator
 * - Format 2: {"col": {"op": "GT", "value": "10"}} - full operator support
 *
 * @param filters_json The JSON object containing filter definitions
 * @param query The query to populate with parsed filter conditions
 * @param[out] error_message Set to error description on failure
 * @return true on success, false on parse error (error_message is set)
 */
bool ParseFiltersFromJson(const json& filters_json, query::Query& query, std::string& error_message) {
  query.filters.clear();
  for (const auto& [key, val] : filters_json.items()) {
    query::FilterCondition filter;
    filter.column = key;

    // Check if value is an object with operator specification
    if (val.is_object() && val.contains("value")) {
      // Format 2: full operator support
      std::string op_str = val.value("op", "EQ");
      auto parsed_op = query::QueryParser::ParseFilterOp(op_str);
      if (!parsed_op.has_value()) {
        error_message = "Invalid filter operator: " + op_str;
        return false;
      }
      filter.op = parsed_op.value();

      // Get the value
      const json& value_field = val["value"];
      if (value_field.is_string()) {
        filter.value = value_field.get<std::string>();
      } else if (value_field.is_number_integer()) {
        filter.value = std::to_string(value_field.get<int64_t>());
      } else if (value_field.is_number_float()) {
        filter.value = std::to_string(value_field.get<double>());
      } else if (value_field.is_boolean()) {
        filter.value = value_field.get<bool>() ? "1" : "0";
      } else {
        error_message = "Invalid filter value type for column: " + key;
        return false;
      }
    } else {
      // Format 1: backward compatible (defaults to EQ)
      filter.op = query::FilterOp::EQ;
      if (val.is_string()) {
        filter.value = val.get<std::string>();
      } else if (val.is_number_integer()) {
        filter.value = std::to_string(val.get<int64_t>());
      } else if (val.is_number_float()) {
        filter.value = std::to_string(val.get<double>());
      } else if (val.is_boolean()) {
        filter.value = val.get<bool>() ? "1" : "0";
      } else {
        error_message = "Invalid filter value type for column: " + key;
        return false;
      }
    }

    query.filters.push_back(std::move(filter));
  }
  return true;
}

}  // namespace

using storage::DocId;

HttpServer::HttpServer(HttpServerConfig config, std::unordered_map<std::string, TableContext*> table_contexts,
                       const config::Config* full_config, mysql::IBinlogReader* binlog_reader,
                       cache::CacheManager* cache_manager, std::atomic<bool>* loading, ServerStats* tcp_stats)
    : config_(std::move(config)),
      table_contexts_(std::move(table_contexts)),
      full_config_(full_config),
      binlog_reader_(binlog_reader),
      cache_manager_(cache_manager),
      loading_(loading),
      tcp_stats_(tcp_stats) {
  parsed_allow_cidrs_ = ParseAllowCidrs(config_.allow_cidrs);

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

  // Use a promise/future to safely communicate bind result from the thread
  auto start_promise = std::make_shared<std::promise<std::string>>();
  auto start_future = start_promise->get_future();

  // Start server in separate thread
  server_thread_ = std::make_unique<std::thread>([this, start_promise]() {
    mygram::utils::StructuredLog()
        .Event("http_server_starting")
        .Field("bind", config_.bind)
        .Field("port", static_cast<uint64_t>(config_.port))
        .Info();

    // Bind first, then signal success/failure before blocking on listen
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
      start_promise->set_value(std::move(error_msg));
      return;
    }

    // Bind succeeded, signal the caller
    start_promise->set_value("");

    // Block on accepting connections (runs until server_->stop() is called)
    if (!server_->listen_after_bind()) {
      running_ = false;
    }
  });

  // Wait for the thread to report bind result (with timeout)
  auto status = start_future.wait_for(std::chrono::seconds(kStartupTimeoutSec));
  if (status == std::future_status::timeout) {
    // Timed out waiting for bind; stop the server and join
    server_->stop();
    if (server_thread_ && server_thread_->joinable()) {
      server_thread_->join();
    }
    running_ = false;
    auto error = MakeError(ErrorCode::kNetworkBindFailed, "HTTP server startup timed out");
    return MakeUnexpected(error);
  }

  std::string thread_error = start_future.get();
  if (!thread_error.empty()) {
    if (server_thread_ && server_thread_->joinable()) {
      server_thread_->join();
    }
    auto error = MakeError(ErrorCode::kNetworkBindFailed, thread_error);
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

void HttpServer::HandleSearch(const httplib::Request& req, httplib::Response& res) {
  stats_.IncrementRequests();

  try {
    // Check if server is loading
    if (loading_ != nullptr && loading_->load()) {
      SendError(res, kHttpServiceUnavailable, "Server is loading, please try again later");
      return;
    }

    // Extract table name from URL
    std::string table = req.matches[1];

    // Lookup table
    auto table_iter = table_contexts_.find(table);
    if (table_iter == table_contexts_.end()) {
      SendError(res, kHttpNotFound, "Table not found: " + table);
      return;
    }
    if (!table_iter->second->index || !table_iter->second->doc_store) {
      SendError(res, kHttpInternalServerError, "Table context has null index or doc_store");
      return;
    }
    auto* table_ctx = table_iter->second;
    auto* current_doc_store = table_ctx->doc_store.get();

    // Parse JSON body
    json body;
    try {
      body = json::parse(req.body);
    } catch (const json::parse_error& e) {
      SendError(res, kHttpBadRequest, "Invalid JSON: " + std::string(e.what()));
      return;
    }

    // Validate required field
    if (!body.contains("q")) {
      SendError(res, kHttpBadRequest, "Missing required field: q");
      return;
    }

    // Validate query text for control characters (CRLF injection prevention)
    std::string query_text = body["q"].get<std::string>();
    for (char c : query_text) {
      if (c == '\r' || c == '\n' || c == '\0') {
        SendError(res, kHttpBadRequest, "Query text contains invalid control characters");
        return;
      }
    }

    // Validate table name for control characters
    for (char c : table) {
      if (c == '\r' || c == '\n' || c == '\0') {
        SendError(res, kHttpBadRequest, "Table name contains invalid control characters");
        return;
      }
    }

    // Build query string for QueryParser
    std::ostringstream query_str;
    query_str << "SEARCH " << table << " " << query_text;

    // Add limit
    if (body.contains("limit")) {
      if (!body["limit"].is_number_integer()) {
        SendError(res, kHttpBadRequest, "Invalid limit: must be an integer");
        return;
      }
      query_str << " LIMIT " << body["limit"].get<int>();
    }

    // Add offset
    if (body.contains("offset")) {
      if (!body["offset"].is_number_integer()) {
        SendError(res, kHttpBadRequest, "Invalid offset: must be an integer");
        return;
      }
      query_str << " OFFSET " << body["offset"].get<int>();
    }

    // Parse and execute query (use per-request parser to avoid data race)
    query::QueryParser query_parser;
    if (max_query_length_ > 0) {
      query_parser.SetMaxQueryLength(max_query_length_);
    }
    auto query = query_parser.Parse(query_str.str());
    if (!query) {
      SendError(res, kHttpBadRequest, "Invalid query: " + query.error().message());
      return;
    }

    // Apply default limit if LIMIT was not explicitly specified in the request
    if (!query->limit_explicit && full_config_ != nullptr) {
      query->limit = static_cast<size_t>(full_config_->api.default_limit);
    }

    // Apply filters from JSON payload
    if (body.contains("filters") && body["filters"].is_object()) {
      std::string filter_error;
      if (!ParseFiltersFromJson(body["filters"], *query, filter_error)) {
        SendError(res, kHttpBadRequest, filter_error);
        return;
      }
    }

    // Build pipeline parameters from table context
    search_pipeline::FullPipelineParams params;
    params.current_index = table_ctx->index.get();
    params.current_doc_store = current_doc_store;
    params.full_config = full_config_;
    params.cache_manager = cache_manager_;
    params.ngram_size = table_ctx->config.ngram_size;
    params.kanji_ngram_size = table_ctx->config.kanji_ngram_size;
    params.cross_boundary_ngrams = table_ctx->config.cross_boundary_ngrams;
    params.filter_threshold = SearchHandler::GetFilterThreshold();
    params.primary_key_column = table_ctx->config.primary_key;
    params.bm25_stats = &table_ctx->bm25_stats;

    // Set synonym dictionary if available
    if (table_ctx->synonym_dict && !table_ctx->synonym_dict->IsEmpty()) {
      params.synonym_dict = table_ctx->synonym_dict.get();
    }

    // Execute the unified search pipeline
    auto pipeline_output = search_pipeline::ExecuteFullPipeline(*query, params);
    if (!pipeline_output.success) {
      SendError(res, kHttpInternalServerError, pipeline_output.error_message);
      return;
    }

    auto& results = pipeline_output.results;
    size_t total_count = results.size();

    // Apply ORDER BY, LIMIT, OFFSET
    std::vector<DocId> sorted_results;
    if (query->order_by.has_value()) {
      auto result =
          query::ResultSorter::SortAndPaginate(results, *current_doc_store, *query, params.primary_key_column);
      if (!result.has_value()) {
        SendError(res, kHttpBadRequest, result.error().message());
        return;
      }
      sorted_results = std::move(result.value());
    } else {
      // No ORDER BY: apply limit/offset directly (preserve DocID order)
      size_t start_idx = std::min(static_cast<size_t>(query->offset), results.size());
      size_t end_idx = std::min(start_idx + query->limit, results.size());

      if (start_idx < results.size()) {
        sorted_results =
            std::vector<DocId>(results.begin() + static_cast<std::vector<DocId>::difference_type>(start_idx),
                               results.begin() + static_cast<std::vector<DocId>::difference_type>(end_idx));
      }
    }

    // Build JSON response
    json response;
    response["count"] = total_count;
    response["limit"] = query->limit;
    response["offset"] = query->offset;

    json results_array = json::array();
    for (const auto& doc_id : sorted_results) {
      auto doc = current_doc_store->GetDocument(doc_id);
      if (doc) {
        json doc_obj;
        doc_obj["doc_id"] = doc->doc_id;
        doc_obj["primary_key"] = doc->primary_key;

        // Add filters
        if (!doc->filters.empty()) {
          json filters_obj;
          for (const auto& [key, val] : doc->filters) {
            filters_obj[key] = FilterValueToJson(val);
          }
          doc_obj["filters"] = filters_obj;
        }

        results_array.push_back(doc_obj);
      }
    }
    response["results"] = results_array;

    SendJson(res, kHttpOk, response);

  } catch (const std::exception& e) {
    mygram::utils::StructuredLog()
        .Event("http_handler_error")
        .Field("handler", "search")
        .Field("error", e.what())
        .Error();
    SendError(res, kHttpInternalServerError, "Internal server error");
  }
}

void HttpServer::HandleCount(const httplib::Request& req, httplib::Response& res) {
  stats_.IncrementRequests();

  try {
    // Check if server is loading
    if (loading_ != nullptr && loading_->load()) {
      SendError(res, kHttpServiceUnavailable, "Server is loading, please try again later");
      return;
    }

    // Extract table name from URL
    std::string table = req.matches[1];

    // Lookup table
    auto table_iter = table_contexts_.find(table);
    if (table_iter == table_contexts_.end()) {
      SendError(res, kHttpNotFound, "Table not found: " + table);
      return;
    }
    if (!table_iter->second->index || !table_iter->second->doc_store) {
      SendError(res, kHttpInternalServerError, "Table context has null index or doc_store");
      return;
    }
    auto* table_ctx = table_iter->second;

    // Parse JSON body
    json body;
    try {
      body = json::parse(req.body);
    } catch (const json::parse_error& e) {
      SendError(res, kHttpBadRequest, "Invalid JSON: " + std::string(e.what()));
      return;
    }

    // Validate required field
    if (!body.contains("q")) {
      SendError(res, kHttpBadRequest, "Missing required field: q");
      return;
    }

    // Validate query text for control characters (CRLF injection prevention)
    std::string query_text = body["q"].get<std::string>();
    for (char c : query_text) {
      if (c == '\r' || c == '\n' || c == '\0') {
        SendError(res, kHttpBadRequest, "Query text contains invalid control characters");
        return;
      }
    }

    // Validate table name for control characters
    for (char c : table) {
      if (c == '\r' || c == '\n' || c == '\0') {
        SendError(res, kHttpBadRequest, "Table name contains invalid control characters");
        return;
      }
    }

    // Build query string for QueryParser (COUNT query)
    std::ostringstream query_str;
    query_str << "COUNT " << table << " " << query_text;

    // Parse and execute query
    query::QueryParser query_parser;
    if (max_query_length_ > 0) {
      query_parser.SetMaxQueryLength(max_query_length_);
    }
    auto query = query_parser.Parse(query_str.str());
    if (!query) {
      SendError(res, kHttpBadRequest, "Invalid query: " + query.error().message());
      return;
    }

    // Apply filters from JSON payload (same logic as search)
    if (body.contains("filters") && body["filters"].is_object()) {
      std::string filter_error;
      if (!ParseFiltersFromJson(body["filters"], *query, filter_error)) {
        SendError(res, kHttpBadRequest, filter_error);
        return;
      }
    }

    // Build pipeline parameters from table context
    search_pipeline::FullPipelineParams params;
    params.current_index = table_ctx->index.get();
    params.current_doc_store = table_ctx->doc_store.get();
    params.full_config = full_config_;
    params.cache_manager = cache_manager_;
    params.ngram_size = table_ctx->config.ngram_size;
    params.kanji_ngram_size = table_ctx->config.kanji_ngram_size;
    params.cross_boundary_ngrams = table_ctx->config.cross_boundary_ngrams;
    params.filter_threshold = SearchHandler::GetFilterThreshold();
    params.primary_key_column = table_ctx->config.primary_key;

    // Set synonym dictionary if available
    if (table_ctx->synonym_dict && !table_ctx->synonym_dict->IsEmpty()) {
      params.synonym_dict = table_ctx->synonym_dict.get();
    }

    // Execute the unified search pipeline
    auto pipeline_output = search_pipeline::ExecuteFullPipeline(*query, params);
    if (!pipeline_output.success) {
      SendError(res, kHttpInternalServerError, pipeline_output.error_message);
      return;
    }

    // Build JSON response - just return count
    json response;
    response["count"] = pipeline_output.results.size();

    SendJson(res, kHttpOk, response);

  } catch (const std::exception& e) {
    mygram::utils::StructuredLog()
        .Event("http_handler_error")
        .Field("handler", "count")
        .Field("error", e.what())
        .Error();
    SendError(res, kHttpInternalServerError, "Internal server error");
  }
}

void HttpServer::HandleGet(const httplib::Request& req, httplib::Response& res) {
  stats_.IncrementRequests();

  try {
    // Check if server is loading
    if (loading_ != nullptr && loading_->load()) {
      SendError(res, kHttpServiceUnavailable, "Server is loading, please try again later");
      return;
    }

    // Extract table name and ID from URL
    std::string table = req.matches[1];
    std::string id_str = req.matches[2];

    // Lookup table
    auto table_iter = table_contexts_.find(table);
    if (table_iter == table_contexts_.end()) {
      SendError(res, kHttpNotFound, "Table not found: " + table);
      return;
    }
    if (!table_iter->second->doc_store) {
      SendError(res, kHttpInternalServerError, "Table context has null doc_store");
      return;
    }
    auto* current_doc_store = table_iter->second->doc_store.get();

    // Parse ID
    uint64_t doc_id = 0;
    try {
      doc_id = std::stoull(id_str);
    } catch (const std::exception& e) {
      SendError(res, kHttpBadRequest, "Invalid document ID");
      return;
    }

    // Get document
    auto doc = current_doc_store->GetDocument(doc_id);
    if (!doc) {
      SendError(res, kHttpNotFound, "Document not found");
      return;
    }

    // Build JSON response
    json response;
    response["doc_id"] = doc->doc_id;
    response["primary_key"] = doc->primary_key;

    if (!doc->filters.empty()) {
      json filters_obj;
      for (const auto& [key, val] : doc->filters) {
        filters_obj[key] = FilterValueToJson(val);
      }
      response["filters"] = filters_obj;
    }

    SendJson(res, kHttpOk, response);

  } catch (const std::exception& e) {
    mygram::utils::StructuredLog().Event("http_handler_error").Field("handler", "get").Field("error", e.what()).Error();
    SendError(res, kHttpInternalServerError, "Internal server error");
  }
}

void HttpServer::HandleInfo(const httplib::Request& /*req*/, httplib::Response& res) {
  // Increment request counter on the effective stats instance
  if (tcp_stats_ != nullptr) {
    tcp_stats_->IncrementRequests();
  } else {
    stats_.IncrementRequests();
  }

  try {
    json response;

    // Use TCP server's stats if available (includes all protocol stats), otherwise use HTTP-only stats
    const ServerStats& effective_stats = (tcp_stats_ != nullptr) ? *tcp_stats_ : stats_;

    // Server info
    response["server"] = "MygramDB";
    response["version"] = ::mygramdb::Version::String();
    response["uptime_seconds"] = effective_stats.GetUptimeSeconds();

    // Statistics (from TCP server if available)
    auto srv_stats = effective_stats.GetStatistics();
    response["total_requests"] = srv_stats.total_requests;
    response["total_commands_processed"] = srv_stats.total_commands_processed;

    // Aggregate memory and index statistics across all tables
    size_t total_index_memory = 0;
    size_t total_doc_memory = 0;
    size_t total_documents = 0;
    size_t total_terms = 0;
    size_t total_postings = 0;
    size_t total_delta_encoded = 0;
    size_t total_roaring_bitmap = 0;

    json tables_obj;
    for (const auto& [table_name, ctx] : table_contexts_) {
      size_t index_mem = ctx->index->MemoryUsage();
      size_t doc_mem = ctx->doc_store->MemoryUsage();
      auto idx_stats = ctx->index->GetStatistics();

      total_index_memory += index_mem;
      total_doc_memory += doc_mem;
      total_documents += ctx->doc_store->Size();
      total_terms += idx_stats.total_terms;
      total_postings += idx_stats.total_postings;
      total_delta_encoded += idx_stats.delta_encoded_lists;
      total_roaring_bitmap += idx_stats.roaring_bitmap_lists;

      // Per-table stats
      json table_obj;
      table_obj["documents"] = ctx->doc_store->Size();
      table_obj["terms"] = idx_stats.total_terms;
      table_obj["postings"] = idx_stats.total_postings;
      table_obj["ngram_size"] = ctx->config.ngram_size;
      table_obj["memory_bytes"] = index_mem + doc_mem;
      table_obj["memory_human"] = mygram::utils::FormatBytes(index_mem + doc_mem);
      tables_obj[table_name] = table_obj;
    }

    size_t total_memory = total_index_memory + total_doc_memory;

    // Update memory usage on the effective stats instance
    if (tcp_stats_ != nullptr) {
      tcp_stats_->UpdateMemoryUsage(total_memory);
    } else {
      stats_.UpdateMemoryUsage(total_memory);
    }

    json memory_obj;
    memory_obj["used_memory_bytes"] = total_memory;
    memory_obj["used_memory_human"] = mygram::utils::FormatBytes(total_memory);
    memory_obj["peak_memory_bytes"] = effective_stats.GetPeakMemoryUsage();
    memory_obj["peak_memory_human"] = mygram::utils::FormatBytes(effective_stats.GetPeakMemoryUsage());
    memory_obj["used_memory_index"] = mygram::utils::FormatBytes(total_index_memory);
    memory_obj["used_memory_documents"] = mygram::utils::FormatBytes(total_doc_memory);

    // System memory information
    auto sys_info = mygram::utils::GetSystemMemoryInfo();
    if (sys_info) {
      memory_obj["total_system_memory"] = sys_info->total_physical_bytes;
      memory_obj["total_system_memory_human"] = mygram::utils::FormatBytes(sys_info->total_physical_bytes);
      memory_obj["available_system_memory"] = sys_info->available_physical_bytes;
      memory_obj["available_system_memory_human"] = mygram::utils::FormatBytes(sys_info->available_physical_bytes);
      if (sys_info->total_physical_bytes > 0) {
        double usage_ratio = 1.0 - static_cast<double>(sys_info->available_physical_bytes) /
                                       static_cast<double>(sys_info->total_physical_bytes);
        memory_obj["system_memory_usage_ratio"] = usage_ratio;
      }
    }

    // Process memory information
    auto proc_info = mygram::utils::GetProcessMemoryInfo();
    if (proc_info) {
      memory_obj["process_rss"] = proc_info->rss_bytes;
      memory_obj["process_rss_human"] = mygram::utils::FormatBytes(proc_info->rss_bytes);
      memory_obj["process_rss_peak"] = proc_info->peak_rss_bytes;
      memory_obj["process_rss_peak_human"] = mygram::utils::FormatBytes(proc_info->peak_rss_bytes);
    }

    // Memory health status
    auto health = mygram::utils::GetMemoryHealthStatus();
    memory_obj["memory_health"] = mygram::utils::MemoryHealthStatusToString(health);

    response["memory"] = memory_obj;

    // Aggregated index statistics
    json index_obj;
    index_obj["total_documents"] = total_documents;
    index_obj["total_terms"] = total_terms;
    index_obj["total_postings"] = total_postings;
    if (total_terms > 0) {
      index_obj["avg_postings_per_term"] = static_cast<double>(total_postings) / static_cast<double>(total_terms);
    }
    index_obj["delta_encoded_lists"] = total_delta_encoded;
    index_obj["roaring_bitmap_lists"] = total_roaring_bitmap;
    response["index"] = index_obj;

    // Per-table breakdown
    response["tables"] = tables_obj;

    // Cache statistics
    json cache_obj;
    if (cache_manager_ != nullptr && cache_manager_->IsEnabled()) {
      auto cache_stats = cache_manager_->GetStatistics();
      cache_obj["enabled"] = true;
      cache_obj["hits"] = cache_stats.cache_hits;
      cache_obj["misses"] = cache_stats.cache_misses;
      cache_obj["misses_not_found"] = cache_stats.cache_misses_not_found;
      cache_obj["misses_ttl_expired"] = cache_stats.cache_misses_ttl_expired;
      cache_obj["misses_invalidated"] = cache_stats.cache_misses_invalidated;
      cache_obj["total_queries"] = cache_stats.total_queries;
      cache_obj["hit_rate"] = cache_stats.HitRate();
      cache_obj["current_entries"] = cache_stats.current_entries;
      cache_obj["memory_bytes"] = cache_stats.current_memory_bytes;
      cache_obj["memory_human"] = mygram::utils::FormatBytes(cache_stats.current_memory_bytes);
      cache_obj["evictions"] = cache_stats.evictions;
      cache_obj["ttl_expirations"] = cache_stats.ttl_expirations;
      cache_obj["invalidations_immediate"] = cache_stats.invalidations_immediate;
      cache_obj["invalidations_deferred"] = cache_stats.invalidations_deferred;
      cache_obj["invalidations_batches"] = cache_stats.invalidations_batches;
      cache_obj["avg_hit_latency_ms"] = cache_stats.AverageCacheHitLatency();
      cache_obj["avg_miss_latency_ms"] = cache_stats.AverageCacheMissLatency();
      cache_obj["total_time_saved_ms"] = cache_stats.TotalTimeSaved();
    } else {
      cache_obj["enabled"] = false;
    }
    response["cache"] = cache_obj;

    SendJson(res, kHttpOk, response);

  } catch (const std::exception& e) {
    mygram::utils::StructuredLog()
        .Event("http_handler_error")
        .Field("handler", "info")
        .Field("error", e.what())
        .Error();
    SendError(res, kHttpInternalServerError, "Internal server error");
  }
}

void HttpServer::HandleHealth(const httplib::Request& /*req*/, httplib::Response& res) {
  stats_.IncrementRequests();

  json response;
  response["status"] = "ok";
  response["timestamp"] =
      std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();

  SendJson(res, kHttpOk, response);
}

void HttpServer::HandleHealthLive(const httplib::Request& /*req*/, httplib::Response& res) {
  stats_.IncrementRequests();

  // Liveness probe: Always return 200 OK if the process is running
  // This is used by orchestrators (Kubernetes, Docker) to detect deadlocks
  json response;
  response["status"] = "alive";
  response["timestamp"] =
      std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();

  SendJson(res, kHttpOk, response);
}

void HttpServer::HandleHealthReady(const httplib::Request& /*req*/, httplib::Response& res) {
  stats_.IncrementRequests();

  // Readiness probe: Return 200 OK if ready to accept traffic, 503 otherwise
  bool is_ready = (loading_ == nullptr || !loading_->load());

  json response;
  if (is_ready) {
    response["status"] = "ready";
    response["loading"] = false;
    response["timestamp"] =
        std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    SendJson(res, kHttpOk, response);
  } else {
    response["status"] = "not_ready";
    response["loading"] = true;
    response["reason"] = "Server is loading";
    response["timestamp"] =
        std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    SendJson(res, kHttpServiceUnavailable, response);
  }
}

void HttpServer::HandleHealthDetail(const httplib::Request& /*req*/, httplib::Response& res) {
  stats_.IncrementRequests();

  // Detailed health: Return comprehensive component status
  json response;

  // Overall status
  bool is_loading = (loading_ != nullptr && loading_->load());
  response["status"] = is_loading ? "degraded" : "healthy";
  response["timestamp"] =
      std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();

  // Calculate uptime (approximate, based on stats initialization)
  static auto start_time = std::chrono::steady_clock::now();
  auto uptime = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - start_time).count();
  response["uptime_seconds"] = uptime;

  // Components status
  json components;

  // Server component
  json server_comp;
  server_comp["status"] = is_loading ? "loading" : "ready";
  server_comp["loading"] = is_loading;
  components["server"] = server_comp;

  // Index component (aggregate from all tables)
  json index_comp;
  size_t total_terms = 0;
  size_t total_documents = 0;
  for (const auto& [table_name, ctx] : table_contexts_) {
    if (ctx != nullptr && ctx->index) {
      total_terms += ctx->index->TermCount();
      // Note: Index doesn't have document count method, use doc_store instead
      if (ctx->doc_store != nullptr) {
        total_documents += ctx->doc_store->Size();
      }
    }
  }
  index_comp["status"] = "ok";
  index_comp["total_terms"] = total_terms;
  index_comp["total_documents"] = total_documents;
  components["index"] = index_comp;

  // Cache component (if available)
  if (cache_manager_ != nullptr) {
    json cache_comp;
    auto cache_stats = cache_manager_->GetStatistics();
    cache_comp["status"] = "ok";
    cache_comp["hit_rate"] = cache_stats.total_queries > 0 ? static_cast<double>(cache_stats.cache_hits) /
                                                                 static_cast<double>(cache_stats.total_queries)
                                                           : 0.0;
    cache_comp["total_hits"] = cache_stats.cache_hits;
    cache_comp["total_misses"] = cache_stats.cache_misses;
    cache_comp["current_entries"] = cache_stats.current_entries;
    components["cache"] = cache_comp;
  }

#ifdef USE_MYSQL
  // Binlog component (if available)
  if (binlog_reader_ != nullptr) {
    json binlog_comp;
    if (binlog_reader_->IsRunning()) {
      binlog_comp["status"] = "connected";
      binlog_comp["running"] = true;
      binlog_comp["current_gtid"] = binlog_reader_->GetCurrentGTID();
      binlog_comp["processed_events"] = binlog_reader_->GetProcessedEvents();
      binlog_comp["queue_size"] = binlog_reader_->GetQueueSize();
    } else {
      binlog_comp["status"] = "disconnected";
      binlog_comp["running"] = false;
    }
    components["binlog"] = binlog_comp;
  }
#endif

  response["components"] = components;

  SendJson(res, kHttpOk, response);
}

void HttpServer::HandleConfig(const httplib::Request& /*req*/, httplib::Response& res) {
  stats_.IncrementRequests();

  if (full_config_ == nullptr) {
    SendError(res, kHttpInternalServerError, "Configuration not available");
    return;
  }

  try {
    json response;

    // MySQL config summary (no credentials)
    json mysql_obj;
    mysql_obj["configured"] = !full_config_->mysql.user.empty() || !full_config_->mysql.host.empty();
    mysql_obj["database_defined"] = !full_config_->mysql.database.empty();
    response["mysql"] = mysql_obj;

    // API config summary (no bind/port exposure)
    json api_obj;
    api_obj["tcp"]["enabled"] = true;
    api_obj["http"]["enabled"] = full_config_->api.http.enable;
    api_obj["http"]["cors_enabled"] = full_config_->api.http.enable_cors;
    response["api"] = api_obj;

    // Network ACL status
    json net_obj;
    net_obj["allow_cidrs_configured"] = !full_config_->network.allow_cidrs.empty();
    response["network"] = net_obj;

    // Replication config summary
    json repl_obj;
    repl_obj["enable"] = full_config_->replication.enable;
    response["replication"] = repl_obj;

    response["notes"] =
        "Sensitive configuration values are redacted over HTTP. Use CONFIG SHOW over a secured connection for details.";

    SendJson(res, kHttpOk, response);

  } catch (const std::exception& e) {
    mygram::utils::StructuredLog()
        .Event("http_handler_error")
        .Field("handler", "config")
        .Field("error", e.what())
        .Error();
    SendError(res, kHttpInternalServerError, "Internal server error");
  }
}

void HttpServer::HandleReplicationStatus(const httplib::Request& /*req*/, httplib::Response& res) {
  stats_.IncrementRequests();

#ifdef USE_MYSQL
  if (binlog_reader_ == nullptr) {
    SendError(res, kHttpServiceUnavailable, "Replication not configured");
    return;
  }

  try {
    json response;
    response["enabled"] = binlog_reader_->IsRunning();
    response["current_gtid"] = binlog_reader_->GetCurrentGTID();

    SendJson(res, kHttpOk, response);

  } catch (const std::exception& e) {
    mygram::utils::StructuredLog()
        .Event("http_handler_error")
        .Field("handler", "replication_status")
        .Field("error", e.what())
        .Error();
    SendError(res, kHttpInternalServerError, "Internal server error");
  }
#else
  SendError(res, kHttpServiceUnavailable, "MySQL replication not compiled");
#endif
}

void HttpServer::HandleMetrics(const httplib::Request& /*req*/, httplib::Response& res) {
  stats_.IncrementRequests();

  try {
    // Use TCP server's stats if available (includes all protocol stats), otherwise use HTTP-only stats
    ServerStats& effective_stats = (tcp_stats_ != nullptr) ? *tcp_stats_ : stats_;

    // Aggregate metrics
    auto aggregated_metrics = StatisticsService::AggregateMetrics(table_contexts_);

    // Update server statistics
    StatisticsService::UpdateServerStatistics(effective_stats, aggregated_metrics);

    // Format response
    std::string metrics = ResponseFormatter::FormatPrometheusMetrics(aggregated_metrics, effective_stats,
                                                                     table_contexts_, binlog_reader_, cache_manager_);
    res.status = kHttpOk;
    res.set_content(metrics, "text/plain; version=0.0.4; charset=utf-8");
  } catch (const std::exception& e) {
    mygram::utils::StructuredLog()
        .Event("http_handler_error")
        .Field("handler", "metrics")
        .Field("error", e.what())
        .Error();
    SendError(res, kHttpInternalServerError, "Internal server error");
  }
}

void HttpServer::SendJson(httplib::Response& res, int status_code, const nlohmann::json& body) {
  res.status = status_code;
  res.set_content(body.dump(), "application/json");
}

void HttpServer::SendError(httplib::Response& res, int status_code, const std::string& message) {
  json error_obj;
  error_obj["error"] = message;
  SendJson(res, status_code, error_obj);
}

}  // namespace mygramdb::server
