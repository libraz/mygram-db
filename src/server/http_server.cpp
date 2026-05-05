/**
 * @file http_server.cpp
 * @brief HTTP server implementation
 */

#include "server/http_server.h"

#include <algorithm>
#include <array>
#include <cctype>
#include <chrono>
#include <limits>
#include <nlohmann/json.hpp>
#include <sstream>
#include <string_view>
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
 * @brief Convert a JSON filter value to its string representation.
 *
 * Handles string, integer, float, and boolean types with appropriate coercion.
 * Returns std::nullopt if the value type is unsupported.
 */
std::optional<std::string> JsonFilterValueToString(const json& val) {
  if (val.is_string()) {
    return val.get<std::string>();
  }
  if (val.is_number_integer()) {
    return std::to_string(val.get<int64_t>());
  }
  if (val.is_number_float()) {
    return std::to_string(val.get<double>());
  }
  if (val.is_boolean()) {
    return val.get<bool>() ? "1" : "0";
  }
  return std::nullopt;
}

/**
 * @brief Validate a table name supplied via the HTTP API.
 *
 * Permitted characters:
 * - ASCII letters, digits, underscore, hyphen, and dot.
 * - Any non-ASCII byte (>= 0x80), which lets UTF-8-encoded names such as
 *   "テーブル" pass through unchanged.
 *
 * Rejected: empty names, ASCII whitespace, ASCII control characters, and any
 * other ASCII punctuation. The goal is to prevent the value from breaking the
 * QueryParser command grammar (e.g. `articles foo` would inject an extra
 * token, and `articles;` would inject a stray punctuation token).
 *
 * @param table Table name from the request URL.
 * @return true if the name is safe to embed in a parser command, false
 *         otherwise.
 */
bool IsValidTableName(std::string_view table) {
  if (table.empty()) {
    return false;
  }
  // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
  constexpr size_t kMaxTableNameLength = 256;
  if (table.size() > kMaxTableNameLength) {
    return false;
  }
  for (char c : table) {
    auto u = static_cast<unsigned char>(c);
    bool ascii_safe =
        (u >= 'a' && u <= 'z') || (u >= 'A' && u <= 'Z') || (u >= '0' && u <= '9') || u == '_' || u == '-' || u == '.';
    bool non_ascii = u >= 0x80;
    if (!ascii_safe && !non_ascii) {
      return false;
    }
  }
  return true;
}

/**
 * @brief Detect QueryParser clause keywords inside a JSON-supplied query string.
 *
 * The HTTP API exposes `limit`, `offset`, and `filters` as dedicated JSON
 * fields. Allowing the same keywords to appear inside the search expression
 * (`q`) would let a caller silently override those JSON values, which is a
 * parameter pollution vulnerability (P1-9). This helper rejects such inputs by
 * scanning for the dangerous keywords as standalone tokens, ignoring contents
 * inside single- or double-quoted regions so that legitimate phrase searches
 * such as `"foo LIMIT bar"` remain valid.
 *
 * Boolean operators (`AND`, `OR`, `NOT`) are intentionally NOT rejected: they
 * are first-class search syntax with no JSON equivalent, and the existing
 * tests/clients depend on them being usable inside `q`.
 *
 * @param query_text Raw query string from the JSON `q` field.
 * @param[out] offending_keyword Set to the matched keyword (uppercase) on
 *             rejection.
 * @return true if the query is safe; false if it embeds a forbidden keyword.
 */
bool ValidateQueryTextNoReservedClauses(std::string_view query_text, std::string& offending_keyword) {
  static const std::array<std::string_view, 7> kForbiddenKeywords = {"LIMIT", "OFFSET",    "ORDER", "FILTER",
                                                                     "SORT",  "HIGHLIGHT", "FUZZY"};

  auto match_keyword = [&](std::string_view token) -> std::string_view {
    for (auto kw : kForbiddenKeywords) {
      if (token.size() != kw.size()) {
        continue;
      }
      bool eq = true;
      for (size_t i = 0; i < kw.size(); ++i) {
        auto a = static_cast<unsigned char>(token[i]);
        auto b = static_cast<unsigned char>(kw[i]);
        if (std::toupper(a) != b) {
          eq = false;
          break;
        }
      }
      if (eq) {
        return kw;
      }
    }
    return {};
  };

  size_t i = 0;
  const size_t n = query_text.size();
  char quote = '\0';

  while (i < n) {
    char c = query_text[i];

    if (quote != '\0') {
      // Inside quotes: skip everything until matching close (honor backslash escape).
      if (c == '\\' && i + 1 < n) {
        i += 2;
        continue;
      }
      if (c == quote) {
        quote = '\0';
      }
      ++i;
      continue;
    }

    if (c == '"' || c == '\'') {
      quote = c;
      ++i;
      continue;
    }

    // Skip ASCII whitespace.
    auto u = static_cast<unsigned char>(c);
    if (std::isspace(u) != 0) {
      ++i;
      continue;
    }

    // Collect a token of non-whitespace, non-quote characters.
    size_t start = i;
    while (i < n) {
      char tc = query_text[i];
      auto tu = static_cast<unsigned char>(tc);
      if (std::isspace(tu) != 0 || tc == '"' || tc == '\'') {
        break;
      }
      ++i;
    }

    std::string_view token = query_text.substr(start, i - start);
    auto matched = match_keyword(token);
    if (!matched.empty()) {
      offending_keyword.assign(matched.begin(), matched.end());
      return false;
    }
  }

  return true;
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

    if (val.is_object() && val.contains("value")) {
      // Format 2: full operator support
      std::string op_str = val.value("op", "EQ");
      auto parsed_op = query::QueryParser::ParseFilterOp(op_str);
      if (!parsed_op.has_value()) {
        error_message = "Invalid filter operator: " + op_str;
        return false;
      }
      filter.op = parsed_op.value();

      auto str_val = JsonFilterValueToString(val["value"]);
      if (!str_val.has_value()) {
        error_message = "Invalid filter value type for column: " + key;
        return false;
      }
      filter.value = std::move(str_val.value());
    } else {
      // Format 1: backward compatible (defaults to EQ)
      filter.op = query::FilterOp::EQ;
      auto str_val = JsonFilterValueToString(val);
      if (!str_val.has_value()) {
        error_message = "Invalid filter value type for column: " + key;
        return false;
      }
      filter.value = std::move(str_val.value());
    }

    query.filters.push_back(std::move(filter));
  }
  return true;
}

}  // namespace

using storage::DocId;

HttpServer::HttpServer(HttpServerConfig config, std::unordered_map<std::string, TableContext*> table_contexts,
                       const config::Config* full_config, mysql::IBinlogReader* binlog_reader,
                       cache::CacheManager* cache_manager, std::atomic<bool>* loading, ServerStats* tcp_stats,
                       std::shared_ptr<RateLimiter> rate_limiter)
    : config_(std::move(config)),
      table_contexts_(std::move(table_contexts)),
      full_config_(full_config),
      binlog_reader_(binlog_reader),
      cache_manager_(cache_manager),
      rate_limiter_(std::move(rate_limiter)),
      loading_(loading),
      tcp_stats_(tcp_stats) {
  parsed_allow_cidrs_ = ParseAllowCidrs(config_.allow_cidrs);

  if (full_config_ != nullptr) {
    const auto configured_limit = full_config_->api.max_query_length;
    max_query_length_ = configured_limit <= 0 ? 0 : static_cast<size_t>(configured_limit);

    // Rate limiter resolution:
    //   - If the embedder injected a shared instance (ServerLifecycleManager
    //     does this so quotas apply across TCP+HTTP), use it.
    //   - Otherwise fall back to constructing a private one from config so
    //     standalone HttpServer instances (and unit tests that pre-date the
    //     shared rate limiter wiring) still rate-limit.
    if (!rate_limiter_ && full_config_->api.rate_limiting.enable) {
      rate_limiter_ = std::make_shared<RateLimiter>(static_cast<size_t>(full_config_->api.rate_limiting.capacity),
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

  // Cap the maximum HTTP body size (Fix N-2). cpp-httplib rejects oversize
  // POST bodies with 413 Payload Too Large before any handler runs, which
  // protects /search and /count from memory-exhaustion attacks via giant
  // JSON payloads. Default 16 MiB; configurable via api.http.max_body_bytes.
  if (config_.max_body_bytes > 0) {
    server_->set_payload_max_length(config_.max_body_bytes);
  }

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
      RecordRequest();
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
      RecordRequest();
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
  // NOTE: Default to "*" rather than the literal "null" string. A literal "null"
  // origin is dangerous because browsers treat null-origin requests from
  // sandboxed iframes as matching, enabling CORS bypass attacks.
  const std::string allow_origin = config_.cors_allow_origin.empty() ? "*" : config_.cors_allow_origin;

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

  // Atomically transition running_ from false -> true. This replaces the prior
  // check-then-set pattern, which let two concurrent Start() calls both pass
  // the load and both proceed to spawn the server thread (P0-C). The CAS uses
  // memory_order_acq_rel on success so that subsequent stores to server_thread_
  // happen-after this acquire, and memory_order_relaxed on failure since the
  // failure path only reads `expected` for diagnostic purposes.
  bool expected = false;
  if (!running_.compare_exchange_strong(expected, true, std::memory_order_acq_rel, std::memory_order_relaxed)) {
    auto error = MakeError(ErrorCode::kNetworkAlreadyRunning, "Server already running");
    mygram::utils::StructuredLog()
        .Event("server_error")
        .Field("operation", "http_server_start")
        .Field("error", error.to_string())
        .Error();
    return MakeUnexpected(error);
  }

  mygram::utils::StructuredLog()
      .Event("http_server_starting")
      .Field("bind", config_.bind)
      .Field("port", static_cast<uint64_t>(config_.port))
      .Info();

  // Bind synchronously on the calling thread.
  //
  // Rationale (Fix H-N1): the previous design spawned a worker thread that
  // called `bind_to_port` then signalled completion through a promise, with
  // the parent waiting on `start_future.wait_for(timeout)`. That introduced
  // a join-deadlock window: on `wait_for` timeout the parent called
  // `server_->stop()` (a no-op when the worker had not yet reached
  // `listen_after_bind`) and then `server_thread_->join()`, which could
  // block indefinitely if the worker was wedged inside `bind_to_port`. The
  // destructor's chained `Stop()` would then run `terminate()` from the
  // joinable-thread invariant.
  //
  // cpp-httplib exposes `bind_to_port` as a synchronous call: it just runs
  // the socket/bind/listen syscalls and returns. There is no benefit to
  // hopping into a worker for that step, and doing so synchronously
  // eliminates the timeout entirely. The worker thread only owns the
  // long-running `listen_after_bind` accept loop, which `Stop()`'s
  // `server_->stop()` reliably tears down via the documented
  // shutdown(svr_sock_) path.
  if (!server_->bind_to_port(config_.bind, config_.port)) {
    std::string error_msg = "Failed to bind to " + config_.bind + ":" + std::to_string(config_.port);
    mygram::utils::StructuredLog()
        .Event("server_error")
        .Field("operation", "http_server_listen")
        .Field("bind", config_.bind)
        .Field("port", static_cast<uint64_t>(config_.port))
        .Field("error", error_msg)
        .Error();
    // Release the running_ gate so subsequent Start()s can retry. Bind
    // happened on this thread, so no worker exists to clean up.
    running_.store(false, std::memory_order_release);
    auto error = MakeError(ErrorCode::kNetworkBindFailed, std::move(error_msg));
    return MakeUnexpected(error);
  }

  // Spawn the worker thread to drive the accept loop. By the time we reach
  // here, the listening socket is bound and ready; `server_->stop()` (called
  // from Stop()) will reliably interrupt `listen_after_bind` by closing the
  // listening socket — but only AFTER the worker reaches the
  // `is_running_=true` flip inside `listen_internal()`. Pre-flip, cpp-httplib's
  // own `Server::stop()` is a no-op (it gates on `is_running_`), so a
  // racing Stop() right after Start() returned would leak the worker thread.
  // We therefore call `server_->wait_until_ready()` immediately after the
  // spawn: it spins on `is_running_` with 1ms sleeps and returns within a
  // few milliseconds in all observed runs. By the time Start() returns,
  // both bind_to_port and the accept-loop entry are committed.
  server_thread_ = std::make_unique<std::thread>([this]() {
    if (!server_->listen_after_bind()) {
      // Abnormal exit from the accept loop. Release the running_ gate so a
      // subsequent Start() can attempt a fresh bind. Stop()'s CAS handles
      // the case where Stop() is the one tearing us down: its CAS observes
      // running_=false here and short-circuits.
      running_.store(false, std::memory_order_release);
    }
  });
  server_->wait_until_ready();

  mygram::utils::StructuredLog()
      .Event("http_server_started")
      .Field("bind", config_.bind)
      .Field("port", static_cast<uint64_t>(config_.port))
      .Info();
  return {};
}

void HttpServer::Stop() {
  // Use compare_exchange to prevent concurrent double-stop, matching the
  // pattern in ConnectionAcceptor::Stop() and SnapshotScheduler::Stop().
  bool expected = true;
  if (!running_.compare_exchange_strong(expected, false)) {
    return;
  }

  mygram::utils::StructuredLog().Event("http_server_stopping").Info();

  if (server_) {
    server_->stop();
  }

  if (server_thread_ && server_thread_->joinable()) {
    server_thread_->join();
  }

  mygram::utils::StructuredLog().Event("http_server_stopped").Info();
}

HttpServer::TableContextLookup HttpServer::ResolveHttpTableContext(const std::string& table_name) {
  TableContextLookup result;

  if (!IsValidTableName(table_name)) {
    result.status = kHttpBadRequest;
    result.message = "Invalid table name (allowed characters: letters, digits, '_', '-', '.')";
    return result;
  }

  auto table_iter = table_contexts_.find(table_name);
  if (table_iter == table_contexts_.end()) {
    result.status = kHttpNotFound;
    result.message = "Table not found: " + table_name;
    return result;
  }

  if (!table_iter->second->index || !table_iter->second->doc_store) {
    result.status = kHttpInternalServerError;
    result.message = "Table context has null index or doc_store";
    return result;
  }

  result.table_ctx = table_iter->second;
  result.status = kHttpOk;
  return result;
}

std::optional<HttpServer::PreparedHttpQuery> HttpServer::PrepareHttpSearchQuery(const httplib::Request& req,
                                                                                httplib::Response& res,
                                                                                const std::string& command,
                                                                                bool apply_pagination) {
  // Check if server is loading
  if (loading_ != nullptr && loading_->load()) {
    SendError(res, kHttpServiceUnavailable, "Server is loading, please try again later");
    return std::nullopt;
  }

  // Validate the URL-bound table name and resolve its context. Errors are
  // surfaced with the precise HTTP status code computed by the helper so
  // callers do not need to know the underlying reason.
  std::string table = req.matches[1];
  auto lookup = ResolveHttpTableContext(table);
  if (lookup.table_ctx == nullptr) {
    SendError(res, lookup.status, lookup.message);
    return std::nullopt;
  }
  auto* table_ctx = lookup.table_ctx;

  // Parse JSON body
  json body;
  try {
    body = json::parse(req.body);
  } catch (const json::parse_error& e) {
    SendError(res, kHttpBadRequest, "Invalid JSON: " + std::string(e.what()));
    return std::nullopt;
  }

  // Validate required field
  if (!body.contains("q")) {
    SendError(res, kHttpBadRequest, "Missing required field: q");
    return std::nullopt;
  }

  // Validate field type before extraction
  if (!body["q"].is_string()) {
    SendError(res, kHttpBadRequest, "Field 'q' must be a string");
    return std::nullopt;
  }

  // Validate query text for control characters (CRLF injection prevention)
  std::string query_text = body["q"].get<std::string>();
  for (char c : query_text) {
    if (c == '\r' || c == '\n' || c == '\0') {
      SendError(res, kHttpBadRequest, "Query text contains invalid control characters");
      return std::nullopt;
    }
  }

  // Reject parser clause keywords smuggled into `q`. JSON-supplied `limit`,
  // `offset`, and `filters` would otherwise be silently overridden by tokens
  // such as `LIMIT 0 OFFSET 999999` embedded in the search text (P1-9).
  {
    std::string offending;
    if (!ValidateQueryTextNoReservedClauses(query_text, offending)) {
      const std::string field_hint = apply_pagination ? "(limit, offset, filters)" : "(filters)";
      SendError(res, kHttpBadRequest,
                "Reserved keyword '" + offending + "' is not allowed in 'q'. Use the dedicated JSON fields " +
                    field_hint + " instead.");
      return std::nullopt;
    }
  }

  // Build query string for QueryParser
  std::ostringstream query_str;
  query_str << command << " " << table << " " << query_text;

  if (apply_pagination) {
    // Add limit
    if (body.contains("limit")) {
      if (!body["limit"].is_number_integer()) {
        SendError(res, kHttpBadRequest, "Invalid limit: must be an integer");
        return std::nullopt;
      }
      query_str << " LIMIT " << body["limit"].get<int>();
    }

    // Add offset
    if (body.contains("offset")) {
      if (!body["offset"].is_number_integer()) {
        SendError(res, kHttpBadRequest, "Invalid offset: must be an integer");
        return std::nullopt;
      }
      query_str << " OFFSET " << body["offset"].get<int>();
    }
  }

  // Parse query (use per-request parser to avoid data race)
  query::QueryParser query_parser;
  if (max_query_length_ > 0) {
    query_parser.SetMaxQueryLength(max_query_length_);
  }
  auto parsed_query = query_parser.Parse(query_str.str());
  if (!parsed_query) {
    SendError(res, kHttpBadRequest, "Invalid query: " + parsed_query.error().message());
    return std::nullopt;
  }

  // Apply default limit if LIMIT was not explicitly specified in the request
  if (apply_pagination && !parsed_query->limit_explicit && full_config_ != nullptr) {
    parsed_query->limit = static_cast<size_t>(full_config_->api.default_limit);
  }

  // Apply filters from JSON payload
  if (body.contains("filters") && body["filters"].is_object()) {
    std::string filter_error;
    if (!ParseFiltersFromJson(body["filters"], *parsed_query, filter_error)) {
      SendError(res, kHttpBadRequest, filter_error);
      return std::nullopt;
    }
  }

  PreparedHttpQuery prepared;
  prepared.table_ctx = table_ctx;
  prepared.body = std::move(body);
  prepared.query = std::move(*parsed_query);
  return prepared;
}

void HttpServer::HandleSearch(const httplib::Request& req, httplib::Response& res) {
  RecordRequest();

  try {
    auto prepared = PrepareHttpSearchQuery(req, res, "SEARCH", /*apply_pagination=*/true);
    if (!prepared) {
      return;
    }
    auto* table_ctx = prepared->table_ctx;
    auto* current_doc_store = table_ctx->doc_store.get();
    auto& query_ref = prepared->query;
    auto* query = &query_ref;

    // Build pipeline parameters via the shared helper. SEARCH attaches BM25
    // stats so the pipeline can score `_score` sorts; COUNT does not.
    auto params = search_pipeline::BuildPipelineParamsFromContext(*table_ctx, full_config_, cache_manager_,
                                                                  SearchHandler::GetFilterThreshold(),
                                                                  /*attach_bm25_stats=*/true);

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
    auto docs = current_doc_store->GetDocumentsBatch(sorted_results);
    for (size_t i = 0; i < docs.size(); ++i) {
      if (docs[i]) {
        json doc_obj;
        doc_obj["doc_id"] = docs[i]->doc_id;
        doc_obj["primary_key"] = docs[i]->primary_key;

        // Add filters
        if (!docs[i]->filters.empty()) {
          json filters_obj;
          for (const auto& [key, val] : docs[i]->filters) {
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
  RecordRequest();

  try {
    auto prepared = PrepareHttpSearchQuery(req, res, "COUNT", /*apply_pagination=*/false);
    if (!prepared) {
      return;
    }
    auto* table_ctx = prepared->table_ctx;
    auto& query_ref = prepared->query;
    auto* query = &query_ref;

    // COUNT does not need BM25 stats (no `_score` sort), so leave them off.
    auto params = search_pipeline::BuildPipelineParamsFromContext(*table_ctx, full_config_, cache_manager_,
                                                                  SearchHandler::GetFilterThreshold(),
                                                                  /*attach_bm25_stats=*/false);

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
  RecordRequest();

  try {
    // Check if server is loading
    if (loading_ != nullptr && loading_->load()) {
      SendError(res, kHttpServiceUnavailable, "Server is loading, please try again later");
      return;
    }

    // Extract table name and ID from URL. Use the shared resolution helper so
    // GET applies the same table-name whitelist and null-context guards as
    // SEARCH and COUNT (M-6 follow-up).
    std::string id_str = req.matches[2];
    auto lookup = ResolveHttpTableContext(req.matches[1]);
    if (lookup.table_ctx == nullptr) {
      SendError(res, lookup.status, lookup.message);
      return;
    }
    auto* current_doc_store = lookup.table_ctx->doc_store.get();

    // Parse ID
    uint64_t doc_id = 0;
    try {
      doc_id = std::stoull(id_str);
    } catch (const std::exception& e) {
      SendError(res, kHttpBadRequest, "Invalid document ID");
      return;
    }

    // Validate DocId range (DocId is uint32_t, stoull returns uint64_t)
    if (doc_id > std::numeric_limits<uint32_t>::max()) {
      SendError(res, kHttpNotFound, "Document not found");
      return;
    }

    // Get document
    auto doc = current_doc_store->GetDocument(static_cast<storage::DocId>(doc_id));
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
  RecordRequest();

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
  // Health probes are intentionally NOT counted in total_requests:
  // they are typically driven by orchestrators (Kubernetes liveness/readiness)
  // at high frequency and would distort QPS metrics for actual application traffic.
  // If you need a separate counter for probe rate, add a dedicated metric instead
  // of resurrecting RecordRequest() here.
  json response;
  response["status"] = "ok";
  response["timestamp"] =
      std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();

  SendJson(res, kHttpOk, response);
}

void HttpServer::HandleHealthLive(const httplib::Request& /*req*/, httplib::Response& res) {
  // Health probe — not counted in total_requests; see HandleHealth.
  // Liveness probe: Always return 200 OK if the process is running
  // This is used by orchestrators (Kubernetes, Docker) to detect deadlocks
  json response;
  response["status"] = "alive";
  response["timestamp"] =
      std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();

  SendJson(res, kHttpOk, response);
}

void HttpServer::HandleHealthReady(const httplib::Request& /*req*/, httplib::Response& res) {
  // Health probe — not counted in total_requests; see HandleHealth.
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
  // Health probe — not counted in total_requests; see HandleHealth.
  // Detailed health: Return comprehensive component status
  json response;

  // Overall status
  bool is_loading = (loading_ != nullptr && loading_->load());
  response["status"] = is_loading ? "degraded" : "healthy";
  response["timestamp"] =
      std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();

  // Uptime from this HttpServer instance's construction time
  auto uptime =
      std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - start_time_).count();
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
  RecordRequest();

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
  RecordRequest();

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
  RecordRequest();

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
