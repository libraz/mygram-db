/**
 * @file http_server.cpp
 * @brief HTTP server implementation
 */

#include "server/http_server.h"

#include <algorithm>
#include <array>
#include <cctype>
#include <chrono>
#include <iterator>
#include <limits>
#include <nlohmann/json.hpp>
#include <sstream>
#include <string_view>
#include <type_traits>
#include <variant>

#include "cache/cache_manager.h"
#include "config/config.h"
#include "query/query_parser.h"
#include "server/handlers/search_handler.h"
#include "server/log_field_names.h"
#include "server/protocol_constants.h"
#include "server/readiness.h"
#include "server/response_formatter.h"
#include "server/search_pipeline.h"
#include "server/statistics_service.h"
#include "server/table_catalog.h"  // For ResolveTableKey
#include "server/tcp_server.h"     // For TableContext definition
#include "storage/document_store.h"
#include "storage/filter_index.h"
#include "utils/memory_utils.h"
#include "utils/network_utils.h"
#include "utils/numeric_parse.h"
#include "utils/roaring_bitmap_ptr.h"
#include "utils/string_utils.h"
#include "utils/structured_log.h"
#include "version.h"

#ifdef USE_MYSQL
#include "server/sync_operation_manager.h"
#endif

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
constexpr int kHttpUnauthorized = 401;
constexpr int kHttpForbidden = 403;
constexpr int kHttpNotFound = 404;
constexpr int kHttpUnsupportedMediaType = 415;
constexpr int kHttpTooManyRequests = 429;
constexpr int kHttpInternalServerError = 500;
constexpr int kHttpServiceUnavailable = 503;
constexpr auto kHttpServerReadyTimeout = std::chrono::seconds(5);

using mygram::utils::Error;
using mygram::utils::ErrorCode;
using mygram::utils::Expected;
using mygram::utils::MakeError;
using mygram::utils::MakeUnexpected;

// cpp-httplib owns the accept loop and does not expose an accept callback. Its
// public TaskQueue seam is nevertheless exactly where a freshly accepted
// socket is handed off. Reserve a slot before enqueueing and release it only
// after process_and_close_socket() returns; this covers both queued and active
// sockets, including clients that never finish an HTTP request.
class CappedHttpTaskQueue final : public httplib::TaskQueue {
 public:
  CappedHttpTaskQueue(size_t max_connections, std::atomic<size_t>* active_connections)
      : max_connections_(max_connections),
        active_connections_(active_connections),
        delegate_(
            std::make_unique<httplib::ThreadPool>(CPPHTTPLIB_THREAD_POOL_COUNT, CPPHTTPLIB_THREAD_POOL_MAX_COUNT)) {}

  bool enqueue(std::function<void()> task) override {
    size_t active = active_connections_->load(std::memory_order_acquire);
    while (active < max_connections_) {
      if (active_connections_->compare_exchange_weak(active, active + 1, std::memory_order_acq_rel,
                                                     std::memory_order_acquire)) {
        const bool queued =
            delegate_->enqueue([active_connections = active_connections_, task = std::move(task)]() mutable {
              struct ReleaseSlot {
                std::atomic<size_t>* active_connections;
                ~ReleaseSlot() { active_connections->fetch_sub(1, std::memory_order_acq_rel); }
              } release{active_connections};
              task();
            });
        if (!queued) {
          active_connections_->fetch_sub(1, std::memory_order_acq_rel);
        }
        return queued;
      }
    }
    return false;
  }

  void shutdown() override { delegate_->shutdown(); }
  void on_idle() override { delegate_->on_idle(); }

 private:
  size_t max_connections_;
  std::atomic<size_t>* active_connections_;
  std::unique_ptr<httplib::TaskQueue> delegate_;
};

Expected<bool, Error> ParseHttpQueryMode(const json& body) {
  if (!body.contains("mode")) {
    return false;
  }
  if (!body["mode"].is_string()) {
    return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, "Field 'mode' must be a string"));
  }
  const std::string mode = body["mode"].get<std::string>();
  if (mode == "literal") {
    return false;
  }
  if (mode == "boolean") {
    return true;
  }
  return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, "Field 'mode' must be either 'literal' or 'boolean'"));
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

// Routes are single-segment `/tables/{identity}/...`, where {identity} is the
// qualified `database.table` or a bare `table` (resolved in single-db configs).
// The identity is always match[1]; GET carries the primary key in match[2].
std::string ExtractRouteTableKey(const httplib::Request& req) {
  return req.matches[1];
}

std::string ExtractRoutePrimaryKey(const httplib::Request& req) {
  return req.matches[2];
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
 * @return Success, or a typed query error
 */
Expected<void, Error> ParseFiltersFromJson(const json& filters_json, query::Query& query) {
  query.filters.clear();
  if (filters_json.size() > query::QueryParser::kMaxTermCount) {
    return MakeUnexpected(
        MakeError(ErrorCode::kQueryInvalidFilter,
                  "Too many FILTER conditions (max " + std::to_string(query::QueryParser::kMaxTermCount) + ")"));
  }

  for (const auto& [key, val] : filters_json.items()) {
    // The column is checked before the value is read so a malformed condition
    // on an unknown column still names the column as the fault.
    if (!query::QueryParser::IsSafeColumnName(key)) {
      return MakeUnexpected(MakeError(ErrorCode::kQueryInvalidFilter, "Invalid filter column"));
    }

    query::FilterCondition filter;
    filter.column = key;

    if (val.is_object() && val.contains("value")) {
      // Format 2: full operator support
      std::string op_str = val.value("op", "EQ");
      auto parsed_op = query::QueryParser::ParseFilterOp(op_str);
      if (!parsed_op.has_value()) {
        return MakeUnexpected(MakeError(ErrorCode::kQueryInvalidFilter, "Invalid filter operator: " + op_str));
      }
      filter.op = parsed_op.value();

      auto str_val = JsonFilterValueToString(val["value"]);
      if (!str_val.has_value()) {
        return MakeUnexpected(
            MakeError(ErrorCode::kQueryInvalidFilter, "Invalid filter value type for column: " + key));
      }
      filter.value = std::move(str_val.value());
    } else {
      // Format 1: backward compatible (defaults to EQ)
      filter.op = query::FilterOp::EQ;
      auto str_val = JsonFilterValueToString(val);
      if (!str_val.has_value()) {
        return MakeUnexpected(
            MakeError(ErrorCode::kQueryInvalidFilter, "Invalid filter value type for column: " + key));
      }
      filter.value = std::move(str_val.value());
    }

    if (auto validated = query::QueryParser::ValidateFilterCondition(filter); !validated) {
      return validated;
    }

    query.filters.push_back(std::move(filter));
  }
  return {};
}

bool EqualsAsciiIgnoreCase(std::string_view lhs, std::string_view rhs) {
  if (lhs.size() != rhs.size()) {
    return false;
  }
  for (size_t i = 0; i < lhs.size(); ++i) {
    auto a = static_cast<unsigned char>(lhs[i]);
    auto b = static_cast<unsigned char>(rhs[i]);
    if (std::tolower(a) != std::tolower(b)) {
      return false;
    }
  }
  return true;
}

bool HasJsonContentType(const httplib::Request& req) {
  const std::string content_type = req.get_header_value("Content-Type");
  const auto media_type_end = content_type.find(';');
  const auto media_type_length = media_type_end == std::string::npos ? content_type.size() : media_type_end;
  std::string_view media_type(content_type.data(), media_type_length);
  while (!media_type.empty() && std::isspace(static_cast<unsigned char>(media_type.front()))) {
    media_type.remove_prefix(1);
  }
  while (!media_type.empty() && std::isspace(static_cast<unsigned char>(media_type.back()))) {
    media_type.remove_suffix(1);
  }
  return EqualsAsciiIgnoreCase(media_type, "application/json");
}

bool ConstantTimeEqual(std::string_view lhs, std::string_view rhs) {
  size_t difference = lhs.size() ^ rhs.size();
  const size_t length = std::max(lhs.size(), rhs.size());
  for (size_t i = 0; i < length; ++i) {
    const unsigned char left = i < lhs.size() ? static_cast<unsigned char>(lhs[i]) : 0;
    const unsigned char right = i < rhs.size() ? static_cast<unsigned char>(rhs[i]) : 0;
    difference |= static_cast<size_t>(left ^ right);
  }
  return difference == 0;
}

int HttpStatusForQueryError(const Error& error) {
  if (error.code() == ErrorCode::kServerShuttingDown || error.code() == ErrorCode::kServerLoading) {
    return kHttpServiceUnavailable;
  }
  if (error.code() == ErrorCode::kInternalError) {
    return kHttpInternalServerError;
  }
  return kHttpBadRequest;
}

Expected<void, Error> ParseSortFromJson(const json& sort_json, query::Query& query) {
  if (!sort_json.is_object()) {
    return MakeUnexpected(MakeError(ErrorCode::kQueryInvalidSort, "Field 'sort' must be an object"));
  }
  // An omitted column orders by the primary key, mirroring the TCP shorthand
  // `SORT ASC` / `SORT DESC`. A named column is resolved as the client wrote it.
  std::string column;
  if (sort_json.contains("column")) {
    if (!sort_json["column"].is_string()) {
      return MakeUnexpected(MakeError(ErrorCode::kQueryInvalidSort, "Field 'sort.column' must be a string"));
    }
    column = sort_json["column"].get<std::string>();
    if (!query::QueryParser::IsSafeSortColumn(column)) {
      return MakeUnexpected(MakeError(ErrorCode::kQueryInvalidSort, "Invalid sort column"));
    }
  }

  query::SortOrder order = query::SortOrder::DESC;
  if (sort_json.contains("order")) {
    if (!sort_json["order"].is_string()) {
      return MakeUnexpected(MakeError(ErrorCode::kQueryInvalidSort, "Field 'sort.order' must be a string"));
    }
    std::string order_str = sort_json["order"].get<std::string>();
    const auto parsed_order = query::QueryParser::ParseSortOrder(order_str);
    if (!parsed_order.has_value()) {
      return MakeUnexpected(MakeError(ErrorCode::kQueryInvalidSort, "Invalid sort order: " + order_str));
    }
    order = *parsed_order;
  }

  query.order_by = query::OrderByClause{std::move(column), order};
  return {};
}

Expected<void, Error> ParseHighlightUint(const json& highlight_json, const char* field_name, uint32_t min_value,
                                         uint32_t max_value, uint32_t& out) {
  if (!highlight_json.contains(field_name)) {
    return {};
  }
  const auto& value = highlight_json[field_name];
  if (!value.is_number_unsigned() && !value.is_number_integer()) {
    return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError,
                                    std::string("Field 'highlight.") + field_name + "' must be an integer"));
  }
  int64_t parsed = value.get<int64_t>();
  if (parsed < static_cast<int64_t>(min_value) || parsed > static_cast<int64_t>(max_value)) {
    std::ostringstream oss;
    oss << "Field 'highlight." << field_name << "' must be between " << min_value << " and " << max_value;
    return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, oss.str()));
  }
  out = static_cast<uint32_t>(parsed);
  return {};
}

Expected<void, Error> ParseHighlightFromJson(const json& highlight_json, query::Query& query) {
  if (!highlight_json.is_object()) {
    return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, "Field 'highlight' must be an object"));
  }

  query::HighlightOptions opts;
  if (highlight_json.contains("open_tag")) {
    if (!highlight_json["open_tag"].is_string()) {
      return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, "Field 'highlight.open_tag' must be a string"));
    }
    opts.open_tag = highlight_json["open_tag"].get<std::string>();
    if (opts.open_tag.size() > query::QueryParser::kMaxHighlightTagLength) {
      return MakeUnexpected(
          MakeError(ErrorCode::kQuerySyntaxError, "Field 'highlight.open_tag' must be at most 256 bytes"));
    }
  }
  if (highlight_json.contains("close_tag")) {
    if (!highlight_json["close_tag"].is_string()) {
      return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, "Field 'highlight.close_tag' must be a string"));
    }
    opts.close_tag = highlight_json["close_tag"].get<std::string>();
    if (opts.close_tag.size() > query::QueryParser::kMaxHighlightTagLength) {
      return MakeUnexpected(
          MakeError(ErrorCode::kQuerySyntaxError, "Field 'highlight.close_tag' must be at most 256 bytes"));
    }
  }

  if (auto result = ParseHighlightUint(highlight_json, "snippet_length", query::QueryParser::kMinSnippetLength,
                                       query::QueryParser::kMaxSnippetLength, opts.snippet_length);
      !result) {
    return result;
  }
  if (auto result = ParseHighlightUint(highlight_json, "max_fragments", query::QueryParser::kMinHighlightFragments,
                                       query::QueryParser::kMaxHighlightFragments, opts.max_fragments);
      !result) {
    return result;
  }

  query.highlight = std::move(opts);
  return {};
}

Expected<void, Error> ParseFuzzyFromJson(const json& fuzzy_json, query::Query& query) {
  if (!fuzzy_json.is_number_unsigned() && !fuzzy_json.is_number_integer()) {
    return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, "Field 'fuzzy' must be an integer"));
  }
  int64_t distance = fuzzy_json.get<int64_t>();
  if (distance < static_cast<int64_t>(query::QueryParser::kMinFuzzyDistance) ||
      distance > static_cast<int64_t>(query::QueryParser::kMaxFuzzyDistance)) {
    return MakeUnexpected(MakeError(ErrorCode::kQuerySyntaxError, "Field 'fuzzy' must be 1 or 2"));
  }
  query.fuzzy_max_distance = static_cast<uint32_t>(distance);
  return {};
}

search_pipeline::RelevanceSortParams BuildRelevanceSortParams(TableContext& table_ctx,
                                                              const config::Config* full_config) {
  search_pipeline::RelevanceSortParams score_params;
  score_params.index = table_ctx.index.get();
  score_params.doc_store = table_ctx.doc_store.get();
  score_params.full_config = full_config;
  score_params.bm25_stats = &table_ctx.bm25_stats;
  score_params.ngram_size = table_ctx.config.ngram_size;
  score_params.kanji_ngram_size = table_ctx.config.kanji_ngram_size;
  score_params.cross_boundary_ngrams = table_ctx.config.cross_boundary_ngrams;
  return score_params;
}

/// @brief Wall-clock seconds since the Unix epoch, as health bodies report it.
int64_t UnixTimestampSeconds() {
  return std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}

/// @brief Serialize a document's filter values as a JSON object.
json FilterMapToJson(const storage::FilterMap& filters) {
  json filters_obj;
  for (const auto& [key, value] : filters) {
    filters_obj[key] = FilterValueToJson(value);
  }
  return filters_obj;
}

}  // namespace

using storage::DocId;

HttpServer::HttpServer(HttpServerConfig config, std::unordered_map<std::string, TableContext*> table_contexts,
                       const config::Config* full_config, mysql::IBinlogReader* binlog_reader,
                       cache::CacheManager* cache_manager, std::atomic<bool>* loading, ServerStats* tcp_stats,
                       std::shared_ptr<RateLimiter> rate_limiter, std::atomic<bool>* replication_paused_for_dump,
                       SyncOperationManager* sync_manager,
                       std::function<bool(const std::string&)> table_syncing_checker,
                       std::function<bool()> any_syncing_checker, std::function<bool()> initial_data_ready_checker,
                       ThreadPool* thread_pool)
    : config_(std::move(config)),
      table_contexts_(std::move(table_contexts)),
      full_config_(full_config),
      binlog_reader_(binlog_reader),
      cache_manager_(cache_manager),
      rate_limiter_(std::move(rate_limiter)),
      loading_(loading),
      tcp_stats_(tcp_stats),
      replication_paused_for_dump_(replication_paused_for_dump),
      thread_pool_(thread_pool),
      sync_manager_(sync_manager),
      table_syncing_checker_(std::move(table_syncing_checker)),
      any_syncing_checker_(std::move(any_syncing_checker)),
      initial_data_ready_checker_(std::move(initial_data_ready_checker)) {
  parsed_allow_cidrs_ = mygram::utils::ParseAllowCidrs(config_.allow_cidrs);

  if (full_config_ != nullptr) {
    const auto configured_limit = full_config_->api.max_query_length;
    default_limit_.store(full_config_->api.default_limit, std::memory_order_release);
    max_query_length_.store(configured_limit <= 0 ? 0 : static_cast<size_t>(configured_limit),
                            std::memory_order_release);
  }

  server_ = std::make_unique<httplib::Server>();

  // Reject at the accepted-socket boundary rather than in a request handler:
  // an idle peer has not sent a request yet, but it already consumes an fd.
  // Config schema validation requires a positive value; clamp direct C++ API
  // callers as a defensive fallback.
  const size_t max_connections = static_cast<size_t>(std::max(config_.max_connections, 1));
  server_->new_task_queue = [max_connections, active_connections = &active_connections_]() {
    return new CappedHttpTaskQueue(max_connections, active_connections);
  };

  // cpp-httplib only substitutes req.remote_addr from X-Forwarded-For when
  // the direct TCP peer exactly matches this allowlist. ACL and shared rate
  // limiting below therefore receive the original client identity without
  // accepting spoofed forwarding headers from direct clients.
  server_->set_trusted_proxies(config_.trusted_proxies);

  // Set timeouts
  server_->set_read_timeout(config_.read_timeout_sec, 0);
  server_->set_write_timeout(config_.write_timeout_sec, 0);

  // Cap the maximum HTTP body size. cpp-httplib rejects oversize
  // POST bodies with 413 Payload Too Large before any handler runs, which
  // protects /search and /count from memory-exhaustion attacks via giant
  // JSON payloads. Default 16 MiB; configurable via api.http.max_body_bytes.
  //
  // 0 means no limit, and the call still has to happen: leaving the cap unset
  // does not lift it, it falls back to cpp-httplib's compiled-in ceiling, so
  // the request would keep getting a 413 the operator asked us not to send.
  server_->set_payload_max_length(config_.max_body_bytes > 0 ? config_.max_body_bytes
                                                             : std::numeric_limits<size_t>::max());

  // Setup network ACL before registering routes
  SetupAccessControl();

  // Setup routes
  SetupRoutes();

  // A missing origin must not silently become a wildcard. CORS is enabled
  // only when the caller explicitly configured an allow-origin value.
  if (config_.enable_cors && !config_.cors_allow_origin.empty()) {
    SetupCors();
  } else if (config_.enable_cors) {
    mygram::utils::StructuredLog().Event("http_cors_disabled_missing_allow_origin").Warn();
  }
}

HttpServer::~HttpServer() {
  Stop();
}

void HttpServer::AdoptSharedComponents(SharedComponents components) {
  tcp_stats_ = components.stats;
  cache_manager_ = components.cache_manager;
  thread_pool_ = components.thread_pool;
  rate_limiter_ = std::move(components.rate_limiter);
  loading_ = components.dump_load_in_progress;
  replication_paused_for_dump_ = components.replication_paused_for_dump;
  sync_manager_ = components.sync_manager;
  table_syncing_checker_ = std::move(components.table_syncing_checker);
  any_syncing_checker_ = std::move(components.any_syncing_checker);
  initial_data_ready_checker_ = std::move(components.initial_data_ready_checker);
  optimize_callback_ = std::move(components.optimize_callback);
  components_adopted_ = true;
}

void HttpServer::EnsureStandaloneRateLimiter() {
  if (components_adopted_ || rate_limiter_ || full_config_ == nullptr || !full_config_->api.rate_limiting.enable) {
    return;
  }
  rate_limiter_ = std::make_shared<RateLimiter>(static_cast<size_t>(full_config_->api.rate_limiting.capacity),
                                                static_cast<size_t>(full_config_->api.rate_limiting.refill_rate),
                                                static_cast<size_t>(full_config_->api.rate_limiting.max_clients));
  mygram::utils::StructuredLog()
      .Event("http_standalone_rate_limiter_initialized")
      .Field("capacity", static_cast<uint64_t>(full_config_->api.rate_limiting.capacity))
      .Field("refill_rate", static_cast<uint64_t>(full_config_->api.rate_limiting.refill_rate))
      .Field("max_clients", static_cast<uint64_t>(full_config_->api.rate_limiting.max_clients))
      .Info();
}

const std::array<HttpServer::RouteDescriptor, HttpServer::kRouteCount>& HttpServer::Routes() {
  using Method = RouteMethod;
  // In the `/tables/{identity}/...` patterns, {identity} is the qualified
  // `database.table` or a bare `table` (resolved in single-database
  // configurations). The pattern matches any non-slash characters to support
  // names with dashes, dots, or unicode.
  //
  // The trailing document-by-primary-key route lives under /tables/ so it
  // cannot shadow /info, /health/*, /config, /metrics or /replication/status,
  // but it does shadow the /tables/{identity}/search family and therefore
  // must stay last.
  //
  // Its primary-key group is greedy (`(.+)`) rather than slash-free. cpp-httplib
  // percent-decodes the target once, before routing, so a `%2F` in the key is
  // already a literal `/` by the time the pattern is applied; a slash-free group
  // would 404 every primary key that contains one — paths, URLs, hierarchical
  // SKUs — even though SEARCH returns those keys and TCP GET resolves them.
  // Greediness is safe here because no other GET route lives under /tables/.
  static const std::array<RouteDescriptor, kRouteCount> kRoutes = {{
      {Method::kPost, R"(/tables/([^/]+)/search)", false, true, true, &HttpServer::HandleSearch},
      {Method::kPost, R"(/tables/([^/]+)/count)", false, true, true, &HttpServer::HandleCount},
      {Method::kPost, R"(/tables/([^/]+)/facet)", false, true, true, &HttpServer::HandleFacet},
      {Method::kGet, "/info", false, true, true, &HttpServer::HandleInfo},
      {Method::kGet, "/health", false, false, false, &HttpServer::HandleHealth},
      {Method::kGet, "/health/live", false, false, false, &HttpServer::HandleHealthLive},
      {Method::kGet, "/health/ready", false, false, false, &HttpServer::HandleHealthReady},
      {Method::kGet, "/health/detail", false, false, true, &HttpServer::HandleHealthDetail},
      {Method::kGet, "/config", true, true, true, &HttpServer::HandleConfig},
      {Method::kGet, "/replication/status", true, true, true, &HttpServer::HandleReplicationStatus},
      {Method::kPost, "/optimize", true, true, true, &HttpServer::HandleOptimize},
      {Method::kGet, "/metrics", false, true, true, &HttpServer::HandleMetrics},
      {Method::kGet, R"(/tables/([^/]+)/(.+))", false, true, true, &HttpServer::HandleGet},
  }};
  return kRoutes;
}

const HttpServer::RouteDescriptor* HttpServer::FindLiteralRoute(const std::string& method, const std::string& path) {
  for (const auto& route : Routes()) {
    const bool method_matches = (route.method == RouteMethod::kGet) ? method == "GET" : method == "POST";
    if (method_matches && route.pattern == path) {
      return &route;
    }
  }
  return nullptr;
}

bool HttpServer::AdminTokenConfigured() const {
  return full_config_ != nullptr && !full_config_->api.admin_token.empty();
}

bool HttpServer::AdminCredentialsAccepted(const httplib::Request& req) const {
  if (!AdminTokenConfigured()) {
    // With no token configured the administrative surface is open by design,
    // exactly as RequestDispatcher treats administrative TCP commands.
    return true;
  }
  constexpr std::string_view kBearerPrefix = "Bearer ";
  const std::string authorization = req.get_header_value("Authorization");
  const bool has_bearer = authorization.size() >= kBearerPrefix.size() &&
                          std::string_view(authorization).substr(0, kBearerPrefix.size()) == kBearerPrefix;
  const std::string_view supplied =
      has_bearer ? std::string_view(authorization).substr(kBearerPrefix.size()) : std::string_view{};
  return has_bearer && ConstantTimeEqual(supplied, full_config_->api.admin_token);
}

void HttpServer::SetupRoutes() {
  for (const auto& route : Routes()) {
    auto invoke = [this, handler = route.handler, requires_admin_token = route.requires_admin_token](
                      const httplib::Request& req, httplib::Response& res) {
      // Credentials are checked here rather than inside each handler, so a
      // route cannot serve administrative state to an uncredentialed caller by
      // omitting a check of its own. The rejection happens before the handler
      // reads any state or assembles any body.
      if (requires_admin_token && !AdminCredentialsAccepted(req)) {
        RecordRequest();
        res.set_header("WWW-Authenticate", "Bearer");
        SendError(res, kHttpUnauthorized, "Administrative endpoint requires a valid bearer token",
                  mygram::utils::ErrorCode::kPermissionDenied);
        return;
      }
      (this->*handler)(req, res);
    };
    const std::string pattern(route.pattern);
    if (route.method == RouteMethod::kGet) {
      server_->Get(pattern, invoke);
    } else {
      server_->Post(pattern, invoke);
    }
  }
}

void HttpServer::SetupAccessControl() {
  server_->set_pre_routing_handler([this](const httplib::Request& req, httplib::Response& res) {
    const std::string& client_ip = req.remote_addr.empty() ? "unknown" : req.remote_addr;

    // The route table decides how a request is accounted and whether it is
    // subject to the shared quota. Only fixed-path routes can be identified
    // before cpp-httplib matches, which is exactly the set that opts out of
    // either; anything unrecognised is counted and rate limited.
    const RouteDescriptor* route = FindLiteralRoute(req.method, req.path);
    const bool counts_requests = route == nullptr || route->counts_requests;
    const bool rate_limited = route == nullptr || route->rate_limited;

    // Reject unauthorized peers before allocating or consuming a shared rate
    // bucket. ACL-denied traffic is already log-suppressed independently and
    // must not evict or exhaust quota state used by allowed clients.
    if (!mygram::utils::IsIPAllowed(req.remote_addr, parsed_allow_cidrs_)) {
      if (counts_requests) {
        RecordRequest();
      }
      GetEffectiveStats().IncrementRequestsDeniedAclHttp();
      const auto decision = denial_log_limiter_.Record("acl:" + client_ip);
      if (decision.should_log) {
        mygram::utils::StructuredLog()
            .Event("http_request_rejected_acl")
            .Field(log_fields::kFieldClientIp, client_ip)
            .Field("suppressed_since_last_log", decision.suppressed_count)
            .Warn();
      }
      SendError(res, kHttpForbidden, "Access denied by network.allow_cidrs",
                mygram::utils::ErrorCode::kPermissionDenied);
      return httplib::Server::HandlerResponse::Handled;
    }

    if (rate_limited && rate_limiter_ && !rate_limiter_->AllowRequest(client_ip)) {
      if (counts_requests) {
        RecordRequest();
      }
      GetEffectiveStats().IncrementRequestsDeniedRateLimitHttp();
      const auto decision = rate_limiter_->RecordDenialLog("http:" + client_ip);
      if (decision.should_log) {
        mygram::utils::StructuredLog()
            .Event("http_rate_limit_exceeded")
            .Field(log_fields::kFieldClientIp, client_ip)
            .Field("suppressed_since_last_log", decision.suppressed_count)
            .Warn();
      }
      SendError(res, kHttpTooManyRequests, "Rate limit exceeded", mygram::utils::ErrorCode::kServerBusy);
      return httplib::Server::HandlerResponse::Handled;
    }

    return httplib::Server::HandlerResponse::Unhandled;
  });
}

void HttpServer::SetupCors() {
  const std::string allow_origin = config_.cors_allow_origin;

  // CORS preflight
  server_->Options(".*", [allow_origin](const httplib::Request& /*req*/, httplib::Response& res) {
    res.set_header("Access-Control-Allow-Origin", allow_origin);
    res.set_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
    res.set_header("Access-Control-Allow-Headers", "Content-Type, Authorization");
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

  EnsureStandaloneRateLimiter();

  std::unique_lock<std::mutex> lifecycle_lock(lifecycle_mutex_);
  lifecycle_cv_.wait(lifecycle_lock, [this]() { return lifecycle_state_ != LifecycleState::kStopping; });

  if (lifecycle_state_ == LifecycleState::kStarting || lifecycle_state_ == LifecycleState::kRunning) {
    auto error = MakeError(ErrorCode::kNetworkAlreadyRunning, "Server already running");
    mygram::utils::StructuredLog().Event("http_server_start_failed").FieldError(error).Error();
    return MakeUnexpected(error);
  }

  // An accept loop may have exited abnormally and published kStopped while
  // leaving its std::thread joinable. Reap that owner before assigning a new
  // thread object; overwriting a joinable std::thread terminates the process.
  if (server_thread_ && server_thread_->joinable()) {
    lifecycle_state_ = LifecycleState::kStopping;
    running_.store(false, std::memory_order_release);
    if (server_) {
      server_->stop();
    }
    auto stale_thread = std::move(server_thread_);
    lifecycle_lock.unlock();
    stale_thread->join();
    lifecycle_lock.lock();
    lifecycle_state_ = LifecycleState::kStopped;
    ++stop_completion_epoch_;
    lifecycle_cv_.notify_all();
  }

  lifecycle_state_ = LifecycleState::kStarting;
  running_.store(false, std::memory_order_release);
  if (after_starting_hook_for_testing_) {
    after_starting_hook_for_testing_();
  }

  mygram::utils::StructuredLog()
      .Event("http_server_starting")
      .Field("bind", config_.bind)
      .Field("port", static_cast<uint64_t>(config_.port))
      .Info();

  // Bind synchronously on the calling thread.
  //
  // The previous design spawned a worker thread that
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
        .Event("http_server_listen_failed")
        .Field("bind", config_.bind)
        .Field("port", static_cast<uint64_t>(config_.port))
        .Field(log_fields::kFieldError, error_msg)
        .Error();
    running_.store(false, std::memory_order_release);
    lifecycle_state_ = LifecycleState::kStopped;
    lifecycle_cv_.notify_all();
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
    server_->listen_after_bind();
    std::lock_guard<std::mutex> lock(lifecycle_mutex_);
    if (lifecycle_state_ == LifecycleState::kRunning) {
      lifecycle_state_ = LifecycleState::kStopped;
      running_.store(false, std::memory_order_release);
      lifecycle_cv_.notify_all();
    }
  });
  const auto ready_deadline = std::chrono::steady_clock::now() + kHttpServerReadyTimeout;
  while (!server_->is_running() && std::chrono::steady_clock::now() < ready_deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  if (!server_->is_running()) {
    lifecycle_state_ = LifecycleState::kStopping;
    server_->stop();
    running_.store(false, std::memory_order_release);
    auto failed_thread = std::move(server_thread_);
    lifecycle_lock.unlock();
    if (failed_thread && failed_thread->joinable()) {
      failed_thread->join();
    }
    lifecycle_lock.lock();
    lifecycle_state_ = LifecycleState::kStopped;
    ++stop_completion_epoch_;
    lifecycle_cv_.notify_all();
    auto error = MakeError(ErrorCode::kNetworkBindFailed, "HTTP server did not become ready before timeout");
    mygram::utils::StructuredLog().Event("http_server_ready_timeout").FieldError(error).Error();
    return MakeUnexpected(error);
  }

  lifecycle_state_ = LifecycleState::kRunning;
  running_.store(true, std::memory_order_release);
  lifecycle_cv_.notify_all();

  mygram::utils::StructuredLog()
      .Event("http_server_started")
      .Field("bind", config_.bind)
      .Field("port", static_cast<uint64_t>(config_.port))
      .Info();
  return {};
}

void HttpServer::Stop() {
  std::unique_lock<std::mutex> lifecycle_lock(lifecycle_mutex_);
  if (lifecycle_state_ == LifecycleState::kStopping) {
    const uint64_t observed_epoch = stop_completion_epoch_;
    lifecycle_cv_.wait(lifecycle_lock, [this, observed_epoch]() { return stop_completion_epoch_ != observed_epoch; });
    return;
  }
  if (lifecycle_state_ == LifecycleState::kStopped && (!server_thread_ || !server_thread_->joinable())) {
    return;
  }

  lifecycle_state_ = LifecycleState::kStopping;
  running_.store(false, std::memory_order_release);

  mygram::utils::StructuredLog().Event("http_server_stopping").Info();

  if (server_) {
    server_->stop();
  }

  std::unique_ptr<std::thread> thread_to_join;
  if (server_thread_ && server_thread_->joinable()) {
    thread_to_join = std::move(server_thread_);
  }
  lifecycle_lock.unlock();

  if (thread_to_join && thread_to_join->joinable()) {
    thread_to_join->join();
  }

  lifecycle_lock.lock();
  lifecycle_state_ = LifecycleState::kStopped;
  ++stop_completion_epoch_;
  lifecycle_cv_.notify_all();
  lifecycle_lock.unlock();

  mygram::utils::StructuredLog().Event("http_server_stopped").Info();
}

void HttpServer::UpdateApiConfig(int default_limit, int max_query_length) {
  default_limit_.store(default_limit, std::memory_order_release);
  max_query_length_.store(max_query_length <= 0 ? 0 : static_cast<size_t>(max_query_length), std::memory_order_release);
}

HttpServer::TableContextLookup HttpServer::ResolveHttpTableContext(const std::string& table_name) {
  TableContextLookup result;

  if (!query::QueryParser::IsSafeTableName(table_name)) {
    result.status = kHttpBadRequest;
    result.message = "Invalid table name (allowed characters: letters, digits, '_', '-', '.')";
    result.code = mygram::utils::ErrorCode::kQueryInvalidToken;
    return result;
  }

  // Multi-database configurations require a qualified `database.table` identity.
  if (config::RequiresQualifiedTableReferences(full_config_) &&
      !query::QueryParser::IsDatabaseQualifiedTableName(table_name)) {
    result.status = kHttpBadRequest;
    result.message = "Bare table names are not supported; use <database>.<table>: " + table_name;
    result.code = mygram::utils::ErrorCode::kQuerySyntaxError;
    return result;
  }

  // Resolve a (possibly bare) identity to the canonical qualified key.
  auto resolved = ResolveTableKey(table_contexts_, table_name);
  if (!resolved.has_value()) {
    result.status = kHttpNotFound;
    result.message = "Table not found: " + table_name;
    result.code = mygram::utils::ErrorCode::kTableNotFound;
    return result;
  }

  auto table_iter = table_contexts_.find(*resolved);
  if (table_iter == table_contexts_.end()) {
    result.status = kHttpNotFound;
    result.message = "Table not found: " + table_name;
    result.code = mygram::utils::ErrorCode::kTableNotFound;
    return result;
  }

  if (!table_iter->second->index || !table_iter->second->doc_store) {
    result.status = kHttpInternalServerError;
    result.message = "Table context has null index or doc_store";
    result.code = mygram::utils::ErrorCode::kInternalError;
    return result;
  }

  result.table_ctx = table_iter->second;
  result.table_key = *resolved;
  result.status = kHttpOk;
  return result;
}

bool HttpServer::RejectIfTableSyncing(const std::string& table_key, httplib::Response& res) const {
  if (table_syncing_checker_ && table_syncing_checker_(table_key)) {
    SendError(res, kHttpServiceUnavailable, "Table '" + table_key + "' is synchronizing, please try again later",
              mygram::utils::ErrorCode::kServerNotReady);
    return true;
  }
#ifdef USE_MYSQL
  if (sync_manager_ == nullptr || table_key.empty()) {
    return false;
  }
  const auto syncing_tables = sync_manager_->GetSyncingTables();
  if (syncing_tables.find(table_key) == syncing_tables.end()) {
    return false;
  }
  SendError(res, kHttpServiceUnavailable, "Table '" + table_key + "' is synchronizing, please try again later",
            mygram::utils::ErrorCode::kServerNotReady);
  return true;
#else
  (void)table_key;
  (void)res;
  return false;
#endif
}

std::optional<HttpServer::PreparedHttpRequest> HttpServer::PrepareHttpJsonRequest(const httplib::Request& req,
                                                                                  httplib::Response& res) {
  if (!HasJsonContentType(req)) {
    SendError(res, kHttpUnsupportedMediaType, "Content-Type must be application/json",
              mygram::utils::ErrorCode::kNetworkInvalidRequest);
    return std::nullopt;
  }
  if (loading_ != nullptr && loading_->load()) {
    SendError(res, kHttpServiceUnavailable, "Server is loading, please try again later",
              mygram::utils::ErrorCode::kServerLoading);
    return std::nullopt;
  }

  auto lookup = ResolveHttpTableContext(ExtractRouteTableKey(req));
  if (lookup.table_ctx == nullptr) {
    SendError(res, lookup.status, lookup.message, lookup.code);
    return std::nullopt;
  }
  if (RejectIfTableSyncing(lookup.table_key, res)) {
    return std::nullopt;
  }
  std::shared_lock<std::shared_mutex> generation_lock(*lookup.table_ctx->generation_mutex);

  json body;
  try {
    body = json::parse(req.body);
  } catch (const json::parse_error& error) {
    SendError(res, kHttpBadRequest, "Invalid JSON: " + std::string(error.what()),
              mygram::utils::ErrorCode::kQuerySyntaxError);
    return std::nullopt;
  }

  PreparedHttpRequest prepared;
  prepared.generation_lock = std::move(generation_lock);
  prepared.table_ctx = lookup.table_ctx;
  prepared.table_key = std::move(lookup.table_key);
  prepared.body = std::move(body);
  return prepared;
}

bool HttpServer::ApplyHttpQueryOptions(const json& body, httplib::Response& res, query::Query& parsed_query,
                                       bool apply_pagination, bool apply_ranked_options) {
  if (apply_pagination) {
    if (body.contains("limit")) {
      if (!body["limit"].is_number_integer()) {
        SendError(res, kHttpBadRequest, "Invalid limit: must be an integer",
                  mygram::utils::ErrorCode::kQueryInvalidLimit);
        return false;
      }
      const int64_t limit = body["limit"].get<int64_t>();
      if (limit <= 0 || limit > config::defaults::kMaxLimit) {
        SendError(res, kHttpBadRequest,
                  "Invalid limit: must be between 1 and " + std::to_string(config::defaults::kMaxLimit),
                  mygram::utils::ErrorCode::kQueryInvalidLimit);
        return false;
      }
      parsed_query.limit = static_cast<uint32_t>(limit);
      parsed_query.limit_explicit = true;
    }

    if (body.contains("offset")) {
      if (!body["offset"].is_number_integer()) {
        SendError(res, kHttpBadRequest, "Invalid offset: must be an integer",
                  mygram::utils::ErrorCode::kQueryInvalidOffset);
        return false;
      }
      const int64_t offset = body["offset"].get<int64_t>();
      if (offset < 0 || static_cast<uint64_t>(offset) > std::numeric_limits<uint32_t>::max()) {
        SendError(res, kHttpBadRequest,
                  "Invalid offset: must be between 0 and " + std::to_string(std::numeric_limits<uint32_t>::max()),
                  mygram::utils::ErrorCode::kQueryInvalidOffset);
        return false;
      }
      parsed_query.offset = static_cast<uint32_t>(offset);
      parsed_query.offset_explicit = true;
    }

    if (!parsed_query.limit_explicit) {
      parsed_query.limit = static_cast<uint32_t>(default_limit_.load(std::memory_order_acquire));
    }
  }

  if (body.contains("filters") && !body["filters"].is_object()) {
    SendError(res, kHttpBadRequest, "Field 'filters' must be an object", mygram::utils::ErrorCode::kQueryInvalidFilter);
    return false;
  }
  if (body.contains("filters")) {
    if (auto result = ParseFiltersFromJson(body["filters"], parsed_query); !result) {
      SendError(res, kHttpBadRequest, result.error());
      return false;
    }
  }

  if (apply_ranked_options && body.contains("sort")) {
    if (auto result = ParseSortFromJson(body["sort"], parsed_query); !result) {
      SendError(res, kHttpBadRequest, result.error());
      return false;
    }
  }
  if (apply_ranked_options && body.contains("highlight")) {
    if (auto result = ParseHighlightFromJson(body["highlight"], parsed_query); !result) {
      SendError(res, kHttpBadRequest, result.error());
      return false;
    }
  }
  if (apply_ranked_options && body.contains("fuzzy")) {
    if (auto result = ParseFuzzyFromJson(body["fuzzy"], parsed_query); !result) {
      SendError(res, kHttpBadRequest, result.error());
      return false;
    }
  }

  query::QueryParser length_validator;
  length_validator.SetMaxQueryLength(max_query_length_.load(std::memory_order_acquire));
  if (auto result = length_validator.ValidateQueryLength(parsed_query); !result) {
    SendError(res, kHttpBadRequest, result.error());
    return false;
  }
  return true;
}

bool HttpServer::ValidateHttpQueryText(const std::string& query_text, httplib::Response& res, bool allow_empty) const {
  for (const char character : query_text) {
    if (character == '\r' || character == '\n' || character == '\0') {
      SendError(res, kHttpBadRequest, "Query text contains invalid control characters",
                mygram::utils::ErrorCode::kQueryInvalidToken);
      return false;
    }
  }
  if (!allow_empty && query_text.empty()) {
    SendError(res, kHttpBadRequest, "Field 'q' must be non-empty", mygram::utils::ErrorCode::kQuerySyntaxError);
    return false;
  }

  // Length is checked once, on the assembled query expression, exactly as the
  // TCP surface checks it.
  return true;
}

std::optional<HttpServer::PreparedHttpQuery> HttpServer::PrepareHttpSearchQuery(const httplib::Request& req,
                                                                                httplib::Response& res,
                                                                                const std::string& command,
                                                                                bool apply_pagination) {
  auto request = PrepareHttpJsonRequest(req, res);
  if (!request) {
    return std::nullopt;
  }
  auto& body = request->body;

  // Validate required field
  if (!body.contains("q")) {
    SendError(res, kHttpBadRequest, "Missing required field: q", mygram::utils::ErrorCode::kQuerySyntaxError);
    return std::nullopt;
  }

  // Validate field type before extraction
  if (!body["q"].is_string()) {
    SendError(res, kHttpBadRequest, "Field 'q' must be a string", mygram::utils::ErrorCode::kQuerySyntaxError);
    return std::nullopt;
  }

  if (!apply_pagination) {
    static constexpr std::array<std::string_view, 5> kCountRejectedFields = {"limit", "offset", "sort", "highlight",
                                                                             "fuzzy"};
    for (const auto field : kCountRejectedFields) {
      if (body.contains(std::string(field))) {
        SendError(res, kHttpBadRequest,
                  "Field '" + std::string(field) +
                      "' is not supported by COUNT; use /search for ranked or paginated "
                      "results",
                  mygram::utils::ErrorCode::kQuerySyntaxError);
        return std::nullopt;
      }
    }
  }

  std::string query_text = body["q"].get<std::string>();
  if (!ValidateHttpQueryText(query_text, res, /*allow_empty=*/false)) {
    return std::nullopt;
  }
  const auto max_query_length = max_query_length_.load(std::memory_order_acquire);

  auto boolean_mode = ParseHttpQueryMode(body);
  if (!boolean_mode) {
    SendError(res, kHttpBadRequest, boolean_mode.error());
    return std::nullopt;
  }

  query::Query parsed_query;
  if (*boolean_mode) {
    parsed_query.type = (command == "COUNT") ? query::QueryType::COUNT : query::QueryType::SEARCH;
    parsed_query.table = request->table_key;
    parsed_query.search_text = query_text;
    parsed_query.search_expression = std::move(query_text);
  } else {
    // Route literal HTTP searches through the same command parser used by the
    // raw TCP protocol. This keeps quote preservation, punctuation, and flat
    // search-text extraction identical across every public surface.
    query::QueryParser parser;
    parser.SetMaxQueryLength(max_query_length);
    auto base_query =
        parser.Parse(command + " " + request->table_key + " " + query::QueryParser::QuoteSearchLiteral(query_text));
    if (!base_query) {
      SendError(res, kHttpBadRequest, base_query.error());
      return std::nullopt;
    }
    parsed_query = std::move(*base_query);
  }

  if (!ApplyHttpQueryOptions(body, res, parsed_query, apply_pagination, apply_pagination)) {
    return std::nullopt;
  }

  PreparedHttpQuery prepared;
  prepared.generation_lock = std::move(request->generation_lock);
  prepared.table_ctx = request->table_ctx;
  prepared.body = std::move(body);
  prepared.query = std::move(parsed_query);
  return prepared;
}

std::optional<HttpServer::PreparedHttpQuery> HttpServer::PrepareHttpFacetQuery(const httplib::Request& req,
                                                                               httplib::Response& res) {
  auto request = PrepareHttpJsonRequest(req, res);
  if (!request) {
    return std::nullopt;
  }
  auto& body = request->body;

  if (!body.contains("column")) {
    SendError(res, kHttpBadRequest, "Missing required field: column", mygram::utils::ErrorCode::kQuerySyntaxError);
    return std::nullopt;
  }
  if (!body["column"].is_string()) {
    SendError(res, kHttpBadRequest, "Field 'column' must be a string", mygram::utils::ErrorCode::kQuerySyntaxError);
    return std::nullopt;
  }

  if (body.contains("q") && !body["q"].is_string()) {
    SendError(res, kHttpBadRequest, "Field 'q' must be a string", mygram::utils::ErrorCode::kQuerySyntaxError);
    return std::nullopt;
  }

  static constexpr std::array<std::string_view, 3> kFacetRejectedFields = {"sort", "highlight", "fuzzy"};
  for (const auto field : kFacetRejectedFields) {
    if (body.contains(std::string(field))) {
      SendError(res, kHttpBadRequest, "Field '" + std::string(field) + "' is not supported by FACET",
                mygram::utils::ErrorCode::kQuerySyntaxError);
      return std::nullopt;
    }
  }

  std::string column = body["column"].get<std::string>();
  if (!query::QueryParser::IsSafeColumnName(column)) {
    SendError(res, kHttpBadRequest, "Invalid facet column", mygram::utils::ErrorCode::kQueryInvalidToken);
    return std::nullopt;
  }

  auto boolean_mode = ParseHttpQueryMode(body);
  if (!boolean_mode) {
    SendError(res, kHttpBadRequest, boolean_mode.error());
    return std::nullopt;
  }

  query::Query parsed_query;
  parsed_query.type = query::QueryType::FACET;
  parsed_query.table = request->table_key;
  parsed_query.facet_column = std::move(column);

  if (body.contains("q")) {
    std::string query_text = body["q"].get<std::string>();
    if (!ValidateHttpQueryText(query_text, res, /*allow_empty=*/true)) {
      return std::nullopt;
    }
    if (!query_text.empty()) {
      parsed_query.search_text = query_text;
      parsed_query.search_expression =
          *boolean_mode ? std::move(query_text) : query::QueryParser::QuoteSearchLiteral(parsed_query.search_text);
    }
  }

  if (!ApplyHttpQueryOptions(body, res, parsed_query, /*apply_pagination=*/true,
                             /*apply_ranked_options=*/false)) {
    return std::nullopt;
  }

  PreparedHttpQuery prepared;
  prepared.generation_lock = std::move(request->generation_lock);
  prepared.table_ctx = request->table_ctx;
  prepared.body = std::move(body);
  prepared.query = std::move(parsed_query);
  return prepared;
}

void HttpServer::HandleSearch(const httplib::Request& req, httplib::Response& res) {
  RecordRequest();

  try {
    auto prepared = PrepareHttpSearchQuery(req, res, "SEARCH", /*apply_pagination=*/true);
    if (!prepared) {
      return;
    }
    RecordCommand(query::QueryType::SEARCH);
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
    if (!pipeline_output) {
      SendError(res, HttpStatusForQueryError(pipeline_output.error()), pipeline_output.error());
      return;
    }

    auto& results = pipeline_output->results;
    size_t total_count = results.size();
    auto topn = search_pipeline::ApplySearchTopNOptimization(
        *query, params.current_index, params.current_doc_store, full_config_, pipeline_output->term_infos,
        pipeline_output->all_search_terms, pipeline_output->semantics_reproducible_by_single_term_ngram_and,
        pipeline_output->cache_hit, params.primary_key_column, results);
    if (topn.applicable) {
      total_count = topn.total_results;
    }

    auto sorted_result = search_pipeline::SortAndPaginateResults(
        *query, results, pipeline_output->all_search_terms, pipeline_output->term_infos,
        BuildRelevanceSortParams(*table_ctx, full_config_), params.primary_key_column);
    if (!sorted_result.has_value()) {
      SendError(res, HttpStatusForQueryError(sorted_result.error()), sorted_result.error());
      return;
    }
    auto sorted_results = std::move(sorted_result.value());

    // Build JSON response
    json response;
    response["count"] = total_count;
    response["limit"] = query->limit;
    response["offset"] = query->offset;

    json results_array = json::array();
    auto docs = current_doc_store->GetDocumentsBatch(sorted_results);
    std::vector<std::string> highlight_snippets;
    if (query->highlight.has_value()) {
      if (!current_doc_store->IsStoreTextsEnabled()) {
        SendError(res, kHttpBadRequest,
                  "HIGHLIGHT requires normalized text storage. Set memory.verify_text to \"ascii\" or \"all\" in "
                  "configuration.",
                  mygram::utils::ErrorCode::kNotImplemented);
        return;
      }
      highlight_snippets = search_pipeline::GenerateHighlightSnippets(
          *query->highlight, pipeline_output->all_search_terms, sorted_results, table_ctx->index.get(),
          current_doc_store, table_ctx->synonym_dict.get());
    }
    for (size_t i = 0; i < docs.size(); ++i) {
      if (docs[i]) {
        json doc_obj;
        doc_obj["primary_key"] = docs[i]->primary_key;

        if (!docs[i]->filters.empty()) {
          doc_obj["filters"] = FilterMapToJson(docs[i]->filters);
        }

        if (i < highlight_snippets.size()) {
          doc_obj["highlight"] = highlight_snippets[i];
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
    SendError(res, kHttpInternalServerError, "Internal server error", mygram::utils::ErrorCode::kInternalError);
  }
}

void HttpServer::HandleCount(const httplib::Request& req, httplib::Response& res) {
  RecordRequest();

  try {
    auto prepared = PrepareHttpSearchQuery(req, res, "COUNT", /*apply_pagination=*/false);
    if (!prepared) {
      return;
    }
    RecordCommand(query::QueryType::COUNT);
    auto* table_ctx = prepared->table_ctx;
    auto& query_ref = prepared->query;
    auto* query = &query_ref;

    // COUNT does not need BM25 stats (no `_score` sort), so leave them off.
    auto params = search_pipeline::BuildPipelineParamsFromContext(*table_ctx, full_config_, cache_manager_,
                                                                  SearchHandler::GetFilterThreshold(),
                                                                  /*attach_bm25_stats=*/false);

    // Execute the unified search pipeline
    auto pipeline_output = search_pipeline::ExecuteFullPipeline(*query, params);
    if (!pipeline_output) {
      SendError(res, HttpStatusForQueryError(pipeline_output.error()), pipeline_output.error());
      return;
    }

    // Build JSON response - just return count
    json response;
    response["count"] = pipeline_output->results.size();

    SendJson(res, kHttpOk, response);

  } catch (const std::exception& e) {
    mygram::utils::StructuredLog()
        .Event("http_handler_error")
        .Field("handler", "count")
        .Field("error", e.what())
        .Error();
    SendError(res, kHttpInternalServerError, "Internal server error", mygram::utils::ErrorCode::kInternalError);
  }
}

void HttpServer::HandleFacet(const httplib::Request& req, httplib::Response& res) {
  RecordRequest();

  try {
    auto prepared = PrepareHttpFacetQuery(req, res);
    if (!prepared) {
      return;
    }
    RecordCommand(query::QueryType::FACET);

    auto* table_ctx = prepared->table_ctx;
    auto& query_ref = prepared->query;
    auto* query = &query_ref;
    auto params = search_pipeline::BuildFacetPipelineParamsFromContext(*table_ctx, full_config_, cache_manager_,
                                                                       SearchHandler::GetFilterThreshold());
    params.load_in_progress = [this]() { return loading_ != nullptr && loading_->load(std::memory_order_acquire); };
    auto facet_output = search_pipeline::ExecuteFacetPipeline(*query, params);
    if (!facet_output) {
      SendError(res, HttpStatusForQueryError(facet_output.error()), facet_output.error());
      return;
    }

    json facets = json::array();
    for (auto& [serialized, count] : facet_output->value_counts) {
      facets.push_back({
          {"value", storage::FilterIndex::DeserializeToDisplayString(serialized)},
          {"count", count},
      });
    }

    json response;
    response["column"] = query->facet_column;
    response["count"] = facets.size();
    response["total_count"] = facet_output->total_values;
    response["facets"] = std::move(facets);

    SendJson(res, kHttpOk, response);
  } catch (const std::exception& e) {
    mygram::utils::StructuredLog()
        .Event("http_handler_error")
        .Field("handler", "facet")
        .Field("error", e.what())
        .Error();
    SendError(res, kHttpInternalServerError, "Internal server error", mygram::utils::ErrorCode::kInternalError);
  }
}

void HttpServer::HandleGet(const httplib::Request& req, httplib::Response& res) {
  RecordRequest();

  try {
    // Check if server is loading
    if (loading_ != nullptr && loading_->load()) {
      SendError(res, kHttpServiceUnavailable, "Server is loading, please try again later",
                mygram::utils::ErrorCode::kServerLoading);
      return;
    }

    // Extract table name and primary key from URL. Use the shared resolution helper so
    // GET applies the same table-name whitelist and null-context guards as
    // SEARCH and COUNT.
    std::string primary_key = ExtractRoutePrimaryKey(req);
    auto lookup = ResolveHttpTableContext(ExtractRouteTableKey(req));
    if (lookup.table_ctx == nullptr) {
      SendError(res, lookup.status, lookup.message, lookup.code);
      return;
    }
    if (RejectIfTableSyncing(lookup.table_key, res)) {
      return;
    }
    std::shared_lock<std::shared_mutex> generation_lock(*lookup.table_ctx->generation_mutex);
    auto* current_doc_store = lookup.table_ctx->doc_store.get();

    RecordCommand(query::QueryType::GET);

    auto doc_id = current_doc_store->GetDocId(primary_key);
    if (!doc_id.has_value()) {
      SendError(res, kHttpNotFound, "Document not found", mygram::utils::ErrorCode::kIndexDocumentNotFound);
      return;
    }

    auto doc = current_doc_store->GetDocument(*doc_id);
    if (!doc) {
      SendError(res, kHttpNotFound, "Document not found", mygram::utils::ErrorCode::kIndexDocumentNotFound);
      return;
    }

    // Build JSON response
    json response;
    response["primary_key"] = doc->primary_key;

    if (!doc->filters.empty()) {
      response["filters"] = FilterMapToJson(doc->filters);
    }

    SendJson(res, kHttpOk, response);

  } catch (const std::exception& e) {
    mygram::utils::StructuredLog().Event("http_handler_error").Field("handler", "get").Field("error", e.what()).Error();
    SendError(res, kHttpInternalServerError, "Internal server error", mygram::utils::ErrorCode::kInternalError);
  }
}

void HttpServer::HandleInfo(const httplib::Request& /*req*/, httplib::Response& res) {
  // Increment request counter on the effective stats instance
  RecordRequest();
  RecordCommand(query::QueryType::INFO);

  try {
    json response;

    // Use TCP server's stats if available (includes all protocol stats), otherwise use HTTP-only stats
    ServerStats& effective_stats = GetEffectiveStats();

    // Server info
    response["server"] = "MygramDB";
    response["version"] = ::mygramdb::Version::String();
    response["uptime_seconds"] = effective_stats.GetUptimeSeconds();

    // Statistics (from TCP server if available)
    auto srv_stats = effective_stats.GetStatistics();
    response["total_requests"] = srv_stats.total_requests;
    response["total_commands_processed"] = srv_stats.total_commands_processed;

    // INFO and /metrics share a bounded snapshot because memory accounting
    // traverses retained document/index data and is intentionally expensive.
    const auto aggregated_metrics = statistics_snapshot_cache_.Get(table_contexts_);

    json tables_obj;
    for (const auto& [table_name, ctx] : table_contexts_) {
      const auto metrics_iter = aggregated_metrics.tables.find(table_name);
      if (metrics_iter == aggregated_metrics.tables.end()) {
        continue;
      }
      const auto& table_metrics = metrics_iter->second;

      // Per-table stats
      json table_obj;
      table_obj["documents"] = table_metrics.documents;
      table_obj["terms"] = table_metrics.terms;
      table_obj["postings"] = table_metrics.postings;
      table_obj["ngram_size"] = ctx->config.ngram_size;
      table_obj["memory_bytes"] = table_metrics.index_memory + table_metrics.document_memory;
      table_obj["memory_human"] =
          mygram::utils::FormatBytes(table_metrics.index_memory + table_metrics.document_memory);
      tables_obj[table_name] = table_obj;
    }

    // Update memory usage on the effective stats instance
    StatisticsService::UpdateServerStatistics(effective_stats, aggregated_metrics);

    json memory_obj;
    memory_obj["used_memory_bytes"] = aggregated_metrics.total_memory;
    memory_obj["used_memory_human"] = mygram::utils::FormatBytes(aggregated_metrics.total_memory);
    const auto peak_memory = effective_stats.GetPeakMemoryUsage();
    memory_obj["peak_memory_bytes"] = peak_memory;
    memory_obj["peak_memory_human"] = mygram::utils::FormatBytes(peak_memory);
    memory_obj["used_memory_index"] = mygram::utils::FormatBytes(aggregated_metrics.total_index_memory);
    memory_obj["used_memory_documents"] = mygram::utils::FormatBytes(aggregated_metrics.total_doc_memory);

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
    index_obj["total_documents"] = aggregated_metrics.total_documents;
    index_obj["total_terms"] = aggregated_metrics.total_terms;
    index_obj["total_postings"] = aggregated_metrics.total_postings;
    if (aggregated_metrics.total_terms > 0) {
      index_obj["avg_postings_per_term"] =
          static_cast<double>(aggregated_metrics.total_postings) / static_cast<double>(aggregated_metrics.total_terms);
    }
    index_obj["delta_encoded_lists"] = aggregated_metrics.total_delta_encoded;
    index_obj["roaring_bitmap_lists"] = aggregated_metrics.total_roaring_bitmap;
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
      cache_obj["invalidation_index_memory_bytes"] = cache_stats.invalidation_index_memory_bytes;
      cache_obj["invalidation_queue_memory_bytes"] = cache_stats.invalidation_queue_memory_bytes;
      cache_obj["accounted_memory_bytes"] = cache_stats.accounted_memory_bytes;
      cache_obj["accounted_memory_human"] = mygram::utils::FormatBytes(cache_stats.accounted_memory_bytes);
      cache_obj["evictions"] = cache_stats.evictions;
      cache_obj["ttl_expirations"] = cache_stats.ttl_expirations;
      cache_obj["rejection_count"] = cache_stats.rejection_count;
      cache_obj["rejection_oversize"] = cache_stats.rejection_oversize;
      cache_obj["rejection_memory_budget"] = cache_stats.rejection_memory_budget;
      cache_obj["rejection_duplicate"] = cache_stats.rejection_duplicate;
      cache_obj["stale_entry_removals"] = cache_stats.stale_entry_removals;
      cache_obj["decompression_failures"] = cache_stats.decompression_failures;
      cache_obj["stale_lru_entries"] = cache_stats.stale_lru_entries;
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
    SendError(res, kHttpInternalServerError, "Internal server error", mygram::utils::ErrorCode::kInternalError);
  }
}

ReadinessInputs HttpServer::CurrentReadinessInputs() const {
  ReadinessInputs inputs;
  inputs.binlog_reader = binlog_reader_;
  inputs.data_initialized = initial_data_ready_checker_ ? initial_data_ready_checker_() : true;
  inputs.loading = loading_ != nullptr && loading_->load();
  inputs.replication_paused_for_dump =
      replication_paused_for_dump_ != nullptr && replication_paused_for_dump_->load(std::memory_order_acquire);
  inputs.sync_in_progress = any_syncing_checker_ ? any_syncing_checker_() :
#ifdef USE_MYSQL
                                                 sync_manager_ != nullptr && sync_manager_->IsAnySyncing();
#else
                                                 false;
#endif
  return inputs;
}

void HttpServer::HandleHealth(const httplib::Request& /*req*/, httplib::Response& res) {
  // Health probes are intentionally NOT counted in total_requests:
  // they are typically driven by orchestrators (Kubernetes liveness/readiness)
  // at high frequency and would distort QPS metrics for actual application traffic.
  // The route table carries that decision (`counts_requests`) so the pre-routing
  // denial branches account a rejected probe the same way.
  json response;
  response["status"] = "ok";
  response["timestamp"] = UnixTimestampSeconds();

  SendJson(res, kHttpOk, response);
}

void HttpServer::HandleHealthLive(const httplib::Request& /*req*/, httplib::Response& res) {
  // Health probe — not counted in total_requests; see HandleHealth.
  // Liveness probe: Always return 200 OK if the process is running
  // This is used by orchestrators (Kubernetes, Docker) to detect deadlocks
  json response;
  response["status"] = "alive";
  response["timestamp"] = UnixTimestampSeconds();

  SendJson(res, kHttpOk, response);
}

void HttpServer::HandleHealthReady(const httplib::Request& req, httplib::Response& res) {
  // Health probe — not counted in total_requests; see HandleHealth.
  // Readiness probe: Return 200 OK if ready to accept traffic, 503 otherwise
  const ReadinessVerdict verdict = EvaluateReadiness(CurrentReadinessInputs());

  json response;
  response["loading"] = verdict.loading;
  response["data_initialized"] = verdict.data_initialized;
#ifdef USE_MYSQL
  if (binlog_reader_ != nullptr) {
    response["replication_running"] = verdict.replication_available();
    response["replication_starting"] = verdict.replication == ReplicationAvailability::kStarting;
    response["replication_paused_for_dump"] = verdict.replication_paused_for_dump;
    response["sync_in_progress"] = verdict.sync_in_progress;
    // The probe names the fault by code and by the code's own description. The
    // reader's message is verbatim MySQL text that can carry the replication
    // account and the address the server sees, so it stays on the credentialed
    // /replication/status route and in the structured log.
    response["replication_last_error"] = ReplicationErrorSummary(*binlog_reader_);
    response["replication_last_error_code"] = static_cast<uint16_t>(binlog_reader_->GetLastErrorCode());
    response["replication_last_applied_unixtime"] = binlog_reader_->GetLastAppliedUnixTime();
    response["replication_seconds_since_last_applied"] = binlog_reader_->GetSecondsSinceLastApplied();
    if (AdminCredentialsAccepted(req)) {
      response["replication_crc_errors"] = binlog_reader_->GetCRCErrors();
      response["replication_schema_incompatible"] = binlog_reader_->HasSchemaIncompatibleError();
    }
  }
#endif

  response["status"] = verdict.ready ? "ready" : "not_ready";
  if (!verdict.ready) {
    response["reason"] = verdict.reason;
  }
  response["timestamp"] = UnixTimestampSeconds();
  SendJson(res, verdict.ready ? kHttpOk : kHttpServiceUnavailable, response);
}

void HttpServer::HandleHealthDetail(const httplib::Request& req, httplib::Response& res) {
  // Health probe — not counted in total_requests; see HandleHealth.
  // Detailed health: Return comprehensive component status
  json response;

  // Overall status. The verdict is the same one /health/ready renders, so this
  // route cannot raise an alert for a state the readiness probe calls healthy.
  const ReadinessVerdict verdict = EvaluateReadiness(CurrentReadinessInputs());
  const bool is_loading = verdict.loading;
  response["status"] = verdict.ready ? "healthy" : "degraded";
  if (!verdict.ready) {
    response["reason"] = verdict.reason;
  }
  response["timestamp"] = UnixTimestampSeconds();

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
      std::shared_lock<std::shared_mutex> generation_lock(*ctx->generation_mutex);
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
    const bool cache_enabled = cache_manager_->IsEnabled();
    cache_comp["status"] = cache_enabled ? "ok" : "disabled";
    cache_comp["enabled"] = cache_enabled;
    cache_comp["hit_rate"] = cache_stats.HitRate();
    cache_comp["total_hits"] = cache_stats.cache_hits;
    cache_comp["total_misses"] = cache_stats.cache_misses;
    cache_comp["current_entries"] = cache_stats.current_entries;
    components["cache"] = cache_comp;
  }

#ifdef USE_MYSQL
  // Binlog component (if available)
  if (binlog_reader_ != nullptr) {
    json binlog_comp;
    // The binlog position, the counters derived from it, and the schema
    // verdict are what GET /replication/status protects. Serving them from an
    // open probe would leave that route gated in name only.
    const bool expose_replication_detail = AdminCredentialsAccepted(req);
    const bool replication_starting = verdict.replication == ReplicationAvailability::kStarting;
    binlog_comp["status"] = ToString(verdict.replication);
    if (verdict.replication == ReplicationAvailability::kRunning || replication_starting) {
      binlog_comp["running"] = !replication_starting;
      binlog_comp["starting"] = replication_starting;
      if (expose_replication_detail) {
        binlog_comp["current_gtid"] = binlog_reader_->GetCurrentGTID();
        binlog_comp["processed_events"] = binlog_reader_->GetProcessedEvents();
        binlog_comp["queue_size"] = binlog_reader_->GetQueueSize();
      }
    } else {
      binlog_comp["running"] = false;
      binlog_comp["paused_for_dump"] = verdict.replication_paused_for_dump;
    }
    binlog_comp["replication_state"] = mysql::ToString(binlog_reader_->GetReplicationState());
    binlog_comp["last_error_code"] = static_cast<uint16_t>(binlog_reader_->GetLastErrorCode());
    binlog_comp["last_error"] = ReplicationErrorSummary(*binlog_reader_);
    binlog_comp["last_applied_unixtime"] = binlog_reader_->GetLastAppliedUnixTime();
    binlog_comp["seconds_since_last_applied"] = binlog_reader_->GetSecondsSinceLastApplied();
    if (expose_replication_detail) {
      binlog_comp["crc_errors"] = binlog_reader_->GetCRCErrors();
      binlog_comp["schema_incompatible"] = binlog_reader_->HasSchemaIncompatibleError();
    }
    components["binlog"] = binlog_comp;
  }
#endif

  response["components"] = components;

  SendJson(res, kHttpOk, response);
}

void HttpServer::HandleConfig(const httplib::Request& /*req*/, httplib::Response& res) {
  RecordRequest();
  RecordCommand(query::QueryType::CONFIG_SHOW);

  if (full_config_ == nullptr) {
    SendError(res, kHttpInternalServerError, "Configuration not available", mygram::utils::ErrorCode::kInternalError);
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
    SendError(res, kHttpInternalServerError, "Internal server error", mygram::utils::ErrorCode::kInternalError);
  }
}

void HttpServer::HandleReplicationStatus(const httplib::Request& /*req*/, httplib::Response& res) {
  RecordRequest();
  RecordCommand(query::QueryType::REPLICATION_STATUS);

#ifdef USE_MYSQL
  if (binlog_reader_ == nullptr) {
    SendError(res, kHttpServiceUnavailable, "Replication not configured", mygram::utils::ErrorCode::kNotImplemented);
    return;
  }

  try {
    json response;
    const bool is_running = binlog_reader_->IsRunning();
    const auto replication_state = binlog_reader_->GetReplicationState();
    response["enabled"] = is_running;
    response["status"] = mysql::ToString(replication_state);
    response["current_gtid"] = binlog_reader_->GetCurrentGTID();
    response["processed_events"] = binlog_reader_->GetProcessedEvents();
    response["queue_size"] = binlog_reader_->GetQueueSize();
    response["crc_errors"] = binlog_reader_->GetCRCErrors();
    response["schema_incompatible"] = binlog_reader_->HasSchemaIncompatibleError();
    response["last_error_code"] = static_cast<uint16_t>(binlog_reader_->GetLastErrorCode());
    response["last_error"] = binlog_reader_->GetLastError();
    response["last_applied_unixtime"] = binlog_reader_->GetLastAppliedUnixTime();
    response["seconds_since_last_applied"] = binlog_reader_->GetSecondsSinceLastApplied();

    SendJson(res, kHttpOk, response);

  } catch (const std::exception& e) {
    mygram::utils::StructuredLog()
        .Event("http_handler_error")
        .Field("handler", "replication_status")
        .Field("error", e.what())
        .Error();
    SendError(res, kHttpInternalServerError, "Internal server error", mygram::utils::ErrorCode::kInternalError);
  }
#else
  SendError(res, kHttpServiceUnavailable, "MySQL replication not compiled", mygram::utils::ErrorCode::kNotImplemented);
#endif
}

void HttpServer::HandleOptimize(const httplib::Request& req, httplib::Response& res) {
  RecordRequest();

  if (!HasJsonContentType(req)) {
    SendError(res, kHttpUnsupportedMediaType, "Content-Type must be application/json",
              mygram::utils::ErrorCode::kNetworkInvalidRequest);
    return;
  }

  json body;
  try {
    body = json::parse(req.body);
  } catch (const json::parse_error& error) {
    SendError(res, kHttpBadRequest, "Invalid JSON: " + std::string(error.what()),
              mygram::utils::ErrorCode::kQuerySyntaxError);
    return;
  }
  if (!body.is_object()) {
    SendError(res, kHttpBadRequest, "Request body must be a JSON object", mygram::utils::ErrorCode::kQuerySyntaxError);
    return;
  }
  for (const auto& [key, value] : body.items()) {
    (void)value;
    if (key != "table") {
      SendError(res, kHttpBadRequest, "Unsupported field: " + key, mygram::utils::ErrorCode::kQuerySyntaxError);
      return;
    }
  }

  std::string table;
  if (body.contains("table")) {
    if (!body["table"].is_string()) {
      SendError(res, kHttpBadRequest, "Field 'table' must be a string", mygram::utils::ErrorCode::kQuerySyntaxError);
      return;
    }
    table = body["table"].get<std::string>();
    auto lookup = ResolveHttpTableContext(table);
    if (lookup.table_ctx == nullptr) {
      SendError(res, lookup.status, lookup.message, lookup.code);
      return;
    }
    table = std::move(lookup.table_key);
  }

  if (!optimize_callback_) {
    SendError(res, kHttpServiceUnavailable, "OPTIMIZE handler is not available",
              mygram::utils::ErrorCode::kServerInitMissingDependency);
    return;
  }

  RecordCommand(query::QueryType::OPTIMIZE);
  const std::string result = optimize_callback_(table);
  constexpr std::string_view kOkPrefix = "OK ";
  if (result.rfind(kOkPrefix, 0) == 0) {
    json response;
    response["status"] = "ok";
    response["result"] = result.substr(kOkPrefix.size());
    SendJson(res, kHttpOk, response);
    return;
  }

  if (const auto error_frame = protocol::ParseErrorFrame(result); error_frame.has_value()) {
    const auto code = error_frame->code.has_value() ? static_cast<mygram::utils::ErrorCode>(*error_frame->code)
                                                    : mygram::utils::ErrorCode::kUnknown;
    SendError(res, kHttpServiceUnavailable, std::string(error_frame->message), code);
    return;
  }
  SendError(res, kHttpServiceUnavailable, result, mygram::utils::ErrorCode::kInternalError);
}

void HttpServer::HandleMetrics(const httplib::Request& /*req*/, httplib::Response& res) {
  RecordRequest();

  try {
    ServerStats& effective_stats = GetEffectiveStats();

    // Aggregate metrics
    auto aggregated_metrics = statistics_snapshot_cache_.Get(table_contexts_);

    // Update server statistics
    StatisticsService::UpdateServerStatistics(effective_stats, aggregated_metrics);

    // Format response
    std::string metrics = ResponseFormatter::FormatPrometheusMetrics(
        aggregated_metrics, effective_stats, table_contexts_, binlog_reader_, cache_manager_, thread_pool_);
    res.status = kHttpOk;
    res.set_content(metrics, "text/plain; version=0.0.4; charset=utf-8");
  } catch (const std::exception& e) {
    mygram::utils::StructuredLog()
        .Event("http_handler_error")
        .Field("handler", "metrics")
        .Field("error", e.what())
        .Error();
    SendError(res, kHttpInternalServerError, "Internal server error", mygram::utils::ErrorCode::kInternalError);
  }
}

void HttpServer::SendJson(httplib::Response& res, int status_code, const nlohmann::json& body) {
  res.status = status_code;
  res.set_content(body.dump(-1, ' ', false, nlohmann::json::error_handler_t::replace), "application/json");
}

void HttpServer::SendError(httplib::Response& res, int status_code, const std::string& message,
                           mygram::utils::ErrorCode code) {
  json error_obj;
  error_obj["error"] = message;
  error_obj["error_code"] = static_cast<std::uint16_t>(code);
  SendJson(res, status_code, error_obj);
}

void HttpServer::SendError(httplib::Response& res, int status_code, const mygram::utils::Error& error) {
  // Render through the same helper the TCP formatter uses, so one Error object
  // names the same cause (GTID, table, host:port) on both surfaces.
  SendError(res, status_code, ResponseFormatter::FormatErrorMessage(error), error.code());
}

}  // namespace mygramdb::server
