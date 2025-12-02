/**
 * @file http_server.cpp
 * @brief HTTP server implementation
 */

#include "server/http_server.h"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <chrono>
#include <nlohmann/json.hpp>
#include <sstream>
#include <type_traits>
#include <variant>

#include "cache/cache_manager.h"
#include "query/result_sorter.h"
#include "server/response_formatter.h"
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

#ifdef USE_MYSQL
#include "mysql/binlog_reader.h"
#endif

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

// Server startup delay (milliseconds)
constexpr int kStartupDelayMs = 100;

std::vector<utils::CIDR> ParseAllowCidrs(const std::vector<std::string>& allow_cidrs) {
  std::vector<utils::CIDR> parsed;
  parsed.reserve(allow_cidrs.size());

  for (const auto& cidr_str : allow_cidrs) {
    auto cidr = utils::CIDR::Parse(cidr_str);
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
}  // namespace

using storage::DocId;

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

    // Check CIDR-based access control first
    if (!utils::IsIPAllowed(req.remote_addr, parsed_allow_cidrs_)) {
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

  // Store error from thread (if any)
  std::string thread_error;
  std::mutex error_mutex;

  // Start server in separate thread
  server_thread_ = std::make_unique<std::thread>([this, &thread_error, &error_mutex]() {
    mygram::utils::StructuredLog()
        .Event("http_server_starting")
        .Field("bind", config_.bind)
        .Field("port", static_cast<uint64_t>(config_.port))
        .Info();

    if (!server_->listen(config_.bind, config_.port)) {
      std::lock_guard<std::mutex> lock(error_mutex);
      thread_error = "Failed to bind to " + config_.bind + ":" + std::to_string(config_.port);
      mygram::utils::StructuredLog()
          .Event("server_error")
          .Field("operation", "http_server_listen")
          .Field("bind", config_.bind)
          .Field("port", static_cast<uint64_t>(config_.port))
          .Field("error", thread_error)
          .Error();
      running_ = false;
      return;
    }
  });

  // Wait a bit for server to start
  std::this_thread::sleep_for(std::chrono::milliseconds(kStartupDelayMs));

  if (!running_) {
    if (server_thread_ && server_thread_->joinable()) {
      server_thread_->join();
    }
    std::lock_guard<std::mutex> lock(error_mutex);
    auto error =
        MakeError(ErrorCode::kNetworkBindFailed, thread_error.empty() ? "Failed to start HTTP server" : thread_error);
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
    auto* current_index = table_iter->second->index.get();
    auto* current_doc_store = table_iter->second->doc_store.get();

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

    // Build query string for QueryParser
    std::ostringstream query_str;
    query_str << "SEARCH " << table << " " << body["q"].get<std::string>();

    // Add limit
    if (body.contains("limit")) {
      query_str << " LIMIT " << body["limit"].get<int>();
    }

    // Add offset
    if (body.contains("offset")) {
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
    // Format 1: {"filters": {"col": "value"}} - backward compatible, defaults to EQ
    // Format 2: {"filters": {"col": {"op": "GT", "value": "10"}}} - full operator support
    if (body.contains("filters") && body["filters"].is_object()) {
      query->filters.clear();
      for (const auto& [key, val] : body["filters"].items()) {
        query::FilterCondition filter;
        filter.column = key;

        // Check if value is an object with operator specification
        if (val.is_object() && val.contains("value")) {
          // Format 2: full operator support
          std::string op_str = val.value("op", "EQ");
          // Parse operator
          if (op_str == "EQ" || op_str == "==" || op_str == "=") {
            filter.op = query::FilterOp::EQ;
          } else if (op_str == "NE" || op_str == "!=" || op_str == "<>") {
            filter.op = query::FilterOp::NE;
          } else if (op_str == "GT" || op_str == ">") {
            filter.op = query::FilterOp::GT;
          } else if (op_str == "GTE" || op_str == ">=" || op_str == "≥") {
            filter.op = query::FilterOp::GTE;
          } else if (op_str == "LT" || op_str == "<") {
            filter.op = query::FilterOp::LT;
          } else if (op_str == "LTE" || op_str == "<=" || op_str == "≤") {
            filter.op = query::FilterOp::LTE;
          } else {
            SendError(res, kHttpBadRequest, "Invalid filter operator: " + op_str);
            return;
          }
          // Get value from nested object
          const auto& val_field = val["value"];
          filter.value = val_field.is_string() ? val_field.get<std::string>() : val_field.dump();
        } else {
          // Format 1: backward compatible, defaults to EQ
          filter.op = query::FilterOp::EQ;
          filter.value = val.is_string() ? val.get<std::string>() : val.dump();
        }
        query->filters.push_back(std::move(filter));
      }
    }

    // Try cache lookup first
    if (cache_manager_ != nullptr && cache_manager_->IsEnabled()) {
      auto cached_result = cache_manager_->Lookup(*query);
      if (cached_result.has_value()) {
        // Cache hit! Return cached results directly
        std::vector<DocId> cached_doc_ids = cached_result.value();  // Make non-const copy

        // Apply ORDER BY, LIMIT, OFFSET on cached results
        std::vector<DocId> sorted_results;
        if (query->order_by.has_value()) {
          // Get primary key column name from table config
          std::string pk_col = "id";
          auto tbl_it = table_contexts_.find(table);
          if (tbl_it != table_contexts_.end()) {
            pk_col = tbl_it->second->config.primary_key;
          }

          auto result = query::ResultSorter::SortAndPaginate(cached_doc_ids, *current_doc_store, *query, pk_col);
          if (!result.has_value()) {
            SendError(res, kHttpBadRequest, result.error().message());
            return;
          }
          sorted_results = std::move(result.value());
        } else {
          size_t start_idx = std::min(static_cast<size_t>(query->offset), cached_doc_ids.size());
          size_t end_idx = std::min(start_idx + query->limit, cached_doc_ids.size());
          if (start_idx < cached_doc_ids.size()) {
            sorted_results =
                std::vector<DocId>(cached_doc_ids.begin() + static_cast<std::vector<DocId>::difference_type>(start_idx),
                                   cached_doc_ids.begin() + static_cast<std::vector<DocId>::difference_type>(end_idx));
          }
        }

        // Build JSON response from cache
        json response;
        response["count"] = cached_doc_ids.size();
        response["limit"] = query->limit;
        response["offset"] = query->offset;

        json results_array = json::array();
        for (const auto& doc_id : sorted_results) {
          auto doc = current_doc_store->GetDocument(doc_id);
          if (doc) {
            json doc_obj;
            doc_obj["doc_id"] = doc->doc_id;
            doc_obj["primary_key"] = doc->primary_key;
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
        return;  // Cache hit, early return
      }
    }

    // Get ngram sizes for this table
    int current_ngram_size = table_iter->second->config.ngram_size;
    int current_kanji_ngram_size = table_iter->second->config.kanji_ngram_size;

    // Collect all search terms (main + AND terms)
    std::vector<std::string> all_search_terms;
    if (!query->search_text.empty()) {
      all_search_terms.push_back(query->search_text);
    }
    all_search_terms.insert(all_search_terms.end(), query->and_terms.begin(), query->and_terms.end());

    // Generate n-grams for each term (same as TCP server)
    struct TermInfo {
      std::vector<std::string> ngrams;
      size_t estimated_size;
    };
    std::vector<TermInfo> term_infos;
    term_infos.reserve(all_search_terms.size());

    for (const auto& search_term : all_search_terms) {
      std::string normalized = utils::NormalizeText(search_term, true, "keep", true);
      std::vector<std::string> ngrams;

      // Always use hybrid n-grams if kanji_ngram_size is configured (same as TCP)
      if (current_kanji_ngram_size > 0) {
        ngrams = utils::GenerateHybridNgrams(normalized, current_ngram_size, current_kanji_ngram_size);
      } else if (current_ngram_size == 0) {
        ngrams = utils::GenerateHybridNgrams(normalized);
      } else {
        ngrams = utils::GenerateNgrams(normalized, current_ngram_size);
      }

      // Estimate result size by checking the smallest posting list
      size_t min_size = std::numeric_limits<size_t>::max();
      for (const auto& ngram : ngrams) {
        const auto* posting = current_index->GetPostingList(ngram);
        if (posting != nullptr) {
          min_size = std::min(min_size, static_cast<size_t>(posting->Size()));
        } else {
          min_size = 0;
          break;
        }
      }

      term_infos.push_back({std::move(ngrams), min_size});
    }

    // Sort terms by estimated size (smallest first for faster intersection)
    std::sort(term_infos.begin(), term_infos.end(),
              [](const TermInfo& lhs, const TermInfo& rhs) { return lhs.estimated_size < rhs.estimated_size; });

    // If any term has zero results, return empty immediately
    std::vector<DocId> results;
    if (!term_infos.empty() && term_infos[0].estimated_size == 0) {
      results.clear();  // Empty results
    } else {
      // Perform intersection (AND of all terms)
      if (!term_infos.empty()) {
        results = current_index->SearchAnd(term_infos[0].ngrams);
        for (size_t i = 1; i < term_infos.size(); ++i) {
          if (results.empty()) {
            break;
          }
          auto term_results = current_index->SearchAnd(term_infos[i].ngrams);
          std::vector<DocId> intersection;
          std::set_intersection(results.begin(), results.end(), term_results.begin(), term_results.end(),
                                std::back_inserter(intersection));
          results = std::move(intersection);
        }
      }
    }

    // Apply NOT terms if specified
    if (!query->not_terms.empty()) {
      std::vector<DocId> not_results_union;
      for (const auto& not_term : query->not_terms) {
        std::string normalized = utils::NormalizeText(not_term, true, "keep", true);
        std::vector<std::string> ngrams;

        // Always use hybrid n-grams if kanji_ngram_size is configured (same as TCP)
        if (current_kanji_ngram_size > 0) {
          ngrams = utils::GenerateHybridNgrams(normalized, current_ngram_size, current_kanji_ngram_size);
        } else if (current_ngram_size == 0) {
          ngrams = utils::GenerateHybridNgrams(normalized);
        } else {
          ngrams = utils::GenerateNgrams(normalized, current_ngram_size);
        }
        auto not_results = current_index->SearchOr(ngrams);
        std::vector<DocId> union_result;
        std::set_union(not_results_union.begin(), not_results_union.end(), not_results.begin(), not_results.end(),
                       std::back_inserter(union_result));
        not_results_union = std::move(union_result);
      }

      // Remove documents matching NOT terms
      std::vector<DocId> filtered_results;
      std::set_difference(results.begin(), results.end(), not_results_union.begin(), not_results_union.end(),
                          std::back_inserter(filtered_results));
      results = std::move(filtered_results);
    }

    // Apply filters if specified
    if (!query->filters.empty()) {
      std::vector<DocId> filtered_results;
      for (const auto& doc_id : results) {
        auto doc = current_doc_store->GetDocument(doc_id);
        if (!doc) {
          continue;
        }

        bool matches_all_filters = true;
        for (const auto& filter_cond : query->filters) {
          auto filter_it = doc->filters.find(filter_cond.column);
          if (filter_it == doc->filters.end()) {
            matches_all_filters = false;
            break;
          }

          const auto& stored_value = filter_it->second;
          bool matches = std::visit(
              [&filter_cond](auto&& val) -> bool {
                using T = std::decay_t<decltype(val)>;
                if constexpr (std::is_same_v<T, std::monostate>) {
                  // NULL value: only match for NE operator
                  return filter_cond.op == query::FilterOp::NE;
                } else if constexpr (std::is_same_v<T, std::string>) {
                  // String comparison with all operators
                  const std::string& str_val = val;
                  switch (filter_cond.op) {
                    case query::FilterOp::EQ:
                      return str_val == filter_cond.value;
                    case query::FilterOp::NE:
                      return str_val != filter_cond.value;
                    case query::FilterOp::GT:
                      return str_val > filter_cond.value;
                    case query::FilterOp::GTE:
                      return str_val >= filter_cond.value;
                    case query::FilterOp::LT:
                      return str_val < filter_cond.value;
                    case query::FilterOp::LTE:
                      return str_val <= filter_cond.value;
                    default:
                      return false;
                  }
                } else if constexpr (std::is_same_v<T, bool>) {
                  // Boolean: convert filter value to bool, only EQ/NE meaningful
                  bool bool_filter = (filter_cond.value == "1" || filter_cond.value == "true");
                  switch (filter_cond.op) {
                    case query::FilterOp::EQ:
                      return val == bool_filter;
                    case query::FilterOp::NE:
                      return val != bool_filter;
                    default:
                      return false;  // GT/GTE/LT/LTE not meaningful for bools
                  }
                } else if constexpr (std::is_same_v<T, double>) {
                  // Floating-point comparison with all operators
                  double filter_val = 0.0;
                  try {
                    filter_val = std::stod(filter_cond.value);
                  } catch (const std::exception&) {
                    return false;  // Invalid number
                  }
                  switch (filter_cond.op) {
                    case query::FilterOp::EQ:
                      return val == filter_val;
                    case query::FilterOp::NE:
                      return val != filter_val;
                    case query::FilterOp::GT:
                      return val > filter_val;
                    case query::FilterOp::GTE:
                      return val >= filter_val;
                    case query::FilterOp::LT:
                      return val < filter_val;
                    case query::FilterOp::LTE:
                      return val <= filter_val;
                    default:
                      return false;
                  }
                } else if constexpr (std::is_same_v<T, storage::TimeValue>) {
                  // TIME value comparison: treat as signed integer (seconds)
                  int64_t filter_val = 0;
                  try {
                    filter_val = std::stoll(filter_cond.value);
                  } catch (const std::exception&) {
                    return false;  // Invalid number
                  }
                  switch (filter_cond.op) {
                    case query::FilterOp::EQ:
                      return val.seconds == filter_val;
                    case query::FilterOp::NE:
                      return val.seconds != filter_val;
                    case query::FilterOp::GT:
                      return val.seconds > filter_val;
                    case query::FilterOp::GTE:
                      return val.seconds >= filter_val;
                    case query::FilterOp::LT:
                      return val.seconds < filter_val;
                    case query::FilterOp::LTE:
                      return val.seconds <= filter_val;
                    default:
                      return false;
                  }
                } else if constexpr (std::is_same_v<T, uint64_t> || std::is_same_v<T, uint32_t> ||
                                     std::is_same_v<T, uint16_t> || std::is_same_v<T, uint8_t>) {
                  // Unsigned integer comparison - use unsigned parsing to handle large values
                  uint64_t filter_val = 0;
                  try {
                    filter_val = std::stoull(filter_cond.value);
                  } catch (const std::exception&) {
                    return false;  // Invalid number
                  }
                  auto unsigned_val = static_cast<uint64_t>(val);
                  switch (filter_cond.op) {
                    case query::FilterOp::EQ:
                      return unsigned_val == filter_val;
                    case query::FilterOp::NE:
                      return unsigned_val != filter_val;
                    case query::FilterOp::GT:
                      return unsigned_val > filter_val;
                    case query::FilterOp::GTE:
                      return unsigned_val >= filter_val;
                    case query::FilterOp::LT:
                      return unsigned_val < filter_val;
                    case query::FilterOp::LTE:
                      return unsigned_val <= filter_val;
                    default:
                      return false;
                  }
                } else {
                  // Signed integer comparison (int8_t, int16_t, int32_t, int64_t)
                  int64_t filter_val = 0;
                  try {
                    filter_val = std::stoll(filter_cond.value);
                  } catch (const std::exception&) {
                    return false;  // Invalid number
                  }
                  auto signed_val = static_cast<int64_t>(val);
                  switch (filter_cond.op) {
                    case query::FilterOp::EQ:
                      return signed_val == filter_val;
                    case query::FilterOp::NE:
                      return signed_val != filter_val;
                    case query::FilterOp::GT:
                      return signed_val > filter_val;
                    case query::FilterOp::GTE:
                      return signed_val >= filter_val;
                    case query::FilterOp::LT:
                      return signed_val < filter_val;
                    case query::FilterOp::LTE:
                      return signed_val <= filter_val;
                    default:
                      return false;
                  }
                }
              },
              stored_value);

          if (!matches) {
            matches_all_filters = false;
            break;
          }
        }

        if (matches_all_filters) {
          filtered_results.push_back(doc_id);
        }
      }
      results = std::move(filtered_results);
    }

    // Store total count before applying ORDER BY and limit/offset
    size_t total_count = results.size();

    // Insert into cache (cache stores results before pagination)
    if (cache_manager_ != nullptr && cache_manager_->IsEnabled()) {
      // Collect all ngrams from term_infos to enable proper cache invalidation
      std::set<std::string> all_ngrams;
      for (const auto& term_info : term_infos) {
        all_ngrams.insert(term_info.ngrams.begin(), term_info.ngrams.end());
      }
      cache_manager_->Insert(*query, results, all_ngrams, 0.0);
    }

    // Apply ORDER BY, LIMIT, OFFSET
    // Only use ResultSorter if ORDER BY is explicitly specified
    std::vector<DocId> sorted_results;
    if (query->order_by.has_value()) {
      // Get primary key column name from table config
      std::string pk_col = "id";
      auto tbl_it = table_contexts_.find(table);
      if (tbl_it != table_contexts_.end()) {
        pk_col = tbl_it->second->config.primary_key;
      }

      // Use ResultSorter for ORDER BY support (same as TCP)
      auto result = query::ResultSorter::SortAndPaginate(results, *current_doc_store, *query, pk_col);
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
    SendError(res, kHttpInternalServerError, "Internal error: " + std::string(e.what()));
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
    auto* current_index = table_iter->second->index.get();
    auto* current_doc_store = table_iter->second->doc_store.get();

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

    // Build query string for QueryParser (COUNT query)
    std::ostringstream query_str;
    query_str << "COUNT " << table << " " << body["q"].get<std::string>();

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
      query->filters.clear();
      for (const auto& [key, val] : body["filters"].items()) {
        query::FilterCondition filter;
        filter.column = key;

        // Check if value is an object with operator specification
        if (val.is_object() && val.contains("value")) {
          // Format 2: full operator support
          std::string op_str = val.value("op", "EQ");
          // Parse operator
          if (op_str == "EQ" || op_str == "==" || op_str == "=") {
            filter.op = query::FilterOp::EQ;
          } else if (op_str == "NE" || op_str == "!=" || op_str == "<>") {
            filter.op = query::FilterOp::NE;
          } else if (op_str == "GT" || op_str == ">") {
            filter.op = query::FilterOp::GT;
          } else if (op_str == "GTE" || op_str == ">=" || op_str == "≥") {
            filter.op = query::FilterOp::GTE;
          } else if (op_str == "LT" || op_str == "<") {
            filter.op = query::FilterOp::LT;
          } else if (op_str == "LTE" || op_str == "<=" || op_str == "≤") {
            filter.op = query::FilterOp::LTE;
          } else {
            SendError(res, kHttpBadRequest, "Invalid filter operator: " + op_str);
            return;
          }

          // Get the value
          json value_field = val["value"];
          if (value_field.is_string()) {
            filter.value = value_field.get<std::string>();
          } else if (value_field.is_number_integer()) {
            filter.value = std::to_string(value_field.get<int64_t>());
          } else if (value_field.is_number_float()) {
            filter.value = std::to_string(value_field.get<double>());
          } else if (value_field.is_boolean()) {
            filter.value = value_field.get<bool>() ? "1" : "0";
          } else {
            SendError(res, kHttpBadRequest, "Invalid filter value type for column: " + key);
            return;
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
            SendError(res, kHttpBadRequest, "Invalid filter value type for column: " + key);
            return;
          }
        }

        query->filters.push_back(filter);
      }
    }

    // Get ngram sizes for this table
    int current_ngram_size = table_iter->second->config.ngram_size;
    int current_kanji_ngram_size = table_iter->second->config.kanji_ngram_size;

    // Collect all search terms (main + AND terms)
    std::vector<std::string> all_search_terms;
    if (!query->search_text.empty()) {
      all_search_terms.push_back(query->search_text);
    }
    all_search_terms.insert(all_search_terms.end(), query->and_terms.begin(), query->and_terms.end());

    // Generate n-grams for each term
    struct TermInfo {
      std::vector<std::string> ngrams;
      size_t estimated_size;
    };
    std::vector<TermInfo> term_infos;
    term_infos.reserve(all_search_terms.size());

    for (const auto& search_term : all_search_terms) {
      std::string normalized = utils::NormalizeText(search_term, true, "keep", true);
      std::vector<std::string> ngrams;

      // Use hybrid n-grams if kanji_ngram_size is configured
      if (current_kanji_ngram_size > 0) {
        ngrams = utils::GenerateHybridNgrams(normalized, current_ngram_size, current_kanji_ngram_size);
      } else if (current_ngram_size == 0) {
        ngrams = utils::GenerateHybridNgrams(normalized);
      } else {
        ngrams = utils::GenerateNgrams(normalized, current_ngram_size);
      }

      // Estimate result size by checking the smallest posting list
      size_t min_size = std::numeric_limits<size_t>::max();
      for (const auto& ngram : ngrams) {
        const auto* posting = current_index->GetPostingList(ngram);
        if (posting != nullptr) {
          min_size = std::min(min_size, static_cast<size_t>(posting->Size()));
        } else {
          min_size = 0;
          break;
        }
      }

      term_infos.push_back({std::move(ngrams), min_size});
    }

    // Sort terms by estimated size (smallest first)
    std::sort(term_infos.begin(), term_infos.end(),
              [](const TermInfo& lhs, const TermInfo& rhs) { return lhs.estimated_size < rhs.estimated_size; });

    // Execute search - perform intersection (AND of all terms)
    std::vector<DocId> results;
    if (!term_infos.empty() && term_infos[0].estimated_size == 0) {
      results.clear();  // Empty results
    } else {
      if (!term_infos.empty()) {
        results = current_index->SearchAnd(term_infos[0].ngrams);
        for (size_t i = 1; i < term_infos.size(); ++i) {
          if (results.empty()) {
            break;
          }
          auto term_results = current_index->SearchAnd(term_infos[i].ngrams);
          std::vector<DocId> intersection;
          std::set_intersection(results.begin(), results.end(), term_results.begin(), term_results.end(),
                                std::back_inserter(intersection));
          results = std::move(intersection);
        }
      }
    }

    // Apply NOT terms if specified
    if (!query->not_terms.empty()) {
      for (const auto& not_term : query->not_terms) {
        std::string normalized = utils::NormalizeText(not_term, true, "keep", true);
        std::vector<std::string> not_ngrams;

        if (current_kanji_ngram_size > 0) {
          not_ngrams = utils::GenerateHybridNgrams(normalized, current_ngram_size, current_kanji_ngram_size);
        } else if (current_ngram_size == 0) {
          not_ngrams = utils::GenerateHybridNgrams(normalized);
        } else {
          not_ngrams = utils::GenerateNgrams(normalized, current_ngram_size);
        }

        auto not_results = current_index->SearchOr(not_ngrams);
        std::vector<DocId> difference;
        std::set_difference(results.begin(), results.end(), not_results.begin(), not_results.end(),
                            std::back_inserter(difference));
        results = std::move(difference);
      }
    }

    // Apply filters (same logic as search)
    if (!query->filters.empty()) {
      std::vector<DocId> filtered_results;
      filtered_results.reserve(results.size());

      for (DocId doc_id : results) {
        auto doc = current_doc_store->GetDocument(doc_id);
        if (!doc) {
          continue;  // Document not found (shouldn't happen)
        }

        bool matches_all_filters = true;
        for (const auto& filter_cond : query->filters) {
          auto stored_value_it = doc->filters.find(filter_cond.column);
          if (stored_value_it == doc->filters.end()) {
            matches_all_filters = false;
            break;
          }

          const storage::FilterValue& stored_value = stored_value_it->second;
          bool matches = std::visit(
              [&filter_cond](const auto& val) -> bool {
                using T = std::decay_t<decltype(val)>;
                if constexpr (std::is_same_v<T, std::monostate>) {
                  return false;  // NULL values don't match any filter
                } else if constexpr (std::is_same_v<T, std::string>) {
                  const std::string& str_val = val;
                  switch (filter_cond.op) {
                    case query::FilterOp::EQ:
                      return str_val == filter_cond.value;
                    case query::FilterOp::NE:
                      return str_val != filter_cond.value;
                    case query::FilterOp::GT:
                      return str_val > filter_cond.value;
                    case query::FilterOp::GTE:
                      return str_val >= filter_cond.value;
                    case query::FilterOp::LT:
                      return str_val < filter_cond.value;
                    case query::FilterOp::LTE:
                      return str_val <= filter_cond.value;
                    default:
                      return false;
                  }
                } else if constexpr (std::is_same_v<T, bool>) {
                  bool bool_filter = (filter_cond.value == "1" || filter_cond.value == "true");
                  switch (filter_cond.op) {
                    case query::FilterOp::EQ:
                      return val == bool_filter;
                    case query::FilterOp::NE:
                      return val != bool_filter;
                    default:
                      return false;
                  }
                } else if constexpr (std::is_same_v<T, double>) {
                  double filter_val = 0.0;
                  try {
                    filter_val = std::stod(filter_cond.value);
                  } catch (const std::exception&) {
                    return false;
                  }
                  switch (filter_cond.op) {
                    case query::FilterOp::EQ:
                      return val == filter_val;
                    case query::FilterOp::NE:
                      return val != filter_val;
                    case query::FilterOp::GT:
                      return val > filter_val;
                    case query::FilterOp::GTE:
                      return val >= filter_val;
                    case query::FilterOp::LT:
                      return val < filter_val;
                    case query::FilterOp::LTE:
                      return val <= filter_val;
                    default:
                      return false;
                  }
                } else if constexpr (std::is_same_v<T, storage::TimeValue>) {
                  // TIME value comparison: treat as signed integer (seconds)
                  int64_t filter_val = 0;
                  try {
                    filter_val = std::stoll(filter_cond.value);
                  } catch (const std::exception&) {
                    return false;  // Invalid number
                  }
                  switch (filter_cond.op) {
                    case query::FilterOp::EQ:
                      return val.seconds == filter_val;
                    case query::FilterOp::NE:
                      return val.seconds != filter_val;
                    case query::FilterOp::GT:
                      return val.seconds > filter_val;
                    case query::FilterOp::GTE:
                      return val.seconds >= filter_val;
                    case query::FilterOp::LT:
                      return val.seconds < filter_val;
                    case query::FilterOp::LTE:
                      return val.seconds <= filter_val;
                    default:
                      return false;
                  }
                } else if constexpr (std::is_same_v<T, uint64_t> || std::is_same_v<T, uint32_t> ||
                                     std::is_same_v<T, uint16_t> || std::is_same_v<T, uint8_t>) {
                  uint64_t filter_val = 0;
                  try {
                    filter_val = std::stoull(filter_cond.value);
                  } catch (const std::exception&) {
                    return false;
                  }
                  auto unsigned_val = static_cast<uint64_t>(val);
                  switch (filter_cond.op) {
                    case query::FilterOp::EQ:
                      return unsigned_val == filter_val;
                    case query::FilterOp::NE:
                      return unsigned_val != filter_val;
                    case query::FilterOp::GT:
                      return unsigned_val > filter_val;
                    case query::FilterOp::GTE:
                      return unsigned_val >= filter_val;
                    case query::FilterOp::LT:
                      return unsigned_val < filter_val;
                    case query::FilterOp::LTE:
                      return unsigned_val <= filter_val;
                    default:
                      return false;
                  }
                } else {
                  // Signed integer comparison
                  int64_t filter_val = 0;
                  try {
                    filter_val = std::stoll(filter_cond.value);
                  } catch (const std::exception&) {
                    return false;
                  }
                  auto signed_val = static_cast<int64_t>(val);
                  switch (filter_cond.op) {
                    case query::FilterOp::EQ:
                      return signed_val == filter_val;
                    case query::FilterOp::NE:
                      return signed_val != filter_val;
                    case query::FilterOp::GT:
                      return signed_val > filter_val;
                    case query::FilterOp::GTE:
                      return signed_val >= filter_val;
                    case query::FilterOp::LT:
                      return signed_val < filter_val;
                    case query::FilterOp::LTE:
                      return signed_val <= filter_val;
                    default:
                      return false;
                  }
                }
              },
              stored_value);

          if (!matches) {
            matches_all_filters = false;
            break;
          }
        }

        if (matches_all_filters) {
          filtered_results.push_back(doc_id);
        }
      }
      results = std::move(filtered_results);
    }

    // Build JSON response - just return count
    json response;
    response["count"] = results.size();

    SendJson(res, kHttpOk, response);

  } catch (const std::exception& e) {
    SendError(res, kHttpInternalServerError, "Internal error: " + std::string(e.what()));
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
    SendError(res, kHttpInternalServerError, "Internal error: " + std::string(e.what()));
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
      table_obj["memory_human"] = utils::FormatBytes(index_mem + doc_mem);
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
    memory_obj["used_memory_human"] = utils::FormatBytes(total_memory);
    memory_obj["peak_memory_bytes"] = effective_stats.GetPeakMemoryUsage();
    memory_obj["peak_memory_human"] = utils::FormatBytes(effective_stats.GetPeakMemoryUsage());
    memory_obj["used_memory_index"] = utils::FormatBytes(total_index_memory);
    memory_obj["used_memory_documents"] = utils::FormatBytes(total_doc_memory);

    // System memory information
    auto sys_info = utils::GetSystemMemoryInfo();
    if (sys_info) {
      memory_obj["total_system_memory"] = sys_info->total_physical_bytes;
      memory_obj["total_system_memory_human"] = utils::FormatBytes(sys_info->total_physical_bytes);
      memory_obj["available_system_memory"] = sys_info->available_physical_bytes;
      memory_obj["available_system_memory_human"] = utils::FormatBytes(sys_info->available_physical_bytes);
      if (sys_info->total_physical_bytes > 0) {
        double usage_ratio = 1.0 - static_cast<double>(sys_info->available_physical_bytes) /
                                       static_cast<double>(sys_info->total_physical_bytes);
        memory_obj["system_memory_usage_ratio"] = usage_ratio;
      }
    }

    // Process memory information
    auto proc_info = utils::GetProcessMemoryInfo();
    if (proc_info) {
      memory_obj["process_rss"] = proc_info->rss_bytes;
      memory_obj["process_rss_human"] = utils::FormatBytes(proc_info->rss_bytes);
      memory_obj["process_rss_peak"] = proc_info->peak_rss_bytes;
      memory_obj["process_rss_peak_human"] = utils::FormatBytes(proc_info->peak_rss_bytes);
    }

    // Memory health status
    auto health = utils::GetMemoryHealthStatus();
    memory_obj["memory_health"] = utils::MemoryHealthStatusToString(health);

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
      cache_obj["misses_invalidated"] = cache_stats.cache_misses_invalidated;
      cache_obj["total_queries"] = cache_stats.total_queries;
      cache_obj["hit_rate"] = cache_stats.HitRate();
      cache_obj["current_entries"] = cache_stats.current_entries;
      cache_obj["memory_bytes"] = cache_stats.current_memory_bytes;
      cache_obj["memory_human"] = utils::FormatBytes(cache_stats.current_memory_bytes);
      cache_obj["evictions"] = cache_stats.evictions;
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
    SendError(res, kHttpInternalServerError, "Internal error: " + std::string(e.what()));
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
    SendError(res, kHttpInternalServerError, "Internal error: " + std::string(e.what()));
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
    SendError(res, kHttpInternalServerError, "Internal error: " + std::string(e.what()));
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
                                                                     table_contexts_, binlog_reader_);
    res.status = kHttpOk;
    res.set_content(metrics, "text/plain; version=0.0.4; charset=utf-8");
  } catch (const std::exception& e) {
    SendError(res, kHttpInternalServerError, "Internal error: " + std::string(e.what()));
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
