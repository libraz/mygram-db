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
#include <variant>

#include "cache/cache_manager.h"
#include "server/response_formatter.h"
#include "server/statistics_service.h"
#include "server/tcp_server.h"  // For TableContext definition
#include "storage/document_store.h"
#include "utils/memory_utils.h"
#include "utils/string_utils.h"
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
constexpr int kHttpNotFound = 404;
constexpr int kHttpInternalServerError = 500;
constexpr int kHttpServiceUnavailable = 503;

// Server startup delay (milliseconds)
constexpr int kStartupDelayMs = 100;
}  // namespace

using storage::DocId;

HttpServer::HttpServer(HttpServerConfig config, std::unordered_map<std::string, TableContext*> table_contexts,
                       const config::Config* full_config,
#ifdef USE_MYSQL
                       mysql::BinlogReader* binlog_reader,
#else
                       void* binlog_reader,
#endif
                       cache::CacheManager* cache_manager)
    : config_(std::move(config)),
      table_contexts_(std::move(table_contexts)),
      full_config_(full_config),
      binlog_reader_(binlog_reader),
      cache_manager_(cache_manager) {
  if (full_config_ != nullptr) {
    const auto configured_limit = full_config_->api.max_query_length;
    const size_t limit = configured_limit <= 0 ? 0 : static_cast<size_t>(configured_limit);
    query_parser_.SetMaxQueryLength(limit);
  }

  server_ = std::make_unique<httplib::Server>();

  // Set timeouts
  server_->set_read_timeout(config_.read_timeout_sec, 0);
  server_->set_write_timeout(config_.write_timeout_sec, 0);

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
  server_->Post(R"(/(\w+)/search)",
                [this](const httplib::Request& req, httplib::Response& res) { HandleSearch(req, res); });

  // GET /{table}/:id - Get document by ID
  server_->Get(R"(/(\w+)/(\d+))", [this](const httplib::Request& req, httplib::Response& res) { HandleGet(req, res); });

  // GET /info - Server information
  server_->Get("/info", [this](const httplib::Request& req, httplib::Response& res) { HandleInfo(req, res); });

  // GET /health - Health check
  server_->Get("/health", [this](const httplib::Request& req, httplib::Response& res) { HandleHealth(req, res); });

  // GET /config - Configuration
  server_->Get("/config", [this](const httplib::Request& req, httplib::Response& res) { HandleConfig(req, res); });

  // GET /replication/status - Replication status
  server_->Get("/replication/status",
               [this](const httplib::Request& req, httplib::Response& res) { HandleReplicationStatus(req, res); });

  // GET /metrics - Prometheus metrics
  server_->Get("/metrics", [this](const httplib::Request& req, httplib::Response& res) { HandleMetrics(req, res); });
}

void HttpServer::SetupCors() {
  // CORS preflight
  server_->Options(".*", [](const httplib::Request& /*req*/, httplib::Response& res) {
    res.set_header("Access-Control-Allow-Origin", "*");
    res.set_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
    res.set_header("Access-Control-Allow-Headers", "Content-Type");
    res.status = kHttpNoContent;
  });

  // Add CORS headers to all responses
  server_->set_post_routing_handler([](const httplib::Request& /*req*/, httplib::Response& res) {
    res.set_header("Access-Control-Allow-Origin", "*");
  });
}

bool HttpServer::Start() {
  if (running_) {
    last_error_ = "Server already running";
    return false;
  }

  // Start server in separate thread
  server_thread_ = std::make_unique<std::thread>([this]() {
    spdlog::info("Starting HTTP server on {}:{}", config_.bind, config_.port);
    running_ = true;

    if (!server_->listen(config_.bind, config_.port)) {
      last_error_ = "Failed to bind to " + config_.bind + ":" + std::to_string(config_.port);
      spdlog::error("{}", last_error_);
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
    return false;
  }

  spdlog::info("HTTP server started successfully on {}:{}", config_.bind, config_.port);
  return true;
}

void HttpServer::Stop() {
  if (!running_) {
    return;
  }

  spdlog::info("Stopping HTTP server...");
  running_ = false;

  if (server_) {
    server_->stop();
  }

  if (server_thread_ && server_thread_->joinable()) {
    server_thread_->join();
  }

  spdlog::info("HTTP server stopped");
}

void HttpServer::HandleSearch(const httplib::Request& req, httplib::Response& res) {
  stats_.IncrementRequests();

  try {
    // Extract table name from URL
    std::string table = req.matches[1];

    // Lookup table
    auto table_iter = table_contexts_.find(table);
    if (table_iter == table_contexts_.end()) {
      SendError(res, kHttpNotFound, "Table not found: " + table);
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

    // Add filters
    if (body.contains("filters") && body["filters"].is_object()) {
      for (const auto& [key, val] : body["filters"].items()) {
        query_str << " FILTER " << key << "=";
        if (val.is_string()) {
          query_str << val.get<std::string>();
        } else {
          query_str << val.dump();
        }
      }
    }

    // Add limit
    if (body.contains("limit")) {
      query_str << " LIMIT " << body["limit"].get<int>();
    }

    // Add offset
    if (body.contains("offset")) {
      query_str << " OFFSET " << body["offset"].get<int>();
    }

    // Parse and execute query
    auto query = query_parser_.Parse(query_str.str());
    if (!query.IsValid()) {
      SendError(res, kHttpBadRequest, "Invalid query: " + query_parser_.GetError());
      return;
    }

    // Get ngram size for this table
    int current_ngram_size = table_iter->second->config.ngram_size;

    // Collect all search terms (main + AND terms)
    std::vector<std::string> all_search_terms;
    if (!query.search_text.empty()) {
      all_search_terms.push_back(query.search_text);
    }
    all_search_terms.insert(all_search_terms.end(), query.and_terms.begin(), query.and_terms.end());

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
      if (current_ngram_size == 0) {
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
    if (!query.not_terms.empty()) {
      std::vector<DocId> not_results_union;
      for (const auto& not_term : query.not_terms) {
        std::string normalized = utils::NormalizeText(not_term, true, "keep", true);
        std::vector<std::string> ngrams;
        if (current_ngram_size == 0) {
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
    if (!query.filters.empty()) {
      std::vector<DocId> filtered_results;
      for (const auto& doc_id : results) {
        auto doc = current_doc_store->GetDocument(doc_id);
        if (!doc) {
          continue;
        }

        bool matches_all_filters = true;
        for (const auto& filter_cond : query.filters) {
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
                  return false;
                } else if constexpr (std::is_same_v<T, std::string>) {
                  return val == filter_cond.value;
                } else {
                  return std::to_string(val) == filter_cond.value;
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

    // Store total count before applying limit/offset
    size_t total_count = results.size();

    // Apply limit and offset
    size_t start_idx = std::min(static_cast<size_t>(query.offset), results.size());
    size_t end_idx = std::min(start_idx + query.limit, results.size());

    if (start_idx < results.size()) {
      results = std::vector<DocId>(results.begin() + static_cast<std::vector<DocId>::difference_type>(start_idx),
                                   results.begin() + static_cast<std::vector<DocId>::difference_type>(end_idx));
    } else {
      results.clear();
    }

    // Build JSON response
    json response;
    response["count"] = total_count;
    response["limit"] = query.limit;
    response["offset"] = query.offset;

    json results_array = json::array();
    for (const auto& doc_id : results) {
      auto doc = current_doc_store->GetDocument(doc_id);
      if (doc) {
        json doc_obj;
        doc_obj["doc_id"] = doc->doc_id;
        doc_obj["primary_key"] = doc->primary_key;

        // Add filters
        if (!doc->filters.empty()) {
          json filters_obj;
          for (const auto& [key, val] : doc->filters) {
            // Convert FilterValue to JSON
            std::visit(
                [&](auto&& arg) {
                  using T = std::decay_t<decltype(arg)>;
                  if constexpr (std::is_same_v<T, int64_t>) {
                    filters_obj[key] = arg;
                  } else if constexpr (std::is_same_v<T, double>) {
                    filters_obj[key] = arg;
                  } else if constexpr (std::is_same_v<T, std::string>) {
                    filters_obj[key] = arg;
                  } else if constexpr (std::is_same_v<T, int64_t>) {
                    filters_obj[key] = arg;
                  }
                },
                val);
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

void HttpServer::HandleGet(const httplib::Request& req, httplib::Response& res) {
  stats_.IncrementRequests();

  try {
    // Extract table name and ID from URL
    std::string table = req.matches[1];
    std::string id_str = req.matches[2];

    // Lookup table
    auto table_iter = table_contexts_.find(table);
    if (table_iter == table_contexts_.end()) {
      SendError(res, kHttpNotFound, "Table not found: " + table);
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
        // Convert FilterValue to JSON
        std::visit(
            [&](auto&& arg) {
              using T = std::decay_t<decltype(arg)>;
              if constexpr (std::is_same_v<T, int64_t>) {
                filters_obj[key] = arg;
              } else if constexpr (std::is_same_v<T, double>) {
                filters_obj[key] = arg;
              } else if constexpr (std::is_same_v<T, std::string>) {
                filters_obj[key] = arg;
              }
            },
            val);
      }
      response["filters"] = filters_obj;
    }

    SendJson(res, kHttpOk, response);

  } catch (const std::exception& e) {
    SendError(res, kHttpInternalServerError, "Internal error: " + std::string(e.what()));
  }
}

void HttpServer::HandleInfo(const httplib::Request& /*req*/, httplib::Response& res) {
  stats_.IncrementRequests();

  try {
    json response;

    // Server info
    response["server"] = "MygramDB";
    response["version"] = mygramdb::Version::String();
    response["uptime_seconds"] = stats_.GetUptimeSeconds();

    // Statistics
    auto srv_stats = stats_.GetStatistics();
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
    stats_.UpdateMemoryUsage(total_memory);

    json memory_obj;
    memory_obj["used_memory_bytes"] = total_memory;
    memory_obj["used_memory_human"] = utils::FormatBytes(total_memory);
    memory_obj["peak_memory_bytes"] = stats_.GetPeakMemoryUsage();
    memory_obj["peak_memory_human"] = utils::FormatBytes(stats_.GetPeakMemoryUsage());
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

void HttpServer::HandleConfig(const httplib::Request& /*req*/, httplib::Response& res) {
  stats_.IncrementRequests();

  if (full_config_ == nullptr) {
    SendError(res, kHttpInternalServerError, "Configuration not available");
    return;
  }

  try {
    json response;

    // MySQL config
    json mysql_obj;
    mysql_obj["host"] = full_config_->mysql.host;
    mysql_obj["port"] = full_config_->mysql.port;
    mysql_obj["database"] = full_config_->mysql.database;
    mysql_obj["user"] = full_config_->mysql.user;
    response["mysql"] = mysql_obj;

    // API config
    json api_obj;
    api_obj["tcp"]["bind"] = full_config_->api.tcp.bind;
    api_obj["tcp"]["port"] = full_config_->api.tcp.port;
    api_obj["http"]["enable"] = full_config_->api.http.enable;
    api_obj["http"]["bind"] = full_config_->api.http.bind;
    api_obj["http"]["port"] = full_config_->api.http.port;
    response["api"] = api_obj;

    // Replication config
    json repl_obj;
    repl_obj["enable"] = full_config_->replication.enable;
    repl_obj["server_id"] = full_config_->replication.server_id;
    response["replication"] = repl_obj;

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
    // Aggregate metrics
    auto aggregated_metrics = StatisticsService::AggregateMetrics(table_contexts_);

    // Update server statistics
    StatisticsService::UpdateServerStatistics(stats_, aggregated_metrics);

    // Format response
    std::string metrics =
        ResponseFormatter::FormatPrometheusMetrics(aggregated_metrics, stats_, table_contexts_, binlog_reader_);
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
