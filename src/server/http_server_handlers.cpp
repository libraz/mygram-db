/**
 * @file http_server_handlers.cpp
 * @brief HTTP server request handler implementations
 */

#include <spdlog/spdlog.h>

#include <algorithm>
#include <chrono>
#include <nlohmann/json.hpp>
#include <sstream>
#include <type_traits>
#include <variant>

#include "cache/cache_manager.h"
#include "query/query_parser.h"
#include "query/result_sorter.h"
#include "server/handlers/search_handler.h"
#include "server/http_server.h"
#include "server/response_formatter.h"
#include "server/search_pipeline.h"
#include "server/statistics_service.h"
#include "server/tcp_server.h"  // For TableContext definition
#include "storage/document_store.h"
#include "utils/memory_utils.h"
#include "utils/structured_log.h"
#include "version.h"

#ifdef USE_MYSQL
#include "mysql/binlog_reader.h"
#endif

using json = nlohmann::json;

namespace mygramdb::server {

namespace {
// HTTP status codes
constexpr int kHttpOk = 200;
constexpr int kHttpBadRequest = 400;
constexpr int kHttpNotFound = 404;
constexpr int kHttpInternalServerError = 500;
constexpr int kHttpServiceUnavailable = 503;

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
 * @brief Parse filter conditions from a JSON body.
 *
 * Supports two formats:
 *   Format 1: {"filters": {"col": "value"}} - backward compatible, defaults to EQ
 *   Format 2: {"filters": {"col": {"op": "GT", "value": "10"}}} - full operator support
 *
 * @param body JSON request body
 * @param[out] filters Output vector of filter conditions
 * @return Empty string on success, or error message on failure
 */
std::string ParseFiltersFromJson(const json& body, std::vector<mygramdb::query::FilterCondition>& filters) {
  if (!body.contains("filters") || !body["filters"].is_object()) {
    return "";
  }

  filters.clear();
  for (const auto& [key, val] : body["filters"].items()) {
    mygramdb::query::FilterCondition filter;
    filter.column = key;

    if (val.is_object() && val.contains("value")) {
      // Format 2: full operator support
      std::string op_str = val.value("op", "EQ");
      auto parsed_op = mygramdb::query::QueryParser::ParseFilterOp(op_str);
      if (!parsed_op.has_value()) {
        return "Invalid filter operator: " + op_str;
      }
      filter.op = parsed_op.value();

      const auto& value_field = val["value"];
      auto str_val = JsonFilterValueToString(value_field);
      if (!str_val.has_value()) {
        return "Invalid filter value type for column: " + key;
      }
      filter.value = std::move(str_val.value());
    } else {
      // Format 1: backward compatible (defaults to EQ)
      filter.op = mygramdb::query::FilterOp::EQ;
      auto str_val = JsonFilterValueToString(val);
      if (!str_val.has_value()) {
        return "Invalid filter value type for column: " + key;
      }
      filter.value = std::move(str_val.value());
    }

    filters.push_back(std::move(filter));
  }

  return "";
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
    std::string filter_error = ParseFiltersFromJson(body, query->filters);
    if (!filter_error.empty()) {
      SendError(res, kHttpBadRequest, filter_error);
      return;
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
    bool current_cross_boundary = table_iter->second->config.cross_boundary_ngrams;

    // Collect all search terms (main + AND terms)
    std::vector<std::string> all_search_terms;
    if (!query->search_text.empty()) {
      all_search_terms.push_back(query->search_text);
    }
    all_search_terms.insert(all_search_terms.end(), query->and_terms.begin(), query->and_terms.end());

    // Generate term infos using shared pipeline
    auto term_infos = search_pipeline::GenerateTermInfos(all_search_terms, current_index, current_ngram_size,
                                                         current_kanji_ngram_size, current_cross_boundary);

    // Sort by estimated size (smallest first for faster intersection)
    std::sort(term_infos.begin(), term_infos.end(),
              [](const SearchTermInfo& a, const SearchTermInfo& b) { return a.estimated_size < b.estimated_size; });

    // Execute search pipeline
    auto pipeline_result = search_pipeline::Execute(
        *query, term_infos, all_search_terms, current_index, current_doc_store, full_config_, current_ngram_size,
        current_kanji_ngram_size, current_cross_boundary, SearchHandler::GetFilterThreshold());

    auto& results = pipeline_result.results;

    // Store total count before applying ORDER BY and limit/offset
    size_t total_count = results.size();

    // Insert into cache (cache stores results before pagination)
    search_pipeline::InsertToCache(cache_manager_, *query, results, term_infos, 0.0, current_ngram_size,
                                   current_kanji_ngram_size, current_cross_boundary);

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
    std::string filter_error = ParseFiltersFromJson(body, query->filters);
    if (!filter_error.empty()) {
      SendError(res, kHttpBadRequest, filter_error);
      return;
    }

    // Get ngram sizes for this table
    int current_ngram_size = table_iter->second->config.ngram_size;
    int current_kanji_ngram_size = table_iter->second->config.kanji_ngram_size;
    bool current_cross_boundary = table_iter->second->config.cross_boundary_ngrams;

    // Collect all search terms (main + AND terms)
    std::vector<std::string> all_search_terms;
    if (!query->search_text.empty()) {
      all_search_terms.push_back(query->search_text);
    }
    all_search_terms.insert(all_search_terms.end(), query->and_terms.begin(), query->and_terms.end());

    // Generate term infos using shared pipeline
    auto term_infos = search_pipeline::GenerateTermInfos(all_search_terms, current_index, current_ngram_size,
                                                         current_kanji_ngram_size, current_cross_boundary);

    // Sort by estimated size (smallest first for faster intersection)
    std::sort(term_infos.begin(), term_infos.end(),
              [](const SearchTermInfo& a, const SearchTermInfo& b) { return a.estimated_size < b.estimated_size; });

    // Execute search pipeline
    auto pipeline_result = search_pipeline::Execute(
        *query, term_infos, all_search_terms, current_index, current_doc_store, full_config_, current_ngram_size,
        current_kanji_ngram_size, current_cross_boundary, SearchHandler::GetFilterThreshold());

    // Build JSON response - just return count
    json response;
    response["count"] = pipeline_result.results.size();

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
                                                                     table_contexts_, binlog_reader_, cache_manager_);
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
