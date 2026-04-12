/**
 * @file search_handler.cpp
 * @brief Handler for SEARCH and COUNT commands
 */

#include "server/handlers/search_handler.h"

#include <algorithm>
#include <chrono>

#include "cache/cache_manager.h"
#include "query/result_sorter.h"
#include "server/search_pipeline.h"

namespace mygramdb::server {

std::string SearchHandler::Handle(const query::Query& query, ConnectionContext& conn_ctx) {
  if (query.type == query::QueryType::SEARCH) {
    return HandleSearch(query, conn_ctx);
  }
  if (query.type == query::QueryType::COUNT) {
    return HandleCount(query, conn_ctx);
  }
  return ResponseFormatter::FormatError("Invalid query type for SearchHandler");
}

std::string SearchHandler::ExecuteSearchPipeline(const query::Query& query, ConnectionContext& conn_ctx,
                                                 PipelineOutput& output) {
  // Check if server is loading
  if (ctx_.dump_load_in_progress) {
    return ResponseFormatter::FormatError("Server is loading, please try again later");
  }

  // Try cache lookup first
  auto cache_lookup_start = std::chrono::high_resolution_clock::now();
  if (ctx_.cache_manager != nullptr && ctx_.cache_manager->IsEnabled()) {
    auto cached_lookup = ctx_.cache_manager->LookupWithMetadata(query);
    if (cached_lookup.has_value()) {
      storage::DocumentStore* cache_doc_store = nullptr;
      index::Index* dummy_index = nullptr;
      int dummy_ngram = 0;
      int dummy_kanji_ngram = 0;
      std::string error =
          GetTableContext(query.table, &dummy_index, &cache_doc_store, &dummy_ngram, &dummy_kanji_ngram);
      if (error.empty() && cache_doc_store != nullptr) {
        auto full_results = cached_lookup.value().results;

        if (!search_pipeline::IsCacheStale(full_results, cache_doc_store)) {
          auto cache_lookup_end = std::chrono::high_resolution_clock::now();
          double cache_lookup_time_ms =
              std::chrono::duration<double, std::milli>(cache_lookup_end - cache_lookup_start).count();

          output.results = std::move(full_results);
          output.current_doc_store = cache_doc_store;
          output.query_time_ms = cache_lookup_time_ms;

          if (conn_ctx.debug_mode) {
            output.debug_info.query_time_ms = cache_lookup_time_ms;
            output.debug_info.final_results = output.results.size();

            auto now = std::chrono::steady_clock::now();
            output.debug_info.cache_info.status = query::CacheDebugInfo::Status::HIT;
            output.debug_info.cache_info.cache_age_ms =
                std::chrono::duration<double, std::milli>(now - cached_lookup.value().created_at).count();
            output.debug_info.cache_info.cache_saved_ms = cached_lookup.value().query_cost_ms;
          }

          // Empty string = success (cache hit)
          return "";
        }
      }
      // Cache stale or table context unavailable - fall through to normal execution
    }
  }

  // Get table context
  std::string error = GetTableContext(query.table, &output.current_index, &output.current_doc_store,
                                      &output.current_ngram_size, &output.current_kanji_ngram_size);
  if (!error.empty()) {
    return error;
  }

  // Verify index is available
  if (output.current_index == nullptr) {
    return ResponseFormatter::FormatError("Index not available");
  }

  // Start timing
  auto start_time = std::chrono::high_resolution_clock::now();
  auto index_start = std::chrono::high_resolution_clock::now();

  // Collect all search terms (main + AND terms)
  output.all_search_terms.push_back(query.search_text);
  output.all_search_terms.insert(output.all_search_terms.end(), query.and_terms.begin(), query.and_terms.end());

  // Collect debug info for search terms
  if (conn_ctx.debug_mode) {
    output.debug_info.search_terms = output.all_search_terms;
  }

  // Generate n-grams for each term and estimate result sizes
  output.current_cross_boundary = output.current_index->GetCrossBoundaryNgrams();
  output.term_infos =
      search_pipeline::GenerateTermInfos(output.all_search_terms, output.current_index, output.current_ngram_size,
                                         output.current_kanji_ngram_size, output.current_cross_boundary);

  // Collect debug info for n-grams and posting list sizes
  if (conn_ctx.debug_mode) {
    for (const auto& ti : output.term_infos) {
      for (const auto& ngram : ti.ngrams) {
        output.debug_info.ngrams_used.push_back(ngram);
      }
      output.debug_info.posting_list_sizes.push_back(ti.estimated_size);
    }
  }

  // Sort terms by estimated size (smallest first for faster intersection)
  std::sort(
      output.term_infos.begin(), output.term_infos.end(),
      [](const SearchTermInfo& lhs, const SearchTermInfo& rhs) { return lhs.estimated_size < rhs.estimated_size; });

  // Execute the core search pipeline (intersection, NOT filter, filters, verify_text)
  auto pipeline_result =
      search_pipeline::Execute(query, output.term_infos, output.all_search_terms, output.current_index,
                               output.current_doc_store, ctx_.full_config, output.current_ngram_size,
                               output.current_kanji_ngram_size, output.current_cross_boundary, filter_threshold_);

  if (pipeline_result.empty_term_detected) {
    output.results.clear();
    auto end_time = std::chrono::high_resolution_clock::now();
    output.query_time_ms = std::chrono::duration<double, std::milli>(end_time - start_time).count();
    if (conn_ctx.debug_mode) {
      output.debug_info.optimization_used = "early-exit (empty posting list)";
      output.debug_info.final_results = 0;
      output.debug_info.query_time_ms = output.query_time_ms;
      output.debug_info.index_time_ms = std::chrono::duration<double, std::milli>(end_time - index_start).count();
    }
    return "";  // Success with empty results
  }

  output.results = std::move(pipeline_result.results);

  if (conn_ctx.debug_mode) {
    output.debug_info.total_candidates = output.results.size();
    output.debug_info.after_intersection = output.results.size();
    output.debug_info.optimization_used = "size-based term ordering";
    output.debug_info.after_not = output.results.size();
    output.debug_info.after_filters = output.results.size();
  }

  // Calculate query execution time
  auto end_time = std::chrono::high_resolution_clock::now();
  output.query_time_ms = std::chrono::duration<double, std::milli>(end_time - start_time).count();

  // Store in cache if enabled
  search_pipeline::InsertToCache(ctx_.cache_manager, query, output.results, output.term_infos, output.query_time_ms,
                                 output.current_ngram_size, output.current_kanji_ngram_size,
                                 output.current_cross_boundary);

  // Populate debug info timing and cache status
  if (conn_ctx.debug_mode) {
    output.debug_info.query_time_ms = output.query_time_ms;
    output.debug_info.index_time_ms = std::chrono::duration<double, std::milli>(end_time - index_start).count();

    if (ctx_.cache_manager != nullptr && ctx_.cache_manager->IsEnabled()) {
      output.debug_info.cache_info.status = query::CacheDebugInfo::Status::MISS_NOT_FOUND;
      output.debug_info.cache_info.query_cost_ms = output.query_time_ms;
    } else {
      output.debug_info.cache_info.status = query::CacheDebugInfo::Status::MISS_DISABLED;
    }
  }

  return "";  // Success
}

std::string SearchHandler::HandleSearch(const query::Query& query, ConnectionContext& conn_ctx) {
  PipelineOutput output;
  std::string pipeline_error = ExecuteSearchPipeline(query, conn_ctx, output);
  if (!pipeline_error.empty()) {
    return pipeline_error;
  }

  // Check if this was an early-exit (empty posting list) or cache hit without index context
  if (output.current_doc_store == nullptr) {
    return ResponseFormatter::FormatError("Document store not available");
  }

  // Check for cache hit path - results are already final, just need pagination
  bool is_cache_hit = (output.debug_info.cache_info.status == query::CacheDebugInfo::Status::HIT);

  size_t total_results = output.results.size();

  // Get primary key column name from table config
  std::string primary_key_column = "id";  // default
  auto table_it = ctx_.table_contexts.find(query.table);
  if (table_it != ctx_.table_contexts.end()) {
    primary_key_column = table_it->second->config.primary_key;
  }

  // For cache hits, skip the SEARCH-specific optimization path
  if (!is_cache_hit && output.current_index != nullptr && !output.term_infos.empty() &&
      output.term_infos[0].estimated_size > 0) {
    // Determine ORDER BY clause (default: primary key DESC)
    query::OrderByClause order_by;
    bool order_by_implicit = false;
    if (query.order_by.has_value()) {
      order_by = query.order_by.value();
    } else {
      order_by.column = "";
      order_by.order = query::SortOrder::DESC;
      order_by_implicit = true;
    }

    auto equals_ignore_case = [](const std::string& lhs, const std::string& rhs) {
      return lhs.size() == rhs.size() &&
             std::equal(lhs.begin(), lhs.end(), rhs.begin(),
                        [](unsigned char a, unsigned char b) { return std::tolower(a) == std::tolower(b); });
    };
    bool is_primary_key_order = order_by.IsPrimaryKey() || equals_ignore_case(order_by.column, primary_key_column);

    if (conn_ctx.debug_mode) {
      std::string order_str = order_by.column.empty() ? primary_key_column : order_by.column;
      order_str += (order_by.order == query::SortOrder::ASC) ? " ASC" : " DESC";
      if (order_by_implicit) {
        order_str += " (default)";
      }
      output.debug_info.order_by_applied = order_str;
      output.debug_info.limit_applied = query.limit;
      output.debug_info.offset_applied = query.offset;
      output.debug_info.limit_explicit = query.limit_explicit;
      output.debug_info.offset_explicit = query.offset_explicit;
    }

    // Check optimization conditions for single-term queries
    // NOLINTBEGIN(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
    constexpr uint32_t kMaxOffsetForOptimization = 10000;
    // NOLINTEND(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
    bool can_optimize = output.term_infos.size() == 1 && query.not_terms.empty() && query.filters.empty() &&
                        query.limit > 0 && query.offset <= kMaxOffsetForOptimization && is_primary_key_order;

    if (can_optimize) {
      // The pipeline already fetched all results; we can apply GetTopN optimization
      total_results = output.results.size();

      if (total_results > 0) {
        // NOLINTBEGIN(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
        constexpr double kReuseThreshold = 0.5;
        // NOLINTEND(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
        size_t index_limit = query.offset + query.limit;
        bool should_reuse = (static_cast<double>(index_limit) / static_cast<double>(total_results)) > kReuseThreshold;

        if (!should_reuse) {
          // Result set is large: use GetTopN optimization
          bool reverse = (order_by.order == query::SortOrder::DESC);
          output.results = output.current_index->SearchAnd(output.term_infos[0].ngrams, index_limit, reverse);
          if (conn_ctx.debug_mode) {
            output.debug_info.total_candidates = output.results.size();
            output.debug_info.after_intersection = output.results.size();
            std::string direction = reverse ? "DESC" : "ASC";
            if (output.term_infos[0].ngrams.size() == 1) {
              output.debug_info.optimization_used = "Index GetTopN (single-ngram + " + direction + " + limit)";
            } else {
              output.debug_info.optimization_used =
                  "Index GetTopN (streaming intersection + " + direction + " + limit)";
            }
          }
        } else if (conn_ctx.debug_mode) {
          output.debug_info.optimization_used = "reuse-fetch (small result set)";
        }
      } else if (conn_ctx.debug_mode) {
        output.debug_info.optimization_used = "no results (optimization skipped)";
      }
    }
  }

  // Sort and paginate results
  auto sorted_result =
      query::ResultSorter::SortAndPaginate(output.results, *output.current_doc_store, query, primary_key_column);

  if (!sorted_result.has_value()) {
    return sorted_result.error();
  }

  auto sorted_results = std::move(sorted_result.value());

  if (conn_ctx.debug_mode) {
    output.debug_info.final_results = sorted_results.size();
    return ResponseFormatter::FormatSearchResponse(sorted_results, total_results, output.current_doc_store,
                                                   &output.debug_info);
  }

  return ResponseFormatter::FormatSearchResponse(sorted_results, total_results, output.current_doc_store);
}

std::string SearchHandler::HandleCount(const query::Query& query, ConnectionContext& conn_ctx) {
  PipelineOutput output;
  std::string pipeline_error = ExecuteSearchPipeline(query, conn_ctx, output);
  if (!pipeline_error.empty()) {
    return pipeline_error;
  }

  if (conn_ctx.debug_mode) {
    output.debug_info.final_results = output.results.size();
    return ResponseFormatter::FormatCountResponse(output.results.size(), &output.debug_info);
  }

  return ResponseFormatter::FormatCountResponse(output.results.size());
}

}  // namespace mygramdb::server
