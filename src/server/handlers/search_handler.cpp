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

std::string SearchHandler::HandleSearch(const query::Query& query, ConnectionContext& conn_ctx) {
  // Check if server is loading
  if (ctx_.dump_load_in_progress) {
    return ResponseFormatter::FormatError("Server is loading, please try again later");
  }

  // Try cache lookup first
  auto cache_lookup_start = std::chrono::high_resolution_clock::now();
  if (ctx_.cache_manager != nullptr && ctx_.cache_manager->IsEnabled()) {
    auto cached_lookup = ctx_.cache_manager->LookupWithMetadata(query);
    if (cached_lookup.has_value()) {
      // Cache hit! Return cached result
      storage::DocumentStore* current_doc_store = nullptr;
      index::Index* dummy_index = nullptr;
      int dummy_ngram = 0;
      int dummy_kanji_ngram = 0;
      std::string error =
          GetTableContext(query.table, &dummy_index, &current_doc_store, &dummy_ngram, &dummy_kanji_ngram);
      if (!error.empty() || current_doc_store == nullptr) {
        // Table context not available, fall through to normal execution
      } else {
        // TOCTOU mitigation: Validate a sample of cached DocIds
        auto full_results = cached_lookup.value().results;

        if (search_pipeline::IsCacheStale(full_results, current_doc_store)) {
          // Cache contains stale DocIds - fall through to normal execution
        } else {
          auto cache_lookup_end = std::chrono::high_resolution_clock::now();
          double cache_lookup_time_ms =
              std::chrono::duration<double, std::milli>(cache_lookup_end - cache_lookup_start).count();

          // Apply pagination to cached results
          size_t total_results = full_results.size();

          // Get primary key column name from table config
          std::string primary_key_column = "id";  // default
          auto table_it = ctx_.table_contexts.find(query.table);
          if (table_it != ctx_.table_contexts.end()) {
            primary_key_column = table_it->second->config.primary_key;
          }

          auto paginated_result =
              query::ResultSorter::SortAndPaginate(full_results, *current_doc_store, query, primary_key_column);

          if (!paginated_result.has_value()) {
            return paginated_result.error();
          }

          auto paginated_results = std::move(paginated_result.value());

          if (conn_ctx.debug_mode) {
            query::DebugInfo debug_info;
            debug_info.query_time_ms = cache_lookup_time_ms;
            debug_info.final_results = paginated_results.size();

            // Cache hit debug info with actual metadata
            auto now = std::chrono::steady_clock::now();
            debug_info.cache_info.status = query::CacheDebugInfo::Status::HIT;
            debug_info.cache_info.cache_age_ms =
                std::chrono::duration<double, std::milli>(now - cached_lookup.value().created_at).count();
            debug_info.cache_info.cache_saved_ms = cached_lookup.value().query_cost_ms;

            return ResponseFormatter::FormatSearchResponse(paginated_results, total_results, current_doc_store,
                                                           &debug_info);
          }

          return ResponseFormatter::FormatSearchResponse(paginated_results, total_results, current_doc_store);
        }  // else (cache not stale)
      }  // else (table context available)
    }  // if (cached_lookup.has_value())
  }  // if (cache enabled)

  // Get table context
  index::Index* current_index = nullptr;
  storage::DocumentStore* current_doc_store = nullptr;
  int current_ngram_size = 0;
  int current_kanji_ngram_size = 0;

  std::string error =
      GetTableContext(query.table, &current_index, &current_doc_store, &current_ngram_size, &current_kanji_ngram_size);
  if (!error.empty()) {
    return error;
  }

  // Verify index is available
  if (current_index == nullptr) {
    return ResponseFormatter::FormatError("Index not available");
  }

  // Start timing
  auto start_time = std::chrono::high_resolution_clock::now();
  auto index_start = std::chrono::high_resolution_clock::now();
  query::DebugInfo debug_info;

  // Collect all search terms (main + AND terms)
  std::vector<std::string> all_search_terms;
  all_search_terms.push_back(query.search_text);
  all_search_terms.insert(all_search_terms.end(), query.and_terms.begin(), query.and_terms.end());

  // Collect debug info for search terms
  if (conn_ctx.debug_mode) {
    debug_info.search_terms = all_search_terms;
  }

  // Generate n-grams for each term and estimate result sizes
  bool current_cross_boundary = current_index->GetCrossBoundaryNgrams();
  auto term_infos = search_pipeline::GenerateTermInfos(all_search_terms, current_index, current_ngram_size,
                                                       current_kanji_ngram_size, current_cross_boundary);

  // Collect debug info for n-grams and posting list sizes
  if (conn_ctx.debug_mode) {
    for (const auto& ti : term_infos) {
      for (const auto& ngram : ti.ngrams) {
        debug_info.ngrams_used.push_back(ngram);
      }
      debug_info.posting_list_sizes.push_back(ti.estimated_size);
    }
  }

  // Sort terms by estimated size (smallest first for faster intersection)
  std::sort(term_infos.begin(), term_infos.end(), [](const SearchTermInfo& lhs, const SearchTermInfo& rhs) {
    return lhs.estimated_size < rhs.estimated_size;
  });

  // If any term has zero results, return empty immediately
  if (!term_infos.empty() && term_infos[0].estimated_size == 0) {
    if (conn_ctx.debug_mode) {
      debug_info.optimization_used = "early-exit (empty posting list)";
      debug_info.final_results = 0;
      auto end_time = std::chrono::high_resolution_clock::now();
      debug_info.query_time_ms = std::chrono::duration<double, std::milli>(end_time - start_time).count();
      debug_info.index_time_ms = std::chrono::duration<double, std::milli>(end_time - index_start).count();
      return ResponseFormatter::FormatSearchResponse({}, 0, current_doc_store, &debug_info);
    }
    return ResponseFormatter::FormatSearchResponse({}, 0, current_doc_store);
  }

  // Get primary key column name from table config (needed for optimization check and sorting)
  std::string primary_key_column = "id";  // default
  auto table_it = ctx_.table_contexts.find(query.table);
  if (table_it != ctx_.table_contexts.end()) {
    primary_key_column = table_it->second->config.primary_key;
  }

  // Determine ORDER BY clause (default: primary key DESC)
  query::OrderByClause order_by;
  bool order_by_implicit = false;
  if (query.order_by.has_value()) {
    order_by = query.order_by.value();
  } else {
    // Default: primary key DESC
    order_by.column = "";  // Empty = primary key
    order_by.order = query::SortOrder::DESC;
    order_by_implicit = true;
  }

  // Check if ordering by primary key (either empty column or explicit primary key column name)
  // Case-insensitive: MySQL column names are case-insensitive
  auto equals_ignore_case = [](const std::string& lhs, const std::string& rhs) {
    return lhs.size() == rhs.size() &&
           std::equal(lhs.begin(), lhs.end(), rhs.begin(),
                      [](unsigned char a, unsigned char b) { return std::tolower(a) == std::tolower(b); });
  };
  bool is_primary_key_order = order_by.IsPrimaryKey() || equals_ignore_case(order_by.column, primary_key_column);

  // Record applied ORDER BY for debug
  if (conn_ctx.debug_mode) {
    std::string order_str = order_by.column.empty() ? primary_key_column : order_by.column;
    order_str += (order_by.order == query::SortOrder::ASC) ? " ASC" : " DESC";
    if (order_by_implicit) {
      order_str += " (default)";
    }
    debug_info.order_by_applied = order_str;
    debug_info.limit_applied = query.limit;
    debug_info.offset_applied = query.offset;
    debug_info.limit_explicit = query.limit_explicit;
    debug_info.offset_explicit = query.offset_explicit;
  }

  // Check optimization conditions
  // NOLINTBEGIN(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
  constexpr uint32_t kMaxOffsetForOptimization = 10000;
  // NOLINTEND(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
  bool can_optimize = term_infos.size() == 1 && query.not_terms.empty() && query.filters.empty() && query.limit > 0 &&
                      query.offset <= kMaxOffsetForOptimization && is_primary_key_order;

  // Calculate total results count
  size_t total_results = 0;
  std::vector<storage::DocId> results;

  if (can_optimize) {
    // Fetch all matching results once for accurate total_results
    auto all_results = current_index->SearchAnd(term_infos[0].ngrams);
    total_results = all_results.size();

    // Check for empty results before division to avoid division by zero
    if (total_results == 0) {
      // NOLINTNEXTLINE(clang-analyzer-deadcode.DeadStores) - Documents that optimization is skipped
      can_optimize = false;
      results = std::move(all_results);  // Empty vector
      if (conn_ctx.debug_mode) {
        debug_info.total_candidates = 0;
        debug_info.after_intersection = 0;
        debug_info.optimization_used = "no results (optimization skipped)";
      }
    } else {
      // Heuristic: reuse fetched results if offset+limit is close to total_results
      // NOLINTBEGIN(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
      constexpr double kReuseThreshold = 0.5;  // Reuse if fetching >50% of results
      // NOLINTEND(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
      size_t index_limit = query.offset + query.limit;
      bool should_reuse = (static_cast<double>(index_limit) / static_cast<double>(total_results)) > kReuseThreshold;

      if (should_reuse) {
        // Reuse the already-fetched results
        results = std::move(all_results);
        // NOLINTNEXTLINE(clang-analyzer-deadcode.DeadStores) - Documents use of standard sort+paginate path
        can_optimize = false;
        if (conn_ctx.debug_mode) {
          debug_info.total_candidates = results.size();
          debug_info.after_intersection = results.size();
          debug_info.optimization_used = "reuse-fetch (small result set)";
        }
      } else {
        // Result set is large: use GetTopN optimization
        bool reverse = (order_by.order == query::SortOrder::DESC);
        results = current_index->SearchAnd(term_infos[0].ngrams, index_limit, reverse);
        if (conn_ctx.debug_mode) {
          debug_info.total_candidates = results.size();
          debug_info.after_intersection = results.size();
          std::string direction = reverse ? "DESC" : "ASC";
          if (term_infos[0].ngrams.size() == 1) {
            debug_info.optimization_used = "Index GetTopN (single-ngram + " + direction + " + limit)";
          } else {
            debug_info.optimization_used = "Index GetTopN (streaming intersection + " + direction + " + limit)";
          }
        }
      }
    }
  } else {
    // Standard path: retrieve all results
    results = current_index->SearchAnd(term_infos[0].ngrams);
    if (conn_ctx.debug_mode) {
      debug_info.total_candidates = results.size();
      debug_info.after_intersection = results.size();
      debug_info.optimization_used = "size-based term ordering";
    }
  }

  // Intersect with remaining terms
  for (size_t i = 1; i < term_infos.size() && !results.empty(); ++i) {
    // Use filter approach when candidate set is small enough
    if (results.size() <= filter_threshold_) {
      // Filter candidates by checking each one against posting lists
      results = current_index->FilterByNgrams(results, term_infos[i].ngrams);
    } else {
      // Full intersection for large result sets
      auto and_results = current_index->SearchAnd(term_infos[i].ngrams);
      std::vector<storage::DocId> intersection;
      intersection.reserve(std::min(results.size(), and_results.size()));
      std::set_intersection(results.begin(), results.end(), and_results.begin(), and_results.end(),
                            std::back_inserter(intersection));
      results = std::move(intersection);
    }
  }

  // Apply NOT filter if present
  if (!query.not_terms.empty()) {
    results = search_pipeline::ApplyNotFilter(results, query.not_terms, current_index, current_ngram_size,
                                              current_kanji_ngram_size, current_cross_boundary);
    if (conn_ctx.debug_mode) {
      debug_info.after_not = results.size();
    }
  } else if (conn_ctx.debug_mode) {
    debug_info.after_not = results.size();
  }

  // Apply filter conditions (use bitmap fast path when possible)
  auto filter_start = std::chrono::high_resolution_clock::now();
  if (!query.filters.empty()) {
    results = search_pipeline::ApplyFiltersWithBitmap(results, query.filters, current_doc_store);
    if (conn_ctx.debug_mode) {
      auto filter_end = std::chrono::high_resolution_clock::now();
      debug_info.filter_time_ms = std::chrono::duration<double, std::milli>(filter_end - filter_start).count();
      debug_info.after_filters = results.size();
    }
  } else if (conn_ctx.debug_mode) {
    debug_info.after_filters = results.size();
  }

  // N-gram post-filter verification: eliminate false positives by checking
  // that each candidate's stored normalized text contains all search terms
  results = search_pipeline::ApplyVerifyTextFilter(results, all_search_terms, current_index, current_doc_store,
                                                   ctx_.full_config);

  // Sort and paginate results
  // Always update total_results to reflect the actual count after all filters
  total_results = results.size();

  auto sorted_result = query::ResultSorter::SortAndPaginate(results, *current_doc_store, query, primary_key_column);

  if (!sorted_result.has_value()) {
    return sorted_result.error();
  }

  auto sorted_results = std::move(sorted_result.value());

  // Calculate query execution time
  auto end_time = std::chrono::high_resolution_clock::now();
  double query_time_ms = std::chrono::duration<double, std::milli>(end_time - start_time).count();

  // Store in cache if enabled
  search_pipeline::InsertToCache(ctx_.cache_manager, query, results, term_infos, query_time_ms, current_ngram_size,
                                 current_kanji_ngram_size, current_cross_boundary);

  // Calculate final debug info
  if (conn_ctx.debug_mode) {
    auto index_end = std::chrono::high_resolution_clock::now();
    debug_info.query_time_ms = query_time_ms;
    debug_info.index_time_ms = std::chrono::duration<double, std::milli>(index_end - index_start).count();
    debug_info.final_results = sorted_results.size();

    // Cache debug info
    if (ctx_.cache_manager != nullptr && ctx_.cache_manager->IsEnabled()) {
      debug_info.cache_info.status = query::CacheDebugInfo::Status::MISS_NOT_FOUND;
      debug_info.cache_info.query_cost_ms = query_time_ms;
    } else {
      debug_info.cache_info.status = query::CacheDebugInfo::Status::MISS_DISABLED;
    }

    return ResponseFormatter::FormatSearchResponse(sorted_results, total_results, current_doc_store, &debug_info);
  }

  return ResponseFormatter::FormatSearchResponse(sorted_results, total_results, current_doc_store);
}

std::string SearchHandler::HandleCount(const query::Query& query, ConnectionContext& conn_ctx) {
  // Check if server is loading
  if (ctx_.dump_load_in_progress) {
    return ResponseFormatter::FormatError("Server is loading, please try again later");
  }

  // Try cache lookup first
  auto cache_lookup_start = std::chrono::high_resolution_clock::now();
  if (ctx_.cache_manager != nullptr && ctx_.cache_manager->IsEnabled()) {
    auto cached_lookup = ctx_.cache_manager->LookupWithMetadata(query);
    if (cached_lookup.has_value()) {
      // Cache hit - validate before returning count
      storage::DocumentStore* current_doc_store = nullptr;
      index::Index* dummy_index = nullptr;
      int dummy_ngram = 0;
      int dummy_kanji_ngram = 0;
      std::string error =
          GetTableContext(query.table, &dummy_index, &current_doc_store, &dummy_ngram, &dummy_kanji_ngram);
      if (!error.empty() || current_doc_store == nullptr) {
        // Table context not available, fall through to normal execution
      } else {
        // TOCTOU mitigation: Validate a sample of cached DocIds
        auto full_results = cached_lookup.value().results;

        if (search_pipeline::IsCacheStale(full_results, current_doc_store)) {
          // Cache contains stale DocIds - fall through to normal execution
        } else {
          auto cache_lookup_end = std::chrono::high_resolution_clock::now();
          double cache_lookup_time_ms =
              std::chrono::duration<double, std::milli>(cache_lookup_end - cache_lookup_start).count();

          if (conn_ctx.debug_mode) {
            query::DebugInfo debug_info;
            debug_info.query_time_ms = cache_lookup_time_ms;
            debug_info.final_results = full_results.size();

            auto now = std::chrono::steady_clock::now();
            debug_info.cache_info.status = query::CacheDebugInfo::Status::HIT;
            debug_info.cache_info.cache_age_ms =
                std::chrono::duration<double, std::milli>(now - cached_lookup.value().created_at).count();
            debug_info.cache_info.cache_saved_ms = cached_lookup.value().query_cost_ms;

            return ResponseFormatter::FormatCountResponse(full_results.size(), &debug_info);
          }

          return ResponseFormatter::FormatCountResponse(full_results.size());
        }
      }
    }
  }

  // Get table context
  index::Index* current_index = nullptr;
  storage::DocumentStore* current_doc_store = nullptr;
  int current_ngram_size = 0;
  int current_kanji_ngram_size = 0;

  std::string error =
      GetTableContext(query.table, &current_index, &current_doc_store, &current_ngram_size, &current_kanji_ngram_size);
  if (!error.empty()) {
    return error;
  }

  // Verify index is available
  if (current_index == nullptr) {
    return ResponseFormatter::FormatError("Index not available");
  }

  // Start timing
  auto start_time = std::chrono::high_resolution_clock::now();
  auto index_start = std::chrono::high_resolution_clock::now();
  query::DebugInfo debug_info;

  // Collect all search terms (main + AND terms)
  std::vector<std::string> all_search_terms;
  all_search_terms.push_back(query.search_text);
  all_search_terms.insert(all_search_terms.end(), query.and_terms.begin(), query.and_terms.end());

  // Collect debug info for search terms
  if (conn_ctx.debug_mode) {
    debug_info.search_terms = all_search_terms;
  }

  // Generate n-grams for each term and estimate result sizes
  bool current_cross_boundary = current_index->GetCrossBoundaryNgrams();
  auto term_infos = search_pipeline::GenerateTermInfos(all_search_terms, current_index, current_ngram_size,
                                                       current_kanji_ngram_size, current_cross_boundary);

  // Collect debug info for n-grams and posting list sizes
  if (conn_ctx.debug_mode) {
    for (const auto& ti : term_infos) {
      for (const auto& ngram : ti.ngrams) {
        debug_info.ngrams_used.push_back(ngram);
      }
      debug_info.posting_list_sizes.push_back(ti.estimated_size);
    }
  }

  // Sort terms by estimated size (smallest first for faster intersection)
  std::sort(term_infos.begin(), term_infos.end(), [](const SearchTermInfo& lhs, const SearchTermInfo& rhs) {
    return lhs.estimated_size < rhs.estimated_size;
  });

  // If any term has zero results, return 0 immediately
  if (!term_infos.empty() && term_infos[0].estimated_size == 0) {
    if (conn_ctx.debug_mode) {
      auto end_time = std::chrono::high_resolution_clock::now();
      debug_info.query_time_ms = std::chrono::duration<double, std::milli>(end_time - start_time).count();
      debug_info.index_time_ms = std::chrono::duration<double, std::milli>(end_time - index_start).count();
      return ResponseFormatter::FormatCountResponse(0, &debug_info);
    }
    return ResponseFormatter::FormatCountResponse(0);
  }

  // Process most selective term first
  auto results = current_index->SearchAnd(term_infos[0].ngrams);

  // Intersect with remaining terms
  for (size_t i = 1; i < term_infos.size() && !results.empty(); ++i) {
    // Use filter approach when candidate set is small enough
    if (results.size() <= filter_threshold_) {
      results = current_index->FilterByNgrams(results, term_infos[i].ngrams);
    } else {
      auto and_results = current_index->SearchAnd(term_infos[i].ngrams);
      std::vector<storage::DocId> intersection;
      intersection.reserve(std::min(results.size(), and_results.size()));
      std::set_intersection(results.begin(), results.end(), and_results.begin(), and_results.end(),
                            std::back_inserter(intersection));
      results = std::move(intersection);
    }
  }

  // Apply NOT filter if present
  if (!query.not_terms.empty()) {
    results = search_pipeline::ApplyNotFilter(results, query.not_terms, current_index, current_ngram_size,
                                              current_kanji_ngram_size, current_cross_boundary);
  }

  // Apply filter conditions (use bitmap fast path when possible)
  if (!query.filters.empty()) {
    results = search_pipeline::ApplyFiltersWithBitmap(results, query.filters, current_doc_store);
  }

  // N-gram post-filter verification
  results = search_pipeline::ApplyVerifyTextFilter(results, all_search_terms, current_index, current_doc_store,
                                                   ctx_.full_config);

  // Calculate query execution time
  auto end_time = std::chrono::high_resolution_clock::now();
  double query_time_ms = std::chrono::duration<double, std::milli>(end_time - start_time).count();

  // Store in cache if enabled
  search_pipeline::InsertToCache(ctx_.cache_manager, query, results, term_infos, query_time_ms, current_ngram_size,
                                 current_kanji_ngram_size, current_cross_boundary);

  // Calculate final debug info
  if (conn_ctx.debug_mode) {
    debug_info.query_time_ms = query_time_ms;
    debug_info.index_time_ms = std::chrono::duration<double, std::milli>(end_time - index_start).count();

    // Cache debug info
    if (ctx_.cache_manager != nullptr && ctx_.cache_manager->IsEnabled()) {
      debug_info.cache_info.status = query::CacheDebugInfo::Status::MISS_NOT_FOUND;
      debug_info.cache_info.query_cost_ms = query_time_ms;
    } else {
      debug_info.cache_info.status = query::CacheDebugInfo::Status::MISS_DISABLED;
    }

    return ResponseFormatter::FormatCountResponse(results.size(), &debug_info);
  }

  return ResponseFormatter::FormatCountResponse(results.size());
}

}  // namespace mygramdb::server
