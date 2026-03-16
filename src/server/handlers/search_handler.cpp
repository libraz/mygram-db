/**
 * @file search_handler.cpp
 * @brief Handler for SEARCH and COUNT commands
 */

#include "server/handlers/search_handler.h"

#include <roaring/roaring.h>

#include <algorithm>
#include <charconv>
#include <chrono>
#include <cmath>
#include <limits>

#include "cache/cache_manager.h"
#include "query/result_sorter.h"
#include "storage/filter_index.h"
#include "utils/string_utils.h"

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
        // If any are stale (document deleted since cache population), invalidate and fall through.
        // Design note: Even after validation, documents may be deleted before response formatting.
        // This is acceptable (eventual consistency): FormatSearchResponse skips missing docs,
        // and ResultSorter::SortAndPaginate handles missing docs gracefully via fallback sort keys.
        auto full_results = cached_lookup.value().results;

        // Validate sample of cached results with adaptive sample size
        // Small result sets: check all; large sets: check at least 10% or 10 entries
        bool cache_stale = false;
        if (!full_results.empty()) {
          size_t sample_size = std::min(full_results.size(), std::max(size_t{10}, full_results.size() / 10));
          size_t step = std::max(size_t{1}, full_results.size() / sample_size);
          for (size_t i = 0; i < full_results.size() && i / step < sample_size; i += step) {
            if (!current_doc_store->GetPrimaryKey(full_results[i]).has_value()) {
              cache_stale = true;
              break;
            }
          }
        }

        if (cache_stale) {
          // Cache contains stale DocIds (documents deleted since cache population)
          // Fall through to normal execution - cache entry will be replaced on next insert
        } else {
          auto cache_lookup_end = std::chrono::high_resolution_clock::now();
          double cache_lookup_time_ms =
              std::chrono::duration<double, std::milli>(cache_lookup_end - cache_lookup_start).count();

          // Apply pagination to cached results
          // Cache stores full results (before pagination) to allow different OFFSET/LIMIT on same query
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
  auto term_infos = GenerateTermInfos(all_search_terms, current_index, current_ngram_size, current_kanji_ngram_size,
                                      conn_ctx.debug_mode ? &debug_info : nullptr, current_cross_boundary);

  // Sort terms by estimated size (smallest first for faster intersection)
  std::sort(term_infos.begin(), term_infos.end(),
            [](const TermInfo& lhs, const TermInfo& rhs) { return lhs.estimated_size < rhs.estimated_size; });

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
    results = ApplyNotFilter(results, query.not_terms, current_index, current_ngram_size, current_kanji_ngram_size,
                             current_cross_boundary);
    if (conn_ctx.debug_mode) {
      debug_info.after_not = results.size();
    }
  } else if (conn_ctx.debug_mode) {
    debug_info.after_not = results.size();
  }

  // Apply filter conditions (use bitmap fast path when possible)
  auto filter_start = std::chrono::high_resolution_clock::now();
  if (!query.filters.empty()) {
    results = ApplyFiltersWithBitmap(results, query.filters, current_doc_store);
    if (conn_ctx.debug_mode) {
      auto filter_end = std::chrono::high_resolution_clock::now();
      debug_info.filter_time_ms = std::chrono::duration<double, std::milli>(filter_end - filter_start).count();
      debug_info.after_filters = results.size();
    }
  } else if (conn_ctx.debug_mode) {
    debug_info.after_filters = results.size();
  }

  // Sort and paginate results
  // Always update total_results to reflect the actual count after all filters
  // (NOT filters and filter conditions) have been applied
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
  if (ctx_.cache_manager != nullptr && ctx_.cache_manager->IsEnabled()) {
    // Collect all ngrams from term_infos
    std::set<std::string> all_ngrams;
    for (const auto& term_info : term_infos) {
      all_ngrams.insert(term_info.ngrams.begin(), term_info.ngrams.end());
    }

    // Insert full result (before pagination) into cache
    ctx_.cache_manager->Insert(query, results, all_ngrams, query_time_ms, current_ngram_size,
                               current_kanji_ngram_size, current_cross_boundary);
  }

  // Calculate final debug info
  if (conn_ctx.debug_mode) {
    auto index_end = std::chrono::high_resolution_clock::now();
    debug_info.query_time_ms = query_time_ms;
    debug_info.index_time_ms = std::chrono::duration<double, std::milli>(index_end - index_start).count();
    debug_info.final_results = sorted_results.size();

    // Cache debug info
    if (ctx_.cache_manager != nullptr && ctx_.cache_manager->IsEnabled()) {
      // Cache was enabled but missed (either not found or invalidated)
      debug_info.cache_info.status = query::CacheDebugInfo::Status::MISS_NOT_FOUND;
      debug_info.cache_info.query_cost_ms = query_time_ms;
    } else {
      // Cache disabled
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

        bool cache_stale = false;
        if (!full_results.empty()) {
          size_t sample_size = std::min(full_results.size(), std::max(size_t{10}, full_results.size() / 10));
          size_t step = std::max(size_t{1}, full_results.size() / sample_size);
          for (size_t i = 0; i < full_results.size() && i / step < sample_size; i += step) {
            if (!current_doc_store->GetPrimaryKey(full_results[i]).has_value()) {
              cache_stale = true;
              break;
            }
          }
        }

        if (cache_stale) {
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
  auto term_infos = GenerateTermInfos(all_search_terms, current_index, current_ngram_size, current_kanji_ngram_size,
                                      conn_ctx.debug_mode ? &debug_info : nullptr, current_cross_boundary);

  // Sort terms by estimated size (smallest first for faster intersection)
  std::sort(term_infos.begin(), term_infos.end(),
            [](const TermInfo& lhs, const TermInfo& rhs) { return lhs.estimated_size < rhs.estimated_size; });

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
    results = ApplyNotFilter(results, query.not_terms, current_index, current_ngram_size, current_kanji_ngram_size,
                             current_cross_boundary);
  }

  // Apply filter conditions (use bitmap fast path when possible)
  if (!query.filters.empty()) {
    results = ApplyFiltersWithBitmap(results, query.filters, current_doc_store);
  }

  // Calculate query execution time
  auto end_time = std::chrono::high_resolution_clock::now();
  double query_time_ms = std::chrono::duration<double, std::milli>(end_time - start_time).count();

  // Store in cache if enabled
  if (ctx_.cache_manager != nullptr && ctx_.cache_manager->IsEnabled()) {
    // Collect all ngrams from term_infos
    std::set<std::string> all_ngrams;
    for (const auto& term_info : term_infos) {
      all_ngrams.insert(term_info.ngrams.begin(), term_info.ngrams.end());
    }

    // Insert result into cache (COUNT caches the full result set like SEARCH)
    ctx_.cache_manager->Insert(query, results, all_ngrams, query_time_ms, current_ngram_size,
                               current_kanji_ngram_size, current_cross_boundary);
  }

  // Calculate final debug info
  if (conn_ctx.debug_mode) {
    debug_info.query_time_ms = query_time_ms;
    debug_info.index_time_ms = std::chrono::duration<double, std::milli>(end_time - index_start).count();

    // Cache debug info
    if (ctx_.cache_manager != nullptr && ctx_.cache_manager->IsEnabled()) {
      // Cache was enabled but missed (either not found or invalidated)
      debug_info.cache_info.status = query::CacheDebugInfo::Status::MISS_NOT_FOUND;
      debug_info.cache_info.query_cost_ms = query_time_ms;
    } else {
      // Cache disabled
      debug_info.cache_info.status = query::CacheDebugInfo::Status::MISS_DISABLED;
    }

    return ResponseFormatter::FormatCountResponse(results.size(), &debug_info);
  }

  return ResponseFormatter::FormatCountResponse(results.size());
}

std::vector<SearchHandler::TermInfo> SearchHandler::GenerateTermInfos(const std::vector<std::string>& search_terms,
                                                                      index::Index* current_index, int ngram_size,
                                                                      int kanji_ngram_size,
                                                                      query::DebugInfo* debug_info,
                                                                      bool cross_boundary_ngrams) {
  std::vector<TermInfo> term_infos;
  term_infos.reserve(search_terms.size());

  for (const auto& search_term : search_terms) {
    std::string normalized = utils::NormalizeText(search_term, current_index->GetNormalizeNfkc(), current_index->GetNormalizeWidth(), current_index->GetNormalizeLower());
    std::vector<std::string> ngrams;

    // Always use hybrid n-grams if kanji_ngram_size is configured
    if (kanji_ngram_size > 0) {
      ngrams = utils::GenerateHybridNgrams(normalized, ngram_size, kanji_ngram_size, cross_boundary_ngrams);
    } else if (ngram_size == 0) {
      ngrams = utils::GenerateHybridNgrams(normalized);
    } else {
      ngrams = utils::GenerateNgrams(normalized, ngram_size);
    }

    // Deduplicate n-grams to avoid redundant PostingList lookups
    std::sort(ngrams.begin(), ngrams.end());
    ngrams.erase(std::unique(ngrams.begin(), ngrams.end()), ngrams.end());

    // Estimate result size by checking the smallest posting list (thread-safe)
    size_t min_size = std::numeric_limits<size_t>::max();
    for (const auto& ngram : ngrams) {
      uint64_t posting_size = current_index->EstimatePostingSize(ngram);
      if (posting_size > 0) {
        min_size = std::min(min_size, static_cast<size_t>(posting_size));
      } else {
        min_size = 0;
        break;
      }
    }

    // Collect debug info for n-grams and posting list sizes
    if (debug_info != nullptr) {
      for (const auto& ngram : ngrams) {
        debug_info->ngrams_used.push_back(ngram);
      }
      debug_info->posting_list_sizes.push_back(min_size);
    }

    term_infos.push_back({std::move(ngrams), min_size});
  }

  return term_infos;
}

std::vector<storage::DocId> SearchHandler::ApplyNotFilter(const std::vector<storage::DocId>& results,
                                                          const std::vector<std::string>& not_terms,
                                                          index::Index* current_index, int ngram_size,
                                                          int kanji_ngram_size, bool cross_boundary_ngrams) {
  // Generate NOT term n-grams
  std::vector<std::string> not_ngrams;
  for (const auto& not_term : not_terms) {
    std::string norm_not = utils::NormalizeText(not_term, current_index->GetNormalizeNfkc(), current_index->GetNormalizeWidth(), current_index->GetNormalizeLower());
    std::vector<std::string> ngrams;
    if (kanji_ngram_size > 0) {
      ngrams = utils::GenerateHybridNgrams(norm_not, ngram_size, kanji_ngram_size, cross_boundary_ngrams);
    } else if (ngram_size == 0) {
      ngrams = utils::GenerateHybridNgrams(norm_not);
    } else {
      ngrams = utils::GenerateNgrams(norm_not, ngram_size);
    }
    not_ngrams.insert(not_ngrams.end(), ngrams.begin(), ngrams.end());
  }

  // Deduplicate n-grams to avoid redundant PostingList lookups in SearchNot
  std::sort(not_ngrams.begin(), not_ngrams.end());
  not_ngrams.erase(std::unique(not_ngrams.begin(), not_ngrams.end()), not_ngrams.end());

  return current_index->SearchNot(results, not_ngrams);
}

namespace {

// Pre-parsed filter values to avoid repeated string parsing in the inner loop
struct ParsedFilterValue {
  double double_val = 0.0;
  int64_t int64_val = 0;
  uint64_t uint64_val = 0;
  bool bool_val = false;
  bool double_valid = false;
  bool int64_valid = false;
  bool uint64_valid = false;
};

inline ParsedFilterValue ParseFilterValue(const std::string& value) {
  ParsedFilterValue parsed_value;

  // Parse as bool
  parsed_value.bool_val = (value == "1" || value == "true");

  // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic) - Required for from_chars range
  const char* end = value.data() + value.size();

  // Parse as double (locale-independent, no exceptions)
  {
    double result = 0.0;
    auto [ptr, ec] = std::from_chars(value.data(), end, result);
    if (ec == std::errc() && ptr == end) {
      parsed_value.double_val = result;
      parsed_value.double_valid = true;
    }
  }

  // Parse as int64_t (locale-independent, no exceptions)
  {
    int64_t result = 0;
    auto [ptr, ec] = std::from_chars(value.data(), end, result);
    if (ec == std::errc() && ptr == end) {
      parsed_value.int64_val = result;
      parsed_value.int64_valid = true;
    }
  }

  // Parse as uint64_t (locale-independent, no exceptions)
  {
    uint64_t result = 0;
    auto [ptr, ec] = std::from_chars(value.data(), end, result);
    if (ec == std::errc() && ptr == end) {
      parsed_value.uint64_val = result;
      parsed_value.uint64_valid = true;
    }
  }

  return parsed_value;
}

}  // namespace

std::vector<storage::DocId> SearchHandler::ApplyFilters(const std::vector<storage::DocId>& results,
                                                        const std::vector<query::FilterCondition>& filters,
                                                        storage::DocumentStore* doc_store) {
  std::vector<storage::DocId> filtered_results;
  filtered_results.reserve(results.size());

  // Pre-parse all filter values once before the main loop
  std::vector<ParsedFilterValue> parsed_values;
  parsed_values.reserve(filters.size());
  for (const auto& filter_cond : filters) {
    parsed_values.push_back(ParseFilterValue(filter_cond.value));
  }

  for (const auto& doc_id : results) {
    bool matches_all_filters = true;

    for (size_t i = 0; i < filters.size(); ++i) {
      const auto& filter_cond = filters[i];
      const auto& parsed_value = parsed_values[i];
      auto stored_value = doc_store->GetFilterValue(doc_id, filter_cond.column);

      // NULL values: only match for NE operator
      if (!stored_value) {
        if (filter_cond.op != query::FilterOp::NE) {
          matches_all_filters = false;
          break;
        }
        continue;  // NULL != anything is true
      }

      // Evaluate filter condition based on operator
      bool matches = std::visit(
          [&](const auto& val) -> bool {
            using T = std::decay_t<decltype(val)>;
            if constexpr (std::is_same_v<T, std::monostate>) {
              // NULL value: handled above
              return filter_cond.op == query::FilterOp::NE;
            } else if constexpr (std::is_same_v<T, std::string>) {
              // String comparison
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
              // Boolean: use pre-parsed bool value
              switch (filter_cond.op) {
                case query::FilterOp::EQ:
                  return val == parsed_value.bool_val;
                case query::FilterOp::NE:
                  return val != parsed_value.bool_val;
                default:
                  return false;  // GT/GTE/LT/LTE not meaningful for bools
              }
            } else if constexpr (std::is_same_v<T, double>) {
              // Floating-point comparison using pre-parsed value
              if (!parsed_value.double_valid) {
                return false;  // Invalid number
              }
              switch (filter_cond.op) {
                case query::FilterOp::EQ: {
                  // Use relative epsilon comparison to handle floating-point rounding
                  // (e.g., 0.1 + 0.2 == 0.3 should match)
                  double max_abs =
                      std::max({1.0, std::abs(val), std::abs(parsed_value.double_val)});
                  return std::abs(val - parsed_value.double_val) <
                         std::numeric_limits<double>::epsilon() * max_abs;
                }
                case query::FilterOp::NE: {
                  double max_abs =
                      std::max({1.0, std::abs(val), std::abs(parsed_value.double_val)});
                  return std::abs(val - parsed_value.double_val) >=
                         std::numeric_limits<double>::epsilon() * max_abs;
                }
                case query::FilterOp::GT:
                  return val > parsed_value.double_val;
                case query::FilterOp::GTE:
                  return val >= parsed_value.double_val;
                case query::FilterOp::LT:
                  return val < parsed_value.double_val;
                case query::FilterOp::LTE:
                  return val <= parsed_value.double_val;
                default:
                  return false;
              }
            } else if constexpr (std::is_same_v<T, storage::TimeValue>) {
              // TIME value comparison using pre-parsed int64 value
              if (!parsed_value.int64_valid) {
                return false;  // Invalid number
              }
              switch (filter_cond.op) {
                case query::FilterOp::EQ:
                  return val.seconds == parsed_value.int64_val;
                case query::FilterOp::NE:
                  return val.seconds != parsed_value.int64_val;
                case query::FilterOp::GT:
                  return val.seconds > parsed_value.int64_val;
                case query::FilterOp::GTE:
                  return val.seconds >= parsed_value.int64_val;
                case query::FilterOp::LT:
                  return val.seconds < parsed_value.int64_val;
                case query::FilterOp::LTE:
                  return val.seconds <= parsed_value.int64_val;
                default:
                  return false;
              }
            } else if constexpr (std::is_same_v<T, uint64_t> || std::is_same_v<T, uint32_t> ||
                                 std::is_same_v<T, uint16_t> || std::is_same_v<T, uint8_t>) {
              // Unsigned integer comparison using pre-parsed uint64 value
              if (!parsed_value.uint64_valid) {
                return false;  // Invalid number
              }
              auto unsigned_val = static_cast<uint64_t>(val);
              switch (filter_cond.op) {
                case query::FilterOp::EQ:
                  return unsigned_val == parsed_value.uint64_val;
                case query::FilterOp::NE:
                  return unsigned_val != parsed_value.uint64_val;
                case query::FilterOp::GT:
                  return unsigned_val > parsed_value.uint64_val;
                case query::FilterOp::GTE:
                  return unsigned_val >= parsed_value.uint64_val;
                case query::FilterOp::LT:
                  return unsigned_val < parsed_value.uint64_val;
                case query::FilterOp::LTE:
                  return unsigned_val <= parsed_value.uint64_val;
                default:
                  return false;
              }
            } else {
              // Signed integer comparison using pre-parsed int64 value
              if (!parsed_value.int64_valid) {
                return false;  // Invalid number
              }
              auto signed_val = static_cast<int64_t>(val);
              switch (filter_cond.op) {
                case query::FilterOp::EQ:
                  return signed_val == parsed_value.int64_val;
                case query::FilterOp::NE:
                  return signed_val != parsed_value.int64_val;
                case query::FilterOp::GT:
                  return signed_val > parsed_value.int64_val;
                case query::FilterOp::GTE:
                  return signed_val >= parsed_value.int64_val;
                case query::FilterOp::LT:
                  return signed_val < parsed_value.int64_val;
                case query::FilterOp::LTE:
                  return signed_val <= parsed_value.int64_val;
                default:
                  return false;
              }
            }
          },
          stored_value.value());

      if (!matches) {
        matches_all_filters = false;
        break;
      }
    }

    if (matches_all_filters) {
      filtered_results.push_back(doc_id);
    }
  }

  return filtered_results;
}

bool SearchHandler::AllFiltersHaveBitmapSupport(const std::vector<query::FilterCondition>& filters,
                                                 storage::DocumentStore* doc_store) {
  (void)doc_store;  // FilterIndex existence is checked by caller
  for (const auto& filter : filters) {
    if (filter.op != query::FilterOp::EQ && filter.op != query::FilterOp::NE) {
      return false;
    }
  }
  return true;
}

/// Build a bitmap union of all type interpretations of a filter value string
static roaring_bitmap_t* BuildTypeUnionBitmap(const storage::FilterIndex* filter_index, const std::string& column,
                                              const std::string& value) {
  roaring_bitmap_t* union_bm = roaring_bitmap_create();

  auto try_add = [&](const storage::FilterValue& fv) {
    std::string key = storage::FilterIndex::SerializeFilterValue(fv);
    const roaring_bitmap_t* bm = filter_index->GetEqBitmap(column, key);
    if (bm != nullptr) {
      roaring_bitmap_or_inplace(union_bm, bm);
    }
  };

  // Try string
  try_add(storage::FilterValue{value});

  // Try bool
  if (value == "1" || value == "true") {
    try_add(storage::FilterValue{true});
  } else if (value == "0" || value == "false") {
    try_add(storage::FilterValue{false});
  }

  // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
  const char* end = value.data() + value.size();

  // Try int64_t and narrower signed types
  {
    int64_t val = 0;
    auto [ptr, ec] = std::from_chars(value.data(), end, val);
    if (ec == std::errc() && ptr == end) {
      try_add(storage::FilterValue{val});
      if (val >= INT8_MIN && val <= INT8_MAX) {
        try_add(storage::FilterValue{static_cast<int8_t>(val)});
      }
      if (val >= INT16_MIN && val <= INT16_MAX) {
        try_add(storage::FilterValue{static_cast<int16_t>(val)});
      }
      if (val >= INT32_MIN && val <= INT32_MAX) {
        try_add(storage::FilterValue{static_cast<int32_t>(val)});
      }
      // Try TimeValue (TIME columns stored as seconds)
      try_add(storage::FilterValue{storage::TimeValue{val}});
    }
  }

  // Try uint64_t and narrower unsigned types
  {
    uint64_t val = 0;
    auto [ptr, ec] = std::from_chars(value.data(), end, val);
    if (ec == std::errc() && ptr == end) {
      try_add(storage::FilterValue{val});
      if (val <= UINT8_MAX) {
        try_add(storage::FilterValue{static_cast<uint8_t>(val)});
      }
      if (val <= UINT16_MAX) {
        try_add(storage::FilterValue{static_cast<uint16_t>(val)});
      }
      if (val <= UINT32_MAX) {
        try_add(storage::FilterValue{static_cast<uint32_t>(val)});
      }
    }
  }

  // Try double
  {
    double val = 0.0;
    auto [ptr, ec] = std::from_chars(value.data(), end, val);
    if (ec == std::errc() && ptr == end) {
      try_add(storage::FilterValue{val});
    }
  }

  return union_bm;
}

std::vector<storage::DocId> SearchHandler::ApplyFiltersWithBitmap(const std::vector<storage::DocId>& results,
                                                                   const std::vector<query::FilterCondition>& filters,
                                                                   storage::DocumentStore* doc_store) {
  // Take a shared_ptr snapshot of filter_index — keeps it alive even if
  // a concurrent writer replaces doc_store's filter_index_ pointer.
  auto filter_index = doc_store->GetFilterIndex();

  // Check if all filters can use bitmap acceleration
  if (filter_index == nullptr || !AllFiltersHaveBitmapSupport(filters, doc_store)) {
    return ApplyFilters(results, filters, doc_store);
  }

  // Convert results vector to a temporary Roaring bitmap
  roaring_bitmap_t* result_bm = roaring_bitmap_create();
  roaring_bitmap_add_many(result_bm, results.size(), results.data());

  for (const auto& filter : filters) {
    if (filter.op == query::FilterOp::EQ) {
      roaring_bitmap_t* match_bm = BuildTypeUnionBitmap(filter_index.get(), filter.column, filter.value);
      roaring_bitmap_and_inplace(result_bm, match_bm);
      roaring_bitmap_free(match_bm);
    } else if (filter.op == query::FilterOp::NE) {
      roaring_bitmap_t* exclude_bm = BuildTypeUnionBitmap(filter_index.get(), filter.column, filter.value);
      roaring_bitmap_andnot_inplace(result_bm, exclude_bm);
      roaring_bitmap_free(exclude_bm);
    }
  }

  // Convert bitmap back to sorted vector
  uint64_t cardinality = roaring_bitmap_get_cardinality(result_bm);
  std::vector<storage::DocId> filtered_results(cardinality);
  roaring_bitmap_to_uint32_array(result_bm, filtered_results.data());
  roaring_bitmap_free(result_bm);

  return filtered_results;
}

}  // namespace mygramdb::server
