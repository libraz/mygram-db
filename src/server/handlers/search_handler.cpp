/**
 * @file search_handler.cpp
 * @brief Handler for SEARCH and COUNT commands
 */

#include "server/handlers/search_handler.h"

#include <algorithm>
#include <chrono>
#include <limits>

#include "cache/cache_manager.h"
#include "query/result_sorter.h"
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
        auto cache_lookup_end = std::chrono::high_resolution_clock::now();
        double cache_lookup_time_ms =
            std::chrono::duration<double, std::milli>(cache_lookup_end - cache_lookup_start).count();

        // Apply pagination to cached results
        // Cache stores full results (before pagination) to allow different OFFSET/LIMIT on same query
        auto full_results = cached_lookup.value().results;
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
  auto term_infos = GenerateTermInfos(all_search_terms, current_index, current_ngram_size, current_kanji_ngram_size,
                                      conn_ctx.debug_mode ? &debug_info : nullptr);

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
  bool is_primary_key_order = order_by.IsPrimaryKey() || order_by.column == primary_key_column;

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

    // Heuristic: reuse fetched results if offset+limit is close to total_results
    // NOLINTBEGIN(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
    constexpr double kReuseThreshold = 0.5;  // Reuse if fetching >50% of results
    // NOLINTEND(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
    size_t index_limit = query.offset + query.limit;
    bool should_reuse = (static_cast<double>(index_limit) / static_cast<double>(total_results)) > kReuseThreshold;

    if (should_reuse) {
      // Reuse the already-fetched results
      results = std::move(all_results);
      can_optimize = false;  // Use standard sort+paginate path
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
    auto and_results = current_index->SearchAnd(term_infos[i].ngrams);
    std::vector<storage::DocId> intersection;
    intersection.reserve(std::min(results.size(), and_results.size()));
    std::set_intersection(results.begin(), results.end(), and_results.begin(), and_results.end(),
                          std::back_inserter(intersection));
    results = std::move(intersection);
  }

  // Apply NOT filter if present
  if (!query.not_terms.empty()) {
    results = ApplyNotFilter(results, query.not_terms, current_index, current_ngram_size, current_kanji_ngram_size);
    if (conn_ctx.debug_mode) {
      debug_info.after_not = results.size();
    }
  } else if (conn_ctx.debug_mode) {
    debug_info.after_not = results.size();
  }

  // Apply filter conditions
  auto filter_start = std::chrono::high_resolution_clock::now();
  if (!query.filters.empty()) {
    results = ApplyFilters(results, query.filters, current_doc_store);
    if (conn_ctx.debug_mode) {
      auto filter_end = std::chrono::high_resolution_clock::now();
      debug_info.filter_time_ms = std::chrono::duration<double, std::milli>(filter_end - filter_start).count();
      debug_info.after_filters = results.size();
    }
  } else if (conn_ctx.debug_mode) {
    debug_info.after_filters = results.size();
  }

  // Sort and paginate results
  if (!can_optimize) {
    total_results = results.size();
  }

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
    ctx_.cache_manager->Insert(query, results, all_ngrams, query_time_ms);
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
      // Cache hit! Return count from cached result
      auto cache_lookup_end = std::chrono::high_resolution_clock::now();
      double cache_lookup_time_ms =
          std::chrono::duration<double, std::milli>(cache_lookup_end - cache_lookup_start).count();

      if (conn_ctx.debug_mode) {
        query::DebugInfo debug_info;
        debug_info.query_time_ms = cache_lookup_time_ms;
        debug_info.final_results = cached_lookup.value().results.size();

        // Cache hit debug info with actual metadata
        auto now = std::chrono::steady_clock::now();
        debug_info.cache_info.status = query::CacheDebugInfo::Status::HIT;
        debug_info.cache_info.cache_age_ms =
            std::chrono::duration<double, std::milli>(now - cached_lookup.value().created_at).count();
        debug_info.cache_info.cache_saved_ms = cached_lookup.value().query_cost_ms;

        return ResponseFormatter::FormatCountResponse(cached_lookup.value().results.size(), &debug_info);
      }

      return ResponseFormatter::FormatCountResponse(cached_lookup.value().results.size());
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
  auto term_infos = GenerateTermInfos(all_search_terms, current_index, current_ngram_size, current_kanji_ngram_size,
                                      conn_ctx.debug_mode ? &debug_info : nullptr);

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
    auto and_results = current_index->SearchAnd(term_infos[i].ngrams);
    std::vector<storage::DocId> intersection;
    intersection.reserve(std::min(results.size(), and_results.size()));
    std::set_intersection(results.begin(), results.end(), and_results.begin(), and_results.end(),
                          std::back_inserter(intersection));
    results = std::move(intersection);
  }

  // Apply NOT filter if present
  if (!query.not_terms.empty()) {
    results = ApplyNotFilter(results, query.not_terms, current_index, current_ngram_size, current_kanji_ngram_size);
  }

  // Apply filter conditions
  if (!query.filters.empty()) {
    results = ApplyFilters(results, query.filters, current_doc_store);
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
    ctx_.cache_manager->Insert(query, results, all_ngrams, query_time_ms);
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
                                                                      query::DebugInfo* debug_info) {
  std::vector<TermInfo> term_infos;
  term_infos.reserve(search_terms.size());

  for (const auto& search_term : search_terms) {
    std::string normalized = utils::NormalizeText(search_term, true, "keep", true);
    std::vector<std::string> ngrams;

    // Always use hybrid n-grams if kanji_ngram_size is configured
    if (kanji_ngram_size > 0) {
      ngrams = utils::GenerateHybridNgrams(normalized, ngram_size, kanji_ngram_size);
    } else if (ngram_size == 0) {
      ngrams = utils::GenerateHybridNgrams(normalized);
    } else {
      ngrams = utils::GenerateNgrams(normalized, ngram_size);
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
                                                          int kanji_ngram_size) {
  // Generate NOT term n-grams
  std::vector<std::string> not_ngrams;
  for (const auto& not_term : not_terms) {
    std::string norm_not = utils::NormalizeText(not_term, true, "keep", true);
    std::vector<std::string> ngrams;
    if (kanji_ngram_size > 0) {
      ngrams = utils::GenerateHybridNgrams(norm_not, ngram_size, kanji_ngram_size);
    } else if (ngram_size == 0) {
      ngrams = utils::GenerateHybridNgrams(norm_not);
    } else {
      ngrams = utils::GenerateNgrams(norm_not, ngram_size);
    }
    not_ngrams.insert(not_ngrams.end(), ngrams.begin(), ngrams.end());
  }

  return current_index->SearchNot(results, not_ngrams);
}

std::vector<storage::DocId> SearchHandler::ApplyFilters(const std::vector<storage::DocId>& results,
                                                        const std::vector<query::FilterCondition>& filters,
                                                        storage::DocumentStore* doc_store) {
  std::vector<storage::DocId> filtered_results;
  filtered_results.reserve(results.size());

  for (const auto& doc_id : results) {
    bool matches_all_filters = true;

    for (const auto& filter_cond : filters) {
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
              // Boolean: convert filter value to bool
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
              // Floating-point comparison
              double filter_val = 0.0;
              try {
                filter_val = std::stod(filter_cond.value);
              } catch (const std::exception&) {
                return false;  // Invalid number
              }
              switch (filter_cond.op) {
                case query::FilterOp::EQ:
                  // NOLINTBEGIN(clang-analyzer-core.UndefinedBinaryOperatorResult)
                  return val == filter_val;
                  // NOLINTEND(clang-analyzer-core.UndefinedBinaryOperatorResult)
                case query::FilterOp::NE:
                  // NOLINTBEGIN(clang-analyzer-core.UndefinedBinaryOperatorResult)
                  return val != filter_val;
                  // NOLINTEND(clang-analyzer-core.UndefinedBinaryOperatorResult)
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

}  // namespace mygramdb::server
