/**
 * @file search_handler.cpp
 * @brief Handler for SEARCH and COUNT commands
 */

#include "server/handlers/search_handler.h"

#include <algorithm>
#include <chrono>

#include "cache/cache_manager.h"
#include "index/bm25_scorer.h"
#include "query/highlighter.h"
#include "query/result_sorter.h"
#include "query/synonym_dictionary.h"
#include "server/search_pipeline.h"
#include "server/table_catalog.h"
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

std::string SearchHandler::ExecuteSearchPipeline(const query::Query& query, ConnectionContext& conn_ctx,
                                                 PipelineOutput& output) {
  // Check if server is loading
  if (ctx_.dump_load_in_progress) {
    return ResponseFormatter::FormatError("Server is loading, please try again later");
  }

  // Get table context (needed for both cache lookup and search)
  auto table_ctx = GetTableContext(query.table);
  if (!table_ctx) {
    return ResponseFormatter::FormatError(table_ctx.error().message());
  }
  output.current_index = table_ctx->index;
  output.current_doc_store = table_ctx->doc_store;
  output.current_ngram_size = table_ctx->ngram_size;
  output.current_kanji_ngram_size = table_ctx->kanji_ngram_size;

  // Verify index is available
  if (output.current_index == nullptr) {
    return ResponseFormatter::FormatError("Index not available");
  }

  // Try cache lookup first
  auto cache_lookup_start = std::chrono::high_resolution_clock::now();
  auto cache_result = search_pipeline::TryCacheLookup(query, ctx_.cache_manager, output.current_doc_store);
  if (cache_result) {
    auto cache_lookup_end = std::chrono::high_resolution_clock::now();
    double cache_lookup_time_ms =
        std::chrono::duration<double, std::milli>(cache_lookup_end - cache_lookup_start).count();

    output.results = std::move(cache_result->results);
    output.query_time_ms = cache_lookup_time_ms;

    if (!query.search_text.empty()) {
      output.all_search_terms.push_back(query.search_text);
    }
    output.all_search_terms.insert(output.all_search_terms.end(), query.and_terms.begin(), query.and_terms.end());

    if (conn_ctx.debug_mode) {
      output.debug_info.query_time_ms = cache_lookup_time_ms;
      output.debug_info.final_results = output.results.size();
      output.debug_info.cache_info.status = query::CacheDebugInfo::Status::HIT;
      output.debug_info.cache_info.cache_age_ms = cache_result->cache_age_ms;
      output.debug_info.cache_info.cache_saved_ms = cache_result->cache_saved_ms;
    }

    // Empty string = success (cache hit)
    return "";
  }

  // Start timing
  auto start_time = std::chrono::high_resolution_clock::now();
  auto index_start = std::chrono::high_resolution_clock::now();

  // Collect all search terms (main + AND terms)
  if (!query.search_text.empty()) {
    output.all_search_terms.push_back(query.search_text);
  }
  output.all_search_terms.insert(output.all_search_terms.end(), query.and_terms.begin(), query.and_terms.end());

  // Collect debug info for search terms
  if (conn_ctx.debug_mode) {
    output.debug_info.search_terms = output.all_search_terms;
  }

  // Check for synonym dictionary
  output.current_cross_boundary = output.current_index->GetCrossBoundaryNgrams();
  query::SynonymDictionary* synonym_dict = nullptr;
  {
    auto* table_ctx = ctx_.table_catalog ? ctx_.table_catalog->GetTable(query.table) : nullptr;
    if (table_ctx != nullptr && table_ctx->synonym_dict && !table_ctx->synonym_dict->IsEmpty()) {
      synonym_dict = table_ctx->synonym_dict.get();
    }
  }

  // Fuzzy search path (takes precedence over synonyms)
  if (query.fuzzy_max_distance.has_value()) {
    // Generate n-grams for each term (same as normal path)
    output.term_infos =
        search_pipeline::GenerateTermInfos(output.all_search_terms, output.current_index, output.current_ngram_size,
                                           output.current_kanji_ngram_size, output.current_cross_boundary);

    if (conn_ctx.debug_mode) {
      for (const auto& ti : output.term_infos) {
        for (const auto& ngram : ti.ngrams) {
          output.debug_info.ngrams_used.push_back(ngram);
        }
        output.debug_info.posting_list_sizes.push_back(ti.estimated_size);
      }
    }

    auto pipeline_result = search_pipeline::ExecuteWithFuzzy(
        query, output.term_infos, output.all_search_terms, *query.fuzzy_max_distance, output.current_index,
        output.current_doc_store, ctx_.full_config, output.current_ngram_size, output.current_kanji_ngram_size,
        output.current_cross_boundary, filter_threshold_);

    if (pipeline_result.empty_term_detected) {
      output.results.clear();
      auto end_time = std::chrono::high_resolution_clock::now();
      output.query_time_ms = std::chrono::duration<double, std::milli>(end_time - start_time).count();
      if (conn_ctx.debug_mode) {
        output.debug_info.optimization_used = "fuzzy-search (empty posting list)";
        output.debug_info.final_results = 0;
        output.debug_info.query_time_ms = output.query_time_ms;
        output.debug_info.index_time_ms = std::chrono::duration<double, std::milli>(end_time - index_start).count();
      }
      return "";
    }

    output.results = std::move(pipeline_result.results);

    if (conn_ctx.debug_mode) {
      output.debug_info.total_candidates = output.results.size();
      output.debug_info.after_intersection = output.results.size();
      output.debug_info.optimization_used = "fuzzy-search (distance=" + std::to_string(*query.fuzzy_max_distance) + ")";
      output.debug_info.after_not = output.results.size();
      output.debug_info.after_filters = output.results.size();
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    output.query_time_ms = std::chrono::duration<double, std::milli>(end_time - start_time).count();

    search_pipeline::InsertToCache(ctx_.cache_manager, query, output.results, output.term_infos, output.query_time_ms,
                                   output.current_ngram_size, output.current_kanji_ngram_size,
                                   output.current_cross_boundary);

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

  if (synonym_dict != nullptr) {
    // Synonym-aware search path
    auto synonym_groups = search_pipeline::ExpandTermsWithSynonyms(
        output.all_search_terms, synonym_dict, output.current_index, output.current_ngram_size,
        output.current_kanji_ngram_size, output.current_cross_boundary);

    if (conn_ctx.debug_mode) {
      for (const auto& group : synonym_groups) {
        for (const auto& variant : group.variants) {
          for (const auto& ngram : variant.ngrams) {
            output.debug_info.ngrams_used.push_back(ngram);
          }
          output.debug_info.posting_list_sizes.push_back(variant.estimated_size);
        }
      }
    }

    auto pipeline_result = search_pipeline::ExecuteWithSynonyms(
        query, synonym_groups, output.current_index, output.current_doc_store, ctx_.full_config,
        output.current_ngram_size, output.current_kanji_ngram_size, output.current_cross_boundary, filter_threshold_);

    if (pipeline_result.empty_term_detected) {
      output.results.clear();
      auto end_time = std::chrono::high_resolution_clock::now();
      output.query_time_ms = std::chrono::duration<double, std::milli>(end_time - start_time).count();
      if (conn_ctx.debug_mode) {
        output.debug_info.optimization_used = "synonym-search (empty posting list)";
        output.debug_info.final_results = 0;
        output.debug_info.query_time_ms = output.query_time_ms;
        output.debug_info.index_time_ms = std::chrono::duration<double, std::milli>(end_time - index_start).count();
      }
      return "";
    }

    output.results = std::move(pipeline_result.results);

    if (conn_ctx.debug_mode) {
      output.debug_info.total_candidates = output.results.size();
      output.debug_info.after_intersection = output.results.size();
      output.debug_info.optimization_used = "synonym-expanded search";
      output.debug_info.after_not = output.results.size();
      output.debug_info.after_filters = output.results.size();
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    output.query_time_ms = std::chrono::duration<double, std::milli>(end_time - start_time).count();

    // Collect all n-grams for cache invalidation
    std::vector<SearchTermInfo> all_term_infos;
    // Reserve capacity to avoid reallocation during synonym term collection
    size_t total_variants = 0;
    for (const auto& group : synonym_groups) {
      total_variants += group.variants.size();
    }
    all_term_infos.reserve(total_variants);
    for (const auto& group : synonym_groups) {
      for (const auto& variant : group.variants) {
        all_term_infos.push_back(variant);
      }
    }
    search_pipeline::InsertToCache(ctx_.cache_manager, query, output.results, all_term_infos, output.query_time_ms,
                                   output.current_ngram_size, output.current_kanji_ngram_size,
                                   output.current_cross_boundary);

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

  // Non-synonym path: generate n-grams for each term and estimate result sizes
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
  {
    auto* table_ctx = ctx_.table_catalog ? ctx_.table_catalog->GetTable(query.table) : nullptr;
    if (table_ctx != nullptr) {
      primary_key_column = table_ctx->config.primary_key;
    }
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

    // Check if this is a BM25 score sort request
    bool is_score_sort = query.order_by.has_value() && query.order_by->IsScoreSort();

    // Check optimization conditions for single-term queries
    // NOLINTBEGIN(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
    constexpr uint32_t kMaxOffsetForOptimization = 10000;
    // NOLINTEND(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
    bool can_optimize = output.term_infos.size() == 1 && query.not_terms.empty() && query.filters.empty() &&
                        query.limit > 0 && query.offset <= kMaxOffsetForOptimization && is_primary_key_order &&
                        !is_score_sort;

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

  // Helper: generate highlight snippets for paginated results
  auto generate_snippets = [&](const std::vector<storage::DocId>& paginated_results) -> std::vector<std::string> {
    if (!query.highlight.has_value()) {
      return {};
    }

    const auto& hl_opts = query.highlight.value();

    // Normalize search terms for matching
    std::vector<std::string> normalized_terms;
    for (const auto& term : output.all_search_terms) {
      if (output.current_index != nullptr) {
        normalized_terms.push_back(output.current_index->NormalizeText(term));
      } else {
        normalized_terms.push_back(term);
      }
    }

    // Check for synonym expansion: include synonym variants as highlight targets
    auto* table_ctx3 = ctx_.table_catalog ? ctx_.table_catalog->GetTable(query.table) : nullptr;
    if (table_ctx3 != nullptr && table_ctx3->synonym_dict && !table_ctx3->synonym_dict->IsEmpty()) {
      auto* syn_dict = table_ctx3->synonym_dict.get();
      std::vector<std::string> expanded;
      for (const auto& nt : normalized_terms) {
        auto synonyms = syn_dict->Expand(nt);
        for (auto& s : synonyms) {
          expanded.push_back(std::move(s));
        }
      }
      mygram::utils::DeduplicateSorted(expanded);
      normalized_terms = std::move(expanded);
    }

    auto batch_texts = output.current_doc_store->GetNormalizedTextBatch(paginated_results);
    std::vector<std::string> snippets;
    snippets.reserve(paginated_results.size());
    for (const auto& text_opt : batch_texts) {
      if (text_opt.has_value()) {
        auto hl_result = query::Highlighter::Generate(text_opt.value(), normalized_terms, hl_opts);
        snippets.push_back(std::move(hl_result.snippet));
      } else {
        snippets.emplace_back();
      }
    }
    return snippets;
  };

  // Validate HIGHLIGHT is possible (requires stored normalized text)
  if (query.highlight.has_value() && output.current_doc_store != nullptr &&
      !output.current_doc_store->IsStoreTextsEnabled()) {
    return ResponseFormatter::FormatError(
        "HIGHLIGHT requires normalized text storage. Set memory.verify_text to \"ascii\" or \"all\" in configuration.");
  }

  // BM25 scoring: compute scores if SORT _score is requested
  bool is_score_sort = query.order_by.has_value() && query.order_by->IsScoreSort();
  if (is_score_sort && !output.results.empty()) {
    // Validate BM25 is enabled
    if (ctx_.full_config == nullptr || !ctx_.full_config->bm25.enable) {
      return ResponseFormatter::FormatError("SORT _score requires BM25 to be enabled in configuration");
    }

    const auto& bm25_config = ctx_.full_config->bm25;
    auto* bm25_table_ctx = ctx_.table_catalog ? ctx_.table_catalog->GetTable(query.table) : nullptr;
    if (bm25_table_ctx == nullptr) {
      return ResponseFormatter::FormatError("table not found");
    }
    const auto& bm25_stats = bm25_table_ctx->bm25_stats;

    index::BM25Params params{bm25_config.k1, bm25_config.b};

    // Regenerate term_infos if empty (e.g., cache hit path skips GenerateTermInfos)
    if (output.term_infos.empty() && !output.all_search_terms.empty() && output.current_index != nullptr) {
      output.term_infos =
          search_pipeline::GenerateTermInfos(output.all_search_terms, output.current_index, output.current_ngram_size,
                                             output.current_kanji_ngram_size, output.current_cross_boundary);
    }

    // Reuse pre-computed term_infos (already normalized and ngram-generated)
    std::vector<std::string> normalized_terms;
    std::vector<uint64_t> term_dfs;
    normalized_terms.reserve(output.all_search_terms.size());
    term_dfs.reserve(output.term_infos.size());
    for (const auto& term : output.all_search_terms) {
      normalized_terms.push_back(output.current_index->NormalizeText(term));
    }
    for (const auto& ti : output.term_infos) {
      uint64_t df = (ti.estimated_size == std::numeric_limits<size_t>::max()) ? 0 : ti.estimated_size;
      term_dfs.push_back(df);
    }

    auto scored = index::BM25Scorer::ScoreDocuments(
        output.results, normalized_terms, term_dfs, *output.current_doc_store,
        bm25_stats.doc_count.load(std::memory_order_relaxed), bm25_stats.avg_doc_length(), params);

    // Extract scores parallel to results
    std::vector<double> scores;
    scores.reserve(scored.size());
    for (const auto& sd : scored) {
      scores.push_back(sd.score);
    }

    auto sort_order = query.order_by->order;
    auto sorted_results =
        query::ResultSorter::SortByScore(output.results, scores, sort_order, query.limit, query.offset);
    size_t score_total = output.results.size();

    if (query.highlight.has_value()) {
      auto snippets = generate_snippets(sorted_results);
      if (conn_ctx.debug_mode) {
        output.debug_info.final_results = sorted_results.size();
        return ResponseFormatter::FormatSearchResponseWithHighlights(
            sorted_results, score_total, output.current_doc_store, snippets, &output.debug_info);
      }
      return ResponseFormatter::FormatSearchResponseWithHighlights(sorted_results, score_total,
                                                                   output.current_doc_store, snippets);
    }

    if (conn_ctx.debug_mode) {
      output.debug_info.final_results = sorted_results.size();
      return ResponseFormatter::FormatSearchResponse(sorted_results, score_total, output.current_doc_store,
                                                     &output.debug_info);
    }
    return ResponseFormatter::FormatSearchResponse(sorted_results, score_total, output.current_doc_store);
  }

  // Sort and paginate results
  auto sorted_result =
      query::ResultSorter::SortAndPaginate(output.results, *output.current_doc_store, query, primary_key_column);

  if (!sorted_result.has_value()) {
    return sorted_result.error().to_string();
  }

  auto sorted_results = std::move(sorted_result.value());

  if (query.highlight.has_value()) {
    auto snippets = generate_snippets(sorted_results);
    if (conn_ctx.debug_mode) {
      output.debug_info.final_results = sorted_results.size();
      return ResponseFormatter::FormatSearchResponseWithHighlights(
          sorted_results, total_results, output.current_doc_store, snippets, &output.debug_info);
    }
    return ResponseFormatter::FormatSearchResponseWithHighlights(sorted_results, total_results,
                                                                 output.current_doc_store, snippets);
  }

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
