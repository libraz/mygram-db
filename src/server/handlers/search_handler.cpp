/**
 * @file search_handler.cpp
 * @brief Handler for SEARCH and COUNT commands
 */

#include "server/handlers/search_handler.h"

#include <algorithm>
#include <chrono>
#include <iterator>

#include "cache/cache_manager.h"
#include "index/bm25_scorer.h"
#include "query/highlighter.h"
#include "query/result_sorter.h"
#include "query/synonym_dictionary.h"
#include "server/search_pipeline.h"
#include "server/table_catalog.h"
#include "utils/string_utils.h"
#include "utils/structured_log.h"

namespace mygramdb::server {

namespace {

/// @brief Pick the optimization_used label for a given path + early-exit state.
const char* OptimizationLabel(search_pipeline::PipelinePath path, bool empty_term_detected) {
  switch (path) {
    case search_pipeline::PipelinePath::FUZZY:
      return empty_term_detected ? "fuzzy-search (empty posting list)" : "";  // detailed label set elsewhere
    case search_pipeline::PipelinePath::SYNONYM:
      return empty_term_detected ? "synonym-search (empty posting list)" : "synonym-expanded search";
    case search_pipeline::PipelinePath::REGULAR:
      return empty_term_detected ? "early-exit (empty posting list)" : "size-based term ordering";
    case search_pipeline::PipelinePath::CACHE_HIT:
    default:
      return "";
  }
}

}  // namespace

std::string SearchHandler::Handle(const query::Query& query, ConnectionContext& conn_ctx) {
  if (query.type == query::QueryType::SEARCH) {
    return HandleSearch(query, conn_ctx);
  }
  if (query.type == query::QueryType::COUNT) {
    return HandleCount(query, conn_ctx);
  }
  return ResponseFormatter::FormatError("Invalid query type for SearchHandler");
}

search_pipeline::FullPipelineParams SearchHandler::BuildPipelineParams(const query::Query& query,
                                                                       const PipelineOutput& output) const {
  (void)query;
  if (output.table_context != nullptr) {
    return search_pipeline::BuildPipelineParamsFromContext(*output.table_context, ctx_.full_config, ctx_.cache_manager,
                                                           filter_threshold_.load(std::memory_order_relaxed),
                                                           /*attach_bm25_stats=*/true);
  }

  // Fallback for the (defensive) case where the catalog lost the entry
  // between GetTableContext() and here. Use the data already projected onto
  // PipelineOutput so the pipeline can still execute against the resolved
  // index/doc_store. The pipeline will return an "Index not available" error
  // if those are also null.
  search_pipeline::FullPipelineParams params;
  params.current_index = output.current_index;
  params.current_doc_store = output.current_doc_store;
  params.full_config = ctx_.full_config;
  params.cache_manager = ctx_.cache_manager;
  params.ngram_size = output.current_ngram_size;
  params.kanji_ngram_size = output.current_kanji_ngram_size;
  params.cross_boundary_ngrams = output.current_cross_boundary;
  params.filter_threshold = filter_threshold_.load(std::memory_order_relaxed);
  return params;
}

void SearchHandler::PopulateInputDebugInfo(search_pipeline::PipelinePath path_taken,
                                           const search_pipeline::FullPipelineParams& params, PipelineOutput& output) {
  // search_terms reflects the terms actually fed to the search engine.
  output.debug_info.search_terms = output.all_search_terms;

  if (path_taken == search_pipeline::PipelinePath::SYNONYM && params.synonym_dict != nullptr) {
    // Synonym path: re-expand terms to recover variant n-grams for debug only.
    // ExecuteFullPipeline drops the per-variant SearchTermInfo after computing
    // results; recreating it here avoids changing the public output struct
    // and only runs when debug_mode is enabled.
    auto synonym_groups = search_pipeline::ExpandTermsWithSynonyms(
        output.all_search_terms, params.synonym_dict, output.current_index, output.current_ngram_size,
        output.current_kanji_ngram_size, output.current_cross_boundary);
    for (const auto& group : synonym_groups) {
      for (const auto& variant : group.variants) {
        for (const auto& ngram : variant.ngrams) {
          output.debug_info.ngrams_used.push_back(ngram);
        }
        output.debug_info.posting_list_sizes.push_back(variant.estimated_size);
      }
    }
    return;
  }

  // Fuzzy / regular path: copy from term_infos populated by ExecuteFullPipeline.
  for (const auto& ti : output.term_infos) {
    for (const auto& ngram : ti.ngrams) {
      output.debug_info.ngrams_used.push_back(ngram);
    }
    output.debug_info.posting_list_sizes.push_back(ti.estimated_size);
  }
}

void SearchHandler::PopulateCacheHitDebugInfo(const search_pipeline::FullPipelineOutput& pipeline_output,
                                              PipelineOutput& output) {
  output.debug_info.query_time_ms = pipeline_output.query_time_ms;
  output.debug_info.final_results = output.results.size();
  output.debug_info.cache_info.status = query::CacheDebugInfo::Status::HIT;
  output.debug_info.cache_info.cache_age_ms = pipeline_output.cache_age_ms;
  output.debug_info.cache_info.cache_saved_ms = pipeline_output.cache_saved_ms;
}

void SearchHandler::PopulateEmptyTermDebugInfo(search_pipeline::PipelinePath path_taken,
                                               const search_pipeline::FullPipelineOutput& pipeline_output,
                                               PipelineOutput& output) {
  output.debug_info.optimization_used = OptimizationLabel(path_taken, /*empty_term_detected=*/true);
  output.debug_info.final_results = 0;
  output.debug_info.query_time_ms = pipeline_output.query_time_ms;
  output.debug_info.index_time_ms = pipeline_output.query_time_ms;
}

void SearchHandler::PopulatePostPipelineDebugInfo(const query::Query& query,
                                                  const search_pipeline::FullPipelineOutput& pipeline_output,
                                                  PipelineOutput& output) const {
  // Successful (non-empty-term) path: report the matched candidates count.
  output.debug_info.total_candidates = output.results.size();
  output.debug_info.after_intersection = output.results.size();
  output.debug_info.after_not = output.results.size();
  output.debug_info.after_filters = output.results.size();

  // Path-specific optimization label.
  if (pipeline_output.path_taken == search_pipeline::PipelinePath::FUZZY) {
    // Fuzzy path: append the configured edit distance.
    int distance = query.fuzzy_max_distance.value_or(1);
    output.debug_info.optimization_used = "fuzzy-search (distance=" + std::to_string(distance) + ")";
  } else {
    output.debug_info.optimization_used = OptimizationLabel(pipeline_output.path_taken, /*empty_term_detected=*/false);
  }

  output.debug_info.query_time_ms = pipeline_output.query_time_ms;
  output.debug_info.index_time_ms = pipeline_output.query_time_ms;

  // Map the pipeline's authoritative miss reason to the debug protocol's
  // CacheDebugInfo::Status. The pipeline distinguishes Stale vs NotFound vs
  // Disabled; we collapse Stale and Disabled into the matching debug enums
  // and report NotFound otherwise. Falling through to a single MISS_NOT_FOUND
  // (the previous behaviour) hid genuinely-disabled and stale cases.
  switch (pipeline_output.cache_miss_reason) {
    case search_pipeline::CacheMissReason::kHit:
      // Should not reach here: the cache-hit branch is handled by
      // PopulateCacheHitDebugInfo. Treat as not-found to keep the field set.
      output.debug_info.cache_info.status = query::CacheDebugInfo::Status::MISS_NOT_FOUND;
      output.debug_info.cache_info.query_cost_ms = output.query_time_ms;
      break;
    case search_pipeline::CacheMissReason::kStale:
      output.debug_info.cache_info.status = query::CacheDebugInfo::Status::MISS_INVALIDATED;
      output.debug_info.cache_info.query_cost_ms = output.query_time_ms;
      break;
    case search_pipeline::CacheMissReason::kNotFound:
    case search_pipeline::CacheMissReason::kCostBelowThreshold:
      output.debug_info.cache_info.status = query::CacheDebugInfo::Status::MISS_NOT_FOUND;
      output.debug_info.cache_info.query_cost_ms = output.query_time_ms;
      break;
    case search_pipeline::CacheMissReason::kDisabled:
      output.debug_info.cache_info.status = query::CacheDebugInfo::Status::MISS_DISABLED;
      break;
  }
}

std::string SearchHandler::ExecuteSearchPipeline(const query::Query& query, ConnectionContext& conn_ctx,
                                                 PipelineOutput& output) {
  // Per-request latency: measure from handler entry through pipeline projection
  // so both cache-hit fast paths and cache-miss paths are observable. The
  // log event is emitted at DEBUG level — operators enable debug logging
  // when investigating slow queries; structured-log filtering by level keeps
  // production overhead minimal.
  const auto request_start = std::chrono::steady_clock::now();

  // Pre-flight: server must not be loading.
  if (auto err = CheckNotLoading(); !err.empty()) {
    return err;
  }
  if (auto err = CheckTableNotSyncing(query.table); !err.empty()) {
    return err;
  }

  // Resolve the table context required by both the search and any debug
  // bookkeeping that follows.
  auto table_ctx = GetTableContext(query.table);
  if (!table_ctx) {
    return ResponseFormatter::FormatError(table_ctx.error().message());
  }
  output.current_index = table_ctx->index;
  output.current_doc_store = table_ctx->doc_store;
  output.table_context = table_ctx->table_context;
  output.current_ngram_size = table_ctx->ngram_size;
  output.current_kanji_ngram_size = table_ctx->kanji_ngram_size;
  if (output.current_index == nullptr) {
    return ResponseFormatter::FormatError("Index not available");
  }
  output.current_cross_boundary = output.current_index->GetCrossBoundaryNgrams();

  // Delegate the actual search to the unified pipeline shared with HTTP.
  auto params = BuildPipelineParams(query, output);
  auto pipeline_output = search_pipeline::ExecuteFullPipeline(query, params);
  if (!pipeline_output.success) {
    return ResponseFormatter::FormatError(pipeline_output.error_message);
  }

  // Project pipeline output into PipelineOutput consumed by HandleSearch /
  // HandleCount.
  output.results = std::move(pipeline_output.results);
  output.all_search_terms = std::move(pipeline_output.all_search_terms);
  output.term_infos = std::move(pipeline_output.term_infos);
  output.query_time_ms = pipeline_output.query_time_ms;
  // Mirror the authoritative cache hit signal so the caller can branch
  // independently of debug_mode (debug_info is only populated when debug_mode
  // is true; relying on it for control flow caused cache hits to skip the
  // optimized early-return path in the common non-debug case).
  output.cache_hit = pipeline_output.cache_hit;

  // Populate debug_info if requested. The cache-hit path produces a different
  // set of debug fields than the cache-miss paths.
  if (conn_ctx.debug_mode) {
    if (pipeline_output.cache_hit) {
      PopulateCacheHitDebugInfo(pipeline_output, output);
    } else {
      PopulateInputDebugInfo(pipeline_output.path_taken, params, output);
      if (pipeline_output.empty_term_detected) {
        PopulateEmptyTermDebugInfo(pipeline_output.path_taken, pipeline_output, output);
      } else {
        PopulatePostPipelineDebugInfo(query, pipeline_output, output);
      }
    }
  }

  // Emit per-request structured log so operators can correlate cache_hit /
  // result_count with end-to-end latency. Done after success projection so
  // the fields are populated; the request_start timer captures any overhead
  // between entry and pipeline completion.
  const auto request_elapsed = std::chrono::steady_clock::now() - request_start;
  const double elapsed_ms = std::chrono::duration<double, std::milli>(request_elapsed).count();
  mygram::utils::StructuredLog()
      .Event("search_completed")
      .Field("table", query.table)
      .Field("query_time_ms", elapsed_ms)
      .Field("result_count", static_cast<int64_t>(output.results.size()))
      .Field("cache_hit", output.cache_hit)
      .Debug();

  return "";  // Success
}

std::vector<std::string> SearchHandler::GenerateHighlightSnippets(
    const query::Query& query, const PipelineOutput& output, const std::vector<storage::DocId>& paginated_results) {
  if (!query.highlight.has_value()) {
    return {};
  }

  const auto& hl_opts = query.highlight.value();

  const auto* synonym_dict = output.table_context != nullptr ? output.table_context->synonym_dict.get() : nullptr;
  auto normalized_terms =
      search_pipeline::BuildHighlightTerms(output.all_search_terms, output.current_index, synonym_dict);

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

  // Check for cache hit path - results are already final, just need pagination.
  // Use the authoritative cache_hit signal that is set regardless of debug
  // mode, instead of the debug_info status which is only populated when
  // conn_ctx.debug_mode == true.
  bool is_cache_hit = output.cache_hit;

  size_t total_results = output.results.size();

  std::string primary_key_column = "id";  // default
  if (output.table_context != nullptr) {
    primary_key_column = output.table_context->config.primary_key;
  }
  const bool is_score_sort = query.order_by.has_value() && query.order_by->IsScoreSort();

  if (conn_ctx.debug_mode) {
    query::OrderByClause order_by;
    bool order_by_implicit = false;
    if (query.order_by.has_value()) {
      order_by = query.order_by.value();
    } else {
      order_by.column = "";
      order_by.order = query::SortOrder::DESC;
      order_by_implicit = true;
    }

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

  auto topn = search_pipeline::ApplySearchTopNOptimization(query, output.current_index, output.term_infos, is_cache_hit,
                                                           primary_key_column, output.results);
  if (topn.applicable) {
    total_results = topn.total_results;
    if (conn_ctx.debug_mode) {
      if (topn.optimized) {
        output.debug_info.total_candidates = output.results.size();
        output.debug_info.after_intersection = output.results.size();
        std::string direction = topn.reverse ? "DESC" : "ASC";
        if (topn.single_ngram) {
          output.debug_info.optimization_used = "Index GetTopN (single-ngram + " + direction + " + limit)";
        } else {
          output.debug_info.optimization_used = "Index GetTopN (streaming intersection + " + direction + " + limit)";
        }
      } else if (topn.reused_existing) {
        output.debug_info.optimization_used = "reuse-fetch (small result set)";
      } else if (topn.no_results) {
        output.debug_info.optimization_used = "no results (optimization skipped)";
      }
    }
  }

  // Validate HIGHLIGHT is possible (requires stored normalized text)
  if (query.highlight.has_value() && output.current_doc_store != nullptr &&
      !output.current_doc_store->IsStoreTextsEnabled()) {
    return ResponseFormatter::FormatError(
        "HIGHLIGHT requires normalized text storage. Set memory.verify_text to \"ascii\" or \"all\" in configuration.");
  }

  // BM25 scoring: compute scores if SORT _score is requested
  if (is_score_sort && !output.results.empty()) {
    // Validate BM25 is enabled
    if (ctx_.full_config == nullptr || !ctx_.full_config->bm25.enable) {
      return ResponseFormatter::FormatError("SORT _score requires BM25 to be enabled in configuration");
    }

    const auto& bm25_config = ctx_.full_config->bm25;
    if (output.table_context == nullptr) {
      return ResponseFormatter::FormatError("table not found");
    }
    const auto& bm25_stats = output.table_context->bm25_stats;

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
    normalized_terms.reserve(output.term_infos.size());
    term_dfs.reserve(output.term_infos.size());
    for (size_t i = 0; i < output.term_infos.size(); ++i) {
      const auto& ti = output.term_infos[i];
      if (!ti.normalized_term.empty()) {
        normalized_terms.push_back(ti.normalized_term);
      } else if (i < output.all_search_terms.size()) {
        normalized_terms.push_back(output.current_index->NormalizeText(output.all_search_terms[i]));
      }
      term_dfs.push_back(ti.term_doc_freq);
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
      auto snippets = GenerateHighlightSnippets(query, output, sorted_results);
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
    return ResponseFormatter::FormatError(sorted_result.error().message());
  }

  auto sorted_results = std::move(sorted_result.value());

  if (query.highlight.has_value()) {
    auto snippets = GenerateHighlightSnippets(query, output, sorted_results);
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
