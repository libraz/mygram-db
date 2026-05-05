/**
 * @file search_handler.h
 * @brief Handler for SEARCH and COUNT commands
 */

#pragma once

#include <atomic>
#include <string>
#include <vector>

#include "server/handlers/command_handler.h"
#include "server/search_pipeline.h"

namespace mygramdb::server {

/**
 * @brief Handler for SEARCH and COUNT queries
 *
 * Handles full-text search with n-gram generation, optimization,
 * filtering, sorting, and pagination. Delegates core search logic
 * to the shared search_pipeline functions.
 */
class SearchHandler : public CommandHandler {
 public:
  explicit SearchHandler(HandlerContext& ctx) : CommandHandler(ctx) {}

  std::string Handle(const query::Query& query, ConnectionContext& conn_ctx) override;

  /**
   * @brief Set the FilterByNgrams/SearchAnd threshold
   * @param threshold Candidate count at or below which FilterByNgrams is used
   */
  static void SetFilterThreshold(size_t threshold) { filter_threshold_.store(threshold, std::memory_order_relaxed); }

  /**
   * @brief Get the current filter threshold
   * @return Current threshold value
   */
  static size_t GetFilterThreshold() { return filter_threshold_.load(std::memory_order_relaxed); }

  /**
   * @brief Post-filter candidates by verifying normalized text contains all search terms
   *
   * Forwards to search_pipeline::PostFilterByText for backward compatibility.
   *
   * @param candidates Candidate DocIDs from bitmap intersection
   * @param normalized_terms Normalized search terms to verify
   * @param doc_store Document store with normalized text
   * @return Verified DocIDs where all terms appear as substrings
   */
  static std::vector<storage::DocId> PostFilterByText(const std::vector<storage::DocId>& candidates,
                                                      const std::vector<std::string>& normalized_terms,
                                                      storage::DocumentStore* doc_store) {
    return search_pipeline::PostFilterByText(candidates, normalized_terms, doc_store);
  }

 private:
  /// Candidate count threshold: at or below this, use FilterByNgrams; above, use full SearchAnd intersection
  static inline std::atomic<size_t> filter_threshold_{
      1000};  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

  /**
   * @brief Result from ExecuteSearchPipeline
   */
  struct PipelineOutput {
    std::vector<storage::DocId> results;        ///< Full result set (before pagination)
    std::vector<std::string> all_search_terms;  ///< All search terms (main + AND)
    std::vector<SearchTermInfo> term_infos;     ///< Term information with n-grams
    double query_time_ms = 0.0;                 ///< Query execution time
    query::DebugInfo debug_info;                ///< Debug info (populated when debug_mode)
    index::Index* current_index = nullptr;
    storage::DocumentStore* current_doc_store = nullptr;
    int current_ngram_size = 0;
    int current_kanji_ngram_size = 0;
    bool current_cross_boundary = false;
  };

  /**
   * @brief Handle SEARCH query
   */
  std::string HandleSearch(const query::Query& query, ConnectionContext& conn_ctx);

  /**
   * @brief Handle COUNT query
   */
  std::string HandleCount(const query::Query& query, ConnectionContext& conn_ctx);

  /**
   * @brief Execute the shared search pipeline (term generation through verify_text filter)
   *
   * Delegates to search_pipeline::ExecuteFullPipeline so the TCP and HTTP code
   * paths share the same orchestration. This wrapper only adds debug-info
   * bookkeeping that the TCP debug protocol exposes; the underlying search
   * execution (cache lookup, fuzzy/synonym/standard paths, NOT/filter/verify
   * application, cache insertion) lives entirely in search_pipeline.
   *
   * @param query Parsed query
   * @param conn_ctx Connection context (for debug mode)
   * @param[out] output Pipeline output
   * @return Empty string on success, or error response string on failure
   */
  std::string ExecuteSearchPipeline(const query::Query& query, ConnectionContext& conn_ctx, PipelineOutput& output);

  /**
   * @brief Build FullPipelineParams from the connection's table context.
   *
   * Pure projection from PipelineOutput's table-context fields plus the
   * shared HandlerContext members onto the params struct consumed by
   * search_pipeline::ExecuteFullPipeline.
   */
  search_pipeline::FullPipelineParams BuildPipelineParams(const query::Query& query,
                                                          const PipelineOutput& output) const;

  /**
   * @brief Populate debug_info entries that describe the search inputs.
   *
   * Records `search_terms`, `ngrams_used`, and `posting_list_sizes` for the
   * fuzzy/synonym/regular paths. Synonym variant ngrams are recovered by a
   * second call to ExpandTermsWithSynonyms because FullPipelineOutput does
   * not expose them; this only happens in debug mode and so does not affect
   * non-debug latency.
   */
  static void PopulateInputDebugInfo(search_pipeline::PipelinePath path_taken,
                                     const search_pipeline::FullPipelineParams& params, PipelineOutput& output);

  /**
   * @brief Populate debug_info for the cache-hit path.
   */
  static void PopulateCacheHitDebugInfo(const search_pipeline::FullPipelineOutput& pipeline_output,
                                        PipelineOutput& output);

  /**
   * @brief Populate debug_info for empty-term early exit (per path).
   */
  static void PopulateEmptyTermDebugInfo(search_pipeline::PipelinePath path_taken,
                                         const search_pipeline::FullPipelineOutput& pipeline_output,
                                         PipelineOutput& output);

  /**
   * @brief Populate debug_info for a successful cache-miss path (per path).
   */
  void PopulatePostPipelineDebugInfo(const query::Query& query,
                                     const search_pipeline::FullPipelineOutput& pipeline_output,
                                     PipelineOutput& output) const;
};

}  // namespace mygramdb::server
