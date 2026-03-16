/**
 * @file search_handler.h
 * @brief Handler for SEARCH and COUNT commands
 */

#pragma once

#include <string>
#include <vector>

#include "server/handlers/command_handler.h"

namespace mygramdb::server {

/**
 * @brief Handler for SEARCH and COUNT queries
 *
 * Handles full-text search with n-gram generation, optimization,
 * filtering, sorting, and pagination.
 */
class SearchHandler : public CommandHandler {
 public:
  explicit SearchHandler(HandlerContext& ctx) : CommandHandler(ctx) {}

  std::string Handle(const query::Query& query, ConnectionContext& conn_ctx) override;

  /**
   * @brief Set the FilterByNgrams/SearchAnd threshold
   * @param threshold Candidate count at or below which FilterByNgrams is used
   */
  static void SetFilterThreshold(size_t threshold) { filter_threshold_ = threshold; }

  /**
   * @brief Get the current filter threshold
   * @return Current threshold value
   */
  static size_t GetFilterThreshold() { return filter_threshold_; }

 private:
  /// Candidate count threshold: at or below this, use FilterByNgrams; above, use full SearchAnd intersection
  static inline size_t filter_threshold_ = 1000;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
  /**
   * @brief Internal structure for term information
   */
  struct TermInfo {
    std::vector<std::string> ngrams;
    size_t estimated_size;
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
   * @brief Generate n-grams for search terms
   * @param search_terms Search terms to process
   * @param current_index Index to use for estimation
   * @param ngram_size N-gram size
   * @param kanji_ngram_size Kanji n-gram size
   * @param debug_info Optional debug info to populate
   * @return Vector of term information with n-grams and size estimates
   */
  static std::vector<TermInfo> GenerateTermInfos(const std::vector<std::string>& search_terms,
                                                 index::Index* current_index, int ngram_size, int kanji_ngram_size,
                                                 query::DebugInfo* debug_info, bool cross_boundary_ngrams = true);

  /**
   * @brief Apply NOT filter to results
   */
  static std::vector<storage::DocId> ApplyNotFilter(const std::vector<storage::DocId>& results,
                                                    const std::vector<std::string>& not_terms,
                                                    index::Index* current_index, int ngram_size, int kanji_ngram_size,
                                                    bool cross_boundary_ngrams = true);

  /**
   * @brief Apply filter conditions to results
   */
  static std::vector<storage::DocId> ApplyFilters(const std::vector<storage::DocId>& results,
                                                  const std::vector<query::FilterCondition>& filters,
                                                  storage::DocumentStore* doc_store);

  /**
   * @brief Apply filter conditions using bitmap intersection (fast path)
   *
   * Converts results to a Roaring bitmap and intersects with pre-built
   * filter bitmaps. Falls back to ApplyFilters for unsupported operators.
   */
  static std::vector<storage::DocId> ApplyFiltersWithBitmap(const std::vector<storage::DocId>& results,
                                                             const std::vector<query::FilterCondition>& filters,
                                                             storage::DocumentStore* doc_store);

  /// Check if all filter conditions can be accelerated with bitmap index
  static bool AllFiltersHaveBitmapSupport(const std::vector<query::FilterCondition>& filters,
                                          storage::DocumentStore* doc_store);
};

}  // namespace mygramdb::server
