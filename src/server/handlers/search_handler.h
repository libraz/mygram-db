/**
 * @file search_handler.h
 * @brief Handler for SEARCH and COUNT commands
 */

#pragma once

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
  static void SetFilterThreshold(size_t threshold) { filter_threshold_ = threshold; }

  /**
   * @brief Get the current filter threshold
   * @return Current threshold value
   */
  static size_t GetFilterThreshold() { return filter_threshold_; }

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
  static inline size_t filter_threshold_ = 1000;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

  /**
   * @brief Handle SEARCH query
   */
  std::string HandleSearch(const query::Query& query, ConnectionContext& conn_ctx);

  /**
   * @brief Handle COUNT query
   */
  std::string HandleCount(const query::Query& query, ConnectionContext& conn_ctx);
};

}  // namespace mygramdb::server
