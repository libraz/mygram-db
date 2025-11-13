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

 private:
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
                                                 query::DebugInfo* debug_info);

  /**
   * @brief Apply NOT filter to results
   */
  static std::vector<storage::DocId> ApplyNotFilter(const std::vector<storage::DocId>& results,
                                                    const std::vector<std::string>& not_terms,
                                                    index::Index* current_index, int ngram_size, int kanji_ngram_size);

  /**
   * @brief Apply filter conditions to results
   */
  static std::vector<storage::DocId> ApplyFilters(const std::vector<storage::DocId>& results,
                                                  const std::vector<query::FilterCondition>& filters,
                                                  storage::DocumentStore* doc_store);
};

}  // namespace mygramdb::server
