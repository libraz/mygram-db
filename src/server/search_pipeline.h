/**
 * @file search_pipeline.h
 * @brief Shared search pipeline - stateless functions for search execution
 *
 * Provides the core search logic (n-gram generation, intersection, NOT filter,
 * filter application, verify_text post-filter, cache insertion) as free functions
 * shared between the TCP SearchHandler and HTTP server.
 */

#pragma once

#include <set>
#include <string>
#include <vector>

#include "index/index.h"
#include "query/query_parser.h"
#include "storage/document_store.h"

// Forward declarations
namespace mygramdb::cache {
class CacheManager;
}  // namespace mygramdb::cache
namespace mygramdb::config {
struct Config;
}  // namespace mygramdb::config

namespace mygramdb::server {

/// @brief Term information with n-grams and estimated posting list size
struct SearchTermInfo {
  std::vector<std::string> ngrams;
  size_t estimated_size;
};

/// @brief Result of search pipeline execution (before pagination)
struct SearchPipelineResult {
  std::vector<storage::DocId> results;  ///< Full results before pagination
  bool empty_term_detected = false;     ///< True if any search term has zero matching docs
};

/// @brief Shared search pipeline - stateless functions for search execution
namespace search_pipeline {

/// @brief Generate n-grams for search terms and estimate result sizes
///
/// For each search term, normalizes text, generates n-grams (hybrid or fixed),
/// deduplicates them, and estimates the result size from the smallest posting list.
///
/// @param search_terms Search terms to process
/// @param current_index Index to use for estimation and normalization settings
/// @param ngram_size N-gram size for ASCII/alphanumeric characters
/// @param kanji_ngram_size N-gram size for CJK characters
/// @param cross_boundary_ngrams Generate n-grams spanning CJK/non-CJK boundaries
/// @return Vector of term information with n-grams and size estimates
std::vector<SearchTermInfo> GenerateTermInfos(const std::vector<std::string>& search_terms, index::Index* current_index,
                                              int ngram_size, int kanji_ngram_size, bool cross_boundary_ngrams);

/// @brief Execute the core search pipeline: intersection, NOT filter, filter application, verify_text
///
/// Performs the full search execution:
/// 1. Intersects posting lists for all search terms (AND semantics)
/// 2. Removes documents matching NOT terms
/// 3. Applies filter conditions (bitmap fast path when possible)
/// 4. Applies verify_text post-filter to eliminate n-gram false positives
///
/// @param query Parsed query with NOT terms and filters
/// @param term_infos Pre-generated term information (should be sorted by estimated_size)
/// @param all_search_terms All search terms (main + AND) for verify_text post-filter
/// @param current_index Index for search operations
/// @param current_doc_store Document store for filter evaluation
/// @param full_config Application config (for verify_text setting; may be nullptr)
/// @param ngram_size N-gram size for ASCII/alphanumeric characters
/// @param kanji_ngram_size N-gram size for CJK characters
/// @param cross_boundary Cross-boundary n-gram setting
/// @param filter_threshold Candidate count at or below which FilterByNgrams is used
/// @return Pipeline result with matching DocIDs
SearchPipelineResult Execute(const query::Query& query, const std::vector<SearchTermInfo>& term_infos,
                             const std::vector<std::string>& all_search_terms, index::Index* current_index,
                             storage::DocumentStore* current_doc_store, const config::Config* full_config,
                             int ngram_size, int kanji_ngram_size, bool cross_boundary, size_t filter_threshold);

/// @brief Apply NOT filter to results
///
/// Generates n-grams for each NOT term and removes matching documents
/// from the result set using SearchNot.
///
/// @param results Current result set
/// @param not_terms Terms to exclude
/// @param current_index Index for NOT search
/// @param ngram_size N-gram size for ASCII/alphanumeric characters
/// @param kanji_ngram_size N-gram size for CJK characters
/// @param cross_boundary_ngrams Generate n-grams spanning CJK/non-CJK boundaries
/// @return Filtered results with NOT-matching documents removed
std::vector<storage::DocId> ApplyNotFilter(const std::vector<storage::DocId>& results,
                                           const std::vector<std::string>& not_terms, index::Index* current_index,
                                           int ngram_size, int kanji_ngram_size, bool cross_boundary_ngrams);

/// @brief Apply filter conditions using bitmap intersection (fast path) with fallback
///
/// Uses Roaring bitmap intersection for EQ/NE filters when a bitmap index is available.
/// Falls back to per-document evaluation for unsupported operators (GT, GTE, LT, LTE).
///
/// @param results Current result set
/// @param filters Filter conditions to apply
/// @param doc_store Document store for filter value lookup
/// @return Filtered results matching all filter conditions
std::vector<storage::DocId> ApplyFiltersWithBitmap(const std::vector<storage::DocId>& results,
                                                   const std::vector<query::FilterCondition>& filters,
                                                   storage::DocumentStore* doc_store);

/// @brief Apply filter conditions per-document (fallback path)
///
/// Evaluates each filter condition against stored document values.
/// Supports all operators: EQ, NE, GT, GTE, LT, LTE.
///
/// @param results Current result set
/// @param filters Filter conditions to apply
/// @param doc_store Document store for filter value lookup
/// @return Filtered results matching all filter conditions
std::vector<storage::DocId> ApplyFilters(const std::vector<storage::DocId>& results,
                                         const std::vector<query::FilterCondition>& filters,
                                         storage::DocumentStore* doc_store);

/// @brief Post-filter candidates by verifying normalized text contains all search terms
///
/// Eliminates false positives from n-gram bitmap intersection by checking
/// that each candidate document's stored normalized text actually contains
/// all normalized search terms as substrings.
///
/// Documents whose normalized text is unavailable (e.g., after snapshot restore
/// where doc_texts_ is empty) are included to avoid false negatives.
///
/// @param candidates Candidate DocIDs from bitmap intersection
/// @param normalized_terms Normalized search terms to verify
/// @param doc_store Document store with normalized text
/// @return Verified DocIDs where all terms appear as substrings
std::vector<storage::DocId> PostFilterByText(const std::vector<storage::DocId>& candidates,
                                             const std::vector<std::string>& normalized_terms,
                                             storage::DocumentStore* doc_store);

/// @brief Apply verify_text post-filter based on config settings
///
/// Checks the verify_text config setting ("off", "ascii", "all") and applies
/// PostFilterByText when appropriate.
///
/// @param results Current result set
/// @param search_terms Original (non-normalized) search terms
/// @param current_index Index (for normalization settings)
/// @param doc_store Document store with normalized text
/// @param full_config Application config (for verify_text setting; may be nullptr)
/// @return Results after verify_text filtering
std::vector<storage::DocId> ApplyVerifyTextFilter(std::vector<storage::DocId> results,
                                                  const std::vector<std::string>& search_terms,
                                                  index::Index* current_index, storage::DocumentStore* doc_store,
                                                  const config::Config* full_config);

/// @brief Check if cached results contain stale DocIds by sampling
///
/// Validates a sample of cached DocIds against the document store.
/// If any sampled document no longer exists, the cache entry is considered stale.
///
/// @param results Cached result set
/// @param doc_store Document store for existence check
/// @return true if cache contains stale DocIds
bool IsCacheStale(const std::vector<storage::DocId>& results, storage::DocumentStore* doc_store);

/// @brief Insert search results into query cache
///
/// Collects all n-grams from term_infos and inserts the query result
/// into the cache manager for future lookups.
///
/// @param cache_manager Cache manager (may be nullptr; no-op if null or disabled)
/// @param query Parsed query (used as cache key)
/// @param results Search results to cache
/// @param term_infos Term information with n-grams (for invalidation tracking)
/// @param query_time_ms Query execution time in milliseconds
/// @param ngram_size N-gram size used for this query
/// @param kanji_ngram_size Kanji n-gram size used for this query
/// @param cross_boundary Cross-boundary setting used for this query
void InsertToCache(cache::CacheManager* cache_manager, const query::Query& query,
                   const std::vector<storage::DocId>& results, const std::vector<SearchTermInfo>& term_infos,
                   double query_time_ms, int ngram_size, int kanji_ngram_size, bool cross_boundary);

}  // namespace search_pipeline
}  // namespace mygramdb::server
