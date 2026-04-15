/**
 * @file facet_handler.cpp
 * @brief Handler for FACET command
 */

#include "server/handlers/facet_handler.h"

#include <roaring/roaring.h>

#include <algorithm>
#include <chrono>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "server/handlers/search_handler.h"
#include "server/search_pipeline.h"
#include "storage/filter_index.h"
#include "utils/string_utils.h"

namespace mygramdb::server {

std::string FacetHandler::Handle(const query::Query& query, ConnectionContext& conn_ctx) {
  if (ctx_.dump_load_in_progress) {
    return ResponseFormatter::FormatError("Server is loading, please try again later");
  }

  // Get table context
  auto table_ctx = GetTableContext(query.table);
  if (!table_ctx) {
    return ResponseFormatter::FormatError(table_ctx.error().message());
  }
  auto* current_index = table_ctx->index;
  auto* current_doc_store = table_ctx->doc_store;
  int ngram_size = table_ctx->ngram_size;
  int kanji_ngram_size = table_ctx->kanji_ngram_size;

  if (current_doc_store == nullptr) {
    return ResponseFormatter::FormatError("Document store not available");
  }

  auto start_time = std::chrono::high_resolution_clock::now();

  // Get filter index snapshot
  auto filter_index = current_doc_store->GetFilterIndex();
  if (!filter_index) {
    return ResponseFormatter::FormatError("Filter index not available");
  }

  // EDGE-5: Re-check dump_load_in_progress after snapshot
  if (ctx_.dump_load_in_progress) {
    return ResponseFormatter::FormatError("Server is loading, please try again later");
  }

  std::vector<std::pair<std::string, uint64_t>> value_counts;

  bool has_search = !query.search_text.empty() || !query.and_terms.empty();
  bool has_not = !query.not_terms.empty();
  bool has_filters = !query.filters.empty();

  if (has_search || has_not || has_filters) {
    // Need to restrict facet to a subset of documents
    std::vector<storage::DocId> results;
    bool cross_boundary = current_index->GetCrossBoundaryNgrams();

    if (has_search) {
      // Run search pipeline (handles search_text + and_terms + NOT + filters)
      std::vector<std::string> all_search_terms;
      if (!query.search_text.empty()) {
        all_search_terms.push_back(query.search_text);
      }
      all_search_terms.insert(all_search_terms.end(), query.and_terms.begin(), query.and_terms.end());

      auto term_infos = search_pipeline::GenerateTermInfos(all_search_terms, current_index, ngram_size,
                                                           kanji_ngram_size, cross_boundary);

      // Sort by estimated size for faster intersection
      std::sort(term_infos.begin(), term_infos.end(),
                [](const SearchTermInfo& a, const SearchTermInfo& b) { return a.estimated_size < b.estimated_size; });

      auto pipeline_result = search_pipeline::Execute(query, term_infos, all_search_terms, current_index,
                                                      current_doc_store, ctx_.full_config, ngram_size, kanji_ngram_size,
                                                      cross_boundary, SearchHandler::GetFilterThreshold());
      results = std::move(pipeline_result.results);
    } else {
      // No search text and no and_terms — start with all docs
      results = current_doc_store->GetAllDocIds();

      // Apply NOT filter (only for non-search path; Execute already handles it)
      if (has_not) {
        results = search_pipeline::ApplyNotFilter(results, query.not_terms, current_index, ngram_size, kanji_ngram_size,
                                                  cross_boundary);
      }

      // Apply column filters (only for non-search path; Execute already handles it)
      if (has_filters) {
        results = search_pipeline::ApplyFiltersWithBitmap(results, query.filters, current_doc_store);
      }
    }

    if (results.empty()) {
      value_counts = {};
    } else {
      // Convert results to Roaring bitmap for efficient facet counting
      // Use unique_ptr with custom deleter for RAII
      std::unique_ptr<roaring_bitmap_t, decltype(&roaring_bitmap_free)> result_bitmap(roaring_bitmap_create(),
                                                                                      roaring_bitmap_free);
      roaring_bitmap_add_many(result_bitmap.get(), results.size(), results.data());

      // Free the results vector before bitmap operations (reduce peak memory)
      results.clear();
      results.shrink_to_fit();

      value_counts = filter_index->GetColumnValueCountsFiltered(query.facet_column, result_bitmap.get());
    }
  } else {
    // Facet all documents (no search, no filters, no NOT)
    value_counts = filter_index->GetColumnValueCounts(query.facet_column);
  }

  // Apply LIMIT
  if (query.limit_explicit && value_counts.size() > query.limit) {
    value_counts.resize(query.limit);
  }

  // Convert serialized values to display strings
  std::vector<std::pair<std::string, uint64_t>> display_counts;
  display_counts.reserve(value_counts.size());
  for (auto& [serialized, count] : value_counts) {
    display_counts.emplace_back(storage::FilterIndex::DeserializeToDisplayString(serialized), count);
  }

  auto end_time = std::chrono::high_resolution_clock::now();
  double query_time_ms = std::chrono::duration<double, std::milli>(end_time - start_time).count();

  if (conn_ctx.debug_mode) {
    query::DebugInfo debug_info;
    debug_info.query_time_ms = query_time_ms;
    debug_info.final_results = display_counts.size();
    return ResponseFormatter::FormatFacetResponse(display_counts, &debug_info);
  }

  return ResponseFormatter::FormatFacetResponse(display_counts);
}

}  // namespace mygramdb::server
