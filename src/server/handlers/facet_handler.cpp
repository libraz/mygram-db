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

#include "server/search_pipeline.h"
#include "storage/filter_index.h"
#include "utils/string_utils.h"

namespace mygramdb::server {

std::string FacetHandler::Handle(const query::Query& query, ConnectionContext& conn_ctx) {
  if (ctx_.dump_load_in_progress) {
    return ResponseFormatter::FormatError("Server is loading, please try again later");
  }

  // Get table context
  index::Index* current_index = nullptr;
  storage::DocumentStore* current_doc_store = nullptr;
  int ngram_size = 0;
  int kanji_ngram_size = 0;
  std::string error =
      GetTableContext(query.table, &current_index, &current_doc_store, &ngram_size, &kanji_ngram_size);
  if (!error.empty()) {
    return error;
  }

  auto start_time = std::chrono::high_resolution_clock::now();

  // Get filter index snapshot
  auto filter_index = current_doc_store->GetFilterIndex();
  if (!filter_index) {
    return ResponseFormatter::FormatError("Filter index not available");
  }

  std::vector<std::pair<std::string, uint64_t>> value_counts;

  if (!query.search_text.empty() || !query.filters.empty()) {
    // Need to restrict facet to a subset of documents
    std::vector<storage::DocId> results;

    if (!query.search_text.empty()) {
      // Run search pipeline
      std::vector<std::string> all_search_terms;
      all_search_terms.push_back(query.search_text);
      all_search_terms.insert(all_search_terms.end(), query.and_terms.begin(), query.and_terms.end());

      bool cross_boundary = current_index->GetCrossBoundaryNgrams();
      auto term_infos =
          search_pipeline::GenerateTermInfos(all_search_terms, current_index, ngram_size, kanji_ngram_size,
                                             cross_boundary);

      // Sort by estimated size for faster intersection
      std::sort(term_infos.begin(), term_infos.end(), [](const SearchTermInfo& a, const SearchTermInfo& b) {
        return a.estimated_size < b.estimated_size;
      });

      // NOLINTBEGIN(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
      constexpr size_t kDefaultFilterThreshold = 1000;
      // NOLINTEND(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
      auto pipeline_result =
          search_pipeline::Execute(query, term_infos, all_search_terms, current_index, current_doc_store,
                                   ctx_.full_config, ngram_size, kanji_ngram_size, cross_boundary,
                                   kDefaultFilterThreshold);
      results = std::move(pipeline_result.results);
    } else {
      // No search text, but has filters — start with all docs
      results = current_doc_store->GetAllDocIds();

      // Apply filters
      if (!query.filters.empty()) {
        results = search_pipeline::ApplyFiltersWithBitmap(results, query.filters, current_doc_store);
      }
    }

    // Apply NOT filter if present (for filter-only path)
    if (query.search_text.empty() && !query.not_terms.empty()) {
      bool cross_boundary = current_index->GetCrossBoundaryNgrams();
      results = search_pipeline::ApplyNotFilter(results, query.not_terms, current_index, ngram_size,
                                                kanji_ngram_size, cross_boundary);
    }

    if (results.empty()) {
      value_counts = {};
    } else {
      // Convert results to Roaring bitmap for efficient facet counting
      // Use unique_ptr with custom deleter for RAII
      std::unique_ptr<roaring_bitmap_t, decltype(&roaring_bitmap_free)> result_bitmap(
          roaring_bitmap_create(), roaring_bitmap_free);
      roaring_bitmap_add_many(result_bitmap.get(), results.size(), results.data());

      value_counts = filter_index->GetColumnValueCountsFiltered(query.facet_column, result_bitmap.get());
    }
  } else {
    // Facet all documents (no search, no filters)
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
