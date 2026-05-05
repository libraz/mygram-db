/**
 * @file facet_handler.cpp
 * @brief Handler for FACET command
 */

#include "server/handlers/facet_handler.h"

#include <algorithm>
#include <chrono>
#include <string>
#include <utility>
#include <vector>

#include "query/synonym_dictionary.h"
#include "server/handlers/search_handler.h"
#include "server/search_pipeline.h"
#include "server/table_catalog.h"
#include "storage/filter_index.h"
#include "utils/roaring_bitmap_ptr.h"
#include "utils/string_utils.h"

namespace mygramdb::server {

std::string FacetHandler::Handle(const query::Query& query, ConnectionContext& conn_ctx) {
  if (auto err = CheckNotLoading(); !err.empty()) {
    return err;
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

  // EDGE-5: Re-check dump_load_in_progress after snapshot.
  //
  // Re-check after acquiring resources is intentional defensive: the
  // `dump_load_in_progress` flag could flip during the resource acquisition
  // window between the first CheckNotLoading() at the top of Handle() and
  // here. Even though the load operation is largely synchronous, this guard
  // ensures we never proceed past resource setup if a load began in the
  // meantime. The check is a single relaxed atomic load -- cheap. Keep
  // both calls; do not "deduplicate" them.
  if (auto err = CheckNotLoading(); !err.empty()) {
    return err;
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
      // Use ExecuteFullPipeline so synonym expansion, fuzzy matching, and
      // result caching apply identically to facet-scoped searches. Mismatched
      // semantics between SEARCH and FACET on the same query was a documented
      // inconsistency (FACET previously called search_pipeline::Execute
      // directly and bypassed the cache and synonym/fuzzy paths entirely).
      //
      // Facets benefit from the search-result cache: keep skip_cache_lookup
      // false (default) so FACET and SEARCH share cached posting-list
      // resolutions.
      auto* facet_table_ctx = ctx_.table_catalog ? ctx_.table_catalog->GetTable(query.table) : nullptr;
      search_pipeline::FullPipelineParams params;
      if (facet_table_ctx != nullptr) {
        params = search_pipeline::BuildPipelineParamsFromContext(*facet_table_ctx, ctx_.full_config, ctx_.cache_manager,
                                                                 SearchHandler::GetFilterThreshold(),
                                                                 /*attach_bm25_stats=*/true);
      } else {
        // Defensive fallback: catalog evicted the entry between GetTable and
        // here. Wire the locals so the pipeline can still execute.
        params.current_index = current_index;
        params.current_doc_store = current_doc_store;
        params.full_config = ctx_.full_config;
        params.cache_manager = ctx_.cache_manager;
        params.ngram_size = ngram_size;
        params.kanji_ngram_size = kanji_ngram_size;
        params.cross_boundary_ngrams = cross_boundary;
        params.filter_threshold = SearchHandler::GetFilterThreshold();
      }

      auto pipeline_output = search_pipeline::ExecuteFullPipeline(query, params);
      if (!pipeline_output.success) {
        return ResponseFormatter::FormatError(pipeline_output.error_message);
      }
      results = std::move(pipeline_output.results);
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
      // Convert results to Roaring bitmap for efficient facet counting using
      // the shared RAII helper. Note: this duplicates the bitmap-construction
      // step performed inside ExecuteFullPipeline; Phase 3 will eliminate the
      // duplication once the pipeline exposes its working bitmap via the
      // output struct.
      auto result_bitmap = mygram::utils::MakeRoaringFromVector(results);

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
