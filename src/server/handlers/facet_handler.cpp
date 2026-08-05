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
  if (auto err = CheckTableNotSyncing(query.table); !err.empty()) {
    return err;
  }

  // Get table context
  auto table_ctx = GetTableContext(query.table);
  if (!table_ctx) {
    return ResponseFormatter::FormatError(table_ctx.error());
  }
  auto* current_doc_store = table_ctx->doc_store;
  search_pipeline::FacetPipelineParams params;
  if (table_ctx->table_context != nullptr) {
    params.search = search_pipeline::BuildPipelineParamsFromContext(
        *table_ctx->table_context, ctx_.full_config, ctx_.cache_manager, SearchHandler::GetFilterThreshold(),
        /*attach_bm25_stats=*/true);
    for (const auto& filter : config::BuildUnifiedFilterConfigs(table_ctx->table_context->config)) {
      params.configured_filter_columns.push_back(filter.name);
    }
  } else {
    params.search.current_index = table_ctx->index;
    params.search.current_doc_store = current_doc_store;
    params.search.full_config = ctx_.full_config;
    params.search.cache_manager = ctx_.cache_manager;
    params.search.ngram_size = table_ctx->ngram_size;
    params.search.kanji_ngram_size = table_ctx->kanji_ngram_size;
    params.search.cross_boundary_ngrams = table_ctx->index != nullptr && table_ctx->index->GetCrossBoundaryNgrams();
    params.search.filter_threshold = SearchHandler::GetFilterThreshold();
  }
  params.load_in_progress = [this]() { return ctx_.dump_load_in_progress.load(std::memory_order_acquire); };

  auto facet_output = search_pipeline::ExecuteFacetPipeline(query, params);
  if (!facet_output) {
    return ResponseFormatter::FormatError(facet_output.error());
  }

  // Convert serialized values to display strings
  std::vector<std::pair<std::string, uint64_t>> display_counts;
  display_counts.reserve(facet_output->value_counts.size());
  for (auto& [serialized, count] : facet_output->value_counts) {
    display_counts.emplace_back(storage::FilterIndex::DeserializeToDisplayString(serialized), count);
  }

  if (conn_ctx.debug_mode) {
    query::DebugInfo debug_info;
    debug_info.query_time_ms = facet_output->query_time_ms;
    debug_info.total_candidates = facet_output->matched_documents;
    debug_info.final_results = facet_output->total_values;
    return ResponseFormatter::FormatFacetResponse(display_counts, facet_output->total_values, &debug_info);
  }

  return ResponseFormatter::FormatFacetResponse(display_counts, facet_output->total_values);
}

}  // namespace mygramdb::server
