/**
 * @file statistics_service.cpp
 * @brief Implementation of StatisticsService
 */

#include "server/statistics_service.h"

#include "index/index.h"
#include "storage/document_store.h"

namespace mygramdb::server {

AggregatedMetrics StatisticsService::AggregateMetrics(const std::unordered_map<std::string, TableContext*>& tables) {
  AggregatedMetrics metrics;

  for (const auto& [table_name, ctx] : tables) {
    // Aggregate memory usage
    metrics.total_index_memory += ctx->index->MemoryUsage();
    metrics.total_doc_memory += ctx->doc_store->MemoryUsage();

    // Aggregate document count
    metrics.total_documents += ctx->doc_store->Size();

    // Aggregate index statistics
    auto idx_stats = ctx->index->GetStatistics();
    metrics.total_terms += idx_stats.total_terms;
    metrics.total_postings += idx_stats.total_postings;
    metrics.total_delta_encoded += idx_stats.delta_encoded_lists;
    metrics.total_roaring_bitmap += idx_stats.roaring_bitmap_lists;

    // Check optimization status
    if (ctx->index->IsOptimizing()) {
      metrics.any_table_optimizing = true;
    }
  }

  // Compute total memory
  metrics.total_memory = metrics.total_index_memory + metrics.total_doc_memory;

  return metrics;
}

void StatisticsService::UpdateServerStatistics(ServerStats& stats, const AggregatedMetrics& metrics) {
  // Update memory usage statistics
  stats.UpdateMemoryUsage(metrics.total_memory);
}

}  // namespace mygramdb::server
