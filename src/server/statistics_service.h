/**
 * @file statistics_service.h
 * @brief Service for aggregating and managing server statistics
 */

#pragma once

#include <cstddef>
#include <string>
#include <unordered_map>

#include "index/index.h"
#include "server/server_stats.h"
#include "server/server_types.h"
#include "storage/document_store.h"

namespace mygramdb::server {

/**
 * @brief Aggregated metrics across all tables
 *
 * This struct contains computed metrics from all table contexts.
 * It separates state aggregation (domain logic) from presentation (formatting).
 */
struct AggregatedMetrics {
  // Memory metrics
  size_t total_index_memory = 0;
  size_t total_doc_memory = 0;
  size_t total_memory = 0;

  // Index metrics
  size_t total_documents = 0;
  size_t total_terms = 0;
  size_t total_postings = 0;
  size_t total_delta_encoded = 0;
  size_t total_roaring_bitmap = 0;

  // Optimization status
  bool any_table_optimizing = false;
};

/**
 * @brief Service for aggregating and managing server statistics
 *
 * This service separates domain logic (metric aggregation and state updates)
 * from presentation logic (response formatting). It provides a clean layer
 * boundary and eliminates side effects in the presentation layer.
 *
 * Key responsibilities:
 * - Aggregate metrics across all table contexts
 * - Update server statistics with computed metrics
 * - Provide reusable aggregation logic for different output formats
 *
 * Design principles:
 * - Static methods (stateless service)
 * - Pure functions for aggregation (no side effects)
 * - Explicit state mutation methods
 */
class StatisticsService {
 public:
  /**
   * @brief Aggregate metrics across all tables
   *
   * This method computes aggregated metrics from table contexts.
   * It is a pure function with no side effects.
   *
   * @param tables Map of table contexts
   * @return Aggregated metrics
   */
  /// Accepts any associative container mapping std::string → TableContext*
  /// (std::unordered_map, absl::flat_hash_map, etc.).
  template <typename MapT>
  static AggregatedMetrics AggregateMetrics(const MapT& tables);

  /**
   * @brief Update server statistics with aggregated metrics
   *
   * This method explicitly updates server statistics.
   * Side effects are localized to this method.
   *
   * @param stats Server statistics to update
   * @param metrics Pre-computed aggregated metrics
   */
  static void UpdateServerStatistics(ServerStats& stats, const AggregatedMetrics& metrics);
};

template <typename MapT>
AggregatedMetrics StatisticsService::AggregateMetrics(const MapT& tables) {
  AggregatedMetrics metrics;

  for (const auto& [table_name, ctx] : tables) {
    metrics.total_index_memory += ctx->index->MemoryUsage();
    metrics.total_doc_memory += ctx->doc_store->MemoryUsage();
    metrics.total_documents += ctx->doc_store->Size();

    auto idx_stats = ctx->index->GetStatistics();
    metrics.total_terms += idx_stats.total_terms;
    metrics.total_postings += idx_stats.total_postings;
    metrics.total_delta_encoded += idx_stats.delta_encoded_lists;
    metrics.total_roaring_bitmap += idx_stats.roaring_bitmap_lists;

    if (ctx->index->IsOptimizing()) {
      metrics.any_table_optimizing = true;
    }
  }

  metrics.total_memory = metrics.total_index_memory + metrics.total_doc_memory;
  return metrics;
}

}  // namespace mygramdb::server
