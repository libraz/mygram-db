/**
 * @file statistics_service.h
 * @brief Service for aggregating and managing server statistics
 */

#pragma once

#include <chrono>
#include <cstddef>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>

#include "index/index.h"
#include "server/server_stats.h"
#include "server/server_types.h"
#include "storage/document_store.h"

namespace mygramdb::server {

struct TableAggregatedMetrics {
  size_t index_memory = 0;
  size_t document_memory = 0;
  size_t documents = 0;
  size_t terms = 0;
  size_t postings = 0;
  size_t delta_encoded = 0;
  size_t roaring_bitmap = 0;
  bool optimizing = false;
};

/**
 * @brief Aggregated metrics across all tables
 *
 * This struct contains computed metrics from all table contexts.
 * It separates state aggregation (domain logic) from presentation (formatting).
 */
struct AggregatedMetrics {
  std::unordered_map<std::string, TableAggregatedMetrics> tables;

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

/**
 * @brief Short-lived shared snapshot for expensive INFO and /metrics scans.
 *
 * DocumentStore::MemoryUsage() traverses retained document data and is not a
 * per-request primitive. This cache serializes refreshes and lets INFO and
 * Prometheus scrapes reuse the same table snapshot for a bounded interval.
 *
 * The interval is what bounds cost, not its length: a one-second floor already
 * caps a polling client at one walk per second whatever its request rate, and
 * every further second buys no protection while making the reported figure
 * older. Memory accounting is read to decide whether a server is close to its
 * limit, so a stale answer is a wrong answer.
 */
class StatisticsSnapshotCache {
 public:
  explicit StatisticsSnapshotCache(std::chrono::steady_clock::duration max_age = std::chrono::seconds(1))
      : max_age_(max_age) {}

  template <typename MapT>
  AggregatedMetrics Get(const MapT& tables,
                        std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now()) {
    std::lock_guard lock(mutex_);
    if (!snapshot_.has_value() || now - captured_at_ >= max_age_) {
      snapshot_ = StatisticsService::AggregateMetrics(tables);
      captured_at_ = now;
    }
    return *snapshot_;
  }

 private:
  std::mutex mutex_;
  std::optional<AggregatedMetrics> snapshot_;
  std::chrono::steady_clock::time_point captured_at_{};
  std::chrono::steady_clock::duration max_age_;
};

template <typename MapT>
AggregatedMetrics StatisticsService::AggregateMetrics(const MapT& tables) {
  AggregatedMetrics metrics;

  for (const auto& [table_name, ctx] : tables) {
    std::shared_lock<std::shared_mutex> generation_lock(*ctx->generation_mutex);
    TableAggregatedMetrics table_metrics;
    table_metrics.index_memory = ctx->index->MemoryUsage();
    table_metrics.document_memory = ctx->doc_store->MemoryUsage();
    table_metrics.documents = ctx->doc_store->Size();

    auto idx_stats = ctx->index->GetStatistics();
    table_metrics.terms = idx_stats.total_terms;
    table_metrics.postings = idx_stats.total_postings;
    table_metrics.delta_encoded = idx_stats.delta_encoded_lists;
    table_metrics.roaring_bitmap = idx_stats.roaring_bitmap_lists;
    table_metrics.optimizing = ctx->index->IsOptimizing();

    metrics.total_index_memory += table_metrics.index_memory;
    metrics.total_doc_memory += table_metrics.document_memory;
    metrics.total_documents += table_metrics.documents;
    metrics.total_terms += table_metrics.terms;
    metrics.total_postings += table_metrics.postings;
    metrics.total_delta_encoded += table_metrics.delta_encoded;
    metrics.total_roaring_bitmap += table_metrics.roaring_bitmap;
    metrics.any_table_optimizing = metrics.any_table_optimizing || table_metrics.optimizing;
    metrics.tables.emplace(table_name, std::move(table_metrics));
  }

  metrics.total_memory = metrics.total_index_memory + metrics.total_doc_memory;
  return metrics;
}

}  // namespace mygramdb::server
