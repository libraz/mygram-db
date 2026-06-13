/**
 * @file cache_manager.h
 * @brief Unified cache manager integrating all cache components
 */

#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "cache/cache_types.h"
#include "cache/invalidation_manager.h"
#include "cache/invalidation_queue.h"
#include "cache/query_cache.h"
#include "config/config.h"
#include "query/query_normalizer.h"
#include "query/query_parser.h"

namespace mygramdb::cache {

/**
 * @brief Cache lookup result with metadata
 */
struct CacheLookupResult {
  std::vector<DocId> results;                        ///< Cached search results
  double query_cost_ms = 0.0;                        ///< Original query execution time
  std::chrono::steady_clock::time_point created_at;  ///< When cache entry was created
};

/**
 * @brief Unified cache manager
 *
 * Integrates QueryCache, InvalidationManager, and InvalidationQueue
 * to provide a simple API for caching and invalidation.
 */
class CacheManager {
 public:
  /**
   * @brief Constructor
   * @param cache_config Cache configuration
   * @param ngram_configs Per-table N-gram configuration for cache invalidation
   */
  CacheManager(const config::CacheConfig& cache_config, NgramConfigMap ngram_configs);

  /**
   * @brief Destructor
   */
  ~CacheManager();

  // Non-copyable, non-movable
  CacheManager(const CacheManager&) = delete;
  CacheManager& operator=(const CacheManager&) = delete;
  CacheManager(CacheManager&&) = delete;
  CacheManager& operator=(CacheManager&&) = delete;

  /**
   * @brief Check if cache is enabled
   */
  [[nodiscard]] bool IsEnabled() const { return enabled_; }

  /**
   * @brief Lookup cached query result
   * @param query Parsed query
   * @return Cached result if found and valid, nullopt otherwise
   */
  [[nodiscard]] std::optional<std::vector<DocId>> Lookup(const query::Query& query);

  /**
   * @brief Lookup cached query result with metadata
   * @param query Parsed query
   * @return Cached result with metadata if found and valid, nullopt otherwise
   */
  [[nodiscard]] std::optional<CacheLookupResult> LookupWithMetadata(const query::Query& query);

  /**
   * @brief Insert query result into cache
   * @param query Parsed query
   * @param result Search result
   * @param ngrams Ngrams used in query (for invalidation tracking)
   * @param query_cost_ms Query execution time
   * @param ngram_size N-gram size used for this query (for invalidation consistency)
   * @param kanji_ngram_size Kanji N-gram size used for this query
   * @param cross_boundary_ngrams Cross-boundary setting used for this query
   * @return true if cached, false otherwise
   */
  bool Insert(const query::Query& query, const std::vector<DocId>& result, const std::vector<std::string>& ngrams,
              double query_cost_ms, int ngram_size = 0, int kanji_ngram_size = 0, bool cross_boundary_ngrams = true);

  /**
   * @brief Capture the current data-change generation for guarded cache inserts
   */
  [[nodiscard]] uint64_t CaptureDataVersion() const { return data_version_.load(std::memory_order_acquire); }
  [[nodiscard]] uint64_t CaptureDataVersion(const std::string& table_name) const;

  /**
   * @brief Insert only if no data invalidation/clear occurred since expected_data_version was captured
   */
  bool InsertIfVersion(const query::Query& query, const std::vector<DocId>& result,
                       const std::vector<std::string>& ngrams, double query_cost_ms, uint64_t expected_data_version,
                       int ngram_size = 0, int kanji_ngram_size = 0, bool cross_boundary_ngrams = true);

  /**
   * @brief Invalidate cache entries affected by data modification
   * @param table_name Table that was modified
   * @param old_text Previous text content (empty if INSERT)
   * @param new_text New text content (empty if DELETE)
   */
  void Invalidate(const std::string& table_name, const std::string& old_text, const std::string& new_text,
                  bool filter_columns_changed = false);

  /**
   * @brief Clear all cache entries
   */
  void Clear();

  /**
   * @brief Clear cache entries for specific table
   * @param table_name Table name
   */
  void ClearTable(const std::string& table_name);

  /**
   * @brief Get cache statistics
   */
  [[nodiscard]] CacheStatisticsSnapshot GetStatistics() const;

  /**
   * @brief Restart the cache from a cold (empty) state.
   * @return true if cache was enabled, false if cache was not initialized at startup
   *
   * After a previous Disable() the cache is empty, because Disable() drains
   * the invalidation queue and clears all entries to avoid serving stale
   * results. Enable() simply restarts the background workers.
   *
   * Note: Cache can only be enabled if it was initialized at startup
   * (cache.enabled = true). If the server was started with cache disabled,
   * this operation will fail.
   */
  bool Enable();

  /**
   * @brief Stop the cache and clear it to avoid stale entries.
   *
   * Stops the cache by draining the invalidation queue, clearing all
   * entries, and stopping the background workers. Re-enabling produces a
   * cold cache. This is the safe default: any data-modification events
   * delivered while the cache is disabled would otherwise be silently
   * dropped, leaving stale entries visible after re-enable.
   *
   * If you need to preserve entries across a temporary disable, a future
   * Pause() API (TODO) would be required; this method intentionally clears.
   */
  void Disable();

  /**
   * @brief Set minimum query cost threshold for caching
   * @param min_query_cost_ms New minimum query cost in milliseconds
   *
   * Only queries with execution time >= min_query_cost_ms will be cached.
   * Changes apply immediately to future cache inserts.
   */
  void SetMinQueryCost(double min_query_cost_ms);

  /**
   * @brief Set TTL for cache entries
   * @param ttl_seconds Time-to-live in seconds (0 = no expiration)
   *
   * Changes apply immediately. Cache entries will be checked for expiration
   * on lookup. Expired entries are treated as cache misses.
   */
  void SetTtl(int ttl_seconds);

  /**
   * @brief Get number of entries currently tracked by InvalidationManager
   * @return Tracked invalidation-metadata entry count, or 0 if cache disabled
   *
   * Exposed for tests/diagnostics that need to compare the QueryCache entry
   * count with the InvalidationManager metadata count to verify they remain
   * in sync (no phantom metadata).
   */
  [[nodiscard]] size_t GetTrackedInvalidationEntries() const;

 private:
  /**
   * @brief Resolve cache key from a query
   * @param query Parsed query
   * @return Resolved CacheKey, or nullopt if the query is not cacheable
   */
  [[nodiscard]] std::optional<CacheKey> ResolveCacheKey(const query::Query& query) const;

  std::atomic<bool> enabled_;
  std::atomic<int> ttl_seconds_;  // TTL configuration in seconds (0 = no expiration)
  std::atomic<uint64_t> data_version_{0};
  std::unordered_map<std::string, uint64_t> table_data_versions_;
  bool table_invalidation_strategy_ = false;

  /// Serializes the *combined* (QueryCache + InvalidationManager) mutating
  /// operations Insert/Clear/ClearTable. Each component already owns an
  /// internal lock that protects its own data structures; this mutex closes
  /// the gap *between* the two component-local critical sections.
  ///
  /// Concretely: without this mutex, Clear() runs query_cache_->Clear() then
  /// invalidation_mgr_->Clear() as two independent locked sections, and a
  /// concurrent Insert() can squeeze in between them and register a key in
  /// InvalidationManager that points to a now-empty QueryCache (phantom
  /// metadata). Holding serialize_mutex_ around the whole Insert / Clear /
  /// ClearTable body guarantees consistency between the two components.
  ///
  /// Lookup is intentionally *not* serialized here: it only touches QueryCache
  /// (which has its own shared_mutex) and never mutates InvalidationManager,
  /// so it cannot observe a phantom-metadata state and must remain on the
  /// fast read path.
  mutable std::mutex serialize_mutex_;

  std::unique_ptr<QueryCache> query_cache_;
  std::unique_ptr<InvalidationManager> invalidation_mgr_;
  std::unique_ptr<InvalidationQueue> invalidation_queue_;
};

}  // namespace mygramdb::cache
