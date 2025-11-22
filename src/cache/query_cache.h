/**
 * @file query_cache.h
 * @brief Query cache with LRU eviction
 */

#pragma once

#include <atomic>
#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <unordered_map>

#include "cache/cache_entry.h"
#include "cache/result_compressor.h"
#include "query/cache_key.h"

namespace mygramdb::cache {

/**
 * @brief Cache statistics snapshot (copyable)
 *
 * Snapshot of cache statistics for reporting.
 * All fields are plain values (no atomic or mutex).
 */
struct CacheStatisticsSnapshot {
  // Query statistics
  uint64_t total_queries = 0;
  uint64_t cache_hits = 0;
  uint64_t cache_misses = 0;
  uint64_t cache_misses_invalidated = 0;
  uint64_t cache_misses_not_found = 0;

  // Invalidation statistics
  uint64_t invalidations_immediate = 0;
  uint64_t invalidations_deferred = 0;
  uint64_t invalidations_batches = 0;

  // Memory statistics
  uint64_t current_entries = 0;
  uint64_t current_memory_bytes = 0;
  uint64_t evictions = 0;

  // Timing statistics
  double total_cache_hit_time_ms = 0.0;
  double total_cache_miss_time_ms = 0.0;
  double total_query_saved_time_ms = 0.0;

  /**
   * @brief Calculate cache hit rate
   */
  [[nodiscard]] double HitRate() const {
    return total_queries > 0 ? static_cast<double>(cache_hits) / static_cast<double>(total_queries) : 0.0;
  }

  /**
   * @brief Calculate average cache hit latency
   */
  [[nodiscard]] double AverageCacheHitLatency() const {
    return cache_hits > 0 ? total_cache_hit_time_ms / static_cast<double>(cache_hits) : 0.0;
  }

  /**
   * @brief Calculate average cache miss latency
   */
  [[nodiscard]] double AverageCacheMissLatency() const {
    return cache_misses > 0 ? total_cache_miss_time_ms / static_cast<double>(cache_misses) : 0.0;
  }

  /**
   * @brief Get total time saved by cache hits
   */
  [[nodiscard]] double TotalTimeSaved() const { return total_query_saved_time_ms; }
};

/**
 * @brief Internal cache statistics (thread-safe, non-copyable)
 *
 * Uses atomic counters and mutex for thread-safe updates.
 */
struct CacheStatistics {
  // Query statistics
  std::atomic<uint64_t> total_queries{0};
  std::atomic<uint64_t> cache_hits{0};
  std::atomic<uint64_t> cache_misses{0};
  std::atomic<uint64_t> cache_misses_invalidated{0};
  std::atomic<uint64_t> cache_misses_not_found{0};

  // Invalidation statistics
  std::atomic<uint64_t> invalidations_immediate{0};
  std::atomic<uint64_t> invalidations_deferred{0};
  std::atomic<uint64_t> invalidations_batches{0};

  // Memory statistics
  std::atomic<uint64_t> current_entries{0};
  std::atomic<uint64_t> current_memory_bytes{0};
  std::atomic<uint64_t> evictions{0};

  // Timing statistics (protected by mutex)
  mutable std::mutex timing_mutex_;
  double total_cache_hit_time_ms{0.0};
  double total_cache_miss_time_ms{0.0};
  double total_query_saved_time_ms{0.0};
};

/**
 * @brief LRU cache for query results
 *
 * Thread-safe query cache with LRU eviction policy.
 * Uses shared_mutex for concurrent reads and exclusive writes.
 */
class QueryCache {
 public:
  /**
   * @brief Callback type for eviction notifications
   * @param key The cache key being evicted
   */
  using EvictionCallback = std::function<void(const CacheKey&)>;

  /**
   * @brief Constructor
   * @param max_memory_bytes Maximum memory usage in bytes
   * @param min_query_cost_ms Minimum query cost to cache (ms)
   */
  explicit QueryCache(size_t max_memory_bytes, double min_query_cost_ms);

  /**
   * @brief Destructor
   */
  ~QueryCache() = default;

  // Non-copyable, non-movable
  QueryCache(const QueryCache&) = delete;
  QueryCache& operator=(const QueryCache&) = delete;
  QueryCache(QueryCache&&) = delete;
  QueryCache& operator=(QueryCache&&) = delete;

  /**
   * @brief Cache lookup result with metadata
   */
  struct LookupMetadata {
    double query_cost_ms = 0.0;                        ///< Original query execution time
    std::chrono::steady_clock::time_point created_at;  ///< When cache entry was created
  };

  /**
   * @brief Lookup cache entry
   * @param key Cache key
   * @return Decompressed result if found and not invalidated, nullopt otherwise
   */
  [[nodiscard]] std::optional<std::vector<DocId>> Lookup(const CacheKey& key);

  /**
   * @brief Lookup cache entry with metadata
   * @param key Cache key
   * @param[out] metadata Output parameter for cache metadata
   * @return Decompressed result if found and not invalidated, nullopt otherwise
   */
  [[nodiscard]] std::optional<std::vector<DocId>> LookupWithMetadata(const CacheKey& key, LookupMetadata& metadata);

  /**
   * @brief Insert cache entry
   * @param key Cache key
   * @param result Search result to cache
   * @param metadata Cache metadata for invalidation
   * @param query_cost_ms Query execution time
   * @return true if inserted, false if not cached (below threshold or eviction failure)
   */
  bool Insert(const CacheKey& key, const std::vector<DocId>& result, const CacheMetadata& metadata,
              double query_cost_ms);

  /**
   * @brief Mark cache entry as invalidated (Phase 1: immediate)
   * @param key Cache key
   * @return true if entry was found and marked
   */
  bool MarkInvalidated(const CacheKey& key);

  /**
   * @brief Erase cache entry (Phase 2: deferred)
   * @param key Cache key
   * @return true if entry was found and erased
   */
  bool Erase(const CacheKey& key);

  /**
   * @brief Clear all cache entries
   */
  void Clear();

  /**
   * @brief Clear cache entries for specific table
   * @param table Table name
   */
  void ClearTable(const std::string& table);

  /**
   * @brief Get cache statistics snapshot (thread-safe)
   */
  [[nodiscard]] CacheStatisticsSnapshot GetStatistics() const {
    CacheStatisticsSnapshot snapshot;
    snapshot.total_queries = stats_.total_queries.load();
    snapshot.cache_hits = stats_.cache_hits.load();
    snapshot.cache_misses = stats_.cache_misses.load();
    snapshot.cache_misses_invalidated = stats_.cache_misses_invalidated.load();
    snapshot.cache_misses_not_found = stats_.cache_misses_not_found.load();
    snapshot.invalidations_immediate = stats_.invalidations_immediate.load();
    snapshot.invalidations_deferred = stats_.invalidations_deferred.load();
    snapshot.invalidations_batches = stats_.invalidations_batches.load();
    snapshot.current_entries = stats_.current_entries.load();
    snapshot.current_memory_bytes = stats_.current_memory_bytes.load();
    snapshot.evictions = stats_.evictions.load();
    {
      std::lock_guard<std::mutex> lock(stats_.timing_mutex_);
      snapshot.total_cache_hit_time_ms = stats_.total_cache_hit_time_ms;
      snapshot.total_cache_miss_time_ms = stats_.total_cache_miss_time_ms;
      snapshot.total_query_saved_time_ms = stats_.total_query_saved_time_ms;
    }
    return snapshot;
  }

  /**
   * @brief Get cache entry metadata (for invalidation manager)
   * @param key Cache key
   * @return Metadata if found, nullopt otherwise
   */
  [[nodiscard]] std::optional<CacheMetadata> GetMetadata(const CacheKey& key) const;

  /**
   * @brief Increment invalidation batch counter
   *
   * Called by InvalidationQueue::ProcessBatch() to track batch invalidations.
   */
  void IncrementInvalidationBatches() { stats_.invalidations_batches++; }

  /**
   * @brief Set callback to be notified when entries are evicted
   * @param callback Function to call when an entry is evicted via LRU
   */
  void SetEvictionCallback(EvictionCallback callback) { eviction_callback_ = std::move(callback); }

  /**
   * @brief Set minimum query cost threshold for caching
   * @param min_query_cost_ms New minimum query cost in milliseconds
   *
   * Queries with cost less than this threshold will not be cached.
   * Changes apply to future Insert() calls.
   */
  void SetMinQueryCost(double min_query_cost_ms) { min_query_cost_ms_ = min_query_cost_ms; }

  /**
   * @brief Get current minimum query cost threshold
   */
  [[nodiscard]] double GetMinQueryCost() const { return min_query_cost_ms_; }

 private:
  // LRU list: most recently used at front
  std::list<CacheKey> lru_list_;

  // Map: cache key -> (cache entry, LRU iterator)
  std::unordered_map<CacheKey, std::pair<CacheEntry, std::list<CacheKey>::iterator>> cache_map_;

  // Configuration
  size_t max_memory_bytes_;
  double min_query_cost_ms_;

  // Memory tracking
  size_t total_memory_bytes_ = 0;

  // Thread safety
  mutable std::shared_mutex mutex_;

  // Statistics
  CacheStatistics stats_;

  // Eviction callback
  EvictionCallback eviction_callback_;

  /**
   * @brief Evict entries to make room for new entry
   * @param required_bytes Bytes needed for new entry
   * @return true if enough space was freed
   */
  bool EvictForSpace(size_t required_bytes);

  /**
   * @brief Move key to front of LRU list (most recently used)
   */
  void Touch(const CacheKey& key);
};

}  // namespace mygramdb::cache
