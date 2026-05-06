/**
 * @file query_cache.h
 * @brief Query cache with LRU eviction
 */

#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "cache/cache_entry.h"
#include "cache/result_compressor.h"
#include "query/cache_key.h"
#include "utils/periodic_worker.h"

namespace mygramdb::cache {

/**
 * @brief Cache statistics snapshot (copyable)
 *
 * Snapshot of cache statistics for reporting.
 * All fields are plain values (no atomic or mutex).
 *
 * M-16 (schema-drift guard): @see kCacheStatsFieldVersion below. Both
 * CacheStatisticsSnapshot and CacheStatistics share the same field set.
 * Whenever you add/remove/rename a field on either struct, you MUST:
 *   1. Mirror the change on the other struct.
 *   2. Update QueryCache::GetStatistics() to copy the new field.
 *   3. Bump kCacheStatsFieldVersion (below) to keep the static_assert wedge
 *      green — the assert is a deliberate tripwire that forces reviewers to
 *      look at the mirror struct and the GetStatistics() copy.
 */
struct CacheStatisticsSnapshot {
  // Query statistics
  uint64_t total_queries = 0;
  uint64_t cache_hits = 0;
  uint64_t cache_misses = 0;
  uint64_t cache_misses_invalidated = 0;
  uint64_t cache_misses_not_found = 0;
  uint64_t cache_misses_ttl_expired = 0;

  // Invalidation statistics
  uint64_t invalidations_immediate = 0;
  uint64_t invalidations_deferred = 0;
  uint64_t invalidations_batches = 0;

  // Memory statistics
  uint64_t current_entries = 0;
  uint64_t current_memory_bytes = 0;
  uint64_t invalidation_index_memory_bytes = 0;  ///< Memory used by InvalidationManager's tracking structures
  uint64_t evictions = 0;
  uint64_t ttl_expirations = 0;         ///< TTL-expired entries removed
  uint64_t decompression_failures = 0;  ///< Entries removed due to decompression failure
  uint64_t rejection_count = 0;         ///< Inserts rejected for being below min_query_cost_ms threshold
  uint64_t forced_clears = 0;           ///< Bulk Clear()/ClearTable() invocations (count of bulk operations)
  uint64_t stale_lru_entries = 0;       ///< LRU-list keys that were missing from cache_map_ (defensive counter)

  // Configuration snapshot (constants taken from QueryCache constructor)
  size_t max_memory_bytes = 0;       ///< Configured cache memory ceiling
  double min_query_cost_ms = 0.0;    ///< Cost threshold below which inserts are rejected
  uint64_t ttl_seconds = 0;          ///< Configured TTL (0 = no expiration)
  bool compression_enabled = false;  ///< Whether LZ4 compression is in use

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
 * @brief Schema version for CacheStatistics / CacheStatisticsSnapshot.
 *
 * Bumped whenever fields are added / removed / renamed on either struct so
 * that the static_assert at the bottom of this file trips and forces the
 * reviewer to keep both structs and QueryCache::GetStatistics() in sync (M-16).
 *
 * Current schema (must match exactly):
 *   17 atomic uint64_t counters in CacheStatistics
 *   3 timing doubles guarded by timing_mutex_ in CacheStatistics
 *   17 plain uint64_t counters in CacheStatisticsSnapshot
 *   1 invalidation_index_memory_bytes counter (snapshot only, populated by
 *     CacheManager from InvalidationManager)
 *   4 configuration snapshot fields (max_memory_bytes, min_query_cost_ms,
 *     ttl_seconds, compression_enabled) — snapshot only
 *   3 timing doubles in CacheStatisticsSnapshot
 *   3 helper methods on snapshot (HitRate, AverageCacheHitLatency,
 *     AverageCacheMissLatency) and 1 accessor (TotalTimeSaved)
 */
inline constexpr uint32_t kCacheStatsFieldVersion = 1;

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
  std::atomic<uint64_t> cache_misses_ttl_expired{0};

  // Invalidation statistics
  std::atomic<uint64_t> invalidations_immediate{0};
  std::atomic<uint64_t> invalidations_deferred{0};
  std::atomic<uint64_t> invalidations_batches{0};

  // Memory statistics
  std::atomic<uint64_t> current_entries{0};
  std::atomic<uint64_t> current_memory_bytes{0};
  std::atomic<uint64_t> evictions{0};
  std::atomic<uint64_t> ttl_expirations{0};
  std::atomic<uint64_t> decompression_failures{0};
  std::atomic<uint64_t> rejection_count{0};    ///< Inserts rejected for being below min_query_cost_ms threshold
  std::atomic<uint64_t> forced_clears{0};      ///< Bulk Clear()/ClearTable() invocations
  std::atomic<uint64_t> stale_lru_entries{0};  ///< LRU keys not found in cache_map_ during EvictForSpace

  // Timing statistics (protected by mutex)
  mutable std::mutex timing_mutex_;
  double total_cache_hit_time_ms{0.0};
  double total_cache_miss_time_ms{0.0};
  double total_query_saved_time_ms{0.0};
};

/// Reason for cache entry removal (used by RemoveEntryLocked)
enum class RemovalReason : std::uint8_t {
  kLRUEviction,
  kTTLExpired,
  kTTLExpiredAlreadyCounted,  ///< TTL expired, stats already counted by Lookup
  kDecompressionFailure,
  kDecompressionFailureAlreadyCounted,  ///< Decompression failed, stats already counted by Lookup
  kTableClear,
  kClear  ///< Whole-cache Clear() (no per-reason counter increment)
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
   * @brief Callback type for batch eviction notifications
   * @param keys Cache keys being evicted in a single bulk operation
   *
   * Optional optimization for callers that have a batch unregister API
   * (e.g. InvalidationManager::UnregisterCacheEntries) and want to amortize
   * lock-acquisition cost across many evictions. When set, this is called
   * INSTEAD OF EvictionCallback for bulk paths (Clear, ClearTable, EvictForSpace,
   * RefreshLRU). Per-key Erase still uses EvictionCallback.
   */
  using BatchEvictionCallback = std::function<void(const std::vector<CacheKey>&)>;

  /**
   * @brief Constructor
   * @param max_memory_bytes Maximum memory usage in bytes
   * @param min_query_cost_ms Minimum query cost to cache (ms)
   * @param ttl_seconds Time-to-live for cache entries in seconds (0 = no expiration)
   * @param compression_enabled Enable LZ4 compression for cached results (default: true)
   */
  explicit QueryCache(size_t max_memory_bytes, double min_query_cost_ms, int ttl_seconds = 0,
                      bool compression_enabled = true);

  /**
   * @brief Destructor - stops background LRU refresh thread
   */
  ~QueryCache();

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
   * @brief Erase cache entry without firing eviction_callback_
   * @param key Cache key
   * @return true if entry was found and erased
   *
   * Identical to Erase() except the eviction callback is suppressed. Used by
   * InvalidationQueue, which performs its own InvalidationManager cleanup and
   * must not double-unregister via the eviction callback (CR-6).
   *
   * Callers that take this path are responsible for any external bookkeeping
   * that the eviction callback would otherwise have performed.
   */
  bool EraseWithoutCallback(const CacheKey& key);

  /**
   * @brief Clear all cache entries
   *
   * Removes all entries. Invokes eviction_callback_ for each removed entry
   * with RemovalReason::kClear, allowing external bookkeeping (e.g.
   * InvalidationManager) to stay consistent with the cache.
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
    snapshot.cache_misses_ttl_expired = stats_.cache_misses_ttl_expired.load();
    snapshot.invalidations_immediate = stats_.invalidations_immediate.load();
    snapshot.invalidations_deferred = stats_.invalidations_deferred.load();
    snapshot.invalidations_batches = stats_.invalidations_batches.load();
    snapshot.current_entries = stats_.current_entries.load();
    snapshot.current_memory_bytes = stats_.current_memory_bytes.load();
    snapshot.evictions = stats_.evictions.load();
    snapshot.ttl_expirations = stats_.ttl_expirations.load();
    snapshot.decompression_failures = stats_.decompression_failures.load();
    snapshot.rejection_count = stats_.rejection_count.load();
    snapshot.forced_clears = stats_.forced_clears.load();
    snapshot.stale_lru_entries = stats_.stale_lru_entries.load();
    // Configuration snapshot. max_memory_bytes_ and compression_enabled_ are
    // const after construction; min_query_cost_ms_ and ttl_seconds_ are atomic
    // and may be retuned at runtime via SetMinQueryCost/SetTtl.
    snapshot.max_memory_bytes = max_memory_bytes_;
    snapshot.min_query_cost_ms = min_query_cost_ms_.load(std::memory_order_relaxed);
    const int ttl_value = ttl_seconds_.load(std::memory_order_relaxed);
    snapshot.ttl_seconds = ttl_value < 0 ? 0 : static_cast<uint64_t>(ttl_value);
    snapshot.compression_enabled = compression_enabled_;
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
   * @brief Set batch eviction callback (optional, for bulk-path optimization)
   * @param callback Function to call with the list of evicted keys
   *
   * If set, bulk eviction paths (Clear, ClearTable, EvictForSpace, RefreshLRU)
   * call this once with all evicted keys INSTEAD OF calling the per-key
   * EvictionCallback. This lets observers (e.g. InvalidationManager) take a
   * single mutex acquisition rather than one per key (H-M7).
   *
   * Per-key paths (Erase) continue to use the per-key EvictionCallback. If
   * BatchEvictionCallback is unset on a bulk path, the bulk path falls back to
   * looping the per-key callback to preserve backward compatibility.
   */
  void SetBatchEvictionCallback(BatchEvictionCallback callback) { batch_eviction_callback_ = std::move(callback); }

  /**
   * @brief Set minimum query cost threshold for caching
   * @param min_query_cost_ms New minimum query cost in milliseconds
   *
   * Queries with cost less than this threshold will not be cached.
   * Changes apply to future Insert() calls.
   */
  void SetMinQueryCost(double min_query_cost_ms) {
    min_query_cost_ms_.store(min_query_cost_ms, std::memory_order_relaxed);
  }

  /**
   * @brief Get current minimum query cost threshold
   */
  [[nodiscard]] double GetMinQueryCost() const { return min_query_cost_ms_.load(std::memory_order_relaxed); }

  /**
   * @brief Set TTL for cache entries
   * @param ttl_seconds Time-to-live in seconds (0 = no expiration)
   *
   * Changes apply immediately to TTL expiration checks.
   */
  void SetTtl(int ttl_seconds) { ttl_seconds_.store(ttl_seconds, std::memory_order_relaxed); }

  /**
   * @brief Get current TTL setting
   */
  [[nodiscard]] int GetTtl() const { return ttl_seconds_.load(std::memory_order_relaxed); }

  /**
   * @brief Check if compression is enabled for cached results
   */
  [[nodiscard]] bool IsCompressionEnabled() const { return compression_enabled_; }

  /**
   * @brief Test-only: replace a cache entry's compressed payload with bytes
   *        guaranteed to fail LZ4 decompression.
   * @param key Cache key of an existing entry to corrupt
   * @return true if the entry was found and corrupted, false otherwise
   *
   * Used by regression tests that verify failure-path behavior
   * (decompression_failures counter dedup, pending-key cleanup, etc.).
   * The corruption is observable on the next Lookup of @p key.
   *
   * NOTE: not part of the public API; intended for white-box tests only.
   */
  bool CorruptEntryForTest(const CacheKey& key);

 private:
  // LRU list: most recently used at front
  std::list<CacheKey> lru_list_;

  // Map: cache key -> (cache entry, LRU iterator)
  std::unordered_map<CacheKey, std::pair<CacheEntry, std::list<CacheKey>::iterator>> cache_map_;

  // Configuration
  size_t max_memory_bytes_;
  std::atomic<double> min_query_cost_ms_;
  std::atomic<int> ttl_seconds_;  ///< Time-to-live in seconds (0 = no expiration)
  bool compression_enabled_;      ///< Enable LZ4 compression for cached results

  // Memory tracking
  size_t total_memory_bytes_ = 0;

  // Thread safety
  mutable std::shared_mutex mutex_;

  // Statistics
  CacheStatistics stats_;

  // Eviction callback (per-key, fired by Erase)
  EvictionCallback eviction_callback_;

  // Optional batch eviction callback (fired by Clear/ClearTable/EvictForSpace/
  // RefreshLRU when set). Falls back to looping eviction_callback_ if unset.
  BatchEvictionCallback batch_eviction_callback_;

  // Keys pending cleanup (collected by Lookup, processed by RefreshLRU)
  // Using unordered_set for deduplication (same key may expire on multiple Lookups)
  static constexpr size_t kMaxPendingKeys = 10000;  ///< Max pending keys per category
  mutable std::mutex expired_keys_mutex_;
  std::unordered_set<CacheKey> pending_expired_keys_;        ///< TTL-expired keys
  std::unordered_set<CacheKey> pending_decompression_keys_;  ///< Decompression-failed keys

  // Background LRU refresh worker. M-8 unified the previous std::thread +
  // std::condition_variable + atomic<bool> trio with the rest of MygramDB's
  // periodic-task plumbing via PeriodicWorker; the worker invokes
  // RefreshLRU() at a fixed cadence and is stopped (with a fast cv-wake)
  // by the destructor.
  mygram::utils::PeriodicWorker lru_refresh_worker_{"query_cache_lru_refresh"};

  /**
   * @brief Evict entries to make room for new entry
   * @param required_bytes Bytes needed for new entry
   * @param[out] evicted_keys If non-null, removed keys are appended here so the
   *             caller can fire eviction_callback_ AFTER releasing mutex_
   *             (H-M3 lock-order safety).
   * @return true if enough space was freed
   * @pre Caller must hold exclusive lock on mutex_
   */
  bool EvictForSpace(size_t required_bytes, std::vector<CacheKey>* evicted_keys = nullptr);

  /**
   * @brief Remove a single cache entry while holding exclusive lock
   * @param iter Iterator to entry in cache_map_
   * @param reason Why the entry is being removed
   * @param[out] evicted_keys If non-null, the removed key is appended here so
   *             that the caller can invoke eviction_callback_ AFTER releasing
   *             the lock (H-M3: avoids QueryCache::mutex_ ->
   *             InvalidationManager::mutex_ acquisition order while a reverse
   *             code path acquires the locks in the opposite order).
   * @pre Caller must hold exclusive lock on mutex_
   *
   * @note This function never calls eviction_callback_ directly. Callers that
   *       need external bookkeeping must pass @p evicted_keys and invoke the
   *       callback themselves after releasing the lock.
   */
  void RemoveEntryLocked(decltype(cache_map_)::iterator iter, RemovalReason reason,
                         std::vector<CacheKey>* evicted_keys = nullptr);

  /**
   * @brief Invoke eviction_callback_ for each key in @p keys.
   *
   * MUST be called WITHOUT holding mutex_. The callback typically acquires
   * InvalidationManager::mutex_ which is a foreign lock; calling it while
   * holding our shared/unique_lock risks lock-order inversion deadlocks
   * with InvalidateAffectedEntries (H-M3).
   *
   * @note Safe to call with an empty vector (no-op).
   */
  void FireEvictionCallbacks(const std::vector<CacheKey>& keys);

  /**
   * @brief Move key to front of LRU list (most recently used)
   * @param key Cache key to move
   * @pre Caller must hold exclusive lock on mutex_
   */
  void Touch(const CacheKey& key);

  /**
   * @brief Refresh LRU list based on access flags
   *
   * Moves entries with accessed_since_refresh=true to front of LRU list.
   * Invoked by lru_refresh_worker_ at a fixed cadence; runs without
   * holding the worker's internal cv mutex.
   */
  void RefreshLRU();

  /**
   * @brief Internal lookup implementation shared by Lookup and LookupWithMetadata
   * @param key Cache key
   * @param metadata If non-null, populated with cache entry metadata on hit
   * @return Decompressed result if found and not invalidated, nullopt otherwise
   */
  std::optional<std::vector<DocId>> LookupInternal(const CacheKey& key, LookupMetadata* metadata);
};

// ---------------------------------------------------------------------------
// M-16 schema-drift sentinel.
//
// The sizes below are deliberately spelled out so a structural change to
// CacheStatisticsSnapshot (or, indirectly, CacheStatistics) trips this
// static_assert at compile time. The intent is to force the author of the
// change to:
//   1. Re-confirm both structs have matching counter fields.
//   2. Update QueryCache::GetStatistics() to copy the new field.
//   3. Bump kCacheStatsFieldVersion above to keep this assertion in sync.
//
// If a legitimate field change makes this assertion fire, recompute the
// expected size from the struct and update kExpectedCacheStatisticsSnapshotSize
// AND kCacheStatsFieldVersion. Do NOT relax this to `>=` — the whole point is
// that drift is loud.
//
// CacheStatistics itself is intentionally NOT fingerprinted because it
// contains a std::mutex whose size is implementation-defined and would make
// the assertion brittle.
// ---------------------------------------------------------------------------
inline constexpr size_t kExpectedCacheStatisticsSnapshotSize = 200;
static_assert(sizeof(CacheStatisticsSnapshot) == kExpectedCacheStatisticsSnapshotSize,
              "CacheStatisticsSnapshot layout changed: also update CacheStatistics, "
              "QueryCache::GetStatistics(), and bump kCacheStatsFieldVersion (see M-16).");

}  // namespace mygramdb::cache
