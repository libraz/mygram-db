/**
 * @file query_cache.cpp
 * @brief Query cache implementation
 */

#include "cache/query_cache.h"

#include <algorithm>
#include <chrono>
#include <cstring>

namespace mygramdb::cache {

QueryCache::QueryCache(size_t max_memory_bytes, double min_query_cost_ms, int ttl_seconds, bool compression_enabled)
    : max_memory_bytes_(max_memory_bytes),
      min_query_cost_ms_(min_query_cost_ms),
      ttl_seconds_(ttl_seconds),
      compression_enabled_(compression_enabled) {
  // Lower the load factor and pre-reserve buckets to minimize cache_map_
  // rehashing under the steady-state working set. Rehash cost aside, this is
  // also a defense-in-depth measure for the iterator-stability contract used
  // by Lookup/MarkInvalidated (see CR-5 note in LookupInternal): the
  // shared_mutex contract already serializes Insert against in-flight Lookups,
  // but keeping rehash rare also keeps invalidation checks cheap.
  //
  // Capacity heuristic: assume an average compressed entry occupies ~256 B
  // including bookkeeping. This is a coarse estimate; the value is only used
  // to size the initial bucket array and the load_factor() invariant guards
  // correctness if it is wrong.
  constexpr size_t kAverageEntryBytes = 256;
  constexpr float kLoadFactor = 0.5F;
  const size_t estimated_entries = max_memory_bytes_ > 0 ? (max_memory_bytes_ / kAverageEntryBytes) : 0;
  cache_map_.max_load_factor(kLoadFactor);
  if (estimated_entries > 0) {
    cache_map_.reserve(estimated_entries);
  }

  // Start background LRU refresh thread
  lru_refresh_thread_ = std::thread(&QueryCache::RefreshLRUWorker, this);
}

QueryCache::~QueryCache() {
  // Stop background LRU refresh thread
  should_stop_.store(true);
  {
    std::lock_guard<std::mutex> lock(stop_mutex_);
    stop_cv_.notify_all();
  }
  if (lru_refresh_thread_.joinable()) {
    lru_refresh_thread_.join();
  }
}

std::optional<std::vector<DocId>> QueryCache::Lookup(const CacheKey& key) {
  return LookupInternal(key, nullptr);
}

std::optional<std::vector<DocId>> QueryCache::LookupWithMetadata(const CacheKey& key, LookupMetadata& metadata) {
  return LookupInternal(key, &metadata);
}

std::optional<std::vector<DocId>> QueryCache::LookupInternal(const CacheKey& key, LookupMetadata* metadata) {
  // Start timing
  auto start_time = std::chrono::high_resolution_clock::now();

  // Helper to record miss latency and return nullopt
  auto record_miss = [&]() -> std::optional<std::vector<DocId>> {
    auto end_time = std::chrono::high_resolution_clock::now();
    double miss_time_ms = std::chrono::duration<double, std::milli>(end_time - start_time).count();
    {
      std::lock_guard<std::mutex> timing_lock(stats_.timing_mutex_);
      stats_.total_cache_miss_time_ms += miss_time_ms;
    }
    return std::nullopt;
  };

  // Shared lock for read.
  //
  // CR-5 (iterator validity): this lookup holds a shared_lock for the entire
  // duration of any iterator dereference of `iter` below. Insert()/Erase() and
  // Clear() take a unique_lock, which the std::shared_mutex contract
  // serializes after all readers; cache_map_ rehash therefore cannot occur
  // while we hold `iter`. The QueryCache constructor additionally caps the
  // load factor at 0.5 and pre-reserves buckets, so steady-state inserts
  // rarely rehash even in isolation.
  //
  // If you change this function to release `lock` before using `iter`, you
  // reintroduce the CR-5 use-after-free — copy the values you need out of
  // the entry first, then release the lock.
  std::shared_lock lock(mutex_);

  stats_.total_queries++;

  auto iter = cache_map_.find(key);
  if (iter == cache_map_.end()) {
    stats_.cache_misses++;
    stats_.cache_misses_not_found++;
    return record_miss();
  }

  // Check invalidation flag
  if (iter->second.first.invalidated.load()) {
    stats_.cache_misses++;
    stats_.cache_misses_invalidated++;
    return record_miss();
  }

  // Check TTL expiration (if TTL is enabled)
  int current_ttl = ttl_seconds_.load(std::memory_order_relaxed);
  if (current_ttl > 0) {
    const auto& entry = iter->second.first;
    auto now = std::chrono::steady_clock::now();
    auto age = std::chrono::duration_cast<std::chrono::seconds>(now - entry.metadata.created_at).count();
    if (age >= current_ttl) {
      // Entry expired - enqueue for cleanup by RefreshLRU
      {
        std::lock_guard<std::mutex> expired_lock(expired_keys_mutex_);
        if (pending_expired_keys_.size() < kMaxPendingKeys) {
          pending_expired_keys_.insert(key);
        }
      }

      stats_.cache_misses++;
      stats_.cache_misses_ttl_expired++;  // Count as TTL-expired miss
      stats_.ttl_expirations++;           // Count TTL expiration at detection time
      return record_miss();
    }
  }

  // Cache hit - copy shared_ptr under lock, decompress outside
  const auto& entry = iter->second.first;
  auto compressed_ptr = entry.compressed;
  const size_t original_size = entry.original_size;
  const double query_cost_ms = entry.query_cost_ms;

  // Populate metadata if requested
  if (metadata != nullptr) {
    metadata->query_cost_ms = query_cost_ms;
    metadata->created_at = entry.metadata.created_at;
  }

  // Lock-free access tracking (no lock upgrade needed)
  // Atomic increment of access count and set dirty flag for background LRU refresh
  iter->second.first.metadata.access_count.fetch_add(1, std::memory_order_relaxed);
  iter->second.first.metadata.accessed_since_refresh.store(true, std::memory_order_relaxed);

  // Release shared lock before decompression
  lock.unlock();

  // Decompress outside lock to reduce shared_lock hold time
  std::vector<DocId> result;
  if (compression_enabled_) {
    auto decompress_result = ResultCompressor::Decompress(*compressed_ptr, original_size);
    if (!decompress_result) {
      // Decompression failed - enqueue for cleanup and treat as miss.
      //
      // Dedup semantic: the decompression_failures counter increments per
      // detection event per entry, NOT per Lookup call. If multiple concurrent
      // Lookups of the same broken entry race here, only the first insert into
      // pending_decompression_keys_ counts. Subsequent Lookups still observe
      // a miss, but do not bump the counter again for the same entry. (Once
      // RefreshLRU drains the set and removes the entry, a re-insert under
      // the same key could trigger another distinct event — that is the
      // intended behavior.)
      bool first_detection = false;
      {
        std::lock_guard<std::mutex> expired_lock(expired_keys_mutex_);
        if (pending_decompression_keys_.size() < kMaxPendingKeys) {
          first_detection = pending_decompression_keys_.insert(key).second;
        }
        // If kMaxPendingKeys cap reached and the key is not already pending,
        // first_detection stays false and we skip the counter increment to
        // avoid drift; the entry will still be served as a miss.
      }

      stats_.cache_misses++;
      if (first_detection) {
        stats_.decompression_failures++;  // Count failure at detection time
      }
      return record_miss();
    }
    result = std::move(*decompress_result);
  } else {
    // No compression - interpret raw bytes as DocId array
    result.resize(original_size);
    std::memcpy(result.data(), compressed_ptr->data(), compressed_ptr->size());
  }

  // Incremented outside the shared lock (after decompression) to avoid holding
  // the lock during CPU-intensive work. This creates a brief window where
  // cache_hits + cache_misses may transiently exceed total_queries in a
  // concurrent Reset() scenario, which is acceptable for monitoring counters
  // (reviewed: no correctness invariant depends on exact counter consistency).
  //
  // The acceptable transient drift is covered by the
  // ConcurrentQueryCountAccuracy regression test in cache_thread_safety_test
  // (and the StatsInvariantHitsPlusMissesEqualsTotal test for the
  // single-threaded invariant). Do not move this increment back under the
  // shared lock without revisiting both tests and the timing-statistics
  // path below.
  stats_.cache_hits++;

  // Record hit latency and saved time
  auto end_time = std::chrono::high_resolution_clock::now();
  double hit_time_ms = std::chrono::duration<double, std::milli>(end_time - start_time).count();
  {
    std::lock_guard<std::mutex> timing_lock(stats_.timing_mutex_);
    stats_.total_cache_hit_time_ms += hit_time_ms;
    stats_.total_query_saved_time_ms += query_cost_ms;
  }

  return result;
}

bool QueryCache::Insert(const CacheKey& key, const std::vector<DocId>& result, const CacheMetadata& metadata,
                        double query_cost_ms) {
  // Check if query cost meets threshold
  if (query_cost_ms < min_query_cost_ms_.load(std::memory_order_relaxed)) {
    // Track inserts skipped because their cost is below the configured
    // threshold. This is the dominant Insert-rejection reason; over-size and
    // already-present rejections below are not counted here (they have their
    // own observability via current_memory_bytes / current_entries).
    stats_.rejection_count.fetch_add(1, std::memory_order_relaxed);
    return false;
  }

  // Compress result (if enabled)
  std::vector<uint8_t> compressed;
  if (compression_enabled_) {
    auto compress_result = ResultCompressor::Compress(result);
    if (!compress_result) {
      return false;
    }
    compressed = std::move(*compress_result);
  } else {
    // Store raw bytes without compression
    compressed.resize(result.size() * sizeof(DocId));
    std::memcpy(compressed.data(), result.data(), compressed.size());
  }

  // Create cache entry to calculate accurate memory usage
  CacheEntry temp_entry;
  temp_entry.compressed = std::make_shared<const std::vector<uint8_t>>(std::move(compressed));
  temp_entry.metadata = metadata;

  const size_t original_count = result.size();  // Number of DocId elements, not bytes
  const size_t compressed_size = temp_entry.compressed->size();
  const size_t entry_memory = temp_entry.MemoryUsage();

  // Don't cache if entry is too large.
  // Safe without lock: max_memory_bytes_ is const after construction (no setter exists).
  if (entry_memory > max_memory_bytes_) {
    return false;
  }

  // Exclusive lock for write
  std::unique_lock lock(mutex_);

  // Check if already exists
  if (cache_map_.find(key) != cache_map_.end()) {
    return false;
  }

  // Evict entries if needed
  if (total_memory_bytes_ + entry_memory > max_memory_bytes_) {
    if (!EvictForSpace(entry_memory)) {
      return false;
    }
  }

  // Complete cache entry (reuse temp_entry to maintain consistent memory calculation)
  temp_entry.key = key;
  temp_entry.original_size = original_count;  // Store count, not bytes
  temp_entry.compressed_size = compressed_size;
  temp_entry.query_cost_ms = query_cost_ms;
  temp_entry.metadata.created_at = std::chrono::steady_clock::now();
  temp_entry.metadata.last_accessed = temp_entry.metadata.created_at;
  temp_entry.invalidated.store(false);

  // Insert into LRU list (front = most recent)
  lru_list_.push_front(key);
  auto lru_it = lru_list_.begin();

  // Insert into cache map using emplace to avoid copy
  cache_map_.emplace(key, std::make_pair(std::move(temp_entry), lru_it));

  // Update memory tracking
  total_memory_bytes_ += entry_memory;
  stats_.current_entries++;
  stats_.current_memory_bytes = total_memory_bytes_;

  return true;
}

bool QueryCache::MarkInvalidated(const CacheKey& key) {
  // Uses shared_lock intentionally for performance: invalidation can be high-frequency
  // and only updates atomic fields (invalidated flag + atomic counter).
  //
  // Thread-safety rationale:
  // - find() is a read on cache_map_, which is safe under shared_lock.
  // - invalidated.store() modifies an atomic member of the value, not the map structure.
  // - stats_.invalidations_immediate is also atomic.
  // - Iterator stability: emplace (used by Insert) does not invalidate existing iterators
  //   in std::unordered_map, so a concurrent Insert under unique_lock is safe.
  // - Erase requires unique_lock, which blocks until all shared_locks are released,
  //   so no iterator can be invalidated while this shared_lock is held.
  std::shared_lock lock(mutex_);

  auto iter = cache_map_.find(key);
  if (iter == cache_map_.end()) {
    return false;
  }

  // Atomic flag set (no lock upgrade needed)
  iter->second.first.invalidated.store(true);
  stats_.invalidations_immediate++;

  return true;
}

bool QueryCache::Erase(const CacheKey& key) {
  std::unique_lock lock(mutex_);

  auto iter = cache_map_.find(key);
  if (iter == cache_map_.end()) {
    return false;
  }

  // Notify eviction callback before structural removal so external bookkeeping
  // (e.g. CacheManager's invalidation_mgr_ unregister) can run while the entry
  // metadata is still accessible. Symmetric with RemoveEntryLocked.
  //
  // CR-6: callers that want to suppress this callback (the InvalidationQueue
  // cleanup path performs its own UnregisterCacheEntry and must not
  // double-unregister) should use EraseWithoutCallback() instead.
  if (eviction_callback_) {
    eviction_callback_(key);
  }

  // Remove from LRU list
  lru_list_.erase(iter->second.second);

  // Update memory tracking
  const size_t entry_memory = iter->second.first.MemoryUsage();
  total_memory_bytes_ -= entry_memory;
  stats_.current_entries--;
  stats_.current_memory_bytes = total_memory_bytes_;
  stats_.invalidations_deferred++;

  // Remove from cache map
  cache_map_.erase(iter);

  return true;
}

bool QueryCache::EraseWithoutCallback(const CacheKey& key) {
  std::unique_lock lock(mutex_);

  auto iter = cache_map_.find(key);
  if (iter == cache_map_.end()) {
    return false;
  }

  // CR-6: deliberately does NOT invoke eviction_callback_. The
  // InvalidationQueue cleanup path performs its own InvalidationManager
  // unregister and must not double-unregister via the callback, which would
  // otherwise race with concurrent Insert and corrupt the reverse indexes
  // (table_to_cache_keys_, ngram_to_cache_keys_).

  // Remove from LRU list
  lru_list_.erase(iter->second.second);

  // Update memory tracking
  const size_t entry_memory = iter->second.first.MemoryUsage();
  total_memory_bytes_ -= entry_memory;
  stats_.current_entries--;
  stats_.current_memory_bytes = total_memory_bytes_;
  stats_.invalidations_deferred++;

  // Remove from cache map
  cache_map_.erase(iter);

  return true;
}

void QueryCache::Clear() {
  std::unique_lock lock(mutex_);

  // Notify eviction callback for every entry before swapping. This keeps
  // external bookkeeping (e.g. InvalidationManager) consistent with the cache:
  // any caller that hooks SetEvictionCallback to clean up per-key metadata
  // would otherwise leak that metadata when Clear() bypasses RemoveEntryLocked.
  //
  // We iterate cache_map_ and call eviction_callback_ directly (rather than
  // looping RemoveEntryLocked, which would do per-entry list/map erase) because
  // the swap below is O(1) and discards the containers wholesale.
  if (eviction_callback_) {
    for (const auto& [key, entry_pair] : cache_map_) {
      eviction_callback_(key);
    }
  }

  // Swap with empty containers to release allocated capacity
  decltype(lru_list_)().swap(lru_list_);
  decltype(cache_map_)().swap(cache_map_);
  total_memory_bytes_ = 0;
  stats_.current_entries = 0;
  stats_.current_memory_bytes = 0;
  // Count this whole-cache clear as a single forced_clears event (operator-
  // initiated bulk eviction), regardless of how many entries were resident.
  stats_.forced_clears.fetch_add(1, std::memory_order_relaxed);
}

void QueryCache::ClearTable(const std::string& table) {
  std::unique_lock lock(mutex_);

  // Find all entries for this table
  std::vector<CacheKey> to_erase;
  for (const auto& [key, entry_pair] : cache_map_) {
    if (entry_pair.first.metadata.table == table) {
      to_erase.push_back(key);
    }
  }

  // Erase entries
  for (const auto& key : to_erase) {
    auto iter = cache_map_.find(key);
    if (iter != cache_map_.end()) {
      RemoveEntryLocked(iter, RemovalReason::kTableClear);
    }
  }
  stats_.current_memory_bytes = total_memory_bytes_;
  // Count this per-table clear as a single forced_clears event regardless of
  // how many entries actually matched the table. This matches Clear()'s
  // bulk-operation accounting.
  stats_.forced_clears.fetch_add(1, std::memory_order_relaxed);
}

bool QueryCache::CorruptEntryForTest(const CacheKey& key) {
  std::unique_lock lock(mutex_);

  auto iter = cache_map_.find(key);
  if (iter == cache_map_.end()) {
    return false;
  }

  // Replace compressed payload with bytes that cannot be a valid LZ4 frame
  // for the recorded original_size. We use 0xFF-only data with a deliberate
  // size mismatch so ResultCompressor::Decompress reports failure.
  constexpr size_t kCorruptPayloadBytes = 8;
  constexpr uint8_t kCorruptByte = 0xFF;
  // Force a large original_size so even if the bytes happened to look valid,
  // the size mismatch triggers a decompression failure (1 MiB of DocIds).
  constexpr size_t kCorruptOriginalSize = 1024 * 1024;

  auto corrupted = std::make_shared<std::vector<uint8_t>>(std::vector<uint8_t>(kCorruptPayloadBytes, kCorruptByte));
  iter->second.first.compressed = std::shared_ptr<const std::vector<uint8_t>>(std::move(corrupted));
  iter->second.first.original_size = kCorruptOriginalSize;
  return true;
}

std::optional<CacheMetadata> QueryCache::GetMetadata(const CacheKey& key) const {
  std::shared_lock lock(mutex_);

  auto iter = cache_map_.find(key);
  if (iter == cache_map_.end()) {
    return std::nullopt;
  }

  return iter->second.first.metadata;
}

bool QueryCache::EvictForSpace(size_t required_bytes) {
  // Evict from LRU tail until enough space is available
  while (total_memory_bytes_ + required_bytes > max_memory_bytes_ && !lru_list_.empty()) {
    // Get least recently used key
    const CacheKey lru_key = lru_list_.back();

    auto iter = cache_map_.find(lru_key);
    if (iter == cache_map_.end()) {
      // Inconsistency - remove from LRU list
      lru_list_.pop_back();
      continue;
    }

    RemoveEntryLocked(iter, RemovalReason::kLRUEviction);
  }

  stats_.current_memory_bytes = total_memory_bytes_;

  // Check if enough space was freed
  return total_memory_bytes_ + required_bytes <= max_memory_bytes_;
}

void QueryCache::RemoveEntryLocked(decltype(cache_map_)::iterator iter, RemovalReason reason) {
  const CacheKey& key = iter->first;

  // Notify eviction callback before deletion (metadata cleanup)
  if (eviction_callback_) {
    eviction_callback_(key);
  }

  // Remove from LRU list
  lru_list_.erase(iter->second.second);

  // Update memory tracking. Sync the public-facing stats_.current_memory_bytes
  // immediately so GetStatistics() never returns a value that is stale (higher
  // than reality) between RemoveEntryLocked and the next RefreshLRU resync.
  const size_t entry_memory = iter->second.first.MemoryUsage();
  total_memory_bytes_ -= entry_memory;
  stats_.current_entries--;
  stats_.current_memory_bytes.store(total_memory_bytes_, std::memory_order_relaxed);

  // Update reason-specific stats
  switch (reason) {
    case RemovalReason::kLRUEviction:
      stats_.evictions++;
      break;
    case RemovalReason::kTTLExpired:
      stats_.ttl_expirations++;
      break;
    case RemovalReason::kTTLExpiredAlreadyCounted:
      // Stats already incremented by Lookup() at detection time
      break;
    case RemovalReason::kDecompressionFailure:
      stats_.decompression_failures++;
      break;
    case RemovalReason::kDecompressionFailureAlreadyCounted:
      // Stats already incremented by Lookup() at detection time
      break;
    case RemovalReason::kTableClear:
      // No additional counter (existing behavior)
      break;
    case RemovalReason::kClear:
      // No additional counter (matches kTableClear). Whole-cache Clear() is
      // not an error condition or LRU pressure event; reason-specific stats
      // are intentionally not incremented.
      break;
  }

  // Remove from cache map
  cache_map_.erase(iter);
}

void QueryCache::Touch(const CacheKey& key) {
  auto iter = cache_map_.find(key);
  if (iter == cache_map_.end()) {
    return;
  }

  // Move to front of LRU list
  lru_list_.erase(iter->second.second);
  lru_list_.push_front(key);
  iter->second.second = lru_list_.begin();
}

void QueryCache::RefreshLRUWorker() {
  constexpr auto kRefreshInterval = std::chrono::milliseconds(100);  // 10 Hz refresh cycle

  while (!should_stop_.load()) {
    {
      std::unique_lock<std::mutex> lock(stop_mutex_);
      stop_cv_.wait_for(lock, kRefreshInterval, [this] { return should_stop_.load(); });
    }

    if (should_stop_.load()) {
      break;
    }

    RefreshLRU();
  }
}

void QueryCache::RefreshLRU() {
  // Drain pending keys from Lookup() before acquiring main lock
  std::unordered_set<CacheKey> lookup_expired_keys;
  std::unordered_set<CacheKey> decomp_failed_keys;
  {
    std::lock_guard<std::mutex> expired_lock(expired_keys_mutex_);
    lookup_expired_keys.swap(pending_expired_keys_);
    decomp_failed_keys.swap(pending_decompression_keys_);
  }

  std::unique_lock lock(mutex_);

  auto now = std::chrono::steady_clock::now();
  int current_ttl = ttl_seconds_.load(std::memory_order_relaxed);

  // Track keys detected as expired during Lookup (stats already counted)
  // Scan-detected expired keys will be collected separately
  std::unordered_set<CacheKey> scan_expired_keys;

  // Update LRU for entries that were accessed since last refresh
  for (auto& [key, entry_pair] : cache_map_) {
    // Check TTL expiration
    if (current_ttl > 0) {
      auto age = std::chrono::duration_cast<std::chrono::seconds>(now - entry_pair.first.metadata.created_at).count();
      if (age >= current_ttl) {
        // Only add to scan set if not already detected by Lookup
        if (lookup_expired_keys.find(key) == lookup_expired_keys.end()) {
          scan_expired_keys.insert(key);
        }
        continue;  // Skip LRU update for expired entries
      }
    }

    if (entry_pair.first.metadata.accessed_since_refresh.exchange(false, std::memory_order_relaxed)) {
      // Entry was accessed, move to front of LRU list
      Touch(key);
      entry_pair.first.metadata.last_accessed = now;
    }
  }

  // Remove Lookup-detected expired entries (stats already counted by Lookup)
  for (const auto& key : lookup_expired_keys) {
    auto iter = cache_map_.find(key);
    if (iter != cache_map_.end()) {
      RemoveEntryLocked(iter, RemovalReason::kTTLExpiredAlreadyCounted);
    }
  }

  // Remove scan-detected expired entries (stats not yet counted)
  for (const auto& key : scan_expired_keys) {
    auto iter = cache_map_.find(key);
    if (iter != cache_map_.end()) {
      RemoveEntryLocked(iter, RemovalReason::kTTLExpired);
    }
  }

  // Remove decompression-failed entries (stats already counted by Lookup)
  for (const auto& key : decomp_failed_keys) {
    auto iter = cache_map_.find(key);
    if (iter != cache_map_.end()) {
      RemoveEntryLocked(iter, RemovalReason::kDecompressionFailureAlreadyCounted);
    }
  }

  // Defensive resync: RemoveEntryLocked / Insert / Erase all keep
  // stats_.current_memory_bytes in sync with total_memory_bytes_ on each
  // mutation, so this assignment is normally a no-op. Kept as a belt-and-
  // suspenders safety net in case a future code path bumps total_memory_bytes_
  // without updating stats_.
  stats_.current_memory_bytes.store(total_memory_bytes_, std::memory_order_relaxed);
}

}  // namespace mygramdb::cache
