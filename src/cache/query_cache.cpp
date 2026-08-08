/**
 * @file query_cache.cpp
 * @brief Query cache implementation
 */

#include "cache/query_cache.h"

#include <algorithm>
#include <chrono>
#include <cstring>

#include "utils/constants.h"
#include "utils/structured_log.h"

namespace mygramdb::cache {

namespace {

/**
 * @brief Approximate per-entry overhead added by std::unordered_map node
 *        bookkeeping.
 *
 * libstdc++/libc++ typically allocate one heap node per element with at least
 * a 'next' pointer plus a cached hash. Glibc malloc adds 16 bytes of internal
 * chunk metadata on top. The constant below is a conservative round number
 * that intentionally over-attributes a few bytes: undercounting cache memory
 * is more dangerous (can drive an OOM) than slightly overcounting (just
 * triggers eviction a touch sooner).
 *
 * NOT a strict accounting figure — it does not account for hash-table bucket
 * arrays (those scale with bucket_count, not entry count, and are amortized).
 * See cache_entry.h MemoryUsage() for the per-entry breakdown.
 */
constexpr size_t kHashMapNodeOverhead = 32;

}  // namespace

QueryCache::QueryCache(size_t max_memory_bytes, double min_query_cost_ms, int ttl_seconds, bool compression_enabled,
                       size_t eviction_batch_size, bool start_background_worker)
    : max_memory_bytes_(max_memory_bytes),
      min_query_cost_ms_(min_query_cost_ms),
      ttl_seconds_(ttl_seconds),
      compression_enabled_(compression_enabled),
      eviction_batch_size_(std::max<size_t>(1, eviction_batch_size)) {
  // Keep a conservative load factor, but let the map grow with actual entries.
  // Reserving from max_memory_bytes_ made an empty 8 GiB cache allocate roughly
  // 536 MiB of buckets during startup. The shared mutex already serializes
  // inserts/rehashes against lookups, so correctness does not depend on a
  // speculative full-budget reserve.
  constexpr float kLoadFactor = 0.5F;
  cache_map_.max_load_factor(kLoadFactor);

  if (start_background_worker) {
    // Start failure here is logged by PeriodicWorker. The interval is fixed
    // and this is a fresh worker, so the expected failure modes cannot occur.
    (void)StartBackgroundWorker();
  }
}

QueryCache::~QueryCache() {
  lru_refresh_worker_.Stop();
}

mygram::utils::Expected<void, mygram::utils::Error> QueryCache::StartBackgroundWorker() {
  constexpr auto kRefreshInterval = std::chrono::milliseconds(100);
  return lru_refresh_worker_.Start([this] { RefreshLRU(); }, kRefreshInterval);
}

std::optional<std::vector<DocId>> QueryCache::Lookup(const CacheKey& key) {
  return LookupInternal(key, std::nullopt, nullptr);
}

std::optional<std::vector<DocId>> QueryCache::Lookup(const CacheKey& key, std::string_view discriminator) {
  return LookupInternal(key, discriminator, nullptr);
}

std::optional<std::vector<DocId>> QueryCache::LookupWithMetadata(const CacheKey& key, LookupMetadata& metadata) {
  return LookupInternal(key, std::nullopt, &metadata);
}

std::optional<std::vector<DocId>> QueryCache::LookupWithMetadata(const CacheKey& key, std::string_view discriminator,
                                                                 LookupMetadata& metadata) {
  return LookupInternal(key, discriminator, &metadata);
}

std::optional<std::vector<DocId>> QueryCache::LookupInternal(const CacheKey& key,
                                                             std::optional<std::string_view> discriminator,
                                                             LookupMetadata* metadata) {
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
  // Iterator validity: this lookup holds a shared_lock for the entire
  // duration of any iterator dereference of `iter` below. Insert()/Erase() and
  // Clear() take a unique_lock, which the std::shared_mutex contract
  // serializes after all readers; cache_map_ rehash therefore cannot occur
  // while we hold `iter`. The QueryCache constructor additionally caps the
  // load factor at 0.5 and pre-reserves buckets, so steady-state inserts
  // rarely rehash even in isolation.
  //
  // If you change this function to release `lock` before using `iter`, you
  // reintroduce the iterator-validity use-after-free — copy the values you need out of
  // the entry first, then release the lock.
  std::shared_lock lock(mutex_);

  stats_.total_queries++;

  auto iter = cache_map_.find(key);
  if (iter == cache_map_.end()) {
    stats_.cache_misses++;
    stats_.cache_misses_not_found++;
    return record_miss();
  }

  // The MD5 digest is an index accelerator, not the cache identity. Compare
  // the canonical query before serving data so a collision fails closed.
  if (discriminator.has_value() && iter->second.first.metadata.cache_discriminator != *discriminator) {
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

  const CacheEntryIdentity entry_identity{key, iter->second.first.metadata.entry_generation};

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
          pending_expired_keys_.insert(entry_identity);
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
    metadata->entry_generation = entry.metadata.entry_generation;
    metadata->data_version = entry.metadata.data_version;
  }

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
          first_detection = pending_decompression_keys_.insert(entry_identity).second;
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

  // Publish access only after the payload decoded successfully. Reacquire a
  // shared lock and verify the generation because the entry may have been
  // erased/replaced while decompression ran outside mutex_.
  {
    std::shared_lock access_lock(mutex_);
    auto access_iter = cache_map_.find(key);
    if (access_iter != cache_map_.end() &&
        access_iter->second.first.metadata.entry_generation == entry_identity.generation) {
      access_iter->second.first.metadata.access_count.fetch_add(1, std::memory_order_relaxed);
      access_iter->second.first.metadata.accessed_since_refresh.store(true, std::memory_order_relaxed);
    }
  }

  // Incremented outside the shared lock (after decompression) to avoid holding
  // the lock during CPU-intensive work. This creates a brief window where
  // cache_hits + cache_misses may transiently exceed total_queries in a
  // concurrent Reset() scenario, which is acceptable for monitoring counters
  // No correctness invariant depends on exact counter consistency.
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

  {
    std::shared_lock lock(mutex_);
    if (cache_map_.find(key) != cache_map_.end()) {
      stats_.rejection_duplicate.fetch_add(1, std::memory_order_relaxed);
      return false;
    }
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
  // account for std::unordered_map node overhead in the per-entry total.
  // MemoryUsage() returns the heap footprint of the entry's payload but does
  // not include the map node header that emplace() will allocate.
  const size_t entry_memory = temp_entry.MemoryUsage() + kHashMapNodeOverhead;

  // Don't cache if entry is too large.
  // Safe without lock: max_memory_bytes_ is const after construction (no setter exists).
  if (entry_memory > max_memory_bytes_) {
    stats_.rejection_oversize.fetch_add(1, std::memory_order_relaxed);
    return false;
  }

  // Collect keys evicted to make room; eviction_callback_ must fire after we
  // release mutex_ to avoid lock-order inversion with InvalidationManager
  //.
  std::vector<CacheEntryIdentity> evicted_entries;

  {
    // Exclusive lock for write
    std::unique_lock lock(mutex_);

    // Check if already exists
    if (cache_map_.find(key) != cache_map_.end()) {
      stats_.rejection_duplicate.fetch_add(1, std::memory_order_relaxed);
      return false;
    }

    // Evict entries if needed
    if (total_memory_bytes_ + entry_memory > max_memory_bytes_) {
      if (!EvictForSpace(entry_memory, &evicted_entries)) {
        // Fire callbacks for whatever was evicted before we bailed out — those
        // entries are gone from cache_map_ and InvalidationManager must learn
        // about them regardless of whether the new insert succeeded.
        lock.unlock();
        FireEvictionCallbacks(evicted_entries);
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
    const std::string table_name = temp_entry.metadata.table;

    // Insert into LRU list (front = most recent)
    lru_list_.push_front(key);
    auto lru_it = lru_list_.begin();

    // Insert into cache map using emplace to avoid copy
    cache_map_.emplace(key, std::make_pair(std::move(temp_entry), lru_it));
    table_to_cache_keys_[table_name].insert(key);

    // Update memory tracking
    total_memory_bytes_ += entry_memory;
    stats_.current_entries++;
    stats_.current_memory_bytes = total_memory_bytes_;
  }

  // Fire eviction callbacks AFTER releasing mutex_ to avoid lock-order inversion:
  // eviction_callback_ acquires InvalidationManager::mutex_, while
  // InvalidateAffectedEntries acquires the locks in the opposite order.
  // Calling the callback under our unique_lock would establish the inverse
  // ordering and risk deadlock.
  FireEvictionCallbacks(evicted_entries);

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

bool QueryCache::MarkInvalidated(const CacheEntryIdentity& identity) {
  std::shared_lock lock(mutex_);
  auto iter = cache_map_.find(identity.key);
  if (iter == cache_map_.end() || iter->second.first.metadata.entry_generation != identity.generation) {
    return false;
  }
  iter->second.first.invalidated.store(true);
  stats_.invalidations_immediate++;
  return true;
}

bool QueryCache::Erase(const CacheKey& key) {
  std::optional<CacheEntryIdentity> removed_identity;
  {
    std::unique_lock lock(mutex_);

    auto iter = cache_map_.find(key);
    if (iter == cache_map_.end()) {
      return false;
    }

    // defer eviction callback until after we release mutex_. The
    // callback typically takes InvalidationManager::mutex_, and the reverse
    // order is taken by InvalidateAffectedEntries -> MarkInvalidated; firing
    // the callback while holding our unique_lock would risk deadlock.
    if (identity_eviction_callback_ || eviction_callback_) {
      removed_identity = CacheEntryIdentity{key, iter->second.first.metadata.entry_generation};
    }

    // Remove from LRU list
    lru_list_.erase(iter->second.second);

    // Update memory tracking. Mirror Insert() by including the map node
    // overhead in the per-entry decrement so total_memory_bytes_
    // stays in sync.
    const size_t entry_memory = iter->second.first.MemoryUsage() + kHashMapNodeOverhead;
    total_memory_bytes_ -= entry_memory;
    stats_.current_entries--;
    stats_.current_memory_bytes = total_memory_bytes_;
    stats_.stale_entry_removals++;
    RemoveTableIndexEntryLocked(key, iter->second.first.metadata.table);

    // Remove from cache map
    cache_map_.erase(iter);
  }

  // callers that want to suppress this callback (the InvalidationQueue
  // cleanup path performs its own UnregisterCacheEntry and must not
  // double-unregister) should use EraseWithoutCallback() instead.
  if (removed_identity.has_value()) {
    FireEvictionCallbacks({*removed_identity});
  }

  return true;
}

bool QueryCache::Erase(const CacheEntryIdentity& identity) {
  bool fire_callback = false;
  {
    std::unique_lock lock(mutex_);
    auto iter = cache_map_.find(identity.key);
    if (iter == cache_map_.end() || iter->second.first.metadata.entry_generation != identity.generation) {
      return false;
    }
    fire_callback = static_cast<bool>(identity_eviction_callback_) || static_cast<bool>(eviction_callback_);
    lru_list_.erase(iter->second.second);
    const size_t entry_memory = iter->second.first.MemoryUsage() + kHashMapNodeOverhead;
    total_memory_bytes_ -= entry_memory;
    stats_.current_entries--;
    stats_.current_memory_bytes = total_memory_bytes_;
    stats_.stale_entry_removals++;
    RemoveTableIndexEntryLocked(identity.key, iter->second.first.metadata.table);
    cache_map_.erase(iter);
  }
  if (fire_callback) {
    FireEvictionCallbacks({identity});
  }
  return true;
}

bool QueryCache::EraseWithoutCallback(const CacheKey& key) {
  std::unique_lock lock(mutex_);

  auto iter = cache_map_.find(key);
  if (iter == cache_map_.end()) {
    return false;
  }

  // deliberately does NOT invoke eviction_callback_. The
  // InvalidationQueue cleanup path performs its own InvalidationManager
  // unregister and must not double-unregister via the callback, which would
  // otherwise race with concurrent Insert and corrupt the reverse indexes
  // (table_to_cache_keys_, ngram_to_cache_keys_).

  // Remove from LRU list
  lru_list_.erase(iter->second.second);

  // Update memory tracking. Symmetric with Insert(): include kHashMapNodeOverhead
  // so total_memory_bytes_ stays in sync.
  const size_t entry_memory = iter->second.first.MemoryUsage() + kHashMapNodeOverhead;
  total_memory_bytes_ -= entry_memory;
  stats_.current_entries--;
  stats_.current_memory_bytes = total_memory_bytes_;
  stats_.invalidations_deferred++;
  RemoveTableIndexEntryLocked(key, iter->second.first.metadata.table);

  // Remove from cache map
  cache_map_.erase(iter);

  return true;
}

bool QueryCache::EraseWithoutCallback(const CacheEntryIdentity& identity) {
  std::unique_lock lock(mutex_);
  auto iter = cache_map_.find(identity.key);
  if (iter == cache_map_.end() || iter->second.first.metadata.entry_generation != identity.generation) {
    return false;
  }
  lru_list_.erase(iter->second.second);
  const size_t entry_memory = iter->second.first.MemoryUsage() + kHashMapNodeOverhead;
  total_memory_bytes_ -= entry_memory;
  stats_.current_entries--;
  stats_.current_memory_bytes = total_memory_bytes_;
  stats_.invalidations_deferred++;
  RemoveTableIndexEntryLocked(identity.key, iter->second.first.metadata.table);
  cache_map_.erase(iter);
  return true;
}

void QueryCache::Clear() {
  // collect keys under the lock and fire eviction callbacks AFTER
  // releasing the lock. This:
  //   (1) avoids QueryCache::mutex_ -> InvalidationManager::mutex_ acquisition
  //       order while InvalidateAffectedEntries takes them in reverse, and
  //   (2) lets the BatchEvictionCallback path acquire InvalidationManager::mutex_
  //       exactly once instead of N times when an external observer is wired up
  //       (e.g. CacheManager -> InvalidationManager).
  std::vector<CacheEntryIdentity> evicted_entries;
  {
    std::unique_lock lock(mutex_);

    // Capture keys before swap. Callback delivery preserves insertion order
    // observed at swap time; ordering is informational because the per-key
    // unregister is independent.
    if (eviction_callback_ || batch_eviction_callback_ || identity_eviction_callback_ ||
        batch_identity_eviction_callback_) {
      evicted_entries.reserve(cache_map_.size());
      for (const auto& [key, entry_pair] : cache_map_) {
        evicted_entries.push_back(CacheEntryIdentity{key, entry_pair.first.metadata.entry_generation});
      }
    }

    // Swap with empty containers to release allocated capacity
    decltype(lru_list_)().swap(lru_list_);
    decltype(cache_map_)().swap(cache_map_);
    decltype(table_to_cache_keys_)().swap(table_to_cache_keys_);
    refresh_cursor_key_.reset();
    total_memory_bytes_ = 0;
    stats_.current_entries = 0;
    stats_.current_memory_bytes = 0;
    // Count this whole-cache clear as a single forced_clears event (operator-
    // initiated bulk eviction), regardless of how many entries were resident.
    stats_.forced_clears.fetch_add(1, std::memory_order_relaxed);
  }

  FireEvictionCallbacks(evicted_entries);
}

void QueryCache::ClearTable(const std::string& table) {
  // collect evicted keys under the lock, fire callbacks after.
  std::vector<CacheEntryIdentity> evicted_entries;
  {
    std::unique_lock lock(mutex_);

    std::vector<CacheKey> to_erase;
    auto table_iter = table_to_cache_keys_.find(table);
    if (table_iter != table_to_cache_keys_.end()) {
      to_erase.reserve(table_iter->second.size());
      for (const auto& key : table_iter->second) {
        to_erase.push_back(key);
      }
    }

    // Erase entries; RemoveEntryLocked appends each removed key to evicted_keys
    // so the eviction callback can fire after we release the lock.
    for (const auto& key : to_erase) {
      auto iter = cache_map_.find(key);
      if (iter != cache_map_.end()) {
        RemoveEntryLocked(iter, RemovalReason::kTableClear, &evicted_entries);
      }
    }
    stats_.current_memory_bytes = total_memory_bytes_;
    // Count this per-table clear as a single forced_clears event regardless of
    // how many entries actually matched the table. This matches Clear()'s
    // bulk-operation accounting.
    stats_.forced_clears.fetch_add(1, std::memory_order_relaxed);
  }

  FireEvictionCallbacks(evicted_entries);
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
  constexpr size_t kCorruptOriginalSize = mygram::constants::kBytesPerMegabyte;

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

size_t QueryCache::MemoryUsage() const {
  size_t total = 0;
  {
    std::shared_lock lock(mutex_);
    total = total_memory_bytes_;
    total += cache_map_.bucket_count() * sizeof(void*);
    total += lru_list_.size() * (sizeof(CacheKey) + (2 * sizeof(void*)));

    total += table_to_cache_keys_.bucket_count() * sizeof(void*);
    for (const auto& [table, keys] : table_to_cache_keys_) {
      total += sizeof(std::string) + table.capacity() + sizeof(void*) + sizeof(size_t);
      total += keys.bucket_count() * sizeof(void*);
      total += keys.size() * (sizeof(CacheKey) + sizeof(void*) + sizeof(size_t));
    }
  }

  {
    std::lock_guard<std::mutex> pending_lock(expired_keys_mutex_);
    total += pending_expired_keys_.bucket_count() * sizeof(void*);
    total += pending_decompression_keys_.bucket_count() * sizeof(void*);
    total += (pending_expired_keys_.size() + pending_decompression_keys_.size()) *
             (sizeof(CacheEntryIdentity) + sizeof(void*) + sizeof(size_t));
  }
  return total;
}

bool QueryCache::EvictLeastRecentlyUsed() {
  std::vector<CacheEntryIdentity> evicted_entries;
  {
    std::unique_lock lock(mutex_);
    while (!lru_list_.empty()) {
      const CacheKey key = lru_list_.back();
      auto iter = cache_map_.find(key);
      if (iter == cache_map_.end()) {
        lru_list_.pop_back();
        stats_.stale_lru_entries.fetch_add(1, std::memory_order_relaxed);
        continue;
      }
      RemoveEntryLocked(iter, RemovalReason::kLRUEviction, &evicted_entries);
      break;
    }
  }
  FireEvictionCallbacks(evicted_entries);
  return !evicted_entries.empty();
}

bool QueryCache::EvictForSpace(size_t required_bytes, std::vector<CacheEntryIdentity>* evicted_entries) {
  // Evict from the LRU tail until enough space is available, then finish the
  // configured batch. Batching amortizes reverse-index callback/locking costs
  // during sustained memory pressure.
  size_t removed_count = 0;
  while ((total_memory_bytes_ + required_bytes > max_memory_bytes_ || removed_count < eviction_batch_size_) &&
         !lru_list_.empty()) {
    // Get least recently used key
    const CacheKey lru_key = lru_list_.back();

    auto iter = cache_map_.find(lru_key);
    if (iter == cache_map_.end()) {
      // stale LRU entry — present in lru_list_ but missing from cache_map_.
      // This violates the cache_map_ <-> lru_list_ invariant maintained by
      // Insert/RemoveEntryLocked/Erase paths. EvictForSpace is defensive and
      // simply pops the dangling key, but if this happens repeatedly it
      // indicates a bug elsewhere that must be investigated. We emit a warning
      // log per occurrence and bump a counter for fleet-level monitoring.
      stats_.stale_lru_entries.fetch_add(1, std::memory_order_relaxed);
      mygram::utils::StructuredLog()
          .Event("query_cache_stale_lru")
          .Field("key_hash_high", lru_key.hash_high)
          .Field("key_hash_low", lru_key.hash_low)
          .Field("required_bytes", static_cast<uint64_t>(required_bytes))
          .Field("total_memory_bytes", static_cast<uint64_t>(total_memory_bytes_))
          .Message("stale LRU entry: present in lru_list_ but missing from cache_map_")
          .Warn();
      lru_list_.pop_back();
      continue;
    }

    RemoveEntryLocked(iter, RemovalReason::kLRUEviction, evicted_entries);
    ++removed_count;
  }

  stats_.current_memory_bytes = total_memory_bytes_;

  // Check if enough space was freed
  return total_memory_bytes_ + required_bytes <= max_memory_bytes_;
}

void QueryCache::RemoveEntryLocked(decltype(cache_map_)::iterator iter, RemovalReason reason,
                                   std::vector<CacheEntryIdentity>* evicted_entries) {
  const CacheKey& key = iter->first;

  // do NOT invoke eviction_callback_ here. The callback typically takes
  // InvalidationManager::mutex_ and we hold QueryCache::mutex_ — calling it
  // inline establishes the inverse of the (IM.mutex_ -> QueryCache.mutex_)
  // ordering used by InvalidationManager::InvalidateAffectedEntries and risks
  // deadlock. Callers append `key` to @p evicted_keys and call
  // FireEvictionCallbacks() AFTER releasing mutex_.
  if (evicted_entries != nullptr) {
    evicted_entries->push_back(CacheEntryIdentity{key, iter->second.first.metadata.entry_generation});
  }

  // Remove from LRU list
  lru_list_.erase(iter->second.second);

  // Update memory tracking. Sync the public-facing stats_.current_memory_bytes
  // immediately so GetStatistics() never returns a value that is stale (higher
  // than reality) between RemoveEntryLocked and the next RefreshLRU resync.
  // Symmetric with Insert: include kHashMapNodeOverhead so total_memory_bytes_
  // tracks the same accounting unit.
  const size_t entry_memory = iter->second.first.MemoryUsage() + kHashMapNodeOverhead;
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
    case RemovalReason::kInvalidated:
      stats_.invalidations_deferred++;
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

  RemoveTableIndexEntryLocked(key, iter->second.first.metadata.table);

  // Remove from cache map
  cache_map_.erase(iter);
}

void QueryCache::RemoveTableIndexEntryLocked(const CacheKey& key, const std::string& table) {
  auto table_iter = table_to_cache_keys_.find(table);
  if (table_iter == table_to_cache_keys_.end()) {
    return;
  }
  table_iter->second.erase(key);
  if (table_iter->second.empty()) {
    table_to_cache_keys_.erase(table_iter);
  }
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

void QueryCache::RefreshLRU() {
  // Drain pending keys from Lookup() before acquiring main lock
  std::unordered_set<CacheEntryIdentity> lookup_expired_entries;
  std::unordered_set<CacheEntryIdentity> decomp_failed_entries;
  {
    std::lock_guard<std::mutex> expired_lock(expired_keys_mutex_);
    lookup_expired_entries.swap(pending_expired_keys_);
    decomp_failed_entries.swap(pending_decompression_keys_);
  }

  // collect evicted keys under the main lock and fire the
  // eviction callback after we release it.
  std::vector<CacheEntryIdentity> evicted_entries;
  {
    std::unique_lock lock(mutex_);

    auto now = std::chrono::steady_clock::now();
    int current_ttl = ttl_seconds_.load(std::memory_order_relaxed);

    // Track keys detected as expired during Lookup (stats already counted)
    // Scan-detected expired keys will be collected separately
    std::unordered_set<CacheEntryIdentity> scan_expired_entries;
    std::unordered_set<CacheEntryIdentity> invalidated_entries;

    // Update LRU for entries that were accessed since last refresh.
    //
    // Walk the LRU list rather than the hash map so that the scan can stop
    // after a bounded slice and resume from the same place on the next tick.
    // std::list iterators are stable across insertions and unrelated erasures,
    // but the resume point is stored as a key so that erasing it simply
    // restarts the cycle instead of leaving a dangling iterator.
    const size_t slice_size = RefreshSliceSizeForTesting(cache_map_.size());

    auto cursor = lru_list_.end();
    if (refresh_cursor_key_.has_value()) {
      auto resume_iter = cache_map_.find(*refresh_cursor_key_);
      if (resume_iter != cache_map_.end()) {
        cursor = resume_iter->second.second;
      }
    }
    if (cursor == lru_list_.end()) {
      cursor = lru_list_.begin();
    }

    size_t visited = 0;
    while (cursor != lru_list_.end() && visited < slice_size) {
      const CacheKey key = *cursor;
      // Advance before Touch() can splice this node to the front.
      ++cursor;
      ++visited;

      auto entry_iter = cache_map_.find(key);
      if (entry_iter == cache_map_.end()) {
        continue;  // Stale LRU node; EvictForSpace reports and pops these
      }
      auto& entry = entry_iter->second.first;

      const CacheEntryIdentity identity{key, entry.metadata.entry_generation};
      if (entry.invalidated.load(std::memory_order_relaxed)) {
        invalidated_entries.insert(identity);
        continue;
      }

      // Check TTL expiration
      if (current_ttl > 0) {
        auto age = std::chrono::duration_cast<std::chrono::seconds>(now - entry.metadata.created_at).count();
        if (age >= current_ttl) {
          // Only add to scan set if not already detected by Lookup
          if (lookup_expired_entries.find(identity) == lookup_expired_entries.end()) {
            scan_expired_entries.insert(identity);
          }
          continue;  // Skip LRU update for expired entries
        }
      }

      if (entry.metadata.accessed_since_refresh.exchange(false, std::memory_order_relaxed)) {
        // Entry was accessed, move to front of LRU list
        Touch(key);
        entry.metadata.last_accessed = now;
      }
    }
    refresh_cursor_key_ = cursor == lru_list_.end() ? std::nullopt : std::optional<CacheKey>(*cursor);

    // Queue overflow can drop deferred Step 2 erasure. The invalidated flag is
    // authoritative, so the periodic scan purges such entries instead of
    // leaving a permanently-missing duplicate key resident until TTL expiry.
    for (const auto& identity : invalidated_entries) {
      auto iter = cache_map_.find(identity.key);
      if (iter != cache_map_.end() && iter->second.first.metadata.entry_generation == identity.generation) {
        RemoveEntryLocked(iter, RemovalReason::kInvalidated, &evicted_entries);
      }
    }

    // Remove Lookup-detected expired entries (stats already counted by Lookup)
    for (const auto& identity : lookup_expired_entries) {
      auto iter = cache_map_.find(identity.key);
      if (iter != cache_map_.end() && iter->second.first.metadata.entry_generation == identity.generation) {
        RemoveEntryLocked(iter, RemovalReason::kTTLExpiredAlreadyCounted, &evicted_entries);
      }
    }

    // Remove scan-detected expired entries (stats not yet counted)
    for (const auto& identity : scan_expired_entries) {
      auto iter = cache_map_.find(identity.key);
      if (iter != cache_map_.end() && iter->second.first.metadata.entry_generation == identity.generation) {
        RemoveEntryLocked(iter, RemovalReason::kTTLExpired, &evicted_entries);
      }
    }

    // Remove decompression-failed entries (stats already counted by Lookup)
    for (const auto& identity : decomp_failed_entries) {
      auto iter = cache_map_.find(identity.key);
      if (iter != cache_map_.end() && iter->second.first.metadata.entry_generation == identity.generation) {
        RemoveEntryLocked(iter, RemovalReason::kDecompressionFailureAlreadyCounted, &evicted_entries);
      }
    }

    // Defensive resync: RemoveEntryLocked / Insert / Erase all keep
    // stats_.current_memory_bytes in sync with total_memory_bytes_ on each
    // mutation, so this assignment is normally a no-op. Kept as a belt-and-
    // suspenders safety net in case a future code path bumps total_memory_bytes_
    // without updating stats_.
    stats_.current_memory_bytes.store(total_memory_bytes_, std::memory_order_relaxed);
  }

  FireEvictionCallbacks(evicted_entries);
}

void QueryCache::FireEvictionCallbacks(const std::vector<CacheEntryIdentity>& entries) {
  if (entries.empty()) {
    return;
  }

  // Prefer the batch callback when wired up: it lets observers acquire
  // their own mutex once instead of N times. Falls back to the per-key callback
  // for backward compatibility with callers that only set EvictionCallback.
  if (batch_identity_eviction_callback_) {
    batch_identity_eviction_callback_(entries);
    return;
  }

  if (identity_eviction_callback_) {
    for (const auto& identity : entries) {
      identity_eviction_callback_(identity);
    }
    return;
  }

  std::vector<CacheKey> keys;
  keys.reserve(entries.size());
  for (const auto& identity : entries) {
    keys.push_back(identity.key);
  }

  if (batch_eviction_callback_) {
    batch_eviction_callback_(keys);
    return;
  }

  if (eviction_callback_) {
    for (const auto& key : keys) {
      eviction_callback_(key);
    }
  }
}

}  // namespace mygramdb::cache
