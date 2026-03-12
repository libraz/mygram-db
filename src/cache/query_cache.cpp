/**
 * @file query_cache.cpp
 * @brief Query cache implementation
 */

#include "cache/query_cache.h"

#include <algorithm>
#include <chrono>

namespace mygramdb::cache {

QueryCache::QueryCache(size_t max_memory_bytes, double min_query_cost_ms, int ttl_seconds)
    : max_memory_bytes_(max_memory_bytes), min_query_cost_ms_(min_query_cost_ms), ttl_seconds_(ttl_seconds) {
  // Start background LRU refresh thread
  lru_refresh_thread_ = std::thread(&QueryCache::RefreshLRUWorker, this);
}

QueryCache::~QueryCache() {
  // Stop background LRU refresh thread
  should_stop_.store(true);
  if (lru_refresh_thread_.joinable()) {
    lru_refresh_thread_.join();
  }
}

std::optional<std::vector<DocId>> QueryCache::Lookup(const CacheKey& key) {
  // Start timing
  auto start_time = std::chrono::high_resolution_clock::now();

  // Shared lock for read
  std::shared_lock lock(mutex_);

  stats_.total_queries++;

  auto iter = cache_map_.find(key);
  if (iter == cache_map_.end()) {
    stats_.cache_misses++;
    stats_.cache_misses_not_found++;

    // Record miss latency
    auto end_time = std::chrono::high_resolution_clock::now();
    double miss_time_ms = std::chrono::duration<double, std::milli>(end_time - start_time).count();
    {
      std::lock_guard<std::mutex> timing_lock(stats_.timing_mutex_);
      stats_.total_cache_miss_time_ms += miss_time_ms;
    }

    return std::nullopt;
  }

  // Check invalidation flag
  if (iter->second.first.invalidated.load()) {
    stats_.cache_misses++;
    stats_.cache_misses_invalidated++;

    // Record miss latency
    auto end_time = std::chrono::high_resolution_clock::now();
    double miss_time_ms = std::chrono::duration<double, std::milli>(end_time - start_time).count();
    {
      std::lock_guard<std::mutex> timing_lock(stats_.timing_mutex_);
      stats_.total_cache_miss_time_ms += miss_time_ms;
    }

    return std::nullopt;
  }

  // Check TTL expiration (if TTL is enabled)
  if (ttl_seconds_ > 0) {
    const auto& entry = iter->second.first;
    auto now = std::chrono::steady_clock::now();
    auto age = std::chrono::duration_cast<std::chrono::seconds>(now - entry.metadata.created_at).count();
    if (age >= ttl_seconds_) {
      // Entry expired - enqueue for cleanup by RefreshLRU
      {
        std::lock_guard<std::mutex> expired_lock(expired_keys_mutex_);
        pending_expired_keys_.push_back(key);
      }

      stats_.cache_misses++;
      stats_.cache_misses_not_found++;  // Treat expired as not found

      // Record miss latency
      auto end_time = std::chrono::high_resolution_clock::now();
      double miss_time_ms = std::chrono::duration<double, std::milli>(end_time - start_time).count();
      {
        std::lock_guard<std::mutex> timing_lock(stats_.timing_mutex_);
        stats_.total_cache_miss_time_ms += miss_time_ms;
      }

      return std::nullopt;
    }
  }

  // Cache hit
  stats_.cache_hits++;

  // Copy compressed data and metadata while holding lock
  const auto& entry = iter->second.first;
  std::vector<uint8_t> compressed_copy = entry.compressed;
  const size_t original_size = entry.original_size;
  const double query_cost_ms = entry.query_cost_ms;

  // Lock-free access tracking (no lock upgrade needed)
  // Atomic increment of access count and set dirty flag for background LRU refresh
  iter->second.first.metadata.access_count.fetch_add(1, std::memory_order_relaxed);
  iter->second.first.metadata.accessed_since_refresh.store(true, std::memory_order_relaxed);

  // Release shared lock before decompression
  lock.unlock();

  // Decompress outside lock to reduce shared_lock hold time
  std::vector<DocId> result;
  try {
    result = ResultCompressor::Decompress(compressed_copy, original_size);
  } catch (const std::exception& e) {
    // Decompression failed, treat as miss
    stats_.cache_misses++;

    auto end_time = std::chrono::high_resolution_clock::now();
    double miss_time_ms = std::chrono::duration<double, std::milli>(end_time - start_time).count();
    {
      std::lock_guard<std::mutex> timing_lock(stats_.timing_mutex_);
      stats_.total_cache_miss_time_ms += miss_time_ms;
    }

    return std::nullopt;
  }

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

std::optional<std::vector<DocId>> QueryCache::LookupWithMetadata(const CacheKey& key, LookupMetadata& metadata) {
  // Start timing
  auto start_time = std::chrono::high_resolution_clock::now();

  // Shared lock for read
  std::shared_lock lock(mutex_);

  stats_.total_queries++;

  auto iter = cache_map_.find(key);
  if (iter == cache_map_.end()) {
    stats_.cache_misses++;
    stats_.cache_misses_not_found++;

    // Record miss latency
    auto end_time = std::chrono::high_resolution_clock::now();
    double miss_time_ms = std::chrono::duration<double, std::milli>(end_time - start_time).count();
    {
      std::lock_guard<std::mutex> timing_lock(stats_.timing_mutex_);
      stats_.total_cache_miss_time_ms += miss_time_ms;
    }

    return std::nullopt;
  }

  // Check invalidation flag
  if (iter->second.first.invalidated.load()) {
    stats_.cache_misses++;
    stats_.cache_misses_invalidated++;

    // Record miss latency
    auto end_time = std::chrono::high_resolution_clock::now();
    double miss_time_ms = std::chrono::duration<double, std::milli>(end_time - start_time).count();
    {
      std::lock_guard<std::mutex> timing_lock(stats_.timing_mutex_);
      stats_.total_cache_miss_time_ms += miss_time_ms;
    }

    return std::nullopt;
  }

  // Check TTL expiration (if TTL is enabled)
  if (ttl_seconds_ > 0) {
    const auto& entry = iter->second.first;
    auto now = std::chrono::steady_clock::now();
    auto age = std::chrono::duration_cast<std::chrono::seconds>(now - entry.metadata.created_at).count();
    if (age >= ttl_seconds_) {
      // Entry expired - enqueue for cleanup by RefreshLRU
      {
        std::lock_guard<std::mutex> expired_lock(expired_keys_mutex_);
        pending_expired_keys_.push_back(key);
      }

      stats_.cache_misses++;
      stats_.cache_misses_not_found++;  // Treat expired as not found

      // Record miss latency
      auto end_time = std::chrono::high_resolution_clock::now();
      double miss_time_ms = std::chrono::duration<double, std::milli>(end_time - start_time).count();
      {
        std::lock_guard<std::mutex> timing_lock(stats_.timing_mutex_);
        stats_.total_cache_miss_time_ms += miss_time_ms;
      }

      return std::nullopt;
    }
  }

  // Cache hit
  stats_.cache_hits++;

  // Copy compressed data and metadata while holding lock
  const auto& entry = iter->second.first;
  std::vector<uint8_t> compressed_copy = entry.compressed;
  const size_t original_size = entry.original_size;
  metadata.query_cost_ms = entry.query_cost_ms;
  metadata.created_at = entry.metadata.created_at;

  // Lock-free access tracking (no lock upgrade needed)
  // Atomic increment of access count and set dirty flag for background LRU refresh
  iter->second.first.metadata.access_count.fetch_add(1, std::memory_order_relaxed);
  iter->second.first.metadata.accessed_since_refresh.store(true, std::memory_order_relaxed);

  // Release shared lock before decompression
  lock.unlock();

  // Decompress outside lock to reduce shared_lock hold time
  std::vector<DocId> result;
  try {
    result = ResultCompressor::Decompress(compressed_copy, original_size);
  } catch (const std::exception& e) {
    // Decompression failed, treat as miss
    stats_.cache_misses++;

    auto end_time = std::chrono::high_resolution_clock::now();
    double miss_time_ms = std::chrono::duration<double, std::milli>(end_time - start_time).count();
    {
      std::lock_guard<std::mutex> timing_lock(stats_.timing_mutex_);
      stats_.total_cache_miss_time_ms += miss_time_ms;
    }

    return std::nullopt;
  }

  // Record hit latency and saved time
  auto end_time = std::chrono::high_resolution_clock::now();
  double hit_time_ms = std::chrono::duration<double, std::milli>(end_time - start_time).count();
  {
    std::lock_guard<std::mutex> timing_lock(stats_.timing_mutex_);
    stats_.total_cache_hit_time_ms += hit_time_ms;
    stats_.total_query_saved_time_ms += metadata.query_cost_ms;  // Time saved by not re-executing
  }

  return result;
}

bool QueryCache::Insert(const CacheKey& key, const std::vector<DocId>& result, const CacheMetadata& metadata,
                        double query_cost_ms) {
  // Check if query cost meets threshold
  if (query_cost_ms < min_query_cost_ms_) {
    return false;
  }

  // Compress result
  std::vector<uint8_t> compressed;
  try {
    compressed = ResultCompressor::Compress(result);
  } catch (const std::exception& e) {
    return false;
  }

  // Create cache entry to calculate accurate memory usage
  CacheEntry temp_entry;
  temp_entry.compressed = std::move(compressed);
  temp_entry.metadata = metadata;

  const size_t original_count = result.size();  // Number of DocId elements, not bytes
  const size_t compressed_size = temp_entry.compressed.size();
  const size_t entry_memory = temp_entry.MemoryUsage();

  // Don't cache if entry is too large
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
  // Snapshot consistency with non-atomic stats fields is not required here.
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

  lru_list_.clear();
  cache_map_.clear();
  total_memory_bytes_ = 0;
  stats_.current_entries = 0;
  stats_.current_memory_bytes = 0;
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
      // Notify eviction callback BEFORE deletion
      // This allows the callback to access entry data if needed
      if (eviction_callback_) {
        eviction_callback_(key);
      }

      // Remove entry (after callback has been notified)
      lru_list_.erase(iter->second.second);
      const size_t entry_memory = iter->second.first.MemoryUsage();
      total_memory_bytes_ -= entry_memory;
      stats_.current_entries--;
      stats_.current_memory_bytes = total_memory_bytes_;
      cache_map_.erase(iter);
    }
  }
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

    // Notify eviction callback BEFORE deletion
    // This allows the callback to access entry data if needed
    if (eviction_callback_) {
      eviction_callback_(lru_key);
    }

    // Remove entry (after callback has been notified)
    const size_t entry_memory = iter->second.first.MemoryUsage();
    lru_list_.pop_back();
    cache_map_.erase(iter);

    // Update memory tracking
    total_memory_bytes_ -= entry_memory;
    stats_.current_entries--;
    stats_.evictions++;
  }

  stats_.current_memory_bytes = total_memory_bytes_;

  // Check if enough space was freed
  return total_memory_bytes_ + required_bytes <= max_memory_bytes_;
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
  while (!should_stop_.load()) {
    // Sleep for 100ms between refresh cycles (10 Hz)
    // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers) - Sleep interval
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    if (should_stop_.load()) {
      break;
    }

    RefreshLRU();
  }
}

void QueryCache::RefreshLRU() {
  // Drain pending expired keys from Lookup() before acquiring main lock
  std::vector<CacheKey> lookup_expired_keys;
  {
    std::lock_guard<std::mutex> expired_lock(expired_keys_mutex_);
    lookup_expired_keys.swap(pending_expired_keys_);
  }

  std::unique_lock lock(mutex_);

  auto now = std::chrono::steady_clock::now();

  // Start with keys detected as expired during Lookup
  std::vector<CacheKey> expired_keys = std::move(lookup_expired_keys);

  // Update LRU for entries that were accessed since last refresh
  for (auto& [key, entry_pair] : cache_map_) {
    // Check TTL expiration
    if (ttl_seconds_ > 0) {
      auto age = std::chrono::duration_cast<std::chrono::seconds>(now - entry_pair.first.metadata.created_at).count();
      if (age >= ttl_seconds_) {
        expired_keys.push_back(key);
        continue;  // Skip LRU update for expired entries
      }
    }

    if (entry_pair.first.metadata.accessed_since_refresh.exchange(false, std::memory_order_relaxed)) {
      // Entry was accessed, move to front of LRU list
      Touch(key);
      entry_pair.first.metadata.last_accessed = now;
    }
  }

  // Remove expired entries
  for (const auto& key : expired_keys) {
    auto iter = cache_map_.find(key);
    if (iter != cache_map_.end()) {
      // Notify eviction callback before deletion
      if (eviction_callback_) {
        eviction_callback_(key);
      }

      lru_list_.erase(iter->second.second);
      const size_t entry_memory = iter->second.first.MemoryUsage();
      total_memory_bytes_ -= entry_memory;
      stats_.current_entries--;
      stats_.evictions++;
      cache_map_.erase(iter);
    }
  }

  // Always sync stats with actual memory tracking
  stats_.current_memory_bytes = total_memory_bytes_;
}

}  // namespace mygramdb::cache
