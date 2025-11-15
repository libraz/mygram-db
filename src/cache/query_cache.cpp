/**
 * @file query_cache.cpp
 * @brief Query cache implementation
 */

#include "cache/query_cache.h"

#include <algorithm>
#include <chrono>

namespace mygramdb::cache {

QueryCache::QueryCache(size_t max_memory_bytes, double min_query_cost_ms)
    : max_memory_bytes_(max_memory_bytes), min_query_cost_ms_(min_query_cost_ms) {}

std::optional<std::vector<DocId>> QueryCache::Lookup(const CacheKey& key) {
  // Start timing
  auto start_time = std::chrono::high_resolution_clock::now();

  stats_.total_queries++;

  // Shared lock for read
  std::shared_lock lock(mutex_);

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

  // Cache hit
  stats_.cache_hits++;

  // Decompress result and copy query_cost_ms before releasing lock
  const auto& entry = iter->second.first;
  std::vector<DocId> result;
  try {
    result = ResultCompressor::Decompress(entry.compressed, entry.original_size);
  } catch (const std::exception& e) {
    // Decompression failed, treat as miss
    stats_.cache_misses++;

    // Record miss latency
    auto end_time = std::chrono::high_resolution_clock::now();
    double miss_time_ms = std::chrono::duration<double, std::milli>(end_time - start_time).count();
    {
      std::lock_guard<std::mutex> timing_lock(stats_.timing_mutex_);
      stats_.total_cache_miss_time_ms += miss_time_ms;
    }

    return std::nullopt;
  }

  // Copy query_cost_ms before releasing lock to avoid use-after-free
  const double query_cost_ms = entry.query_cost_ms;

  // Update access time (need to upgrade to unique lock)
  lock.unlock();
  std::unique_lock write_lock(mutex_);

  // Re-check existence (might have been evicted)
  iter = cache_map_.find(key);
  if (iter != cache_map_.end()) {
    Touch(key);
    iter->second.first.metadata.last_accessed = std::chrono::steady_clock::now();
    iter->second.first.metadata.access_count++;

    // Record hit latency and saved time
    auto end_time = std::chrono::high_resolution_clock::now();
    double hit_time_ms = std::chrono::duration<double, std::milli>(end_time - start_time).count();
    {
      std::lock_guard<std::mutex> timing_lock(stats_.timing_mutex_);
      stats_.total_cache_hit_time_ms += hit_time_ms;
      stats_.total_query_saved_time_ms += query_cost_ms;
    }
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

  // Calculate memory usage
  const size_t original_count = result.size();  // Number of DocId elements, not bytes
  const size_t compressed_size = compressed.size();
  size_t ngrams_size = 0;
  for (const auto& ngram : metadata.ngrams) {
    ngrams_size += ngram.capacity();
  }
  const size_t entry_memory = sizeof(CacheEntry) + compressed_size + ngrams_size;

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

  // Create cache entry
  CacheEntry entry;
  entry.key = key;
  entry.compressed = std::move(compressed);
  entry.original_size = original_count;  // Store count, not bytes
  entry.compressed_size = compressed_size;
  entry.query_cost_ms = query_cost_ms;
  entry.metadata = metadata;
  entry.metadata.created_at = std::chrono::steady_clock::now();
  entry.metadata.last_accessed = entry.metadata.created_at;
  entry.invalidated.store(false);

  // Insert into LRU list (front = most recent)
  lru_list_.push_front(key);
  auto lru_it = lru_list_.begin();

  // Insert into cache map using emplace to avoid copy
  cache_map_.emplace(key, std::make_pair(std::move(entry), lru_it));

  // Update memory tracking
  total_memory_bytes_ += entry_memory;
  stats_.current_entries++;
  stats_.current_memory_bytes = total_memory_bytes_;

  return true;
}

bool QueryCache::MarkInvalidated(const CacheKey& key) {
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
      lru_list_.erase(iter->second.second);
      const size_t entry_memory = iter->second.first.MemoryUsage();
      total_memory_bytes_ -= entry_memory;
      stats_.current_entries--;
      cache_map_.erase(iter);
    }
  }

  stats_.current_memory_bytes = total_memory_bytes_;
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

    // Remove entry
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

}  // namespace mygramdb::cache
