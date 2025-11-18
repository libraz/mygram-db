/**
 * @file cache_entry.h
 * @brief Cache entry structure with metadata for invalidation
 */

#pragma once

#include <atomic>
#include <chrono>
#include <set>
#include <string>
#include <vector>

#include "query/cache_key.h"
#include "query/query_parser.h"

namespace mygramdb::cache {

/**
 * @brief Metadata for cache entry invalidation tracking
 *
 * Stores information needed to determine when a cache entry should be invalidated.
 * This includes ngrams used in the query, which enables fine-grained invalidation
 * based on data changes.
 */
struct CacheMetadata {
  CacheKey key;                                         ///< Cache key (MD5 hash)
  std::string table;                                    ///< Table name
  std::set<std::string> ngrams;                         ///< All ngrams used in this query
  std::vector<query::FilterCondition> filters;          ///< Filter conditions (for future optimization)
  std::chrono::steady_clock::time_point created_at;     ///< Creation time
  std::chrono::steady_clock::time_point last_accessed;  ///< Last access time
  uint32_t access_count = 0;                            ///< Number of times accessed
};

/**
 * @brief Cache entry containing compressed results and metadata
 *
 * Stores the compressed search results along with metadata for tracking,
 * eviction, and invalidation decisions.
 */
struct CacheEntry {
  CacheKey key;                          ///< Cache key (16 bytes)
  std::vector<uint8_t> compressed;       ///< LZ4-compressed result
  size_t original_size = 0;              ///< Uncompressed size (bytes)
  size_t compressed_size = 0;            ///< Compressed size (bytes)
  double query_cost_ms = 0.0;            ///< Query execution time (ms)
  CacheMetadata metadata;                ///< Metadata for invalidation
  std::atomic<bool> invalidated{false};  ///< Invalidation flag (for two-phase invalidation)

  // Default constructor
  CacheEntry() = default;

  // Copy constructor (atomic must be loaded/stored explicitly)
  CacheEntry(const CacheEntry& other)
      : key(other.key),
        compressed(other.compressed),
        original_size(other.original_size),
        compressed_size(other.compressed_size),
        query_cost_ms(other.query_cost_ms),
        metadata(other.metadata),
        invalidated(other.invalidated.load()) {}

  // Move constructor
  CacheEntry(CacheEntry&& other) noexcept
      : key(other.key),  // CacheKey is trivially copyable
        compressed(std::move(other.compressed)),
        original_size(other.original_size),
        compressed_size(other.compressed_size),
        query_cost_ms(other.query_cost_ms),
        metadata(std::move(other.metadata)),
        invalidated(other.invalidated.load()) {}

  // Destructor
  ~CacheEntry() = default;

  // Copy assignment
  CacheEntry& operator=(const CacheEntry& other) {
    if (this != &other) {
      key = other.key;
      compressed = other.compressed;
      original_size = other.original_size;
      compressed_size = other.compressed_size;
      query_cost_ms = other.query_cost_ms;
      metadata = other.metadata;
      invalidated.store(other.invalidated.load());
    }
    return *this;
  }

  // Move assignment
  CacheEntry& operator=(CacheEntry&& other) noexcept {
    if (this != &other) {
      key = other.key;  // CacheKey is trivially copyable
      compressed = std::move(other.compressed);
      original_size = other.original_size;
      compressed_size = other.compressed_size;
      query_cost_ms = other.query_cost_ms;
      metadata = std::move(other.metadata);
      invalidated.store(other.invalidated.load());
    }
    return *this;
  }

  /**
   * @brief Calculate memory footprint of this entry
   * @return Memory usage in bytes
   */
  [[nodiscard]] size_t MemoryUsage() const {
    // Entry overhead + compressed data + ngrams
    size_t ngrams_size = 0;
    for (const auto& ngram : metadata.ngrams) {
      ngrams_size += ngram.capacity();
    }
    return sizeof(CacheEntry) + compressed.capacity() + ngrams_size;
  }
};

}  // namespace mygramdb::cache
