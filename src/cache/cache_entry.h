/**
 * @file cache_entry.h
 * @brief Cache entry structure with metadata for invalidation
 */

#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
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
  std::vector<std::string> ngrams;                      ///< All ngrams used in this query (sorted)
  std::vector<query::FilterCondition> filters;          ///< Filter conditions (for future optimization)
  int ngram_size = 0;                                   ///< N-gram size used for this query's ngrams
  int kanji_ngram_size = 0;                             ///< Kanji N-gram size used for this query's ngrams
  bool cross_boundary_ngrams = true;                    ///< Cross-boundary setting used for this query's ngrams
  std::chrono::steady_clock::time_point created_at;     ///< Creation time
  std::chrono::steady_clock::time_point last_accessed;  ///< Last access time
  std::atomic<uint32_t> access_count{0};                ///< Number of times accessed (atomic for lock-free update)
  std::atomic<bool> accessed_since_refresh{false};      ///< Dirty flag for background LRU refresh

  // Default constructor
  CacheMetadata() = default;

  // Destructor
  ~CacheMetadata() = default;

  // Copy constructor (atomics must be loaded/stored explicitly)
  CacheMetadata(const CacheMetadata& other)
      : key(other.key),
        table(other.table),
        ngrams(other.ngrams),
        filters(other.filters),
        ngram_size(other.ngram_size),
        kanji_ngram_size(other.kanji_ngram_size),
        cross_boundary_ngrams(other.cross_boundary_ngrams),
        created_at(other.created_at),
        last_accessed(other.last_accessed),
        access_count(other.access_count.load()),
        accessed_since_refresh(other.accessed_since_refresh.load()) {}

  // Move constructor
  CacheMetadata(CacheMetadata&& other) noexcept
      : key(other.key),
        table(std::move(other.table)),
        ngrams(std::move(other.ngrams)),
        filters(std::move(other.filters)),
        ngram_size(other.ngram_size),
        kanji_ngram_size(other.kanji_ngram_size),
        cross_boundary_ngrams(other.cross_boundary_ngrams),
        created_at(other.created_at),
        last_accessed(other.last_accessed),
        access_count(other.access_count.load()),
        accessed_since_refresh(other.accessed_since_refresh.load()) {}

  // Copy assignment
  CacheMetadata& operator=(const CacheMetadata& other) {
    if (this != &other) {
      key = other.key;
      table = other.table;
      ngrams = other.ngrams;
      filters = other.filters;
      ngram_size = other.ngram_size;
      kanji_ngram_size = other.kanji_ngram_size;
      cross_boundary_ngrams = other.cross_boundary_ngrams;
      created_at = other.created_at;
      last_accessed = other.last_accessed;
      access_count.store(other.access_count.load());
      accessed_since_refresh.store(other.accessed_since_refresh.load());
    }
    return *this;
  }

  // Move assignment
  CacheMetadata& operator=(CacheMetadata&& other) noexcept {
    if (this != &other) {
      key = other.key;
      table = std::move(other.table);
      ngrams = std::move(other.ngrams);
      filters = std::move(other.filters);
      ngram_size = other.ngram_size;
      kanji_ngram_size = other.kanji_ngram_size;
      cross_boundary_ngrams = other.cross_boundary_ngrams;
      created_at = other.created_at;
      last_accessed = other.last_accessed;
      access_count.store(other.access_count.load());
      accessed_since_refresh.store(other.accessed_since_refresh.load());
    }
    return *this;
  }
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
  size_t original_element_count = 0;     ///< Number of DocId elements in the original result
  size_t compressed_size = 0;            ///< Compressed size (bytes)
  size_t stored_memory_footprint = 0;    ///< Memory footprint recorded at insertion time
  double query_cost_ms = 0.0;            ///< Query execution time (ms)
  CacheMetadata metadata;                ///< Metadata for invalidation
  std::atomic<bool> invalidated{false};  ///< Invalidation flag (for two-phase invalidation)

  // Default constructor
  CacheEntry() = default;

  // Copy constructor (atomic must be loaded/stored explicitly)
  CacheEntry(const CacheEntry& other)
      : key(other.key),
        compressed(other.compressed),
        original_element_count(other.original_element_count),
        compressed_size(other.compressed_size),
        stored_memory_footprint(other.stored_memory_footprint),
        query_cost_ms(other.query_cost_ms),
        metadata(other.metadata),
        invalidated(other.invalidated.load()) {}

  // Move constructor
  CacheEntry(CacheEntry&& other) noexcept
      : key(other.key),  // CacheKey is trivially copyable
        compressed(std::move(other.compressed)),
        original_element_count(other.original_element_count),
        compressed_size(other.compressed_size),
        stored_memory_footprint(other.stored_memory_footprint),
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
      original_element_count = other.original_element_count;
      compressed_size = other.compressed_size;
      stored_memory_footprint = other.stored_memory_footprint;
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
      original_element_count = other.original_element_count;
      compressed_size = other.compressed_size;
      stored_memory_footprint = other.stored_memory_footprint;
      query_cost_ms = other.query_cost_ms;
      metadata = std::move(other.metadata);
      invalidated.store(other.invalidated.load());
    }
    return *this;
  }

  /**
   * @brief Calculate heap-only memory footprint of this entry
   *
   * Returns only the heap allocations owned by this entry, without
   * counting sizeof(*this). Use this for cache memory tracking where
   * the entry struct is already stored in a container that accounts
   * for its inline size.
   *
   * @return Heap memory usage in bytes
   */
  [[nodiscard]] size_t HeapFootprint() const {
    // Compressed data heap allocation
    size_t size = compressed.capacity();

    // Ngrams: vector buffer + each string's heap allocation
    size += metadata.ngrams.capacity() * sizeof(std::string);
    for (const auto& ngram : metadata.ngrams) {
      size += ngram.capacity();
    }

    // Table string heap allocation
    size += metadata.table.capacity();

    // Filters vector buffer + each FilterCondition's string heap allocations
    size += metadata.filters.capacity() * sizeof(query::FilterCondition);
    for (const auto& filter : metadata.filters) {
      size += filter.column.capacity() + filter.value.capacity();
    }

    return size;
  }

  /**
   * @brief Calculate total memory footprint including sizeof(*this)
   *
   * Includes both the inline struct size and all heap allocations.
   * Use MemoryUsage() when the entry is standalone (not stored in a container).
   * Use HeapFootprint() when the entry is inside a map or other container.
   *
   * @return Total memory usage in bytes
   */
  [[nodiscard]] size_t MemoryUsage() const { return sizeof(CacheEntry) + HeapFootprint(); }
};

}  // namespace mygramdb::cache
