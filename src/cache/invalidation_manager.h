/**
 * @file invalidation_manager.h
 * @brief ngram-based cache invalidation tracking
 */

#pragma once

#include <map>
#include <memory>
#include <shared_mutex>
#include <string>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "cache/cache_entry.h"
#include "query/cache_key.h"

namespace mygramdb::cache {

// Forward declaration
class QueryCache;

/**
 * @brief Manages cache invalidation based on ngram tracking
 *
 * Tracks which ngrams each cached query uses, and maintains a reverse index
 * to quickly find affected cache entries when data changes.
 *
 * This enables precise invalidation: only queries that actually use changed
 * ngrams are invalidated, unlike MySQL's coarse table-level invalidation.
 */
/**
 * @brief Minimal metadata stored by InvalidationManager for invalidation tracking
 *
 * Only stores fields needed for reverse index management, avoiding full
 * CacheMetadata duplication.
 */
struct InvalidationMetadata {
  std::string table;                  ///< Table name
  std::vector<std::string> ngrams;    ///< Ngrams (sorted) for reverse index cleanup
  int ngram_size = 0;                 ///< N-gram size used when entry was created
  int kanji_ngram_size = 0;           ///< Kanji N-gram size used when entry was created
  bool cross_boundary_ngrams = true;  ///< Cross-boundary setting used when entry was created
  bool has_filters = false;           ///< Whether the cached query used filter conditions
};

class InvalidationManager {
 public:
  /**
   * @brief Constructor
   * @param cache Pointer to query cache
   */
  explicit InvalidationManager(QueryCache* cache);

  /**
   * @brief Destructor
   */
  ~InvalidationManager() = default;

  // Non-copyable, non-movable
  InvalidationManager(const InvalidationManager&) = delete;
  InvalidationManager& operator=(const InvalidationManager&) = delete;
  InvalidationManager(InvalidationManager&&) = delete;
  InvalidationManager& operator=(InvalidationManager&&) = delete;

  /**
   * @brief Register cache entry with ngrams for invalidation tracking
   * @param key Cache key
   * @param metadata Query metadata including ngrams
   */
  void RegisterCacheEntry(const CacheKey& key, const CacheMetadata& metadata);

  /**
   * @brief Invalidate cache entries affected by text change
   *
   * Performs Phase 1 invalidation (immediate mark) by extracting ngrams
   * from old and new text, finding changed ngrams, and marking affected
   * cache entries as invalidated.
   *
   * @param table_name Table that was modified
   * @param old_text Previous text content (empty if INSERT)
   * @param new_text New text content (empty if DELETE)
   * @param ngram_size N-gram size (for ASCII/alphanumeric)
   * @param kanji_ngram_size N-gram size (for CJK characters)
   * @return Set of cache keys that were marked invalidated
   */
  std::unordered_set<CacheKey> InvalidateAffectedEntries(const std::string& table_name, const std::string& old_text,
                                                         const std::string& new_text, int ngram_size,
                                                         int kanji_ngram_size, bool cross_boundary_ngrams = true,
                                                         bool filter_columns_changed = false);

  /**
   * @brief Unregister cache entry from invalidation tracking
   *
   * Called when entry is evicted or explicitly erased from cache.
   *
   * @param key Cache key to unregister
   */
  void UnregisterCacheEntry(const CacheKey& key);

  /**
   * @brief Clear all invalidation tracking for a table
   * @param table_name Table name
   */
  void ClearTable(const std::string& table_name);

  /**
   * @brief Clear all invalidation tracking
   */
  void Clear();

  /**
   * @brief Get number of tracked cache entries
   */
  [[nodiscard]] size_t GetTrackedEntryCount() const;

  /**
   * @brief Get number of tracked ngrams for a table
   * @param table_name Table name
   */
  [[nodiscard]] size_t GetTrackedNgramCount(const std::string& table_name) const;

  /**
   * @brief Estimate memory usage of invalidation tracking structures
   * @return Estimated memory usage in bytes
   */
  [[nodiscard]] size_t MemoryUsage() const;

 private:
  QueryCache* cache_;  ///< Pointer to query cache

  // Reverse index: (table, ngram) -> set of cache keys using this ngram
  std::unordered_map<std::string,                                      // table_name
                     std::unordered_map<std::string,                   // ngram
                                        std::unordered_set<CacheKey>>  // cache keys
                     >
      ngram_to_cache_keys_;

  // Map: cache key -> minimal invalidation metadata (table + ngrams only)
  std::unordered_map<CacheKey, InvalidationMetadata> cache_metadata_;

  // Reverse index: table -> set of cache keys registered for that table.
  //
  // Performance: This auxiliary index trades O(k) extra memory per cache entry
  // (one set membership per table) for O(k) ClearTable cost where k is the
  // number of entries in the affected table — instead of an O(N) scan of
  // cache_metadata_ across all tables. Maintained alongside cache_metadata_
  // under the same mutex_, so the two views are always consistent.
  std::unordered_map<std::string, std::unordered_set<CacheKey>> table_to_cache_keys_;

  // Per-table ngram settings reference count: table -> (ngram_size, kanji_ngram_size, cross_boundary) -> count
  // Enables O(1) lookup of distinct historical ngram settings instead of O(N) scan over cache_metadata_
  std::unordered_map<std::string, std::map<std::tuple<int, int, bool>, size_t>> table_ngram_settings_;

  // Thread safety
  mutable std::shared_mutex mutex_;

  /**
   * @brief Internal helper: unregister cache entry without locking
   * @param key Cache key to unregister
   * @note Assumes mutex_ is already held by caller
   */
  void UnregisterCacheEntryUnlocked(const CacheKey& key);

  /**
   * @brief Extract ngrams from text
   * @param text Text to extract ngrams from
   * @param ngram_size N-gram size (for ASCII/alphanumeric)
   * @param kanji_ngram_size N-gram size (for CJK characters)
   * @return Set of ngrams
   */
  static std::vector<std::string> ExtractNgrams(const std::string& text, int ngram_size, int kanji_ngram_size,
                                                bool cross_boundary_ngrams = true);
};

}  // namespace mygramdb::cache
