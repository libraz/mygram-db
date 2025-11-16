/**
 * @file invalidation_manager.h
 * @brief ngram-based cache invalidation tracking
 */

#pragma once

#include <memory>
#include <set>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "cache/cache_entry.h"
#include "cache/cache_key.h"

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
                                                         int kanji_ngram_size);

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

 private:
  QueryCache* cache_;  ///< Pointer to query cache

  // Reverse index: (table, ngram) -> set of cache keys using this ngram
  std::unordered_map<std::string,                                      // table_name
                     std::unordered_map<std::string,                   // ngram
                                        std::unordered_set<CacheKey>>  // cache keys
                     >
      ngram_to_cache_keys_;

  // Map: cache key -> metadata
  std::unordered_map<CacheKey, CacheMetadata> cache_metadata_;

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
  static std::set<std::string> ExtractNgrams(const std::string& text, int ngram_size, int kanji_ngram_size);

  /**
   * @brief Check if character is CJK (Chinese, Japanese, Korean)
   */
  static bool IsCJK(uint32_t codepoint);
};

}  // namespace mygramdb::cache
