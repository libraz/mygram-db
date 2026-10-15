/**
 * @file invalidation_manager.h
 * @brief ngram-based cache invalidation tracking
 */

#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <shared_mutex>
#include <string>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <vector>

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
/**
 * @brief Minimal metadata stored by InvalidationManager for invalidation tracking
 *
 * Only stores fields needed for reverse index management, avoiding full
 * CacheMetadata duplication.
 */
struct InvalidationMetadata {
  uint64_t entry_generation = 0;
  uint64_t data_version = 0;
  std::string table;                  ///< Table name
  std::vector<std::string> ngrams;    ///< Ngrams (sorted) for reverse index cleanup
  int ngram_size = 0;                 ///< N-gram size used when entry was created
  int kanji_ngram_size = 0;           ///< Kanji N-gram size used when entry was created
  bool cross_boundary_ngrams = true;  ///< Cross-boundary setting used when entry was created
  bool has_filters = false;           ///< Whether the cached query used filter conditions
  bool has_not_terms = false;         ///< Whether the cached query used NOT terms
  bool invalidate_on_any_text_change = false;
  bool pending = false;                    ///< Query execution has started but no resident payload exists yet
  bool invalidated = false;                ///< A matching mutation occurred while the insert was pending
  bool ngram_settings_registered = false;  ///< This identity incremented the table's settings reference count
  std::string pending_discriminator;       ///< Temporary canonical key retained only until promotion
  std::vector<query::FilterCondition> pending_filters;  ///< Temporary filter details retained until promotion
  size_t estimated_bytes = 0;                           ///< Incremental memory charge for this registration
};

class InvalidationManager {
 public:
  enum class PendingRegistrationStatus : uint8_t {
    kValid,
    kMissing,
    kInvalidated,
    kDiscriminatorMismatch,
    kNotPending,
    kResidentConflict,
  };

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

  /** Register a query whose execution is in progress. */
  bool RegisterPendingCacheEntry(const CacheEntryIdentity& identity, CacheMetadata metadata);

  /** Promote a still-valid pending registration to a resident entry. */
  PendingRegistrationStatus PromotePendingCacheEntry(const CacheEntryIdentity& identity,
                                                     const std::string& expected_discriminator);

  /** Copy full cache metadata from a valid pending registration. */
  PendingRegistrationStatus CopyPendingCacheMetadata(const CacheEntryIdentity& identity,
                                                     const std::string& expected_discriminator,
                                                     CacheMetadata& metadata) const;

  /** Remove a pending registration. This is safe after Clear()/ClearTable(). */
  void CancelPendingCacheEntry(const CacheEntryIdentity& identity);

  /** Test seam that can throw after any partial registration index update. */
  void SetAfterRegistrationStepHookForTest(std::function<void()> hook);

  /** Test seam invoked immediately before allocating the resident identity slot. */
  void SetBeforeResidentIdentityInsertHookForTest(std::function<void()> hook);

  /**
   * @brief Invalidate cache entries affected by text change
   *
   * Performs Step 1 invalidation (immediate mark) by extracting ngrams
   * from old and new text, finding changed ngrams, and marking affected
   * cache entries as invalidated.
   *
   * @param table_name Table that was modified
   * @param old_text Previous text content (empty if INSERT)
   * @param new_text New text content (empty if DELETE)
   * @param ngram_size N-gram size (for ASCII/alphanumeric)
   * @param kanji_ngram_size N-gram size (for CJK characters)
   * @return Set of resident entry identities that were marked invalidated
   */
  std::unordered_set<CacheEntryIdentity> InvalidateAffectedEntryIdentities(
      const std::string& table_name, const std::string& old_text, const std::string& new_text, int ngram_size,
      int kanji_ngram_size, bool cross_boundary_ngrams = true, bool filter_columns_changed = false);

  /**
   * @brief Unregister cache entry from invalidation tracking
   *
   * Called when entry is evicted or explicitly erased from cache.
   *
   * @param key Resident cache key to unregister
   */
  void UnregisterCacheEntry(const CacheKey& key);
  void UnregisterCacheEntry(const CacheEntryIdentity& identity);

  /** Unregister a batch of entry identities under a single mutex acquisition; missing identities are ignored. */
  void UnregisterCacheEntryIdentities(const std::vector<CacheEntryIdentity>& identities);

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

  /** @brief Get number of pending query registrations. */
  [[nodiscard]] size_t GetTrackedPendingEntryCount() const;

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

  /**
   * @brief Perform a full diagnostic walk of all tracking structures
   *
   * This is intentionally separate from MemoryUsage(): it is O(N) and must
   * not be called from the cache insertion hot path.
   */
  [[nodiscard]] size_t DiagnosticMemoryUsage() const;

 private:
  QueryCache* cache_;  ///< Pointer to query cache

  // Reverse index: (table, ngram) -> concrete resident and pending identities.
  std::unordered_map<std::string,                                                // table_name
                     std::unordered_map<std::string,                             // ngram
                                        std::unordered_set<CacheEntryIdentity>>  // entry identities
                     >
      ngram_to_entry_identities_;

  // Identity keyed so a new in-flight query can coexist with an older resident
  // incarnation of the same canonical key.
  std::unordered_map<CacheEntryIdentity, InvalidationMetadata> entry_metadata_;

  // Compatibility index for legacy key-only unregister APIs. It contains
  // residents only; pending registrations are always addressed by identity.
  std::unordered_map<CacheKey, CacheEntryIdentity> resident_identities_;

  // Reverse index: table -> set of resident and pending entry identities.
  //
  // Performance: This auxiliary index trades O(k) extra memory per entry
  // (one set membership per table) for O(k) ClearTable cost where k is the
  // number of entries in the affected table — instead of an O(N) scan of
  // entry_metadata_ across all tables. Maintained alongside entry_metadata_
  // under the same mutex_, so the two views are always consistent.
  std::unordered_map<std::string, std::unordered_set<CacheEntryIdentity>> table_to_entry_identities_;

  // Reverse indexes for the two invalidation triggers that are not keyed by
  // ngram. Without them every row event walks every entry registered for
  // the table just to test one boolean per entry, which turns binlog apply into
  // an O(entries per table) sweep per changed row. Both are strict subsets of
  // table_to_entry_identities_ and are maintained under the same mutex.
  std::unordered_map<std::string, std::unordered_set<CacheEntryIdentity>> table_to_filtered_entry_identities_;
  std::unordered_map<std::string, std::unordered_set<CacheEntryIdentity>> table_to_text_sensitive_entry_identities_;

  // Per-table ngram settings reference count: table -> (ngram_size, kanji_ngram_size, cross_boundary) -> count
  // Enables O(1) lookup of distinct historical ngram settings instead of O(N) scan over entry_metadata_
  std::unordered_map<std::string, std::map<std::tuple<int, int, bool>, size_t>> table_ngram_settings_;

  // Hot-path memory estimate. Each registration receives a conservative,
  // self-contained charge and unregister subtracts the same charge, making
  // MemoryUsage() O(1) without walking the reverse index.
  std::atomic<size_t> estimated_bytes_{0};

  // Thread safety
  mutable std::shared_mutex mutex_;

  // Hooks are configured before a test operation and invoked only while mutex_
  // is held. They let exception-safety tests fail between index mutations.
  std::function<void()> after_registration_step_hook_for_test_;
  std::function<void()> before_resident_identity_insert_hook_for_test_;

  /**
   * @brief Internal helper: unregister cache entry without locking
   * @param key Resident cache key to unregister
   * @note Assumes mutex_ is already held by caller
   */
  void UnregisterCacheEntryUnlocked(const CacheEntryIdentity& identity);
  void RegisterCacheEntryUnlocked(const CacheEntryIdentity& identity, CacheMetadata metadata, bool pending);
  void RunRegistrationStepHookForTest();
  void RunBeforeResidentIdentityInsertHookForTest();

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
