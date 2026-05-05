/**
 * @file invalidation_manager.cpp
 * @brief Invalidation manager implementation
 */

#include "cache/invalidation_manager.h"

#include <algorithm>
#include <set>
#include <tuple>

#include "cache/query_cache.h"
#include "utils/string_utils.h"

namespace mygramdb::cache {

InvalidationManager::InvalidationManager(QueryCache* cache) : cache_(cache) {}

void InvalidationManager::RegisterCacheEntry(const CacheKey& key, const CacheMetadata& metadata) {
  std::unique_lock lock(mutex_);

  // If entry already exists, unregister it first to clean up stale reverse index entries
  // This prevents memory leaks when a cache entry is re-registered with different ngrams
  auto existing = cache_metadata_.find(key);
  if (existing != cache_metadata_.end()) {
    UnregisterCacheEntryUnlocked(key);
  }

  // Store minimal invalidation metadata (table + ngrams + ngram settings)
  InvalidationMetadata inv_meta;
  inv_meta.table = metadata.table;
  inv_meta.ngrams = metadata.ngrams;
  inv_meta.ngram_size = metadata.ngram_size;
  inv_meta.kanji_ngram_size = metadata.kanji_ngram_size;
  inv_meta.cross_boundary_ngrams = metadata.cross_boundary_ngrams;
  inv_meta.has_filters = !metadata.filters.empty();
  cache_metadata_[key] = std::move(inv_meta);

  const auto& stored_meta = cache_metadata_[key];

  // Update reverse index: ngram -> cache keys
  for (const auto& ngram : stored_meta.ngrams) {
    ngram_to_cache_keys_[stored_meta.table][ngram].insert(key);
  }

  // Update table -> cache keys reverse index for O(k) ClearTable.
  table_to_cache_keys_[stored_meta.table].insert(key);

  // Track per-table ngram settings for O(1) lookup during invalidation
  if (stored_meta.ngram_size > 0) {
    auto settings_key =
        std::make_tuple(stored_meta.ngram_size, stored_meta.kanji_ngram_size, stored_meta.cross_boundary_ngrams);
    table_ngram_settings_[stored_meta.table][settings_key]++;
  }
}

std::unordered_set<CacheKey> InvalidationManager::InvalidateAffectedEntries(
    const std::string& table_name, const std::string& old_text, const std::string& new_text, int ngram_size,
    int kanji_ngram_size, bool cross_boundary_ngrams, bool filter_columns_changed) {
  // Extract ngrams from old and new text (both are sorted vectors)
  std::vector<std::string> old_ngrams = ExtractNgrams(old_text, ngram_size, kanji_ngram_size, cross_boundary_ngrams);
  std::vector<std::string> new_ngrams = ExtractNgrams(new_text, ngram_size, kanji_ngram_size, cross_boundary_ngrams);

  // Find changed ngrams (symmetric difference on sorted vectors)
  std::vector<std::string> changed_ngrams;
  std::set_symmetric_difference(old_ngrams.begin(), old_ngrams.end(), new_ngrams.begin(), new_ngrams.end(),
                                std::back_inserter(changed_ngrams));

  // Find affected cache keys
  std::unordered_set<CacheKey> affected_keys;

  {
    std::shared_lock lock(mutex_);

    auto table_it = ngram_to_cache_keys_.find(table_name);
    if (table_it == ngram_to_cache_keys_.end()) {
      return affected_keys;  // No entries for this table
    }

    // Collect distinct historical ngram settings for this table that differ
    // from the current settings. Uses O(1) lookup via table_ngram_settings_ instead
    // of O(N) scan over all cache_metadata_ entries.
    std::set<std::tuple<int, int, bool>> historical_settings;
    auto tbl_settings_it = table_ngram_settings_.find(table_name);
    if (tbl_settings_it != table_ngram_settings_.end()) {
      auto current_key = std::make_tuple(ngram_size, kanji_ngram_size, cross_boundary_ngrams);
      for (const auto& [settings_key, count] : tbl_settings_it->second) {
        if (settings_key != current_key) {
          historical_settings.insert(settings_key);
        }
      }
    }

    // Generate changed ngrams with each historical setting and merge
    for (const auto& [hist_ngram, hist_kanji, hist_cross] : historical_settings) {
      auto hist_old = ExtractNgrams(old_text, hist_ngram, hist_kanji, hist_cross);
      auto hist_new = ExtractNgrams(new_text, hist_ngram, hist_kanji, hist_cross);
      std::vector<std::string> hist_changed;
      std::set_symmetric_difference(hist_old.begin(), hist_old.end(), hist_new.begin(), hist_new.end(),
                                    std::back_inserter(hist_changed));
      // Merge into changed_ngrams (both are sorted)
      std::vector<std::string> merged;
      merged.reserve(changed_ngrams.size() + hist_changed.size());
      std::merge(changed_ngrams.begin(), changed_ngrams.end(), hist_changed.begin(), hist_changed.end(),
                 std::back_inserter(merged));
      merged.erase(std::unique(merged.begin(), merged.end()), merged.end());
      changed_ngrams = std::move(merged);
    }

    // For each changed ngram, collect affected cache keys
    for (const auto& ngram : changed_ngrams) {
      auto ngram_it = table_it->second.find(ngram);
      if (ngram_it != table_it->second.end()) {
        for (const auto& cache_key : ngram_it->second) {
          affected_keys.insert(cache_key);
        }
      }
    }

    // When filter columns changed, invalidate all cache entries for this table
    // that have filter conditions. Queries with filters may return different
    // results regardless of whether text also changed.
    //
    // H-M2: use the table_to_cache_keys_ reverse index to walk only the keys
    // belonging to @p table_name (O(k) where k = entries in this table)
    // instead of scanning every entry across every table (O(N)). For each
    // table-local key, dereference cache_metadata_ to confirm has_filters.
    if (filter_columns_changed) {
      auto tk_it = table_to_cache_keys_.find(table_name);
      if (tk_it != table_to_cache_keys_.end()) {
        for (const auto& cache_key : tk_it->second) {
          auto meta_it = cache_metadata_.find(cache_key);
          if (meta_it != cache_metadata_.end() && meta_it->second.has_filters) {
            affected_keys.insert(cache_key);
          }
        }
      }
    }
  }

  // Phase 1: Immediate invalidation (mark entries as invalidated)
  for (const auto& key : affected_keys) {
    if (cache_ != nullptr) {
      cache_->MarkInvalidated(key);
    }
  }

  return affected_keys;
}

// Internal helper: unregister cache entry without locking (assumes mutex is already held)
void InvalidationManager::UnregisterCacheEntryUnlocked(const CacheKey& key) {
  // Find metadata
  auto metadata_it = cache_metadata_.find(key);
  if (metadata_it == cache_metadata_.end()) {
    return;
  }

  const auto& metadata = metadata_it->second;

  // Remove from reverse index
  auto table_it = ngram_to_cache_keys_.find(metadata.table);
  if (table_it != ngram_to_cache_keys_.end()) {
    for (const auto& ngram : metadata.ngrams) {
      auto ngram_it = table_it->second.find(ngram);
      if (ngram_it != table_it->second.end()) {
        ngram_it->second.erase(key);

        // Remove ngram entry if no more cache keys
        if (ngram_it->second.empty()) {
          table_it->second.erase(ngram_it);
        }
      }
    }

    // Remove table entry if no more ngrams
    if (table_it->second.empty()) {
      ngram_to_cache_keys_.erase(table_it);
    }
  }

  // Decrement per-table ngram settings reference count
  if (metadata.ngram_size > 0) {
    auto settings_key = std::make_tuple(metadata.ngram_size, metadata.kanji_ngram_size, metadata.cross_boundary_ngrams);
    auto tbl_it = table_ngram_settings_.find(metadata.table);
    if (tbl_it != table_ngram_settings_.end()) {
      auto set_it = tbl_it->second.find(settings_key);
      if (set_it != tbl_it->second.end()) {
        if (--set_it->second == 0) {
          tbl_it->second.erase(set_it);
        }
        if (tbl_it->second.empty()) {
          table_ngram_settings_.erase(tbl_it);
        }
      }
    }
  }

  // Remove from table -> cache keys reverse index.
  auto tk_it = table_to_cache_keys_.find(metadata.table);
  if (tk_it != table_to_cache_keys_.end()) {
    tk_it->second.erase(key);
    if (tk_it->second.empty()) {
      table_to_cache_keys_.erase(tk_it);
    }
  }

  // Remove metadata
  cache_metadata_.erase(metadata_it);
}

void InvalidationManager::UnregisterCacheEntry(const CacheKey& key) {
  std::unique_lock lock(mutex_);
  UnregisterCacheEntryUnlocked(key);
}

void InvalidationManager::UnregisterCacheEntries(const std::vector<CacheKey>& keys) {
  if (keys.empty()) {
    return;
  }
  // Single lock acquisition for the whole batch (H-M7). UnregisterCacheEntryUnlocked
  // is idempotent on missing keys, so callers do not need to deduplicate.
  std::unique_lock lock(mutex_);
  for (const auto& key : keys) {
    UnregisterCacheEntryUnlocked(key);
  }
}

void InvalidationManager::ClearTable(const std::string& table_name) {
  std::unique_lock lock(mutex_);

  // Use the reverse index (table -> cache keys) for O(k) lookup of entries
  // belonging to this table, where k = entries in this table. Previously this
  // scanned all cache_metadata_ entries (O(N) across all tables).
  std::vector<CacheKey> to_remove;
  auto tk_it = table_to_cache_keys_.find(table_name);
  if (tk_it != table_to_cache_keys_.end()) {
    to_remove.reserve(tk_it->second.size());
    for (const auto& key : tk_it->second) {
      to_remove.push_back(key);
    }
  }

  // Remove entries while holding lock (use unlocked version to avoid deadlock).
  // UnregisterCacheEntryUnlocked also keeps table_to_cache_keys_ in sync, so
  // the table's set will be erased once empty.
  for (const auto& key : to_remove) {
    UnregisterCacheEntryUnlocked(key);
  }

  // Defensive cleanup: in case any auxiliary structure still has the table
  // entry (e.g., entries registered with ngram_size=0 left ngram_to_cache_keys_
  // populated even though they didn't bump table_ngram_settings_ counts), make
  // sure the table is fully removed.
  ngram_to_cache_keys_.erase(table_name);
  table_ngram_settings_.erase(table_name);
  table_to_cache_keys_.erase(table_name);
}

void InvalidationManager::Clear() {
  std::unique_lock lock(mutex_);
  // Swap with empty maps to release allocated capacity (clear() retains bucket memory)
  decltype(ngram_to_cache_keys_)().swap(ngram_to_cache_keys_);
  decltype(cache_metadata_)().swap(cache_metadata_);
  decltype(table_ngram_settings_)().swap(table_ngram_settings_);
  decltype(table_to_cache_keys_)().swap(table_to_cache_keys_);
}

size_t InvalidationManager::GetTrackedEntryCount() const {
  std::shared_lock lock(mutex_);
  return cache_metadata_.size();
}

size_t InvalidationManager::GetTrackedNgramCount(const std::string& table_name) const {
  std::shared_lock lock(mutex_);

  auto table_it = ngram_to_cache_keys_.find(table_name);
  if (table_it == ngram_to_cache_keys_.end()) {
    return 0;
  }

  return table_it->second.size();
}

size_t InvalidationManager::MemoryUsage() const {
  std::shared_lock lock(mutex_);

  size_t total = 0;

  // cache_metadata_ map overhead
  // Each bucket is a pointer, each entry is a node with key + value + hash + pointer
  total += cache_metadata_.bucket_count() * sizeof(void*);
  for (const auto& [key, meta] : cache_metadata_) {
    total += sizeof(CacheKey) + sizeof(InvalidationMetadata);
    total += meta.table.capacity();
    total += meta.ngrams.capacity() * sizeof(std::string);
    for (const auto& ngram : meta.ngrams) {
      total += ngram.capacity();
    }
    // Node overhead (next pointer + hash)
    total += sizeof(void*) + sizeof(size_t);
  }

  // ngram_to_cache_keys_ 3-level map overhead
  total += ngram_to_cache_keys_.bucket_count() * sizeof(void*);
  for (const auto& [table_name, ngram_map] : ngram_to_cache_keys_) {
    // Table-level entry: string key + inner map
    total += table_name.capacity() + sizeof(void*) + sizeof(size_t);
    total += ngram_map.bucket_count() * sizeof(void*);

    for (const auto& [ngram, key_set] : ngram_map) {
      // Ngram-level entry: string key + inner set
      total += ngram.capacity() + sizeof(void*) + sizeof(size_t);
      total += key_set.bucket_count() * sizeof(void*);

      // Each CacheKey in the set
      for (const auto& cache_key : key_set) {
        (void)cache_key;
        total += sizeof(CacheKey) + sizeof(void*) + sizeof(size_t);
      }
    }
  }

  // table_ngram_settings_ overhead
  for (const auto& [table_name, settings_map] : table_ngram_settings_) {
    total += table_name.capacity() + sizeof(std::map<std::tuple<int, int, bool>, size_t>);
    total += settings_map.size() * (sizeof(std::tuple<int, int, bool>) + sizeof(size_t) + 3 * sizeof(void*));
  }

  // table_to_cache_keys_ overhead (reverse index for O(k) ClearTable)
  total += table_to_cache_keys_.bucket_count() * sizeof(void*);
  for (const auto& [table_name, key_set] : table_to_cache_keys_) {
    total += table_name.capacity() + sizeof(void*) + sizeof(size_t);
    total += key_set.bucket_count() * sizeof(void*);
    // Each CacheKey membership in the set
    for (const auto& cache_key : key_set) {
      (void)cache_key;
      total += sizeof(CacheKey) + sizeof(void*) + sizeof(size_t);
    }
  }

  return total;
}

std::vector<std::string> InvalidationManager::ExtractNgrams(const std::string& text, int ngram_size,
                                                            int kanji_ngram_size, bool cross_boundary_ngrams) {
  if (text.empty()) {
    return {};
  }

  // Use existing utility function for ngram generation
  std::vector<std::string> ngrams =
      mygram::utils::GenerateHybridNgrams(text, ngram_size, kanji_ngram_size, cross_boundary_ngrams);

  // Sort and deduplicate
  mygram::utils::DeduplicateSorted(ngrams);

  return ngrams;
}

}  // namespace mygramdb::cache
