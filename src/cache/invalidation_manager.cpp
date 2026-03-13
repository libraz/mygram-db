/**
 * @file invalidation_manager.cpp
 * @brief Invalidation manager implementation
 */

#include "cache/invalidation_manager.h"

#include <algorithm>

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

  // Store minimal invalidation metadata (table + ngrams only)
  InvalidationMetadata inv_meta;
  inv_meta.table = metadata.table;
  inv_meta.ngrams = metadata.ngrams;
  cache_metadata_[key] = std::move(inv_meta);

  // Update reverse index: ngram -> cache keys
  for (const auto& ngram : cache_metadata_[key].ngrams) {
    ngram_to_cache_keys_[cache_metadata_[key].table][ngram].insert(key);
  }
}

std::unordered_set<CacheKey> InvalidationManager::InvalidateAffectedEntries(const std::string& table_name,
                                                                            const std::string& old_text,
                                                                            const std::string& new_text, int ngram_size,
                                                                            int kanji_ngram_size,
                                                                            bool cross_boundary_ngrams) {
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

    // For each changed ngram, collect affected cache keys
    for (const auto& ngram : changed_ngrams) {
      auto ngram_it = table_it->second.find(ngram);
      if (ngram_it != table_it->second.end()) {
        for (const auto& cache_key : ngram_it->second) {
          affected_keys.insert(cache_key);
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

  // Remove metadata
  cache_metadata_.erase(metadata_it);
}

void InvalidationManager::UnregisterCacheEntry(const CacheKey& key) {
  std::unique_lock lock(mutex_);
  UnregisterCacheEntryUnlocked(key);
}

void InvalidationManager::ClearTable(const std::string& table_name) {
  std::unique_lock lock(mutex_);

  // Find all cache keys for this table
  std::vector<CacheKey> to_remove;
  for (const auto& [key, metadata] : cache_metadata_) {
    if (metadata.table == table_name) {
      to_remove.push_back(key);
    }
  }

  // Remove entries while holding lock (use unlocked version to avoid deadlock)
  for (const auto& key : to_remove) {
    UnregisterCacheEntryUnlocked(key);
  }

  // Remove table from reverse index (already holding lock)
  ngram_to_cache_keys_.erase(table_name);
}

void InvalidationManager::Clear() {
  std::unique_lock lock(mutex_);
  ngram_to_cache_keys_.clear();
  cache_metadata_.clear();
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

  return total;
}

std::vector<std::string> InvalidationManager::ExtractNgrams(const std::string& text, int ngram_size,
                                                            int kanji_ngram_size, bool cross_boundary_ngrams) {
  if (text.empty()) {
    return {};
  }

  // Use existing utility function for ngram generation
  std::vector<std::string> ngrams = utils::GenerateHybridNgrams(text, ngram_size, kanji_ngram_size, cross_boundary_ngrams);

  // Sort and deduplicate
  std::sort(ngrams.begin(), ngrams.end());
  ngrams.erase(std::unique(ngrams.begin(), ngrams.end()), ngrams.end());

  return ngrams;
}

bool InvalidationManager::IsCJK(uint32_t codepoint) {
  // Unicode ranges for CJK and Japanese characters
  // CJK Unified Ideographs: U+4E00 - U+9FFF
  // CJK Extension A: U+3400 - U+4DBF
  // Hiragana: U+3040 - U+309F
  // Katakana: U+30A0 - U+30FF
  // NOLINTBEGIN(readability-magic-numbers,cppcoreguidelines-avoid-magic-numbers)
  return (codepoint >= 0x4E00 && codepoint <= 0x9FFF) || (codepoint >= 0x3400 && codepoint <= 0x4DBF) ||
         (codepoint >= 0x3040 && codepoint <= 0x309F) || (codepoint >= 0x30A0 && codepoint <= 0x30FF);
  // NOLINTEND(readability-magic-numbers,cppcoreguidelines-avoid-magic-numbers)
}

}  // namespace mygramdb::cache
