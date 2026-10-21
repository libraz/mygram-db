/**
 * @file invalidation_manager.cpp
 * @brief Invalidation manager implementation
 */

#include "cache/invalidation_manager.h"

#include <algorithm>
#include <set>
#include <stdexcept>
#include <tuple>

#include "cache/query_cache.h"
#include "utils/fd_guard.h"
#include "utils/string_utils.h"

namespace mygramdb::cache {

namespace {

size_t EstimateRegistrationBytes(const InvalidationMetadata& metadata) {
  size_t total = sizeof(CacheEntryIdentity) + sizeof(InvalidationMetadata) + sizeof(void*) + sizeof(size_t);
  total += metadata.table.capacity();
  total += metadata.ngrams.capacity() * sizeof(std::string);
  total += sizeof(std::string) + metadata.table.capacity() + sizeof(void*) + sizeof(size_t);
  total += sizeof(CacheEntryIdentity) + sizeof(void*) + sizeof(size_t);
  if (metadata.ngram_size > 0) {
    total += sizeof(std::tuple<int, int, bool>) + sizeof(size_t) + (3 * sizeof(void*));
  }
  for (const auto& ngram : metadata.ngrams) {
    total += ngram.capacity();
    // Reverse-index ngram node plus this cache-key membership. Charging the
    // node per membership is conservative and, importantly, independently
    // subtractable when registrations are removed in any order.
    total += sizeof(std::string) + ngram.capacity() + sizeof(void*) + sizeof(size_t);
    total += sizeof(CacheEntryIdentity) + sizeof(void*) + sizeof(size_t);
  }
  if (metadata.pending) {
    total += metadata.pending_discriminator.capacity();
    total += metadata.pending_filters.capacity() * sizeof(query::FilterCondition);
    for (const auto& filter : metadata.pending_filters) {
      total += filter.column.capacity() + filter.value.capacity();
    }
  } else {
    total += sizeof(CacheKey) + sizeof(CacheEntryIdentity) + sizeof(void*) + sizeof(size_t);
  }
  return total;
}

}  // namespace

InvalidationManager::InvalidationManager(QueryCache* cache) : cache_(cache) {}

void InvalidationManager::RegisterCacheEntry(const CacheKey& key, const CacheMetadata& metadata) {
  std::unique_lock lock(mutex_);

  const auto existing_resident = resident_identities_.find(key);
  if (existing_resident != resident_identities_.end()) {
    UnregisterCacheEntryUnlocked(existing_resident->second);
  }

  const CacheEntryIdentity identity{key, metadata.entry_generation};
  UnregisterCacheEntryUnlocked(identity);
  RegisterCacheEntryUnlocked(identity, metadata, false);
}

bool InvalidationManager::RegisterPendingCacheEntry(const CacheEntryIdentity& identity, CacheMetadata metadata) {
  std::unique_lock lock(mutex_);
  if (entry_metadata_.find(identity) != entry_metadata_.end()) {
    return false;
  }
  RegisterCacheEntryUnlocked(identity, std::move(metadata), true);
  return true;
}

void InvalidationManager::RegisterCacheEntryUnlocked(const CacheEntryIdentity& identity, CacheMetadata metadata,
                                                     bool pending) {
  // Store minimal invalidation metadata (table + ngrams + ngram settings)
  InvalidationMetadata inv_meta;
  inv_meta.entry_generation = identity.generation;
  inv_meta.data_version = metadata.data_version;
  inv_meta.table = std::move(metadata.table);
  inv_meta.ngrams = std::move(metadata.ngrams);
  inv_meta.ngram_size = metadata.ngram_size;
  inv_meta.kanji_ngram_size = metadata.kanji_ngram_size;
  inv_meta.cross_boundary_ngrams = metadata.cross_boundary_ngrams;
  inv_meta.has_filters = !metadata.filters.empty();
  inv_meta.has_not_terms = metadata.has_not_terms;
  inv_meta.invalidate_on_any_text_change = metadata.invalidate_on_any_text_change;
  inv_meta.pending = pending;
  if (pending) {
    inv_meta.pending_discriminator = std::move(metadata.cache_discriminator);
    inv_meta.pending_filters = std::move(metadata.filters);
  }
  const size_t registration_bytes = EstimateRegistrationBytes(inv_meta);
  // Do not publish a memory charge until every index insertion has succeeded.
  // The rollback guard below can then remove partially-created memberships
  // without allocating and without needing to guess which step failed.
  inv_meta.estimated_bytes = 0;
  const auto [metadata_it, inserted] = entry_metadata_.emplace(identity, std::move(inv_meta));
  if (!inserted) {
    throw std::logic_error("cache invalidation identity is already registered");
  }
  auto rollback = mygramdb::utils::ScopeGuard([this, &identity]() noexcept { UnregisterCacheEntryUnlocked(identity); });
  RunRegistrationStepHookForTest();

  auto& stored_meta = metadata_it->second;

  // Update reverse index: ngram -> concrete entry identities.
  for (const auto& ngram : stored_meta.ngrams) {
    ngram_to_entry_identities_[stored_meta.table][ngram].insert(identity);
    RunRegistrationStepHookForTest();
  }

  // Update table -> identities reverse index for O(k) ClearTable.
  table_to_entry_identities_[stored_meta.table].insert(identity);
  RunRegistrationStepHookForTest();
  if (stored_meta.has_filters) {
    table_to_filtered_entry_identities_[stored_meta.table].insert(identity);
    RunRegistrationStepHookForTest();
  }
  if (stored_meta.has_not_terms || stored_meta.invalidate_on_any_text_change) {
    table_to_text_sensitive_entry_identities_[stored_meta.table].insert(identity);
    RunRegistrationStepHookForTest();
  }

  if (!pending) {
    RunBeforeResidentIdentityInsertHookForTest();
    resident_identities_.try_emplace(identity.key, identity);
    RunRegistrationStepHookForTest();
  }

  // Track per-table ngram settings for O(1) lookup during invalidation
  if (stored_meta.ngram_size > 0) {
    auto settings_key =
        std::make_tuple(stored_meta.ngram_size, stored_meta.kanji_ngram_size, stored_meta.cross_boundary_ngrams);
    table_ngram_settings_[stored_meta.table][settings_key]++;
    stored_meta.ngram_settings_registered = true;
    RunRegistrationStepHookForTest();
  }

  metadata_it->second.estimated_bytes = registration_bytes;
  estimated_bytes_.fetch_add(registration_bytes, std::memory_order_relaxed);
  rollback.Release();
}

InvalidationManager::PendingRegistrationStatus InvalidationManager::PromotePendingCacheEntry(
    const CacheEntryIdentity& identity, const std::string& expected_discriminator) {
  std::unique_lock lock(mutex_);
  const auto metadata_it = entry_metadata_.find(identity);
  if (metadata_it == entry_metadata_.end()) {
    return PendingRegistrationStatus::kMissing;
  }
  if (!metadata_it->second.pending) {
    return PendingRegistrationStatus::kNotPending;
  }
  if (metadata_it->second.invalidated) {
    return PendingRegistrationStatus::kInvalidated;
  }
  if (metadata_it->second.pending_discriminator != expected_discriminator) {
    return PendingRegistrationStatus::kDiscriminatorMismatch;
  }
  const auto resident_it = resident_identities_.find(identity.key);
  if (resident_it != resident_identities_.end() && !(resident_it->second == identity)) {
    return PendingRegistrationStatus::kResidentConflict;
  }

  // Allocate the compatibility-index node before mutating pending metadata.
  // If allocation throws, the registration remains fully pending and can be
  // cancelled by its owner or retried without losing its invalidation data.
  RunBeforeResidentIdentityInsertHookForTest();
  resident_identities_.try_emplace(identity.key, identity);

  auto& metadata = metadata_it->second;
  const size_t previous_estimate = metadata.estimated_bytes;
  metadata.pending = false;
  std::string{}.swap(metadata.pending_discriminator);
  std::vector<query::FilterCondition>{}.swap(metadata.pending_filters);
  metadata.estimated_bytes = EstimateRegistrationBytes(metadata);
  if (previous_estimate >= metadata.estimated_bytes) {
    estimated_bytes_.fetch_sub(previous_estimate - metadata.estimated_bytes, std::memory_order_relaxed);
  } else {
    estimated_bytes_.fetch_add(metadata.estimated_bytes - previous_estimate, std::memory_order_relaxed);
  }
  return PendingRegistrationStatus::kValid;
}

InvalidationManager::PendingRegistrationStatus InvalidationManager::CopyPendingCacheMetadata(
    const CacheEntryIdentity& identity, const std::string& expected_discriminator, CacheMetadata& metadata) const {
  std::shared_lock lock(mutex_);
  const auto metadata_it = entry_metadata_.find(identity);
  if (metadata_it == entry_metadata_.end()) {
    return PendingRegistrationStatus::kMissing;
  }
  if (!metadata_it->second.pending) {
    return PendingRegistrationStatus::kNotPending;
  }
  if (metadata_it->second.invalidated) {
    return PendingRegistrationStatus::kInvalidated;
  }
  if (metadata_it->second.pending_discriminator != expected_discriminator) {
    return PendingRegistrationStatus::kDiscriminatorMismatch;
  }

  const auto& pending_metadata = metadata_it->second;
  metadata.key = identity.key;
  metadata.entry_generation = identity.generation;
  metadata.data_version = pending_metadata.data_version;
  metadata.cache_discriminator = pending_metadata.pending_discriminator;
  metadata.table = pending_metadata.table;
  metadata.ngrams = pending_metadata.ngrams;
  metadata.filters = pending_metadata.pending_filters;
  metadata.ngram_size = pending_metadata.ngram_size;
  metadata.kanji_ngram_size = pending_metadata.kanji_ngram_size;
  metadata.cross_boundary_ngrams = pending_metadata.cross_boundary_ngrams;
  metadata.has_not_terms = pending_metadata.has_not_terms;
  metadata.invalidate_on_any_text_change = pending_metadata.invalidate_on_any_text_change;
  metadata.access_count.store(0, std::memory_order_relaxed);
  return PendingRegistrationStatus::kValid;
}

void InvalidationManager::CancelPendingCacheEntry(const CacheEntryIdentity& identity) {
  std::unique_lock lock(mutex_);
  const auto metadata_it = entry_metadata_.find(identity);
  if (metadata_it != entry_metadata_.end() && metadata_it->second.pending) {
    UnregisterCacheEntryUnlocked(identity);
  }
}

void InvalidationManager::SetAfterRegistrationStepHookForTest(std::function<void()> hook) {
  std::unique_lock lock(mutex_);
  after_registration_step_hook_for_test_ = std::move(hook);
}

void InvalidationManager::SetBeforeResidentIdentityInsertHookForTest(std::function<void()> hook) {
  std::unique_lock lock(mutex_);
  before_resident_identity_insert_hook_for_test_ = std::move(hook);
}

void InvalidationManager::RunRegistrationStepHookForTest() {
  if (after_registration_step_hook_for_test_) {
    after_registration_step_hook_for_test_();
  }
}

void InvalidationManager::RunBeforeResidentIdentityInsertHookForTest() {
  if (before_resident_identity_insert_hook_for_test_) {
    before_resident_identity_insert_hook_for_test_();
  }
}

std::vector<std::string> InvalidationManager::ChangedNgramsFor(const std::string& old_text, const std::string& new_text,
                                                               const NgramSetting& setting) {
  const auto& [ngram_size, kanji_ngram_size, cross_boundary_ngrams] = setting;
  // Extract ngrams from old and new text (both are sorted vectors)
  const auto old_ngrams = ExtractNgrams(old_text, ngram_size, kanji_ngram_size, cross_boundary_ngrams);
  const auto new_ngrams = ExtractNgrams(new_text, ngram_size, kanji_ngram_size, cross_boundary_ngrams);
  std::vector<std::string> changed_ngrams;
  std::set_symmetric_difference(old_ngrams.begin(), old_ngrams.end(), new_ngrams.begin(), new_ngrams.end(),
                                std::back_inserter(changed_ngrams));
  return changed_ngrams;
}

ChangedNgrams InvalidationManager::PrecomputeChangedNgrams(const std::string& table_name, const std::string& old_text,
                                                           const std::string& new_text, int ngram_size,
                                                           int kanji_ngram_size, bool cross_boundary_ngrams) const {
  std::set<NgramSetting> settings{NgramSetting{ngram_size, kanji_ngram_size, cross_boundary_ngrams}};
  {
    std::shared_lock lock(mutex_);
    const auto table_entries_it = table_to_entry_identities_.find(table_name);
    if (table_entries_it == table_to_entry_identities_.end() || table_entries_it->second.empty()) {
      return {};
    }
    const auto settings_it = table_ngram_settings_.find(table_name);
    if (settings_it != table_ngram_settings_.end()) {
      for (const auto& [setting, count] : settings_it->second) {
        settings.insert(setting);
      }
    }
  }

  ChangedNgrams changed;
  for (const auto& setting : settings) {
    changed.emplace(setting, ChangedNgramsFor(old_text, new_text, setting));
  }
  return changed;
}

std::unordered_set<CacheEntryIdentity> InvalidationManager::InvalidateAffectedEntryIdentities(
    const std::string& table_name, const std::string& old_text, const std::string& new_text, int ngram_size,
    int kanji_ngram_size, bool cross_boundary_ngrams, bool filter_columns_changed, const ChangedNgrams* precomputed) {
  {
    std::shared_lock lock(mutex_);
    const auto table_entries_it = table_to_entry_identities_.find(table_name);
    if (table_entries_it == table_to_entry_identities_.end() || table_entries_it->second.empty()) {
      return {};
    }
  }

  // Precomputed settings are reused; any other setting is extracted here.
  auto changed_ngrams_for = [&](const NgramSetting& setting) {
    if (precomputed != nullptr) {
      const auto precomputed_it = precomputed->find(setting);
      if (precomputed_it != precomputed->end()) {
        return precomputed_it->second;
      }
    }
    return ChangedNgramsFor(old_text, new_text, setting);
  };

  // Find changed ngrams (symmetric difference on sorted vectors)
  std::vector<std::string> changed_ngrams =
      changed_ngrams_for(NgramSetting{ngram_size, kanji_ngram_size, cross_boundary_ngrams});

  // Return residents for deferred erasure and mark pending identities in place.
  std::unordered_set<CacheEntryIdentity> affected_entries;

  {
    std::unique_lock lock(mutex_);

    auto table_it = ngram_to_entry_identities_.find(table_name);
    auto add_affected = [&](const CacheEntryIdentity& identity) {
      auto meta_it = entry_metadata_.find(identity);
      if (meta_it == entry_metadata_.end()) {
        return;
      }
      if (meta_it->second.pending) {
        meta_it->second.invalidated = true;
      } else {
        affected_entries.insert(identity);
      }
    };

    // Collect distinct historical ngram settings for this table that differ
    // from the current settings. Uses O(1) lookup via table_ngram_settings_ instead
    // of O(N) scan over all entry_metadata_ entries.
    std::set<NgramSetting> historical_settings;
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
    for (const auto& setting : historical_settings) {
      const std::vector<std::string> hist_changed = changed_ngrams_for(setting);
      // Merge into changed_ngrams (both are sorted)
      std::vector<std::string> merged;
      merged.reserve(changed_ngrams.size() + hist_changed.size());
      std::merge(changed_ngrams.begin(), changed_ngrams.end(), hist_changed.begin(), hist_changed.end(),
                 std::back_inserter(merged));
      merged.erase(std::unique(merged.begin(), merged.end()), merged.end());
      changed_ngrams = std::move(merged);
    }

    // For each changed ngram, collect affected cache keys
    if (table_it != ngram_to_entry_identities_.end()) {
      for (const auto& ngram : changed_ngrams) {
        auto ngram_it = table_it->second.find(ngram);
        if (ngram_it != table_it->second.end()) {
          for (const auto& identity : ngram_it->second) {
            add_affected(identity);
          }
        }
      }
    }

    // When filter columns changed, invalidate all cache entries for this table
    // that have filter conditions. Queries with filters may return different
    // results regardless of whether text also changed.
    //
    // table_to_filtered_entry_identities_ already holds exactly the filtered entries
    // for this table, so the sweep is O(matching entries) rather than O(entries
    // in the table) — no per-key metadata lookup is needed to reject entries
    // that could never be affected.
    if (filter_columns_changed) {
      auto tk_it = table_to_filtered_entry_identities_.find(table_name);
      if (tk_it != table_to_filtered_entry_identities_.end()) {
        for (const auto& identity : tk_it->second) {
          add_affected(identity);
        }
      }
    }

    // NOT terms (whose result set can grow when unrelated text changes) and
    // exact/fuzzy queries are invalidated for every row-level text event. An
    // empty/empty event can still represent an insert/delete with no searchable
    // text, so retain conservative behavior for text-sensitive entries.
    if (old_text != new_text || (old_text.empty() && new_text.empty())) {
      auto tk_it = table_to_text_sensitive_entry_identities_.find(table_name);
      if (tk_it != table_to_text_sensitive_entry_identities_.end()) {
        for (const auto& identity : tk_it->second) {
          add_affected(identity);
        }
      }
    }
  }

  // Step 1: Immediate invalidation (mark entries as invalidated)
  for (const auto& identity : affected_entries) {
    if (cache_ != nullptr) {
      cache_->MarkInvalidated(identity);
    }
  }

  return affected_entries;
}

// Internal helper: unregister cache entry without locking (assumes mutex is already held)
void InvalidationManager::UnregisterCacheEntryUnlocked(const CacheEntryIdentity& identity) {
  auto metadata_it = entry_metadata_.find(identity);
  if (metadata_it == entry_metadata_.end()) {
    return;
  }

  const auto& metadata = metadata_it->second;
  const size_t removed_estimate = metadata.estimated_bytes;

  // Remove from reverse index
  auto table_it = ngram_to_entry_identities_.find(metadata.table);
  if (table_it != ngram_to_entry_identities_.end()) {
    for (const auto& ngram : metadata.ngrams) {
      auto ngram_it = table_it->second.find(ngram);
      if (ngram_it != table_it->second.end()) {
        ngram_it->second.erase(identity);

        // Remove ngram entry if no more cache keys
        if (ngram_it->second.empty()) {
          table_it->second.erase(ngram_it);
        }
      }
    }

    // Remove table entry if no more ngrams
    if (table_it->second.empty()) {
      ngram_to_entry_identities_.erase(table_it);
    }
  }

  // Decrement a settings reference only when this identity completed that
  // increment. A failed registration can have metadata and reverse-index
  // nodes before reaching the settings-index step; its rollback must not
  // decrement another resident's same-settings reference.
  if (metadata.ngram_size > 0) {
    auto settings_key = std::make_tuple(metadata.ngram_size, metadata.kanji_ngram_size, metadata.cross_boundary_ngrams);
    auto tbl_it = table_ngram_settings_.find(metadata.table);
    if (tbl_it != table_ngram_settings_.end()) {
      auto set_it = tbl_it->second.find(settings_key);
      if (metadata.ngram_settings_registered && set_it != tbl_it->second.end()) {
        if (--set_it->second == 0) {
          tbl_it->second.erase(set_it);
        }
      }
      // operator[] may have created an empty table/settings node before a
      // later allocation failed. Remove empty shells without touching any
      // existing nonzero reference count.
      if (tbl_it->second.empty()) {
        table_ngram_settings_.erase(tbl_it);
      }
    }
  }

  // Remove from the table -> identity reverse indexes.
  const auto erase_from = [&identity, &metadata](auto& index) {
    auto table_it = index.find(metadata.table);
    if (table_it == index.end()) {
      return;
    }
    table_it->second.erase(identity);
    if (table_it->second.empty()) {
      index.erase(table_it);
    }
  };
  erase_from(table_to_entry_identities_);
  erase_from(table_to_filtered_entry_identities_);
  erase_from(table_to_text_sensitive_entry_identities_);

  auto resident_it = resident_identities_.find(identity.key);
  if (resident_it != resident_identities_.end() && resident_it->second == identity) {
    resident_identities_.erase(resident_it);
  }

  // Remove metadata.
  entry_metadata_.erase(metadata_it);
  estimated_bytes_.fetch_sub(removed_estimate, std::memory_order_relaxed);
}

void InvalidationManager::UnregisterCacheEntry(const CacheKey& key) {
  std::unique_lock lock(mutex_);
  const auto resident_it = resident_identities_.find(key);
  if (resident_it != resident_identities_.end()) {
    UnregisterCacheEntryUnlocked(resident_it->second);
  }
}

void InvalidationManager::UnregisterCacheEntry(const CacheEntryIdentity& identity) {
  std::unique_lock lock(mutex_);
  UnregisterCacheEntryUnlocked(identity);
}

void InvalidationManager::UnregisterCacheEntryIdentities(const std::vector<CacheEntryIdentity>& identities) {
  if (identities.empty()) {
    return;
  }
  std::unique_lock lock(mutex_);
  for (const auto& identity : identities) {
    UnregisterCacheEntryUnlocked(identity);
  }
}

void InvalidationManager::ClearTable(const std::string& table_name) {
  std::unique_lock lock(mutex_);

  // The table reverse index yields this table's identities in O(k).
  std::vector<CacheEntryIdentity> to_remove;
  auto tk_it = table_to_entry_identities_.find(table_name);
  if (tk_it != table_to_entry_identities_.end()) {
    to_remove.reserve(tk_it->second.size());
    for (const auto& identity : tk_it->second) {
      to_remove.push_back(identity);
    }
  }

  // Remove entries while holding lock (use unlocked version to avoid deadlock).
  // UnregisterCacheEntryUnlocked also keeps table_to_entry_identities_ in sync, so
  // the table's set will be erased once empty.
  for (const auto& identity : to_remove) {
    UnregisterCacheEntryUnlocked(identity);
  }

  // Defensive cleanup: in case any auxiliary structure still has the table
  // entry (e.g., entries registered with ngram_size=0 left ngram_to_entry_identities_
  // populated even though they didn't bump table_ngram_settings_ counts), make
  // sure the table is fully removed.
  ngram_to_entry_identities_.erase(table_name);
  table_ngram_settings_.erase(table_name);
  table_to_entry_identities_.erase(table_name);
  table_to_filtered_entry_identities_.erase(table_name);
  table_to_text_sensitive_entry_identities_.erase(table_name);
}

void InvalidationManager::Clear() {
  std::unique_lock lock(mutex_);
  // Swap with empty maps to release allocated capacity (clear() retains bucket memory)
  decltype(ngram_to_entry_identities_)().swap(ngram_to_entry_identities_);
  decltype(entry_metadata_)().swap(entry_metadata_);
  decltype(resident_identities_)().swap(resident_identities_);
  decltype(table_ngram_settings_)().swap(table_ngram_settings_);
  decltype(table_to_entry_identities_)().swap(table_to_entry_identities_);
  decltype(table_to_filtered_entry_identities_)().swap(table_to_filtered_entry_identities_);
  decltype(table_to_text_sensitive_entry_identities_)().swap(table_to_text_sensitive_entry_identities_);
  estimated_bytes_.store(0, std::memory_order_relaxed);
}

size_t InvalidationManager::GetTrackedEntryCount() const {
  std::shared_lock lock(mutex_);
  return resident_identities_.size();
}

size_t InvalidationManager::GetTrackedPendingEntryCount() const {
  std::shared_lock lock(mutex_);
  return entry_metadata_.size() - resident_identities_.size();
}

size_t InvalidationManager::GetTrackedNgramCount(const std::string& table_name) const {
  std::shared_lock lock(mutex_);

  auto table_it = ngram_to_entry_identities_.find(table_name);
  if (table_it == ngram_to_entry_identities_.end()) {
    return 0;
  }

  return table_it->second.size();
}

size_t InvalidationManager::MemoryUsage() const {
  return estimated_bytes_.load(std::memory_order_relaxed);
}

size_t InvalidationManager::DiagnosticMemoryUsage() const {
  std::shared_lock lock(mutex_);

  size_t total = 0;

  // entry_metadata_ map overhead
  // Each bucket is a pointer, each entry is a node with key + value + hash + pointer
  total += entry_metadata_.bucket_count() * sizeof(void*);
  for (const auto& [identity, meta] : entry_metadata_) {
    (void)identity;
    total += sizeof(CacheEntryIdentity) + sizeof(InvalidationMetadata);
    total += meta.table.capacity();
    total += meta.ngrams.capacity() * sizeof(std::string);
    for (const auto& ngram : meta.ngrams) {
      total += ngram.capacity();
    }
    if (meta.pending) {
      total += meta.pending_discriminator.capacity();
      total += meta.pending_filters.capacity() * sizeof(query::FilterCondition);
      for (const auto& filter : meta.pending_filters) {
        total += filter.column.capacity() + filter.value.capacity();
      }
    }
    // Node overhead (next pointer + hash)
    total += sizeof(void*) + sizeof(size_t);
  }

  // ngram_to_entry_identities_ 3-level map overhead
  total += ngram_to_entry_identities_.bucket_count() * sizeof(void*);
  for (const auto& [table_name, ngram_map] : ngram_to_entry_identities_) {
    // Table-level entry: string key + inner map
    total += table_name.capacity() + sizeof(void*) + sizeof(size_t);
    total += ngram_map.bucket_count() * sizeof(void*);

    for (const auto& [ngram, key_set] : ngram_map) {
      // Ngram-level entry: string key + inner set
      total += ngram.capacity() + sizeof(void*) + sizeof(size_t);
      total += key_set.bucket_count() * sizeof(void*);

      // Each identity in the set
      for (const auto& identity : key_set) {
        (void)identity;
        total += sizeof(CacheEntryIdentity) + sizeof(void*) + sizeof(size_t);
      }
    }
  }

  // table_ngram_settings_ overhead
  for (const auto& [table_name, settings_map] : table_ngram_settings_) {
    total += table_name.capacity() + sizeof(std::map<std::tuple<int, int, bool>, size_t>);
    total += settings_map.size() * (sizeof(std::tuple<int, int, bool>) + sizeof(size_t) + 3 * sizeof(void*));
  }

  // table -> identity reverse indexes (O(k) ClearTable plus the two
  // non-ngram invalidation triggers)
  const auto account_table_index = [&total](const auto& index) {
    total += index.bucket_count() * sizeof(void*);
    for (const auto& [table_name, key_set] : index) {
      total += table_name.capacity() + sizeof(void*) + sizeof(size_t);
      total += key_set.bucket_count() * sizeof(void*);
      // Each identity membership in the set
      total += key_set.size() * (sizeof(CacheEntryIdentity) + sizeof(void*) + sizeof(size_t));
    }
  };
  account_table_index(table_to_entry_identities_);
  account_table_index(table_to_filtered_entry_identities_);
  account_table_index(table_to_text_sensitive_entry_identities_);

  // Resident compatibility lookup used by legacy key-only unregister calls.
  total += resident_identities_.bucket_count() * sizeof(void*);
  total +=
      resident_identities_.size() * (sizeof(CacheKey) + sizeof(CacheEntryIdentity) + sizeof(void*) + sizeof(size_t));

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
