/**
 * @file cache_manager.cpp
 * @brief Cache manager implementation
 */

#include "cache/cache_manager.h"

#include "cache/cache_key.h"

namespace mygramdb::cache {

CacheManager::CacheManager(const config::CacheConfig& cache_config, NgramConfigMap ngram_configs)
    : enabled_(cache_config.enabled),
      ttl_seconds_(cache_config.ttl_seconds),
      table_invalidation_strategy_(cache_config.invalidation_strategy == "table") {
  // Create the cache internals even when runtime enforcement starts disabled.
  // That lets SET cache.enabled=true enable caching without a server restart.
  query_cache_ = std::make_unique<QueryCache>(cache_config.max_memory_bytes, cache_config.min_query_cost_ms,
                                              ttl_seconds_, cache_config.compression_enabled);

  // Create invalidation manager
  invalidation_mgr_ = std::make_unique<InvalidationManager>(query_cache_.get());

  // Set eviction callback to clean up invalidation metadata. Per-key path
  // is used by Erase(); bulk paths (Clear/ClearTable/EvictForSpace/RefreshLRU)
  // route through the batch callback below to amortize the
  // InvalidationManager::mutex_ acquisition.
  query_cache_->SetEvictionCallback([this](const CacheKey& key) {
    if (invalidation_mgr_) {
      invalidation_mgr_->UnregisterCacheEntry(key);
    }
  });

  // Batch eviction callback: takes InvalidationManager::mutex_ exactly once
  // for the whole batch instead of once per key. Bound to the same
  // invalidation_mgr_ as the per-key callback.
  query_cache_->SetBatchEvictionCallback([this](const std::vector<CacheKey>& keys) {
    if (invalidation_mgr_) {
      invalidation_mgr_->UnregisterCacheEntries(keys);
    }
  });

  // Create invalidation queue with per-table ngram settings
  invalidation_queue_ =
      std::make_unique<InvalidationQueue>(query_cache_.get(), invalidation_mgr_.get(), std::move(ngram_configs));
  invalidation_queue_->SetBatchSize(cache_config.invalidation.batch_size);
  invalidation_queue_->SetMaxDelay(cache_config.invalidation.max_delay_ms);
  if (enabled_) {
    invalidation_queue_->Start();
  }
}

CacheManager::~CacheManager() {
  if (invalidation_queue_) {
    invalidation_queue_->Stop();
  }
  // Explicitly destroy query_cache_ first. QueryCache::~QueryCache() joins the
  // LRU background worker before returning, so eviction callbacks can keep
  // referencing invalidation_mgr_ until the cache is fully torn down. Do not
  // rewrite callbacks here: QueryCache invokes them outside its mutex, and the
  // callback setters are intentionally not synchronized.
  query_cache_.reset();
}

std::optional<CacheKey> CacheManager::ResolveCacheKey(const query::Query& query) const {
  if (!enabled_ || !query_cache_) {
    return std::nullopt;
  }

  // Only cache SEARCH and COUNT queries
  if (query.type != query::QueryType::SEARCH && query.type != query::QueryType::COUNT) {
    return std::nullopt;
  }

  // Trust only table/index-aware canonical keys produced by search_pipeline.
  // Parser/default keys lack primary-key and normalization context.
  if (query.cache_key.has_value() && query.cache_key_is_canonical) {
    CacheKey key;
    key.hash_high = query.cache_key.value().first;
    key.hash_low = query.cache_key.value().second;
    return key;
  }

  // Fallback: compute cache key on-the-fly (for backwards compatibility)
  const std::string normalized = QueryNormalizer::Normalize(query);
  if (normalized.empty()) {
    return std::nullopt;
  }
  return CacheKeyGenerator::Generate(normalized);
}

std::optional<std::vector<DocId>> CacheManager::Lookup(const query::Query& query) {
  auto key = ResolveCacheKey(query);
  if (!key.has_value()) {
    return std::nullopt;
  }

  return query_cache_->Lookup(key.value());
}

std::optional<CacheLookupResult> CacheManager::LookupWithMetadata(const query::Query& query) {
  auto key = ResolveCacheKey(query);
  if (!key.has_value()) {
    return std::nullopt;
  }

  // Lookup in cache with metadata
  QueryCache::LookupMetadata metadata;
  auto result = query_cache_->LookupWithMetadata(key.value(), metadata);
  if (!result.has_value()) {
    return std::nullopt;
  }

  // Package result with metadata
  CacheLookupResult lookup_result;
  lookup_result.results = std::move(result.value());
  lookup_result.query_cost_ms = metadata.query_cost_ms;
  lookup_result.created_at = metadata.created_at;

  return lookup_result;
}

bool CacheManager::Insert(const query::Query& query, const std::vector<DocId>& result,
                          const std::vector<std::string>& ngrams, double query_cost_ms, int ngram_size,
                          int kanji_ngram_size, bool cross_boundary_ngrams) {
  return InsertIfVersion(query, result, ngrams, query_cost_ms, CaptureDataVersion(query.table), ngram_size,
                         kanji_ngram_size, cross_boundary_ngrams);
}

uint64_t CacheManager::CaptureDataVersion(const std::string& table_name) const {
  std::lock_guard<std::mutex> serialize_lock(serialize_mutex_);
  auto it = table_data_versions_.find(table_name);
  if (it == table_data_versions_.end()) {
    return 0;
  }
  return it->second;
}

bool CacheManager::InsertIfVersion(const query::Query& query, const std::vector<DocId>& result,
                                   const std::vector<std::string>& ngrams, double query_cost_ms,
                                   uint64_t expected_data_version, int ngram_size, int kanji_ngram_size,
                                   bool cross_boundary_ngrams) {
  if (!enabled_ || !query_cache_ || !invalidation_mgr_) {
    return false;
  }

  // Only cache SEARCH and COUNT queries
  if (query.type != query::QueryType::SEARCH && query.type != query::QueryType::COUNT) {
    return false;
  }

  // Use the same key derivation as Lookup to ensure consistency
  auto resolved_key = ResolveCacheKey(query);
  if (!resolved_key.has_value()) {
    return false;
  }
  const CacheKey key = resolved_key.value();

  // Prepare metadata for invalidation tracking.
  //
  // created_at / last_accessed are intentionally left default-constructed.
  // here. QueryCache::Insert() stamps them with std::chrono::steady_clock::now()
  // immediately before the entry enters cache_map_, and that is the
  // authoritative timestamp used for TTL accounting. InvalidationManager
  // copies only table/ngrams/ngram_size/kanji_ngram_size/cross_boundary_ngrams/
  // has_filters into its own InvalidationMetadata (see RegisterCacheEntry), so
  // it does not depend on created_at on this path.
  CacheMetadata metadata;
  metadata.key = key;
  metadata.table = query.table;
  metadata.ngrams.assign(ngrams.begin(), ngrams.end());
  metadata.filters = query.filters;
  metadata.ngram_size = ngram_size;
  metadata.kanji_ngram_size = kanji_ngram_size;
  metadata.cross_boundary_ngrams = cross_boundary_ngrams;
  metadata.has_not_terms = !query.not_terms.empty();
  metadata.access_count = 0;

  // Serialize Insert with Clear/ClearTable so that we never end up with a key
  // registered in InvalidationManager but not present in QueryCache (or vice
  // versa). See serialize_mutex_ doc in cache_manager.h.
  std::lock_guard<std::mutex> serialize_lock(serialize_mutex_);
  if (!enabled_.load(std::memory_order_acquire)) {
    return false;
  }
  const auto table_version_it = table_data_versions_.find(query.table);
  const uint64_t current_table_version = table_version_it == table_data_versions_.end() ? 0 : table_version_it->second;
  if (current_table_version != expected_data_version) {
    return false;
  }

  // Insert into cache
  const bool inserted = query_cache_->Insert(key, result, metadata, query_cost_ms);

  // Register with invalidation manager
  if (inserted) {
    invalidation_mgr_->RegisterCacheEntry(key, metadata);
  }

  return inserted;
}

void CacheManager::Invalidate(const std::string& table_name, const std::string& old_text, const std::string& new_text,
                              bool filter_columns_changed) {
  if (!enabled_) {
    return;
  }

  if (table_invalidation_strategy_) {
    ClearTable(table_name);
    return;
  }

  if (!invalidation_queue_) {
    return;
  }

  data_version_.fetch_add(1, std::memory_order_acq_rel);
  {
    std::lock_guard<std::mutex> serialize_lock(serialize_mutex_);
    ++table_data_versions_[table_name];
  }

  // Enqueue for asynchronous invalidation
  invalidation_queue_->Enqueue(table_name, old_text, new_text, filter_columns_changed);
}

void CacheManager::Clear() {
  if (!enabled_) {
    return;
  }

  // Serialize with Insert so that no key is added to InvalidationManager
  // between query_cache_->Clear() and invalidation_mgr_->Clear() that would
  // leave behind phantom metadata. See serialize_mutex_ doc in cache_manager.h.
  std::lock_guard<std::mutex> serialize_lock(serialize_mutex_);
  data_version_.fetch_add(1, std::memory_order_acq_rel);
  table_data_versions_.clear();

  if (query_cache_) {
    // QueryCache::Clear() invokes eviction_callback_ for every entry, which
    // calls invalidation_mgr_->UnregisterCacheEntry and keeps the per-key
    // metadata consistent with the cache. We still call invalidation_mgr_->
    // Clear() afterwards because eviction callbacks only unregister keys we
    // know about; they do not free the bucket-level allocations of
    // ngram_to_cache_keys_/table_ngram_settings_, and Clear() additionally
    // releases that capacity.
    query_cache_->Clear();
  }
  if (invalidation_mgr_) {
    invalidation_mgr_->Clear();
  }
}

void CacheManager::ClearTable(const std::string& table_name) {
  if (!enabled_) {
    return;
  }

  // Serialize with Insert; same rationale as Clear(). See serialize_mutex_
  // doc in cache_manager.h.
  std::lock_guard<std::mutex> serialize_lock(serialize_mutex_);
  data_version_.fetch_add(1, std::memory_order_acq_rel);
  ++table_data_versions_[table_name];

  if (query_cache_) {
    query_cache_->ClearTable(table_name);
  }
  if (invalidation_mgr_) {
    invalidation_mgr_->ClearTable(table_name);
  }
}

CacheStatisticsSnapshot CacheManager::GetStatistics() const {
  if (!enabled_ || !query_cache_) {
    return CacheStatisticsSnapshot{};
  }

  auto snapshot = query_cache_->GetStatistics();

  // Add InvalidationManager memory usage
  if (invalidation_mgr_) {
    snapshot.invalidation_index_memory_bytes = invalidation_mgr_->MemoryUsage();
  }

  return snapshot;
}

bool CacheManager::Enable() {
  // Cache can only be enabled if it was initialized at startup
  if (!query_cache_ || !invalidation_mgr_ || !invalidation_queue_) {
    return false;
  }

  // Enable order: start the invalidation queue BEFORE flipping
  // enabled_ to true. Otherwise an Invalidate() observing enabled_ == true
  // would call invalidation_queue_->Enqueue while running_ is still false;
  // Enqueue would either fall through to its synchronous fallback (an
  // inconsistency vs. the async contract) or, worse, observe a stale
  // stopped_ == true (set by the previous Stop() and reset by Start()) and
  // silently drop the event entirely.
  //
  // Symmetric with Disable(), which stops the queue first and then flips
  // enabled_ to false.
  if (!invalidation_queue_->IsRunning()) {
    invalidation_queue_->Start();
  }

  enabled_.store(true, std::memory_order_release);

  return true;
}

void CacheManager::Disable() {
  // Disable race: flip enabled_ to false BEFORE Clear() so that any
  // concurrent Insert observing the post-flip state short-circuits at the
  // enabled_ check and never inserts after Clear() has finished. The previous
  // ordering (Clear() then enabled_ = false) left a window where Insert and
  // Clear serialized on serialize_mutex_; once Clear released the mutex,
  // Insert could acquire it (with enabled_ still true) and re-populate the
  // cache that Disable() had just emptied.
  //
  // We use std::memory_order_release so the flip happens-before the Clear
  // call below from this thread's perspective and is visible to readers that
  // pair it with an acquire load on enabled_.
  enabled_.store(false, std::memory_order_release);

  // Stop the invalidation queue after closing the public enabled_ gate so no
  // new invalidation work can be enqueued while Stop() drains pending work.
  if (invalidation_queue_ && invalidation_queue_->IsRunning()) {
    invalidation_queue_->Stop();
  }

  //
  // Clear() short-circuits when !enabled_, so we run query_cache_->Clear()
  // and invalidation_mgr_->Clear() directly here. This is the safe default:
  // invalidation events arriving during the disabled window would be
  // silently dropped, so we proactively flush rather than leave potentially
  // stale entries to be served on re-enable.
  // Run the same body as CacheManager::Clear() but bypass the enabled_ guard.
  // We still take serialize_mutex_ to serialize against any in-flight Insert
  // that already passed the enabled_ check before we flipped it. Insert()
  // re-checks enabled_ after acquiring the same mutex, so a caller that was
  // queued behind this Clear exits without adding post-disable metadata.
  std::lock_guard<std::mutex> serialize_lock(serialize_mutex_);
  data_version_.fetch_add(1, std::memory_order_acq_rel);
  table_data_versions_.clear();
  if (query_cache_) {
    query_cache_->Clear();
  }
  if (invalidation_mgr_) {
    invalidation_mgr_->Clear();
  }
}

void CacheManager::SetMinQueryCost(double min_query_cost_ms) {
  if (query_cache_) {
    query_cache_->SetMinQueryCost(min_query_cost_ms);
  }
}

void CacheManager::SetTtl(int ttl_seconds) {
  ttl_seconds_ = ttl_seconds;
  // Apply TTL to query cache if it exists
  if (query_cache_) {
    query_cache_->SetTtl(ttl_seconds);
  }
}

size_t CacheManager::GetTrackedInvalidationEntries() const {
  if (!invalidation_mgr_) {
    return 0;
  }
  return invalidation_mgr_->GetTrackedEntryCount();
}

}  // namespace mygramdb::cache
