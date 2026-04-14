/**
 * @file cache_manager.cpp
 * @brief Cache manager implementation
 */

#include "cache/cache_manager.h"

#include "query/cache_key.h"

namespace mygramdb::cache {

CacheManager::CacheManager(const config::CacheConfig& cache_config, NgramConfigMap ngram_configs)
    : enabled_(cache_config.enabled), ttl_seconds_(cache_config.ttl_seconds) {
  if (enabled_) {
    // Create query cache with TTL support
    query_cache_ = std::make_unique<QueryCache>(cache_config.max_memory_bytes, cache_config.min_query_cost_ms,
                                                ttl_seconds_, cache_config.compression_enabled);

    // Create invalidation manager
    invalidation_mgr_ = std::make_unique<InvalidationManager>(query_cache_.get());

    // Set eviction callback to clean up invalidation metadata
    query_cache_->SetEvictionCallback([this](const CacheKey& key) {
      if (invalidation_mgr_) {
        invalidation_mgr_->UnregisterCacheEntry(key);
      }
    });

    // Create invalidation queue with per-table ngram settings
    invalidation_queue_ =
        std::make_unique<InvalidationQueue>(query_cache_.get(), invalidation_mgr_.get(), std::move(ngram_configs));
    invalidation_queue_->SetBatchSize(cache_config.invalidation.batch_size);
    invalidation_queue_->SetMaxDelay(cache_config.invalidation.max_delay_ms);
    invalidation_queue_->Start();
  }
}

CacheManager::~CacheManager() {
  if (invalidation_queue_) {
    invalidation_queue_->Stop();
  }
  // Clear eviction callback before destroying invalidation_mgr_ to prevent
  // use-after-free: QueryCache's LRU thread may still fire eviction callbacks
  // that reference invalidation_mgr_ during destruction.
  if (query_cache_) {
    query_cache_->SetEvictionCallback(nullptr);
  }
  // Explicitly destroy query_cache_ first to join its LRU background thread
  // before invalidation_mgr_ is destroyed (member destruction order is reverse
  // declaration order, which would destroy invalidation_mgr_ first).
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

  // Use precomputed cache key if available (performance optimization)
  if (query.cache_key.has_value()) {
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

  // Prepare metadata for invalidation tracking
  CacheMetadata metadata;
  metadata.key = key;
  metadata.table = query.table;
  metadata.ngrams.assign(ngrams.begin(), ngrams.end());
  metadata.filters = query.filters;
  metadata.ngram_size = ngram_size;
  metadata.kanji_ngram_size = kanji_ngram_size;
  metadata.cross_boundary_ngrams = cross_boundary_ngrams;
  metadata.created_at = std::chrono::steady_clock::now();
  metadata.last_accessed = metadata.created_at;
  metadata.access_count = 0;

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
  if (!enabled_ || !invalidation_queue_) {
    return;
  }

  // Enqueue for asynchronous invalidation
  invalidation_queue_->Enqueue(table_name, old_text, new_text, filter_columns_changed);
}

void CacheManager::Clear() {
  if (!enabled_) {
    return;
  }

  if (query_cache_) {
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

  enabled_ = true;

  // Start invalidation queue if not already running
  if (!invalidation_queue_->IsRunning()) {
    invalidation_queue_->Start();
  }

  return true;
}

void CacheManager::Disable() {
  enabled_ = false;

  // Stop invalidation queue
  if (invalidation_queue_ && invalidation_queue_->IsRunning()) {
    invalidation_queue_->Stop();
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

}  // namespace mygramdb::cache
