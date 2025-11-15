/**
 * @file cache_manager.cpp
 * @brief Cache manager implementation
 */

#include "cache/cache_manager.h"

#include "cache/cache_key.h"

namespace mygramdb::cache {

CacheManager::CacheManager(const config::CacheConfig& cache_config, int ngram_size, int kanji_ngram_size)
    : enabled_(cache_config.enabled), ngram_size_(ngram_size), kanji_ngram_size_(kanji_ngram_size) {
  if (enabled_) {
    // Create query cache
    const size_t max_memory_bytes = static_cast<size_t>(cache_config.max_memory_mb) * 1024 * 1024;
    query_cache_ = std::make_unique<QueryCache>(max_memory_bytes, cache_config.min_query_cost_ms);

    // Create invalidation manager
    invalidation_mgr_ = std::make_unique<InvalidationManager>(query_cache_.get());

    // Create invalidation queue
    invalidation_queue_ =
        std::make_unique<InvalidationQueue>(query_cache_.get(), invalidation_mgr_.get(), ngram_size, kanji_ngram_size);
    invalidation_queue_->SetBatchSize(cache_config.invalidation.batch_size);
    invalidation_queue_->SetMaxDelay(cache_config.invalidation.max_delay_ms);
    invalidation_queue_->Start();
  }
}

CacheManager::~CacheManager() {
  if (invalidation_queue_) {
    invalidation_queue_->Stop();
  }
}

std::optional<std::vector<DocId>> CacheManager::Lookup(const query::Query& query) {
  if (!enabled_ || !query_cache_) {
    return std::nullopt;
  }

  // Only cache SEARCH and COUNT queries
  if (query.type != query::QueryType::SEARCH && query.type != query::QueryType::COUNT) {
    return std::nullopt;
  }

  // Normalize query and generate cache key
  const std::string normalized = QueryNormalizer::Normalize(query);
  if (normalized.empty()) {
    return std::nullopt;
  }

  const CacheKey key = CacheKeyGenerator::Generate(normalized);

  // Lookup in cache
  return query_cache_->Lookup(key);
}

bool CacheManager::Insert(const query::Query& query, const std::vector<DocId>& result,
                          const std::set<std::string>& ngrams, double query_cost_ms) {
  if (!enabled_ || !query_cache_ || !invalidation_mgr_) {
    return false;
  }

  // Only cache SEARCH and COUNT queries
  if (query.type != query::QueryType::SEARCH && query.type != query::QueryType::COUNT) {
    return false;
  }

  // Normalize query and generate cache key
  const std::string normalized = QueryNormalizer::Normalize(query);
  if (normalized.empty()) {
    return false;
  }

  const CacheKey key = CacheKeyGenerator::Generate(normalized);

  // Prepare metadata for invalidation tracking
  CacheMetadata metadata;
  metadata.key = key;
  metadata.table = query.table;
  metadata.ngrams = ngrams;
  metadata.filters = query.filters;
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

void CacheManager::Invalidate(const std::string& table_name, const std::string& old_text, const std::string& new_text) {
  if (!enabled_ || !invalidation_queue_) {
    return;
  }

  // Enqueue for asynchronous invalidation
  invalidation_queue_->Enqueue(table_name, old_text, new_text);
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

  return query_cache_->GetStatistics();
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

}  // namespace mygramdb::cache
