/**
 * @file invalidation_queue.cpp
 * @brief Invalidation queue implementation
 */

#include "cache/invalidation_queue.h"

#include "cache/invalidation_manager.h"
#include "cache/query_cache.h"
#include "server/server_types.h"

namespace mygramdb::cache {

InvalidationQueue::InvalidationQueue(QueryCache* cache, InvalidationManager* invalidation_mgr,
                                     const std::unordered_map<std::string, server::TableContext*>& table_contexts)
    : cache_(cache), invalidation_mgr_(invalidation_mgr), table_contexts_(table_contexts) {}

InvalidationQueue::~InvalidationQueue() {
  Stop();
}

void InvalidationQueue::Enqueue(const std::string& table_name, const std::string& old_text,
                                const std::string& new_text) {
  // Get ngram settings for this specific table
  int ngram_size = 3;        // Default
  int kanji_ngram_size = 2;  // Default
  auto table_iter = table_contexts_.find(table_name);
  if (table_iter != table_contexts_.end()) {
    ngram_size = table_iter->second->config.ngram_size;
    kanji_ngram_size = table_iter->second->config.kanji_ngram_size;
  }

  if (!running_.load()) {
    // If worker not running, process immediately
    if (invalidation_mgr_ != nullptr) {
      auto affected_keys =
          invalidation_mgr_->InvalidateAffectedEntries(table_name, old_text, new_text, ngram_size, kanji_ngram_size);

      // Phase 2: Erase from cache immediately (no queuing)
      for (const auto& key : affected_keys) {
        if (cache_ != nullptr) {
          cache_->Erase(key);
        }
      }
    }
    return;
  }

  // Phase 1: Immediate invalidation (mark entries)
  std::unordered_set<CacheKey> affected_keys;
  if (invalidation_mgr_ != nullptr) {
    affected_keys =
        invalidation_mgr_->InvalidateAffectedEntries(table_name, old_text, new_text, ngram_size, kanji_ngram_size);
  }

  // Phase 2: Queue for deferred deletion
  {
    std::lock_guard<std::mutex> lock(queue_mutex_);

    // Add affected keys to pending set
    for (const auto& key : affected_keys) {
      const std::string composite_key = MakeCompositeKey(table_name, key.ToString());
      if (pending_ngrams_.find(composite_key) == pending_ngrams_.end()) {
        pending_ngrams_[composite_key] = std::chrono::steady_clock::now();
      }
    }
  }

  // Wake up worker if batch size reached
  queue_cv_.notify_one();
}

void InvalidationQueue::Start() {
  if (running_.load()) {
    return;
  }

  running_.store(true);
  worker_thread_ = std::thread(&InvalidationQueue::WorkerLoop, this);
}

void InvalidationQueue::Stop() {
  if (!running_.load()) {
    return;
  }

  running_.store(false);
  queue_cv_.notify_all();

  if (worker_thread_.joinable()) {
    worker_thread_.join();
  }

  // Process remaining items
  ProcessBatch();
}

size_t InvalidationQueue::GetPendingCount() const {
  std::lock_guard<std::mutex> lock(queue_mutex_);
  return pending_ngrams_.size();
}

void InvalidationQueue::WorkerLoop() {
  while (running_.load()) {
    std::unique_lock<std::mutex> lock(queue_mutex_);

    // Wait for trigger: batch size reached or max delay elapsed
    auto oldest_timestamp = std::chrono::steady_clock::time_point::max();
    if (!pending_ngrams_.empty()) {
      // Find oldest entry
      for (const auto& [key, timestamp] : pending_ngrams_) {
        if (timestamp < oldest_timestamp) {
          oldest_timestamp = timestamp;
        }
      }
    }

    const auto now = std::chrono::steady_clock::now();
    const auto time_since_oldest = now - oldest_timestamp;

    if (pending_ngrams_.size() >= batch_size_ || time_since_oldest >= max_delay_) {
      // Process batch
      lock.unlock();
      ProcessBatch();
    } else {
      // Wait for signal or timeout
      const auto remaining_delay = max_delay_ - time_since_oldest;
      queue_cv_.wait_for(lock, remaining_delay,
                         [this] { return !running_.load() || pending_ngrams_.size() >= batch_size_; });
    }
  }
}

void InvalidationQueue::ProcessBatch() {
  std::unordered_map<std::string, std::chrono::steady_clock::time_point> batch;

  {
    std::lock_guard<std::mutex> lock(queue_mutex_);
    if (pending_ngrams_.empty()) {
      return;
    }

    // Move pending items to batch
    batch = std::move(pending_ngrams_);
    pending_ngrams_.clear();
  }

  // Process batch: erase invalidated entries from cache
  std::unordered_set<CacheKey> keys_to_erase;

  for (const auto& [composite_key, timestamp] : batch) {
    // Parse composite key (format: "table:cache_key_hex")
    const size_t colon_pos = composite_key.find(':');
    if (colon_pos == std::string::npos) {
      continue;
    }

    const std::string table_name = composite_key.substr(0, colon_pos);
    const std::string key_hex = composite_key.substr(colon_pos + 1);

    // Parse cache key from hex string (128-bit MD5 = 32 hex chars)
    constexpr size_t kMD5HexLength = 32;
    if (key_hex.length() != kMD5HexLength) {
      continue;
    }

    try {
      const uint64_t hash_high = std::stoull(key_hex.substr(0, 16), nullptr, 16);
      const uint64_t hash_low = std::stoull(key_hex.substr(16, 16), nullptr, 16);
      const CacheKey key(hash_high, hash_low);

      keys_to_erase.insert(key);
    } catch (const std::exception& e) {
      // Invalid hex string, skip
      continue;
    }
  }

  // Erase entries from cache
  for (const auto& key : keys_to_erase) {
    if (cache_ != nullptr) {
      cache_->Erase(key);
    }
    if (invalidation_mgr_ != nullptr) {
      invalidation_mgr_->UnregisterCacheEntry(key);
    }
  }

  // Update batch statistics
  if (cache_ != nullptr) {
    cache_->IncrementInvalidationBatches();
  }
}

std::string InvalidationQueue::MakeCompositeKey(const std::string& table, const std::string& ngram) {
  return table + ":" + ngram;
}

}  // namespace mygramdb::cache
