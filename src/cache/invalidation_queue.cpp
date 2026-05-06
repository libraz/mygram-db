/**
 * @file invalidation_queue.cpp
 * @brief Invalidation queue implementation
 */

#include "cache/invalidation_queue.h"

#include "cache/invalidation_manager.h"
#include "cache/query_cache.h"
#include "utils/structured_log.h"

namespace mygramdb::cache {

InvalidationQueue::InvalidationQueue(QueryCache* cache, InvalidationManager* invalidation_mgr,
                                     NgramConfigMap ngram_configs)
    : cache_(cache), invalidation_mgr_(invalidation_mgr), ngram_configs_(std::move(ngram_configs)) {}

InvalidationQueue::~InvalidationQueue() {
  Stop();
}

void InvalidationQueue::Enqueue(const std::string& table_name, const std::string& old_text, const std::string& new_text,
                                bool filter_columns_changed) {
  // Get ngram settings for this specific table
  int ngram_size = 2;                 // Default (match index::kDefaultNgramSize)
  int kanji_ngram_size = 1;           // Default (match index::kDefaultKanjiNgramSize)
  bool cross_boundary_ngrams = true;  // Default
  auto config_iter = ngram_configs_.find(table_name);
  if (config_iter != ngram_configs_.end()) {
    ngram_size = config_iter->second.ngram_size;
    kanji_ngram_size = config_iter->second.kanji_ngram_size;
    cross_boundary_ngrams = config_iter->second.cross_boundary_ngrams;
  }

  // Phase 1: Immediate invalidation (mark entries)
  std::unordered_set<CacheKey> affected_keys;
  if (invalidation_mgr_ != nullptr) {
    affected_keys = invalidation_mgr_->InvalidateAffectedEntries(
        table_name, old_text, new_text, ngram_size, kanji_ngram_size, cross_boundary_ngrams, filter_columns_changed);
  }

  // Phase 2: Queue for deferred deletion or process immediately
  // Check stopped_ and running_ inside lock to prevent TOCTOU race with Stop()
  bool process_immediately = false;
  {
    std::lock_guard<std::mutex> lock(queue_mutex_);

    // Reject enqueues after Stop() to prevent use-after-free
    if (stopped_.load()) {
      mygram::utils::StructuredLog()
          .Event("cache_invalidation_queue_enqueue_after_stop")
          .Field("count", static_cast<uint64_t>(affected_keys.size()))
          .Warn();
      return;
    }

    if (!running_.load()) {
      // Worker not running (not yet started), process after releasing lock to avoid
      // nested lock acquisition (queue_mutex_ -> InvalidationManager::mutex_ -> QueryCache::mutex_)
      process_immediately = true;
    } else {
      // Worker is running, add to queue with backpressure check
      if (pending_cache_keys_.size() >= max_queue_size_) {
        // Queue full - drop new entries (Phase 1 already marked entries as invalidated,
        // so correctness is preserved; Phase 2 erasure will happen on next RefreshLRU/eviction)
        mygram::utils::StructuredLog()
            .Event("cache_invalidation_queue_overflow")
            .Field("queue_size", static_cast<uint64_t>(pending_cache_keys_.size()))
            .Field("max_queue_size", static_cast<uint64_t>(max_queue_size_))
            .Field("dropped_count", static_cast<uint64_t>(affected_keys.size()))
            .Warn();
      } else {
        // Use emplace to preserve existing entries' original timestamps,
        // preventing oldest_timestamp_ from becoming stale after re-enqueue.
        // Composite key uses a typed pair (table + CacheKey) to avoid the
        // hex round-trip that the previous string-based encoding required
        // on the invalidation hot path.
        auto now = std::chrono::steady_clock::now();
        for (const auto& key : affected_keys) {
          pending_cache_keys_.emplace(PendingKey{table_name, key}, now);
        }
        if (now < oldest_timestamp_) {
          oldest_timestamp_ = now;
        }
      }
    }
  }

  if (process_immediately) {
    // Process outside lock to prevent deadlock from nested lock acquisition.
    //
    // CR-6 (single-source unregister): we used to call
    // invalidation_mgr_->UnregisterCacheEntry(key) here directly *and* rely on
    // QueryCache::Erase to fire eviction_callback_, which in CacheManager is
    // wired to call UnregisterCacheEntry as well. The double-unregister was
    // a no-op for the metadata map (the second find() returned end()) but it
    // created a race window for the auxiliary reverse indexes
    // (table_to_cache_keys_, ngram_to_cache_keys_) when a concurrent Insert
    // re-registered the same key between the two unregisters.
    //
    // Fix: use EraseWithoutCallback so the eviction callback stays out of the
    // picture on this path, and clean up the InvalidationManager metadata
    // explicitly here. The invariant is:
    //
    //   "On the invalidation-queue cleanup path, UnregisterCacheEntry fires
    //    exactly once per affected key — directly from the queue, never via
    //    eviction_callback_."
    //
    // This also ensures cleanup works in tests that wire an InvalidationQueue
    // to a QueryCache without CacheManager's eviction callback installed.
    for (const auto& key : affected_keys) {
      if (cache_ != nullptr) {
        cache_->EraseWithoutCallback(key);
      }
      if (invalidation_mgr_ != nullptr) {
        invalidation_mgr_->UnregisterCacheEntry(key);
      }
    }
    return;
  }

  // Wake up worker (running_ already verified inside lock)
  queue_cv_.notify_one();
}

void InvalidationQueue::Start() {
  // Atomically check and set running_ to prevent concurrent Start() calls
  bool expected = false;
  if (!running_.compare_exchange_strong(expected, true)) {
    return;  // Already running
  }

  // H-M5: reset stopped_ on every successful Start() so that a Stop()/Start()
  // cycle (e.g. CacheManager::Disable() followed by Enable()) does not leave
  // stopped_ permanently set, which would cause every subsequent Enqueue to
  // be silently dropped at the early-out below in Enqueue.
  //
  // Ordering: stopped_ is reset BEFORE the worker thread starts so that any
  // Enqueue that races with Start() and observes running_ == true after the
  // CAS above also observes stopped_ == false (release/acquire pair via the
  // queue_mutex_ in Enqueue).
  stopped_.store(false, std::memory_order_release);

  worker_thread_ = std::thread(&InvalidationQueue::WorkerLoop, this);
}

void InvalidationQueue::Stop() {
  // stopped_ is stored *before* acquiring queue_mutex_ on purpose: callers
  // that already completed Phase 1 (the lockless preparation in Enqueue) will
  // then try to acquire queue_mutex_, observe stopped_ == true, log a warning,
  // and return without enqueueing. Moving this store inside the lock would
  // open a window where late Phase-1 callers acquire the lock before stopped_
  // is set and enqueue post-shutdown work into a doomed queue. This is the
  // documented shutdown contract — do not move the store inside the lock.
  stopped_.store(true);

  // Atomically check and clear running_ to prevent concurrent Stop() calls
  bool expected = true;
  if (!running_.compare_exchange_strong(expected, false)) {
    return;  // Already stopped
  }

  queue_cv_.notify_all();

  if (worker_thread_.joinable()) {
    worker_thread_.join();
  }

  // Process remaining items after worker thread has joined.
  // Requires cache_ and invalidation_mgr_ to still be alive at this point.
  ProcessBatch();
}

size_t InvalidationQueue::GetPendingCount() const {
  std::lock_guard<std::mutex> lock(queue_mutex_);
  return pending_cache_keys_.size();
}

void InvalidationQueue::WorkerLoop() {
  while (running_.load()) {
    std::unique_lock<std::mutex> lock(queue_mutex_);

    // Wait for trigger: batch size reached or max delay elapsed
    if (!pending_cache_keys_.empty()) {
      const auto now = std::chrono::steady_clock::now();
      const auto time_since_oldest = now - oldest_timestamp_;

      if (pending_cache_keys_.size() >= batch_size_ || time_since_oldest >= max_delay_) {
        // Check running_ before processing to handle spurious wakeup and shutdown
        if (!running_.load()) {
          break;
        }

        // Process batch
        lock.unlock();
        ProcessBatch();
      } else {
        // Wait for signal or timeout
        const auto remaining_delay = max_delay_ - time_since_oldest;
        queue_cv_.wait_for(lock, remaining_delay,
                           [this] { return !running_.load() || pending_cache_keys_.size() >= batch_size_; });

        // After wakeup, check running_ before continuing
        if (!running_.load()) {
          break;
        }
      }
    } else {
      // Queue is empty: wait indefinitely for new items
      queue_cv_.wait(lock, [this] { return !running_.load() || !pending_cache_keys_.empty(); });

      // After wakeup, check running_ before continuing
      if (!running_.load()) {
        break;
      }
    }
  }
}

void InvalidationQueue::ProcessBatch() {
  std::unordered_map<PendingKey, std::chrono::steady_clock::time_point, PendingKeyHash> batch;

  {
    std::lock_guard<std::mutex> lock(queue_mutex_);
    if (pending_cache_keys_.empty()) {
      return;
    }

    // Move pending items to batch
    // std::move leaves pending_cache_keys_ in a valid but empty state for
    // std::unordered_map, so explicit clear() is unnecessary
    batch = std::move(pending_cache_keys_);
    oldest_timestamp_ = std::chrono::steady_clock::time_point::max();
  }

  // Process batch: erase invalidated entries from cache.
  // Typed PendingKey (table + CacheKey) is consumed directly — no string
  // parsing, no hex round-trip.
  std::unordered_set<CacheKey> keys_to_erase;
  for (const auto& [pending_key, timestamp] : batch) {
    keys_to_erase.insert(pending_key.key);
  }

  // Erase entries from cache and clean up their metadata.
  //
  // CR-6 (single-source unregister): UnregisterCacheEntry must fire exactly
  // once per affected key. Previously this loop called both
  // invalidation_mgr_->UnregisterCacheEntry(key) AND cache_->Erase(key); when
  // CacheManager installs an eviction callback that also calls
  // UnregisterCacheEntry, the second call from the eviction path raced with
  // any concurrent Insert that re-registered the same key, corrupting the
  // auxiliary reverse indexes (table_to_cache_keys_ counts could go negative
  // / desynchronize from cache_metadata_).
  //
  // Fix: call EraseWithoutCallback so the eviction callback never runs on
  // this path, and unregister the metadata explicitly. This keeps the
  // queue's cleanup self-contained and avoids any double-unregister.
  //
  // Invariant: "On the invalidation-queue cleanup path, UnregisterCacheEntry
  // fires exactly once per affected key — directly from the queue, never via
  // eviction_callback_."
  for (const auto& key : keys_to_erase) {
    if (cache_ != nullptr) {
      cache_->EraseWithoutCallback(key);
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

}  // namespace mygramdb::cache
