/**
 * @file invalidation_queue.cpp
 * @brief Invalidation queue implementation
 */

#include "cache/invalidation_queue.h"

#include <algorithm>
#include <exception>
#include <future>

#include "cache/invalidation_manager.h"
#include "cache/query_cache.h"
#include "index/index.h"
#include "utils/error.h"
#include "utils/expected.h"
#include "utils/structured_log.h"

namespace mygramdb::cache {

InvalidationQueue::InvalidationQueue(QueryCache* cache, InvalidationManager* invalidation_mgr,
                                     NgramConfigMap ngram_configs, WorkerThreadFactory worker_thread_factory)
    : cache_(cache),
      invalidation_mgr_(invalidation_mgr),
      ngram_configs_(std::move(ngram_configs)),
      worker_thread_factory_(std::move(worker_thread_factory)) {
  if (!worker_thread_factory_) {
    worker_thread_factory_ = [](std::function<void()> worker) { return std::thread(std::move(worker)); };
  }
}

InvalidationQueue::~InvalidationQueue() {
  Stop();
}

void InvalidationQueue::Enqueue(const std::string& table_name, const std::string& old_text, const std::string& new_text,
                                bool filter_columns_changed) {
  {
    std::lock_guard<std::mutex> lock(queue_mutex_);
    if (stopped_.load()) {
      mygram::utils::StructuredLog()
          .Event("cache_invalidation_queue_enqueue_after_stop")
          .Field("count", static_cast<uint64_t>(0))
          .Warn();
      return;
    }
  }

  // Get ngram settings for this specific table
  int ngram_size = index::kDefaultNgramSize;
  int kanji_ngram_size = index::kDefaultKanjiNgramSize;
  bool cross_boundary_ngrams = true;  // Default
  auto config_iter = ngram_configs_.find(table_name);
  if (config_iter != ngram_configs_.end()) {
    ngram_size = config_iter->second.ngram_size;
    kanji_ngram_size = config_iter->second.kanji_ngram_size;
    cross_boundary_ngrams = config_iter->second.cross_boundary_ngrams;
  }

  // Step 1: Immediate invalidation (mark entries)
  std::unordered_set<CacheEntryIdentity> affected_entries;
  if (invalidation_mgr_ != nullptr) {
    affected_entries = invalidation_mgr_->InvalidateAffectedEntryIdentities(
        table_name, old_text, new_text, ngram_size, kanji_ngram_size, cross_boundary_ngrams, filter_columns_changed);
  }

  // Step 2: Queue for deferred deletion or process immediately
  // Check stopped_ and running_ inside lock to prevent TOCTOU race with Stop()
  bool process_immediately = false;
  {
    std::lock_guard<std::mutex> lock(queue_mutex_);

    // Reject enqueues after Stop() to prevent use-after-free
    if (stopped_.load()) {
      mygram::utils::StructuredLog()
          .Event("cache_invalidation_queue_enqueue_after_stop")
          .Field("count", static_cast<uint64_t>(affected_entries.size()))
          .Warn();
      return;
    }

    if (!running_.load()) {
      // Worker not running (not yet started), process after releasing lock to avoid
      // nested lock acquisition (queue_mutex_ -> InvalidationManager::mutex_ -> QueryCache::mutex_)
      process_immediately = true;
    } else {
      // Worker is running, add to queue with per-entry backpressure. Checking
      // only once before the loop allowed one large invalidation result to
      // overshoot max_queue_size_ by an arbitrary amount.
      size_t dropped_count = 0;
      auto now = std::chrono::steady_clock::now();
      bool inserted_any = false;
      for (const auto& identity : affected_entries) {
        PendingKey pending_key{table_name, identity};
        if (pending_cache_keys_.size() >= max_queue_size_ &&
            pending_cache_keys_.find(pending_key) == pending_cache_keys_.end()) {
          ++dropped_count;
          continue;
        }
        auto [iter, inserted] = pending_cache_keys_.emplace(std::move(pending_key), now);
        if (inserted) {
          pending_entry_memory_bytes_ += PendingEntryMemoryUsage(iter->first);
          inserted_any = true;
        }
      }
      if (dropped_count > 0) {
        // Step 1 already marked entries as invalidated, so correctness is
        // preserved; deferred erasure will happen on RefreshLRU/eviction.
        mygram::utils::StructuredLog()
            .Event("cache_invalidation_queue_overflow")
            .Field("queue_size", static_cast<uint64_t>(pending_cache_keys_.size()))
            .Field("max_queue_size", static_cast<uint64_t>(max_queue_size_))
            .Field("dropped_count", static_cast<uint64_t>(dropped_count))
            .Warn();
      }
      if (inserted_any) {
        // Preserve existing entries' original timestamps so the oldest
        // timestamp cannot move forward after a duplicate enqueue.
        if (now < oldest_timestamp_) {
          oldest_timestamp_ = now;
        }
      }
    }
  }

  if (process_immediately) {
    // Process outside lock to prevent deadlock from nested lock acquisition.
    //
    // Single-source unregister: we used to call
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
    for (const auto& identity : affected_entries) {
      if (cache_ != nullptr) {
        cache_->EraseWithoutCallback(identity);
      }
      if (invalidation_mgr_ != nullptr) {
        invalidation_mgr_->UnregisterCacheEntry(identity);
      }
    }
    return;
  }

  // Wake up worker (running_ already verified inside lock)
  queue_cv_.notify_one();
}

mygram::utils::Expected<void, mygram::utils::Error> InvalidationQueue::Start() {
  // Protect the complete transition, not just the flags. In particular,
  // std::thread assignment must never race with Stop() joining the same object.
  std::lock_guard<std::mutex> state_lock(state_mutex_);
  if (running_.load(std::memory_order_acquire)) {
    return {};  // Already running
  }

  // The new thread waits until construction has succeeded and the state is
  // published. Without this gate, publishing running_ first exposes a
  // non-existent worker if std::thread construction throws; publishing it
  // afterwards lets WorkerLoop observe false and exit before Start returns.
  std::promise<void> start_promise;
  const std::shared_future<void> start_ready = start_promise.get_future().share();
  try {
    worker_thread_ = worker_thread_factory_([this, start_ready] {
      start_ready.wait();
      WorkerLoop();
    });
  } catch (const std::exception& e) {
    {
      std::lock_guard<std::mutex> queue_lock(queue_mutex_);
      running_.store(false, std::memory_order_release);
      stopped_.store(true, std::memory_order_release);
    }
    return mygram::utils::MakeUnexpected(
        mygram::utils::MakeError(mygram::utils::ErrorCode::kCacheWorkerStartFailed,
                                 std::string("Failed to start invalidation queue worker: ") + e.what()));
  }

  {
    // Publish both flags under the mutex used by Enqueue and WorkerLoop.
    std::lock_guard<std::mutex> queue_lock(queue_mutex_);
    stopped_.store(false, std::memory_order_release);
    running_.store(true, std::memory_order_release);
  }
  start_promise.set_value();

  return {};
}

void InvalidationQueue::Stop() {
  // Start and Stop own worker_thread_ only while holding state_mutex_. This
  // prevents a concurrent restart from assigning to a still-joinable thread.
  std::lock_guard<std::mutex> state_lock(state_mutex_);

  // Publish the wait predicate while holding the same mutex used by
  // WorkerLoop's condition-variable wait. Updating only the atomic and then
  // notifying can lose the wakeup between predicate evaluation and sleep.
  {
    std::lock_guard<std::mutex> lock(queue_mutex_);
    stopped_.store(true, std::memory_order_release);
    if (!running_.load(std::memory_order_acquire)) {
      return;  // Already stopped
    }
    running_.store(false, std::memory_order_release);
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

void InvalidationQueue::SetMaxQueueSize(size_t max_queue_size) {
  std::lock_guard<std::mutex> lock(queue_mutex_);
  max_queue_size_ = std::max<size_t>(1, max_queue_size);
}

size_t InvalidationQueue::MemoryUsage() const {
  std::lock_guard<std::mutex> lock(queue_mutex_);
  return pending_entry_memory_bytes_ + pending_cache_keys_.bucket_count() * sizeof(void*) +
         processing_memory_bytes_.load(std::memory_order_relaxed);
}

size_t InvalidationQueue::PendingEntryMemoryUsage(const PendingKey& key) {
  // value_type already contains the string object and identity. Add the
  // table's dynamic allocation plus conservative hash-node/allocator links.
  using ValueType = decltype(pending_cache_keys_)::value_type;
  return sizeof(ValueType) + key.table.capacity() + (2 * sizeof(void*)) + sizeof(size_t);
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
  size_t batch_memory = 0;

  {
    std::lock_guard<std::mutex> lock(queue_mutex_);
    if (pending_cache_keys_.empty()) {
      return;
    }

    // Move pending items to batch
    // std::move leaves pending_cache_keys_ in a valid but empty state for
    // std::unordered_map, so explicit clear() is unnecessary
    batch = std::move(pending_cache_keys_);
    batch_memory = pending_entry_memory_bytes_ + batch.bucket_count() * sizeof(void*);
    processing_memory_bytes_.fetch_add(batch_memory, std::memory_order_relaxed);
    pending_entry_memory_bytes_ = 0;
    oldest_timestamp_ = std::chrono::steady_clock::time_point::max();
  }

  // Erase entries from cache and clean up their metadata.
  //
  // Single-source unregister: UnregisterCacheEntry must fire exactly
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
  // A cache entry belongs to exactly one table, so PendingKey identities are
  // already unique. Processing the map directly avoids materializing a second
  // unordered_set whose peak memory would duplicate the batch.
  for (const auto& [pending_key, timestamp] : batch) {
    const auto& identity = pending_key.identity;
    if (cache_ != nullptr) {
      cache_->EraseWithoutCallback(identity);
    }
    if (invalidation_mgr_ != nullptr) {
      invalidation_mgr_->UnregisterCacheEntry(identity);
    }
  }

  // Update batch statistics
  if (cache_ != nullptr) {
    cache_->IncrementInvalidationBatches();
  }
  batch.clear();
  batch.rehash(0);
  processing_memory_bytes_.fetch_sub(batch_memory, std::memory_order_relaxed);
}

}  // namespace mygramdb::cache
