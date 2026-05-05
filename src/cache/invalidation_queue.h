/**
 * @file invalidation_queue.h
 * @brief Asynchronous cache invalidation queue with deduplication
 */

#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#include "cache/cache_types.h"
#include "query/cache_key.h"

namespace mygramdb::cache {

// Forward declarations
class QueryCache;
class InvalidationManager;

/**
 * @brief Invalidation event
 *
 * Represents a data modification event that requires cache invalidation.
 */
struct InvalidationEvent {
  std::string table_name;
  std::string old_text;
  std::string new_text;
  std::chrono::steady_clock::time_point timestamp;

  InvalidationEvent(std::string table, std::string old_txt, std::string new_txt)
      : table_name(std::move(table)),
        old_text(std::move(old_txt)),
        new_text(std::move(new_txt)),
        timestamp(std::chrono::steady_clock::now()) {}
};

/**
 * @brief Asynchronous invalidation queue with batching and deduplication
 *
 * Queues invalidation events and processes them in batches to reduce
 * CPU load during bulk operations. Automatically deduplicates ngrams
 * to avoid redundant invalidation work.
 *
 * Two-phase invalidation:
 * 1. Phase 1 (Immediate): Extract ngrams, mark cache entries as invalidated
 * 2. Phase 2 (Deferred): Batch process, erase invalidated entries from cache
 */
class InvalidationQueue {
 public:
  /**
   * @brief Constructor
   * @param cache Pointer to query cache
   * @param invalidation_mgr Pointer to invalidation manager
   * @param table_contexts Map of table name to TableContext pointer (for per-table ngram settings)
   * @note table_contexts must remain valid for the lifetime of this InvalidationQueue instance
   */
  InvalidationQueue(QueryCache* cache, InvalidationManager* invalidation_mgr, NgramConfigMap ngram_configs);

  /**
   * @brief Destructor
   */
  ~InvalidationQueue();

  // Non-copyable, non-movable
  InvalidationQueue(const InvalidationQueue&) = delete;
  InvalidationQueue& operator=(const InvalidationQueue&) = delete;
  InvalidationQueue(InvalidationQueue&&) = delete;
  InvalidationQueue& operator=(InvalidationQueue&&) = delete;

  /**
   * @brief Enqueue invalidation event (non-blocking)
   *
   * Extracts ngrams from old/new text and marks affected cache entries
   * as invalidated immediately (Phase 1). Actual erasure is deferred
   * to background worker (Phase 2).
   *
   * @param table_name Table that was modified
   * @param old_text Previous text content
   * @param new_text New text content
   */
  void Enqueue(const std::string& table_name, const std::string& old_text, const std::string& new_text,
               bool filter_columns_changed = false);

  /**
   * @brief Start background worker thread for batch processing
   */
  void Start();

  /**
   * @brief Stop worker thread gracefully
   *
   * Caller must ensure that the CacheManager (cache_) and InvalidationManager
   * (invalidation_mgr_) outlive this object, as Stop() processes remaining
   * items after the worker thread joins.
   */
  void Stop();

  /**
   * @brief Check if worker is running
   */
  [[nodiscard]] bool IsRunning() const { return running_.load(); }

  /**
   * @brief Set batch size threshold
   * @param batch_size Process after N unique (table, cache_key) pairs
   */
  void SetBatchSize(size_t batch_size) { batch_size_ = batch_size; }

  /**
   * @brief Set maximum delay before processing
   * @param max_delay_ms Max delay in milliseconds
   */
  void SetMaxDelay(int max_delay_ms) { max_delay_ = std::chrono::milliseconds(max_delay_ms); }

  /**
   * @brief Set maximum queue size for backpressure
   * @param max_queue_size Max pending entries before dropping new ones
   */
  void SetMaxQueueSize(size_t max_queue_size) { max_queue_size_ = max_queue_size; }

  /**
   * @brief Get pending invalidation count
   */
  [[nodiscard]] size_t GetPendingCount() const;

 private:
  QueryCache* cache_;                      ///< Pointer to query cache
  InvalidationManager* invalidation_mgr_;  ///< Pointer to invalidation manager
  NgramConfigMap ngram_configs_;           ///< Per-table N-gram settings for invalidation

  /**
   * @brief Typed composite key for the pending-invalidation map.
   *
   * Keying directly on a (table, CacheKey) pair avoids the previous
   * CacheKey -> hex string -> CacheKey round-trip on the invalidation hot
   * path. The previous string-based encoding allocated O(n) strings per
   * Enqueue and parsed them back in ProcessBatch via stoull(); this typed
   * pair eliminates both the allocation and the parsing.
   */
  struct PendingKey {
    std::string table;
    CacheKey key;

    bool operator==(const PendingKey& other) const noexcept { return table == other.table && key == other.key; }
  };

  /**
   * @brief Hash for PendingKey.
   *
   * Combines the table-name hash with the CacheKey hash using the same
   * Fibonacci-mixed combiner used by std::hash<CacheKey>. Plain XOR would
   * re-introduce the swapped-pair collision issue documented in cache_key.h.
   */
  struct PendingKeyHash {
    size_t operator()(const PendingKey& pending) const noexcept {
      constexpr std::uint64_t kFibonacci = 0x9E3779B97F4A7C15ULL;
      constexpr unsigned int kShiftLeft = 6;
      constexpr unsigned int kShiftRight = 2;
      const std::uint64_t table_hash = std::hash<std::string>{}(pending.table);
      const std::uint64_t key_hash = std::hash<CacheKey>{}(pending.key);
      std::uint64_t combined = table_hash;
      combined ^= key_hash + kFibonacci + (combined << kShiftLeft) + (combined >> kShiftRight);
      return static_cast<size_t>(combined);
    }
  };

  // Pending invalidations: (table, CacheKey) -> first seen timestamp.
  // unordered_map automatically deduplicates by PendingKey equality.
  std::unordered_map<PendingKey, std::chrono::steady_clock::time_point, PendingKeyHash> pending_cache_keys_;

  mutable std::mutex queue_mutex_;
  std::condition_variable queue_cv_;
  std::thread worker_thread_;
  std::atomic<bool> running_{false};
  std::atomic<bool> stopped_{false};  ///< Set by Stop(), prevents post-shutdown enqueues

  // Configuration defaults (match CacheConfig::invalidation defaults)
  static constexpr size_t kDefaultBatchSize = 1000;
  static constexpr int kDefaultMaxDelayMs = 100;
  static constexpr size_t kDefaultMaxQueueSize = 100000;  ///< Max pending entries before dropping

  size_t batch_size_ = kDefaultBatchSize;                    ///< Process after N cache keys
  std::chrono::milliseconds max_delay_{kDefaultMaxDelayMs};  ///< Max delay before processing

  size_t max_queue_size_ = kDefaultMaxQueueSize;  ///< Max pending entries (backpressure)

  /// Oldest timestamp in pending_cache_keys_ (avoids O(n) scan in WorkerLoop)
  std::chrono::steady_clock::time_point oldest_timestamp_{std::chrono::steady_clock::time_point::max()};

  /**
   * @brief Worker thread main loop
   */
  void WorkerLoop();

  /**
   * @brief Process batch of pending invalidations
   */
  void ProcessBatch();
};

}  // namespace mygramdb::cache
