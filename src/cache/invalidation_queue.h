/**
 * @file invalidation_queue.h
 * @brief Asynchronous cache invalidation queue with deduplication
 */

#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#include "cache/cache_key.h"

namespace mygramdb::server {
struct TableContext;
}  // namespace mygramdb::server

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
  InvalidationQueue(QueryCache* cache, InvalidationManager* invalidation_mgr,
                    const std::unordered_map<std::string, server::TableContext*>& table_contexts);

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
  void Enqueue(const std::string& table_name, const std::string& old_text, const std::string& new_text);

  /**
   * @brief Start background worker thread for batch processing
   */
  void Start();

  /**
   * @brief Stop worker thread gracefully
   */
  void Stop();

  /**
   * @brief Check if worker is running
   */
  [[nodiscard]] bool IsRunning() const { return running_.load(); }

  /**
   * @brief Set batch size threshold
   * @param batch_size Process after N unique (table, ngram) pairs
   */
  void SetBatchSize(size_t batch_size) { batch_size_ = batch_size; }

  /**
   * @brief Set maximum delay before processing
   * @param max_delay_ms Max delay in milliseconds
   */
  void SetMaxDelay(int max_delay_ms) { max_delay_ = std::chrono::milliseconds(max_delay_ms); }

  /**
   * @brief Get pending invalidation count
   */
  [[nodiscard]] size_t GetPendingCount() const;

 private:
  QueryCache* cache_;                      ///< Pointer to query cache
  InvalidationManager* invalidation_mgr_;  ///< Pointer to invalidation manager
  const std::unordered_map<std::string, server::TableContext*>&
      table_contexts_;  ///< Reference to table contexts for per-table ngram settings

  // Pending invalidations: (table, ngram) -> first seen timestamp
  // Using map to automatically deduplicate
  std::unordered_map<std::string,  // Composite key: "table:ngram"
                     std::chrono::steady_clock::time_point>
      pending_ngrams_;

  mutable std::mutex queue_mutex_;
  std::condition_variable queue_cv_;
  std::thread worker_thread_;
  std::atomic<bool> running_{false};

  // Configuration defaults (match CacheConfig::invalidation defaults)
  static constexpr size_t kDefaultBatchSize = 1000;
  static constexpr int kDefaultMaxDelayMs = 100;

  size_t batch_size_ = kDefaultBatchSize;                    ///< Process after N ngrams
  std::chrono::milliseconds max_delay_{kDefaultMaxDelayMs};  ///< Max delay before processing

  /**
   * @brief Worker thread main loop
   */
  void WorkerLoop();

  /**
   * @brief Process batch of pending invalidations
   */
  void ProcessBatch();

  /**
   * @brief Create composite key for deduplication
   */
  static std::string MakeCompositeKey(const std::string& table, const std::string& cache_key);
};

}  // namespace mygramdb::cache
