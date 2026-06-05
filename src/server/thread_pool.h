/**
 * @file thread_pool.h
 * @brief Thread pool for handling concurrent client connections
 */

#pragma once

#include <atomic>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

namespace mygramdb::server {

/**
 * @brief Thread pool for executing tasks concurrently
 *
 * Features:
 * - Fixed number of worker threads
 * - Bounded task queue with backpressure
 * - Graceful shutdown
 * - Thread-safe task submission
 */
class ThreadPool {
 public:
  using Task = std::function<void()>;
  static constexpr uint32_t kDefaultShutdownTimeoutMs = 30000;

  /**
   * @brief Construct thread pool
   * @param num_threads Number of worker threads (0 = CPU count)
   * @param queue_size Maximum queue size (0 = unbounded)
   */
  explicit ThreadPool(size_t num_threads = 0, size_t queue_size = 0);

  /**
   * @brief Destructor - waits for all tasks to complete
   */
  ~ThreadPool();

  // Non-copyable and non-movable
  ThreadPool(const ThreadPool&) = delete;
  ThreadPool& operator=(const ThreadPool&) = delete;
  ThreadPool(ThreadPool&&) = delete;
  ThreadPool& operator=(ThreadPool&&) = delete;

  /**
   * @brief Submit task to pool
   * @param task Task to execute
   * @return true if submitted, false if queue is full
   */
  bool Submit(Task task);

  /**
   * @brief Get number of worker threads
   */
  size_t GetThreadCount() const { return workers_.size(); }

  /**
   * @brief Get number of pending tasks
   */
  size_t GetQueueSize() const;

  /**
   * @brief Check if pool is shutting down
   */
  bool IsShutdown() const { return shutdown_; }

  /**
   * @brief Shutdown pool and wait for all tasks
   * @param graceful If true, wait for pending tasks to complete. If false, abandon pending tasks.
   * @param timeout_ms Maximum time to wait for pending tasks (0 = no timeout)
   */
  void Shutdown(bool graceful = true, uint32_t timeout_ms = kDefaultShutdownTimeoutMs);

 private:
  std::vector<std::thread> workers_;
  std::queue<Task> tasks_;

  mutable std::mutex queue_mutex_;
  std::condition_variable condition_;
  std::atomic<bool> shutdown_{false};
  std::atomic<size_t> active_workers_{0};  // Number of workers currently executing tasks

  // Idle-state notification used by Shutdown(timeout_ms) to avoid the previous
  // 10ms polling loop. Lives under a separate mutex so signalling never
  // requires queue_mutex_ to be held while a waiter is blocked on this CV.
  mutable std::mutex idle_cv_mutex_;
  std::condition_variable idle_cv_;

  size_t max_queue_size_;

  /**
   * @brief Worker thread function
   */
  void WorkerThread();

  /**
   * @brief Notify any thread waiting on idle_cv_ that the pool may be drained.
   *
   * Safe to call from worker threads after they finish a task, and from
   * Shutdown() to ensure the waiter wakes when shutdown_ flips to true.
   */
  void NotifyIdleObservers();
};

}  // namespace mygramdb::server
