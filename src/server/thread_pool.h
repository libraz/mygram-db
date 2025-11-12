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
   */
  void Shutdown();

 private:
  std::vector<std::thread> workers_;
  std::queue<Task> tasks_;

  mutable std::mutex queue_mutex_;
  std::condition_variable condition_;
  std::atomic<bool> shutdown_{false};

  size_t max_queue_size_;

  /**
   * @brief Worker thread function
   */
  void WorkerThread();
};

}  // namespace mygramdb::server
