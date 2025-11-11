/**
 * @file thread_pool.cpp
 * @brief Thread pool implementation
 */

#include "thread_pool.h"
#include <spdlog/spdlog.h>

namespace mygramdb {
namespace server {

ThreadPool::ThreadPool(size_t num_threads, size_t queue_size)
    : max_queue_size_(queue_size) {
  // Default to CPU count if not specified
  if (num_threads == 0) {
    num_threads = std::thread::hardware_concurrency();
    if (num_threads == 0) {
      num_threads = 4;  // Fallback
    }
  }

  spdlog::info("Creating thread pool with {} workers, queue size: {}",
               num_threads, queue_size == 0 ? "unbounded" : std::to_string(queue_size));

  // Start worker threads
  workers_.reserve(num_threads);
  for (size_t i = 0; i < num_threads; ++i) {
    workers_.emplace_back(&ThreadPool::WorkerThread, this);
  }
}

ThreadPool::~ThreadPool() {
  Shutdown();
}

bool ThreadPool::Submit(Task task) {
  {
    std::unique_lock<std::mutex> lock(queue_mutex_);

    // Check if shutting down
    if (shutdown_) {
      return false;
    }

    // Check queue size limit
    if (max_queue_size_ > 0 && tasks_.size() >= max_queue_size_) {
      return false;  // Queue is full
    }

    // Add task to queue
    tasks_.push(std::move(task));
  }

  // Notify one worker
  condition_.notify_one();
  return true;
}

size_t ThreadPool::GetQueueSize() const {
  std::scoped_lock lock(queue_mutex_);
  return tasks_.size();
}

void ThreadPool::Shutdown() {
  {
    std::scoped_lock lock(queue_mutex_);
    if (shutdown_) {
      return;  // Already shutting down
    }
    shutdown_ = true;
  }

  // Wake up all workers
  condition_.notify_all();

  // Wait for all workers to finish
  for (auto& worker : workers_) {
    if (worker.joinable()) {
      worker.join();
    }
  }

  spdlog::info("Thread pool shut down");
}

void ThreadPool::WorkerThread() {
  while (true) {
    Task task;

    {
      std::unique_lock<std::mutex> lock(queue_mutex_);

      // Wait for task or shutdown
      condition_.wait(lock, [this] {
        return shutdown_ || !tasks_.empty();
      });

      // Exit if shutting down and no more tasks
      if (shutdown_ && tasks_.empty()) {
        return;
      }

      // Get next task
      if (!tasks_.empty()) {
        task = std::move(tasks_.front());
        tasks_.pop();
      }
    }

    // Execute task (outside lock)
    if (task) {
      try {
        task();
      } catch (const std::exception& e) {
        spdlog::error("Exception in worker thread: {}", e.what());
      } catch (...) {
        spdlog::error("Unknown exception in worker thread");
      }
    }
  }
}

}  // namespace server
}  // namespace mygramdb
