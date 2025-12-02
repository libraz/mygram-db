/**
 * @file thread_pool.cpp
 * @brief Thread pool implementation
 */

#include "thread_pool.h"

#include <spdlog/spdlog.h>

#include <chrono>

#include "utils/fd_guard.h"
#include "utils/structured_log.h"

namespace mygramdb::server {

ThreadPool::ThreadPool(size_t num_threads, size_t queue_size) : max_queue_size_(queue_size) {
  // Default to CPU count if not specified
  if (num_threads == 0) {
    num_threads = std::thread::hardware_concurrency();
    if (num_threads == 0) {
      num_threads = 4;  // Fallback
    }
  }

  mygram::utils::StructuredLog()
      .Event("thread_pool_created")
      .Field("workers", static_cast<uint64_t>(num_threads))
      .Field("queue_size", queue_size == 0 ? "unbounded" : std::to_string(queue_size))
      .Debug();

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

void ThreadPool::Shutdown(bool graceful, uint32_t timeout_ms) {
  size_t pending_tasks = 0;

  {
    std::scoped_lock lock(queue_mutex_);
    if (shutdown_) {
      return;  // Already shutting down
    }

    pending_tasks = tasks_.size();

    // If not graceful, clear pending tasks
    if (!graceful && pending_tasks > 0) {
      mygram::utils::StructuredLog()
          .Event("server_warning")
          .Field("operation", "thread_pool_shutdown")
          .Field("type", "non_graceful_shutdown")
          .Field("pending_tasks", static_cast<uint64_t>(pending_tasks))
          .Warn();
      // Clear the queue
      while (!tasks_.empty()) {
        tasks_.pop();
      }
      pending_tasks = 0;
    }

    shutdown_ = true;
  }

  // Wake up all workers
  condition_.notify_all();

  if (graceful && pending_tasks > 0) {
    mygram::utils::StructuredLog()
        .Event("thread_pool_graceful_shutdown")
        .Field("pending_tasks", static_cast<uint64_t>(pending_tasks))
        .Info();

    if (timeout_ms > 0) {
      // Wait with timeout by polling queue status and active workers
      auto start = std::chrono::steady_clock::now();
      auto deadline = start + std::chrono::milliseconds(timeout_ms);

      // Poll until timeout or all tasks complete
      constexpr int kShutdownPollIntervalMs = 10;  // Poll interval for graceful shutdown
      while (std::chrono::steady_clock::now() < deadline) {
        size_t remaining = GetQueueSize();
        size_t active = active_workers_.load();
        if (remaining == 0 && active == 0) {
          break;  // All tasks completed (queue empty and no workers executing)
        }
        // Sleep briefly before checking again
        std::this_thread::sleep_for(std::chrono::milliseconds(kShutdownPollIntervalMs));
      }

      auto elapsed = std::chrono::steady_clock::now() - start;
      if (elapsed >= std::chrono::milliseconds(timeout_ms)) {
        // Timeout reached - log warning but still wait for workers to finish
        // IMPORTANT: We do NOT detach() workers because:
        // - Detached threads may access the pool's members after destruction (use-after-free)
        // - This causes undefined behavior and potential crashes
        // - The timeout only controls how long we wait for tasks to complete
        // - After timeout, we still wait for workers to finish their current tasks
        size_t remaining_tasks = GetQueueSize();
        if (remaining_tasks > 0) {
          mygram::utils::StructuredLog()
              .Event("server_warning")
              .Field("operation", "thread_pool_shutdown")
              .Field("type", "timeout_reached")
              .Field("remaining_tasks", static_cast<uint64_t>(remaining_tasks))
              .Warn();
        }
      }

      // Always join workers to ensure clean shutdown (even after timeout)
      for (auto& worker : workers_) {
        if (worker.joinable()) {
          worker.join();
        }
      }

      if (elapsed < std::chrono::milliseconds(timeout_ms)) {
        mygram::utils::StructuredLog()
            .Event("thread_pool_shutdown")
            .Field("type", "graceful")
            .Field("status", "all_tasks_completed")
            .Debug();
      }
    } else {
      // No timeout - wait for all workers
      for (auto& worker : workers_) {
        if (worker.joinable()) {
          worker.join();
        }
      }
      mygram::utils::StructuredLog()
          .Event("thread_pool_shutdown")
          .Field("type", "graceful")
          .Field("status", "all_tasks_completed")
          .Debug();
    }
  } else {
    // Non-graceful or no pending tasks - just join workers
    for (auto& worker : workers_) {
      if (worker.joinable()) {
        worker.join();
      }
    }
    if (!graceful) {
      mygram::utils::StructuredLog()
          .Event("thread_pool_shutdown")
          .Field("type", "immediate")
          .Field("status", "non_graceful")
          .Debug();
    } else {
      mygram::utils::StructuredLog()
          .Event("thread_pool_shutdown")
          .Field("type", "graceful")
          .Field("status", "no_pending_tasks")
          .Debug();
    }
  }
}

void ThreadPool::WorkerThread() {
  while (true) {
    Task task;

    {
      std::unique_lock<std::mutex> lock(queue_mutex_);

      // Wait for task or shutdown
      condition_.wait(lock, [this] { return shutdown_ || !tasks_.empty(); });

      // Exit if shutting down and no more tasks
      if (shutdown_ && tasks_.empty()) {
        return;
      }

      // Get next task
      // Note: This check is technically redundant after the wait() condition,
      // but kept for defensive programming and clarity
      if (!tasks_.empty()) {
        task = std::move(tasks_.front());
        tasks_.pop();
      }
    }

    // Execute task (outside lock)
    if (task) {
      // RAII guard to ensure active_workers_ is properly managed
      // even if task() throws an exception that escapes the catch blocks
      active_workers_++;
      mygramdb::utils::ScopeGuard worker_guard([this]() { active_workers_--; });

      try {
        task();
      } catch (const std::exception& e) {
        mygram::utils::StructuredLog()
            .Event("server_error")
            .Field("type", "worker_thread_exception")
            .Field("error", e.what())
            .Error();
      } catch (...) {
        mygram::utils::StructuredLog().Event("server_error").Field("type", "worker_thread_unknown_exception").Error();
      }
      // Note: worker_guard will automatically decrement active_workers_
    }
  }
}

}  // namespace mygramdb::server
