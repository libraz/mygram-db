/**
 * @file thread_pool.cpp
 * @brief Thread pool implementation
 */

#include "thread_pool.h"

#include <spdlog/spdlog.h>

#include <chrono>
#include <sstream>

#include "utils/fd_guard.h"
#include "utils/structured_log.h"

namespace mygramdb::server {

constexpr unsigned int kFallbackCpuCount = 4;  ///< Fallback when runtime can't detect core count

ThreadPool::ThreadPool(size_t num_threads, size_t queue_size) : max_queue_size_(queue_size) {
  // Auto-size the pool when not specified.
  //
  // Under the reactor I/O model, persistent client connections do NOT pin
  // worker threads — they live in the reactor's connection map and only
  // consume a worker briefly (via a drain task) when a complete command
  // frame arrives. Consequently the worker count no longer has to scale with
  // the concurrent-connection count; scaling by CPU count is sufficient.
  //
  // The floor of 4 keeps small VMs and containers responsive even when
  // `hardware_concurrency()` reports a very small value (e.g. 1-2 cores) and
  // gives the reactor somewhere to land burst dispatch tasks.
  if (num_threads == 0) {
    unsigned hw = std::thread::hardware_concurrency();
    if (hw == 0) {
      hw = kFallbackCpuCount;  // Fallback when the runtime can't detect core count
    }
    constexpr size_t kMinAutoWorkers = 4;
    constexpr size_t kAutoWorkerCpuMultiplier = 2;
    num_threads = std::max<size_t>(static_cast<size_t>(hw) * kAutoWorkerCpuMultiplier, kMinAutoWorkers);
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

  // Wake up all workers (and any idle-cv waiter, since shutdown_ is observed
  // by the wait predicate as a wake-up condition for non-graceful paths).
  condition_.notify_all();
  NotifyIdleObservers();

  if (graceful && pending_tasks > 0) {
    mygram::utils::StructuredLog()
        .Event("thread_pool_graceful_shutdown")
        .Field("pending_tasks", static_cast<uint64_t>(pending_tasks))
        .Info();

    if (timeout_ms > 0) {
      // Wait for the queue to drain and all workers to become idle.
      //
      // Previously this was a 10ms polling loop; now a condition_variable is
      // used so the waiter wakes promptly when the last in-flight task
      // finishes. The cv lives under its own mutex (idle_cv_mutex_) and is
      // never acquired while queue_mutex_ is held — this preserves the lock
      // ordering invariant the worker hot path depends on.
      auto start = std::chrono::steady_clock::now();
      auto timeout_duration = std::chrono::milliseconds(timeout_ms);

      {
        std::unique_lock<std::mutex> idle_lock(idle_cv_mutex_);
        idle_cv_.wait_for(idle_lock, timeout_duration,
                          [this] { return GetQueueSize() == 0 && active_workers_.load() == 0; });
      }

      auto elapsed = std::chrono::steady_clock::now() - start;
      if (elapsed >= timeout_duration) {
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
      // even if task() throws an exception that escapes the catch blocks.
      // After the decrement, also notify any Shutdown() waiter so it can wake
      // promptly when the pool fully drains, instead of relying on a polling
      // sleep.
      active_workers_++;
      mygram::utils::ScopeGuard worker_guard([this]() {
        active_workers_--;
        // Cheap check: only signal when we're plausibly idle. The waiter
        // re-checks the predicate under idle_cv_mutex_, so spurious wakeups
        // are safe; we just want to avoid the syscall on every task.
        if (active_workers_.load() == 0) {
          NotifyIdleObservers();
        }
      });

      try {
        task();
      } catch (const std::exception& e) {
        mygram::utils::StructuredLog()
            .Event("server_error")
            .Field("type", "worker_thread_exception")
            .Field("error", e.what())
            .Error();
      } catch (...) {
        std::ostringstream tid;
        tid << std::this_thread::get_id();
        mygram::utils::StructuredLog()
            .Event("server_error")
            .Field("type", "worker_thread_unknown_exception")
            .Field("thread_id", tid.str())
            .Error();
      }
      // Note: worker_guard will automatically decrement active_workers_ and
      // call NotifyIdleObservers() if the pool is now idle.
    }
  }
}

void ThreadPool::NotifyIdleObservers() {
  // Briefly take idle_cv_mutex_ to avoid the lost-wakeup race where the
  // waiter has just evaluated the predicate but not yet entered wait_for.
  // Holding the lock around notify_all() is the standard pattern.
  std::lock_guard<std::mutex> lock(idle_cv_mutex_);
  idle_cv_.notify_all();
}

}  // namespace mygramdb::server
