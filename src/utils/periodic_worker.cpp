/**
 * @file periodic_worker.cpp
 * @brief Implementation of PeriodicWorker.
 */

#include "utils/periodic_worker.h"

#include <exception>
#include <utility>

#include "utils/structured_log.h"

namespace mygram::utils {

PeriodicWorker::PeriodicWorker(std::string name) : name_(std::move(name)) {}

PeriodicWorker::~PeriodicWorker() {
  Stop();
}

Expected<void, Error> PeriodicWorker::Start(Task task, std::chrono::milliseconds interval) {
  if (running_.load(std::memory_order_acquire)) {
    auto error = MakeError(ErrorCode::kNetworkAlreadyRunning, "PeriodicWorker '" + name_ + "' already running");
    StructuredLog().Event("periodic_worker_start_failed").Field("name", name_).Field("error", error.to_string()).Error();
    return MakeUnexpected(error);
  }
  if (interval.count() <= 0) {
    auto error = MakeError(ErrorCode::kInvalidArgument,
                           "PeriodicWorker '" + name_ + "' interval must be > 0; got " +
                               std::to_string(interval.count()) + " ms");
    StructuredLog().Event("periodic_worker_start_failed").Field("name", name_).Field("error", error.to_string()).Error();
    return MakeUnexpected(error);
  }
  if (!task) {
    auto error = MakeError(ErrorCode::kInvalidArgument, "PeriodicWorker '" + name_ + "' task must not be empty");
    StructuredLog().Event("periodic_worker_start_failed").Field("name", name_).Field("error", error.to_string()).Error();
    return MakeUnexpected(error);
  }

  task_ = std::move(task);
  interval_ = interval;
  should_stop_.store(false, std::memory_order_release);
  running_.store(true, std::memory_order_release);

  // Spawn last so the thread observes a fully-initialized worker. The
  // thread constructor synchronizes-with the new thread's first action,
  // so any writes above (task_, interval_, atomics) happen-before the
  // thread reads them.
  thread_ = std::thread(&PeriodicWorker::Loop, this);

  StructuredLog()
      .Event("periodic_worker_started")
      .Field("name", name_)
      .Field("interval_ms", static_cast<uint64_t>(interval.count()))
      .Debug();
  return {};
}

void PeriodicWorker::Stop() noexcept {
  // compare_exchange ensures only one Stop() call performs the join
  // sequence; concurrent Stop() calls (from dtor + explicit Stop) are
  // serialized into a single shutdown.
  bool expected = true;
  if (!running_.compare_exchange_strong(expected, false, std::memory_order_acq_rel)) {
    return;
  }

  // Hold mutex_ briefly to publish should_stop_ before notify so a
  // worker currently checking the predicate cannot miss the wake.
  {
    std::lock_guard<std::mutex> lock(mutex_);
    should_stop_.store(true, std::memory_order_release);
  }
  cv_.notify_all();

  if (thread_.joinable()) {
    thread_.join();
  }

  // Clear the task to drop any captured shared state (e.g. shared_ptrs)
  // so the worker does not hold them beyond Stop(). Safe to do here:
  // the worker thread has joined and no other thread reads task_.
  task_ = {};

  StructuredLog().Event("periodic_worker_stopped").Field("name", name_).Debug();
}

void PeriodicWorker::Loop() {
  std::unique_lock<std::mutex> lock(mutex_);
  while (!should_stop_.load(std::memory_order_acquire)) {
    // wait_for releases mutex_ while sleeping; the predicate makes the
    // wait stop-aware so Stop() can preempt within microseconds of
    // notify_all() instead of holding the join for the full interval.
    cv_.wait_for(lock, interval_, [this] { return should_stop_.load(std::memory_order_acquire); });
    if (should_stop_.load(std::memory_order_acquire)) {
      break;
    }

    // Release mutex_ around the actual task so notify_all() from Stop()
    // can preempt promptly even if the task is long-running. The cv
    // mutex serves only to coordinate the wait; the task callback is
    // expected to manage its own locking.
    lock.unlock();
    try {
      task_();
    } catch (const std::exception& ex) {
      // A throwing callback would otherwise call std::terminate via the
      // worker thread's stack unwind. Swallow + log so periodic work is
      // resilient to transient task failures (e.g. I/O errors during a
      // sweep). Operators can correlate via the named event.
      StructuredLog()
          .Event("periodic_worker_task_failed")
          .Field("name", name_)
          .Field("error", ex.what())
          .Warn();
    } catch (...) {
      StructuredLog().Event("periodic_worker_task_failed").Field("name", name_).Field("error", "unknown").Warn();
    }
    lock.lock();
  }
}

}  // namespace mygram::utils
