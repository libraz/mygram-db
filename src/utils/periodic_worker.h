/**
 * @file periodic_worker.h
 * @brief Background thread that runs a callback at a fixed interval.
 *
 * Several modules in MygramDB needed the same shape of a background
 * thread: "sleep up to N ms, then run a maintenance task; on shutdown,
 * wake immediately instead of holding the join() for the full
 * interval." This type centralizes that pattern so each module no longer
 * carries its own std::condition_variable + atomic<bool> + std::thread
 * plumbing.
 *
 * Lifetime model:
 *   - PeriodicWorker is constructed without arguments. The worker
 *     thread is NOT spawned in the constructor; callers explicitly
 *     invoke Start(...) so the callback can be installed before any
 *     wakeup occurs (avoids a published-but-uncallable thread state).
 *   - Start() is rejected on an already-running worker; recycle via
 *     Stop() first if you need to swap callbacks.
 *   - Stop() is fast: it sets the stop flag, notifies the cv, and
 *     joins. The cv predicate observes the stop flag, so a worker
 *     currently sleeping wakes within microseconds of Stop().
 *   - The destructor calls Stop(); embedders do not need to remember
 *     to do so manually.
 *
 * Threading guarantees:
 *   - Start() and Stop() are NOT thread-safe with each other; embedders
 *     must serialize them (typically by calling them from the owning
 *     object's controller, which already serializes its own lifecycle).
 *   - The callback runs on the worker thread WITHOUT holding any
 *     PeriodicWorker mutex, so it can take its own locks freely.
 *   - The first invocation of the callback fires after one interval —
 *     PeriodicWorker does NOT call the callback eagerly at Start. If
 *     you need an eager first run, do it inline before Start().
 *
 * Failure handling: if the callback throws, the worker logs a structured
 * warning and continues with the next interval. We deliberately do not
 * propagate the exception out of the worker thread (which would call
 * std::terminate); the contract is "best-effort periodic execution",
 * and surfacing the failure to operators via logs keeps the worker
 * resilient to transient failures (e.g. transient I/O errors during a
 * sweep).
 */

#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>

#include "utils/error.h"
#include "utils/expected.h"
#include "utils/namespace_compat.h"

namespace mygramdb::utils {

class PeriodicWorker {
 public:
  using Task = std::function<void()>;

  /**
   * @brief Construct an idle worker. Must be Start()-ed before doing
   *        any work.
   *
   * @param name Short identifier embedded in structured log events
   *             (e.g. "rate_limiter_sweeper"); used for both Start /
   *             Stop event names and any callback-failure warning.
   *             Captured by value so the caller does not need to keep
   *             the underlying string alive.
   */
  explicit PeriodicWorker(std::string name);

  ~PeriodicWorker();

  PeriodicWorker(const PeriodicWorker&) = delete;
  PeriodicWorker& operator=(const PeriodicWorker&) = delete;
  PeriodicWorker(PeriodicWorker&&) = delete;
  PeriodicWorker& operator=(PeriodicWorker&&) = delete;

  /**
   * @brief Start the worker thread.
   *
   * Calling Start() on an already-running worker fails with
   * `kNetworkAlreadyRunning` (the closest fit in the existing error
   * code space; the code is not specific to networking, only to the
   * "already-running" semantic).
   *
   * @param task     Callback to run every @p interval. Captured by
   *                 std::function so std::move-friendly callables work.
   * @param interval Wall-clock interval between callback invocations.
   *                 Must be > 0; a non-positive interval yields an
   *                 `kInvalidArgument` error rather than busy-spinning.
   */
  Expected<void, Error> Start(Task task, std::chrono::milliseconds interval);

  /**
   * @brief Stop the worker thread (idempotent).
   *
   * Returns immediately if the worker is not running. Otherwise sets
   * the stop flag, notifies the cv, and joins the worker thread.
   * Safe to call multiple times.
   */
  void Stop() noexcept;

  /**
   * @brief Whether the worker is currently running.
   *
   * Consulting this is mainly useful in tests; production code should
   * pair Start() / Stop() through the owning object's lifecycle and
   * not introspect via this query.
   */
  bool IsRunning() const noexcept { return running_.load(std::memory_order_acquire); }

 private:
  void Loop();

  std::string name_;
  Task task_;
  std::chrono::milliseconds interval_{0};
  std::atomic<bool> running_{false};
  std::atomic<bool> should_stop_{false};
  std::mutex mutex_;
  std::condition_variable cv_;
  std::thread thread_;
};

}  // namespace mygramdb::utils
