/**
 * @file periodic_worker_test.cpp
 * @brief Unit tests for utils::PeriodicWorker.
 *
 * The worker is the unified replacement for several hand-rolled
 * std::condition_variable + std::thread + atomic<bool> trios. The tests
 * here pin down the contract that the rate-limiter sweeper and
 * query-cache LRU refresher both depend on:
 *
 *   1. Start() rejects bogus inputs (already-running, interval<=0,
 *      empty task) with structured errors instead of silently spinning.
 *   2. The callback fires AT LEAST once when given enough time, and
 *      stays fast on Stop() (i.e. Stop returns within a small fraction
 *      of the configured interval, NOT after the interval elapses).
 *   3. Destructor calls Stop() so callers do not leak threads.
 *   4. A throwing callback does not crash the worker; subsequent
 *      ticks still fire.
 *
 * Test labels: this file contains a SLOW test (Stop() promptness) that
 * sleeps for ~200 ms; CMake registration must include LABELS "SLOW".
 */
#include "utils/periodic_worker.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <stdexcept>
#include <thread>

using mygram::utils::PeriodicWorker;

TEST(PeriodicWorkerTest, StartFailsOnZeroInterval) {
  PeriodicWorker worker("test_zero_interval");
  auto result = worker.Start([] {}, std::chrono::milliseconds(0));
  ASSERT_FALSE(result.has_value()) << "interval=0 must be rejected, not silently busy-spin";
}

TEST(PeriodicWorkerTest, StartFailsOnEmptyTask) {
  PeriodicWorker worker("test_empty_task");
  auto result = worker.Start({}, std::chrono::milliseconds(50));
  ASSERT_FALSE(result.has_value()) << "empty std::function task must be rejected";
}

TEST(PeriodicWorkerTest, DoubleStartFailsOnSecondCall) {
  PeriodicWorker worker("test_double_start");
  ASSERT_TRUE(worker.Start([] {}, std::chrono::milliseconds(50)).has_value());
  auto second = worker.Start([] {}, std::chrono::milliseconds(50));
  EXPECT_FALSE(second.has_value()) << "starting an already-running worker must error";
  worker.Stop();
}

TEST(PeriodicWorkerTest, StopOnIdleWorkerIsNoOp) {
  // Construct, never Start, immediately destruct: must not crash, must
  // not block on a never-spawned thread.
  PeriodicWorker worker("test_idle_stop");
  worker.Stop();
  EXPECT_FALSE(worker.IsRunning());
}

TEST(PeriodicWorkerTest, DestructorStopsRunningWorker) {
  std::atomic<int> ticks{0};
  {
    PeriodicWorker worker("test_dtor_stop");
    ASSERT_TRUE(worker.Start([&ticks] { ticks.fetch_add(1, std::memory_order_relaxed); }, std::chrono::milliseconds(20))
                    .has_value());
    // Give the worker a chance to fire at least once.
    std::this_thread::sleep_for(std::chrono::milliseconds(80));
  }
  // After dtor, no further ticks are possible. Capture the count and
  // confirm it does not increase even after a brief pause.
  const int before = ticks.load();
  std::this_thread::sleep_for(std::chrono::milliseconds(80));
  EXPECT_EQ(before, ticks.load()) << "ticks must stop after destructor";
  EXPECT_GE(before, 1) << "callback should have fired at least once during the lifetime";
}

TEST(PeriodicWorkerTest, CallbackFiresMultipleTimes) {
  std::atomic<int> ticks{0};
  PeriodicWorker worker("test_multi_tick");
  ASSERT_TRUE(worker.Start([&ticks] { ticks.fetch_add(1, std::memory_order_relaxed); }, std::chrono::milliseconds(20))
                  .has_value());
  // Wait long enough that we expect at least 3 ticks even on slow CI.
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  worker.Stop();
  EXPECT_GE(ticks.load(), 2) << "expected the worker to fire repeatedly, not just once";
}

TEST(PeriodicWorkerTest, StopReturnsFastEvenWithLongInterval) {
  // Pin down the "Stop is fast" guarantee: with a 5-second interval, Stop
  // must NOT wait the full 5 seconds for the wait_for to expire. The cv
  // wake should let it return within milliseconds. We allow a generous
  // margin (500 ms) so this stays robust on slow CI.
  PeriodicWorker worker("test_fast_stop");
  ASSERT_TRUE(worker.Start([] {}, std::chrono::seconds(5)).has_value());
  // Let the worker enter wait_for() before stopping.
  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  const auto t0 = std::chrono::steady_clock::now();
  worker.Stop();
  const auto elapsed = std::chrono::steady_clock::now() - t0;
  EXPECT_LT(elapsed, std::chrono::milliseconds(500)) << "Stop() must not wait for the full interval";
}

TEST(PeriodicWorkerTest, ThrowingCallbackDoesNotKillWorker) {
  // A throwing callback would call std::terminate via the worker
  // thread's stack unwind; the worker must catch and continue so a
  // transient task failure does not bring the whole process down.
  std::atomic<int> ticks{0};
  PeriodicWorker worker("test_throwing_callback");
  ASSERT_TRUE(worker
                  .Start(
                      [&ticks] {
                        const int n = ticks.fetch_add(1, std::memory_order_relaxed);
                        if (n == 0) {
                          throw std::runtime_error("first tick fails");
                        }
                      },
                      std::chrono::milliseconds(20))
                  .has_value());
  std::this_thread::sleep_for(std::chrono::milliseconds(150));
  worker.Stop();
  EXPECT_GE(ticks.load(), 2) << "subsequent ticks must keep firing after a throwing call";
}

TEST(PeriodicWorkerTest, NoExtraTaskAfterStopRequested) {
  // Regression test for the post-unlock recheck in Loop(). Without the
  // recheck, the worker could fire one extra task() invocation AFTER
  // Stop() had already published should_stop_ = true, because the
  // original code released mutex_ between the in-lock should_stop_
  // check and the task() call. A Stop() that arrived in that unlocked
  // window would set should_stop_ and notify, but the worker had
  // already committed to calling task() once more.
  //
  // Stop() joins the worker, so any "extra" task DOES complete before
  // Stop() returns; what the fix actually prevents is a task() call
  // that happens *after the program logically requested shutdown*.
  // We detect that by having the callback observe should_stop_-style
  // state via a test-controlled atomic: the test sets `stop_requested`
  // immediately before Stop(), and the callback records any invocation
  // that sees `stop_requested == true`. With the fix, no callback
  // observes the request flag; without the fix, occasional callbacks
  // do, because they raced past the in-lock check before
  // stop_requested + Stop() were issued, and then re-acquired the cpu
  // after the unlock with the request flag already true.
  //
  // We use a long-running callback (sleep) and a short interval so the
  // worker spends most time inside task_(). Right after the worker
  // exits a task call and re-enters Loop(), the test thread sets
  // stop_requested + calls Stop(). The buggy code would, on the very
  // next loop turn, fall through the unlock-to-task_() window with
  // stop_requested already true; the fix observes should_stop_ in the
  // post-unlock recheck and breaks first.
  //
  // 200 iterations to make the narrow window reliable on slow CI.
  constexpr int kIterations = 200;
  for (int i = 0; i < kIterations; ++i) {
    std::atomic<int> ticks{0};
    std::atomic<int> ticks_after_stop_requested{0};
    std::atomic<bool> stop_requested{false};

    PeriodicWorker worker("test_no_extra_after_stop");
    ASSERT_TRUE(worker
                    .Start(
                        [&] {
                          if (stop_requested.load(std::memory_order_acquire)) {
                            ticks_after_stop_requested.fetch_add(1, std::memory_order_relaxed);
                          }
                          ticks.fetch_add(1, std::memory_order_relaxed);
                          // Long-ish task body so the worker spends most
                          // of its time outside the wait_for() and the
                          // unlock-to-task_() race window is exercised
                          // every loop turn.
                          std::this_thread::sleep_for(std::chrono::milliseconds(2));
                        },
                        std::chrono::milliseconds(1))
                    .has_value());

    // Wait for the worker to enter steady-state ticking so it is
    // cycling through the unlock-task_()-relock loop, not just sitting
    // in the first wait_for().
    while (ticks.load(std::memory_order_acquire) < 2) {
      std::this_thread::sleep_for(std::chrono::microseconds(100));
    }

    // Set stop_requested BEFORE Stop(). Stop() inside takes mutex_,
    // sets should_stop_, notifies, and joins. Under the bug, a worker
    // thread sitting between lock.unlock() and task_() will still call
    // task_() once more — and because we set stop_requested first,
    // that invocation will be counted in ticks_after_stop_requested.
    stop_requested.store(true, std::memory_order_release);
    worker.Stop();

    const int leaked = ticks_after_stop_requested.load(std::memory_order_acquire);
    ASSERT_EQ(0, leaked) << "task fired after stop was requested (iteration " << i << ")";
  }
}

TEST(PeriodicWorkerTest, RestartAfterStopWorks) {
  // After Stop(), Start() must accept again so the same worker instance
  // can be recycled (e.g. by tests that reset state between cases).
  std::atomic<int> ticks{0};
  PeriodicWorker worker("test_restart");
  ASSERT_TRUE(worker.Start([&ticks] { ticks.fetch_add(1); }, std::chrono::milliseconds(20)).has_value());
  std::this_thread::sleep_for(std::chrono::milliseconds(60));
  worker.Stop();
  const int after_first = ticks.load();
  EXPECT_GE(after_first, 1);

  ASSERT_TRUE(worker.Start([&ticks] { ticks.fetch_add(1); }, std::chrono::milliseconds(20)).has_value());
  std::this_thread::sleep_for(std::chrono::milliseconds(60));
  worker.Stop();
  EXPECT_GT(ticks.load(), after_first) << "second start must fire its own ticks";
}
