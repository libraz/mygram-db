/**
 * @file thread_pool_shutdown_test.cpp
 * @brief Test graceful shutdown behavior of ThreadPool
 *
 * This test verifies that:
 * 1. Pending tasks are completed during graceful shutdown
 * 2. Timeout mechanism works correctly
 * 3. Warning is logged for incomplete tasks
 * 4. Multiple shutdown calls are safe
 */

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

#include "server/thread_pool.h"

namespace mygramdb::server {

class ThreadPoolShutdownTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create thread pool with 4 workers
    pool_ = std::make_unique<ThreadPool>(4);
  }

  void TearDown() override {
    if (pool_) {
      pool_->Shutdown();
      pool_.reset();
    }
  }

  std::unique_ptr<ThreadPool> pool_;
};

/**
 * @brief Test that pending tasks are completed during graceful shutdown
 */
TEST_F(ThreadPoolShutdownTest, GracefulShutdownCompletesTasks) {
  std::atomic<int> completed_tasks{0};
  const int total_tasks = 100;

  // Submit many tasks
  for (int i = 0; i < total_tasks; ++i) {
    pool_->Submit([&completed_tasks]() {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      completed_tasks++;
    });
  }

  // Graceful shutdown should wait for all tasks to complete
  pool_->Shutdown(/*graceful=*/true);

  // All tasks should have completed
  EXPECT_EQ(completed_tasks.load(), total_tasks) << "All tasks should complete during graceful shutdown";
}

/**
 * @brief Test that immediate shutdown may not complete all tasks
 */
TEST_F(ThreadPoolShutdownTest, ImmediateShutdownMaySkipTasks) {
  std::atomic<int> completed_tasks{0};
  const int total_tasks = 100;

  // Submit many tasks
  for (int i = 0; i < total_tasks; ++i) {
    pool_->Submit([&completed_tasks]() {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      completed_tasks++;
    });
  }

  // Immediate shutdown may not complete all tasks
  pool_->Shutdown(/*graceful=*/false);

  int completed = completed_tasks.load();
  EXPECT_GT(completed, 0) << "Some tasks should have started";
  // We don't assert that completed < total_tasks because it's timing-dependent
  std::cout << "Completed " << completed << "/" << total_tasks << " tasks with immediate shutdown" << std::endl;
}

/**
 * @brief Test graceful shutdown with timeout
 *
 * Note: Timeout controls how long we wait for queued tasks to complete.
 * After timeout, we still wait for active workers to finish their current tasks.
 * This ensures all workers are properly joined (no detach).
 */
TEST_F(ThreadPoolShutdownTest, GracefulShutdownWithTimeout) {
  std::atomic<int> completed_tasks{0};
  const int total_tasks = 50;

  // Submit tasks that take varying amounts of time
  for (int i = 0; i < total_tasks; ++i) {
    pool_->Submit([&completed_tasks, i]() {
      // First 25 tasks complete quickly, last 25 take longer
      if (i < 25) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      } else {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
      }
      completed_tasks++;
    });
  }

  auto start = std::chrono::steady_clock::now();

  // Shutdown with 500ms timeout
  pool_->Shutdown(/*graceful=*/true, /*timeout_ms=*/500);

  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start);

  int completed = completed_tasks.load();
  std::cout << "Completed " << completed << "/" << total_tasks << " tasks with 500ms timeout" << std::endl;

  // All tasks should complete because we wait for workers to finish
  EXPECT_EQ(completed, total_tasks) << "All tasks should complete (workers are joined)";

  // Should take longer than timeout because we wait for active workers
  EXPECT_GT(duration.count(), 500) << "Should wait for workers beyond timeout";
}

/**
 * @brief Test that multiple shutdown calls are safe
 */
TEST_F(ThreadPoolShutdownTest, MultipleShutdownCallsAreSafe) {
  std::atomic<int> completed_tasks{0};

  // Submit some tasks
  for (int i = 0; i < 10; ++i) {
    pool_->Submit([&completed_tasks]() {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      completed_tasks++;
    });
  }

  // Call shutdown multiple times
  pool_->Shutdown(/*graceful=*/true);
  EXPECT_NO_THROW(pool_->Shutdown(/*graceful=*/true)) << "Second shutdown should be safe";
  EXPECT_NO_THROW(pool_->Shutdown(/*graceful=*/false)) << "Third shutdown should be safe";
}

/**
 * @brief Test that tasks submitted after shutdown are rejected
 */
TEST_F(ThreadPoolShutdownTest, TasksRejectedAfterShutdown) {
  std::atomic<int> completed_tasks{0};

  // Submit initial tasks
  for (int i = 0; i < 5; ++i) {
    pool_->Submit([&completed_tasks]() {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      completed_tasks++;
    });
  }

  // Shutdown
  pool_->Shutdown(/*graceful=*/true);

  // Try to submit more tasks (should be rejected or no-op)
  pool_->Submit([&completed_tasks]() { completed_tasks++; });

  // Give some time for any erroneously submitted tasks to run
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // Should only have completed the initial 5 tasks
  EXPECT_EQ(completed_tasks.load(), 5) << "Tasks submitted after shutdown should be rejected";
}

/**
 * @brief Test queue size during shutdown
 */
TEST_F(ThreadPoolShutdownTest, QueueSizeDuringShutdown) {
  std::atomic<bool> start_processing{false};
  std::atomic<int> completed_tasks{0};

  // Submit tasks that wait for signal
  for (int i = 0; i < 20; ++i) {
    pool_->Submit([&start_processing, &completed_tasks]() {
      // Wait for signal to start processing
      while (!start_processing.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
      }
      completed_tasks++;
    });
  }

  // Give tasks time to queue up
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // Check queue size before starting processing
  size_t queue_size = pool_->GetQueueSize();
  EXPECT_GT(queue_size, 0) << "Queue should have pending tasks";

  // Start processing and shutdown gracefully
  start_processing = true;
  pool_->Shutdown(/*graceful=*/true);

  // Queue should be empty after graceful shutdown
  EXPECT_EQ(pool_->GetQueueSize(), 0) << "Queue should be empty after graceful shutdown";
  EXPECT_EQ(completed_tasks.load(), 20) << "All tasks should complete";
}

/**
 * @brief Test that worker threads are properly joined
 */
TEST_F(ThreadPoolShutdownTest, WorkerThreadsJoined) {
  // Get initial thread count (approximate)
  std::atomic<int> active_workers{0};

  // Submit tasks that increment counter
  for (int i = 0; i < 10; ++i) {
    pool_->Submit([&active_workers]() {
      active_workers++;
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
      active_workers--;
    });
  }

  // Let tasks start
  std::this_thread::sleep_for(std::chrono::milliseconds(20));

  // Shutdown gracefully
  pool_->Shutdown(/*graceful=*/true);

  // All workers should have finished
  EXPECT_EQ(active_workers.load(), 0) << "All workers should be idle after shutdown";
}

/**
 * @brief Stress test: many tasks with graceful shutdown
 */
TEST_F(ThreadPoolShutdownTest, StressTestGracefulShutdown) {
  std::atomic<int> completed_tasks{0};
  const int total_tasks = 1000;

  // Submit many quick tasks
  for (int i = 0; i < total_tasks; ++i) {
    pool_->Submit([&completed_tasks]() {
      // Quick task
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
      completed_tasks++;
    });
  }

  auto start = std::chrono::steady_clock::now();

  // Graceful shutdown
  pool_->Shutdown(/*graceful=*/true);

  auto duration = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - start);

  // Should complete within reasonable time (30 seconds)
  EXPECT_LT(duration.count(), 30) << "Shutdown should complete within 30 seconds";

  // All tasks should complete
  EXPECT_EQ(completed_tasks.load(), total_tasks) << "All tasks should complete";
}

/**
 * @brief Test shutdown behavior with no pending tasks
 */
TEST_F(ThreadPoolShutdownTest, ShutdownWithNoTasks) {
  // Shutdown immediately without submitting tasks
  EXPECT_NO_THROW(pool_->Shutdown(/*graceful=*/true)) << "Shutdown with no tasks should be safe";
  EXPECT_EQ(pool_->GetQueueSize(), 0) << "Queue should be empty";
}

/**
 * @brief Test that GetQueueSize returns 0 after shutdown
 */
TEST_F(ThreadPoolShutdownTest, QueueSizeAfterShutdown) {
  // Submit some tasks
  for (int i = 0; i < 10; ++i) {
    pool_->Submit([]() { std::this_thread::sleep_for(std::chrono::milliseconds(10)); });
  }

  pool_->Shutdown(/*graceful=*/true);

  EXPECT_EQ(pool_->GetQueueSize(), 0) << "Queue should be empty after shutdown";
}

/**
 * @brief Test timeout with workers actively executing tasks
 *
 * This test verifies that the timeout mechanism correctly waits for both:
 * 1. The task queue to be empty
 * 2. All active workers to finish executing their current tasks
 *
 * Previously, the timeout only checked queue size, which could cause premature
 * timeout if workers were still executing tasks.
 */
TEST_F(ThreadPoolShutdownTest, TimeoutWithActiveWorkers) {
  std::atomic<int> completed_tasks{0};
  std::atomic<int> long_tasks_started{0};

  // Submit tasks that take exactly 300ms each
  // With 4 workers, first 4 start immediately, next 4 wait in queue
  for (int i = 0; i < 8; ++i) {
    pool_->Submit([&completed_tasks, &long_tasks_started]() {
      long_tasks_started++;
      std::this_thread::sleep_for(std::chrono::milliseconds(300));
      completed_tasks++;
    });
  }

  // Give time for first 4 tasks to start
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  auto start = std::chrono::steady_clock::now();

  // Shutdown with 400ms timeout
  // Expected: First 4 tasks complete (~300ms), remaining 4 start but timeout
  pool_->Shutdown(/*graceful=*/true, /*timeout_ms=*/400);

  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start);

  // Should timeout after ~400ms (not immediately when queue becomes empty)
  EXPECT_GE(duration.count(), 350) << "Should wait for active workers before timeout";
  EXPECT_LT(duration.count(), 650) << "Should respect timeout even with active workers";

  int completed = completed_tasks.load();
  std::cout << "Completed " << completed << "/8 tasks with 400ms timeout" << std::endl;
  std::cout << "Long tasks started: " << long_tasks_started.load() << std::endl;

  // At least the first 4 should complete (they started before timeout)
  EXPECT_GE(completed, 4) << "Tasks already executing should complete";
}

/**
 * @brief Test that workers are properly joined even after timeout
 *
 * This test verifies that when shutdown timeout is reached, the pool still
 * waits for all workers to complete before destruction. This ensures no
 * use-after-free issues can occur.
 *
 * Previously, workers were detached after timeout, which could cause crashes.
 * Now, we always join workers to ensure safe cleanup.
 */
TEST_F(ThreadPoolShutdownTest, WorkersJoinedAfterTimeout) {
  auto shared_counter = std::make_shared<std::atomic<int>>(0);
  auto start_time = std::chrono::steady_clock::now();

  {
    // Create a separate pool for this test
    ThreadPool temp_pool(2);

    // Submit long-running tasks (500ms each)
    for (int i = 0; i < 4; ++i) {
      temp_pool.Submit([shared_counter]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        (*shared_counter)++;
      });
    }

    // Give tasks time to start
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    // Shutdown with very short timeout (100ms)
    // The timeout will be exceeded, but workers should still be joined
    temp_pool.Shutdown(/*graceful=*/true, /*timeout_ms=*/100);

    // temp_pool destructor waits for all workers to join
  }

  auto elapsed =
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start_time).count();

  // All tasks should have completed because we wait for workers to join
  int completed = shared_counter->load();
  std::cout << "Tasks completed: " << completed << "/4" << std::endl;
  std::cout << "Total time: " << elapsed << "ms" << std::endl;

  // All started tasks should complete (2 workers × 2 tasks = 4 total)
  EXPECT_EQ(completed, 4) << "All tasks should complete (workers are joined, not detached)";

  // Should take longer than timeout (100ms) because we wait for workers
  EXPECT_GT(elapsed, 100) << "Should wait for workers to finish after timeout";
}

}  // namespace mygramdb::server
