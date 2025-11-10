/**
 * @file thread_pool_test.cpp
 * @brief Unit tests for ThreadPool
 */

#include "server/thread_pool.h"
#include <gtest/gtest.h>
#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

namespace mygramdb {
namespace server {

class ThreadPoolTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Nothing to do
  }

  void TearDown() override {
    // Nothing to do
  }
};

TEST_F(ThreadPoolTest, Construction) {
  // Default construction (CPU count threads)
  ThreadPool pool1;
  EXPECT_GT(pool1.GetThreadCount(), 0);
  EXPECT_FALSE(pool1.IsShutdown());

  // Explicit thread count
  ThreadPool pool2(4);
  EXPECT_EQ(pool2.GetThreadCount(), 4);
  EXPECT_FALSE(pool2.IsShutdown());

  // Explicit thread count with queue size
  ThreadPool pool3(2, 100);
  EXPECT_EQ(pool3.GetThreadCount(), 2);
  EXPECT_FALSE(pool3.IsShutdown());
}

TEST_F(ThreadPoolTest, SubmitAndExecute) {
  ThreadPool pool(2);
  std::atomic<int> counter{0};

  // Submit a simple task
  bool submitted = pool.Submit([&counter]() {
    counter++;
  });

  EXPECT_TRUE(submitted);

  // Wait for task to complete
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  EXPECT_EQ(counter, 1);
}

TEST_F(ThreadPoolTest, MultipleTasks) {
  ThreadPool pool(4);
  std::atomic<int> counter{0};
  const int num_tasks = 100;

  // Submit multiple tasks
  for (int i = 0; i < num_tasks; ++i) {
    bool submitted = pool.Submit([&counter]() {
      counter.fetch_add(1);
    });
    EXPECT_TRUE(submitted);
  }

  // Wait for all tasks to complete
  std::this_thread::sleep_for(std::chrono::milliseconds(500));
  EXPECT_EQ(counter, num_tasks);
}

TEST_F(ThreadPoolTest, QueueSize) {
  ThreadPool pool(2, 0);  // Unbounded queue
  EXPECT_EQ(pool.GetQueueSize(), 0);

  std::atomic<bool> start_execution{false};
  std::atomic<int> tasks_completed{0};

  // Submit tasks that will block
  for (int i = 0; i < 5; ++i) {
    pool.Submit([&start_execution, &tasks_completed]() {
      while (!start_execution.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      }
      tasks_completed++;
    });
  }

  // Give threads time to start processing
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  // Queue should have tasks waiting (5 tasks - 2 executing = 3 in queue)
  size_t queue_size = pool.GetQueueSize();
  EXPECT_GE(queue_size, 0);  // At least 0 (may have started executing some)
  EXPECT_LE(queue_size, 3);  // At most 3 (if 2 are executing)

  // Allow tasks to complete
  start_execution = true;
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  EXPECT_EQ(tasks_completed, 5);
  EXPECT_EQ(pool.GetQueueSize(), 0);
}

TEST_F(ThreadPoolTest, BoundedQueue) {
  ThreadPool pool(2, 5);  // 2 threads, queue size 5

  std::atomic<bool> start_execution{false};
  std::atomic<int> tasks_completed{0};
  int successful_submissions = 0;

  // Try to submit more tasks than queue size + thread count
  for (int i = 0; i < 10; ++i) {
    bool submitted = pool.Submit([&start_execution, &tasks_completed]() {
      while (!start_execution.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      }
      tasks_completed++;
    });
    if (submitted) {
      successful_submissions++;
    }
  }

  // Should only accept 7 tasks (2 executing + 5 in queue)
  EXPECT_LE(successful_submissions, 7);

  // Allow tasks to complete
  start_execution = true;
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  EXPECT_EQ(tasks_completed, successful_submissions);
}

TEST_F(ThreadPoolTest, QueueFullRejectsTasks) {
  ThreadPool pool(1, 2);  // 1 thread, queue size 2

  std::atomic<bool> start_execution{false};
  std::atomic<int> tasks_started{0};
  int successful_submissions = 0;

  // Try to submit more tasks than capacity (1 executing + 2 in queue = 3 total)
  for (int i = 0; i < 5; ++i) {
    bool submitted = pool.Submit([&start_execution, &tasks_started]() {
      tasks_started++;
      while (!start_execution.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      }
    });
    if (submitted) {
      successful_submissions++;
    }
  }

  // Should only accept 3 tasks (1 executing + 2 in queue)
  EXPECT_LE(successful_submissions, 3);
  EXPECT_GE(successful_submissions, 1);  // At least 1 should be accepted

  // Clean up
  start_execution = true;
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // Verify that only accepted tasks executed
  EXPECT_EQ(tasks_started, successful_submissions);
}

TEST_F(ThreadPoolTest, Shutdown) {
  ThreadPool pool(2);
  std::atomic<int> counter{0};

  // Submit some tasks
  for (int i = 0; i < 5; ++i) {
    pool.Submit([&counter]() {
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
      counter++;
    });
  }

  EXPECT_FALSE(pool.IsShutdown());

  // Shutdown and wait for tasks to complete
  pool.Shutdown();
  EXPECT_TRUE(pool.IsShutdown());

  // All tasks should have completed
  EXPECT_EQ(counter, 5);
}

TEST_F(ThreadPoolTest, ShutdownRejectsTasks) {
  ThreadPool pool(2);
  std::atomic<int> counter{0};

  // Submit a task
  bool sub1 = pool.Submit([&counter]() {
    counter++;
  });
  EXPECT_TRUE(sub1);

  // Shutdown
  pool.Shutdown();

  // Try to submit after shutdown
  bool sub2 = pool.Submit([&counter]() {
    counter++;
  });
  EXPECT_FALSE(sub2);

  // Wait and verify only first task executed
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  EXPECT_EQ(counter, 1);
}

TEST_F(ThreadPoolTest, DestructorWaitsForTasks) {
  std::atomic<int> counter{0};

  {
    ThreadPool pool(2);

    // Submit tasks
    for (int i = 0; i < 10; ++i) {
      pool.Submit([&counter]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        counter++;
      });
    }

    // Destructor should wait for all tasks
  }

  // After destructor, all tasks should be complete
  EXPECT_EQ(counter, 10);
}

TEST_F(ThreadPoolTest, ConcurrentSubmissions) {
  ThreadPool pool(4);
  std::atomic<int> counter{0};
  const int num_threads = 10;
  const int tasks_per_thread = 100;

  std::vector<std::thread> submitters;
  for (int i = 0; i < num_threads; ++i) {
    submitters.emplace_back([&pool, &counter]() {
      for (int j = 0; j < tasks_per_thread; ++j) {
        pool.Submit([&counter]() {
          counter.fetch_add(1);
        });
      }
    });
  }

  // Wait for all submitters to finish
  for (auto& t : submitters) {
    t.join();
  }

  // Wait for all tasks to execute
  pool.Shutdown();

  EXPECT_EQ(counter, num_threads * tasks_per_thread);
}

TEST_F(ThreadPoolTest, ExceptionHandling) {
  ThreadPool pool(2);
  std::atomic<int> counter{0};

  // Submit a task that throws
  pool.Submit([]() {
    throw std::runtime_error("Test exception");
  });

  // Submit a normal task
  pool.Submit([&counter]() {
    counter++;
  });

  // Wait for tasks to complete
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // Normal task should still execute despite exception in first task
  EXPECT_EQ(counter, 1);
}

TEST_F(ThreadPoolTest, TaskOrdering) {
  ThreadPool pool(1);  // Single thread to ensure ordering
  std::vector<int> execution_order;
  std::mutex mutex;

  // Submit tasks in order
  for (int i = 0; i < 10; ++i) {
    pool.Submit([i, &execution_order, &mutex]() {
      std::lock_guard<std::mutex> lock(mutex);
      execution_order.push_back(i);
    });
  }

  // Wait for completion
  pool.Shutdown();

  // Verify FIFO ordering
  ASSERT_EQ(execution_order.size(), 10);
  for (int i = 0; i < 10; ++i) {
    EXPECT_EQ(execution_order[i], i);
  }
}

}  // namespace server
}  // namespace mygramdb
