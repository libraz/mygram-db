/**
 * @file dump_progress_test.cpp
 * @brief Thread safety tests for DumpProgress struct
 */

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

#include "server/server_types.h"

namespace mygramdb::server {

/**
 * @brief Test that JoinWorker is safe to call concurrently
 *
 * DumpProgress::JoinWorker now acquires the mutex before accessing
 * worker_thread, preventing a data race with other methods that also
 * hold the mutex and modify worker_thread.
 */
TEST(DumpProgressTest, JoinWorkerIsThreadSafe) {
  DumpProgress progress;

  // Create a worker thread that finishes quickly
  std::atomic<bool> worker_started{false};
  std::atomic<bool> worker_may_finish{false};
  {
    std::lock_guard<std::mutex> lock(progress.mutex);
    progress.worker_thread = std::make_unique<std::thread>([&]() {
      worker_started.store(true);
      while (!worker_may_finish.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
      }
    });
  }

  // Wait for worker to start
  while (!worker_started.load()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  // Concurrently call JoinWorker and IsInProgress/GetElapsedSeconds
  // to verify no data race on worker_thread access
  std::atomic<bool> stop{false};
  std::vector<std::thread> readers;
  for (int i = 0; i < 3; ++i) {
    readers.emplace_back([&]() {
      while (!stop.load()) {
        // These methods hold the mutex; JoinWorker must also hold
        // the mutex when accessing worker_thread to be safe
        progress.IsInProgress();
        progress.GetElapsedSeconds();
        std::this_thread::sleep_for(std::chrono::microseconds(100));
      }
    });
  }

  // Let readers run briefly, then allow the worker to finish
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  worker_may_finish.store(true);

  // JoinWorker should succeed without deadlock or data race
  progress.JoinWorker();

  stop.store(true);
  for (auto& t : readers) {
    t.join();
  }

  // Verify the worker thread was cleaned up
  {
    std::lock_guard<std::mutex> lock(progress.mutex);
    EXPECT_EQ(progress.worker_thread, nullptr);
  }
}

/**
 * @brief Test that JoinWorker can be called multiple times safely
 */
TEST(DumpProgressTest, JoinWorkerIdempotent) {
  DumpProgress progress;

  // Create and join a worker
  {
    std::lock_guard<std::mutex> lock(progress.mutex);
    progress.worker_thread = std::make_unique<std::thread>([]() {
      // Quick worker
    });
  }

  progress.JoinWorker();
  // Calling again should be a no-op (not crash or deadlock)
  progress.JoinWorker();
  progress.JoinWorker();

  {
    std::lock_guard<std::mutex> lock(progress.mutex);
    EXPECT_EQ(progress.worker_thread, nullptr);
  }
}

/**
 * @brief Test that concurrent Reset and JoinWorker do not deadlock
 */
TEST(DumpProgressTest, ResetAndJoinWorkerNonDeadlock) {
  DumpProgress progress;

  for (int iteration = 0; iteration < 10; ++iteration) {
    // Set up a worker thread
    std::atomic<bool> may_finish{false};
    {
      std::lock_guard<std::mutex> lock(progress.mutex);
      progress.worker_thread = std::make_unique<std::thread>([&may_finish]() {
        while (!may_finish.load()) {
          std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
      });
    }

    may_finish.store(true);

    // Concurrently call Reset and JoinWorker
    std::thread reset_thread([&]() { progress.Reset(DumpStatus::SAVING, "/tmp/test", 1); });
    std::thread join_thread([&]() { progress.JoinWorker(); });

    reset_thread.join();
    join_thread.join();
  }
}

TEST(DumpProgressTest, GetSnapshotReturnsConsistentProgressFields) {
  DumpProgress progress;
  progress.Reset(DumpStatus::SAVING, "/tmp/snapshot.dmp", 3);
  progress.UpdateTable("articles", 2);

  auto snapshot = progress.GetSnapshot();
  EXPECT_EQ(snapshot.status, DumpStatus::SAVING);
  EXPECT_TRUE(snapshot.IsInProgress());
  EXPECT_EQ(snapshot.filepath, "/tmp/snapshot.dmp");
  EXPECT_EQ(snapshot.current_table, "articles");
  EXPECT_EQ(snapshot.tables_processed, 2);
  EXPECT_EQ(snapshot.tables_total, 3);
  EXPECT_GE(snapshot.elapsed_seconds, 0.0);

  progress.Complete("/tmp/snapshot.dmp");
  auto completed = progress.GetSnapshot();
  EXPECT_EQ(completed.status, DumpStatus::COMPLETED);
  EXPECT_FALSE(completed.IsInProgress());
  EXPECT_EQ(completed.last_result_filepath, "/tmp/snapshot.dmp");
  EXPECT_TRUE(completed.error_message.empty());
  EXPECT_GE(completed.elapsed_seconds, 0.0);
}

}  // namespace mygramdb::server
