/**
 * @file binlog_reader_stop_contract_test.cpp
 * @brief Tests for the IBinlogReader::Stop() synchronous contract (CR-9).
 *
 * The contract: Stop() MUST NOT return until the reader's internal worker
 * thread(s) have fully terminated and are guaranteed to make no further
 * calls into the index/document store. Callers (DumpHandler,
 * SyncOperationManager, SnapshotScheduler) rely on this to safely Clear()
 * downstream state immediately after Stop().
 *
 * These tests use a lightweight mock IBinlogReader implementation that
 * spawns a worker thread and verifies Stop() joins it before returning.
 * They do not require a MySQL connection.
 */

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <string>
#include <thread>

#include "mysql/binlog_reader_interface.h"
#include "mysql/null_binlog_reader.h"

namespace mygramdb::mysql {

/**
 * @brief Mock reader that exercises the synchronous-Stop contract.
 *
 * Spawns a worker thread on Start() that increments tick_count_ in a loop
 * until should_stop_ is set. Stop() must join the worker before returning;
 * the test asserts that worker_running_ becomes observably false at the
 * moment Stop() returns.
 */
class SynchronousStopMockReader final : public IBinlogReader {
 public:
  SynchronousStopMockReader() = default;
  ~SynchronousStopMockReader() override { Stop(); }

  mygram::utils::Expected<void, mygram::utils::Error> Start() override {
    if (worker_running_.exchange(true)) {
      return {};  // already running
    }
    should_stop_.store(false, std::memory_order_release);
    tick_count_.store(0, std::memory_order_release);
    worker_ = std::thread([this]() {
      while (!should_stop_.load(std::memory_order_acquire)) {
        tick_count_.fetch_add(1, std::memory_order_relaxed);
        std::this_thread::sleep_for(std::chrono::microseconds(100));
      }
      // Sentinel: write a distinctive final value so Stop() can verify the
      // worker has fully exited (not just signalled).
      worker_finished_.store(true, std::memory_order_release);
    });
    return {};
  }

  void Stop() override {
    if (!worker_running_.exchange(false)) {
      return;  // already stopped / never started
    }
    should_stop_.store(true, std::memory_order_release);
    if (worker_.joinable()) {
      worker_.join();
    }
    // Synchronous contract: by the time Stop() returns, the worker thread
    // has fully exited and worker_finished_ is true.
  }

  bool IsRunning() const override { return worker_running_.load(std::memory_order_acquire); }
  std::string GetCurrentGTID() const override { return ""; }
  void SetCurrentGTID(const std::string&) override {}
  std::string GetLastError() const override { return ""; }
  uint64_t GetProcessedEvents() const override { return tick_count_.load(std::memory_order_relaxed); }
  size_t GetQueueSize() const override { return 0; }

  bool WorkerFinished() const { return worker_finished_.load(std::memory_order_acquire); }
  uint64_t TickCount() const { return tick_count_.load(std::memory_order_relaxed); }

 private:
  std::atomic<bool> worker_running_{false};
  std::atomic<bool> should_stop_{false};
  std::atomic<bool> worker_finished_{false};
  std::atomic<uint64_t> tick_count_{0};
  std::thread worker_;
};

/**
 * @brief CR-9: After Stop() returns, the worker thread is fully exited.
 */
TEST(BinlogReaderStopContractTest, StopJoinsWorkerThreadSynchronously) {
  SynchronousStopMockReader reader;
  ASSERT_TRUE(reader.Start());

  // Let the worker run a bit so we know it's actually live.
  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  EXPECT_GT(reader.TickCount(), 0u) << "Worker should be incrementing ticks";

  // Stop must synchronously join.
  reader.Stop();
  EXPECT_TRUE(reader.WorkerFinished()) << "Stop() returned but worker has not yet executed its exit sentinel — "
                                          "this violates the IBinlogReader::Stop() synchronous contract";
  EXPECT_FALSE(reader.IsRunning());
}

/**
 * @brief CR-9: After Stop(), tick_count is stable (worker truly stopped).
 *
 * If Stop() returned before joining, the worker would continue to tick for
 * some time afterward. We capture tick_count immediately after Stop(),
 * sleep briefly, and assert it has not advanced.
 */
TEST(BinlogReaderStopContractTest, NoWorkActivityAfterStopReturns) {
  SynchronousStopMockReader reader;
  ASSERT_TRUE(reader.Start());
  std::this_thread::sleep_for(std::chrono::milliseconds(20));

  reader.Stop();
  const uint64_t ticks_after_stop = reader.TickCount();

  // Sleep an order of magnitude longer than the worker's loop period.
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  EXPECT_EQ(reader.TickCount(), ticks_after_stop)
      << "Worker thread continued to tick after Stop() returned — Stop is not synchronous";
}

/**
 * @brief CR-9: Stop() is idempotent — calling it twice does not crash or hang.
 */
TEST(BinlogReaderStopContractTest, StopIsIdempotent) {
  SynchronousStopMockReader reader;
  ASSERT_TRUE(reader.Start());
  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  reader.Stop();
  reader.Stop();  // must be safe
  reader.Stop();  // must be safe
  EXPECT_FALSE(reader.IsRunning());
}

/**
 * @brief CR-9: NullBinlogReader's Stop() trivially satisfies the contract.
 *
 * The null reader has no worker thread, so Stop() is a no-op. The contract
 * is still satisfied (trivially).
 */
TEST(BinlogReaderStopContractTest, NullReaderStopIsTriviallySynchronous) {
  NullBinlogReader reader;
  reader.Stop();
  reader.Stop();
  EXPECT_FALSE(reader.IsRunning());
}

}  // namespace mygramdb::mysql
