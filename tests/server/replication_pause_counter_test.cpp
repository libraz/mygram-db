/**
 * @file replication_pause_counter_test.cpp
 * @brief Unit tests for replication_rp::Scope and the underlying counter.
 *
 * Each test owns an independent Counter instance. This verifies the
 * non-global injection model and prevents state leakage between tests.
 */
#include "server/replication_pause_counter.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <thread>
#include <utility>

#include "mysql/binlog_reader_interface.h"
#include "server/replication_pause_guard.h"

namespace rp = mygramdb::server::replication_pause;

namespace {

class PauseGuardReader final : public mygramdb::mysql::IBinlogReader {
 public:
  mygram::utils::Expected<void, mygram::utils::Error> Start() override {
    ++start_count;
    if (fail_start) {
      return mygram::utils::MakeUnexpected(
          mygram::utils::MakeError(mygram::utils::ErrorCode::kMySQLConnectionFailed, "restart failed"));
    }
    running = true;
    return {};
  }
  void Stop() override {
    ++stop_count;
    running = false;
  }
  bool IsRunning() const override { return running; }
  std::string GetCurrentGTID() const override { return gtid; }
  void SetCurrentGTID(const std::string& value) override { gtid = value; }
  std::string GetLastError() const override { return {}; }
  uint64_t GetProcessedEvents() const override { return 0; }
  size_t GetQueueSize() const override { return 0; }

  bool running = true;
  bool fail_start = false;
  std::string gtid = "uuid:42";
  int start_count = 0;
  int stop_count = 0;
};

}  // namespace

class ReplicationPauseScopeTest : public ::testing::Test {
 protected:
  rp::Counter counter_;
};

TEST_F(ReplicationPauseScopeTest, AcquireOnFreshCounterReportsFirstPauser) {
  rp::Scope scope(counter_);
  EXPECT_FALSE(scope.held());
  EXPECT_TRUE(scope.Acquire()) << "first Acquire() on a 0-counter must report 0->1 transition";
  EXPECT_TRUE(scope.held());
  EXPECT_TRUE(counter_.IsPaused());
}

TEST_F(ReplicationPauseScopeTest, SecondConcurrentScopeIsNotFirstPauser) {
  rp::Scope first(counter_);
  ASSERT_TRUE(first.Acquire());
  rp::Scope second(counter_);
  EXPECT_FALSE(second.Acquire()) << "counter is already 1; this Acquire() is the 1->2 transition";
  EXPECT_TRUE(counter_.IsPaused());
}

TEST_F(ReplicationPauseScopeTest, ExplicitReleaseReportsLastReleaser) {
  rp::Scope scope(counter_);
  ASSERT_TRUE(scope.Acquire());
  EXPECT_TRUE(scope.Release()) << "single holder Release() must report 1->0 transition";
  EXPECT_FALSE(scope.held());
  EXPECT_FALSE(counter_.IsPaused());
}

TEST_F(ReplicationPauseScopeTest, ReleaseSuppressesDestructorRelease) {
  // After explicit Release(), the dtor must not decrement again. Otherwise
  // a second pauser would see a phantom 0 underflow on its own release.
  {
    rp::Scope scope(counter_);
    ASSERT_TRUE(scope.Acquire());
    EXPECT_TRUE(scope.Release());
  }
  EXPECT_EQ(0, static_cast<int>(counter_.IsPaused()));

  rp::Scope second(counter_);
  EXPECT_TRUE(second.Acquire()) << "counter must be back at 0 with no underflow saturation";
}

TEST_F(ReplicationPauseScopeTest, DestructorReleasesIfNotExplicit) {
  // The RAII safety net: if the caller forgets to Release(), the dtor must
  // drop the counter so the program does not deadlock on a leaked pause.
  {
    rp::Scope scope(counter_);
    ASSERT_TRUE(scope.Acquire());
    ASSERT_TRUE(counter_.IsPaused());
  }
  EXPECT_FALSE(counter_.IsPaused()) << "dtor must release a held but unreleased Scope";
}

TEST_F(ReplicationPauseScopeTest, DoubleAcquireIsNoOp) {
  // Acquiring twice on the same Scope is a programming bug; the helper
  // defends by no-oping the second call so the destructor only releases
  // once. Without this guard, double-acquire would leak +1 in the counter.
  rp::Scope scope(counter_);
  ASSERT_TRUE(scope.Acquire());
  EXPECT_FALSE(scope.Acquire()) << "second Acquire() must be a no-op returning false";

  ASSERT_TRUE(scope.Release());
  EXPECT_FALSE(counter_.IsPaused()) << "counter must be 0 after one Acquire/Release pair";
}

TEST_F(ReplicationPauseScopeTest, DoubleReleaseIsNoOp) {
  // Symmetric to double-Acquire: a second Release() must not underflow.
  rp::Scope scope(counter_);
  ASSERT_TRUE(scope.Acquire());
  EXPECT_TRUE(scope.Release());
  EXPECT_FALSE(scope.Release()) << "second Release() must be a no-op returning false";
}

TEST_F(ReplicationPauseScopeTest, ReleaseWithoutAcquireIsNoOp) {
  rp::Scope scope(counter_);
  EXPECT_FALSE(scope.Release()) << "Release() without Acquire() must not touch the counter";
  EXPECT_FALSE(counter_.IsPaused());
}

TEST_F(ReplicationPauseScopeTest, MoveConstructionTransfersOwnership) {
  // After move, the source must not release in its dtor (would double-
  // decrement), and the sink must release on its dtor (the only remaining
  // owner of the increment).
  rp::Scope source(counter_);
  ASSERT_TRUE(source.Acquire());
  ASSERT_TRUE(counter_.IsPaused());

  {
    rp::Scope sink(std::move(source));
    EXPECT_TRUE(sink.held());
    // Source must report no held state so its dtor does nothing.
    EXPECT_FALSE(source.held());  // NOLINT(bugprone-use-after-move) — testing post-move state intentionally
  }

  EXPECT_FALSE(counter_.IsPaused()) << "sink's dtor released; source's dtor must have been a no-op";
}

TEST_F(ReplicationPauseScopeTest, MultipleScopesNestRequestRelease) {
  // Smoke test for the production pattern: two operations request pause
  // concurrently, only the first one observes "first pauser", only the
  // last one observes "last releaser".
  rp::Scope first(counter_);
  rp::Scope second(counter_);
  EXPECT_TRUE(first.Acquire());
  EXPECT_FALSE(second.Acquire());

  EXPECT_FALSE(first.Release()) << "first to release while second still holds is NOT last_releaser";
  EXPECT_TRUE(second.Release()) << "second (and only remaining) holder IS last_releaser";
  EXPECT_FALSE(counter_.IsPaused());
}

TEST(ReplicationPauseGuardTest, PauseDrainsAndRestoreRestartsReader) {
  rp::Counter counter;
  PauseGuardReader reader;
  std::atomic<bool> paused{false};

  rp::Guard guard(&reader, counter, paused, nullptr, "test");
  const auto result = guard.Pause();

  EXPECT_TRUE(result.engaged);
  EXPECT_TRUE(result.first_pauser);
  EXPECT_EQ(result.drained_gtid, "uuid:42");
  EXPECT_EQ(reader.stop_count, 1);
  EXPECT_FALSE(reader.running);
  EXPECT_TRUE(paused.load(std::memory_order_acquire));

  ASSERT_TRUE(guard.Restore().has_value());
  EXPECT_EQ(reader.start_count, 1);
  EXPECT_TRUE(reader.running);
  EXPECT_FALSE(paused.load(std::memory_order_acquire));
  EXPECT_FALSE(counter.IsPaused());
}

TEST(ReplicationPauseGuardTest, DestructorRestoresAfterEarlyReturn) {
  rp::Counter counter;
  PauseGuardReader reader;
  reader.gtid.clear();
  std::atomic<bool> paused{false};

  {
    rp::Guard guard(&reader, counter, paused, nullptr, "test");
    EXPECT_TRUE(guard.Pause().engaged);
    // Models the empty-GTID early return in SnapshotScheduler.
  }

  EXPECT_EQ(reader.stop_count, 1);
  EXPECT_EQ(reader.start_count, 1);
  EXPECT_TRUE(reader.running);
  EXPECT_FALSE(paused.load(std::memory_order_acquire));
  EXPECT_FALSE(counter.IsPaused());
}

TEST(ReplicationPauseGuardTest, LaterOperationJoinsAlreadyStoppedPause) {
  rp::Counter counter;
  PauseGuardReader reader;
  std::atomic<bool> paused{false};
  rp::Guard first(&reader, counter, paused, nullptr, "first");
  ASSERT_TRUE(first.Pause().first_pauser);

  rp::Guard second(&reader, counter, paused, nullptr, "second");
  const auto second_result = second.Pause();
  EXPECT_TRUE(second_result.engaged);
  EXPECT_FALSE(second_result.first_pauser);
  EXPECT_EQ(second_result.drained_gtid, "uuid:42");

  ASSERT_TRUE(first.Restore().has_value());
  EXPECT_EQ(reader.start_count, 0) << "the first operation must not restart under the later operation";
  EXPECT_TRUE(paused.load(std::memory_order_acquire));

  ASSERT_TRUE(second.Restore().has_value());
  EXPECT_EQ(reader.start_count, 1);
  EXPECT_TRUE(reader.running);
  EXPECT_FALSE(paused.load(std::memory_order_acquire));
}

TEST(ReplicationPauseGuardTest, RestoreReportsStartFailureAndClearsPauseState) {
  rp::Counter counter;
  PauseGuardReader reader;
  std::atomic<bool> paused{false};
  rp::Guard guard(&reader, counter, paused, nullptr, "test");
  ASSERT_TRUE(guard.Pause().engaged);
  reader.fail_start = true;

  const auto restored = guard.Restore();
  ASSERT_FALSE(restored.has_value());
  EXPECT_EQ(restored.error().code(), mygram::utils::ErrorCode::kMySQLConnectionFailed);
  EXPECT_FALSE(paused.load(std::memory_order_acquire));
  EXPECT_FALSE(counter.IsPaused());
}

TEST_F(ReplicationPauseScopeTest, LaterPauserWaitsForPublishedDrainedGtid) {
  rp::Scope first(counter_);
  ASSERT_TRUE(first.Acquire());

  auto waiter = std::async(std::launch::async, [this]() { return counter_.WaitForDrainedGTID(); });
  EXPECT_EQ(waiter.wait_for(std::chrono::milliseconds(20)), std::future_status::timeout)
      << "a later pauser must not observe GTID until the first pauser publishes the drained position";

  counter_.PublishDrainedGTID("uuid:42");
  EXPECT_EQ(waiter.wait_for(std::chrono::seconds(1)), std::future_status::ready);
  EXPECT_EQ(waiter.get(), "uuid:42");
  EXPECT_TRUE(first.Release());
}

TEST_F(ReplicationPauseScopeTest, NewFirstPauserResetsPreviousDrainedGtid) {
  {
    rp::Scope first(counter_);
    ASSERT_TRUE(first.Acquire());
    counter_.PublishDrainedGTID("uuid:1");
    ASSERT_TRUE(first.Release());
  }

  rp::Scope next(counter_);
  ASSERT_TRUE(next.Acquire());

  auto waiter = std::async(std::launch::async, [this]() { return counter_.WaitForDrainedGTID(); });
  EXPECT_EQ(waiter.wait_for(std::chrono::milliseconds(20)), std::future_status::timeout)
      << "a new pause epoch must not reuse the previous drained GTID";

  counter_.PublishDrainedGTID("uuid:2");
  EXPECT_EQ(waiter.wait_for(std::chrono::seconds(1)), std::future_status::ready);
  EXPECT_EQ(waiter.get(), "uuid:2");
  EXPECT_TRUE(next.Release());
}

TEST_F(ReplicationPauseScopeTest, ReleaseWithoutPublishWakesDrainedGtidWaiter) {
  rp::Scope first(counter_);
  ASSERT_TRUE(first.Acquire());

  auto waiter = std::async(std::launch::async, [this]() { return counter_.WaitForDrainedGTID(); });
  EXPECT_EQ(waiter.wait_for(std::chrono::milliseconds(20)), std::future_status::timeout)
      << "waiter should block while pause epoch is active and no GTID is published";

  EXPECT_TRUE(first.Release());
  EXPECT_EQ(waiter.wait_for(std::chrono::seconds(1)), std::future_status::ready);
  EXPECT_EQ(waiter.get(), "");
}
