/**
 * @file replication_pause_counter_test.cpp
 * @brief Unit tests for replication_rp::Scope and the underlying counter.
 *
 * Each test resets the process-wide counter via ResetForTesting() at setup
 * because the counter is a function-local static shared across the whole
 * process; tests run sequentially within a binary but the test binary may be
 * invoked alongside others, and other production code in the same process
 * would also share this counter (but the test binary has none, so a clean
 * slate at SetUp() is sufficient).
 */
#include "server/replication_pause_counter.h"

#include <gtest/gtest.h>

#include <utility>

namespace rp = mygramdb::server::replication_pause;

class ReplicationPauseScopeTest : public ::testing::Test {
 protected:
  void SetUp() override { rp::ResetForTesting(); }
  void TearDown() override { rp::ResetForTesting(); }
};

TEST_F(ReplicationPauseScopeTest, AcquireOnFreshCounterReportsFirstPauser) {
  rp::Scope scope;
  EXPECT_FALSE(scope.held());
  EXPECT_TRUE(scope.Acquire()) << "first Acquire() on a 0-counter must report 0->1 transition";
  EXPECT_TRUE(scope.held());
  EXPECT_TRUE(rp::IsPaused());
}

TEST_F(ReplicationPauseScopeTest, SecondConcurrentScopeIsNotFirstPauser) {
  rp::Scope first;
  ASSERT_TRUE(first.Acquire());
  rp::Scope second;
  EXPECT_FALSE(second.Acquire()) << "counter is already 1; this Acquire() is the 1->2 transition";
  EXPECT_TRUE(rp::IsPaused());
}

TEST_F(ReplicationPauseScopeTest, ExplicitReleaseReportsLastReleaser) {
  rp::Scope scope;
  ASSERT_TRUE(scope.Acquire());
  EXPECT_TRUE(scope.Release()) << "single holder Release() must report 1->0 transition";
  EXPECT_FALSE(scope.held());
  EXPECT_FALSE(rp::IsPaused());
}

TEST_F(ReplicationPauseScopeTest, ReleaseSuppressesDestructorRelease) {
  // After explicit Release(), the dtor must not decrement again. Otherwise
  // a second pauser would see a phantom 0 underflow on its own release.
  {
    rp::Scope scope;
    ASSERT_TRUE(scope.Acquire());
    EXPECT_TRUE(scope.Release());
  }
  EXPECT_EQ(0, static_cast<int>(rp::IsPaused()));

  rp::Scope second;
  EXPECT_TRUE(second.Acquire()) << "counter must be back at 0 with no underflow saturation";
}

TEST_F(ReplicationPauseScopeTest, DestructorReleasesIfNotExplicit) {
  // The RAII safety net: if the caller forgets to Release(), the dtor must
  // drop the counter so the program does not deadlock on a leaked pause.
  {
    rp::Scope scope;
    ASSERT_TRUE(scope.Acquire());
    ASSERT_TRUE(rp::IsPaused());
  }
  EXPECT_FALSE(rp::IsPaused()) << "dtor must release a held but unreleased Scope";
}

TEST_F(ReplicationPauseScopeTest, DoubleAcquireIsNoOp) {
  // Acquiring twice on the same Scope is a programming bug; the helper
  // defends by no-oping the second call so the destructor only releases
  // once. Without this guard, double-acquire would leak +1 in the counter.
  rp::Scope scope;
  ASSERT_TRUE(scope.Acquire());
  EXPECT_FALSE(scope.Acquire()) << "second Acquire() must be a no-op returning false";

  ASSERT_TRUE(scope.Release());
  EXPECT_FALSE(rp::IsPaused()) << "counter must be 0 after one Acquire/Release pair";
}

TEST_F(ReplicationPauseScopeTest, DoubleReleaseIsNoOp) {
  // Symmetric to double-Acquire: a second Release() must not underflow.
  rp::Scope scope;
  ASSERT_TRUE(scope.Acquire());
  EXPECT_TRUE(scope.Release());
  EXPECT_FALSE(scope.Release()) << "second Release() must be a no-op returning false";
}

TEST_F(ReplicationPauseScopeTest, ReleaseWithoutAcquireIsNoOp) {
  rp::Scope scope;
  EXPECT_FALSE(scope.Release()) << "Release() without Acquire() must not touch the counter";
  EXPECT_FALSE(rp::IsPaused());
}

TEST_F(ReplicationPauseScopeTest, MoveConstructionTransfersOwnership) {
  // After move, the source must not release in its dtor (would double-
  // decrement), and the sink must release on its dtor (the only remaining
  // owner of the increment).
  rp::Scope source;
  ASSERT_TRUE(source.Acquire());
  ASSERT_TRUE(rp::IsPaused());

  {
    rp::Scope sink(std::move(source));
    EXPECT_TRUE(sink.held());
    // Source must report no held state so its dtor does nothing.
    EXPECT_FALSE(source.held());  // NOLINT(bugprone-use-after-move) — testing post-move state intentionally
  }

  EXPECT_FALSE(rp::IsPaused()) << "sink's dtor released; source's dtor must have been a no-op";
}

TEST_F(ReplicationPauseScopeTest, MultipleScopesNestRequestRelease) {
  // Smoke test for the production pattern: two operations request pause
  // concurrently, only the first one observes "first pauser", only the
  // last one observes "last releaser".
  rp::Scope first;
  rp::Scope second;
  EXPECT_TRUE(first.Acquire());
  EXPECT_FALSE(second.Acquire());

  EXPECT_FALSE(first.Release()) << "first to release while second still holds is NOT last_releaser";
  EXPECT_TRUE(second.Release()) << "second (and only remaining) holder IS last_releaser";
  EXPECT_FALSE(rp::IsPaused());
}
