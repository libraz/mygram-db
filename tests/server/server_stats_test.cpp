/**
 * @file server_stats_test.cpp
 * @brief Unit tests for ServerStats replication statistics
 */

#include "server/server_stats.h"

#include <gtest/gtest.h>

#include <thread>
#include <vector>

using namespace mygramdb::server;

/**
 * @brief Test fixture for ServerStats tests
 */
class ServerStatsTest : public ::testing::Test {
 protected:
  void SetUp() override { stats_ = std::make_unique<ServerStats>(); }

  std::unique_ptr<ServerStats> stats_;
};

/**
 * @brief Test that replication statistics counters are initialized to zero
 */
TEST_F(ServerStatsTest, ReplicationStatsInitializedToZero) {
  EXPECT_EQ(stats_->GetReplInsertsApplied(), 0);
  EXPECT_EQ(stats_->GetReplInsertsSkipped(), 0);
  EXPECT_EQ(stats_->GetReplUpdatesApplied(), 0);
  EXPECT_EQ(stats_->GetReplUpdatesAdded(), 0);
  EXPECT_EQ(stats_->GetReplUpdatesRemoved(), 0);
  EXPECT_EQ(stats_->GetReplUpdatesModified(), 0);
  EXPECT_EQ(stats_->GetReplUpdatesSkipped(), 0);
  EXPECT_EQ(stats_->GetReplDeletesApplied(), 0);
  EXPECT_EQ(stats_->GetReplDeletesSkipped(), 0);
  EXPECT_EQ(stats_->GetReplDdlExecuted(), 0);
  EXPECT_EQ(stats_->GetReplEventsSkippedOtherTables(), 0);
}

/**
 * @brief Test that insert statistics counters increment correctly
 */
TEST_F(ServerStatsTest, IncrementInsertStats) {
  stats_->IncrementReplInsertApplied();
  EXPECT_EQ(stats_->GetReplInsertsApplied(), 1);

  stats_->IncrementReplInsertApplied();
  EXPECT_EQ(stats_->GetReplInsertsApplied(), 2);

  stats_->IncrementReplInsertSkipped();
  EXPECT_EQ(stats_->GetReplInsertsSkipped(), 1);

  stats_->IncrementReplInsertSkipped();
  stats_->IncrementReplInsertSkipped();
  EXPECT_EQ(stats_->GetReplInsertsSkipped(), 3);
}

/**
 * @brief Test that update statistics counters increment correctly
 */
TEST_F(ServerStatsTest, IncrementUpdateStats) {
  // Test updates_added (also increments updates_applied)
  stats_->IncrementReplUpdateAdded();
  EXPECT_EQ(stats_->GetReplUpdatesAdded(), 1);
  EXPECT_EQ(stats_->GetReplUpdatesApplied(), 1);

  stats_->IncrementReplUpdateAdded();
  EXPECT_EQ(stats_->GetReplUpdatesAdded(), 2);
  EXPECT_EQ(stats_->GetReplUpdatesApplied(), 2);

  // Test updates_removed (also increments updates_applied)
  stats_->IncrementReplUpdateRemoved();
  EXPECT_EQ(stats_->GetReplUpdatesRemoved(), 1);
  EXPECT_EQ(stats_->GetReplUpdatesApplied(), 3);

  // Test updates_modified (also increments updates_applied)
  stats_->IncrementReplUpdateModified();
  EXPECT_EQ(stats_->GetReplUpdatesModified(), 1);
  EXPECT_EQ(stats_->GetReplUpdatesApplied(), 4);

  // Test updates_skipped
  stats_->IncrementReplUpdateSkipped();
  EXPECT_EQ(stats_->GetReplUpdatesSkipped(), 1);
  EXPECT_EQ(stats_->GetReplUpdatesApplied(), 4);  // Should not increment applied
}

/**
 * @brief Test that delete statistics counters increment correctly
 */
TEST_F(ServerStatsTest, IncrementDeleteStats) {
  stats_->IncrementReplDeleteApplied();
  stats_->IncrementReplDeleteApplied();
  EXPECT_EQ(stats_->GetReplDeletesApplied(), 2);

  stats_->IncrementReplDeleteSkipped();
  EXPECT_EQ(stats_->GetReplDeletesSkipped(), 1);
}

/**
 * @brief Test that DDL and other table statistics counters increment correctly
 */
TEST_F(ServerStatsTest, IncrementDdlAndOtherTableStats) {
  stats_->IncrementReplDdlExecuted();
  stats_->IncrementReplDdlExecuted();
  stats_->IncrementReplDdlExecuted();
  EXPECT_EQ(stats_->GetReplDdlExecuted(), 3);

  stats_->IncrementReplEventsSkippedOtherTables();
  stats_->IncrementReplEventsSkippedOtherTables();
  EXPECT_EQ(stats_->GetReplEventsSkippedOtherTables(), 2);
}

/**
 * @brief Test that GetStatistics returns correct replication statistics
 */
TEST_F(ServerStatsTest, GetStatisticsReturnsReplicationStats) {
  stats_->IncrementReplInsertApplied();
  stats_->IncrementReplInsertSkipped();
  stats_->IncrementReplUpdateAdded();
  stats_->IncrementReplUpdateRemoved();
  stats_->IncrementReplUpdateModified();
  stats_->IncrementReplUpdateSkipped();
  stats_->IncrementReplDeleteApplied();
  stats_->IncrementReplDeleteSkipped();
  stats_->IncrementReplDdlExecuted();
  stats_->IncrementReplEventsSkippedOtherTables();

  Statistics snapshot = stats_->GetStatistics();

  EXPECT_EQ(snapshot.repl_inserts_applied, 1);
  EXPECT_EQ(snapshot.repl_inserts_skipped, 1);
  EXPECT_EQ(snapshot.repl_updates_applied, 3);  // Added + Removed + Modified
  EXPECT_EQ(snapshot.repl_updates_added, 1);
  EXPECT_EQ(snapshot.repl_updates_removed, 1);
  EXPECT_EQ(snapshot.repl_updates_modified, 1);
  EXPECT_EQ(snapshot.repl_updates_skipped, 1);
  EXPECT_EQ(snapshot.repl_deletes_applied, 1);
  EXPECT_EQ(snapshot.repl_deletes_skipped, 1);
  EXPECT_EQ(snapshot.repl_ddl_executed, 1);
  EXPECT_EQ(snapshot.repl_events_skipped_other_tables, 1);
}

/**
 * @brief Test that Reset clears replication statistics
 */
TEST_F(ServerStatsTest, ResetClearsReplicationStats) {
  stats_->IncrementReplInsertApplied();
  stats_->IncrementReplUpdateAdded();
  stats_->IncrementReplDeleteApplied();
  stats_->IncrementReplDdlExecuted();

  EXPECT_GT(stats_->GetReplInsertsApplied(), 0);
  EXPECT_GT(stats_->GetReplUpdatesApplied(), 0);
  EXPECT_GT(stats_->GetReplDeletesApplied(), 0);
  EXPECT_GT(stats_->GetReplDdlExecuted(), 0);

  stats_->Reset();

  EXPECT_EQ(stats_->GetReplInsertsApplied(), 0);
  EXPECT_EQ(stats_->GetReplInsertsSkipped(), 0);
  EXPECT_EQ(stats_->GetReplUpdatesApplied(), 0);
  EXPECT_EQ(stats_->GetReplUpdatesAdded(), 0);
  EXPECT_EQ(stats_->GetReplUpdatesRemoved(), 0);
  EXPECT_EQ(stats_->GetReplUpdatesModified(), 0);
  EXPECT_EQ(stats_->GetReplUpdatesSkipped(), 0);
  EXPECT_EQ(stats_->GetReplDeletesApplied(), 0);
  EXPECT_EQ(stats_->GetReplDeletesSkipped(), 0);
  EXPECT_EQ(stats_->GetReplDdlExecuted(), 0);
  EXPECT_EQ(stats_->GetReplEventsSkippedOtherTables(), 0);
}

/**
 * @brief Test that replication statistics are thread-safe
 */
TEST_F(ServerStatsTest, ReplicationStatsThreadSafe) {
  constexpr int kNumThreads = 10;
  constexpr int kIncrementsPerThread = 1000;

  std::vector<std::thread> threads;

  // Launch threads that increment different counters
  for (int i = 0; i < kNumThreads; ++i) {
    threads.emplace_back([this]() {
      for (int j = 0; j < kIncrementsPerThread; ++j) {
        stats_->IncrementReplInsertApplied();
        stats_->IncrementReplUpdateAdded();
        stats_->IncrementReplDeleteApplied();
      }
    });
  }

  // Wait for all threads
  for (auto& thread : threads) {
    thread.join();
  }

  // Verify counts
  EXPECT_EQ(stats_->GetReplInsertsApplied(), kNumThreads * kIncrementsPerThread);
  EXPECT_EQ(stats_->GetReplUpdatesApplied(), kNumThreads * kIncrementsPerThread);
  EXPECT_EQ(stats_->GetReplUpdatesAdded(), kNumThreads * kIncrementsPerThread);
  EXPECT_EQ(stats_->GetReplDeletesApplied(), kNumThreads * kIncrementsPerThread);
}

/**
 * @brief Test combined statistics scenario
 */
TEST_F(ServerStatsTest, CombinedStatisticsScenario) {
  // Simulate a replication scenario:
  // - 100 inserts applied, 20 skipped
  // - 50 updates: 10 added, 5 removed, 30 modified, 5 skipped
  // - 30 deletes applied, 10 skipped
  // - 2 DDL operations
  // - 15 events from other tables

  for (int i = 0; i < 100; ++i)
    stats_->IncrementReplInsertApplied();
  for (int i = 0; i < 20; ++i)
    stats_->IncrementReplInsertSkipped();

  for (int i = 0; i < 10; ++i)
    stats_->IncrementReplUpdateAdded();
  for (int i = 0; i < 5; ++i)
    stats_->IncrementReplUpdateRemoved();
  for (int i = 0; i < 30; ++i)
    stats_->IncrementReplUpdateModified();
  for (int i = 0; i < 5; ++i)
    stats_->IncrementReplUpdateSkipped();

  for (int i = 0; i < 30; ++i)
    stats_->IncrementReplDeleteApplied();
  for (int i = 0; i < 10; ++i)
    stats_->IncrementReplDeleteSkipped();

  for (int i = 0; i < 2; ++i)
    stats_->IncrementReplDdlExecuted();

  for (int i = 0; i < 15; ++i)
    stats_->IncrementReplEventsSkippedOtherTables();

  // Verify all counts
  EXPECT_EQ(stats_->GetReplInsertsApplied(), 100);
  EXPECT_EQ(stats_->GetReplInsertsSkipped(), 20);
  EXPECT_EQ(stats_->GetReplUpdatesApplied(), 45);  // 10 + 5 + 30
  EXPECT_EQ(stats_->GetReplUpdatesAdded(), 10);
  EXPECT_EQ(stats_->GetReplUpdatesRemoved(), 5);
  EXPECT_EQ(stats_->GetReplUpdatesModified(), 30);
  EXPECT_EQ(stats_->GetReplUpdatesSkipped(), 5);
  EXPECT_EQ(stats_->GetReplDeletesApplied(), 30);
  EXPECT_EQ(stats_->GetReplDeletesSkipped(), 10);
  EXPECT_EQ(stats_->GetReplDdlExecuted(), 2);
  EXPECT_EQ(stats_->GetReplEventsSkippedOtherTables(), 15);

  // Verify total applied events: inserts_applied + updates_applied + deletes_applied + ddl
  uint64_t total_applied = stats_->GetReplInsertsApplied() + stats_->GetReplUpdatesApplied() +
                           stats_->GetReplDeletesApplied() + stats_->GetReplDdlExecuted();
  EXPECT_EQ(total_applied, 177);  // 100 + 45 + 30 + 2

  // Verify total skipped events
  uint64_t total_skipped = stats_->GetReplInsertsSkipped() + stats_->GetReplUpdatesSkipped() +
                           stats_->GetReplDeletesSkipped() + stats_->GetReplEventsSkippedOtherTables();
  EXPECT_EQ(total_skipped, 50);  // 20 + 5 + 10 + 15
}
