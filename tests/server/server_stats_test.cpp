/**
 * @file server_stats_test.cpp
 * @brief Unit tests for ServerStats replication statistics
 */

#include "server/server_stats.h"

#include <gtest/gtest.h>

#include <thread>
#include <vector>

#include "query/query_parser.h"

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

/**
 * @brief Every QueryType increments GetTotalCommands exactly once.
 *
 * Regression test for the bug where IncrementCommand only handled SEARCH /
 * COUNT / GET / INFO / SAVE / LOAD / REPLICATION_* / CONFIG_* / UNKNOWN, and
 * silently no-op'd for DUMP_*, SYNC*, OPTIMIZE, DEBUG_*, CACHE_*, SET,
 * SHOW_VARIABLES, FACET. GetTotalCommands therefore undercounted.
 *
 * All values of QueryType must be covered by either a specific counter or the
 * cmd_other_ aggregate counter.
 */
TEST_F(ServerStatsTest, AllQueryTypesAreCounted) {
  using mygramdb::query::QueryType;
  // List every value of QueryType (mirrors src/query/query_parser.h).
  constexpr QueryType kAllTypes[] = {
      QueryType::SEARCH,
      QueryType::COUNT,
      QueryType::GET,
      QueryType::INFO,
      QueryType::DUMP_SAVE,
      QueryType::DUMP_LOAD,
      QueryType::DUMP_VERIFY,
      QueryType::DUMP_INFO,
      QueryType::DUMP_STATUS,
      QueryType::SAVE,
      QueryType::LOAD,
      QueryType::REPLICATION_STATUS,
      QueryType::REPLICATION_STOP,
      QueryType::REPLICATION_START,
      QueryType::SYNC,
      QueryType::SYNC_STATUS,
      QueryType::SYNC_STOP,
      QueryType::CONFIG_HELP,
      QueryType::CONFIG_SHOW,
      QueryType::CONFIG_VERIFY,
      QueryType::OPTIMIZE,
      QueryType::DEBUG_ON,
      QueryType::DEBUG_OFF,
      QueryType::CACHE_CLEAR,
      QueryType::CACHE_STATS,
      QueryType::CACHE_ENABLE,
      QueryType::CACHE_DISABLE,
      QueryType::SET,
      QueryType::SHOW_VARIABLES,
      QueryType::FACET,
      QueryType::UNKNOWN,
  };

  uint64_t expected_total = 0;
  for (QueryType type : kAllTypes) {
    uint64_t before = stats_->GetTotalCommands();
    stats_->IncrementCommand(type);
    uint64_t after = stats_->GetTotalCommands();
    EXPECT_EQ(after, before + 1) << "IncrementCommand did not increment GetTotalCommands "
                                    "for QueryType value "
                                 << static_cast<int>(type);
    ++expected_total;
  }

  EXPECT_EQ(stats_->GetTotalCommands(), expected_total);
  // Sanity check via the snapshot path too.
  Statistics snapshot = stats_->GetStatistics();
  EXPECT_EQ(snapshot.total_commands_processed, expected_total);
}

/**
 * @brief IncrementRequests bumps total_requests in the GetStatistics snapshot.
 */
TEST_F(ServerStatsTest, IncrementRequestsBumpsTotalRequests) {
  EXPECT_EQ(stats_->GetTotalRequests(), 0u);

  stats_->IncrementRequests();
  EXPECT_EQ(stats_->GetTotalRequests(), 1u);

  stats_->IncrementRequests();
  stats_->IncrementRequests();
  EXPECT_EQ(stats_->GetTotalRequests(), 3u);

  Statistics snapshot = stats_->GetStatistics();
  EXPECT_EQ(snapshot.total_requests, 3u);
}
