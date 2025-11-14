/**
 * @file sync_handler_test.cpp
 * @brief Unit tests for SYNC command handler
 */

#ifdef USE_MYSQL

#include "server/handlers/sync_handler.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <spdlog/spdlog.h>

#include "query/query_parser.h"
#include "server/tcp_server.h"

namespace mygramdb::server {

/**
 * @brief Test fixture for SyncHandler
 */
class SyncHandlerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    spdlog::set_level(spdlog::level::debug);

    // Create test table context
    table_ctx_ = std::make_unique<TableContext>();
    table_ctx_->name = "test_table";
    table_ctx_->config.name = "test_table";
    table_ctx_->config.ngram_size = 2;
    table_ctx_->index = std::make_unique<index::Index>(2);
    table_ctx_->doc_store = std::make_unique<storage::DocumentStore>();

    // Setup table contexts map
    table_contexts_["test_table"] = table_ctx_.get();

    // Create full config
    config_ = std::make_unique<config::Config>();
    config::TableConfig table_config;
    table_config.name = "test_table";
    table_config.ngram_size = 2;
    config_->tables.push_back(table_config);

    stats_ = std::make_unique<ServerStats>();
  }

  void TearDown() override {
    // Cleanup
  }

  std::unique_ptr<TableContext> table_ctx_;
  std::unordered_map<std::string, TableContext*> table_contexts_;
  std::unique_ptr<config::Config> config_;
  std::unique_ptr<ServerStats> stats_;
  std::atomic<bool> loading_{false};
  std::atomic<bool> read_only_{false};
  std::atomic<bool> optimization_in_progress_{false};
  std::unordered_set<std::string> syncing_tables_;
  std::mutex syncing_tables_mutex_;
};

// ============================================================================
// Query Parser Tests
// ============================================================================

TEST_F(SyncHandlerTest, ParseSyncCommand) {
  query::QueryParser parser;

  // Test SYNC <table>
  auto query = parser.Parse("SYNC test_table");
  EXPECT_EQ(query::QueryType::SYNC, query.type);
  EXPECT_EQ("test_table", query.table);
  EXPECT_TRUE(query.IsValid());
}

TEST_F(SyncHandlerTest, ParseSyncStatusCommand) {
  query::QueryParser parser;

  // Test SYNC STATUS
  auto query = parser.Parse("SYNC STATUS");
  EXPECT_EQ(query::QueryType::SYNC_STATUS, query.type);
  EXPECT_EQ("", query.table);
  EXPECT_TRUE(query.IsValid());
}

TEST_F(SyncHandlerTest, ParseSyncInvalidCommand) {
  query::QueryParser parser;

  // Test SYNC without arguments (should fail)
  auto query = parser.Parse("SYNC");
  EXPECT_EQ(query::QueryType::UNKNOWN, query.type);
  EXPECT_FALSE(query.IsValid());
  EXPECT_FALSE(parser.GetError().empty());
}

// ============================================================================
// Conflict Detection Tests
// ============================================================================

TEST_F(SyncHandlerTest, ConflictDetectionDuringSync) {
  // Simulate SYNC in progress
  {
    std::lock_guard<std::mutex> lock(syncing_tables_mutex_);
    syncing_tables_.insert("test_table");
  }

  // Verify syncing table is tracked
  {
    std::lock_guard<std::mutex> lock(syncing_tables_mutex_);
    EXPECT_EQ(1, syncing_tables_.size());
    EXPECT_TRUE(syncing_tables_.find("test_table") != syncing_tables_.end());
  }

  // Cleanup
  {
    std::lock_guard<std::mutex> lock(syncing_tables_mutex_);
    syncing_tables_.clear();
  }
}

TEST_F(SyncHandlerTest, MultipleSyncOperations) {
  // Simulate multiple SYNC operations on different tables
  {
    std::lock_guard<std::mutex> lock(syncing_tables_mutex_);
    syncing_tables_.insert("table1");
    syncing_tables_.insert("table2");
    syncing_tables_.insert("table3");
  }

  // Verify all tables are tracked
  {
    std::lock_guard<std::mutex> lock(syncing_tables_mutex_);
    EXPECT_EQ(3, syncing_tables_.size());
    EXPECT_TRUE(syncing_tables_.find("table1") != syncing_tables_.end());
    EXPECT_TRUE(syncing_tables_.find("table2") != syncing_tables_.end());
    EXPECT_TRUE(syncing_tables_.find("table3") != syncing_tables_.end());
  }

  // Cleanup
  {
    std::lock_guard<std::mutex> lock(syncing_tables_mutex_);
    syncing_tables_.clear();
  }
}

// ============================================================================
// SyncState Tests
// ============================================================================

TEST_F(SyncHandlerTest, SyncStateInitialization) {
  SyncState state;

  // Check initial state
  EXPECT_FALSE(state.is_running);
  EXPECT_EQ("", state.table_name);
  EXPECT_EQ(0, state.total_rows);
  EXPECT_EQ(0, state.processed_rows.load());
  EXPECT_EQ("", state.status);
  EXPECT_EQ("", state.error_message);
  EXPECT_EQ("", state.gtid);
  EXPECT_EQ("", state.replication_status);
}

TEST_F(SyncHandlerTest, SyncStateProgress) {
  SyncState state;

  state.is_running = true;
  state.table_name = "test_table";
  state.status = "IN_PROGRESS";
  state.total_rows = 10000;
  state.processed_rows = 5000;

  EXPECT_TRUE(state.is_running);
  EXPECT_EQ("test_table", state.table_name);
  EXPECT_EQ("IN_PROGRESS", state.status);
  EXPECT_EQ(10000, state.total_rows);
  EXPECT_EQ(5000, state.processed_rows.load());

  // Simulate progress
  state.processed_rows = 7500;
  EXPECT_EQ(7500, state.processed_rows.load());

  // Complete
  state.processed_rows = 10000;
  state.status = "COMPLETED";
  state.is_running = false;
  state.gtid = "uuid:123";
  state.replication_status = "STARTED";

  EXPECT_FALSE(state.is_running);
  EXPECT_EQ("COMPLETED", state.status);
  EXPECT_EQ(10000, state.processed_rows.load());
  EXPECT_EQ("uuid:123", state.gtid);
  EXPECT_EQ("STARTED", state.replication_status);
}

TEST_F(SyncHandlerTest, SyncStateFailure) {
  SyncState state;

  state.is_running = true;
  state.table_name = "test_table";
  state.status = "IN_PROGRESS";
  state.processed_rows = 100;

  // Simulate failure
  state.status = "FAILED";
  state.error_message = "MySQL connection lost";
  state.is_running = false;

  EXPECT_FALSE(state.is_running);
  EXPECT_EQ("FAILED", state.status);
  EXPECT_EQ("MySQL connection lost", state.error_message);
  EXPECT_EQ(100, state.processed_rows.load());
}

}  // namespace mygramdb::server

#endif  // USE_MYSQL
