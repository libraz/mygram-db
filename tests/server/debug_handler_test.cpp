/**
 * @file debug_handler_test.cpp
 * @brief Unit tests for DebugHandler (DEBUG and OPTIMIZE commands)
 */

#include "server/handlers/debug_handler.h"

#include <gtest/gtest.h>
#include <spdlog/spdlog.h>

#include "index/index.h"
#include "query/query_parser.h"
#include "server/response_formatter.h"
#include "storage/document_store.h"

namespace mygramdb::server {

class DebugHandlerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    spdlog::set_level(spdlog::level::debug);

    // Create test table context
    table_ctx_ = std::make_unique<TableContext>();
    table_ctx_->name = "test_table";
    table_ctx_->config.ngram_size = 2;
    table_ctx_->index = std::make_unique<index::Index>(2);
    table_ctx_->doc_store = std::make_unique<storage::DocumentStore>();

    // Setup table contexts map
    table_contexts_["test_table"] = table_ctx_.get();

    // Create handler context
    config_ = std::make_unique<config::Config>();
    config::TableConfig table_config;
    table_config.name = "test_table";
    table_config.ngram_size = 2;
    config_->tables.push_back(table_config);

    stats_ = std::make_unique<ServerStats>();

    handler_ctx_ = std::make_unique<HandlerContext>(HandlerContext{
        .table_contexts = table_contexts_,
        .stats = *stats_,
        .full_config = config_.get(),
        .dump_dir = "/tmp",
        .loading = loading_,
        .read_only = read_only_,
        .optimization_in_progress = optimization_in_progress_,
        .replication_paused_for_dump = replication_paused_for_dump_,
        .mysql_reconnecting = mysql_reconnecting_,
#ifdef USE_MYSQL
        .binlog_reader = nullptr,
        .sync_manager = nullptr,
#else
        .binlog_reader = nullptr,
#endif
    });

    // Create handler
    handler_ = std::make_unique<DebugHandler>(*handler_ctx_);

    // Add test data
    AddTestData();
  }

  void TearDown() override {
    // Reset flags
    loading_ = false;
    read_only_ = false;
    optimization_in_progress_ = false;
  }

  void AddTestData() {
    // Add documents to index and doc_store
    auto doc_id1 = table_ctx_->doc_store->AddDocument("1", {{"content", "hello world"}});
    table_ctx_->index->AddDocument(static_cast<index::DocId>(*doc_id1), "hello world");

    auto doc_id2 = table_ctx_->doc_store->AddDocument("2", {{"content", "test document"}});
    table_ctx_->index->AddDocument(static_cast<index::DocId>(*doc_id2), "test document");

    auto doc_id3 = table_ctx_->doc_store->AddDocument("3", {{"content", "another test"}});
    table_ctx_->index->AddDocument(static_cast<index::DocId>(*doc_id3), "another test");
  }

  std::unique_ptr<TableContext> table_ctx_;
  std::unordered_map<std::string, TableContext*> table_contexts_;
  std::unique_ptr<config::Config> config_;
  std::unique_ptr<ServerStats> stats_;
  std::atomic<bool> loading_{false};
  std::atomic<bool> read_only_{false};
  std::atomic<bool> optimization_in_progress_{false};
  std::atomic<bool> replication_paused_for_dump_{false};
  std::atomic<bool> mysql_reconnecting_{false};
  std::unique_ptr<HandlerContext> handler_ctx_;
  std::unique_ptr<DebugHandler> handler_;
  ConnectionContext conn_ctx_;
};

// ============================================================================
// DEBUG Tests
// ============================================================================

TEST_F(DebugHandlerTest, DebugOnBasic) {
  query::Query query;
  query.type = query::QueryType::DEBUG_ON;

  EXPECT_FALSE(conn_ctx_.debug_mode);

  std::string response = handler_->Handle(query, conn_ctx_);

  EXPECT_EQ("OK DEBUG_ON", response);
  EXPECT_TRUE(conn_ctx_.debug_mode);
}

TEST_F(DebugHandlerTest, DebugOffBasic) {
  conn_ctx_.debug_mode = true;

  query::Query query;
  query.type = query::QueryType::DEBUG_OFF;

  std::string response = handler_->Handle(query, conn_ctx_);

  EXPECT_EQ("OK DEBUG_OFF", response);
  EXPECT_FALSE(conn_ctx_.debug_mode);
}

// ============================================================================
// OPTIMIZE Blocking Tests
// ============================================================================

TEST_F(DebugHandlerTest, OptimizeBlockedDuringDumpLoad) {
  // Simulate DUMP LOAD in progress
  loading_ = true;

  query::Query query;
  query.type = query::QueryType::OPTIMIZE;
  query.table = "test_table";

  std::string response = handler_->Handle(query, conn_ctx_);

  EXPECT_TRUE(response.find("ERROR") == 0 || response.find("Cannot optimize while DUMP LOAD is in progress") == 0)
      << "Response: " << response;
  EXPECT_TRUE(response.find("DUMP LOAD") != std::string::npos) << "Response: " << response;

  // Verify optimization_in_progress was not set
  EXPECT_FALSE(optimization_in_progress_);
}

TEST_F(DebugHandlerTest, OptimizeAllowedDuringDumpSave) {
  // Simulate DUMP SAVE in progress (read_only flag set)
  read_only_ = true;

  query::Query query;
  query.type = query::QueryType::OPTIMIZE;
  query.table = "test_table";

  std::string response = handler_->Handle(query, conn_ctx_);

  // OPTIMIZE should be allowed during DUMP SAVE (for auto-save support)
  // Should not contain blocking messages
  EXPECT_TRUE(response.find("Cannot optimize while DUMP SAVE") == std::string::npos) << "Response: " << response;

  // After completion, optimization_in_progress should be reset
  EXPECT_FALSE(optimization_in_progress_);
}

TEST_F(DebugHandlerTest, OptimizeBlockedWhenAlreadyRunning) {
  // Simulate another OPTIMIZE already running
  optimization_in_progress_ = true;

  query::Query query;
  query.type = query::QueryType::OPTIMIZE;
  query.table = "test_table";

  std::string response = handler_->Handle(query, conn_ctx_);

  EXPECT_TRUE(response.find("ERROR") == 0 || response.find("Another OPTIMIZE operation is already in progress") == 0)
      << "Response: " << response;

  // Verify flag remained true
  EXPECT_TRUE(optimization_in_progress_);
}

TEST_F(DebugHandlerTest, OptimizeAllowedWhenNoBlockingOperations) {
  // Ensure all flags are false
  EXPECT_FALSE(loading_);
  EXPECT_FALSE(read_only_);
  EXPECT_FALSE(optimization_in_progress_);

  query::Query query;
  query.type = query::QueryType::OPTIMIZE;
  query.table = "test_table";

  std::string response = handler_->Handle(query, conn_ctx_);

  // Should succeed (might return OK or error depending on optimization result)
  // At minimum, should not contain the blocking messages
  EXPECT_TRUE(response.find("DUMP LOAD") == std::string::npos) << "Response: " << response;
  EXPECT_TRUE(response.find("DUMP SAVE") == std::string::npos) << "Response: " << response;

  // After completion, optimization_in_progress should be reset
  EXPECT_FALSE(optimization_in_progress_);
}

TEST_F(DebugHandlerTest, OptimizeFlagResetAfterCompletion) {
  // Verify optimization_in_progress is reset even if optimization completes
  EXPECT_FALSE(optimization_in_progress_);

  query::Query query;
  query.type = query::QueryType::OPTIMIZE;
  query.table = "test_table";

  std::string response = handler_->Handle(query, conn_ctx_);

  // After handle completes, flag should be reset
  EXPECT_FALSE(optimization_in_progress_) << "optimization_in_progress should be reset after completion";
}

TEST_F(DebugHandlerTest, OptimizeInvalidTable) {
  query::Query query;
  query.type = query::QueryType::OPTIMIZE;
  query.table = "nonexistent_table";

  std::string response = handler_->Handle(query, conn_ctx_);

  EXPECT_TRUE(response.find("ERROR") == 0) << "Response: " << response;
  // optimization_in_progress should still be reset even on error
  EXPECT_FALSE(optimization_in_progress_);
}

}  // namespace mygramdb::server
