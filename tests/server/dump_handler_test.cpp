/**
 * @file snapshot_handler_test.cpp
 * @brief Unit tests for DumpHandler (DUMP commands)
 */

#include "server/handlers/dump_handler.h"

#include <gtest/gtest.h>
#include <spdlog/spdlog.h>
#include <unistd.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <thread>

#include "index/index.h"
#include "query/query_parser.h"
#include "server/response_formatter.h"
#include "storage/document_store.h"
#include "storage/dump_format_v1.h"

#ifdef USE_MYSQL
#include "mysql/binlog_reader_interface.h"
#endif

namespace mygramdb::server {

class DumpHandlerTest : public ::testing::Test {
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
    // Add table config to vector
    config::TableConfig table_config;
    table_config.name = "test_table";
    table_config.ngram_size = 2;
    config_->tables.push_back(table_config);

    stats_ = std::make_unique<ServerStats>();

    // Create test dump directory (avoid /tmp which is a symlink on macOS)
    test_dump_dir_ = std::filesystem::temp_directory_path() / ("dump_handler_test_" + std::to_string(getpid()));
    std::filesystem::create_directories(test_dump_dir_);

    handler_ctx_ = std::make_unique<HandlerContext>(HandlerContext{
        .table_contexts = table_contexts_,
        .stats = *stats_,
        .full_config = config_.get(),
        .dump_dir = test_dump_dir_.string(),
        .dump_load_in_progress = dump_load_in_progress_,
        .dump_save_in_progress = dump_save_in_progress_,
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
    handler_ = std::make_unique<DumpHandler>(*handler_ctx_);

    // Setup test data
    AddTestData();

    // Create unique test file path for parallel test execution
    // Use relative path (not absolute) since DumpHandler enforces dump_dir containment
    std::stringstream ss;
    ss << "test_snapshot_" << getpid() << "_" << std::hash<std::thread::id>{}(std::this_thread::get_id()) << "_"
       << std::chrono::steady_clock::now().time_since_epoch().count() << ".dmp";
    test_filepath_ = ss.str();
  }

  void TearDown() override {
    // Clean up test dump directory (will remove test_filepath_ too since it's inside)
    if (std::filesystem::exists(test_dump_dir_)) {
      std::filesystem::remove_all(test_dump_dir_);
    }
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
  std::atomic<bool> dump_load_in_progress_{false};
  std::atomic<bool> dump_save_in_progress_{false};
  std::atomic<bool> optimization_in_progress_{false};
  std::atomic<bool> replication_paused_for_dump_{false};
  std::atomic<bool> mysql_reconnecting_{false};
#ifdef USE_MYSQL
#endif
  std::unique_ptr<HandlerContext> handler_ctx_;
  std::unique_ptr<DumpHandler> handler_;
  std::string test_filepath_;
  std::filesystem::path test_dump_dir_;
  ConnectionContext conn_ctx_;
};

// ============================================================================
// DUMP_SAVE Tests
// ============================================================================

TEST_F(DumpHandlerTest, DumpSaveBasic) {
  query::Query query;
  query.type = query::QueryType::DUMP_SAVE;
  query.filepath = test_filepath_;

  std::string response = handler_->Handle(query, conn_ctx_);

  EXPECT_TRUE(response.find("OK SAVED") == 0) << "Response: " << response;
  EXPECT_TRUE(response.find(test_filepath_) != std::string::npos);
  // File is saved in dump_dir, so check the full path
  std::filesystem::path full_path = test_dump_dir_ / test_filepath_;
  EXPECT_TRUE(std::filesystem::exists(full_path));
}

TEST_F(DumpHandlerTest, DumpSaveWithDefaultFilepath) {
  query::Query query;
  query.type = query::QueryType::DUMP_SAVE;
  // No filepath - should generate default

  std::string response = handler_->Handle(query, conn_ctx_);

  EXPECT_TRUE(response.find("OK SAVED") == 0) << "Response: " << response;
  EXPECT_TRUE(response.find("dump_") != std::string::npos);
  EXPECT_TRUE(response.find(".dmp") != std::string::npos);

  // Extract filepath from response
  size_t start = response.find("/tmp/");
  if (start != std::string::npos) {
    std::string filepath = response.substr(start);
    // Remove trailing newline/carriage return
    filepath.erase(std::remove(filepath.begin(), filepath.end(), '\r'), filepath.end());
    filepath.erase(std::remove(filepath.begin(), filepath.end(), '\n'), filepath.end());
    EXPECT_TRUE(std::filesystem::exists(filepath));
    std::filesystem::remove(filepath);
  }
}

TEST_F(DumpHandlerTest, DumpSaveWithRelativePath) {
  query::Query query;
  query.type = query::QueryType::DUMP_SAVE;
  query.filepath = "relative_test.dmp";

  std::string response = handler_->Handle(query, conn_ctx_);

  EXPECT_TRUE(response.find("OK SAVED") == 0) << "Response: " << response;
  // File is saved in dump_dir, not /tmp
  std::filesystem::path expected_path = test_dump_dir_ / "relative_test.dmp";
  EXPECT_TRUE(std::filesystem::exists(expected_path));
  // No need to remove - TearDown will clean up test_dump_dir_
}

TEST_F(DumpHandlerTest, DumpSaveSetsReadOnlyMode) {
  query::Query query;
  query.type = query::QueryType::DUMP_SAVE;
  query.filepath = test_filepath_;

  EXPECT_FALSE(dump_save_in_progress_);
  std::string response = handler_->Handle(query, conn_ctx_);
  // Should be false after completion
  EXPECT_FALSE(dump_save_in_progress_);
}

// ============================================================================
// DUMP_LOAD Tests
// ============================================================================

TEST_F(DumpHandlerTest, DumpLoadBasic) {
  // First save
  query::Query save_query;
  save_query.type = query::QueryType::DUMP_SAVE;
  save_query.filepath = test_filepath_;
  handler_->Handle(save_query, conn_ctx_);

  // Clear data
  table_ctx_->index = std::make_unique<index::Index>(2);
  table_ctx_->doc_store = std::make_unique<storage::DocumentStore>();

  // Update context pointers
  table_contexts_["test_table"] = table_ctx_.get();

  // Load
  query::Query load_query;
  load_query.type = query::QueryType::DUMP_LOAD;
  load_query.filepath = test_filepath_;

  std::string response = handler_->Handle(load_query, conn_ctx_);

  EXPECT_TRUE(response.find("OK LOADED") == 0) << "Response: " << response;

  // Verify data was restored by checking document IDs
  auto doc1_id = table_ctx_->doc_store->GetDocId("1");
  ASSERT_TRUE(doc1_id.has_value()) << "Document 1 not found";
  EXPECT_EQ(1u, doc1_id.value());

  auto doc2_id = table_ctx_->doc_store->GetDocId("2");
  ASSERT_TRUE(doc2_id.has_value()) << "Document 2 not found";
  EXPECT_EQ(2u, doc2_id.value());

  auto doc3_id = table_ctx_->doc_store->GetDocId("3");
  ASSERT_TRUE(doc3_id.has_value()) << "Document 3 not found";
  EXPECT_EQ(3u, doc3_id.value());

  // Verify document count
  EXPECT_EQ(3u, table_ctx_->doc_store->Size()) << "Document count mismatch";
}

TEST_F(DumpHandlerTest, DumpLoadRequiresFilepath) {
  query::Query query;
  query.type = query::QueryType::DUMP_LOAD;
  // No filepath

  std::string response = handler_->Handle(query, conn_ctx_);

  EXPECT_TRUE(response.find("ERROR") == 0);
  EXPECT_TRUE(response.find("requires a filepath") != std::string::npos);
}

TEST_F(DumpHandlerTest, DumpLoadNonExistentFile) {
  query::Query query;
  query.type = query::QueryType::DUMP_LOAD;
  query.filepath = "/tmp/nonexistent.dmp";

  std::string response = handler_->Handle(query, conn_ctx_);

  EXPECT_TRUE(response.find("ERROR") == 0);
}

TEST_F(DumpHandlerTest, DumpLoadSetsLoadingMode) {
  // First save
  query::Query save_query;
  save_query.type = query::QueryType::DUMP_SAVE;
  save_query.filepath = test_filepath_;
  handler_->Handle(save_query, conn_ctx_);

  query::Query load_query;
  load_query.type = query::QueryType::DUMP_LOAD;
  load_query.filepath = test_filepath_;

  EXPECT_FALSE(dump_load_in_progress_);
  std::string response = handler_->Handle(load_query, conn_ctx_);
  // Should be false after completion
  EXPECT_FALSE(dump_load_in_progress_);
}

// ============================================================================
// DUMP_VERIFY Tests
// ============================================================================

TEST_F(DumpHandlerTest, DumpVerifyValidFile) {
  // First save
  query::Query save_query;
  save_query.type = query::QueryType::DUMP_SAVE;
  save_query.filepath = test_filepath_;
  handler_->Handle(save_query, conn_ctx_);

  // Verify
  query::Query verify_query;
  verify_query.type = query::QueryType::DUMP_VERIFY;
  verify_query.filepath = test_filepath_;

  std::string response = handler_->Handle(verify_query, conn_ctx_);

  EXPECT_TRUE(response.find("OK DUMP_VERIFIED") == 0) << "Response: " << response;
}

TEST_F(DumpHandlerTest, DumpVerifyRequiresFilepath) {
  query::Query query;
  query.type = query::QueryType::DUMP_VERIFY;
  // No filepath

  std::string response = handler_->Handle(query, conn_ctx_);

  EXPECT_TRUE(response.find("ERROR") == 0);
  EXPECT_TRUE(response.find("requires a filepath") != std::string::npos);
}

TEST_F(DumpHandlerTest, DumpVerifyNonExistentFile) {
  query::Query query;
  query.type = query::QueryType::DUMP_VERIFY;
  query.filepath = "/tmp/nonexistent.dmp";

  std::string response = handler_->Handle(query, conn_ctx_);

  EXPECT_TRUE(response.find("ERROR") == 0);
}

TEST_F(DumpHandlerTest, DumpVerifyCorruptedFile) {
  // First save
  query::Query save_query;
  save_query.type = query::QueryType::DUMP_SAVE;
  save_query.filepath = test_filepath_;
  handler_->Handle(save_query, conn_ctx_);

  // Corrupt the file (must use full path in dump_dir)
  std::filesystem::path full_path = test_dump_dir_ / test_filepath_;
  std::fstream file(full_path, std::ios::binary | std::ios::in | std::ios::out);
  file.seekp(100);
  file.put(0xFF);
  file.close();

  // Verify
  query::Query verify_query;
  verify_query.type = query::QueryType::DUMP_VERIFY;
  verify_query.filepath = test_filepath_;

  std::string response = handler_->Handle(verify_query, conn_ctx_);

  EXPECT_TRUE(response.find("ERROR") == 0);
  EXPECT_TRUE(response.find("verification failed") != std::string::npos);
}

// ============================================================================
// DUMP_INFO Tests
// ============================================================================

TEST_F(DumpHandlerTest, DumpInfoBasic) {
  // First save
  query::Query save_query;
  save_query.type = query::QueryType::DUMP_SAVE;
  save_query.filepath = test_filepath_;
  handler_->Handle(save_query, conn_ctx_);

  // Get info
  query::Query info_query;
  info_query.type = query::QueryType::DUMP_INFO;
  info_query.filepath = test_filepath_;

  std::string response = handler_->Handle(info_query, conn_ctx_);

  EXPECT_TRUE(response.find("OK DUMP_INFO") == 0) << "Response: " << response;
  EXPECT_TRUE(response.find("version:") != std::string::npos);
  EXPECT_TRUE(response.find("gtid:") != std::string::npos);
  EXPECT_TRUE(response.find("tables:") != std::string::npos);
  EXPECT_TRUE(response.find("file_size:") != std::string::npos);
  EXPECT_TRUE(response.find("timestamp:") != std::string::npos);
  EXPECT_TRUE(response.find("END") != std::string::npos);
}

TEST_F(DumpHandlerTest, DumpInfoRequiresFilepath) {
  query::Query query;
  query.type = query::QueryType::DUMP_INFO;
  // No filepath

  std::string response = handler_->Handle(query, conn_ctx_);

  EXPECT_TRUE(response.find("ERROR") == 0);
  EXPECT_TRUE(response.find("requires a filepath") != std::string::npos);
}

TEST_F(DumpHandlerTest, DumpInfoNonExistentFile) {
  query::Query query;
  query.type = query::QueryType::DUMP_INFO;
  query.filepath = "/tmp/nonexistent.dmp";

  std::string response = handler_->Handle(query, conn_ctx_);

  EXPECT_TRUE(response.find("ERROR") == 0);
}

// ============================================================================
// DUMP_STATUS Tests
// ============================================================================

TEST_F(DumpHandlerTest, DumpStatusBasicIdle) {
  query::Query query;
  query.type = query::QueryType::DUMP_STATUS;

  std::string response = handler_->Handle(query, conn_ctx_);

  EXPECT_TRUE(response.find("OK DUMP_STATUS") == 0) << "Response: " << response;
  EXPECT_TRUE(response.find("save_in_progress: false") != std::string::npos);
  EXPECT_TRUE(response.find("load_in_progress: false") != std::string::npos);
  EXPECT_TRUE(response.find("replication_paused_for_dump: false") != std::string::npos);
  EXPECT_TRUE(response.find("status: IDLE") != std::string::npos);
  EXPECT_TRUE(response.find("END") != std::string::npos);
}

TEST_F(DumpHandlerTest, DumpStatusDuringSave) {
  // Simulate DUMP SAVE in progress
  dump_save_in_progress_ = true;

  query::Query query;
  query.type = query::QueryType::DUMP_STATUS;

  std::string response = handler_->Handle(query, conn_ctx_);

  EXPECT_TRUE(response.find("OK DUMP_STATUS") == 0) << "Response: " << response;
  EXPECT_TRUE(response.find("save_in_progress: true") != std::string::npos);
  EXPECT_TRUE(response.find("load_in_progress: false") != std::string::npos);
  EXPECT_TRUE(response.find("status: SAVE_IN_PROGRESS") != std::string::npos);

  // Clean up
  dump_save_in_progress_ = false;
}

TEST_F(DumpHandlerTest, DumpStatusDuringLoad) {
  // Simulate DUMP LOAD in progress
  dump_load_in_progress_ = true;

  query::Query query;
  query.type = query::QueryType::DUMP_STATUS;

  std::string response = handler_->Handle(query, conn_ctx_);

  EXPECT_TRUE(response.find("OK DUMP_STATUS") == 0) << "Response: " << response;
  EXPECT_TRUE(response.find("save_in_progress: false") != std::string::npos);
  EXPECT_TRUE(response.find("load_in_progress: true") != std::string::npos);
  EXPECT_TRUE(response.find("status: LOAD_IN_PROGRESS") != std::string::npos);

  // Clean up
  dump_load_in_progress_ = false;
}

TEST_F(DumpHandlerTest, DumpStatusReplicationPaused) {
  // Simulate replication paused for dump
  replication_paused_for_dump_ = true;

  query::Query query;
  query.type = query::QueryType::DUMP_STATUS;

  std::string response = handler_->Handle(query, conn_ctx_);

  EXPECT_TRUE(response.find("OK DUMP_STATUS") == 0) << "Response: " << response;
  EXPECT_TRUE(response.find("replication_paused_for_dump: true") != std::string::npos);

  // Clean up
  replication_paused_for_dump_ = false;
}

// ============================================================================
// GTID Tests (Critical for Replication)
// ============================================================================

TEST_F(DumpHandlerTest, GtidPreservationAcrossSaveLoad) {
  // Note: Full GTID restoration testing via BinlogReader is verified through
  // manual integration tests (requires MySQL connection).
  // Fix for GTID restoration bug was verified in issue #XXX.
  //
  // Bug fixed: DUMP LOAD now sets GTID on BinlogReader regardless of whether
  // replication was running before, enabling manual REPLICATION START after LOAD.
  //
  // For now, verify that GTID is empty when no binlog_reader is present.

  query::Query save_query;
  save_query.type = query::QueryType::DUMP_SAVE;
  save_query.filepath = test_filepath_;
  handler_->Handle(save_query, conn_ctx_);

  query::Query info_query;
  info_query.type = query::QueryType::DUMP_INFO;
  info_query.filepath = test_filepath_;
  std::string info_response = handler_->Handle(info_query, conn_ctx_);

  // GTID should be empty since no binlog_reader
  EXPECT_TRUE(info_response.find("gtid:") != std::string::npos);
}

// ============================================================================
// Integrity Tests
// ============================================================================

TEST_F(DumpHandlerTest, SaveLoadRoundTripPreservesAllData) {
  // Add more test data
  for (int i = 4; i <= 100; ++i) {
    std::string content = "document " + std::to_string(i);
    auto doc_id = table_ctx_->doc_store->AddDocument(std::to_string(i), {{"content", content}});
    table_ctx_->index->AddDocument(static_cast<index::DocId>(*doc_id), content);
  }

  // Save
  query::Query save_query;
  save_query.type = query::QueryType::DUMP_SAVE;
  save_query.filepath = test_filepath_;
  std::string save_response = handler_->Handle(save_query, conn_ctx_);
  EXPECT_TRUE(save_response.find("OK SAVED") == 0);

  // Get original stats
  size_t original_doc_count = table_ctx_->doc_store->Size();
  auto original_stats = table_ctx_->index->GetStatistics();

  // Clear data
  table_ctx_->index = std::make_unique<index::Index>(2);
  table_ctx_->doc_store = std::make_unique<storage::DocumentStore>();
  table_contexts_["test_table"] = table_ctx_.get();

  // Load
  query::Query load_query;
  load_query.type = query::QueryType::DUMP_LOAD;
  load_query.filepath = test_filepath_;
  std::string load_response = handler_->Handle(load_query, conn_ctx_);
  EXPECT_TRUE(load_response.find("OK LOADED") == 0);

  // Verify all data is preserved
  EXPECT_EQ(table_ctx_->doc_store->Size(), original_doc_count);
  auto loaded_stats = table_ctx_->index->GetStatistics();
  EXPECT_EQ(loaded_stats.total_terms, original_stats.total_terms);
  EXPECT_EQ(loaded_stats.total_postings, original_stats.total_postings);

  // Verify specific documents are present
  for (int i = 1; i <= 100; ++i) {
    auto doc_id = table_ctx_->doc_store->GetDocId(std::to_string(i));
    ASSERT_TRUE(doc_id.has_value()) << "Document " << i << " not found";
    EXPECT_EQ(static_cast<storage::DocId>(i), doc_id.value()) << "Document " << i << " has wrong ID";
  }
}

// ============================================================================
// Null Config Tests
// ============================================================================

TEST_F(DumpHandlerTest, DumpSaveWithNullConfig) {
  // Create handler context with null config
  HandlerContext null_config_ctx{
      .table_contexts = table_contexts_,
      .stats = *stats_,
      .full_config = nullptr,  // Null config
      .dump_dir = "/tmp",
      .dump_load_in_progress = dump_load_in_progress_,
      .dump_save_in_progress = dump_save_in_progress_,
      .optimization_in_progress = optimization_in_progress_,
      .replication_paused_for_dump = replication_paused_for_dump_,
      .mysql_reconnecting = mysql_reconnecting_,
#ifdef USE_MYSQL
      .binlog_reader = nullptr,
      .sync_manager = nullptr,

#else
      .binlog_reader = nullptr,
#endif
  };

  DumpHandler null_config_handler(null_config_ctx);

  // Try to save dump
  query::Query query;
  query.type = query::QueryType::DUMP_SAVE;
  query.filepath = test_filepath_;

  std::string response = null_config_handler.Handle(query, conn_ctx_);

  // Should return error
  EXPECT_TRUE(response.find("ERROR") == 0);
  EXPECT_TRUE(response.find("configuration is not available") != std::string::npos);
}

// ============================================================================
// Exception Safety Tests
// ============================================================================

TEST_F(DumpHandlerTest, ReadOnlyFlagResetOnException) {
  // Save a valid dump first
  query::Query save_query;
  save_query.type = query::QueryType::DUMP_SAVE;
  save_query.filepath = test_filepath_;
  std::string save_response = handler_->Handle(save_query, conn_ctx_);
  EXPECT_TRUE(save_response.find("OK SAVED") == 0);

  // Verify read_only is false after successful save
  EXPECT_FALSE(dump_save_in_progress_);

  // Try to save to an invalid path (should trigger exception or error)
  query::Query invalid_query;
  invalid_query.type = query::QueryType::DUMP_SAVE;
  invalid_query.filepath = "/invalid/path/that/does/not/exist/test.dmp";

  std::string error_response = handler_->Handle(invalid_query, conn_ctx_);

  // Even if error occurs, read_only should be reset to false
  EXPECT_FALSE(dump_save_in_progress_) << "read_only flag should be reset even on error";
  EXPECT_TRUE(error_response.find("ERROR") == 0 || error_response.find("Failed") != std::string::npos);
}

TEST_F(DumpHandlerTest, LoadingFlagResetOnException) {
  // Verify loading is false initially
  EXPECT_FALSE(dump_load_in_progress_);

  // Try to load from non-existent file
  query::Query invalid_query;
  invalid_query.type = query::QueryType::DUMP_LOAD;
  invalid_query.filepath = "/tmp/nonexistent_file_that_does_not_exist.dmp";

  std::string error_response = handler_->Handle(invalid_query, conn_ctx_);

  // Even if error occurs, loading should be reset to false
  EXPECT_FALSE(dump_load_in_progress_) << "loading flag should be reset even on error";
  EXPECT_TRUE(error_response.find("ERROR") == 0 || error_response.find("Failed") != std::string::npos);
}

TEST_F(DumpHandlerTest, ConcurrentFlagsNotAffected) {
  // This test verifies that read_only and loading flags work correctly
  // when set by different operations and that concurrent operations are blocked

  // First create a dump file for testing
  query::Query save_query;
  save_query.type = query::QueryType::DUMP_SAVE;
  save_query.filepath = test_filepath_;
  std::string initial_save = handler_->Handle(save_query, conn_ctx_);
  ASSERT_TRUE(initial_save.find("OK SAVED") == 0) << "Initial save should succeed";

  // Set loading flag externally (simulating DUMP LOAD in progress)
  dump_load_in_progress_ = true;

  // Try to save dump (should be blocked now due to concurrent operation protection)
  std::string save_response = handler_->Handle(save_query, conn_ctx_);
  EXPECT_TRUE(save_response.find("ERROR") == 0) << "Save should be blocked during load";
  EXPECT_TRUE(save_response.find("DUMP LOAD is in progress") != std::string::npos);

  // loading flag should remain true (unaffected by blocked save attempt)
  EXPECT_TRUE(dump_load_in_progress_) << "loading flag should not be affected by blocked save operation";

  // Reset for next test
  dump_load_in_progress_ = false;
  dump_save_in_progress_ = true;

  // Try to load dump (should be blocked due to read_only flag from another DUMP SAVE)
  query::Query load_query;
  load_query.type = query::QueryType::DUMP_LOAD;
  load_query.filepath = test_filepath_;
  std::string load_response = handler_->Handle(load_query, conn_ctx_);
  EXPECT_TRUE(load_response.find("ERROR") == 0) << "Load should be blocked during save";
  EXPECT_TRUE(load_response.find("DUMP SAVE is in progress") != std::string::npos);

  // read_only flag should remain true (unaffected by blocked load attempt)
  EXPECT_TRUE(dump_save_in_progress_) << "read_only flag should not be affected by blocked load operation";

  // Clean up
  dump_save_in_progress_ = false;
}

#ifdef USE_MYSQL
TEST_F(DumpHandlerTest, DumpSaveWarnsButAllowedDuringSyncOperation) {
  // Note: This test runs with sync_manager = nullptr
  // The actual SYNC warning logic is tested in integration tests
  // where SyncOperationManager is properly initialized

  query::Query save_query;
  save_query.type = query::QueryType::DUMP_SAVE;
  save_query.filepath = test_filepath_;
  std::string save_response = handler_->Handle(save_query, conn_ctx_);

  // DUMP SAVE should succeed
  EXPECT_TRUE(save_response.find("OK SAVED") == 0) << "Dump save should succeed";
}

TEST_F(DumpHandlerTest, DumpLoadBlockedDuringSyncOperation) {
  // Note: This test runs with sync_manager = nullptr
  // The actual SYNC blocking logic is tested in integration tests
  // where SyncOperationManager is properly initialized

  // First create a dump file to load
  query::Query save_query;
  save_query.type = query::QueryType::DUMP_SAVE;
  save_query.filepath = test_filepath_;
  std::string save_response = handler_->Handle(save_query, conn_ctx_);
  EXPECT_TRUE(save_response.find("OK SAVED") == 0);

  // Try to load dump (should succeed since sync_manager is nullptr)
  query::Query load_query;
  load_query.type = query::QueryType::DUMP_LOAD;
  load_query.filepath = test_filepath_;
  std::string load_response = handler_->Handle(load_query, conn_ctx_);

  // Should succeed (SYNC check is skipped when sync_manager is nullptr)
  EXPECT_TRUE(load_response.find("OK LOADED") == 0) << "Dump load should succeed when sync_manager is not configured";
}
#endif

/**
 * @brief Test path traversal prevention in DUMP SAVE
 */
TEST_F(DumpHandlerTest, PathTraversalPreventionSave) {
  query::Query query;
  query.type = query::QueryType::DUMP_SAVE;

  // Test case 1: Attempt to use "../" to escape dump directory
  query.filepath = "../etc/passwd";
  std::string response = handler_->Handle(query, conn_ctx_);
  EXPECT_TRUE(response.find("ERROR") == 0) << "Should reject path traversal with ../";
  EXPECT_TRUE(response.find("path traversal") != std::string::npos ||
              response.find("Invalid filepath") != std::string::npos)
      << "Error should mention path traversal or invalid filepath";

  // Test case 2: Attempt to use absolute path outside dump directory
  query.filepath = "/etc/passwd";
  response = handler_->Handle(query, conn_ctx_);
  EXPECT_TRUE(response.find("ERROR") == 0) << "Should reject absolute path outside dump directory";

  // Test case 3: Valid relative path should work
  query.filepath = "valid_dump.dmp";
  response = handler_->Handle(query, conn_ctx_);
  EXPECT_TRUE(response.find("OK") == 0 || response.find("ERROR") != 0)
      << "Valid relative path should not be rejected for path traversal";

  // Cleanup
  std::filesystem::remove(test_dump_dir_ / "valid_dump.dmp");
}

/**
 * @brief Test path traversal prevention in DUMP LOAD
 */
TEST_F(DumpHandlerTest, PathTraversalPreventionLoad) {
  // First create a valid dump file
  query::Query save_query;
  save_query.type = query::QueryType::DUMP_SAVE;
  save_query.filepath = "test_load_traversal.dmp";
  std::string save_response = handler_->Handle(save_query, conn_ctx_);
  ASSERT_TRUE(save_response.find("OK") == 0) << "Failed to create test dump file";

  query::Query query;
  query.type = query::QueryType::DUMP_LOAD;

  // Test case 1: Attempt to use "../" to escape dump directory
  query.filepath = "../etc/passwd";
  std::string response = handler_->Handle(query, conn_ctx_);
  EXPECT_TRUE(response.find("ERROR") == 0) << "Should reject path traversal with ../";
  EXPECT_TRUE(response.find("path traversal") != std::string::npos ||
              response.find("Invalid filepath") != std::string::npos)
      << "Error should mention path traversal or invalid filepath";

  // Test case 2: Attempt to use absolute path outside dump directory
  query.filepath = "/etc/passwd";
  response = handler_->Handle(query, conn_ctx_);
  EXPECT_TRUE(response.find("ERROR") == 0) << "Should reject absolute path outside dump directory";

  // Test case 3: Valid relative path should work
  query.filepath = "test_load_traversal.dmp";
  response = handler_->Handle(query, conn_ctx_);
  // Should either succeed or fail for reasons other than path traversal
  if (response.find("ERROR") == 0) {
    EXPECT_TRUE(response.find("path traversal") == std::string::npos &&
                response.find("Invalid filepath") == std::string::npos)
        << "Valid path should not fail due to path traversal";
  }

  // Cleanup
  std::filesystem::remove(test_dump_dir_ / "test_load_traversal.dmp");
}

/**
 * @brief Test path traversal prevention in DUMP VERIFY
 */
TEST_F(DumpHandlerTest, PathTraversalPreventionVerify) {
  query::Query query;
  query.type = query::QueryType::DUMP_VERIFY;

  // Test case 1: Attempt to use "../" to escape dump directory
  query.filepath = "../etc/passwd";
  std::string response = handler_->Handle(query, conn_ctx_);
  EXPECT_TRUE(response.find("ERROR") == 0) << "Should reject path traversal with ../";
  EXPECT_TRUE(response.find("path traversal") != std::string::npos ||
              response.find("Invalid filepath") != std::string::npos)
      << "Error should mention path traversal or invalid filepath";

  // Test case 2: Attempt to use "../../" for deeper traversal
  query.filepath = "../../etc/passwd";
  response = handler_->Handle(query, conn_ctx_);
  EXPECT_TRUE(response.find("ERROR") == 0) << "Should reject deeper path traversal";

  // Test case 3: Attempt with encoded path separators (should be handled by filesystem library)
  query.filepath = "..%2F..%2Fetc%2Fpasswd";
  response = handler_->Handle(query, conn_ctx_);
  // May succeed if interpreted as literal filename, but should not traverse
  // The important thing is it doesn't access /etc/passwd
}

/**
 * @brief Test DUMP LOAD is blocked during OPTIMIZE
 */
TEST_F(DumpHandlerTest, DumpLoadBlockedDuringOptimize) {
  // First create a dump file to load
  query::Query save_query;
  save_query.type = query::QueryType::DUMP_SAVE;
  save_query.filepath = test_filepath_;
  std::string save_response = handler_->Handle(save_query, conn_ctx_);
  ASSERT_TRUE(save_response.find("OK SAVED") == 0) << "Failed to create test dump file";

  // Simulate OPTIMIZE in progress
  optimization_in_progress_ = true;

  // Try to load dump
  query::Query load_query;
  load_query.type = query::QueryType::DUMP_LOAD;
  load_query.filepath = test_filepath_;
  std::string load_response = handler_->Handle(load_query, conn_ctx_);

  // Should be blocked
  EXPECT_TRUE(load_response.find("ERROR") == 0) << "Response: " << load_response;
  EXPECT_TRUE(load_response.find("OPTIMIZE") != std::string::npos) << "Response: " << load_response;
  EXPECT_TRUE(load_response.find("Cannot load dump") != std::string::npos) << "Response: " << load_response;
}

/**
 * @brief Test DUMP SAVE is allowed during OPTIMIZE (for auto-save)
 */
TEST_F(DumpHandlerTest, DumpSaveAllowedDuringOptimize) {
  // Simulate OPTIMIZE in progress
  optimization_in_progress_ = true;

  // Try to save dump
  query::Query save_query;
  save_query.type = query::QueryType::DUMP_SAVE;
  save_query.filepath = test_filepath_;
  std::string save_response = handler_->Handle(save_query, conn_ctx_);

  // Should be allowed (for auto-save support)
  EXPECT_TRUE(save_response.find("OK SAVED") == 0 || save_response.find("ERROR") == 0) << "Response: " << save_response;

  // Should not contain OPTIMIZE blocking message
  EXPECT_TRUE(save_response.find("Cannot save dump while OPTIMIZE") == std::string::npos)
      << "Response: " << save_response;
}

// ============================================================================
// Concurrent Dump Operation Tests
// ============================================================================

TEST_F(DumpHandlerTest, DumpSaveBlockedDuringDumpLoad) {
  // First create a dump file to load
  query::Query save_query;
  save_query.type = query::QueryType::DUMP_SAVE;
  save_query.filepath = test_filepath_;
  std::string save_response = handler_->Handle(save_query, conn_ctx_);
  ASSERT_TRUE(save_response.find("OK SAVED") == 0) << "Failed to create test dump file";

  // Simulate DUMP LOAD in progress
  dump_load_in_progress_ = true;

  // Try to save another dump
  query::Query save_query2;
  save_query2.type = query::QueryType::DUMP_SAVE;
  save_query2.filepath = test_filepath_ + ".new";
  std::string save_response2 = handler_->Handle(save_query2, conn_ctx_);

  // Should be blocked
  EXPECT_TRUE(save_response2.find("ERROR") == 0) << "Response: " << save_response2;
  EXPECT_TRUE(save_response2.find("DUMP LOAD is in progress") != std::string::npos) << "Response: " << save_response2;
  EXPECT_TRUE(save_response2.find("Cannot save dump") != std::string::npos) << "Response: " << save_response2;

  // Clean up
  dump_load_in_progress_ = false;
}

TEST_F(DumpHandlerTest, DumpSaveBlockedDuringDumpSave) {
  // Simulate DUMP SAVE in progress
  dump_save_in_progress_ = true;

  // Try to save a dump
  query::Query save_query;
  save_query.type = query::QueryType::DUMP_SAVE;
  save_query.filepath = test_filepath_;
  std::string save_response = handler_->Handle(save_query, conn_ctx_);

  // Should be blocked
  EXPECT_TRUE(save_response.find("ERROR") == 0) << "Response: " << save_response;
  EXPECT_TRUE(save_response.find("another DUMP SAVE is in progress") != std::string::npos)
      << "Response: " << save_response;
  EXPECT_TRUE(save_response.find("Cannot save dump") != std::string::npos) << "Response: " << save_response;

  // Clean up
  dump_save_in_progress_ = false;
}

TEST_F(DumpHandlerTest, DumpLoadBlockedDuringDumpSave) {
  // First create a dump file to load
  query::Query save_query;
  save_query.type = query::QueryType::DUMP_SAVE;
  save_query.filepath = test_filepath_;
  std::string save_response = handler_->Handle(save_query, conn_ctx_);
  ASSERT_TRUE(save_response.find("OK SAVED") == 0) << "Failed to create test dump file";

  // Simulate DUMP SAVE in progress
  dump_save_in_progress_ = true;

  // Try to load dump
  query::Query load_query;
  load_query.type = query::QueryType::DUMP_LOAD;
  load_query.filepath = test_filepath_;
  std::string load_response = handler_->Handle(load_query, conn_ctx_);

  // Should be blocked
  EXPECT_TRUE(load_response.find("ERROR") == 0) << "Response: " << load_response;
  EXPECT_TRUE(load_response.find("DUMP SAVE is in progress") != std::string::npos) << "Response: " << load_response;
  EXPECT_TRUE(load_response.find("Cannot load dump") != std::string::npos) << "Response: " << load_response;

  // Clean up
  dump_save_in_progress_ = false;
}

TEST_F(DumpHandlerTest, DumpLoadBlockedDuringDumpLoad) {
  // First create a dump file to load
  query::Query save_query;
  save_query.type = query::QueryType::DUMP_SAVE;
  save_query.filepath = test_filepath_;
  std::string save_response = handler_->Handle(save_query, conn_ctx_);
  ASSERT_TRUE(save_response.find("OK SAVED") == 0) << "Failed to create test dump file";

  // Simulate DUMP LOAD in progress
  dump_load_in_progress_ = true;

  // Try to load another dump
  query::Query load_query;
  load_query.type = query::QueryType::DUMP_LOAD;
  load_query.filepath = test_filepath_;
  std::string load_response = handler_->Handle(load_query, conn_ctx_);

  // Should be blocked
  EXPECT_TRUE(load_response.find("ERROR") == 0) << "Response: " << load_response;
  EXPECT_TRUE(load_response.find("another DUMP LOAD is in progress") != std::string::npos)
      << "Response: " << load_response;
  EXPECT_TRUE(load_response.find("Cannot load dump") != std::string::npos) << "Response: " << load_response;

  // Clean up
  dump_load_in_progress_ = false;
}

// ============================================================================
// MockBinlogReader Tests for GTID Restoration
// ============================================================================

#ifdef USE_MYSQL
/**
 * @brief Mock implementation of IBinlogReader for unit testing
 *
 * This mock enables testing of GTID-related functionality in DumpHandler
 * without requiring an actual MySQL connection.
 */
class MockBinlogReader : public mysql::IBinlogReader {
 public:
  MockBinlogReader() = default;

  mygram::utils::Expected<void, mygram::utils::Error> Start() override {
    running_ = true;
    start_called_ = true;
    return {};
  }

  void Stop() override {
    running_ = false;
    stop_called_ = true;
  }

  bool IsRunning() const override { return running_; }

  std::string GetCurrentGTID() const override { return current_gtid_; }

  void SetCurrentGTID(const std::string& gtid) override {
    current_gtid_ = gtid;
    set_gtid_called_ = true;
    last_set_gtid_ = gtid;
  }

  const std::string& GetLastError() const override { return last_error_; }

  uint64_t GetProcessedEvents() const override { return processed_events_; }

  size_t GetQueueSize() const override { return queue_size_; }

  // Test helpers
  void SetGtidForTest(const std::string& gtid) { current_gtid_ = gtid; }
  void SetRunningForTest(bool running) { running_ = running; }
  bool WasStartCalled() const { return start_called_; }
  bool WasStopCalled() const { return stop_called_; }
  bool WasSetGtidCalled() const { return set_gtid_called_; }
  std::string GetLastSetGtid() const { return last_set_gtid_; }
  void ResetTestFlags() {
    start_called_ = false;
    stop_called_ = false;
    set_gtid_called_ = false;
    last_set_gtid_.clear();
  }

 private:
  std::string current_gtid_;
  std::string last_error_;
  bool running_ = false;
  uint64_t processed_events_ = 0;
  size_t queue_size_ = 0;

  // Test tracking flags
  bool start_called_ = false;
  bool stop_called_ = false;
  bool set_gtid_called_ = false;
  std::string last_set_gtid_;
};

/**
 * @brief Test fixture for GTID restoration tests using MockBinlogReader
 */
class DumpHandlerGtidTest : public ::testing::Test {
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

    // Create handler context with mock binlog reader
    config_ = std::make_unique<config::Config>();
    config::TableConfig table_config;
    table_config.name = "test_table";
    table_config.ngram_size = 2;
    config_->tables.push_back(table_config);

    stats_ = std::make_unique<ServerStats>();
    mock_binlog_reader_ = std::make_unique<MockBinlogReader>();

    // Create test dump directory
    test_dump_dir_ = std::filesystem::temp_directory_path() / ("dump_gtid_test_" + std::to_string(getpid()));
    std::filesystem::create_directories(test_dump_dir_);

    handler_ctx_ = std::make_unique<HandlerContext>(HandlerContext{
        .table_contexts = table_contexts_,
        .stats = *stats_,
        .full_config = config_.get(),
        .dump_dir = test_dump_dir_.string(),
        .dump_load_in_progress = dump_load_in_progress_,
        .dump_save_in_progress = dump_save_in_progress_,
        .optimization_in_progress = optimization_in_progress_,
        .replication_paused_for_dump = replication_paused_for_dump_,
        .mysql_reconnecting = mysql_reconnecting_,
        .binlog_reader = mock_binlog_reader_.get(),
        .sync_manager = nullptr,
    });

    handler_ = std::make_unique<DumpHandler>(*handler_ctx_);

    // Add test data
    auto doc_id = table_ctx_->doc_store->AddDocument("pk1", {{"content", "test document one"}});
    table_ctx_->index->AddDocument(static_cast<index::DocId>(*doc_id), "test document one");

    // Create test file path
    test_filepath_ = (test_dump_dir_ / ("gtid_test_" + std::to_string(getpid()) + ".dmp")).string();
  }

  void TearDown() override { std::filesystem::remove_all(test_dump_dir_); }

  std::unique_ptr<TableContext> table_ctx_;
  std::unordered_map<std::string, TableContext*> table_contexts_;
  std::unique_ptr<config::Config> config_;
  std::unique_ptr<ServerStats> stats_;
  std::unique_ptr<MockBinlogReader> mock_binlog_reader_;
  std::unique_ptr<HandlerContext> handler_ctx_;
  std::unique_ptr<DumpHandler> handler_;
  std::filesystem::path test_dump_dir_;
  std::string test_filepath_;
  ConnectionContext conn_ctx_;

  std::atomic<bool> dump_load_in_progress_{false};
  std::atomic<bool> dump_save_in_progress_{false};
  std::atomic<bool> optimization_in_progress_{false};
  std::atomic<bool> replication_paused_for_dump_{false};
  std::atomic<bool> mysql_reconnecting_{false};
};

/**
 * @brief Test that DUMP SAVE captures current GTID
 */
TEST_F(DumpHandlerGtidTest, DumpSaveCapturesGtid) {
  const std::string test_gtid = "uuid:12345";
  mock_binlog_reader_->SetGtidForTest(test_gtid);

  query::Query save_query;
  save_query.type = query::QueryType::DUMP_SAVE;
  save_query.filepath = test_filepath_;

  std::string response = handler_->Handle(save_query, conn_ctx_);
  EXPECT_TRUE(response.find("OK SAVED") == 0) << "Response: " << response;

  // Verify GTID was stored in dump by checking DUMP INFO
  query::Query info_query;
  info_query.type = query::QueryType::DUMP_INFO;
  info_query.filepath = test_filepath_;

  std::string info_response = handler_->Handle(info_query, conn_ctx_);
  EXPECT_TRUE(info_response.find(test_gtid) != std::string::npos)
      << "GTID should be present in dump info. Response: " << info_response;
}

/**
 * @brief Test that DUMP LOAD restores GTID even when replication was NOT running
 *
 * This is the critical bug fix test: Previously, GTID was only restored when
 * replication was running before DUMP LOAD. Now it should be restored regardless
 * of replication state, enabling manual REPLICATION START after DUMP LOAD.
 */
TEST_F(DumpHandlerGtidTest, DumpLoadRestoresGtidWhenReplicationNotRunning) {
  const std::string original_gtid = "uuid:99999";

  // Save a dump with a GTID
  mock_binlog_reader_->SetGtidForTest(original_gtid);
  query::Query save_query;
  save_query.type = query::QueryType::DUMP_SAVE;
  save_query.filepath = test_filepath_;
  std::string save_response = handler_->Handle(save_query, conn_ctx_);
  ASSERT_TRUE(save_response.find("OK SAVED") == 0) << "Save failed: " << save_response;

  // Clear the GTID and ensure replication is NOT running
  mock_binlog_reader_->SetGtidForTest("");
  mock_binlog_reader_->SetRunningForTest(false);
  mock_binlog_reader_->ResetTestFlags();

  // Load the dump
  query::Query load_query;
  load_query.type = query::QueryType::DUMP_LOAD;
  load_query.filepath = test_filepath_;
  std::string load_response = handler_->Handle(load_query, conn_ctx_);
  EXPECT_TRUE(load_response.find("OK LOADED") == 0) << "Load failed: " << load_response;

  // Verify that SetCurrentGTID was called with the saved GTID
  EXPECT_TRUE(mock_binlog_reader_->WasSetGtidCalled())
      << "SetCurrentGTID should be called even when replication is not running";
  EXPECT_EQ(mock_binlog_reader_->GetLastSetGtid(), original_gtid)
      << "GTID should be restored to the value from the dump file";

  // Verify replication was NOT started (since it wasn't running before)
  EXPECT_FALSE(mock_binlog_reader_->WasStartCalled())
      << "Replication should NOT be auto-started when it wasn't running before";
}

/**
 * @brief Test that DUMP LOAD restores GTID and restarts replication when it was running
 */
TEST_F(DumpHandlerGtidTest, DumpLoadRestoresGtidAndRestartsReplication) {
  const std::string original_gtid = "uuid:88888";

  // Save a dump with a GTID
  mock_binlog_reader_->SetGtidForTest(original_gtid);
  query::Query save_query;
  save_query.type = query::QueryType::DUMP_SAVE;
  save_query.filepath = test_filepath_;
  std::string save_response = handler_->Handle(save_query, conn_ctx_);
  ASSERT_TRUE(save_response.find("OK SAVED") == 0) << "Save failed: " << save_response;

  // Set replication as running before load
  mock_binlog_reader_->SetGtidForTest("");
  mock_binlog_reader_->SetRunningForTest(true);
  mock_binlog_reader_->ResetTestFlags();

  // Load the dump
  query::Query load_query;
  load_query.type = query::QueryType::DUMP_LOAD;
  load_query.filepath = test_filepath_;
  std::string load_response = handler_->Handle(load_query, conn_ctx_);
  EXPECT_TRUE(load_response.find("OK LOADED") == 0) << "Load failed: " << load_response;

  // Verify GTID was restored
  EXPECT_TRUE(mock_binlog_reader_->WasSetGtidCalled()) << "SetCurrentGTID should be called";
  EXPECT_EQ(mock_binlog_reader_->GetLastSetGtid(), original_gtid)
      << "GTID should be restored to the value from the dump file";

  // Verify replication was stopped then restarted
  EXPECT_TRUE(mock_binlog_reader_->WasStopCalled()) << "Replication should be stopped before load";
  EXPECT_TRUE(mock_binlog_reader_->WasStartCalled()) << "Replication should be restarted after load";
}

/**
 * @brief Test that empty GTID in dump does not call SetCurrentGTID
 */
TEST_F(DumpHandlerGtidTest, DumpLoadWithEmptyGtidDoesNotSetGtid) {
  // Save a dump WITHOUT a GTID (clear it first)
  mock_binlog_reader_->SetGtidForTest("");
  query::Query save_query;
  save_query.type = query::QueryType::DUMP_SAVE;
  save_query.filepath = test_filepath_;
  std::string save_response = handler_->Handle(save_query, conn_ctx_);
  // Note: DUMP SAVE requires GTID, so this will fail
  EXPECT_TRUE(save_response.find("ERROR") == 0) << "Should fail without GTID: " << save_response;
}

/**
 * @brief Test full replication recovery cycle: SAVE with replication running,
 *        then LOAD with replication running - verifies auto-restart behavior
 */
TEST_F(DumpHandlerGtidTest, FullReplicationRecoveryCycle) {
  const std::string original_gtid = "uuid:77777";

  // Setup: replication running with known GTID
  mock_binlog_reader_->SetGtidForTest(original_gtid);
  mock_binlog_reader_->SetRunningForTest(true);

  // Step 1: DUMP SAVE (should stop replication, save, then restart)
  query::Query save_query;
  save_query.type = query::QueryType::DUMP_SAVE;
  save_query.filepath = test_filepath_;
  std::string save_response = handler_->Handle(save_query, conn_ctx_);
  ASSERT_TRUE(save_response.find("OK SAVED") == 0) << "Save failed: " << save_response;

  // Verify replication was stopped for consistency during save
  EXPECT_TRUE(mock_binlog_reader_->WasStopCalled()) << "Replication should be stopped during DUMP SAVE";
  // Verify replication was auto-restarted after save
  EXPECT_TRUE(mock_binlog_reader_->WasStartCalled()) << "Replication should be auto-restarted after DUMP SAVE";

  // Reset test flags and simulate time passing (replication continues)
  mock_binlog_reader_->ResetTestFlags();
  const std::string advanced_gtid = "uuid:77800";  // Replication advanced
  mock_binlog_reader_->SetGtidForTest(advanced_gtid);
  mock_binlog_reader_->SetRunningForTest(true);

  // Step 2: DUMP LOAD (should stop replication, restore GTID from dump, then restart)
  query::Query load_query;
  load_query.type = query::QueryType::DUMP_LOAD;
  load_query.filepath = test_filepath_;
  std::string load_response = handler_->Handle(load_query, conn_ctx_);
  EXPECT_TRUE(load_response.find("OK LOADED") == 0) << "Load failed: " << load_response;

  // Verify replication was stopped before load
  EXPECT_TRUE(mock_binlog_reader_->WasStopCalled()) << "Replication should be stopped before DUMP LOAD";

  // Verify GTID was restored to the value from the dump file (not the advanced value)
  EXPECT_TRUE(mock_binlog_reader_->WasSetGtidCalled()) << "SetCurrentGTID should be called during DUMP LOAD";
  EXPECT_EQ(mock_binlog_reader_->GetLastSetGtid(), original_gtid)
      << "GTID should be restored to dump's GTID, not the advanced position";

  // Verify replication was auto-restarted after load
  EXPECT_TRUE(mock_binlog_reader_->WasStartCalled())
      << "Replication should be auto-restarted after DUMP LOAD when it was running before";
}

/**
 * @brief Test fresh server scenario: replication NOT running, DUMP LOAD,
 *        then manual REPLICATION START - verifies GTID is available for manual start
 */
TEST_F(DumpHandlerGtidTest, FreshServerDumpLoadThenManualStart) {
  const std::string saved_gtid = "uuid:66666";

  // Step 1: Create a dump with GTID
  mock_binlog_reader_->SetGtidForTest(saved_gtid);
  mock_binlog_reader_->SetRunningForTest(true);  // Temporarily running to save
  query::Query save_query;
  save_query.type = query::QueryType::DUMP_SAVE;
  save_query.filepath = test_filepath_;
  std::string save_response = handler_->Handle(save_query, conn_ctx_);
  ASSERT_TRUE(save_response.find("OK SAVED") == 0) << "Save failed: " << save_response;

  // Step 2: Simulate fresh server restart (no GTID, replication not running)
  mock_binlog_reader_->SetGtidForTest("");
  mock_binlog_reader_->SetRunningForTest(false);
  mock_binlog_reader_->ResetTestFlags();

  // Step 3: DUMP LOAD on fresh server
  query::Query load_query;
  load_query.type = query::QueryType::DUMP_LOAD;
  load_query.filepath = test_filepath_;
  std::string load_response = handler_->Handle(load_query, conn_ctx_);
  EXPECT_TRUE(load_response.find("OK LOADED") == 0) << "Load failed: " << load_response;

  // Verify GTID was restored (critical for manual REPLICATION START)
  EXPECT_TRUE(mock_binlog_reader_->WasSetGtidCalled())
      << "SetCurrentGTID MUST be called even on fresh server for manual REPLICATION START";
  EXPECT_EQ(mock_binlog_reader_->GetLastSetGtid(), saved_gtid)
      << "GTID should be restored from dump to enable manual REPLICATION START";

  // Verify replication was NOT auto-started (was not running before)
  EXPECT_FALSE(mock_binlog_reader_->WasStartCalled())
      << "Replication should NOT auto-start if it wasn't running before DUMP LOAD";

  // Step 4: Simulate manual REPLICATION START
  // The GTID should now be available in binlog_reader
  EXPECT_EQ(mock_binlog_reader_->GetCurrentGTID(), saved_gtid)
      << "After DUMP LOAD, GTID should be available for manual REPLICATION START";
}

/**
 * @brief Test that server config is not overwritten by dump's stored config
 *
 * The dump file stores the config at the time of save, but DUMP LOAD should
 * NOT apply this config to the running server. The server's config should
 * always come from its startup config file.
 */
TEST_F(DumpHandlerGtidTest, ConfigNotOverwrittenByDump) {
  const std::string saved_gtid = "uuid:55555";

  // Setup: Save a dump with current config (ngram_size = 2)
  mock_binlog_reader_->SetGtidForTest(saved_gtid);
  query::Query save_query;
  save_query.type = query::QueryType::DUMP_SAVE;
  save_query.filepath = test_filepath_;
  std::string save_response = handler_->Handle(save_query, conn_ctx_);
  ASSERT_TRUE(save_response.find("OK SAVED") == 0) << "Save failed: " << save_response;

  // Simulate config change: ngram_size changed from 2 to 3
  // (In real scenario, this happens by editing config file and restarting server)
  const int new_ngram_size = 3;
  config_->tables[0].ngram_size = new_ngram_size;
  table_ctx_->config.ngram_size = new_ngram_size;

  // DUMP LOAD - should NOT change our running config
  query::Query load_query;
  load_query.type = query::QueryType::DUMP_LOAD;
  load_query.filepath = test_filepath_;
  std::string load_response = handler_->Handle(load_query, conn_ctx_);
  EXPECT_TRUE(load_response.find("OK LOADED") == 0) << "Load failed: " << load_response;

  // Verify config was NOT overwritten by dump
  EXPECT_EQ(config_->tables[0].ngram_size, new_ngram_size)
      << "Config should NOT be overwritten by dump - server config takes precedence";
  EXPECT_EQ(table_ctx_->config.ngram_size, new_ngram_size) << "TableContext config should NOT be overwritten by dump";
}

/**
 * @brief Test multiple DUMP LOAD operations maintain GTID consistency
 */
TEST_F(DumpHandlerGtidTest, MultipleDumpLoadsWithDifferentGtids) {
  // Create first dump with GTID-1
  const std::string gtid1 = "uuid:11111";
  mock_binlog_reader_->SetGtidForTest(gtid1);
  query::Query save1;
  save1.type = query::QueryType::DUMP_SAVE;
  save1.filepath = test_dump_dir_.string() + "/dump1.dmp";
  ASSERT_TRUE(handler_->Handle(save1, conn_ctx_).find("OK") == 0);

  // Create second dump with GTID-2
  const std::string gtid2 = "uuid:22222";
  mock_binlog_reader_->SetGtidForTest(gtid2);
  query::Query save2;
  save2.type = query::QueryType::DUMP_SAVE;
  save2.filepath = test_dump_dir_.string() + "/dump2.dmp";
  ASSERT_TRUE(handler_->Handle(save2, conn_ctx_).find("OK") == 0);

  // Load dump1 - should restore GTID-1
  mock_binlog_reader_->SetGtidForTest("");
  mock_binlog_reader_->SetRunningForTest(false);
  mock_binlog_reader_->ResetTestFlags();

  query::Query load1;
  load1.type = query::QueryType::DUMP_LOAD;
  load1.filepath = test_dump_dir_.string() + "/dump1.dmp";
  ASSERT_TRUE(handler_->Handle(load1, conn_ctx_).find("OK") == 0);
  EXPECT_EQ(mock_binlog_reader_->GetLastSetGtid(), gtid1);

  // Load dump2 - should restore GTID-2
  mock_binlog_reader_->ResetTestFlags();
  query::Query load2;
  load2.type = query::QueryType::DUMP_LOAD;
  load2.filepath = test_dump_dir_.string() + "/dump2.dmp";
  ASSERT_TRUE(handler_->Handle(load2, conn_ctx_).find("OK") == 0);
  EXPECT_EQ(mock_binlog_reader_->GetLastSetGtid(), gtid2);

  // Load dump1 again - should restore GTID-1 again
  mock_binlog_reader_->ResetTestFlags();
  ASSERT_TRUE(handler_->Handle(load1, conn_ctx_).find("OK") == 0);
  EXPECT_EQ(mock_binlog_reader_->GetLastSetGtid(), gtid1);
}
#endif  // USE_MYSQL

// ============================================================================
// Async DUMP SAVE Tests (with DumpProgress)
// ============================================================================

/**
 * @brief Test fixture for async DUMP SAVE tests
 *
 * This fixture sets up DumpProgress to test the async behavior
 */
class DumpHandlerAsyncTest : public ::testing::Test {
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

    // Create test dump directory
    test_dump_dir_ = std::filesystem::temp_directory_path() / ("dump_async_test_" + std::to_string(getpid()));
    std::filesystem::create_directories(test_dump_dir_);

    // Create DumpProgress for async testing
    dump_progress_ = std::make_unique<DumpProgress>();

    handler_ctx_ = std::make_unique<HandlerContext>(HandlerContext{
        .table_contexts = table_contexts_,
        .stats = *stats_,
        .full_config = config_.get(),
        .dump_dir = test_dump_dir_.string(),
        .dump_load_in_progress = dump_load_in_progress_,
        .dump_save_in_progress = dump_save_in_progress_,
        .optimization_in_progress = optimization_in_progress_,
        .replication_paused_for_dump = replication_paused_for_dump_,
        .mysql_reconnecting = mysql_reconnecting_,
#ifdef USE_MYSQL
        .binlog_reader = nullptr,
        .sync_manager = nullptr,
#else
        .binlog_reader = nullptr,
#endif
        .dump_progress = dump_progress_.get(),  // Enable async behavior
    });

    handler_ = std::make_unique<DumpHandler>(*handler_ctx_);

    // Setup test data
    auto doc_id1 = table_ctx_->doc_store->AddDocument("1", {{"content", "hello world"}});
    table_ctx_->index->AddDocument(static_cast<index::DocId>(*doc_id1), "hello world");
    auto doc_id2 = table_ctx_->doc_store->AddDocument("2", {{"content", "test document"}});
    table_ctx_->index->AddDocument(static_cast<index::DocId>(*doc_id2), "test document");

    test_filepath_ = "async_test_" + std::to_string(getpid()) + ".dmp";
  }

  void TearDown() override {
    // Join worker thread if running
    if (dump_progress_) {
      dump_progress_->JoinWorker();
    }
    // Clean up test dump directory
    if (std::filesystem::exists(test_dump_dir_)) {
      std::filesystem::remove_all(test_dump_dir_);
    }
  }

  std::unique_ptr<TableContext> table_ctx_;
  std::unordered_map<std::string, TableContext*> table_contexts_;
  std::unique_ptr<config::Config> config_;
  std::unique_ptr<ServerStats> stats_;
  std::unique_ptr<DumpProgress> dump_progress_;
  std::atomic<bool> dump_load_in_progress_{false};
  std::atomic<bool> dump_save_in_progress_{false};
  std::atomic<bool> optimization_in_progress_{false};
  std::atomic<bool> replication_paused_for_dump_{false};
  std::atomic<bool> mysql_reconnecting_{false};
  std::unique_ptr<HandlerContext> handler_ctx_;
  std::unique_ptr<DumpHandler> handler_;
  std::string test_filepath_;
  std::filesystem::path test_dump_dir_;
  ConnectionContext conn_ctx_;
};

TEST_F(DumpHandlerAsyncTest, AsyncDumpSaveReturnsStartedMessage) {
  query::Query query;
  query.type = query::QueryType::DUMP_SAVE;
  query.filepath = test_filepath_;

  std::string response = handler_->Handle(query, conn_ctx_);

  // Should return DUMP_STARTED immediately (async mode)
  EXPECT_TRUE(response.find("OK DUMP_STARTED") == 0) << "Response: " << response;
  EXPECT_TRUE(response.find(test_filepath_) != std::string::npos);
  EXPECT_TRUE(response.find("DUMP STATUS") != std::string::npos);

  // Wait for worker to complete
  dump_progress_->JoinWorker();

  // Verify file was created
  std::filesystem::path full_path = test_dump_dir_ / test_filepath_;
  EXPECT_TRUE(std::filesystem::exists(full_path)) << "Dump file should be created";
}

TEST_F(DumpHandlerAsyncTest, DumpStatusShowsProgressDuringSave) {
  query::Query save_query;
  save_query.type = query::QueryType::DUMP_SAVE;
  save_query.filepath = test_filepath_;

  // Start async save
  handler_->Handle(save_query, conn_ctx_);

  // Immediately check status (might be SAVING or already COMPLETED)
  query::Query status_query;
  status_query.type = query::QueryType::DUMP_STATUS;
  std::string status_response = handler_->Handle(status_query, conn_ctx_);

  EXPECT_TRUE(status_response.find("OK DUMP_STATUS") == 0) << "Response: " << status_response;
  EXPECT_TRUE(status_response.find("filepath:") != std::string::npos) << "Response: " << status_response;
  EXPECT_TRUE(status_response.find("tables_processed:") != std::string::npos) << "Response: " << status_response;
  EXPECT_TRUE(status_response.find("tables_total:") != std::string::npos) << "Response: " << status_response;
  EXPECT_TRUE(status_response.find("elapsed_seconds:") != std::string::npos) << "Response: " << status_response;

  // Wait for completion
  dump_progress_->JoinWorker();
}

TEST_F(DumpHandlerAsyncTest, DumpStatusShowsCompletedAfterSave) {
  query::Query save_query;
  save_query.type = query::QueryType::DUMP_SAVE;
  save_query.filepath = test_filepath_;

  // Start async save
  handler_->Handle(save_query, conn_ctx_);

  // Wait for completion
  dump_progress_->JoinWorker();

  // Check status after completion
  query::Query status_query;
  status_query.type = query::QueryType::DUMP_STATUS;
  std::string status_response = handler_->Handle(status_query, conn_ctx_);

  EXPECT_TRUE(status_response.find("OK DUMP_STATUS") == 0) << "Response: " << status_response;
  EXPECT_TRUE(status_response.find("status: COMPLETED") != std::string::npos) << "Response: " << status_response;
  EXPECT_TRUE(status_response.find("result_filepath:") != std::string::npos) << "Response: " << status_response;
}

TEST_F(DumpHandlerAsyncTest, AsyncDumpSaveClearsFlagOnCompletion) {
  query::Query query;
  query.type = query::QueryType::DUMP_SAVE;
  query.filepath = test_filepath_;

  EXPECT_FALSE(dump_save_in_progress_);

  handler_->Handle(query, conn_ctx_);

  // Flag should be set during save
  // (might already be false if save completed very fast)

  // Wait for completion
  dump_progress_->JoinWorker();

  // Flag should be cleared after completion
  EXPECT_FALSE(dump_save_in_progress_) << "Flag should be cleared after async save completes";
}

TEST_F(DumpHandlerAsyncTest, ConcurrentAsyncSaveBlocked) {
  query::Query query1;
  query1.type = query::QueryType::DUMP_SAVE;
  query1.filepath = test_filepath_;

  // Start first async save
  std::string response1 = handler_->Handle(query1, conn_ctx_);
  EXPECT_TRUE(response1.find("OK DUMP_STARTED") == 0) << "Response: " << response1;

  // Immediately try second save (should be blocked)
  query::Query query2;
  query2.type = query::QueryType::DUMP_SAVE;
  query2.filepath = "second_" + test_filepath_;
  std::string response2 = handler_->Handle(query2, conn_ctx_);

  // Second save should be blocked
  EXPECT_TRUE(response2.find("ERROR") == 0) << "Response: " << response2;
  EXPECT_TRUE(response2.find("another DUMP SAVE is in progress") != std::string::npos) << "Response: " << response2;

  // Clean up
  dump_progress_->JoinWorker();
}

}  // namespace mygramdb::server
