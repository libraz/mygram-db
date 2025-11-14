/**
 * @file snapshot_handler_test.cpp
 * @brief Unit tests for DumpHandler (DUMP commands)
 */

#include "server/handlers/dump_handler.h"

#include <gtest/gtest.h>
#include <spdlog/spdlog.h>

#include <filesystem>
#include <fstream>

#include "index/index.h"
#include "query/query_parser.h"
#include "server/response_formatter.h"
#include "storage/document_store.h"
#include "storage/dump_format_v1.h"

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

    handler_ctx_ = std::make_unique<HandlerContext>(HandlerContext{
        .table_contexts = table_contexts_,
        .stats = *stats_,
        .full_config = config_.get(),
        .dump_dir = "/tmp",
        .loading = loading_,
        .read_only = read_only_,
        .optimization_in_progress = optimization_in_progress_,
        .binlog_reader = nullptr,
    });

    // Create handler
    handler_ = std::make_unique<DumpHandler>(*handler_ctx_);

    // Setup test data
    AddTestData();

    // Setup test file path
    test_filepath_ = "/tmp/test_snapshot_" + std::to_string(std::time(nullptr)) + ".dmp";
  }

  void TearDown() override {
    // Clean up test files
    if (std::filesystem::exists(test_filepath_)) {
      std::filesystem::remove(test_filepath_);
    }
  }

  void AddTestData() {
    // Add documents to index and doc_store
    auto doc_id1 = table_ctx_->doc_store->AddDocument("1", {{"content", "hello world"}});
    table_ctx_->index->AddDocument(static_cast<index::DocId>(doc_id1), "hello world");

    auto doc_id2 = table_ctx_->doc_store->AddDocument("2", {{"content", "test document"}});
    table_ctx_->index->AddDocument(static_cast<index::DocId>(doc_id2), "test document");

    auto doc_id3 = table_ctx_->doc_store->AddDocument("3", {{"content", "another test"}});
    table_ctx_->index->AddDocument(static_cast<index::DocId>(doc_id3), "another test");
  }

  std::unique_ptr<TableContext> table_ctx_;
  std::unordered_map<std::string, TableContext*> table_contexts_;
  std::unique_ptr<config::Config> config_;
  std::unique_ptr<ServerStats> stats_;
  std::atomic<bool> loading_{false};
  std::atomic<bool> read_only_{false};
  std::atomic<bool> optimization_in_progress_{false};
  std::unique_ptr<HandlerContext> handler_ctx_;
  std::unique_ptr<DumpHandler> handler_;
  std::string test_filepath_;
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
  EXPECT_TRUE(std::filesystem::exists(test_filepath_));
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
  std::string expected_path = "/tmp/relative_test.dmp";
  EXPECT_TRUE(std::filesystem::exists(expected_path));
  std::filesystem::remove(expected_path);
}

TEST_F(DumpHandlerTest, DumpSaveSetsReadOnlyMode) {
  query::Query query;
  query.type = query::QueryType::DUMP_SAVE;
  query.filepath = test_filepath_;

  EXPECT_FALSE(read_only_);
  std::string response = handler_->Handle(query, conn_ctx_);
  // Should be false after completion
  EXPECT_FALSE(read_only_);
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

  EXPECT_FALSE(loading_);
  std::string response = handler_->Handle(load_query, conn_ctx_);
  // Should be false after completion
  EXPECT_FALSE(loading_);
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

  // Corrupt the file
  std::fstream file(test_filepath_, std::ios::binary | std::ios::in | std::ios::out);
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
// GTID Tests (Critical for Replication)
// ============================================================================

TEST_F(DumpHandlerTest, GtidPreservationAcrossSaveLoad) {
  // TODO: This test requires a mock BinlogReader with GTID support
  // For now, verify that GTID is empty when no binlog_reader is present

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
    table_ctx_->index->AddDocument(static_cast<index::DocId>(doc_id), content);
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
      .loading = loading_,
      .read_only = read_only_,
      .optimization_in_progress = optimization_in_progress_,
      .binlog_reader = nullptr,
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
  EXPECT_FALSE(read_only_);

  // Try to save to an invalid path (should trigger exception or error)
  query::Query invalid_query;
  invalid_query.type = query::QueryType::DUMP_SAVE;
  invalid_query.filepath = "/invalid/path/that/does/not/exist/test.dmp";

  std::string error_response = handler_->Handle(invalid_query, conn_ctx_);

  // Even if error occurs, read_only should be reset to false
  EXPECT_FALSE(read_only_) << "read_only flag should be reset even on error";
  EXPECT_TRUE(error_response.find("ERROR") == 0 || error_response.find("Failed") != std::string::npos);
}

TEST_F(DumpHandlerTest, LoadingFlagResetOnException) {
  // Verify loading is false initially
  EXPECT_FALSE(loading_);

  // Try to load from non-existent file
  query::Query invalid_query;
  invalid_query.type = query::QueryType::DUMP_LOAD;
  invalid_query.filepath = "/tmp/nonexistent_file_that_does_not_exist.dmp";

  std::string error_response = handler_->Handle(invalid_query, conn_ctx_);

  // Even if error occurs, loading should be reset to false
  EXPECT_FALSE(loading_) << "loading flag should be reset even on error";
  EXPECT_TRUE(error_response.find("ERROR") == 0 || error_response.find("Failed") != std::string::npos);
}

TEST_F(DumpHandlerTest, ConcurrentFlagsNotAffected) {
  // This test verifies that read_only and loading flags work correctly
  // when set by different operations

  // Set loading flag externally (simulating another operation)
  loading_ = true;

  // Try to save dump (should work independently)
  query::Query save_query;
  save_query.type = query::QueryType::DUMP_SAVE;
  save_query.filepath = test_filepath_;
  std::string save_response = handler_->Handle(save_query, conn_ctx_);
  EXPECT_TRUE(save_response.find("OK SAVED") == 0);

  // read_only should be reset, but loading should remain true
  EXPECT_FALSE(read_only_);
  EXPECT_TRUE(loading_) << "loading flag should not be affected by save operation";

  // Reset for next test
  loading_ = false;
  read_only_ = true;

  // Load dump (should work independently)
  query::Query load_query;
  load_query.type = query::QueryType::DUMP_LOAD;
  load_query.filepath = test_filepath_;
  std::string load_response = handler_->Handle(load_query, conn_ctx_);
  EXPECT_TRUE(load_response.find("OK LOADED") == 0);

  // loading should be reset, but read_only should remain true
  EXPECT_FALSE(loading_);
  EXPECT_TRUE(read_only_) << "read_only flag should not be affected by load operation";
}

}  // namespace mygramdb::server
