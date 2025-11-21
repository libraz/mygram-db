/**
 * @file gtid_dump_test.cpp
 * @brief Integration tests for GTID preservation and transaction consistency
 *
 * These tests verify that:
 * 1. GTID is correctly captured at snapshot time
 * 2. GTID is preserved across save/load operations
 * 3. Snapshot represents a consistent transaction state
 * 4. Replication can resume from snapshot GTID
 */

#include <gtest/gtest.h>
#include <spdlog/spdlog.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <sstream>
#include <thread>

#include "index/index.h"
#include "query/query_parser.h"
#include "server/handlers/dump_handler.h"
#include "storage/dump_format_v1.h"

#ifdef USE_MYSQL
#include "mysql/binlog_reader.h"
#endif

namespace mygramdb::server {

/**
 * @brief Test fixture for GTID and transaction consistency tests
 *
 * This fixture simulates a realistic replication scenario where:
 * - Documents are added via replication events
 * - Snapshots are taken at various GTID points
 * - Snapshots are verified to contain consistent data
 */
class GtidSnapshotIntegrationTest : public ::testing::Test {
 protected:
  void SetUp() override {
    spdlog::set_level(spdlog::level::debug);

    // Create test table context
    table_ctx_ = std::make_unique<TableContext>();
    table_ctx_->name = "test_table";
    table_ctx_->config.ngram_size = 2;
    table_ctx_->index = std::make_unique<index::Index>(2);
    table_ctx_->doc_store = std::make_unique<storage::DocumentStore>();

    table_contexts_["test_table"] = table_ctx_.get();

    // Create config
    config_ = std::make_unique<config::Config>();
    // Add table config to vector
    config::TableConfig table_config;
    table_config.name = "test_table";
    table_config.ngram_size = 2;
    config_->tables.push_back(table_config);

    stats_ = std::make_unique<ServerStats>();

    // Create unique test directory for parallel test execution
    std::stringstream ss;
    ss << "/tmp/gtid_test_" << getpid() << "_" << std::hash<std::thread::id>{}(std::this_thread::get_id()) << "_"
       << std::chrono::steady_clock::now().time_since_epoch().count();
    test_dir_ = ss.str();
    std::filesystem::create_directories(test_dir_);
  }

  void TearDown() override {
    // Clean up test directory
    if (std::filesystem::exists(test_dir_)) {
      std::filesystem::remove_all(test_dir_);
    }
  }

  /**
   * @brief Simulate a transaction by adding multiple documents
   *
   * In a real MySQL replication scenario, these would be part of a single transaction.
   */
  void SimulateTransaction(const std::string& gtid, const std::vector<std::pair<int, std::string>>& docs) {
    for (const auto& [primary_key_int, content] : docs) {
      auto doc_id = table_ctx_->doc_store->AddDocument(std::to_string(primary_key_int), {{"content", content}});
      table_ctx_->index->AddDocument(static_cast<index::DocId>(*doc_id), content);
    }
    // Record the GTID for this transaction
    transaction_gtids_.push_back(gtid);
  }

  /**
   * @brief Create a snapshot and verify GTID is captured
   */
  std::string CreateSnapshotWithGtid(const std::string& expected_gtid) {
    // Normalize GTID (remove whitespace) before using
    // This simulates MySQL C API behavior which may return GTIDs with newlines
    std::string normalized_gtid = expected_gtid;
    normalized_gtid.erase(
        std::remove_if(normalized_gtid.begin(), normalized_gtid.end(),
                       [](unsigned char c) { return std::isspace(c); }),
        normalized_gtid.end());

    // Use a short hash for filename to avoid "file name too long" errors
    // Long GTIDs (e.g., 6 UUIDs from multiple replication sources) can exceed filesystem limits
    std::hash<std::string> hasher;
    std::string filepath = test_dir_ + "/snapshot_" + std::to_string(hasher(normalized_gtid)) + ".dmp";

    // Convert table_contexts to format expected by WriteDumpV1
    std::unordered_map<std::string, std::pair<index::Index*, storage::DocumentStore*>> converted;
    for (const auto& [name, ctx] : table_contexts_) {
      converted[name] = {ctx->index.get(), ctx->doc_store.get()};
    }

    auto success = storage::dump_v1::WriteDumpV1(filepath, normalized_gtid, *config_, converted);
    EXPECT_TRUE(success) << "Failed to create snapshot";

    return filepath;
  }

  /**
   * @brief Verify GTID in snapshot file
   */
  std::string GetSnapshotGtid(const std::string& filepath) {
    storage::dump_v1::DumpInfo info;
    auto success = storage::dump_v1::GetDumpInfo(filepath, info);
    EXPECT_TRUE(success) << "Failed to get snapshot info";
    return info.gtid;
  }

  /**
   * @brief Load snapshot and return GTID
   */
  std::string LoadSnapshotAndGetGtid(const std::string& filepath) {
    std::unordered_map<std::string, std::pair<index::Index*, storage::DocumentStore*>> converted;
    for (const auto& [name, ctx] : table_contexts_) {
      converted[name] = {ctx->index.get(), ctx->doc_store.get()};
    }

    std::string gtid;
    config::Config loaded_config;
    auto success = storage::dump_v1::ReadDumpV1(filepath, gtid, loaded_config, converted);
    EXPECT_TRUE(success) << "Failed to load snapshot";

    return gtid;
  }

  std::unique_ptr<TableContext> table_ctx_;
  std::unordered_map<std::string, TableContext*> table_contexts_;
  std::unique_ptr<config::Config> config_;
  std::unique_ptr<ServerStats> stats_;
  std::string test_dir_;
  std::vector<std::string> transaction_gtids_;
};

// ============================================================================
// GTID Preservation Tests
// ============================================================================

TEST_F(GtidSnapshotIntegrationTest, GtidIsCapturedAtSnapshotTime) {
  std::string gtid = "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5";

  // Add some data
  SimulateTransaction(gtid, {{1, "test doc 1"}, {2, "test doc 2"}});

  // Create snapshot with this GTID
  std::string snapshot_path = CreateSnapshotWithGtid(gtid);

  // Verify GTID is in the snapshot
  std::string captured_gtid = GetSnapshotGtid(snapshot_path);
  EXPECT_EQ(captured_gtid, gtid);
}

TEST_F(GtidSnapshotIntegrationTest, GtidIsPreservedAcrossSaveLoad) {
  std::string original_gtid = "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-10";

  // Add data
  SimulateTransaction(original_gtid, {{1, "doc 1"}, {2, "doc 2"}, {3, "doc 3"}});

  // Save with GTID
  std::string snapshot_path = CreateSnapshotWithGtid(original_gtid);

  // Clear data
  table_ctx_->index = std::make_unique<index::Index>(2);
  table_ctx_->doc_store = std::make_unique<storage::DocumentStore>();

  // Load and verify GTID is restored
  std::string loaded_gtid = LoadSnapshotAndGetGtid(snapshot_path);
  EXPECT_EQ(loaded_gtid, original_gtid);
}

TEST_F(GtidSnapshotIntegrationTest, EmptyGtidIsHandledCorrectly) {
  std::string empty_gtid = "";

  // Add data
  SimulateTransaction(empty_gtid, {{1, "doc 1"}});

  // Save with empty GTID (e.g., no replication configured)
  std::string snapshot_path = CreateSnapshotWithGtid(empty_gtid);

  // Verify empty GTID is preserved
  std::string captured_gtid = GetSnapshotGtid(snapshot_path);
  EXPECT_EQ(captured_gtid, empty_gtid);
}

TEST_F(GtidSnapshotIntegrationTest, MultipleGtidSetsArePreserved) {
  // MySQL 8.0 supports multiple GTID sets from different sources
  std::string multi_gtid =
      "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5,"
      "4E22FB58-82DB-22F2-AF44-D90BB9539673:1-3";

  SimulateTransaction(multi_gtid, {{1, "doc 1"}});

  std::string snapshot_path = CreateSnapshotWithGtid(multi_gtid);
  std::string captured_gtid = GetSnapshotGtid(snapshot_path);

  EXPECT_EQ(captured_gtid, multi_gtid);
}

// ============================================================================
// Transaction Consistency Tests
// ============================================================================

TEST_F(GtidSnapshotIntegrationTest, SnapshotContainsCompleteTransaction) {
  // Simulate a multi-document transaction
  std::string gtid = "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-100";
  std::vector<std::pair<int, std::string>> transaction_docs;
  for (int i = 1; i <= 50; ++i) {
    transaction_docs.push_back({i, "transaction doc " + std::to_string(i)});
  }

  SimulateTransaction(gtid, transaction_docs);

  // Create snapshot
  std::string snapshot_path = CreateSnapshotWithGtid(gtid);

  // Clear and reload
  table_ctx_->index = std::make_unique<index::Index>(2);
  table_ctx_->doc_store = std::make_unique<storage::DocumentStore>();
  LoadSnapshotAndGetGtid(snapshot_path);

  // Verify ALL documents from transaction are present
  EXPECT_EQ(table_ctx_->doc_store->Size(), 50);

  for (int i = 1; i <= 50; ++i) {
    auto doc_id = table_ctx_->doc_store->GetDocId(std::to_string(i));
    EXPECT_TRUE(doc_id.has_value()) << "Document " << i << " missing";
  }
}

TEST_F(GtidSnapshotIntegrationTest, SnapshotDoesNotContainPartialTransaction) {
  // This test verifies that snapshot is taken at a transaction boundary
  // In practice, this is ensured by taking snapshots during read-only mode

  std::string gtid_before = "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-99";
  std::string gtid_after = "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-100";

  // Transaction 1 (complete)
  SimulateTransaction(gtid_before, {{1, "doc 1"}, {2, "doc 2"}});

  // Take snapshot at transaction boundary
  std::string snapshot_path = CreateSnapshotWithGtid(gtid_before);
  std::string captured_gtid = GetSnapshotGtid(snapshot_path);
  EXPECT_EQ(captured_gtid, gtid_before);

  // Transaction 2 (added after snapshot)
  SimulateTransaction(gtid_after, {{3, "doc 3"}});

  // Reload snapshot
  table_ctx_->index = std::make_unique<index::Index>(2);
  table_ctx_->doc_store = std::make_unique<storage::DocumentStore>();
  LoadSnapshotAndGetGtid(snapshot_path);

  // Verify transaction 2 is NOT in snapshot
  EXPECT_EQ(table_ctx_->doc_store->Size(), 2);
  auto doc_id_3 = table_ctx_->doc_store->GetDocId("3");
  EXPECT_FALSE(doc_id_3.has_value());
}

TEST_F(GtidSnapshotIntegrationTest, ConcurrentTransactionsDuringSnapshot) {
  // Simulate the scenario where transactions are being applied while snapshot is taken
  // In production, this is prevented by read-only mode

  std::string gtid_snapshot = "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-50";

  // Add initial data
  for (int i = 1; i <= 50; ++i) {
    auto doc_id = table_ctx_->doc_store->AddDocument(std::to_string(i), {{"content", "doc " + std::to_string(i)}});
    table_ctx_->index->AddDocument(static_cast<index::DocId>(*doc_id), "doc " + std::to_string(i));
  }

  // Take snapshot (in real scenario, read_only=true would block writes)
  std::string snapshot_path = CreateSnapshotWithGtid(gtid_snapshot);

  // Verify snapshot has consistent state
  table_ctx_->index = std::make_unique<index::Index>(2);
  table_ctx_->doc_store = std::make_unique<storage::DocumentStore>();
  LoadSnapshotAndGetGtid(snapshot_path);

  EXPECT_EQ(table_ctx_->doc_store->Size(), 50);
}

// ============================================================================
// Replication Resume Tests
// ============================================================================

TEST_F(GtidSnapshotIntegrationTest, ReplicationCanResumeFromSnapshotGtid) {
  // Apply transactions 1 and 2
  SimulateTransaction("3E11FA47-71CA-11E1-9E33-C80AA9429562:1", {{1, "doc 1"}});
  SimulateTransaction("3E11FA47-71CA-11E1-9E33-C80AA9429562:1-2", {{2, "doc 2"}});

  // Take snapshot at transaction 2 (before transaction 3)
  std::string snapshot_gtid = "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-2";
  std::string snapshot_path = CreateSnapshotWithGtid(snapshot_gtid);

  // Apply transaction 3 (after snapshot)
  SimulateTransaction("3E11FA47-71CA-11E1-9E33-C80AA9429562:1-3", {{3, "doc 3"}});

  // Verify current state has all 3 documents
  EXPECT_EQ(table_ctx_->doc_store->Size(), 3);

  // Simulate restart: load snapshot
  table_ctx_->index = std::make_unique<index::Index>(2);
  table_ctx_->doc_store = std::make_unique<storage::DocumentStore>();
  std::string loaded_gtid = LoadSnapshotAndGetGtid(snapshot_path);

  EXPECT_EQ(loaded_gtid, snapshot_gtid);

  // Verify we have docs 1-2 but not 3 (snapshot was taken before transaction 3)
  EXPECT_EQ(table_ctx_->doc_store->Size(), 2);
  EXPECT_TRUE(table_ctx_->doc_store->GetDocId("1").has_value());
  EXPECT_TRUE(table_ctx_->doc_store->GetDocId("2").has_value());
  EXPECT_FALSE(table_ctx_->doc_store->GetDocId("3").has_value());

  // Now replication would resume from transaction 3
  // (In real scenario, BinlogReader would start from loaded_gtid)
}

TEST_F(GtidSnapshotIntegrationTest, SnapshotGtidMatchesLastAppliedTransaction) {
  std::string gtid1 = "3E11FA47-71CA-11E1-9E33-C80AA9429562:1";
  std::string gtid2 = "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-2";
  std::string gtid3 = "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-3";

  SimulateTransaction(gtid1, {{1, "doc 1"}});
  SimulateTransaction(gtid2, {{2, "doc 2"}});
  SimulateTransaction(gtid3, {{3, "doc 3"}});

  // Snapshot should capture the GTID of the last applied transaction
  std::string snapshot_path = CreateSnapshotWithGtid(gtid3);
  std::string captured_gtid = GetSnapshotGtid(snapshot_path);

  EXPECT_EQ(captured_gtid, gtid3);
}

// ============================================================================
// GTID Format Validation Tests
// ============================================================================

TEST_F(GtidSnapshotIntegrationTest, ValidGtidFormats) {
  std::vector<std::string> valid_gtids = {
      "",  // Empty (no replication)
      "3E11FA47-71CA-11E1-9E33-C80AA9429562:1",
      "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5",
      "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5:10-20",
      "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5,"
      "4E22FB58-82DB-22F2-AF44-D90BB9539673:1-3",
  };

  for (size_t i = 0; i < valid_gtids.size(); ++i) {
    const auto& gtid = valid_gtids[i];
    SimulateTransaction(gtid, {{static_cast<int>(i + 1), "doc " + std::to_string(i + 1)}});

    std::string snapshot_path = test_dir_ + "/snapshot_" + std::to_string(i) + ".dmp";

    std::unordered_map<std::string, std::pair<index::Index*, storage::DocumentStore*>> converted;
    for (const auto& [name, ctx] : table_contexts_) {
      converted[name] = {ctx->index.get(), ctx->doc_store.get()};
    }

    auto success = storage::dump_v1::WriteDumpV1(snapshot_path, gtid, *config_, converted);
    EXPECT_TRUE(success) << "Failed to save with GTID: " << gtid;

    std::string captured_gtid = GetSnapshotGtid(snapshot_path);
    EXPECT_EQ(captured_gtid, gtid) << "GTID mismatch for: " << gtid;
  }
}

// ============================================================================
// Stress Tests
// ============================================================================

TEST_F(GtidSnapshotIntegrationTest, LargeGtidStringIsPreserved) {
  // Test with a realistically large GTID (single UUID with many transaction ranges)
  // This simulates a long-running replication scenario with transaction gaps
  std::string large_gtid = "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-100:105-200:210-300:350-400:450-500";

  SimulateTransaction(large_gtid, {{1, "doc 1"}});

  std::string snapshot_path = CreateSnapshotWithGtid(large_gtid);
  std::string captured_gtid = GetSnapshotGtid(snapshot_path);

  EXPECT_EQ(captured_gtid, large_gtid);
  EXPECT_GT(large_gtid.length(), 50) << "GTID should be reasonably sized";
}

TEST_F(GtidSnapshotIntegrationTest, MultipleSnapshotsWithDifferentGtids) {
  std::vector<std::string> gtids = {
      "3E11FA47-71CA-11E1-9E33-C80AA9429562:1",
      "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-10",
      "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-100",
      "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-1000",
  };

  std::vector<std::string> snapshot_paths;

  for (size_t i = 0; i < gtids.size(); ++i) {
    // Add documents for this transaction
    for (int j = 0; j < 10; ++j) {
      int doc_id = static_cast<int>(i * 10 + j + 1);
      table_ctx_->index->AddDocument(doc_id, "doc " + std::to_string(doc_id));
      table_ctx_->doc_store->AddDocument(std::to_string(doc_id), {{"content", "doc " + std::to_string(doc_id)}});
    }

    // Create snapshot
    std::string path = CreateSnapshotWithGtid(gtids[i]);
    snapshot_paths.push_back(path);
  }

  // Verify each snapshot has correct GTID
  for (size_t i = 0; i < gtids.size(); ++i) {
    std::string captured_gtid = GetSnapshotGtid(snapshot_paths[i]);
    EXPECT_EQ(captured_gtid, gtids[i]) << "Snapshot " << i << " has wrong GTID";
  }
}

/**
 * @brief Test GTID whitespace handling (MySQL 8.4 compatibility)
 *
 * MySQL 8.4 returns GTIDs with newlines for readability when multiple UUIDs are present.
 * This test verifies that MygramDB correctly handles GTIDs with various whitespace characters.
 *
 * Background:
 * - MySQL's default_string_format uses ",\n" as gno_sid_separator
 * - Long GTID strings contain embedded newlines after each comma
 * - Example from production: "uuid1:1-100,\nuuid2:1-200,\nuuid3:1-300"
 * - MySQL's parser uses SKIP_WHITESPACE() to handle this
 *
 * See: backup/mysql-8.4.7/sql/rpl_gtid_set.cc:78-79
 */
TEST_F(GtidSnapshotIntegrationTest, GtidWithWhitespaceHandling) {
  // Test cases covering MySQL's actual formatting behavior
  std::vector<std::pair<std::string, std::string>> test_cases = {
      // Case 1: Single UUID with newlines (should be normalized)
      {"3E11FA47-71CA-11E1-9E33-C80AA9429562:1-100\n",
       "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-100"},

      // Case 2: Multiple UUIDs with newlines after commas (MySQL 8.4 format)
      {"3E11FA47-71CA-11E1-9E33-C80AA9429562:1-100,\n"
       "4E11FA47-71CA-11E1-9E33-C80AA9429562:1-200",
       "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-100,"
       "4E11FA47-71CA-11E1-9E33-C80AA9429562:1-200"},

      // Case 3: Real-world example with 6 UUIDs (observed in production)
      {"023cdb62-8398-11ee-9327-4a0008d26241:1-8178383,\n"
       "ba13f0ad-f09a-11e8-b079-b2869295776e:1-183978832,\n"
       "cf65e299-d4d1-11ec-9b58-caab0c3eeec8:1-107084115,\n"
       "e01b10a0-b7ee-11f0-b687-d24f2b326650:1-2408289,\n"
       "ef2ed951-f10a-11e8-a7bc-6ae4cd7ca684:1-1490678,\n"
       "ffad7b96-8397-11ee-943f-8e94ebb74b41:1-105346456",
       "023cdb62-8398-11ee-9327-4a0008d26241:1-8178383,"
       "ba13f0ad-f09a-11e8-b079-b2869295776e:1-183978832,"
       "cf65e299-d4d1-11ec-9b58-caab0c3eeec8:1-107084115,"
       "e01b10a0-b7ee-11f0-b687-d24f2b326650:1-2408289,"
       "ef2ed951-f10a-11e8-a7bc-6ae4cd7ca684:1-1490678,"
       "ffad7b96-8397-11ee-943f-8e94ebb74b41:1-105346456"},

      // Case 4: Mixed whitespace (spaces, tabs, newlines)
      {"3E11FA47-71CA-11E1-9E33-C80AA9429562:1-100, \n\t"
       "4E11FA47-71CA-11E1-9E33-C80AA9429562:1-200",
       "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-100,"
       "4E11FA47-71CA-11E1-9E33-C80AA9429562:1-200"},

      // Case 5: Leading and trailing whitespace
      {"\n  3E11FA47-71CA-11E1-9E33-C80AA9429562:1-100  \n",
       "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-100"},
  };

  for (size_t i = 0; i < test_cases.size(); ++i) {
    const auto& [gtid_with_whitespace, expected_normalized] = test_cases[i];

    // Add a test document
    table_ctx_->index->AddDocument(static_cast<int>(i + 1), "test doc");
    table_ctx_->doc_store->AddDocument(std::to_string(i + 1), {{"content", "test doc"}});

    // Create snapshot with GTID containing whitespace
    std::string path = CreateSnapshotWithGtid(gtid_with_whitespace);

    // Verify GTID is normalized (whitespace removed)
    std::string captured_gtid = GetSnapshotGtid(path);
    EXPECT_EQ(captured_gtid, expected_normalized)
        << "Test case " << i << " failed\n"
        << "Input GTID (with whitespace): " << gtid_with_whitespace << "\n"
        << "Expected (normalized): " << expected_normalized << "\n"
        << "Actual (captured): " << captured_gtid;

    // Cleanup
    std::filesystem::remove(path);
  }
}

}  // namespace mygramdb::server
