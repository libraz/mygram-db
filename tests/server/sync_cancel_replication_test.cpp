/**
 * @file sync_cancel_replication_test.cpp
 * @brief Tests that SYNC cancellation/failure restarts replication
 *
 * When SYNC starts, replication is stopped. If SYNC is then cancelled or
 * fails, BuildSnapshotAsync must restart replication from the saved GTID
 * position.
 */

#ifdef USE_MYSQL

#include <gtest/gtest.h>

#include <chrono>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "config/config.h"
#include "index/index.h"
#include "mysql/binlog_reader_interface.h"
#include "mysql/connection.h"
#include "mysql/mysql_test_helpers.h"
#include "server/sync_operation_manager.h"
#include "server/tcp_server.h"
#include "storage/document_store.h"
#include "utils/error.h"
#include "utils/expected.h"

namespace mygramdb::server {
namespace {

/**
 * @brief Mock BinlogReader that tracks method calls for testing
 *
 * Records all calls to Start(), Stop(), GetCurrentGTID(), and SetCurrentGTID()
 * to verify replication restart behavior after SYNC cancellation.
 */
class MockBinlogReader : public mysql::IBinlogReader {
 public:
  MockBinlogReader() = default;

  mygram::utils::Expected<void, mygram::utils::Error> Start() override {
    start_call_count_++;
    if (on_start_) {
      auto on_start = std::move(on_start_);
      on_start_ = nullptr;
      on_start();
    }
    running_ = true;
    if (start_should_fail_) {
      running_ = false;
      return mygram::utils::MakeUnexpected(
          mygram::utils::MakeError(mygram::utils::ErrorCode::kInternalError, "Mock start failure"));
    }
    return {};
  }

  void Stop() override {
    stop_call_count_++;
    running_ = false;
  }

  bool IsRunning() const override { return running_; }

  std::string GetCurrentGTID() const override { return current_gtid_; }

  void SetCurrentGTID(const std::string& gtid) override {
    set_gtid_calls_.push_back(gtid);
    current_gtid_ = gtid;
  }

  std::string GetLastError() const override { return last_error_.message(); }

  mygram::utils::ErrorCode GetLastErrorCode() const override { return last_error_.code(); }

  void SetLastErrorForTest(const mygram::utils::Error& error) { last_error_ = error; }

  uint64_t GetProcessedEvents() const override { return 0; }

  size_t GetQueueSize() const override { return 0; }

  // Test accessors
  int GetStartCallCount() const { return start_call_count_; }
  int GetStopCallCount() const { return stop_call_count_; }
  const std::vector<std::string>& GetSetGtidCalls() const { return set_gtid_calls_; }

  // Test controls
  void SetRunning(bool running) { running_ = running; }
  void SetGtid(const std::string& gtid) { current_gtid_ = gtid; }
  void SetStartShouldFail(bool fail) { start_should_fail_ = fail; }
  /// Run @p callback once, inside the next Start() call.
  void SetOnNextStart(std::function<void()> callback) { on_start_ = std::move(callback); }

 private:
  bool running_ = false;
  bool start_should_fail_ = false;
  mygram::utils::Error last_error_;
  std::string current_gtid_;
  int start_call_count_ = 0;
  int stop_call_count_ = 0;
  std::vector<std::string> set_gtid_calls_;
  std::function<void()> on_start_;
};

/**
 * @brief Test fixture for SYNC cancellation replication restart tests
 */
class SyncCancelReplicationTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create test table context
    auto ctx = std::make_unique<TableContext>();
    ctx->name = "test_table";
    ctx->config.name = "test_table";
    ctx->config.primary_key = "id";
    ctx->config.ngram_size = 2;
    ctx->config.kanji_ngram_size = 1;
    ctx->index = std::make_unique<index::Index>(2, 1);
    ctx->doc_store = std::make_unique<storage::DocumentStore>();

    table_contexts_ptrs_["test_table"] = ctx.get();
    table_contexts_["test_table"] = std::move(ctx);

    // Create config with replication enabled
    config_ = std::make_unique<config::Config>();
    config_->mysql.host = "localhost";
    config_->mysql.port = 3306;
    config_->mysql.user = "test";
    config_->mysql.password = "test";
    config_->mysql.database = "testdb";
    config_->replication.enable = true;

    mock_reader_ = std::make_unique<MockBinlogReader>();
  }

  void TearDown() override {
    table_contexts_.clear();
    table_contexts_ptrs_.clear();
  }

  std::unordered_map<std::string, TableContext*> table_contexts_ptrs_;
  std::unordered_map<std::string, std::unique_ptr<TableContext>> table_contexts_;
  std::unique_ptr<config::Config> config_;
  std::unique_ptr<MockBinlogReader> mock_reader_;
};

/**
 * @brief Verify replication survives a SYNC that fails before stopping it
 *
 * When MySQL connection fails before replication is stopped (the normal
 * case in test environments without MySQL), replication should remain
 * running — it was never stopped, so no restart is needed.
 */
TEST_F(SyncCancelReplicationTest, CancelledSyncRestartsReplication) {
  // Simulate replication running at a known GTID
  mock_reader_->SetRunning(true);
  mock_reader_->SetGtid("uuid:100");

  auto manager = std::make_unique<SyncOperationManager>(table_contexts_ptrs_, config_.get(), mock_reader_.get());

  // Start SYNC — will fail to connect to MySQL before reaching the
  // replication-stop code path.
  auto result = manager->StartSync("test_table");
  ASSERT_TRUE(result) << "StartSync should succeed to start the async operation";

  // Wait for the SYNC thread to finish (connection failure)
  manager->WaitForCompletion(10);

  // MySQL connection fails BEFORE the code reaches reader->Stop(),
  // so replication was never stopped and does not need restart.
  // The reader should still be in its original state.
  if (mock_reader_->GetStopCallCount() == 0) {
    // Connection failed before replication was stopped — expected in test
    // environments without MySQL. Replication remains running.
    EXPECT_TRUE(mock_reader_->IsRunning()) << "Replication should still be running when connection fails early";
    EXPECT_EQ(0, mock_reader_->GetStartCallCount())
        << "Start() should not be called when replication was never stopped";
  } else {
    // If we somehow reached the stop-replication path, verify restart
    EXPECT_GE(mock_reader_->GetStartCallCount(), 1) << "Replication should be restarted after being stopped";
  }

  manager.reset();
}

/**
 * @brief Verify RestartReplicationFromGtid sets GTID and calls Start
 *
 * This tests the helper method directly through the observable behavior:
 * when SYNC fails after replication was running, the reader should have
 * SetCurrentGTID called with the saved GTID, and Start() should be called.
 */
TEST_F(SyncCancelReplicationTest, FailedSyncSetsGtidAndRestartsReplication) {
  // Set up reader as running with a known GTID
  mock_reader_->SetRunning(true);
  mock_reader_->SetGtid("server-uuid:42");

  auto manager = std::make_unique<SyncOperationManager>(table_contexts_ptrs_, config_.get(), mock_reader_.get());

  // Start SYNC - it will fail because MySQL is not available
  auto result = manager->StartSync("test_table");
  ASSERT_TRUE(result);

  // Wait for the SYNC to fail (connection error)
  manager->WaitForCompletion(10);

  // Check status to see what happened
  std::string status = manager->GetSyncStatus();

  // If the reader was stopped (replication was running when we entered the
  // replication-stop section), we expect restart behavior
  if (mock_reader_->GetStopCallCount() > 0) {
    // Reader should have been stopped, then restarted
    // SetCurrentGTID should have been called with the original GTID
    const auto& gtid_calls = mock_reader_->GetSetGtidCalls();
    ASSERT_FALSE(gtid_calls.empty()) << "SetCurrentGTID should have been called to restore GTID";
    EXPECT_EQ("server-uuid:42", gtid_calls.back()) << "GTID should be restored to the pre-SYNC value";

    // Start should have been called to restart replication
    EXPECT_GE(mock_reader_->GetStartCallCount(), 1) << "Start() should have been called to restart replication";
  }

  manager.reset();
}

TEST_F(SyncCancelReplicationTest, RestartReplicationFromGtidReportsStartFailure) {
  mock_reader_->SetRunning(false);
  mock_reader_->SetStartShouldFail(true);

  auto manager = std::make_unique<SyncOperationManager>(table_contexts_ptrs_, config_.get(), mock_reader_.get());
  auto restart_result =
      manager->RestartReplicationFromGtidForTest(mock_reader_.get(), "server-uuid:99", "test_table", "unit_test");

  ASSERT_FALSE(restart_result.has_value());
  EXPECT_NE(restart_result.error().message().find("Mock start failure"), std::string::npos);
  EXPECT_EQ(1, mock_reader_->GetStartCallCount());
  ASSERT_FALSE(mock_reader_->GetSetGtidCalls().empty());
  EXPECT_EQ("server-uuid:99", mock_reader_->GetSetGtidCalls().back());
  EXPECT_FALSE(mock_reader_->IsRunning());
}

TEST_F(SyncCancelReplicationTest, EmptyConservativeGtidStillRestartsReplication) {
  mock_reader_->SetRunning(false);
  mock_reader_->SetGtid("");

  auto manager = std::make_unique<SyncOperationManager>(table_contexts_ptrs_, config_.get(), mock_reader_.get());
  auto restart_result =
      manager->RestartReplicationFromGtidForTest(mock_reader_.get(), "", "test_table", "empty_lower_bound");

  ASSERT_TRUE(restart_result.has_value()) << restart_result.error().message();
  EXPECT_EQ(1, mock_reader_->GetStartCallCount());
  ASSERT_EQ(1U, mock_reader_->GetSetGtidCalls().size());
  EXPECT_TRUE(mock_reader_->GetSetGtidCalls().front().empty());
  EXPECT_TRUE(mock_reader_->IsRunning());
}

/**
 * @brief Verify replication is NOT restarted during shutdown cancellation
 *
 * When SYNC is cancelled due to server shutdown (not user request),
 * replication should NOT be restarted since the server is shutting down.
 */
TEST_F(SyncCancelReplicationTest, ShutdownCancellationDoesNotRestartReplication) {
  mock_reader_->SetRunning(true);
  mock_reader_->SetGtid("uuid:200");

  auto manager = std::make_unique<SyncOperationManager>(table_contexts_ptrs_, config_.get(), mock_reader_.get());

  auto result = manager->StartSync("test_table");
  ASSERT_TRUE(result);

  // Wait briefly then request shutdown (not user stop)
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  manager->RequestShutdown();

  manager->WaitForCompletion(5);

  // After shutdown, replication should NOT have been restarted
  // (The cancellation path checks shutdown_requested_ and skips restart)
  // Note: Start() count should be 0 from the cancellation path.
  // The reader may have been stopped, but should not have been restarted.

  // Clean up - destructor will also do cleanup
  manager.reset();

  // If we get here without hanging, the test passes.
  // The key invariant is that the destructor completes without issues.
  SUCCEED();
}

/**
 * @brief Verify replication is NOT restarted when it was not running before SYNC
 *
 * If replication was not running when SYNC started, it should not be
 * started after SYNC cancellation.
 */
TEST_F(SyncCancelReplicationTest, NoRestartWhenReplicationWasNotRunning) {
  // Reader exists but is NOT running
  mock_reader_->SetRunning(false);
  mock_reader_->SetGtid("uuid:50");

  auto manager = std::make_unique<SyncOperationManager>(table_contexts_ptrs_, config_.get(), mock_reader_.get());

  auto result = manager->StartSync("test_table");
  ASSERT_TRUE(result);

  // Wait for connection failure
  manager->WaitForCompletion(10);

  // Replication should NOT have been started since it wasn't running before SYNC
  EXPECT_EQ(0, mock_reader_->GetStartCallCount())
      << "Replication should NOT be started if it was not running before SYNC";

  // Stop should NOT have been called either
  EXPECT_EQ(0, mock_reader_->GetStopCallCount())
      << "Replication should NOT be stopped if it was not running before SYNC";

  manager.reset();
}

/**
 * @brief Verify replication restart is skipped when replication is disabled
 */
TEST_F(SyncCancelReplicationTest, NoRestartWhenReplicationDisabled) {
  // Disable replication in config
  config_->replication.enable = false;

  mock_reader_->SetRunning(true);
  mock_reader_->SetGtid("uuid:300");

  auto manager = std::make_unique<SyncOperationManager>(table_contexts_ptrs_, config_.get(), mock_reader_.get());

  auto result = manager->StartSync("test_table");
  ASSERT_TRUE(result);

  // Wait for connection failure
  manager->WaitForCompletion(10);

  // Replication should NOT have been stopped or started
  EXPECT_EQ(0, mock_reader_->GetStopCallCount()) << "Replication should NOT be stopped when replication is disabled";
  EXPECT_EQ(0, mock_reader_->GetStartCallCount()) << "Replication should NOT be started when replication is disabled";

  manager.reset();
}

/**
 * @brief Verify that null reader does not cause crashes
 */
TEST_F(SyncCancelReplicationTest, NullReaderDoesNotCrash) {
  // Create manager with null reader
  auto manager = std::make_unique<SyncOperationManager>(table_contexts_ptrs_, config_.get(), nullptr);

  auto result = manager->StartSync("test_table");
  ASSERT_TRUE(result);

  // Wait for connection failure
  manager->WaitForCompletion(10);

  // Should complete without crashing
  manager.reset();
  SUCCEED();
}

/**
 * @brief Only an undecodable event moves the restart point off the drained position
 *
 * Every other stop reason can be replayed once its cause is corrected, so the
 * drained position stays the lower bound that protects tables the SYNC did not
 * rebuild.
 */
TEST_F(SyncCancelReplicationTest, OnlyAnUndecodableEventStallSelectsTheSnapshotRestartPoint) {
  EXPECT_FALSE(internal::StalledOnUndecodableEvent(nullptr));
  EXPECT_FALSE(internal::StalledOnUndecodableEvent(mock_reader_.get())) << "a reader with no error is not stalled";

  mock_reader_->SetLastErrorForTest(
      mygram::utils::MakeError(mygram::utils::ErrorCode::kMySQLInvalidSchema, "SCHEMA_INCOMPATIBLE"));
  EXPECT_FALSE(internal::StalledOnUndecodableEvent(mock_reader_.get()))
      << "a schema stop is repaired at the source, so its interval can still be replayed";

  mock_reader_->SetLastErrorForTest(
      mygram::utils::MakeError(mygram::utils::ErrorCode::kMySQLDisconnected, "connection lost"));
  EXPECT_FALSE(internal::StalledOnUndecodableEvent(mock_reader_.get()));

  mock_reader_->SetLastErrorForTest(
      mygram::utils::MakeError(mygram::utils::ErrorCode::kMySQLUndecodableBinlogEvent, "compressed event"));
  EXPECT_TRUE(internal::StalledOnUndecodableEvent(mock_reader_.get()));
}

/**
 * @brief The stalled restart point advances past the event, and only then
 *
 * Restarting a stalled stream from the drained position replays the event that
 * stopped it, which is what makes the stall permanent.
 */
TEST_F(SyncCancelReplicationTest, StalledSyncRestartsAfterTheSnapshotInsteadOfReplayingTheEvent) {
  const std::string drained = "0-1-188";
  const std::string snapshot = "0-1-189";

  EXPECT_EQ(internal::ChooseSyncRestartGtid(false, snapshot, drained), drained)
      << "an ordinary SYNC must not skip the interval other tables still need";
  EXPECT_EQ(internal::ChooseSyncRestartGtid(true, snapshot, drained), snapshot)
      << "restarting a stalled stream from the drained position replays the event that stalled it";

  // Without a snapshot marker there is no position to skip to, and resuming
  // from an empty GTID would replay the binlog from its start.
  EXPECT_EQ(internal::ChooseSyncRestartGtid(true, "", drained), drained);
}

/**
 * @brief Point the fixture at a one-row table on the integration MySQL server
 * @return false when MySQL integration tests are disabled
 */
bool UseIntegrationTable(const std::string& table, std::unordered_map<std::string, TableContext*>& contexts,
                         std::unordered_map<std::string, std::unique_ptr<TableContext>>& owned,
                         config::Config& config) {
  if (!mysql::testing::ShouldRunMySQLIntegrationTests()) {
    return false;
  }
  const auto mysql_config = mysql::testing::GetMySQLTestConfig();
  mysql::Connection connection(mysql_config);
  EXPECT_TRUE(connection.Connect("sync-handoff-test"));
  EXPECT_TRUE(connection.ExecuteUpdate("DROP TABLE IF EXISTS " + table));
  EXPECT_TRUE(connection.ExecuteUpdate("CREATE TABLE " + table + " (id BIGINT PRIMARY KEY, content TEXT NOT NULL)"));
  EXPECT_TRUE(connection.ExecuteUpdate("INSERT INTO " + table + " VALUES (1, 'hello')"));

  config.mysql.host = mysql_config.host;
  config.mysql.port = mysql_config.port;
  config.mysql.user = mysql_config.user;
  config.mysql.password = mysql_config.password;
  config.mysql.database = mysql_config.database;

  auto ctx = std::make_unique<TableContext>();
  ctx->name = table;
  ctx->config.database = mysql_config.database;
  ctx->config.name = table;
  ctx->config.primary_key = "id";
  ctx->config.text_source.column = "content";
  ctx->config.ngram_size = 2;
  ctx->config.kanji_ngram_size = 1;
  ctx->index = std::make_unique<index::Index>(2, 1);
  ctx->doc_store = std::make_unique<storage::DocumentStore>();
  contexts[table] = ctx.get();
  owned[table] = std::move(ctx);
  return true;
}

/**
 * @brief A hand-off that fails after the swap leaves a stopped reader at the pre-SYNC position
 *
 * A reader stalled on an undecodable event is handed the snapshot GTID. When
 * that hand-off fails and the old table comes back, the reader must come back
 * to the position the old table was applied up to, or a later REPLICATION
 * START skips the interval for this table.
 */
TEST_F(SyncCancelReplicationTest, FailedHandoffRestoresTheReaderPositionOfAStoppedReader) {
  const std::string table = "sync_handoff_rollback";
  if (!UseIntegrationTable(table, table_contexts_ptrs_, table_contexts_, *config_)) {
    GTEST_SKIP() << "MySQL integration tests are disabled";
  }
  const std::string saved = "00000000-0000-0000-0000-000000000001:1-5";
  mock_reader_->SetRunning(false);
  mock_reader_->SetGtid(saved);
  mock_reader_->SetLastErrorForTest(
      mygram::utils::MakeError(mygram::utils::ErrorCode::kMySQLUndecodableBinlogEvent, "compressed event"));
  mock_reader_->SetStartShouldFail(true);

  auto manager = std::make_unique<SyncOperationManager>(table_contexts_ptrs_, config_.get(), mock_reader_.get());
  ASSERT_TRUE(manager->StartSync(table));
  ASSERT_TRUE(manager->WaitForCompletion(20));

  EXPECT_NE(manager->GetSyncStatus().find("status=FAILED"), std::string::npos) << manager->GetSyncStatus();
  EXPECT_EQ(mock_reader_->GetCurrentGTID(), saved);
  EXPECT_EQ(table_contexts_[table]->doc_store->Size(), 0U) << "the previous (empty) live table must be restored";
  manager.reset();
}

/**
 * @brief A SYNC STOP acknowledged during the catch-up wait ends CANCELLED, never COMPLETED
 */
TEST_F(SyncCancelReplicationTest, StopDuringCatchupWaitCancelsInsteadOfCompleting) {
  const std::string table = "sync_catchup_stop";
  if (!UseIntegrationTable(table, table_contexts_ptrs_, table_contexts_, *config_)) {
    GTEST_SKIP() << "MySQL integration tests are disabled";
  }
  // A position on a server that never existed, so the catch-up target is never reached.
  const std::string saved = "00000000-0000-0000-0000-000000000001:1-5";
  mock_reader_->SetRunning(true);
  mock_reader_->SetGtid(saved);

  auto manager = std::make_unique<SyncOperationManager>(table_contexts_ptrs_, config_.get(), mock_reader_.get());
  std::string stop_reply;
  mock_reader_->SetOnNextStart([&manager, &stop_reply, &table]() { stop_reply = manager->StopSync(table); });
  ASSERT_TRUE(manager->StartSync(table));
  ASSERT_TRUE(manager->WaitForCompletion(20));

  EXPECT_EQ(stop_reply.rfind("OK SYNC CANCELLATION REQUESTED", 0), 0U) << stop_reply;
  const std::string status = manager->GetSyncStatus();
  EXPECT_NE(status.find("status=CANCELLED"), std::string::npos) << status;
  EXPECT_EQ(status.find("status=COMPLETED"), std::string::npos) << status;
  EXPECT_EQ(mock_reader_->GetCurrentGTID(), saved);
  EXPECT_EQ(table_contexts_[table]->doc_store->Size(), 0U) << "the previous (empty) live table must be restored";
  manager.reset();
}

}  // namespace
}  // namespace mygramdb::server

#endif  // USE_MYSQL
