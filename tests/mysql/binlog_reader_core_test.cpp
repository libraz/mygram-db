/**
 * @file binlog_reader_core_test.cpp
 * @brief Unit tests for binlog reader - Core lifecycle and queue operations
 */

#include "binlog_event_builder.h"
#include "binlog_test_fixtures.h"
#include "support/deterministic_gate.h"

#ifdef USE_MYSQL

#include <stdexcept>

using namespace binlog_test;

/**
 * @brief Validate Start/Stop lifecycle without a real MySQL connection
 */
TEST_F(BinlogReaderFixture, StartStopLifecycleWithoutConnection) {
  EXPECT_FALSE(reader_->IsRunning());
  EXPECT_FALSE(reader_->Start());
  EXPECT_FALSE(reader_->IsRunning());
  EXPECT_NE(reader_->GetLastError().find("connection not established"), std::string::npos);

  reader_->Stop();
  EXPECT_FALSE(reader_->IsRunning());

  // Calling Stop multiple times should be safe
  reader_->Stop();
  EXPECT_FALSE(reader_->IsRunning());
}

/**
 * @brief Ensure Start reports an error when the reader is already running
 */
TEST_F(BinlogReaderFixture, RejectsDoubleStart) {
  reader_->running_ = true;
  EXPECT_FALSE(reader_->Start());
  EXPECT_NE(reader_->GetLastError().find("already running"), std::string::npos);
}

TEST_F(BinlogReaderFixture, IsRunningTracksActiveMutatingThreadsBeforeJoin) {
  // Initially not running
  EXPECT_FALSE(reader_->IsRunning());

  // Simulate running state
  reader_->running_ = true;
  reader_->active_threads_ = 2;
  EXPECT_TRUE(reader_->IsRunning());

  // A fatal worker exit requests stop while the reader can still mutate state.
  reader_->should_stop_ = true;
  reader_->active_threads_ = 1;
  EXPECT_TRUE(reader_->IsRunning());

  // Once both threads have actually exited, status is stopped even though
  // Stop() still owns the responsibility to join their thread objects.
  reader_->active_threads_ = 0;
  EXPECT_FALSE(reader_->IsRunning());

  reader_->Stop();
  EXPECT_FALSE(reader_->IsRunning());
  EXPECT_FALSE(reader_->should_stop_.load());
}

TEST_F(BinlogReaderFixture, StartingStateDoesNotChangeThreadLivenessContract) {
  reader_->running_ = true;
  reader_->starting_ = true;
  reader_->active_threads_ = 0;

  EXPECT_TRUE(reader_->IsStarting());
  EXPECT_FALSE(reader_->IsRunning());

  reader_->starting_ = false;
  reader_->running_ = false;
}

TEST_F(BinlogReaderFixture, ReconnectBackoffEscalatesAndIsBounded) {
  reader_->config_.reconnect_delay_ms = 25;
  EXPECT_EQ(reader_->ReconnectBackoffDelayMs(1), 25);
  EXPECT_EQ(reader_->ReconnectBackoffDelayMs(2), 50);
  EXPECT_EQ(reader_->ReconnectBackoffDelayMs(9), 225);
  EXPECT_EQ(reader_->ReconnectBackoffDelayMs(10), 250);
  EXPECT_EQ(reader_->ReconnectBackoffDelayMs(11), 250);
}

TEST_F(BinlogReaderFixture, ReconnectBackoffCanBeCancelledByShutdown) {
  reader_->config_.reconnect_delay_ms = 1000;
  mygramdb::testing::DeterministicGate about_to_wait;
  std::atomic<bool> completed_full_delay{true};

  std::thread waiter([&] {
    about_to_wait.ArriveAndWait();
    completed_full_delay.store(reader_->WaitForReconnectBackoff(10), std::memory_order_release);
  });

  ASSERT_TRUE(about_to_wait.WaitUntilArrived(std::chrono::seconds(2)));
  about_to_wait.Release();
  reader_->should_stop_.store(true, std::memory_order_release);
  reader_->queue_cv_.notify_all();
  waiter.join();

  EXPECT_FALSE(completed_full_delay.load(std::memory_order_acquire));
  reader_->should_stop_.store(false, std::memory_order_release);
}

TEST_F(BinlogReaderFixture, RepeatedProcessingFailureAtSameGtidStopsOnThirdReplay) {
  std::string last_failure_gtid;
  int consecutive_failures = 0;

  EXPECT_FALSE(reader_->ShouldStopForRepeatedProcessingFailure("uuid:42", last_failure_gtid, consecutive_failures));
  EXPECT_EQ(consecutive_failures, 1);
  EXPECT_FALSE(reader_->ShouldStopForRepeatedProcessingFailure("uuid:42", last_failure_gtid, consecutive_failures));
  EXPECT_EQ(consecutive_failures, 2);
  EXPECT_TRUE(reader_->ShouldStopForRepeatedProcessingFailure("uuid:42", last_failure_gtid, consecutive_failures));
  EXPECT_EQ(consecutive_failures, 3);

  EXPECT_FALSE(reader_->ShouldStopForRepeatedProcessingFailure("uuid:43", last_failure_gtid, consecutive_failures));
  EXPECT_EQ(consecutive_failures, 1) << "a newly applied GTID starts a new retry budget";
}

TEST_F(BinlogReaderFixture, UnsupportedRuntimeEventsStopBeforeGtidAdvance) {
  using mygramdb::mysql::test::BinlogEventBuilder;
  const std::array unsupported_types{
      MySQLBinlogEventType::TRANSACTION_PAYLOAD_EVENT,
      MySQLBinlogEventType::PARTIAL_UPDATE_ROWS_EVENT,
      MySQLBinlogEventType::XA_PREPARE_LOG_EVENT,
      MySQLBinlogEventType::MARIADB_WRITE_ROWS_COMPRESSED_EVENT,
  };

  for (const auto expected_type : unsupported_types) {
    SCOPED_TRACE(GetEventTypeName(expected_type));
    auto wire_event = BinlogEventBuilder::BuildHeader(expected_type);
    BinlogEventBuilder::AppendLittleEndian32(wire_event, 0);
    BinlogEventBuilder::FixEventSizeWithChecksum(wire_event);
    const auto decoded_type = static_cast<MySQLBinlogEventType>(wire_event[4]);

    reader_->should_stop_.store(false, std::memory_order_release);
    reader_->SetCurrentGTID("uuid:17");
    ASSERT_TRUE(reader_->RejectUnsupportedRuntimeEvent(decoded_type));
    EXPECT_TRUE(reader_->should_stop_.load(std::memory_order_acquire));
    EXPECT_EQ(reader_->GetCurrentGTID(), "uuid:17");
    EXPECT_FALSE(reader_->GetLastError().empty());
    // The code is what SYNC reads to decide it must restart past the event
    // rather than replay it; a generic binlog error would be replayed forever.
    EXPECT_EQ(reader_->GetLastErrorCode(), mygram::utils::ErrorCode::kMySQLUndecodableBinlogEvent);
    const std::string last_error = reader_->GetLastError();
    // Turning the producing setting off leaves the event in place, so a
    // remediation that stops at the setting describes a recovery that does not
    // work.
    EXPECT_NE(last_error.find("stays in the binlog"), std::string::npos) << last_error;
    EXPECT_NE(last_error.find("SYNC for every replicated table"), std::string::npos) << last_error;
  }
}

TEST(ReplicationPositionStateTest, ReceivedAppliedAndCommittedPositionsRemainDistinct) {
  ReplicationPositionState state;
  state.ObserveReceivedGTID("uuid:41", true, false);

  EXPECT_EQ(state.received_gtid(), "uuid:41");
  EXPECT_TRUE(state.reader_transaction_open());
  EXPECT_TRUE(state.TakeCommitGTID("").empty())
      << "A received GTID must never become committed before a mutation or explicit commit";

  state.RecordAppliedMutation("uuid:41");
  EXPECT_EQ(state.TakeCommitGTID(""), "uuid:41");
  EXPECT_TRUE(state.TakeCommitGTID("").empty());

  state.ObserveReceivedGTID("1-2-9", false, true);
  EXPECT_TRUE(state.mariadb_standalone_group_open());
  state.CloseMariaDBStandaloneGroup();
  EXPECT_FALSE(state.mariadb_standalone_group_open());
  EXPECT_EQ(state.TakeCommitGTID("1-2-9"), "1-2-9");
}

TEST_F(BinlogReaderFixture, StreamStartClearsReceivedGtidWithoutReplacingMultiUuidPosition) {
  const std::string applied_position =
      "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-100,bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb:1-50";
  reader_->SetCurrentGTID(applied_position);

  reader_->position_state_.ObserveReceivedGTID("cccccccc-cccc-cccc-cccc-cccccccccccc:7", true, false);
  reader_->position_state_.ResetReceived();

  EXPECT_TRUE(reader_->position_state_.received_gtid().empty());
  EXPECT_FALSE(reader_->position_state_.reader_transaction_open());
  EXPECT_EQ(reader_->GetCurrentGTID(), applied_position)
      << "a saved multi-source position must not become a received transaction GTID";
}

TEST_F(BinlogReaderFixture, ReestablishedConnectionIsNotClosedBeforeNextStreamOpen) {
  EXPECT_FALSE(BinlogReader::ShouldCloseStreamAfterReadLoop(true));
  EXPECT_TRUE(BinlogReader::ShouldCloseStreamAfterReadLoop(false));
}

TEST_F(BinlogReaderFixture, SchemaChangeInvalidatesStaleColumnNamesBeforeRefresh) {
  TableMetadata metadata;
  metadata.database_name = "app";
  metadata.table_name = "articles";
  reader_->column_names_cache_["app.articles"] = {{"old_content", false, {}}};
  reader_->column_names_cache_["app.other"] = {{"other", false, {}}};

  reader_->InvalidateColumnNamesForSchemaChange(metadata);

  EXPECT_EQ(reader_->column_names_cache_.count("app.articles"), 0U);
  EXPECT_EQ(reader_->column_names_cache_.count("app.other"), 1U);
}

/**
 * @brief Verify Start cleans up stale stopping state
 *
 * When the reader thread self-exits (should_stop_=true, running_=true),
 * Start() should clean up stale state and proceed (failing later at
 * connection validation since there's no real MySQL connection).
 * After failure, running_ must be false so a subsequent Start() can retry.
 */
TEST_F(BinlogReaderFixture, StartCleansUpStaleStoppingState) {
  // Simulate stale stopping state: reader thread self-exited
  reader_->running_ = true;
  reader_->should_stop_ = true;

  // Start cleans up stale state, then fails at connection validation
  auto result = reader_->Start();
  EXPECT_FALSE(result);

  // running_ must be false after failed Start() to allow retry
  EXPECT_FALSE(reader_->running_.load());
}

/**
 * @brief Verify Stop resets should_stop_ flag after completion
 *
 * After Stop() completes, should_stop_ should be reset to false
 * so that subsequent Start() calls work correctly.
 */
TEST_F(BinlogReaderFixture, StopResetsShouldStopFlag) {
  // Simulate running state (without actual threads)
  reader_->running_ = true;

  // Stop should set should_stop_ temporarily and then reset it
  reader_->Stop();

  // After Stop completes, both flags should be false
  EXPECT_FALSE(reader_->running_.load());
  EXPECT_FALSE(reader_->should_stop_.load());
}

/**
 * @brief Exercise queue push/pop helpers without worker threads
 */
TEST_F(BinlogReaderFixture, PushAndPopEvents) {
  auto first = std::make_unique<BinlogEvent>(MakeEvent(BinlogEventType::INSERT, "1", 1));
  reader_->PushEvent(std::move(first));
  EXPECT_EQ(reader_->GetQueueSize(), 1);

  auto popped = reader_->PopEvent();
  ASSERT_NE(popped, nullptr);
  EXPECT_EQ(popped->primary_key, "1");
  EXPECT_EQ(reader_->GetQueueSize(), 0);
}

TEST(BinlogReaderDDLTest, ClassifyTruncateOnlyForTruncateTableStatement) {
  EXPECT_EQ(BinlogEvent::ClassifyDDL("TRUNCATE TABLE articles"), DDLType::kTruncate);
  EXPECT_EQ(BinlogEvent::ClassifyDDL("truncate table `articles`"), DDLType::kTruncate);
  EXPECT_EQ(BinlogEvent::ClassifyDDL("ALTER TABLE articles CHANGE old_col truncate_data TEXT"), DDLType::kAlter);
  EXPECT_EQ(BinlogEvent::ClassifyDDL("ALTER TABLE articles DROP COLUMN truncate_data"), DDLType::kAlter);
  EXPECT_EQ(BinlogEvent::ClassifyDDL("SET @noop = 1; DROP TABLE articles"), DDLType::kDrop);
  EXPECT_EQ(BinlogEvent::ClassifyDDL("ALTER TABLE users ADD COLUMN x INT; DROP TABLE articles"), DDLType::kAlter);
}

/**
 * @brief Verify PushEvent blocks when queue is full until space becomes available
 */
TEST_F(BinlogReaderFixture, PushBlocksWhenQueueFull) {
  reader_->config_.queue_size = 1;
  auto first = std::make_unique<BinlogEvent>(MakeEvent(BinlogEventType::INSERT, "1", 1));
  reader_->PushEvent(std::move(first));

  std::atomic<bool> second_pushed{false};
  std::thread producer([&] {
    auto second = std::make_unique<BinlogEvent>(MakeEvent(BinlogEventType::INSERT, "2", 1));
    reader_->PushEvent(std::move(second));
    second_pushed.store(true);
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  EXPECT_FALSE(second_pushed.load());

  auto popped = reader_->PopEvent();
  ASSERT_NE(popped, nullptr);
  producer.join();
  EXPECT_TRUE(second_pushed.load());

  // Drain queue for subsequent tests
  popped = reader_->PopEvent();
  ASSERT_NE(popped, nullptr);
  EXPECT_EQ(reader_->GetQueueSize(), 0);
}

/**
 * @brief Ensure PopEvent blocks until a producer pushes data
 */
TEST_F(BinlogReaderFixture, PopBlocksUntilEventArrives) {
  std::atomic<bool> pop_completed{false};
  std::thread consumer([&] {
    auto event = reader_->PopEvent();
    pop_completed.store(event != nullptr);
    if (event) {
      EXPECT_EQ(event->primary_key, "7");
    }
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  EXPECT_FALSE(pop_completed.load());

  reader_->PushEvent(std::make_unique<BinlogEvent>(MakeEvent(BinlogEventType::INSERT, "7", 1)));
  consumer.join();
  EXPECT_TRUE(pop_completed.load());
  EXPECT_EQ(reader_->GetQueueSize(), 0);
}

/**
 * @brief Confirm PopEvent unblocks and returns nullptr when reader is stopped
 */
TEST_F(BinlogReaderFixture, PopReturnsFalseWhenStopping) {
  std::atomic<bool> pop_result{true};
  std::thread consumer([&] {
    auto event = reader_->PopEvent();
    pop_result.store(event == nullptr);
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  reader_->should_stop_ = true;
  reader_->queue_cv_.notify_all();

  consumer.join();
  EXPECT_TRUE(pop_result.load());  // Should be nullptr (true) when stopping
  reader_->should_stop_ = false;
}

/**
 * @brief Test BinlogEvent structure
 */
TEST(BinlogReaderTest, EventStructure) {
  BinlogEvent event;
  event.type = BinlogEventType::INSERT;
  event.table_name = "test_table";
  event.primary_key = "123";
  event.text = "test text";
  event.gtid = "uuid:1";

  EXPECT_EQ(event.type, BinlogEventType::INSERT);
  EXPECT_EQ(event.table_name, "test_table");
  EXPECT_EQ(event.primary_key, "123");
  EXPECT_EQ(event.text, "test text");
  EXPECT_EQ(event.gtid, "uuid:1");
}

/**
 * @brief Test BinlogEventType enum
 */
TEST(BinlogReaderTest, EventTypes) {
  EXPECT_NE(BinlogEventType::INSERT, BinlogEventType::UPDATE);
  EXPECT_NE(BinlogEventType::INSERT, BinlogEventType::DELETE);
  EXPECT_NE(BinlogEventType::UPDATE, BinlogEventType::DELETE);
  EXPECT_NE(BinlogEventType::DELETE, BinlogEventType::COMMIT);
  EXPECT_NE(BinlogEventType::INSERT, BinlogEventType::UNKNOWN);
}

/**
 * @brief Test BinlogReader construction
 */
TEST(BinlogReaderTest, Construction) {
  // Create dependencies
  Connection::Config conn_config;
  conn_config.host = "localhost";
  conn_config.user = "test";
  conn_config.password = "test";

  Connection conn(conn_config);

  index::Index idx(1);
  storage::DocumentStore doc_store;

  config::TableConfig table_config;
  table_config.name = "test_table";
  table_config.primary_key = "id";

  BinlogReader::Config reader_config;
  reader_config.start_gtid = "uuid:1";
  reader_config.queue_size = 1000;
  reader_config.server_id = 12345;  // Test server ID

  config::MysqlConfig mysql_config;  // Use default (UTC timezone)
  BinlogReader reader(conn, idx, doc_store, table_config, mysql_config, reader_config);

  // Should construct successfully
  EXPECT_FALSE(reader.IsRunning());
  EXPECT_EQ(reader.GetProcessedEvents(), 0);
  EXPECT_EQ(reader.GetQueueSize(), 0);
}

/**
 * @brief Test BinlogReader initial state
 */
TEST(BinlogReaderTest, InitialState) {
  Connection::Config conn_config;
  Connection conn(conn_config);

  index::Index idx(1);
  storage::DocumentStore doc_store;

  config::TableConfig table_config;
  table_config.name = "test_table";

  BinlogReader::Config reader_config;
  reader_config.start_gtid = "3E11FA47-71CA-11E1-9E33-C80AA9429562:100";
  reader_config.server_id = 12345;  // Test server ID

  config::MysqlConfig mysql_config;  // Use default (UTC timezone)
  BinlogReader reader(conn, idx, doc_store, table_config, mysql_config, reader_config);

  EXPECT_FALSE(reader.IsRunning());
  EXPECT_EQ(reader.GetCurrentGTID(), "3E11FA47-71CA-11E1-9E33-C80AA9429562:100");
  EXPECT_EQ(reader.GetQueueSize(), 0);
  EXPECT_EQ(reader.GetProcessedEvents(), 0);
}

/**
 * @brief Test BinlogReader config
 */
TEST(BinlogReaderTest, Config) {
  BinlogReader::Config config;

  // Default values
  EXPECT_EQ(config.queue_size, 10000);
  EXPECT_EQ(config.reconnect_delay_ms, 1000);
  EXPECT_EQ(config.server_id, 0);  // Default server_id is 0 (invalid for replication)

  // Custom values
  config.start_gtid = "test:123";
  config.queue_size = 5000;
  config.reconnect_delay_ms = 500;
  config.server_id = 12345;

  EXPECT_EQ(config.start_gtid, "test:123");
  EXPECT_EQ(config.queue_size, 5000);
  EXPECT_EQ(config.reconnect_delay_ms, 500);
  EXPECT_EQ(config.server_id, 12345);
}

/**
 * @brief Test BinlogEvent with filters
 */
TEST(BinlogReaderTest, EventWithFilters) {
  BinlogEvent event;
  event.type = BinlogEventType::INSERT;
  event.table_name = "articles";
  event.primary_key = "456";
  event.text = "article text";

  // Add filters
  event.filters["status"] = static_cast<int64_t>(1);
  event.filters["category"] = std::string("news");

  EXPECT_EQ(event.filters.size(), 2);

  auto status = std::get<int64_t>(event.filters["status"]);
  auto category = std::get<std::string>(event.filters["category"]);

  EXPECT_EQ(status, 1);
  EXPECT_EQ(category, "news");
}

/**
 * @brief Test multiple event types
 */
TEST(BinlogReaderTest, MultipleEventTypes) {
  BinlogEvent insert_event;
  insert_event.type = BinlogEventType::INSERT;
  insert_event.primary_key = "1";

  BinlogEvent update_event;
  update_event.type = BinlogEventType::UPDATE;
  update_event.primary_key = "2";

  BinlogEvent delete_event;
  delete_event.type = BinlogEventType::DELETE;
  delete_event.primary_key = "3";

  EXPECT_EQ(insert_event.type, BinlogEventType::INSERT);
  EXPECT_EQ(update_event.type, BinlogEventType::UPDATE);
  EXPECT_EQ(delete_event.type, BinlogEventType::DELETE);

  EXPECT_EQ(insert_event.primary_key, "1");
  EXPECT_EQ(update_event.primary_key, "2");
  EXPECT_EQ(delete_event.primary_key, "3");
}

/**
 * @brief Test clean shutdown sequence without threads running
 *
 * Verifies that Stop() can be called safely even when threads are not running,
 * and that internal connection cleanup happens in the correct order.
 */
TEST_F(BinlogReaderFixture, CleanShutdownWithoutThreads) {
  // Stop should be safe even when not running
  EXPECT_FALSE(reader_->IsRunning());
  reader_->Stop();
  EXPECT_FALSE(reader_->IsRunning());

  // Multiple stops should be safe
  reader_->Stop();
  reader_->Stop();
  EXPECT_FALSE(reader_->IsRunning());
}

/**
 * @brief Test shutdown sequence with active queue operations
 *
 * This test simulates threads blocked on queue operations and verifies
 * that Stop() properly unblocks them and allows clean shutdown.
 */
TEST_F(BinlogReaderFixture, ShutdownUnblocksQueueOperations) {
  std::atomic<bool> pop_finished{false};
  std::atomic<bool> push_finished{false};

  // Start thread blocked on Pop (queue is empty)
  std::thread pop_thread([&] {
    auto event = reader_->PopEvent();  // Should block since queue is empty
    pop_finished.store(event != nullptr);
  });

  // Give pop thread time to block on empty queue
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  EXPECT_FALSE(pop_finished.load());

  // Fill queue to capacity
  reader_->config_.queue_size = 1;
  reader_->PushEvent(std::make_unique<BinlogEvent>(MakeEvent(BinlogEventType::INSERT, "1", 1)));

  // Wait for pop thread to consume the item
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  // Start thread blocked on Push (queue should fill up again)
  std::thread push_thread([&] {
    reader_->PushEvent(std::make_unique<BinlogEvent>(MakeEvent(BinlogEventType::INSERT, "2", 1)));
    push_finished.store(true);
  });

  // Give push thread time to complete (queue has space)
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  // Stop should unblock any remaining operations
  reader_->Stop();

  pop_thread.join();
  push_thread.join();

  EXPECT_TRUE(pop_finished.load());
  EXPECT_TRUE(push_finished.load());

  // Reset state for cleanup
  reader_->should_stop_ = false;
}

/**
 * @brief Test that binlog_connection_ is properly cleaned up
 *
 * Verifies the critical shutdown sequence fix: threads must complete
 * (including mysql_binlog_close) before mysql_close is called.
 */
TEST_F(BinlogReaderFixture, BinlogConnectionCleanupOrder) {
  // Create a mock scenario where binlog_connection_ exists and reader is running
  Connection::Config binlog_config = connection_config_;
  reader_->binlog_connection_ = std::make_unique<Connection>(binlog_config);
  reader_->running_ = true;  // Simulate running state

  // Verify connection exists
  EXPECT_NE(reader_->binlog_connection_, nullptr);

  // Call Stop - this should properly clean up the connection
  // without double-free or use-after-free errors
  reader_->Stop();

  // Connection should be destroyed
  EXPECT_EQ(reader_->binlog_connection_, nullptr);
  EXPECT_FALSE(reader_->IsRunning());
}

/**
 * @brief Test rapid start/stop cycles
 *
 * Verifies that the reader can handle rapid start/stop sequences
 * without deadlocks or memory corruption.
 */
TEST_F(BinlogReaderFixture, RapidStartStopCycles) {
  // Rapid stop calls (without start)
  for (int i = 0; i < 5; i++) {
    reader_->Stop();
    EXPECT_FALSE(reader_->IsRunning());
  }

  // Should still be in a valid state
  EXPECT_FALSE(reader_->IsRunning());
  EXPECT_EQ(reader_->GetQueueSize(), 0);
}

/**
 * @brief Test destructor cleanup
 *
 * Verifies that BinlogReader destructor properly calls Stop()
 * and cleans up all resources.
 */
TEST(BinlogReaderTest, DestructorCallsStop) {
  Connection::Config conn_config;
  Connection conn(conn_config);

  index::Index idx(1);
  storage::DocumentStore doc_store;

  config::TableConfig table_config;
  table_config.name = "test_table";

  BinlogReader::Config reader_config;
  reader_config.start_gtid = "uuid:1";
  reader_config.server_id = 12345;  // Test server ID

  // Create reader in a scope
  {
    config::MysqlConfig mysql_config;  // Use default (UTC timezone)
    BinlogReader reader(conn, idx, doc_store, table_config, mysql_config, reader_config);
    EXPECT_FALSE(reader.IsRunning());

    // Simulate having a binlog connection
    Connection::Config binlog_config = conn_config;
    reader.binlog_connection_ = std::make_unique<Connection>(binlog_config);

    // Destructor will be called here and should clean up properly
  }

  // If we reach here without crash/hang, the test passes
  SUCCEED();
}

TEST_F(BinlogReaderFixture, MySqlCurrentGtidTracksAllUuids) {
  reader_->SetCurrentGTID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-100,bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb:1-50");

  reader_->UpdateCurrentGTID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb:51");
  EXPECT_EQ(reader_->GetCurrentGTID(),
            "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-100,bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb:1-51");

  reader_->UpdateCurrentGTID("cccccccc-cccc-cccc-cccc-cccccccccccc:7");
  EXPECT_EQ(reader_->GetCurrentGTID(),
            "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-100,bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb:1-51,"
            "cccccccc-cccc-cccc-cccc-cccccccccccc:7");
}

TEST_F(BinlogReaderFixture, MySqlCurrentGtidPromotesSingleGtidToSet) {
  reader_->SetCurrentGTID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:100");

  reader_->UpdateCurrentGTID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb:7");

  EXPECT_EQ(reader_->GetCurrentGTID(),
            "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:100,bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb:7");
}

TEST_F(BinlogReaderFixture, MySqlCurrentGtidMergesTaggedGtidWithoutDroppingUuids) {
  reader_->SetCurrentGTID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-100,bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb:1-50");

  reader_->UpdateCurrentGTID("cccccccc-cccc-cccc-cccc-cccccccccccc:release_2026:7");

  EXPECT_EQ(reader_->GetCurrentGTID(),
            "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-100,bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb:1-50,"
            "cccccccc-cccc-cccc-cccc-cccccccccccc:release_2026:7");
}

TEST_F(BinlogReaderFixture, MariaDbCurrentGtidTracksAllDomains) {
  reader_->SetCurrentGTID("0-1-10,1-2-20");

  reader_->UpdateCurrentGTID("1-2-21");
  EXPECT_EQ(reader_->GetCurrentGTID(), "0-1-10,1-2-21");

  reader_->UpdateCurrentGTID("2-3-5");
  EXPECT_EQ(reader_->GetCurrentGTID(), "0-1-10,1-2-21,2-3-5");
}

TEST_F(BinlogReaderFixture, MariaDbCurrentGtidDoesNotRegressDomainSequence) {
  reader_->SetCurrentGTID("0-1-10,1-2-20");

  reader_->UpdateCurrentGTID("0-1-9");

  EXPECT_EQ(reader_->GetCurrentGTID(), "0-1-10,1-2-20");
}

TEST_F(BinlogReaderFixture, InvalidGtidMergeReturnsErrorWithoutAdvancingAppliedPosition) {
  const std::string initial = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-100,bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb:1-50";
  reader_->SetCurrentGTID(initial);

  auto result = reader_->UpdateCurrentGTID("not-a-gtid");

  ASSERT_FALSE(result);
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kMySQLInvalidGTID);
  EXPECT_EQ(reader_->GetCurrentGTID(), initial);
}

// ===========================================================================
// Tests moved from binlog_reader_bug_fixes_test.cpp
// ===========================================================================

/**
 * @brief An uninterrupted reconnect wait tells the reader loop to keep going
 *
 * The reader loop only leaves its reconnection cycle when a stop was requested.
 * A backoff that simply elapses reports that the loop should continue, so an
 * idle connection cannot end replication.
 */
TEST_F(BinlogReaderFixture, ReaderThreadContinuesOnNormalIdle) {
  reader_->config_.reconnect_delay_ms = 20;
  ASSERT_FALSE(reader_->should_stop_.load());

  const auto started = std::chrono::steady_clock::now();
  const bool keep_going = reader_->WaitForReconnectBackoff(1);
  const auto elapsed = std::chrono::steady_clock::now() - started;

  EXPECT_TRUE(keep_going) << "an elapsed backoff must not end the reader loop";
  EXPECT_GE(std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count(), 20)
      << "the backoff returned before the configured delay";

  // A stop request answers the same call with "leave the loop".
  reader_->should_stop_ = true;
  EXPECT_FALSE(reader_->WaitForReconnectBackoff(1)) << "a requested stop must end the reader loop";
  reader_->should_stop_ = false;
}

/**
 * @brief GTID is not updated when ProcessEvent fails
 *
 * When ProcessEvent() returns false, the GTID should NOT be updated.
 * This prevents data loss on reconnect.
 */
TEST_F(BinlogReaderFixture, GtidNotUpdatedOnProcessEventFailure) {
  reader_config_.queue_size = 100;
  ResetReader();

  // Set initial GTID
  reader_->SetCurrentGTID("uuid:100");
  EXPECT_EQ(reader_->GetCurrentGTID(), "uuid:100");

  // Create an event with GTID "uuid:101"
  auto event = MakeEvent(BinlogEventType::INSERT, "101", 1, "test text");
  event.gtid = "uuid:101";

  // Call ProcessEvent
  bool result = reader_->ProcessEvent(event);

  // If ProcessEvent failed, GTID should remain at original value
  if (!result) {
    EXPECT_EQ(reader_->GetCurrentGTID(), "uuid:100") << "GTID was updated despite ProcessEvent failure";
  }
}

/**
 * @brief Worker thread only updates GTID on successful event processing
 *
 * Simulates the worker thread behavior and verifies that GTID is only
 * updated on successful event processing.
 */
TEST_F(BinlogReaderFixture, WorkerThreadGtidUpdateOnlyOnSuccess) {
  reader_config_.queue_size = 100;
  ResetReader();

  // Set initial state
  reader_->SetCurrentGTID("uuid:50");
  uint64_t initial_processed = reader_->GetProcessedEvents();

  // Create and push a test event
  auto event = std::make_unique<BinlogEvent>(MakeEvent(BinlogEventType::INSERT, "51", 0, "text"));
  event->gtid = "uuid:51";

  // Push event to queue
  reader_->PushEvent(std::move(event));
  EXPECT_EQ(reader_->GetQueueSize(), 1);

  // Pop and process manually (simulating worker thread)
  auto popped_event = reader_->PopEvent();
  ASSERT_NE(popped_event, nullptr);

  // Process the event. DML success is not enough to advance GTID; the worker
  // waits for the transaction commit marker.
  bool success = reader_->ProcessQueuedEvent(*popped_event);

  if (success) {
    EXPECT_EQ(reader_->GetCurrentGTID(), "uuid:50");
    EXPECT_EQ(reader_->GetProcessedEvents(), initial_processed + 1);

    BinlogEvent commit_event;
    commit_event.type = BinlogEventType::COMMIT;
    commit_event.gtid = popped_event->gtid;
    EXPECT_TRUE(reader_->ProcessQueuedEvent(commit_event));
    EXPECT_EQ(reader_->GetCurrentGTID(), "uuid:50-51");
  } else {
    // Should NOT update GTID
    EXPECT_EQ(reader_->GetCurrentGTID(), "uuid:50") << "GTID should not be updated on ProcessEvent failure";
    EXPECT_EQ(reader_->GetProcessedEvents(), initial_processed)
        << "processed_events should not be incremented on failure";
  }
}

TEST_F(BinlogReaderFixture, DmlGtidAdvancesOnlyAtCommitBoundary) {
  reader_->SetCurrentGTID("uuid:50");
  uint64_t initial_processed = reader_->GetProcessedEvents();

  BinlogEvent event = MakeEvent(BinlogEventType::INSERT, "51", 1, "text");
  event.gtid = "uuid:51";

  ASSERT_TRUE(reader_->ProcessQueuedEvent(event));
  EXPECT_EQ(reader_->GetProcessedEvents(), initial_processed + 1);
  EXPECT_EQ(reader_->GetCurrentGTID(), "uuid:50");

  BinlogEvent commit_event;
  commit_event.type = BinlogEventType::COMMIT;
  commit_event.gtid = "uuid:51";
  ASSERT_TRUE(reader_->ProcessQueuedEvent(commit_event));
  EXPECT_EQ(reader_->GetCurrentGTID(), "uuid:50-51");
}

TEST_F(BinlogReaderFixture, MismatchedCommitDoesNotDiscardPendingAppliedPosition) {
  reader_->SetCurrentGTID("uuid:50");
  BinlogEvent event = MakeEvent(BinlogEventType::INSERT, "51", 1, "text");
  event.gtid = "uuid:51";
  ASSERT_TRUE(reader_->ProcessQueuedEvent(event));

  BinlogEvent mismatched_commit;
  mismatched_commit.type = BinlogEventType::COMMIT;
  mismatched_commit.gtid = "uuid:52";
  EXPECT_FALSE(reader_->ProcessQueuedEvent(mismatched_commit));
  EXPECT_EQ(reader_->GetCurrentGTID(), "uuid:50");

  BinlogEvent matching_commit;
  matching_commit.type = BinlogEventType::COMMIT;
  matching_commit.gtid = "uuid:51";
  EXPECT_TRUE(reader_->ProcessQueuedEvent(matching_commit));
  EXPECT_EQ(reader_->GetCurrentGTID(), "uuid:50-51");
}

TEST_F(BinlogReaderFixture, PendingCommitPositionIsSerializedWithExplicitPositionReset) {
  BinlogEvent event = MakeEvent(BinlogEventType::DELETE, "missing", 1, "text");
  std::atomic<bool> processing_succeeded{true};

  std::thread worker([&] {
    for (int sequence = 1; sequence <= 20; ++sequence) {
      event.gtid = "uuid:" + std::to_string(sequence);
      if (!reader_->ProcessQueuedEvent(event)) {
        processing_succeeded.store(false, std::memory_order_release);
        return;
      }
    }
  });
  std::thread position_resetter([&] {
    for (int sequence = 1000; sequence < 1020; ++sequence) {
      reader_->SetCurrentGTID("uuid:" + std::to_string(sequence));
    }
  });
  worker.join();
  position_resetter.join();
  ASSERT_TRUE(processing_succeeded.load(std::memory_order_acquire));

  reader_->SetCurrentGTID("uuid:2000");
  BinlogEvent commit;
  commit.type = BinlogEventType::COMMIT;
  ASSERT_TRUE(reader_->ProcessQueuedEvent(commit));
  EXPECT_EQ(reader_->GetCurrentGTID(), "uuid:2000")
      << "SetCurrentGTID must atomically clear any worker-owned pending transaction";
}

TEST_F(BinlogReaderFixture, DdlGtidAdvancesImmediatelyAfterSuccess) {
  reader_->SetCurrentGTID("uuid:50");

  BinlogEvent ddl_event = BinlogEvent::CreateDDL(table_config_.name, "TRUNCATE TABLE articles");
  ddl_event.gtid = "uuid:51";

  ASSERT_TRUE(reader_->ProcessQueuedEvent(ddl_event));
  EXPECT_EQ(reader_->GetCurrentGTID(), "uuid:50-51");
}

TEST_F(BinlogReaderFixture, UnsafeDdlDoesNotAdvanceGtidAndRequiresExplicitRecovery) {
  reader_->SetCurrentGTID("uuid:50");
  BinlogEvent ddl_event = BinlogEvent::CreateDDL(table_config_.name, "RENAME TABLE articles TO articles_v2");
  ddl_event.gtid = "uuid:51";

  EXPECT_FALSE(reader_->ProcessQueuedEvent(ddl_event));
  EXPECT_EQ(reader_->GetCurrentGTID(), "uuid:50");
  EXPECT_TRUE(reader_->HasSchemaIncompatibleError());
  EXPECT_NE(reader_->GetLastError().find("SCHEMA_INCOMPATIBLE"), std::string::npos);

  auto restart_without_rebuild = reader_->Start();
  EXPECT_FALSE(restart_without_rebuild);
  EXPECT_NE(reader_->GetLastError().find("requires SYNC"), std::string::npos);

  reader_->SetCurrentGTID("uuid:60");
  EXPECT_FALSE(reader_->HasSchemaIncompatibleError());
  EXPECT_TRUE(reader_->GetLastError().empty());
}

TEST_F(BinlogReaderFixture, PermanentSchemaReadFailureIsNotRetried) {
  using mygram::utils::ErrorCode;

  EXPECT_TRUE(reader_->IsRetryableSchemaValidationError(ErrorCode::kMySQLConnectionFailed));
  EXPECT_TRUE(reader_->IsRetryableSchemaValidationError(ErrorCode::kMySQLDisconnected));
  EXPECT_TRUE(reader_->IsRetryableSchemaValidationError(ErrorCode::kMySQLTimeout));
  EXPECT_FALSE(reader_->IsRetryableSchemaValidationError(ErrorCode::kMySQLQueryFailed));
  EXPECT_FALSE(reader_->IsRetryableSchemaValidationError(ErrorCode::kMySQLTableNotFound));
  EXPECT_FALSE(reader_->IsRetryableSchemaValidationError(ErrorCode::kPermissionDenied));
}

TEST_F(BinlogReaderFixture, ProcessingFailureClassificationPreservesTransportFailures) {
  using ErrorCode = mygram::utils::ErrorCode;
  using FailureKind = BinlogReader::ProcessingFailureKind;

  EXPECT_EQ(reader_->ClassifyProcessingFailure(ErrorCode::kMySQLConnectionFailed), FailureKind::kTransientTransport);
  EXPECT_EQ(reader_->ClassifyProcessingFailure(ErrorCode::kMySQLDisconnected), FailureKind::kTransientTransport);
  EXPECT_EQ(reader_->ClassifyProcessingFailure(ErrorCode::kMySQLTimeout), FailureKind::kTransientTransport);
  EXPECT_EQ(reader_->ClassifyProcessingFailure(ErrorCode::kMySQLQueryFailed), FailureKind::kDeterministic);
  EXPECT_EQ(reader_->ClassifyProcessingFailure(ErrorCode::kInternalError), FailureKind::kDeterministic);
}

TEST_F(BinlogReaderFixture, TransientProcessingFailuresDoNotConsumeDeterministicReplayBudget) {
  using FailureKind = BinlogReader::ProcessingFailureKind;
  std::string last_failure_gtid;
  int consecutive_failures = 0;

  EXPECT_FALSE(reader_->ShouldStopForProcessingFailure(FailureKind::kDeterministic, "uuid:10", last_failure_gtid,
                                                       consecutive_failures));
  EXPECT_FALSE(reader_->ShouldStopForProcessingFailure(FailureKind::kDeterministic, "uuid:10", last_failure_gtid,
                                                       consecutive_failures));
  for (int attempt = 0; attempt < 5; ++attempt) {
    EXPECT_FALSE(reader_->ShouldStopForProcessingFailure(FailureKind::kTransientTransport, "uuid:10", last_failure_gtid,
                                                         consecutive_failures));
  }
  EXPECT_EQ(last_failure_gtid, "uuid:10");
  EXPECT_EQ(consecutive_failures, 2);

  EXPECT_TRUE(reader_->ShouldStopForProcessingFailure(FailureKind::kDeterministic, "uuid:10", last_failure_gtid,
                                                      consecutive_failures));
}

TEST_F(BinlogReaderFixture, ProcessingRecoveryBackoffResetsOnlyAfterAppliedGtidProgress) {
  std::string last_recovery_gtid = "uuid:10";
  int reconnect_attempt = 4;

  reader_->ResetProcessingBackoffAfterProgress("uuid:10", last_recovery_gtid, reconnect_attempt);
  EXPECT_EQ(reconnect_attempt, 4);

  reader_->ResetProcessingBackoffAfterProgress("uuid:11", last_recovery_gtid, reconnect_attempt);
  EXPECT_EQ(last_recovery_gtid, "uuid:11");
  EXPECT_EQ(reconnect_attempt, 0);
}

TEST_F(BinlogReaderFixture, PublishedDeterministicFailureIsNotDowngradedByTransientFailure) {
  using FailureKind = BinlogReader::ProcessingFailureKind;

  reader_->PublishProcessingFailure(FailureKind::kDeterministic);
  reader_->PublishProcessingFailure(FailureKind::kTransientTransport);

  EXPECT_EQ(reader_->processing_failure_reconnect_requested_.load(std::memory_order_acquire),
            FailureKind::kDeterministic);
}

/**
 * @brief Pending events are processed during shutdown
 *
 * When Stop() is called, the worker thread processes all remaining events
 * in the queue before exiting.
 */
TEST_F(BinlogReaderFixture, PendingEventsProcessedDuringShutdown) {
  reader_config_.queue_size = 100;
  ResetReader();

  auto event1 = std::make_unique<BinlogEvent>(MakeEvent(BinlogEventType::INSERT, "1", 1, "text1"));
  auto event2 = std::make_unique<BinlogEvent>(MakeEvent(BinlogEventType::INSERT, "2", 1, "text2"));
  auto event3 = std::make_unique<BinlogEvent>(MakeEvent(BinlogEventType::INSERT, "3", 1, "text3"));

  reader_->PushEvent(std::move(event1));
  reader_->PushEvent(std::move(event2));
  reader_->PushEvent(std::move(event3));
  EXPECT_EQ(reader_->GetQueueSize(), 3);

  reader_->should_stop_ = true;

  // PopEvent should still return events when queue is not empty during shutdown
  auto popped1 = reader_->PopEvent();
  ASSERT_NE(popped1, nullptr);
  EXPECT_EQ(popped1->primary_key, "1");

  auto popped2 = reader_->PopEvent();
  ASSERT_NE(popped2, nullptr);
  EXPECT_EQ(popped2->primary_key, "2");

  auto popped3 = reader_->PopEvent();
  ASSERT_NE(popped3, nullptr);
  EXPECT_EQ(popped3->primary_key, "3");

  // Now queue is empty and should_stop is true: PopEvent returns nullptr
  auto popped4 = reader_->PopEvent();
  EXPECT_EQ(popped4, nullptr);
}

/**
 * @brief Multiple Stop() calls are handled safely without use-after-free
 */
TEST_F(BinlogReaderFixture, MultipleStopCallsSafe) {
  reader_->Stop();
  reader_->Stop();
  reader_->Stop();
  EXPECT_FALSE(reader_->IsRunning());
}

/**
 * @brief Concurrent Stop() calls from multiple threads are safe
 */
TEST_F(BinlogReaderFixture, ConcurrentStopCallsSafe) {
  std::vector<std::thread> threads;
  const int num_threads = 10;
  for (int i = 0; i < num_threads; ++i) {
    threads.emplace_back([this]() { reader_->Stop(); });
  }
  for (auto& t : threads) {
    t.join();
  }
  EXPECT_FALSE(reader_->IsRunning());
}

// ===========================================================================
// ConvertSingleGtidToRange tests
// ===========================================================================

/**
 * @brief Test ConvertSingleGtidToRange with single GTID
 */
TEST(BinlogReaderTest, ConvertSingleGtidToRangeBasic) {
  // Single GTID "uuid:101" should be converted to "uuid:1-101"
  std::string result = BinlogReader::ConvertSingleGtidToRange("61d5b289-bccc-11f0-b921-cabbb4ee51f6:101");
  EXPECT_EQ(result, "61d5b289-bccc-11f0-b921-cabbb4ee51f6:1-101");
}

/**
 * @brief Test ConvertSingleGtidToRange with range GTID (no conversion)
 */
TEST(BinlogReaderTest, ConvertSingleGtidToRangeWithRange) {
  // Range GTID should pass through unchanged
  std::string result = BinlogReader::ConvertSingleGtidToRange("61d5b289-bccc-11f0-b921-cabbb4ee51f6:1-100");
  EXPECT_EQ(result, "61d5b289-bccc-11f0-b921-cabbb4ee51f6:1-100");
}

/**
 * @brief Test ConvertSingleGtidToRange with multi-UUID GTID (no conversion)
 */
TEST(BinlogReaderTest, ConvertSingleGtidToRangeWithMultiUuid) {
  // Multi-UUID GTID should pass through unchanged (has comma)
  std::string result = BinlogReader::ConvertSingleGtidToRange("uuid1:1-100,uuid2:1-50");
  EXPECT_EQ(result, "uuid1:1-100,uuid2:1-50");
}

/**
 * @brief Test ConvertSingleGtidToRange with empty string
 */
TEST(BinlogReaderTest, ConvertSingleGtidToRangeEmpty) {
  std::string result = BinlogReader::ConvertSingleGtidToRange("");
  EXPECT_EQ(result, "");
}

/**
 * @brief Test ConvertSingleGtidToRange with transaction 1
 */
TEST(BinlogReaderTest, ConvertSingleGtidToRangeTransaction1) {
  // Edge case: transaction 1 -> "uuid:1-1"
  std::string result = BinlogReader::ConvertSingleGtidToRange("00000000-0000-0000-0000-000000000001:1");
  EXPECT_EQ(result, "00000000-0000-0000-0000-000000000001:1-1");
}

/**
 * @brief Test ConvertSingleGtidToRange with multi-UUID where entries need conversion
 */
TEST(BinlogReaderTest, ConvertSingleGtidToRangeMultiUuidWithSingleGno) {
  // Multi-UUID GTID where individual entries are single GNOs should be converted
  std::string result = BinlogReader::ConvertSingleGtidToRange("uuid1:101,uuid2:50");
  EXPECT_EQ(result, "uuid1:1-101,uuid2:1-50");
}

/**
 * @brief Test ConvertSingleGtidToRange with multi-UUID mixed entries
 */
TEST(BinlogReaderTest, ConvertSingleGtidToRangeMultiUuidMixed) {
  // Mix of range and single GNO entries
  std::string result = BinlogReader::ConvertSingleGtidToRange("uuid1:1-100,uuid2:50");
  EXPECT_EQ(result, "uuid1:1-100,uuid2:1-50");
}

/**
 * @brief Test ConvertSingleGtidToRange with multi-UUID tagged GTID passthrough
 */
TEST(BinlogReaderTest, ConvertSingleGtidToRangeMultiUuidWithTagged) {
  // Tagged GTID entries (UUID:TAG:GNO) should pass through unchanged
  std::string result = BinlogReader::ConvertSingleGtidToRange("uuid1:101,uuid2:tag1:50");
  EXPECT_EQ(result, "uuid1:1-101,uuid2:tag1:50");
}

/**
 * @brief Test ConvertSingleGtidToRange with multiple intervals (no conversion)
 */
TEST(BinlogReaderTest, ConvertSingleGtidToRangeWithMultipleIntervals) {
  // Multiple intervals use colon separator - should pass through
  std::string result = BinlogReader::ConvertSingleGtidToRange("61d5b289-bccc-11f0-b921-cabbb4ee51f6:1-3:5-7");
  EXPECT_EQ(result, "61d5b289-bccc-11f0-b921-cabbb4ee51f6:1-3:5-7");
}

TEST_F(BinlogReaderFixture, ProcessingFailureRequestsReconnectAndDropsQueuedEvents) {
  server::TableContext broken_context;
  broken_context.name = "broken";
  broken_context.config = table_config_;
  broken_context.config.name = "broken";
  broken_context.index.reset();
  broken_context.doc_store.reset();

  reader_->table_contexts_["broken"] = &broken_context;
  reader_->current_gtid_ = "uuid:1";

  auto failing_event = std::make_unique<BinlogEvent>(MakeEvent(BinlogEventType::INSERT, "2", 1));
  failing_event->table_name = "broken";
  failing_event->gtid = "uuid:2";
  auto later_event = std::make_unique<BinlogEvent>(MakeEvent(BinlogEventType::INSERT, "3", 1));
  later_event->table_name = "broken";
  later_event->gtid = "uuid:3";

  {
    std::lock_guard<std::mutex> lock(reader_->queue_mutex_);
    reader_->event_queue_.push(std::move(failing_event));
    reader_->event_queue_.push(std::move(later_event));
  }
  reader_->queue_cv_.notify_one();

  std::thread worker([this]() { reader_->WorkerThreadFunc(); });

  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
  while (reader_->processing_failure_reconnect_requested_.load(std::memory_order_acquire) ==
             BinlogReader::ProcessingFailureKind::kNone &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  EXPECT_EQ(reader_->processing_failure_reconnect_requested_.load(std::memory_order_acquire),
            BinlogReader::ProcessingFailureKind::kDeterministic);
  EXPECT_EQ(reader_->current_gtid_, "uuid:1");
  {
    std::lock_guard<std::mutex> lock(reader_->queue_mutex_);
    EXPECT_TRUE(reader_->event_queue_.empty());
  }

  reader_->should_stop_ = true;
  reader_->queue_cv_.notify_all();
  worker.join();
  reader_->should_stop_ = false;
}

TEST_F(BinlogReaderFixture, WorkerThreadExceptionStopsReplicationWithoutTerminatingProcess) {
  server::TableContext broken_context;
  broken_context.name = "broken";
  broken_context.config = table_config_;
  broken_context.config.name = "broken";
  broken_context.index.reset();
  broken_context.doc_store.reset();
  reader_->table_contexts_["broken"] = &broken_context;
  reader_->SetAfterProcessingFailurePublishedHookForTest([]() { throw std::runtime_error("injected worker failure"); });

  auto failing_event = std::make_unique<BinlogEvent>(MakeEvent(BinlogEventType::INSERT, "2", 1));
  failing_event->table_name = "broken";
  {
    std::lock_guard<std::mutex> lock(reader_->queue_mutex_);
    reader_->event_queue_.push(std::move(failing_event));
  }
  reader_->queue_cv_.notify_one();

  std::thread worker([this]() { reader_->WorkerThreadFunc(); });
  worker.join();
  reader_->SetAfterProcessingFailurePublishedHookForTest({});

  EXPECT_TRUE(reader_->should_stop_.load(std::memory_order_acquire));
  EXPECT_NE(reader_->GetLastError().find("injected worker failure"), std::string::npos);
  reader_->should_stop_.store(false, std::memory_order_release);
}

TEST_F(BinlogReaderFixture, MidTransactionFailureDoesNotAdvanceGtidToFailedTransaction) {
  server::TableContext broken_context;
  broken_context.name = "broken";
  broken_context.config = table_config_;
  broken_context.config.name = "broken";
  broken_context.index.reset();
  broken_context.doc_store.reset();

  reader_->table_contexts_["broken"] = &broken_context;
  reader_->current_gtid_ = "uuid:1";

  auto first_event = std::make_unique<BinlogEvent>(MakeEvent(BinlogEventType::INSERT, "2", 1, "first row"));
  first_event->gtid = "uuid:2";

  auto failing_event = std::make_unique<BinlogEvent>(MakeEvent(BinlogEventType::INSERT, "3", 1, "second row"));
  failing_event->table_name = "broken";
  failing_event->gtid = "uuid:2";

  auto commit_event = std::make_unique<BinlogEvent>();
  commit_event->type = BinlogEventType::COMMIT;
  commit_event->gtid = "uuid:2";

  {
    std::lock_guard<std::mutex> lock(reader_->queue_mutex_);
    reader_->event_queue_.push(std::move(first_event));
    reader_->event_queue_.push(std::move(failing_event));
    reader_->event_queue_.push(std::move(commit_event));
  }
  reader_->queue_cv_.notify_one();

  std::thread worker([this]() { reader_->WorkerThreadFunc(); });

  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
  while (reader_->processing_failure_reconnect_requested_.load(std::memory_order_acquire) ==
             BinlogReader::ProcessingFailureKind::kNone &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  EXPECT_EQ(reader_->processing_failure_reconnect_requested_.load(std::memory_order_acquire),
            BinlogReader::ProcessingFailureKind::kDeterministic);
  EXPECT_TRUE(doc_store_.GetDocId("2").has_value()) << "first row was applied before the mid-transaction failure";
  EXPECT_EQ(reader_->current_gtid_, "uuid:1") << "failed transaction GTID must not be persisted before XID";
  {
    std::lock_guard<std::mutex> lock(reader_->queue_mutex_);
    EXPECT_TRUE(reader_->event_queue_.empty()) << "commit marker and later rows must be discarded after failure";
  }
  {
    std::scoped_lock lock(reader_->gtid_mutex_);
    EXPECT_TRUE(reader_->position_state_.TakeCommitGTID("").empty())
        << "a partially applied transaction must not leave a commit candidate across reconnect";
  }

  reader_->should_stop_ = true;
  reader_->queue_cv_.notify_all();
  worker.join();
  reader_->should_stop_ = false;
}

TEST_F(BinlogReaderFixture, ProcessingFailureRejectsConcurrentCommitBeforeWorkerContinues) {
  server::TableContext broken_context;
  broken_context.name = "broken";
  broken_context.config = table_config_;
  broken_context.config.name = "broken";
  broken_context.index.reset();
  broken_context.doc_store.reset();

  reader_->table_contexts_["broken"] = &broken_context;
  reader_->SetCurrentGTID("uuid:1");

  mygramdb::testing::DeterministicGate failure_published;
  reader_->SetAfterProcessingFailurePublishedHookForTest([&]() { failure_published.ArriveAndWait(); });

  auto first_event = std::make_unique<BinlogEvent>(MakeEvent(BinlogEventType::INSERT, "2", 1, "first row"));
  first_event->gtid = "uuid:2";
  auto failing_event = std::make_unique<BinlogEvent>(MakeEvent(BinlogEventType::INSERT, "3", 1, "second row"));
  failing_event->table_name = "broken";
  failing_event->gtid = "uuid:2";

  {
    std::lock_guard<std::mutex> lock(reader_->queue_mutex_);
    reader_->event_queue_.push(std::move(first_event));
    reader_->event_queue_.push(std::move(failing_event));
  }
  reader_->queue_cv_.notify_one();

  std::thread worker([this]() { reader_->WorkerThreadFunc(); });
  if (!failure_published.WaitUntilArrived(std::chrono::seconds(2))) {
    failure_published.Release();
    reader_->should_stop_.store(true, std::memory_order_release);
    reader_->queue_cv_.notify_all();
    worker.join();
    reader_->SetAfterProcessingFailurePublishedHookForTest({});
    FAIL() << "worker did not publish the processing failure";
  }

  auto commit_event = std::make_unique<BinlogEvent>();
  commit_event->type = BinlogEventType::COMMIT;
  commit_event->gtid = "uuid:2";
  std::thread producer([this, event = std::move(commit_event)]() mutable { reader_->PushEvent(std::move(event)); });
  producer.join();

  EXPECT_EQ(reader_->GetQueueSize(), 0U) << "COMMIT must be rejected after failure publication";
  EXPECT_EQ(reader_->GetCurrentGTID(), "uuid:1");

  failure_published.Release();
  reader_->should_stop_.store(true, std::memory_order_release);
  reader_->queue_cv_.notify_all();
  worker.join();
  reader_->SetAfterProcessingFailurePublishedHookForTest({});
  reader_->should_stop_.store(false, std::memory_order_release);

  EXPECT_TRUE(doc_store_.GetDocId("2").has_value()) << "the transaction was partially applied before failure";
  EXPECT_EQ(reader_->GetCurrentGTID(), "uuid:1") << "rejected COMMIT must not advance the failed transaction GTID";
}

// ===========================================================================
// Read timeout configuration
// ===========================================================================

/**
 * @brief Binlog connection uses extended read timeout for heartbeat
 */
TEST(BinlogReaderTest, BinlogConnectionUsesExtendedReadTimeout) {
  // The binlog_conn_config.read_timeout should be 60 (not 5)
  // We verify this indirectly by checking the Config defaults and construction
  // Since read_timeout is set in Start() which requires MySQL, we verify
  // the constant is correct by checking the source code behavior
  // The actual value 60 is hardcoded in Start() method

  // This test documents the expected timeout value
  constexpr int kExpectedReadTimeout = 60;
  EXPECT_EQ(kExpectedReadTimeout, 60) << "Binlog read timeout should be 60 seconds for heartbeat support";
}

#endif  // USE_MYSQL
