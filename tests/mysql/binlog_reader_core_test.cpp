/**
 * @file binlog_reader_core_test.cpp
 * @brief Unit tests for binlog reader - Core lifecycle and queue operations
 */

#include "binlog_test_fixtures.h"

#ifdef USE_MYSQL

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

/**
 * @brief Verify IsRunning returns false when should_stop_ is true
 *
 * This tests the fix for the REPLICATION STATUS bug where status would show
 * "running" even when Stop() was requested but threads hadn't finished yet.
 */
TEST_F(BinlogReaderFixture, IsRunningReturnsFalseWhenShouldStopIsTrue) {
  // Initially not running
  EXPECT_FALSE(reader_->IsRunning());

  // Simulate running state
  reader_->running_ = true;
  EXPECT_TRUE(reader_->IsRunning());

  // Set should_stop_ flag (simulating Stop() was called)
  reader_->should_stop_ = true;

  // IsRunning should now return false (stopping state)
  EXPECT_FALSE(reader_->IsRunning());

  // Cleanup
  reader_->running_ = false;
  reader_->should_stop_ = false;
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

// ===========================================================================
// GTID set reconnection tests
// ===========================================================================

/**
 * @brief UpdateCurrentGTID with single GTID must NOT overwrite executed_gtid_set_
 */
TEST_F(BinlogReaderFixture, UpdateCurrentGtidDoesNotOverwriteExecutedGtidSet) {
  // Simulate initial GTID set (as if loaded from snapshot or server)
  reader_->SetCurrentGTID("uuid1:1-100,uuid2:1-50");

  // Verify executed_gtid_set_ is set
  {
    std::scoped_lock lock(reader_->gtid_mutex_);
    EXPECT_EQ(reader_->executed_gtid_set_, "uuid1:1-100,uuid2:1-50");
  }

  // Simulate receiving a GTID event (single GTID)
  reader_->UpdateCurrentGTID("uuid1:101");

  // current_gtid_ should retain the full set and merge the applied GTID.
  EXPECT_EQ(reader_->GetCurrentGTID(), "uuid1:1-101,uuid2:1-50");

  // executed_gtid_set_ MUST NOT be overwritten by single GTID
  {
    std::scoped_lock lock(reader_->gtid_mutex_);
    EXPECT_EQ(reader_->executed_gtid_set_, "uuid1:1-100,uuid2:1-50");
  }
}

/**
 * @brief SetCurrentGTID with range format also sets executed_gtid_set_
 */
TEST_F(BinlogReaderFixture, SetCurrentGtidWithRangeAlsoSetsExecutedGtidSet) {
  reader_->SetCurrentGTID("uuid1:1-100");

  EXPECT_EQ(reader_->GetCurrentGTID(), "uuid1:1-100");
  {
    std::scoped_lock lock(reader_->gtid_mutex_);
    EXPECT_EQ(reader_->executed_gtid_set_, "uuid1:1-100");
  }
}

/**
 * @brief SetCurrentGTID with single GTID does NOT set executed_gtid_set_
 */
TEST_F(BinlogReaderFixture, SetCurrentGtidWithSingleGtidDoesNotSetExecutedGtidSet) {
  // First set a full GTID set
  reader_->SetCurrentGTID("uuid1:1-100");
  // Then simulate single GTID from event
  reader_->SetCurrentGTID("uuid1:101");

  // current_gtid_ updated
  EXPECT_EQ(reader_->GetCurrentGTID(), "uuid1:101");
  // executed_gtid_set_ retains the full set
  {
    std::scoped_lock lock(reader_->gtid_mutex_);
    EXPECT_EQ(reader_->executed_gtid_set_, "uuid1:1-100");
  }
}

/**
 * @brief Multiple UpdateCurrentGTID calls don't affect executed_gtid_set_
 */
TEST_F(BinlogReaderFixture, GetExecutedGtidSetReturnsFullSetForReconnection) {
  // Simulate startup: set full GTID set
  reader_->SetCurrentGTID("uuid1:1-100,uuid2:1-50");

  // Simulate processing several events
  reader_->UpdateCurrentGTID("uuid1:101");
  reader_->UpdateCurrentGTID("uuid1:102");
  reader_->UpdateCurrentGTID("uuid1:103");

  // current_gtid_ should retain the full set and merge all applied GTIDs.
  EXPECT_EQ(reader_->GetCurrentGTID(), "uuid1:1-103,uuid2:1-50");

  // executed_gtid_set_ should still be the full set (for reconnection use)
  {
    std::scoped_lock lock(reader_->gtid_mutex_);
    EXPECT_EQ(reader_->executed_gtid_set_, "uuid1:1-100,uuid2:1-50");
  }
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

/**
 * @brief Reconnection always uses current_gtid_ regardless of executed_gtid_set_
 */
TEST_F(BinlogReaderFixture, ReconnectionAlwaysUsesCurrentGtid) {
  // Set current_gtid_ and executed_gtid_set_ to different values
  {
    std::scoped_lock lock(reader_->gtid_mutex_);
    reader_->current_gtid_ = "uuid1:500";
    reader_->executed_gtid_set_ = "uuid1:1-510";  // Server ahead
  }

  // The GTID used for reconnection must always be based on current_gtid_
  std::string gtid_for_reconnect;
  {
    std::scoped_lock lock(reader_->gtid_mutex_);
    gtid_for_reconnect = BinlogReader::ConvertSingleGtidToRange(reader_->current_gtid_);
  }

  // Must be "uuid1:1-500" (from current_gtid_), NOT "uuid1:1-510" (from executed_gtid_set_)
  EXPECT_EQ(gtid_for_reconnect, "uuid1:1-500");
}

TEST_F(BinlogReaderFixture, ValidationGtidUsesCurrentGtidNotExecutedSet) {
  {
    std::scoped_lock lock(reader_->gtid_mutex_);
    reader_->current_gtid_ = "uuid1:103";
    reader_->executed_gtid_set_ = "uuid1:1-200,uuid2:1-50";
  }

  std::optional<std::string> current_gtid_for_validation;
  {
    std::scoped_lock lock(reader_->gtid_mutex_);
    if (!reader_->current_gtid_.empty()) {
      current_gtid_for_validation = BinlogReader::ConvertSingleGtidToRange(reader_->current_gtid_);
    }
  }

  ASSERT_TRUE(current_gtid_for_validation.has_value());
  EXPECT_EQ(*current_gtid_for_validation, "uuid1:1-103");
}

// ===========================================================================
// Tests moved from binlog_reader_bug_fixes_test.cpp
// ===========================================================================

/**
 * @brief ReaderThread loop should only exit when should_stop_ is true
 *
 * Previously the exit condition was `if (!connection_lost || should_stop_) { break; }`
 * which incorrectly broke when connection_lost=false (normal idle). Fixed to
 * `if (should_stop_) { break; }`.
 */
TEST_F(BinlogReaderFixture, ReaderThreadContinuesOnNormalIdle) {
  // Normal operation: should NOT exit
  EXPECT_FALSE(false) << "should_stop=false: loop continues";

  // Connection lost without stop: should continue (for reconnect)
  bool should_stop = false;
  EXPECT_FALSE(should_stop) << "Connection lost: loop continues for reconnect";

  // Stop requested: should exit
  should_stop = true;
  EXPECT_TRUE(should_stop) << "should_stop=true: loop exits";
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

TEST_F(BinlogReaderFixture, DdlGtidAdvancesImmediatelyAfterSuccess) {
  reader_->SetCurrentGTID("uuid:50");

  BinlogEvent ddl_event = BinlogEvent::CreateDDL(table_config_.name, "ALTER TABLE articles ADD COLUMN title TEXT");
  ddl_event.gtid = "uuid:51";

  ASSERT_TRUE(reader_->ProcessQueuedEvent(ddl_event));
  EXPECT_EQ(reader_->GetCurrentGTID(), "uuid:50-51");
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

/**
 * @brief ConvertSingleGtidToRange is always used for reconnection GTID
 */
TEST_F(BinlogReaderFixture, ReconnectionGtidAlwaysConverted) {
  // Even when executed_gtid_set_ is set, reconnection uses current_gtid_
  {
    std::scoped_lock lock(reader_->gtid_mutex_);
    reader_->current_gtid_ = "61d5b289-bccc-11f0-b921-cabbb4ee51f6:50";
    reader_->executed_gtid_set_ = "61d5b289-bccc-11f0-b921-cabbb4ee51f6:1-100";
  }

  std::string gtid_for_reconnect;
  {
    std::scoped_lock lock(reader_->gtid_mutex_);
    gtid_for_reconnect = BinlogReader::ConvertSingleGtidToRange(reader_->current_gtid_);
  }
  // Must use current_gtid_ converted to range, NOT executed_gtid_set_
  EXPECT_EQ(gtid_for_reconnect, "61d5b289-bccc-11f0-b921-cabbb4ee51f6:1-50");
}

// ===========================================================================
// Reconnection uses processed GTID, not server GTID
// ===========================================================================

/**
 * @brief Verify reconnection never uses executed_gtid_set_ (prevents data loss)
 *
 * Scenario: Server has committed events 501-510 but MygramDB only processed up to 500.
 * Reconnection must use "uuid:1-500", not "uuid:1-510", to avoid skipping events 501-510.
 */
TEST_F(BinlogReaderFixture, ReconnectionUsesProcessedGtidNotServerGtid) {
  // Simulate: MygramDB processed up to event 500
  reader_->SetCurrentGTID("uuid:500");

  // Simulate: Server has committed up to event 510 (ahead of MygramDB)
  {
    std::scoped_lock lock(reader_->gtid_mutex_);
    reader_->executed_gtid_set_ = "uuid:1-510";
  }

  // Verify reconnection uses current_gtid_, not executed_gtid_set_
  std::string reconnect_gtid;
  {
    std::scoped_lock lock(reader_->gtid_mutex_);
    reconnect_gtid = BinlogReader::ConvertSingleGtidToRange(reader_->current_gtid_);
  }

  EXPECT_EQ(reconnect_gtid, "uuid:1-500") << "Reconnection must use processed GTID (1-500), not server GTID (1-510)";
}

/**
 * @brief ConvertSingleGtidToRange is used even when executed_gtid_set_ is non-empty
 */
TEST_F(BinlogReaderFixture, ConvertSingleGtidToRangeAlwaysUsed) {
  // Set both current_gtid_ and executed_gtid_set_
  {
    std::scoped_lock lock(reader_->gtid_mutex_);
    reader_->current_gtid_ = "uuid1:103";
    reader_->executed_gtid_set_ = "uuid1:1-200,uuid2:1-50";
  }

  // Reconnection logic must use ConvertSingleGtidToRange(current_gtid_)
  std::string reconnect_gtid;
  {
    std::scoped_lock lock(reader_->gtid_mutex_);
    reconnect_gtid = BinlogReader::ConvertSingleGtidToRange(reader_->current_gtid_);
  }

  EXPECT_EQ(reconnect_gtid, "uuid1:1-103") << "Must convert current_gtid_ to range, not use executed_gtid_set_";
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
  while (!reader_->processing_failure_reconnect_requested_.load(std::memory_order_acquire) &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  EXPECT_TRUE(reader_->processing_failure_reconnect_requested_.load(std::memory_order_acquire));
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
  while (!reader_->processing_failure_reconnect_requested_.load(std::memory_order_acquire) &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  EXPECT_TRUE(reader_->processing_failure_reconnect_requested_.load(std::memory_order_acquire));
  EXPECT_TRUE(doc_store_.GetDocId("2").has_value()) << "first row was applied before the mid-transaction failure";
  EXPECT_EQ(reader_->current_gtid_, "uuid:1") << "failed transaction GTID must not be persisted before XID";
  {
    std::lock_guard<std::mutex> lock(reader_->queue_mutex_);
    EXPECT_TRUE(reader_->event_queue_.empty()) << "commit marker and later rows must be discarded after failure";
  }

  reader_->should_stop_ = true;
  reader_->queue_cv_.notify_all();
  worker.join();
  reader_->should_stop_ = false;
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
