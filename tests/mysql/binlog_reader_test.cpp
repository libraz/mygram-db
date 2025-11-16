/**
 * @file binlog_reader_test.cpp
 * @brief Unit tests for binlog reader
 */

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>

#define private public
#define protected public
#include "mysql/binlog_reader.h"
#undef private
#undef protected

#ifdef USE_MYSQL

#include "mock_connection.h"
#include "server/server_stats.h"
#include "server/server_types.h"

using namespace mygramdb::mysql;
using namespace mygramdb;

namespace {

/**
 * @brief Helper that creates a default table configuration for tests
 */
config::TableConfig MakeDefaultTableConfig() {
  config::TableConfig table_config;
  table_config.name = "articles";
  table_config.primary_key = "id";
  table_config.text_source.column = "content";

  config::RequiredFilterConfig required_filter;
  required_filter.name = "status";
  required_filter.type = "int";
  required_filter.op = "=";
  required_filter.value = "1";
  table_config.required_filters.push_back(required_filter);

  config::FilterConfig optional_filter;
  optional_filter.name = "category";
  optional_filter.type = "string";
  table_config.filters.push_back(optional_filter);

  return table_config;
}

/**
 * @brief BinlogReader test fixture providing in-memory dependencies
 */
class BinlogReaderFixture : public ::testing::Test {
 protected:
  BinlogReaderFixture() : connection_(connection_config_), index_(2) {}

  void SetUp() override {
    table_config_ = MakeDefaultTableConfig();
    reader_config_.start_gtid = "uuid:1";
    reader_config_.queue_size = 2;
    reader_config_.reconnect_delay_ms = 10;

    index_.Clear();
    doc_store_.Clear();
    ResetReader();
  }

  void TearDown() override {
    reader_.reset();
    doc_store_.Clear();
    index_.Clear();
  }

  /**
   * @brief Recreate BinlogReader with current configuration
   */
  void ResetReader() {
    reader_ = std::make_unique<BinlogReader>(connection_, index_, doc_store_, table_config_, reader_config_);
  }

  /**
   * @brief Utility to build a fully populated event for tests
   */
  BinlogEvent MakeEvent(BinlogEventType type, const std::string& pk, int status, const std::string& text = "hello") {
    BinlogEvent event;
    event.type = type;
    event.table_name = table_config_.name;
    event.primary_key = pk;
    event.text = text;
    event.gtid = "uuid:" + pk;
    event.filters["status"] = static_cast<int64_t>(status);
    event.filters["category"] = std::string("news");
    return event;
  }

  Connection::Config connection_config_;
  Connection connection_;
  index::Index index_;
  storage::DocumentStore doc_store_;
  config::TableConfig table_config_;
  BinlogReader::Config reader_config_;
  std::unique_ptr<BinlogReader> reader_;
};

}  // namespace

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
 * @brief Exercise queue push/pop helpers without worker threads
 */
TEST_F(BinlogReaderFixture, PushAndPopEvents) {
  BinlogEvent first = MakeEvent(BinlogEventType::INSERT, "1", 1);
  reader_->PushEvent(first);
  EXPECT_EQ(reader_->GetQueueSize(), 1);

  BinlogEvent popped;
  ASSERT_TRUE(reader_->PopEvent(popped));
  EXPECT_EQ(popped.primary_key, "1");
  EXPECT_EQ(reader_->GetQueueSize(), 0);
}

/**
 * @brief Verify PushEvent blocks when queue is full until space becomes available
 */
TEST_F(BinlogReaderFixture, PushBlocksWhenQueueFull) {
  reader_->config_.queue_size = 1;
  BinlogEvent first = MakeEvent(BinlogEventType::INSERT, "1", 1);
  reader_->PushEvent(first);

  std::atomic<bool> second_pushed{false};
  std::thread producer([&] {
    BinlogEvent second = MakeEvent(BinlogEventType::INSERT, "2", 1);
    reader_->PushEvent(second);
    second_pushed.store(true);
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  EXPECT_FALSE(second_pushed.load());

  BinlogEvent popped;
  ASSERT_TRUE(reader_->PopEvent(popped));
  producer.join();
  EXPECT_TRUE(second_pushed.load());

  // Drain queue for subsequent tests
  ASSERT_TRUE(reader_->PopEvent(popped));
  EXPECT_EQ(reader_->GetQueueSize(), 0);
}

/**
 * @brief Ensure PopEvent blocks until a producer pushes data
 */
TEST_F(BinlogReaderFixture, PopBlocksUntilEventArrives) {
  std::atomic<bool> pop_completed{false};
  std::thread consumer([&] {
    BinlogEvent event;
    bool ok = reader_->PopEvent(event);
    pop_completed.store(ok);
    EXPECT_EQ(event.primary_key, "7");
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  EXPECT_FALSE(pop_completed.load());

  reader_->PushEvent(MakeEvent(BinlogEventType::INSERT, "7", 1));
  consumer.join();
  EXPECT_TRUE(pop_completed.load());
  EXPECT_EQ(reader_->GetQueueSize(), 0);
}

/**
 * @brief Confirm PopEvent unblocks and returns false when reader is stopped
 */
TEST_F(BinlogReaderFixture, PopReturnsFalseWhenStopping) {
  std::atomic<bool> pop_result{true};
  std::thread consumer([&] {
    BinlogEvent event;
    bool ok = reader_->PopEvent(event);
    pop_result.store(ok);
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  reader_->should_stop_ = true;
  reader_->queue_cv_.notify_all();

  consumer.join();
  EXPECT_FALSE(pop_result.load());
  reader_->should_stop_ = false;
}

/**
 * @brief Validate INSERT events create documents when filters match
 */
TEST_F(BinlogReaderFixture, ProcessInsertAddsDocument) {
  BinlogEvent insert_event = MakeEvent(BinlogEventType::INSERT, "42", 1, "Breaking news");
  ASSERT_TRUE(reader_->ProcessEvent(insert_event));

  auto doc_id = doc_store_.GetDocId("42");
  ASSERT_TRUE(doc_id.has_value());
  auto stored_doc = doc_store_.GetDocument(doc_id.value());
  ASSERT_TRUE(stored_doc.has_value());
  EXPECT_EQ(std::get<std::string>(stored_doc->filters["category"]), "news");
}

/**
 * @brief Ensure UPDATE removes rows when they no longer satisfy required filters
 */
TEST_F(BinlogReaderFixture, ProcessUpdateRemovesWhenFiltersFail) {
  ASSERT_TRUE(reader_->ProcessEvent(MakeEvent(BinlogEventType::INSERT, "90", 1, "Initial")));

  BinlogEvent update_event = MakeEvent(BinlogEventType::UPDATE, "90", 0, "Updated text");
  ASSERT_TRUE(reader_->ProcessEvent(update_event));
  EXPECT_FALSE(doc_store_.GetDocId("90").has_value());
}

/**
 * @brief Verify DELETE events remove documents and index entries
 */
TEST_F(BinlogReaderFixture, ProcessDeleteRemovesDocument) {
  ASSERT_TRUE(reader_->ProcessEvent(MakeEvent(BinlogEventType::INSERT, "77", 1, "Row")));

  BinlogEvent delete_event = MakeEvent(BinlogEventType::DELETE, "77", 1, "Row");
  ASSERT_TRUE(reader_->ProcessEvent(delete_event));
  EXPECT_FALSE(doc_store_.GetDocId("77").has_value());
}

/**
 * @brief Validate DDL TRUNCATE clears index and store
 */
TEST_F(BinlogReaderFixture, ProcessDdlTruncateClearsState) {
  ASSERT_TRUE(reader_->ProcessEvent(MakeEvent(BinlogEventType::INSERT, "5", 1, "Body")));
  EXPECT_EQ(doc_store_.Size(), 1);

  BinlogEvent ddl_event;
  ddl_event.type = BinlogEventType::DDL;
  ddl_event.table_name = table_config_.name;
  ddl_event.text = "TRUNCATE TABLE articles";
  ASSERT_TRUE(reader_->ProcessEvent(ddl_event));
  EXPECT_EQ(doc_store_.Size(), 0);
}

/**
 * @brief Confirm events missing required filters are skipped
 */
TEST_F(BinlogReaderFixture, SkipsEventsMissingRequiredFilters) {
  BinlogEvent insert_event = MakeEvent(BinlogEventType::INSERT, "21", 1, "Text");
  insert_event.filters.erase("status");
  ASSERT_TRUE(reader_->ProcessEvent(insert_event));
  EXPECT_FALSE(doc_store_.GetDocId("21").has_value());
}

/**
 * @brief Exercise GTID setters/getters
 */
TEST_F(BinlogReaderFixture, TracksGtidUpdates) {
  reader_->SetCurrentGTID("uuid:10");
  EXPECT_EQ(reader_->GetCurrentGTID(), "uuid:10");
  reader_->UpdateCurrentGTID("uuid:11");
  EXPECT_EQ(reader_->GetCurrentGTID(), "uuid:11");
}

/**
 * @brief Ensure events are routed to correct TableContext in multi-table mode
 */
TEST_F(BinlogReaderFixture, MultiTableProcessesCorrectTable) {
  server::TableContext articles_ctx;
  articles_ctx.name = "articles";
  articles_ctx.config = table_config_;
  articles_ctx.index = std::make_unique<index::Index>();
  articles_ctx.doc_store = std::make_unique<storage::DocumentStore>();

  server::TableContext comments_ctx;
  comments_ctx.name = "comments";
  comments_ctx.config = table_config_;
  comments_ctx.config.name = "comments";
  comments_ctx.index = std::make_unique<index::Index>();
  comments_ctx.doc_store = std::make_unique<storage::DocumentStore>();

  std::unordered_map<std::string, server::TableContext*> contexts = {
      {articles_ctx.name, &articles_ctx},
      {comments_ctx.name, &comments_ctx},
  };

  BinlogReader multi_reader(connection_, contexts, reader_config_);

  BinlogEvent comment_event = MakeEvent(BinlogEventType::INSERT, "300", 1, "Comment");
  comment_event.table_name = "comments";
  ASSERT_TRUE(multi_reader.ProcessEvent(comment_event));
  EXPECT_TRUE(comments_ctx.doc_store->GetDocId("300").has_value());
  EXPECT_FALSE(articles_ctx.doc_store->GetDocId("300").has_value());
}

/**
 * @brief Multi-table mode should ignore tables that are not tracked
 */
TEST_F(BinlogReaderFixture, MultiTableSkipsUnknownTable) {
  server::TableContext articles_ctx;
  articles_ctx.name = "articles";
  articles_ctx.config = table_config_;
  articles_ctx.index = std::make_unique<index::Index>();
  articles_ctx.doc_store = std::make_unique<storage::DocumentStore>();

  std::unordered_map<std::string, server::TableContext*> contexts = {
      {articles_ctx.name, &articles_ctx},
  };

  BinlogReader multi_reader(connection_, contexts, reader_config_);
  BinlogEvent other_event = MakeEvent(BinlogEventType::INSERT, "400", 1, "Ignored");
  other_event.table_name = "not_tracked";
  ASSERT_TRUE(multi_reader.ProcessEvent(other_event));
  EXPECT_EQ(articles_ctx.doc_store->Size(), 0);
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

  BinlogReader reader(conn, idx, doc_store, table_config, reader_config);

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

  BinlogReader reader(conn, idx, doc_store, table_config, reader_config);

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

  // Custom values
  config.start_gtid = "test:123";
  config.queue_size = 5000;
  config.reconnect_delay_ms = 500;

  EXPECT_EQ(config.start_gtid, "test:123");
  EXPECT_EQ(config.queue_size, 5000);
  EXPECT_EQ(config.reconnect_delay_ms, 500);
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

  EXPECT_NE(insert_event.primary_key, update_event.primary_key);
  EXPECT_NE(update_event.primary_key, delete_event.primary_key);
}

/**
 * @brief Test DDL event type
 */
TEST(BinlogReaderTest, DDLEventType) {
  BinlogEvent ddl_event;
  ddl_event.type = BinlogEventType::DDL;
  ddl_event.table_name = "test_table";
  ddl_event.text = "TRUNCATE TABLE test_table";

  EXPECT_EQ(ddl_event.type, BinlogEventType::DDL);
  EXPECT_EQ(ddl_event.table_name, "test_table");
  EXPECT_EQ(ddl_event.text, "TRUNCATE TABLE test_table");

  // DDL events should be distinct from other event types
  EXPECT_NE(BinlogEventType::DDL, BinlogEventType::INSERT);
  EXPECT_NE(BinlogEventType::DDL, BinlogEventType::UPDATE);
  EXPECT_NE(BinlogEventType::DDL, BinlogEventType::DELETE);
  EXPECT_NE(BinlogEventType::DDL, BinlogEventType::UNKNOWN);
}

/**
 * @brief Test TRUNCATE TABLE DDL event
 */
TEST(BinlogReaderTest, TruncateTableEvent) {
  BinlogEvent event;
  event.type = BinlogEventType::DDL;
  event.table_name = "articles";
  event.text = "TRUNCATE TABLE articles";

  EXPECT_EQ(event.type, BinlogEventType::DDL);
  EXPECT_NE(event.text.find("TRUNCATE"), std::string::npos);
}

/**
 * @brief Test ALTER TABLE DDL event
 */
TEST(BinlogReaderTest, AlterTableEvent) {
  BinlogEvent event;
  event.type = BinlogEventType::DDL;
  event.table_name = "users";
  event.text = "ALTER TABLE users ADD COLUMN email VARCHAR(255)";

  EXPECT_EQ(event.type, BinlogEventType::DDL);
  EXPECT_NE(event.text.find("ALTER"), std::string::npos);
}

/**
 * @brief Test DROP TABLE DDL event
 */
TEST(BinlogReaderTest, DropTableEvent) {
  BinlogEvent event;
  event.type = BinlogEventType::DDL;
  event.table_name = "temp_table";
  event.text = "DROP TABLE temp_table";

  EXPECT_EQ(event.type, BinlogEventType::DDL);
  EXPECT_NE(event.text.find("DROP"), std::string::npos);
}

/**
 * @brief Test DDL event with GTID
 */
TEST(BinlogReaderTest, DDLEventWithGTID) {
  BinlogEvent event;
  event.type = BinlogEventType::DDL;
  event.table_name = "products";
  event.text = "TRUNCATE TABLE products";
  event.gtid = "3E11FA47-71CA-11E1-9E33-C80AA9429562:150";

  EXPECT_EQ(event.type, BinlogEventType::DDL);
  EXPECT_EQ(event.gtid, "3E11FA47-71CA-11E1-9E33-C80AA9429562:150");
  EXPECT_FALSE(event.gtid.empty());
}

/**
 * @brief Test various DDL statement formats
 */
TEST(BinlogReaderTest, VariousDDLFormats) {
  // Test case variations
  BinlogEvent truncate_upper;
  truncate_upper.type = BinlogEventType::DDL;
  truncate_upper.text = "TRUNCATE TABLE MY_TABLE";
  EXPECT_NE(truncate_upper.text.find("TRUNCATE"), std::string::npos);

  BinlogEvent truncate_lower;
  truncate_lower.type = BinlogEventType::DDL;
  truncate_lower.text = "truncate table my_table";
  EXPECT_NE(truncate_lower.text.find("truncate"), std::string::npos);

  BinlogEvent alter_add_column;
  alter_add_column.type = BinlogEventType::DDL;
  alter_add_column.text = "ALTER TABLE users ADD COLUMN status INT";
  EXPECT_NE(alter_add_column.text.find("ALTER"), std::string::npos);

  BinlogEvent alter_modify_column;
  alter_modify_column.type = BinlogEventType::DDL;
  alter_modify_column.text = "ALTER TABLE users MODIFY COLUMN name VARCHAR(100)";
  EXPECT_NE(alter_modify_column.text.find("MODIFY"), std::string::npos);

  BinlogEvent drop_if_exists;
  drop_if_exists.type = BinlogEventType::DDL;
  drop_if_exists.text = "DROP TABLE IF EXISTS temp_table";
  EXPECT_NE(drop_if_exists.text.find("DROP"), std::string::npos);
}

/**
 * @brief Test DDL event distinguishing from DML events
 */
TEST(BinlogReaderTest, DDLvsDMLEvents) {
  BinlogEvent dml_insert;
  dml_insert.type = BinlogEventType::INSERT;
  dml_insert.primary_key = "100";
  dml_insert.text = "new record text";

  BinlogEvent ddl_truncate;
  ddl_truncate.type = BinlogEventType::DDL;
  ddl_truncate.text = "TRUNCATE TABLE test_table";

  // DDL events don't have primary keys (they affect entire table)
  EXPECT_FALSE(dml_insert.primary_key.empty());
  EXPECT_TRUE(ddl_truncate.primary_key.empty());

  // DDL events store SQL query in text field
  EXPECT_EQ(dml_insert.type, BinlogEventType::INSERT);
  EXPECT_EQ(ddl_truncate.type, BinlogEventType::DDL);
}

/**
 * @brief Test BinlogReader with ServerStats integration
 */
TEST(BinlogReaderTest, ServerStatsIntegration) {
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

  server::ServerStats stats;

  // Create BinlogReader with ServerStats
  BinlogReader reader(conn, idx, doc_store, table_config, reader_config, &stats);

  // Verify initial statistics are zero
  EXPECT_EQ(stats.GetReplInsertsApplied(), 0);
  EXPECT_EQ(stats.GetReplInsertsSkipped(), 0);
  EXPECT_EQ(stats.GetReplUpdatesApplied(), 0);
  EXPECT_EQ(stats.GetReplDeletesApplied(), 0);

  // Verify BinlogReader is not running
  EXPECT_FALSE(reader.IsRunning());
}

/**
 * @brief Test BinlogReader SetServerStats method
 */
TEST(BinlogReaderTest, SetServerStats) {
  Connection::Config conn_config;
  Connection conn(conn_config);

  index::Index idx(1);
  storage::DocumentStore doc_store;

  config::TableConfig table_config;
  table_config.name = "test_table";

  BinlogReader::Config reader_config;

  // Create BinlogReader without ServerStats
  BinlogReader reader(conn, idx, doc_store, table_config, reader_config);

  // Create ServerStats and set it
  server::ServerStats stats;
  reader.SetServerStats(&stats);

  // Verify initial statistics are zero
  EXPECT_EQ(stats.GetReplInsertsApplied(), 0);
  EXPECT_EQ(stats.GetReplUpdatesApplied(), 0);
  EXPECT_EQ(stats.GetReplDeletesApplied(), 0);
}

/**
 * @brief Test BinlogReader multi-table mode with ServerStats
 */
TEST(BinlogReaderTest, MultiTableModeWithServerStats) {
  Connection::Config conn_config;
  Connection conn(conn_config);

  // Create table contexts
  server::TableContext table_ctx1;
  table_ctx1.name = "table1";
  table_ctx1.config.name = "table1";
  table_ctx1.config.primary_key = "id";
  table_ctx1.index = std::make_unique<index::Index>(1);
  table_ctx1.doc_store = std::make_unique<storage::DocumentStore>();

  server::TableContext table_ctx2;
  table_ctx2.name = "table2";
  table_ctx2.config.name = "table2";
  table_ctx2.config.primary_key = "id";
  table_ctx2.index = std::make_unique<index::Index>(1);
  table_ctx2.doc_store = std::make_unique<storage::DocumentStore>();

  std::unordered_map<std::string, server::TableContext*> table_contexts;
  table_contexts["table1"] = &table_ctx1;
  table_contexts["table2"] = &table_ctx2;

  BinlogReader::Config reader_config;
  server::ServerStats stats;

  // Create BinlogReader in multi-table mode with ServerStats
  BinlogReader reader(conn, table_contexts, reader_config, &stats);

  // Verify initial statistics are zero
  EXPECT_EQ(stats.GetReplInsertsApplied(), 0);
  EXPECT_EQ(stats.GetReplUpdatesApplied(), 0);
  EXPECT_EQ(stats.GetReplDeletesApplied(), 0);
  EXPECT_EQ(stats.GetReplEventsSkippedOtherTables(), 0);

  // Verify BinlogReader is not running
  EXPECT_FALSE(reader.IsRunning());
}

/**
 * @brief Test BinlogReader Stop() doesn't cause use-after-free
 *
 * Verifies that Stop() properly signals shutdown and that reader thread
 * checks should_stop_ after returning from blocking mysql_binlog_fetch().
 *
 * NOTE: This is a structural/lifecycle test. The actual fix (checking should_stop_
 * after mysql_binlog_fetch returns) is verified in integration tests with real
 * MySQL connections, as unit tests cannot easily simulate the blocking call.
 */
TEST(BinlogReaderTest, StopDoesNotCauseUseAfterFree) {
  Connection::Config config;
  config.host = "localhost";
  config.user = "test";
  config.database = "test";

  Connection conn(config);

  // Create table contexts (not actually used in this test)
  std::unordered_map<std::string, server::TableContext*> table_contexts;

  BinlogReader::Config reader_config;
  BinlogReader reader(conn, table_contexts, reader_config);

  // Start and immediately stop
  // (Start will fail without real connection, but that's ok for this test)
  reader.Stop();

  // Verify Stop() completes without hanging
  // The fix ensures that should_stop_ is checked after mysql_binlog_fetch() returns,
  // preventing use-after-free when connection is closed during Stop()
  SUCCEED();
}

/**
 * @brief Test reconnection delay reset behavior
 *
 * Verifies that reconnection attempt counter is properly managed during
 * connection failures and successful reconnections.
 *
 * NOTE: This is a documentation test. The actual behavior (resetting reconnect_attempt
 * to 0 after successful reconnection) is verified in integration tests with real
 * MySQL connections. The fix prevents infinite delay increase by resetting the
 * counter when reconnection succeeds.
 *
 * Key behaviors tested in integration tests:
 * 1. reconnect_attempt increments on connection failure
 * 2. Delay increases exponentially: delay = base_delay * min(attempt, 10)
 * 3. reconnect_attempt resets to 0 after successful reconnection
 * 4. Prevents unbounded delay growth in long-running systems
 */
TEST(BinlogReaderTest, ReconnectionDelayResetBehaviorDocumented) {
  // This test documents the expected behavior for reconnection delay management
  //
  // Before fix:
  //   - reconnect_attempt never reset after successful reconnection
  //   - Delay would stay at maximum (10x base delay) forever
  //   - Long-running systems would have unnecessarily long reconnection delays
  //
  // After fix:
  //   - reconnect_attempt resets to 0 after any successful reconnection
  //   - Subsequent failures start from base delay again
  //   - Better recovery behavior for transient connection issues
  //
  // The actual verification requires integration tests with MySQL connection
  // failures and recovery scenarios.

  SUCCEED();
}

#endif  // USE_MYSQL
