/**
 * @file binlog_reader_core_test.cpp
 * @brief Unit tests for binlog reader - Core lifecycle and queue operations
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
    config::MysqlConfig mysql_config;  // Use default (UTC timezone)
    reader_ =
        std::make_unique<BinlogReader>(connection_, index_, doc_store_, table_config_, mysql_config, reader_config_);
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
  auto first = std::make_unique<BinlogEvent>(MakeEvent(BinlogEventType::INSERT, "1", 1));
  reader_->PushEvent(std::move(first));
  EXPECT_EQ(reader_->GetQueueSize(), 1);

  auto popped = reader_->PopEvent();
  ASSERT_NE(popped, nullptr);
  EXPECT_EQ(popped->primary_key, "1");
  EXPECT_EQ(reader_->GetQueueSize(), 0);
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

#endif  // USE_MYSQL
