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

  EXPECT_EQ(insert_event.primary_key, "1");
  EXPECT_EQ(update_event.primary_key, "2");
  EXPECT_EQ(delete_event.primary_key, "3");
}

#endif  // USE_MYSQL
