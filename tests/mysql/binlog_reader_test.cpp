/**
 * @file binlog_reader_test.cpp
 * @brief Unit tests for binlog reader
 */

#include "mysql/binlog_reader.h"

#include <gtest/gtest.h>

#ifdef USE_MYSQL

using namespace mygramdb::mysql;
using namespace mygramdb;

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

#endif  // USE_MYSQL
