/**
 * @file binlog_reader_resource_test.cpp
 * @brief Test resource management in BinlogReader
 *
 * This test verifies that:
 * 1. Resources are properly cleaned up on exception
 * 2. Multiple Start/Stop cycles don't leak resources
 * 3. Thread cleanup is correct in all error paths
 */

#ifdef USE_MYSQL

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>

#include "config/config.h"
#include "index/index.h"
#include "loader/initial_loader.h"
#include "mysql/binlog_reader.h"
#include "mysql/connection.h"
#include "mysql_test_helpers.h"
#include "server/server_stats.h"
#include "storage/document_store.h"

namespace mygramdb::mysql {

class BinlogReaderResourceTest : public ::testing::Test {
 protected:
  void SetUp() override {
    if (!mygramdb::mysql::testing::ShouldRunMySQLIntegrationTests()) {
      GTEST_SKIP() << "MySQL integration tests are disabled. "
                   << "Set ENABLE_MYSQL_INTEGRATION_TESTS=1 to enable.";
    }

    // Setup basic components
    index_ = std::make_unique<index::Index>(2, 1);
    doc_store_ = std::make_unique<storage::DocumentStore>();
    stats_ = std::make_unique<server::ServerStats>();

    // Setup MySQL connection config
    config::MysqlConfig mysql_config;
    mysql_config.host = std::getenv("MYSQL_HOST") ? std::getenv("MYSQL_HOST") : "127.0.0.1";
    mysql_config.port = std::getenv("MYSQL_PORT") ? std::atoi(std::getenv("MYSQL_PORT")) : 3306;
    mysql_config.user = std::getenv("MYSQL_USER") ? std::getenv("MYSQL_USER") : "root";
    mysql_config.password = std::getenv("MYSQL_PASSWORD") ? std::getenv("MYSQL_PASSWORD") : "";
    mysql_config.database = std::getenv("MYSQL_DATABASE") ? std::getenv("MYSQL_DATABASE") : "test";

    Connection::Config conn_config;
    conn_config.host = mysql_config.host;
    conn_config.port = static_cast<uint16_t>(mysql_config.port);
    conn_config.user = mysql_config.user;
    conn_config.password = mysql_config.password;
    conn_config.database = mysql_config.database;
    conn_config.connect_timeout = 10;
    conn_config.read_timeout = 30;
    conn_config.write_timeout = 30;

    connection_ = std::make_unique<Connection>(conn_config);

    // Setup table config
    table_config_.name = "test_table";
    table_config_.primary_key = "id";
    table_config_.text_source.column = "content";
  }

  void TearDown() override {
    // Ensure reader is stopped
    if (reader_) {
      reader_->Stop();
      reader_.reset();
    }
    connection_.reset();
  }

  std::unique_ptr<Connection> connection_;
  std::unique_ptr<index::Index> index_;
  std::unique_ptr<storage::DocumentStore> doc_store_;
  std::unique_ptr<server::ServerStats> stats_;
  config::TableConfig table_config_;
  std::unique_ptr<BinlogReader> reader_;
};

/**
 * @brief Test multiple Start/Stop cycles
 */
TEST_F(BinlogReaderResourceTest, MultipleStartStopCycles) {
  // Connect to MySQL (skip test if connection fails)
  auto connect_result = connection_->Connect("test");
  if (!connect_result) {
    GTEST_SKIP() << "MySQL connection failed: " << connect_result.error().message();
  }

  // Check if GTID mode is enabled
  auto gtid_mode_enabled = connection_->IsGTIDModeEnabled();
  if (!gtid_mode_enabled) {
    GTEST_SKIP() << "Failed to query MySQL GTID mode: " << gtid_mode_enabled.error().message();
  }
  if (!*gtid_mode_enabled) {
    GTEST_SKIP() << "MySQL GTID mode is not enabled";
  }

  BinlogReader::Config reader_config;
  reader_config.queue_size = 100;
  reader_config.server_id = 12345;  // Test server ID

  reader_ = std::make_unique<BinlogReader>(*connection_, *index_, *doc_store_, table_config_, config::MysqlConfig{},
                                           reader_config, stats_.get());

  // Perform multiple Start/Stop cycles
  for (int i = 0; i < 3; ++i) {
    auto start_result = reader_->Start();
    if (!start_result) {
      // Start might fail due to table validation, which is acceptable for this test
      GTEST_SKIP() << "Start failed: " << reader_->GetLastError();
    }

    EXPECT_TRUE(reader_->IsRunning()) << "Reader should be running after Start() (cycle " << i << ")";

    // Let it run briefly
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Stop
    reader_->Stop();
    EXPECT_FALSE(reader_->IsRunning()) << "Reader should not be running after Stop() (cycle " << i << ")";

    // Small delay between cycles
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
}

/**
 * @brief Test that concurrent Start() calls are handled safely
 */
TEST_F(BinlogReaderResourceTest, ConcurrentStartAttempts) {
  // Connect to MySQL
  auto connect_result = connection_->Connect("test");
  if (!connect_result) {
    GTEST_SKIP() << "MySQL connection failed: " << connect_result.error().message();
  }

  auto gtid_mode_enabled = connection_->IsGTIDModeEnabled();
  if (!gtid_mode_enabled) {
    GTEST_SKIP() << "Failed to query MySQL GTID mode: " << gtid_mode_enabled.error().message();
  }
  if (!*gtid_mode_enabled) {
    GTEST_SKIP() << "MySQL GTID mode is not enabled";
  }

  BinlogReader::Config reader_config;
  reader_config.queue_size = 100;
  reader_config.server_id = 12345;  // Test server ID

  reader_ = std::make_unique<BinlogReader>(*connection_, *index_, *doc_store_, table_config_, config::MysqlConfig{},
                                           reader_config, stats_.get());

  std::atomic<int> successful_starts{0};
  std::atomic<int> failed_starts{0};

  // Try to start from multiple threads
  std::vector<std::thread> threads;
  for (int i = 0; i < 5; ++i) {
    threads.emplace_back([&]() {
      auto result = reader_->Start();
      if (result) {
        successful_starts++;
      } else {
        failed_starts++;
      }
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }

  // Only one Start should succeed
  EXPECT_EQ(successful_starts.load(), 1) << "Only one Start() should succeed";
  EXPECT_EQ(failed_starts.load(), 4) << "Four Start() calls should fail";

  // Clean up
  reader_->Stop();
}

/**
 * @brief Test destructor cleanup
 */
TEST_F(BinlogReaderResourceTest, DestructorCleanup) {
  // Connect to MySQL
  auto connect_result = connection_->Connect("test");
  if (!connect_result) {
    GTEST_SKIP() << "MySQL connection failed";
  }

  auto gtid_mode_enabled = connection_->IsGTIDModeEnabled();
  if (!gtid_mode_enabled) {
    GTEST_SKIP() << "Failed to query MySQL GTID mode: " << gtid_mode_enabled.error().message();
  }
  if (!*gtid_mode_enabled) {
    GTEST_SKIP() << "MySQL GTID mode is not enabled";
  }

  BinlogReader::Config reader_config;
  reader_config.queue_size = 100;
  reader_config.server_id = 12345;  // Test server ID

  {
    BinlogReader reader(*connection_, *index_, *doc_store_, table_config_, config::MysqlConfig{}, reader_config,
                        stats_.get());

    auto start_result = reader.Start();
    if (start_result) {
      EXPECT_TRUE(reader.IsRunning());
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    // Destructor should clean up properly
  }  // reader goes out of scope here

  // If we reach here without hanging, destructor worked correctly
  SUCCEED() << "Destructor completed successfully";
}

/**
 * @brief Test that queue doesn't overflow during normal operation
 */
TEST_F(BinlogReaderResourceTest, QueueSizeManagement) {
  auto connect_result = connection_->Connect("test");
  if (!connect_result) {
    GTEST_SKIP() << "MySQL connection failed";
  }

  auto gtid_mode_enabled = connection_->IsGTIDModeEnabled();
  if (!gtid_mode_enabled) {
    GTEST_SKIP() << "Failed to query MySQL GTID mode: " << gtid_mode_enabled.error().message();
  }
  if (!*gtid_mode_enabled) {
    GTEST_SKIP() << "MySQL GTID mode is not enabled";
  }

  BinlogReader::Config reader_config;
  reader_config.queue_size = 10;    // Small queue to test backpressure
  reader_config.server_id = 12345;  // Test server ID

  reader_ = std::make_unique<BinlogReader>(*connection_, *index_, *doc_store_, table_config_, config::MysqlConfig{},
                                           reader_config, stats_.get());

  auto start_result = reader_->Start();
  if (!start_result) {
    GTEST_SKIP() << "Start failed: " << reader_->GetLastError();
  }

  // Let it run for a bit
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  // Check queue size is reasonable
  size_t queue_size = reader_->GetQueueSize();
  EXPECT_LE(queue_size, reader_config.queue_size) << "Queue size should not exceed configured limit";

  reader_->Stop();
}

/**
 * @brief Test GTID management across restarts
 */
TEST_F(BinlogReaderResourceTest, GTIDPersistence) {
  auto connect_result = connection_->Connect("test");
  if (!connect_result) {
    GTEST_SKIP() << "MySQL connection failed";
  }

  auto gtid_mode_enabled = connection_->IsGTIDModeEnabled();
  if (!gtid_mode_enabled) {
    GTEST_SKIP() << "Failed to query MySQL GTID mode: " << gtid_mode_enabled.error().message();
  }
  if (!*gtid_mode_enabled) {
    GTEST_SKIP() << "MySQL GTID mode is not enabled";
  }

  BinlogReader::Config reader_config;
  reader_config.queue_size = 100;
  reader_config.server_id = 12345;  // Test server ID

  reader_ = std::make_unique<BinlogReader>(*connection_, *index_, *doc_store_, table_config_, config::MysqlConfig{},
                                           reader_config, stats_.get());

  // Set initial GTID
  std::string initial_gtid = "test-uuid:1-100";
  reader_->SetCurrentGTID(initial_gtid);

  // Verify GTID was set
  std::string retrieved_gtid = reader_->GetCurrentGTID();
  EXPECT_EQ(retrieved_gtid, initial_gtid) << "GTID should be preserved";

  // Start and stop (if possible)
  auto start_result = reader_->Start();
  if (start_result) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    reader_->Stop();

    // GTID should still be accessible after Stop
    retrieved_gtid = reader_->GetCurrentGTID();
    EXPECT_FALSE(retrieved_gtid.empty()) << "GTID should be preserved after Stop";
  }
}

TEST_F(BinlogReaderResourceTest, SnapshotAndLiveBinlogPreserveFilterTextAndEmptyStateParity) {
  auto connect_result = connection_->Connect("binlog-parity-main");
  if (!connect_result) {
    GTEST_SKIP() << "MySQL connection failed: " << connect_result.error().message();
  }
  auto connection_config = mysql::testing::GetMySQLTestConfig();
  Connection writer(connection_config);
  auto writer_connect = writer.Connect("binlog-parity-writer");
  ASSERT_TRUE(writer_connect) << writer_connect.error().message();

  const auto suffix = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count() & 0x7fffffff);
  const std::string table = "mygram_it_binlog_parity_" + suffix;
  auto cleanup = [&]() { (void)writer.ExecuteUpdate("DROP TABLE IF EXISTS " + table); };
  cleanup();

  ASSERT_TRUE(writer.ExecuteUpdate("CREATE TABLE " + table +
                                   " (id VARCHAR(32) PRIMARY KEY, title TEXT NULL, middle TEXT NULL, body TEXT NULL, "
                                   "category VARCHAR(32), deleted_at DATETIME NULL, enabled BOOLEAN NOT NULL) "
                                   "ENGINE=InnoDB"));
  ASSERT_TRUE(writer.ExecuteUpdate("INSERT INTO " + table + " VALUES ('1', 'alpha', '', 'beta', 'news', NULL, 1)"));

  table_config_.name = table;
  table_config_.database = connection_config.database;
  table_config_.primary_key = "id";
  table_config_.text_source.column.clear();
  table_config_.text_source.concat = {"title", "middle", "body"};
  table_config_.text_source.delimiter = "|";
  table_config_.ngram_size = 2;
  config::RequiredFilterConfig deleted_filter;
  deleted_filter.name = "deleted_at";
  deleted_filter.type = "datetime";
  deleted_filter.op = "IS NULL";
  table_config_.required_filters.push_back(deleted_filter);
  config::RequiredFilterConfig enabled_filter;
  enabled_filter.name = "enabled";
  enabled_filter.type = "boolean";
  enabled_filter.op = "=";
  enabled_filter.value = "1";
  table_config_.required_filters.push_back(enabled_filter);
  config::FilterConfig category_filter;
  category_filter.name = "category";
  category_filter.type = "string";
  table_config_.filters.push_back(category_filter);

  config::MysqlConfig mysql_config;
  mysql_config.datetime_timezone = "+00:00";
  loader::InitialLoader initial_loader(*connection_, *index_, *doc_store_, table_config_, mysql_config);
  auto load_result = initial_loader.Load();
  ASSERT_TRUE(load_result) << load_result.error().message();
  ASSERT_EQ(doc_store_->Size(), 1);
  auto initial_doc_id = doc_store_->GetDocId("1");
  ASSERT_TRUE(initial_doc_id.has_value());
  EXPECT_EQ(doc_store_->GetNormalizedText(*initial_doc_id), std::optional<std::string>{"alpha|beta"});

  BinlogReader::Config reader_config;
  reader_config.start_gtid = initial_loader.GetStartGTID();
  reader_config.queue_size = 100;
  reader_config.reconnect_delay_ms = 50;
  reader_config.server_id = 43115;
  reader_ = std::make_unique<BinlogReader>(*connection_, *index_, *doc_store_, table_config_, mysql_config,
                                           reader_config, stats_.get());
  auto start_result = reader_->Start();
  ASSERT_TRUE(start_result) << start_result.error().message() << ": " << reader_->GetLastError();

  auto wait_until = [&](const auto& predicate) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (std::chrono::steady_clock::now() < deadline) {
      if (predicate()) {
        return true;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return false;
  };

  // Filter-only UPDATE must re-materialize the FULL after image with the
  // configured delimiter and keep the existing postings.
  ASSERT_TRUE(writer.ExecuteUpdate("UPDATE " + table + " SET category = 'sports' WHERE id = '1'"));
  ASSERT_TRUE(wait_until([&]() {
    auto doc_id = doc_store_->GetDocId("1");
    if (!doc_id.has_value()) {
      return false;
    }
    auto category = doc_store_->GetFilterValue(*doc_id, "category");
    return category.has_value() && std::holds_alternative<std::string>(*category) &&
           std::get<std::string>(*category) == "sports";
  })) << reader_->GetLastError();
  EXPECT_EQ(index_->SearchAnd({"al"}).size(), 1);
  EXPECT_EQ(doc_store_->GetNormalizedText(*doc_store_->GetDocId("1")), std::optional<std::string>{"alpha|beta"});

  // A real present-empty after image clears old postings but retains the doc.
  ASSERT_TRUE(writer.ExecuteUpdate("UPDATE " + table + " SET title = '', middle = '', body = '' WHERE id = '1'"));
  ASSERT_TRUE(wait_until([&]() {
    auto doc_id = doc_store_->GetDocId("1");
    return doc_id.has_value() && !doc_store_->GetNormalizedText(*doc_id).has_value();
  })) << reader_->GetLastError();
  EXPECT_TRUE(index_->SearchAnd({"al"}).empty());

  // Exercise both directions of NULL and boolean required-filter transitions.
  ASSERT_TRUE(writer.ExecuteUpdate("UPDATE " + table + " SET deleted_at = '2026-07-15 00:00:00' WHERE id = '1'"));
  ASSERT_TRUE(wait_until([&]() { return !doc_store_->GetDocId("1").has_value(); })) << reader_->GetLastError();
  ASSERT_TRUE(writer.ExecuteUpdate("UPDATE " + table + " SET deleted_at = NULL WHERE id = '1'"));
  ASSERT_TRUE(wait_until([&]() { return doc_store_->GetDocId("1").has_value(); })) << reader_->GetLastError();
  ASSERT_TRUE(writer.ExecuteUpdate("UPDATE " + table + " SET enabled = 0 WHERE id = '1'"));
  ASSERT_TRUE(wait_until([&]() { return !doc_store_->GetDocId("1").has_value(); })) << reader_->GetLastError();
  ASSERT_TRUE(writer.ExecuteUpdate("UPDATE " + table + " SET enabled = 1 WHERE id = '1'"));
  ASSERT_TRUE(wait_until([&]() { return doc_store_->GetDocId("1").has_value(); })) << reader_->GetLastError();

  ASSERT_TRUE(writer.ExecuteUpdate("INSERT INTO " + table + " VALUES ('2', 'gamma', '', 'delta', 'tech', NULL, 1)"));
  ASSERT_TRUE(wait_until([&]() { return doc_store_->GetDocId("2").has_value(); })) << reader_->GetLastError();
  EXPECT_EQ(index_->SearchAnd({"ga"}).size(), 1);

  const std::string binary_primary_key("ab\0pk", 5);
  const std::string binary_text("ab\0cd|tail", 10);
  const std::string binary_filter("red\0blue", 8);
  ASSERT_TRUE(writer.ExecuteUpdate("INSERT INTO " + table +
                                   " VALUES (CONCAT('ab', CHAR(0), 'pk'), CONCAT('ab', CHAR(0), 'cd'), '', 'tail', "
                                   "CONCAT('red', CHAR(0), 'blue'), NULL, 1)"));
  ASSERT_TRUE(wait_until([&]() { return doc_store_->GetDocId(binary_primary_key).has_value(); }))
      << reader_->GetLastError();
  auto binary_doc_id = doc_store_->GetDocId(binary_primary_key);
  ASSERT_TRUE(binary_doc_id.has_value());
  EXPECT_EQ(doc_store_->GetNormalizedText(*binary_doc_id), std::optional<std::string>{binary_text});
  auto live_binary_filter = doc_store_->GetFilterValue(*binary_doc_id, "category");
  ASSERT_TRUE(live_binary_filter.has_value());
  ASSERT_TRUE(std::holds_alternative<std::string>(*live_binary_filter));
  EXPECT_EQ(std::get<std::string>(*live_binary_filter), binary_filter);

  reader_->Stop();
  ASSERT_FALSE(reader_->IsRunning());

  // Rebuild from a fresh snapshot and compare the final externally relevant
  // state to the incrementally maintained store/index.
  Connection sync_connection(connection_config);
  ASSERT_TRUE(sync_connection.Connect("binlog-parity-resync"));
  index::Index sync_index(2, 1);
  storage::DocumentStore sync_store;
  loader::InitialLoader sync_loader(sync_connection, sync_index, sync_store, table_config_, mysql_config);
  auto sync_result = sync_loader.Load();
  ASSERT_TRUE(sync_result) << sync_result.error().message();
  ASSERT_EQ(sync_store.Size(), doc_store_->Size());
  const std::vector<std::string> primary_keys = {"1", "2", binary_primary_key};
  for (const auto& primary_key : primary_keys) {
    auto live_doc_id = doc_store_->GetDocId(primary_key);
    auto sync_doc_id = sync_store.GetDocId(primary_key);
    ASSERT_TRUE(live_doc_id.has_value());
    ASSERT_TRUE(sync_doc_id.has_value());
    EXPECT_EQ(doc_store_->GetNormalizedText(*live_doc_id), sync_store.GetNormalizedText(*sync_doc_id));
    EXPECT_EQ(doc_store_->GetDocument(*live_doc_id)->filters, sync_store.GetDocument(*sync_doc_id)->filters);
  }
  EXPECT_EQ(index_->SearchAnd({"ga"}).size(), sync_index.SearchAnd({"ga"}).size());
  EXPECT_EQ(index_->SearchAnd({"al"}).size(), sync_index.SearchAnd({"al"}).size());

  cleanup();
}

TEST_F(BinlogReaderResourceTest, DdlSchemaValidationContinuesSafeChangesAndStopsBeforeUnsafeGtid) {
  auto connect_result = connection_->Connect("ddl-schema-main");
  if (!connect_result) {
    GTEST_SKIP() << "MySQL connection failed: " << connect_result.error().message();
  }
  const auto connection_config = mysql::testing::GetMySQLTestConfig();
  Connection writer(connection_config);
  auto writer_connect = writer.Connect("ddl-schema-writer");
  ASSERT_TRUE(writer_connect) << writer_connect.error().message();

  auto wait_until = [](const auto& predicate) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (std::chrono::steady_clock::now() < deadline) {
      if (predicate())
        return true;
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return predicate();
  };

  const std::vector<std::string> unsafe_ddls = {
      "ALTER TABLE {table} RENAME COLUMN content TO renamed_content",
      "ALTER TABLE {table} DROP PRIMARY KEY, ADD PRIMARY KEY (alternate_id)",
      "ALTER TABLE {table} MODIFY COLUMN status VARCHAR(16) NOT NULL",
      "RENAME TABLE {table} TO {table}_renamed",
  };

  for (size_t scenario = 0; scenario < unsafe_ddls.size(); ++scenario) {
    const auto suffix = std::to_string(
        (std::chrono::steady_clock::now().time_since_epoch().count() + static_cast<int64_t>(scenario)) & 0x7fffffff);
    const std::string table = "mygram_it_ddl_guard_" + suffix;
    const std::string renamed_table = table + "_renamed";
    auto cleanup = [&]() {
      (void)writer.ExecuteUpdate("DROP TABLE IF EXISTS " + table);
      (void)writer.ExecuteUpdate("DROP TABLE IF EXISTS " + renamed_table);
    };
    cleanup();
    ASSERT_TRUE(writer.ExecuteUpdate("CREATE TABLE " + table +
                                     " (id BIGINT NOT NULL PRIMARY KEY, alternate_id BIGINT NOT NULL, "
                                     "content TEXT, status INT NOT NULL, UNIQUE KEY uq_alternate (alternate_id)) "
                                     "ENGINE=InnoDB"));
    ASSERT_TRUE(writer.ExecuteUpdate("INSERT INTO " + table + " VALUES (1, 101, 'alpha', 1)"));

    index_->Clear();
    doc_store_->Clear();
    table_config_ = {};
    table_config_.name = table;
    table_config_.database = connection_config.database;
    table_config_.primary_key = "id";
    table_config_.text_source.column = "content";
    table_config_.filters.push_back({"status", "int", false, false, ""});

    config::MysqlConfig mysql_config;
    loader::InitialLoader loader(*connection_, *index_, *doc_store_, table_config_, mysql_config);
    auto load = loader.Load();
    ASSERT_TRUE(load) << load.error().message();

    BinlogReader::Config reader_config;
    reader_config.start_gtid = loader.GetStartGTID();
    reader_config.queue_size = 100;
    reader_config.reconnect_delay_ms = 50;
    reader_config.server_id = static_cast<uint32_t>(43210 + scenario);
    reader_ = std::make_unique<BinlogReader>(*connection_, *index_, *doc_store_, table_config_, mysql_config,
                                             reader_config, stats_.get());
    auto start = reader_->Start();
    ASSERT_TRUE(start) << start.error().message() << ": " << reader_->GetLastError();

    const uint64_t before_safe = reader_->GetProcessedEvents();
    ASSERT_TRUE(writer.ExecuteUpdate("ALTER TABLE " + table + " ADD COLUMN unrelated INT NULL"));
    ASSERT_TRUE(wait_until([&] { return reader_->GetProcessedEvents() > before_safe; }));
    EXPECT_TRUE(reader_->IsRunning());
    EXPECT_FALSE(reader_->HasSchemaIncompatibleError());

    const uint64_t before_index = reader_->GetProcessedEvents();
    ASSERT_TRUE(writer.ExecuteUpdate("ALTER TABLE " + table + " ADD INDEX idx_status (status)"));
    ASSERT_TRUE(wait_until([&] { return reader_->GetProcessedEvents() > before_index; }));
    ASSERT_TRUE(reader_->IsRunning());
    const std::string safe_gtid = reader_->GetCurrentGTID();

    std::string ddl = unsafe_ddls[scenario];
    size_t marker = 0;
    while ((marker = ddl.find("{table}", marker)) != std::string::npos) {
      ddl.replace(marker, 7, table);
      marker += table.size();
    }
    ASSERT_TRUE(writer.ExecuteUpdate(ddl)) << ddl;
    ASSERT_TRUE(wait_until([&] { return !reader_->IsRunning(); })) << ddl;
    EXPECT_TRUE(reader_->HasSchemaIncompatibleError()) << ddl;
    EXPECT_NE(reader_->GetLastError().find("SCHEMA_INCOMPATIBLE"), std::string::npos) << ddl;
    EXPECT_EQ(reader_->GetCurrentGTID(), safe_gtid) << "unsafe DDL GTID advanced: " << ddl;

    reader_->Stop();
    reader_.reset();
    cleanup();
  }
}

}  // namespace mygramdb::mysql

#endif  // USE_MYSQL
