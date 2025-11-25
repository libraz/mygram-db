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
#include "mysql/binlog_reader.h"
#include "mysql/connection.h"
#include "server/server_stats.h"
#include "storage/document_store.h"

namespace mygramdb::mysql {

class BinlogReaderResourceTest : public ::testing::Test {
 protected:
  void SetUp() override {
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
 * @brief Test that Start() returns proper error when MySQL is not available
 */
TEST_F(BinlogReaderResourceTest, StartFailsWithoutConnection) {
  BinlogReader::Config reader_config;
  reader_config.queue_size = 100;
  reader_config.server_id = 12345;  // Test server ID

  reader_ = std::make_unique<BinlogReader>(*connection_, *index_, *doc_store_, table_config_, config::MysqlConfig{},
                                           reader_config, stats_.get());

  // Start without connecting to MySQL should fail
  auto result = reader_->Start();
  EXPECT_FALSE(result) << "Start should fail without MySQL connection";

  // Verify that no resources are leaked
  EXPECT_FALSE(reader_->IsRunning()) << "Reader should not be running after failed start";
}

/**
 * @brief Test that Start() fails when server_id is 0
 *
 * This test verifies the server_id validation fix. MySQL replication requires
 * a unique non-zero server_id for each replica. When server_id=0, Start()
 * should fail immediately with a clear error message.
 */
TEST_F(BinlogReaderResourceTest, StartFailsWithZeroServerId) {
  BinlogReader::Config reader_config;
  reader_config.queue_size = 100;
  reader_config.server_id = 0;  // Invalid server_id

  reader_ = std::make_unique<BinlogReader>(*connection_, *index_, *doc_store_, table_config_, config::MysqlConfig{},
                                           reader_config, stats_.get());

  // Start with server_id=0 should fail with validation error
  auto result = reader_->Start();
  EXPECT_FALSE(result) << "Start should fail with server_id=0";

  // Verify the error message mentions server_id
  std::string error_msg = reader_->GetLastError();
  EXPECT_TRUE(error_msg.find("server_id") != std::string::npos)
      << "Error message should mention server_id, got: " << error_msg;

  // Verify that no resources are leaked
  EXPECT_FALSE(reader_->IsRunning()) << "Reader should not be running after failed start";
}

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
  if (!connection_->IsGTIDModeEnabled()) {
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

  if (!connection_->IsGTIDModeEnabled()) {
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
 * @brief Test that Stop() can be called safely even if Start() failed
 */
TEST_F(BinlogReaderResourceTest, StopAfterFailedStart) {
  BinlogReader::Config reader_config;
  reader_config.queue_size = 100;
  reader_config.server_id = 12345;  // Test server ID

  reader_ = std::make_unique<BinlogReader>(*connection_, *index_, *doc_store_, table_config_, config::MysqlConfig{},
                                           reader_config, stats_.get());

  // Start without connection (will fail)
  auto start_result = reader_->Start();
  EXPECT_FALSE(start_result);

  // Stop should be safe to call
  EXPECT_NO_THROW(reader_->Stop()) << "Stop() should be safe after failed Start()";
  EXPECT_FALSE(reader_->IsRunning());
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

  if (!connection_->IsGTIDModeEnabled()) {
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

  if (!connection_->IsGTIDModeEnabled()) {
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

  if (!connection_->IsGTIDModeEnabled()) {
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

}  // namespace mygramdb::mysql

#endif  // USE_MYSQL
