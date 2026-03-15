/**
 * @file binlog_reader_resource_unit_test.cpp
 * @brief Unit tests for BinlogReader resource management (no MySQL connection required)
 *
 * These tests verify BinlogReader behavior when MySQL is not connected:
 * 1. Start() fails gracefully without connection
 * 2. Start() validates server_id before connecting
 * 3. Stop() is safe after a failed Start()
 */

#ifdef USE_MYSQL

#include <gtest/gtest.h>

#include <string>

#include "config/config.h"
#include "index/index.h"
#include "mysql/binlog_reader.h"
#include "mysql/connection.h"
#include "server/server_stats.h"
#include "storage/document_store.h"

namespace mygramdb::mysql {

class BinlogReaderResourceUnitTest : public ::testing::Test {
 protected:
  void SetUp() override {
    index_ = std::make_unique<index::Index>(2, 1);
    doc_store_ = std::make_unique<storage::DocumentStore>();
    stats_ = std::make_unique<server::ServerStats>();

    Connection::Config conn_config;
    conn_config.host = "127.0.0.1";
    conn_config.port = 3306;
    conn_config.user = "test";
    conn_config.password = "test";
    conn_config.database = "test";
    conn_config.connect_timeout = 10;
    conn_config.read_timeout = 30;
    conn_config.write_timeout = 30;

    connection_ = std::make_unique<Connection>(conn_config);

    table_config_.name = "test_table";
    table_config_.primary_key = "id";
    table_config_.text_source.column = "content";
  }

  void TearDown() override {
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
TEST_F(BinlogReaderResourceUnitTest, StartFailsWithoutConnection) {
  BinlogReader::Config reader_config;
  reader_config.queue_size = 100;
  reader_config.server_id = 12345;

  reader_ = std::make_unique<BinlogReader>(*connection_, *index_, *doc_store_, table_config_, config::MysqlConfig{},
                                           reader_config, stats_.get());

  auto result = reader_->Start();
  EXPECT_FALSE(result) << "Start should fail without MySQL connection";
  EXPECT_FALSE(reader_->IsRunning()) << "Reader should not be running after failed start";
}

/**
 * @brief Test that Start() fails when server_id is 0
 */
TEST_F(BinlogReaderResourceUnitTest, StartFailsWithZeroServerId) {
  BinlogReader::Config reader_config;
  reader_config.queue_size = 100;
  reader_config.server_id = 0;

  reader_ = std::make_unique<BinlogReader>(*connection_, *index_, *doc_store_, table_config_, config::MysqlConfig{},
                                           reader_config, stats_.get());

  auto result = reader_->Start();
  EXPECT_FALSE(result) << "Start should fail with server_id=0";

  std::string error_msg = reader_->GetLastError();
  EXPECT_TRUE(error_msg.find("server_id") != std::string::npos)
      << "Error message should mention server_id, got: " << error_msg;

  EXPECT_FALSE(reader_->IsRunning()) << "Reader should not be running after failed start";
}

/**
 * @brief Test that Stop() can be called safely even if Start() failed
 */
TEST_F(BinlogReaderResourceUnitTest, StopAfterFailedStart) {
  BinlogReader::Config reader_config;
  reader_config.queue_size = 100;
  reader_config.server_id = 12345;

  reader_ = std::make_unique<BinlogReader>(*connection_, *index_, *doc_store_, table_config_, config::MysqlConfig{},
                                           reader_config, stats_.get());

  auto start_result = reader_->Start();
  EXPECT_FALSE(start_result);

  EXPECT_NO_THROW(reader_->Stop()) << "Stop() should be safe after failed Start()";
  EXPECT_FALSE(reader_->IsRunning());
}

}  // namespace mygramdb::mysql

#endif  // USE_MYSQL
