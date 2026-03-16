/**
 * @file binlog_test_fixtures.h
 * @brief Shared test fixtures and helpers for binlog reader tests
 */

#ifndef MYGRAM_TESTS_MYSQL_BINLOG_TEST_FIXTURES_H_
#define MYGRAM_TESTS_MYSQL_BINLOG_TEST_FIXTURES_H_

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

namespace binlog_test {

/**
 * @brief Helper that creates a default table configuration for tests
 */
inline config::TableConfig MakeDefaultTableConfig() {
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
    reader_config_.server_id = 12345;

    index_.Clear();
    doc_store_.Clear();
    ResetReader();
  }

  void TearDown() override {
    reader_.reset();
    doc_store_.Clear();
    index_.Clear();
  }

  void ResetReader() {
    config::MysqlConfig mysql_config;
    reader_ =
        std::make_unique<BinlogReader>(connection_, index_, doc_store_, table_config_, mysql_config, reader_config_);
  }

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

}  // namespace binlog_test

#endif  // USE_MYSQL

#endif  // MYGRAM_TESTS_MYSQL_BINLOG_TEST_FIXTURES_H_
