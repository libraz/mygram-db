/**
 * @file structured_log_test.cpp
 * @brief Tests for structured logging utilities
 */

#include "utils/structured_log.h"

#include <gtest/gtest.h>
#include <spdlog/sinks/ostream_sink.h>
#include <spdlog/spdlog.h>

#include <sstream>

using namespace mygram::utils;

/**
 * @brief Test fixture for structured logging tests
 *
 * Captures log output to a stringstream for verification.
 */
class StructuredLogTest : public ::testing::Test {
 protected:
  std::ostringstream log_stream_;
  std::shared_ptr<spdlog::logger> original_logger_;

  void SetUp() override {
    // Save original logger
    original_logger_ = spdlog::default_logger();

    // Create logger that writes to stringstream
    auto sink = std::make_shared<spdlog::sinks::ostream_sink_mt>(log_stream_);
    auto logger = std::make_shared<spdlog::logger>("test", sink);
    logger->set_pattern("%v");  // Only log the message, no timestamp/level
    spdlog::set_default_logger(logger);
  }

  void TearDown() override {
    // Restore original logger
    spdlog::set_default_logger(original_logger_);
  }

  std::string GetLogOutput() {
    spdlog::default_logger()->flush();
    return log_stream_.str();
  }
};

/**
 * @brief Test basic structured log with event only
 */
TEST_F(StructuredLogTest, EventOnly) {
  StructuredLog().Event("test_event").Info();

  std::string output = GetLogOutput();
  EXPECT_NE(output.find("\"event\":\"test_event\""), std::string::npos);
  EXPECT_EQ(output.find("\"message\""), std::string::npos);  // No message field
}

/**
 * @brief Test structured log with event and message
 */
TEST_F(StructuredLogTest, EventAndMessage) {
  StructuredLog().Event("test_event").Message("Test message").Info();

  std::string output = GetLogOutput();
  EXPECT_NE(output.find("\"event\":\"test_event\""), std::string::npos);
  EXPECT_NE(output.find("\"message\":\"Test message\""), std::string::npos);
}

/**
 * @brief Test structured log with string fields
 */
TEST_F(StructuredLogTest, StringFields) {
  StructuredLog().Event("test_event").Field("field1", "value1").Field("field2", "value2").Info();

  std::string output = GetLogOutput();
  EXPECT_NE(output.find("\"field1\":\"value1\""), std::string::npos);
  EXPECT_NE(output.find("\"field2\":\"value2\""), std::string::npos);
}

/**
 * @brief Test structured log with integer fields
 */
TEST_F(StructuredLogTest, IntegerFields) {
  StructuredLog()
      .Event("test_event")
      .Field("int_field", static_cast<int64_t>(42))
      .Field("uint_field", static_cast<uint64_t>(100))
      .Info();

  std::string output = GetLogOutput();
  EXPECT_NE(output.find("\"int_field\":\"42\""), std::string::npos);
  EXPECT_NE(output.find("\"uint_field\":\"100\""), std::string::npos);
}

/**
 * @brief Test structured log with boolean fields
 */
TEST_F(StructuredLogTest, BooleanFields) {
  StructuredLog().Event("test_event").Field("bool_true", true).Field("bool_false", false).Info();

  std::string output = GetLogOutput();
  EXPECT_NE(output.find("\"bool_true\":true"), std::string::npos);
  EXPECT_NE(output.find("\"bool_false\":false"), std::string::npos);
}

/**
 * @brief Test structured log with double fields
 */
TEST_F(StructuredLogTest, DoubleFields) {
  StructuredLog().Event("test_event").Field("double_field", 3.14159).Info();

  std::string output = GetLogOutput();
  EXPECT_NE(output.find("\"double_field\":"), std::string::npos);
  EXPECT_NE(output.find("3.14"), std::string::npos);
}

/**
 * @brief Test JSON escaping for special characters
 */
TEST_F(StructuredLogTest, JSONEscaping) {
  StructuredLog()
      .Event("test_event")
      .Field("field_with_quotes", "value with \"quotes\"")
      .Field("field_with_newline", "line1\nline2")
      .Field("field_with_backslash", "path\\to\\file")
      .Info();

  std::string output = GetLogOutput();
  EXPECT_NE(output.find("\\\"quotes\\\""), std::string::npos);
  EXPECT_NE(output.find("\\n"), std::string::npos);
  EXPECT_NE(output.find("\\\\"), std::string::npos);
}

/**
 * @brief Test error level logging
 */
TEST_F(StructuredLogTest, ErrorLevel) {
  StructuredLog().Event("error_event").Error();

  std::string output = GetLogOutput();
  EXPECT_NE(output.find("\"event\":\"error_event\""), std::string::npos);
}

/**
 * @brief Test warning level logging
 */
TEST_F(StructuredLogTest, WarnLevel) {
  StructuredLog().Event("warn_event").Warn();

  std::string output = GetLogOutput();
  EXPECT_NE(output.find("\"event\":\"warn_event\""), std::string::npos);
}

/**
 * @brief Test critical level logging
 */
TEST_F(StructuredLogTest, CriticalLevel) {
  StructuredLog().Event("critical_event").Critical();

  std::string output = GetLogOutput();
  EXPECT_NE(output.find("\"event\":\"critical_event\""), std::string::npos);
}

/**
 * @brief Test LogMySQLConnectionError helper
 */
TEST_F(StructuredLogTest, MySQLConnectionErrorHelper) {
  LogMySQLConnectionError("localhost", 3306, "Connection refused");

  std::string output = GetLogOutput();
  EXPECT_NE(output.find("\"event\":\"mysql_connection_error\""), std::string::npos);
  EXPECT_NE(output.find("\"host\":\"localhost\""), std::string::npos);
  EXPECT_NE(output.find("\"port\":\"3306\""), std::string::npos);
  EXPECT_NE(output.find("\"error\":\"Connection refused\""), std::string::npos);
}

/**
 * @brief Test LogMySQLQueryError helper
 */
TEST_F(StructuredLogTest, MySQLQueryErrorHelper) {
  LogMySQLQueryError("SELECT * FROM table", "Table not found");

  std::string output = GetLogOutput();
  EXPECT_NE(output.find("\"event\":\"mysql_query_error\""), std::string::npos);
  EXPECT_NE(output.find("\"query\":\"SELECT * FROM table\""), std::string::npos);
  EXPECT_NE(output.find("\"error\":\"Table not found\""), std::string::npos);
}

/**
 * @brief Test LogBinlogError helper
 */
TEST_F(StructuredLogTest, BinlogErrorHelper) {
  LogBinlogError("connection_lost", "uuid:1-10", "Connection timeout", 3);

  std::string output = GetLogOutput();
  EXPECT_NE(output.find("\"event\":\"binlog_error\""), std::string::npos);
  EXPECT_NE(output.find("\"type\":\"connection_lost\""), std::string::npos);
  EXPECT_NE(output.find("\"gtid\":\"uuid:1-10\""), std::string::npos);
  EXPECT_NE(output.find("\"retry_count\":\"3\""), std::string::npos);
  EXPECT_NE(output.find("\"error\":\"Connection timeout\""), std::string::npos);
}

/**
 * @brief Test LogStorageError helper
 */
TEST_F(StructuredLogTest, StorageErrorHelper) {
  LogStorageError("read", "/path/to/file.dump", "File not found");

  std::string output = GetLogOutput();
  EXPECT_NE(output.find("\"event\":\"storage_error\""), std::string::npos);
  EXPECT_NE(output.find("\"operation\":\"read\""), std::string::npos);
  EXPECT_NE(output.find("\"filepath\":\"/path/to/file.dump\""), std::string::npos);
  EXPECT_NE(output.find("\"error\":\"File not found\""), std::string::npos);
}

/**
 * @brief Test LogQueryParseError helper
 */
TEST_F(StructuredLogTest, QueryParseErrorHelper) {
  LogQueryParseError("INVALID QUERY", "Unexpected token", 8);

  std::string output = GetLogOutput();
  EXPECT_NE(output.find("\"event\":\"query_parse_error\""), std::string::npos);
  EXPECT_NE(output.find("\"query\":\"INVALID QUERY\""), std::string::npos);
  EXPECT_NE(output.find("\"error\":\"Unexpected token\""), std::string::npos);
  EXPECT_NE(output.find("\"position\":\"8\""), std::string::npos);
}

/**
 * @brief Test long query truncation
 */
TEST_F(StructuredLogTest, LongQueryTruncation) {
  std::string long_query(300, 'A');  // 300 character query
  LogMySQLQueryError(long_query, "Query too long");

  std::string output = GetLogOutput();
  // Query should be truncated to 200 characters
  EXPECT_EQ(output.find(std::string(201, 'A')), std::string::npos);
  EXPECT_NE(output.find(std::string(200, 'A')), std::string::npos);
}

/**
 * @brief Test chaining multiple fields
 */
TEST_F(StructuredLogTest, MultipleFieldTypes) {
  StructuredLog()
      .Event("mixed_event")
      .Field("str_field", "test")
      .Field("int_field", static_cast<int64_t>(42))
      .Field("bool_field", true)
      .Field("double_field", 3.14)
      .Message("Mixed types test")
      .Info();

  std::string output = GetLogOutput();
  EXPECT_NE(output.find("\"str_field\":\"test\""), std::string::npos);
  EXPECT_NE(output.find("\"int_field\":\"42\""), std::string::npos);
  EXPECT_NE(output.find("\"bool_field\":true"), std::string::npos);
  EXPECT_NE(output.find("\"double_field\":"), std::string::npos);
  EXPECT_NE(output.find("\"message\":\"Mixed types test\""), std::string::npos);
}
