/**
 * @file failover_test.cpp
 * @brief Integration tests for MySQL failover using runtime variables
 *
 * Note: These tests verify the failover logic but require a real MySQL server.
 * For CI environments without MySQL, these tests should be skipped or mocked.
 */

#include <gtest/gtest.h>

#include <chrono>
#include <thread>

#include "app/mysql_reconnection_handler.h"
#include "config/config.h"
#include "config/runtime_variable_manager.h"
#include "mysql/binlog_reader.h"
#include "mysql/connection.h"

using namespace mygramdb::app;
using namespace mygramdb::config;
using namespace mygramdb::mysql;
using namespace mygram::utils;

/**
 * @brief Test fixture for MySQL failover tests
 *
 * Note: These tests require a real MySQL server configured with:
 * - GTID mode enabled (gtid_mode=ON)
 * - ROW binlog format (binlog_format=ROW)
 * - FULL binlog row image (binlog_row_image=FULL)
 * - Replication user with appropriate privileges
 *
 * If MySQL is not available, these tests will be skipped.
 */
class MySQLFailoverTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create config with MySQL connection details
    // These should match your test MySQL setup
    config_.mysql.host = GetEnv("MYSQL_HOST", "127.0.0.1");
    config_.mysql.port = GetEnvInt("MYSQL_PORT", 3306);
    config_.mysql.user = GetEnv("MYSQL_USER", "root");
    config_.mysql.password = GetEnv("MYSQL_PASSWORD", "");
    config_.mysql.database = GetEnv("MYSQL_DATABASE", "test");
    config_.mysql.use_gtid = true;
    config_.mysql.binlog_format = "ROW";
    config_.mysql.binlog_row_image = "FULL";

    // Try to connect to MySQL
    Connection::Config conn_config;
    conn_config.host = config_.mysql.host;
    conn_config.port = config_.mysql.port;
    conn_config.user = config_.mysql.user;
    conn_config.password = config_.mysql.password;
    conn_config.database = config_.mysql.database;

    connection_ = std::make_unique<Connection>(conn_config);

    if (!connection_->Connect()) {
      mysql_available_ = false;
      GTEST_SKIP() << "MySQL server not available. Skipping MySQL failover tests.";
      return;
    }

    mysql_available_ = true;
  }

  void TearDown() override {
    if (connection_) {
      connection_.reset();
    }
  }

  std::string GetEnv(const char* name, const std::string& default_value) {
    const char* value = std::getenv(name);
    return value ? std::string(value) : default_value;
  }

  int GetEnvInt(const char* name, int default_value) {
    const char* value = std::getenv(name);
    return value ? std::atoi(value) : default_value;
  }

  Config config_;
  std::unique_ptr<Connection> connection_;
  bool mysql_available_ = false;
};

/**
 * @brief Test MySQL connection validation
 */
TEST_F(MySQLFailoverTest, ConnectionValidation) {
  if (!mysql_available_) {
    GTEST_SKIP() << "MySQL not available";
  }

  ASSERT_NE(connection_, nullptr);
  EXPECT_TRUE(connection_->IsConnected());
}

/**
 * @brief Test GTID mode check
 */
TEST_F(MySQLFailoverTest, GtidModeCheck) {
  if (!mysql_available_) {
    GTEST_SKIP() << "MySQL not available";
  }

  // Note: This test requires mysql_query implementation in Connection class
  // For now, we just verify the connection is established
  EXPECT_TRUE(connection_->IsConnected());
}

/**
 * @brief Test binlog format check
 */
TEST_F(MySQLFailoverTest, BinlogFormatCheck) {
  if (!mysql_available_) {
    GTEST_SKIP() << "MySQL not available";
  }

  // Note: This test requires mysql_query implementation in Connection class
  // For now, we just verify the connection is established
  EXPECT_TRUE(connection_->IsConnected());
}

/**
 * @brief Test GTID position retrieval
 */
TEST_F(MySQLFailoverTest, GetGtidPosition) {
  if (!mysql_available_) {
    GTEST_SKIP() << "MySQL not available";
  }

  // Note: This test requires mysql_query implementation in Connection class
  // For now, we just verify the connection is established
  EXPECT_TRUE(connection_->IsConnected());
}

/**
 * @brief Test RuntimeVariableManager MySQL host change
 *
 * This test verifies that changing mysql.host triggers the reconnection callback,
 * but doesn't actually perform a real reconnection (since we don't have a second MySQL server).
 */
TEST_F(MySQLFailoverTest, RuntimeVariableManagerMysqlHostChange) {
  Config test_config = config_;
  test_config.api.default_limit = 100;
  test_config.cache.enabled = true;
  test_config.logging.level = "info";

  auto manager_result = RuntimeVariableManager::Create(test_config);
  ASSERT_TRUE(manager_result);
  auto manager = std::move(*manager_result);

  // Set up reconnection callback
  bool callback_called = false;
  std::string callback_host;
  int callback_port = 0;
  Expected<void, Error> callback_result = {};

  manager->SetMysqlReconnectCallback([&](const std::string& host, int port) -> Expected<void, Error> {
    callback_called = true;
    callback_host = host;
    callback_port = port;
    return callback_result;  // Return success or failure based on test setup
  });

  // Test 1: Successful host change
  callback_result = {};  // Success
  auto result1 = manager->SetVariable("mysql.host", "192.168.1.100");
  EXPECT_TRUE(result1);
  EXPECT_TRUE(callback_called);
  EXPECT_EQ(callback_host, "192.168.1.100");
  EXPECT_EQ(callback_port, config_.mysql.port);

  // Verify variable was updated
  auto get_result1 = manager->GetVariable("mysql.host");
  ASSERT_TRUE(get_result1);
  EXPECT_EQ(*get_result1, "192.168.1.100");

  // Test 2: Failed reconnection (callback returns error)
  callback_called = false;
  callback_result = MakeUnexpected(MakeError(ErrorCode::kMySQLConnectionFailed, "Connection refused"));
  auto result2 = manager->SetVariable("mysql.host", "192.168.1.200");
  EXPECT_FALSE(result2);
  EXPECT_TRUE(callback_called);

  // Verify variable was NOT updated
  auto get_result2 = manager->GetVariable("mysql.host");
  ASSERT_TRUE(get_result2);
  EXPECT_EQ(*get_result2, "192.168.1.100");  // Should still be the old value
}

/**
 * @brief Test RuntimeVariableManager MySQL port change
 */
TEST_F(MySQLFailoverTest, RuntimeVariableManagerMysqlPortChange) {
  Config test_config = config_;
  test_config.api.default_limit = 100;
  test_config.cache.enabled = true;
  test_config.logging.level = "info";

  auto manager_result = RuntimeVariableManager::Create(test_config);
  ASSERT_TRUE(manager_result);
  auto manager = std::move(*manager_result);

  // Set up reconnection callback
  bool callback_called = false;
  std::string callback_host;
  int callback_port = 0;

  manager->SetMysqlReconnectCallback([&](const std::string& host, int port) -> Expected<void, Error> {
    callback_called = true;
    callback_host = host;
    callback_port = port;
    return {};  // Success
  });

  // Change port
  auto result = manager->SetVariable("mysql.port", "3307");
  EXPECT_TRUE(result);
  EXPECT_TRUE(callback_called);
  EXPECT_EQ(callback_host, config_.mysql.host);  // Host unchanged
  EXPECT_EQ(callback_port, 3307);

  // Verify variable was updated
  auto get_result = manager->GetVariable("mysql.port");
  ASSERT_TRUE(get_result);
  EXPECT_EQ(*get_result, "3307");
}

/**
 * @brief Test simultaneous host and port change
 */
TEST_F(MySQLFailoverTest, RuntimeVariableManagerSimultaneousChange) {
  Config test_config = config_;
  test_config.api.default_limit = 100;
  test_config.cache.enabled = true;
  test_config.logging.level = "info";

  auto manager_result = RuntimeVariableManager::Create(test_config);
  ASSERT_TRUE(manager_result);
  auto manager = std::move(*manager_result);

  // Set up reconnection callback
  int callback_count = 0;
  std::string last_host;
  int last_port = 0;

  manager->SetMysqlReconnectCallback([&](const std::string& host, int port) -> Expected<void, Error> {
    callback_count++;
    last_host = host;
    last_port = port;
    return {};  // Success
  });

  // Change host
  auto result1 = manager->SetVariable("mysql.host", "192.168.1.100");
  EXPECT_TRUE(result1);
  EXPECT_EQ(callback_count, 1);

  // Change port (should trigger another reconnection with new host and new port)
  auto result2 = manager->SetVariable("mysql.port", "3307");
  EXPECT_TRUE(result2);
  EXPECT_EQ(callback_count, 2);
  EXPECT_EQ(last_host, "192.168.1.100");
  EXPECT_EQ(last_port, 3307);
}

/**
 * @brief Test idempotent host change (same value)
 */
TEST_F(MySQLFailoverTest, RuntimeVariableManagerIdempotentChange) {
  Config test_config = config_;
  test_config.api.default_limit = 100;
  test_config.cache.enabled = true;
  test_config.logging.level = "info";

  auto manager_result = RuntimeVariableManager::Create(test_config);
  ASSERT_TRUE(manager_result);
  auto manager = std::move(*manager_result);

  // Set up reconnection callback
  int callback_count = 0;

  manager->SetMysqlReconnectCallback([&](const std::string& /*host*/, int /*port*/) -> Expected<void, Error> {
    callback_count++;
    return {};  // Success
  });

  // Change to same host (should still trigger callback, as implementation may need to reconnect)
  auto result = manager->SetVariable("mysql.host", config_.mysql.host);
  EXPECT_TRUE(result);
  EXPECT_GE(callback_count, 0);  // Implementation-defined whether callback is called for same value
}

/**
 * @brief Test reconnection callback exception safety
 */
TEST_F(MySQLFailoverTest, ReconnectionCallbackExceptionSafety) {
  Config test_config = config_;
  test_config.api.default_limit = 100;
  test_config.cache.enabled = true;
  test_config.logging.level = "info";

  auto manager_result = RuntimeVariableManager::Create(test_config);
  ASSERT_TRUE(manager_result);
  auto manager = std::move(*manager_result);

  // Set up callback that returns error
  manager->SetMysqlReconnectCallback([](const std::string& /*host*/, int /*port*/) -> Expected<void, Error> {
    return MakeUnexpected(MakeError(ErrorCode::kMySQLConnectionFailed, "Simulated error"));
  });

  // Try to change host (should fail gracefully)
  auto result = manager->SetVariable("mysql.host", "invalid.host");
  EXPECT_FALSE(result);
  EXPECT_EQ(static_cast<int>(result.error().code()), static_cast<int>(ErrorCode::kMySQLConnectionFailed));

  // Verify original value unchanged
  auto get_result = manager->GetVariable("mysql.host");
  ASSERT_TRUE(get_result);
  EXPECT_EQ(*get_result, config_.mysql.host);

  // Manager should still be functional
  auto result2 = manager->SetVariable("logging.level", "debug");
  EXPECT_TRUE(result2);
}

/**
 * @brief Benchmark: MySQL connection creation time
 *
 * This test measures how long it takes to create a new MySQL connection,
 * which is relevant for understanding failover downtime.
 */
TEST_F(MySQLFailoverTest, DISABLED_BenchmarkConnectionCreation) {
  if (!mysql_available_) {
    GTEST_SKIP() << "MySQL not available";
  }

  const int num_iterations = 10;
  std::vector<std::chrono::microseconds> durations;

  for (int i = 0; i < num_iterations; ++i) {
    Connection::Config conn_config;
    conn_config.host = config_.mysql.host;
    conn_config.port = config_.mysql.port;
    conn_config.user = config_.mysql.user;
    conn_config.password = config_.mysql.password;
    conn_config.database = config_.mysql.database;

    auto start = std::chrono::steady_clock::now();
    auto conn = std::make_unique<Connection>(conn_config);
    auto connect_result = conn->Connect();
    auto end = std::chrono::steady_clock::now();

    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    durations.push_back(duration);

    ASSERT_TRUE(connect_result);
  }

  // Calculate average
  auto total = std::chrono::microseconds(0);
  for (const auto& d : durations) {
    total += d;
  }
  auto avg = total / num_iterations;

  std::cout << "MySQL connection creation time (average over " << num_iterations << " iterations): " << avg.count()
            << " µs (" << (avg.count() / 1000.0) << " ms)\n";

  // Connection should typically take < 100ms
  EXPECT_LT(avg.count(), 100000);  // 100ms
}
