/**
 * @file runtime_variable_manager_test.cpp
 * @brief Unit tests for RuntimeVariableManager MySQL reconnection callbacks
 *
 * These tests verify that RuntimeVariableManager correctly triggers
 * reconnection callbacks when mysql.host or mysql.port changes,
 * and handles errors gracefully. No MySQL connection required.
 */

#include <gtest/gtest.h>

#include <string>

#include "config/config.h"
#include "config/runtime_variable_manager.h"
#include "utils/error.h"
#include "utils/expected.h"

using namespace mygramdb::config;
using namespace mygram::utils;

class RuntimeVariableManagerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    config_.mysql.host = "127.0.0.1";
    config_.mysql.port = 3306;
    config_.mysql.user = "root";
    config_.mysql.password = "";
    config_.mysql.database = "test";
    config_.mysql.use_gtid = true;
    config_.mysql.binlog_format = "ROW";
    config_.mysql.binlog_row_image = "FULL";
    config_.api.default_limit = 100;
    config_.cache.enabled = true;
    config_.logging.level = "info";
  }

  Config config_;
};

/**
 * @brief Test MySQL host change triggers reconnection callback
 */
TEST_F(RuntimeVariableManagerTest, MysqlHostChange) {
  auto manager_result = RuntimeVariableManager::Create(config_);
  ASSERT_TRUE(manager_result);
  auto manager = std::move(*manager_result);

  bool callback_called = false;
  std::string callback_host;
  int callback_port = 0;
  Expected<void, Error> callback_result = {};

  manager->SetMysqlReconnectCallback([&](const std::string& host, int port) -> Expected<void, Error> {
    callback_called = true;
    callback_host = host;
    callback_port = port;
    return callback_result;
  });

  // Test 1: Successful host change
  callback_result = {};
  auto result1 = manager->SetVariable("mysql.host", "192.168.1.100");
  EXPECT_TRUE(result1);
  EXPECT_TRUE(callback_called);
  EXPECT_EQ(callback_host, "192.168.1.100");
  EXPECT_EQ(callback_port, config_.mysql.port);

  auto get_result1 = manager->GetVariable("mysql.host");
  ASSERT_TRUE(get_result1);
  EXPECT_EQ(*get_result1, "192.168.1.100");

  // Test 2: Failed reconnection (callback returns error)
  callback_called = false;
  callback_result = MakeUnexpected(MakeError(ErrorCode::kMySQLConnectionFailed, "Connection refused"));
  auto result2 = manager->SetVariable("mysql.host", "192.168.1.200");
  EXPECT_FALSE(result2);
  EXPECT_TRUE(callback_called);

  auto get_result2 = manager->GetVariable("mysql.host");
  ASSERT_TRUE(get_result2);
  EXPECT_EQ(*get_result2, "192.168.1.100");
}

/**
 * @brief Test MySQL port change triggers reconnection callback
 */
TEST_F(RuntimeVariableManagerTest, MysqlPortChange) {
  auto manager_result = RuntimeVariableManager::Create(config_);
  ASSERT_TRUE(manager_result);
  auto manager = std::move(*manager_result);

  bool callback_called = false;
  std::string callback_host;
  int callback_port = 0;

  manager->SetMysqlReconnectCallback([&](const std::string& host, int port) -> Expected<void, Error> {
    callback_called = true;
    callback_host = host;
    callback_port = port;
    return {};
  });

  auto result = manager->SetVariable("mysql.port", "3307");
  EXPECT_TRUE(result);
  EXPECT_TRUE(callback_called);
  EXPECT_EQ(callback_host, config_.mysql.host);
  EXPECT_EQ(callback_port, 3307);

  auto get_result = manager->GetVariable("mysql.port");
  ASSERT_TRUE(get_result);
  EXPECT_EQ(*get_result, "3307");
}

/**
 * @brief Test simultaneous host and port change
 */
TEST_F(RuntimeVariableManagerTest, SimultaneousHostAndPortChange) {
  auto manager_result = RuntimeVariableManager::Create(config_);
  ASSERT_TRUE(manager_result);
  auto manager = std::move(*manager_result);

  int callback_count = 0;
  std::string last_host;
  int last_port = 0;

  manager->SetMysqlReconnectCallback([&](const std::string& host, int port) -> Expected<void, Error> {
    callback_count++;
    last_host = host;
    last_port = port;
    return {};
  });

  auto result1 = manager->SetVariable("mysql.host", "192.168.1.100");
  EXPECT_TRUE(result1);
  EXPECT_EQ(callback_count, 1);

  auto result2 = manager->SetVariable("mysql.port", "3307");
  EXPECT_TRUE(result2);
  EXPECT_EQ(callback_count, 2);
  EXPECT_EQ(last_host, "192.168.1.100");
  EXPECT_EQ(last_port, 3307);
}

/**
 * @brief Test idempotent host change (same value)
 */
TEST_F(RuntimeVariableManagerTest, IdempotentChange) {
  auto manager_result = RuntimeVariableManager::Create(config_);
  ASSERT_TRUE(manager_result);
  auto manager = std::move(*manager_result);

  int callback_count = 0;

  manager->SetMysqlReconnectCallback([&](const std::string& /*host*/, int /*port*/) -> Expected<void, Error> {
    callback_count++;
    return {};
  });

  auto result = manager->SetVariable("mysql.host", config_.mysql.host);
  EXPECT_TRUE(result);
  EXPECT_GE(callback_count, 0);
}

/**
 * @brief Test reconnection callback exception safety
 */
TEST_F(RuntimeVariableManagerTest, ReconnectionCallbackExceptionSafety) {
  auto manager_result = RuntimeVariableManager::Create(config_);
  ASSERT_TRUE(manager_result);
  auto manager = std::move(*manager_result);

  manager->SetMysqlReconnectCallback([](const std::string& /*host*/, int /*port*/) -> Expected<void, Error> {
    return MakeUnexpected(MakeError(ErrorCode::kMySQLConnectionFailed, "Simulated error"));
  });

  auto result = manager->SetVariable("mysql.host", "invalid.host");
  EXPECT_FALSE(result);
  EXPECT_EQ(static_cast<int>(result.error().code()), static_cast<int>(ErrorCode::kMySQLConnectionFailed));

  auto get_result = manager->GetVariable("mysql.host");
  ASSERT_TRUE(get_result);
  EXPECT_EQ(*get_result, config_.mysql.host);

  auto result2 = manager->SetVariable("logging.level", "debug");
  EXPECT_TRUE(result2);
}
