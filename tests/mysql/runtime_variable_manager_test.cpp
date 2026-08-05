/**
 * @file runtime_variable_manager_test.cpp
 * @brief Unit tests for RuntimeVariableManager MySQL endpoint safety
 *
 * MySQL endpoints are startup-only. Client-issued SET commands must not
 * redirect stored credentials or invoke the reconnection callback.
 */

#include "config/runtime_variable_manager.h"

#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "config/config.h"
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
 * @brief MySQL endpoint changes are rejected before any callback can run.
 */
TEST_F(RuntimeVariableManagerTest, MysqlEndpointChangesAreRejectedWithoutReconnectCallback) {
  auto manager_result = RuntimeVariableManager::Create(config_);
  ASSERT_TRUE(manager_result);
  auto manager = std::move(*manager_result);

  bool callback_called = false;
  manager->SetMysqlReconnectCallback([&](const std::string&, int) -> Expected<void, Error> {
    callback_called = true;
    return {};
  });

  for (const auto& [name, value] :
       std::vector<std::pair<std::string, std::string>>{{"mysql.host", "192.0.2.10"}, {"mysql.port", "3307"}}) {
    const auto result = manager->SetVariable(name, value);
    ASSERT_FALSE(result) << name;
    EXPECT_EQ(result.error().code(), ErrorCode::kInvalidArgument) << name;
    EXPECT_NE(result.error().message().find("immutable"), std::string::npos) << name;
  }
  EXPECT_FALSE(callback_called);

  const auto host = manager->GetVariable("mysql.host");
  const auto port = manager->GetVariable("mysql.port");
  ASSERT_TRUE(host);
  ASSERT_TRUE(port);
  EXPECT_EQ(*host, config_.mysql.host);
  EXPECT_EQ(*port, std::to_string(config_.mysql.port));
}
