/**
 * @file runtime_variable_manager_test.cpp
 * @brief Unit tests for RuntimeVariableManager
 */

#include "config/runtime_variable_manager.h"

#include <gtest/gtest.h>

#include <thread>

#include "cache/cache_manager.h"
#include "config/config.h"
#include "utils/error.h"
#include "utils/expected.h"

using namespace mygramdb::config;
using namespace mygram::utils;

namespace {

/**
 * @brief Create minimal valid config for testing
 */
Config CreateTestConfig() {
  Config config;

  // MySQL config (minimal required fields)
  config.mysql.host = "127.0.0.1";
  config.mysql.port = 3306;
  config.mysql.user = "test_user";
  config.mysql.password = "test_pass";
  config.mysql.database = "test_db";
  config.mysql.use_gtid = true;
  config.mysql.binlog_format = "ROW";
  config.mysql.binlog_row_image = "FULL";

  // API config
  config.api.default_limit = 100;
  config.api.max_query_length = 128;

  // Rate limiting config
  config.api.rate_limiting.enable = true;
  config.api.rate_limiting.capacity = 100;
  config.api.rate_limiting.refill_rate = 10;

  // Cache config
  config.cache.enabled = true;
  config.cache.min_query_cost_ms = 10.0;
  config.cache.ttl_seconds = 3600;

  // Logging config
  config.logging.level = "info";
  config.logging.format = "json";

  return config;
}

}  // namespace

/**
 * @brief Test RuntimeVariableManager creation
 */
TEST(RuntimeVariableManagerTest, Create) {
  Config config = CreateTestConfig();
  auto manager_result = RuntimeVariableManager::Create(config);
  ASSERT_TRUE(manager_result) << "Failed to create RuntimeVariableManager: " << manager_result.error().to_string();
  auto manager = std::move(*manager_result);
  EXPECT_NE(manager, nullptr);
}

/**
 * @brief Test GetVariable for mutable variables
 */
TEST(RuntimeVariableManagerTest, GetMutableVariables) {
  Config config = CreateTestConfig();
  auto manager = std::move(*RuntimeVariableManager::Create(config));

  // Logging variables
  auto level_result = manager->GetVariable("logging.level");
  ASSERT_TRUE(level_result);
  EXPECT_EQ(*level_result, "info");

  auto format_result = manager->GetVariable("logging.format");
  ASSERT_TRUE(format_result);
  EXPECT_EQ(*format_result, "json");

  // MySQL variables
  auto host_result = manager->GetVariable("mysql.host");
  ASSERT_TRUE(host_result);
  EXPECT_EQ(*host_result, "127.0.0.1");

  auto port_result = manager->GetVariable("mysql.port");
  ASSERT_TRUE(port_result);
  EXPECT_EQ(*port_result, "3306");

  // API variables
  auto limit_result = manager->GetVariable("api.default_limit");
  ASSERT_TRUE(limit_result);
  EXPECT_EQ(*limit_result, "100");

  auto max_query_result = manager->GetVariable("api.max_query_length");
  ASSERT_TRUE(max_query_result);
  EXPECT_EQ(*max_query_result, "128");

  // Cache variables
  auto cache_enabled_result = manager->GetVariable("cache.enabled");
  ASSERT_TRUE(cache_enabled_result);
  EXPECT_EQ(*cache_enabled_result, "true");

  auto cache_cost_result = manager->GetVariable("cache.min_query_cost_ms");
  ASSERT_TRUE(cache_cost_result);
  EXPECT_DOUBLE_EQ(std::stod(*cache_cost_result), 10.0);

  auto cache_ttl_result = manager->GetVariable("cache.ttl_seconds");
  ASSERT_TRUE(cache_ttl_result);
  EXPECT_EQ(*cache_ttl_result, "3600");
}

/**
 * @brief Test GetVariable for immutable variables
 */
TEST(RuntimeVariableManagerTest, GetImmutableVariables) {
  Config config = CreateTestConfig();
  auto manager = std::move(*RuntimeVariableManager::Create(config));

  // MySQL immutable variables
  auto user_result = manager->GetVariable("mysql.user");
  ASSERT_TRUE(user_result);
  EXPECT_EQ(*user_result, "test_user");

  auto password_result = manager->GetVariable("mysql.password");
  ASSERT_TRUE(password_result);
  EXPECT_EQ(*password_result, "test_pass");

  auto database_result = manager->GetVariable("mysql.database");
  ASSERT_TRUE(database_result);
  EXPECT_EQ(*database_result, "test_db");
}

/**
 * @brief Test GetVariable for unknown variable
 */
TEST(RuntimeVariableManagerTest, GetUnknownVariable) {
  Config config = CreateTestConfig();
  auto manager = std::move(*RuntimeVariableManager::Create(config));

  auto result = manager->GetVariable("unknown.variable");
  EXPECT_FALSE(result);
  EXPECT_EQ(static_cast<int>(result.error().code()), static_cast<int>(ErrorCode::kInvalidArgument));
}

/**
 * @brief Test IsMutable
 */
TEST(RuntimeVariableManagerTest, IsMutable) {
  Config config = CreateTestConfig();
  auto manager = std::move(*RuntimeVariableManager::Create(config));

  // Mutable variables
  EXPECT_TRUE(manager->IsMutable("logging.level"));
  EXPECT_TRUE(manager->IsMutable("logging.format"));
  EXPECT_TRUE(manager->IsMutable("mysql.host"));
  EXPECT_TRUE(manager->IsMutable("mysql.port"));
  EXPECT_TRUE(manager->IsMutable("api.default_limit"));
  EXPECT_TRUE(manager->IsMutable("api.max_query_length"));
  EXPECT_TRUE(manager->IsMutable("cache.enabled"));
  EXPECT_TRUE(manager->IsMutable("cache.min_query_cost_ms"));
  EXPECT_TRUE(manager->IsMutable("cache.ttl_seconds"));
  EXPECT_TRUE(manager->IsMutable("api.rate_limiting.capacity"));
  EXPECT_TRUE(manager->IsMutable("api.rate_limiting.refill_rate"));

  // Immutable variables
  EXPECT_FALSE(manager->IsMutable("mysql.user"));
  EXPECT_FALSE(manager->IsMutable("mysql.password"));
  EXPECT_FALSE(manager->IsMutable("mysql.database"));
  EXPECT_FALSE(manager->IsMutable("mysql.use_gtid"));

  // Unknown variables
  EXPECT_FALSE(manager->IsMutable("unknown.variable"));
}

/**
 * @brief Test GetAllVariables without prefix
 */
TEST(RuntimeVariableManagerTest, GetAllVariablesNoPrefix) {
  Config config = CreateTestConfig();
  auto manager = std::move(*RuntimeVariableManager::Create(config));

  auto all_vars = manager->GetAllVariables();

  // Should contain at least the known variables
  EXPECT_GT(all_vars.size(), 10);

  // Check some known variables
  ASSERT_NE(all_vars.find("logging.level"), all_vars.end());
  EXPECT_EQ(all_vars["logging.level"].value, "info");
  EXPECT_TRUE(all_vars["logging.level"].mutable_);

  ASSERT_NE(all_vars.find("mysql.host"), all_vars.end());
  EXPECT_EQ(all_vars["mysql.host"].value, "127.0.0.1");
  EXPECT_TRUE(all_vars["mysql.host"].mutable_);

  ASSERT_NE(all_vars.find("mysql.user"), all_vars.end());
  EXPECT_EQ(all_vars["mysql.user"].value, "test_user");
  EXPECT_FALSE(all_vars["mysql.user"].mutable_);
}

/**
 * @brief Test GetAllVariables with prefix filter
 */
TEST(RuntimeVariableManagerTest, GetAllVariablesWithPrefix) {
  Config config = CreateTestConfig();
  auto manager = std::move(*RuntimeVariableManager::Create(config));

  // Filter by "logging"
  auto logging_vars = manager->GetAllVariables("logging");
  EXPECT_GE(logging_vars.size(), 2);
  for (const auto& [name, info] : logging_vars) {
    EXPECT_TRUE(name.find("logging") == 0) << "Variable " << name << " does not start with 'logging'";
  }

  // Filter by "mysql"
  auto mysql_vars = manager->GetAllVariables("mysql");
  EXPECT_GT(mysql_vars.size(), 0);
  for (const auto& [name, info] : mysql_vars) {
    EXPECT_TRUE(name.find("mysql") == 0) << "Variable " << name << " does not start with 'mysql'";
  }

  // Filter by "cache"
  auto cache_vars = manager->GetAllVariables("cache");
  EXPECT_GE(cache_vars.size(), 3);
  for (const auto& [name, info] : cache_vars) {
    EXPECT_TRUE(name.find("cache") == 0) << "Variable " << name << " does not start with 'cache'";
  }
}

/**
 * @brief Test SetVariable for logging.level (valid values)
 */
TEST(RuntimeVariableManagerTest, SetLoggingLevelValid) {
  Config config = CreateTestConfig();
  auto manager = std::move(*RuntimeVariableManager::Create(config));

  std::vector<std::string> valid_levels = {"debug", "info", "warn", "error"};
  for (const auto& level : valid_levels) {
    auto result = manager->SetVariable("logging.level", level);
    EXPECT_TRUE(result) << "Failed to set logging.level to " << level << ": " << result.error().to_string();

    auto get_result = manager->GetVariable("logging.level");
    ASSERT_TRUE(get_result);
    EXPECT_EQ(*get_result, level);
  }
}

/**
 * @brief Test SetVariable for logging.level (invalid value)
 */
TEST(RuntimeVariableManagerTest, SetLoggingLevelInvalid) {
  Config config = CreateTestConfig();
  auto manager = std::move(*RuntimeVariableManager::Create(config));

  auto result = manager->SetVariable("logging.level", "invalid_level");
  EXPECT_FALSE(result);
  EXPECT_EQ(static_cast<int>(result.error().code()), static_cast<int>(ErrorCode::kInvalidArgument));

  // Original value should remain unchanged
  auto get_result = manager->GetVariable("logging.level");
  ASSERT_TRUE(get_result);
  EXPECT_EQ(*get_result, "info");
}

/**
 * @brief Test SetVariable for logging.format (valid values)
 */
TEST(RuntimeVariableManagerTest, SetLoggingFormatValid) {
  Config config = CreateTestConfig();
  auto manager = std::move(*RuntimeVariableManager::Create(config));

  std::vector<std::string> valid_formats = {"json", "text"};
  for (const auto& format : valid_formats) {
    auto result = manager->SetVariable("logging.format", format);
    EXPECT_TRUE(result) << "Failed to set logging.format to " << format << ": " << result.error().to_string();

    auto get_result = manager->GetVariable("logging.format");
    ASSERT_TRUE(get_result);
    EXPECT_EQ(*get_result, format);
  }
}

/**
 * @brief Test SetVariable for logging.format (invalid value)
 */
TEST(RuntimeVariableManagerTest, SetLoggingFormatInvalid) {
  Config config = CreateTestConfig();
  auto manager = std::move(*RuntimeVariableManager::Create(config));

  auto result = manager->SetVariable("logging.format", "xml");
  EXPECT_FALSE(result);
  EXPECT_EQ(static_cast<int>(result.error().code()), static_cast<int>(ErrorCode::kInvalidArgument));

  // Original value should remain unchanged
  auto get_result = manager->GetVariable("logging.format");
  ASSERT_TRUE(get_result);
  EXPECT_EQ(*get_result, "json");
}

/**
 * @brief Test SetVariable for api.default_limit (valid values)
 */
TEST(RuntimeVariableManagerTest, SetApiDefaultLimitValid) {
  Config config = CreateTestConfig();
  auto manager = std::move(*RuntimeVariableManager::Create(config));

  std::vector<int> valid_limits = {5, 50, 500, 1000};
  for (int limit : valid_limits) {
    auto result = manager->SetVariable("api.default_limit", std::to_string(limit));
    EXPECT_TRUE(result) << "Failed to set api.default_limit to " << limit << ": " << result.error().to_string();

    auto get_result = manager->GetVariable("api.default_limit");
    ASSERT_TRUE(get_result);
    EXPECT_EQ(*get_result, std::to_string(limit));
  }
}

/**
 * @brief Test SetVariable for api.default_limit (invalid values - out of range)
 */
TEST(RuntimeVariableManagerTest, SetApiDefaultLimitOutOfRange) {
  Config config = CreateTestConfig();
  auto manager = std::move(*RuntimeVariableManager::Create(config));

  // Below minimum
  auto result1 = manager->SetVariable("api.default_limit", "4");
  EXPECT_FALSE(result1);

  // Above maximum
  auto result2 = manager->SetVariable("api.default_limit", "1001");
  EXPECT_FALSE(result2);

  // Original value should remain unchanged
  auto get_result = manager->GetVariable("api.default_limit");
  ASSERT_TRUE(get_result);
  EXPECT_EQ(*get_result, "100");
}

/**
 * @brief Test SetVariable for api.default_limit (invalid value - not a number)
 */
TEST(RuntimeVariableManagerTest, SetApiDefaultLimitNotNumber) {
  Config config = CreateTestConfig();
  auto manager = std::move(*RuntimeVariableManager::Create(config));

  auto result = manager->SetVariable("api.default_limit", "not_a_number");
  EXPECT_FALSE(result);

  // Original value should remain unchanged
  auto get_result = manager->GetVariable("api.default_limit");
  ASSERT_TRUE(get_result);
  EXPECT_EQ(*get_result, "100");
}

/**
 * @brief Test SetVariable for cache.enabled (toggle)
 */
TEST(RuntimeVariableManagerTest, SetCacheEnabled) {
  Config config = CreateTestConfig();
  auto manager = std::move(*RuntimeVariableManager::Create(config));

  bool callback_called = false;
  bool callback_value = false;

  // Set toggle callback
  manager->SetCacheToggleCallback([&](bool enabled) -> Expected<void, Error> {
    callback_called = true;
    callback_value = enabled;
    return {};
  });

  // Disable cache
  callback_called = false;
  auto result1 = manager->SetVariable("cache.enabled", "false");
  EXPECT_TRUE(result1);
  EXPECT_TRUE(callback_called);
  EXPECT_FALSE(callback_value);

  auto get_result1 = manager->GetVariable("cache.enabled");
  ASSERT_TRUE(get_result1);
  EXPECT_EQ(*get_result1, "false");

  // Enable cache
  callback_called = false;
  auto result2 = manager->SetVariable("cache.enabled", "true");
  EXPECT_TRUE(result2);
  EXPECT_TRUE(callback_called);
  EXPECT_TRUE(callback_value);

  auto get_result2 = manager->GetVariable("cache.enabled");
  ASSERT_TRUE(get_result2);
  EXPECT_EQ(*get_result2, "true");
}

/**
 * @brief Test SetVariable for cache.min_query_cost_ms (valid values)
 */
TEST(RuntimeVariableManagerTest, SetCacheMinQueryCostValid) {
  Config config = CreateTestConfig();
  auto manager = std::move(*RuntimeVariableManager::Create(config));

  std::vector<double> valid_costs = {0.0, 5.5, 10.0, 50.5, 100.0};
  for (double cost : valid_costs) {
    auto result = manager->SetVariable("cache.min_query_cost_ms", std::to_string(cost));
    EXPECT_TRUE(result) << "Failed to set cache.min_query_cost_ms to " << cost << ": " << result.error().to_string();

    auto get_result = manager->GetVariable("cache.min_query_cost_ms");
    ASSERT_TRUE(get_result);
    EXPECT_EQ(std::stod(*get_result), cost);
  }
}

/**
 * @brief Test SetVariable for cache.min_query_cost_ms (invalid - negative)
 */
TEST(RuntimeVariableManagerTest, SetCacheMinQueryCostNegative) {
  Config config = CreateTestConfig();
  auto manager = std::move(*RuntimeVariableManager::Create(config));

  auto result = manager->SetVariable("cache.min_query_cost_ms", "-1.0");
  EXPECT_FALSE(result);

  // Original value should remain unchanged
  auto get_result = manager->GetVariable("cache.min_query_cost_ms");
  ASSERT_TRUE(get_result);
  EXPECT_DOUBLE_EQ(std::stod(*get_result), 10.0);
}

/**
 * @brief Test SetVariable for immutable variable (should fail)
 */
TEST(RuntimeVariableManagerTest, SetImmutableVariable) {
  Config config = CreateTestConfig();
  auto manager = std::move(*RuntimeVariableManager::Create(config));

  // Try to set mysql.user (immutable)
  auto result1 = manager->SetVariable("mysql.user", "new_user");
  EXPECT_FALSE(result1);
  EXPECT_EQ(static_cast<int>(result1.error().code()), static_cast<int>(ErrorCode::kInvalidArgument));

  // Try to set mysql.password (immutable)
  auto result2 = manager->SetVariable("mysql.password", "new_pass");
  EXPECT_FALSE(result2);

  // Try to set mysql.database (immutable)
  auto result3 = manager->SetVariable("mysql.database", "new_db");
  EXPECT_FALSE(result3);

  // Original values should remain unchanged
  auto user_result = manager->GetVariable("mysql.user");
  ASSERT_TRUE(user_result);
  EXPECT_EQ(*user_result, "test_user");
}

/**
 * @brief Test SetVariable for unknown variable
 */
TEST(RuntimeVariableManagerTest, SetUnknownVariable) {
  Config config = CreateTestConfig();
  auto manager = std::move(*RuntimeVariableManager::Create(config));

  auto result = manager->SetVariable("unknown.variable", "value");
  EXPECT_FALSE(result);
  EXPECT_EQ(static_cast<int>(result.error().code()), static_cast<int>(ErrorCode::kInvalidArgument));
}

/**
 * @brief Test SetVariable for mysql.host (with callback)
 */
TEST(RuntimeVariableManagerTest, SetMysqlHost) {
  Config config = CreateTestConfig();
  auto manager = std::move(*RuntimeVariableManager::Create(config));

  bool callback_called = false;
  std::string callback_host;
  int callback_port = 0;

  // Set reconnection callback
  manager->SetMysqlReconnectCallback([&](const std::string& host, int port) -> Expected<void, Error> {
    callback_called = true;
    callback_host = host;
    callback_port = port;
    return {};
  });

  // Change host
  callback_called = false;
  auto result = manager->SetVariable("mysql.host", "192.168.1.100");
  EXPECT_TRUE(result);
  EXPECT_TRUE(callback_called);
  EXPECT_EQ(callback_host, "192.168.1.100");
  EXPECT_EQ(callback_port, 3306);  // Port should remain unchanged

  auto get_result = manager->GetVariable("mysql.host");
  ASSERT_TRUE(get_result);
  EXPECT_EQ(*get_result, "192.168.1.100");
}

/**
 * @brief Test SetVariable for mysql.port (with callback)
 */
TEST(RuntimeVariableManagerTest, SetMysqlPort) {
  Config config = CreateTestConfig();
  auto manager = std::move(*RuntimeVariableManager::Create(config));

  bool callback_called = false;
  std::string callback_host;
  int callback_port = 0;

  // Set reconnection callback
  manager->SetMysqlReconnectCallback([&](const std::string& host, int port) -> Expected<void, Error> {
    callback_called = true;
    callback_host = host;
    callback_port = port;
    return {};
  });

  // Change port
  callback_called = false;
  auto result = manager->SetVariable("mysql.port", "3307");
  EXPECT_TRUE(result);
  EXPECT_TRUE(callback_called);
  EXPECT_EQ(callback_host, "127.0.0.1");  // Host should remain unchanged
  EXPECT_EQ(callback_port, 3307);

  auto get_result = manager->GetVariable("mysql.port");
  ASSERT_TRUE(get_result);
  EXPECT_EQ(*get_result, "3307");
}

/**
 * @brief Test SetVariable for rate_limiting.capacity
 */
TEST(RuntimeVariableManagerTest, SetRateLimitingCapacity) {
  Config config = CreateTestConfig();
  auto manager = std::move(*RuntimeVariableManager::Create(config));

  bool callback_called = false;
  bool callback_enabled = false;
  size_t callback_capacity = 0;
  size_t callback_refill_rate = 0;

  // Set rate limiter callback
  manager->SetRateLimiterCallback([&](bool enabled, size_t capacity, size_t refill_rate) {
    callback_called = true;
    callback_enabled = enabled;
    callback_capacity = capacity;
    callback_refill_rate = refill_rate;
  });

  // Change capacity
  callback_called = false;
  auto result = manager->SetVariable("api.rate_limiting.capacity", "200");
  EXPECT_TRUE(result);
  EXPECT_TRUE(callback_called);
  EXPECT_EQ(callback_capacity, 200);
  EXPECT_EQ(callback_refill_rate, 10);  // Refill rate should remain unchanged

  auto get_result = manager->GetVariable("api.rate_limiting.capacity");
  ASSERT_TRUE(get_result);
  EXPECT_EQ(*get_result, "200");
}

/**
 * @brief Test SetVariable for rate_limiting.refill_rate
 */
TEST(RuntimeVariableManagerTest, SetRateLimitingRefillRate) {
  Config config = CreateTestConfig();
  auto manager = std::move(*RuntimeVariableManager::Create(config));

  bool callback_called = false;
  bool callback_enabled = false;
  size_t callback_capacity = 0;
  size_t callback_refill_rate = 0;

  // Set rate limiter callback
  manager->SetRateLimiterCallback([&](bool enabled, size_t capacity, size_t refill_rate) {
    callback_called = true;
    callback_enabled = enabled;
    callback_capacity = capacity;
    callback_refill_rate = refill_rate;
  });

  // Change refill rate
  callback_called = false;
  auto result = manager->SetVariable("api.rate_limiting.refill_rate", "20");
  EXPECT_TRUE(result);
  EXPECT_TRUE(callback_called);
  EXPECT_EQ(callback_capacity, 100);  // Capacity should remain unchanged
  EXPECT_EQ(callback_refill_rate, 20);

  auto get_result = manager->GetVariable("api.rate_limiting.refill_rate");
  ASSERT_TRUE(get_result);
  EXPECT_EQ(*get_result, "20");
}

/**
 * @brief Test concurrent read access (thread safety)
 */
TEST(RuntimeVariableManagerTest, ConcurrentReadAccess) {
  Config config = CreateTestConfig();
  auto manager = std::move(*RuntimeVariableManager::Create(config));

  const int num_threads = 10;
  const int num_iterations = 100;
  std::vector<std::thread> threads;
  std::atomic<int> errors{0};

  // Spawn multiple reader threads
  for (int i = 0; i < num_threads; ++i) {
    threads.emplace_back([&]() {
      for (int j = 0; j < num_iterations; ++j) {
        auto result = manager->GetVariable("logging.level");
        if (!result) {
          errors++;
        }

        auto all_vars = manager->GetAllVariables();
        if (all_vars.empty()) {
          errors++;
        }
      }
    });
  }

  // Wait for all threads to complete
  for (auto& thread : threads) {
    thread.join();
  }

  // No errors should occur
  EXPECT_EQ(errors, 0);
}

/**
 * @brief Test concurrent read/write access (thread safety)
 */
TEST(RuntimeVariableManagerTest, ConcurrentReadWriteAccess) {
  Config config = CreateTestConfig();
  auto manager = std::move(*RuntimeVariableManager::Create(config));

  const int num_reader_threads = 5;
  const int num_writer_threads = 5;
  const int num_iterations = 50;
  std::vector<std::thread> threads;
  std::atomic<int> errors{0};

  // Spawn reader threads
  for (int i = 0; i < num_reader_threads; ++i) {
    threads.emplace_back([&]() {
      for (int j = 0; j < num_iterations; ++j) {
        auto result = manager->GetVariable("api.default_limit");
        if (!result) {
          errors++;
        }
      }
    });
  }

  // Spawn writer threads
  for (int i = 0; i < num_writer_threads; ++i) {
    threads.emplace_back([&]() {
      for (int j = 0; j < num_iterations; ++j) {
        int value = 50 + (j % 50);  // Values between 50 and 100
        auto result = manager->SetVariable("api.default_limit", std::to_string(value));
        if (!result) {
          errors++;
        }
      }
    });
  }

  // Wait for all threads to complete
  for (auto& thread : threads) {
    thread.join();
  }

  // No errors should occur
  EXPECT_EQ(errors, 0);

  // Final value should be valid
  auto final_result = manager->GetVariable("api.default_limit");
  ASSERT_TRUE(final_result);
  int final_value = std::stoi(*final_result);
  EXPECT_GE(final_value, 50);
  EXPECT_LE(final_value, 100);
}

/**
 * @brief Test MySQL reconnection callback failure
 */
TEST(RuntimeVariableManagerTest, MysqlReconnectCallbackFailure) {
  Config config = CreateTestConfig();
  auto manager = std::move(*RuntimeVariableManager::Create(config));

  // Set callback that always fails
  manager->SetMysqlReconnectCallback([](const std::string& /*host*/, int /*port*/) -> Expected<void, Error> {
    return MakeUnexpected(MakeError(ErrorCode::kMySQLConnectionFailed, "Simulated connection failure"));
  });

  // Try to change host (should fail)
  auto result = manager->SetVariable("mysql.host", "192.168.1.100");
  EXPECT_FALSE(result);
  EXPECT_EQ(static_cast<int>(result.error().code()), static_cast<int>(ErrorCode::kMySQLConnectionFailed));

  // Original value should remain unchanged
  auto get_result = manager->GetVariable("mysql.host");
  ASSERT_TRUE(get_result);
  EXPECT_EQ(*get_result, "127.0.0.1");
}

/**
 * @brief Test cache toggle callback failure
 */
TEST(RuntimeVariableManagerTest, CacheToggleCallbackFailure) {
  Config config = CreateTestConfig();
  auto manager = std::move(*RuntimeVariableManager::Create(config));

  // Set callback that always fails
  manager->SetCacheToggleCallback([](bool /*enabled*/) -> Expected<void, Error> {
    return MakeUnexpected(MakeError(ErrorCode::kInternalError, "Simulated cache toggle failure"));
  });

  // Try to toggle cache (should fail)
  auto result = manager->SetVariable("cache.enabled", "false");
  EXPECT_FALSE(result);
  EXPECT_EQ(static_cast<int>(result.error().code()), static_cast<int>(ErrorCode::kInternalError));

  // Original value should remain unchanged
  auto get_result = manager->GetVariable("cache.enabled");
  ASSERT_TRUE(get_result);
  EXPECT_EQ(*get_result, "true");
}

/**
 * @brief Test SetVariable for api.max_query_length (valid values)
 */
TEST(RuntimeVariableManagerTest, SetApiMaxQueryLengthValid) {
  Config config = CreateTestConfig();
  auto manager = std::move(*RuntimeVariableManager::Create(config));

  std::vector<int> valid_lengths = {1, 64, 256, 512, 1024};
  for (int length : valid_lengths) {
    auto result = manager->SetVariable("api.max_query_length", std::to_string(length));
    EXPECT_TRUE(result) << "Failed to set api.max_query_length to " << length << ": " << result.error().to_string();

    auto get_result = manager->GetVariable("api.max_query_length");
    ASSERT_TRUE(get_result);
    EXPECT_EQ(*get_result, std::to_string(length));
  }
}

/**
 * @brief Test SetVariable for api.max_query_length (invalid - zero or negative)
 */
TEST(RuntimeVariableManagerTest, SetApiMaxQueryLengthInvalid) {
  Config config = CreateTestConfig();
  auto manager = std::move(*RuntimeVariableManager::Create(config));

  // Zero
  auto result1 = manager->SetVariable("api.max_query_length", "0");
  EXPECT_FALSE(result1);

  // Negative
  auto result2 = manager->SetVariable("api.max_query_length", "-1");
  EXPECT_FALSE(result2);

  // Original value should remain unchanged
  auto get_result = manager->GetVariable("api.max_query_length");
  ASSERT_TRUE(get_result);
  EXPECT_EQ(*get_result, "128");
}

/**
 * @brief Test SetVariable for cache.ttl_seconds (valid values)
 */
TEST(RuntimeVariableManagerTest, SetCacheTtlSecondsValid) {
  Config config = CreateTestConfig();
  auto manager = std::move(*RuntimeVariableManager::Create(config));

  std::vector<int> valid_ttls = {0, 60, 3600, 7200, 86400};  // 0 = no expiration
  for (int ttl : valid_ttls) {
    auto result = manager->SetVariable("cache.ttl_seconds", std::to_string(ttl));
    EXPECT_TRUE(result) << "Failed to set cache.ttl_seconds to " << ttl << ": " << result.error().to_string();

    auto get_result = manager->GetVariable("cache.ttl_seconds");
    ASSERT_TRUE(get_result);
    EXPECT_EQ(*get_result, std::to_string(ttl));
  }
}

/**
 * @brief Test SetVariable for cache.ttl_seconds (invalid - negative)
 */
TEST(RuntimeVariableManagerTest, SetCacheTtlSecondsNegative) {
  Config config = CreateTestConfig();
  auto manager = std::move(*RuntimeVariableManager::Create(config));

  auto result = manager->SetVariable("cache.ttl_seconds", "-1");
  EXPECT_FALSE(result);

  // Original value should remain unchanged
  auto get_result = manager->GetVariable("cache.ttl_seconds");
  ASSERT_TRUE(get_result);
  EXPECT_EQ(*get_result, "3600");
}

/**
 * @brief Test SetVariable for api.rate_limiting.enable
 */
TEST(RuntimeVariableManagerTest, SetRateLimitingEnable) {
  Config config = CreateTestConfig();
  auto manager = std::move(*RuntimeVariableManager::Create(config));

  // Disable rate limiting
  auto result1 = manager->SetVariable("api.rate_limiting.enable", "false");
  EXPECT_TRUE(result1);

  auto get_result1 = manager->GetVariable("api.rate_limiting.enable");
  ASSERT_TRUE(get_result1);
  EXPECT_EQ(*get_result1, "false");

  // Enable rate limiting
  auto result2 = manager->SetVariable("api.rate_limiting.enable", "true");
  EXPECT_TRUE(result2);

  auto get_result2 = manager->GetVariable("api.rate_limiting.enable");
  ASSERT_TRUE(get_result2);
  EXPECT_EQ(*get_result2, "true");
}

/**
 * @brief Test boundary values for api.default_limit
 */
TEST(RuntimeVariableManagerTest, SetApiDefaultLimitBoundary) {
  Config config = CreateTestConfig();
  auto manager = std::move(*RuntimeVariableManager::Create(config));

  // Minimum valid value
  auto result1 = manager->SetVariable("api.default_limit", "5");
  EXPECT_TRUE(result1);
  auto get_result1 = manager->GetVariable("api.default_limit");
  ASSERT_TRUE(get_result1);
  EXPECT_EQ(*get_result1, "5");

  // Maximum valid value
  auto result2 = manager->SetVariable("api.default_limit", "1000");
  EXPECT_TRUE(result2);
  auto get_result2 = manager->GetVariable("api.default_limit");
  ASSERT_TRUE(get_result2);
  EXPECT_EQ(*get_result2, "1000");

  // Just below minimum (should fail)
  auto result3 = manager->SetVariable("api.default_limit", "4");
  EXPECT_FALSE(result3);

  // Just above maximum (should fail)
  auto result4 = manager->SetVariable("api.default_limit", "1001");
  EXPECT_FALSE(result4);

  // Value should remain at 1000
  auto get_result3 = manager->GetVariable("api.default_limit");
  ASSERT_TRUE(get_result3);
  EXPECT_EQ(*get_result3, "1000");
}

/**
 * @brief Test zero value for cache.min_query_cost_ms (disable cost-based caching)
 */
TEST(RuntimeVariableManagerTest, SetCacheMinQueryCostZero) {
  Config config = CreateTestConfig();
  auto manager = std::move(*RuntimeVariableManager::Create(config));

  // Set to zero (disable cost-based caching)
  auto result = manager->SetVariable("cache.min_query_cost_ms", "0.0");
  EXPECT_TRUE(result);

  auto get_result = manager->GetVariable("cache.min_query_cost_ms");
  ASSERT_TRUE(get_result);
  EXPECT_DOUBLE_EQ(std::stod(*get_result), 0.0);
}

/**
 * @brief Test simultaneous MySQL host and port change
 */
TEST(RuntimeVariableManagerTest, SetMysqlHostAndPortSimultaneous) {
  Config config = CreateTestConfig();
  auto manager = std::move(*RuntimeVariableManager::Create(config));

  int callback_count = 0;
  std::string last_host;
  int last_port = 0;

  // Set reconnection callback
  manager->SetMysqlReconnectCallback([&](const std::string& host, int port) -> Expected<void, Error> {
    callback_count++;
    last_host = host;
    last_port = port;
    return {};
  });

  // Change host first
  auto result1 = manager->SetVariable("mysql.host", "192.168.1.100");
  EXPECT_TRUE(result1);
  EXPECT_EQ(callback_count, 1);
  EXPECT_EQ(last_host, "192.168.1.100");
  EXPECT_EQ(last_port, 3306);  // Port unchanged

  // Then change port (should trigger reconnection with new host and new port)
  auto result2 = manager->SetVariable("mysql.port", "3307");
  EXPECT_TRUE(result2);
  EXPECT_EQ(callback_count, 2);
  EXPECT_EQ(last_host, "192.168.1.100");  // Host from previous change
  EXPECT_EQ(last_port, 3307);

  // Verify both values updated
  auto host_result = manager->GetVariable("mysql.host");
  ASSERT_TRUE(host_result);
  EXPECT_EQ(*host_result, "192.168.1.100");

  auto port_result = manager->GetVariable("mysql.port");
  ASSERT_TRUE(port_result);
  EXPECT_EQ(*port_result, "3307");
}

/**
 * @brief Test partial failure in rate limiting parameters
 */
TEST(RuntimeVariableManagerTest, SetRateLimitingPartialFailure) {
  Config config = CreateTestConfig();
  auto manager = std::move(*RuntimeVariableManager::Create(config));

  bool callback_called = false;
  manager->SetRateLimiterCallback(
      [&](bool /*enabled*/, size_t /*capacity*/, size_t /*refill_rate*/) { callback_called = true; });

  // Valid capacity change
  callback_called = false;
  auto result1 = manager->SetVariable("api.rate_limiting.capacity", "200");
  EXPECT_TRUE(result1);
  EXPECT_TRUE(callback_called);

  // Invalid capacity (zero)
  callback_called = false;
  auto result2 = manager->SetVariable("api.rate_limiting.capacity", "0");
  EXPECT_FALSE(result2);
  EXPECT_FALSE(callback_called);  // Callback should not be called on failure

  // Original value should remain
  auto get_result = manager->GetVariable("api.rate_limiting.capacity");
  ASSERT_TRUE(get_result);
  EXPECT_EQ(*get_result, "200");
}

/**
 * @brief Test error messages for type conversion failures
 */
TEST(RuntimeVariableManagerTest, ErrorMessageTypeConversion) {
  Config config = CreateTestConfig();
  auto manager = std::move(*RuntimeVariableManager::Create(config));

  // String value for integer variable
  auto result1 = manager->SetVariable("api.default_limit", "not_a_number");
  EXPECT_FALSE(result1);
  EXPECT_NE(result1.error().message().find("Invalid"), std::string::npos);

  // String value for boolean variable
  auto result2 = manager->SetVariable("cache.enabled", "maybe");
  EXPECT_FALSE(result2);
  EXPECT_NE(result2.error().message().find("Invalid"), std::string::npos);

  // String value for float variable
  auto result3 = manager->SetVariable("cache.min_query_cost_ms", "invalid");
  EXPECT_FALSE(result3);
  EXPECT_NE(result3.error().message().find("Invalid"), std::string::npos);
}

/**
 * @brief Test error messages for range validation
 */
TEST(RuntimeVariableManagerTest, ErrorMessageRangeValidation) {
  Config config = CreateTestConfig();
  auto manager = std::move(*RuntimeVariableManager::Create(config));

  // Out of range (too low)
  auto result1 = manager->SetVariable("api.default_limit", "1");
  EXPECT_FALSE(result1);
  // Error message should contain some indication of the problem
  EXPECT_FALSE(result1.error().message().empty());

  // Out of range (too high)
  auto result2 = manager->SetVariable("api.default_limit", "10000");
  EXPECT_FALSE(result2);
  // Error message should contain some indication of the problem
  EXPECT_FALSE(result2.error().message().empty());
}

/**
 * @brief Test idempotent variable setting (same value)
 */
TEST(RuntimeVariableManagerTest, SetVariableIdempotent) {
  Config config = CreateTestConfig();
  auto manager = std::move(*RuntimeVariableManager::Create(config));

  int callback_count = 0;
  manager->SetMysqlReconnectCallback([&](const std::string& /*host*/, int /*port*/) -> Expected<void, Error> {
    callback_count++;
    return {};
  });

  // Set to same value as current
  auto result = manager->SetVariable("mysql.host", "127.0.0.1");
  EXPECT_TRUE(result);

  // Implementation may or may not call callback for idempotent change
  // Just verify it doesn't fail
  auto get_result = manager->GetVariable("mysql.host");
  ASSERT_TRUE(get_result);
  EXPECT_EQ(*get_result, "127.0.0.1");
}

/**
 * @brief Test GetAllVariables returns mutable flag correctly
 */
TEST(RuntimeVariableManagerTest, GetAllVariablesMutableFlag) {
  Config config = CreateTestConfig();
  auto manager = std::move(*RuntimeVariableManager::Create(config));

  auto all_vars = manager->GetAllVariables();

  // Mutable variables should have mutable_ = true
  EXPECT_TRUE(all_vars["logging.level"].mutable_);
  EXPECT_TRUE(all_vars["mysql.host"].mutable_);
  EXPECT_TRUE(all_vars["cache.enabled"].mutable_);

  // Immutable variables should have mutable_ = false
  EXPECT_FALSE(all_vars["mysql.user"].mutable_);
  EXPECT_FALSE(all_vars["mysql.password"].mutable_);
  EXPECT_FALSE(all_vars["mysql.database"].mutable_);
}

/**
 * @brief Test very large values for rate limiting parameters
 */
TEST(RuntimeVariableManagerTest, SetRateLimitingLargeValues) {
  Config config = CreateTestConfig();
  auto manager = std::move(*RuntimeVariableManager::Create(config));

  bool callback_called = false;
  size_t callback_capacity = 0;

  manager->SetRateLimiterCallback([&](bool /*enabled*/, size_t capacity, size_t /*refill_rate*/) {
    callback_called = true;
    callback_capacity = capacity;
  });

  // Large but valid values
  auto result1 = manager->SetVariable("api.rate_limiting.capacity", "10000");
  EXPECT_TRUE(result1);
  EXPECT_TRUE(callback_called);
  EXPECT_EQ(callback_capacity, 10000);

  callback_called = false;
  auto result2 = manager->SetVariable("api.rate_limiting.refill_rate", "1000");
  EXPECT_TRUE(result2);
  EXPECT_TRUE(callback_called);
}

/**
 * @brief Test floating point precision for cache.min_query_cost_ms
 */
TEST(RuntimeVariableManagerTest, SetCacheMinQueryCostPrecision) {
  Config config = CreateTestConfig();
  auto manager = std::move(*RuntimeVariableManager::Create(config));

  std::vector<double> precise_values = {0.1, 1.5, 10.25, 99.99};
  for (double value : precise_values) {
    auto result = manager->SetVariable("cache.min_query_cost_ms", std::to_string(value));
    EXPECT_TRUE(result);

    auto get_result = manager->GetVariable("cache.min_query_cost_ms");
    ASSERT_TRUE(get_result);
    EXPECT_NEAR(std::stod(*get_result), value, 0.01);  // Allow small floating point error
  }
}
