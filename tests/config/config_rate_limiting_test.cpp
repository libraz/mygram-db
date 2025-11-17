/**
 * @file config_rate_limiting_test.cpp
 * @brief Tests for rate limiting configuration
 */

#include <gtest/gtest.h>

#include <fstream>
#include <string>

#include "config/config.h"

using namespace mygramdb::config;

class ConfigRateLimitingTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create temporary config file
    config_file_ = "/tmp/mygramdb_test_rate_limiting_config.yaml";
  }

  void TearDown() override {
    // Clean up
    std::remove(config_file_.c_str());
  }

  void WriteConfigFile(const std::string& content) {
    std::ofstream ofs(config_file_);
    ofs << content;
    ofs.close();
  }

  std::string config_file_;
};

/**
 * @brief Test default rate limiting configuration (disabled)
 */
TEST_F(ConfigRateLimitingTest, DefaultDisabled) {
  WriteConfigFile(R"(
mysql:
  user: test
  database: testdb

tables:
  - name: test_table
)");

  auto config_result = LoadConfig(config_file_);
  ASSERT_TRUE(config_result) << "Failed to load config: " << config_result.error().to_string();
  Config config = *config_result;

  // Rate limiting should be disabled by default
  EXPECT_FALSE(config.api.rate_limiting.enable);
  EXPECT_EQ(config.api.rate_limiting.capacity, 100);
  EXPECT_EQ(config.api.rate_limiting.refill_rate, 10);
  EXPECT_EQ(config.api.rate_limiting.max_clients, 10000);
}

/**
 * @brief Test enabling rate limiting
 */
TEST_F(ConfigRateLimitingTest, EnableRateLimiting) {
  WriteConfigFile(R"(
mysql:
  user: test
  database: testdb

tables:
  - name: test_table

api:
  rate_limiting:
    enable: true
)");

  auto config_result = LoadConfig(config_file_);
  ASSERT_TRUE(config_result) << "Failed to load config: " << config_result.error().to_string();
  Config config = *config_result;

  EXPECT_TRUE(config.api.rate_limiting.enable);
  EXPECT_EQ(config.api.rate_limiting.capacity, 100);       // Default
  EXPECT_EQ(config.api.rate_limiting.refill_rate, 10);     // Default
  EXPECT_EQ(config.api.rate_limiting.max_clients, 10000);  // Default
}

/**
 * @brief Test custom rate limiting configuration
 */
TEST_F(ConfigRateLimitingTest, CustomConfiguration) {
  WriteConfigFile(R"(
mysql:
  user: test
  database: testdb

tables:
  - name: test_table

api:
  rate_limiting:
    enable: true
    capacity: 50
    refill_rate: 5
    max_clients: 5000
)");

  auto config_result = LoadConfig(config_file_);
  ASSERT_TRUE(config_result) << "Failed to load config: " << config_result.error().to_string();
  Config config = *config_result;

  EXPECT_TRUE(config.api.rate_limiting.enable);
  EXPECT_EQ(config.api.rate_limiting.capacity, 50);
  EXPECT_EQ(config.api.rate_limiting.refill_rate, 5);
  EXPECT_EQ(config.api.rate_limiting.max_clients, 5000);
}

/**
 * @brief Test rate limiting disabled explicitly
 */
TEST_F(ConfigRateLimitingTest, ExplicitlyDisabled) {
  WriteConfigFile(R"(
mysql:
  user: test
  database: testdb

tables:
  - name: test_table

api:
  rate_limiting:
    enable: false
    capacity: 200
    refill_rate: 20
)");

  auto config_result = LoadConfig(config_file_);
  ASSERT_TRUE(config_result) << "Failed to load config: " << config_result.error().to_string();
  Config config = *config_result;

  EXPECT_FALSE(config.api.rate_limiting.enable);
  // Other values should still be loaded even if disabled
  EXPECT_EQ(config.api.rate_limiting.capacity, 200);
  EXPECT_EQ(config.api.rate_limiting.refill_rate, 20);
}

/**
 * @brief Test partial configuration (some defaults)
 */
TEST_F(ConfigRateLimitingTest, PartialConfiguration) {
  WriteConfigFile(R"(
mysql:
  user: test
  database: testdb

tables:
  - name: test_table

api:
  rate_limiting:
    enable: true
    capacity: 1000
)");

  auto config_result = LoadConfig(config_file_);
  ASSERT_TRUE(config_result) << "Failed to load config: " << config_result.error().to_string();
  Config config = *config_result;

  EXPECT_TRUE(config.api.rate_limiting.enable);
  EXPECT_EQ(config.api.rate_limiting.capacity, 1000);
  EXPECT_EQ(config.api.rate_limiting.refill_rate, 10);     // Default
  EXPECT_EQ(config.api.rate_limiting.max_clients, 10000);  // Default
}
