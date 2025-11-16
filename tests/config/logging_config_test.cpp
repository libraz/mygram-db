/**
 * @file logging_config_test.cpp
 * @brief Unit tests for logging configuration
 */

#include <gtest/gtest.h>

#include <cstdio>
#include <filesystem>
#include <fstream>

#include "config/config.h"

using namespace mygramdb::config;

namespace {

/**
 * @brief Create a temporary YAML config file with logging settings
 */
std::string CreateTempConfig(const std::string& logging_section) {
  const char* temp_file = std::tmpnam(nullptr);
  std::string temp_path = std::string(temp_file) + ".yaml";

  std::ofstream ofs(temp_path);
  ofs << "mysql:\n"
      << "  host: \"127.0.0.1\"\n"
      << "  port: 3306\n"
      << "  user: \"test\"\n"
      << "  password: \"test\"\n"
      << "  database: \"test\"\n"
      << "  use_gtid: true\n"
      << "\n"
      << "tables:\n"
      << "  - name: \"test_table\"\n"
      << "    primary_key: \"id\"\n"
      << "    text_source:\n"
      << "      column: \"content\"\n"
      << "\n"
      << "replication:\n"
      << "  enable: false\n"
      << "  server_id: 12345\n"
      << "\n"
      << logging_section;
  ofs.close();

  return temp_path;
}

}  // namespace

/**
 * @brief Test default logging configuration (empty file setting)
 */
TEST(LoggingConfigTest, DefaultStdout) {
  std::string config_path = CreateTempConfig(
      "logging:\n"
      "  level: \"info\"\n"
      "  json: true\n"
      "  file: \"\"\n");

  Config config = LoadConfig(config_path);

  EXPECT_EQ(config.logging.level, "info");
  EXPECT_TRUE(config.logging.json);
  EXPECT_EQ(config.logging.file, "");

  std::filesystem::remove(config_path);
}

/**
 * @brief Test file logging configuration
 */
TEST(LoggingConfigTest, FileLogging) {
  std::string config_path = CreateTempConfig(
      "logging:\n"
      "  level: \"debug\"\n"
      "  json: false\n"
      "  file: \"/var/log/mygramdb/mygramdb.log\"\n");

  Config config = LoadConfig(config_path);

  EXPECT_EQ(config.logging.level, "debug");
  EXPECT_FALSE(config.logging.json);
  EXPECT_EQ(config.logging.file, "/var/log/mygramdb/mygramdb.log");

  std::filesystem::remove(config_path);
}

/**
 * @brief Test logging configuration with only level (file defaults to empty)
 */
TEST(LoggingConfigTest, OnlyLevel) {
  std::string config_path = CreateTempConfig(
      "logging:\n"
      "  level: \"warn\"\n");

  Config config = LoadConfig(config_path);

  EXPECT_EQ(config.logging.level, "warn");
  EXPECT_TRUE(config.logging.json);    // Default
  EXPECT_EQ(config.logging.file, "");  // Default

  std::filesystem::remove(config_path);
}

/**
 * @brief Test all log levels
 */
TEST(LoggingConfigTest, AllLogLevels) {
  const std::vector<std::string> levels = {"debug", "info", "warn", "error"};

  for (const auto& level : levels) {
    std::string config_path = CreateTempConfig(
        "logging:\n"
        "  level: \"" +
        level + "\"\n");

    Config config = LoadConfig(config_path);
    EXPECT_EQ(config.logging.level, level);

    std::filesystem::remove(config_path);
  }
}

/**
 * @brief Test logging configuration without logging section (uses defaults)
 */
TEST(LoggingConfigTest, NoLoggingSection) {
  std::string config_path = CreateTempConfig("");

  Config config = LoadConfig(config_path);

  EXPECT_EQ(config.logging.level, "info");  // Default
  EXPECT_TRUE(config.logging.json);         // Default
  EXPECT_EQ(config.logging.file, "");       // Default

  std::filesystem::remove(config_path);
}

/**
 * @brief Test relative file path
 */
TEST(LoggingConfigTest, RelativeFilePath) {
  std::string config_path = CreateTempConfig(
      "logging:\n"
      "  level: \"info\"\n"
      "  file: \"./logs/mygramdb.log\"\n");

  Config config = LoadConfig(config_path);

  EXPECT_EQ(config.logging.file, "./logs/mygramdb.log");

  std::filesystem::remove(config_path);
}

/**
 * @brief Test absolute file path
 */
TEST(LoggingConfigTest, AbsoluteFilePath) {
  std::string config_path = CreateTempConfig(
      "logging:\n"
      "  level: \"info\"\n"
      "  file: \"/tmp/test-mygramdb.log\"\n");

  Config config = LoadConfig(config_path);

  EXPECT_EQ(config.logging.file, "/tmp/test-mygramdb.log");

  std::filesystem::remove(config_path);
}

/**
 * @brief Test JSON format combinations
 */
TEST(LoggingConfigTest, JsonFormatCombinations) {
  // JSON enabled + file logging
  {
    std::string config_path = CreateTempConfig(
        "logging:\n"
        "  level: \"info\"\n"
        "  json: true\n"
        "  file: \"/tmp/test.log\"\n");

    Config config = LoadConfig(config_path);
    EXPECT_TRUE(config.logging.json);
    EXPECT_EQ(config.logging.file, "/tmp/test.log");

    std::filesystem::remove(config_path);
  }

  // JSON disabled + stdout
  {
    std::string config_path = CreateTempConfig(
        "logging:\n"
        "  level: \"info\"\n"
        "  json: false\n"
        "  file: \"\"\n");

    Config config = LoadConfig(config_path);
    EXPECT_FALSE(config.logging.json);
    EXPECT_EQ(config.logging.file, "");

    std::filesystem::remove(config_path);
  }
}
