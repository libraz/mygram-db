/**
 * @file logging_config_test.cpp
 * @brief Unit tests for logging configuration
 */

#include <gtest/gtest.h>
#include <unistd.h>

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
  // Use mkstemp for secure temporary file creation
  char temp_template[] = "/tmp/mygramdb_test_XXXXXX.yaml";
  // Create a mutable buffer for the template (mkstemp modifies it)
  char temp_buffer[256];
  std::snprintf(temp_buffer, sizeof(temp_buffer), "/tmp/mygramdb_test_XXXXXX");

  int fd = mkstemp(temp_buffer);
  if (fd == -1) {
    throw std::runtime_error("Failed to create temporary file");
  }
  close(fd);  // Close the file descriptor, we'll use ofstream

  std::string temp_path = std::string(temp_buffer) + ".yaml";
  std::filesystem::rename(temp_buffer, temp_path);

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
      "  format: \"json\"\n"
      "  file: \"\"\n");

  auto config_result = LoadConfig(config_path);
  ASSERT_TRUE(config_result) << "Failed to load config: " << config_result.error().to_string();
  Config config = *config_result;

  EXPECT_EQ(config.logging.level, "info");
  EXPECT_EQ(config.logging.format, "json");
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
      "  format: \"text\"\n"
      "  file: \"/var/log/mygramdb/mygramdb.log\"\n");

  auto config_result = LoadConfig(config_path);
  ASSERT_TRUE(config_result) << "Failed to load config: " << config_result.error().to_string();
  Config config = *config_result;

  EXPECT_EQ(config.logging.level, "debug");
  EXPECT_EQ(config.logging.format, "text");
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

  auto config_result = LoadConfig(config_path);
  ASSERT_TRUE(config_result) << "Failed to load config: " << config_result.error().to_string();
  Config config = *config_result;

  EXPECT_EQ(config.logging.level, "warn");
  EXPECT_EQ(config.logging.format, "json");  // Default
  EXPECT_EQ(config.logging.file, "");        // Default

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

    auto config_result = LoadConfig(config_path);
    ASSERT_TRUE(config_result) << "Failed to load config: " << config_result.error().to_string();
    Config config = *config_result;
    EXPECT_EQ(config.logging.level, level);

    std::filesystem::remove(config_path);
  }
}

/**
 * @brief Test logging configuration without logging section (uses defaults)
 */
TEST(LoggingConfigTest, NoLoggingSection) {
  std::string config_path = CreateTempConfig("");

  auto config_result = LoadConfig(config_path);
  ASSERT_TRUE(config_result) << "Failed to load config: " << config_result.error().to_string();
  Config config = *config_result;

  EXPECT_EQ(config.logging.level, "info");   // Default
  EXPECT_EQ(config.logging.format, "json");  // Default
  EXPECT_EQ(config.logging.file, "");        // Default

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

  auto config_result = LoadConfig(config_path);
  ASSERT_TRUE(config_result) << "Failed to load config: " << config_result.error().to_string();
  Config config = *config_result;

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

  auto config_result = LoadConfig(config_path);
  ASSERT_TRUE(config_result) << "Failed to load config: " << config_result.error().to_string();
  Config config = *config_result;

  EXPECT_EQ(config.logging.file, "/tmp/test-mygramdb.log");

  std::filesystem::remove(config_path);
}

/**
 * @brief Test log format combinations
 */
TEST(LoggingConfigTest, LogFormatCombinations) {
  // JSON format + file logging
  {
    std::string config_path = CreateTempConfig(
        "logging:\n"
        "  level: \"info\"\n"
        "  format: \"json\"\n"
        "  file: \"/tmp/test.log\"\n");

    auto config_result = LoadConfig(config_path);
    ASSERT_TRUE(config_result) << "Failed to load config: " << config_result.error().to_string();
    Config config = *config_result;
    EXPECT_EQ(config.logging.format, "json");
    EXPECT_EQ(config.logging.file, "/tmp/test.log");

    std::filesystem::remove(config_path);
  }

  // TEXT format + stdout
  {
    std::string config_path = CreateTempConfig(
        "logging:\n"
        "  level: \"info\"\n"
        "  format: \"text\"\n"
        "  file: \"\"\n");

    auto config_result = LoadConfig(config_path);
    ASSERT_TRUE(config_result) << "Failed to load config: " << config_result.error().to_string();
    Config config = *config_result;
    EXPECT_EQ(config.logging.format, "text");
    EXPECT_EQ(config.logging.file, "");

    std::filesystem::remove(config_path);
  }
}
