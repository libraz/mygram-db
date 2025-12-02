/**
 * @file configuration_manager_test.cpp
 * @brief Unit tests for ConfigurationManager logging functionality
 */

#include "app/configuration_manager.h"

#include <gtest/gtest.h>
#include <spdlog/sinks/stdout_sinks.h>
#include <spdlog/spdlog.h>
#include <unistd.h>

#include <cstdio>
#include <filesystem>
#include <fstream>
#include <string>

#include "config/config.h"
#include "utils/structured_log.h"

using namespace mygramdb::app;
using namespace mygramdb::config;

namespace {

/**
 * @brief Test fixture that properly manages spdlog state
 *
 * This fixture ensures each test starts with clean spdlog state:
 * - Drops all existing loggers
 * - Resets to stdout logging
 * - Restores default log level
 */
class ConfigurationManagerTestFixture : public ::testing::Test {
 protected:
  void SetUp() override {
    // Drop all existing loggers
    spdlog::drop_all();

    // Create fresh stdout logger as default
    auto console_sink = std::make_shared<spdlog::sinks::stdout_sink_mt>();
    auto logger = std::make_shared<spdlog::logger>("mygramdb_test", console_sink);
    spdlog::set_default_logger(logger);

    // Reset to info level
    spdlog::set_level(spdlog::level::info);

    // Reset structured log format
    mygram::utils::StructuredLog::SetFormat(mygram::utils::LogFormat::TEXT);
  }

  void TearDown() override {
    // Clean up loggers after each test
    spdlog::drop_all();

    // Restore default console logger
    auto console_sink = std::make_shared<spdlog::sinks::stdout_sink_mt>();
    auto logger = std::make_shared<spdlog::logger>("default", console_sink);
    spdlog::set_default_logger(logger);
    spdlog::set_level(spdlog::level::info);
  }
};

/**
 * @brief Create a temporary YAML config file with custom logging settings
 */
std::string CreateTempConfig(const std::string& log_level, const std::string& log_format, const std::string& log_file) {
  // Use mkstemp for secure temporary file creation
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
      << "logging:\n"
      << "  level: \"" << log_level << "\"\n"
      << "  format: \"" << log_format << "\"\n";

  if (!log_file.empty()) {
    ofs << "  file: \"" << log_file << "\"\n";
  }

  ofs.close();
  return temp_path;
}

/**
 * @brief Helper to read file contents
 */
std::string ReadFile(const std::string& path) {
  std::ifstream ifs(path);
  if (!ifs.is_open()) {
    return "";
  }
  std::string content((std::istreambuf_iterator<char>(ifs)), std::istreambuf_iterator<char>());
  return content;
}

}  // namespace

/**
 * @brief Test that ApplyLoggingConfig correctly sets log level for stdout
 */
TEST_F(ConfigurationManagerTestFixture, ApplyLoggingConfigStdoutLevel) {
  // Create config with debug level, stdout output
  std::string config_path = CreateTempConfig("debug", "json", "");

  auto config_mgr_result = ConfigurationManager::Create(config_path, "");
  ASSERT_TRUE(config_mgr_result) << "Failed to create ConfigurationManager: " << config_mgr_result.error().to_string();

  auto config_mgr = std::move(*config_mgr_result);

  // Apply logging config
  auto result = config_mgr->ApplyLoggingConfig();
  ASSERT_TRUE(result) << "ApplyLoggingConfig failed: " << result.error().to_string();

  // Verify log level is set to debug
  EXPECT_EQ(spdlog::get_level(), spdlog::level::debug);

  // Cleanup
  std::filesystem::remove(config_path);
}

/**
 * @brief Test that ApplyLoggingConfig correctly sets log level for file output
 */
TEST_F(ConfigurationManagerTestFixture, ApplyLoggingConfigFileLevel) {
  std::string log_file = "/tmp/test_config_mgr_" + std::to_string(getpid()) + ".log";

  // Clean up any existing log file
  if (std::filesystem::exists(log_file)) {
    std::filesystem::remove(log_file);
  }

  // Create config with warn level, file output
  std::string config_path = CreateTempConfig("warn", "json", log_file);

  auto config_mgr_result = ConfigurationManager::Create(config_path, "");
  ASSERT_TRUE(config_mgr_result) << "Failed to create ConfigurationManager: " << config_mgr_result.error().to_string();

  auto config_mgr = std::move(*config_mgr_result);

  // Apply logging config
  auto result = config_mgr->ApplyLoggingConfig();
  ASSERT_TRUE(result) << "ApplyLoggingConfig failed: " << result.error().to_string();

  // Verify log level is set to warn
  EXPECT_EQ(spdlog::get_level(), spdlog::level::warn);

  // Verify log file was created
  EXPECT_TRUE(std::filesystem::exists(log_file));

  // Test that info messages are NOT logged (below warn level)
  spdlog::info("This should NOT be logged");
  spdlog::default_logger()->flush();

  // Test that warn messages ARE logged
  spdlog::warn("This SHOULD be logged");
  spdlog::default_logger()->flush();

  // Read log file contents
  std::string log_contents = ReadFile(log_file);

  // Verify only warn message is present
  EXPECT_EQ(log_contents.find("This should NOT be logged"), std::string::npos)
      << "Info message should not be in log file";
  EXPECT_NE(log_contents.find("This SHOULD be logged"), std::string::npos) << "Warn message should be in log file";

  // Cleanup
  std::filesystem::remove(config_path);
  std::filesystem::remove(log_file);
}

/**
 * @brief Test that ApplyLoggingConfig correctly applies all log levels
 */
TEST_F(ConfigurationManagerTestFixture, ApplyLoggingConfigAllLevels) {
  const std::vector<std::pair<std::string, spdlog::level::level_enum>> levels = {{"debug", spdlog::level::debug},
                                                                                 {"info", spdlog::level::info},
                                                                                 {"warn", spdlog::level::warn},
                                                                                 {"error", spdlog::level::err}};

  for (const auto& [level_str, level_enum] : levels) {
    std::string config_path = CreateTempConfig(level_str, "json", "");

    auto config_mgr_result = ConfigurationManager::Create(config_path, "");
    ASSERT_TRUE(config_mgr_result) << "Failed to create ConfigurationManager for level: " << level_str;

    auto config_mgr = std::move(*config_mgr_result);

    auto result = config_mgr->ApplyLoggingConfig();
    ASSERT_TRUE(result) << "ApplyLoggingConfig failed for level: " << level_str;

    EXPECT_EQ(spdlog::get_level(), level_enum) << "Log level mismatch for: " << level_str;

    std::filesystem::remove(config_path);
  }
}

/**
 * @brief Test that ApplyLoggingConfig creates log directory if it doesn't exist
 */
TEST_F(ConfigurationManagerTestFixture, ApplyLoggingConfigCreatesDirectory) {
  std::string log_dir = "/tmp/test_config_mgr_dir_" + std::to_string(getpid());
  std::string log_file = log_dir + "/test.log";

  // Ensure directory doesn't exist
  if (std::filesystem::exists(log_dir)) {
    std::filesystem::remove_all(log_dir);
  }

  std::string config_path = CreateTempConfig("info", "json", log_file);

  auto config_mgr_result = ConfigurationManager::Create(config_path, "");
  ASSERT_TRUE(config_mgr_result);

  auto config_mgr = std::move(*config_mgr_result);

  auto result = config_mgr->ApplyLoggingConfig();
  ASSERT_TRUE(result) << "ApplyLoggingConfig failed: " << result.error().to_string();

  // Verify directory was created
  EXPECT_TRUE(std::filesystem::exists(log_dir)) << "Log directory should be created";
  EXPECT_TRUE(std::filesystem::is_directory(log_dir)) << "Log path should be a directory";

  // Verify log file can be written to
  spdlog::info("Test message");
  spdlog::default_logger()->flush();

  EXPECT_TRUE(std::filesystem::exists(log_file)) << "Log file should exist";
  EXPECT_GT(std::filesystem::file_size(log_file), 0) << "Log file should not be empty";

  // Cleanup
  std::filesystem::remove(config_path);
  std::filesystem::remove_all(log_dir);
}

/**
 * @brief Test that ApplyLoggingConfig correctly sets format (JSON vs TEXT)
 */
TEST_F(ConfigurationManagerTestFixture, ApplyLoggingConfigFormat) {
  // Test JSON format
  {
    std::string config_path = CreateTempConfig("info", "json", "");

    auto config_mgr_result = ConfigurationManager::Create(config_path, "");
    ASSERT_TRUE(config_mgr_result);

    auto config_mgr = std::move(*config_mgr_result);
    auto result = config_mgr->ApplyLoggingConfig();
    ASSERT_TRUE(result);

    EXPECT_EQ(::mygram::utils::StructuredLog::GetFormat(), ::mygram::utils::LogFormat::JSON);

    std::filesystem::remove(config_path);
  }

  // Test TEXT format
  {
    std::string config_path = CreateTempConfig("info", "text", "");

    auto config_mgr_result = ConfigurationManager::Create(config_path, "");
    ASSERT_TRUE(config_mgr_result);

    auto config_mgr = std::move(*config_mgr_result);
    auto result = config_mgr->ApplyLoggingConfig();
    ASSERT_TRUE(result);

    EXPECT_EQ(::mygram::utils::StructuredLog::GetFormat(), ::mygram::utils::LogFormat::TEXT);

    std::filesystem::remove(config_path);
  }
}

/**
 * @brief Test that file logger receives correct log level (regression test for bug)
 */
TEST_F(ConfigurationManagerTestFixture, ApplyLoggingConfigFileLoggerReceivesLevel) {
  std::string log_file = "/tmp/test_file_logger_level_" + std::to_string(getpid()) + ".log";

  // Clean up any existing log file
  if (std::filesystem::exists(log_file)) {
    std::filesystem::remove(log_file);
  }

  // Create config with DEBUG level and file output
  std::string config_path = CreateTempConfig("debug", "text", log_file);

  auto config_mgr_result = ConfigurationManager::Create(config_path, "");
  ASSERT_TRUE(config_mgr_result);

  auto config_mgr = std::move(*config_mgr_result);

  // Apply logging config
  auto result = config_mgr->ApplyLoggingConfig();
  ASSERT_TRUE(result) << "ApplyLoggingConfig failed: " << result.error().to_string();

  // Log messages at different levels
  spdlog::debug("DEBUG message");
  spdlog::info("INFO message");
  spdlog::warn("WARN message");
  spdlog::default_logger()->flush();

  // Read log file
  std::string log_contents = ReadFile(log_file);

  // Verify all messages are present (debug level should allow all)
  EXPECT_NE(log_contents.find("DEBUG message"), std::string::npos)
      << "DEBUG message should be logged with debug level. Log contents:\n"
      << log_contents;
  EXPECT_NE(log_contents.find("INFO message"), std::string::npos) << "INFO message should be logged with debug level";
  EXPECT_NE(log_contents.find("WARN message"), std::string::npos) << "WARN message should be logged with debug level";

  // Cleanup
  std::filesystem::remove(config_path);
  std::filesystem::remove(log_file);
}

/**
 * @brief Test that ApplyLoggingConfig handles invalid log file path gracefully
 */
TEST_F(ConfigurationManagerTestFixture, ApplyLoggingConfigInvalidPath) {
  // Use an invalid path (root directory, typically not writable)
  std::string invalid_path = "/invalid_root_path_123456/test.log";

  std::string config_path = CreateTempConfig("info", "json", invalid_path);

  auto config_mgr_result = ConfigurationManager::Create(config_path, "");
  ASSERT_TRUE(config_mgr_result);

  auto config_mgr = std::move(*config_mgr_result);

  // ApplyLoggingConfig should fail gracefully
  auto result = config_mgr->ApplyLoggingConfig();
  EXPECT_FALSE(result) << "ApplyLoggingConfig should fail with invalid path";
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kIOError);

  // Cleanup
  std::filesystem::remove(config_path);
}

/**
 * @brief Test that ReopenLogFile successfully reopens log file for rotation
 */
TEST_F(ConfigurationManagerTestFixture, ReopenLogFileRotation) {
  std::string log_dir = "/tmp/test_log_rotation_" + std::to_string(getpid());
  std::string log_file = log_dir + "/app.log";
  std::string rotated_file = log_dir + "/app.log.1";

  // Ensure clean state
  if (std::filesystem::exists(log_dir)) {
    std::filesystem::remove_all(log_dir);
  }
  std::filesystem::create_directories(log_dir);

  std::string config_path = CreateTempConfig("info", "text", log_file);

  auto config_mgr_result = ConfigurationManager::Create(config_path, "");
  ASSERT_TRUE(config_mgr_result);

  auto config_mgr = std::move(*config_mgr_result);

  // Apply logging config first
  auto apply_result = config_mgr->ApplyLoggingConfig();
  ASSERT_TRUE(apply_result) << "ApplyLoggingConfig failed: " << apply_result.error().to_string();

  // Write message before rotation
  spdlog::info("Before rotation message");
  spdlog::default_logger()->flush();

  // Simulate log rotation: rename current log file
  ASSERT_TRUE(std::filesystem::exists(log_file)) << "Log file should exist before rotation";
  std::filesystem::rename(log_file, rotated_file);

  // Call ReopenLogFile
  auto reopen_result = config_mgr->ReopenLogFile();
  ASSERT_TRUE(reopen_result) << "ReopenLogFile failed: " << reopen_result.error().to_string();

  // Write message after rotation
  spdlog::info("After rotation message");
  spdlog::default_logger()->flush();

  // Verify both files exist
  EXPECT_TRUE(std::filesystem::exists(log_file)) << "New log file should be created after reopen";
  EXPECT_TRUE(std::filesystem::exists(rotated_file)) << "Rotated log file should still exist";

  // Verify content of old (rotated) log
  std::string old_content = ReadFile(rotated_file);
  EXPECT_NE(old_content.find("Before rotation message"), std::string::npos)
      << "Old log should contain message before rotation";
  EXPECT_EQ(old_content.find("After rotation message"), std::string::npos)
      << "Old log should NOT contain message after rotation";

  // Verify content of new log
  std::string new_content = ReadFile(log_file);
  EXPECT_NE(new_content.find("After rotation message"), std::string::npos)
      << "New log should contain message after rotation";
  EXPECT_NE(new_content.find("log_file_reopened"), std::string::npos)
      << "New log should contain reopen confirmation message";

  // Cleanup
  std::filesystem::remove(config_path);
  std::filesystem::remove_all(log_dir);
}

/**
 * @brief Test that ReopenLogFile is no-op when logging to stdout
 */
TEST_F(ConfigurationManagerTestFixture, ReopenLogFileStdoutNoOp) {
  // Create config without log file (stdout logging)
  std::string config_path = CreateTempConfig("info", "text", "");

  auto config_mgr_result = ConfigurationManager::Create(config_path, "");
  ASSERT_TRUE(config_mgr_result);

  auto config_mgr = std::move(*config_mgr_result);

  // Apply logging config
  auto apply_result = config_mgr->ApplyLoggingConfig();
  ASSERT_TRUE(apply_result);

  // ReopenLogFile should succeed (no-op for stdout)
  auto reopen_result = config_mgr->ReopenLogFile();
  EXPECT_TRUE(reopen_result) << "ReopenLogFile should succeed for stdout logging";

  // Cleanup
  std::filesystem::remove(config_path);
}

/**
 * @brief Test that ReopenLogFile preserves log level
 */
TEST_F(ConfigurationManagerTestFixture, ReopenLogFilePreservesLevel) {
  std::string log_dir = "/tmp/test_log_level_" + std::to_string(getpid());
  std::string log_file = log_dir + "/app.log";

  // Ensure clean state
  if (std::filesystem::exists(log_dir)) {
    std::filesystem::remove_all(log_dir);
  }
  std::filesystem::create_directories(log_dir);

  // Create config with debug level
  std::string config_path = CreateTempConfig("debug", "text", log_file);

  auto config_mgr_result = ConfigurationManager::Create(config_path, "");
  ASSERT_TRUE(config_mgr_result);

  auto config_mgr = std::move(*config_mgr_result);

  // Apply logging config
  auto apply_result = config_mgr->ApplyLoggingConfig();
  ASSERT_TRUE(apply_result);
  EXPECT_EQ(spdlog::get_level(), spdlog::level::debug);

  // Reopen log file
  auto reopen_result = config_mgr->ReopenLogFile();
  ASSERT_TRUE(reopen_result);

  // Log level should be preserved
  EXPECT_EQ(spdlog::get_level(), spdlog::level::debug) << "Log level should be preserved after reopen";

  // Cleanup
  std::filesystem::remove(config_path);
  std::filesystem::remove_all(log_dir);
}
