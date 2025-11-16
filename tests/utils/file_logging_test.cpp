/**
 * @file file_logging_test.cpp
 * @brief Integration tests for file logging functionality
 */

#include <gtest/gtest.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/spdlog.h>
#include <unistd.h>

#include <cstdio>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <string>

namespace {

/**
 * @brief Generate a unique temporary file path using mkstemp
 * @return Temporary file path
 */
std::string GenerateTempFilePath() {
  char temp_template[] = "/tmp/mygramdb_test_XXXXXX";
  int fd = mkstemp(temp_template);
  if (fd == -1) {
    throw std::runtime_error("Failed to create temporary file");
  }
  close(fd);
  unlink(temp_template);  // Remove the file, we just need the path
  return std::string(temp_template);
}

/**
 * @brief Read entire file content
 */
std::string ReadFileContent(const std::string& filepath) {
  std::ifstream ifs(filepath);
  if (!ifs) {
    return "";
  }
  std::string content((std::istreambuf_iterator<char>(ifs)), std::istreambuf_iterator<char>());
  return content;
}

}  // namespace

/**
 * @brief Test basic file logging
 */
TEST(FileLoggingTest, BasicFileLogging) {
  std::string temp_file = GenerateTempFilePath();
  std::string log_file = temp_file + ".log";

  // Ensure file doesn't exist
  std::filesystem::remove(log_file);

  try {
    // Create file logger
    auto logger = spdlog::basic_logger_mt("test_logger", log_file);
    logger->info("Test message 1");
    logger->warn("Test message 2");
    logger->error("Test message 3");
    logger->flush();

    // Read log file
    std::string content = ReadFileContent(log_file);

    // Verify content
    EXPECT_NE(content.find("Test message 1"), std::string::npos);
    EXPECT_NE(content.find("Test message 2"), std::string::npos);
    EXPECT_NE(content.find("Test message 3"), std::string::npos);

    // Cleanup
    spdlog::drop("test_logger");
    std::filesystem::remove(log_file);
  } catch (const spdlog::spdlog_ex& ex) {
    FAIL() << "spdlog exception: " << ex.what();
  }
}

/**
 * @brief Test file logging with directory creation
 */
TEST(FileLoggingTest, DirectoryCreation) {
  std::string temp_dir = GenerateTempFilePath();
  std::string log_dir = temp_dir + "_logs";
  std::string log_file = log_dir + "/test.log";

  // Ensure directory doesn't exist
  std::filesystem::remove_all(log_dir);

  try {
    // Create directory
    std::filesystem::path log_path(log_file);
    std::filesystem::path parent_dir = log_path.parent_path();
    if (!parent_dir.empty() && !std::filesystem::exists(parent_dir)) {
      std::filesystem::create_directories(parent_dir);
    }

    // Create file logger
    auto logger = spdlog::basic_logger_mt("test_logger_dir", log_file);
    logger->info("Test with directory creation");
    logger->flush();

    // Verify file exists
    EXPECT_TRUE(std::filesystem::exists(log_file));

    // Verify content
    std::string content = ReadFileContent(log_file);
    EXPECT_NE(content.find("Test with directory creation"), std::string::npos);

    // Cleanup
    spdlog::drop("test_logger_dir");
    std::filesystem::remove_all(log_dir);
  } catch (const spdlog::spdlog_ex& ex) {
    FAIL() << "spdlog exception: " << ex.what();
  }
}

/**
 * @brief Test file logging with multiple messages
 */
TEST(FileLoggingTest, MultipleMessages) {
  std::string temp_file = GenerateTempFilePath();
  std::string log_file = temp_file + ".log";

  std::filesystem::remove(log_file);

  try {
    auto logger = spdlog::basic_logger_mt("test_logger_multi", log_file);

    // Write multiple messages
    const int num_messages = 100;
    for (int i = 0; i < num_messages; ++i) {
      logger->info("Message number {}", i);
    }
    logger->flush();

    // Read and verify
    std::string content = ReadFileContent(log_file);
    EXPECT_NE(content.find("Message number 0"), std::string::npos);
    EXPECT_NE(content.find("Message number 99"), std::string::npos);

    // Count lines
    size_t line_count = std::count(content.begin(), content.end(), '\n');
    EXPECT_EQ(line_count, num_messages);

    // Cleanup
    spdlog::drop("test_logger_multi");
    std::filesystem::remove(log_file);
  } catch (const spdlog::spdlog_ex& ex) {
    FAIL() << "spdlog exception: " << ex.what();
  }
}

/**
 * @brief Test file logging with nested directory path
 */
TEST(FileLoggingTest, NestedDirectoryPath) {
  std::string temp_base = GenerateTempFilePath();
  std::string log_dir = temp_base + "_logs/subdir1/subdir2";
  std::string log_file = log_dir + "/nested.log";

  std::filesystem::remove_all(temp_base + "_logs");

  try {
    // Create nested directories
    std::filesystem::path log_path(log_file);
    std::filesystem::path parent_dir = log_path.parent_path();
    if (!parent_dir.empty() && !std::filesystem::exists(parent_dir)) {
      std::filesystem::create_directories(parent_dir);
    }

    // Create logger
    auto logger = spdlog::basic_logger_mt("test_logger_nested", log_file);
    logger->info("Nested directory test");
    logger->flush();

    // Verify
    EXPECT_TRUE(std::filesystem::exists(log_file));
    std::string content = ReadFileContent(log_file);
    EXPECT_NE(content.find("Nested directory test"), std::string::npos);

    // Cleanup
    spdlog::drop("test_logger_nested");
    std::filesystem::remove_all(temp_base + "_logs");
  } catch (const spdlog::spdlog_ex& ex) {
    FAIL() << "spdlog exception: " << ex.what();
  }
}

/**
 * @brief Test switching from stdout to file logger
 */
TEST(FileLoggingTest, SwitchToFileLogger) {
  std::string temp_file = GenerateTempFilePath();
  std::string log_file = temp_file + ".log";

  std::filesystem::remove(log_file);

  try {
    // Start with default logger (stdout)
    spdlog::info("This goes to stdout");

    // Switch to file logger
    auto file_logger = spdlog::basic_logger_mt("mygramdb", log_file);
    spdlog::set_default_logger(file_logger);
    spdlog::info("This goes to file");
    file_logger->flush();

    // Verify file content
    std::string content = ReadFileContent(log_file);
    EXPECT_NE(content.find("This goes to file"), std::string::npos);
    EXPECT_EQ(content.find("This goes to stdout"), std::string::npos);  // Should not be in file

    // Cleanup
    spdlog::set_default_logger(spdlog::default_logger());
    spdlog::drop("mygramdb");
    std::filesystem::remove(log_file);
  } catch (const spdlog::spdlog_ex& ex) {
    FAIL() << "spdlog exception: " << ex.what();
  }
}

/**
 * @brief Test file logger error handling (invalid path)
 */
TEST(FileLoggingTest, InvalidPath) {
  // Try to create logger with invalid path (read-only directory on Unix)
  std::string invalid_path = "/root/impossible/path/test.log";

  bool exception_thrown = false;
  try {
    auto logger = spdlog::basic_logger_mt("test_invalid", invalid_path);
  } catch (const spdlog::spdlog_ex& ex) {
    exception_thrown = true;
  }

  EXPECT_TRUE(exception_thrown);

  // Cleanup if somehow created
  try {
    spdlog::drop("test_invalid");
  } catch (...) {
    // Ignore
  }
}
