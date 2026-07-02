/**
 * @file configuration_manager.cpp
 * @brief Configuration manager implementation
 */

#include "app/configuration_manager.h"

#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/spdlog.h>

#include <filesystem>
#include <iostream>
#include <mutex>

#include "utils/structured_log.h"

namespace mygramdb::app {

namespace {

std::mutex& LoggerReconfigurationMutex() {
  static std::mutex mutex;
  return mutex;
}

std::shared_ptr<spdlog::logger> CreateFileLogger(const std::string& path) {
  auto sink = std::make_shared<spdlog::sinks::basic_file_sink_mt>(path, true);
  return std::make_shared<spdlog::logger>("mygramdb", std::move(sink));
}

std::string AbsolutePathString(const std::string& path) {
  if (path.empty()) {
    return path;
  }
  std::filesystem::path fs_path(path);
  if (fs_path.is_absolute()) {
    return fs_path.lexically_normal().string();
  }
  return std::filesystem::absolute(fs_path).lexically_normal().string();
}

}  // namespace

mygram::utils::Expected<std::unique_ptr<ConfigurationManager>, mygram::utils::Error> ConfigurationManager::Create(
    const std::string& config_file, const std::string& schema_file) {
  // Load initial configuration
  auto config_result = config::LoadConfig(config_file, schema_file);
  if (!config_result) {
    return mygram::utils::MakeUnexpected(config_result.error());
  }

  // Create manager with loaded config
  auto manager = std::unique_ptr<ConfigurationManager>(
      new ConfigurationManager(config_file, schema_file, std::move(*config_result)));

  return manager;
}

ConfigurationManager::ConfigurationManager(std::string config_file, std::string schema_file,
                                           config::Config initial_config)
    : config_file_(std::move(config_file)), schema_file_(std::move(schema_file)), config_(std::move(initial_config)) {}

int ConfigurationManager::PrintConfigTest() const {
  std::cout << "Configuration file syntax is OK\n";
  std::cout << "Configuration details:\n";
  std::cout << "  MySQL: " << config_.mysql.user << "@" << config_.mysql.host << ":" << config_.mysql.port << "\n";
  std::cout << "  Tables: " << config_.tables.size() << "\n";
  for (const auto& table : config_.tables) {
    std::cout << "    - " << table.name << " (primary_key: " << table.primary_key
              << ", ngram_size: " << table.ngram_size << ")\n";
  }
  std::cout << "  API TCP: " << config_.api.tcp.bind << ":" << config_.api.tcp.port << "\n";
  std::cout << "  Replication: " << (config_.replication.enable ? "enabled" : "disabled") << "\n";
  std::cout << "  Logging level: " << config_.logging.level << "\n";
  return 0;
}

mygram::utils::Expected<void, mygram::utils::Error> ConfigurationManager::ApplyLoggingConfig() const {
  std::lock_guard<std::mutex> reconfigure_lock(LoggerReconfigurationMutex());

  // Configure log output (file or stdout) BEFORE setting level
  if (!config_.logging.file.empty()) {
    try {
      // Ensure log directory exists
      std::filesystem::path log_path(config_.logging.file);
      std::filesystem::path log_dir = log_path.parent_path();
      if (!log_dir.empty() && !std::filesystem::exists(log_dir)) {
        std::filesystem::create_directories(log_dir);
      }

      // Create a fresh logger and publish it as the default without dropping
      // the previous logger out from under concurrent log calls.
      auto file_logger = CreateFileLogger(config_.logging.file);
      spdlog::set_default_logger(file_logger);
    } catch (const spdlog::spdlog_ex& ex) {
      return mygram::utils::MakeUnexpected(mygram::utils::MakeError(
          mygram::utils::ErrorCode::kIOError, "Log file initialization failed: " + std::string(ex.what())));
    } catch (const std::exception& ex) {
      return mygram::utils::MakeUnexpected(mygram::utils::MakeError(
          mygram::utils::ErrorCode::kIOError, "Failed to create log directory: " + std::string(ex.what())));
    }
  }

  // Apply logging level (must be AFTER setting default logger)
  if (config_.logging.level == "debug") {
    spdlog::set_level(spdlog::level::debug);
  } else if (config_.logging.level == "info") {
    spdlog::set_level(spdlog::level::info);
  } else if (config_.logging.level == "warn") {
    spdlog::set_level(spdlog::level::warn);
  } else if (config_.logging.level == "error") {
    spdlog::set_level(spdlog::level::err);
  } else {
    spdlog::warn("Unknown log level '{}', keeping current level", config_.logging.level);
  }

  // Apply structured log format (JSON or TEXT)
  mygram::utils::StructuredLog::SetFormat(mygram::utils::StructuredLog::ParseFormat(config_.logging.format));

  // Log confirmation message (after logger is configured)
  if (!config_.logging.file.empty()) {
    mygram::utils::StructuredLog().Event("logging_to_file").Field("path", config_.logging.file).Info();
  }

  return {};
}

mygram::utils::Expected<void, mygram::utils::Error> ConfigurationManager::AbsolutizeDaemonPaths() {
  try {
    config_.dump.dir = AbsolutePathString(config_.dump.dir);
    config_.logging.file = AbsolutePathString(config_.logging.file);
  } catch (const std::exception& ex) {
    return mygram::utils::MakeUnexpected(mygram::utils::MakeError(
        mygram::utils::ErrorCode::kIOError, "Failed to absolutize daemon paths: " + std::string(ex.what())));
  }

  return {};
}

mygram::utils::Expected<void, mygram::utils::Error> ConfigurationManager::ReopenLogFile() const {
  // No-op if logging to stdout
  if (config_.logging.file.empty()) {
    return {};
  }

  std::lock_guard<std::mutex> reconfigure_lock(LoggerReconfigurationMutex());

  try {
    // Get current log level before swapping logger
    auto current_level = spdlog::get_level();

    // Create a new file logger and swap it into the default logger slot.
    // The old default logger remains alive until existing shared owners finish.
    auto file_logger = CreateFileLogger(config_.logging.file);
    spdlog::set_default_logger(file_logger);

    // Restore log level
    spdlog::set_level(current_level);

    mygram::utils::StructuredLog().Event("log_file_reopened").Info();
  } catch (const spdlog::spdlog_ex& ex) {
    return mygram::utils::MakeUnexpected(mygram::utils::MakeError(mygram::utils::ErrorCode::kIOError,
                                                                  "Log file reopen failed: " + std::string(ex.what())));
  }

  return {};
}

}  // namespace mygramdb::app
