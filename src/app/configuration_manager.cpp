/**
 * @file configuration_manager.cpp
 * @brief Configuration manager implementation
 */

#include "app/configuration_manager.h"

#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/spdlog.h>

#include <filesystem>
#include <iostream>

namespace mygramdb::app {

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

mygram::utils::Expected<void, mygram::utils::Error> ConfigurationManager::Reload() {
  spdlog::info("Reloading configuration from: {}", config_file_);

  // Load new configuration
  auto reload_result = config::LoadConfig(config_file_, schema_file_);
  if (!reload_result) {
    // Log error but don't update config (keep old config)
    spdlog::error("Failed to reload configuration: {}", reload_result.error().to_string());
    spdlog::warn("Continuing with current configuration");
    return mygram::utils::MakeUnexpected(reload_result.error());
  }

  config::Config new_config = std::move(*reload_result);

  // Apply logging level changes
  if (new_config.logging.level != config_.logging.level) {
    if (new_config.logging.level == "debug") {
      spdlog::set_level(spdlog::level::debug);
    } else if (new_config.logging.level == "info") {
      spdlog::set_level(spdlog::level::info);
    } else if (new_config.logging.level == "warn") {
      spdlog::set_level(spdlog::level::warn);
    } else if (new_config.logging.level == "error") {
      spdlog::set_level(spdlog::level::err);
    }
    spdlog::info("Logging level changed: {} -> {}", config_.logging.level, new_config.logging.level);
  }

  // Update config (atomic replacement)
  config_ = std::move(new_config);

  spdlog::info("Configuration reload completed successfully");
  return {};
}

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

mygram::utils::Expected<void, mygram::utils::Error> ConfigurationManager::ApplyLoggingConfig() {
  // Apply logging level
  if (config_.logging.level == "debug") {
    spdlog::set_level(spdlog::level::debug);
  } else if (config_.logging.level == "info") {
    spdlog::set_level(spdlog::level::info);
  } else if (config_.logging.level == "warn") {
    spdlog::set_level(spdlog::level::warn);
  } else if (config_.logging.level == "error") {
    spdlog::set_level(spdlog::level::err);
  }

  // Configure log output (file or stdout)
  if (!config_.logging.file.empty()) {
    try {
      // Ensure log directory exists
      std::filesystem::path log_path(config_.logging.file);
      std::filesystem::path log_dir = log_path.parent_path();
      if (!log_dir.empty() && !std::filesystem::exists(log_dir)) {
        std::filesystem::create_directories(log_dir);
      }

      // Create file logger
      auto file_logger = spdlog::basic_logger_mt("mygramdb", config_.logging.file);
      spdlog::set_default_logger(file_logger);
      spdlog::info("Logging to file: {}", config_.logging.file);
    } catch (const spdlog::spdlog_ex& ex) {
      return mygram::utils::MakeUnexpected(mygram::utils::MakeError(
          mygram::utils::ErrorCode::kIOError, "Log file initialization failed: " + std::string(ex.what())));
    } catch (const std::exception& ex) {
      return mygram::utils::MakeUnexpected(mygram::utils::MakeError(
          mygram::utils::ErrorCode::kIOError, "Failed to create log directory: " + std::string(ex.what())));
    }
  }

  return {};
}

}  // namespace mygramdb::app
