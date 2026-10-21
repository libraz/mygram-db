/**
 * @file configuration_manager.cpp
 * @brief Configuration manager implementation
 */

#include "app/configuration_manager.h"

#include <spdlog/spdlog.h>

#include <filesystem>
#include <fstream>
#include <iostream>

#include "query/synonym_dictionary.h"
#include "server/log_field_names.h"
#include "utils/log_manager.h"
#include "utils/string_utils.h"
#include "utils/structured_log.h"

namespace mygramdb::app {

namespace {

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

mygram::utils::Expected<void, mygram::utils::Error> ValidateReadableFile(const std::string& path,
                                                                         const std::string& setting_name) {
  std::error_code status_error;
  const auto status = std::filesystem::status(path, status_error);
  if (status_error || !std::filesystem::is_regular_file(status)) {
    return mygram::utils::MakeUnexpected(mygram::utils::MakeError(
        mygram::utils::ErrorCode::kIOError, setting_name + " does not reference a readable regular file: " + path));
  }

  std::ifstream input(path, std::ios::binary);
  if (!input.is_open() || input.peek() == std::ifstream::traits_type::eof()) {
    return mygram::utils::MakeUnexpected(mygram::utils::MakeError(
        mygram::utils::ErrorCode::kIOError, setting_name + " references an unreadable or empty file: " + path));
  }

  return {};
}

mygram::utils::Expected<void, mygram::utils::Error> ValidateConfigTestReferences(const config::Config& config) {
  if (config.mysql.ssl_enable) {
    const std::pair<const std::string&, const char*> tls_files[] = {
        {config.mysql.ssl_ca, "mysql.ssl_ca"},
        {config.mysql.ssl_cert, "mysql.ssl_cert"},
        {config.mysql.ssl_key, "mysql.ssl_key"},
    };
    for (const auto& [path, setting_name] : tls_files) {
      if (!path.empty()) {
        if (auto result = ValidateReadableFile(path, setting_name); !result) {
          return result;
        }
      }
    }
  }

  for (const auto& table : config.tables) {
    if (!table.synonyms.enable) {
      continue;
    }

    query::SynonymDictionary dictionary;
    const auto normalizer = [&config](std::string_view text) {
      return mygram::utils::NormalizeText(text, config.memory.normalize.nfkc, config.memory.normalize.width,
                                          config.memory.normalize.lower);
    };
    auto result = dictionary.LoadFromFile(table.synonyms.file, normalizer);
    if (!result) {
      return mygram::utils::MakeUnexpected(mygram::utils::MakeError(
          result.error().code(), "tables['" + table.name + "'].synonyms.file: " + result.error().message()));
    }
  }

  return {};
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
  if (auto result = ValidateConfigTestReferences(config_); !result) {
    std::cerr << "Configuration reference validation failed: " << result.error().message() << "\n";
    return 1;
  }

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
  auto& log_manager = mygram::utils::LogManager::Instance();

  // Configure log output (file or stdout) BEFORE setting level
  if (!config_.logging.file.empty()) {
    try {
      // Ensure log directory exists
      std::filesystem::path log_path(config_.logging.file);
      std::filesystem::path log_dir = log_path.parent_path();
      if (!log_dir.empty() && !std::filesystem::exists(log_dir)) {
        std::filesystem::create_directories(log_dir);
      }
    } catch (const std::exception& ex) {
      return mygram::utils::MakeUnexpected(mygram::utils::MakeError(
          mygram::utils::ErrorCode::kIOError, "Failed to create log directory: " + std::string(ex.what())));
    }

    if (auto opened = log_manager.OpenFile(config_.logging.file); !opened) {
      return mygram::utils::MakeUnexpected(mygram::utils::MakeError(
          mygram::utils::ErrorCode::kIOError, "Log file initialization failed: " + opened.error().message()));
    }
  }

  if (auto level = mygram::utils::LogManager::ParseLevel(config_.logging.level); level.has_value()) {
    log_manager.SetLevel(*level);
  } else {
    spdlog::warn("Unknown log level '{}', keeping current level", config_.logging.level);
  }

  // Apply structured log format (JSON or TEXT)
  mygram::utils::StructuredLog::SetFormat(mygram::utils::StructuredLog::ParseFormat(config_.logging.format));

  // Log confirmation message (after logger is configured)
  if (!config_.logging.file.empty()) {
    mygram::utils::StructuredLog()
        .Event("logging_to_file")
        .Field(server::log_fields::kFieldFilepath, config_.logging.file)
        .Info();
  }

  return {};
}

mygram::utils::Expected<void, mygram::utils::Error> ConfigurationManager::AbsolutizeDaemonPaths() {
  try {
    config_.dump.dir = AbsolutePathString(config_.dump.dir);
    config_.logging.file = AbsolutePathString(config_.logging.file);
    config_.mysql.ssl_ca = AbsolutePathString(config_.mysql.ssl_ca);
    config_.mysql.ssl_cert = AbsolutePathString(config_.mysql.ssl_cert);
    config_.mysql.ssl_key = AbsolutePathString(config_.mysql.ssl_key);
    config_.api.unix_socket.path = AbsolutePathString(config_.api.unix_socket.path);
    for (auto& table : config_.tables) {
      table.synonyms.file = AbsolutePathString(table.synonyms.file);
    }
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

  // The default logger and its level stay untouched; only the file underneath is replaced.
  if (auto reopened = mygram::utils::LogManager::Instance().OpenFile(config_.logging.file); !reopened) {
    return mygram::utils::MakeUnexpected(mygram::utils::MakeError(
        mygram::utils::ErrorCode::kIOError, "Log file reopen failed: " + reopened.error().message()));
  }

  mygram::utils::StructuredLog().Event("log_file_reopened").Info();

  return {};
}

}  // namespace mygramdb::app
