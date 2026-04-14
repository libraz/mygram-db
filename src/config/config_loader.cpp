/**
 * @file config_loader.cpp
 * @brief Configuration file loading (YAML, JSON, auto-detect)
 */

#include <spdlog/spdlog.h>
#include <yaml-cpp/yaml.h>

#include <fstream>
#include <nlohmann/json.hpp>
#include <sstream>
#include <string>

#include "config/config.h"
#include "config/config_internal.h"
#include "utils/structured_log.h"

namespace mygramdb::config {

namespace {

using json = nlohmann::json;

// NOLINTNEXTLINE(performance-enum-size)
enum class FileFormat { kYaml, kJson, kUnknown };

constexpr size_t kJsonExtLength = 5;  // ".json"
constexpr size_t kYamlExtLength = 5;  // ".yaml"
constexpr size_t kYmlExtLength = 4;   // ".yml"

FileFormat DetectFileFormat(const std::string& path) {
  if (path.size() >= kJsonExtLength && path.substr(path.size() - kJsonExtLength) == ".json") {
    return FileFormat::kJson;
  }
  if (path.size() >= kYamlExtLength && path.substr(path.size() - kYamlExtLength) == ".yaml") {
    return FileFormat::kYaml;
  }
  if (path.size() >= kYmlExtLength && path.substr(path.size() - kYmlExtLength) == ".yml") {
    return FileFormat::kYaml;
  }
  return FileFormat::kUnknown;
}

}  // namespace

namespace internal {

mygram::utils::Expected<std::string, mygram::utils::Error> ReadFileToString(const std::string& path) {
  using mygram::utils::Error;
  using mygram::utils::ErrorCode;
  using mygram::utils::MakeError;
  using mygram::utils::MakeUnexpected;

  // Check if file exists
  std::ifstream file(path);
  if (!file.is_open()) {
    // Try to determine the reason for failure
    std::ifstream test_file(path, std::ios::in);
    if (!test_file.good()) {
      // File doesn't exist or is not accessible
      std::stringstream err_msg;
      err_msg << "Failed to open configuration file: " << path << "\n";
      err_msg << "  Possible reasons:\n";
      err_msg << "    - File does not exist\n";
      err_msg << "    - Insufficient read permissions\n";
      err_msg << "    - Invalid file path\n";
      err_msg << "  Please verify:\n";
      err_msg << "    - The file path is correct\n";
      err_msg << "    - The file exists and is readable\n";
      err_msg << "  Example config: examples/config.yaml";
      return MakeUnexpected(MakeError(ErrorCode::kConfigFileNotFound, err_msg.str(), path));
    }
    return MakeUnexpected(MakeError(ErrorCode::kConfigFileNotFound, "Failed to open file: " + path, path));
  }
  std::stringstream buffer;
  buffer << file.rdbuf();

  // Check if file is empty
  std::string content = buffer.str();
  if (content.empty()) {
    std::stringstream err_msg;
    err_msg << "Configuration file is empty: " << path << "\n";
    err_msg << "  Please provide a valid configuration file.\n";
    err_msg << "  Example config: examples/config.yaml";
    return MakeUnexpected(MakeError(ErrorCode::kConfigParseError, err_msg.str(), path));
  }

  return content;
}

}  // namespace internal

mygram::utils::Expected<Config, mygram::utils::Error> LoadConfigJson(const std::string& path,
                                                                     const std::string& schema_path) {
  using mygram::utils::Error;
  using mygram::utils::ErrorCode;
  using mygram::utils::MakeError;
  using mygram::utils::MakeUnexpected;

  try {
    // Read config file
    auto config_str_result = internal::ReadFileToString(path);
    if (!config_str_result) {
      return MakeUnexpected(config_str_result.error());
    }
    std::string config_str = *config_str_result;

    // Parse JSON
    json config_json;
    try {
      config_json = json::parse(config_str);
    } catch (const json::parse_error& e) {
      std::stringstream err_msg;
      err_msg << "JSON parse error in configuration file: " << path << "\n";
      err_msg << "  Error details: " << e.what() << "\n";
      if (e.byte != 0) {
        err_msg << "  Error position: byte " << e.byte << "\n";
      }
      err_msg << "  Common issues:\n";
      err_msg << "    - Missing or extra commas\n";
      err_msg << "    - Unquoted string values\n";
      err_msg << "    - Invalid escape sequences\n";
      err_msg << "    - Mismatched brackets or braces\n";
      err_msg << "  Tip: Use a JSON validator to check syntax\n";
      err_msg << "  Example config: examples/config.yaml";
      return MakeUnexpected(MakeError(ErrorCode::kConfigJsonError, err_msg.str(), path));
    }

    // Read schema if provided
    std::string schema_str;
    if (!schema_path.empty()) {
      auto schema_result = internal::ReadFileToString(schema_path);
      if (!schema_result) {
        return MakeUnexpected(schema_result.error());
      }
      schema_str = *schema_result;
    }

    // Validate config
    auto validation_result = ValidateConfigJson(config_str, schema_str);
    if (!validation_result) {
      return MakeUnexpected(validation_result.error());
    }

    // Parse config from JSON object
    auto config_result = internal::ParseConfigFromJson(config_json);
    if (!config_result) {
      return MakeUnexpected(config_result.error());
    }
    auto& config = *config_result;

    // Apply log format immediately so subsequent logs use the configured format
    mygram::utils::StructuredLog::SetFormat(mygram::utils::StructuredLog::ParseFormat(config.logging.format));

    mygram::utils::StructuredLog()
        .Event("config_loaded")
        .Field("path", path)
        .Field("tables", static_cast<uint64_t>(config.tables.size()))
        .Field("mysql_host", config.mysql.host)
        .Field("mysql_port", static_cast<uint64_t>(config.mysql.port))
        .Info();

    return config;
  } catch (const std::exception& e) {
    return MakeUnexpected(MakeError(ErrorCode::kConfigParseError,
                                    std::string("Failed to load config from ") + path + ": " + e.what(), path));
  }
}

mygram::utils::Expected<Config, mygram::utils::Error> LoadConfigYaml(const std::string& path) {
  using mygram::utils::Error;
  using mygram::utils::ErrorCode;
  using mygram::utils::MakeError;
  using mygram::utils::MakeUnexpected;

  try {
    YAML::Node yaml_root = YAML::LoadFile(path);
    json json_root = internal::YamlToJson(yaml_root);

    // Parse config from JSON object
    auto config_result = internal::ParseConfigFromJson(json_root);
    if (!config_result) {
      return MakeUnexpected(config_result.error());
    }
    auto& config = *config_result;

    // Apply log format immediately so subsequent logs use the configured format
    mygram::utils::StructuredLog::SetFormat(mygram::utils::StructuredLog::ParseFormat(config.logging.format));

    mygram::utils::StructuredLog()
        .Event("config_loaded")
        .Field("path", path)
        .Field("tables", static_cast<uint64_t>(config.tables.size()))
        .Field("mysql_host", config.mysql.host)
        .Field("mysql_port", static_cast<uint64_t>(config.mysql.port))
        .Info();

    return config;
  } catch (const YAML::Exception& e) {
    std::stringstream err_msg;
    err_msg << "YAML parse error in configuration file: " << path << "\n";
    err_msg << "  Error details: " << e.what() << "\n";
    if (e.mark.line != static_cast<size_t>(-1)) {
      err_msg << "  Error location: line " << (e.mark.line + 1) << ", column " << (e.mark.column + 1) << "\n";
    }
    err_msg << "  Common issues:\n";
    err_msg << "    - Incorrect indentation (use spaces, not tabs)\n";
    err_msg << "    - Missing colon after key name\n";
    err_msg << "    - Unquoted special characters in values\n";
    err_msg << "    - Inconsistent list formatting\n";
    err_msg << "  Tip: Check YAML syntax, especially indentation\n";
    err_msg << "  Example config: examples/config.yaml";
    return MakeUnexpected(MakeError(ErrorCode::kConfigYamlError, err_msg.str(), path));
  } catch (const std::exception& e) {
    return MakeUnexpected(MakeError(ErrorCode::kConfigParseError,
                                    std::string("Failed to load config from ") + path + ": " + e.what(), path));
  }
}

mygram::utils::Expected<Config, mygram::utils::Error> LoadConfig(const std::string& path,
                                                                 const std::string& schema_path) {
  using mygram::utils::Error;
  using mygram::utils::ErrorCode;
  using mygram::utils::MakeError;
  using mygram::utils::MakeUnexpected;

  FileFormat format = DetectFileFormat(path);

  switch (format) {
    case FileFormat::kJson:
      mygram::utils::StructuredLog()
          .Event("config_format_detected")
          .Field("format", "json")
          .Field("path", path)
          .Debug();
      return LoadConfigJson(path, schema_path);

    case FileFormat::kYaml:
      mygram::utils::StructuredLog()
          .Event("config_format_detected")
          .Field("format", "yaml")
          .Field("path", path)
          .Debug();
      // Parse YAML once, validate, then reuse the parsed JSON
      try {
        YAML::Node yaml_root = YAML::LoadFile(path);
        json json_root = internal::YamlToJson(yaml_root);
        std::string config_json_str = json_root.dump();
        std::string schema_str;
        if (!schema_path.empty()) {
          auto schema_result = internal::ReadFileToString(schema_path);
          if (!schema_result) {
            return MakeUnexpected(schema_result.error());
          }
          schema_str = *schema_result;
        }
        // ValidateConfigJson will use embedded schema if schema_str is empty
        auto validation_result = ValidateConfigJson(config_json_str, schema_str);
        if (!validation_result) {
          return MakeUnexpected(validation_result.error());
        }

        // Reuse the already-parsed JSON instead of re-reading and re-parsing the file
        auto config_result = internal::ParseConfigFromJson(json_root);
        if (!config_result) {
          return MakeUnexpected(config_result.error());
        }
        auto& config = *config_result;

        // Apply log format immediately so subsequent logs use the configured format
        mygram::utils::StructuredLog::SetFormat(mygram::utils::StructuredLog::ParseFormat(config.logging.format));

        mygram::utils::StructuredLog()
            .Event("config_loaded")
            .Field("path", path)
            .Field("tables", static_cast<uint64_t>(config.tables.size()))
            .Field("mysql_host", config.mysql.host)
            .Field("mysql_port", static_cast<uint64_t>(config.mysql.port))
            .Info();

        return config;
      } catch (const YAML::Exception& e) {
        std::stringstream err_msg;
        err_msg << "YAML parse error in configuration file: " << path << "\n";
        err_msg << "  Error details: " << e.what() << "\n";
        if (e.mark.line != static_cast<size_t>(-1)) {
          err_msg << "  Error location: line " << (e.mark.line + 1) << ", column " << (e.mark.column + 1) << "\n";
        }
        err_msg << "  Common issues:\n";
        err_msg << "    - Incorrect indentation (use spaces, not tabs)\n";
        err_msg << "    - Missing colon after key name\n";
        err_msg << "    - Unquoted special characters in values\n";
        err_msg << "    - Inconsistent list formatting\n";
        err_msg << "  Tip: Check YAML syntax, especially indentation\n";
        err_msg << "  Example config: examples/config.yaml";
        return MakeUnexpected(MakeError(ErrorCode::kConfigYamlError, err_msg.str(), path));
      } catch (const std::exception& e) {
        return MakeUnexpected(MakeError(ErrorCode::kConfigParseError,
                                        std::string("Failed to load config from ") + path + ": " + e.what(), path));
      }

    case FileFormat::kUnknown:
    default:
      // Try YAML first (legacy default), then JSON
      mygram::utils::StructuredLog()
          .Event("config_format_unknown")
          .Field("path", path)
          .Field("trying", "yaml_first")
          .Debug();
      auto yaml_result = LoadConfigYaml(path);
      if (yaml_result) {
        return yaml_result;
      }

      mygram::utils::StructuredLog()
          .Event("config_fallback")
          .Field("path", path)
          .Field("from", "yaml")
          .Field("to", "json")
          .Debug();
      auto json_result = LoadConfigJson(path, schema_path);
      if (json_result) {
        return json_result;
      }

      // Both failed - return a combined error
      std::stringstream err_msg;
      err_msg << "Failed to load configuration file: " << path << "\n";
      err_msg << "  File format could not be determined (.yaml, .yml, or .json expected)\n";
      err_msg << "  Attempted YAML parsing: " << yaml_result.error().message() << "\n";
      err_msg << "  Attempted JSON parsing: " << json_result.error().message() << "\n";
      err_msg << "  Please ensure the file has the correct extension and valid syntax.";
      return MakeUnexpected(MakeError(ErrorCode::kConfigParseError, err_msg.str(), path));
  }
}

}  // namespace mygramdb::config
