/**
 * @file config_internal.h
 * @brief Internal declarations shared across config translation units
 */

#pragma once

#include <yaml-cpp/yaml.h>

#include <nlohmann/json.hpp>
#include <string>

#include "config/config.h"
#include "utils/error.h"
#include "utils/expected.h"

namespace mygramdb::config::internal {

/**
 * @brief Convert YAML node to JSON object recursively
 */
nlohmann::json YamlToJson(const YAML::Node& node);

/**
 * @brief Parse configuration from JSON object
 */
Config ParseConfigFromJson(const nlohmann::json& root);

/**
 * @brief Read file contents as string
 */
mygram::utils::Expected<std::string, mygram::utils::Error> ReadFileToString(const std::string& path);

/**
 * @brief Validate path for directory traversal attacks
 * @param path Path to validate
 * @param field_name Name of the field for error messages
 * @throws std::runtime_error if path contains traversal sequences
 */
void ValidatePathNoTraversal(const std::string& path, const std::string& field_name);

/**
 * @brief Validate bind address format
 * @param address Bind address to validate
 * @param field_name Name of the field for error messages
 * @throws std::runtime_error if address is invalid
 */
void ValidateBindAddress(const std::string& address, const std::string& field_name);

}  // namespace mygramdb::config::internal
