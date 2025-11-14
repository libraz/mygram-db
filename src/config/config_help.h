/**
 * @file config_help.h
 * @brief Configuration help system for runtime configuration guidance
 */

#pragma once

#include <cstdint>
#include <map>
#include <nlohmann/json.hpp>
#include <optional>
#include <string>
#include <vector>

#include "config/config.h"

namespace mygramdb::config {

/**
 * @brief Configuration help information
 */
struct ConfigHelpInfo {
  std::string path;                          // e.g., "mysql.port"
  std::string type;                          // e.g., "integer"
  std::string description;                   // From schema
  std::optional<std::string> default_value;  // If specified
  std::vector<std::string> allowed_values;   // For enums
  std::optional<int64_t> minimum;            // For numbers
  std::optional<int64_t> maximum;            // For numbers
  std::optional<double> minimum_number;      // For floating point numbers
  std::optional<double> maximum_number;      // For floating point numbers
  bool required = false;                     // If required in parent
};

/**
 * @brief Configuration schema explorer
 *
 * Provides runtime access to JSON Schema metadata for configuration help
 */
class ConfigSchemaExplorer {
 public:
  /**
   * @brief Initialize from embedded schema
   *
   * @throws std::runtime_error if schema cannot be loaded or parsed
   */
  ConfigSchemaExplorer();

  /**
   * @brief Get help for a configuration path
   *
   * @param path Dot-separated path (e.g., "mysql.port")
   * @return Help information, or nullopt if path not found
   */
  std::optional<ConfigHelpInfo> GetHelp(const std::string& path) const;

  /**
   * @brief List all available paths at a given level
   *
   * @param parent_path Parent path (empty for root)
   * @return Map of child names to descriptions
   */
  std::map<std::string, std::string> ListPaths(const std::string& parent_path = "") const;

  /**
   * @brief Format help as human-readable text
   *
   * @param info Help information
   * @return Formatted help text
   */
  static std::string FormatHelp(const ConfigHelpInfo& info);

  /**
   * @brief Format path listing as human-readable text
   *
   * @param paths Map of paths to descriptions
   * @param parent_path Parent path for context (optional)
   * @return Formatted path list
   */
  static std::string FormatPathList(const std::map<std::string, std::string>& paths,
                                    const std::string& parent_path = "");

 private:
  nlohmann::json schema_;  // Parsed schema

  /**
   * @brief Find schema node for a given path
   *
   * @param path Dot-separated path
   * @return Schema node, or nullopt if not found
   */
  std::optional<nlohmann::json> FindSchemaNode(const std::string& path) const;

  /**
   * @brief Extract help information from a schema node
   *
   * @param path Configuration path
   * @param node Schema node
   * @return Help information
   */
  static ConfigHelpInfo ExtractHelpInfo(const std::string& path, const nlohmann::json& node);

  /**
   * @brief Split path into components
   *
   * @param path Dot-separated path
   * @return Vector of path components
   */
  static std::vector<std::string> SplitPath(const std::string& path);
};

/**
 * @brief Check if a field path contains sensitive information
 *
 * @param path Configuration path (e.g., "mysql.password")
 * @return True if the field is sensitive (should be masked)
 */
bool IsSensitiveField(const std::string& path);

/**
 * @brief Mask sensitive value for display
 *
 * @param path Configuration path
 * @param value Original value
 * @return Masked value (e.g., "***") if sensitive, otherwise original value
 */
std::string MaskSensitiveValue(const std::string& path, const std::string& value);

/**
 * @brief Format current config for display (mask sensitive fields)
 *
 * @param config Configuration object
 * @param path Optional path to show only specific section
 * @return Formatted YAML string with sensitive fields masked
 * @throws std::runtime_error if path is invalid
 */
std::string FormatConfigForDisplay(const Config& config, const std::string& path = "");

}  // namespace mygramdb::config
