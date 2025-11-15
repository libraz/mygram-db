/**
 * @file config_help.cpp
 * @brief Configuration help system implementation
 */

#include "config/config_help.h"

#include <algorithm>
#include <cctype>
#include <sstream>
#include <stdexcept>

#include "config/config_schema_embedded.h"

namespace mygramdb::config {

namespace {

/**
 * @brief Convert string to lowercase for case-insensitive comparison
 *
 * @param str Input string
 * @return Lowercase string
 */
std::string ToLower(const std::string& str) {
  std::string result = str;
  std::transform(result.begin(), result.end(), result.begin(),
                 [](unsigned char ch_value) { return std::tolower(ch_value); });
  return result;
}

/**
 * @brief Convert JSON value to string representation
 *
 * @param value JSON value
 * @return String representation
 */
std::string JsonValueToString(const nlohmann::json& value) {
  if (value.is_string()) {
    return "\"" + value.get<std::string>() + "\"";
  }
  if (value.is_boolean()) {
    return value.get<bool>() ? "true" : "false";
  }
  if (value.is_number()) {
    return value.dump();
  }
  if (value.is_null()) {
    return "null";
  }
  return value.dump();
}

/**
 * @brief Convert Config struct to JSON for display
 *
 * @param config Configuration object
 * @return JSON representation
 */
nlohmann::json ConfigToJson(const Config& config) {
  nlohmann::json json;

  // MySQL configuration
  json["mysql"] = {
      {"host", config.mysql.host},
      {"port", config.mysql.port},
      {"user", config.mysql.user},
      {"password", config.mysql.password},
      {"database", config.mysql.database},
      {"use_gtid", config.mysql.use_gtid},
      {"binlog_format", config.mysql.binlog_format},
      {"binlog_row_image", config.mysql.binlog_row_image},
      {"connect_timeout_ms", config.mysql.connect_timeout_ms},
      {"read_timeout_ms", config.mysql.read_timeout_ms},
      {"write_timeout_ms", config.mysql.write_timeout_ms},
  };

  // Tables configuration
  json["tables"] = nlohmann::json::array();
  for (const auto& table : config.tables) {
    nlohmann::json table_json = {
        {"name", table.name},
        {"primary_key", table.primary_key},
        {"ngram_size", table.ngram_size},
        {"kanji_ngram_size", table.kanji_ngram_size},
    };

    // Text source
    if (!table.text_source.column.empty()) {
      table_json["text_source"]["column"] = table.text_source.column;
    }
    if (!table.text_source.concat.empty()) {
      table_json["text_source"]["concat"] = table.text_source.concat;
      table_json["text_source"]["delimiter"] = table.text_source.delimiter;
    }

    // Required filters
    if (!table.required_filters.empty()) {
      table_json["required_filters"] = nlohmann::json::array();
      for (const auto& filter : table.required_filters) {
        table_json["required_filters"].push_back({
            {"name", filter.name},
            {"type", filter.type},
            {"op", filter.op},
            {"value", filter.value},
            {"bitmap_index", filter.bitmap_index},
        });
      }
    }

    // Filters
    if (!table.filters.empty()) {
      table_json["filters"] = nlohmann::json::array();
      for (const auto& filter : table.filters) {
        nlohmann::json filter_json = {
            {"name", filter.name},
            {"type", filter.type},
            {"dict_compress", filter.dict_compress},
            {"bitmap_index", filter.bitmap_index},
        };
        if (!filter.bucket.empty()) {
          filter_json["bucket"] = filter.bucket;
        }
        table_json["filters"].push_back(filter_json);
      }
    }

    // Posting configuration
    table_json["posting"] = {
        {"block_size", table.posting.block_size},
        {"freq_bits", table.posting.freq_bits},
        {"use_roaring", table.posting.use_roaring},
    };

    json["tables"].push_back(table_json);
  }

  // Build configuration
  json["build"] = {
      {"mode", config.build.mode},
      {"batch_size", config.build.batch_size},
      {"parallelism", config.build.parallelism},
      {"throttle_ms", config.build.throttle_ms},
  };

  // Replication configuration
  json["replication"] = {
      {"enable", config.replication.enable},
      {"auto_initial_snapshot", config.replication.auto_initial_snapshot},
      {"server_id", config.replication.server_id},
      {"start_from", config.replication.start_from},
      {"queue_size", config.replication.queue_size},
      {"reconnect_backoff_min_ms", config.replication.reconnect_backoff_min_ms},
      {"reconnect_backoff_max_ms", config.replication.reconnect_backoff_max_ms},
  };

  // Memory configuration
  json["memory"] = {
      {"hard_limit_mb", config.memory.hard_limit_mb},
      {"soft_target_mb", config.memory.soft_target_mb},
      {"arena_chunk_mb", config.memory.arena_chunk_mb},
      {"roaring_threshold", config.memory.roaring_threshold},
      {"minute_epoch", config.memory.minute_epoch},
      {"normalize",
       {
           {"nfkc", config.memory.normalize.nfkc},
           {"width", config.memory.normalize.width},
           {"lower", config.memory.normalize.lower},
       }},
  };

  // Dump configuration
  json["dump"] = {
      {"dir", config.dump.dir},
      {"default_filename", config.dump.default_filename},
      {"interval_sec", config.dump.interval_sec},
      {"retain", config.dump.retain},
  };

  // API configuration
  json["api"] = {
      {"tcp",
       {
           {"bind", config.api.tcp.bind},
           {"port", config.api.tcp.port},
       }},
      {"http",
       {
           {"enable", config.api.http.enable},
           {"bind", config.api.http.bind},
           {"port", config.api.http.port},
           {"enable_cors", config.api.http.enable_cors},
           {"cors_allow_origin", config.api.http.cors_allow_origin},
       }},
      {"default_limit", config.api.default_limit},
      {"max_query_length", config.api.max_query_length},
  };

  // Network configuration
  if (!config.network.allow_cidrs.empty()) {
    json["network"]["allow_cidrs"] = config.network.allow_cidrs;
  }

  // Logging configuration
  json["logging"] = {
      {"level", config.logging.level},
      {"json", config.logging.json},
  };

  // Cache configuration
  json["cache"] = {
      {"enabled", config.cache.enabled},
      {"max_memory_mb", config.cache.max_memory_mb},
      {"min_query_cost_ms", config.cache.min_query_cost_ms},
      {"ttl_seconds", config.cache.ttl_seconds},
      {"invalidation_strategy", config.cache.invalidation_strategy},
      {"compression_enabled", config.cache.compression_enabled},
      {"eviction_batch_size", config.cache.eviction_batch_size},
      {"invalidation",
       {
           {"batch_size", config.cache.invalidation.batch_size},
           {"max_delay_ms", config.cache.invalidation.max_delay_ms},
       }},
  };

  return json;
}

/**
 * @brief Split path into components (local helper)
 *
 * @param path Dot-separated path
 * @return Vector of path components
 */
std::vector<std::string> SplitPathHelper(const std::string& path) {
  std::vector<std::string> result;
  if (path.empty()) {
    return result;
  }

  std::istringstream stream(path);
  std::string part;
  while (std::getline(stream, part, '.')) {
    if (!part.empty()) {
      result.push_back(part);
    }
  }

  return result;
}

/**
 * @brief Navigate JSON object by dot-separated path
 *
 * @param json JSON object
 * @param path Dot-separated path (e.g., "mysql.port")
 * @return JSON value at path, or nullopt if not found
 */
std::optional<nlohmann::json> NavigateJsonPath(const nlohmann::json& json, const std::string& path) {
  if (path.empty()) {
    return json;
  }

  std::vector<std::string> parts = SplitPathHelper(path);
  nlohmann::json current = json;

  for (const auto& part : parts) {
    if (current.is_object() && current.contains(part)) {
      current = current[part];
    } else if (current.is_array() && !current.empty()) {
      // For array paths without index, return first element
      current = current[0];
      if (current.is_object() && current.contains(part)) {
        current = current[part];
      } else {
        return std::nullopt;
      }
    } else {
      return std::nullopt;
    }
  }

  return current;
}

/**
 * @brief Mask sensitive fields recursively in JSON
 *
 * @param json JSON object (modified in place)
 * @param path Current path (for field detection)
 */
void MaskSensitiveFieldsRecursive(nlohmann::json& json, const std::string& path) {
  if (json.is_object()) {
    for (const auto& [key, child] : json.items()) {
      std::string child_path;
      if (path.empty()) {
        child_path = key;
      } else {
        child_path.reserve(path.size() + 1 + key.size());
        child_path.assign(path);
        child_path.push_back('.');
        child_path.append(key);
      }
      if (IsSensitiveField(child_path)) {
        json[key] = "***";
      } else if (child.is_object() || child.is_array()) {
        MaskSensitiveFieldsRecursive(json[key], child_path);
      }
    }
  } else if (json.is_array()) {
    for (auto& child : json) {
      MaskSensitiveFieldsRecursive(child, path);
    }
  }
}

/**
 * @brief Convert JSON to YAML-like string format
 *
 * @param json JSON object
 * @param indent Current indentation level
 * @return YAML-formatted string
 */
std::string JsonToYaml(const nlohmann::json& json, int indent = 0) {
  std::ostringstream oss;
  std::string indent_str(indent * 2, ' ');

  if (json.is_object()) {
    for (const auto& [key, child] : json.items()) {
      oss << indent_str << key << ":";
      if (child.is_object() || child.is_array()) {
        oss << "\n" << JsonToYaml(child, indent + 1);
      } else {
        oss << " " << JsonValueToString(child) << "\n";
      }
    }
  } else if (json.is_array()) {
    for (const auto& item : json) {
      oss << indent_str << "-";
      if (item.is_object()) {
        // First property on same line, rest indented
        bool first = true;
        for (const auto& [key, value] : item.items()) {
          if (first) {
            oss << " " << key << ":";
            if (value.is_object() || value.is_array()) {
              oss << "\n" << JsonToYaml(value, indent + 2);
            } else {
              oss << " " << JsonValueToString(value) << "\n";
            }
            first = false;
          } else {
            oss << std::string((indent + 1) * 2, ' ') << key << ":";
            if (value.is_object() || value.is_array()) {
              oss << "\n" << JsonToYaml(value, indent + 2);
            } else {
              oss << " " << JsonValueToString(value) << "\n";
            }
          }
        }
      } else {
        oss << " " << JsonValueToString(item) << "\n";
      }
    }
  } else {
    oss << indent_str << JsonValueToString(json) << "\n";
  }

  return oss.str();
}

}  // namespace

// ConfigSchemaExplorer implementation

ConfigSchemaExplorer::ConfigSchemaExplorer() {
  try {
    schema_ = nlohmann::json::parse(kConfigSchemaJson);
  } catch (const nlohmann::json::exception& e) {
    throw std::runtime_error("Failed to parse embedded JSON Schema: " + std::string(e.what()));
  }
}

std::optional<ConfigHelpInfo> ConfigSchemaExplorer::GetHelp(const std::string& path) const {
  auto node = FindSchemaNode(path);
  if (!node.has_value()) {
    return std::nullopt;
  }

  return ExtractHelpInfo(path, node.value());
}

std::map<std::string, std::string> ConfigSchemaExplorer::ListPaths(const std::string& parent_path) const {
  std::map<std::string, std::string> result;

  auto node = parent_path.empty() ? std::make_optional(schema_) : FindSchemaNode(parent_path);
  if (!node.has_value()) {
    return result;
  }

  auto current = node.value();

  // Handle array type - navigate to items schema
  if (current.contains("type") && current["type"] == "array" && current.contains("items")) {
    current = current["items"];
  }

  // List properties
  if (current.contains("properties")) {
    const auto& properties = current["properties"];
    for (const auto& [key, property] : properties.items()) {
      std::string description;
      if (property.contains("description")) {
        description = property["description"].get<std::string>();
      }
      result[key] = description;
    }
  }

  return result;
}

std::string ConfigSchemaExplorer::FormatHelp(const ConfigHelpInfo& info) {
  std::ostringstream oss;

  oss << info.path << "\n\n";

  // Type information
  oss << "Type: " << info.type;
  if (!info.allowed_values.empty()) {
    oss << " (enum)";
  }
  oss << "\n";

  // Default value
  if (info.default_value.has_value()) {
    oss << "Default: " << info.default_value.value() << "\n";
  }

  // Range for numbers
  if (info.minimum.has_value() || info.maximum.has_value()) {
    oss << "Range: ";
    if (info.minimum.has_value()) {
      oss << info.minimum.value();
    } else {
      oss << "-∞";
    }
    oss << " - ";
    if (info.maximum.has_value()) {
      oss << info.maximum.value();
    } else {
      oss << "+∞";
    }
    oss << "\n";
  } else if (info.minimum_number.has_value() || info.maximum_number.has_value()) {
    oss << "Range: ";
    if (info.minimum_number.has_value()) {
      oss << info.minimum_number.value();
    } else {
      oss << "-∞";
    }
    oss << " - ";
    if (info.maximum_number.has_value()) {
      oss << info.maximum_number.value();
    } else {
      oss << "+∞";
    }
    oss << "\n";
  }

  // Allowed values for enums
  if (!info.allowed_values.empty()) {
    oss << "Allowed values:\n";
    for (const auto& value : info.allowed_values) {
      oss << "  - " << value << "\n";
    }
  }

  // Required flag
  if (info.required) {
    oss << "Required: yes\n";
  }

  // Description
  if (!info.description.empty()) {
    oss << "Description: " << info.description << "\n";
  }

  return oss.str();
}

std::string ConfigSchemaExplorer::FormatPathList(const std::map<std::string, std::string>& paths,
                                                 const std::string& parent_path) {
  std::ostringstream oss;

  if (parent_path.empty()) {
    oss << "Available configuration sections:\n";
  } else {
    oss << "Available paths under '" << parent_path << "':\n";
  }

  // Find maximum key length for alignment
  size_t max_key_length = 0;
  for (const auto& [key, _] : paths) {
    max_key_length = std::max(max_key_length, key.length());
  }

  for (const auto& [key, description] : paths) {
    oss << "  " << key;
    if (!description.empty()) {
      // Pad for alignment
      for (size_t i = key.length(); i < max_key_length + 2; ++i) {
        oss << " ";
      }
      oss << "- " << description;
    }
    oss << "\n";
  }

  if (!parent_path.empty()) {
    oss << "\nUse \"CONFIG HELP " << parent_path << ".<path>\" for detailed information.\n";
  } else {
    oss << "\nUse \"CONFIG HELP <section>\" for detailed information.\n";
  }

  return oss.str();
}

std::optional<nlohmann::json> ConfigSchemaExplorer::FindSchemaNode(const std::string& path) const {
  if (path.empty()) {
    return schema_;
  }

  auto parts = SplitPath(path);
  nlohmann::json current = schema_;

  for (const auto& part : parts) {
    // Handle array type - navigate to items schema
    if (current.contains("type") && current["type"] == "array" && current.contains("items")) {
      current = current["items"];
    }

    // Navigate to property
    if (current.contains("properties") && current["properties"].contains(part)) {
      current = current["properties"][part];
    } else {
      return std::nullopt;  // Path not found
    }
  }

  return current;
}

ConfigHelpInfo ConfigSchemaExplorer::ExtractHelpInfo(const std::string& path, const nlohmann::json& node) {
  ConfigHelpInfo info;
  info.path = path;

  // Extract type
  if (node.contains("type")) {
    if (node["type"].is_string()) {
      info.type = node["type"].get<std::string>();
    } else if (node["type"].is_array()) {
      // Handle union types (e.g., ["string", "null"])
      std::ostringstream oss;
      bool first = true;
      for (const auto& type : node["type"]) {
        if (!first) {
          oss << " | ";
        }
        oss << type.get<std::string>();
        first = false;
      }
      info.type = oss.str();
    }
  }

  // Extract description
  if (node.contains("description")) {
    info.description = node["description"].get<std::string>();
  }

  // Extract default value
  if (node.contains("default")) {
    info.default_value = JsonValueToString(node["default"]);
  }

  // Extract enum values
  if (node.contains("enum")) {
    for (const auto& value : node["enum"]) {
      info.allowed_values.push_back(JsonValueToString(value));
    }
  }

  // Extract numeric constraints
  if (node.contains("minimum")) {
    if (node["minimum"].is_number_integer()) {
      info.minimum = node["minimum"].get<int64_t>();
    } else if (node["minimum"].is_number_float()) {
      info.minimum_number = node["minimum"].get<double>();
    }
  }

  if (node.contains("maximum")) {
    if (node["maximum"].is_number_integer()) {
      info.maximum = node["maximum"].get<int64_t>();
    } else if (node["maximum"].is_number_float()) {
      info.maximum_number = node["maximum"].get<double>();
    }
  }

  // Note: required flag detection requires parent node analysis
  // This is complex and may need to be handled by the caller

  return info;
}

std::vector<std::string> ConfigSchemaExplorer::SplitPath(const std::string& path) {
  std::vector<std::string> result;
  if (path.empty()) {
    return result;
  }

  std::istringstream stream(path);
  std::string part;
  while (std::getline(stream, part, '.')) {
    if (!part.empty()) {
      result.push_back(part);
    }
  }

  return result;
}

// Standalone functions

bool IsSensitiveField(const std::string& path) {
  std::string lower_path = ToLower(path);
  return lower_path.find("password") != std::string::npos || lower_path.find("secret") != std::string::npos ||
         lower_path.find("key") != std::string::npos || lower_path.find("token") != std::string::npos;
}

std::string MaskSensitiveValue(const std::string& path, const std::string& value) {
  if (IsSensitiveField(path) && !value.empty()) {
    return "***";
  }
  return value;
}

std::string FormatConfigForDisplay(const Config& config, const std::string& path) {
  // Convert config struct to JSON
  nlohmann::json config_json = ConfigToJson(config);

  // Navigate to specified path if provided
  if (!path.empty()) {
    auto node = NavigateJsonPath(config_json, path);
    if (!node.has_value()) {
      throw std::runtime_error("Path not found: " + path);
    }
    config_json = node.value();
  }

  // Mask sensitive fields
  MaskSensitiveFieldsRecursive(config_json, path);

  // Convert to YAML format
  return JsonToYaml(config_json);
}

}  // namespace mygramdb::config
