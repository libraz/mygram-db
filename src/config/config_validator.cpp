/**
 * @file config_validator.cpp
 * @brief Configuration validation (JSON Schema, path traversal, bind address)
 */

#include <cctype>
#ifndef _WIN32
#include <arpa/inet.h>
#endif
#include <charconv>
#include <cmath>
#include <cstdint>
#include <nlohmann/json-schema.hpp>
#include <nlohmann/json.hpp>
#include <sstream>
#include <string>
#include <string_view>

#include "config/config.h"
#include "config/config_internal.h"
#include "config_schema_embedded.h"  // Auto-generated embedded schema
#include "utils/constants.h"
#include "utils/memory_utils.h"
#include "utils/numeric_parse.h"
#include "utils/string_utils.h"
#include "utils/structured_log.h"

namespace mygramdb::config {

namespace internal {
namespace {

using json = nlohmann::json;

const json& BuiltInSchema() {
  static const json schema = json::parse(kConfigSchemaJson);
  return schema;
}

const json* FindScalarSchema(std::string_view dotted_key) {
  const json* current = &BuiltInSchema();
  size_t start = 0;
  while (start < dotted_key.size()) {
    const size_t end = dotted_key.find('.', start);
    const auto component = dotted_key.substr(start, end == std::string_view::npos ? end : end - start);
    if (!current->is_object() || !current->contains("properties")) {
      return nullptr;
    }
    const auto& properties = (*current)["properties"];
    if (!properties.is_object() || !properties.contains(std::string(component))) {
      return nullptr;
    }
    current = &properties[std::string(component)];
    if (end == std::string_view::npos) {
      return current;
    }
    start = end + 1;
  }
  return nullptr;
}

bool IsValidIpLiteral(const std::string& address) {
#ifndef _WIN32
  in_addr ipv4_addr{};
  if (inet_pton(AF_INET, address.c_str(), &ipv4_addr) == 1) {
    return true;
  }

  in6_addr ipv6_addr{};
  if (inet_pton(AF_INET6, address.c_str(), &ipv6_addr) == 1) {
    return true;
  }

  const size_t scope_pos = address.find('%');
  if (scope_pos != std::string::npos && scope_pos > 0 && scope_pos + 1 < address.size()) {
    const std::string ipv6_part = address.substr(0, scope_pos);
    const std::string scope_part = address.substr(scope_pos + 1);
    for (char character : scope_part) {
      const auto chr = static_cast<unsigned char>(character);
      if (std::isalnum(chr) == 0 && character != '_' && character != '-' && character != '.') {
        return false;
      }
    }
    return inet_pton(AF_INET6, ipv6_part.c_str(), &ipv6_addr) == 1;
  }
#endif
  return false;
}

bool IsValidHostname(const std::string& address) {
  if (address.empty() || address.size() > 253 || address.front() == '.' || address.back() == '.') {
    return false;
  }

  size_t label_start = 0;
  while (label_start < address.size()) {
    size_t label_end = address.find('.', label_start);
    if (label_end == std::string::npos) {
      label_end = address.size();
    }
    const size_t label_len = label_end - label_start;
    if (label_len == 0 || label_len > 63) {
      return false;
    }
    for (size_t i = label_start; i < label_end; ++i) {
      const char character = address[i];
      const auto chr = static_cast<unsigned char>(character);
      const bool edge = i == label_start || i + 1 == label_end;
      if (std::isalnum(chr) != 0) {
        continue;
      }
      if (character == '-' && !edge) {
        continue;
      }
      return false;
    }
    label_start = label_end + 1;
  }
  return true;
}

}  // namespace

mygram::utils::Expected<json, mygram::utils::Error> ValidateConfigValue(std::string_view dotted_key,
                                                                        std::string_view raw_value) {
  using mygram::utils::ErrorCode;
  using mygram::utils::MakeError;
  using mygram::utils::MakeUnexpected;

  const auto invalid_value = [dotted_key](const std::string& reason) {
    return MakeUnexpected(
        MakeError(ErrorCode::kConfigInvalidValue, "Invalid value for '" + std::string(dotted_key) + "': " + reason));
  };

  const json* leaf_schema = nullptr;
  try {
    leaf_schema = FindScalarSchema(dotted_key);
  } catch (const std::exception& error) {
    return MakeUnexpected(MakeError(ErrorCode::kConfigSchemaError,
                                    "Failed to load built-in configuration schema: " + std::string(error.what())));
  }
  if (leaf_schema == nullptr || !leaf_schema->is_object() || !leaf_schema->contains("type") ||
      !(*leaf_schema)["type"].is_string()) {
    return MakeUnexpected(MakeError(ErrorCode::kConfigSchemaError,
                                    "No scalar schema leaf for runtime variable '" + std::string(dotted_key) + "'"));
  }

  const std::string type = (*leaf_schema)["type"].get<std::string>();
  json parsed_value;
  if (type == "boolean") {
    if (raw_value == "true" || raw_value == "1" || raw_value == "yes" || raw_value == "on") {
      parsed_value = true;
    } else if (raw_value == "false" || raw_value == "0" || raw_value == "no" || raw_value == "off") {
      parsed_value = false;
    } else {
      return invalid_value("Invalid boolean value: " + std::string(raw_value));
    }
  } else if (type == "integer") {
    const auto parsed = mygram::utils::ParseNumeric<int64_t>(raw_value);
    if (!parsed.has_value()) {
      return invalid_value("Invalid integer value: " + std::string(raw_value));
    }
    parsed_value = *parsed;
  } else if (type == "number") {
    const auto parsed = mygram::utils::ParseNumeric<double>(raw_value);
    if (!parsed.has_value()) {
      // ParseNumeric rejects non-finite values. Distinguish those from malformed
      // or out-of-range input to preserve the configuration error message.
      double non_finite = 0.0;
      const auto [end, error] = std::from_chars(raw_value.data(), raw_value.data() + raw_value.size(), non_finite,
                                                std::chars_format::general);
      if (error == std::errc{} && end == raw_value.data() + raw_value.size() && !std::isfinite(non_finite)) {
        return invalid_value("Double value must be finite: " + std::string(raw_value));
      }
      return invalid_value("Invalid double value: " + std::string(raw_value));
    }
    parsed_value = *parsed;
  } else if (type == "string") {
    parsed_value = std::string(raw_value);
  } else {
    return MakeUnexpected(MakeError(
        ErrorCode::kConfigSchemaError,
        "Unsupported scalar schema type '" + type + "' for runtime variable '" + std::string(dotted_key) + "'"));
  }

  try {
    nlohmann::json_schema::json_validator validator;
    validator.set_root_schema(*leaf_schema);
    validator.validate(parsed_value);
  } catch (const std::exception& error) {
    return invalid_value(error.what());
  }
  return parsed_value;
}

mygram::utils::Expected<void, mygram::utils::Error> ValidateFinalConfigState(
    const Config& config, std::optional<std::uint64_t> physical_memory_bytes, std::string* offending_key) {
  using mygram::utils::ErrorCode;
  using mygram::utils::MakeError;
  using mygram::utils::MakeUnexpected;

  if (config.replication.enable && config.replication.server_id == 0) {
    if (offending_key != nullptr) {
      *offending_key = "replication.server_id";
    }
    return MakeUnexpected(MakeError(
        ErrorCode::kConfigValidationError,
        "Replication configuration error: server_id must be set to a non-zero value when replication is enabled"));
  }

  if (!config.cache.enabled || config.cache.max_memory_bytes == 0 || !physical_memory_bytes.has_value()) {
    return {};
  }

  const std::uint64_t max_allowed_cache = *physical_memory_bytes / 2;
  if (config.cache.max_memory_bytes <= max_allowed_cache) {
    return {};
  }

  if (offending_key != nullptr) {
    *offending_key = "cache.enabled";
  }
  constexpr std::uint64_t kBytesPerMegabyte = mygram::constants::kBytesPerMegabyte;
  std::stringstream error_message;
  error_message << "Cache configuration error: max_memory_mb exceeds safe limit\n";
  error_message << "  Configured cache size: " << mygram::utils::FormatBytes(config.cache.max_memory_bytes) << "\n";
  error_message << "  Physical memory: " << mygram::utils::FormatBytes(*physical_memory_bytes) << "\n";
  error_message << "  Maximum allowed (50% of physical memory): " << mygram::utils::FormatBytes(max_allowed_cache)
                << "\n";
  error_message << "  Recommendation:\n";
  error_message << "    - Set cache.max_memory_mb to at most " << (max_allowed_cache / kBytesPerMegabyte) << " MB\n";
  error_message << "    - Consider system memory requirements for index and operations\n";
  error_message << "  Example:\n";
  error_message << "    cache:\n";
  error_message << "      max_memory_mb: " << (max_allowed_cache / kBytesPerMegabyte);
  return MakeUnexpected(MakeError(ErrorCode::kConfigInvalidValue, error_message.str()));
}

mygram::utils::Expected<void, mygram::utils::Error> ValidatePathNoTraversal(const std::string& path,
                                                                            const std::string& field_name) {
  using mygram::utils::ErrorCode;
  using mygram::utils::MakeError;
  using mygram::utils::MakeUnexpected;

  if (path.empty()) {
    return {};  // Empty paths are allowed (will be validated elsewhere if required)
  }

  // Check for ".." as a path component (not just substring)
  if (path == ".." || path.find("/../") != std::string::npos || path.find("../") == 0 ||
      (path.size() >= 3 && path.substr(path.size() - 3) == "/..")) {
    return MakeUnexpected(
        MakeError(ErrorCode::kConfigInvalidValue,
                  "Path traversal detected in '" + field_name +
                      "': path contains '..' component which is not allowed for security reasons. "
                      "Use absolute paths or paths relative to the working directory without parent references."));
  }

  // Check for null bytes (could be used to truncate paths)
  if (path.find('\0') != std::string::npos) {
    return MakeUnexpected(
        MakeError(ErrorCode::kConfigInvalidValue, "Invalid path in '" + field_name + "': path contains null bytes."));
  }

  return {};
}

mygram::utils::Expected<void, mygram::utils::Error> ValidateBindAddress(const std::string& address,
                                                                        const std::string& field_name) {
  using mygram::utils::ErrorCode;
  using mygram::utils::MakeError;
  using mygram::utils::MakeUnexpected;

  if (address.empty()) {
    return {};  // Empty addresses use defaults
  }

  // Check for null bytes
  if (address.find('\0') != std::string::npos) {
    return MakeUnexpected(MakeError(ErrorCode::kConfigInvalidValue,
                                    "Invalid bind address in '" + field_name + "': address contains null bytes."));
  }

  // Check for path traversal patterns
  if (address.find("..") != std::string::npos) {
    return MakeUnexpected(MakeError(ErrorCode::kConfigInvalidValue,
                                    "Invalid bind address in '" + field_name +
                                        "': address contains '..' which is not allowed. "
                                        "Use a valid IP address (e.g., 127.0.0.1, 0.0.0.0, ::1) or hostname."));
  }

  // Check for path separators (bind addresses should not contain slashes)
  if (address.find('/') != std::string::npos) {
    return MakeUnexpected(MakeError(ErrorCode::kConfigInvalidValue,
                                    "Invalid bind address in '" + field_name +
                                        "': address contains '/' which is not allowed. "
                                        "Use a valid IP address (e.g., 127.0.0.1, 0.0.0.0, ::1) or hostname."));
  }

  // Check for whitespace
  for (char character : address) {
    if (std::isspace(static_cast<unsigned char>(character)) != 0) {
      return MakeUnexpected(MakeError(ErrorCode::kConfigInvalidValue,
                                      "Invalid bind address in '" + field_name +
                                          "': address contains whitespace. "
                                          "Use a valid IP address (e.g., 127.0.0.1, 0.0.0.0, ::1) or hostname."));
    }
  }

  if (!IsValidIpLiteral(address) && !IsValidHostname(address)) {
    return MakeUnexpected(MakeError(ErrorCode::kConfigInvalidValue,
                                    "Invalid bind address in '" + field_name +
                                        "': address is not a valid IP address or hostname. "
                                        "Use a valid IP address (e.g., 127.0.0.1, 0.0.0.0, ::1) or hostname."));
  }

  return {};
}

}  // namespace internal

mygram::utils::Expected<void, mygram::utils::Error> ValidateConfigJson(const std::string& config_json_str,
                                                                       const std::string& schema_json_str) {
  using json = nlohmann::json;
  using mygram::utils::Error;
  using mygram::utils::ErrorCode;
  using mygram::utils::MakeError;
  using mygram::utils::MakeUnexpected;
  using nlohmann::json_schema::json_validator;

  try {
    json config_json = json::parse(config_json_str);

    auto validate_against = [&](const json& schema,
                                const std::string& schema_name) -> mygram::utils::Expected<void, Error> {
      json_validator validator;
      validator.set_root_schema(schema);

      try {
        validator.validate(config_json);
      } catch (const std::exception& e) {
        std::stringstream err_msg;
        err_msg << "Configuration validation failed (" << schema_name << "):\n";
        err_msg << "  " << e.what() << "\n\n";
        err_msg << "  Common configuration issues:\n";
        err_msg << "    - Missing required schema fields (such as tables); startup also requires effective mysql.user "
                   "and mysql.database values\n";
        err_msg << "    - Invalid data types (string instead of number, etc.)\n";
        err_msg << "    - Invalid enum values (check allowed values)\n";
        err_msg << "    - Table configuration missing 'name' or 'text_source'\n";
        err_msg << "    - Invalid filter operators or types\n\n";
        err_msg << "  Please check your configuration against the schema.\n";
        err_msg << "  Example config: examples/config.yaml";
        return MakeUnexpected(MakeError(ErrorCode::kConfigValidationError, err_msg.str()));
      }
      return {};
    };

    // A custom schema adds deployment-specific constraints; it must never
    // replace the built-in safety contract and relax ranges, enums, required
    // fields, or additionalProperties checks understood by the executable.
    auto built_in_result = validate_against(internal::BuiltInSchema(), "built-in schema");
    if (!built_in_result) {
      return MakeUnexpected(built_in_result.error());
    }
    if (!schema_json_str.empty()) {
      auto custom_result = validate_against(json::parse(schema_json_str), "custom schema");
      if (!custom_result) {
        return MakeUnexpected(custom_result.error());
      }
    }
    mygram::utils::StructuredLog().Event("config_validation_passed").Debug();
  } catch (const json::parse_error& e) {
    return MakeUnexpected(MakeError(ErrorCode::kConfigParseError, std::string("JSON parse error: ") + e.what()));
  }

  return {};
}

}  // namespace mygramdb::config
