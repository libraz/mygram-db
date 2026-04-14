/**
 * @file config_validator.cpp
 * @brief Configuration validation (JSON Schema, path traversal, bind address)
 */

#include <cctype>
#include <nlohmann/json-schema.hpp>
#include <nlohmann/json.hpp>
#include <sstream>
#include <string>

#include "config/config.h"
#include "config/config_internal.h"
#include "config_schema_embedded.h"  // Auto-generated embedded schema
#include "utils/structured_log.h"

namespace mygramdb::config {

namespace internal {

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

    // Use embedded schema if no custom schema provided
    std::string schema_to_use = schema_json_str.empty() ? std::string(kConfigSchemaJson) : schema_json_str;

    json schema_json = json::parse(schema_to_use);

    json_validator validator;
    validator.set_root_schema(schema_json);

    try {
      validator.validate(config_json);
      mygram::utils::StructuredLog().Event("config_validation_passed").Debug();
    } catch (const std::exception& e) {
      std::stringstream err_msg;
      err_msg << "Configuration validation failed:\n";
      err_msg << "  " << e.what() << "\n\n";
      err_msg << "  Common configuration issues:\n";
      err_msg << "    - Missing required fields (mysql.host, mysql.user, tables, etc.)\n";
      err_msg << "    - Invalid data types (string instead of number, etc.)\n";
      err_msg << "    - Invalid enum values (check allowed values)\n";
      err_msg << "    - Table configuration missing 'name' or 'text_source'\n";
      err_msg << "    - Invalid filter operators or types\n\n";
      err_msg << "  Please check your configuration against the schema.\n";
      err_msg << "  Example config: examples/config.yaml";
      return MakeUnexpected(MakeError(ErrorCode::kConfigValidationError, err_msg.str()));
    }
  } catch (const json::parse_error& e) {
    return MakeUnexpected(MakeError(ErrorCode::kConfigParseError, std::string("JSON parse error: ") + e.what()));
  }

  return {};
}

}  // namespace mygramdb::config
