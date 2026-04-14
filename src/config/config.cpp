/**
 * @file config.cpp
 * @brief Configuration parser implementation with JSON Schema validation
 */

#include "config/config.h"

#include <spdlog/spdlog.h>
#include <yaml-cpp/yaml.h>

#include <nlohmann/json.hpp>
#include <set>
#include <sstream>

#include "config/config_internal.h"
#include "utils/datetime_converter.h"
#include "utils/memory_utils.h"
#include "utils/string_utils.h"
#include "utils/structured_log.h"

namespace mygramdb::config {

namespace {

using json = nlohmann::json;

constexpr size_t kGtidPrefixLength = 5;  // "gtid="

/**
 * @brief Get value from environment variable
 *
 * BUG-0091: Enables reading sensitive configuration from environment variables
 * instead of storing them in configuration files.
 *
 * @param env_var_name Environment variable name
 * @return Value if environment variable is set, std::nullopt otherwise
 */
std::optional<std::string> GetEnvValue(const char* env_var_name) {
  // NOLINTNEXTLINE(concurrency-mt-unsafe): getenv is safe when not modifying env
  const char* value = std::getenv(env_var_name);
  // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic) - Required for null-terminator check
  if (value != nullptr && value[0] != '\0') {
    return std::string(value);
  }
  return std::nullopt;
}

/**
 * @brief Get configuration value with environment variable override
 *
 * Priority: Environment variable > JSON value > Default
 *
 * @param json_value Value from JSON config (optional)
 * @param env_var_name Environment variable name to check
 * @param default_value Default value if neither is set
 * @return Final configuration value
 */
std::string GetConfigValueWithEnvOverride(const std::optional<std::string>& json_value, const char* env_var_name,
                                          const std::string& default_value = "") {
  // Environment variable takes precedence
  if (auto env_value = GetEnvValue(env_var_name); env_value.has_value()) {
    return *env_value;
  }
  // Fall back to JSON value
  if (json_value.has_value()) {
    return *json_value;
  }
  // Use default
  return default_value;
}

/**
 * @brief Validate path for directory traversal attacks
 * @param path Path to validate
 * @param field_name Name of the field for error messages
 * @return Expected<void, Error> on success, or error if path contains traversal sequences
 */
mygram::utils::Expected<void, mygram::utils::Error> ValidatePathNoTraversal(const std::string& path,
                                                                            const std::string& field_name) {
  using mygram::utils::ErrorCode;
  using mygram::utils::MakeError;
  using mygram::utils::MakeUnexpected;

  if (path.empty()) {
    return {};  // Empty paths are allowed (will be validated elsewhere if required)
  }

  // Check for path traversal patterns
  if (path.find("..") != std::string::npos) {
    return MakeUnexpected(
        MakeError(ErrorCode::kConfigInvalidValue,
                  "Path traversal detected in '" + field_name +
                      "': path contains '..' which is not allowed for security reasons. "
                      "Use absolute paths or paths relative to the working directory without parent references."));
  }

  // Check for null bytes (could be used to truncate paths)
  if (path.find('\0') != std::string::npos) {
    return MakeUnexpected(
        MakeError(ErrorCode::kConfigInvalidValue, "Invalid path in '" + field_name + "': path contains null bytes."));
  }

  return {};
}

/**
 * @brief Validate bind address format
 *
 * Checks that a bind address is a reasonable IP address or hostname.
 * Rejects path traversal sequences, null bytes, and obviously invalid formats
 * (e.g., paths with slashes, whitespace).
 *
 * @param address Bind address to validate
 * @param field_name Name of the field for error messages
 * @return Expected<void, Error> on success, or error if address is invalid
 */
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

/**
 * @brief Convert YAML node to JSON object recursively
 */
json YamlToJson(const YAML::Node& node) {
  switch (node.Type()) {
    case YAML::NodeType::Null:
      return {};
    case YAML::NodeType::Scalar: {
      try {
        return json::parse(node.as<std::string>());
      } catch (const json::parse_error&) {
        // Not valid JSON, return as plain string
        return node.as<std::string>();
      }
    }
    case YAML::NodeType::Sequence: {
      json result = json::array();
      for (const auto& item : node) {
        result.push_back(YamlToJson(item));
      }
      return result;
    }
    case YAML::NodeType::Map: {
      json result = json::object();
      for (const auto& key_value : node) {
        result[key_value.first.as<std::string>()] = YamlToJson(key_value.second);
      }
      return result;
    }
    default:
      return {};
  }
}

/**
 * @brief Parse MySQL configuration from JSON
 *
 * BUG-0091: Sensitive values (password, user) can be provided via environment variables:
 *   - MYGRAM_MYSQL_PASSWORD: MySQL password (takes precedence over config file)
 *   - MYGRAM_MYSQL_USER: MySQL username (takes precedence over config file)
 *   - MYGRAM_MYSQL_HOST: MySQL host (takes precedence over config file)
 *   - MYGRAM_MYSQL_PORT: MySQL port (takes precedence over config file)
 *   - MYGRAM_MYSQL_DATABASE: MySQL database (takes precedence over config file)
 */
mygram::utils::Expected<MysqlConfig, mygram::utils::Error> ParseMysqlConfig(const json& json_obj) {
  MysqlConfig config;

  // Host: environment variable takes precedence
  {
    std::optional<std::string> json_value;
    if (json_obj.contains("host")) {
      json_value = json_obj["host"].get<std::string>();
    }
    config.host = GetConfigValueWithEnvOverride(json_value, "MYGRAM_MYSQL_HOST", config.host);
  }

  {
    // Port: environment variable takes precedence
    auto env_port = GetEnvValue("MYGRAM_MYSQL_PORT");
    if (env_port.has_value()) {
      try {
        config.port = std::stoi(env_port.value());
      } catch (const std::exception&) {
        // Invalid port in environment variable, fall through to config file value
        mygram::utils::StructuredLog()
            .Event("config_env_override_rejected")
            .Field("variable", "MYGRAM_MYSQL_PORT")
            .Field("value", env_port.value())
            .Field("reason", "invalid integer value")
            .Warn();
      }
    } else if (json_obj.contains("port")) {
      config.port = json_obj["port"].get<int>();
    }
  }

  // User: environment variable takes precedence
  {
    std::optional<std::string> json_value;
    if (json_obj.contains("user")) {
      json_value = json_obj["user"].get<std::string>();
    }
    config.user = GetConfigValueWithEnvOverride(json_value, "MYGRAM_MYSQL_USER", config.user);
  }

  // Password: environment variable takes precedence (security best practice)
  {
    std::optional<std::string> json_value;
    if (json_obj.contains("password")) {
      json_value = json_obj["password"].get<std::string>();
    }
    config.password = GetConfigValueWithEnvOverride(json_value, "MYGRAM_MYSQL_PASSWORD", config.password);
  }

  // Database: environment variable takes precedence
  {
    std::optional<std::string> json_value;
    if (json_obj.contains("database")) {
      json_value = json_obj["database"].get<std::string>();
    }
    config.database = GetConfigValueWithEnvOverride(json_value, "MYGRAM_MYSQL_DATABASE", config.database);
  }
  if (json_obj.contains("use_gtid")) {
    config.use_gtid = json_obj["use_gtid"].get<bool>();
  }
  if (json_obj.contains("binlog_format")) {
    config.binlog_format = json_obj["binlog_format"].get<std::string>();
  }
  if (json_obj.contains("binlog_row_image")) {
    config.binlog_row_image = json_obj["binlog_row_image"].get<std::string>();
  }
  if (json_obj.contains("connect_timeout_ms")) {
    config.connect_timeout_ms = json_obj["connect_timeout_ms"].get<int>();
  }
  if (json_obj.contains("read_timeout_ms")) {
    config.read_timeout_ms = json_obj["read_timeout_ms"].get<int>();
  }
  if (json_obj.contains("write_timeout_ms")) {
    config.write_timeout_ms = json_obj["write_timeout_ms"].get<int>();
  }
  if (json_obj.contains("session_timeout_sec")) {
    config.session_timeout_sec = json_obj["session_timeout_sec"].get<int>();
  }
  if (json_obj.contains("ssl_enable")) {
    config.ssl_enable = json_obj["ssl_enable"].get<bool>();
  }
  if (json_obj.contains("ssl_ca")) {
    config.ssl_ca = json_obj["ssl_ca"].get<std::string>();
    if (auto v = ValidatePathNoTraversal(config.ssl_ca, "mysql.ssl_ca"); !v) {
      return mygram::utils::MakeUnexpected(v.error());
    }
  }
  if (json_obj.contains("ssl_cert")) {
    config.ssl_cert = json_obj["ssl_cert"].get<std::string>();
    if (auto v = ValidatePathNoTraversal(config.ssl_cert, "mysql.ssl_cert"); !v) {
      return mygram::utils::MakeUnexpected(v.error());
    }
  }
  if (json_obj.contains("ssl_key")) {
    config.ssl_key = json_obj["ssl_key"].get<std::string>();
    if (auto v = ValidatePathNoTraversal(config.ssl_key, "mysql.ssl_key"); !v) {
      return mygram::utils::MakeUnexpected(v.error());
    }
  }
  if (json_obj.contains("ssl_verify_server_cert")) {
    config.ssl_verify_server_cert = json_obj["ssl_verify_server_cert"].get<bool>();
  }
  if (json_obj.contains("datetime_timezone")) {
    config.datetime_timezone = json_obj["datetime_timezone"].get<std::string>();
  }

  return config;
}

/**
 * @brief Parse required filter configuration from JSON
 */
mygram::utils::Expected<RequiredFilterConfig, mygram::utils::Error> ParseRequiredFilterConfig(const json& json_obj) {
  using mygram::utils::ErrorCode;
  using mygram::utils::MakeError;
  using mygram::utils::MakeUnexpected;

  RequiredFilterConfig config;

  // Validate required fields: name and type are mandatory
  if (!json_obj.contains("name") || json_obj["name"].get<std::string>().empty()) {
    return MakeUnexpected(MakeError(ErrorCode::kConfigMissingRequired,
                                    "Required filter error: 'name' field is required\n"
                                    "  Example:\n"
                                    "    required_filters:\n"
                                    "      - name: status\n"
                                    "        type: int\n"
                                    "        op: \"=\"\n"
                                    "        value: 1"));
  }
  config.name = json_obj["name"].get<std::string>();

  if (!json_obj.contains("type") || json_obj["type"].get<std::string>().empty()) {
    return MakeUnexpected(MakeError(ErrorCode::kConfigMissingRequired,
                                    "Required filter error: 'type' field is required for filter '" + config.name +
                                        "'\n"
                                        "  Valid types: int, string, datetime, bool, float\n"
                                        "  Example:\n"
                                        "    required_filters:\n"
                                        "      - name: status\n"
                                        "        type: int\n"
                                        "        op: \"=\"\n"
                                        "        value: 1"));
  }
  config.type = json_obj["type"].get<std::string>();

  if (json_obj.contains("op") || json_obj.contains("operator")) {
    // Support both "op" and "operator" as key names
    config.op = json_obj.contains("op") ? json_obj["op"].get<std::string>() : json_obj["operator"].get<std::string>();
  }
  if (json_obj.contains("value")) {
    // value can be string or number, convert to string
    if (json_obj["value"].is_string()) {
      config.value = json_obj["value"].get<std::string>();
    } else if (json_obj["value"].is_number_integer()) {
      // Integer types: format without decimal point
      config.value = std::to_string(json_obj["value"].get<int64_t>());
    } else if (json_obj["value"].is_number_float()) {
      // Floating point types: format with decimal point
      config.value = std::to_string(json_obj["value"].get<double>());
    } else if (json_obj["value"].is_boolean()) {
      config.value = json_obj["value"].get<bool>() ? "1" : "0";
    }
  }
  if (json_obj.contains("bitmap_index")) {
    config.bitmap_index = json_obj["bitmap_index"].get<bool>();
  }

  // Validate operator
  std::vector<std::string> valid_ops = {"=", "!=", "<", ">", "<=", ">=", "IS NULL", "IS NOT NULL"};
  if (std::find(valid_ops.begin(), valid_ops.end(), config.op) == valid_ops.end()) {
    std::stringstream err_msg;
    err_msg << "Invalid operator in required_filters: '" << config.op << "'\n";
    err_msg << "  Valid operators: =, !=, <, >, <=, >=, IS NULL, IS NOT NULL\n";
    err_msg << "  Example:\n";
    err_msg << "    required_filters:\n";
    err_msg << "      - name: status\n";
    err_msg << "        type: int\n";
    err_msg << "        op: \"=\"\n";
    err_msg << "        value: 1";
    return MakeUnexpected(MakeError(ErrorCode::kConfigInvalidValue, err_msg.str()));
  }

  // Validate: IS NULL/IS NOT NULL should not have a value
  if ((config.op == "IS NULL" || config.op == "IS NOT NULL") && !config.value.empty()) {
    std::stringstream err_msg;
    err_msg << "Required filter error: Operator '" << config.op << "' should not have a value\n";
    err_msg << "  For NULL checks, omit the 'value' field.\n";
    err_msg << "  Example:\n";
    err_msg << "    required_filters:\n";
    err_msg << "      - name: deleted_at\n";
    err_msg << "        type: datetime\n";
    err_msg << "        op: \"IS NULL\"";
    return MakeUnexpected(MakeError(ErrorCode::kConfigInvalidValue, err_msg.str()));
  }

  // Validate: Other operators should have a value
  auto is_null_operator = [&config]() -> bool { return config.op == "IS NULL" || config.op == "IS NOT NULL"; };

  if (!is_null_operator() && config.value.empty()) {
    std::stringstream err_msg;
    err_msg << "Required filter error: Operator '" << config.op << "' requires a value\n";
    err_msg << "  Please provide a 'value' field for this operator.\n";
    err_msg << "  Example:\n";
    err_msg << "    required_filters:\n";
    err_msg << "      - name: status\n";
    err_msg << "        type: int\n";
    err_msg << "        op: \"" << config.op << "\"\n";
    err_msg << "        value: 1";
    return MakeUnexpected(MakeError(ErrorCode::kConfigInvalidValue, err_msg.str()));
  }

  return config;
}

/**
 * @brief Parse filter configuration from JSON
 */
FilterConfig ParseFilterConfig(const json& json_obj) {
  FilterConfig config;

  if (json_obj.contains("name")) {
    config.name = json_obj["name"].get<std::string>();
  }
  if (json_obj.contains("type")) {
    config.type = json_obj["type"].get<std::string>();
  }
  if (json_obj.contains("dict_compress")) {
    config.dict_compress = json_obj["dict_compress"].get<bool>();
  }
  if (json_obj.contains("bitmap_index")) {
    config.bitmap_index = json_obj["bitmap_index"].get<bool>();
  }
  if (json_obj.contains("bucket")) {
    config.bucket = json_obj["bucket"].get<std::string>();
  }

  return config;
}

/**
 * @brief Parse table configuration from JSON
 */
mygram::utils::Expected<TableConfig, mygram::utils::Error> ParseTableConfig(const json& json_obj) {
  using mygram::utils::ErrorCode;
  using mygram::utils::MakeError;
  using mygram::utils::MakeUnexpected;

  TableConfig config;

  if (!json_obj.contains("name")) {
    std::stringstream err_msg;
    err_msg << "Table configuration error: Missing required 'name' field\n";
    err_msg << "  Each table configuration must have a 'name' field.\n";
    err_msg << "  Example:\n";
    err_msg << "    tables:\n";
    err_msg << "      - name: my_table\n";
    err_msg << "        primary_key: id\n";
    err_msg << "        text_source:\n";
    err_msg << "          column: content";
    return MakeUnexpected(MakeError(ErrorCode::kConfigMissingRequired, err_msg.str()));
  }
  config.name = json_obj["name"].get<std::string>();

  if (json_obj.contains("primary_key")) {
    config.primary_key = json_obj["primary_key"].get<std::string>();
  }
  if (json_obj.contains("ngram_size")) {
    config.ngram_size = json_obj["ngram_size"].get<int>();
  }
  if (json_obj.contains("kanji_ngram_size")) {
    config.kanji_ngram_size = json_obj["kanji_ngram_size"].get<int>();
    if (config.kanji_ngram_size < 0 || config.kanji_ngram_size > 10) {
      return MakeUnexpected(
          MakeError(ErrorCode::kConfigInvalidValue, "Configuration error in table '" + config.name +
                                                        "': kanji_ngram_size must be between 0 and 10 (got " +
                                                        std::to_string(config.kanji_ngram_size) + ")"));
    }
  }
  // If kanji_ngram_size is 0 or not specified, use ngram_size
  if (config.kanji_ngram_size == 0) {
    config.kanji_ngram_size = config.ngram_size;
  }
  if (json_obj.contains("cross_boundary_ngrams")) {
    config.cross_boundary_ngrams = json_obj["cross_boundary_ngrams"].get<bool>();
  }

  // Check for deprecated where_clause
  if (json_obj.contains("where_clause")) {
    std::stringstream err_msg;
    err_msg << "Configuration error in table '" << config.name << "': 'where_clause' is no longer supported\n";
    err_msg << "  Please use 'required_filters' instead.\n";
    err_msg << "  Migration guide:\n";
    err_msg << "    Old format: where_clause: \"status = 1\"\n";
    err_msg << "    New format:\n";
    err_msg << "      required_filters:\n";
    err_msg << "        - name: status\n";
    err_msg << "          type: int\n";
    err_msg << "          op: \"=\"\n";
    err_msg << "          value: 1";
    return MakeUnexpected(MakeError(ErrorCode::kConfigInvalidValue, err_msg.str()));
  }

  // Parse text_source
  if (json_obj.contains("text_source")) {
    const auto& text_source = json_obj["text_source"];
    if (text_source.contains("column")) {
      config.text_source.column = text_source["column"].get<std::string>();
    }
    if (text_source.contains("concat")) {
      config.text_source.concat = text_source["concat"].get<std::vector<std::string>>();
    }
    if (text_source.contains("delimiter")) {
      config.text_source.delimiter = text_source["delimiter"].get<std::string>();
    }
  }

  // Parse required_filters
  if (json_obj.contains("required_filters")) {
    for (const auto& filter_json : json_obj["required_filters"]) {
      auto filter_result = ParseRequiredFilterConfig(filter_json);
      if (!filter_result) {
        return MakeUnexpected(filter_result.error());
      }
      config.required_filters.push_back(std::move(*filter_result));
    }
  }

  // Parse filters (optional filters for search-time filtering)
  if (json_obj.contains("filters")) {
    for (const auto& filter_json : json_obj["filters"]) {
      config.filters.push_back(ParseFilterConfig(filter_json));
    }
  }

  // Parse posting config
  if (json_obj.contains("posting")) {
    const auto& posting = json_obj["posting"];
    if (posting.contains("block_size")) {
      config.posting.block_size = posting["block_size"].get<int>();
    }
    if (posting.contains("freq_bits")) {
      config.posting.freq_bits = posting["freq_bits"].get<int>();
    }
    if (posting.contains("use_roaring")) {
      config.posting.use_roaring = posting["use_roaring"].get<std::string>();
    }
  }

  // Parse synonyms config
  if (json_obj.contains("synonyms")) {
    const auto& syn = json_obj["synonyms"];
    if (syn.contains("enable")) {
      config.synonyms.enable = syn["enable"].get<bool>();
    }
    if (syn.contains("file")) {
      config.synonyms.file = syn["file"].get<std::string>();
    }
  }

  return config;
}

/**
 * @brief Parse configuration from JSON object
 */
mygram::utils::Expected<Config, mygram::utils::Error> ParseConfigFromJson(const json& root) {
  using mygram::utils::ErrorCode;
  using mygram::utils::MakeError;
  using mygram::utils::MakeUnexpected;

  Config config;

  // Parse MySQL config
  if (root.contains("mysql")) {
    auto mysql_result = ParseMysqlConfig(root["mysql"]);
    if (!mysql_result) {
      return MakeUnexpected(mysql_result.error());
    }
    config.mysql = std::move(*mysql_result);
  }

  // Parse global index config (legacy format)
  int global_ngram_size = 1;  // default
  if (root.contains("index") && root["index"].contains("ngram_size")) {
    global_ngram_size = root["index"]["ngram_size"].get<int>();
  }

  // Parse tables
  if (root.contains("tables")) {
    for (const auto& table_json : root["tables"]) {
      auto table_result = ParseTableConfig(table_json);
      if (!table_result) {
        return MakeUnexpected(table_result.error());
      }
      auto& table = *table_result;
      // Apply global ngram_size if not set per-table
      if (!table_json.contains("ngram_size")) {
        table.ngram_size = global_ngram_size;
      }
      config.tables.push_back(std::move(table));
    }
  }

  // Parse build config
  if (root.contains("build")) {
    const auto& build = root["build"];
    if (build.contains("mode")) {
      config.build.mode = build["mode"].get<std::string>();
    }
    if (build.contains("batch_size")) {
      config.build.batch_size = build["batch_size"].get<int>();
    }
    if (build.contains("parallelism")) {
      config.build.parallelism = build["parallelism"].get<int>();
    }
    if (build.contains("throttle_ms")) {
      config.build.throttle_ms = build["throttle_ms"].get<int>();
    }
  }

  // Parse replication config
  if (root.contains("replication")) {
    const auto& repl = root["replication"];
    if (repl.contains("enable")) {
      config.replication.enable = repl["enable"].get<bool>();
    }
    if (repl.contains("auto_initial_snapshot")) {
      config.replication.auto_initial_snapshot = repl["auto_initial_snapshot"].get<bool>();
    }
    if (repl.contains("server_id")) {
      config.replication.server_id = repl["server_id"].get<uint32_t>();
    }
    if (repl.contains("start_from")) {
      config.replication.start_from = repl["start_from"].get<std::string>();
    }
    if (repl.contains("queue_size")) {
      config.replication.queue_size = repl["queue_size"].get<int>();
    }
    if (repl.contains("reconnect_backoff_min_ms")) {
      config.replication.reconnect_backoff_min_ms = repl["reconnect_backoff_min_ms"].get<int>();
    }
    if (repl.contains("reconnect_backoff_max_ms")) {
      config.replication.reconnect_backoff_max_ms = repl["reconnect_backoff_max_ms"].get<int>();
    }

    // Validate server_id
    if (config.replication.enable && config.replication.server_id == 0) {
      std::stringstream err_msg;
      err_msg << "Replication configuration error: server_id must be set when replication is enabled\n";
      err_msg << "  The server_id must be a unique non-zero value.\n";
      err_msg << "  Example:\n";
      err_msg << "    replication:\n";
      err_msg << "      enable: true\n";
      err_msg << "      server_id: 100";
      return MakeUnexpected(MakeError(ErrorCode::kConfigValidationError, err_msg.str()));
    }

    // Validate start_from
    if (config.replication.enable) {
      const auto& start = config.replication.start_from;

      // Lambda to check if start_from value is valid
      auto is_valid_start_from = [&start]() -> bool {
        return start == "snapshot" || start == "latest" || start.find("gtid=") == 0;
      };

      if (!is_valid_start_from()) {
        std::stringstream err_msg;
        err_msg << "Replication configuration error: Invalid start_from value: '" << start << "'\n";
        err_msg << "  Valid options:\n";
        err_msg << "    - snapshot    : Start from snapshot GTID\n";
        err_msg << "    - latest      : Start from current GTID\n";
        err_msg << "    - gtid=<UUID:txn> : Start from specific GTID\n";
        err_msg << "  Example:\n";
        err_msg << "    replication:\n";
        err_msg << "      start_from: snapshot";
        return MakeUnexpected(MakeError(ErrorCode::kConfigInvalidValue, err_msg.str()));
      }

      // If gtid= format, validate the GTID format
      if (start.find("gtid=") == 0) {
        std::string gtid_str = start.substr(kGtidPrefixLength);  // Remove "gtid=" prefix
        // Basic GTID format check: UUID:transaction_id
        // Full validation will be done when connecting to MySQL
        if (gtid_str.find(':') == std::string::npos) {
          std::stringstream err_msg;
          err_msg << "Replication configuration error: Invalid GTID format: '" << gtid_str << "'\n";
          err_msg << "  Expected format: gtid=UUID:transaction_id\n";
          err_msg << "  Example: gtid=3E11FA47-71CA-11E1-9E33-C80AA9429562:1";
          return MakeUnexpected(MakeError(ErrorCode::kConfigInvalidValue, err_msg.str()));
        }
      }
    }
  }

  // Parse memory config
  if (root.contains("memory")) {
    const auto& mem = root["memory"];
    if (mem.contains("hard_limit_mb")) {
      config.memory.hard_limit_mb = mem["hard_limit_mb"].get<int>();
    }
    if (mem.contains("soft_target_mb")) {
      config.memory.soft_target_mb = mem["soft_target_mb"].get<int>();
    }
    if (mem.contains("arena_chunk_mb")) {
      config.memory.arena_chunk_mb = mem["arena_chunk_mb"].get<int>();
    }
    if (mem.contains("roaring_threshold")) {
      config.memory.roaring_threshold = mem["roaring_threshold"].get<double>();
    }
    if (mem.contains("minute_epoch")) {
      config.memory.minute_epoch = mem["minute_epoch"].get<bool>();
    }

    if (mem.contains("normalize")) {
      const auto& norm = mem["normalize"];
      if (norm.contains("nfkc")) {
        config.memory.normalize.nfkc = norm["nfkc"].get<bool>();
      }
      if (norm.contains("width")) {
        config.memory.normalize.width = norm["width"].get<std::string>();
      }
      if (norm.contains("lower")) {
        config.memory.normalize.lower = norm["lower"].get<bool>();
      }
    }
    if (mem.contains("verify_text")) {
      config.memory.verify_text = mem["verify_text"].get<std::string>();
    }
  }

  // Parse dump config
  if (root.contains("dump")) {
    const auto& dmp = root["dump"];
    if (dmp.contains("dir")) {
      config.dump.dir = dmp["dir"].get<std::string>();
      if (auto v = ValidatePathNoTraversal(config.dump.dir, "dump.dir"); !v) {
        return MakeUnexpected(v.error());
      }
    }
    if (dmp.contains("interval_sec")) {
      config.dump.interval_sec = dmp["interval_sec"].get<int>();
    }
    if (dmp.contains("retain")) {
      config.dump.retain = dmp["retain"].get<int>();
    }
  }

  // Parse API config (both old "server" format and new "api" format)
  if (root.contains("server")) {
    // Legacy format: server.host, server.port
    const auto& server = root["server"];
    if (server.contains("host")) {
      config.api.tcp.bind = server["host"].get<std::string>();
      if (auto v = ValidateBindAddress(config.api.tcp.bind, "server.host"); !v) {
        return MakeUnexpected(v.error());
      }
    }
    if (server.contains("port")) {
      config.api.tcp.port = server["port"].get<int>();
    }
  }
  if (root.contains("api")) {
    const auto& api = root["api"];
    if (api.contains("tcp")) {
      const auto& tcp = api["tcp"];
      if (tcp.contains("bind")) {
        config.api.tcp.bind = tcp["bind"].get<std::string>();
        if (auto v = ValidateBindAddress(config.api.tcp.bind, "api.tcp.bind"); !v) {
          return MakeUnexpected(v.error());
        }
      }
      if (tcp.contains("port")) {
        config.api.tcp.port = tcp["port"].get<int>();
      }
      if (tcp.contains("max_connections")) {
        config.api.tcp.max_connections = tcp["max_connections"].get<int>();
      }
      if (tcp.contains("worker_threads")) {
        config.api.tcp.worker_threads = tcp["worker_threads"].get<int>();
      }
      if (tcp.contains("recv_timeout_sec")) {
        config.api.tcp.recv_timeout_sec = tcp["recv_timeout_sec"].get<int>();
      }
      if (tcp.contains("thread_pool_queue_size")) {
        config.api.tcp.thread_pool_queue_size = tcp["thread_pool_queue_size"].get<int>();
      }
      if (tcp.contains("keepalive")) {
        const auto& ka = tcp["keepalive"];
        if (ka.contains("enabled")) {
          config.api.tcp.keepalive.enabled = ka["enabled"].get<bool>();
        }
        if (ka.contains("idle_sec")) {
          config.api.tcp.keepalive.idle_sec = ka["idle_sec"].get<int>();
        }
        if (ka.contains("interval_sec")) {
          config.api.tcp.keepalive.interval_sec = ka["interval_sec"].get<int>();
        }
        if (ka.contains("probe_count")) {
          config.api.tcp.keepalive.probe_count = ka["probe_count"].get<int>();
        }
      }
      if (tcp.contains("max_write_queue_bytes")) {
        config.api.tcp.max_write_queue_bytes = tcp["max_write_queue_bytes"].get<int64_t>();
      }
    }
    if (api.contains("http")) {
      const auto& http = api["http"];
      if (http.contains("enable")) {
        config.api.http.enable = http["enable"].get<bool>();
      }
      if (http.contains("bind")) {
        config.api.http.bind = http["bind"].get<std::string>();
        if (auto v = ValidateBindAddress(config.api.http.bind, "api.http.bind"); !v) {
          return MakeUnexpected(v.error());
        }
      }
      if (http.contains("port")) {
        config.api.http.port = http["port"].get<int>();
      }
      if (http.contains("enable_cors")) {
        config.api.http.enable_cors = http["enable_cors"].get<bool>();
      }
      if (http.contains("cors_allow_origin")) {
        config.api.http.cors_allow_origin = http["cors_allow_origin"].get<std::string>();
      }
    }
    if (api.contains("default_limit")) {
      config.api.default_limit = api["default_limit"].get<int>();
    }
    if (api.contains("max_query_length")) {
      config.api.max_query_length = api["max_query_length"].get<int>();
    }
    if (api.contains("rate_limiting")) {
      const auto& rate_limiting = api["rate_limiting"];
      if (rate_limiting.contains("enable")) {
        config.api.rate_limiting.enable = rate_limiting["enable"].get<bool>();
      }
      if (rate_limiting.contains("capacity")) {
        config.api.rate_limiting.capacity = rate_limiting["capacity"].get<int>();
      }
      if (rate_limiting.contains("refill_rate")) {
        config.api.rate_limiting.refill_rate = rate_limiting["refill_rate"].get<int>();
      }
      if (rate_limiting.contains("max_clients")) {
        config.api.rate_limiting.max_clients = rate_limiting["max_clients"].get<int>();
      }
    }
    if (api.contains("unix_socket")) {
      const auto& unix_socket = api["unix_socket"];
      if (unix_socket.contains("path")) {
        config.api.unix_socket.path = unix_socket["path"].get<std::string>();
      }
    }
  }

  // Parse network config
  if (root.contains("network")) {
    const auto& net = root["network"];
    if (net.contains("allow_cidrs")) {
      config.network.allow_cidrs = net["allow_cidrs"].get<std::vector<std::string>>();
    }
  }

  // Parse logging config
  if (root.contains("logging")) {
    const auto& log = root["logging"];
    if (log.contains("level")) {
      config.logging.level = log["level"].get<std::string>();
    }
    if (log.contains("format")) {
      config.logging.format = log["format"].get<std::string>();
    }
    if (log.contains("file")) {
      config.logging.file = log["file"].get<std::string>();
      if (auto v = ValidatePathNoTraversal(config.logging.file, "logging.file"); !v) {
        return MakeUnexpected(v.error());
      }
    }
  }

  // Parse cache config
  if (root.contains("cache")) {
    const auto& cache = root["cache"];
    if (cache.contains("enabled")) {
      config.cache.enabled = cache["enabled"].get<bool>();
    }
    if (cache.contains("max_memory_mb")) {
      constexpr size_t kBytesPerMB = 1024 * 1024;    // Bytes in one megabyte
      constexpr int64_t kMaxMemoryMB = 1024 * 1024;  // 1TB max (reasonable upper limit)
      int64_t max_memory_mb = cache["max_memory_mb"].get<int64_t>();

      // Validate to prevent integer overflow
      if (max_memory_mb < 0) {
        return MakeUnexpected(MakeError(
            ErrorCode::kConfigInvalidValue,
            "Configuration error: cache.max_memory_mb cannot be negative (got " + std::to_string(max_memory_mb) + ")"));
      }
      if (max_memory_mb > kMaxMemoryMB) {
        return MakeUnexpected(MakeError(ErrorCode::kConfigInvalidValue,
                                        "Configuration error: cache.max_memory_mb exceeds maximum allowed value (" +
                                            std::to_string(kMaxMemoryMB) +
                                            " MB). Got: " + std::to_string(max_memory_mb) + " MB"));
      }

      config.cache.max_memory_bytes = static_cast<size_t>(max_memory_mb) * kBytesPerMB;
    }
    if (cache.contains("min_query_cost_ms")) {
      config.cache.min_query_cost_ms = cache["min_query_cost_ms"].get<double>();
    }
    if (cache.contains("ttl_seconds")) {
      config.cache.ttl_seconds = cache["ttl_seconds"].get<int>();
    }
    if (cache.contains("invalidation_strategy")) {
      config.cache.invalidation_strategy = cache["invalidation_strategy"].get<std::string>();
    }
    if (cache.contains("compression_enabled")) {
      config.cache.compression_enabled = cache["compression_enabled"].get<bool>();
    }
    if (cache.contains("eviction_batch_size")) {
      config.cache.eviction_batch_size = cache["eviction_batch_size"].get<int>();
    }
    if (cache.contains("invalidation")) {
      const auto& invalidation = cache["invalidation"];
      if (invalidation.contains("batch_size")) {
        config.cache.invalidation.batch_size = invalidation["batch_size"].get<int>();
      }
      if (invalidation.contains("max_delay_ms")) {
        config.cache.invalidation.max_delay_ms = invalidation["max_delay_ms"].get<int>();
      }
    }

    // Validate cache memory against physical memory
    if (config.cache.enabled && config.cache.max_memory_bytes > 0) {
      auto system_info = mygram::utils::GetSystemMemoryInfo();
      if (system_info) {
        constexpr double kMaxCacheRatio = 0.5;       // Maximum 50% of physical memory
        constexpr size_t kBytesPerMB = 1024 * 1024;  // Bytes in one megabyte
        auto max_allowed_cache =
            static_cast<uint64_t>(static_cast<double>(system_info->total_physical_bytes) * kMaxCacheRatio);

        if (config.cache.max_memory_bytes > max_allowed_cache) {
          std::stringstream err_msg;
          err_msg << "Cache configuration error: max_memory_mb exceeds safe limit\n";
          err_msg << "  Configured cache size: " << mygram::utils::FormatBytes(config.cache.max_memory_bytes) << "\n";
          err_msg << "  Physical memory: " << mygram::utils::FormatBytes(system_info->total_physical_bytes) << "\n";
          err_msg << "  Maximum allowed (50% of physical memory): " << mygram::utils::FormatBytes(max_allowed_cache)
                  << "\n";
          err_msg << "  Recommendation:\n";
          err_msg << "    - Set cache.max_memory_mb to at most " << (max_allowed_cache / kBytesPerMB) << " MB\n";
          err_msg << "    - Consider system memory requirements for index and operations\n";
          err_msg << "  Example:\n";
          err_msg << "    cache:\n";
          err_msg << "      max_memory_mb: " << (max_allowed_cache / kBytesPerMB);
          return MakeUnexpected(MakeError(ErrorCode::kConfigInvalidValue, err_msg.str()));
        }
      } else {
        mygram::utils::StructuredLog()
            .Event("config_warning")
            .Field("type", "cache_memory_validation_skipped")
            .Field("reason", "system_memory_info_unavailable")
            .Warn();
      }
    }
  }

  // BM25 scoring configuration
  if (root.contains("bm25")) {
    const auto& bm25 = root["bm25"];
    if (bm25.contains("enable")) {
      config.bm25.enable = bm25["enable"].get<bool>();
    }
    if (bm25.contains("k1")) {
      config.bm25.k1 = bm25["k1"].get<double>();
    }
    if (bm25.contains("b")) {
      config.bm25.b = bm25["b"].get<double>();
    }
  }

  return config;
}

}  // namespace

// ValidateConfigJson is defined in config_validator.cpp
// LoadConfigJson, LoadConfigYaml, LoadConfig are defined in config_loader.cpp

mygram::utils::Expected<mygram::utils::DateTimeProcessor, mygram::utils::Error> MysqlConfig::CreateDateTimeProcessor()
    const {
  auto timezone_result = mygram::utils::TimezoneOffset::Parse(datetime_timezone);
  if (!timezone_result) {
    return MakeUnexpected(timezone_result.error());
  }
  return mygram::utils::DateTimeProcessor(*timezone_result);
}

// Provide definitions for internal namespace declarations in config_internal.h.
// These forward to the anonymous-namespace implementations defined earlier in this file.
// The anonymous namespace is visible within this translation unit, so unqualified
// lookup finds the anonymous-namespace overloads (not the internal:: ones) because
// the anonymous namespace is a sibling, not a parent.
namespace internal {

mygram::utils::Expected<Config, mygram::utils::Error> ParseConfigFromJson(const nlohmann::json& root) {
  // Use a namespace-qualified call via a local using-declaration to avoid
  // infinite recursion. The anonymous namespace injects its names into the
  // enclosing namespace (mygramdb::config), so we can capture a pointer to
  // the anonymous-namespace version before the internal:: version is in scope.
  using ParseFn = mygram::utils::Expected<Config, mygram::utils::Error> (*)(const nlohmann::json&);
  // NOLINTNEXTLINE(misc-redundant-expression)
  static const ParseFn parse_impl = &::mygramdb::config::ParseConfigFromJson;
  return parse_impl(root);
}

nlohmann::json YamlToJson(const YAML::Node& node) {
  using YamlFn = nlohmann::json (*)(const YAML::Node&);
  // NOLINTNEXTLINE(misc-redundant-expression)
  static const YamlFn yaml_impl = &::mygramdb::config::YamlToJson;
  return yaml_impl(node);
}

}  // namespace internal

}  // namespace mygramdb::config
