/**
 * @file config.cpp
 * @brief Configuration parser implementation with JSON Schema validation
 */

#include "config/config.h"

#include <spdlog/spdlog.h>
#include <yaml-cpp/yaml.h>

#include <fstream>
#include <nlohmann/json-schema.hpp>
#include <nlohmann/json.hpp>
#include <set>
#include <sstream>
#include <stdexcept>

#include "config_schema_embedded.h"  // Auto-generated embedded schema

#ifdef USE_MYSQL
#include "mysql/connection.h"
#endif

namespace mygramdb::config {

namespace {

using json = nlohmann::json;
using nlohmann::json_schema::json_validator;

constexpr size_t kGtidPrefixLength = 5;  // "gtid="

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
      } catch (...) {
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
 */
MysqlConfig ParseMysqlConfig(const json& json_obj) {
  MysqlConfig config;

  if (json_obj.contains("host")) {
    config.host = json_obj["host"].get<std::string>();
  }
  if (json_obj.contains("port")) {
    config.port = json_obj["port"].get<int>();
  }
  if (json_obj.contains("user")) {
    config.user = json_obj["user"].get<std::string>();
  }
  if (json_obj.contains("password")) {
    config.password = json_obj["password"].get<std::string>();
  }
  if (json_obj.contains("database")) {
    config.database = json_obj["database"].get<std::string>();
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

  return config;
}

/**
 * @brief Parse required filter configuration from JSON
 */
RequiredFilterConfig ParseRequiredFilterConfig(const json& json_obj) {
  RequiredFilterConfig config;

  if (json_obj.contains("name")) {
    config.name = json_obj["name"].get<std::string>();
  }
  if (json_obj.contains("type")) {
    config.type = json_obj["type"].get<std::string>();
  }
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
    throw std::runtime_error(err_msg.str());
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
    throw std::runtime_error(err_msg.str());
  }

  // Validate: Other operators should have a value
  if (config.op != "IS NULL" && config.op != "IS NOT NULL" && config.value.empty()) {
    std::stringstream err_msg;
    err_msg << "Required filter error: Operator '" << config.op << "' requires a value\n";
    err_msg << "  Please provide a 'value' field for this operator.\n";
    err_msg << "  Example:\n";
    err_msg << "    required_filters:\n";
    err_msg << "      - name: status\n";
    err_msg << "        type: int\n";
    err_msg << "        op: \"" << config.op << "\"\n";
    err_msg << "        value: 1";
    throw std::runtime_error(err_msg.str());
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
TableConfig ParseTableConfig(const json& json_obj) {
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
    throw std::runtime_error(err_msg.str());
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
  }
  // If kanji_ngram_size is 0 or not specified, use ngram_size
  if (config.kanji_ngram_size == 0) {
    config.kanji_ngram_size = config.ngram_size;
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
    throw std::runtime_error(err_msg.str());
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
      config.required_filters.push_back(ParseRequiredFilterConfig(filter_json));
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

  return config;
}

/**
 * @brief Parse configuration from JSON object
 */
Config ParseConfigFromJson(const json& root) {
  Config config;

  // Parse MySQL config
  if (root.contains("mysql")) {
    config.mysql = ParseMysqlConfig(root["mysql"]);
  }

  // Parse global index config (legacy format)
  int global_ngram_size = 1;  // default
  if (root.contains("index") && root["index"].contains("ngram_size")) {
    global_ngram_size = root["index"]["ngram_size"].get<int>();
  }

  // Parse tables
  if (root.contains("tables")) {
    for (const auto& table_json : root["tables"]) {
      TableConfig table = ParseTableConfig(table_json);
      // Apply global ngram_size if not set per-table
      if (!table_json.contains("ngram_size")) {
        table.ngram_size = global_ngram_size;
      }
      config.tables.push_back(table);
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
      throw std::runtime_error(err_msg.str());
    }

    // Validate start_from
    if (config.replication.enable) {
      const auto& start = config.replication.start_from;
      if (start != "snapshot" && start != "latest" && start.find("gtid=") != 0) {
        std::stringstream err_msg;
        err_msg << "Replication configuration error: Invalid start_from value: '" << start << "'\n";
        err_msg << "  Valid options:\n";
        err_msg << "    - snapshot    : Start from snapshot GTID\n";
        err_msg << "    - latest      : Start from current GTID\n";
        err_msg << "    - gtid=<UUID:txn> : Start from specific GTID\n";
        err_msg << "  Example:\n";
        err_msg << "    replication:\n";
        err_msg << "      start_from: snapshot";
        throw std::runtime_error(err_msg.str());
      }

      // If gtid= format, validate the GTID format
      if (start.find("gtid=") == 0) {
        std::string gtid_str = start.substr(kGtidPrefixLength);  // Remove "gtid=" prefix
#ifdef USE_MYSQL
        auto gtid = mysql::GTID::Parse(gtid_str);
        if (!gtid.has_value()) {
          std::stringstream err_msg;
          err_msg << "Replication configuration error: Invalid GTID format: '" << gtid_str << "'\n";
          err_msg << "  Expected format: gtid=UUID:transaction_id\n";
          err_msg << "  Example: gtid=3E11FA47-71CA-11E1-9E33-C80AA9429562:1\n";
          err_msg << "  Where:\n";
          err_msg << "    - UUID is the MySQL server UUID\n";
          err_msg << "    - transaction_id is the transaction number";
          throw std::runtime_error(err_msg.str());
        }
#else
        // Basic GTID format check when MySQL is not available
        if (gtid_str.find(':') == std::string::npos) {
          std::stringstream err_msg;
          err_msg << "Replication configuration error: Invalid GTID format: '" << gtid_str << "'\n";
          err_msg << "  Expected format: gtid=UUID:transaction_id\n";
          err_msg << "  Example: gtid=3E11FA47-71CA-11E1-9E33-C80AA9429562:1";
          throw std::runtime_error(err_msg.str());
        }
#endif
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
  }

  // Parse dump config
  if (root.contains("dump")) {
    const auto& dmp = root["dump"];
    if (dmp.contains("dir")) {
      config.dump.dir = dmp["dir"].get<std::string>();
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
      }
      if (tcp.contains("port")) {
        config.api.tcp.port = tcp["port"].get<int>();
      }
    }
    if (api.contains("http")) {
      const auto& http = api["http"];
      if (http.contains("enable")) {
        config.api.http.enable = http["enable"].get<bool>();
      }
      if (http.contains("bind")) {
        config.api.http.bind = http["bind"].get<std::string>();
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
    if (log.contains("json")) {
      config.logging.json = log["json"].get<bool>();
    }
  }

  // Parse cache config
  if (root.contains("cache")) {
    const auto& cache = root["cache"];
    if (cache.contains("enabled")) {
      config.cache.enabled = cache["enabled"].get<bool>();
    }
    if (cache.contains("max_memory_mb")) {
      config.cache.max_memory_mb = cache["max_memory_mb"].get<int>();
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
  }

  return config;
}

/**
 * @brief Read file contents as string
 */
std::string ReadFileToString(const std::string& path) {
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
      throw std::runtime_error(err_msg.str());
    }
    throw std::runtime_error("Failed to open file: " + path);
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
    throw std::runtime_error(err_msg.str());
  }

  return content;
}

/**
 * @brief Detect file format based on extension
 */
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

void ValidateConfigJson(const std::string& config_json_str, const std::string& schema_json_str) {
  try {
    json config_json = json::parse(config_json_str);

    // Use embedded schema if no custom schema provided
    std::string schema_to_use = schema_json_str.empty() ? std::string(kConfigSchemaJson) : schema_json_str;

    json schema_json = json::parse(schema_to_use);

    json_validator validator;
    validator.set_root_schema(schema_json);

    try {
      validator.validate(config_json);
      spdlog::debug("Configuration validation passed");
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
      throw std::runtime_error(err_msg.str());
    }
  } catch (const json::parse_error& e) {
    throw std::runtime_error(std::string("JSON parse error: ") + e.what());
  }
}

Config LoadConfigJson(const std::string& path, const std::string& schema_path) {
  try {
    std::string config_str = ReadFileToString(path);
    json config_json = json::parse(config_str);

    // Always validate - use embedded schema if no custom schema provided
    std::string schema_str;
    if (!schema_path.empty()) {
      schema_str = ReadFileToString(schema_path);
    }
    // ValidateConfigJson will use embedded schema if schema_str is empty
    ValidateConfigJson(config_str, schema_str);

    Config config = ParseConfigFromJson(config_json);

    spdlog::info("Configuration loaded successfully from {}", path);
    spdlog::info("  Tables: {}", config.tables.size());
    spdlog::info("  MySQL: {}:{}@{}:{}", config.mysql.user, std::string(config.mysql.password.length(), '*'),
                 config.mysql.host, config.mysql.port);

    return config;
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
    throw std::runtime_error(err_msg.str());
  } catch (const std::exception& e) {
    throw std::runtime_error(std::string("Failed to load config from ") + path + ": " + e.what());
  }
}

Config LoadConfigYaml(const std::string& path) {
  try {
    YAML::Node yaml_root = YAML::LoadFile(path);
    json json_root = YamlToJson(yaml_root);

    Config config = ParseConfigFromJson(json_root);

    spdlog::info("Configuration loaded successfully from {}", path);
    spdlog::info("  Tables: {}", config.tables.size());
    spdlog::info("  MySQL: {}:{}@{}:{}", config.mysql.user, std::string(config.mysql.password.length(), '*'),
                 config.mysql.host, config.mysql.port);

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
    throw std::runtime_error(err_msg.str());
  } catch (const std::exception& e) {
    throw std::runtime_error(std::string("Failed to load config from ") + path + ": " + e.what());
  }
}

Config LoadConfig(const std::string& path, const std::string& schema_path) {
  FileFormat format = DetectFileFormat(path);

  switch (format) {
    case FileFormat::kJson:
      spdlog::debug("Detected JSON format for config file: {}", path);
      return LoadConfigJson(path, schema_path);

    case FileFormat::kYaml:
      spdlog::debug("Detected YAML format for config file: {}", path);
      // Always validate YAML configs - convert to JSON first
      try {
        YAML::Node yaml_root = YAML::LoadFile(path);
        json json_root = YamlToJson(yaml_root);
        std::string config_json_str = json_root.dump();
        std::string schema_str;
        if (!schema_path.empty()) {
          schema_str = ReadFileToString(schema_path);
        }
        // ValidateConfigJson will use embedded schema if schema_str is empty
        ValidateConfigJson(config_json_str, schema_str);
      } catch (const std::exception& e) {
        // Re-throw with original error message (already enhanced)
        throw;
      }
      return LoadConfigYaml(path);

    case FileFormat::kUnknown:
    default:
      // Try YAML first (legacy default), then JSON
      try {
        spdlog::debug("Unknown file format, trying YAML first: {}", path);
        return LoadConfigYaml(path);
      } catch (const std::exception& yaml_error) {
        try {
          spdlog::debug("YAML parsing failed, trying JSON: {}", path);
          return LoadConfigJson(path, schema_path);
        } catch (const std::exception& json_error) {
          std::stringstream err_msg;
          err_msg << "Failed to load configuration file: " << path << "\n";
          err_msg << "  File format could not be determined (.yaml, .yml, or .json expected)\n";
          err_msg << "  Attempted YAML parsing: " << yaml_error.what() << "\n";
          err_msg << "  Attempted JSON parsing: " << json_error.what() << "\n";
          err_msg << "  Please ensure the file has the correct extension and valid syntax.";
          throw std::runtime_error(err_msg.str());
        }
      }
  }
}

}  // namespace mygramdb::config
