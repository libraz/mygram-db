/**
 * @file config.cpp
 * @brief Configuration parser implementation with JSON Schema validation
 */

#include "config/config.h"
#include "config_schema_embedded.h"  // Auto-generated embedded schema

#include <fstream>
#include <nlohmann/json-schema.hpp>
#include <nlohmann/json.hpp>
#include <set>
#include <spdlog/spdlog.h>
#include <sstream>
#include <stdexcept>
#include <yaml-cpp/yaml.h>

#ifdef USE_MYSQL
#include "mysql/connection.h"
#endif

namespace mygramdb {
namespace config {

namespace {

using json = nlohmann::json;
using nlohmann::json_schema::json_validator;

/**
 * @brief Convert YAML node to JSON object recursively
 */
json YamlToJson(const YAML::Node& node) {
  switch (node.Type()) {
    case YAML::NodeType::Null:
      return json();
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
      for (const auto& kv : node) {
        result[kv.first.as<std::string>()] = YamlToJson(kv.second);
      }
      return result;
    }
    default:
      return json();
  }
}

/**
 * @brief Parse MySQL configuration from JSON
 */
MysqlConfig ParseMysqlConfig(const json& j) {
  MysqlConfig config;

  if (j.contains("host")) {
    config.host = j["host"].get<std::string>();
  }
  if (j.contains("port")) {
    config.port = j["port"].get<int>();
  }
  if (j.contains("user")) {
    config.user = j["user"].get<std::string>();
  }
  if (j.contains("password")) {
    config.password = j["password"].get<std::string>();
  }
  if (j.contains("database")) {
    config.database = j["database"].get<std::string>();
  }
  if (j.contains("use_gtid")) {
    config.use_gtid = j["use_gtid"].get<bool>();
  }
  if (j.contains("binlog_format")) {
    config.binlog_format = j["binlog_format"].get<std::string>();
  }
  if (j.contains("binlog_row_image")) {
    config.binlog_row_image = j["binlog_row_image"].get<std::string>();
  }
  if (j.contains("connect_timeout_ms")) {
    config.connect_timeout_ms = j["connect_timeout_ms"].get<int>();
  }

  return config;
}

/**
 * @brief Parse filter configuration from JSON
 */
FilterConfig ParseFilterConfig(const json& j) {
  FilterConfig config;

  if (j.contains("name")) {
    config.name = j["name"].get<std::string>();
  }
  if (j.contains("type")) {
    config.type = j["type"].get<std::string>();
  }
  if (j.contains("dict_compress")) {
    config.dict_compress = j["dict_compress"].get<bool>();
  }
  if (j.contains("bitmap_index")) {
    config.bitmap_index = j["bitmap_index"].get<bool>();
  }
  if (j.contains("bucket")) {
    config.bucket = j["bucket"].get<std::string>();
  }

  return config;
}

/**
 * @brief Parse table configuration from JSON
 */
TableConfig ParseTableConfig(const json& j) {
  TableConfig config;

  if (!j.contains("name")) {
    throw std::runtime_error("Table configuration missing 'name' field");
  }
  config.name = j["name"].get<std::string>();

  if (j.contains("primary_key")) {
    config.primary_key = j["primary_key"].get<std::string>();
  }
  if (j.contains("ngram_size")) {
    config.ngram_size = j["ngram_size"].get<int>();
  }
  if (j.contains("where_clause")) {
    config.where_clause = j["where_clause"].get<std::string>();
  }

  // Parse text_source
  if (j.contains("text_source")) {
    const auto& text_source = j["text_source"];
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

  // Parse filters
  if (j.contains("filters")) {
    for (const auto& filter_json : j["filters"]) {
      config.filters.push_back(ParseFilterConfig(filter_json));
    }
  }

  // Parse posting config
  if (j.contains("posting")) {
    const auto& posting = j["posting"];
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
    if (repl.contains("server_id")) {
      config.replication.server_id = repl["server_id"].get<uint32_t>();
    }
    if (repl.contains("start_from")) {
      config.replication.start_from = repl["start_from"].get<std::string>();
    }
    if (repl.contains("state_file")) {
      config.replication.state_file = repl["state_file"].get<std::string>();
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
      throw std::runtime_error(
          "replication.server_id must be set to a non-zero value when replication is enabled");
    }

    // Validate start_from
    if (config.replication.enable) {
      const auto& start = config.replication.start_from;
      if (start != "snapshot" && start != "latest" && start != "state_file" &&
          start.find("gtid=") != 0) {
        throw std::runtime_error(
            "replication.start_from must be one of: snapshot, latest, state_file, or "
            "gtid=<UUID:txn>");
      }

      // If gtid= format, validate the GTID format
      if (start.find("gtid=") == 0) {
        std::string gtid_str = start.substr(5);  // Remove "gtid=" prefix
#ifdef USE_MYSQL
        auto gtid = mysql::GTID::Parse(gtid_str);
        if (!gtid.has_value()) {
          throw std::runtime_error("Invalid GTID format in replication.start_from: " + gtid_str +
                                   ". Expected format: gtid=UUID:transaction_id (e.g., "
                                   "gtid=3E11FA47-71CA-11E1-9E33-C80AA9429562:1)");
        }
#else
        // Basic GTID format check when MySQL is not available
        if (gtid_str.find(':') == std::string::npos) {
          throw std::runtime_error("Invalid GTID format in replication.start_from: " + gtid_str +
                                   ". Expected format: gtid=UUID:transaction_id (e.g., "
                                   "gtid=3E11FA47-71CA-11E1-9E33-C80AA9429562:1)");
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

  // Parse snapshot config
  if (root.contains("snapshot")) {
    const auto& snap = root["snapshot"];
    if (snap.contains("dir")) {
      config.snapshot.dir = snap["dir"].get<std::string>();
    }
    if (snap.contains("interval_sec")) {
      config.snapshot.interval_sec = snap["interval_sec"].get<int>();
    }
    if (snap.contains("retain")) {
      config.snapshot.retain = snap["retain"].get<int>();
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

  return config;
}

/**
 * @brief Read file contents as string
 */
std::string ReadFileToString(const std::string& path) {
  std::ifstream file(path);
  if (!file.is_open()) {
    throw std::runtime_error("Failed to open file: " + path);
  }
  std::stringstream buffer;
  buffer << file.rdbuf();
  return buffer.str();
}

/**
 * @brief Detect file format based on extension
 */
enum class FileFormat { YAML, JSON, UNKNOWN };

FileFormat DetectFileFormat(const std::string& path) {
  if (path.size() >= 5 && path.substr(path.size() - 5) == ".json") {
    return FileFormat::JSON;
  }
  if (path.size() >= 5 && path.substr(path.size() - 5) == ".yaml") {
    return FileFormat::YAML;
  }
  if (path.size() >= 4 && path.substr(path.size() - 4) == ".yml") {
    return FileFormat::YAML;
  }
  return FileFormat::UNKNOWN;
}

}  // namespace

void ValidateConfigJson(const std::string& config_json_str, const std::string& schema_json_str) {
  try {
    json config_json = json::parse(config_json_str);

    // Use embedded schema if no custom schema provided
    std::string schema_to_use = schema_json_str.empty()
                                  ? std::string(kConfigSchemaJson)
                                  : schema_json_str;

    json schema_json = json::parse(schema_to_use);

    json_validator validator;
    validator.set_root_schema(schema_json);

    try {
      validator.validate(config_json);
      spdlog::debug("Configuration validation passed");
    } catch (const std::exception& e) {
      throw std::runtime_error(std::string("Configuration validation failed: ") + e.what());
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
    spdlog::info("  MySQL: {}:{}@{}:{}", config.mysql.user,
                 std::string(config.mysql.password.length(), '*'), config.mysql.host,
                 config.mysql.port);

    return config;
  } catch (const json::parse_error& e) {
    throw std::runtime_error(std::string("JSON parse error in ") + path + ": " + e.what());
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
    spdlog::info("  MySQL: {}:{}@{}:{}", config.mysql.user,
                 std::string(config.mysql.password.length(), '*'), config.mysql.host,
                 config.mysql.port);

    return config;
  } catch (const YAML::Exception& e) {
    throw std::runtime_error(std::string("YAML parse error in ") + path + ": " + e.what());
  } catch (const std::exception& e) {
    throw std::runtime_error(std::string("Failed to load config from ") + path + ": " + e.what());
  }
}

Config LoadConfig(const std::string& path, const std::string& schema_path) {
  FileFormat format = DetectFileFormat(path);

  switch (format) {
    case FileFormat::JSON:
      spdlog::debug("Detected JSON format for config file: {}", path);
      return LoadConfigJson(path, schema_path);

    case FileFormat::YAML:
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
        throw std::runtime_error(std::string("YAML config validation failed: ") + e.what());
      }
      return LoadConfigYaml(path);

    case FileFormat::UNKNOWN:
    default:
      // Try YAML first (legacy default), then JSON
      try {
        spdlog::debug("Unknown file format, trying YAML first: {}", path);
        return LoadConfigYaml(path);
      } catch (...) {
        spdlog::debug("YAML parsing failed, trying JSON: {}", path);
        return LoadConfigJson(path, schema_path);
      }
  }
}

}  // namespace config
}  // namespace mygramdb
