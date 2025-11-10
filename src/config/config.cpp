/**
 * @file config.cpp
 * @brief Configuration YAML parser implementation
 */

#include "config/config.h"
#include <yaml-cpp/yaml.h>
#include <stdexcept>
#include <set>
#include <spdlog/spdlog.h>

#ifdef USE_MYSQL
#include "mysql/connection.h"
#endif

namespace mygramdb {
namespace config {

namespace {

/**
 * @brief Check for unknown keys in a YAML node
 * @param node YAML node to check
 * @param known_keys Set of known/valid keys
 * @param section_name Name of the section for error messages
 * @throws std::runtime_error if unknown keys are found
 */
void CheckUnknownKeys(const YAML::Node& node,
                      const std::set<std::string>& known_keys,
                      const std::string& section_name) {
  if (!node.IsMap()) return;

  std::vector<std::string> unknown_keys;
  for (const auto& kv : node) {
    std::string key = kv.first.as<std::string>();
    if (known_keys.find(key) == known_keys.end()) {
      unknown_keys.push_back(key);
    }
  }

  if (!unknown_keys.empty()) {
    std::string error = "Unknown key(s) in [" + section_name + "]: ";
    for (size_t i = 0; i < unknown_keys.size(); ++i) {
      if (i > 0) error += ", ";
      error += "'" + unknown_keys[i] + "'";
    }
    throw std::runtime_error(error);
  }
}

/**
 * @brief Parse MySQL configuration from YAML node
 */
MysqlConfig ParseMysqlConfig(const YAML::Node& node) {
  // Check for unknown keys
  CheckUnknownKeys(node, {
    "host", "port", "user", "password", "database",
    "use_gtid", "binlog_format", "binlog_row_image", "connect_timeout_ms"
  }, "mysql");

  MysqlConfig config;

  if (node["host"]) config.host = node["host"].as<std::string>();
  if (node["port"]) config.port = node["port"].as<int>();
  if (node["user"]) config.user = node["user"].as<std::string>();
  if (node["password"]) config.password = node["password"].as<std::string>();
  if (node["database"]) config.database = node["database"].as<std::string>();
  if (node["use_gtid"]) config.use_gtid = node["use_gtid"].as<bool>();
  if (node["binlog_format"]) config.binlog_format = node["binlog_format"].as<std::string>();
  if (node["binlog_row_image"]) config.binlog_row_image = node["binlog_row_image"].as<std::string>();
  if (node["connect_timeout_ms"]) config.connect_timeout_ms = node["connect_timeout_ms"].as<int>();

  return config;
}

/**
 * @brief Parse table configuration from YAML node
 */
TableConfig ParseTableConfig(const YAML::Node& node) {
  TableConfig config;

  if (!node["name"]) {
    throw std::runtime_error("Table configuration missing 'name' field");
  }
  config.name = node["name"].as<std::string>();

  if (node["primary_key"]) config.primary_key = node["primary_key"].as<std::string>();
  if (node["ngram_size"]) config.ngram_size = node["ngram_size"].as<int>();
  if (node["where_clause"]) config.where_clause = node["where_clause"].as<std::string>();

  // Parse text_source
  if (node["text_source"]) {
    const auto& ts = node["text_source"];
    if (ts["column"]) {
      config.text_source.column = ts["column"].as<std::string>();
    }
    if (ts["concat"]) {
      config.text_source.concat = ts["concat"].as<std::vector<std::string>>();
    }
    if (ts["delimiter"]) {
      config.text_source.delimiter = ts["delimiter"].as<std::string>();
    }
  }

  // Parse filters
  if (node["filters"]) {
    for (const auto& filter_node : node["filters"]) {
      FilterConfig filter;
      if (filter_node["name"]) filter.name = filter_node["name"].as<std::string>();
      if (filter_node["type"]) filter.type = filter_node["type"].as<std::string>();
      if (filter_node["dict_compress"]) filter.dict_compress = filter_node["dict_compress"].as<bool>();
      if (filter_node["bitmap_index"]) filter.bitmap_index = filter_node["bitmap_index"].as<bool>();
      if (filter_node["bucket"]) filter.bucket = filter_node["bucket"].as<std::string>();
      config.filters.push_back(filter);
    }
  }

  // Parse posting config
  if (node["posting"]) {
    const auto& posting = node["posting"];
    if (posting["block_size"]) config.posting.block_size = posting["block_size"].as<int>();
    if (posting["freq_bits"]) config.posting.freq_bits = posting["freq_bits"].as<int>();
    if (posting["use_roaring"]) config.posting.use_roaring = posting["use_roaring"].as<std::string>();
  }

  return config;
}

}  // namespace

Config LoadConfig(const std::string& path) {
  Config config;

  try {
    YAML::Node root = YAML::LoadFile(path);

    // Check for unknown top-level keys
    CheckUnknownKeys(root, {
      "mysql", "tables", "build", "replication", "memory",
      "snapshot", "api", "network", "logging"
    }, "root");

    // Parse MySQL config
    if (root["mysql"]) {
      config.mysql = ParseMysqlConfig(root["mysql"]);
    }

    // Parse global index config (legacy format)
    int global_ngram_size = 1;  // default
    if (root["index"] && root["index"]["ngram_size"]) {
      global_ngram_size = root["index"]["ngram_size"].as<int>();
    }

    // Parse tables
    if (root["tables"]) {
      for (const auto& table_node : root["tables"]) {
        TableConfig table = ParseTableConfig(table_node);
        // Apply global ngram_size if not set per-table
        if (!table_node["ngram_size"]) {
          table.ngram_size = global_ngram_size;
        }
        config.tables.push_back(table);
      }
    }

    // Parse build config
    if (root["build"]) {
      const auto& build = root["build"];
      if (build["mode"]) config.build.mode = build["mode"].as<std::string>();
      if (build["batch_size"]) config.build.batch_size = build["batch_size"].as<int>();
      if (build["parallelism"]) config.build.parallelism = build["parallelism"].as<int>();
      if (build["throttle_ms"]) config.build.throttle_ms = build["throttle_ms"].as<int>();
    }

    // Parse replication config
    if (root["replication"]) {
      const auto& repl = root["replication"];
      if (repl["enable"]) config.replication.enable = repl["enable"].as<bool>();
      if (repl["server_id"]) config.replication.server_id = repl["server_id"].as<uint32_t>();
      if (repl["start_from"]) config.replication.start_from = repl["start_from"].as<std::string>();
      if (repl["state_file"]) config.replication.state_file = repl["state_file"].as<std::string>();
      if (repl["queue_size"]) config.replication.queue_size = repl["queue_size"].as<int>();

      // Validate server_id
      if (config.replication.enable && config.replication.server_id == 0) {
        throw std::runtime_error("replication.server_id must be set to a non-zero value when replication is enabled");
      }

      // Validate start_from
      if (config.replication.enable) {
        const auto& start = config.replication.start_from;
        if (start != "snapshot" && start != "latest" && start != "state_file" &&
            start.find("gtid=") != 0) {
          throw std::runtime_error("replication.start_from must be one of: snapshot, latest, state_file, or gtid=<UUID:txn>");
        }

        // If gtid= format, validate the GTID format
        if (start.find("gtid=") == 0) {
          std::string gtid_str = start.substr(5);  // Remove "gtid=" prefix
#ifdef USE_MYSQL
          auto gtid = mysql::GTID::Parse(gtid_str);
          if (!gtid.has_value()) {
            throw std::runtime_error("Invalid GTID format in replication.start_from: " + gtid_str +
                                     ". Expected format: gtid=UUID:transaction_id (e.g., gtid=3E11FA47-71CA-11E1-9E33-C80AA9429562:1)");
          }
#else
          // Basic GTID format check when MySQL is not available
          if (gtid_str.find(':') == std::string::npos) {
            throw std::runtime_error("Invalid GTID format in replication.start_from: " + gtid_str +
                                     ". Expected format: gtid=UUID:transaction_id (e.g., gtid=3E11FA47-71CA-11E1-9E33-C80AA9429562:1)");
          }
#endif
        }
      }
    }

    // Parse memory config
    if (root["memory"]) {
      const auto& mem = root["memory"];
      if (mem["hard_limit_mb"]) config.memory.hard_limit_mb = mem["hard_limit_mb"].as<int>();
      if (mem["soft_target_mb"]) config.memory.soft_target_mb = mem["soft_target_mb"].as<int>();
      if (mem["arena_chunk_mb"]) config.memory.arena_chunk_mb = mem["arena_chunk_mb"].as<int>();
      if (mem["roaring_threshold"]) config.memory.roaring_threshold = mem["roaring_threshold"].as<double>();
      if (mem["minute_epoch"]) config.memory.minute_epoch = mem["minute_epoch"].as<bool>();

      if (mem["normalize"]) {
        const auto& norm = mem["normalize"];
        if (norm["nfkc"]) config.memory.normalize.nfkc = norm["nfkc"].as<bool>();
        if (norm["width"]) config.memory.normalize.width = norm["width"].as<std::string>();
        if (norm["lower"]) config.memory.normalize.lower = norm["lower"].as<bool>();
      }
    }

    // Parse snapshot config
    if (root["snapshot"]) {
      const auto& snap = root["snapshot"];
      if (snap["dir"]) config.snapshot.dir = snap["dir"].as<std::string>();
      if (snap["interval_sec"]) config.snapshot.interval_sec = snap["interval_sec"].as<int>();
      if (snap["retain"]) config.snapshot.retain = snap["retain"].as<int>();
    }

    // Parse API config (both old "server" format and new "api" format)
    if (root["server"]) {
      // Legacy format: server.host, server.port
      const auto& server = root["server"];
      if (server["host"]) config.api.tcp.bind = server["host"].as<std::string>();
      if (server["port"]) config.api.tcp.port = server["port"].as<int>();
    }
    if (root["api"]) {
      const auto& api = root["api"];
      if (api["tcp"]) {
        const auto& tcp = api["tcp"];
        if (tcp["bind"]) config.api.tcp.bind = tcp["bind"].as<std::string>();
        if (tcp["port"]) config.api.tcp.port = tcp["port"].as<int>();
      }
      if (api["http"]) {
        const auto& http = api["http"];
        if (http["enable"]) config.api.http.enable = http["enable"].as<bool>();
        if (http["bind"]) config.api.http.bind = http["bind"].as<std::string>();
        if (http["port"]) config.api.http.port = http["port"].as<int>();
      }
    }

    // Parse network config
    if (root["network"]) {
      const auto& net = root["network"];
      if (net["allow_cidrs"]) {
        config.network.allow_cidrs = net["allow_cidrs"].as<std::vector<std::string>>();
      }
    }

    // Parse logging config
    if (root["logging"]) {
      const auto& log = root["logging"];
      if (log["level"]) config.logging.level = log["level"].as<std::string>();
      if (log["json"]) config.logging.json = log["json"].as<bool>();
    }

    spdlog::info("Configuration loaded successfully from {}", path);
    spdlog::info("  Tables: {}", config.tables.size());
    spdlog::info("  MySQL: {}:{}@{}:{}", config.mysql.user,
                 std::string(config.mysql.password.length(), '*'),
                 config.mysql.host, config.mysql.port);

  } catch (const YAML::Exception& e) {
    throw std::runtime_error(std::string("YAML parse error: ") + e.what());
  }

  return config;
}

}  // namespace config
}  // namespace mygramdb
