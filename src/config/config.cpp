/**
 * @file config.cpp
 * @brief Configuration YAML parser implementation
 */

#include "config/config.h"
#include <yaml-cpp/yaml.h>
#include <stdexcept>
#include <spdlog/spdlog.h>

namespace mygramdb {
namespace config {

namespace {

/**
 * @brief Parse MySQL configuration from YAML node
 */
MysqlConfig ParseMysqlConfig(const YAML::Node& node) {
  MysqlConfig config;

  if (node["host"]) config.host = node["host"].as<std::string>();
  if (node["port"]) config.port = node["port"].as<int>();
  if (node["user"]) config.user = node["user"].as<std::string>();
  if (node["password"]) config.password = node["password"].as<std::string>();
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

    // Parse MySQL config
    if (root["mysql"]) {
      config.mysql = ParseMysqlConfig(root["mysql"]);
    }

    // Parse tables
    if (root["tables"]) {
      for (const auto& table_node : root["tables"]) {
        config.tables.push_back(ParseTableConfig(table_node));
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
      if (repl["start_from"]) config.replication.start_from = repl["start_from"].as<std::string>();
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

    // Parse API config
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
