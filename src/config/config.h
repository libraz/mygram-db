/**
 * @file config.h
 * @brief Configuration structures and YAML parser
 */

#pragma once

#include <string>
#include <vector>

namespace mygramdb {
namespace config {

/**
 * @brief MySQL connection configuration
 */
struct MysqlConfig {
  std::string host = "127.0.0.1";
  int port = 3306;
  std::string user;
  std::string password;
  std::string database;
  bool use_gtid = true;
  std::string binlog_format = "ROW";
  std::string binlog_row_image = "FULL";
  int connect_timeout_ms = 3000;
};

/**
 * @brief Required filter configuration (data existence condition)
 *
 * Required filters define conditions that data must satisfy to be indexed.
 * Data that does not match these conditions will not be indexed at all.
 * During binlog replication, data that transitions out of these conditions
 * will be removed from the index, and data that transitions into these
 * conditions will be added to the index.
 */
struct RequiredFilterConfig {
  std::string name;  // Column name
  std::string type;  // Type options (same as FilterConfig)
  std::string op;    // Operator: "=", "!=", "<", ">", "<=", ">=", "IS NULL", "IS NOT NULL"
  std::string value; // Value (empty for IS NULL/IS NOT NULL operators)
  bool bitmap_index = false;  // Enable bitmap index for search-time filtering
};

/**
 * @brief Optional filter configuration (search-time filtering)
 *
 * Optional filters are used for filtering during searches.
 * They do not affect which data is indexed.
 */
struct FilterConfig {
  std::string name;
  std::string type;  // Type options:
                     // Integer: "tinyint", "tinyint_unsigned", "smallint", "smallint_unsigned",
                     //          "int", "int_unsigned", "bigint" (or legacy "int")
                     // Float: "float", "double"
                     // String: "string", "varchar", "text"
                     // Date: "datetime", "date", "timestamp"
  bool dict_compress = false;
  bool bitmap_index = false;
  std::string bucket;  // For datetime: "minute", "hour", "day"
};

/**
 * @brief Text source configuration
 */
struct TextSourceConfig {
  std::string column;  // Single column
  std::vector<std::string> concat;  // Multiple columns to concatenate
  std::string delimiter = " ";
};

/**
 * @brief Posting list configuration
 */
struct PostingConfig {
  int block_size = 128;
  int freq_bits = 0;  // 0=boolean, 4, or 8
  std::string use_roaring = "auto";  // "auto", "always", "never"
};

/**
 * @brief Table configuration
 */
struct TableConfig {
  std::string name;
  std::string primary_key = "id";
  TextSourceConfig text_source;
  std::vector<RequiredFilterConfig> required_filters;  // Data existence conditions
  std::vector<FilterConfig> filters;  // Optional filters for search-time filtering
  int ngram_size = 2;              // N-gram size for ASCII/alphanumeric characters
  int kanji_ngram_size = 0;        // N-gram size for CJK (kanji/kana) characters (0 = use ngram_size)
  PostingConfig posting;
};

/**
 * @brief Build configuration
 */
struct BuildConfig {
  std::string mode = "select_snapshot";
  int batch_size = 5000;
  int parallelism = 2;
  int throttle_ms = 0;
};

/**
 * @brief Replication configuration
 */
struct ReplicationConfig {
  bool enable = true;
  uint32_t server_id = 0;  // MySQL server ID for replication (must be unique, 0 = disabled)
  std::string start_from = "snapshot";  // "snapshot", "gtid=<UUID:txn>", "latest", "state_file"
  std::string state_file = "./mygramdb_replication.state";  // File to persist current GTID position
  int queue_size = 10000;  // Queue size for binlog events
  int reconnect_backoff_min_ms = 500;
  int reconnect_backoff_max_ms = 10000;
};

/**
 * @brief Memory configuration
 */
struct MemoryConfig {
  int hard_limit_mb = 8192;
  int soft_target_mb = 4096;
  int arena_chunk_mb = 64;
  double roaring_threshold = 0.18;
  bool minute_epoch = true;

  struct {
    bool nfkc = true;
    std::string width = "narrow";
    bool lower = false;
  } normalize;
};

/**
 * @brief Snapshot configuration
 */
struct SnapshotConfig {
  std::string dir = "/var/lib/mygramdb/snapshots";
  int interval_sec = 600;
  int retain = 3;
};

/**
 * @brief API configuration
 */
struct ApiConfig {
  struct {
    std::string bind = "0.0.0.0";
    int port = 11311;
  } tcp;

  struct {
    bool enable = true;
    std::string bind = "127.0.0.1";
    int port = 8080;
  } http;
};

/**
 * @brief Network security configuration
 */
struct NetworkConfig {
  std::vector<std::string> allow_cidrs;
};

/**
 * @brief Logging configuration
 */
struct LoggingConfig {
  std::string level = "info";
  bool json = true;
};

/**
 * @brief Root configuration
 */
struct Config {
  MysqlConfig mysql;
  std::vector<TableConfig> tables;
  BuildConfig build;
  ReplicationConfig replication;
  MemoryConfig memory;
  SnapshotConfig snapshot;
  ApiConfig api;
  NetworkConfig network;
  LoggingConfig logging;
};

/**
 * @brief Load configuration from YAML or JSON file
 *
 * Automatically detects file format based on extension (.yaml, .yml, .json).
 * Validates configuration against JSON Schema if available.
 *
 * @param path Path to configuration file (YAML or JSON)
 * @param schema_path Optional path to JSON Schema file for validation
 * @return Configuration object
 * @throws std::runtime_error if file cannot be read, parsed, or validation fails
 */
Config LoadConfig(const std::string& path, const std::string& schema_path = "");

/**
 * @brief Load configuration from YAML file (legacy compatibility)
 *
 * @param path Path to YAML configuration file
 * @return Configuration object
 * @throws std::runtime_error if file cannot be read or parsed
 * @deprecated Use LoadConfig() which auto-detects format
 */
Config LoadConfigYaml(const std::string& path);

/**
 * @brief Load configuration from JSON file
 *
 * @param path Path to JSON configuration file
 * @param schema_path Optional path to JSON Schema file for validation
 * @return Configuration object
 * @throws std::runtime_error if file cannot be read, parsed, or validation fails
 */
Config LoadConfigJson(const std::string& path, const std::string& schema_path = "");

/**
 * @brief Validate JSON configuration against schema
 *
 * @param config_json JSON configuration object
 * @param schema_json JSON Schema object
 * @throws std::runtime_error if validation fails with detailed error message
 */
void ValidateConfigJson(const std::string& config_json_str, const std::string& schema_json_str);

}  // namespace config
}  // namespace mygramdb
