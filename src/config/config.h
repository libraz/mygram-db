/**
 * @file config.h
 * @brief Configuration structures and YAML parser
 */

#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "utils/error.h"
#include "utils/expected.h"

namespace mygramdb::config {

// Default values for configuration
namespace defaults {

// MySQL connection defaults
constexpr int kMysqlPort = 3306;
constexpr int kMysqlConnectTimeoutMs = 3000;
constexpr int kMysqlReadTimeoutMs = 3600000;   // 1 hour (for long-running binlog connections)
constexpr int kMysqlWriteTimeoutMs = 3600000;  // 1 hour

// Posting list defaults
constexpr int kPostingBlockSize = 128;

// Build defaults
constexpr int kBuildBatchSize = 5000;

// Replication defaults
constexpr int kReplicationQueueSize = 10000;
constexpr int kReconnectBackoffMinMs = 500;
constexpr int kReconnectBackoffMaxMs = 10000;

// Memory defaults
constexpr int kMemoryHardLimitMb = 8192;
constexpr int kMemorySoftTargetMb = 4096;
constexpr int kMemoryArenaChunkMb = 64;
constexpr double kRoaringThreshold = 0.18;

// Dump defaults
constexpr int kDumpIntervalSec = 0;  // 0 = disabled by default (set to 7200 for 120-minute intervals)
constexpr const char* kDumpDefaultFilename = "mygramdb.dmp";

// API defaults
constexpr int kTcpPort = 11016;
constexpr int kHttpPort = 8080;

// Query defaults
constexpr int kDefaultLimit = 100;
constexpr int kMinLimit = 5;
constexpr int kMaxLimit = 1000;
constexpr int kDefaultQueryLengthLimit = 128;

}  // namespace defaults

/**
 * @brief MySQL connection configuration
 */
struct MysqlConfig {
  std::string host = "127.0.0.1";
  int port = defaults::kMysqlPort;
  std::string user;
  std::string password;
  std::string database;
  bool use_gtid = true;
  std::string binlog_format = "ROW";
  std::string binlog_row_image = "FULL";
  int connect_timeout_ms = defaults::kMysqlConnectTimeoutMs;
  int read_timeout_ms = defaults::kMysqlReadTimeoutMs;
  int write_timeout_ms = defaults::kMysqlWriteTimeoutMs;
  // SSL/TLS settings
  bool ssl_enable = false;
  std::string ssl_ca;
  std::string ssl_cert;
  std::string ssl_key;
  bool ssl_verify_server_cert = true;
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
  std::string name;           // Column name
  std::string type;           // Type options (same as FilterConfig)
  std::string op;             // Operator: "=", "!=", "<", ">", "<=", ">=", "IS NULL", "IS NOT NULL"
  std::string value;          // Value (empty for IS NULL/IS NOT NULL operators)
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
  std::string column;               // Single column
  std::vector<std::string> concat;  // Multiple columns to concatenate
  std::string delimiter = " ";
};

/**
 * @brief Posting list configuration
 */
struct PostingConfig {
  int block_size = defaults::kPostingBlockSize;
  int freq_bits = 0;                 // 0=boolean, 4, or 8
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
  std::vector<FilterConfig> filters;                   // Optional filters for search-time filtering
  int ngram_size = 2;                                  // N-gram size for ASCII/alphanumeric characters
  int kanji_ngram_size = 0;  // N-gram size for CJK (kanji/kana) characters (0 = use ngram_size)
  PostingConfig posting;
};

/**
 * @brief Build configuration
 */
struct BuildConfig {
  std::string mode = "select_snapshot";
  int batch_size = defaults::kBuildBatchSize;
  int parallelism = 2;
  int throttle_ms = 0;
};

/**
 * @brief Replication configuration
 */
struct ReplicationConfig {
  bool enable = true;
  bool auto_initial_snapshot = false;   // Automatically build snapshots on server startup (default: false)
  uint32_t server_id = 0;               // MySQL server ID for replication (must be unique, 0 = disabled)
  std::string start_from = "snapshot";  // "snapshot", "gtid=<UUID:txn>", "latest"
  int queue_size = defaults::kReplicationQueueSize;
  int reconnect_backoff_min_ms = defaults::kReconnectBackoffMinMs;
  int reconnect_backoff_max_ms = defaults::kReconnectBackoffMaxMs;
};

/**
 * @brief Memory configuration
 */
struct MemoryConfig {
  int hard_limit_mb = defaults::kMemoryHardLimitMb;
  int soft_target_mb = defaults::kMemorySoftTargetMb;
  int arena_chunk_mb = defaults::kMemoryArenaChunkMb;
  double roaring_threshold = defaults::kRoaringThreshold;
  bool minute_epoch = true;

  struct {
    bool nfkc = true;
    std::string width = "narrow";
    bool lower = false;
  } normalize;
};

/**
 * @brief Dump configuration
 */
struct DumpConfig {
  std::string dir = "/var/lib/mygramdb/dumps";
  std::string default_filename = defaults::kDumpDefaultFilename;
  int interval_sec = defaults::kDumpIntervalSec;
  int retain = 3;
};

/**
 * @brief API configuration
 */
struct ApiConfig {
  // Rate limiting defaults
  static constexpr int kDefaultRateLimitCapacity = 100;      ///< Default burst size
  static constexpr int kDefaultRateLimitRefillRate = 10;     ///< Default tokens per second
  static constexpr int kDefaultRateLimitMaxClients = 10000;  ///< Default max tracked clients

  struct {
    std::string bind = "127.0.0.1";
    int port = defaults::kTcpPort;
  } tcp;

  struct {
    bool enable = false;  // Disabled by default (set to true to enable HTTP API)
    std::string bind = "127.0.0.1";
    int port = defaults::kHttpPort;
    bool enable_cors = false;
    std::string cors_allow_origin;
  } http;

  /**
   * @brief Default LIMIT for SEARCH queries when not explicitly specified
   * Valid range: 5-1000 (enforced by validation)
   */
  int default_limit = defaults::kDefaultLimit;

  /**
   * @brief Maximum length (in characters) allowed for query expressions (search text + conditions)
   */
  int max_query_length = defaults::kDefaultQueryLengthLimit;

  /**
   * @brief Rate limiting configuration (token bucket algorithm)
   */
  struct {
    bool enable = false;                            ///< Enable rate limiting (default: false)
    int capacity = kDefaultRateLimitCapacity;       ///< Maximum tokens per client (burst size)
    int refill_rate = kDefaultRateLimitRefillRate;  ///< Tokens added per second per client
    int max_clients = kDefaultRateLimitMaxClients;  ///< Maximum number of tracked clients
  } rate_limiting;
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
  std::string file;  ///< Log file path (empty = stdout, path = file output)
};

/**
 * @brief Query cache configuration
 */
struct CacheConfig {
  bool enabled = true;                          ///< Enable/disable cache (default: true)
  size_t max_memory_bytes = 32 * 1024 * 1024;   ///< Maximum cache memory in bytes (default: 32MB)  // NOLINT
  double min_query_cost_ms = 10.0;              ///< Minimum query cost to cache (default: 10ms)  // NOLINT
  int ttl_seconds = 3600;                       ///< Cache entry TTL (default: 1 hour, 0 = no TTL)  // NOLINT
  std::string invalidation_strategy = "ngram";  ///< Invalidation strategy: "ngram", "table"

  // Advanced tuning
  bool compression_enabled = true;  ///< Enable LZ4 compression (default: true)
  int eviction_batch_size = 10;     ///< Number of entries to evict at once (default: 10)  // NOLINT

  // Invalidation queue settings
  struct {
    int batch_size = 1000;   ///< Process after N unique (table, ngram) pairs  // NOLINT
    int max_delay_ms = 100;  ///< Max delay before processing (ms)  // NOLINT
  } invalidation;
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
  DumpConfig dump;
  ApiConfig api;
  NetworkConfig network;
  LoggingConfig logging;
  CacheConfig cache;
};

/**
 * @brief Load configuration from YAML or JSON file
 *
 * Automatically detects file format based on extension (.yaml, .yml, .json).
 * Validates configuration against JSON Schema if available.
 *
 * @param path Path to configuration file (YAML or JSON)
 * @param schema_path Optional path to JSON Schema file for validation
 * @return Expected<Config, Error> with configuration or error
 */
mygram::utils::Expected<Config, mygram::utils::Error> LoadConfig(const std::string& path,
                                                                 const std::string& schema_path = "");

/**
 * @brief Load configuration from YAML file (legacy compatibility)
 *
 * @param path Path to YAML configuration file
 * @return Expected<Config, Error> with configuration or error
 * @deprecated Use LoadConfig() which auto-detects format
 */
mygram::utils::Expected<Config, mygram::utils::Error> LoadConfigYaml(const std::string& path);

/**
 * @brief Load configuration from JSON file
 *
 * @param path Path to JSON configuration file
 * @param schema_path Optional path to JSON Schema file for validation
 * @return Expected<Config, Error> with configuration or error
 */
mygram::utils::Expected<Config, mygram::utils::Error> LoadConfigJson(const std::string& path,
                                                                     const std::string& schema_path = "");

/**
 * @brief Validate JSON configuration against schema
 *
 * @param config_json_str JSON configuration string
 * @param schema_json_str JSON Schema string
 * @return Expected<void, Error> with success or validation error
 */
mygram::utils::Expected<void, mygram::utils::Error> ValidateConfigJson(const std::string& config_json_str,
                                                                       const std::string& schema_json_str);

}  // namespace mygramdb::config
