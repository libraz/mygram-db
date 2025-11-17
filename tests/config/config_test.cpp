/**
 * @file config_test.cpp
 * @brief Unit tests for configuration parser
 */

#include "config/config.h"

#include <gtest/gtest.h>

#include <fstream>
#include <nlohmann/json.hpp>

#include "utils/memory_utils.h"

using namespace mygramdb::config;
using json = nlohmann::json;

/**
 * @brief Test loading valid configuration file
 */
TEST(ConfigTest, LoadValidConfig) {
  auto config_result = LoadConfig("test_config.yaml");
  ASSERT_TRUE(config_result) << "Failed to load config: " << config_result.error().to_string();
  Config config = *config_result;

  // MySQL config
  EXPECT_EQ(config.mysql.host, "127.0.0.1");
  EXPECT_EQ(config.mysql.port, 3306);
  EXPECT_EQ(config.mysql.user, "test_user");
  EXPECT_EQ(config.mysql.password, "test_pass");
  EXPECT_TRUE(config.mysql.use_gtid);
  EXPECT_EQ(config.mysql.binlog_format, "ROW");
  EXPECT_EQ(config.mysql.binlog_row_image, "FULL");
  EXPECT_EQ(config.mysql.connect_timeout_ms, 5000);
  EXPECT_EQ(config.mysql.read_timeout_ms, 7200000);
  EXPECT_EQ(config.mysql.write_timeout_ms, 7200000);

  // Tables
  ASSERT_EQ(config.tables.size(), 1);
  const auto& table = config.tables[0];
  EXPECT_EQ(table.name, "test_table");
  EXPECT_EQ(table.primary_key, "id");
  EXPECT_EQ(table.text_source.column, "content");
  EXPECT_EQ(table.ngram_size, 1);

  // Filters
  ASSERT_EQ(table.filters.size(), 2);
  EXPECT_EQ(table.filters[0].name, "status");
  EXPECT_EQ(table.filters[0].type, "int");
  EXPECT_TRUE(table.filters[0].dict_compress);
  EXPECT_TRUE(table.filters[0].bitmap_index);

  EXPECT_EQ(table.filters[1].name, "created_at");
  EXPECT_EQ(table.filters[1].type, "datetime");
  EXPECT_EQ(table.filters[1].bucket, "hour");

  // Posting config
  EXPECT_EQ(table.posting.block_size, 64);
  EXPECT_EQ(table.posting.freq_bits, 0);
  EXPECT_EQ(table.posting.use_roaring, "auto");

  // Build config
  EXPECT_EQ(config.build.mode, "select_snapshot");
  EXPECT_EQ(config.build.batch_size, 1000);
  EXPECT_EQ(config.build.parallelism, 1);
  EXPECT_EQ(config.build.throttle_ms, 0);

  // Replication config
  EXPECT_TRUE(config.replication.enable);
  EXPECT_EQ(config.replication.server_id, 100U);
  EXPECT_EQ(config.replication.start_from, "snapshot");

  // Memory config
  EXPECT_EQ(config.memory.hard_limit_mb, 1024);
  EXPECT_EQ(config.memory.soft_target_mb, 512);
  EXPECT_EQ(config.memory.arena_chunk_mb, 32);
  EXPECT_DOUBLE_EQ(config.memory.roaring_threshold, 0.2);
  EXPECT_TRUE(config.memory.minute_epoch);
  EXPECT_TRUE(config.memory.normalize.nfkc);
  EXPECT_EQ(config.memory.normalize.width, "narrow");
  EXPECT_FALSE(config.memory.normalize.lower);

  // Dump config
  EXPECT_EQ(config.dump.dir, "/tmp/test_dumps");
  EXPECT_EQ(config.dump.interval_sec, 300);
  EXPECT_EQ(config.dump.retain, 2);

  // API config
  EXPECT_EQ(config.api.tcp.bind, "127.0.0.1");
  EXPECT_EQ(config.api.tcp.port, 11016);
  EXPECT_FALSE(config.api.http.enable);
  EXPECT_EQ(config.api.http.bind, "127.0.0.1");
  EXPECT_EQ(config.api.http.port, 8080);

  // Network config
  ASSERT_EQ(config.network.allow_cidrs.size(), 1);
  EXPECT_EQ(config.network.allow_cidrs[0], "127.0.0.1/32");

  // Logging config
  EXPECT_EQ(config.logging.level, "debug");
  EXPECT_FALSE(config.logging.json);
}

/**
 * @brief Test loading non-existent file
 */
TEST(ConfigTest, LoadNonExistentFile) {
  auto result = LoadConfig("non_existent.yaml");
  EXPECT_FALSE(result);
}

/**
 * @brief Test loading invalid YAML
 */
TEST(ConfigTest, LoadInvalidYAML) {
  // Create invalid YAML file
  std::ofstream f("invalid.yaml");
  f << "invalid: yaml: content: [\n";
  f.close();

  auto result = LoadConfig("invalid.yaml");
  EXPECT_FALSE(result);
}

/**
 * @brief Test default values
 */
TEST(ConfigTest, DefaultValues) {
  // Create minimal config
  std::ofstream f("minimal.yaml");
  f << "mysql:\n";
  f << "  host: localhost\n";
  f << "  user: root\n";
  f << "  password: pass\n";
  f << "  database: testdb\n";
  f << "tables:\n";
  f << "  - name: test\n";
  f << "    text_source:\n";
  f << "      column: text\n";
  f.close();

  auto config_result = LoadConfig("minimal.yaml");
  ASSERT_TRUE(config_result) << "Failed to load config: " << config_result.error().to_string();
  Config config = *config_result;

  // Check defaults
  EXPECT_EQ(config.mysql.port, 3306);
  EXPECT_TRUE(config.mysql.use_gtid);
  EXPECT_EQ(config.mysql.connect_timeout_ms, 3000);   // Default: 3 seconds
  EXPECT_EQ(config.mysql.read_timeout_ms, 3600000);   // Default: 1 hour
  EXPECT_EQ(config.mysql.write_timeout_ms, 3600000);  // Default: 1 hour
  EXPECT_EQ(config.build.batch_size, 5000);
  EXPECT_EQ(config.memory.hard_limit_mb, 8192);
  EXPECT_EQ(config.api.tcp.port, 11016);
  EXPECT_FALSE(config.api.http.enable);
}

/**
 * @brief Test table with concatenated text source
 */
TEST(ConfigTest, ConcatenatedTextSource) {
  std::ofstream f("concat.yaml");
  f << "mysql:\n";
  f << "  host: localhost\n";
  f << "  user: root\n";
  f << "  password: pass\n";
  f << "  database: testdb\n";
  f << "tables:\n";
  f << "  - name: articles\n";
  f << "    text_source:\n";
  f << "      concat: [\"title\", \"body\"]\n";
  f << "      delimiter: \" | \"\n";
  f.close();

  auto config_result = LoadConfig("concat.yaml");
  ASSERT_TRUE(config_result) << "Failed to load config: " << config_result.error().to_string();
  Config config = *config_result;

  ASSERT_EQ(config.tables.size(), 1);
  const auto& table = config.tables[0];
  EXPECT_TRUE(table.text_source.column.empty());
  ASSERT_EQ(table.text_source.concat.size(), 2);
  EXPECT_EQ(table.text_source.concat[0], "title");
  EXPECT_EQ(table.text_source.concat[1], "body");
  EXPECT_EQ(table.text_source.delimiter, " | ");
}

/**
 * @brief Test invalid server_id (0 with replication enabled)
 */
TEST(ConfigTest, InvalidServerId) {
  std::ofstream f("invalid_server_id.yaml");
  f << "mysql:\n";
  f << "  host: localhost\n";
  f << "  user: root\n";
  f << "  password: pass\n";
  f << "  database: testdb\n";
  f << "tables:\n";
  f << "  - name: test\n";
  f << "    text_source:\n";
  f << "      column: content\n";
  f << "replication:\n";
  f << "  enable: true\n";
  f << "  start_from: snapshot\n";
  f.close();

  auto result = LoadConfig("invalid_server_id.yaml");
  EXPECT_FALSE(result);
  if (!result) {
    std::string error_msg = result.error().message();
    // Schema validation happens first, so check for schema error or server_id error
    bool valid_error = (error_msg.find("replication.server_id must be set to a non-zero value") != std::string::npos) ||
                       (error_msg.find("server_id") != std::string::npos) ||
                       (error_msg.find("required property") != std::string::npos);
    EXPECT_TRUE(valid_error) << "Actual error: " << error_msg;
  }
}

/**
 * @brief Test invalid GTID format
 */
TEST(ConfigTest, InvalidGTIDFormat) {
  std::ofstream f("invalid_gtid.yaml");
  f << "mysql:\n";
  f << "  host: localhost\n";
  f << "  user: root\n";
  f << "  password: pass\n";
  f << "  database: testdb\n";
  f << "tables:\n";
  f << "  - name: test\n";
  f << "    text_source:\n";
  f << "      column: content\n";
  f << "replication:\n";
  f << "  enable: true\n";
  f << "  server_id: 100\n";
  f << "  start_from: gtid=invalid-format\n";
  f.close();

  auto result = LoadConfig("invalid_gtid.yaml");
  EXPECT_FALSE(result);
  if (!result) {
    std::string error_msg = result.error().message();
    EXPECT_TRUE(error_msg.find("Invalid GTID format") != std::string::npos);
  }
}

/**
 * @brief Test invalid start_from value
 */
TEST(ConfigTest, InvalidStartFrom) {
  std::ofstream f("invalid_start_from.yaml");
  f << "mysql:\n";
  f << "  host: localhost\n";
  f << "  user: root\n";
  f << "  password: pass\n";
  f << "  database: testdb\n";
  f << "tables:\n";
  f << "  - name: test\n";
  f << "    text_source:\n";
  f << "      column: content\n";
  f << "replication:\n";
  f << "  enable: true\n";
  f << "  server_id: 100\n";
  f << "  start_from: invalid_option\n";
  f.close();

  auto result = LoadConfig("invalid_start_from.yaml");
  EXPECT_FALSE(result);
  if (!result) {
    std::string error_msg = result.error().message();
    EXPECT_TRUE(error_msg.find("Replication configuration error") != std::string::npos);
    EXPECT_TRUE(error_msg.find("Invalid start_from value") != std::string::npos);
  }
}

/**
 * @brief Test loading valid JSON configuration file
 */
TEST(ConfigTest, LoadValidJSONConfig) {
  // Create JSON config file
  json j = {{"mysql",
             {{"host", "127.0.0.1"},
              {"port", 3306},
              {"user", "json_user"},
              {"password", "json_pass"},
              {"database", "json_db"},
              {"use_gtid", true},
              {"binlog_format", "ROW"},
              {"binlog_row_image", "FULL"},
              {"connect_timeout_ms", 5000}}},
            {"tables",
             {{{"name", "json_table"},
               {"primary_key", "id"},
               {"text_source", {{"column", "content"}}},
               {"ngram_size", 2},
               {"posting", {{"block_size", 256}, {"freq_bits", 8}, {"use_roaring", "always"}}}}}},
            {"replication", {{"enable", true}, {"server_id", 200}, {"start_from", "latest"}}},
            {"logging", {{"level", "info"}, {"json", true}}}};

  std::ofstream f("test_config.json");
  f << j.dump(2);
  f.close();

  auto config_result = LoadConfig("test_config.json");
  ASSERT_TRUE(config_result) << "Failed to load config: " << config_result.error().to_string();
  Config config = *config_result;

  // MySQL config
  EXPECT_EQ(config.mysql.host, "127.0.0.1");
  EXPECT_EQ(config.mysql.port, 3306);
  EXPECT_EQ(config.mysql.user, "json_user");
  EXPECT_EQ(config.mysql.password, "json_pass");
  EXPECT_EQ(config.mysql.database, "json_db");

  // Tables
  ASSERT_EQ(config.tables.size(), 1);
  const auto& table = config.tables[0];
  EXPECT_EQ(table.name, "json_table");
  EXPECT_EQ(table.primary_key, "id");
  EXPECT_EQ(table.text_source.column, "content");
  EXPECT_EQ(table.ngram_size, 2);

  // Posting config
  EXPECT_EQ(table.posting.block_size, 256);
  EXPECT_EQ(table.posting.freq_bits, 8);
  EXPECT_EQ(table.posting.use_roaring, "always");

  // Replication config
  EXPECT_TRUE(config.replication.enable);
  EXPECT_EQ(config.replication.server_id, 200U);
  EXPECT_EQ(config.replication.start_from, "latest");

  // Logging config
  EXPECT_EQ(config.logging.level, "info");
  EXPECT_TRUE(config.logging.json);
}

/**
 * @brief Test loading JSON config with built-in schema validation
 */
TEST(ConfigTest, LoadJSONConfigWithSchemaValidation) {
  // Create valid JSON config
  json config_json = {{"mysql", {{"user", "test_user"}, {"password", "test_pass"}, {"database", "test_db"}}},
                      {"tables", {{{"name", "test_table"}, {"text_source", {{"column", "content"}}}}}},
                      {"replication", {{"server_id", 100}}}};

  std::ofstream f("valid_config.json");
  f << config_json.dump(2);
  f.close();

  // Should load successfully with built-in schema validation
  auto config_result = LoadConfig("valid_config.json");
  ASSERT_TRUE(config_result) << "Failed to load config: " << config_result.error().to_string();
  Config config = *config_result;
  EXPECT_EQ(config.mysql.user, "test_user");
  EXPECT_EQ(config.tables.size(), 1);
}

/**
 * @brief Test JSON config with invalid data against built-in schema
 */
TEST(ConfigTest, LoadInvalidJSONWithSchemaValidation) {
  // Create invalid JSON config (missing required "user" field)
  json config_json = {{"mysql", {{"password", "test_pass"}, {"database", "test_db"}}},
                      {"tables", {{{"name", "test_table"}, {"text_source", {{"column", "content"}}}}}}};

  std::ofstream f("invalid_config.json");
  f << config_json.dump(2);
  f.close();

  // Should throw validation error (built-in schema requires "user" field)
  auto result = LoadConfig("invalid_config.json");
  EXPECT_FALSE(result);
}

/**
 * @brief Test auto-detection of file format
 */
TEST(ConfigTest, AutoDetectFormat) {
  // Create YAML file without .yaml extension
  std::ofstream yaml_file("config_no_ext");
  yaml_file << "mysql:\n";
  yaml_file << "  host: localhost\n";
  yaml_file << "  user: root\n";
  yaml_file << "  password: pass\n";
  yaml_file << "  database: testdb\n";
  yaml_file << "tables:\n";
  yaml_file << "  - name: test\n";
  yaml_file << "    text_source:\n";
  yaml_file << "      column: text\n";
  yaml_file.close();

  // Should auto-detect as YAML and load successfully
  auto config_result = LoadConfig("config_no_ext");
  ASSERT_TRUE(config_result) << "Failed to load config: " << config_result.error().to_string();
  Config config = *config_result;
  EXPECT_EQ(config.mysql.user, "root");
}

/**
 * @brief Test loading invalid JSON
 */
TEST(ConfigTest, LoadInvalidJSON) {
  // Create invalid JSON file
  std::ofstream f("invalid.json");
  f << "{\"mysql\": {\"user\": \"test\",}}\n";  // trailing comma is invalid
  f.close();

  auto result = LoadConfig("invalid.json");
  EXPECT_FALSE(result);
}

/**
 * @brief Test JSON config with unknown keys (should fail with schema validation)
 */
TEST(ConfigTest, JSONConfigWithUnknownKeys) {
  // Create JSON config with unknown field
  json config_json = {{"mysql",
                       {{"user", "test_user"},
                        {"password", "test_pass"},
                        {"database", "test_db"},
                        {"unknown_field", "should_be_rejected"}}},
                      {"tables", {{{"name", "test_table"}, {"text_source", {{"column", "content"}}}}}},
                      {"replication", {{"server_id", 100}}}};

  std::ofstream f("unknown_keys.json");
  f << config_json.dump(2);
  f.close();

  // With built-in schema validation, unknown keys should be rejected
  auto result = LoadConfig("unknown_keys.json");
  EXPECT_FALSE(result);
}

/**
 * @brief Test LoadConfigYaml legacy function
 */
TEST(ConfigTest, LoadConfigYamlLegacy) {
  std::ofstream f("legacy.yaml");
  f << "mysql:\n";
  f << "  host: localhost\n";
  f << "  user: legacy_user\n";
  f << "  password: pass\n";
  f << "  database: testdb\n";
  f << "tables:\n";
  f << "  - name: test\n";
  f << "    text_source:\n";
  f << "      column: text\n";
  f.close();

  auto config_result = LoadConfigYaml("legacy.yaml");
  ASSERT_TRUE(config_result) << "Failed to load config: " << config_result.error().to_string();
  Config config = *config_result;
  EXPECT_EQ(config.mysql.user, "legacy_user");
}

/**
 * @brief Test LoadConfigJson function
 */
TEST(ConfigTest, LoadConfigJsonFunction) {
  json config_json = {{"mysql", {{"user", "json_func_user"}, {"password", "pass"}, {"database", "db"}}},
                      {"tables", {{{"name", "test"}, {"text_source", {{"column", "content"}}}}}},
                      {"replication", {{"server_id", 300}}}};

  std::ofstream f("func_test.json");
  f << config_json.dump(2);
  f.close();

  auto config_result = LoadConfigJson("func_test.json");
  ASSERT_TRUE(config_result) << "Failed to load config: " << config_result.error().to_string();
  Config config = *config_result;
  EXPECT_EQ(config.mysql.user, "json_func_user");
  EXPECT_EQ(config.replication.server_id, 300U);
}

/**
 * @brief Test MySQL SSL/TLS configuration defaults
 */
TEST(ConfigTest, MysqlSslDefaults) {
  std::ofstream f("ssl_defaults.yaml");
  f << "mysql:\n";
  f << "  host: localhost\n";
  f << "  user: root\n";
  f << "  password: pass\n";
  f << "  database: testdb\n";
  f << "tables:\n";
  f << "  - name: test\n";
  f << "    text_source:\n";
  f << "      column: text\n";
  f.close();

  auto config_result = LoadConfig("ssl_defaults.yaml");
  ASSERT_TRUE(config_result) << "Failed to load config: " << config_result.error().to_string();
  Config config = *config_result;

  // Check SSL defaults
  EXPECT_FALSE(config.mysql.ssl_enable);
  EXPECT_TRUE(config.mysql.ssl_ca.empty());
  EXPECT_TRUE(config.mysql.ssl_cert.empty());
  EXPECT_TRUE(config.mysql.ssl_key.empty());
  EXPECT_TRUE(config.mysql.ssl_verify_server_cert);
}

/**
 * @brief Test MySQL SSL/TLS configuration with all options
 */
TEST(ConfigTest, MysqlSslConfiguration) {
  std::ofstream f("ssl_config.yaml");
  f << "mysql:\n";
  f << "  host: localhost\n";
  f << "  user: root\n";
  f << "  password: pass\n";
  f << "  database: testdb\n";
  f << "  ssl_enable: true\n";
  f << "  ssl_ca: /path/to/ca-cert.pem\n";
  f << "  ssl_cert: /path/to/client-cert.pem\n";
  f << "  ssl_key: /path/to/client-key.pem\n";
  f << "  ssl_verify_server_cert: false\n";
  f << "tables:\n";
  f << "  - name: test\n";
  f << "    text_source:\n";
  f << "      column: text\n";
  f.close();

  auto config_result = LoadConfig("ssl_config.yaml");
  ASSERT_TRUE(config_result) << "Failed to load config: " << config_result.error().to_string();
  Config config = *config_result;

  EXPECT_TRUE(config.mysql.ssl_enable);
  EXPECT_EQ(config.mysql.ssl_ca, "/path/to/ca-cert.pem");
  EXPECT_EQ(config.mysql.ssl_cert, "/path/to/client-cert.pem");
  EXPECT_EQ(config.mysql.ssl_key, "/path/to/client-key.pem");
  EXPECT_FALSE(config.mysql.ssl_verify_server_cert);
}

/**
 * @brief Test MySQL SSL/TLS with partial configuration
 */
TEST(ConfigTest, MysqlSslPartialConfiguration) {
  std::ofstream f("ssl_partial.yaml");
  f << "mysql:\n";
  f << "  host: localhost\n";
  f << "  user: root\n";
  f << "  password: pass\n";
  f << "  database: testdb\n";
  f << "  ssl_enable: true\n";
  f << "  ssl_ca: /path/to/ca-cert.pem\n";
  f << "tables:\n";
  f << "  - name: test\n";
  f << "    text_source:\n";
  f << "      column: text\n";
  f.close();

  auto config_result = LoadConfig("ssl_partial.yaml");
  ASSERT_TRUE(config_result) << "Failed to load config: " << config_result.error().to_string();
  Config config = *config_result;

  EXPECT_TRUE(config.mysql.ssl_enable);
  EXPECT_EQ(config.mysql.ssl_ca, "/path/to/ca-cert.pem");
  EXPECT_TRUE(config.mysql.ssl_cert.empty());
  EXPECT_TRUE(config.mysql.ssl_key.empty());
  EXPECT_TRUE(config.mysql.ssl_verify_server_cert);  // default
}

/**
 * @brief Test cache memory exceeding 50% of physical memory
 */
TEST(ConfigTest, CacheMemoryExceedsPhysicalMemoryLimit) {
  auto system_info = mygramdb::utils::GetSystemMemoryInfo();
  if (!system_info) {
    GTEST_SKIP() << "Cannot get system memory info, skipping test";
  }

  // Calculate more than 50% of physical memory in MB
  uint64_t physical_memory_mb = system_info->total_physical_bytes / 1024 / 1024;
  uint64_t excessive_cache_mb = static_cast<uint64_t>(physical_memory_mb * 0.6);  // 60% > 50%

  std::ofstream f("cache_excessive.yaml");
  f << "mysql:\n";
  f << "  host: localhost\n";
  f << "  user: root\n";
  f << "  password: pass\n";
  f << "  database: testdb\n";
  f << "tables:\n";
  f << "  - name: test\n";
  f << "    text_source:\n";
  f << "      column: text\n";
  f << "cache:\n";
  f << "  enabled: true\n";
  f << "  max_memory_mb: " << excessive_cache_mb << "\n";
  f.close();

  auto result = LoadConfig("cache_excessive.yaml");
  EXPECT_FALSE(result);
  if (!result) {
    std::string error_msg = result.error().message();
    EXPECT_TRUE(error_msg.find("Cache configuration error") != std::string::npos);
    EXPECT_TRUE(error_msg.find("exceeds safe limit") != std::string::npos);
  }
}

/**
 * @brief Test cache memory within 50% of physical memory
 */
TEST(ConfigTest, CacheMemoryWithinPhysicalMemoryLimit) {
  auto system_info = mygramdb::utils::GetSystemMemoryInfo();
  if (!system_info) {
    GTEST_SKIP() << "Cannot get system memory info, skipping test";
  }

  // Calculate less than 50% of physical memory in MB
  uint64_t physical_memory_mb = system_info->total_physical_bytes / 1024 / 1024;
  uint64_t safe_cache_mb = static_cast<uint64_t>(physical_memory_mb * 0.3);  // 30% < 50%

  std::ofstream f("cache_safe.yaml");
  f << "mysql:\n";
  f << "  host: localhost\n";
  f << "  user: root\n";
  f << "  password: pass\n";
  f << "  database: testdb\n";
  f << "tables:\n";
  f << "  - name: test\n";
  f << "    text_source:\n";
  f << "      column: text\n";
  f << "cache:\n";
  f << "  enabled: true\n";
  f << "  max_memory_mb: " << safe_cache_mb << "\n";
  f.close();

  // Should load successfully
  auto config_result = LoadConfig("cache_safe.yaml");
  ASSERT_TRUE(config_result) << "Failed to load config: " << config_result.error().to_string();
  Config config = *config_result;
  EXPECT_TRUE(config.cache.enabled);
  EXPECT_EQ(config.cache.max_memory_bytes, safe_cache_mb * 1024 * 1024);
}

/**
 * @brief Test cache disabled does not trigger memory validation
 */
TEST(ConfigTest, CacheDisabledNoMemoryValidation) {
  auto system_info = mygramdb::utils::GetSystemMemoryInfo();
  if (!system_info) {
    GTEST_SKIP() << "Cannot get system memory info, skipping test";
  }

  // Even with excessive memory setting, disabled cache should not fail
  uint64_t physical_memory_mb = system_info->total_physical_bytes / 1024 / 1024;
  uint64_t excessive_cache_mb = static_cast<uint64_t>(physical_memory_mb * 0.9);  // 90% > 50%

  std::ofstream f("cache_disabled.yaml");
  f << "mysql:\n";
  f << "  host: localhost\n";
  f << "  user: root\n";
  f << "  password: pass\n";
  f << "  database: testdb\n";
  f << "tables:\n";
  f << "  - name: test\n";
  f << "    text_source:\n";
  f << "      column: text\n";
  f << "cache:\n";
  f << "  enabled: false\n";
  f << "  max_memory_mb: " << excessive_cache_mb << "\n";
  f.close();

  // Should load successfully because cache is disabled
  auto config_result = LoadConfig("cache_disabled.yaml");
  ASSERT_TRUE(config_result) << "Failed to load config: " << config_result.error().to_string();
  Config config = *config_result;
  EXPECT_FALSE(config.cache.enabled);
}
