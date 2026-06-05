/**
 * @file config_test.cpp
 * @brief Unit tests for configuration parser
 */

#include "config/config.h"

#include <gtest/gtest.h>

#include <fstream>
#include <nlohmann/json.hpp>

#include "config/config_internal.h"
#include "utils/error.h"
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
  EXPECT_EQ(config.mysql.session_timeout_sec, 3600);  // Default value

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
  EXPECT_EQ(config.logging.format, "text");
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
  EXPECT_EQ(config.memory.normalize.width, "narrow");
  EXPECT_FALSE(config.memory.normalize.lower);
  EXPECT_EQ(config.api.tcp.port, 11016);
  EXPECT_FALSE(config.api.http.enable);
  // H-N8: HTTP read/write timeouts must default to kHttpTimeoutSec when not
  // specified in the YAML; they are no longer hardcoded inside HttpServer.
  EXPECT_EQ(config.api.http.read_timeout_sec, defaults::kHttpTimeoutSec);
  EXPECT_EQ(config.api.http.write_timeout_sec, defaults::kHttpTimeoutSec);
}

TEST(ConfigTest, SchemaExposedConfigKeysAreParsedFromYaml) {
  std::ofstream f("schema_exposed_keys.yaml");
  f << "mysql:\n";
  f << "  host: localhost\n";
  f << "  user: root\n";
  f << "  password: pass\n";
  f << "  database: testdb\n";
  f << "index:\n";
  f << "  ngram_size: 3\n";
  f << "tables:\n";
  f << "  - name: test\n";
  f << "    text_source:\n";
  f << "      column: text\n";
  f << "    synonyms:\n";
  f << "      enable: true\n";
  f << "      file: synonyms.tsv\n";
  f << "dump:\n";
  f << "  default_filename: custom.dmp\n";
  f << "api:\n";
  f << "  http:\n";
  f << "    enable: true\n";
  f << "    max_body_bytes: 1048576\n";
  f << "  unix_socket:\n";
  f << "    path: /tmp/mygramdb-test.sock\n";
  f << "  max_query_length: 0\n";
  f << "bm25:\n";
  f << "  enable: true\n";
  f << "  k1: 1.7\n";
  f << "  b: 0.6\n";
  f.close();

  auto config_result = LoadConfig("schema_exposed_keys.yaml");
  ASSERT_TRUE(config_result) << "Failed to load config: " << config_result.error().to_string();
  const Config& config = *config_result;

  ASSERT_EQ(config.tables.size(), 1);
  EXPECT_EQ(config.tables[0].ngram_size, 3);
  EXPECT_TRUE(config.tables[0].synonyms.enable);
  EXPECT_EQ(config.tables[0].synonyms.file, "synonyms.tsv");
  EXPECT_EQ(config.dump.default_filename, "custom.dmp");
  EXPECT_TRUE(config.api.http.enable);
  EXPECT_EQ(config.api.http.max_body_bytes, 1048576);
  EXPECT_EQ(config.api.unix_socket.path, "/tmp/mygramdb-test.sock");
  EXPECT_EQ(config.api.max_query_length, 0);
  EXPECT_TRUE(config.bm25.enable);
  EXPECT_DOUBLE_EQ(config.bm25.k1, 1.7);
  EXPECT_DOUBLE_EQ(config.bm25.b, 0.6);
}

TEST(ConfigTest, GlobalNgramSizeAlsoAppliesToImplicitKanjiNgramSize) {
  json config_json = {{"index", {{"ngram_size", 3}}},
                      {"tables", json::array({{{"name", "test"}, {"text_source", {{"column", "text"}}}}})}};

  auto config_result = internal::ParseConfigFromJson(config_json);
  ASSERT_TRUE(config_result) << "Failed to load config: " << config_result.error().to_string();
  ASSERT_EQ(config_result->tables.size(), 1);
  EXPECT_EQ(config_result->tables[0].ngram_size, 3);
  EXPECT_EQ(config_result->tables[0].kanji_ngram_size, 3);
}

TEST(ConfigTest, ExplicitKanjiNgramSizeOverridesGlobalNgramSize) {
  json config_json = {
      {"index", {{"ngram_size", 3}}},
      {"tables", json::array({{{"name", "test"}, {"kanji_ngram_size", 1}, {"text_source", {{"column", "text"}}}}})}};

  auto config_result = internal::ParseConfigFromJson(config_json);
  ASSERT_TRUE(config_result) << "Failed to load config: " << config_result.error().to_string();
  ASSERT_EQ(config_result->tables.size(), 1);
  EXPECT_EQ(config_result->tables[0].ngram_size, 3);
  EXPECT_EQ(config_result->tables[0].kanji_ngram_size, 1);
}

/**
 * @brief Regression test for H-N8: api.http.read_timeout_sec /
 *        write_timeout_sec must propagate from YAML into Config.
 *
 * Pre-fix the fields did not exist on `ApiConfig::http`, so the corresponding
 * YAML keys were silently dropped and HttpServer always ran with the
 * hardcoded 5-second defaults. This test pins the wiring: an operator that
 * sets these keys must see them on the parsed Config struct.
 */
TEST(ConfigTest, HttpTimeoutsParsedFromYaml) {
  std::ofstream f("http_timeout.yaml");
  f << "mysql:\n";
  f << "  host: localhost\n";
  f << "  user: root\n";
  f << "  password: pass\n";
  f << "  database: testdb\n";
  f << "tables:\n";
  f << "  - name: test\n";
  f << "    text_source:\n";
  f << "      column: text\n";
  f << "api:\n";
  f << "  http:\n";
  f << "    enable: true\n";
  f << "    read_timeout_sec: 30\n";
  f << "    write_timeout_sec: 45\n";
  f.close();

  auto config_result = LoadConfig("http_timeout.yaml");
  ASSERT_TRUE(config_result) << "Failed to load config: " << config_result.error().to_string();
  Config config = *config_result;

  EXPECT_TRUE(config.api.http.enable);
  EXPECT_EQ(config.api.http.read_timeout_sec, 30);
  EXPECT_EQ(config.api.http.write_timeout_sec, 45);
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
            {"logging", {{"level", "info"}, {"format", "json"}}}};

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
  EXPECT_EQ(config.logging.format, "json");
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
  auto system_info = mygram::utils::GetSystemMemoryInfo();
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
  auto system_info = mygram::utils::GetSystemMemoryInfo();
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
  auto system_info = mygram::utils::GetSystemMemoryInfo();
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

/**
 * @brief Test session_timeout_sec default value
 */
TEST(ConfigTest, SessionTimeoutDefault) {
  std::string config_yaml = R"(
mysql:
  host: "127.0.0.1"
  port: 3306
  user: "test_user"
  password: "test_pass"
  database: "testdb"
tables:
  - name: "test_table"
    primary_key: "id"
    text_source:
      column: "content"
build:
  mode: "select_snapshot"
replication:
  enable: false
memory:
  hard_limit_mb: 512
api:
  tcp:
    bind: "127.0.0.1"
    port: 11016
network:
  allow_cidrs:
    - "127.0.0.1/32"
)";

  std::ofstream f("test_session_timeout_default.yaml");
  f << config_yaml;
  f.close();

  auto config_result = LoadConfig("test_session_timeout_default.yaml");
  ASSERT_TRUE(config_result) << "Failed to load config: " << config_result.error().to_string();
  Config config = *config_result;

  // Should use default value (3600 seconds = 1 hour)
  EXPECT_EQ(config.mysql.session_timeout_sec, 3600);

  std::remove("test_session_timeout_default.yaml");
}

/**
 * @brief Test session_timeout_sec custom value
 */
TEST(ConfigTest, SessionTimeoutCustomValue) {
  std::string config_yaml = R"(
mysql:
  host: "127.0.0.1"
  port: 3306
  user: "test_user"
  password: "test_pass"
  database: "testdb"
  session_timeout_sec: 7200
tables:
  - name: "test_table"
    primary_key: "id"
    text_source:
      column: "content"
build:
  mode: "select_snapshot"
replication:
  enable: false
memory:
  hard_limit_mb: 512
api:
  tcp:
    bind: "127.0.0.1"
    port: 11016
network:
  allow_cidrs:
    - "127.0.0.1/32"
)";

  std::ofstream f("test_session_timeout_custom.yaml");
  f << config_yaml;
  f.close();

  auto config_result = LoadConfig("test_session_timeout_custom.yaml");
  ASSERT_TRUE(config_result) << "Failed to load config: " << config_result.error().to_string();
  Config config = *config_result;

  // Should use custom value (7200 seconds = 2 hours)
  EXPECT_EQ(config.mysql.session_timeout_sec, 7200);

  std::remove("test_session_timeout_custom.yaml");
}

/**
 * @brief Test session_timeout_sec validation (minimum)
 */
TEST(ConfigTest, SessionTimeoutValidationMin) {
  std::string config_yaml = R"(
mysql:
  host: "127.0.0.1"
  port: 3306
  user: "test_user"
  password: "test_pass"
  database: "testdb"
  session_timeout_sec: 30
tables:
  - name: "test_table"
    primary_key: "id"
    text_source:
      column: "content"
build:
  mode: "select_snapshot"
replication:
  enable: false
memory:
  hard_limit_mb: 512
api:
  tcp:
    bind: "127.0.0.1"
    port: 11016
network:
  allow_cidrs:
    - "127.0.0.1/32"
)";

  std::ofstream f("test_session_timeout_min.yaml");
  f << config_yaml;
  f.close();

  auto config_result = LoadConfig("test_session_timeout_min.yaml");
  // Should fail validation (minimum is 60)
  EXPECT_FALSE(config_result);
  if (!config_result) {
    EXPECT_TRUE(config_result.error().message().find("session_timeout_sec") != std::string::npos);
  }

  std::remove("test_session_timeout_min.yaml");
}

/**
 * @brief Test session_timeout_sec validation (maximum)
 */
TEST(ConfigTest, SessionTimeoutValidationMax) {
  std::string config_yaml = R"(
mysql:
  host: "127.0.0.1"
  port: 3306
  user: "test_user"
  password: "test_pass"
  database: "testdb"
  session_timeout_sec: 90000
tables:
  - name: "test_table"
    primary_key: "id"
    text_source:
      column: "content"
build:
  mode: "select_snapshot"
replication:
  enable: false
memory:
  hard_limit_mb: 512
api:
  tcp:
    bind: "127.0.0.1"
    port: 11016
network:
  allow_cidrs:
    - "127.0.0.1/32"
)";

  std::ofstream f("test_session_timeout_max.yaml");
  f << config_yaml;
  f.close();

  auto config_result = LoadConfig("test_session_timeout_max.yaml");
  // Should fail validation (maximum is 86400 = 24 hours)
  EXPECT_FALSE(config_result);
  if (!config_result) {
    EXPECT_TRUE(config_result.error().message().find("session_timeout_sec") != std::string::npos);
  }

  std::remove("test_session_timeout_max.yaml");
}

/**
 * @brief Test MySQL port range validation rejects out-of-range ports
 */
TEST(ConfigTest, MysqlPortRangeValidation) {
  // Port 0 should be rejected
  {
    std::ofstream f("port_zero.yaml");
    f << "mysql:\n";
    f << "  host: localhost\n";
    f << "  user: root\n";
    f << "  password: pass\n";
    f << "  database: testdb\n";
    f << "  port: 0\n";
    f << "tables:\n";
    f << "  - name: test\n";
    f << "    text_source:\n";
    f << "      column: text\n";
    f.close();

    auto result = LoadConfig("port_zero.yaml");
    EXPECT_FALSE(result) << "Port 0 should be rejected";
    std::remove("port_zero.yaml");
  }

  // Negative port should be rejected
  {
    std::ofstream f("port_negative.yaml");
    f << "mysql:\n";
    f << "  host: localhost\n";
    f << "  user: root\n";
    f << "  password: pass\n";
    f << "  database: testdb\n";
    f << "  port: -1\n";
    f << "tables:\n";
    f << "  - name: test\n";
    f << "    text_source:\n";
    f << "      column: text\n";
    f.close();

    auto result = LoadConfig("port_negative.yaml");
    EXPECT_FALSE(result) << "Port -1 should be rejected";
    std::remove("port_negative.yaml");
  }

  // Port exceeding 65535 should be rejected
  {
    std::ofstream f("port_too_large.yaml");
    f << "mysql:\n";
    f << "  host: localhost\n";
    f << "  user: root\n";
    f << "  password: pass\n";
    f << "  database: testdb\n";
    f << "  port: 99999\n";
    f << "tables:\n";
    f << "  - name: test\n";
    f << "    text_source:\n";
    f << "      column: text\n";
    f.close();

    auto result = LoadConfig("port_too_large.yaml");
    EXPECT_FALSE(result) << "Port 99999 should be rejected";
    std::remove("port_too_large.yaml");
  }
}

/**
 * @brief Test ToFilterConfig converts a single RequiredFilterConfig correctly
 */
TEST(ConfigTest, ToFilterConfigConvertsSingleConfig) {
  RequiredFilterConfig req;
  req.name = "status";
  req.type = "int";
  req.op = "=";
  req.value = "1";
  req.bitmap_index = true;

  FilterConfig result = ToFilterConfig(req);

  EXPECT_EQ(result.name, "status");
  EXPECT_EQ(result.type, "int");
  EXPECT_TRUE(result.bitmap_index);
  EXPECT_FALSE(result.dict_compress);
  // bucket and other FilterConfig-only fields should be default (empty)
  EXPECT_TRUE(result.bucket.empty());
}

/**
 * @brief Test ToFilterConfigs converts a vector of RequiredFilterConfigs
 */
TEST(ConfigTest, ToFilterConfigsConvertsVector) {
  std::vector<RequiredFilterConfig> required;

  RequiredFilterConfig req1;
  req1.name = "status";
  req1.type = "int";
  req1.op = "=";
  req1.value = "1";
  req1.bitmap_index = true;
  required.push_back(req1);

  RequiredFilterConfig req2;
  req2.name = "category";
  req2.type = "string";
  req2.op = "!=";
  req2.value = "deleted";
  req2.bitmap_index = false;
  required.push_back(req2);

  std::vector<FilterConfig> result = ToFilterConfigs(required);

  ASSERT_EQ(result.size(), 2);

  EXPECT_EQ(result[0].name, "status");
  EXPECT_EQ(result[0].type, "int");
  EXPECT_TRUE(result[0].bitmap_index);
  EXPECT_FALSE(result[0].dict_compress);

  EXPECT_EQ(result[1].name, "category");
  EXPECT_EQ(result[1].type, "string");
  EXPECT_FALSE(result[1].bitmap_index);
  EXPECT_FALSE(result[1].dict_compress);
}

/**
 * @brief Test ToFilterConfigs with empty input returns empty vector
 */
TEST(ConfigTest, ToFilterConfigsEmptyInput) {
  std::vector<RequiredFilterConfig> empty_required;

  std::vector<FilterConfig> result = ToFilterConfigs(empty_required);

  EXPECT_TRUE(result.empty());
}

// =============================================================================
// Expected<Config, Error> contract tests
// =============================================================================

/**
 * @brief Test that loading an invalid config returns an Error via Expected
 *        (not via exception)
 */
TEST(ConfigTest, InvalidConfigReturnsExpectedError) {
  // Create a config with a semantic error (invalid replication with server_id=0)
  std::ofstream f("expected_error_test.yaml");
  f << "mysql:\n";
  f << "  host: localhost\n";
  f << "  user: root\n";
  f << "  password: pass\n";
  f << "  database: testdb\n";
  f << "tables:\n";
  f << "  - name: test\n";
  f << "    text_source:\n";
  f << "      column: text\n";
  f << "replication:\n";
  f << "  enable: true\n";
  f << "  start_from: snapshot\n";
  // Missing server_id with replication enabled - should cause an error
  f.close();

  auto result = LoadConfig("expected_error_test.yaml");

  // Must return an error through Expected, not throw
  EXPECT_FALSE(result);
  if (!result) {
    // Error should have a meaningful message
    EXPECT_FALSE(result.error().message().empty());
    // Error code should be set
    EXPECT_NE(result.error().code(), mygram::utils::ErrorCode::kSuccess);
  }

  std::remove("expected_error_test.yaml");
}

/**
 * @brief Test that a valid minimal config returns Expected with value
 */
TEST(ConfigTest, ValidConfigReturnsExpectedValue) {
  std::ofstream f("expected_value_test.yaml");
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

  auto result = LoadConfig("expected_value_test.yaml");

  // Must succeed
  ASSERT_TRUE(result) << "Expected success, got error: " << result.error().to_string();

  // Access the value without exceptions
  const Config& config = *result;
  EXPECT_EQ(config.mysql.host, "localhost");
  EXPECT_EQ(config.mysql.user, "root");
  ASSERT_EQ(config.tables.size(), 1);
  EXPECT_EQ(config.tables[0].name, "test");

  std::remove("expected_value_test.yaml");
}

/**
 * @brief Test that a completely empty file returns Error (not throws)
 */
TEST(ConfigTest, EmptyFileReturnsExpectedError) {
  std::ofstream f("empty_config_test.yaml");
  f << "";
  f.close();

  auto result = LoadConfig("empty_config_test.yaml");
  EXPECT_FALSE(result);

  std::remove("empty_config_test.yaml");
}

/**
 * @brief Test ngram_size range validation (must be 1-10)
 */
TEST(ConfigTest, NgramSizeRangeValidation) {
  // ngram_size = 0 should be rejected
  {
    std::ofstream f("ngram_zero.yaml");
    f << "mysql:\n";
    f << "  host: localhost\n";
    f << "  user: root\n";
    f << "  password: pass\n";
    f << "  database: testdb\n";
    f << "tables:\n";
    f << "  - name: test\n";
    f << "    ngram_size: 0\n";
    f << "    text_source:\n";
    f << "      column: text\n";
    f.close();

    auto result = LoadConfig("ngram_zero.yaml");
    EXPECT_FALSE(result) << "ngram_size=0 should be rejected";
    std::remove("ngram_zero.yaml");
  }

  // ngram_size = 11 should be rejected
  {
    std::ofstream f("ngram_too_large.yaml");
    f << "mysql:\n";
    f << "  host: localhost\n";
    f << "  user: root\n";
    f << "  password: pass\n";
    f << "  database: testdb\n";
    f << "tables:\n";
    f << "  - name: test\n";
    f << "    ngram_size: 11\n";
    f << "    text_source:\n";
    f << "      column: text\n";
    f.close();

    auto result = LoadConfig("ngram_too_large.yaml");
    EXPECT_FALSE(result) << "ngram_size=11 should be rejected";
    std::remove("ngram_too_large.yaml");
  }

  // ngram_size = 5 should be accepted
  {
    std::ofstream f("ngram_valid.yaml");
    f << "mysql:\n";
    f << "  host: localhost\n";
    f << "  user: root\n";
    f << "  password: pass\n";
    f << "  database: testdb\n";
    f << "tables:\n";
    f << "  - name: test\n";
    f << "    ngram_size: 5\n";
    f << "    text_source:\n";
    f << "      column: text\n";
    f.close();

    auto result = LoadConfig("ngram_valid.yaml");
    EXPECT_TRUE(result) << "ngram_size=5 should be accepted";
    if (result) {
      EXPECT_EQ(result->tables[0].ngram_size, 5);
    }
    std::remove("ngram_valid.yaml");
  }
}

/**
 * @brief Test batch_size range validation (must be positive)
 */
TEST(ConfigTest, BatchSizeRangeValidation) {
  // batch_size = 0 should be rejected
  {
    std::ofstream f("batch_zero.yaml");
    f << "mysql:\n";
    f << "  host: localhost\n";
    f << "  user: root\n";
    f << "  password: pass\n";
    f << "  database: testdb\n";
    f << "tables:\n";
    f << "  - name: test\n";
    f << "    text_source:\n";
    f << "      column: text\n";
    f << "build:\n";
    f << "  batch_size: 0\n";
    f.close();

    auto result = LoadConfig("batch_zero.yaml");
    EXPECT_FALSE(result) << "batch_size=0 should be rejected";
    std::remove("batch_zero.yaml");
  }

  // batch_size = -1 should be rejected
  {
    std::ofstream f("batch_negative.yaml");
    f << "mysql:\n";
    f << "  host: localhost\n";
    f << "  user: root\n";
    f << "  password: pass\n";
    f << "  database: testdb\n";
    f << "tables:\n";
    f << "  - name: test\n";
    f << "    text_source:\n";
    f << "      column: text\n";
    f << "build:\n";
    f << "  batch_size: -1\n";
    f.close();

    auto result = LoadConfig("batch_negative.yaml");
    EXPECT_FALSE(result) << "batch_size=-1 should be rejected";
    std::remove("batch_negative.yaml");
  }
}

/**
 * @brief Test queue_size range validation (must be positive)
 */
TEST(ConfigTest, QueueSizeRangeValidation) {
  // queue_size = 0 should be rejected
  {
    std::ofstream f("queue_zero.yaml");
    f << "mysql:\n";
    f << "  host: localhost\n";
    f << "  user: root\n";
    f << "  password: pass\n";
    f << "  database: testdb\n";
    f << "tables:\n";
    f << "  - name: test\n";
    f << "    text_source:\n";
    f << "      column: text\n";
    f << "replication:\n";
    f << "  enable: true\n";
    f << "  server_id: 100\n";
    f << "  start_from: snapshot\n";
    f << "  queue_size: 0\n";
    f.close();

    auto result = LoadConfig("queue_zero.yaml");
    EXPECT_FALSE(result) << "queue_size=0 should be rejected";
    std::remove("queue_zero.yaml");
  }

  // queue_size = -5 should be rejected
  {
    std::ofstream f("queue_negative.yaml");
    f << "mysql:\n";
    f << "  host: localhost\n";
    f << "  user: root\n";
    f << "  password: pass\n";
    f << "  database: testdb\n";
    f << "tables:\n";
    f << "  - name: test\n";
    f << "    text_source:\n";
    f << "      column: text\n";
    f << "replication:\n";
    f << "  enable: true\n";
    f << "  server_id: 100\n";
    f << "  start_from: snapshot\n";
    f << "  queue_size: -5\n";
    f.close();

    auto result = LoadConfig("queue_negative.yaml");
    EXPECT_FALSE(result) << "queue_size=-5 should be rejected";
    std::remove("queue_negative.yaml");
  }
}
