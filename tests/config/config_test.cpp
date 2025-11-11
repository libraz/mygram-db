/**
 * @file config_test.cpp
 * @brief Unit tests for configuration parser
 */

#include "config/config.h"

#include <gtest/gtest.h>

#include <fstream>
#include <nlohmann/json.hpp>

using namespace mygramdb::config;
using json = nlohmann::json;

/**
 * @brief Test loading valid configuration file
 */
TEST(ConfigTest, LoadValidConfig) {
  Config config = LoadConfig("test_config.yaml");

  // MySQL config
  EXPECT_EQ(config.mysql.host, "127.0.0.1");
  EXPECT_EQ(config.mysql.port, 3306);
  EXPECT_EQ(config.mysql.user, "test_user");
  EXPECT_EQ(config.mysql.password, "test_pass");
  EXPECT_TRUE(config.mysql.use_gtid);
  EXPECT_EQ(config.mysql.binlog_format, "ROW");
  EXPECT_EQ(config.mysql.binlog_row_image, "FULL");
  EXPECT_EQ(config.mysql.connect_timeout_ms, 5000);

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

  // Snapshot config
  EXPECT_EQ(config.snapshot.dir, "/tmp/test_snapshots");
  EXPECT_EQ(config.snapshot.interval_sec, 300);
  EXPECT_EQ(config.snapshot.retain, 2);

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
  EXPECT_THROW(LoadConfig("non_existent.yaml"), std::runtime_error);
}

/**
 * @brief Test loading invalid YAML
 */
TEST(ConfigTest, LoadInvalidYAML) {
  // Create invalid YAML file
  std::ofstream f("invalid.yaml");
  f << "invalid: yaml: content: [\n";
  f.close();

  EXPECT_THROW(LoadConfig("invalid.yaml"), std::runtime_error);
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

  Config config = LoadConfig("minimal.yaml");

  // Check defaults
  EXPECT_EQ(config.mysql.port, 3306);
  EXPECT_TRUE(config.mysql.use_gtid);
  EXPECT_EQ(config.build.batch_size, 5000);
  EXPECT_EQ(config.memory.hard_limit_mb, 8192);
  EXPECT_EQ(config.api.tcp.port, 11016);
  EXPECT_TRUE(config.api.http.enable);
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

  Config config = LoadConfig("concat.yaml");

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

  EXPECT_THROW(
      {
        try {
          LoadConfig("invalid_server_id.yaml");
        } catch (const std::runtime_error& e) {
          std::string error_msg(e.what());
          // Schema validation happens first, so check for schema error or server_id error
          bool valid_error =
              (error_msg.find("replication.server_id must be set to a non-zero value") !=
               std::string::npos) ||
              (error_msg.find("server_id") != std::string::npos) ||
              (error_msg.find("required property") != std::string::npos);
          EXPECT_TRUE(valid_error) << "Actual error: " << error_msg;
          throw;
        }
      },
      std::runtime_error);
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

  EXPECT_THROW(
      {
        try {
          LoadConfig("invalid_gtid.yaml");
        } catch (const std::runtime_error& e) {
          std::string error_msg(e.what());
          EXPECT_TRUE(error_msg.find("Invalid GTID format") != std::string::npos);
          throw;
        }
      },
      std::runtime_error);
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

  EXPECT_THROW(
      {
        try {
          LoadConfig("invalid_start_from.yaml");
        } catch (const std::runtime_error& e) {
          std::string error_msg(e.what());
          EXPECT_TRUE(error_msg.find("replication.start_from must be one of: snapshot, latest, "
                                     "state_file, or gtid=<UUID:txn>") != std::string::npos);
          throw;
        }
      },
      std::runtime_error);
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

  Config config = LoadConfig("test_config.json");

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
  json config_json = {
      {"mysql", {{"user", "test_user"}, {"password", "test_pass"}, {"database", "test_db"}}},
      {"tables", {{{"name", "test_table"}, {"text_source", {{"column", "content"}}}}}},
      {"replication", {{"server_id", 100}}}};

  std::ofstream f("valid_config.json");
  f << config_json.dump(2);
  f.close();

  // Should load successfully with built-in schema validation
  Config config = LoadConfig("valid_config.json");
  EXPECT_EQ(config.mysql.user, "test_user");
  EXPECT_EQ(config.tables.size(), 1);
}

/**
 * @brief Test JSON config with invalid data against built-in schema
 */
TEST(ConfigTest, LoadInvalidJSONWithSchemaValidation) {
  // Create invalid JSON config (missing required "user" field)
  json config_json = {
      {"mysql", {{"password", "test_pass"}, {"database", "test_db"}}},
      {"tables", {{{"name", "test_table"}, {"text_source", {{"column", "content"}}}}}}};

  std::ofstream f("invalid_config.json");
  f << config_json.dump(2);
  f.close();

  // Should throw validation error (built-in schema requires "user" field)
  EXPECT_THROW(LoadConfig("invalid_config.json"), std::runtime_error);
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
  Config config = LoadConfig("config_no_ext");
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

  EXPECT_THROW(LoadConfig("invalid.json"), std::runtime_error);
}

/**
 * @brief Test JSON config with unknown keys (should fail with schema validation)
 */
TEST(ConfigTest, JSONConfigWithUnknownKeys) {
  // Create JSON config with unknown field
  json config_json = {
      {"mysql",
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
  EXPECT_THROW(LoadConfig("unknown_keys.json"), std::runtime_error);
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

  Config config = LoadConfigYaml("legacy.yaml");
  EXPECT_EQ(config.mysql.user, "legacy_user");
}

/**
 * @brief Test LoadConfigJson function
 */
TEST(ConfigTest, LoadConfigJsonFunction) {
  json config_json = {
      {"mysql", {{"user", "json_func_user"}, {"password", "pass"}, {"database", "db"}}},
      {"tables", {{{"name", "test"}, {"text_source", {{"column", "content"}}}}}},
      {"replication", {{"server_id", 300}}}};

  std::ofstream f("func_test.json");
  f << config_json.dump(2);
  f.close();

  Config config = LoadConfigJson("func_test.json");
  EXPECT_EQ(config.mysql.user, "json_func_user");
  EXPECT_EQ(config.replication.server_id, 300U);
}
