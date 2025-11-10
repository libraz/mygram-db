/**
 * @file config_test.cpp
 * @brief Unit tests for configuration parser
 */

#include "config/config.h"
#include <gtest/gtest.h>
#include <fstream>

using namespace mygramdb::config;

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
  EXPECT_EQ(config.api.tcp.port, 11311);
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
  EXPECT_EQ(config.api.tcp.port, 11311);
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
