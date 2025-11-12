/**
 * @file required_filters_formatting_test.cpp
 * @brief Unit tests for required_filters integer/float value formatting
 */

#include <gtest/gtest.h>

#include <fstream>
#include <nlohmann/json.hpp>

#include "config/config.h"

using namespace mygramdb::config;
using json = nlohmann::json;

/**
 * @brief Test integer value formatting (should not have decimal point)
 */
TEST(RequiredFiltersFormattingTest, IntegerValueFormat) {
  // Create temporary YAML config with integer filter value
  const char* yaml_content = R"(
mysql:
  host: "127.0.0.1"
  port: 3306
  user: "test"
  password: "test"
  database: "test"

tables:
  - name: "test_table"
    primary_key: "id"
    text_source:
      column: "content"
    required_filters:
      - name: "enabled"
        type: "int"
        op: "="
        value: 1
      - name: "id"
        type: "int"
        op: "<"
        value: 100000

build:
  mode: "select_snapshot"

api:
  tcp:
    bind: "127.0.0.1"
    port: 11016

logging:
  level: "info"
)";

  // Write temporary config file
  std::string temp_file = "/tmp/test_integer_format.yaml";
  std::ofstream ofs(temp_file);
  ofs << yaml_content;
  ofs.close();

  // Load and check
  Config config = LoadConfig(temp_file);

  ASSERT_EQ(config.tables.size(), 1);
  const auto& table = config.tables[0];

  ASSERT_EQ(table.required_filters.size(), 2);

  // Check first filter: enabled = 1 (integer, not "1.000000")
  EXPECT_EQ(table.required_filters[0].name, "enabled");
  EXPECT_EQ(table.required_filters[0].value, "1");                          // Should be "1", not "1.000000"
  EXPECT_EQ(table.required_filters[0].value.find("."), std::string::npos);  // No decimal point

  // Check second filter: id < 100000 (integer, not "100000.000000")
  EXPECT_EQ(table.required_filters[1].name, "id");
  EXPECT_EQ(table.required_filters[1].value, "100000");                     // Should be "100000", not "100000.000000"
  EXPECT_EQ(table.required_filters[1].value.find("."), std::string::npos);  // No decimal point

  // Clean up
  std::remove(temp_file.c_str());
}

/**
 * @brief Test float value formatting (should have decimal point)
 */
TEST(RequiredFiltersFormattingTest, FloatValueFormat) {
  // Create temporary YAML config with float filter value
  const char* yaml_content = R"(
mysql:
  host: "127.0.0.1"
  port: 3306
  user: "test"
  password: "test"
  database: "test"

tables:
  - name: "test_table"
    primary_key: "id"
    text_source:
      column: "content"
    required_filters:
      - name: "price"
        type: "float"
        op: ">"
        value: 99.99
      - name: "rating"
        type: "double"
        op: ">="
        value: 4.5

build:
  mode: "select_snapshot"

api:
  tcp:
    bind: "127.0.0.1"
    port: 11016

logging:
  level: "info"
)";

  // Write temporary config file
  std::string temp_file = "/tmp/test_float_format.yaml";
  std::ofstream ofs(temp_file);
  ofs << yaml_content;
  ofs.close();

  // Load and check
  Config config = LoadConfig(temp_file);

  ASSERT_EQ(config.tables.size(), 1);
  const auto& table = config.tables[0];

  ASSERT_EQ(table.required_filters.size(), 2);

  // Check float values have decimal points
  EXPECT_EQ(table.required_filters[0].name, "price");
  EXPECT_NE(table.required_filters[0].value.find("."), std::string::npos);  // Has decimal point

  EXPECT_EQ(table.required_filters[1].name, "rating");
  EXPECT_NE(table.required_filters[1].value.find("."), std::string::npos);  // Has decimal point

  // Clean up
  std::remove(temp_file.c_str());
}

/**
 * @brief Test string value formatting (should be unchanged)
 */
TEST(RequiredFiltersFormattingTest, StringValueFormat) {
  // Create temporary YAML config with string filter value
  const char* yaml_content = R"(
mysql:
  host: "127.0.0.1"
  port: 3306
  user: "test"
  password: "test"
  database: "test"

tables:
  - name: "test_table"
    primary_key: "id"
    text_source:
      column: "content"
    required_filters:
      - name: "status"
        type: "varchar"
        op: "="
        value: "active"

build:
  mode: "select_snapshot"

api:
  tcp:
    bind: "127.0.0.1"
    port: 11016

logging:
  level: "info"
)";

  // Write temporary config file
  std::string temp_file = "/tmp/test_string_format.yaml";
  std::ofstream ofs(temp_file);
  ofs << yaml_content;
  ofs.close();

  // Load and check
  Config config = LoadConfig(temp_file);

  ASSERT_EQ(config.tables.size(), 1);
  const auto& table = config.tables[0];

  ASSERT_EQ(table.required_filters.size(), 1);

  // Check string value is unchanged
  EXPECT_EQ(table.required_filters[0].name, "status");
  EXPECT_EQ(table.required_filters[0].value, "active");

  // Clean up
  std::remove(temp_file.c_str());
}

/**
 * @brief Test tinyint value formatting for boolean-like values (should be "0" or "1")
 */
TEST(RequiredFiltersFormattingTest, BooleanValueFormat) {
  // Create temporary YAML config with tinyint filter value (boolean-like)
  // MySQL doesn't have a native boolean type; it uses tinyint(1) instead
  const char* yaml_content = R"(
mysql:
  host: "127.0.0.1"
  port: 3306
  user: "test"
  password: "test"
  database: "test"

tables:
  - name: "test_table"
    primary_key: "id"
    text_source:
      column: "content"
    required_filters:
      - name: "is_active"
        type: "tinyint"
        op: "="
        value: 1
      - name: "is_deleted"
        type: "tinyint"
        op: "="
        value: 0

build:
  mode: "select_snapshot"

api:
  tcp:
    bind: "127.0.0.1"
    port: 11016

logging:
  level: "info"
)";

  // Write temporary config file
  std::string temp_file = "/tmp/test_boolean_format.yaml";
  std::ofstream ofs(temp_file);
  ofs << yaml_content;
  ofs.close();

  // Load and check
  Config config = LoadConfig(temp_file);

  ASSERT_EQ(config.tables.size(), 1);
  const auto& table = config.tables[0];

  ASSERT_EQ(table.required_filters.size(), 2);

  // Check tinyint values are formatted as "1" or "0" (not "1.000000" or "0.000000")
  EXPECT_EQ(table.required_filters[0].name, "is_active");
  EXPECT_EQ(table.required_filters[0].value, "1");  // Integer 1, not "1.000000"

  EXPECT_EQ(table.required_filters[1].name, "is_deleted");
  EXPECT_EQ(table.required_filters[1].value, "0");  // Integer 0, not "0.000000"

  // Clean up
  std::remove(temp_file.c_str());
}

/**
 * @brief Test mixed integer and float values in same config
 */
TEST(RequiredFiltersFormattingTest, MixedIntegerAndFloatValues) {
  // Create temporary YAML config with mixed values
  const char* yaml_content = R"(
mysql:
  host: "127.0.0.1"
  port: 3306
  user: "test"
  password: "test"
  database: "test"

tables:
  - name: "test_table"
    primary_key: "id"
    text_source:
      column: "content"
    required_filters:
      - name: "count"
        type: "int"
        op: ">"
        value: 10
      - name: "percentage"
        type: "float"
        op: "<"
        value: 50.5
      - name: "limit"
        type: "bigint"
        op: "<="
        value: 1000000

build:
  mode: "select_snapshot"

api:
  tcp:
    bind: "127.0.0.1"
    port: 11016

logging:
  level: "info"
)";

  // Write temporary config file
  std::string temp_file = "/tmp/test_mixed_format.yaml";
  std::ofstream ofs(temp_file);
  ofs << yaml_content;
  ofs.close();

  // Load and check
  Config config = LoadConfig(temp_file);

  ASSERT_EQ(config.tables.size(), 1);
  const auto& table = config.tables[0];

  ASSERT_EQ(table.required_filters.size(), 3);

  // Integer value: no decimal point
  EXPECT_EQ(table.required_filters[0].value, "10");
  EXPECT_EQ(table.required_filters[0].value.find("."), std::string::npos);

  // Float value: has decimal point
  EXPECT_EQ(table.required_filters[1].name, "percentage");
  EXPECT_NE(table.required_filters[1].value.find("."), std::string::npos);

  // Large integer value: no decimal point
  EXPECT_EQ(table.required_filters[2].value, "1000000");
  EXPECT_EQ(table.required_filters[2].value.find("."), std::string::npos);

  // Clean up
  std::remove(temp_file.c_str());
}
