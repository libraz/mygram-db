/**
 * @file required_filters_test.cpp
 * @brief Unit tests for required_filters configuration and evaluation
 */

#include <gtest/gtest.h>

#include <cstdio>
#include <fstream>
#include <variant>

#include "config/config.h"
#include "storage/document_store.h"

using namespace mygramdb;

/**
 * @brief Test parsing required_filters from YAML configuration
 */
TEST(RequiredFiltersTest, ParseYamlConfig) {
  std::string yaml_content = R"(
mysql:
  host: "127.0.0.1"
  user: "test_user"
  password: "test_pass"
  database: "test"

tables:
  - name: "articles"
    text_source:
      column: "content"
    required_filters:
      - name: "enabled"
        type: "int"
        op: "="
        value: 1
        bitmap_index: false

      - name: "deleted_at"
        type: "datetime"
        op: "IS NULL"
        bitmap_index: false

    filters:
      - name: "status"
        type: "int"

replication:
  server_id: 12345
)";

  // Write to temp file
  std::string temp_file = "/tmp/test_required_filters.yaml";
  std::ofstream ofs(temp_file);
  ofs << yaml_content;
  ofs.close();

  // Load config
  auto config = config::LoadConfig(temp_file);

  ASSERT_EQ(config.tables.size(), 1);
  const auto& table = config.tables[0];

  // Check required_filters
  ASSERT_EQ(table.required_filters.size(), 2);

  EXPECT_EQ(table.required_filters[0].name, "enabled");
  EXPECT_EQ(table.required_filters[0].type, "int");
  EXPECT_EQ(table.required_filters[0].op, "=");
  // Integer values should not have decimal point (e.g., "1" not "1.000000")
  EXPECT_EQ(table.required_filters[0].value, "1");
  EXPECT_FALSE(table.required_filters[0].bitmap_index);

  EXPECT_EQ(table.required_filters[1].name, "deleted_at");
  EXPECT_EQ(table.required_filters[1].type, "datetime");
  EXPECT_EQ(table.required_filters[1].op, "IS NULL");
  EXPECT_TRUE(table.required_filters[1].value.empty());

  // Check optional filters
  ASSERT_EQ(table.filters.size(), 1);
  EXPECT_EQ(table.filters[0].name, "status");

  // Cleanup
  std::remove(temp_file.c_str());
}

/**
 * @brief Test parsing required_filters from JSON configuration
 */
TEST(RequiredFiltersTest, ParseJsonConfig) {
  std::string json_content = R"({
  "mysql": {
    "host": "127.0.0.1",
    "user": "test_user",
    "password": "test_pass",
    "database": "test"
  },
  "tables": [
    {
      "name": "articles",
      "text_source": {
        "column": "content"
      },
      "required_filters": [
        {
          "name": "enabled",
          "type": "int",
          "op": "=",
          "value": 1
        },
        {
          "name": "priority",
          "type": "int",
          "op": ">",
          "value": 0
        }
      ],
      "filters": [
        {
          "name": "category",
          "type": "string"
        }
      ]
    }
  ],
  "replication": {
    "server_id": 12345
  }
})";

  // Write to temp file
  std::string temp_file = "/tmp/test_required_filters.json";
  std::ofstream ofs(temp_file);
  ofs << json_content;
  ofs.close();

  // Load config
  auto config = config::LoadConfig(temp_file);

  ASSERT_EQ(config.tables.size(), 1);
  const auto& table = config.tables[0];

  // Check required_filters
  ASSERT_EQ(table.required_filters.size(), 2);

  EXPECT_EQ(table.required_filters[0].name, "enabled");
  EXPECT_EQ(table.required_filters[0].op, "=");
  EXPECT_EQ(table.required_filters[0].value, "1");  // Integer: no decimal point

  EXPECT_EQ(table.required_filters[1].name, "priority");
  EXPECT_EQ(table.required_filters[1].op, ">");
  EXPECT_EQ(table.required_filters[1].value, "0");  // Integer: no decimal point

  // Cleanup
  std::remove(temp_file.c_str());
}

/**
 * @brief Test validation of invalid operators
 */
TEST(RequiredFiltersTest, InvalidOperator) {
  std::string json_content = R"({
  "mysql": {
    "host": "127.0.0.1",
    "user": "test_user",
    "password": "test_pass",
    "database": "test"
  },
  "tables": [
    {
      "name": "articles",
      "text_source": {
        "column": "content"
      },
      "required_filters": [
        {
          "name": "enabled",
          "type": "int",
          "op": "LIKE",
          "value": 1
        }
      ]
    }
  ],
  "replication": {
    "server_id": 12345
  }
})";

  std::string temp_file = "/tmp/test_invalid_operator.json";
  std::ofstream ofs(temp_file);
  ofs << json_content;
  ofs.close();

  // Should throw exception for invalid operator
  EXPECT_THROW({ auto config = config::LoadConfig(temp_file); }, std::runtime_error);

  std::remove(temp_file.c_str());
}

/**
 * @brief Test where_clause deprecation (should throw error)
 */
TEST(RequiredFiltersTest, WhereClauseDeprecated) {
  std::string json_content = R"({
  "mysql": {
    "host": "127.0.0.1",
    "user": "test_user",
    "password": "test_pass",
    "database": "test"
  },
  "tables": [
    {
      "name": "articles",
      "text_source": {
        "column": "content"
      },
      "where_clause": "enabled = 1"
    }
  ],
  "replication": {
    "server_id": 12345
  }
})";

  std::string temp_file = "/tmp/test_where_clause.json";
  std::ofstream ofs(temp_file);
  ofs << json_content;
  ofs.close();

  // Should throw exception for deprecated where_clause
  EXPECT_THROW({ auto config = config::LoadConfig(temp_file); }, std::runtime_error);

  std::remove(temp_file.c_str());
}

/**
 * @brief Test all supported operators
 */
TEST(RequiredFiltersTest, AllOperators) {
  std::vector<std::string> operators = {"=", "!=", "<", ">", "<=", ">=", "IS NULL", "IS NOT NULL"};

  for (const auto& op : operators) {
    std::string value_field = (op == "IS NULL" || op == "IS NOT NULL") ? "" : R"("value": 1,)";

    std::string json_content = R"({
  "mysql": {
    "host": "127.0.0.1",
    "user": "test_user",
    "password": "test_pass",
    "database": "test"
  },
  "tables": [
    {
      "name": "articles",
      "text_source": {
        "column": "content"
      },
      "required_filters": [
        {
          "name": "test_col",
          "type": "int",
          "op": ")" + op + R"(",
          )" + value_field + R"(
          "bitmap_index": false
        }
      ]
    }
  ],
  "replication": {
    "server_id": 12345
  }
})";

    std::string temp_file = "/tmp/test_operator_" + op + ".json";
    std::ofstream ofs(temp_file);
    ofs << json_content;
    ofs.close();

    // Should parse successfully
    EXPECT_NO_THROW({
      auto config = config::LoadConfig(temp_file);
      ASSERT_EQ(config.tables[0].required_filters.size(), 1);
      EXPECT_EQ(config.tables[0].required_filters[0].op, op);
    }) << "Failed for operator: "
       << op;

    std::remove(temp_file.c_str());
  }
}

/**
 * @brief Test that IS NULL operator should not have a value
 */
TEST(RequiredFiltersTest, IsNullShouldNotHaveValue) {
  std::string json_content = R"({
  "mysql": {
    "host": "127.0.0.1",
    "user": "test_user",
    "password": "test_pass",
    "database": "test"
  },
  "tables": [
    {
      "name": "articles",
      "text_source": {
        "column": "content"
      },
      "required_filters": [
        {
          "name": "deleted_at",
          "type": "datetime",
          "op": "IS NULL",
          "value": "something"
        }
      ]
    }
  ],
  "replication": {
    "server_id": 12345
  }
})";

  std::string temp_file = "/tmp/test_is_null_value.json";
  std::ofstream ofs(temp_file);
  ofs << json_content;
  ofs.close();

  // Should throw exception
  EXPECT_THROW({ auto config = config::LoadConfig(temp_file); }, std::runtime_error);

  std::remove(temp_file.c_str());
}

/**
 * @brief Test that comparison operators must have a value
 */
TEST(RequiredFiltersTest, ComparisonMustHaveValue) {
  std::string json_content = R"({
  "mysql": {
    "host": "127.0.0.1",
    "user": "test_user",
    "password": "test_pass",
    "database": "test"
  },
  "tables": [
    {
      "name": "articles",
      "text_source": {
        "column": "content"
      },
      "required_filters": [
        {
          "name": "enabled",
          "type": "int",
          "op": "="
        }
      ]
    }
  ],
  "replication": {
    "server_id": 12345
  }
})";

  std::string temp_file = "/tmp/test_no_value.json";
  std::ofstream ofs(temp_file);
  ofs << json_content;
  ofs.close();

  // Should throw exception
  EXPECT_THROW({ auto config = config::LoadConfig(temp_file); }, std::runtime_error);

  std::remove(temp_file.c_str());
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
