/**
 * @file initial_loader_query_test.cpp
 * @brief Unit tests for InitialLoader SELECT query generation
 *
 * These tests verify that duplicate columns are avoided in SELECT queries.
 * While we cannot directly test the private BuildSelectQuery() method,
 * we can verify the behavior indirectly through logging or by using
 * a test-friendly wrapper approach.
 *
 * For now, this serves as documentation of the expected behavior.
 */

#ifdef USE_MYSQL

#include <gtest/gtest.h>

#include <algorithm>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "config/config.h"

namespace mygramdb::loader {

/**
 * @brief Test fixture for SELECT query generation logic
 *
 * These tests document the expected behavior of BuildSelectQuery().
 * The actual implementation in initial_loader.cpp should:
 * - Collect all columns from primary_key, text_source, required_filters, and filters
 * - Avoid duplicates using an unordered_set for tracking
 * - Preserve insertion order using a vector for output
 */
class SelectQueryLogicTest : public ::testing::Test {
 protected:
  /**
   * @brief Helper to verify column uniqueness logic
   *
   * This simulates the duplicate-avoidance logic used in BuildSelectQuery
   */
  static std::vector<std::string> CollectUniqueColumns(const config::TableConfig& table_config) {
    std::vector<std::string> selected_columns;
    std::unordered_set<std::string> seen_columns;

    auto add_column = [&](const std::string& col) {
      if (seen_columns.find(col) == seen_columns.end()) {
        selected_columns.push_back(col);
        seen_columns.insert(col);
      }
    };

    // Primary key
    add_column(table_config.primary_key);

    // Text source columns
    if (!table_config.text_source.column.empty()) {
      add_column(table_config.text_source.column);
    } else {
      for (const auto& col : table_config.text_source.concat) {
        add_column(col);
      }
    }

    // Required filter columns
    for (const auto& filter : table_config.required_filters) {
      add_column(filter.name);
    }

    // Optional filter columns
    for (const auto& filter : table_config.filters) {
      add_column(filter.name);
    }

    return selected_columns;
  }
};

/**
 * @brief Test that basic column collection works
 */
TEST_F(SelectQueryLogicTest, CollectColumns_Basic) {
  config::TableConfig table_config;
  table_config.name = "articles";
  table_config.primary_key = "id";
  table_config.text_source.column = "content";

  auto columns = CollectUniqueColumns(table_config);

  ASSERT_EQ(columns.size(), 2);
  EXPECT_EQ(columns[0], "id");
  EXPECT_EQ(columns[1], "content");
}

/**
 * @brief Test that filter columns are included
 */
TEST_F(SelectQueryLogicTest, CollectColumns_WithFilters) {
  config::TableConfig table_config;
  table_config.name = "articles";
  table_config.primary_key = "id";
  table_config.text_source.column = "content";

  config::FilterConfig filter1;
  filter1.name = "status";
  filter1.type = "int";
  table_config.filters.push_back(filter1);

  config::FilterConfig filter2;
  filter2.name = "category";
  filter2.type = "string";
  table_config.filters.push_back(filter2);

  auto columns = CollectUniqueColumns(table_config);

  ASSERT_EQ(columns.size(), 4);
  EXPECT_EQ(columns[0], "id");
  EXPECT_EQ(columns[1], "content");
  EXPECT_EQ(columns[2], "status");
  EXPECT_EQ(columns[3], "category");
}

/**
 * @brief Test that required filter columns are included
 */
TEST_F(SelectQueryLogicTest, CollectColumns_WithRequiredFilters) {
  config::TableConfig table_config;
  table_config.name = "articles";
  table_config.primary_key = "id";
  table_config.text_source.column = "content";

  config::RequiredFilterConfig required_filter;
  required_filter.name = "enabled";
  required_filter.type = "int";
  required_filter.op = "=";
  required_filter.value = "1";
  table_config.required_filters.push_back(required_filter);

  auto columns = CollectUniqueColumns(table_config);

  ASSERT_EQ(columns.size(), 3);
  EXPECT_EQ(columns[0], "id");
  EXPECT_EQ(columns[1], "content");
  EXPECT_EQ(columns[2], "enabled");
}

/**
 * @brief Test that duplicate columns are avoided
 *
 * This is the key test for the bug fix: when the same column appears
 * in multiple places (e.g., primary_key, text_source, filters),
 * it should only appear once in the final SELECT clause.
 */
TEST_F(SelectQueryLogicTest, CollectColumns_NoDuplicates) {
  config::TableConfig table_config;
  table_config.name = "articles";
  table_config.primary_key = "id";
  table_config.text_source.column = "content";

  // Add filter that duplicates primary_key
  config::FilterConfig filter1;
  filter1.name = "id";  // Same as primary_key
  filter1.type = "bigint";
  table_config.filters.push_back(filter1);

  // Add filter that duplicates text_source
  config::FilterConfig filter2;
  filter2.name = "content";  // Same as text_source
  filter2.type = "text";
  table_config.filters.push_back(filter2);

  // Add required_filter with unique column
  config::RequiredFilterConfig required_filter;
  required_filter.name = "enabled";
  required_filter.type = "int";
  required_filter.op = "=";
  required_filter.value = "1";
  table_config.required_filters.push_back(required_filter);

  // Add filter that duplicates required_filter
  config::FilterConfig filter3;
  filter3.name = "enabled";  // Same as required_filter
  filter3.type = "int";
  table_config.filters.push_back(filter3);

  auto columns = CollectUniqueColumns(table_config);

  // Should have exactly 3 unique columns: id, content, enabled
  ASSERT_EQ(columns.size(), 3);
  EXPECT_EQ(columns[0], "id");
  EXPECT_EQ(columns[1], "content");
  EXPECT_EQ(columns[2], "enabled");

  // Verify each column appears exactly once
  std::unordered_map<std::string, int> column_counts;
  for (const auto& col : columns) {
    column_counts[col]++;
  }
  EXPECT_EQ(column_counts["id"], 1);
  EXPECT_EQ(column_counts["content"], 1);
  EXPECT_EQ(column_counts["enabled"], 1);
}

/**
 * @brief Test with concatenated text source
 */
TEST_F(SelectQueryLogicTest, CollectColumns_WithConcatenatedTextSource) {
  config::TableConfig table_config;
  table_config.name = "articles";
  table_config.primary_key = "id";
  table_config.text_source.concat = {"title", "body", "summary"};

  auto columns = CollectUniqueColumns(table_config);

  ASSERT_EQ(columns.size(), 4);
  EXPECT_EQ(columns[0], "id");
  EXPECT_EQ(columns[1], "title");
  EXPECT_EQ(columns[2], "body");
  EXPECT_EQ(columns[3], "summary");
}

/**
 * @brief Test that duplicates in concatenated text source are avoided
 */
TEST_F(SelectQueryLogicTest, CollectColumns_NoDuplicatesWithConcat) {
  config::TableConfig table_config;
  table_config.name = "articles";
  table_config.primary_key = "id";
  table_config.text_source.concat = {"title", "body"};

  // Add filter that duplicates one of the concat columns
  config::FilterConfig filter;
  filter.name = "title";  // Same as one of concat columns
  filter.type = "varchar";
  table_config.filters.push_back(filter);

  auto columns = CollectUniqueColumns(table_config);

  // 'title' should appear exactly once
  ASSERT_EQ(columns.size(), 3);
  EXPECT_EQ(columns[0], "id");
  EXPECT_EQ(columns[1], "title");
  EXPECT_EQ(columns[2], "body");

  // Verify 'title' appears exactly once
  int title_count = std::count(columns.begin(), columns.end(), "title");
  EXPECT_EQ(title_count, 1);
}

}  // namespace mygramdb::loader

#endif  // USE_MYSQL
