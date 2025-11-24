/**
 * @file rows_parser_test.cpp
 * @brief Unit tests for MySQL ROWS event parser
 */

#include "mysql/rows_parser.h"

#include <gtest/gtest.h>

#include <cstring>
#include <vector>

#include "mysql/binlog_util.h"
#include "mysql/table_metadata.h"

#ifdef USE_MYSQL

using namespace mygramdb::mysql;

class RowsParserTest : public ::testing::Test {
 protected:
  // Helper function to create a simple WRITE_ROWS event buffer
  std::vector<unsigned char> CreateWriteRowsEvent(const TableMetadata& table_meta,
                                                  const std::vector<std::vector<std::string>>& rows) {
    std::vector<unsigned char> buffer;

    // Common header (19 bytes) - simplified
    // We'll fill in event_size later at bytes [9-12]
    for (int i = 0; i < 19; i++) {
      buffer.push_back(0);
    }

    // Post-header: table_id (6 bytes)
    uint64_t table_id = table_meta.table_id;
    for (int i = 0; i < 6; i++) {
      buffer.push_back((table_id >> (i * 8)) & 0xFF);
    }

    // Post-header: flags (2 bytes)
    buffer.push_back(0);
    buffer.push_back(0);

    // Body: column count (packed integer)
    uint64_t col_count = table_meta.columns.size();
    if (col_count < 251) {
      buffer.push_back(static_cast<unsigned char>(col_count));
    }

    // Body: columns_present bitmap (all columns present)
    size_t bitmap_size = (col_count + 7) / 8;
    for (size_t i = 0; i < bitmap_size; i++) {
      buffer.push_back(0xFF);  // All bits set
    }

    // Body: rows
    for (const auto& row : rows) {
      // NULL bitmap (no NULLs for simplicity)
      for (size_t i = 0; i < bitmap_size; i++) {
        buffer.push_back(0x00);
      }

      // Row data
      for (size_t col_idx = 0; col_idx < col_count; col_idx++) {
        const auto& col_meta = table_meta.columns[col_idx];
        const std::string& value = row[col_idx];

        switch (col_meta.type) {
          case ColumnType::LONG: {
            // 4 bytes, little-endian
            int32_t int_val = std::stoi(value);
            buffer.push_back(int_val & 0xFF);
            buffer.push_back((int_val >> 8) & 0xFF);
            buffer.push_back((int_val >> 16) & 0xFF);
            buffer.push_back((int_val >> 24) & 0xFF);
            break;
          }

          case ColumnType::LONGLONG: {
            // 8 bytes, little-endian
            int64_t long_val = std::stoll(value);
            for (int i = 0; i < 8; i++) {
              buffer.push_back((long_val >> (i * 8)) & 0xFF);
            }
            break;
          }

          case ColumnType::VARCHAR: {
            // Length (1 or 2 bytes) + data
            size_t str_len = value.length();
            if (col_meta.metadata > 255) {
              // 2 byte length
              buffer.push_back(str_len & 0xFF);
              buffer.push_back((str_len >> 8) & 0xFF);
            } else {
              // 1 byte length
              buffer.push_back(static_cast<unsigned char>(str_len));
            }
            for (char c : value) {
              buffer.push_back(static_cast<unsigned char>(c));
            }
            break;
          }

          case ColumnType::BLOB: {
            // Length bytes (1-4) + data
            size_t str_len = value.length();
            switch (col_meta.metadata) {
              case 1:  // TINYTEXT
                buffer.push_back(static_cast<unsigned char>(str_len));
                break;
              case 2:  // TEXT
                buffer.push_back(str_len & 0xFF);
                buffer.push_back((str_len >> 8) & 0xFF);
                break;
              case 3:  // MEDIUMTEXT
                buffer.push_back(str_len & 0xFF);
                buffer.push_back((str_len >> 8) & 0xFF);
                buffer.push_back((str_len >> 16) & 0xFF);
                break;
              case 4:  // LONGTEXT
                buffer.push_back(str_len & 0xFF);
                buffer.push_back((str_len >> 8) & 0xFF);
                buffer.push_back((str_len >> 16) & 0xFF);
                buffer.push_back((str_len >> 24) & 0xFF);
                break;
            }
            for (char c : value) {
              buffer.push_back(static_cast<unsigned char>(c));
            }
            break;
          }

          default:
            // Unsupported type
            break;
        }
      }
    }

    // Add 4-byte checksum placeholder (required even when checksums disabled)
    // Parser expects: event_size = header + data + 4-byte checksum
    buffer.push_back(0);
    buffer.push_back(0);
    buffer.push_back(0);
    buffer.push_back(0);

    // Now fill in event_size at bytes [9-12] (total buffer size)
    uint32_t event_size = buffer.size();
    buffer[9] = event_size & 0xFF;
    buffer[10] = (event_size >> 8) & 0xFF;
    buffer[11] = (event_size >> 16) & 0xFF;
    buffer[12] = (event_size >> 24) & 0xFF;

    return buffer;
  }
};

TEST_F(RowsParserTest, ParseSimpleIntRow) {
  // Create table metadata
  TableMetadata table_meta;
  table_meta.table_id = 100;
  table_meta.database_name = "test_db";
  table_meta.table_name = "test_table";

  ColumnMetadata col_id;
  col_id.type = ColumnType::LONG;
  col_id.name = "id";
  col_id.metadata = 0;
  table_meta.columns.push_back(col_id);

  ColumnMetadata col_value;
  col_value.type = ColumnType::LONG;
  col_value.name = "value";
  col_value.metadata = 0;
  table_meta.columns.push_back(col_value);

  // Create event with one row: id=123, value=456
  std::vector<std::vector<std::string>> rows = {{"123", "456"}};
  auto buffer = CreateWriteRowsEvent(table_meta, rows);

  // Parse the event
  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "");

  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(1, result->size());

  const auto& row = result->front();
  EXPECT_EQ("123", row.primary_key);
  EXPECT_EQ("123", row.columns.at("id"));
  EXPECT_EQ("456", row.columns.at("value"));
}

TEST_F(RowsParserTest, ParseVarcharRow) {
  // Create table metadata
  TableMetadata table_meta;
  table_meta.table_id = 101;
  table_meta.database_name = "test_db";
  table_meta.table_name = "test_table";

  ColumnMetadata col_id;
  col_id.type = ColumnType::LONG;
  col_id.name = "id";
  col_id.metadata = 0;
  table_meta.columns.push_back(col_id);

  ColumnMetadata col_name;
  col_name.type = ColumnType::VARCHAR;
  col_name.name = "name";
  col_name.metadata = 255;  // max length 255 (1 byte length prefix)
  table_meta.columns.push_back(col_name);

  // Create event with one row: id=1, name="test"
  std::vector<std::vector<std::string>> rows = {{"1", "test"}};
  auto buffer = CreateWriteRowsEvent(table_meta, rows);

  // Parse the event
  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "name");

  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(1, result->size());

  const auto& row = result->front();
  EXPECT_EQ("1", row.primary_key);
  EXPECT_EQ("test", row.text);
  EXPECT_EQ("test", row.columns.at("name"));
}

TEST_F(RowsParserTest, ParseTextRow) {
  // Create table metadata
  TableMetadata table_meta;
  table_meta.table_id = 102;
  table_meta.database_name = "test_db";
  table_meta.table_name = "test_table";

  ColumnMetadata col_id;
  col_id.type = ColumnType::LONGLONG;
  col_id.name = "id";
  col_id.metadata = 0;
  table_meta.columns.push_back(col_id);

  ColumnMetadata col_content;
  col_content.type = ColumnType::BLOB;  // TEXT is stored as BLOB
  col_content.name = "content";
  col_content.metadata = 2;  // 2 byte length prefix (TEXT)
  table_meta.columns.push_back(col_content);

  // Create event with one row: id=100, content="Hello, World!"
  std::vector<std::vector<std::string>> rows = {{"100", "Hello, World!"}};
  auto buffer = CreateWriteRowsEvent(table_meta, rows);

  // Parse the event
  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "content");

  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(1, result->size());

  const auto& row = result->front();
  EXPECT_EQ("100", row.primary_key);
  EXPECT_EQ("Hello, World!", row.text);
  EXPECT_EQ("Hello, World!", row.columns.at("content"));
}

TEST_F(RowsParserTest, ParseMultipleRows) {
  // Create table metadata
  TableMetadata table_meta;
  table_meta.table_id = 103;
  table_meta.database_name = "test_db";
  table_meta.table_name = "test_table";

  ColumnMetadata col_id;
  col_id.type = ColumnType::LONG;
  col_id.name = "id";
  col_id.metadata = 0;
  table_meta.columns.push_back(col_id);

  ColumnMetadata col_name;
  col_name.type = ColumnType::VARCHAR;
  col_name.name = "name";
  col_name.metadata = 100;
  table_meta.columns.push_back(col_name);

  // Create event with three rows
  std::vector<std::vector<std::string>> rows = {{"1", "Alice"}, {"2", "Bob"}, {"3", "Charlie"}};
  auto buffer = CreateWriteRowsEvent(table_meta, rows);

  // Parse the event
  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "name");

  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(3, result->size());

  EXPECT_EQ("1", (*result)[0].primary_key);
  EXPECT_EQ("Alice", (*result)[0].text);

  EXPECT_EQ("2", (*result)[1].primary_key);
  EXPECT_EQ("Bob", (*result)[1].text);

  EXPECT_EQ("3", (*result)[2].primary_key);
  EXPECT_EQ("Charlie", (*result)[2].text);
}

TEST_F(RowsParserTest, ParseInvalidBuffer) {
  TableMetadata table_meta;
  table_meta.table_id = 104;
  table_meta.columns.resize(2);

  // Parse with nullptr buffer
  auto result1 = ParseWriteRowsEvent(nullptr, 100, &table_meta, "id", "");
  EXPECT_FALSE(result1.has_value());

  // Parse with nullptr table_meta
  unsigned char dummy_buffer[100] = {0};
  auto result2 = ParseWriteRowsEvent(dummy_buffer, 100, nullptr, "id", "");
  EXPECT_FALSE(result2.has_value());

  // Parse with too short buffer (less than post-header)
  auto result3 = ParseWriteRowsEvent(dummy_buffer, 10, &table_meta, "id", "");
  EXPECT_FALSE(result3.has_value());
}

// Test ExtractFilters function
TEST_F(RowsParserTest, ExtractFiltersBasic) {
  RowData row_data;
  row_data.primary_key = "123";
  row_data.columns["status"] = "1";
  row_data.columns["category"] = "tech";
  row_data.columns["count"] = "42";

  std::vector<mygramdb::config::FilterConfig> filter_configs;
  filter_configs.push_back({"status", "tinyint_unsigned", false, false, ""});
  filter_configs.push_back({"category", "string", false, false, ""});
  filter_configs.push_back({"count", "int", false, false, ""});

  auto filters = ExtractFilters(row_data, filter_configs);

  EXPECT_EQ(filters.size(), 3);
  EXPECT_TRUE(std::holds_alternative<uint8_t>(filters["status"]));
  EXPECT_EQ(std::get<uint8_t>(filters["status"]), 1);
  EXPECT_TRUE(std::holds_alternative<std::string>(filters["category"]));
  EXPECT_EQ(std::get<std::string>(filters["category"]), "tech");
  EXPECT_TRUE(std::holds_alternative<int32_t>(filters["count"]));
  EXPECT_EQ(std::get<int32_t>(filters["count"]), 42);
}

TEST_F(RowsParserTest, ExtractFiltersWithNullValues) {
  RowData row_data;
  row_data.primary_key = "123";
  row_data.columns["status"] = "";  // NULL value
  row_data.columns["category"] = "tech";

  std::vector<mygramdb::config::FilterConfig> filter_configs;
  filter_configs.push_back({"status", "int", false, false, ""});
  filter_configs.push_back({"category", "string", false, false, ""});

  auto filters = ExtractFilters(row_data, filter_configs);

  // NULL values should be skipped
  EXPECT_EQ(filters.size(), 1);
  EXPECT_TRUE(filters.find("status") == filters.end());
  EXPECT_TRUE(filters.find("category") != filters.end());
  EXPECT_EQ(std::get<std::string>(filters["category"]), "tech");
}

TEST_F(RowsParserTest, ExtractFiltersMissingColumn) {
  RowData row_data;
  row_data.primary_key = "123";
  row_data.columns["status"] = "1";

  std::vector<mygramdb::config::FilterConfig> filter_configs;
  filter_configs.push_back({"status", "int", false, false, ""});
  filter_configs.push_back({"missing_col", "string", false, false, ""});  // Column not in row data

  auto filters = ExtractFilters(row_data, filter_configs);

  // Should only extract existing columns
  EXPECT_EQ(filters.size(), 1);
  EXPECT_TRUE(filters.find("status") != filters.end());
  EXPECT_TRUE(filters.find("missing_col") == filters.end());
}

TEST_F(RowsParserTest, ExtractFiltersInvalidTypeConversion) {
  RowData row_data;
  row_data.primary_key = "123";
  row_data.columns["count"] = "invalid_number";  // Invalid integer string

  std::vector<mygramdb::config::FilterConfig> filter_configs;
  filter_configs.push_back({"count", "int", false, false, ""});

  auto filters = ExtractFilters(row_data, filter_configs);

  // Invalid conversion should be skipped (exception caught internally)
  EXPECT_EQ(filters.size(), 0);
}

TEST_F(RowsParserTest, ExtractFiltersAllTypes) {
  RowData row_data;
  row_data.primary_key = "123";
  row_data.columns["bool_col"] = "1";
  row_data.columns["tinyint_col"] = "-128";
  row_data.columns["tinyint_u_col"] = "255";
  row_data.columns["smallint_col"] = "-32768";
  row_data.columns["smallint_u_col"] = "65535";
  row_data.columns["int_col"] = "-2147483648";
  row_data.columns["int_u_col"] = "4294967295";
  row_data.columns["bigint_col"] = "-9223372036854775808";
  row_data.columns["float_col"] = "3.14";
  row_data.columns["string_col"] = "test";

  std::vector<mygramdb::config::FilterConfig> filter_configs;
  filter_configs.push_back({"bool_col", "boolean", false, false, ""});
  filter_configs.push_back({"tinyint_col", "tinyint", false, false, ""});
  filter_configs.push_back({"tinyint_u_col", "tinyint_unsigned", false, false, ""});
  filter_configs.push_back({"smallint_col", "smallint", false, false, ""});
  filter_configs.push_back({"smallint_u_col", "smallint_unsigned", false, false, ""});
  filter_configs.push_back({"int_col", "int", false, false, ""});
  filter_configs.push_back({"int_u_col", "int_unsigned", false, false, ""});
  filter_configs.push_back({"bigint_col", "bigint", false, false, ""});
  filter_configs.push_back({"float_col", "float", false, false, ""});
  filter_configs.push_back({"string_col", "string", false, false, ""});

  auto filters = ExtractFilters(row_data, filter_configs);

  EXPECT_EQ(filters.size(), 10);
  EXPECT_EQ(std::get<bool>(filters["bool_col"]), true);
  EXPECT_EQ(std::get<int8_t>(filters["tinyint_col"]), -128);
  EXPECT_EQ(std::get<uint8_t>(filters["tinyint_u_col"]), 255);
  EXPECT_EQ(std::get<int16_t>(filters["smallint_col"]), -32768);
  EXPECT_EQ(std::get<uint16_t>(filters["smallint_u_col"]), 65535);
  EXPECT_EQ(std::get<int32_t>(filters["int_col"]), -2147483648);
  EXPECT_EQ(std::get<uint32_t>(filters["int_u_col"]), 4294967295U);
  EXPECT_EQ(std::get<int64_t>(filters["bigint_col"]), -9223372036854775807LL - 1);
  EXPECT_NEAR(std::get<double>(filters["float_col"]), 3.14, 0.01);
  EXPECT_EQ(std::get<std::string>(filters["string_col"]), "test");
}

#endif  // USE_MYSQL
