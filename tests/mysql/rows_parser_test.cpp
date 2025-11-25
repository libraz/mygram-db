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

// =============================================================================
// Date/Time Type Parsing Tests
// =============================================================================

/**
 * @brief Test fixture for datetime/time parsing
 *
 * Tests the MySQL DATETIME2/TIME2/TIMESTAMP2/DATE parsing implementation
 * based on MySQL 8.4.7 source code (mysys/my_time.cc).
 */
class DateTimeParsingTest : public RowsParserTest {
 protected:
  /**
   * @brief Encode DATETIME2 value to MySQL binary format
   *
   * Based on MySQL source mysys/my_time.cc:
   * - DATETIMEF_INT_OFS = 0x8000000000LL
   * - Packed format: (year * 13 + month) << 22 | day << 17 | hour << 12 | minute << 6 | second
   * - 5 bytes, big-endian
   */
  std::vector<unsigned char> EncodeDatetime2(unsigned int year, unsigned int month, unsigned int day, unsigned int hour,
                                             unsigned int minute, unsigned int second, uint8_t precision = 0,
                                             uint32_t microseconds = 0) {
    std::vector<unsigned char> result;

    // Calculate packed datetime value
    int64_t ym = static_cast<int64_t>(year) * 13 + month;
    int64_t ymd = (ym << 5) | day;
    int64_t hms = (static_cast<int64_t>(hour) << 12) | (minute << 6) | second;
    int64_t intpart = (ymd << 17) | hms;

    // Add offset (DATETIMEF_INT_OFS)
    constexpr int64_t kDatetimeIntOfs = 0x8000000000LL;
    uint64_t packed = static_cast<uint64_t>(intpart + kDatetimeIntOfs);

    // Write 5 bytes in big-endian
    result.push_back((packed >> 32) & 0xFF);
    result.push_back((packed >> 24) & 0xFF);
    result.push_back((packed >> 16) & 0xFF);
    result.push_back((packed >> 8) & 0xFF);
    result.push_back(packed & 0xFF);

    // Add fractional seconds if precision > 0
    if (precision > 0) {
      int frac_bytes = (precision + 1) / 2;
      uint32_t frac = microseconds;
      // Convert microseconds to the appropriate precision
      switch (precision) {
        case 1:
          frac = microseconds / 100000;
          break;
        case 2:
          frac = microseconds / 10000;
          break;
        case 3:
          frac = microseconds / 1000;
          break;
        case 4:
          frac = microseconds / 100;
          break;
        case 5:
          frac = microseconds / 10;
          break;
        case 6:
          frac = microseconds;
          break;
      }
      // Write fractional bytes in big-endian
      for (int i = frac_bytes - 1; i >= 0; i--) {
        result.push_back((frac >> (i * 8)) & 0xFF);
      }
    }

    return result;
  }

  /**
   * @brief Encode TIME2 value to MySQL binary format
   *
   * Based on MySQL source mysys/my_time.cc:
   * - TIMEF_INT_OFS = 0x800000
   * - Packed format: hour << 12 | minute << 6 | second
   * - 3 bytes, big-endian
   */
  std::vector<unsigned char> EncodeTime2(unsigned int hour, unsigned int minute, unsigned int second,
                                         bool negative = false, uint8_t precision = 0, uint32_t microseconds = 0) {
    std::vector<unsigned char> result;

    // Calculate packed time value
    int32_t intpart = (static_cast<int32_t>(hour) << 12) | (minute << 6) | second;
    if (negative) {
      intpart = -intpart;
    }

    // Add offset (TIMEF_INT_OFS)
    constexpr int32_t kTimefIntOfs = 0x800000;
    uint32_t packed = static_cast<uint32_t>(intpart + kTimefIntOfs);

    // Write 3 bytes in big-endian
    result.push_back((packed >> 16) & 0xFF);
    result.push_back((packed >> 8) & 0xFF);
    result.push_back(packed & 0xFF);

    // Add fractional seconds if precision > 0
    if (precision > 0) {
      int frac_bytes = (precision + 1) / 2;
      uint32_t frac = microseconds;
      switch (precision) {
        case 1:
          frac = microseconds / 100000;
          break;
        case 2:
          frac = microseconds / 10000;
          break;
        case 3:
          frac = microseconds / 1000;
          break;
        case 4:
          frac = microseconds / 100;
          break;
        case 5:
          frac = microseconds / 10;
          break;
        case 6:
          frac = microseconds;
          break;
      }
      for (int i = frac_bytes - 1; i >= 0; i--) {
        result.push_back((frac >> (i * 8)) & 0xFF);
      }
    }

    return result;
  }

  /**
   * @brief Encode TIMESTAMP2 value to MySQL binary format
   *
   * 4 bytes for seconds (big-endian) + fractional seconds
   */
  std::vector<unsigned char> EncodeTimestamp2(uint32_t timestamp, uint8_t precision = 0, uint32_t microseconds = 0) {
    std::vector<unsigned char> result;

    // Write 4 bytes in big-endian
    result.push_back((timestamp >> 24) & 0xFF);
    result.push_back((timestamp >> 16) & 0xFF);
    result.push_back((timestamp >> 8) & 0xFF);
    result.push_back(timestamp & 0xFF);

    // Add fractional seconds if precision > 0
    if (precision > 0) {
      int frac_bytes = (precision + 1) / 2;
      uint32_t frac = microseconds;
      switch (precision) {
        case 1:
          frac = microseconds / 100000;
          break;
        case 2:
          frac = microseconds / 10000;
          break;
        case 3:
          frac = microseconds / 1000;
          break;
        case 4:
          frac = microseconds / 100;
          break;
        case 5:
          frac = microseconds / 10;
          break;
        case 6:
          frac = microseconds;
          break;
      }
      for (int i = frac_bytes - 1; i >= 0; i--) {
        result.push_back((frac >> (i * 8)) & 0xFF);
      }
    }

    return result;
  }

  /**
   * @brief Encode DATE value to MySQL binary format
   *
   * 3 bytes: | year (14 bits) | month (4 bits) | day (5 bits) |
   */
  std::vector<unsigned char> EncodeDate(unsigned int year, unsigned int month, unsigned int day) {
    std::vector<unsigned char> result;

    uint32_t val = (year << 9) | (month << 5) | day;
    result.push_back(val & 0xFF);
    result.push_back((val >> 8) & 0xFF);
    result.push_back((val >> 16) & 0xFF);

    return result;
  }

  /**
   * @brief Encode TIME (old format) value to MySQL binary format
   *
   * 3 bytes: HHMMSS as integer
   */
  std::vector<unsigned char> EncodeTime(unsigned int hour, unsigned int minute, unsigned int second) {
    std::vector<unsigned char> result;

    uint32_t val = hour * 10000 + minute * 100 + second;
    result.push_back(val & 0xFF);
    result.push_back((val >> 8) & 0xFF);
    result.push_back((val >> 16) & 0xFF);

    return result;
  }

  /**
   * @brief Create a WRITE_ROWS event with datetime column
   */
  std::vector<unsigned char> CreateDateTimeEvent(const TableMetadata& table_meta,
                                                 const std::vector<unsigned char>& datetime_bytes) {
    std::vector<unsigned char> buffer;

    // Common header (19 bytes)
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

    // Body: column count (packed integer) - 2 columns (id + datetime)
    buffer.push_back(2);

    // Body: columns_present bitmap (all columns present)
    buffer.push_back(0xFF);

    // Row: NULL bitmap (no NULLs)
    buffer.push_back(0x00);

    // Row data: id column (4 bytes INT)
    int32_t id_val = 1;
    buffer.push_back(id_val & 0xFF);
    buffer.push_back((id_val >> 8) & 0xFF);
    buffer.push_back((id_val >> 16) & 0xFF);
    buffer.push_back((id_val >> 24) & 0xFF);

    // Row data: datetime column
    for (unsigned char b : datetime_bytes) {
      buffer.push_back(b);
    }

    // 4-byte checksum placeholder
    buffer.push_back(0);
    buffer.push_back(0);
    buffer.push_back(0);
    buffer.push_back(0);

    // Fill in event_size at bytes [9-12]
    uint32_t event_size = buffer.size();
    buffer[9] = event_size & 0xFF;
    buffer[10] = (event_size >> 8) & 0xFF;
    buffer[11] = (event_size >> 16) & 0xFF;
    buffer[12] = (event_size >> 24) & 0xFF;

    return buffer;
  }

  /**
   * @brief Create table metadata with id + datetime column
   */
  TableMetadata CreateDateTimeTableMeta(ColumnType datetime_type, uint16_t metadata = 0) {
    TableMetadata table_meta;
    table_meta.table_id = 200;
    table_meta.database_name = "test_db";
    table_meta.table_name = "datetime_test";

    ColumnMetadata col_id;
    col_id.type = ColumnType::LONG;
    col_id.name = "id";
    col_id.metadata = 0;
    table_meta.columns.push_back(col_id);

    ColumnMetadata col_dt;
    col_dt.type = datetime_type;
    col_dt.name = "dt_col";
    col_dt.metadata = metadata;
    table_meta.columns.push_back(col_dt);

    return table_meta;
  }
};

/**
 * @test DATETIME2 parsing - basic date parsing
 *
 * Verifies that DATETIME2 values are correctly parsed.
 * This was the main bug fix: the offset subtraction and ym/13 calculation.
 */
TEST_F(DateTimeParsingTest, Datetime2BasicParsing) {
  // Test case: 2025-11-25 14:30:45
  auto datetime_bytes = EncodeDatetime2(2025, 11, 25, 14, 30, 45);
  auto table_meta = CreateDateTimeTableMeta(ColumnType::DATETIME2, 0);
  auto buffer = CreateDateTimeEvent(table_meta, datetime_bytes);

  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "");

  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(1, result->size());

  const auto& row = result->front();
  EXPECT_EQ("2025-11-25 14:30:45", row.columns.at("dt_col"));
}

/**
 * @test DATETIME2 parsing - edge case: year boundary
 */
TEST_F(DateTimeParsingTest, Datetime2YearBoundary) {
  // Test case: 2000-01-01 00:00:00 (Y2K)
  auto datetime_bytes = EncodeDatetime2(2000, 1, 1, 0, 0, 0);
  auto table_meta = CreateDateTimeTableMeta(ColumnType::DATETIME2, 0);
  auto buffer = CreateDateTimeEvent(table_meta, datetime_bytes);

  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "");

  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(1, result->size());
  EXPECT_EQ("2000-01-01 00:00:00", result->front().columns.at("dt_col"));
}

/**
 * @test DATETIME2 parsing - edge case: max time values
 */
TEST_F(DateTimeParsingTest, Datetime2MaxTimeValues) {
  // Test case: 2023-12-31 23:59:59
  auto datetime_bytes = EncodeDatetime2(2023, 12, 31, 23, 59, 59);
  auto table_meta = CreateDateTimeTableMeta(ColumnType::DATETIME2, 0);
  auto buffer = CreateDateTimeEvent(table_meta, datetime_bytes);

  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "");

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ("2023-12-31 23:59:59", result->front().columns.at("dt_col"));
}

/**
 * @test DATETIME2 parsing - with fractional seconds (precision 6)
 */
TEST_F(DateTimeParsingTest, Datetime2WithMicroseconds) {
  // Test case: 2025-06-15 10:20:30.123456
  auto datetime_bytes = EncodeDatetime2(2025, 6, 15, 10, 20, 30, 6, 123456);
  auto table_meta = CreateDateTimeTableMeta(ColumnType::DATETIME2, 6);
  auto buffer = CreateDateTimeEvent(table_meta, datetime_bytes);

  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "");

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ("2025-06-15 10:20:30.123456", result->front().columns.at("dt_col"));
}

/**
 * @test DATETIME2 parsing - with fractional seconds (precision 3)
 */
TEST_F(DateTimeParsingTest, Datetime2WithMilliseconds) {
  // Test case: 2025-06-15 10:20:30.123 (precision 3 = milliseconds)
  auto datetime_bytes = EncodeDatetime2(2025, 6, 15, 10, 20, 30, 3, 123000);
  auto table_meta = CreateDateTimeTableMeta(ColumnType::DATETIME2, 3);
  auto buffer = CreateDateTimeEvent(table_meta, datetime_bytes);

  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "");

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ("2025-06-15 10:20:30.123000", result->front().columns.at("dt_col"));
}

/**
 * @test TIME2 parsing - basic time
 */
TEST_F(DateTimeParsingTest, Time2BasicParsing) {
  // Test case: 14:30:45
  auto time_bytes = EncodeTime2(14, 30, 45);
  auto table_meta = CreateDateTimeTableMeta(ColumnType::TIME2, 0);
  auto buffer = CreateDateTimeEvent(table_meta, time_bytes);

  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "");

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ("14:30:45", result->front().columns.at("dt_col"));
}

/**
 * @test TIME2 parsing - with microseconds
 */
TEST_F(DateTimeParsingTest, Time2WithMicroseconds) {
  // Test case: 10:20:30.654321
  auto time_bytes = EncodeTime2(10, 20, 30, false, 6, 654321);
  auto table_meta = CreateDateTimeTableMeta(ColumnType::TIME2, 6);
  auto buffer = CreateDateTimeEvent(table_meta, time_bytes);

  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "");

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ("10:20:30.654321", result->front().columns.at("dt_col"));
}

/**
 * @test TIME2 parsing - max hour value
 */
TEST_F(DateTimeParsingTest, Time2MaxHour) {
  // Test case: 838:59:59 (MySQL TIME max is 838:59:59)
  auto time_bytes = EncodeTime2(838, 59, 59);
  auto table_meta = CreateDateTimeTableMeta(ColumnType::TIME2, 0);
  auto buffer = CreateDateTimeEvent(table_meta, time_bytes);

  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "");

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ("838:59:59", result->front().columns.at("dt_col"));
}

/**
 * @test TIME (old format) parsing
 */
TEST_F(DateTimeParsingTest, TimeOldFormat) {
  // Test case: 12:34:56
  auto time_bytes = EncodeTime(12, 34, 56);
  auto table_meta = CreateDateTimeTableMeta(ColumnType::TIME, 0);
  auto buffer = CreateDateTimeEvent(table_meta, time_bytes);

  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "");

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ("12:34:56", result->front().columns.at("dt_col"));
}

/**
 * @test TIMESTAMP2 parsing - basic
 */
TEST_F(DateTimeParsingTest, Timestamp2BasicParsing) {
  // Test case: Unix timestamp 1732545600 (2024-11-25 12:00:00 UTC)
  auto timestamp_bytes = EncodeTimestamp2(1732545600);
  auto table_meta = CreateDateTimeTableMeta(ColumnType::TIMESTAMP2, 0);
  auto buffer = CreateDateTimeEvent(table_meta, timestamp_bytes);

  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "");

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ("1732545600", result->front().columns.at("dt_col"));
}

/**
 * @test TIMESTAMP2 parsing - with microseconds
 */
TEST_F(DateTimeParsingTest, Timestamp2WithMicroseconds) {
  // Test case: 1732545600.123456
  auto timestamp_bytes = EncodeTimestamp2(1732545600, 6, 123456);
  auto table_meta = CreateDateTimeTableMeta(ColumnType::TIMESTAMP2, 6);
  auto buffer = CreateDateTimeEvent(table_meta, timestamp_bytes);

  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "");

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ("1732545600.123456", result->front().columns.at("dt_col"));
}

/**
 * @test DATE parsing
 */
TEST_F(DateTimeParsingTest, DateParsing) {
  // Test case: 2025-11-25
  auto date_bytes = EncodeDate(2025, 11, 25);
  auto table_meta = CreateDateTimeTableMeta(ColumnType::DATE, 0);
  auto buffer = CreateDateTimeEvent(table_meta, date_bytes);

  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "");

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ("2025-11-25", result->front().columns.at("dt_col"));
}

/**
 * @test DATE parsing - leap year
 */
TEST_F(DateTimeParsingTest, DateLeapYear) {
  // Test case: 2024-02-29 (leap year)
  auto date_bytes = EncodeDate(2024, 2, 29);
  auto table_meta = CreateDateTimeTableMeta(ColumnType::DATE, 0);
  auto buffer = CreateDateTimeEvent(table_meta, date_bytes);

  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "");

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ("2024-02-29", result->front().columns.at("dt_col"));
}

/**
 * @test DATETIME2 bug reproduction - the original bug case
 *
 * This test reproduces the bug where 2025-11-25 14:30:00 was being
 * parsed as 0110-00-25 14:30:00 due to missing offset and wrong
 * bit extraction (was using bitwise instead of ym/13, ym%13).
 */
TEST_F(DateTimeParsingTest, Datetime2BugReproduction) {
  // The exact case from the bug report
  auto datetime_bytes = EncodeDatetime2(2025, 11, 25, 14, 30, 0);
  auto table_meta = CreateDateTimeTableMeta(ColumnType::DATETIME2, 0);
  auto buffer = CreateDateTimeEvent(table_meta, datetime_bytes);

  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "");

  ASSERT_TRUE(result.has_value());
  // Before fix: was "0110-00-25 14:30:00" (year=110, month=0)
  // After fix: should be "2025-11-25 14:30:00"
  EXPECT_EQ("2025-11-25 14:30:00", result->front().columns.at("dt_col"));
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
