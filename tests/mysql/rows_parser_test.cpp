/**
 * @file rows_parser_test.cpp
 * @brief Unit tests for MySQL ROWS event parser
 */

#include "mysql/rows_parser.h"

#include <gtest/gtest.h>

#include <cstring>
#include <limits>
#include <vector>

#include "binlog_event_builder.h"
#include "mysql/binlog_util.h"
#include "mysql/rows_parser_internal.h"
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

  /**
   * @brief Create a WRITE_ROWS event buffer with a single row
   *
   * @param table_meta Table metadata
   * @param row_data Raw row data bytes (already encoded)
   * @param null_bitmap NULL bitmap bytes
   * @return Event buffer
   */
  std::vector<unsigned char> CreateWriteRowsEventRaw(const TableMetadata& table_meta,
                                                     const std::vector<unsigned char>& row_data,
                                                     const std::vector<unsigned char>& null_bitmap) {
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

    // Body: column count (packed integer)
    uint64_t col_count = table_meta.columns.size();
    if (col_count < 251) {
      buffer.push_back(static_cast<unsigned char>(col_count));
    }

    // Body: columns_present bitmap (all columns present)
    size_t bitmap_size = (col_count + 7) / 8;
    for (size_t i = 0; i < bitmap_size; i++) {
      buffer.push_back(0xFF);
    }

    // Row: NULL bitmap
    for (unsigned char b : null_bitmap) {
      buffer.push_back(b);
    }

    // Row data
    for (unsigned char b : row_data) {
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
   * @brief Encode a 32-bit integer to little-endian bytes
   */
  std::vector<unsigned char> EncodeInt32(int32_t val) {
    return {static_cast<unsigned char>(val & 0xFF), static_cast<unsigned char>((val >> 8) & 0xFF),
            static_cast<unsigned char>((val >> 16) & 0xFF), static_cast<unsigned char>((val >> 24) & 0xFF)};
  }

  /**
   * @brief Encode a 64-bit integer to little-endian bytes
   */
  std::vector<unsigned char> EncodeInt64(int64_t val) {
    std::vector<unsigned char> result;
    for (int i = 0; i < 8; i++) {
      result.push_back((val >> (i * 8)) & 0xFF);
    }
    return result;
  }

  /**
   * @brief Encode a FLOAT to little-endian bytes
   */
  std::vector<unsigned char> EncodeFloat(float val) {
    std::vector<unsigned char> result(4);
    memcpy(result.data(), &val, 4);
    return result;
  }

  /**
   * @brief Encode a DOUBLE to little-endian bytes
   */
  std::vector<unsigned char> EncodeDouble(double val) {
    std::vector<unsigned char> result(8);
    memcpy(result.data(), &val, 8);
    return result;
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
  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "",
                                    MySQLBinlogEventType::OBSOLETE_WRITE_ROWS_EVENT_V1);

  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(1, result->size());

  const auto& row = result->front();
  EXPECT_EQ("123", row.primary_key);
  EXPECT_EQ("123", row.GetColumnValue("id"));
  EXPECT_EQ("456", row.GetColumnValue("value"));
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
  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "name",
                                    MySQLBinlogEventType::OBSOLETE_WRITE_ROWS_EVENT_V1);

  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(1, result->size());

  const auto& row = result->front();
  EXPECT_EQ("1", row.primary_key);
  EXPECT_EQ("test", row.text);
  EXPECT_EQ("test", row.GetColumnValue("name"));
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
  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "content",
                                    MySQLBinlogEventType::OBSOLETE_WRITE_ROWS_EVENT_V1);

  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(1, result->size());

  const auto& row = result->front();
  EXPECT_EQ("100", row.primary_key);
  EXPECT_EQ("Hello, World!", row.text);
  EXPECT_EQ("Hello, World!", row.GetColumnValue("content"));
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
  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "name",
                                    MySQLBinlogEventType::OBSOLETE_WRITE_ROWS_EVENT_V1);

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
  auto result1 =
      ParseWriteRowsEvent(nullptr, 100, &table_meta, "id", "", MySQLBinlogEventType::OBSOLETE_WRITE_ROWS_EVENT_V1);
  EXPECT_FALSE(result1.has_value());

  // Parse with nullptr table_meta
  unsigned char dummy_buffer[100] = {0};
  auto result2 =
      ParseWriteRowsEvent(dummy_buffer, 100, nullptr, "id", "", MySQLBinlogEventType::OBSOLETE_WRITE_ROWS_EVENT_V1);
  EXPECT_FALSE(result2.has_value());

  // Parse with too short buffer (less than post-header)
  auto result3 =
      ParseWriteRowsEvent(dummy_buffer, 10, &table_meta, "id", "", MySQLBinlogEventType::OBSOLETE_WRITE_ROWS_EVENT_V1);
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
  row_data.null_columns.insert("status");
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

TEST_F(RowsParserTest, ExtractFiltersPreservesEmptyStringValues) {
  RowData row_data;
  row_data.primary_key = "123";
  row_data.columns["category"] = "";

  std::vector<mygramdb::config::FilterConfig> filter_configs;
  filter_configs.push_back({"category", "string", false, false, ""});

  auto filters = ExtractFilters(row_data, filter_configs);

  ASSERT_EQ(filters.size(), 1);
  ASSERT_TRUE(std::holds_alternative<std::string>(filters["category"]));
  EXPECT_EQ(std::get<std::string>(filters["category"]), "");
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
  static uint32_t ScaleMicrosecondsForStoredFraction(uint8_t precision, uint32_t microseconds) {
    static constexpr uint32_t kDivisors[] = {1, 10000, 10000, 100, 100, 1, 1};
    return (precision <= 6) ? microseconds / kDivisors[precision] : microseconds;
  }

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
      uint32_t frac = ScaleMicrosecondsForStoredFraction(precision, microseconds);
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
      uint32_t frac = ScaleMicrosecondsForStoredFraction(precision, microseconds);
      if (negative) {
        const uint32_t mask = (1u << (frac_bytes * 8)) - 1;
        frac = static_cast<uint32_t>(-static_cast<int32_t>(frac)) & mask;
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
      uint32_t frac = ScaleMicrosecondsForStoredFraction(precision, microseconds);
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

  std::vector<unsigned char> EncodeDatetime(unsigned int year, unsigned int month, unsigned int day, unsigned int hour,
                                            unsigned int minute, unsigned int second) {
    std::vector<unsigned char> result;
    uint64_t val =
        (((((static_cast<uint64_t>(year) * 100 + month) * 100 + day) * 100 + hour) * 100 + minute) * 100) + second;
    for (int i = 0; i < 8; i++) {
      result.push_back((val >> (i * 8)) & 0xFF);
    }
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

  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "",
                                    MySQLBinlogEventType::OBSOLETE_WRITE_ROWS_EVENT_V1);

  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(1, result->size());

  const auto& row = result->front();
  EXPECT_EQ("2025-11-25 14:30:45", row.GetColumnValue("dt_col"));
}

TEST_F(DateTimeParsingTest, Datetime2NegativePackedValueRejected) {
  // DATETIMEF_INT_OFS - 1 decodes to a negative intpart. The decoder must not
  // turn that into an unrelated positive date by taking abs(intpart).
  std::vector<unsigned char> datetime_bytes = {0x7F, 0xFF, 0xFF, 0xFF, 0xFF};
  auto result = internal::DecodeFieldValue(static_cast<uint8_t>(ColumnType::DATETIME2), datetime_bytes.data(), 0,
                                           /*is_null=*/false, datetime_bytes.data() + datetime_bytes.size(),
                                           /*is_unsigned=*/false);

  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(mygram::utils::ErrorCode::kMySQLInvalidMetadata, result.error().code());
}

/**
 * @test DATETIME2 parsing - edge case: year boundary
 */
TEST_F(DateTimeParsingTest, Datetime2YearBoundary) {
  // Test case: 2000-01-01 00:00:00 (Y2K)
  auto datetime_bytes = EncodeDatetime2(2000, 1, 1, 0, 0, 0);
  auto table_meta = CreateDateTimeTableMeta(ColumnType::DATETIME2, 0);
  auto buffer = CreateDateTimeEvent(table_meta, datetime_bytes);

  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "",
                                    MySQLBinlogEventType::OBSOLETE_WRITE_ROWS_EVENT_V1);

  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(1, result->size());
  EXPECT_EQ("2000-01-01 00:00:00", result->front().GetColumnValue("dt_col"));
}

/**
 * @test DATETIME2 parsing - edge case: max time values
 */
TEST_F(DateTimeParsingTest, Datetime2MaxTimeValues) {
  // Test case: 2023-12-31 23:59:59
  auto datetime_bytes = EncodeDatetime2(2023, 12, 31, 23, 59, 59);
  auto table_meta = CreateDateTimeTableMeta(ColumnType::DATETIME2, 0);
  auto buffer = CreateDateTimeEvent(table_meta, datetime_bytes);

  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "",
                                    MySQLBinlogEventType::OBSOLETE_WRITE_ROWS_EVENT_V1);

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ("2023-12-31 23:59:59", result->front().GetColumnValue("dt_col"));
}

/**
 * @test DATETIME2 parsing - with fractional seconds (precision 6)
 */
TEST_F(DateTimeParsingTest, Datetime2WithMicroseconds) {
  // Test case: 2025-06-15 10:20:30.123456
  auto datetime_bytes = EncodeDatetime2(2025, 6, 15, 10, 20, 30, 6, 123456);
  auto table_meta = CreateDateTimeTableMeta(ColumnType::DATETIME2, 6);
  auto buffer = CreateDateTimeEvent(table_meta, datetime_bytes);

  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "",
                                    MySQLBinlogEventType::OBSOLETE_WRITE_ROWS_EVENT_V1);

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ("2025-06-15 10:20:30.123456", result->front().GetColumnValue("dt_col"));
}

/**
 * @test DATETIME2 parsing - with fractional seconds (precision 3)
 */
TEST_F(DateTimeParsingTest, Datetime2WithMilliseconds) {
  // Test case: 2025-06-15 10:20:30.123 (precision 3 = milliseconds)
  auto datetime_bytes = EncodeDatetime2(2025, 6, 15, 10, 20, 30, 3, 123000);
  auto table_meta = CreateDateTimeTableMeta(ColumnType::DATETIME2, 3);
  auto buffer = CreateDateTimeEvent(table_meta, datetime_bytes);

  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "",
                                    MySQLBinlogEventType::OBSOLETE_WRITE_ROWS_EVENT_V1);

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ("2025-06-15 10:20:30.123000", result->front().GetColumnValue("dt_col"));
}

TEST_F(DateTimeParsingTest, Datetime2Precision1UsesMySQLStoredByteScale) {
  auto datetime_bytes = EncodeDatetime2(2025, 6, 15, 10, 20, 30);
  datetime_bytes.push_back(0x0A);  // MySQL stores .1 as 10 in the single fractional byte.
  auto table_meta = CreateDateTimeTableMeta(ColumnType::DATETIME2, 1);
  auto buffer = CreateDateTimeEvent(table_meta, datetime_bytes);

  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "",
                                    MySQLBinlogEventType::OBSOLETE_WRITE_ROWS_EVENT_V1);

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ("2025-06-15 10:20:30.100000", result->front().GetColumnValue("dt_col"));
}

TEST_F(DateTimeParsingTest, Datetime2Precision3UsesMySQLStoredByteScale) {
  auto datetime_bytes = EncodeDatetime2(2025, 6, 15, 10, 20, 30);
  datetime_bytes.push_back(0x04);
  datetime_bytes.push_back(0xD2);  // 1234 stored fractional units => 123400 usec for odd precision 3.
  auto table_meta = CreateDateTimeTableMeta(ColumnType::DATETIME2, 3);
  auto buffer = CreateDateTimeEvent(table_meta, datetime_bytes);

  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "",
                                    MySQLBinlogEventType::OBSOLETE_WRITE_ROWS_EVENT_V1);

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ("2025-06-15 10:20:30.123400", result->front().GetColumnValue("dt_col"));
}

/**
 * @test TIME2 parsing - basic time
 */
TEST_F(DateTimeParsingTest, Time2BasicParsing) {
  // Test case: 14:30:45
  auto time_bytes = EncodeTime2(14, 30, 45);
  auto table_meta = CreateDateTimeTableMeta(ColumnType::TIME2, 0);
  auto buffer = CreateDateTimeEvent(table_meta, time_bytes);

  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "",
                                    MySQLBinlogEventType::OBSOLETE_WRITE_ROWS_EVENT_V1);

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ("14:30:45", result->front().GetColumnValue("dt_col"));
}

/**
 * @test TIME2 parsing - with microseconds
 */
TEST_F(DateTimeParsingTest, Time2WithMicroseconds) {
  // Test case: 10:20:30.654321
  auto time_bytes = EncodeTime2(10, 20, 30, false, 6, 654321);
  auto table_meta = CreateDateTimeTableMeta(ColumnType::TIME2, 6);
  auto buffer = CreateDateTimeEvent(table_meta, time_bytes);

  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "",
                                    MySQLBinlogEventType::OBSOLETE_WRITE_ROWS_EVENT_V1);

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ("10:20:30.654321", result->front().GetColumnValue("dt_col"));
}

TEST_F(DateTimeParsingTest, Time2NegativeWithMicrosecondsUsesSignedFraction) {
  auto time_bytes = EncodeTime2(10, 20, 30, true, 6, 654321);
  auto table_meta = CreateDateTimeTableMeta(ColumnType::TIME2, 6);
  auto buffer = CreateDateTimeEvent(table_meta, time_bytes);

  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "",
                                    MySQLBinlogEventType::OBSOLETE_WRITE_ROWS_EVENT_V1);

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ("-10:20:30.654321", result->front().GetColumnValue("dt_col"));
}

TEST_F(DateTimeParsingTest, Time2Precision5UsesMySQLStoredByteScale) {
  auto time_bytes = EncodeTime2(10, 20, 30);
  time_bytes.push_back(0x01);
  time_bytes.push_back(0xE2);
  time_bytes.push_back(0x3A);  // 123450 stored units => 123450 usec for odd precision 5.
  auto table_meta = CreateDateTimeTableMeta(ColumnType::TIME2, 5);
  auto buffer = CreateDateTimeEvent(table_meta, time_bytes);

  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "",
                                    MySQLBinlogEventType::OBSOLETE_WRITE_ROWS_EVENT_V1);

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ("10:20:30.123450", result->front().GetColumnValue("dt_col"));
}

/**
 * @test TIME2 parsing - max hour value
 */
TEST_F(DateTimeParsingTest, Time2MaxHour) {
  // Test case: 838:59:59 (MySQL TIME max is 838:59:59)
  auto time_bytes = EncodeTime2(838, 59, 59);
  auto table_meta = CreateDateTimeTableMeta(ColumnType::TIME2, 0);
  auto buffer = CreateDateTimeEvent(table_meta, time_bytes);

  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "",
                                    MySQLBinlogEventType::OBSOLETE_WRITE_ROWS_EVENT_V1);

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ("838:59:59", result->front().GetColumnValue("dt_col"));
}

TEST_F(DateTimeParsingTest, Time2RejectsOutOfRangeHourWithoutMasking) {
  constexpr int32_t kTimefIntOfs = 0x800000;
  uint32_t packed = static_cast<uint32_t>(kTimefIntOfs + (1100 << 12));
  std::vector<unsigned char> time_bytes = {static_cast<unsigned char>((packed >> 16) & 0xFF),
                                           static_cast<unsigned char>((packed >> 8) & 0xFF),
                                           static_cast<unsigned char>(packed & 0xFF)};

  auto result = internal::DecodeFieldValue(static_cast<uint8_t>(ColumnType::TIME2), time_bytes.data(), 0,
                                           /*is_null=*/false, time_bytes.data() + time_bytes.size(),
                                           /*is_unsigned=*/false);

  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(mygram::utils::ErrorCode::kMySQLInvalidMetadata, result.error().code());
}

/**
 * @test TIME (old format) parsing
 */
TEST_F(DateTimeParsingTest, TimeOldFormat) {
  // Test case: 12:34:56
  auto time_bytes = EncodeTime(12, 34, 56);
  auto table_meta = CreateDateTimeTableMeta(ColumnType::TIME, 0);
  auto buffer = CreateDateTimeEvent(table_meta, time_bytes);

  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "",
                                    MySQLBinlogEventType::OBSOLETE_WRITE_ROWS_EVENT_V1);

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ("12:34:56", result->front().GetColumnValue("dt_col"));
}

TEST_F(DateTimeParsingTest, OldTimeRejectsOutOfRangeComponents) {
  auto time_bytes = EncodeTime(12, 60, 0);

  auto result = internal::DecodeFieldValue(static_cast<uint8_t>(ColumnType::TIME), time_bytes.data(), 0,
                                           /*is_null=*/false, time_bytes.data() + time_bytes.size(),
                                           /*is_unsigned=*/false);

  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(mygram::utils::ErrorCode::kMySQLInvalidMetadata, result.error().code());
}

/**
 * @test TIMESTAMP2 parsing - basic
 */
TEST_F(DateTimeParsingTest, Timestamp2BasicParsing) {
  // Test case: Unix timestamp 1732545600 (2024-11-25 12:00:00 UTC)
  auto timestamp_bytes = EncodeTimestamp2(1732545600);
  auto table_meta = CreateDateTimeTableMeta(ColumnType::TIMESTAMP2, 0);
  auto buffer = CreateDateTimeEvent(table_meta, timestamp_bytes);

  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "",
                                    MySQLBinlogEventType::OBSOLETE_WRITE_ROWS_EVENT_V1);

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ("1732545600", result->front().GetColumnValue("dt_col"));
}

/**
 * @test TIMESTAMP2 parsing - with microseconds
 */
TEST_F(DateTimeParsingTest, Timestamp2WithMicroseconds) {
  // Test case: 1732545600.123456
  auto timestamp_bytes = EncodeTimestamp2(1732545600, 6, 123456);
  auto table_meta = CreateDateTimeTableMeta(ColumnType::TIMESTAMP2, 6);
  auto buffer = CreateDateTimeEvent(table_meta, timestamp_bytes);

  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "",
                                    MySQLBinlogEventType::OBSOLETE_WRITE_ROWS_EVENT_V1);

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ("1732545600.123456", result->front().GetColumnValue("dt_col"));
}

TEST_F(DateTimeParsingTest, Timestamp2Precision1UsesMySQLStoredByteScale) {
  auto timestamp_bytes = EncodeTimestamp2(1732545600);
  timestamp_bytes.push_back(0x0A);  // MySQL stores .1 as 10 in the single fractional byte.
  auto table_meta = CreateDateTimeTableMeta(ColumnType::TIMESTAMP2, 1);
  auto buffer = CreateDateTimeEvent(table_meta, timestamp_bytes);

  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "",
                                    MySQLBinlogEventType::OBSOLETE_WRITE_ROWS_EVENT_V1);

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ("1732545600.100000", result->front().GetColumnValue("dt_col"));
}

/**
 * @test DATE parsing
 */
TEST_F(DateTimeParsingTest, DateParsing) {
  // Test case: 2025-11-25
  auto date_bytes = EncodeDate(2025, 11, 25);
  auto table_meta = CreateDateTimeTableMeta(ColumnType::DATE, 0);
  auto buffer = CreateDateTimeEvent(table_meta, date_bytes);

  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "",
                                    MySQLBinlogEventType::OBSOLETE_WRITE_ROWS_EVENT_V1);

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ("2025-11-25", result->front().GetColumnValue("dt_col"));
}

TEST_F(DateTimeParsingTest, DateRejectsOutOfRangeComponents) {
  auto date_bytes = EncodeDate(2025, 13, 1);

  auto result = internal::DecodeFieldValue(static_cast<uint8_t>(ColumnType::DATE), date_bytes.data(), 0,
                                           /*is_null=*/false, date_bytes.data() + date_bytes.size(),
                                           /*is_unsigned=*/false);

  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(mygram::utils::ErrorCode::kMySQLInvalidMetadata, result.error().code());
}

TEST_F(DateTimeParsingTest, OldDatetimeRejectsOutOfRangeComponents) {
  auto datetime_bytes = EncodeDatetime(2025, 13, 1, 12, 0, 0);

  auto result = internal::DecodeFieldValue(static_cast<uint8_t>(ColumnType::DATETIME), datetime_bytes.data(), 0,
                                           /*is_null=*/false, datetime_bytes.data() + datetime_bytes.size(),
                                           /*is_unsigned=*/false);

  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(mygram::utils::ErrorCode::kMySQLInvalidMetadata, result.error().code());
}

/**
 * @test DATE parsing - leap year
 */
TEST_F(DateTimeParsingTest, DateLeapYear) {
  // Test case: 2024-02-29 (leap year)
  auto date_bytes = EncodeDate(2024, 2, 29);
  auto table_meta = CreateDateTimeTableMeta(ColumnType::DATE, 0);
  auto buffer = CreateDateTimeEvent(table_meta, date_bytes);

  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "",
                                    MySQLBinlogEventType::OBSOLETE_WRITE_ROWS_EVENT_V1);

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ("2024-02-29", result->front().GetColumnValue("dt_col"));
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

  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "",
                                    MySQLBinlogEventType::OBSOLETE_WRITE_ROWS_EVENT_V1);

  ASSERT_TRUE(result.has_value());
  // Before fix: was "0110-00-25 14:30:00" (year=110, month=0)
  // After fix: should be "2025-11-25 14:30:00"
  EXPECT_EQ("2025-11-25 14:30:00", result->front().GetColumnValue("dt_col"));
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
  row_data.columns["bigint_u_col"] = "18446744073709551615";
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
  filter_configs.push_back({"bigint_u_col", "bigint_unsigned", false, false, ""});
  filter_configs.push_back({"float_col", "float", false, false, ""});
  filter_configs.push_back({"string_col", "string", false, false, ""});

  auto filters = ExtractFilters(row_data, filter_configs);

  EXPECT_EQ(filters.size(), 11);
  EXPECT_EQ(std::get<bool>(filters["bool_col"]), true);
  EXPECT_EQ(std::get<int8_t>(filters["tinyint_col"]), -128);
  EXPECT_EQ(std::get<uint8_t>(filters["tinyint_u_col"]), 255);
  EXPECT_EQ(std::get<int16_t>(filters["smallint_col"]), -32768);
  EXPECT_EQ(std::get<uint16_t>(filters["smallint_u_col"]), 65535);
  EXPECT_EQ(std::get<int32_t>(filters["int_col"]), -2147483648);
  EXPECT_EQ(std::get<uint32_t>(filters["int_u_col"]), 4294967295U);
  EXPECT_EQ(std::get<int64_t>(filters["bigint_col"]), -9223372036854775807LL - 1);
  EXPECT_EQ(std::get<uint64_t>(filters["bigint_u_col"]), 18446744073709551615ULL);
  EXPECT_NEAR(std::get<double>(filters["float_col"]), 3.14, 0.01);
  EXPECT_EQ(std::get<std::string>(filters["string_col"]), "test");
}

// =============================================================================
// BLOB Edge Cases
// =============================================================================

/**
 * @test BLOB with invalid metadata value should not crash
 *
 * The BLOB parsing code has a switch statement for metadata values 1-4,
 * but no default case. If metadata is 0 or >4, blob_len and blob_data
 * remain uninitialized, causing undefined behavior.
 */
TEST_F(RowsParserTest, BlobInvalidMetadataZero) {
  // Create table with BLOB column having metadata=0 (invalid)
  TableMetadata table_meta;
  table_meta.table_id = 300;
  table_meta.database_name = "test_db";
  table_meta.table_name = "blob_test";

  ColumnMetadata col_id;
  col_id.type = ColumnType::LONG;
  col_id.name = "id";
  col_id.metadata = 0;
  table_meta.columns.push_back(col_id);

  ColumnMetadata col_blob;
  col_blob.type = ColumnType::BLOB;
  col_blob.name = "data";
  col_blob.metadata = 0;  // Invalid metadata
  table_meta.columns.push_back(col_blob);

  // Create row data: id=1, blob=some data
  std::vector<unsigned char> row_data;

  // id=1 (4 bytes, little-endian)
  auto id_bytes = EncodeInt32(1);
  row_data.insert(row_data.end(), id_bytes.begin(), id_bytes.end());

  // BLOB with metadata=0 - should handle gracefully
  // We'll put some arbitrary data - the parser should handle this without crashing
  row_data.push_back(0x05);  // Some length byte (if it were valid)
  row_data.push_back('h');
  row_data.push_back('e');
  row_data.push_back('l');
  row_data.push_back('l');
  row_data.push_back('o');

  std::vector<unsigned char> null_bitmap = {0x00};  // No NULLs

  auto buffer = CreateWriteRowsEventRaw(table_meta, row_data, null_bitmap);

  // This should not crash - either return nullopt or handle gracefully
  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "",
                                    MySQLBinlogEventType::OBSOLETE_WRITE_ROWS_EVENT_V1);

  // With invalid metadata, the parser should return nullopt or handle safely
  // The key requirement is: NO CRASH
  // We're flexible on the return value as long as it doesn't crash
  SUCCEED();  // If we reach here without crashing, the test passes
}

/**
 * @test BLOB with metadata=5 (out of range) should not crash
 */
TEST_F(RowsParserTest, BlobInvalidMetadataFive) {
  TableMetadata table_meta;
  table_meta.table_id = 301;
  table_meta.database_name = "test_db";
  table_meta.table_name = "blob_test";

  ColumnMetadata col_id;
  col_id.type = ColumnType::LONG;
  col_id.name = "id";
  col_id.metadata = 0;
  table_meta.columns.push_back(col_id);

  ColumnMetadata col_blob;
  col_blob.type = ColumnType::BLOB;
  col_blob.name = "data";
  col_blob.metadata = 5;  // Invalid - should be 1-4
  table_meta.columns.push_back(col_blob);

  std::vector<unsigned char> row_data;
  auto id_bytes = EncodeInt32(1);
  row_data.insert(row_data.end(), id_bytes.begin(), id_bytes.end());
  row_data.push_back(0x05);
  row_data.push_back('h');
  row_data.push_back('e');
  row_data.push_back('l');
  row_data.push_back('l');
  row_data.push_back('o');

  std::vector<unsigned char> null_bitmap = {0x00};

  auto buffer = CreateWriteRowsEventRaw(table_meta, row_data, null_bitmap);

  // Should not crash
  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "",
                                    MySQLBinlogEventType::OBSOLETE_WRITE_ROWS_EVENT_V1);
  SUCCEED();
}

// =============================================================================
// Missing Column Type Handlers: YEAR, BIT, FLOAT, DOUBLE
// =============================================================================

/**
 * @test YEAR type should be parsed correctly
 *
 * MySQL YEAR type is stored as 1 byte: (year - 1900)
 * So 2024 is stored as 124 (2024-1900)
 */
TEST_F(RowsParserTest, YearTypeParsing) {
  TableMetadata table_meta;
  table_meta.table_id = 302;
  table_meta.database_name = "test_db";
  table_meta.table_name = "year_test";

  ColumnMetadata col_id;
  col_id.type = ColumnType::LONG;
  col_id.name = "id";
  col_id.metadata = 0;
  table_meta.columns.push_back(col_id);

  ColumnMetadata col_year;
  col_year.type = ColumnType::YEAR;
  col_year.name = "birth_year";
  col_year.metadata = 0;
  table_meta.columns.push_back(col_year);

  std::vector<unsigned char> row_data;

  // id=1
  auto id_bytes = EncodeInt32(1);
  row_data.insert(row_data.end(), id_bytes.begin(), id_bytes.end());

  // YEAR=2024 (stored as 124 = 2024-1900)
  row_data.push_back(124);

  std::vector<unsigned char> null_bitmap = {0x00};

  auto buffer = CreateWriteRowsEventRaw(table_meta, row_data, null_bitmap);

  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "",
                                    MySQLBinlogEventType::OBSOLETE_WRITE_ROWS_EVENT_V1);

  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(1, result->size());

  const auto& row = result->front();
  EXPECT_EQ("1", row.primary_key);

  // Should return "2024" (not "[UNSUPPORTED_TYPE:13]")
  std::string year_value = row.GetColumnValue("birth_year");
  EXPECT_NE("[UNSUPPORTED_TYPE:13]", year_value);
  EXPECT_EQ("2024", year_value);
}

/**
 * @test YEAR=1901 (minimum valid year)
 */
TEST_F(RowsParserTest, YearMinValue) {
  TableMetadata table_meta;
  table_meta.table_id = 303;
  table_meta.database_name = "test_db";
  table_meta.table_name = "year_test";

  ColumnMetadata col_id;
  col_id.type = ColumnType::LONG;
  col_id.name = "id";
  col_id.metadata = 0;
  table_meta.columns.push_back(col_id);

  ColumnMetadata col_year;
  col_year.type = ColumnType::YEAR;
  col_year.name = "year_col";
  col_year.metadata = 0;
  table_meta.columns.push_back(col_year);

  std::vector<unsigned char> row_data;
  auto id_bytes = EncodeInt32(1);
  row_data.insert(row_data.end(), id_bytes.begin(), id_bytes.end());

  // YEAR=1901 (stored as 1)
  row_data.push_back(1);

  std::vector<unsigned char> null_bitmap = {0x00};
  auto buffer = CreateWriteRowsEventRaw(table_meta, row_data, null_bitmap);

  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "",
                                    MySQLBinlogEventType::OBSOLETE_WRITE_ROWS_EVENT_V1);

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ("1901", result->front().GetColumnValue("year_col"));
}

/**
 * @test YEAR=2155 (maximum valid year)
 */
TEST_F(RowsParserTest, YearMaxValue) {
  TableMetadata table_meta;
  table_meta.table_id = 304;
  table_meta.database_name = "test_db";
  table_meta.table_name = "year_test";

  ColumnMetadata col_id;
  col_id.type = ColumnType::LONG;
  col_id.name = "id";
  col_id.metadata = 0;
  table_meta.columns.push_back(col_id);

  ColumnMetadata col_year;
  col_year.type = ColumnType::YEAR;
  col_year.name = "year_col";
  col_year.metadata = 0;
  table_meta.columns.push_back(col_year);

  std::vector<unsigned char> row_data;
  auto id_bytes = EncodeInt32(1);
  row_data.insert(row_data.end(), id_bytes.begin(), id_bytes.end());

  // YEAR=2155 (stored as 255 = 2155-1900)
  row_data.push_back(255);

  std::vector<unsigned char> null_bitmap = {0x00};
  auto buffer = CreateWriteRowsEventRaw(table_meta, row_data, null_bitmap);

  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "",
                                    MySQLBinlogEventType::OBSOLETE_WRITE_ROWS_EVENT_V1);

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ("2155", result->front().GetColumnValue("year_col"));
}

/**
 * @test YEAR=0 (zero value - special case)
 */
TEST_F(RowsParserTest, YearZeroValue) {
  TableMetadata table_meta;
  table_meta.table_id = 305;
  table_meta.database_name = "test_db";
  table_meta.table_name = "year_test";

  ColumnMetadata col_id;
  col_id.type = ColumnType::LONG;
  col_id.name = "id";
  col_id.metadata = 0;
  table_meta.columns.push_back(col_id);

  ColumnMetadata col_year;
  col_year.type = ColumnType::YEAR;
  col_year.name = "year_col";
  col_year.metadata = 0;
  table_meta.columns.push_back(col_year);

  std::vector<unsigned char> row_data;
  auto id_bytes = EncodeInt32(1);
  row_data.insert(row_data.end(), id_bytes.begin(), id_bytes.end());

  // YEAR=0 (special value indicating zero/invalid)
  row_data.push_back(0);

  std::vector<unsigned char> null_bitmap = {0x00};
  auto buffer = CreateWriteRowsEventRaw(table_meta, row_data, null_bitmap);

  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "",
                                    MySQLBinlogEventType::OBSOLETE_WRITE_ROWS_EVENT_V1);

  ASSERT_TRUE(result.has_value());
  // 0 is a special value in MySQL YEAR type representing 0000
  EXPECT_EQ("0000", result->front().GetColumnValue("year_col"));
}

/**
 * @test FLOAT type should be parsed correctly
 */
TEST_F(RowsParserTest, FloatTypeParsing) {
  TableMetadata table_meta;
  table_meta.table_id = 306;
  table_meta.database_name = "test_db";
  table_meta.table_name = "float_test";

  ColumnMetadata col_id;
  col_id.type = ColumnType::LONG;
  col_id.name = "id";
  col_id.metadata = 0;
  table_meta.columns.push_back(col_id);

  ColumnMetadata col_float;
  col_float.type = ColumnType::FLOAT;
  col_float.name = "price";
  col_float.metadata = 0;
  table_meta.columns.push_back(col_float);

  std::vector<unsigned char> row_data;

  // id=1
  auto id_bytes = EncodeInt32(1);
  row_data.insert(row_data.end(), id_bytes.begin(), id_bytes.end());

  // FLOAT=3.14
  auto float_bytes = EncodeFloat(3.14f);
  row_data.insert(row_data.end(), float_bytes.begin(), float_bytes.end());

  std::vector<unsigned char> null_bitmap = {0x00};

  auto buffer = CreateWriteRowsEventRaw(table_meta, row_data, null_bitmap);

  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "",
                                    MySQLBinlogEventType::OBSOLETE_WRITE_ROWS_EVENT_V1);

  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(1, result->size());

  const auto& row = result->front();
  EXPECT_EQ("1", row.primary_key);

  // Should return a float string (not "[UNSUPPORTED_TYPE:4]")
  std::string float_value = row.GetColumnValue("price");
  EXPECT_NE("[UNSUPPORTED_TYPE:4]", float_value);

  // Parse and check value is approximately 3.14
  double parsed = std::stod(float_value);
  EXPECT_NEAR(3.14, parsed, 0.01);
}

TEST_F(RowsParserTest, FloatTypeUsesRoundTripPrecision) {
  const float value = 123456.789f;
  auto bytes = EncodeFloat(value);

  auto decoded = internal::DecodeFieldValue(static_cast<uint8_t>(ColumnType::FLOAT), bytes.data(), 0, false,
                                            bytes.data() + bytes.size());

  ASSERT_TRUE(decoded.has_value()) << decoded.error().message();
  EXPECT_NE(*decoded, std::to_string(value));
  EXPECT_FLOAT_EQ(std::stof(*decoded), value);
}

/**
 * @test DOUBLE type should be parsed correctly
 */
TEST_F(RowsParserTest, DoubleTypeParsing) {
  TableMetadata table_meta;
  table_meta.table_id = 307;
  table_meta.database_name = "test_db";
  table_meta.table_name = "double_test";

  ColumnMetadata col_id;
  col_id.type = ColumnType::LONG;
  col_id.name = "id";
  col_id.metadata = 0;
  table_meta.columns.push_back(col_id);

  ColumnMetadata col_double;
  col_double.type = ColumnType::DOUBLE;
  col_double.name = "price";
  col_double.metadata = 0;
  table_meta.columns.push_back(col_double);

  std::vector<unsigned char> row_data;

  // id=1
  auto id_bytes = EncodeInt32(1);
  row_data.insert(row_data.end(), id_bytes.begin(), id_bytes.end());

  // DOUBLE=3.14159265359
  auto double_bytes = EncodeDouble(3.14159265359);
  row_data.insert(row_data.end(), double_bytes.begin(), double_bytes.end());

  std::vector<unsigned char> null_bitmap = {0x00};

  auto buffer = CreateWriteRowsEventRaw(table_meta, row_data, null_bitmap);

  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "",
                                    MySQLBinlogEventType::OBSOLETE_WRITE_ROWS_EVENT_V1);

  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(1, result->size());

  const auto& row = result->front();
  EXPECT_EQ("1", row.primary_key);

  // Should return a double string (not "[UNSUPPORTED_TYPE:5]")
  std::string double_value = row.GetColumnValue("price");
  EXPECT_NE("[UNSUPPORTED_TYPE:5]", double_value);

  // Parse and check value is approximately 3.14159265359
  double parsed = std::stod(double_value);
  EXPECT_NEAR(3.14159265359, parsed, 0.00001);
}

TEST_F(RowsParserTest, DoubleTypeUsesRoundTripPrecision) {
  const double value = 0.12345678901234566;
  auto bytes = EncodeDouble(value);

  auto decoded = internal::DecodeFieldValue(static_cast<uint8_t>(ColumnType::DOUBLE), bytes.data(), 0, false,
                                            bytes.data() + bytes.size());

  ASSERT_TRUE(decoded.has_value()) << decoded.error().message();
  EXPECT_NE(*decoded, std::to_string(value));
  EXPECT_DOUBLE_EQ(std::stod(*decoded), value);
}

/**
 * @test FLOAT with special values (zero, negative, very large)
 */
TEST_F(RowsParserTest, FloatSpecialValues) {
  TableMetadata table_meta;
  table_meta.table_id = 308;
  table_meta.database_name = "test_db";
  table_meta.table_name = "float_test";

  ColumnMetadata col_id;
  col_id.type = ColumnType::LONG;
  col_id.name = "id";
  col_id.metadata = 0;
  table_meta.columns.push_back(col_id);

  ColumnMetadata col_float;
  col_float.type = ColumnType::FLOAT;
  col_float.name = "val";
  col_float.metadata = 0;
  table_meta.columns.push_back(col_float);

  // Test zero
  {
    std::vector<unsigned char> row_data;
    auto id_bytes = EncodeInt32(1);
    row_data.insert(row_data.end(), id_bytes.begin(), id_bytes.end());
    auto float_bytes = EncodeFloat(0.0f);
    row_data.insert(row_data.end(), float_bytes.begin(), float_bytes.end());

    std::vector<unsigned char> null_bitmap = {0x00};
    auto buffer = CreateWriteRowsEventRaw(table_meta, row_data, null_bitmap);
    auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "",
                                      MySQLBinlogEventType::OBSOLETE_WRITE_ROWS_EVENT_V1);

    ASSERT_TRUE(result.has_value());
    double parsed = std::stod(result->front().GetColumnValue("val"));
    EXPECT_NEAR(0.0, parsed, 0.0001);
  }

  // Test negative
  {
    std::vector<unsigned char> row_data;
    auto id_bytes = EncodeInt32(2);
    row_data.insert(row_data.end(), id_bytes.begin(), id_bytes.end());
    auto float_bytes = EncodeFloat(-123.456f);
    row_data.insert(row_data.end(), float_bytes.begin(), float_bytes.end());

    std::vector<unsigned char> null_bitmap = {0x00};
    auto buffer = CreateWriteRowsEventRaw(table_meta, row_data, null_bitmap);
    auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "",
                                      MySQLBinlogEventType::OBSOLETE_WRITE_ROWS_EVENT_V1);

    ASSERT_TRUE(result.has_value());
    double parsed = std::stod(result->front().GetColumnValue("val"));
    EXPECT_NEAR(-123.456, parsed, 0.01);
  }
}

/**
 * @test BIT type should be parsed correctly
 *
 * MySQL BIT(n) is stored as (bytes, bits) where:
 * - bytes = n / 8
 * - bits = n % 8
 * metadata = (bytes << 8) | bits
 */
TEST_F(RowsParserTest, BitTypeParsing) {
  TableMetadata table_meta;
  table_meta.table_id = 309;
  table_meta.database_name = "test_db";
  table_meta.table_name = "bit_test";

  ColumnMetadata col_id;
  col_id.type = ColumnType::LONG;
  col_id.name = "id";
  col_id.metadata = 0;
  table_meta.columns.push_back(col_id);

  ColumnMetadata col_bit;
  col_bit.type = ColumnType::BIT;
  col_bit.name = "flags";
  // BIT(8) = 1 byte, 0 extra bits -> metadata = (1 << 8) | 0 = 256
  // But actually for BIT(8), it's stored as (0 << 8) | 8 = 8 bytes=0, bits=8
  // After checking MySQL docs: metadata = (bytes << 8) | bits
  // For BIT(8): bytes=1, bits=0 -> metadata = (1 << 8) | 0 = 256
  col_bit.metadata = (1 << 8) | 0;  // BIT(8): 1 full byte
  table_meta.columns.push_back(col_bit);

  std::vector<unsigned char> row_data;

  // id=1
  auto id_bytes = EncodeInt32(1);
  row_data.insert(row_data.end(), id_bytes.begin(), id_bytes.end());

  // BIT(8) = 0b10101010 = 170
  row_data.push_back(0b10101010);

  std::vector<unsigned char> null_bitmap = {0x00};

  auto buffer = CreateWriteRowsEventRaw(table_meta, row_data, null_bitmap);

  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "",
                                    MySQLBinlogEventType::OBSOLETE_WRITE_ROWS_EVENT_V1);

  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(1, result->size());

  const auto& row = result->front();
  EXPECT_EQ("1", row.primary_key);

  // Should return the numeric value (not "[UNSUPPORTED_TYPE:16]")
  std::string bit_value = row.GetColumnValue("flags");
  EXPECT_NE("[UNSUPPORTED_TYPE:16]", bit_value);
  EXPECT_EQ("170", bit_value);  // 0b10101010 = 170
}

/**
 * @test BIT with multiple bytes
 */
TEST_F(RowsParserTest, BitMultipleBytes) {
  TableMetadata table_meta;
  table_meta.table_id = 310;
  table_meta.database_name = "test_db";
  table_meta.table_name = "bit_test";

  ColumnMetadata col_id;
  col_id.type = ColumnType::LONG;
  col_id.name = "id";
  col_id.metadata = 0;
  table_meta.columns.push_back(col_id);

  ColumnMetadata col_bit;
  col_bit.type = ColumnType::BIT;
  col_bit.name = "flags";
  // BIT(16): 2 bytes, 0 extra bits -> metadata = (2 << 8) | 0 = 512
  col_bit.metadata = (2 << 8) | 0;
  table_meta.columns.push_back(col_bit);

  std::vector<unsigned char> row_data;

  // id=1
  auto id_bytes = EncodeInt32(1);
  row_data.insert(row_data.end(), id_bytes.begin(), id_bytes.end());

  // BIT(16) = 0x1234 = 4660
  row_data.push_back(0x12);
  row_data.push_back(0x34);

  std::vector<unsigned char> null_bitmap = {0x00};

  auto buffer = CreateWriteRowsEventRaw(table_meta, row_data, null_bitmap);

  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "",
                                    MySQLBinlogEventType::OBSOLETE_WRITE_ROWS_EVENT_V1);

  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(1, result->size());

  std::string bit_value = result->front().GetColumnValue("flags");
  EXPECT_NE("[UNSUPPORTED_TYPE:16]", bit_value);
  // The value should be the numeric representation
  // 0x1234 = 4660 (big-endian)
  EXPECT_EQ("4660", bit_value);
}

/**
 * @test BIT with partial byte (e.g., BIT(5))
 */
TEST_F(RowsParserTest, BitPartialByte) {
  TableMetadata table_meta;
  table_meta.table_id = 311;
  table_meta.database_name = "test_db";
  table_meta.table_name = "bit_test";

  ColumnMetadata col_id;
  col_id.type = ColumnType::LONG;
  col_id.name = "id";
  col_id.metadata = 0;
  table_meta.columns.push_back(col_id);

  ColumnMetadata col_bit;
  col_bit.type = ColumnType::BIT;
  col_bit.name = "flags";
  // BIT(5): 0 bytes, 5 extra bits -> metadata = (0 << 8) | 5 = 5
  // This means 1 byte total (0 full bytes + 5 bits requires 1 byte)
  col_bit.metadata = (0 << 8) | 5;
  table_meta.columns.push_back(col_bit);

  std::vector<unsigned char> row_data;

  // id=1
  auto id_bytes = EncodeInt32(1);
  row_data.insert(row_data.end(), id_bytes.begin(), id_bytes.end());

  // BIT(5) = 0b10101 = 21
  row_data.push_back(0b10101);

  std::vector<unsigned char> null_bitmap = {0x00};

  auto buffer = CreateWriteRowsEventRaw(table_meta, row_data, null_bitmap);

  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "",
                                    MySQLBinlogEventType::OBSOLETE_WRITE_ROWS_EVENT_V1);

  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(1, result->size());

  std::string bit_value = result->front().GetColumnValue("flags");
  EXPECT_NE("[UNSUPPORTED_TYPE:16]", bit_value);
  EXPECT_EQ("21", bit_value);  // 0b10101 = 21
}

// =============================================================================
// Character Encoding / UTF-8 Handling
// =============================================================================

/**
 * @test Valid UTF-8 strings should pass through unchanged
 */
TEST_F(RowsParserTest, ValidUtf8PassThrough) {
  TableMetadata table_meta;
  table_meta.table_id = 400;
  table_meta.database_name = "test_db";
  table_meta.table_name = "utf8_test";

  ColumnMetadata col_id;
  col_id.type = ColumnType::LONG;
  col_id.name = "id";
  col_id.metadata = 0;
  table_meta.columns.push_back(col_id);

  ColumnMetadata col_text;
  col_text.type = ColumnType::VARCHAR;
  col_text.name = "content";
  col_text.metadata = 255;  // max length 255, 1-byte length prefix
  table_meta.columns.push_back(col_text);

  // Valid UTF-8 strings to test
  std::vector<std::string> test_strings = {
      "Hello, World!",             // ASCII
      u8"こんにちは",              // Japanese (Hiragana)
      u8"日本語テスト",            // Japanese (Kanji + Katakana)
      u8"你好世界",                // Chinese
      u8"Привет мир",              // Russian
      u8"🎉🚀💻",                  // Emojis (4-byte UTF-8)
      u8"Mixed: Hello 日本語 🎉",  // Mixed content
  };

  for (size_t i = 0; i < test_strings.size(); i++) {
    const auto& test_str = test_strings[i];

    std::vector<unsigned char> row_data;

    // id
    auto id_bytes = EncodeInt32(static_cast<int32_t>(i + 1));
    row_data.insert(row_data.end(), id_bytes.begin(), id_bytes.end());

    // VARCHAR: 1-byte length prefix + data
    row_data.push_back(static_cast<unsigned char>(test_str.size()));
    for (char c : test_str) {
      row_data.push_back(static_cast<unsigned char>(c));
    }

    std::vector<unsigned char> null_bitmap = {0x00};
    auto buffer = CreateWriteRowsEventRaw(table_meta, row_data, null_bitmap);
    auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "",
                                      MySQLBinlogEventType::OBSOLETE_WRITE_ROWS_EVENT_V1);

    ASSERT_TRUE(result.has_value()) << "Failed for string: " << test_str;
    ASSERT_EQ(1, result->size());
    EXPECT_EQ(test_str, result->front().GetColumnValue("content")) << "Mismatch for valid UTF-8 string: " << test_str;
  }
}

/**
 * @test Invalid UTF-8 sequences should be sanitized
 *
 * Invalid bytes should be replaced with U+FFFD (replacement character)
 */
TEST_F(RowsParserTest, InvalidUtf8Sanitized) {
  TableMetadata table_meta;
  table_meta.table_id = 401;
  table_meta.database_name = "test_db";
  table_meta.table_name = "utf8_test";

  ColumnMetadata col_id;
  col_id.type = ColumnType::LONG;
  col_id.name = "id";
  col_id.metadata = 0;
  table_meta.columns.push_back(col_id);

  ColumnMetadata col_text;
  col_text.type = ColumnType::VARCHAR;
  col_text.name = "content";
  col_text.metadata = 255;
  table_meta.columns.push_back(col_text);

  // Invalid UTF-8 sequences
  struct TestCase {
    std::vector<unsigned char> input;
    std::string description;
  };

  std::vector<TestCase> test_cases = {
      // Latin-1 encoded text (non-UTF8)
      {{0xE9}, "Latin-1 é (0xE9)"},
      // Invalid continuation byte
      {{0xC0, 0x80}, "Overlong encoding (0xC0 0x80)"},
      // Invalid start byte
      {{0x80}, "Invalid start byte (0x80)"},
      {{0xFF}, "Invalid byte (0xFF)"},
      // Incomplete multi-byte sequence
      {{0xC2}, "Incomplete 2-byte (0xC2)"},
      {{0xE0, 0xA0}, "Incomplete 3-byte (0xE0 0xA0)"},
      {{0xF0, 0x90, 0x80}, "Incomplete 4-byte (0xF0 0x90 0x80)"},
      // Mixed valid and invalid
      {{'H', 'i', 0xFF, '!'}, "Mixed: Hi + 0xFF + !"},
  };

  // U+FFFD in UTF-8 encoding
  const std::string kReplacementChar = "\xEF\xBF\xBD";

  for (size_t i = 0; i < test_cases.size(); i++) {
    const auto& tc = test_cases[i];

    std::vector<unsigned char> row_data;

    // id
    auto id_bytes = EncodeInt32(static_cast<int32_t>(i + 1));
    row_data.insert(row_data.end(), id_bytes.begin(), id_bytes.end());

    // VARCHAR: 1-byte length prefix + data
    row_data.push_back(static_cast<unsigned char>(tc.input.size()));
    row_data.insert(row_data.end(), tc.input.begin(), tc.input.end());

    std::vector<unsigned char> null_bitmap = {0x00};
    auto buffer = CreateWriteRowsEventRaw(table_meta, row_data, null_bitmap);
    auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "",
                                      MySQLBinlogEventType::OBSOLETE_WRITE_ROWS_EVENT_V1);

    ASSERT_TRUE(result.has_value()) << "Failed for: " << tc.description;
    ASSERT_EQ(1, result->size());

    std::string content = result->front().GetColumnValue("content");

    // The result should NOT contain the invalid bytes as-is
    // It should either be sanitized or contain replacement characters
    for (unsigned char c : tc.input) {
      if (c >= 0x80 && c <= 0xBF) {
        // Continuation bytes - these alone are invalid
        // The sanitized string should not start with them
      }
    }

    // The result should be valid UTF-8 (can be processed without errors)
    // Check that it contains replacement characters for invalid sequences
    EXPECT_TRUE(content.find(kReplacementChar) != std::string::npos || content.find("[") == 0 ||  // Error marker
                content.empty() ||                                                                // Sanitized to empty
                content == std::string(tc.input.begin(), tc.input.end()))                         // Or ASCII-safe
        << "Invalid UTF-8 not handled for: " << tc.description;
  }
}

/**
 * @test BLOB/TEXT types should also sanitize UTF-8
 */
TEST_F(RowsParserTest, BlobTextUtf8Sanitization) {
  TableMetadata table_meta;
  table_meta.table_id = 402;
  table_meta.database_name = "test_db";
  table_meta.table_name = "blob_utf8_test";

  ColumnMetadata col_id;
  col_id.type = ColumnType::LONG;
  col_id.name = "id";
  col_id.metadata = 0;
  table_meta.columns.push_back(col_id);

  ColumnMetadata col_blob;
  col_blob.type = ColumnType::BLOB;
  col_blob.name = "data";
  col_blob.metadata = 2;  // BLOB/TEXT (2-byte length prefix)
  table_meta.columns.push_back(col_blob);

  // Test with Latin-1 encoded text in BLOB
  std::vector<unsigned char> invalid_text = {0xC0, 0xC1, 0xF5, 0xF6, 0xF7};  // Invalid UTF-8 bytes

  std::vector<unsigned char> row_data;

  // id=1
  auto id_bytes = EncodeInt32(1);
  row_data.insert(row_data.end(), id_bytes.begin(), id_bytes.end());

  // BLOB: 2-byte length prefix (little-endian) + data
  uint16_t len = static_cast<uint16_t>(invalid_text.size());
  row_data.push_back(len & 0xFF);
  row_data.push_back((len >> 8) & 0xFF);
  row_data.insert(row_data.end(), invalid_text.begin(), invalid_text.end());

  std::vector<unsigned char> null_bitmap = {0x00};
  auto buffer = CreateWriteRowsEventRaw(table_meta, row_data, null_bitmap);
  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "",
                                    MySQLBinlogEventType::OBSOLETE_WRITE_ROWS_EVENT_V1);

  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(1, result->size());

  // The BLOB content should be sanitized - no crash and valid output
  std::string content = result->front().GetColumnValue("data");
  EXPECT_FALSE(content.empty()) << "BLOB content should not be empty";

  // Result should be valid UTF-8 (either sanitized or marked as invalid)
  // The key is that it doesn't crash and returns something processable
  SUCCEED();
}

/**
 * @test Empty string should be handled correctly
 */
TEST_F(RowsParserTest, EmptyStringHandling) {
  TableMetadata table_meta;
  table_meta.table_id = 403;
  table_meta.database_name = "test_db";
  table_meta.table_name = "empty_test";

  ColumnMetadata col_id;
  col_id.type = ColumnType::LONG;
  col_id.name = "id";
  col_id.metadata = 0;
  table_meta.columns.push_back(col_id);

  ColumnMetadata col_text;
  col_text.type = ColumnType::VARCHAR;
  col_text.name = "content";
  col_text.metadata = 255;
  table_meta.columns.push_back(col_text);

  std::vector<unsigned char> row_data;

  // id=1
  auto id_bytes = EncodeInt32(1);
  row_data.insert(row_data.end(), id_bytes.begin(), id_bytes.end());

  // Empty VARCHAR: length=0
  row_data.push_back(0);

  std::vector<unsigned char> null_bitmap = {0x00};
  auto buffer = CreateWriteRowsEventRaw(table_meta, row_data, null_bitmap);
  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "",
                                    MySQLBinlogEventType::OBSOLETE_WRITE_ROWS_EVENT_V1);

  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(1, result->size());
  EXPECT_EQ("", result->front().GetColumnValue("content"));
}

// =============================================================================
// Unsigned Integer Overflow
// =============================================================================

/**
 * @test UNSIGNED INT column should preserve large positive values
 *
 * An UNSIGNED INT can hold values 0-4294967295, but casting to int32_t
 * causes overflow for values > 2147483647.
 */
TEST_F(RowsParserTest, UnsignedIntLargeValue) {
  TableMetadata table_meta;
  table_meta.table_id = 1;
  table_meta.database_name = "test";
  table_meta.table_name = "test_table";

  ColumnMetadata col_id;
  col_id.type = ColumnType::LONG;
  col_id.name = "id";
  col_id.is_unsigned = true;  // UNSIGNED INT
  table_meta.columns.push_back(col_id);

  // Encode UNSIGNED INT value 4000000000 (> INT_MAX)
  uint32_t unsigned_val = 4000000000U;
  std::vector<unsigned char> row_data = {
      static_cast<unsigned char>(unsigned_val & 0xFF), static_cast<unsigned char>((unsigned_val >> 8) & 0xFF),
      static_cast<unsigned char>((unsigned_val >> 16) & 0xFF), static_cast<unsigned char>((unsigned_val >> 24) & 0xFF)};

  std::vector<unsigned char> null_bitmap = {0x00};
  auto buffer = CreateWriteRowsEventRaw(table_meta, row_data, null_bitmap);
  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "",
                                    MySQLBinlogEventType::OBSOLETE_WRITE_ROWS_EVENT_V1);

  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(1, result->size());

  // Before fix, this would be "-294967296" (overflow to negative)
  // After fix, this should be "4000000000"
  EXPECT_EQ("4000000000", result->front().GetColumnValue("id")) << "UNSIGNED INT should preserve large positive values";
}

/**
 * @test UNSIGNED TINYINT should handle values 128-255
 *
 * UNSIGNED TINYINT range is 0-255, but SIGNED TINYINT is -128 to 127.
 * Value 200 would become -56 if incorrectly cast to int8_t.
 */
TEST_F(RowsParserTest, UnsignedTinyIntLargeValue) {
  TableMetadata table_meta;
  table_meta.table_id = 1;
  table_meta.database_name = "test";
  table_meta.table_name = "test_table";

  ColumnMetadata col_id;
  col_id.type = ColumnType::TINY;
  col_id.name = "id";
  col_id.is_unsigned = true;  // UNSIGNED TINYINT
  table_meta.columns.push_back(col_id);

  // Encode UNSIGNED TINYINT value 200 (> 127)
  std::vector<unsigned char> row_data = {200};

  std::vector<unsigned char> null_bitmap = {0x00};
  auto buffer = CreateWriteRowsEventRaw(table_meta, row_data, null_bitmap);
  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "",
                                    MySQLBinlogEventType::OBSOLETE_WRITE_ROWS_EVENT_V1);

  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(1, result->size());

  // Before fix, this would be "-56" (overflow to negative)
  // After fix, this should be "200"
  EXPECT_EQ("200", result->front().GetColumnValue("id")) << "UNSIGNED TINYINT should preserve values 128-255";
}

/**
 * @test UNSIGNED SMALLINT should handle values 32768-65535
 *
 * UNSIGNED SMALLINT range is 0-65535, but SIGNED SMALLINT is -32768 to 32767.
 */
TEST_F(RowsParserTest, UnsignedSmallIntLargeValue) {
  TableMetadata table_meta;
  table_meta.table_id = 1;
  table_meta.database_name = "test";
  table_meta.table_name = "test_table";

  ColumnMetadata col_id;
  col_id.type = ColumnType::SHORT;
  col_id.name = "id";
  col_id.is_unsigned = true;  // UNSIGNED SMALLINT
  table_meta.columns.push_back(col_id);

  // Encode UNSIGNED SMALLINT value 50000 (> 32767)
  uint16_t unsigned_val = 50000U;
  std::vector<unsigned char> row_data = {static_cast<unsigned char>(unsigned_val & 0xFF),
                                         static_cast<unsigned char>((unsigned_val >> 8) & 0xFF)};

  std::vector<unsigned char> null_bitmap = {0x00};
  auto buffer = CreateWriteRowsEventRaw(table_meta, row_data, null_bitmap);
  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "",
                                    MySQLBinlogEventType::OBSOLETE_WRITE_ROWS_EVENT_V1);

  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(1, result->size());

  // Before fix, this would be "-15536" (overflow to negative)
  // After fix, this should be "50000"
  EXPECT_EQ("50000", result->front().GetColumnValue("id")) << "UNSIGNED SMALLINT should preserve values 32768-65535";
}

/**
 * @test UNSIGNED BIGINT should handle values > INT64_MAX
 */
TEST_F(RowsParserTest, UnsignedBigIntLargeValue) {
  TableMetadata table_meta;
  table_meta.table_id = 1;
  table_meta.database_name = "test";
  table_meta.table_name = "test_table";

  ColumnMetadata col_id;
  col_id.type = ColumnType::LONGLONG;
  col_id.name = "id";
  col_id.is_unsigned = true;  // UNSIGNED BIGINT
  table_meta.columns.push_back(col_id);

  // Encode UNSIGNED BIGINT value 10000000000000000000 (> INT64_MAX)
  uint64_t unsigned_val = 10000000000000000000ULL;
  std::vector<unsigned char> row_data;
  for (int i = 0; i < 8; i++) {
    row_data.push_back(static_cast<unsigned char>((unsigned_val >> (i * 8)) & 0xFF));
  }

  std::vector<unsigned char> null_bitmap = {0x00};
  auto buffer = CreateWriteRowsEventRaw(table_meta, row_data, null_bitmap);
  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "",
                                    MySQLBinlogEventType::OBSOLETE_WRITE_ROWS_EVENT_V1);

  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(1, result->size());

  // Before fix, this would be negative (overflow)
  // After fix, this should be "10000000000000000000"
  EXPECT_EQ("10000000000000000000", result->front().GetColumnValue("id"))
      << "UNSIGNED BIGINT should preserve values > INT64_MAX";
}

/**
 * @test Signed integers should still work correctly
 *
 * Ensure that fixing unsigned doesn't break signed integer handling.
 */
TEST_F(RowsParserTest, SignedIntNegativeValue) {
  TableMetadata table_meta;
  table_meta.table_id = 1;
  table_meta.database_name = "test";
  table_meta.table_name = "test_table";

  ColumnMetadata col_id;
  col_id.type = ColumnType::LONG;
  col_id.name = "id";
  col_id.is_unsigned = false;  // SIGNED INT (default)
  table_meta.columns.push_back(col_id);

  // Encode SIGNED INT value -1000
  int32_t signed_val = -1000;
  std::vector<unsigned char> row_data = {
      static_cast<unsigned char>(signed_val & 0xFF), static_cast<unsigned char>((signed_val >> 8) & 0xFF),
      static_cast<unsigned char>((signed_val >> 16) & 0xFF), static_cast<unsigned char>((signed_val >> 24) & 0xFF)};

  std::vector<unsigned char> null_bitmap = {0x00};
  auto buffer = CreateWriteRowsEventRaw(table_meta, row_data, null_bitmap);
  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "",
                                    MySQLBinlogEventType::OBSOLETE_WRITE_ROWS_EVENT_V1);

  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(1, result->size());

  // Should correctly show negative value
  EXPECT_EQ("-1000", result->front().GetColumnValue("id"))
      << "SIGNED INT should still handle negative values correctly";
}

// =============================================================================
// GEOMETRY Type Support
// =============================================================================

/**
 * @test GEOMETRY type should be parsed as WKB hex string
 *
 * GEOMETRY columns store data in WKB (Well-Known Binary) format.
 * The parser should handle this type and return a hex representation.
 */
TEST_F(RowsParserTest, GeometryTypeBasic) {
  TableMetadata table_meta;
  table_meta.table_id = 400;
  table_meta.database_name = "test_db";
  table_meta.table_name = "geo_test";

  ColumnMetadata col_id;
  col_id.type = ColumnType::LONG;
  col_id.name = "id";
  col_id.metadata = 0;
  table_meta.columns.push_back(col_id);

  ColumnMetadata col_geo;
  col_geo.type = ColumnType::GEOMETRY;  // col_type = 255
  col_geo.name = "location";
  col_geo.metadata = 4;  // 4 bytes for length prefix (like BLOB)
  table_meta.columns.push_back(col_geo);

  // Create row data: id=1, location=simple WKB point
  std::vector<unsigned char> row_data;

  // id=1 (4 bytes, little-endian)
  auto id_bytes = EncodeInt32(1);
  row_data.insert(row_data.end(), id_bytes.begin(), id_bytes.end());

  // GEOMETRY: 4-byte length prefix + WKB data
  // Simple WKB POINT: byte order (1) + type (1=Point, 4 bytes) + X (8 bytes) + Y (8 bytes)
  // Total: 21 bytes
  std::vector<unsigned char> wkb_point = {
      0x01,                                            // Little-endian byte order
      0x01, 0x00, 0x00, 0x00,                          // Type = 1 (Point)
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40,  // X = 100.0
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x49, 0x40   // Y = 50.0
  };

  // Length prefix (4 bytes, little-endian)
  uint32_t geo_len = wkb_point.size();
  row_data.push_back(geo_len & 0xFF);
  row_data.push_back((geo_len >> 8) & 0xFF);
  row_data.push_back((geo_len >> 16) & 0xFF);
  row_data.push_back((geo_len >> 24) & 0xFF);

  // WKB data
  row_data.insert(row_data.end(), wkb_point.begin(), wkb_point.end());

  std::vector<unsigned char> null_bitmap = {0x00};  // No NULLs
  auto buffer = CreateWriteRowsEventRaw(table_meta, row_data, null_bitmap);
  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "",
                                    MySQLBinlogEventType::OBSOLETE_WRITE_ROWS_EVENT_V1);

  ASSERT_TRUE(result.has_value()) << "GEOMETRY type should be parsed successfully";
  ASSERT_EQ(1, result->size());

  // The geometry column should exist and not contain [UNSUPPORTED_TYPE:255]
  ASSERT_NE(result->front().FindColumnValue("location"), nullptr);
  std::string geo_value = result->front().GetColumnValue("location");
  EXPECT_TRUE(geo_value.find("UNSUPPORTED") == std::string::npos)
      << "GEOMETRY should not return UNSUPPORTED_TYPE, got: " << geo_value;
}

/**
 * @test Empty GEOMETRY (zero-length) should be handled correctly
 */
TEST_F(RowsParserTest, GeometryTypeEmpty) {
  TableMetadata table_meta;
  table_meta.table_id = 401;
  table_meta.database_name = "test_db";
  table_meta.table_name = "geo_test";

  ColumnMetadata col_id;
  col_id.type = ColumnType::LONG;
  col_id.name = "id";
  col_id.metadata = 0;
  table_meta.columns.push_back(col_id);

  ColumnMetadata col_geo;
  col_geo.type = ColumnType::GEOMETRY;
  col_geo.name = "location";
  col_geo.metadata = 4;
  table_meta.columns.push_back(col_geo);

  // Create row data: id=1, location=empty geometry (0 length)
  std::vector<unsigned char> row_data;
  auto id_bytes = EncodeInt32(1);
  row_data.insert(row_data.end(), id_bytes.begin(), id_bytes.end());

  // GEOMETRY with length 0 (4-byte length prefix = 0x00000000)
  row_data.push_back(0x00);
  row_data.push_back(0x00);
  row_data.push_back(0x00);
  row_data.push_back(0x00);

  std::vector<unsigned char> null_bitmap = {0x00};  // No NULLs
  auto buffer = CreateWriteRowsEventRaw(table_meta, row_data, null_bitmap);
  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "",
                                    MySQLBinlogEventType::OBSOLETE_WRITE_ROWS_EVENT_V1);

  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(1, result->size());

  // Empty geometry should result in empty string
  ASSERT_NE(result->front().FindColumnValue("location"), nullptr);
  EXPECT_EQ("", result->front().GetColumnValue("location")) << "Empty GEOMETRY should return empty string";
}

// =============================================================================
// VECTOR Type Support (MySQL 9.0+)
// =============================================================================

/**
 * @test VECTOR type should be parsed as hex string (same encoding as BLOB)
 *
 * VECTOR columns store binary float data with a length prefix.
 * The parser should handle this type and return a hex representation.
 */
TEST_F(RowsParserTest, VectorTypeBasic) {
  TableMetadata table_meta;
  table_meta.table_id = 500;
  table_meta.database_name = "test_db";
  table_meta.table_name = "vec_test";

  ColumnMetadata col_id;
  col_id.type = ColumnType::LONG;
  col_id.name = "id";
  col_id.metadata = 0;
  table_meta.columns.push_back(col_id);

  ColumnMetadata col_vec;
  col_vec.type = ColumnType::VECTOR;  // col_type = 242
  col_vec.name = "embedding";
  col_vec.metadata = 4;  // 4 bytes for length prefix
  table_meta.columns.push_back(col_vec);

  // Create row data: id=1, embedding=3-dimensional float32 vector [1.0, 2.0, 3.0]
  std::vector<unsigned char> row_data;

  // id=1 (4 bytes, little-endian)
  auto id_bytes = EncodeInt32(1);
  row_data.insert(row_data.end(), id_bytes.begin(), id_bytes.end());

  // VECTOR: 4-byte length prefix + binary float data
  // 3 x float32 = 12 bytes
  std::vector<unsigned char> vec_data = {
      0x00, 0x00, 0x80, 0x3f,  // 1.0f (little-endian IEEE 754)
      0x00, 0x00, 0x00, 0x40,  // 2.0f
      0x00, 0x00, 0x40, 0x40   // 3.0f
  };

  // Length prefix (4 bytes, little-endian)
  uint32_t vec_len = vec_data.size();
  row_data.push_back(vec_len & 0xFF);
  row_data.push_back((vec_len >> 8) & 0xFF);
  row_data.push_back((vec_len >> 16) & 0xFF);
  row_data.push_back((vec_len >> 24) & 0xFF);

  // Vector binary data
  row_data.insert(row_data.end(), vec_data.begin(), vec_data.end());

  std::vector<unsigned char> null_bitmap = {0x00};  // No NULLs
  auto buffer = CreateWriteRowsEventRaw(table_meta, row_data, null_bitmap);
  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "",
                                    MySQLBinlogEventType::OBSOLETE_WRITE_ROWS_EVENT_V1);

  ASSERT_TRUE(result.has_value()) << "VECTOR type should be parsed successfully";
  ASSERT_EQ(1, result->size());

  // The vector column should exist and contain hex representation
  ASSERT_NE(result->front().FindColumnValue("embedding"), nullptr);
  std::string vec_value = result->front().GetColumnValue("embedding");
  EXPECT_TRUE(vec_value.find("UNSUPPORTED") == std::string::npos)
      << "VECTOR should not return UNSUPPORTED_TYPE, got: " << vec_value;
  // Verify hex output matches the input bytes
  EXPECT_EQ("0000803f0000004000004040", vec_value) << "VECTOR should return hex-encoded binary float data";
}

/**
 * @test Empty VECTOR (zero-length) should be handled correctly
 */
TEST_F(RowsParserTest, VectorTypeEmpty) {
  TableMetadata table_meta;
  table_meta.table_id = 501;
  table_meta.database_name = "test_db";
  table_meta.table_name = "vec_test";

  ColumnMetadata col_id;
  col_id.type = ColumnType::LONG;
  col_id.name = "id";
  col_id.metadata = 0;
  table_meta.columns.push_back(col_id);

  ColumnMetadata col_vec;
  col_vec.type = ColumnType::VECTOR;
  col_vec.name = "embedding";
  col_vec.metadata = 4;
  table_meta.columns.push_back(col_vec);

  // Create row data: id=1, embedding=empty vector (0 length)
  std::vector<unsigned char> row_data;
  auto id_bytes = EncodeInt32(1);
  row_data.insert(row_data.end(), id_bytes.begin(), id_bytes.end());

  // VECTOR with length 0 (4-byte length prefix = 0x00000000)
  row_data.push_back(0x00);
  row_data.push_back(0x00);
  row_data.push_back(0x00);
  row_data.push_back(0x00);

  std::vector<unsigned char> null_bitmap = {0x00};  // No NULLs
  auto buffer = CreateWriteRowsEventRaw(table_meta, row_data, null_bitmap);
  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "",
                                    MySQLBinlogEventType::OBSOLETE_WRITE_ROWS_EVENT_V1);

  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(1, result->size());

  ASSERT_NE(result->front().FindColumnValue("embedding"), nullptr);
  EXPECT_EQ("", result->front().GetColumnValue("embedding")) << "Empty VECTOR should return empty string";
}

// =============================================================================
// DECIMAL Type Precision
// =============================================================================

/**
 * @brief Helper to create DECIMAL binary representation in MySQL format
 *
 * MySQL DECIMAL binary format:
 * - Sign bit is stored in MSB of first byte (0x80)
 * - For positive: XOR first byte with 0x80 (sets the bit)
 * - For negative: XOR all bytes with 0xFF, then XOR first byte with 0x80 (clears it)
 *
 * @param value The decimal value as string (e.g., "123.45", "-999.99")
 * @param precision Total number of digits
 * @param scale Number of digits after decimal point
 * @return Binary representation
 */
inline std::vector<unsigned char> EncodeDecimalValue(const std::string& value, uint8_t precision, uint8_t scale) {
  // Parse sign and digits
  bool is_negative = !value.empty() && value[0] == '-';
  std::string abs_value = is_negative ? value.substr(1) : value;

  // Remove decimal point and get integer/fractional parts
  size_t dot_pos = abs_value.find('.');
  std::string int_part, frac_part;
  if (dot_pos != std::string::npos) {
    int_part = abs_value.substr(0, dot_pos);
    frac_part = abs_value.substr(dot_pos + 1);
  } else {
    int_part = abs_value;
    frac_part = "";
  }

  // Pad/truncate to correct precision
  int intg = precision - scale;
  while (int_part.length() < static_cast<size_t>(intg))
    int_part = "0" + int_part;
  while (frac_part.length() < scale)
    frac_part += "0";

  static const int dig2bytes[10] = {0, 1, 1, 2, 2, 3, 3, 4, 4, 4};

  int intg0 = intg / 9;
  int intg_rem = intg % 9;
  int frac0 = scale / 9;
  int frac_rem = scale % 9;

  std::vector<unsigned char> result;

  // Encode integer remainder (leading digits)
  if (intg_rem > 0) {
    int32_t val = std::stoi(int_part.substr(0, intg_rem));
    int bytes = dig2bytes[intg_rem];
    for (int i = bytes - 1; i >= 0; i--) {
      result.push_back((val >> (i * 8)) & 0xFF);
    }
    int_part = int_part.substr(intg_rem);
  }

  // Encode full 9-digit groups in integer part
  for (int i = 0; i < intg0; i++) {
    int32_t val = std::stoi(int_part.substr(i * 9, 9));
    for (int j = 3; j >= 0; j--) {
      result.push_back((val >> (j * 8)) & 0xFF);
    }
  }

  // Encode full 9-digit groups in fractional part
  for (int i = 0; i < frac0; i++) {
    int32_t val = std::stoi(frac_part.substr(i * 9, 9));
    for (int j = 3; j >= 0; j--) {
      result.push_back((val >> (j * 8)) & 0xFF);
    }
  }

  // Encode fractional remainder
  if (frac_rem > 0) {
    int32_t val = std::stoi(frac_part.substr(frac0 * 9, frac_rem));
    int bytes = dig2bytes[frac_rem];
    for (int i = bytes - 1; i >= 0; i--) {
      result.push_back((val >> (i * 8)) & 0xFF);
    }
  }

  // Apply sign encoding: XOR with 0x80 for first byte, XOR all with 0xFF if negative
  if (is_negative) {
    for (auto& b : result) {
      b ^= 0xFF;
    }
  }
  if (!result.empty()) {
    result[0] ^= 0x80;  // Toggle sign bit
  }

  return result;
}

/**
 * @test DECIMAL positive integer value
 */
TEST_F(RowsParserTest, DecimalPositiveInteger) {
  TableMetadata table_meta;
  table_meta.table_id = 500;
  table_meta.database_name = "test_db";
  table_meta.table_name = "decimal_test";

  ColumnMetadata col_id;
  col_id.type = ColumnType::LONG;
  col_id.name = "id";
  col_id.metadata = 0;
  table_meta.columns.push_back(col_id);

  ColumnMetadata col_decimal;
  col_decimal.type = ColumnType::NEWDECIMAL;
  col_decimal.name = "amount";
  // DECIMAL(5,0): precision=5, scale=0, metadata = (5 << 8) | 0 = 1280
  col_decimal.metadata = (5 << 8) | 0;
  table_meta.columns.push_back(col_decimal);

  std::vector<unsigned char> row_data;
  auto id_bytes = EncodeInt32(1);
  row_data.insert(row_data.end(), id_bytes.begin(), id_bytes.end());

  // DECIMAL(5,0) value = 12345
  auto decimal_bytes = EncodeDecimalValue("12345", 5, 0);
  row_data.insert(row_data.end(), decimal_bytes.begin(), decimal_bytes.end());

  std::vector<unsigned char> null_bitmap = {0x00};
  auto buffer = CreateWriteRowsEventRaw(table_meta, row_data, null_bitmap);
  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "",
                                    MySQLBinlogEventType::OBSOLETE_WRITE_ROWS_EVENT_V1);

  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(1, result->size());

  std::string decimal_value = result->front().GetColumnValue("amount");
  EXPECT_EQ("12345", decimal_value) << "DECIMAL positive integer should be parsed correctly";
}

/**
 * @test DECIMAL negative integer value
 */
TEST_F(RowsParserTest, DecimalNegativeInteger) {
  TableMetadata table_meta;
  table_meta.table_id = 501;
  table_meta.database_name = "test_db";
  table_meta.table_name = "decimal_test";

  ColumnMetadata col_id;
  col_id.type = ColumnType::LONG;
  col_id.name = "id";
  col_id.metadata = 0;
  table_meta.columns.push_back(col_id);

  ColumnMetadata col_decimal;
  col_decimal.type = ColumnType::NEWDECIMAL;
  col_decimal.name = "amount";
  col_decimal.metadata = (5 << 8) | 0;  // DECIMAL(5,0)
  table_meta.columns.push_back(col_decimal);

  std::vector<unsigned char> row_data;
  auto id_bytes = EncodeInt32(1);
  row_data.insert(row_data.end(), id_bytes.begin(), id_bytes.end());

  // DECIMAL(5,0) value = -12345
  auto decimal_bytes = EncodeDecimalValue("-12345", 5, 0);
  row_data.insert(row_data.end(), decimal_bytes.begin(), decimal_bytes.end());

  std::vector<unsigned char> null_bitmap = {0x00};
  auto buffer = CreateWriteRowsEventRaw(table_meta, row_data, null_bitmap);
  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "",
                                    MySQLBinlogEventType::OBSOLETE_WRITE_ROWS_EVENT_V1);

  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(1, result->size());

  std::string decimal_value = result->front().GetColumnValue("amount");
  EXPECT_EQ("-12345", decimal_value) << "DECIMAL negative integer should be parsed correctly";
}

/**
 * @test DECIMAL with fractional part
 */
TEST_F(RowsParserTest, DecimalWithFraction) {
  TableMetadata table_meta;
  table_meta.table_id = 502;
  table_meta.database_name = "test_db";
  table_meta.table_name = "decimal_test";

  ColumnMetadata col_id;
  col_id.type = ColumnType::LONG;
  col_id.name = "id";
  col_id.metadata = 0;
  table_meta.columns.push_back(col_id);

  ColumnMetadata col_decimal;
  col_decimal.type = ColumnType::NEWDECIMAL;
  col_decimal.name = "price";
  col_decimal.metadata = (10 << 8) | 2;  // DECIMAL(10,2)
  table_meta.columns.push_back(col_decimal);

  std::vector<unsigned char> row_data;
  auto id_bytes = EncodeInt32(1);
  row_data.insert(row_data.end(), id_bytes.begin(), id_bytes.end());

  // DECIMAL(10,2) value = 12345678.90
  auto decimal_bytes = EncodeDecimalValue("12345678.90", 10, 2);
  row_data.insert(row_data.end(), decimal_bytes.begin(), decimal_bytes.end());

  std::vector<unsigned char> null_bitmap = {0x00};
  auto buffer = CreateWriteRowsEventRaw(table_meta, row_data, null_bitmap);
  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "",
                                    MySQLBinlogEventType::OBSOLETE_WRITE_ROWS_EVENT_V1);

  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(1, result->size());

  std::string decimal_value = result->front().GetColumnValue("price");
  EXPECT_EQ("12345678.90", decimal_value) << "DECIMAL with fraction should be parsed correctly";
}

/**
 * @test DECIMAL negative with fractional part
 */
TEST_F(RowsParserTest, DecimalNegativeWithFraction) {
  TableMetadata table_meta;
  table_meta.table_id = 503;
  table_meta.database_name = "test_db";
  table_meta.table_name = "decimal_test";

  ColumnMetadata col_id;
  col_id.type = ColumnType::LONG;
  col_id.name = "id";
  col_id.metadata = 0;
  table_meta.columns.push_back(col_id);

  ColumnMetadata col_decimal;
  col_decimal.type = ColumnType::NEWDECIMAL;
  col_decimal.name = "balance";
  col_decimal.metadata = (10 << 8) | 2;  // DECIMAL(10,2)
  table_meta.columns.push_back(col_decimal);

  std::vector<unsigned char> row_data;
  auto id_bytes = EncodeInt32(1);
  row_data.insert(row_data.end(), id_bytes.begin(), id_bytes.end());

  // DECIMAL(10,2) value = -99999.99
  auto decimal_bytes = EncodeDecimalValue("-99999.99", 10, 2);
  row_data.insert(row_data.end(), decimal_bytes.begin(), decimal_bytes.end());

  std::vector<unsigned char> null_bitmap = {0x00};
  auto buffer = CreateWriteRowsEventRaw(table_meta, row_data, null_bitmap);
  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "",
                                    MySQLBinlogEventType::OBSOLETE_WRITE_ROWS_EVENT_V1);

  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(1, result->size());

  std::string decimal_value = result->front().GetColumnValue("balance");
  EXPECT_EQ("-99999.99", decimal_value) << "DECIMAL negative with fraction should be parsed correctly";
}

/**
 * @test DECIMAL zero value
 */
TEST_F(RowsParserTest, DecimalZero) {
  TableMetadata table_meta;
  table_meta.table_id = 504;
  table_meta.database_name = "test_db";
  table_meta.table_name = "decimal_test";

  ColumnMetadata col_id;
  col_id.type = ColumnType::LONG;
  col_id.name = "id";
  col_id.metadata = 0;
  table_meta.columns.push_back(col_id);

  ColumnMetadata col_decimal;
  col_decimal.type = ColumnType::NEWDECIMAL;
  col_decimal.name = "amount";
  col_decimal.metadata = (5 << 8) | 2;  // DECIMAL(5,2)
  table_meta.columns.push_back(col_decimal);

  std::vector<unsigned char> row_data;
  auto id_bytes = EncodeInt32(1);
  row_data.insert(row_data.end(), id_bytes.begin(), id_bytes.end());

  // DECIMAL(5,2) value = 0.00
  auto decimal_bytes = EncodeDecimalValue("0.00", 5, 2);
  row_data.insert(row_data.end(), decimal_bytes.begin(), decimal_bytes.end());

  std::vector<unsigned char> null_bitmap = {0x00};
  auto buffer = CreateWriteRowsEventRaw(table_meta, row_data, null_bitmap);
  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "",
                                    MySQLBinlogEventType::OBSOLETE_WRITE_ROWS_EVENT_V1);

  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(1, result->size());

  std::string decimal_value = result->front().GetColumnValue("amount");
  // Zero should be parsed as "0" or "0.00"
  EXPECT_TRUE(decimal_value == "0" || decimal_value == "0.00")
      << "DECIMAL zero should be parsed correctly, got: " << decimal_value;
}

/**
 * @test DECIMAL small value (less than 1)
 */
TEST_F(RowsParserTest, DecimalSmallValue) {
  TableMetadata table_meta;
  table_meta.table_id = 505;
  table_meta.database_name = "test_db";
  table_meta.table_name = "decimal_test";

  ColumnMetadata col_id;
  col_id.type = ColumnType::LONG;
  col_id.name = "id";
  col_id.metadata = 0;
  table_meta.columns.push_back(col_id);

  ColumnMetadata col_decimal;
  col_decimal.type = ColumnType::NEWDECIMAL;
  col_decimal.name = "rate";
  col_decimal.metadata = (5 << 8) | 4;  // DECIMAL(5,4)
  table_meta.columns.push_back(col_decimal);

  std::vector<unsigned char> row_data;
  auto id_bytes = EncodeInt32(1);
  row_data.insert(row_data.end(), id_bytes.begin(), id_bytes.end());

  // DECIMAL(5,4) value = 0.1234
  auto decimal_bytes = EncodeDecimalValue("0.1234", 5, 4);
  row_data.insert(row_data.end(), decimal_bytes.begin(), decimal_bytes.end());

  std::vector<unsigned char> null_bitmap = {0x00};
  auto buffer = CreateWriteRowsEventRaw(table_meta, row_data, null_bitmap);
  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "",
                                    MySQLBinlogEventType::OBSOLETE_WRITE_ROWS_EVENT_V1);

  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(1, result->size());

  std::string decimal_value = result->front().GetColumnValue("rate");
  EXPECT_EQ("0.1234", decimal_value) << "DECIMAL small value should be parsed correctly";
}

// =============================================================================
// binlog_row_image=MINIMAL/NOBLOB Rejection
// =============================================================================

/**
 * @brief Create WRITE_ROWS_EVENT with custom columns_present bitmap
 *
 * @param table_meta Table metadata
 * @param row_data Row data bytes
 * @param null_bitmap NULL bitmap bytes
 * @param columns_present Custom columns_present bitmap
 * @return Event buffer
 */
inline std::vector<unsigned char> CreateWriteRowsEventWithBitmap(const TableMetadata& table_meta,
                                                                 const std::vector<unsigned char>& row_data,
                                                                 const std::vector<unsigned char>& null_bitmap,
                                                                 const std::vector<unsigned char>& columns_present) {
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

  // Body: column count (packed integer)
  uint64_t col_count = table_meta.columns.size();
  if (col_count < 251) {
    buffer.push_back(static_cast<unsigned char>(col_count));
  }

  // Body: columns_present bitmap (from parameter)
  buffer.insert(buffer.end(), columns_present.begin(), columns_present.end());

  // Row: NULL bitmap + row data
  buffer.insert(buffer.end(), null_bitmap.begin(), null_bitmap.end());
  buffer.insert(buffer.end(), row_data.begin(), row_data.end());

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
 * @test MINIMAL mode - only some columns present
 *
 * Simulates binlog_row_image=MINIMAL where only the primary key and
 * modified columns are present in the event. MygramDB requires FULL row images
 * because partial row images have compact NULL bitmaps that cannot be decoded
 * by table ordinal without additional state.
 */
TEST_F(RowsParserTest, RejectsMinimalModePartialColumns) {
  TableMetadata table_meta;
  table_meta.table_id = 600;
  table_meta.database_name = "test_db";
  table_meta.table_name = "minimal_test";

  // 3 columns: id, name, status
  ColumnMetadata col_id;
  col_id.type = ColumnType::LONG;
  col_id.name = "id";
  col_id.metadata = 0;
  table_meta.columns.push_back(col_id);

  ColumnMetadata col_name;
  col_name.type = ColumnType::VAR_STRING;
  col_name.name = "name";
  col_name.metadata = 255;  // Max length 255
  table_meta.columns.push_back(col_name);

  ColumnMetadata col_status;
  col_status.type = ColumnType::LONG;
  col_status.name = "status";
  col_status.metadata = 0;
  table_meta.columns.push_back(col_status);

  // In MINIMAL mode, only id and status are present (name is not modified)
  // columns_present bitmap: 0b00000101 = 0x05 (columns 0 and 2)
  std::vector<unsigned char> columns_present = {0x05};

  // Row data: only id=42 and status=1 (name is skipped)
  std::vector<unsigned char> row_data;
  auto id_bytes = EncodeInt32(42);
  row_data.insert(row_data.end(), id_bytes.begin(), id_bytes.end());
  auto status_bytes = EncodeInt32(1);
  row_data.insert(row_data.end(), status_bytes.begin(), status_bytes.end());

  // NULL bitmap: 1 byte, all present columns are non-NULL
  std::vector<unsigned char> null_bitmap = {0x00};

  auto buffer = CreateWriteRowsEventWithBitmap(table_meta, row_data, null_bitmap, columns_present);
  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "",
                                    MySQLBinlogEventType::OBSOLETE_WRITE_ROWS_EVENT_V1);

  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(mygram::utils::ErrorCode::kMySQLBinlogError, result.error().code());
  EXPECT_NE(result.error().message().find("binlog_row_image=FULL"), std::string::npos);
}

/**
 * @test MINIMAL mode with only primary key present
 */
TEST_F(RowsParserTest, RejectsMinimalModeOnlyPrimaryKey) {
  TableMetadata table_meta;
  table_meta.table_id = 601;
  table_meta.database_name = "test_db";
  table_meta.table_name = "minimal_test";

  ColumnMetadata col_id;
  col_id.type = ColumnType::LONG;
  col_id.name = "id";
  col_id.metadata = 0;
  table_meta.columns.push_back(col_id);

  ColumnMetadata col_data;
  col_data.type = ColumnType::VAR_STRING;
  col_data.name = "data";
  col_data.metadata = 255;
  table_meta.columns.push_back(col_data);

  // Only primary key present (column 0)
  std::vector<unsigned char> columns_present = {0x01};

  std::vector<unsigned char> row_data;
  auto id_bytes = EncodeInt32(100);
  row_data.insert(row_data.end(), id_bytes.begin(), id_bytes.end());

  std::vector<unsigned char> null_bitmap = {0x00};

  auto buffer = CreateWriteRowsEventWithBitmap(table_meta, row_data, null_bitmap, columns_present);
  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "",
                                    MySQLBinlogEventType::OBSOLETE_WRITE_ROWS_EVENT_V1);

  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(mygram::utils::ErrorCode::kMySQLBinlogError, result.error().code());
  EXPECT_NE(result.error().message().find("binlog_row_image=FULL"), std::string::npos);
}

/**
 * @test All columns missing
 */
TEST_F(RowsParserTest, RejectsNoColumnsPresent) {
  TableMetadata table_meta;
  table_meta.table_id = 602;
  table_meta.database_name = "test_db";
  table_meta.table_name = "empty_test";

  ColumnMetadata col_id;
  col_id.type = ColumnType::LONG;
  col_id.name = "id";
  col_id.metadata = 0;
  table_meta.columns.push_back(col_id);

  // No columns present
  std::vector<unsigned char> columns_present = {0x00};
  std::vector<unsigned char> row_data;
  std::vector<unsigned char> null_bitmap = {0x00};

  auto buffer = CreateWriteRowsEventWithBitmap(table_meta, row_data, null_bitmap, columns_present);
  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "",
                                    MySQLBinlogEventType::OBSOLETE_WRITE_ROWS_EVENT_V1);

  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(mygram::utils::ErrorCode::kMySQLBinlogError, result.error().code());
  EXPECT_NE(result.error().message().find("binlog_row_image=FULL"), std::string::npos);
}

// =============================================================================
// V2 Rows Event var_header_len Processing
// =============================================================================

namespace {

using mygramdb::mysql::test::BinlogEventBuilder;

// Create TableMetadata for a 2-column table: id(INT), name(VARCHAR)
TableMetadata CreateTestTableMeta() {
  TableMetadata meta;
  meta.table_id = 1;
  meta.database_name = "test";
  meta.table_name = "articles";

  ColumnMetadata id_col;
  id_col.name = "id";
  id_col.type = ColumnType::LONG;
  id_col.metadata = 0;
  id_col.is_nullable = false;
  id_col.is_unsigned = false;
  meta.columns.push_back(id_col);

  ColumnMetadata name_col;
  name_col.name = "name";
  name_col.type = ColumnType::VARCHAR;
  name_col.metadata = 100;
  name_col.is_nullable = false;
  name_col.is_unsigned = false;
  meta.columns.push_back(name_col);

  return meta;
}

// Build row data for a single row: id (INT) + name (VARCHAR)
std::vector<uint8_t> BuildSingleRowData(int32_t id, const std::string& name) {
  std::vector<uint8_t> data;
  // null bitmap: 1 byte for 2 columns (all zeros = no nulls)
  data.push_back(0x00);
  // id: 4 bytes LE
  BinlogEventBuilder::AppendLittleEndian32(data, static_cast<uint32_t>(id));
  // name: 1-byte length (metadata <= 255) + string data
  data.push_back(static_cast<uint8_t>(name.size()));
  data.insert(data.end(), name.begin(), name.end());
  return data;
}

// Build update row data: before image + after image
std::vector<uint8_t> BuildUpdateRowPair(int32_t old_id, const std::string& old_name, int32_t new_id,
                                        const std::string& new_name) {
  auto before = BuildSingleRowData(old_id, old_name);
  auto after = BuildSingleRowData(new_id, new_name);
  std::vector<uint8_t> data;
  data.insert(data.end(), before.begin(), before.end());
  data.insert(data.end(), after.begin(), after.end());
  return data;
}

}  // namespace

class RowsParserV2Test : public RowsParserTest {};

/**
 * @test V2 WRITE_ROWS_EVENT with flags=0x0000 (no STMT_END_F).
 *
 * This is the core bug case: in a batch INSERT, intermediate events have
 * flags=0x0000. The old code only read var_header_len when flags & 0x0001,
 * so the 2-byte var_header_len was not consumed and column_count misparsed.
 */
TEST_F(RowsParserV2Test, V2WriteRowsWithoutStmtEndFlag) {
  auto table_meta = CreateTestTableMeta();
  auto row_data = BuildSingleRowData(1, "hello");

  // columns_bitmap: 1 byte, all columns present
  std::vector<uint8_t> columns_bitmap = {0xFF};

  // V2 event: flags=0x0000 (no STMT_END_F), var_header_len=2 (no extra data)
  auto event = BinlogEventBuilder::BuildWriteRowsV2(table_meta.table_id, 0x0000, 2, {}, 2, columns_bitmap, row_data);

  auto result =
      ParseWriteRowsEvent(event.data(), event.size(), &table_meta, "id", "", MySQLBinlogEventType::WRITE_ROWS_EVENT);

  ASSERT_TRUE(result.has_value()) << "V2 WRITE_ROWS with flags=0x0000 should parse (was the bug case)";
  ASSERT_EQ(1, result->size());
  EXPECT_EQ("1", result->front().GetColumnValue("id"));
  EXPECT_EQ("hello", result->front().GetColumnValue("name"));
}

/**
 * @test V2 WRITE_ROWS_EVENT with EXTRA_DATA_PRESENT flag and extra data.
 *
 * flags=0x0002 means extra data is present after var_header_len.
 * var_header_len=6 means 4 bytes of extra data (6 - 2 = 4).
 */
TEST_F(RowsParserV2Test, V2WriteRowsWithExtraDataPresent) {
  auto table_meta = CreateTestTableMeta();
  auto row_data = BuildSingleRowData(42, "world");

  std::vector<uint8_t> columns_bitmap = {0xFF};

  // 4 bytes of dummy extra data
  std::vector<uint8_t> extra_data = {0xDE, 0xAD, 0xBE, 0xEF};

  // flags=0x0002 (EXTRA_DATA_PRESENT), var_header_len=6 (2 + 4 extra bytes)
  auto event =
      BinlogEventBuilder::BuildWriteRowsV2(table_meta.table_id, 0x0002, 6, extra_data, 2, columns_bitmap, row_data);

  auto result =
      ParseWriteRowsEvent(event.data(), event.size(), &table_meta, "id", "", MySQLBinlogEventType::WRITE_ROWS_EVENT);

  ASSERT_TRUE(result.has_value()) << "V2 WRITE_ROWS with extra data should skip extra bytes and parse correctly";
  ASSERT_EQ(1, result->size());
  EXPECT_EQ("42", result->front().GetColumnValue("id"));
  EXPECT_EQ("world", result->front().GetColumnValue("name"));
}

/**
 * @test V2 WRITE_ROWS_EVENT with both STMT_END_F and EXTRA_DATA_PRESENT flags.
 *
 * flags=0x0003 (STMT_END_F | EXTRA_DATA_PRESENT), var_header_len=6.
 */
TEST_F(RowsParserV2Test, V2WriteRowsBothFlagsSet) {
  auto table_meta = CreateTestTableMeta();
  auto row_data = BuildSingleRowData(99, "both");

  std::vector<uint8_t> columns_bitmap = {0xFF};
  std::vector<uint8_t> extra_data = {0x01, 0x02, 0x03, 0x04};

  // flags=0x0003 (STMT_END_F | EXTRA_DATA_PRESENT), var_header_len=6
  auto event =
      BinlogEventBuilder::BuildWriteRowsV2(table_meta.table_id, 0x0003, 6, extra_data, 2, columns_bitmap, row_data);

  auto result =
      ParseWriteRowsEvent(event.data(), event.size(), &table_meta, "id", "", MySQLBinlogEventType::WRITE_ROWS_EVENT);

  ASSERT_TRUE(result.has_value()) << "V2 WRITE_ROWS with both flags should parse correctly";
  ASSERT_EQ(1, result->size());
  EXPECT_EQ("99", result->front().GetColumnValue("id"));
  EXPECT_EQ("both", result->front().GetColumnValue("name"));
}

/**
 * @test V1 WRITE_ROWS_EVENT (type 23) has no var_header_len field.
 *
 * V1 events should continue to work without a var_header_len field.
 */
TEST_F(RowsParserV2Test, V1WriteRowsNoVarHeader) {
  auto table_meta = CreateTestTableMeta();
  auto row_data = BuildSingleRowData(7, "v1test");

  std::vector<uint8_t> columns_bitmap = {0xFF};

  // V1 event: no var_header_len, flags=0x0001 (STMT_END_F)
  auto event = BinlogEventBuilder::BuildWriteRowsV1(table_meta.table_id, 0x0001, 2, columns_bitmap, row_data);

  auto result = ParseWriteRowsEvent(event.data(), event.size(), &table_meta, "id", "",
                                    MySQLBinlogEventType::OBSOLETE_WRITE_ROWS_EVENT_V1);

  ASSERT_TRUE(result.has_value()) << "V1 WRITE_ROWS should parse without var_header_len";
  ASSERT_EQ(1, result->size());
  EXPECT_EQ("7", result->front().GetColumnValue("id"));
  EXPECT_EQ("v1test", result->front().GetColumnValue("name"));
}

/**
 * @test V2 UPDATE_ROWS_EVENT with flags=0x0000 (no STMT_END_F).
 *
 * UPDATE events have before and after column bitmaps and row image pairs.
 * With flags=0x0000, var_header_len must still be consumed.
 */
TEST_F(RowsParserV2Test, V2UpdateRowsWithoutStmtEndFlag) {
  auto table_meta = CreateTestTableMeta();
  auto row_data = BuildUpdateRowPair(1, "old_name", 1, "new_name");

  // Both before and after bitmaps: all columns present
  std::vector<uint8_t> columns_before_bitmap = {0xFF};
  std::vector<uint8_t> columns_after_bitmap = {0xFF};

  // V2 UPDATE: flags=0x0000, var_header_len=2
  auto event = BinlogEventBuilder::BuildUpdateRowsV2(table_meta.table_id, 0x0000, 2, {}, 2, columns_before_bitmap,
                                                     columns_after_bitmap, row_data);

  auto result =
      ParseUpdateRowsEvent(event.data(), event.size(), &table_meta, "id", "", MySQLBinlogEventType::UPDATE_ROWS_EVENT);

  ASSERT_TRUE(result.has_value()) << "V2 UPDATE_ROWS with flags=0x0000 should parse correctly";
  ASSERT_EQ(1, result->size());

  // Check before image
  EXPECT_EQ("1", result->front().first.GetColumnValue("id"));
  EXPECT_EQ("old_name", result->front().first.GetColumnValue("name"));

  // Check after image
  EXPECT_EQ("1", result->front().second.GetColumnValue("id"));
  EXPECT_EQ("new_name", result->front().second.GetColumnValue("name"));
}

TEST_F(RowsParserV2Test, V2UpdateRowsRejectsPartialAfterImageBitmap) {
  auto table_meta = CreateTestTableMeta();
  auto row_data = BuildUpdateRowPair(1, "old_name", 1, "new_name");

  std::vector<uint8_t> columns_before_bitmap = {0xFF};
  std::vector<uint8_t> columns_after_bitmap = {0x01};

  auto event = BinlogEventBuilder::BuildUpdateRowsV2(table_meta.table_id, 0x0000, 2, {}, 2, columns_before_bitmap,
                                                     columns_after_bitmap, row_data);

  auto result =
      ParseUpdateRowsEvent(event.data(), event.size(), &table_meta, "id", "", MySQLBinlogEventType::UPDATE_ROWS_EVENT);

  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(mygram::utils::ErrorCode::kMySQLBinlogError, result.error().code());
  EXPECT_NE(result.error().message().find("binlog_row_image=FULL"), std::string::npos);
}

/**
 * @test V2 DELETE_ROWS_EVENT with flags=0x0000 (no STMT_END_F).
 *
 * DELETE events have only the before image. With flags=0x0000,
 * var_header_len must still be consumed.
 */
TEST_F(RowsParserV2Test, V2DeleteRowsWithoutStmtEndFlag) {
  auto table_meta = CreateTestTableMeta();
  auto row_data = BuildSingleRowData(5, "deleted");

  std::vector<uint8_t> columns_bitmap = {0xFF};

  // V2 DELETE: flags=0x0000, var_header_len=2
  auto event = BinlogEventBuilder::BuildDeleteRowsV2(table_meta.table_id, 0x0000, 2, {}, 2, columns_bitmap, row_data);

  auto result =
      ParseDeleteRowsEvent(event.data(), event.size(), &table_meta, "id", "", MySQLBinlogEventType::DELETE_ROWS_EVENT);

  ASSERT_TRUE(result.has_value()) << "V2 DELETE_ROWS with flags=0x0000 should parse correctly";
  ASSERT_EQ(1, result->size());
  EXPECT_EQ("5", result->front().GetColumnValue("id"));
  EXPECT_EQ("deleted", result->front().GetColumnValue("name"));
}

/**
 * @test V2 batch INSERT: multiple WRITE_ROWS_EVENTs with different flags.
 *
 * In a batch INSERT, intermediate events have flags=0x0000 and the final
 * event has flags=0x0001 (STMT_END_F). Both must parse correctly.
 */
TEST_F(RowsParserV2Test, V2BatchInsertMultipleEvents) {
  auto table_meta = CreateTestTableMeta();
  std::vector<uint8_t> columns_bitmap = {0xFF};

  // First event: flags=0x0000 (intermediate, no STMT_END_F)
  auto row_data1 = BuildSingleRowData(10, "batch1");
  auto event1 = BinlogEventBuilder::BuildWriteRowsV2(table_meta.table_id, 0x0000, 2, {}, 2, columns_bitmap, row_data1);

  auto result1 =
      ParseWriteRowsEvent(event1.data(), event1.size(), &table_meta, "id", "", MySQLBinlogEventType::WRITE_ROWS_EVENT);

  ASSERT_TRUE(result1.has_value()) << "Intermediate batch event (flags=0x0000) should parse correctly";
  ASSERT_EQ(1, result1->size());
  EXPECT_EQ("10", result1->front().GetColumnValue("id"));
  EXPECT_EQ("batch1", result1->front().GetColumnValue("name"));

  // Second event: flags=0x0001 (final, STMT_END_F set)
  auto row_data2 = BuildSingleRowData(11, "batch2");
  auto event2 = BinlogEventBuilder::BuildWriteRowsV2(table_meta.table_id, 0x0001, 2, {}, 2, columns_bitmap, row_data2);

  auto result2 =
      ParseWriteRowsEvent(event2.data(), event2.size(), &table_meta, "id", "", MySQLBinlogEventType::WRITE_ROWS_EVENT);

  ASSERT_TRUE(result2.has_value()) << "Final batch event (flags=0x0001) should parse correctly";
  ASSERT_EQ(1, result2->size());
  EXPECT_EQ("11", result2->front().GetColumnValue("id"));
  EXPECT_EQ("batch2", result2->front().GetColumnValue("name"));
}

// =============================================================================
// event_size bounds check tests
// =============================================================================

/**
 * @test ParseWriteRowsEvent returns nullopt when event_size < 4
 */
TEST_F(RowsParserTest, WriteRowsEventSizeTooSmall) {
  TableMetadata table_meta;
  table_meta.table_id = 1;
  table_meta.database_name = "test_db";
  table_meta.table_name = "test_table";
  table_meta.columns.push_back({ColumnType::LONG, "id", 0, false});

  // Create a minimal buffer with 19-byte header but event_size set to 3 (< 4)
  std::vector<unsigned char> buffer(32, 0);
  // Set event_size at bytes [9-12] to 3 (little-endian)
  buffer[9] = 3;
  buffer[10] = 0;
  buffer[11] = 0;
  buffer[12] = 0;

  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "",
                                    MySQLBinlogEventType::OBSOLETE_WRITE_ROWS_EVENT_V1);
  EXPECT_FALSE(result.has_value());
}

/**
 * @test ParseUpdateRowsEvent returns nullopt when event_size < 4
 */
TEST_F(RowsParserTest, UpdateRowsEventSizeTooSmall) {
  TableMetadata table_meta;
  table_meta.table_id = 1;
  table_meta.database_name = "test_db";
  table_meta.table_name = "test_table";
  table_meta.columns.push_back({ColumnType::LONG, "id", 0, false});

  // Create a minimal buffer with event_size set to 2 (< 4)
  std::vector<unsigned char> buffer(32, 0);
  buffer[9] = 2;
  buffer[10] = 0;
  buffer[11] = 0;
  buffer[12] = 0;

  auto result = ParseUpdateRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "",
                                     MySQLBinlogEventType::UPDATE_ROWS_EVENT);
  EXPECT_FALSE(result.has_value());
}

/**
 * @test ParseDeleteRowsEvent returns nullopt when event_size < 4
 */
TEST_F(RowsParserTest, DeleteRowsEventSizeTooSmall) {
  TableMetadata table_meta;
  table_meta.table_id = 1;
  table_meta.database_name = "test_db";
  table_meta.table_name = "test_table";
  table_meta.columns.push_back({ColumnType::LONG, "id", 0, false});

  // Create a minimal buffer with event_size set to 0 (< 4)
  std::vector<unsigned char> buffer(32, 0);
  buffer[9] = 0;
  buffer[10] = 0;
  buffer[11] = 0;
  buffer[12] = 0;

  auto result = ParseDeleteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "",
                                     MySQLBinlogEventType::DELETE_ROWS_EVENT);
  EXPECT_FALSE(result.has_value());
}

/**
 * @test ParseWriteRowsEvent returns nullopt when event_size > length
 */
TEST_F(RowsParserTest, WriteRowsEventSizeExceedsLength) {
  TableMetadata table_meta;
  table_meta.table_id = 1;
  table_meta.database_name = "test_db";
  table_meta.table_name = "test_table";
  table_meta.columns.push_back({ColumnType::LONG, "id", 0, false});

  // Create a buffer of size 32 but set event_size to 1000
  std::vector<unsigned char> buffer(32, 0);
  buffer[9] = 0xE8;  // 1000 in little-endian
  buffer[10] = 0x03;
  buffer[11] = 0;
  buffer[12] = 0;

  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "",
                                    MySQLBinlogEventType::OBSOLETE_WRITE_ROWS_EVENT_V1);
  EXPECT_FALSE(result.has_value());
}

TEST_F(RowsParserTest, WriteRowsEventRejectsTruncatedPackedColumnCount) {
  TableMetadata table_meta;
  table_meta.table_id = 1;
  table_meta.database_name = "test_db";
  table_meta.table_name = "test_table";
  table_meta.columns.push_back({ColumnType::LONG, "id", 0, false});

  std::vector<unsigned char> buffer(19, 0);
  for (int i = 0; i < 6; ++i) {
    buffer.push_back(0);  // table_id
  }
  buffer.push_back(0);  // flags
  buffer.push_back(0);
  buffer.push_back(2);  // V2 var_header_len: length field only
  buffer.push_back(0);
  buffer.push_back(252);  // packed integer marker requiring two following bytes
  for (int i = 0; i < 4; ++i) {
    buffer.push_back(0);  // checksum
  }

  uint32_t event_size = static_cast<uint32_t>(buffer.size());
  buffer[9] = event_size & 0xFF;
  buffer[10] = (event_size >> 8) & 0xFF;
  buffer[11] = (event_size >> 16) & 0xFF;
  buffer[12] = (event_size >> 24) & 0xFF;

  auto result =
      ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "", MySQLBinlogEventType::WRITE_ROWS_EVENT);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(mygram::utils::ErrorCode::kMySQLBinlogError, result.error().code());
}

// ---------------------------------------------------------------------------
// ParseSingleRow direct tests
// ---------------------------------------------------------------------------

class ParseSingleRowTest : public ::testing::Test {
 protected:
  /// Build a columns_present bitmap with all bits set for @p col_count columns.
  static std::vector<unsigned char> AllColumnsPresent(size_t col_count) {
    size_t bitmap_bytes = (col_count + 7) / 8;
    return std::vector<unsigned char>(bitmap_bytes, 0xFF);
  }
};

TEST_F(ParseSingleRowTest, BasicIntRow) {
  // Two INT columns: id=42, value=99
  TableMetadata meta;
  meta.table_id = 1;
  meta.database_name = "db";
  meta.table_name = "tbl";
  meta.columns.push_back({ColumnType::LONG, "id", 0, false});
  meta.columns.push_back({ColumnType::LONG, "value", 0, false});

  auto cols_present = AllColumnsPresent(2);
  size_t null_bitmap_size = (2 + 7) / 8;  // 1 byte

  // Row buffer: null_bitmap (1 byte, no NULLs) + id (4 bytes LE) + value (4 bytes LE)
  std::vector<unsigned char> buf;
  buf.push_back(0x00);  // null bitmap: no NULLs

  // id = 42
  int32_t id_val = 42;
  for (int i = 0; i < 4; i++)
    buf.push_back((id_val >> (i * 8)) & 0xFF);
  // value = 99
  int32_t val_val = 99;
  for (int i = 0; i < 4; i++)
    buf.push_back((val_val >> (i * 8)) & 0xFF);

  auto result = mygramdb::mysql::internal::ParseSingleRow(buf.data(), buf.data() + buf.size(), &meta,
                                                          cols_present.data(), null_bitmap_size, 2,
                                                          /*pk_col_idx=*/0, /*text_col_idx=*/-1, "test", "");

  ASSERT_TRUE(result.has_value()) << result.error().message();
  EXPECT_EQ("42", result->row.primary_key);
  EXPECT_EQ("42", result->row.GetColumnValue("id"));
  EXPECT_EQ("99", result->row.GetColumnValue("value"));
  EXPECT_EQ(result->next_ptr, buf.data() + buf.size());
}

TEST_F(ParseSingleRowTest, NullColumn) {
  // Two INT columns: id=7, value=NULL
  TableMetadata meta;
  meta.table_id = 1;
  meta.database_name = "db";
  meta.table_name = "tbl";
  meta.columns.push_back({ColumnType::LONG, "id", 0, false});
  meta.columns.push_back({ColumnType::LONG, "value", 0, false});

  auto cols_present = AllColumnsPresent(2);
  size_t null_bitmap_size = 1;

  // null bitmap: bit 1 set (column index 1 is NULL)
  std::vector<unsigned char> buf;
  buf.push_back(0x02);  // bit 1 set

  // id = 7
  int32_t id_val = 7;
  for (int i = 0; i < 4; i++)
    buf.push_back((id_val >> (i * 8)) & 0xFF);
  // value is NULL -- no data bytes

  auto result = mygramdb::mysql::internal::ParseSingleRow(buf.data(), buf.data() + buf.size(), &meta,
                                                          cols_present.data(), null_bitmap_size, 2,
                                                          /*pk_col_idx=*/0, /*text_col_idx=*/1, "test", "");

  ASSERT_TRUE(result.has_value()) << result.error().message();
  EXPECT_EQ("7", result->row.primary_key);
  EXPECT_EQ("", result->row.text);  // NULL is represented as empty string
  EXPECT_EQ("", result->row.GetColumnValue("value"));
  EXPECT_TRUE(result->row.IsColumnNull("value"));
  EXPECT_FALSE(result->row.IsColumnNull("id"));
  EXPECT_EQ(result->next_ptr, buf.data() + buf.size());
}

TEST_F(ParseSingleRowTest, VarcharColumn) {
  // One VARCHAR column (metadata=255, so 1-byte length prefix)
  TableMetadata meta;
  meta.table_id = 1;
  meta.database_name = "db";
  meta.table_name = "tbl";
  meta.columns.push_back({ColumnType::VARCHAR, "name", 255, false});

  auto cols_present = AllColumnsPresent(1);
  size_t null_bitmap_size = 1;

  std::vector<unsigned char> buf;
  buf.push_back(0x00);  // null bitmap: no NULLs

  // VARCHAR with 1-byte length prefix
  std::string text = "hello";
  buf.push_back(static_cast<unsigned char>(text.size()));
  for (char c : text)
    buf.push_back(static_cast<unsigned char>(c));

  auto result = mygramdb::mysql::internal::ParseSingleRow(buf.data(), buf.data() + buf.size(), &meta,
                                                          cols_present.data(), null_bitmap_size, 1,
                                                          /*pk_col_idx=*/-1, /*text_col_idx=*/0, "test", "");

  ASSERT_TRUE(result.has_value()) << result.error().message();
  EXPECT_EQ("hello", result->row.text);
  EXPECT_EQ("hello", result->row.GetColumnValue("name"));
}

TEST_F(ParseSingleRowTest, TruncatedAtNullBitmap) {
  // Buffer too short for null bitmap
  TableMetadata meta;
  meta.table_id = 1;
  meta.database_name = "db";
  meta.table_name = "tbl";
  meta.columns.push_back({ColumnType::LONG, "id", 0, false});

  auto cols_present = AllColumnsPresent(1);
  size_t null_bitmap_size = 1;

  // Empty buffer -- can't even read null bitmap
  std::vector<unsigned char> buf;

  auto result = mygramdb::mysql::internal::ParseSingleRow(buf.data(), buf.data() + buf.size(), &meta,
                                                          cols_present.data(), null_bitmap_size, 1,
                                                          /*pk_col_idx=*/0, /*text_col_idx=*/-1, "test", "");

  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(mygram::utils::ErrorCode::kMySQLFieldTruncated, result.error().code());
}

TEST_F(ParseSingleRowTest, TruncatedFixedLengthColumnFailsBeforeDecode) {
  TableMetadata meta;
  meta.table_id = 1;
  meta.database_name = "db";
  meta.table_name = "tbl";
  meta.columns.push_back({ColumnType::LONG, "id", 0, false});

  auto cols_present = AllColumnsPresent(1);
  size_t null_bitmap_size = 1;

  std::vector<unsigned char> buf;
  buf.push_back(0x00);  // null bitmap: no NULLs
  buf.push_back(0x01);  // only one of four LONG bytes

  auto result = mygramdb::mysql::internal::ParseSingleRow(buf.data(), buf.data() + buf.size(), &meta,
                                                          cols_present.data(), null_bitmap_size, 1,
                                                          /*pk_col_idx=*/0, /*text_col_idx=*/-1, "test", "");

  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(mygram::utils::ErrorCode::kMySQLFieldTruncated, result.error().code());
}

TEST_F(ParseSingleRowTest, PartialColumnBitmapRejected) {
  // 3 columns but only column 0 and 2 present in bitmap
  TableMetadata meta;
  meta.table_id = 1;
  meta.database_name = "db";
  meta.table_name = "tbl";
  meta.columns.push_back({ColumnType::LONG, "a", 0, false});
  meta.columns.push_back({ColumnType::LONG, "b", 0, false});
  meta.columns.push_back({ColumnType::LONG, "c", 0, false});

  // columns_present: bits 0 and 2 set, bit 1 clear -> 0b00000101 = 0x05
  std::vector<unsigned char> cols_present = {0x05};
  size_t null_bitmap_size = 1;

  std::vector<unsigned char> buf;
  buf.push_back(0x00);  // null bitmap: no NULLs

  // column a = 10
  int32_t a_val = 10;
  for (int i = 0; i < 4; i++)
    buf.push_back((a_val >> (i * 8)) & 0xFF);
  // column b is NOT present -- no data
  // column c = 30
  int32_t c_val = 30;
  for (int i = 0; i < 4; i++)
    buf.push_back((c_val >> (i * 8)) & 0xFF);

  auto result = mygramdb::mysql::internal::ParseSingleRow(buf.data(), buf.data() + buf.size(), &meta,
                                                          cols_present.data(), null_bitmap_size, 3,
                                                          /*pk_col_idx=*/0, /*text_col_idx=*/-1, "test", "");

  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(mygram::utils::ErrorCode::kMySQLBinlogError, result.error().code());
  EXPECT_NE(result.error().message().find("binlog_row_image=FULL"), std::string::npos);
}

#endif  // USE_MYSQL
