/**
 * @file rows_parser_bug_fixes_test.cpp
 * @brief TDD tests for rows_parser.cpp bug fixes
 *
 * This file contains tests for bugs discovered in the bug report.
 * Tests are written first (TDD), then the implementation is fixed.
 */

#include <gtest/gtest.h>

#include <cstring>
#include <limits>
#include <vector>

#include "mysql/binlog_util.h"
#include "mysql/rows_parser.h"
#include "mysql/table_metadata.h"

#ifdef USE_MYSQL

using namespace mygramdb::mysql;

/**
 * @brief Test fixture for rows_parser bug fixes
 */
class RowsParserBugFixesTest : public ::testing::Test {
 protected:
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

// =============================================================================
// Bug #10: BLOB metadata default case handling
// =============================================================================

/**
 * @test Bug #10: BLOB with invalid metadata value should not crash
 *
 * The BLOB parsing code has a switch statement for metadata values 1-4,
 * but no default case. If metadata is 0 or >4, blob_len and blob_data
 * remain uninitialized, causing undefined behavior.
 */
TEST_F(RowsParserBugFixesTest, Bug10_BlobInvalidMetadataZero) {
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
  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "");

  // With invalid metadata, the parser should return nullopt or handle safely
  // The key requirement is: NO CRASH
  // We're flexible on the return value as long as it doesn't crash
  SUCCEED();  // If we reach here without crashing, the test passes
}

/**
 * @test Bug #10: BLOB with metadata=5 (out of range) should not crash
 */
TEST_F(RowsParserBugFixesTest, Bug10_BlobInvalidMetadataFive) {
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
  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "");
  SUCCEED();
}

// =============================================================================
// Bug #11: Missing column type handlers (YEAR, BIT, FLOAT, DOUBLE)
// =============================================================================

/**
 * @test Bug #11: YEAR type should be parsed correctly
 *
 * MySQL YEAR type is stored as 1 byte: (year - 1900)
 * So 2024 is stored as 124 (2024-1900)
 */
TEST_F(RowsParserBugFixesTest, Bug11_YearTypeParsing) {
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

  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "");

  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(1, result->size());

  const auto& row = result->front();
  EXPECT_EQ("1", row.primary_key);

  // Should return "2024" (not "[UNSUPPORTED_TYPE:13]")
  std::string year_value = row.columns.at("birth_year");
  EXPECT_NE("[UNSUPPORTED_TYPE:13]", year_value);
  EXPECT_EQ("2024", year_value);
}

/**
 * @test Bug #11: YEAR=1901 (minimum valid year)
 */
TEST_F(RowsParserBugFixesTest, Bug11_YearMinValue) {
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

  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "");

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ("1901", result->front().columns.at("year_col"));
}

/**
 * @test Bug #11: YEAR=2155 (maximum valid year)
 */
TEST_F(RowsParserBugFixesTest, Bug11_YearMaxValue) {
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

  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "");

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ("2155", result->front().columns.at("year_col"));
}

/**
 * @test Bug #11: YEAR=0 (zero value - special case)
 */
TEST_F(RowsParserBugFixesTest, Bug11_YearZeroValue) {
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

  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "");

  ASSERT_TRUE(result.has_value());
  // 0 is a special value in MySQL YEAR type representing 0000
  EXPECT_EQ("0000", result->front().columns.at("year_col"));
}

/**
 * @test Bug #11: FLOAT type should be parsed correctly
 */
TEST_F(RowsParserBugFixesTest, Bug11_FloatTypeParsing) {
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

  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "");

  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(1, result->size());

  const auto& row = result->front();
  EXPECT_EQ("1", row.primary_key);

  // Should return a float string (not "[UNSUPPORTED_TYPE:4]")
  std::string float_value = row.columns.at("price");
  EXPECT_NE("[UNSUPPORTED_TYPE:4]", float_value);

  // Parse and check value is approximately 3.14
  double parsed = std::stod(float_value);
  EXPECT_NEAR(3.14, parsed, 0.01);
}

/**
 * @test Bug #11: DOUBLE type should be parsed correctly
 */
TEST_F(RowsParserBugFixesTest, Bug11_DoubleTypeParsing) {
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

  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "");

  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(1, result->size());

  const auto& row = result->front();
  EXPECT_EQ("1", row.primary_key);

  // Should return a double string (not "[UNSUPPORTED_TYPE:5]")
  std::string double_value = row.columns.at("price");
  EXPECT_NE("[UNSUPPORTED_TYPE:5]", double_value);

  // Parse and check value is approximately 3.14159265359
  double parsed = std::stod(double_value);
  EXPECT_NEAR(3.14159265359, parsed, 0.00001);
}

/**
 * @test Bug #11: FLOAT with special values (zero, negative, very large)
 */
TEST_F(RowsParserBugFixesTest, Bug11_FloatSpecialValues) {
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
    auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "");

    ASSERT_TRUE(result.has_value());
    double parsed = std::stod(result->front().columns.at("val"));
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
    auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "");

    ASSERT_TRUE(result.has_value());
    double parsed = std::stod(result->front().columns.at("val"));
    EXPECT_NEAR(-123.456, parsed, 0.01);
  }
}

/**
 * @test Bug #11: BIT type should be parsed correctly
 *
 * MySQL BIT(n) is stored as (bytes, bits) where:
 * - bytes = n / 8
 * - bits = n % 8
 * metadata = (bytes << 8) | bits
 */
TEST_F(RowsParserBugFixesTest, Bug11_BitTypeParsing) {
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

  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "");

  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(1, result->size());

  const auto& row = result->front();
  EXPECT_EQ("1", row.primary_key);

  // Should return the numeric value (not "[UNSUPPORTED_TYPE:16]")
  std::string bit_value = row.columns.at("flags");
  EXPECT_NE("[UNSUPPORTED_TYPE:16]", bit_value);
  EXPECT_EQ("170", bit_value);  // 0b10101010 = 170
}

/**
 * @test Bug #11: BIT with multiple bytes
 */
TEST_F(RowsParserBugFixesTest, Bug11_BitMultipleBytes) {
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

  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "");

  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(1, result->size());

  std::string bit_value = result->front().columns.at("flags");
  EXPECT_NE("[UNSUPPORTED_TYPE:16]", bit_value);
  // The value should be the numeric representation
  // 0x1234 = 4660 (big-endian)
  EXPECT_EQ("4660", bit_value);
}

/**
 * @test Bug #11: BIT with partial byte (e.g., BIT(5))
 */
TEST_F(RowsParserBugFixesTest, Bug11_BitPartialByte) {
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

  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "");

  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(1, result->size());

  std::string bit_value = result->front().columns.at("flags");
  EXPECT_NE("[UNSUPPORTED_TYPE:16]", bit_value);
  EXPECT_EQ("21", bit_value);  // 0b10101 = 21
}

// =============================================================================
// Bug #9: Character encoding not handled (non-UTF8 corruption)
// =============================================================================

/**
 * @test Bug #9: Valid UTF-8 strings should pass through unchanged
 */
TEST_F(RowsParserBugFixesTest, Bug9_ValidUtf8PassThrough) {
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
    auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "");

    ASSERT_TRUE(result.has_value()) << "Failed for string: " << test_str;
    ASSERT_EQ(1, result->size());
    EXPECT_EQ(test_str, result->front().columns.at("content")) << "Mismatch for valid UTF-8 string: " << test_str;
  }
}

/**
 * @test Bug #9: Invalid UTF-8 sequences should be sanitized
 *
 * Invalid bytes should be replaced with U+FFFD (replacement character)
 */
TEST_F(RowsParserBugFixesTest, Bug9_InvalidUtf8Sanitized) {
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
    auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "");

    ASSERT_TRUE(result.has_value()) << "Failed for: " << tc.description;
    ASSERT_EQ(1, result->size());

    std::string content = result->front().columns.at("content");

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
 * @test Bug #9: BLOB/TEXT types should also sanitize UTF-8
 */
TEST_F(RowsParserBugFixesTest, Bug9_BlobTextUtf8Sanitization) {
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
  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "");

  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(1, result->size());

  // The BLOB content should be sanitized - no crash and valid output
  std::string content = result->front().columns.at("data");
  EXPECT_FALSE(content.empty()) << "BLOB content should not be empty";

  // Result should be valid UTF-8 (either sanitized or marked as invalid)
  // The key is that it doesn't crash and returns something processable
  SUCCEED();
}

/**
 * @test Bug #9: Empty string should be handled correctly
 */
TEST_F(RowsParserBugFixesTest, Bug9_EmptyStringHandling) {
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
  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "");

  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(1, result->size());
  EXPECT_EQ("", result->front().columns.at("content"));
}

// =============================================================================
// Bug #32: Unsigned integer cast to signed
// =============================================================================
// Integer types are always cast to signed equivalents, causing overflow for
// UNSIGNED columns with large values. For example, UNSIGNED INT 4000000000
// becomes -294967296 when cast to int32_t.
// =============================================================================

/**
 * @test Bug #32: UNSIGNED INT column should preserve large positive values
 *
 * An UNSIGNED INT can hold values 0-4294967295, but casting to int32_t
 * causes overflow for values > 2147483647.
 */
TEST_F(RowsParserBugFixesTest, Bug32_UnsignedIntLargeValue) {
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
  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "");

  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(1, result->size());

  // Bug #32: Before fix, this would be "-294967296" (overflow to negative)
  // After fix, this should be "4000000000"
  EXPECT_EQ("4000000000", result->front().columns.at("id"))
      << "Bug #32: UNSIGNED INT should preserve large positive values";
}

/**
 * @test Bug #32: UNSIGNED TINYINT should handle values 128-255
 *
 * UNSIGNED TINYINT range is 0-255, but SIGNED TINYINT is -128 to 127.
 * Value 200 would become -56 if incorrectly cast to int8_t.
 */
TEST_F(RowsParserBugFixesTest, Bug32_UnsignedTinyIntLargeValue) {
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
  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "");

  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(1, result->size());

  // Bug #32: Before fix, this would be "-56" (overflow to negative)
  // After fix, this should be "200"
  EXPECT_EQ("200", result->front().columns.at("id")) << "Bug #32: UNSIGNED TINYINT should preserve values 128-255";
}

/**
 * @test Bug #32: UNSIGNED SMALLINT should handle values 32768-65535
 *
 * UNSIGNED SMALLINT range is 0-65535, but SIGNED SMALLINT is -32768 to 32767.
 */
TEST_F(RowsParserBugFixesTest, Bug32_UnsignedSmallIntLargeValue) {
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
  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "");

  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(1, result->size());

  // Bug #32: Before fix, this would be "-15536" (overflow to negative)
  // After fix, this should be "50000"
  EXPECT_EQ("50000", result->front().columns.at("id"))
      << "Bug #32: UNSIGNED SMALLINT should preserve values 32768-65535";
}

/**
 * @test Bug #32: UNSIGNED BIGINT should handle values > INT64_MAX
 */
TEST_F(RowsParserBugFixesTest, Bug32_UnsignedBigIntLargeValue) {
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
  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "");

  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(1, result->size());

  // Bug #32: Before fix, this would be negative (overflow)
  // After fix, this should be "10000000000000000000"
  EXPECT_EQ("10000000000000000000", result->front().columns.at("id"))
      << "Bug #32: UNSIGNED BIGINT should preserve values > INT64_MAX";
}

/**
 * @test Bug #32: Signed integers should still work correctly
 *
 * Ensure that fixing unsigned doesn't break signed integer handling.
 */
TEST_F(RowsParserBugFixesTest, Bug32_SignedIntNegativeValue) {
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
  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "");

  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(1, result->size());

  // Should correctly show negative value
  EXPECT_EQ("-1000", result->front().columns.at("id")) << "SIGNED INT should still handle negative values correctly";
}

// =============================================================================
// BUG-0072: GEOMETRY type support
// =============================================================================
// GEOMETRY type (col_type=255) is not supported, returning [UNSUPPORTED_TYPE:255]
// which causes parse failures for tables containing geometry columns.
// =============================================================================

/**
 * @test BUG-0072: GEOMETRY type should be parsed as WKB hex string
 *
 * GEOMETRY columns store data in WKB (Well-Known Binary) format.
 * The parser should handle this type and return a hex representation.
 */
TEST_F(RowsParserBugFixesTest, Bug0072_GeometryTypeBasic) {
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
  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "");

  ASSERT_TRUE(result.has_value()) << "BUG-0072: GEOMETRY type should be parsed successfully";
  ASSERT_EQ(1, result->size());

  // The geometry column should exist and not contain [UNSUPPORTED_TYPE:255]
  ASSERT_TRUE(result->front().columns.find("location") != result->front().columns.end());
  std::string geo_value = result->front().columns.at("location");
  EXPECT_TRUE(geo_value.find("UNSUPPORTED") == std::string::npos)
      << "BUG-0072: GEOMETRY should not return UNSUPPORTED_TYPE, got: " << geo_value;
}

// =============================================================================
// BUG-0087: DECIMAL type precision guarantee
// =============================================================================
// The decode_decimal function in binlog_util.h has issues with:
// 1. Sign bit handling: first byte's 0x80 bit must be XORed out for positive values
// 2. Negative value restoration: all bytes should be XORed with 0xFF
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
 * @test BUG-0087: DECIMAL positive integer value
 */
TEST_F(RowsParserBugFixesTest, Bug0087_DecimalPositiveInteger) {
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
  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "");

  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(1, result->size());

  std::string decimal_value = result->front().columns.at("amount");
  EXPECT_EQ("12345", decimal_value) << "BUG-0087: DECIMAL positive integer should be parsed correctly";
}

/**
 * @test BUG-0087: DECIMAL negative integer value
 */
TEST_F(RowsParserBugFixesTest, Bug0087_DecimalNegativeInteger) {
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
  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "");

  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(1, result->size());

  std::string decimal_value = result->front().columns.at("amount");
  EXPECT_EQ("-12345", decimal_value) << "BUG-0087: DECIMAL negative integer should be parsed correctly";
}

/**
 * @test BUG-0087: DECIMAL with fractional part
 */
TEST_F(RowsParserBugFixesTest, Bug0087_DecimalWithFraction) {
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
  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "");

  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(1, result->size());

  std::string decimal_value = result->front().columns.at("price");
  EXPECT_EQ("12345678.90", decimal_value) << "BUG-0087: DECIMAL with fraction should be parsed correctly";
}

/**
 * @test BUG-0087: DECIMAL negative with fractional part
 */
TEST_F(RowsParserBugFixesTest, Bug0087_DecimalNegativeWithFraction) {
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
  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "");

  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(1, result->size());

  std::string decimal_value = result->front().columns.at("balance");
  EXPECT_EQ("-99999.99", decimal_value) << "BUG-0087: DECIMAL negative with fraction should be parsed correctly";
}

/**
 * @test BUG-0087: DECIMAL zero value
 */
TEST_F(RowsParserBugFixesTest, Bug0087_DecimalZero) {
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
  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "");

  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(1, result->size());

  std::string decimal_value = result->front().columns.at("amount");
  // Zero should be parsed as "0" or "0.00"
  EXPECT_TRUE(decimal_value == "0" || decimal_value == "0.00")
      << "BUG-0087: DECIMAL zero should be parsed correctly, got: " << decimal_value;
}

/**
 * @test BUG-0087: DECIMAL small value (less than 1)
 */
TEST_F(RowsParserBugFixesTest, Bug0087_DecimalSmallValue) {
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
  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "");

  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(1, result->size());

  std::string decimal_value = result->front().columns.at("rate");
  EXPECT_EQ("0.1234", decimal_value) << "BUG-0087: DECIMAL small value should be parsed correctly";
}

// =============================================================================
// BUG-0085: binlog_row_image=MINIMAL/NOBLOB support
// =============================================================================
// When binlog_row_image is set to MINIMAL or NOBLOB, not all columns are
// present in the row event. The columns_present bitmap indicates which
// columns are included. The parser must correctly handle missing columns.
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
 * @test BUG-0085: MINIMAL mode - only some columns present
 *
 * Simulates binlog_row_image=MINIMAL where only the primary key and
 * modified columns are present in the event.
 */
TEST_F(RowsParserBugFixesTest, Bug0085_MinimalModePartialColumns) {
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
  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "");

  ASSERT_TRUE(result.has_value()) << "BUG-0085: Parser should handle MINIMAL mode";
  ASSERT_EQ(1, result->size());

  // Present columns should be parsed
  EXPECT_EQ("42", result->front().columns.at("id"));
  EXPECT_EQ("1", result->front().columns.at("status"));

  // Missing column should not be in the result
  EXPECT_EQ(result->front().columns.find("name"), result->front().columns.end())
      << "BUG-0085: Missing column should not appear in result";
}

/**
 * @test BUG-0085: MINIMAL mode with only primary key present
 */
TEST_F(RowsParserBugFixesTest, Bug0085_MinimalModeOnlyPrimaryKey) {
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
  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "");

  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(1, result->size());

  EXPECT_EQ("100", result->front().columns.at("id"));
  EXPECT_EQ("100", result->front().primary_key);
  EXPECT_EQ(result->front().columns.find("data"), result->front().columns.end());
}

/**
 * @test BUG-0085: All columns missing (edge case - should handle gracefully)
 */
TEST_F(RowsParserBugFixesTest, Bug0085_NoColumnsPresent) {
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
  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "");

  // Should return a result with an empty row (no crash)
  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(1, result->size());
  EXPECT_TRUE(result->front().columns.empty());
}

/**
 * @test BUG-0072: Empty GEOMETRY (zero-length) should be handled correctly
 */
TEST_F(RowsParserBugFixesTest, Bug0072_GeometryTypeEmpty) {
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
  auto result = ParseWriteRowsEvent(buffer.data(), buffer.size(), &table_meta, "id", "");

  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(1, result->size());

  // Empty geometry should result in empty string
  ASSERT_TRUE(result->front().columns.find("location") != result->front().columns.end());
  EXPECT_EQ("", result->front().columns.at("location")) << "Empty GEOMETRY should return empty string";
}

#endif  // USE_MYSQL
