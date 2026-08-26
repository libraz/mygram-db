/**
 * @file rows_parser_decimal_metadata_test.cpp
 * @brief Wire-level tests for NEWDECIMAL metadata that MySQL would never emit
 *
 * A TABLE_MAP event carries DECIMAL precision and scale verbatim, so a peer
 * can declare a scale larger than the precision. The row parser has to size
 * such a field without reading outside its digit tables, whether or not the
 * column's value is decoded.
 */

#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <vector>

#include "binlog_event_builder.h"
#include "mysql/binlog_event_parser.h"
#include "mysql/binlog_event_types.h"
#include "mysql/rows_parser.h"
#include "mysql/table_metadata.h"
#include "utils/error.h"

#ifdef USE_MYSQL

namespace {

using mygram::utils::ErrorCode;
using mygramdb::mysql::BinlogEventParser;
using mygramdb::mysql::ColumnType;
using mygramdb::mysql::MySQLBinlogEventType;
using mygramdb::mysql::ParseWriteRowsEvent;
using mygramdb::mysql::RetainedColumns;
using mygramdb::mysql::TableMetadata;
using mygramdb::mysql::test::BinlogEventBuilder;

constexpr uint64_t kTableId = 0x42;

/**
 * @brief Build a TABLE_MAP event for a table of INT plus one DECIMAL column.
 *
 * @param precision DECIMAL precision as declared on the wire
 * @param scale     DECIMAL scale as declared on the wire
 * @return Complete event bytes, header through checksum
 */
std::vector<uint8_t> BuildDecimalTableMap(uint8_t precision, uint8_t scale) {
  auto buf = BinlogEventBuilder::BuildHeader(MySQLBinlogEventType::TABLE_MAP_EVENT);

  BinlogEventBuilder::AppendTableId(buf, kTableId);
  BinlogEventBuilder::AppendLittleEndian16(buf, 0);  // flags

  const std::string database_name = "testdb";
  buf.push_back(static_cast<uint8_t>(database_name.size()));
  buf.insert(buf.end(), database_name.begin(), database_name.end());
  buf.push_back(0);

  const std::string table_name = "prices";
  buf.push_back(static_cast<uint8_t>(table_name.size()));
  buf.insert(buf.end(), table_name.begin(), table_name.end());
  buf.push_back(0);

  BinlogEventBuilder::AppendPackedInt(buf, 2);  // column count
  buf.push_back(static_cast<uint8_t>(ColumnType::LONG));
  buf.push_back(static_cast<uint8_t>(ColumnType::NEWDECIMAL));

  // Only NEWDECIMAL carries metadata here: (precision << 8) | scale.
  BinlogEventBuilder::AppendPackedInt(buf, 2);
  BinlogEventBuilder::AppendLittleEndian16(buf, static_cast<uint16_t>((precision << 8) | scale));

  buf.push_back(0x00);  // NULL bitmap: neither column is nullable

  BinlogEventBuilder::AppendLittleEndian32(buf, 0);  // checksum placeholder
  BinlogEventBuilder::FixEventSizeWithChecksum(buf);
  return buf;
}

/**
 * @brief Build a single-row WRITE_ROWS event for the table above.
 *
 * @param key          Value of the INT key column
 * @param decimal_body Raw bytes standing in for the DECIMAL field
 */
std::vector<uint8_t> BuildDecimalWriteRows(int32_t key, const std::vector<uint8_t>& decimal_body) {
  std::vector<uint8_t> row_data;
  row_data.push_back(0x00);  // NULL bitmap: both columns carry a value
  BinlogEventBuilder::AppendLittleEndian32(row_data, static_cast<uint32_t>(key));
  row_data.insert(row_data.end(), decimal_body.begin(), decimal_body.end());

  return BinlogEventBuilder::BuildWriteRowsV2(kTableId, /*flags=*/0, /*var_header_len=*/2, /*extra_data=*/{},
                                              /*column_count=*/2, /*columns_bitmap=*/{0x03}, row_data);
}

TEST(RowsParserDecimalMetadataTest, TableMapKeepsDeclaredPrecisionAndScale) {
  auto table_map = BuildDecimalTableMap(/*precision=*/1, /*scale=*/5);

  auto metadata = BinlogEventParser::ParseTableMapEvent(table_map.data(), table_map.size());

  ASSERT_TRUE(metadata.has_value());
  ASSERT_EQ(2U, metadata->columns.size());
  EXPECT_EQ(ColumnType::NEWDECIMAL, metadata->columns[1].type);
  EXPECT_EQ(static_cast<uint16_t>((1 << 8) | 5), metadata->columns[1].metadata);
}

TEST(RowsParserDecimalMetadataTest, ScaleAbovePrecisionIsRejectedWhenTheColumnIsNotDecoded) {
  auto table_map = BuildDecimalTableMap(/*precision=*/1, /*scale=*/5);
  auto metadata = BinlogEventParser::ParseTableMapEvent(table_map.data(), table_map.size());
  ASSERT_TRUE(metadata.has_value());

  auto event = BuildDecimalWriteRows(/*key=*/7, std::vector<uint8_t>(8, 0x80));

  // Retaining only the key column routes the DECIMAL through the field-size
  // path without decoding its value.
  RetainedColumns retained;
  retained.by_ordinal = {true, false};

  auto result = ParseWriteRowsEvent(event.data(), event.size(), &*metadata, "col_0", "",
                                    MySQLBinlogEventType::WRITE_ROWS_EVENT, &retained);

  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(ErrorCode::kMySQLUnsupportedType, result.error().code());
}

TEST(RowsParserDecimalMetadataTest, ScaleAbovePrecisionIsRejectedWhenTheColumnIsDecoded) {
  auto table_map = BuildDecimalTableMap(/*precision=*/1, /*scale=*/5);
  auto metadata = BinlogEventParser::ParseTableMapEvent(table_map.data(), table_map.size());
  ASSERT_TRUE(metadata.has_value());

  auto event = BuildDecimalWriteRows(/*key=*/7, std::vector<uint8_t>(8, 0x80));

  auto result = ParseWriteRowsEvent(event.data(), event.size(), &*metadata, "col_0", "",
                                    MySQLBinlogEventType::WRITE_ROWS_EVENT, nullptr);

  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(ErrorCode::kMySQLInvalidMetadata, result.error().code());
}

TEST(RowsParserDecimalMetadataTest, PrecisionAboveTheProtocolMaximumIsRejected) {
  auto table_map = BuildDecimalTableMap(/*precision=*/200, /*scale=*/2);
  auto metadata = BinlogEventParser::ParseTableMapEvent(table_map.data(), table_map.size());
  ASSERT_TRUE(metadata.has_value());

  // 89 bytes is what the unchecked size rule asks for at this precision, so the
  // row is long enough to be consumed whole if the metadata is not validated.
  auto event = BuildDecimalWriteRows(/*key=*/7, std::vector<uint8_t>(89, 0x80));

  RetainedColumns retained;
  retained.by_ordinal = {true, false};

  auto result = ParseWriteRowsEvent(event.data(), event.size(), &*metadata, "col_0", "",
                                    MySQLBinlogEventType::WRITE_ROWS_EVENT, &retained);

  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(ErrorCode::kMySQLUnsupportedType, result.error().code());
}

TEST(RowsParserDecimalMetadataTest, WellFormedDecimalStillParses) {
  // DECIMAL(10,2): four bytes for the eight integer digits plus one byte for
  // the two fractional digits.
  auto table_map = BuildDecimalTableMap(/*precision=*/10, /*scale=*/2);
  auto metadata = BinlogEventParser::ParseTableMapEvent(table_map.data(), table_map.size());
  ASSERT_TRUE(metadata.has_value());

  // 1234.56 encoded with the sign bit set on the leading byte.
  const std::vector<uint8_t> decimal_body = {0x80, 0x00, 0x04, 0xD2, 0x38};
  auto event = BuildDecimalWriteRows(/*key=*/7, decimal_body);

  auto result = ParseWriteRowsEvent(event.data(), event.size(), &*metadata, "col_0", "",
                                    MySQLBinlogEventType::WRITE_ROWS_EVENT, nullptr);

  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(1U, result->size());
  EXPECT_EQ("7", result->front().primary_key);
  EXPECT_EQ("1234.56", result->front().GetColumnValue("col_1"));
}

}  // namespace

#endif  // USE_MYSQL
