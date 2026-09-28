/**
 * @file table_map_metadata_test.cpp
 * @brief Wire-level tests for TABLE_MAP per-column metadata
 *
 * The metadata bytes are laid out the way MySQL's Field::do_save_field_metadata
 * writes them: VARCHAR and BIT little-endian, STRING (including ENUM/SET) and
 * NEWDECIMAL as two independent bytes with the first one high, and one byte for
 * the BLOB family, JSON, GEOMETRY, FLOAT/DOUBLE and the fractional temporals.
 * MariaDB's COMPRESSED columns reuse the VARCHAR and BLOB layouts; their row
 * images below are bytes captured from a MariaDB 11.8 binlog.
 */

#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "binlog_event_builder.h"
#include "mysql/binlog_event_parser.h"
#include "mysql/binlog_event_types.h"
#include "mysql/binlog_util.h"
#include "mysql/rows_parser.h"
#include "mysql/table_metadata.h"
#include "utils/error.h"

#ifdef USE_MYSQL

namespace {

using mygramdb::mysql::BinlogEventParser;
using mygramdb::mysql::ColumnType;
using mygramdb::mysql::MySQLBinlogEventType;
using mygramdb::mysql::ParseWriteRowsEvent;
using mygramdb::mysql::RetainedColumns;
using mygramdb::mysql::test::BinlogEventBuilder;

constexpr uint64_t kTableId = 0x51;
constexpr uint8_t kRealTypeEnum = 0xF7;
constexpr uint8_t kRealTypeSet = 0xF8;

/// One column as it appears on the wire: type code plus its raw metadata bytes.
struct WireColumn {
  uint8_t type;
  std::vector<uint8_t> metadata;
};

/**
 * @brief Build a TABLE_MAP event from wire columns.
 *
 * @param columns          Column type codes and metadata bytes, in order
 * @param metadata_len_adj Added to the metadata block length the event declares
 * @return Complete event bytes, header through checksum
 */
std::vector<uint8_t> BuildTableMap(const std::vector<WireColumn>& columns, int metadata_len_adj = 0) {
  auto buf = BinlogEventBuilder::BuildHeader(MySQLBinlogEventType::TABLE_MAP_EVENT);

  BinlogEventBuilder::AppendTableId(buf, kTableId);
  BinlogEventBuilder::AppendLittleEndian16(buf, 0);  // flags

  const std::string database_name = "testdb";
  buf.push_back(static_cast<uint8_t>(database_name.size()));
  buf.insert(buf.end(), database_name.begin(), database_name.end());
  buf.push_back(0);

  const std::string table_name = "items";
  buf.push_back(static_cast<uint8_t>(table_name.size()));
  buf.insert(buf.end(), table_name.begin(), table_name.end());
  buf.push_back(0);

  BinlogEventBuilder::AppendPackedInt(buf, columns.size());
  std::vector<uint8_t> metadata_block;
  for (const auto& column : columns) {
    buf.push_back(column.type);
    metadata_block.insert(metadata_block.end(), column.metadata.begin(), column.metadata.end());
  }

  BinlogEventBuilder::AppendPackedInt(
      buf, static_cast<uint64_t>(static_cast<int>(metadata_block.size()) + metadata_len_adj));
  buf.insert(buf.end(), metadata_block.begin(), metadata_block.end());
  if (metadata_len_adj > 0) {
    buf.insert(buf.end(), static_cast<size_t>(metadata_len_adj), 0x00);
  }

  buf.insert(buf.end(), (columns.size() + 7) / 8, 0x00);  // NULL bitmap: nothing nullable

  BinlogEventBuilder::AppendLittleEndian32(buf, 0);  // checksum placeholder
  BinlogEventBuilder::FixEventSizeWithChecksum(buf);
  return buf;
}

uint8_t Code(ColumnType type) {
  return static_cast<uint8_t>(type);
}

/// CHAR(n) metadata bytes as Field_string writes them for a byte length of @p field_length.
std::vector<uint8_t> CharMetadata(uint16_t field_length) {
  return {static_cast<uint8_t>(Code(ColumnType::STRING) ^ ((field_length & 0x300) >> 4)),
          static_cast<uint8_t>(field_length & 0xFF)};
}

TEST(TableMapMetadataTest, EveryMetadataCarryingTypeReadsItsOwnBytes) {
  const std::vector<std::pair<WireColumn, uint16_t>> cases = {
      {{Code(ColumnType::LONG), {}}, 0},
      {{Code(ColumnType::VARCHAR), {0xFC, 0x03}}, 1020},            // VARCHAR(255) utf8mb4
      {{Code(ColumnType::JSON), {0x04}}, 4},                        // JSON in the middle
      {{Code(ColumnType::VARCHAR), {0x28, 0x00}}, 40},              // must not shift after JSON
      {{Code(ColumnType::GEOMETRY), {0x04}}, 4},                    // GEOMETRY in the middle
      {{Code(ColumnType::DATETIME2), {0x03}}, 3},                   // must not shift after GEOMETRY
      {{Code(ColumnType::NEWDECIMAL), {10, 2}}, (10 << 8) | 2},     // DECIMAL(10,2)
      {{Code(ColumnType::STRING), CharMetadata(40)}, 0xFE28},       // CHAR(10) utf8mb4
      {{Code(ColumnType::STRING), CharMetadata(1020)}, 0xCEFC},     // CHAR(255) utf8mb4
      {{Code(ColumnType::STRING), {kRealTypeEnum, 0x01}}, 0xF701},  // ENUM, 1-byte pack
      {{Code(ColumnType::STRING), {kRealTypeEnum, 0x02}}, 0xF702},  // ENUM, 2-byte pack
      {{Code(ColumnType::STRING), {kRealTypeSet, 0x01}}, 0xF801},   // SET, 1-byte pack
      {{Code(ColumnType::STRING), {kRealTypeSet, 0x08}}, 0xF808},   // SET, 8-byte pack
      {{Code(ColumnType::BIT), {0x02, 0x01}}, 0x0102},              // BIT(10): 2 bits, 1 byte
      {{Code(ColumnType::TINY_BLOB), {0x01}}, 1},
      {{Code(ColumnType::BLOB), {0x02}}, 2},
      {{Code(ColumnType::MEDIUM_BLOB), {0x03}}, 3},
      {{Code(ColumnType::LONG_BLOB), {0x04}}, 4},
      {{Code(ColumnType::VECTOR), {0x04}}, 4},
      {{Code(ColumnType::VARCHAR_COMPRESSED), {0xB1, 0x04}}, 1201},  // VARCHAR(300) utf8mb4 COMPRESSED
      {{Code(ColumnType::BLOB_COMPRESSED), {0x02}}, 2},              // TEXT COMPRESSED
      {{Code(ColumnType::FLOAT), {0x04}}, 4},
      {{Code(ColumnType::DOUBLE), {0x08}}, 8},
      {{Code(ColumnType::TIMESTAMP2), {0x06}}, 6},
      {{Code(ColumnType::TIME2), {0x01}}, 1},
      {{Code(ColumnType::DATE), {}}, 0},
      {{Code(ColumnType::YEAR), {}}, 0},
      {{Code(ColumnType::NEWDECIMAL), {65, 30}}, (65 << 8) | 30},  // last column: block must add up
  };

  std::vector<WireColumn> columns;
  columns.reserve(cases.size());
  for (const auto& entry : cases) {
    columns.push_back(entry.first);
  }
  auto event = BuildTableMap(columns);

  auto metadata = BinlogEventParser::ParseTableMapEvent(event.data(), event.size());

  ASSERT_TRUE(metadata.has_value());
  ASSERT_EQ(cases.size(), metadata->columns.size());
  for (size_t i = 0; i < cases.size(); ++i) {
    EXPECT_EQ(cases[i].first.type, Code(metadata->columns[i].type)) << "column " << i;
    EXPECT_EQ(cases[i].second, metadata->columns[i].metadata) << "column " << i;
  }
  EXPECT_EQ(10, metadata->columns[6].metadata >> 8) << "DECIMAL precision";
  EXPECT_EQ(2, metadata->columns[6].metadata & 0xFF) << "DECIMAL scale";
}

TEST(TableMapMetadataTest, UnknownColumnTypeFailsTheEvent) {
  // 244 is not a code this parser can size, so every later column would be misread.
  auto event = BuildTableMap({{Code(ColumnType::LONG), {}}, {244, {0x01}}, {Code(ColumnType::VARCHAR), {0xFF, 0x00}}});

  EXPECT_FALSE(BinlogEventParser::ParseTableMapEvent(event.data(), event.size()).has_value());
}

TEST(TableMapMetadataTest, MetadataBlockLongerThanTheColumnsNeedFailsTheEvent) {
  auto event = BuildTableMap({{Code(ColumnType::LONG), {}}, {Code(ColumnType::JSON), {0x04}}}, /*metadata_len_adj=*/1);

  EXPECT_FALSE(BinlogEventParser::ParseTableMapEvent(event.data(), event.size()).has_value());
}

TEST(TableMapMetadataTest, MetadataBlockShorterThanTheColumnsNeedFailsTheEvent) {
  // The declared block ends inside the DECIMAL entry; the stray byte becomes the NULL bitmap.
  auto event = BuildTableMap({{Code(ColumnType::LONG), {}}, {Code(ColumnType::NEWDECIMAL), {10, 2}}},
                             /*metadata_len_adj=*/-1);

  EXPECT_FALSE(BinlogEventParser::ParseTableMapEvent(event.data(), event.size()).has_value());
}

TEST(TableMapMetadataTest, RowAfterJsonGeometryDecimalCharEnumSetDecodesAtTheRightOffsets) {
  // id INT, attrs JSON, title VARCHAR(255) utf8mb4, price DECIMAL(10,2), code CHAR(10) utf8mb4,
  // label CHAR(255) utf8mb4, status ENUM, tags SET, shape GEOMETRY, body TEXT
  auto table_map = BuildTableMap({
      {Code(ColumnType::LONG), {}},
      {Code(ColumnType::JSON), {0x04}},
      {Code(ColumnType::VARCHAR), {0xFC, 0x03}},
      {Code(ColumnType::NEWDECIMAL), {10, 2}},
      {Code(ColumnType::STRING), CharMetadata(40)},
      {Code(ColumnType::STRING), CharMetadata(1020)},
      {Code(ColumnType::STRING), {kRealTypeEnum, 0x01}},
      {Code(ColumnType::STRING), {kRealTypeSet, 0x01}},
      {Code(ColumnType::GEOMETRY), {0x04}},
      {Code(ColumnType::BLOB), {0x02}},
  });
  auto metadata = BinlogEventParser::ParseTableMapEvent(table_map.data(), table_map.size());
  ASSERT_TRUE(metadata.has_value());
  metadata->columns[6].enum_set_values = {"draft", "published"};
  metadata->columns[7].enum_set_values = {"a", "b", "c"};

  std::vector<uint8_t> row;
  row.insert(row.end(), {0x00, 0x00});               // NULL bitmap
  BinlogEventBuilder::AppendLittleEndian32(row, 7);  // id
  BinlogEventBuilder::AppendLittleEndian32(row, 2);  // attrs: JSON literal true
  row.insert(row.end(), {0x04, 0x01});
  BinlogEventBuilder::AppendLittleEndian16(row, 5);  // title: 2-byte length prefix
  row.insert(row.end(), {'h', 'e', 'l', 'l', 'o'});
  row.insert(row.end(), {0x80, 0x00, 0x04, 0xD2, 0x38});  // price: 1234.56
  row.insert(row.end(), {0x03, 'a', 'b', 'c'});           // code: 1-byte length prefix
  BinlogEventBuilder::AppendLittleEndian16(row, 4);       // label: 2-byte length prefix
  row.insert(row.end(), {'w', 'i', 'd', 'e'});
  row.push_back(0x02);                               // status: ordinal 2
  row.push_back(0x05);                               // tags: a,c
  BinlogEventBuilder::AppendLittleEndian32(row, 3);  // shape: opaque WKB
  row.insert(row.end(), {0x01, 0x02, 0x03});
  BinlogEventBuilder::AppendLittleEndian16(row, 4);  // body
  row.insert(row.end(), {'t', 'e', 'x', 't'});

  auto event = BinlogEventBuilder::BuildWriteRowsV2(kTableId, /*flags=*/0, /*var_header_len=*/2, /*extra_data=*/{},
                                                    /*column_count=*/10, /*columns_bitmap=*/{0xFF, 0x03}, row);

  // JSON and GEOMETRY are not retained, so they are only sized from their metadata.
  RetainedColumns retained;
  retained.by_ordinal = {true, false, true, true, true, true, true, true, false, true};

  auto result = ParseWriteRowsEvent(event.data(), event.size(), &*metadata, "col_0", "",
                                    MySQLBinlogEventType::WRITE_ROWS_EVENT, &retained);

  ASSERT_TRUE(result.has_value()) << result.error().message();
  ASSERT_EQ(1U, result->size());
  const auto& decoded = result->front();
  EXPECT_EQ("7", decoded.primary_key);
  EXPECT_EQ("hello", decoded.GetColumnValue("col_2"));
  EXPECT_EQ("1234.56", decoded.GetColumnValue("col_3"));
  EXPECT_EQ("abc", decoded.GetColumnValue("col_4"));
  EXPECT_EQ("wide", decoded.GetColumnValue("col_5"));
  EXPECT_EQ("published", decoded.GetColumnValue("col_6"));
  EXPECT_EQ("a,c", decoded.GetColumnValue("col_7"));
  EXPECT_EQ("text", decoded.GetColumnValue("col_9"));
}

TEST(TableMapMetadataTest, LengthPrefixNearUint32MaxDoesNotWrapPastTheBoundsCheck) {
  for (const auto type : {ColumnType::LONG_BLOB, ColumnType::JSON, ColumnType::GEOMETRY}) {
    SCOPED_TRACE(static_cast<int>(type));
    const std::vector<uint8_t> field = {0xFE, 0xFF, 0xFF, 0xFF, 'x', 'y'};
    EXPECT_EQ(mygramdb::mysql::binlog_util::calc_field_size(Code(type), field.data(), 4), 4ULL + 0xFFFFFFFEULL);

    auto table_map = BuildTableMap({{Code(ColumnType::LONG), {}}, {Code(type), {0x04}}});
    auto metadata = BinlogEventParser::ParseTableMapEvent(table_map.data(), table_map.size());
    ASSERT_TRUE(metadata.has_value());

    // A 32-bit sum of the prefix and this length wraps to 2, which fits the row.
    std::vector<uint8_t> row = {0x00};
    BinlogEventBuilder::AppendLittleEndian32(row, 7);
    row.insert(row.end(), field.begin(), field.end());
    auto event = BinlogEventBuilder::BuildWriteRowsV2(kTableId, /*flags=*/0, /*var_header_len=*/2, /*extra_data=*/{},
                                                      /*column_count=*/2, /*columns_bitmap=*/{0x03}, row);
    RetainedColumns retained;
    retained.by_ordinal = {true, false};

    auto result = ParseWriteRowsEvent(event.data(), event.size(), &*metadata, "col_0", "",
                                      MySQLBinlogEventType::WRITE_ROWS_EVENT, &retained);

    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kMySQLFieldTruncated) << result.error().message();
  }
}

/// id INT, v VARCHAR(300) COMPRESSED, s VARCHAR(20) COMPRESSED, b TEXT COMPRESSED, m MEDIUMTEXT COMPRESSED
std::vector<uint8_t> BuildCompressedTableMap() {
  return BuildTableMap({
      {Code(ColumnType::LONG), {}},
      {Code(ColumnType::VARCHAR_COMPRESSED), {0xB1, 0x04}},
      {Code(ColumnType::VARCHAR_COMPRESSED), {0x51, 0x00}},
      {Code(ColumnType::BLOB_COMPRESSED), {0x02}},
      {Code(ColumnType::BLOB_COMPRESSED), {0x03}},
  });
}

std::string Repeat(const std::string& unit, int count) {
  std::string out;
  for (int i = 0; i < count; ++i) {
    out += unit;
  }
  return out;
}

/// REPEAT('日本語テキスト', 40) deflated by MariaDB, without the zlib wrapper.
const std::vector<uint8_t> kJapaneseDeflate = {0x7b, 0x36, 0x7d, 0xe9, 0xb3, 0x39, 0x6b, 0x5e, 0xac, 0x9a, 0xf7, 0xb8,
                                               0xb9, 0xed, 0x71, 0xd3, 0xda, 0xc7, 0x4d, 0x3b, 0x1f, 0x37, 0x77, 0x3c,
                                               0x1b, 0x15, 0x1c, 0x15, 0x1c, 0x15, 0x24, 0x45, 0x10, 0x00};

std::vector<uint8_t> CapturedCompressedRows() {
  std::vector<uint8_t> rows;
  // Row 1, column_compression_zlib_wrap=OFF: header 0x8N selects raw deflate
  // with an N-byte original length; 's' and 'm' are short enough to be stored
  // behind the 0x00 "not compressed" header.
  rows.insert(rows.end(), {0xe0, 0x01, 0x00, 0x00, 0x00});
  rows.insert(rows.end(), {0x0a, 0x00, 0x89, 0xb4, 0x4b, 0x4c, 0x4a, 0x4e, 0x1c, 0x6a, 0x08, 0x00});
  rows.insert(rows.end(), {0x03, 0x00, 'h', 'i'});
  rows.insert(rows.end(), {0x25, 0x00, 0x8a, 0x03, 0x48});
  rows.insert(rows.end(), kJapaneseDeflate.begin(), kJapaneseDeflate.end());
  rows.insert(rows.end(), {0x02, 0x00, 0x00, 0x00, 'x'});
  // Row 2, column_compression_zlib_wrap=ON: the same values behind a zlib
  // header and Adler-32 trailer; 's' is NULL.
  rows.insert(rows.end(), {0xe4, 0x02, 0x00, 0x00, 0x00});
  rows.insert(rows.end(), {0x10, 0x00, 0x81, 0xb4, 0x78, 0x9c, 0x4b, 0x4c, 0x4a, 0x4e, 0x1c, 0x6a, 0x08, 0x00, 0x5d,
                           0xa8, 0x44, 0xe9});
  rows.insert(rows.end(), {0x2b, 0x00, 0x82, 0x03, 0x48, 0x78, 0x9c});
  rows.insert(rows.end(), kJapaneseDeflate.begin(), kJapaneseDeflate.end());
  rows.insert(rows.end(), {0x2f, 0x0c, 0x45, 0xaf});
  rows.insert(rows.end(), {0x06, 0x00, 0x00, 0x00, 's', 'h', 'o', 'r', 't'});
  // Row 3: empty values are stored with no header byte at all.
  rows.insert(rows.end(), {0xe0, 0x03, 0x00, 0x00, 0x00});
  rows.insert(rows.end(), {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00});  // v, s, b, m length prefixes
  return rows;
}

TEST(TableMapMetadataTest, MariaDbCompressedColumnsInflateToTheStoredText) {
  auto table_map = BuildCompressedTableMap();
  auto metadata = BinlogEventParser::ParseTableMapEvent(table_map.data(), table_map.size());
  ASSERT_TRUE(metadata.has_value());

  auto event =
      BinlogEventBuilder::BuildWriteRowsV2(kTableId, /*flags=*/0, /*var_header_len=*/2, /*extra_data=*/{},
                                           /*column_count=*/5, /*columns_bitmap=*/{0x1F}, CapturedCompressedRows());
  RetainedColumns retained;
  retained.by_ordinal = {true, true, true, true, true};

  auto result = ParseWriteRowsEvent(event.data(), event.size(), &*metadata, "col_0", "",
                                    MySQLBinlogEventType::WRITE_ROWS_EVENT, &retained);

  ASSERT_TRUE(result.has_value()) << result.error().message();
  ASSERT_EQ(3U, result->size());
  const std::string abc = Repeat("abc", 60);
  const std::string japanese = Repeat("日本語テキスト", 40);
  for (size_t i = 0; i < 2; ++i) {
    SCOPED_TRACE(i);
    EXPECT_EQ(abc, (*result)[i].GetColumnValue("col_1"));
    EXPECT_EQ(japanese, (*result)[i].GetColumnValue("col_3"));
  }
  EXPECT_EQ("hi", (*result)[0].GetColumnValue("col_2"));
  EXPECT_EQ("x", (*result)[0].GetColumnValue("col_4"));
  EXPECT_EQ("short", (*result)[1].GetColumnValue("col_4"));
  EXPECT_EQ("3", (*result)[2].primary_key);
  EXPECT_EQ("", (*result)[2].GetColumnValue("col_1"));
  EXPECT_EQ("", (*result)[2].GetColumnValue("col_4"));
}

TEST(TableMapMetadataTest, SkippedMariaDbCompressedColumnsKeepLaterColumnsAligned) {
  auto table_map = BuildCompressedTableMap();
  auto metadata = BinlogEventParser::ParseTableMapEvent(table_map.data(), table_map.size());
  ASSERT_TRUE(metadata.has_value());

  auto event =
      BinlogEventBuilder::BuildWriteRowsV2(kTableId, /*flags=*/0, /*var_header_len=*/2, /*extra_data=*/{},
                                           /*column_count=*/5, /*columns_bitmap=*/{0x1F}, CapturedCompressedRows());
  RetainedColumns retained;
  retained.by_ordinal = {true, false, false, false, true};

  auto result = ParseWriteRowsEvent(event.data(), event.size(), &*metadata, "col_0", "",
                                    MySQLBinlogEventType::WRITE_ROWS_EVENT, &retained);

  ASSERT_TRUE(result.has_value()) << result.error().message();
  ASSERT_EQ(3U, result->size());
  EXPECT_EQ("x", (*result)[0].GetColumnValue("col_4"));
  EXPECT_EQ("short", (*result)[1].GetColumnValue("col_4"));
  EXPECT_EQ("3", (*result)[2].primary_key);
}

TEST(TableMapMetadataTest, UndecodableMariaDbCompressedPayloadFailsTheEvent) {
  const std::vector<std::vector<uint8_t>> payloads = {
      {0x89, 0xb4, 0xde, 0xad, 0xbe, 0xef},                          // not a deflate stream
      {0x89, 0xb5, 0x4b, 0x4c, 0x4a, 0x4e, 0x1c, 0x6a, 0x08, 0x00},  // claims one byte more than it inflates to
      {0x81, 0xb4, 0x4b, 0x4c, 0x4a, 0x4e, 0x1c, 0x6a, 0x08, 0x00},  // zlib wrapper announced, raw stream sent
      {0x90, 0x01, 'x'},                                             // compression method this build does not know
      {0x80, 'x'},                                                   // zlib with no original-length bytes
  };
  auto table_map = BuildTableMap({{Code(ColumnType::LONG), {}}, {Code(ColumnType::VARCHAR_COMPRESSED), {0xB1, 0x04}}});
  auto metadata = BinlogEventParser::ParseTableMapEvent(table_map.data(), table_map.size());
  ASSERT_TRUE(metadata.has_value());

  for (size_t i = 0; i < payloads.size(); ++i) {
    SCOPED_TRACE(i);
    std::vector<uint8_t> row = {0x00};
    BinlogEventBuilder::AppendLittleEndian32(row, 1);
    BinlogEventBuilder::AppendLittleEndian16(row, static_cast<uint16_t>(payloads[i].size()));
    row.insert(row.end(), payloads[i].begin(), payloads[i].end());
    auto event = BinlogEventBuilder::BuildWriteRowsV2(kTableId, /*flags=*/0, /*var_header_len=*/2, /*extra_data=*/{},
                                                      /*column_count=*/2, /*columns_bitmap=*/{0x03}, row);
    RetainedColumns retained;
    retained.by_ordinal = {true, true};

    auto result = ParseWriteRowsEvent(event.data(), event.size(), &*metadata, "col_0", "",
                                      MySQLBinlogEventType::WRITE_ROWS_EVENT, &retained);

    EXPECT_FALSE(result.has_value());
  }
}

}  // namespace

#endif  // USE_MYSQL
