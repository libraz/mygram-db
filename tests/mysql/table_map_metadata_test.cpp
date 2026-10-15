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
 * @param optional_metadata Raw TLV bytes placed after the NULL bitmap
 * @return Complete event bytes, header through checksum
 */
std::vector<uint8_t> BuildTableMap(const std::vector<WireColumn>& columns, int metadata_len_adj = 0,
                                   const std::vector<uint8_t>& optional_metadata = {}) {
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
  buf.insert(buf.end(), optional_metadata.begin(), optional_metadata.end());

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

/// One optional-metadata TLV: type byte, packed length, value.
std::vector<uint8_t> Tlv(uint8_t type, const std::vector<uint8_t>& value) {
  std::vector<uint8_t> out = {type};
  BinlogEventBuilder::AppendPackedInt(out, value.size());
  out.insert(out.end(), value.begin(), value.end());
  return out;
}

/// Length-prefixed strings back to back, as COLUMN_NAME stores them.
std::vector<uint8_t> LengthPrefixed(const std::vector<std::string>& strings) {
  std::vector<uint8_t> out;
  for (const auto& text : strings) {
    BinlogEventBuilder::AppendPackedInt(out, text.size());
    out.insert(out.end(), text.begin(), text.end());
  }
  return out;
}

/// One ENUM_STR_VALUE / SET_STR_VALUE group: label count, then the labels.
std::vector<uint8_t> LabelGroup(const std::vector<std::string>& labels) {
  std::vector<uint8_t> out;
  BinlogEventBuilder::AppendPackedInt(out, labels.size());
  const auto body = LengthPrefixed(labels);
  out.insert(out.end(), body.begin(), body.end());
  return out;
}

std::vector<uint8_t> Concat(const std::vector<std::vector<uint8_t>>& parts) {
  std::vector<uint8_t> out;
  for (const auto& part : parts) {
    out.insert(out.end(), part.begin(), part.end());
  }
  return out;
}

constexpr uint8_t kSignedness = 1;
constexpr uint8_t kDefaultCharset = 2;
constexpr uint8_t kColumnName = 4;
constexpr uint8_t kSetStrValue = 5;
constexpr uint8_t kEnumStrValue = 6;
constexpr uint8_t kSimplePrimaryKey = 8;

/// id INT, title VARCHAR, flag TINY, status ENUM, tags SET, price DECIMAL(10,2), ratio DOUBLE
std::vector<WireColumn> OptionalMetadataColumns() {
  return {{Code(ColumnType::LONG), {}},
          {Code(ColumnType::VARCHAR), {0x28, 0x00}},
          {Code(ColumnType::TINY), {}},
          {Code(ColumnType::STRING), {kRealTypeEnum, 0x01}},
          {Code(ColumnType::STRING), {kRealTypeSet, 0x01}},
          {Code(ColumnType::NEWDECIMAL), {10, 2}},
          {Code(ColumnType::DOUBLE), {0x08}}};
}

std::vector<uint8_t> FullOptionalMetadata() {
  return Concat({
      Tlv(kSignedness, {0xA0}),  // id unsigned, flag signed, price unsigned, ratio signed
      Tlv(kDefaultCharset, {45, 1, 8}),
      Tlv(kColumnName, LengthPrefixed({"id", "title", "flag", "status", "tags", "price", "ratio"})),
      Tlv(kSetStrValue, LabelGroup({"a", "b", "c"})),
      Tlv(kEnumStrValue, LabelGroup({"draft", "published"})),
      Tlv(kSimplePrimaryKey, {0}),
  });
}

TEST(TableMapMetadataTest, FullOptionalMetadataSuppliesNamesSignednessAndLabels) {
  auto event = BuildTableMap(OptionalMetadataColumns(), 0, FullOptionalMetadata());

  auto metadata = BinlogEventParser::ParseTableMapEvent(event.data(), event.size());

  ASSERT_TRUE(metadata.has_value());
  EXPECT_TRUE(metadata->has_column_names);
  EXPECT_TRUE(metadata->signedness_known);
  EXPECT_TRUE(metadata->enum_set_labels_known);
  EXPECT_TRUE(metadata->HasFullRowMetadata());

  const std::vector<std::string> names = {"id", "title", "flag", "status", "tags", "price", "ratio"};
  ASSERT_EQ(names.size(), metadata->columns.size());
  for (size_t i = 0; i < names.size(); ++i) {
    EXPECT_EQ(names[i], metadata->columns[i].name) << i;
  }
  EXPECT_EQ(3U, *metadata->FindColumnOrdinal("status"));

  // The signedness bitmap covers numeric columns only, in column order.
  EXPECT_TRUE(metadata->columns[0].is_unsigned);
  EXPECT_FALSE(metadata->columns[1].is_unsigned);
  EXPECT_FALSE(metadata->columns[2].is_unsigned);
  EXPECT_TRUE(metadata->columns[5].is_unsigned);
  EXPECT_FALSE(metadata->columns[6].is_unsigned);

  EXPECT_EQ((std::vector<std::string>{"draft", "published"}), metadata->columns[3].enum_set_values);
  EXPECT_EQ((std::vector<std::string>{"a", "b", "c"}), metadata->columns[4].enum_set_values);
  EXPECT_TRUE(metadata->columns[0].enum_set_values.empty());
}

TEST(TableMapMetadataTest, RowDecodesWithTheLabelsAndSignednessFromTheTableMap) {
  auto event = BuildTableMap(OptionalMetadataColumns(), 0, FullOptionalMetadata());
  auto metadata = BinlogEventParser::ParseTableMapEvent(event.data(), event.size());
  ASSERT_TRUE(metadata.has_value());

  std::vector<uint8_t> row;
  row.insert(row.end(), {0x00});                              // NULL bitmap: 7 columns, none null
  BinlogEventBuilder::AppendLittleEndian32(row, 0xFFFFFFFF);  // id: unsigned
  row.push_back(0x02);                                        // title: 1-byte length prefix
  row.insert(row.end(), {'h', 'i'});
  row.push_back(0xFF);                                    // flag: signed TINY
  row.push_back(0x02);                                    // status: published
  row.push_back(0x05);                                    // tags: a,c
  row.insert(row.end(), {0x80, 0x00, 0x04, 0xD2, 0x38});  // price: 1234.56
  row.insert(row.end(), {0, 0, 0, 0, 0, 0, 0xF0, 0x3F});  // ratio: 1.0
  auto rows = BinlogEventBuilder::BuildWriteRowsV2(kTableId, 0, 2, {}, 7, {0x7F}, row);

  auto result = ParseWriteRowsEvent(rows.data(), rows.size(), &*metadata, "id", "",
                                    MySQLBinlogEventType::WRITE_ROWS_EVENT, nullptr);

  ASSERT_TRUE(result.has_value()) << result.error().message();
  ASSERT_EQ(1U, result->size());
  const auto& decoded = result->front();
  EXPECT_EQ("4294967295", decoded.primary_key);
  EXPECT_EQ("hi", decoded.GetColumnValue("title"));
  EXPECT_EQ("-1", decoded.GetColumnValue("flag"));
  EXPECT_EQ("published", decoded.GetColumnValue("status"));
  EXPECT_EQ("a,c", decoded.GetColumnValue("tags"));
  EXPECT_EQ("1234.56", decoded.GetColumnValue("price"));
}

TEST(TableMapMetadataTest, TableMapWithoutOptionalMetadataKeepsPlaceholderNames) {
  auto event = BuildTableMap(OptionalMetadataColumns());

  auto metadata = BinlogEventParser::ParseTableMapEvent(event.data(), event.size());

  ASSERT_TRUE(metadata.has_value());
  EXPECT_FALSE(metadata->has_column_names);
  EXPECT_FALSE(metadata->signedness_known);
  EXPECT_FALSE(metadata->enum_set_labels_known);
  EXPECT_FALSE(metadata->HasFullRowMetadata());
  EXPECT_EQ("col_0", metadata->columns[0].name);
  EXPECT_EQ("col_6", metadata->columns[6].name);

  // Nothing numeric and nothing ENUM/SET: there is nothing for the block to supply.
  auto text_only = BuildTableMap({{Code(ColumnType::VARCHAR), {0x28, 0x00}}, {Code(ColumnType::BLOB), {0x02}}});
  auto text_metadata = BinlogEventParser::ParseTableMapEvent(text_only.data(), text_only.size());
  ASSERT_TRUE(text_metadata.has_value());
  EXPECT_TRUE(text_metadata->signedness_known);
  EXPECT_TRUE(text_metadata->enum_set_labels_known);
  EXPECT_FALSE(text_metadata->has_column_names);
}

TEST(TableMapMetadataTest, MinimalOptionalMetadataLeavesNamesAndLabelsUnknown) {
  // binlog_row_metadata=MINIMAL logs signedness and charsets but not names or labels.
  auto event =
      BuildTableMap(OptionalMetadataColumns(), 0, Concat({Tlv(kSignedness, {0xA0}), Tlv(kDefaultCharset, {45, 1, 8})}));

  auto metadata = BinlogEventParser::ParseTableMapEvent(event.data(), event.size());

  ASSERT_TRUE(metadata.has_value());
  EXPECT_TRUE(metadata->signedness_known);
  EXPECT_TRUE(metadata->columns[0].is_unsigned);
  EXPECT_FALSE(metadata->has_column_names);
  EXPECT_FALSE(metadata->enum_set_labels_known);
  EXPECT_FALSE(metadata->HasFullRowMetadata());
}

TEST(TableMapMetadataTest, UnknownOptionalMetadataTypesAreSkippedByLength) {
  auto event = BuildTableMap(OptionalMetadataColumns(), 0,
                             Concat({Tlv(99, {1, 2, 3, 4, 5}), FullOptionalMetadata(), Tlv(200, {})}));

  auto metadata = BinlogEventParser::ParseTableMapEvent(event.data(), event.size());

  ASSERT_TRUE(metadata.has_value());
  EXPECT_TRUE(metadata->HasFullRowMetadata());
  EXPECT_EQ("ratio", metadata->columns[6].name);
}

TEST(TableMapMetadataTest, MalformedOptionalMetadataFailsTheEvent) {
  const auto names = LengthPrefixed({"id", "title", "flag", "status", "tags", "price", "ratio"});
  const std::vector<std::pair<const char*, std::vector<uint8_t>>> cases = {
      {"type byte without a length", {kColumnName}},
      {"length past the end", {kColumnName, 50, 'a'}},
      {"NULL length marker", {kColumnName, 251}},
      {"signedness shorter than the numeric columns need", Tlv(kSignedness, {})},
      {"signedness longer than the numeric columns need", Tlv(kSignedness, {0xA0, 0x00})},
      {"one name too few", Tlv(kColumnName, LengthPrefixed({"id", "title", "flag", "status", "tags", "price"}))},
      {"one name too many", Tlv(kColumnName, Concat({names, LengthPrefixed({"extra"})}))},
      {"name cut short", Tlv(kColumnName, {2, 'i'})},
      {"two SET groups for one SET column", Tlv(kSetStrValue, Concat({LabelGroup({"a"}), LabelGroup({"b"})}))},
      {"no ENUM group for the ENUM column", Tlv(kEnumStrValue, {})},
      {"label count past the end", Tlv(kEnumStrValue, {5, 1, 'x'})},
      {"label cut short", Tlv(kEnumStrValue, {1, 4, 'a', 'b'})},
  };
  for (const auto& [description, optional_metadata] : cases) {
    SCOPED_TRACE(description);
    auto event = BuildTableMap(OptionalMetadataColumns(), 0, optional_metadata);

    auto metadata = BinlogEventParser::ParseTableMapEvent(event.data(), event.size());

    ASSERT_FALSE(metadata.has_value());
    // The frame is intact, so a reconnect would only replay the same bytes.
    EXPECT_EQ(metadata.error().code(), mygram::utils::ErrorCode::kMySQLUndecodableBinlogEvent);

    // A table whose rows are not decoded never has the block read.
    auto skipped = BinlogEventParser::ParseTableMapEvent(event.data(), event.size(),
                                                         [](const mygramdb::mysql::TableMetadata&) { return false; });
    ASSERT_TRUE(skipped.has_value()) << skipped.error().message();
    EXPECT_FALSE(skipped->HasFullRowMetadata());
  }
}

TEST(TableMapMetadataTest, SignednessBitmapCountsOnlyNumericColumnTypes) {
  // YEAR, FLOAT, DOUBLE, DECIMAL and the integer types carry a bit; BIT, TIME2, JSON and text do not.
  auto event = BuildTableMap({{Code(ColumnType::BIT), {0x00, 0x01}},
                              {Code(ColumnType::YEAR), {}},
                              {Code(ColumnType::TIME2), {0x00}},
                              {Code(ColumnType::FLOAT), {0x04}},
                              {Code(ColumnType::JSON), {0x04}},
                              {Code(ColumnType::INT24), {}},
                              {Code(ColumnType::LONGLONG), {}}},
                             0, Tlv(kSignedness, {0x50}));  // YEAR 0, FLOAT 1, INT24 0, LONGLONG 1

  auto metadata = BinlogEventParser::ParseTableMapEvent(event.data(), event.size());

  ASSERT_TRUE(metadata.has_value());
  EXPECT_FALSE(metadata->columns[0].is_unsigned);
  EXPECT_FALSE(metadata->columns[1].is_unsigned);
  EXPECT_TRUE(metadata->columns[3].is_unsigned);
  EXPECT_FALSE(metadata->columns[5].is_unsigned);
  EXPECT_TRUE(metadata->columns[6].is_unsigned);
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

  auto metadata = BinlogEventParser::ParseTableMapEvent(event.data(), event.size());
  ASSERT_FALSE(metadata.has_value());
  // The frame is intact, so every replay fails the same way: SYNC has to restart past it.
  EXPECT_EQ(metadata.error().code(), mygram::utils::ErrorCode::kMySQLUndecodableBinlogEvent);
}

TEST(TableMapMetadataTest, MetadataBlockLongerThanTheColumnsNeedFailsTheEvent) {
  auto event = BuildTableMap({{Code(ColumnType::LONG), {}}, {Code(ColumnType::JSON), {0x04}}}, /*metadata_len_adj=*/1);

  auto metadata = BinlogEventParser::ParseTableMapEvent(event.data(), event.size());
  ASSERT_FALSE(metadata.has_value());
  EXPECT_EQ(metadata.error().code(), mygram::utils::ErrorCode::kMySQLBinlogError);
}

TEST(TableMapMetadataTest, MetadataBlockShorterThanTheColumnsNeedFailsTheEvent) {
  // The declared block ends inside the DECIMAL entry; the stray byte becomes the NULL bitmap.
  auto event = BuildTableMap({{Code(ColumnType::LONG), {}}, {Code(ColumnType::NEWDECIMAL), {10, 2}}},
                             /*metadata_len_adj=*/-1);

  auto metadata = BinlogEventParser::ParseTableMapEvent(event.data(), event.size());
  ASSERT_FALSE(metadata.has_value());
  EXPECT_EQ(metadata.error().code(), mygram::utils::ErrorCode::kMySQLBinlogError);
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
    EXPECT_EQ(mygramdb::mysql::binlog_util::calc_field_size(Code(type), field.data(), 4, field.data() + field.size()),
              4ULL + 0xFFFFFFFEULL);

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
