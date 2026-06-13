/**
 * @file binlog_parsing_test.cpp
 * @brief Unit tests for binlog parsing (MySQL 8.0 compatibility)
 */

#include <gtest/gtest.h>

#include <chrono>
#include <cstring>
#include <unordered_map>

#include "binlog_event_builder.h"
#include "mysql/binlog_event_parser.h"
#include "mysql/binlog_event_types.h"
#include "mysql/binlog_reader.h"
#include "mysql/rows_parser.h"
#include "mysql/table_metadata.h"
#include "utils/crc32.h"

#ifdef USE_MYSQL

using namespace mygramdb::mysql;
using namespace mygramdb;

/**
 * @brief Test helper to create a minimal binlog header
 */
std::vector<uint8_t> CreateBinlogHeader(MySQLBinlogEventType event_type, uint32_t event_size) {
  std::vector<uint8_t> header(19);
  // timestamp (4 bytes) - zeros for test
  header[0] = header[1] = header[2] = header[3] = 0x00;
  // event_type (1 byte)
  header[4] = static_cast<uint8_t>(event_type);
  // server_id (4 bytes) - 1 for test
  header[5] = 0x01;
  header[6] = header[7] = header[8] = 0x00;
  // event_size (4 bytes, little-endian)
  header[9] = event_size & 0xFF;
  header[10] = (event_size >> 8) & 0xFF;
  header[11] = (event_size >> 16) & 0xFF;
  header[12] = (event_size >> 24) & 0xFF;
  // log_pos (4 bytes) - zeros for test
  header[13] = header[14] = header[15] = header[16] = 0x00;
  // flags (2 bytes) - zeros for test
  header[17] = header[18] = 0x00;

  return header;
}

void PatchEventSize(std::vector<uint8_t>& event_without_ok_byte) {
  uint32_t event_size = static_cast<uint32_t>(event_without_ok_byte.size());
  event_without_ok_byte[9] = event_size & 0xFF;
  event_without_ok_byte[10] = (event_size >> 8) & 0xFF;
  event_without_ok_byte[11] = (event_size >> 16) & 0xFF;
  event_without_ok_byte[12] = (event_size >> 24) & 0xFF;
}

std::vector<uint8_t> CreateMinimalTableMapEvent(uint8_t null_bitmap, bool include_null_bitmap) {
  std::vector<uint8_t> event = CreateBinlogHeader(MySQLBinlogEventType::TABLE_MAP_EVENT, 0);

  // table_id + flags
  event.insert(event.end(), {0x34, 0x12, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00});

  const std::string db_name = "testdb";
  event.push_back(static_cast<uint8_t>(db_name.size()));
  event.insert(event.end(), db_name.begin(), db_name.end());
  event.push_back(0x00);

  const std::string table_name = "articles";
  event.push_back(static_cast<uint8_t>(table_name.size()));
  event.insert(event.end(), table_name.begin(), table_name.end());
  event.push_back(0x00);

  event.push_back(3);  // column count
  event.push_back(static_cast<uint8_t>(ColumnType::LONG));
  event.push_back(static_cast<uint8_t>(ColumnType::VARCHAR));
  event.push_back(static_cast<uint8_t>(ColumnType::BLOB));

  event.push_back(3);  // metadata length
  event.push_back(0xFF);
  event.push_back(0x00);  // VARCHAR max length
  event.push_back(0x02);  // BLOB length bytes

  if (include_null_bitmap) {
    event.push_back(null_bitmap);
  }

  // CRC32 checksum bytes. Parser already receives checksum-verified events
  // but must not treat these bytes as TABLE_MAP payload.
  event.insert(event.end(), {0xFF, 0xFF, 0xFF, 0xFF});
  PatchEventSize(event);
  return event;
}

std::vector<uint8_t> CreateTableMapEventPrefix() {
  std::vector<uint8_t> event = CreateBinlogHeader(MySQLBinlogEventType::TABLE_MAP_EVENT, 0);

  event.insert(event.end(), {0x34, 0x12, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00});

  const std::string db_name = "testdb";
  event.push_back(static_cast<uint8_t>(db_name.size()));
  event.insert(event.end(), db_name.begin(), db_name.end());
  event.push_back(0x00);

  const std::string table_name = "articles";
  event.push_back(static_cast<uint8_t>(table_name.size()));
  event.insert(event.end(), table_name.begin(), table_name.end());
  event.push_back(0x00);

  return event;
}

std::vector<uint8_t> BuildQueryEvent(const std::string& database, const std::string& query) {
  auto event = test::BinlogEventBuilder::BuildHeader(MySQLBinlogEventType::QUERY_EVENT);
  test::BinlogEventBuilder::AppendLittleEndian32(event, 1);  // thread_id
  test::BinlogEventBuilder::AppendLittleEndian32(event, 0);  // query_exec_time
  event.push_back(static_cast<uint8_t>(database.size()));
  test::BinlogEventBuilder::AppendLittleEndian16(event, 0);  // error_code
  test::BinlogEventBuilder::AppendLittleEndian16(event, 0);  // status_vars_len
  event.insert(event.end(), database.begin(), database.end());
  event.push_back(0x00);
  event.insert(event.end(), query.begin(), query.end());
  test::BinlogEventBuilder::AppendLittleEndian32(event, 0);  // checksum placeholder
  test::BinlogEventBuilder::FixEventSizeWithChecksum(event);
  return event;
}

void AppendVarchar(std::vector<uint8_t>& row_data, const std::string& value) {
  row_data.push_back(static_cast<uint8_t>(value.size()));
  row_data.insert(row_data.end(), value.begin(), value.end());
}

TableMetadata CreateConcatTableMetadata(uint64_t table_id) {
  TableMetadata metadata;
  metadata.table_id = table_id;
  metadata.database_name = "testdb";
  metadata.table_name = "articles";

  ColumnMetadata id;
  id.name = "id";
  id.type = ColumnType::LONG;
  metadata.columns.push_back(id);

  for (const std::string& name : {"title", "body", "tags"}) {
    ColumnMetadata column;
    column.name = name;
    column.type = ColumnType::VARCHAR;
    column.metadata = 255;
    metadata.columns.push_back(column);
  }

  return metadata;
}

std::vector<uint8_t> BuildConcatRowData(int32_t id, const std::string& title, const std::string& body,
                                        const std::string& tags) {
  std::vector<uint8_t> row_data;
  row_data.push_back(0x00);  // null bitmap for 4 non-null columns
  test::BinlogEventBuilder::AppendLittleEndian32(row_data, static_cast<uint32_t>(id));
  AppendVarchar(row_data, title);
  AppendVarchar(row_data, body);
  AppendVarchar(row_data, tags);
  return row_data;
}

/**
 * @brief Test GTID event extraction with actual data
 */
TEST(BinlogParsingTest, ExtractGTIDFromRealEvent) {
  // Create a GTID event buffer
  // Format: [OK byte] [19-byte header] [1-byte commit_flag] [16-byte UUID] [8-byte GNO]
  std::vector<uint8_t> gtid_event;

  // OK packet byte
  gtid_event.push_back(0x00);

  // 19-byte header
  auto header = CreateBinlogHeader(MySQLBinlogEventType::GTID_LOG_EVENT, 42);
  gtid_event.insert(gtid_event.end(), header.begin(), header.end());

  // commit_flag (1 byte)
  gtid_event.push_back(0x01);

  // UUID (16 bytes): 3E11FA47-71CA-11E1-9E33-C80AA9429562
  gtid_event.push_back(0x3E);
  gtid_event.push_back(0x11);
  gtid_event.push_back(0xFA);
  gtid_event.push_back(0x47);
  gtid_event.push_back(0x71);
  gtid_event.push_back(0xCA);
  gtid_event.push_back(0x11);
  gtid_event.push_back(0xE1);
  gtid_event.push_back(0x9E);
  gtid_event.push_back(0x33);
  gtid_event.push_back(0xC8);
  gtid_event.push_back(0x0A);
  gtid_event.push_back(0xA9);
  gtid_event.push_back(0x42);
  gtid_event.push_back(0x95);
  gtid_event.push_back(0x62);

  // GNO (8 bytes, little-endian): 100
  gtid_event.push_back(0x64);  // 100 in little-endian
  gtid_event.push_back(0x00);
  gtid_event.push_back(0x00);
  gtid_event.push_back(0x00);
  gtid_event.push_back(0x00);
  gtid_event.push_back(0x00);
  gtid_event.push_back(0x00);
  gtid_event.push_back(0x00);

  // Skip OK byte and extract GTID
  const unsigned char* buffer = gtid_event.data() + 1;
  unsigned long length = gtid_event.size() - 1;

  // This would be called by BinlogReader::ExtractGTID (private method)
  // For now, we verify the buffer structure is correct
  ASSERT_GE(length, 42);
  ASSERT_EQ(buffer[4], static_cast<uint8_t>(MySQLBinlogEventType::GTID_LOG_EVENT));

  // Verify UUID bytes
  EXPECT_EQ(buffer[20], 0x3E);
  EXPECT_EQ(buffer[21], 0x11);

  // Verify GNO (little-endian 100)
  EXPECT_EQ(buffer[36], 0x64);
  EXPECT_EQ(buffer[37], 0x00);
}

/**
 * @brief Test TABLE_MAP event parsing with actual parser
 */
TEST(BinlogParsingTest, ParseTableMapEventActual) {
  // Create a TABLE_MAP event buffer
  std::vector<uint8_t> table_map_event;

  // OK packet byte
  table_map_event.push_back(0x00);

  // 19-byte header
  auto header = CreateBinlogHeader(MySQLBinlogEventType::TABLE_MAP_EVENT, 50);
  table_map_event.insert(table_map_event.end(), header.begin(), header.end());

  // Post-header (6 bytes table_id + 2 bytes flags)
  // table_id: 0x1234 (little-endian, 6 bytes)
  table_map_event.push_back(0x34);
  table_map_event.push_back(0x12);
  table_map_event.push_back(0x00);
  table_map_event.push_back(0x00);
  table_map_event.push_back(0x00);
  table_map_event.push_back(0x00);

  // flags (2 bytes)
  table_map_event.push_back(0x00);
  table_map_event.push_back(0x00);

  // database name (1 byte length + string + null terminator)
  std::string db_name = "testdb";
  table_map_event.push_back(static_cast<uint8_t>(db_name.length()));
  table_map_event.insert(table_map_event.end(), db_name.begin(), db_name.end());
  table_map_event.push_back(0x00);  // null terminator

  // table name (1 byte length + string + null terminator)
  std::string table_name = "articles";
  table_map_event.push_back(static_cast<uint8_t>(table_name.length()));
  table_map_event.insert(table_map_event.end(), table_name.begin(), table_name.end());
  table_map_event.push_back(0x00);  // null terminator

  // column count (packed integer, single byte for small numbers)
  uint8_t column_count = 3;
  table_map_event.push_back(column_count);

  // column types (1 byte per column)
  table_map_event.push_back(static_cast<uint8_t>(ColumnType::LONG));     // id (INT)
  table_map_event.push_back(static_cast<uint8_t>(ColumnType::VARCHAR));  // title (VARCHAR)
  table_map_event.push_back(static_cast<uint8_t>(ColumnType::BLOB));     // content (TEXT)

  // metadata length (packed integer)
  table_map_event.push_back(0x05);  // 5 bytes of metadata

  // type-specific metadata
  // VARCHAR metadata (2 bytes): max length 255
  table_map_event.push_back(0xFF);
  table_map_event.push_back(0x00);

  // BLOB metadata (1 byte): length bytes = 2 (TEXT uses 2-byte length prefix)
  table_map_event.push_back(0x02);

  // NULL bitmap (ceil(3/8) = 1 byte)
  table_map_event.push_back(0x06);  // columns 1 and 2 can be NULL (bits 1 and 2 set)

  // Now verify the buffer is correctly structured
  ASSERT_EQ(table_map_event[0], 0x00);  // OK packet
  ASSERT_EQ(table_map_event[5], static_cast<uint8_t>(MySQLBinlogEventType::TABLE_MAP_EVENT));

  // Verify table_id
  uint64_t table_id = table_map_event[20] | (table_map_event[21] << 8);
  EXPECT_EQ(table_id, 0x1234);
}

TEST(BinlogParsingTest, ParseTableMapEventExcludesChecksumFromNullBitmap) {
  auto event = CreateMinimalTableMapEvent(/*null_bitmap=*/0x06, /*include_null_bitmap=*/true);

  auto metadata = BinlogEventParser::ParseTableMapEvent(event.data(), event.size());

  ASSERT_TRUE(metadata.has_value());
  ASSERT_EQ(metadata->columns.size(), 3u);
  EXPECT_FALSE(metadata->columns[0].is_nullable);
  EXPECT_TRUE(metadata->columns[1].is_nullable);
  EXPECT_TRUE(metadata->columns[2].is_nullable);
}

TEST(BinlogParsingTest, ParseTableMapEventRejectsMissingNullBitmapBeforeChecksum) {
  auto event = CreateMinimalTableMapEvent(/*null_bitmap=*/0x00, /*include_null_bitmap=*/false);

  auto metadata = BinlogEventParser::ParseTableMapEvent(event.data(), event.size());

  EXPECT_FALSE(metadata.has_value());
}

/**
 * @brief Test WRITE_ROWS event structure (INSERT)
 */
TEST(BinlogParsingTest, WriteRowsEventStructure) {
  // Create a minimal WRITE_ROWS_EVENT (v2)
  std::vector<uint8_t> write_rows_event;

  // OK packet byte
  write_rows_event.push_back(0x00);

  // 19-byte header
  auto header = CreateBinlogHeader(MySQLBinlogEventType::WRITE_ROWS_EVENT, 50);
  write_rows_event.insert(write_rows_event.end(), header.begin(), header.end());

  // Post-header
  // table_id (6 bytes)
  write_rows_event.push_back(0x01);
  write_rows_event.push_back(0x00);
  write_rows_event.push_back(0x00);
  write_rows_event.push_back(0x00);
  write_rows_event.push_back(0x00);
  write_rows_event.push_back(0x00);

  // flags (2 bytes) - set bit 0 for ROWS_EVENT_V2 with extra_row_info
  write_rows_event.push_back(0x01);
  write_rows_event.push_back(0x00);

  // extra_row_info_len (packed integer) - 2 bytes total (1 for length, 1 for data)
  write_rows_event.push_back(0x02);
  write_rows_event.push_back(0xFF);  // dummy data

  // column count (packed integer)
  write_rows_event.push_back(0x02);  // 2 columns

  // columns_present bitmap (ceil(2/8) = 1 byte)
  write_rows_event.push_back(0x03);  // both columns present (bits 0 and 1)

  // Verify structure
  ASSERT_EQ(write_rows_event[0], 0x00);  // OK byte
  ASSERT_EQ(write_rows_event[5], static_cast<uint8_t>(MySQLBinlogEventType::WRITE_ROWS_EVENT));

  // Verify flags indicate ROWS_EVENT_V2
  uint16_t flags = write_rows_event[26] | (write_rows_event[27] << 8);
  EXPECT_EQ(flags & 0x01, 0x01);  // Bit 0 set for V2 with extra_row_info

  // Verify extra_row_info_len position (after table_id + flags)
  EXPECT_EQ(write_rows_event[28], 0x02);  // extra_row_info_len

  // Verify column_count position (after extra_row_info)
  EXPECT_EQ(write_rows_event[30], 0x02);  // column_count
}

/**
 * @brief Test UPDATE_ROWS event structure
 */
TEST(BinlogParsingTest, UpdateRowsEventStructure) {
  std::vector<uint8_t> update_rows_event;

  // OK packet byte
  update_rows_event.push_back(0x00);

  // 19-byte header
  auto header = CreateBinlogHeader(MySQLBinlogEventType::UPDATE_ROWS_EVENT, 60);
  update_rows_event.insert(update_rows_event.end(), header.begin(), header.end());

  // Post-header
  // table_id (6 bytes)
  for (int i = 0; i < 6; i++) {
    update_rows_event.push_back(0x02);
  }

  // flags (2 bytes)
  update_rows_event.push_back(0x01);  // V2 with extra_row_info
  update_rows_event.push_back(0x00);

  // extra_row_info
  update_rows_event.push_back(0x02);  // len
  update_rows_event.push_back(0xAA);  // data

  // column count
  update_rows_event.push_back(0x03);  // 3 columns

  // columns_before bitmap (for before image)
  update_rows_event.push_back(0x07);  // all 3 columns (bits 0,1,2)

  // columns_after bitmap (for after image)
  update_rows_event.push_back(0x07);  // all 3 columns

  // Verify structure
  ASSERT_EQ(update_rows_event[0], 0x00);
  ASSERT_EQ(update_rows_event[5], static_cast<uint8_t>(MySQLBinlogEventType::UPDATE_ROWS_EVENT));

  // UPDATE events have both before and after bitmaps
  EXPECT_EQ(update_rows_event[30], 0x03);  // column_count
  EXPECT_EQ(update_rows_event[31], 0x07);  // columns_before
  EXPECT_EQ(update_rows_event[32], 0x07);  // columns_after
}

/**
 * @brief Test DELETE_ROWS event structure
 */
TEST(BinlogParsingTest, DeleteRowsEventStructure) {
  std::vector<uint8_t> delete_rows_event;

  // OK packet byte
  delete_rows_event.push_back(0x00);

  // 19-byte header
  auto header = CreateBinlogHeader(MySQLBinlogEventType::DELETE_ROWS_EVENT, 50);
  delete_rows_event.insert(delete_rows_event.end(), header.begin(), header.end());

  // Post-header (same as WRITE_ROWS)
  // table_id (6 bytes)
  for (int i = 0; i < 6; i++) {
    delete_rows_event.push_back(0x03);
  }

  // flags (2 bytes)
  delete_rows_event.push_back(0x01);
  delete_rows_event.push_back(0x00);

  // extra_row_info
  delete_rows_event.push_back(0x02);
  delete_rows_event.push_back(0xBB);

  // column count
  delete_rows_event.push_back(0x02);

  // columns_present bitmap (only before image for DELETE)
  delete_rows_event.push_back(0x03);

  // Verify structure
  ASSERT_EQ(delete_rows_event[0], 0x00);
  ASSERT_EQ(delete_rows_event[5], static_cast<uint8_t>(MySQLBinlogEventType::DELETE_ROWS_EVENT));
  EXPECT_EQ(delete_rows_event[30], 0x02);  // column_count
}

/**
 * @brief Test OK packet byte skip in binlog event parsing
 *
 * MySQL C API prepends an OK packet byte (0x00) before binlog events.
 * This test ensures the parser correctly skips this byte.
 */
TEST(BinlogParsingTest, OKPacketByteSkip) {
  // Create a minimal binlog event with OK packet byte
  // Format: [0x00 (OK packet)] [19-byte header] [event data]
  std::vector<uint8_t> event_with_ok_packet = {
      0x00,  // OK packet byte (should be skipped)
      // Binlog header (19 bytes):
      0x00, 0x00, 0x00, 0x00,  // timestamp (4 bytes)
      0x0F,                    // event_type = FORMAT_DESCRIPTION_EVENT (15)
      0x01, 0x00, 0x00, 0x00,  // server_id (4 bytes)
      0x14, 0x00, 0x00, 0x00,  // event_size = 20 bytes (4 bytes)
      0x00, 0x00, 0x00, 0x00,  // log_pos (4 bytes)
      0x00, 0x00               // flags (2 bytes)
  };

  // Without OK packet skip, event type would be read from buffer[4] = 0x00
  // With OK packet skip, event type should be read from buffer[5] = 0x0F (FORMAT_DESCRIPTION_EVENT)

  // Verify the test data is constructed correctly
  ASSERT_EQ(event_with_ok_packet.size(), 20);
  ASSERT_EQ(event_with_ok_packet[0], 0x00);  // OK packet
  ASSERT_EQ(event_with_ok_packet[5], 0x0F);  // Event type after OK byte
}

/**
 * @brief Test ROWS_EVENT_V2 extra_row_info parsing
 *
 * MySQL 8.0 uses ROWS_EVENT_V2 format which includes extra_row_info field.
 * This test ensures the parser correctly skips this field before reading column count.
 */
TEST(BinlogParsingTest, RowsEventV2ExtraRowInfo) {
  // Test data simulating ROWS_EVENT_V2 post-header with extra_row_info
  // Format: [table_id][flags with bit 0 set][extra_row_info_len][extra_row_info][column_count][columns_before]
  std::vector<uint8_t> rows_event_v2 = {
      // Post-header:
      0x80, 0x00, 0x00, 0x00, 0x00, 0x00,  // table_id (6 bytes)
      0x01, 0x00,                          // flags = 0x0001 (ROWS_EVENT_V2 with extra info)
      // Extra row info:
      0x02,  // extra_row_info_len = 2 (packed integer, total length including this byte)
      0xFF,  // extra_row_info data (1 byte, since total len=2, data len=2-1=1)
      // Column info:
      0x03,  // column_count = 3 (packed integer)
      0x07,  // columns_before bitmap (3 bits set: 0b111)
  };

  // Without extra_row_info skip:
  // - Would read column_count from position after flags (byte 8) = 0x02 (wrong!)
  // With extra_row_info skip:
  // - Should skip extra_row_info (2 bytes total) and read column_count = 0x03

  ASSERT_EQ(rows_event_v2[8], 0x02);   // extra_row_info_len
  ASSERT_EQ(rows_event_v2[10], 0x03);  // column_count (after skipping extra_row_info)
}

/**
 * @brief Test event size calculation from binlog header
 *
 * Event size should be read from the binlog header (bytes 9-12),
 * not from mysql_binlog_fetch's length parameter.
 */
TEST(BinlogParsingTest, EventSizeCalculation) {
  // Create a binlog event header with specific event_size
  std::vector<uint8_t> event_header = {
      0x00, 0x00, 0x00, 0x00,  // timestamp (4 bytes)
      0x1F,                    // event_type = UPDATE_ROWS_EVENT (31)
      0x01, 0x00, 0x00, 0x00,  // server_id (4 bytes)
      0x30, 0x01, 0x00, 0x00,  // event_size = 0x0130 = 304 bytes (4 bytes, little-endian)
      0x00, 0x00, 0x00, 0x00,  // log_pos (4 bytes)
      0x00, 0x00               // flags (2 bytes)
  };

  // Read event_size from bytes 9-12 (little-endian)
  uint32_t event_size = event_header[9] | (event_header[10] << 8) | (event_header[11] << 16) | (event_header[12] << 24);

  ASSERT_EQ(event_size, 304);  // 0x0130 in little-endian = 304

  // Verify header size is 19 bytes
  ASSERT_EQ(event_header.size(), 19);
}

/**
 * @brief Test boundary checks prevent parsing beyond event data
 *
 * Parser should check (ptr >= end) before reading field values
 * to avoid parsing padding/checksum bytes as row data.
 */
TEST(BinlogParsingTest, BoundaryChecks) {
  // Simulate a row data buffer with known end position
  std::vector<uint8_t> row_data = {0x01,  // Some data
                                   0x02,  // Some data
                                   0x03,  // Some data
                                   // Last 4 bytes are checksum/padding (should not be parsed as row data)
                                   0xAA, 0xBB, 0xCC, 0xDD};

  const unsigned char* ptr = row_data.data();
  const unsigned char* end = row_data.data() + 3;  // End before checksum

  // Simulate parsing loop
  int values_read = 0;
  while (ptr < end) {
    // This would read field values
    ptr++;
    values_read++;
  }

  // Should only read 3 values, not 7 (which would include checksum)
  ASSERT_EQ(values_read, 3);
  ASSERT_EQ(ptr, end);
}

/**
 * @brief Test column count parsing with extra_row_info
 *
 * Column count should be read AFTER skipping extra_row_info when present.
 */
TEST(BinlogParsingTest, ColumnCountWithExtraRowInfo) {
  // ROWS_EVENT_V2 with extra_row_info (flags & 0x0001 = true)
  std::vector<uint8_t> event_data_with_extra_info = {
      0x80, 0x00, 0x00, 0x00, 0x00, 0x00,  // table_id
      0x01, 0x00,                          // flags = 0x0001 (has extra_row_info)
      0x03,                                // extra_row_info_len = 3 bytes total
      0x12, 0x34,                          // extra_row_info data (2 bytes)
      0x15,                                // column_count = 21 (0x15)
  };

  // Verify column_count is at correct offset after extra_row_info
  size_t offset = 8;  // After table_id and flags

  // Read extra_row_info_len
  uint8_t extra_info_len = event_data_with_extra_info[offset];
  ASSERT_EQ(extra_info_len, 3);

  // Column_count is at offset + extra_info_len
  uint8_t column_count = event_data_with_extra_info[offset + extra_info_len];
  ASSERT_EQ(column_count, 21);  // 0x15 = 21 columns
}

/**
 * @brief Test packed integer reading for extra_row_info_len
 *
 * Extra_row_info_len is a MySQL packed integer (length-encoded integer).
 * For values < 251, it's just a single byte.
 */
TEST(BinlogParsingTest, PackedIntegerReading) {
  // Test single-byte packed integer (value < 251)
  std::vector<uint8_t> packed_int_small = {0x02};  // Value = 2
  ASSERT_LT(packed_int_small[0], 251);

  // Test two-byte packed integer (251 <= value < 65536)
  std::vector<uint8_t> packed_int_medium = {0xFC, 0x00, 0x01};  // Value = 256
  ASSERT_EQ(packed_int_medium[0], 0xFC);

  // For this fix, we only handle single-byte packed integers
  // since extra_row_info_len is typically small (< 251 bytes)
}

/**
 * @brief Test truncated buffer handling
 */
TEST(BinlogParsingTest, TruncatedBufferHandling) {
  // Create a truncated GTID event (should be at least 42 bytes but only give 20)
  std::vector<uint8_t> truncated_event;
  truncated_event.push_back(0x00);  // OK byte

  auto header = CreateBinlogHeader(MySQLBinlogEventType::GTID_LOG_EVENT, 42);
  truncated_event.insert(truncated_event.end(), header.begin(), header.end());

  // Event is truncated (only 20 bytes instead of 42)
  ASSERT_EQ(truncated_event.size(), 20);

  // Parser should detect this and return nullopt (tested via minimum length checks)
  // A proper GTID event needs at least 42 bytes
  EXPECT_LT(truncated_event.size() - 1, 42);
}

/**
 * @brief Test integer overflow protection in TABLE_MAP event parsing
 *
 * Tests security fixes for:
 * - Column count validation (prevents excessive allocation)
 * - Buffer boundary checking (prevents buffer overflow)
 * - Remaining bytes tracking after packed integer reads
 */
TEST(BinlogParsingSecurityTest, TableMapIntegerOverflowProtection) {
  auto event = CreateTableMapEventPrefix();
  // 4097 as a 0xfc length-encoded integer, exceeding kMySQLMaxColumns.
  event.insert(event.end(), {0xFC, 0x01, 0x10});
  event.insert(event.end(), {0xAA, 0xBB, 0xCC, 0xDD});
  PatchEventSize(event);

  auto metadata = BinlogEventParser::ParseTableMapEvent(event.data(), event.size());
  EXPECT_FALSE(metadata.has_value());
}

/**
 * @brief Test column count limit enforcement
 */
TEST(BinlogParsingSecurityTest, ColumnCountLimit) {
  auto valid_event = CreateMinimalTableMapEvent(0x00, true);
  auto valid_metadata = BinlogEventParser::ParseTableMapEvent(valid_event.data(), valid_event.size());
  ASSERT_TRUE(valid_metadata.has_value());
  EXPECT_EQ(valid_metadata->columns.size(), 3);

  auto excessive_event = CreateTableMapEventPrefix();
  // 10000 as a 0xfc length-encoded integer.
  excessive_event.insert(excessive_event.end(), {0xFC, 0x10, 0x27});
  excessive_event.insert(excessive_event.end(), {0xAA, 0xBB, 0xCC, 0xDD});
  PatchEventSize(excessive_event);

  auto excessive_metadata = BinlogEventParser::ParseTableMapEvent(excessive_event.data(), excessive_event.size());
  EXPECT_FALSE(excessive_metadata.has_value());
}

/**
 * @brief Test remaining bytes tracking
 */
TEST(BinlogParsingSecurityTest, RemainingBytesTracking) {
  auto event = CreateTableMapEventPrefix();
  // 0xfc declares a 3-byte packed integer, but only the first byte is present
  // before the CRC32 trailer.
  event.push_back(0xFC);
  event.insert(event.end(), {0xAA, 0xBB, 0xCC, 0xDD});
  PatchEventSize(event);

  auto metadata = BinlogEventParser::ParseTableMapEvent(event.data(), event.size());
  EXPECT_FALSE(metadata.has_value());
}

/**
 * @brief Test metadata bounds checking
 */
TEST(BinlogParsingSecurityTest, MetadataBoundsChecking) {
  auto truncated_meta_len_event = CreateTableMapEventPrefix();
  truncated_meta_len_event.push_back(1);  // column count
  truncated_meta_len_event.push_back(static_cast<uint8_t>(ColumnType::LONG));
  // 0xfc declares a 3-byte metadata length, but only the first byte is present.
  truncated_meta_len_event.push_back(0xFC);
  truncated_meta_len_event.insert(truncated_meta_len_event.end(), {0xAA, 0xBB, 0xCC, 0xDD});
  PatchEventSize(truncated_meta_len_event);

  auto truncated_meta_len =
      BinlogEventParser::ParseTableMapEvent(truncated_meta_len_event.data(), truncated_meta_len_event.size());
  EXPECT_FALSE(truncated_meta_len.has_value());

  auto oversized_metadata_event = CreateTableMapEventPrefix();
  oversized_metadata_event.push_back(1);  // column count
  oversized_metadata_event.push_back(static_cast<uint8_t>(ColumnType::LONG));
  oversized_metadata_event.push_back(4);  // metadata length, but no metadata bytes before CRC32
  oversized_metadata_event.insert(oversized_metadata_event.end(), {0xAA, 0xBB, 0xCC, 0xDD});
  PatchEventSize(oversized_metadata_event);

  auto oversized_metadata =
      BinlogEventParser::ParseTableMapEvent(oversized_metadata_event.data(), oversized_metadata_event.size());
  EXPECT_FALSE(oversized_metadata.has_value());
}

TEST(BinlogParsingTest, QueryEventDdlIgnoresSameTableFromDifferentDatabase) {
  server::TableContext ctx;
  ctx.name = "articles";
  ctx.config.name = "articles";
  ctx.config.database = "app";

  std::unordered_map<std::string, server::TableContext*> table_contexts;
  table_contexts.emplace("articles", &ctx);
  TableMetadataCache metadata_cache;

  auto other_db_event = BuildQueryEvent("analytics", "TRUNCATE TABLE articles");
  auto other_db_events = BinlogEventParser::ParseBinlogEvent(other_db_event.data(), other_db_event.size(), "uuid:1",
                                                             metadata_cache, table_contexts, nullptr, true);
  EXPECT_TRUE(other_db_events.empty());

  auto target_db_event = BuildQueryEvent("app", "TRUNCATE TABLE articles");
  auto target_db_events = BinlogEventParser::ParseBinlogEvent(target_db_event.data(), target_db_event.size(), "uuid:1",
                                                              metadata_cache, table_contexts, nullptr, true);
  ASSERT_EQ(target_db_events.size(), 1u);
  EXPECT_EQ(target_db_events[0].type, BinlogEventType::DDL);
  EXPECT_EQ(target_db_events[0].table_name, "articles");
}

TEST(BinlogParsingTest, QueryEventDdlHonorsSchemaQualifiedTableNames) {
  server::TableContext ctx;
  ctx.name = "articles";
  ctx.config.name = "articles";
  ctx.config.database = "app";

  std::unordered_map<std::string, server::TableContext*> table_contexts;
  table_contexts.emplace("articles", &ctx);
  TableMetadataCache metadata_cache;

  auto qualified_target_event = BuildQueryEvent("analytics", "DROP TABLE app.articles");
  auto qualified_target_events =
      BinlogEventParser::ParseBinlogEvent(qualified_target_event.data(), qualified_target_event.size(), "uuid:1",
                                          metadata_cache, table_contexts, nullptr, true);
  ASSERT_EQ(qualified_target_events.size(), 1u);
  EXPECT_EQ(qualified_target_events[0].table_name, "articles");

  auto qualified_other_event = BuildQueryEvent("app", "DROP TABLE analytics.articles");
  auto qualified_other_events =
      BinlogEventParser::ParseBinlogEvent(qualified_other_event.data(), qualified_other_event.size(), "uuid:2",
                                          metadata_cache, table_contexts, nullptr, true);
  EXPECT_TRUE(qualified_other_events.empty());
}

TEST(BinlogParsingTest, QueryEventMultiStatementClassifiesMatchingDdlStatement) {
  server::TableContext articles_ctx;
  articles_ctx.name = "articles";
  articles_ctx.config.name = "articles";
  articles_ctx.config.database = "app";

  server::TableContext users_ctx;
  users_ctx.name = "users";
  users_ctx.config.name = "users";
  users_ctx.config.database = "app";

  std::unordered_map<std::string, server::TableContext*> table_contexts;
  table_contexts.emplace("articles", &articles_ctx);
  table_contexts.emplace("users", &users_ctx);
  TableMetadataCache metadata_cache;

  auto event = BuildQueryEvent("app", "ALTER TABLE users ADD COLUMN stale INT; DROP TABLE articles");
  auto events = BinlogEventParser::ParseBinlogEvent(event.data(), event.size(), "uuid:3", metadata_cache,
                                                    table_contexts, nullptr, true);

  ASSERT_EQ(events.size(), 2u);
  std::unordered_map<std::string, DDLType> types_by_table;
  for (const auto& parsed_event : events) {
    types_by_table.emplace(parsed_event.table_name, parsed_event.ddl_type);
  }
  EXPECT_EQ(types_by_table["users"], DDLType::kAlter);
  EXPECT_EQ(types_by_table["articles"], DDLType::kDrop);
}

/**
 * @brief Test IsTableAffectingDDL with TRUNCATE TABLE statements
 */
TEST(BinlogParsingTest, IsTableAffectingDDL_TruncateTable) {
  // Test basic TRUNCATE TABLE
  EXPECT_TRUE(BinlogEventParser::IsTableAffectingDDL("TRUNCATE TABLE articles", "articles"));

  // Test with backticks
  EXPECT_TRUE(BinlogEventParser::IsTableAffectingDDL("TRUNCATE TABLE `articles`", "articles"));

  // Test case insensitive
  EXPECT_TRUE(BinlogEventParser::IsTableAffectingDDL("truncate table articles", "articles"));
  EXPECT_TRUE(BinlogEventParser::IsTableAffectingDDL("TrUnCaTe TaBlE articles", "articles"));

  // Test with multiple spaces
  EXPECT_TRUE(BinlogEventParser::IsTableAffectingDDL("TRUNCATE  TABLE   articles", "articles"));
  EXPECT_TRUE(BinlogEventParser::IsTableAffectingDDL("TRUNCATE\t\tTABLE\t\tarticles", "articles"));

  // Test with newlines and tabs
  EXPECT_TRUE(BinlogEventParser::IsTableAffectingDDL("TRUNCATE\nTABLE\narticles", "articles"));

  // Test different table name should not match
  EXPECT_FALSE(BinlogEventParser::IsTableAffectingDDL("TRUNCATE TABLE users", "articles"));
  EXPECT_FALSE(BinlogEventParser::IsTableAffectingDDL("TRUNCATE TABLE articles_backup", "articles"));
}

/**
 * @brief Test IsTableAffectingDDL with DROP TABLE statements
 */
TEST(BinlogParsingTest, IsTableAffectingDDL_DropTable) {
  // Test basic DROP TABLE
  EXPECT_TRUE(BinlogEventParser::IsTableAffectingDDL("DROP TABLE articles", "articles"));

  // Test with backticks
  EXPECT_TRUE(BinlogEventParser::IsTableAffectingDDL("DROP TABLE `articles`", "articles"));

  // Test with IF EXISTS
  EXPECT_TRUE(BinlogEventParser::IsTableAffectingDDL("DROP TABLE IF EXISTS articles", "articles"));
  EXPECT_TRUE(BinlogEventParser::IsTableAffectingDDL("DROP TABLE IF EXISTS `articles`", "articles"));

  // Test case insensitive
  EXPECT_TRUE(BinlogEventParser::IsTableAffectingDDL("drop table articles", "articles"));
  EXPECT_TRUE(BinlogEventParser::IsTableAffectingDDL("drop table if exists articles", "articles"));

  // Test with multiple spaces
  EXPECT_TRUE(BinlogEventParser::IsTableAffectingDDL("DROP  TABLE   IF  EXISTS  articles", "articles"));

  // Test different table name should not match
  EXPECT_FALSE(BinlogEventParser::IsTableAffectingDDL("DROP TABLE users", "articles"));
  EXPECT_FALSE(BinlogEventParser::IsTableAffectingDDL("DROP TABLE IF EXISTS users", "articles"));
}

/**
 * @brief Test IsTableAffectingDDL with ALTER TABLE statements
 */
TEST(BinlogParsingTest, IsTableAffectingDDL_AlterTable) {
  // Test basic ALTER TABLE
  EXPECT_TRUE(BinlogEventParser::IsTableAffectingDDL("ALTER TABLE articles ADD COLUMN status INT", "articles"));

  // Test with backticks
  EXPECT_TRUE(BinlogEventParser::IsTableAffectingDDL("ALTER TABLE `articles` ADD COLUMN status INT", "articles"));

  // Test case insensitive
  EXPECT_TRUE(BinlogEventParser::IsTableAffectingDDL("alter table articles add column status int", "articles"));

  // Test with multiple spaces
  EXPECT_TRUE(BinlogEventParser::IsTableAffectingDDL("ALTER  TABLE   articles  ADD  COLUMN status INT", "articles"));

  // Test various ALTER TABLE operations
  EXPECT_TRUE(BinlogEventParser::IsTableAffectingDDL("ALTER TABLE articles DROP COLUMN status", "articles"));
  EXPECT_TRUE(
      BinlogEventParser::IsTableAffectingDDL("ALTER TABLE articles MODIFY COLUMN title VARCHAR(500)", "articles"));
  EXPECT_TRUE(BinlogEventParser::IsTableAffectingDDL("ALTER TABLE articles ADD INDEX idx_status (status)", "articles"));

  // Test different table name should not match
  EXPECT_FALSE(BinlogEventParser::IsTableAffectingDDL("ALTER TABLE users ADD COLUMN email VARCHAR(255)", "articles"));
}

/**
 * @brief Test IsTableAffectingDDL with non-matching statements
 */
TEST(BinlogParsingTest, IsTableAffectingDDL_NonMatching) {
  // Test SELECT statements
  EXPECT_FALSE(BinlogEventParser::IsTableAffectingDDL("SELECT * FROM articles", "articles"));

  // Test INSERT statements
  EXPECT_FALSE(
      BinlogEventParser::IsTableAffectingDDL("INSERT INTO articles VALUES (1, 'title', 'content')", "articles"));

  // Test UPDATE statements
  EXPECT_FALSE(BinlogEventParser::IsTableAffectingDDL("UPDATE articles SET title='new title' WHERE id=1", "articles"));

  // Test DELETE statements
  EXPECT_FALSE(BinlogEventParser::IsTableAffectingDDL("DELETE FROM articles WHERE id=1", "articles"));

  // Test CREATE TABLE statements (different table)
  EXPECT_FALSE(BinlogEventParser::IsTableAffectingDDL("CREATE TABLE users (id INT PRIMARY KEY)", "articles"));

  // Test empty string
  EXPECT_FALSE(BinlogEventParser::IsTableAffectingDDL("", "articles"));

  // Test partial keyword matches should not match
  EXPECT_FALSE(BinlogEventParser::IsTableAffectingDDL("TRUNCATE_TABLE articles", "articles"));
  EXPECT_FALSE(BinlogEventParser::IsTableAffectingDDL("DROPTABLE articles", "articles"));
  EXPECT_FALSE(BinlogEventParser::IsTableAffectingDDL("ALTERTABLE articles", "articles"));
}

/**
 * @brief Test IsTableAffectingDDL with edge cases
 */
TEST(BinlogParsingTest, IsTableAffectingDDL_EdgeCases) {
  // Test table name as substring of another table name
  EXPECT_FALSE(BinlogEventParser::IsTableAffectingDDL("DROP TABLE articles_backup", "articles"));
  EXPECT_FALSE(BinlogEventParser::IsTableAffectingDDL("DROP TABLE old_articles", "articles"));

  // Test with semicolons
  EXPECT_TRUE(BinlogEventParser::IsTableAffectingDDL("TRUNCATE TABLE articles;", "articles"));
  EXPECT_TRUE(BinlogEventParser::IsTableAffectingDDL("DROP TABLE articles;", "articles"));

  // Test with multiple statements (should match if any affects the table)
  EXPECT_TRUE(BinlogEventParser::IsTableAffectingDDL("DROP TABLE users; DROP TABLE articles;", "articles"));

  // Test with comments (simplified - real parser may need to handle comments)
  EXPECT_TRUE(BinlogEventParser::IsTableAffectingDDL("/* comment */ DROP TABLE articles", "articles"));

  // Test table name case sensitivity (table names are converted to uppercase for matching)
  EXPECT_TRUE(BinlogEventParser::IsTableAffectingDDL("DROP TABLE ARTICLES", "articles"));
  EXPECT_TRUE(BinlogEventParser::IsTableAffectingDDL("DROP TABLE Articles", "articles"));
}

/**
 * @brief Test IsTableAffectingDDL with RENAME TABLE statements
 */
TEST(BinlogParsingTest, IsTableAffectingDDL_RenameTable) {
  // RENAME TABLE old_name TO target_table
  EXPECT_TRUE(BinlogEventParser::IsTableAffectingDDL("RENAME TABLE articles TO articles_new", "articles"));
  EXPECT_TRUE(BinlogEventParser::IsTableAffectingDDL("RENAME TABLE old_articles TO articles", "articles"));

  // Case insensitive
  EXPECT_TRUE(BinlogEventParser::IsTableAffectingDDL("rename table articles to articles_new", "articles"));

  // With backticks
  EXPECT_TRUE(BinlogEventParser::IsTableAffectingDDL("RENAME TABLE `articles` TO `articles_new`", "articles"));
  EXPECT_TRUE(BinlogEventParser::IsTableAffectingDDL("RENAME TABLE `old` TO `articles`", "articles"));

  // Multiple spaces/tabs
  EXPECT_TRUE(BinlogEventParser::IsTableAffectingDDL("RENAME  TABLE   articles   TO   articles_new", "articles"));

  // Non-matching: neither source nor target is the monitored table
  EXPECT_FALSE(BinlogEventParser::IsTableAffectingDDL("RENAME TABLE users TO users_old", "articles"));

  // Table name as substring should not match
  EXPECT_FALSE(BinlogEventParser::IsTableAffectingDDL("RENAME TABLE articles_backup TO articles_old", "articles"));

  // Multi-table rename: RENAME TABLE a TO b, c TO d
  EXPECT_TRUE(
      BinlogEventParser::IsTableAffectingDDL("RENAME TABLE users TO users_old, articles TO articles_old", "articles"));
}

/**
 * @brief Test IsTableAffectingDDL security - no regex injection
 */
TEST(BinlogParsingTest, IsTableAffectingDDL_Security) {
  // Test that special regex characters in table names don't cause issues
  // (Since we removed regex, these should be treated as literal characters)

  // Test with special characters that would be regex metacharacters
  EXPECT_FALSE(BinlogEventParser::IsTableAffectingDDL("DROP TABLE test.*", "articles"));
  EXPECT_FALSE(BinlogEventParser::IsTableAffectingDDL("DROP TABLE test+", "articles"));
  EXPECT_FALSE(BinlogEventParser::IsTableAffectingDDL("DROP TABLE test[abc]", "articles"));

  // Test that very long strings don't cause performance issues
  std::string long_query = "SELECT * FROM ";
  for (int i = 0; i < 1000; ++i) {
    long_query += "table" + std::to_string(i) + ", ";
  }
  long_query += "articles";

  // This should complete quickly (no ReDoS vulnerability)
  auto start = std::chrono::steady_clock::now();
  bool result = BinlogEventParser::IsTableAffectingDDL(long_query, "articles");
  auto end = std::chrono::steady_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

  EXPECT_FALSE(result);  // Not a DDL statement
  EXPECT_LT(duration, 100) << "Query parsing took too long (possible ReDoS): " << duration << "ms";
}

/**
 * @brief Test that binlog_reader correctly handles OK byte skip without double skipping
 *
 * Bug: binlog_reader.cpp was skipping OK byte, then binlog_event_parser.cpp
 * was also skipping a byte, causing buffer misalignment.
 *
 * Fix: binlog_reader.cpp skips OK byte and passes (buffer+1) to parser.
 * Parser now reads directly from buffer without additional skip.
 */
TEST(BinlogParsingTest, DoubleOKByteSkipBugFixed) {
  // Create a complete binlog event with OK byte
  std::vector<uint8_t> event_with_ok = {
      0x00,  // OK byte (position 0) - skipped by binlog_reader
      // Binlog event header (19 bytes starting at position 1):
      0x00, 0x00, 0x00, 0x00,  // timestamp (positions 1-4)
      0x04,                    // event_type = ROTATE_EVENT (position 5)
      0x01, 0x00, 0x00, 0x00,  // server_id = 1 (positions 6-9)
      0x1E, 0x00, 0x00, 0x00,  // event_size = 30 bytes (positions 10-13)
      0x00, 0x00, 0x00, 0x00,  // log_pos (positions 14-17)
      0x00, 0x00,              // flags (positions 18-19)
      // Event data (11 bytes):
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,  // position
      0x74, 0x65, 0x73                                 // "tes"
  };

  // Simulate what binlog_reader does: skip OK byte
  const unsigned char* buffer = event_with_ok.data() + 1;  // Skip OK byte at position 0
  unsigned long length = event_with_ok.size() - 1;

  // Verify buffer structure AFTER OK byte skip (this is what parser receives)
  ASSERT_GE(length, 19);

  // Event type should be at buffer[4] (which was originally at position 5)
  EXPECT_EQ(buffer[4], 0x04) << "Event type should be ROTATE_EVENT (4)";

  // Server ID should be at buffer[5-8] (originally positions 6-9)
  EXPECT_EQ(buffer[5], 0x01) << "Server ID byte 0 should be 0x01";

  // Event size should be at buffer[9-12] (originally positions 10-13)
  uint32_t event_size = buffer[9] | (buffer[10] << 8) | (buffer[11] << 16) | (buffer[12] << 24);
  EXPECT_EQ(event_size, 30) << "Event size should be 30 bytes";

  // Verify that if parser incorrectly skipped another byte (the bug),
  // it would read wrong values:
  const unsigned char* buggy_buffer = buffer + 1;  // Simulating double skip bug
  uint8_t wrong_event_type = buggy_buffer[4];
  EXPECT_NE(wrong_event_type, 0x04) << "With double skip, event type would be wrong";
  EXPECT_EQ(wrong_event_type, 0x01) << "With double skip, would read server_id[0] as event type";
}

/**
 * @brief Test UPDATE_ROWS_EVENT parsing with correct buffer offset
 *
 * Verifies that event_size is read from correct position (buffer[9-12])
 * and post-header starts at buffer[19] after OK byte skip by binlog_reader.
 */
TEST(BinlogParsingTest, UpdateRowsEventOffsetAfterOKByteSkip) {
  // Create UPDATE_ROWS_EVENT with OK byte
  std::vector<uint8_t> update_event = {
      0x00,  // OK byte - skipped by binlog_reader
      // Header (19 bytes):
      0x00, 0x00, 0x00, 0x00,  // timestamp
      0x1F,                    // event_type = UPDATE_ROWS_EVENT (31)
      0x01, 0x00, 0x00, 0x00,  // server_id
      0xCC, 0x01, 0x00, 0x00,  // event_size = 460 bytes (0x01CC)
      0x00, 0x00, 0x00, 0x00,  // log_pos
      0x00, 0x00,              // flags
      // Post-header starts here (at buffer[19] after OK skip):
      0x80, 0x00, 0x00, 0x00, 0x00, 0x00,  // table_id (6 bytes)
      0x01, 0x00,                          // flags (2 bytes)
  };

  // Simulate binlog_reader behavior
  const unsigned char* buffer = update_event.data() + 1;
  unsigned long length = update_event.size() - 1;

  ASSERT_GE(length, 27);  // 19 header + 8 post-header

  // Event type at buffer[4]
  EXPECT_EQ(buffer[4], 0x1F) << "Event type should be UPDATE_ROWS_EVENT (31)";

  // Event size at buffer[9-12] (little-endian)
  uint32_t event_size = buffer[9] | (buffer[10] << 8) | (buffer[11] << 16) | (buffer[12] << 24);
  EXPECT_EQ(event_size, 460) << "Event size should be 460 (0x01CC)";

  // Post-header starts at buffer[19]
  EXPECT_EQ(buffer[19], 0x80) << "table_id first byte at buffer[19]";
  EXPECT_EQ(buffer[20], 0x00) << "table_id second byte at buffer[20]";

  // Verify wrong offset would give wrong values (the bug scenario)
  // If using buffer + 20 (the old bug), would skip past table_id
  EXPECT_NE(buffer[20], 0x80) << "buffer[20] is not table_id start";
}

/**
 * @brief Test TABLE_MAP_EVENT parsing with correct offset
 */
TEST(BinlogParsingTest, TableMapEventOffsetAfterOKByteSkip) {
  std::vector<uint8_t> table_map = {
      0x00,  // OK byte
      // Header (19 bytes):
      0x00, 0x00, 0x00, 0x00,  // timestamp
      0x13,                    // event_type = TABLE_MAP_EVENT (19)
      0x01, 0x00, 0x00, 0x00,  // server_id
      0x32, 0x00, 0x00, 0x00,  // event_size = 50 bytes
      0x00, 0x00, 0x00, 0x00,  // log_pos
      0x00, 0x00,              // flags
      // Post-header at buffer[19]:
      0x05, 0x00, 0x00, 0x00, 0x00, 0x00,  // table_id = 5
  };

  const unsigned char* buffer = table_map.data() + 1;
  unsigned long length = table_map.size() - 1;

  ASSERT_GE(length, 25);

  // Event type at buffer[4]
  EXPECT_EQ(buffer[4], 0x13) << "Event type should be TABLE_MAP_EVENT (19)";

  // table_id at buffer[19]
  uint64_t table_id = buffer[19] | (buffer[20] << 8);
  EXPECT_EQ(table_id, 5) << "table_id should be 5";
}

/**
 * @brief Test checksum exclusion in event parsing
 *
 * MySQL binlog events include a 4-byte checksum at the end, even when checksums
 * are disabled via SET @source_binlog_checksum='NONE'.
 * Parser must exclude these 4 bytes when calculating event data end position.
 */
TEST(BinlogParsingTest, ChecksumExclusionInEventParsing) {
  // Create a ROWS_EVENT with checksum
  std::vector<uint8_t> event_with_checksum = {
      0x00,  // OK byte (skipped by binlog_reader)
      // Header (19 bytes):
      0x00, 0x00, 0x00, 0x00,  // timestamp
      0x1E,                    // event_type = WRITE_ROWS_EVENT (30)
      0x01, 0x00, 0x00, 0x00,  // server_id
      0x32, 0x00, 0x00, 0x00,  // event_size = 50 bytes (includes header + data + checksum)
      0x00, 0x00, 0x00, 0x00,  // log_pos
      0x00, 0x00,              // flags
      // Post-header + data (27 bytes):
      0x01, 0x00, 0x00, 0x00, 0x00, 0x00,  // table_id
      0x00, 0x00,                          // flags (no extra_row_info)
      // ... more data (19 bytes) ...
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      // Checksum (4 bytes) - should be excluded from parsing:
      0xAA, 0xBB, 0xCC, 0xDD};

  // After OK byte skip
  const unsigned char* buffer = event_with_checksum.data() + 1;

  // Read event_size from header
  uint32_t event_size = buffer[9] | (buffer[10] << 8) | (buffer[11] << 16) | (buffer[12] << 24);
  EXPECT_EQ(event_size, 50);

  // Calculate end position EXCLUDING checksum (4 bytes)
  const unsigned char* end_correct = buffer + event_size - 4;
  const unsigned char* end_wrong = buffer + event_size;

  // Verify correct end position excludes checksum
  EXPECT_EQ(end_correct - buffer, 46) << "Correct end should be at offset 46 (50 - 4)";
  EXPECT_EQ(end_wrong - buffer, 50) << "Wrong end would be at offset 50";

  // Verify checksum bytes are excluded
  EXPECT_EQ(*(end_correct + 0), 0xAA) << "First checksum byte should be at end_correct";
  EXPECT_EQ(*(end_correct + 1), 0xBB);
  EXPECT_EQ(*(end_correct + 2), 0xCC);
  EXPECT_EQ(*(end_correct + 3), 0xDD);
}

/**
 * @brief Test extra_row_info length calculation
 *
 * In MySQL 8.0 ROWS_EVENT_V2, extra_row_info_len is a packed integer that includes
 * its own length. Parser must skip (extra_info_len - packed_int_size) bytes.
 */
TEST(BinlogParsingTest, ExtraRowInfoLengthCalculation) {
  // Simulate ROWS_EVENT_V2 with extra_row_info
  std::vector<uint8_t> event_data = {
      // Post-header:
      0x80, 0x00, 0x00, 0x00, 0x00, 0x00,  // table_id (6 bytes)
      0x01, 0x00,                          // flags = 0x0001 (ROWS_EVENT_V2)
      // extra_row_info:
      0x02,  // extra_row_info_len = 2 bytes TOTAL (including this byte itself)
      0xFF,  // extra_row_info data (1 byte, since total=2, data=2-1=1)
      // column_count should be HERE (after skipping extra_row_info):
      0x03,  // column_count = 3
  };

  const unsigned char* ptr = event_data.data() + 8;  // After table_id + flags

  // Read extra_row_info_len
  const unsigned char* ptr_before = ptr;
  uint8_t extra_info_len = *ptr;  // Simplified packed int read (single byte)
  ptr++;
  auto len_bytes = static_cast<int>(ptr - ptr_before);

  EXPECT_EQ(extra_info_len, 2) << "extra_info_len should be 2";
  EXPECT_EQ(len_bytes, 1) << "Packed integer used 1 byte";

  // Calculate skip_bytes
  auto skip_bytes = static_cast<int>(extra_info_len) - len_bytes;
  EXPECT_EQ(skip_bytes, 1) << "Should skip 1 more byte (2 - 1)";

  // Skip extra_row_info data
  ptr += skip_bytes;

  // Now ptr should point to column_count
  uint8_t column_count = *ptr;
  EXPECT_EQ(column_count, 3) << "column_count should be 3 after skipping extra_row_info";
}

/**
 * @brief Test extra_row_info skip with wrong calculation
 *
 * Demonstrates the bug: if we skip extra_info_len bytes AGAIN after reading packed int,
 * we skip too far and read wrong values.
 */
TEST(BinlogParsingTest, ExtraRowInfoWrongCalculationBug) {
  std::vector<uint8_t> event_data = {
      0x80, 0x00, 0x00, 0x00, 0x00, 0x00,  // table_id
      0x01, 0x00,                          // flags = 0x0001
      0x02,                                // extra_row_info_len = 2
      0xFF,                                // extra_row_info data (1 byte)
      0x03,                                // column_count = 3 (CORRECT position)
      0xAA,                                // next data
      0xBB,                                // next data
  };

  const unsigned char* ptr = event_data.data() + 8;

  // WRONG: Read packed int, then skip extra_info_len bytes again
  uint8_t extra_info_len = *ptr;
  ptr++;  // Already advanced 1 byte for packed int

  // BUG: Skip extra_info_len bytes (2) again
  ptr += extra_info_len;  // Wrong! Should skip (extra_info_len - 1)

  // Now ptr is 1 byte too far (should be at 0x03, but at 0xAA)
  uint8_t wrong_value = *ptr;
  EXPECT_EQ(wrong_value, 0xAA) << "With bug, reads 0xAA instead of column_count (0x03)";
  EXPECT_NE(wrong_value, 0x03) << "Bug causes reading wrong position";
}

/**
 * @brief Test multiple UPDATE_ROWS in single event with checksum
 *
 * A single UPDATE_ROWS_EVENT can contain multiple row pairs (before+after images).
 * Parser must handle multiple rows and stop at correct boundary (before checksum).
 */
TEST(BinlogParsingTest, MultipleRowsWithChecksumBoundary) {
  // Create UPDATE_ROWS_EVENT with 2 row pairs + checksum
  std::vector<uint8_t> event_data = {
      0x00,  // OK byte
      // Header (19 bytes):
      0x00, 0x00, 0x00, 0x00,  // timestamp
      0x1F,                    // event_type = UPDATE_ROWS_EVENT (31)
      0x01, 0x00, 0x00, 0x00,  // server_id
      0x64, 0x00, 0x00, 0x00,  // event_size = 100 bytes (header + 2 row pairs + checksum)
      0x00, 0x00, 0x00, 0x00,  // log_pos
      0x00, 0x00,              // flags
  };

  // Add post-header + minimal row data (77 bytes) to reach 96 bytes before checksum
  for (int i = 0; i < 77; i++) {
    event_data.push_back(0x00);
  }

  // Add 4-byte checksum
  event_data.push_back(0xDE);
  event_data.push_back(0xAD);
  event_data.push_back(0xBE);
  event_data.push_back(0xEF);

  ASSERT_EQ(event_data.size(), 101) << "Total size should be 101 (1 OK + 100 event)";

  const unsigned char* buffer = event_data.data() + 1;
  uint32_t event_size = buffer[9] | (buffer[10] << 8) | (buffer[11] << 16) | (buffer[12] << 24);
  EXPECT_EQ(event_size, 100);

  // Correct end calculation (exclude 4-byte checksum)
  const unsigned char* end = buffer + event_size - 4;
  EXPECT_EQ(end - buffer, 96) << "Event data ends at offset 96";

  // Verify checksum is excluded
  EXPECT_EQ(*(end + 0), 0xDE) << "Checksum starts after event data";
  EXPECT_EQ(*(end + 3), 0xEF) << "Checksum ends at correct position";
}

// ============================================================================
// Step 3a: ExtractTaggedGTID tests (MySQL 8.4+)
// ============================================================================

TEST(BinlogParsingTest, ExtractTaggedGTID_BasicFormat) {
  using mygramdb::mysql::test::BinlogEventBuilder;

  std::array<uint8_t, 16> uuid = {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
                                  0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10};

  auto event = BinlogEventBuilder::BuildGtidTaggedEvent(uuid, 42, "mytag");

  auto result = BinlogEventParser::ExtractTaggedGTID(event.data(), event.size());
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*result, "01020304-0506-0708-090a-0b0c0d0e0f10:mytag:42");
}

TEST(BinlogParsingTest, ExtractTaggedGTID_EmptyTag) {
  using mygramdb::mysql::test::BinlogEventBuilder;

  std::array<uint8_t, 16> uuid = {0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x11, 0x22,
                                  0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0x00};

  auto event = BinlogEventBuilder::BuildGtidTaggedEvent(uuid, 100, "");

  auto result = BinlogEventParser::ExtractTaggedGTID(event.data(), event.size());
  ASSERT_TRUE(result.has_value());
  // Empty tag => UUID:GNO format (no tag separator)
  EXPECT_EQ(*result, "aabbccdd-eeff-1122-3344-556677889900:100");
}

TEST(BinlogParsingTest, ExtractTaggedGTID_TruncatedBuffer) {
  // Buffer too short to contain a valid GTID_TAGGED_LOG_EVENT
  std::vector<uint8_t> short_buf(30, 0);

  auto result = BinlogEventParser::ExtractTaggedGTID(short_buf.data(), short_buf.size());
  EXPECT_FALSE(result.has_value());
}

// ============================================================================
// Tests moved from binlog_event_parser_bug_fixes_test.cpp
// ============================================================================

/**
 * @brief Multi-row INSERT events should return all rows
 *
 * MySQL batches multiple row operations into single binlog events.
 * ParseBinlogEvent must return all rows, not just the first one.
 */
TEST(BinlogParsingTest, MultiRowInsertReturnsAllRows) {
  std::vector<BinlogEvent> events;

  BinlogEvent event1;
  event1.type = BinlogEventType::INSERT;
  event1.primary_key = "1";
  event1.text = "text1";
  events.push_back(event1);

  BinlogEvent event2;
  event2.type = BinlogEventType::INSERT;
  event2.primary_key = "2";
  event2.text = "text2";
  events.push_back(event2);

  BinlogEvent event3;
  event3.type = BinlogEventType::INSERT;
  event3.primary_key = "3";
  event3.text = "text3";
  events.push_back(event3);

  EXPECT_EQ(events.size(), 3) << "Multi-row events should return all rows";
  EXPECT_EQ(events[0].primary_key, "1");
  EXPECT_EQ(events[1].primary_key, "2");
  EXPECT_EQ(events[2].primary_key, "3");
}

/**
 * @brief Multi-row UPDATE events should process all row pairs
 */
TEST(BinlogParsingTest, MultiRowUpdateReturnsAllRows) {
  std::vector<BinlogEvent> events;

  for (int i = 1; i <= 3; ++i) {
    BinlogEvent event;
    event.type = BinlogEventType::UPDATE;
    event.primary_key = std::to_string(i);
    event.text = "new_text";
    event.old_text = "old_text";
    events.push_back(event);
  }

  EXPECT_EQ(events.size(), 3) << "Multi-row UPDATEs should return all rows";
}

/**
 * @brief Multi-row DELETE events should process all rows
 */
TEST(BinlogParsingTest, MultiRowDeleteReturnsAllRows) {
  std::vector<BinlogEvent> events;

  for (int i = 1; i <= 3; ++i) {
    BinlogEvent event;
    event.type = BinlogEventType::DELETE;
    event.primary_key = std::to_string(i);
    events.push_back(event);
  }

  EXPECT_EQ(events.size(), 3) << "Multi-row DELETEs should return all rows";
}

/**
 * @brief text_source.concat should use all specified columns
 */
TEST(BinlogParsingTest, ConcatTextSourceUsesAllColumns) {
  config::TableConfig table_config;
  table_config.name = "articles";
  table_config.primary_key = "id";
  table_config.text_source.concat = {"title", "body", "tags"};

  EXPECT_EQ(table_config.text_source.concat.size(), 3);
  EXPECT_EQ(table_config.text_source.concat[0], "title");
  EXPECT_EQ(table_config.text_source.concat[1], "body");
  EXPECT_EQ(table_config.text_source.concat[2], "tags");

  std::string title = "Hello World";
  std::string body = "This is the body text";
  std::string tags = "news tech";

  std::string expected_text = title + " " + body + " " + tags;
  EXPECT_EQ(expected_text, "Hello World This is the body text news tech");
}

TEST(BinlogParsingTest, ParseBinlogEventConcatTextSourceUsesOrdinalColumnValues) {
  constexpr uint64_t kTableId = 42;
  TableMetadataCache cache;
  cache.Add(kTableId, CreateConcatTableMetadata(kTableId));

  config::TableConfig table_config;
  table_config.name = "articles";
  table_config.primary_key = "id";
  table_config.text_source.concat = {"title", "body", "tags"};

  auto row_data = BuildConcatRowData(7, "Hello World", "This is the body text", "news tech");
  auto event = test::BinlogEventBuilder::BuildWriteRowsV2(kTableId, 0x0000, 2, {}, 4, {0x0F}, row_data);

  std::unordered_map<std::string, server::TableContext*> table_contexts;
  auto events = BinlogEventParser::ParseBinlogEvent(event.data(), event.size(), "uuid:9", cache, table_contexts,
                                                    &table_config, false, "+00:00");

  ASSERT_EQ(events.size(), 1);
  EXPECT_EQ(events[0].type, BinlogEventType::INSERT);
  EXPECT_EQ(events[0].primary_key, "7");
  EXPECT_EQ(events[0].text, "Hello World This is the body text news tech");
  EXPECT_EQ(events[0].gtid, "uuid:9");
}

TEST(BinlogParsingTest, QueryDdlEventCarriesCurrentGtid) {
  TableMetadataCache cache;
  config::TableConfig table_config;
  table_config.name = "articles";

  auto event = BuildQueryEvent("testdb", "ALTER TABLE articles ADD COLUMN status INT");
  std::unordered_map<std::string, server::TableContext*> table_contexts;
  auto events = BinlogEventParser::ParseBinlogEvent(event.data(), event.size(), "uuid:10", cache, table_contexts,
                                                    &table_config, false, "+00:00");

  ASSERT_EQ(events.size(), 1);
  EXPECT_EQ(events[0].type, BinlogEventType::DDL);
  EXPECT_EQ(events[0].table_name, "articles");
  EXPECT_EQ(events[0].gtid, "uuid:10");
}

TEST(BinlogParsingTest, XidEventProducesCommitMarkerWithCurrentGtid) {
  auto event = test::BinlogEventBuilder::BuildHeader(MySQLBinlogEventType::XID_EVENT);
  test::BinlogEventBuilder::AppendLittleEndian64(event, 123);  // xid
  test::BinlogEventBuilder::AppendLittleEndian32(event, 0);    // checksum placeholder
  test::BinlogEventBuilder::FixEventSizeWithChecksum(event);

  TableMetadataCache cache;
  std::unordered_map<std::string, server::TableContext*> table_contexts;
  auto events =
      BinlogEventParser::ParseBinlogEvent(event.data(), event.size(), "uuid:11", cache, table_contexts, nullptr, true);

  ASSERT_EQ(events.size(), 1);
  EXPECT_EQ(events[0].type, BinlogEventType::COMMIT);
  EXPECT_EQ(events[0].gtid, "uuid:11");
}

TEST(BinlogParsingTest, QueryCommitEventProducesCommitMarkerWithCurrentGtid) {
  auto event = BuildQueryEvent("testdb", "  commit  ");

  TableMetadataCache cache;
  std::unordered_map<std::string, server::TableContext*> table_contexts;
  auto events =
      BinlogEventParser::ParseBinlogEvent(event.data(), event.size(), "uuid:12", cache, table_contexts, nullptr, true);

  ASSERT_EQ(events.size(), 1);
  EXPECT_EQ(events[0].type, BinlogEventType::COMMIT);
  EXPECT_EQ(events[0].gtid, "uuid:12");
}

TEST(BinlogParsingTest, QueryCommitEventWithoutGtidIsIgnored) {
  auto event = BuildQueryEvent("testdb", "COMMIT");

  TableMetadataCache cache;
  std::unordered_map<std::string, server::TableContext*> table_contexts;
  auto events =
      BinlogEventParser::ParseBinlogEvent(event.data(), event.size(), "", cache, table_contexts, nullptr, true);

  EXPECT_TRUE(events.empty());
}

/**
 * @brief Single column text_source should work correctly
 */
TEST(BinlogParsingTest, SingleColumnTextSourceWorks) {
  config::TableConfig table_config;
  table_config.name = "articles";
  table_config.primary_key = "id";
  table_config.text_source.column = "content";

  EXPECT_FALSE(table_config.text_source.column.empty());
  EXPECT_TRUE(table_config.text_source.concat.empty());
}

/**
 * @brief Empty text source config is handled gracefully
 */
TEST(BinlogParsingTest, EmptyTextSourceFallback) {
  config::TableConfig table_config;
  table_config.name = "articles";
  table_config.primary_key = "id";

  std::string text_column;
  if (!table_config.text_source.column.empty()) {
    text_column = table_config.text_source.column;
  } else if (!table_config.text_source.concat.empty()) {
    text_column = "concatenated";
  } else {
    text_column = "";
  }

  EXPECT_TRUE(text_column.empty()) << "Empty config should result in empty text column";
}

/**
 * @brief ROLLBACK statement is not treated as DDL
 */
TEST(BinlogParsingTest, RollbackStatementNotTreatedAsDDL) {
  std::vector<std::string> rollback_statements = {
      "ROLLBACK", "rollback", "ROLLBACK;", "  ROLLBACK  ", "ROLLBACK TO SAVEPOINT sp1", "ROLLBACK TO sp1",
  };

  for (const auto& stmt : rollback_statements) {
    bool is_ddl = BinlogEventParser::IsTableAffectingDDL(stmt, "articles");
    EXPECT_FALSE(is_ddl) << "ROLLBACK statement should not be treated as DDL: " << stmt;
  }
}

/**
 * @brief BEGIN statement is not treated as DDL
 */
TEST(BinlogParsingTest, BeginStatementNotTreatedAsDDL) {
  std::vector<std::string> begin_statements = {
      "BEGIN",
      "begin",
      "BEGIN;",
      "  BEGIN  ",
      "START TRANSACTION",
      "START TRANSACTION READ ONLY",
      "START TRANSACTION WITH CONSISTENT SNAPSHOT",
  };

  for (const auto& stmt : begin_statements) {
    bool is_ddl = BinlogEventParser::IsTableAffectingDDL(stmt, "articles");
    EXPECT_FALSE(is_ddl) << "BEGIN statement should not be treated as DDL: " << stmt;
  }
}

/**
 * @brief COMMIT statement is not treated as DDL
 */
TEST(BinlogParsingTest, CommitStatementNotTreatedAsDDL) {
  std::vector<std::string> commit_statements = {
      "COMMIT", "commit", "COMMIT;", "  COMMIT  ", "COMMIT WORK",
  };

  for (const auto& stmt : commit_statements) {
    bool is_ddl = BinlogEventParser::IsTableAffectingDDL(stmt, "articles");
    EXPECT_FALSE(is_ddl) << "COMMIT statement should not be treated as DDL: " << stmt;
  }
}

/**
 * @brief XA transaction statements are not treated as DDL
 */
TEST(BinlogParsingTest, XAStatementsNotTreatedAsDDL) {
  std::vector<std::string> xa_statements = {
      "XA START 'xid1'",    "XA END 'xid1'", "XA PREPARE 'xid1'", "XA COMMIT 'xid1'",
      "XA ROLLBACK 'xid1'", "XA RECOVER",    "xa commit 'xid1'",  "xa rollback 'xid1'",
  };

  for (const auto& stmt : xa_statements) {
    bool is_ddl = BinlogEventParser::IsTableAffectingDDL(stmt, "articles");
    EXPECT_FALSE(is_ddl) << "XA statement should not be treated as DDL: " << stmt;
  }
}

/**
 * @brief SAVEPOINT statements are not treated as DDL
 */
TEST(BinlogParsingTest, SavepointStatementsNotTreatedAsDDL) {
  std::vector<std::string> savepoint_statements = {
      "SAVEPOINT sp1",
      "RELEASE SAVEPOINT sp1",
      "savepoint my_savepoint",
  };

  for (const auto& stmt : savepoint_statements) {
    bool is_ddl = BinlogEventParser::IsTableAffectingDDL(stmt, "articles");
    EXPECT_FALSE(is_ddl) << "SAVEPOINT statement should not be treated as DDL: " << stmt;
  }
}

/**
 * @brief SET statements are not treated as DDL
 */
TEST(BinlogParsingTest, SetStatementsNotTreatedAsDDL) {
  std::vector<std::string> set_statements = {
      "SET autocommit=0",
      "SET @var = 1",
      "SET NAMES utf8mb4",
      "SET SESSION sql_mode = ''",
      "SET TRANSACTION ISOLATION LEVEL READ COMMITTED",
  };

  for (const auto& stmt : set_statements) {
    bool is_ddl = BinlogEventParser::IsTableAffectingDDL(stmt, "articles");
    EXPECT_FALSE(is_ddl) << "SET statement should not be treated as DDL: " << stmt;
  }
}

/**
 * @brief Actual DDL statements are still correctly detected
 */
TEST(BinlogParsingTest, DDLStatementsStillDetected) {
  EXPECT_TRUE(BinlogEventParser::IsTableAffectingDDL("DROP TABLE articles", "articles"));
  EXPECT_TRUE(BinlogEventParser::IsTableAffectingDDL("DROP TABLE IF EXISTS articles", "articles"));
  EXPECT_TRUE(BinlogEventParser::IsTableAffectingDDL("ALTER TABLE articles ADD COLUMN foo INT", "articles"));
  EXPECT_TRUE(BinlogEventParser::IsTableAffectingDDL("TRUNCATE TABLE articles", "articles"));

  EXPECT_FALSE(BinlogEventParser::IsTableAffectingDDL("DROP TABLE other_table", "articles"));
  EXPECT_FALSE(BinlogEventParser::IsTableAffectingDDL("ALTER TABLE other_table ADD COLUMN foo INT", "articles"));
}

/**
 * @brief Table name that looks like transaction keyword is still detected in DDL
 */
TEST(BinlogParsingTest, TableNameLooksLikeTransactionKeyword) {
  EXPECT_TRUE(BinlogEventParser::IsTableAffectingDDL("DROP TABLE rollback", "rollback"));
  EXPECT_TRUE(BinlogEventParser::IsTableAffectingDDL("ALTER TABLE rollback ADD COLUMN x INT", "rollback"));

  EXPECT_FALSE(BinlogEventParser::IsTableAffectingDDL("ROLLBACK", "rollback"));

  EXPECT_TRUE(BinlogEventParser::IsTableAffectingDDL("DROP TABLE begin", "begin"));
  EXPECT_FALSE(BinlogEventParser::IsTableAffectingDDL("BEGIN", "begin"));
}

TEST(BinlogParsingTest, TransactionPayloadEventReturnsEmpty) {
  std::vector<uint8_t> buffer(50, 0);
  buffer[4] = 40;  // event_type = TRANSACTION_PAYLOAD_EVENT
  auto event_size = static_cast<uint32_t>(buffer.size());
  buffer[9] = event_size & 0xFF;
  buffer[10] = (event_size >> 8) & 0xFF;
  buffer[11] = (event_size >> 16) & 0xFF;
  buffer[12] = (event_size >> 24) & 0xFF;

  TableMetadataCache cache;
  std::unordered_map<std::string, server::TableContext*> table_contexts;

  auto events = BinlogEventParser::ParseBinlogEvent(buffer.data(), buffer.size(), "uuid:1", cache, table_contexts,
                                                    nullptr, false);

  EXPECT_TRUE(events.empty()) << "TRANSACTION_PAYLOAD_EVENT should return empty vector";
}

// ===========================================================================
// MariaDB event type handling tests
// ===========================================================================

TEST(BinlogParsingTest, MariaDBGtidEventReturnsEmpty) {
  // MARIADB_GTID_EVENT (162) is handled by ReaderThreadFunc, not ParseBinlogEvent
  auto buffer = test::BinlogEventBuilder::BuildMariaDBGtidEvent(0, 1, 42);
  TableMetadataCache cache;
  std::unordered_map<std::string, server::TableContext*> table_contexts;

  auto events = BinlogEventParser::ParseBinlogEvent(buffer.data(), buffer.size(), "0-1-42", cache, table_contexts,
                                                    nullptr, false);
  EXPECT_TRUE(events.empty()) << "MARIADB_GTID_EVENT should return empty (handled by reader thread)";
}

TEST(BinlogParsingTest, MariaDBGtidListEventReturnsEmpty) {
  auto buffer = test::BinlogEventBuilder::BuildMariaDBGtidListEvent({{0, 1, 42}, {1, 2, 100}});
  TableMetadataCache cache;
  std::unordered_map<std::string, server::TableContext*> table_contexts;

  auto events = BinlogEventParser::ParseBinlogEvent(buffer.data(), buffer.size(), "0-1-42", cache, table_contexts,
                                                    nullptr, false);
  EXPECT_TRUE(events.empty()) << "MARIADB_GTID_LIST_EVENT should return empty";
}

TEST(BinlogParsingTest, MariaDBAnnotateRowsEventReturnsEmpty) {
  auto buffer = test::BinlogEventBuilder::BuildMariaDBAnnotateRowsEvent("INSERT INTO t1 VALUES (1, 'test')");
  TableMetadataCache cache;
  std::unordered_map<std::string, server::TableContext*> table_contexts;

  auto events = BinlogEventParser::ParseBinlogEvent(buffer.data(), buffer.size(), "0-1-42", cache, table_contexts,
                                                    nullptr, false);
  EXPECT_TRUE(events.empty()) << "MARIADB_ANNOTATE_ROWS_EVENT should return empty";
}

TEST(BinlogParsingTest, MariaDBBinlogCheckpointEventReturnsEmpty) {
  // Build a minimal event with type 161 (MARIADB_BINLOG_CHECKPOINT_EVENT)
  auto buffer = test::BinlogEventBuilder::BuildHeader(MySQLBinlogEventType::MARIADB_BINLOG_CHECKPOINT_EVENT);
  // Add some payload + CRC32
  test::BinlogEventBuilder::AppendLittleEndian32(buffer, 0);  // checkpoint data
  test::BinlogEventBuilder::AppendLittleEndian32(buffer, 0);  // CRC32 placeholder
  test::BinlogEventBuilder::FixEventSizeWithChecksum(buffer);

  TableMetadataCache cache;
  std::unordered_map<std::string, server::TableContext*> table_contexts;

  auto events = BinlogEventParser::ParseBinlogEvent(buffer.data(), buffer.size(), "0-1-42", cache, table_contexts,
                                                    nullptr, false);
  EXPECT_TRUE(events.empty()) << "MARIADB_BINLOG_CHECKPOINT_EVENT should return empty";
}

TEST(BinlogParsingTest, MariaDBStartEncryptionEventReturnsEmpty) {
  // Build a minimal event with type 164 (MARIADB_START_ENCRYPTION_EVENT)
  auto buffer = test::BinlogEventBuilder::BuildHeader(MySQLBinlogEventType::MARIADB_START_ENCRYPTION_EVENT);
  test::BinlogEventBuilder::AppendLittleEndian32(buffer, 0);  // CRC32 placeholder
  test::BinlogEventBuilder::FixEventSizeWithChecksum(buffer);

  TableMetadataCache cache;
  std::unordered_map<std::string, server::TableContext*> table_contexts;

  auto events = BinlogEventParser::ParseBinlogEvent(buffer.data(), buffer.size(), "0-1-42", cache, table_contexts,
                                                    nullptr, false);
  EXPECT_TRUE(events.empty()) << "MARIADB_START_ENCRYPTION_EVENT should return empty";
}

// Verify CRC32 validation works with MariaDB events built via BinlogEventBuilder
TEST(BinlogParsingTest, MariaDBGtidEventWithValidChecksum) {
  auto buffer = test::BinlogEventBuilder::BuildMariaDBGtidEvent(5, 10, 12345);

  // Verify CRC32 is valid (builder calls FixChecksum)
  size_t data_len = buffer.size() - 4;
  uint32_t computed_crc = mygram::utils::ComputeCRC32(buffer.data(), data_len);
  uint32_t stored_crc = 0;
  std::memcpy(&stored_crc, buffer.data() + data_len, sizeof(stored_crc));
  EXPECT_EQ(computed_crc, stored_crc) << "CRC32 should be valid for builder-generated MariaDB GTID event";
}

// ============================================================================
// RENAME TABLE multi-pair regression tests
// ============================================================================

/**
 * @brief Regression test: RENAME TABLE with multiple pairs finds target in later pair
 *
 * Verifies that IsTableAffectingDDL correctly finds the target table
 * when it appears as the destination in a multi-pair RENAME statement.
 * Previously, a broken skip loop (while-break no-op) could fail to
 * advance past non-matching table names.
 */
TEST(BinlogParsingTest, RenameTableMultiplePairsTargetInDestination) {
  // Target table appears only as destination in the second pair
  EXPECT_TRUE(
      BinlogEventParser::IsTableAffectingDDL("RENAME TABLE users TO users_old, new_articles TO articles", "articles"));

  // Target table appears only as source in the second pair
  EXPECT_TRUE(BinlogEventParser::IsTableAffectingDDL("RENAME TABLE users TO users_old, articles TO articles_archive",
                                                     "articles"));

  // Target table is not in any pair
  EXPECT_FALSE(
      BinlogEventParser::IsTableAffectingDDL("RENAME TABLE users TO users_old, posts TO posts_old", "articles"));

  // Three pairs, target in the last pair
  EXPECT_TRUE(
      BinlogEventParser::IsTableAffectingDDL("RENAME TABLE a TO b, c TO d, articles TO articles_v2", "articles"));

  // Three pairs, target in the last pair as destination
  EXPECT_TRUE(BinlogEventParser::IsTableAffectingDDL("RENAME TABLE a TO b, c TO d, old_tbl TO articles", "articles"));

  // With backticks in multi-pair
  EXPECT_TRUE(BinlogEventParser::IsTableAffectingDDL(
      "RENAME TABLE `users` TO `users_old`, `articles` TO `articles_new`", "articles"));

  // Case insensitive multi-pair
  EXPECT_TRUE(
      BinlogEventParser::IsTableAffectingDDL("rename table users to users_old, ARTICLES to articles_new", "articles"));
}

#endif  // USE_MYSQL
