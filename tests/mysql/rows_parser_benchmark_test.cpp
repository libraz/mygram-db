/**
 * @file rows_parser_benchmark_test.cpp
 * @brief Cost of decoding row images from a table wider than the configuration.
 *
 * Row images arrive with every column the table has, but only the primary key,
 * the text source and the configured filter columns are ever read back. This
 * measures decoding the same event with and without the retained-column mask,
 * so the cost of the columns nothing reads is visible directly. Timings are
 * reported; the assertions cover the decoded values agreeing.
 */

#include <gtest/gtest.h>

#include <chrono>
#include <iomanip>
#include <iostream>
#include <string>
#include <vector>

#include "binlog_event_builder.h"
#include "mysql/rows_parser.h"
#include "mysql/table_metadata.h"

#ifdef USE_MYSQL

namespace mygramdb::mysql {
namespace {

template <typename Body>
double TimeMs(Body&& body) {
  const auto start = std::chrono::steady_clock::now();
  body();
  const auto end = std::chrono::steady_clock::now();
  return std::chrono::duration<double, std::milli>(end - start).count();
}

constexpr size_t kUnreferencedColumns = 27;
constexpr size_t kPayloadBytes = 200;
constexpr size_t kRowsPerEvent = 2000;
constexpr size_t kParses = 20;

/// A table whose configured columns are a small minority of its width.
TableMetadata WideTableMetadata() {
  TableMetadata metadata;
  metadata.table_id = 4200;
  metadata.database_name = "test_db";
  metadata.table_name = "wide_rows";

  const auto add_column = [&](ColumnType type, const std::string& name, uint16_t column_metadata) {
    ColumnMetadata column;
    column.type = type;
    column.name = name;
    column.metadata = column_metadata;
    metadata.columns.push_back(column);
  };

  add_column(ColumnType::LONG, "id", 0);
  add_column(ColumnType::VARCHAR, "body", 255);
  add_column(ColumnType::LONG, "status", 0);
  for (size_t i = 0; i < kUnreferencedColumns; ++i) {
    add_column(ColumnType::VARCHAR, "payload" + std::to_string(i), 255);
  }
  return metadata;
}

config::TableConfig WideTableConfig() {
  config::TableConfig table_config;
  table_config.name = "wide_rows";
  table_config.primary_key = "id";
  table_config.text_source.column = "body";
  config::FilterConfig status_filter;
  status_filter.name = "status";
  table_config.filters.push_back(status_filter);
  return table_config;
}

void AppendLong(std::vector<uint8_t>& row_data, int32_t value) {
  for (int i = 0; i < 4; ++i) {
    row_data.push_back(static_cast<uint8_t>((value >> (i * 8)) & 0xFF));
  }
}

/// VARCHAR with metadata <= 255 carries a single length byte.
void AppendShortVarchar(std::vector<uint8_t>& row_data, const std::string& value) {
  row_data.push_back(static_cast<uint8_t>(value.size()));
  row_data.insert(row_data.end(), value.begin(), value.end());
}

std::vector<uint8_t> BuildWideWriteRowsEvent(const TableMetadata& metadata) {
  const size_t column_count = metadata.columns.size();
  const size_t bitmap_bytes = (column_count + 7) / 8;
  const std::vector<uint8_t> columns_bitmap(bitmap_bytes, 0xFF);
  const std::string payload(kPayloadBytes, 'x');

  std::vector<uint8_t> row_data;
  for (size_t row = 0; row < kRowsPerEvent; ++row) {
    row_data.insert(row_data.end(), bitmap_bytes, 0x00);  // No NULLs
    AppendLong(row_data, static_cast<int32_t>(row + 1));
    AppendShortVarchar(row_data, "searchable body " + std::to_string(row));
    AppendLong(row_data, static_cast<int32_t>(row % 3));
    for (size_t i = 0; i < kUnreferencedColumns; ++i) {
      AppendShortVarchar(row_data, payload);
    }
  }

  return test::BinlogEventBuilder::BuildWriteRowsV1(metadata.table_id, 0, column_count, columns_bitmap, row_data);
}

class RowsParserBenchmarkTest : public ::testing::Test {};

/**
 * @brief Decoding cost of a wide table, with and without the retained mask.
 *
 * Every unreferenced column decoded is a string allocated, copied and then
 * dropped, on the replication thread, once per row. Skipping them by advancing
 * over the field instead is what keeps decode cost proportional to the
 * configuration rather than to the table definition.
 */
TEST_F(RowsParserBenchmarkTest, DecodeCostOfColumnsNothingReads) {
  const TableMetadata metadata = WideTableMetadata();
  const auto buffer = BuildWideWriteRowsEvent(metadata);
  const auto retained = BuildRetainedColumns(metadata, WideTableConfig());

  const auto parse = [&](const RetainedColumns* mask) {
    return ParseWriteRowsEvent(buffer.data(), buffer.size(), &metadata, "id", "body",
                               MySQLBinlogEventType::OBSOLETE_WRITE_ROWS_EVENT_V1, mask);
  };

  auto full = parse(nullptr);
  ASSERT_TRUE(full.has_value()) << "the fixture event did not parse";
  ASSERT_EQ(full->size(), kRowsPerEvent);
  auto retained_rows = parse(&retained);
  ASSERT_TRUE(retained_rows.has_value());
  ASSERT_EQ(retained_rows->size(), kRowsPerEvent);

  // The columns the configuration names decode identically either way, and the
  // rest are reported as absent rather than as empty values.
  for (size_t row = 0; row < kRowsPerEvent; ++row) {
    ASSERT_EQ((*retained_rows)[row].primary_key, (*full)[row].primary_key) << "row " << row;
    ASSERT_EQ((*retained_rows)[row].text, (*full)[row].text) << "row " << row;
    ASSERT_EQ((*retained_rows)[row].GetColumnValue("status"), (*full)[row].GetColumnValue("status")) << "row " << row;
  }
  EXPECT_EQ((*retained_rows)[0].FindColumnValue("payload0"), nullptr)
      << "an unreferenced column was decoded despite the retained mask";
  EXPECT_NE((*full)[0].FindColumnValue("payload0"), nullptr) << "the unmasked parse stopped decoding every column";

  const double full_ms = TimeMs([&] {
    for (size_t i = 0; i < kParses; ++i) {
      auto result = parse(nullptr);
      ASSERT_TRUE(result.has_value());
    }
  });
  const double retained_ms = TimeMs([&] {
    for (size_t i = 0; i < kParses; ++i) {
      auto result = parse(&retained);
      ASSERT_TRUE(result.has_value());
    }
  });

  std::cout << "\nRow decode: " << kRowsPerEvent << " rows x " << metadata.columns.size() << " columns, "
            << (metadata.columns.size() - kUnreferencedColumns) << " configured\n";
  std::cout << "  " << std::left << std::setw(40) << "ms per event (all columns -> retained)" << std::right
            << std::fixed << std::setprecision(2) << std::setw(10) << (full_ms / kParses) << "  ->" << std::setw(10)
            << (retained_ms / kParses) << std::endl;

  EXPECT_LT(retained_ms, full_ms) << "skipping the unreferenced columns did not reduce decode cost";
}

}  // namespace
}  // namespace mygramdb::mysql

#endif  // USE_MYSQL
