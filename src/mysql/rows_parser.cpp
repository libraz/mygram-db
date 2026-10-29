/**
 * @file rows_parser.cpp
 * @brief Parser for MySQL ROWS events (WRITE/UPDATE/DELETE)
 *
 * libmybinlog decodes the row images; this file owns what replication requires
 * of them and what RowData publishes. Before decoding, the event prologue is
 * checked here: both column bitmaps must select every column, because a
 * configured column missing from an image cannot be told apart from one that
 * changed (binlog_row_image=FULL is required). Only the retained columns are
 * materialized; the rest are sized and skipped. A column type the support
 * table marks undecodable fails the event when it is retained or carries a
 * value. Decoded values are rendered by CanonicalizeBinlogValue(), which keeps
 * them byte-identical to the snapshot path.
 */

#include "mysql/rows_parser.h"

#include <optional>
#include <utility>

#include "mybinlog/row_decoder.h"
#include "mybinlog/table_map.h"
#include "mysql/column_type_support.h"
#include "mysql/rows_parser_internal.h"
#include "mysql/value_canonicalizer.h"
#include "utils/constants.h"
#include "utils/structured_log.h"

#ifdef USE_MYSQL

// NOLINTBEGIN(cppcoreguidelines-pro-*,cppcoreguidelines-avoid-*,readability-magic-numbers,readability-function-cognitive-complexity,readability-else-after-return)

namespace mygramdb::mysql {

namespace {

using mygram::utils::Error;
using mygram::utils::ErrorCode;
using mygram::utils::Expected;
using mygram::utils::MakeError;
using mygram::utils::MakeUnexpected;

/// Ceiling on one decoded value, and on the expansion of one MariaDB COMPRESSED value.
constexpr size_t kMaxFieldBytes = 256 * 1024 * 1024;

bool IsV2Event(mybinlog::EventType event_type) {
  return event_type == mybinlog::EventType::kWriteRowsEvent || event_type == mybinlog::EventType::kUpdateRowsEvent ||
         event_type == mybinlog::EventType::kDeleteRowsEvent;
}

uint16_t ReadLe16(const unsigned char* data) {
  return static_cast<uint16_t>(data[0] | (data[1] << 8));
}

uint32_t ReadLe32(const unsigned char* data) {
  return static_cast<uint32_t>(data[0]) | (static_cast<uint32_t>(data[1]) << 8) |
         (static_cast<uint32_t>(data[2]) << 16) | (static_cast<uint32_t>(data[3]) << 24);
}

size_t BitmapBytes(uint64_t bits) {
  return static_cast<size_t>((bits + 7) / 8);
}

bool AllColumnsPresent(const unsigned char* columns_present, uint64_t column_count) {
  for (uint64_t col_idx = 0; col_idx < column_count; ++col_idx) {
    if ((columns_present[col_idx / 8] & (1U << (col_idx % 8))) == 0) {
      return false;
    }
  }
  return true;
}

enum class PackedIntRead : uint8_t { kOk, kTruncated, kReserved };

/// A length-encoded integer; the reserved first bytes 0xFB and 0xFF are not one.
PackedIntRead ReadPackedInteger(const unsigned char** ptr, const unsigned char* end, uint64_t* value) {
  const unsigned char* cursor = *ptr;
  if (cursor >= end) {
    return PackedIntRead::kTruncated;
  }
  const unsigned char first = *cursor++;
  size_t width = 0;
  if (first < 0xFB) {
    *value = first;
    *ptr = cursor;
    return PackedIntRead::kOk;
  }
  if (first == 0xFC) {
    width = 2;
  } else if (first == 0xFD) {
    width = 3;
  } else if (first == 0xFE) {
    width = 8;
  } else {
    return PackedIntRead::kReserved;
  }
  if (static_cast<size_t>(end - cursor) < width) {
    return PackedIntRead::kTruncated;
  }
  uint64_t result = 0;
  for (size_t index = 0; index < width; ++index) {
    result |= static_cast<uint64_t>(cursor[index]) << (8 * index);
  }
  *value = result;
  *ptr = cursor + width;
  return PackedIntRead::kOk;
}

Error TooShort(const char* event_type_label, const char* section) {
  mygram::utils::StructuredLog()
      .Event("mysql_binlog_error")
      .Field("type", std::string(event_type_label) + "_too_short")
      .Field("section", section)
      .Error();
  return MakeError(ErrorCode::kMySQLBinlogError, std::string(event_type_label) + " event too short for " + section);
}

/// Where a ROWS event's body lies, once its prologue has been checked.
struct RowsEventLayout {
  mybinlog::EventHeader header;
  const uint8_t* body = nullptr;  ///< Bytes after the common header
  size_t body_size = 0;           ///< Body length, checksum trailer excluded
  bool has_rows = false;          ///< Bytes remain after the column bitmaps
  int pk_col_idx = -1;            ///< Ordinal of the primary key column, or -1
  int text_col_idx = -1;          ///< Ordinal of the text column, or -1
};

/**
 * @brief Check a ROWS event's prologue: sizes, column count and FULL column bitmaps.
 *
 * @param update Whether the event carries the after-image bitmap of an UPDATE
 * @param event_type_label Short label for log messages (e.g. "write_rows")
 */
Expected<RowsEventLayout, Error> ReadRowsEventLayout(const unsigned char* buffer, unsigned long length,
                                                     mybinlog::EventType event_type,
                                                     const TableMetadata* table_metadata,
                                                     const std::string& pk_column_name,
                                                     const std::string& text_column_name, bool update,
                                                     const char* event_type_label) {
  // The common header is read before anything else, so the whole of it has to
  // be present first. Checking the declared event size alone is not enough:
  // that field itself lives inside the header, four bytes in from offset 9.
  if ((buffer == nullptr) || length < mygram::constants::kBinlogEventHeaderLen) {
    return MakeUnexpected(MakeError(ErrorCode::kMySQLBinlogError,
                                    std::string(event_type_label) + " event shorter than the binlog event header"));
  }

  const uint32_t event_size = ReadLe32(buffer + 9);
  if (event_size < 4 || event_size > length) {
    return MakeUnexpected(
        MakeError(ErrorCode::kMySQLBinlogError, std::string(event_type_label) + " event: invalid event_size"));
  }

  const unsigned char* body = buffer + mygram::constants::kBinlogEventHeaderLen;
  // The event size counts the 4-byte CRC32 trailer, which the reader verified before dispatch.
  const unsigned char* end = buffer + event_size - mygram::constants::kBinlogChecksumSize;
  const unsigned char* ptr = body;

  // Post-header: table_id (6 bytes) + flags (2 bytes)
  if (ptr + 8 > end) {
    return MakeUnexpected(TooShort(event_type_label, "post-header"));
  }
  ptr += 8;

  // V2 rows events (type >= 30) carry a var_header_len that counts itself.
  if (IsV2Event(event_type)) {
    if (ptr + 2 > end) {
      return MakeUnexpected(TooShort(event_type_label, "var_header_len"));
    }
    const uint16_t var_header_len = ReadLe16(ptr);
    if (var_header_len < 2 || ptr + var_header_len > end) {
      mygram::utils::StructuredLog()
          .Event("mysql_binlog_error")
          .Field("type", "invalid_var_header_len")
          .Field("event_type", event_type_label)
          .Field("var_header_len", static_cast<uint64_t>(var_header_len))
          .Error();
      return MakeUnexpected(
          MakeError(ErrorCode::kMySQLBinlogError, std::string(event_type_label) + " event: invalid var_header_len"));
    }
    ptr += var_header_len;
  }

  uint64_t column_count = 0;
  switch (ReadPackedInteger(&ptr, end, &column_count)) {
    case PackedIntRead::kOk:
      break;
    case PackedIntRead::kTruncated:
      return MakeUnexpected(TooShort(event_type_label, "column_count"));
    case PackedIntRead::kReserved:
      return MakeUnexpected(MakeError(ErrorCode::kMySQLInvalidMetadata,
                                      std::string(event_type_label) + " event: column_count is not a packed integer"));
  }
  if (column_count == 0) {
    return MakeUnexpected(
        MakeError(ErrorCode::kMySQLInvalidMetadata, std::string(event_type_label) + " event: column_count is 0"));
  }

  if (column_count != table_metadata->columns.size()) {
    mygram::utils::StructuredLog()
        .Event("mysql_binlog_error")
        .Field("type", "column_count_mismatch")
        .Field("event_type", event_type_label)
        .Field("event_columns", column_count)
        .Field("table_columns", static_cast<uint64_t>(table_metadata->columns.size()))
        .Error();
    return MakeUnexpected(
        MakeError(ErrorCode::kMySQLBinlogError, std::string(event_type_label) + " event: column_count mismatch"));
  }

  // columns_present bitmap (before image for UPDATE, the only image for WRITE/DELETE)
  const size_t bitmap_size = BitmapBytes(column_count);
  if (ptr + bitmap_size > end) {
    return MakeUnexpected(TooShort(event_type_label, "columns_present bitmap"));
  }
  if (!AllColumnsPresent(ptr, column_count)) {
    mygram::utils::StructuredLog()
        .Event("mysql_binlog_error")
        .Field("type", "partial_columns_present_bitmap")
        .Field("event_type", event_type_label)
        .Field("required", "binlog_row_image=FULL")
        .Error();
    return MakeUnexpected(MakeError(
        ErrorCode::kMySQLBinlogError,
        std::string(event_type_label) + " event: partial columns_present bitmap requires binlog_row_image=FULL"));
  }
  ptr += bitmap_size;

  if (update) {
    if (ptr + bitmap_size > end) {
      mygram::utils::StructuredLog()
          .Event("mysql_binlog_error")
          .Field("type", "update_rows_too_short")
          .Field("section", "columns_after_image bitmap")
          .Error();
      return MakeUnexpected(
          MakeError(ErrorCode::kMySQLBinlogError, "UPDATE_ROWS event too short for columns_after_image bitmap"));
    }
    if (!AllColumnsPresent(ptr, column_count)) {
      mygram::utils::StructuredLog()
          .Event("mysql_binlog_error")
          .Field("type", "partial_columns_after_image_bitmap")
          .Field("event_type", "update_rows")
          .Field("required", "binlog_row_image=FULL")
          .Error();
      return MakeUnexpected(MakeError(ErrorCode::kMySQLBinlogError,
                                      "UPDATE_ROWS event: partial columns_after_image bitmap requires "
                                      "binlog_row_image=FULL"));
    }
    ptr += bitmap_size;
  }

  RowsEventLayout layout;
  layout.header.type_code = static_cast<uint8_t>(event_type);
  layout.header.event_length = event_size;
  layout.body = body;
  layout.body_size = static_cast<size_t>(end - body);
  layout.has_rows = ptr < end;
  if (const auto ordinal = table_metadata->FindColumnOrdinal(pk_column_name); ordinal.has_value()) {
    layout.pk_col_idx = static_cast<int>(*ordinal);
  }
  if (const auto ordinal = table_metadata->FindColumnOrdinal(text_column_name); ordinal.has_value()) {
    layout.text_col_idx = static_cast<int>(*ordinal);
  }
  return layout;
}

/// What a row image's column contributes to RowData.
enum class ColumnUse : uint8_t {
  kSkip,    ///< Nothing reads it: sized and skipped
  kRetain,  ///< Decoded and published
  kProbe,   ///< Undecodable and not retained: decoded only to see whether it holds a value
};

/// libmybinlog's view of a table, and what each column of a row image is decoded for.
struct DecodePlan {
  mybinlog::TableMap table;
  mybinlog::DecodeOptions options;
  std::vector<ColumnUse> uses;
  bool refuses_retained_column = false;  ///< A retained column has a type the support table cannot decode
  size_t refused_column = 0;             ///< The first such column
};

bool IsDecodable(ColumnType type) {
  return DescribeColumnType(type).binlog_decoding == BinlogDecoding::kDecoded;
}

Error UnsupportedTypeError(ColumnType type) {
  return MakeError(ErrorCode::kMySQLUnsupportedType,
                   "Unsupported column type: " + std::to_string(static_cast<unsigned>(type)));
}

mybinlog::TableMapColumn ToTableMapColumn(const ColumnMetadata& column) {
  mybinlog::TableMapColumn mapped;
  mapped.type = column.type;
  mapped.meta = column.metadata;
  mapped.nullable = column.is_nullable;
  mapped.is_unsigned = column.is_unsigned;
  return mapped;
}

DecodePlan BuildDecodePlan(const TableMetadata& meta, const RetainedColumns* retained_columns, int pk_col_idx,
                           int text_col_idx) {
  DecodePlan plan;
  const size_t column_count = meta.columns.size();
  plan.table.table_id = meta.table_id;
  plan.table.has_signedness = true;
  plan.table.columns.reserve(column_count);
  plan.uses.resize(column_count);
  plan.options.max_value_bytes = kMaxFieldBytes;
  plan.options.max_decompressed_bytes = kMaxFieldBytes;

  bool selects_every_column = true;
  for (size_t col_idx = 0; col_idx < column_count; ++col_idx) {
    const auto& column = meta.columns[col_idx];
    plan.table.columns.push_back(ToTableMapColumn(column));
    // Nothing reads a column that is neither the key, a text source nor a
    // filter column, so decoding it would allocate and copy a potentially
    // multi-megabyte value for no consumer.
    const bool retain = static_cast<int>(col_idx) == text_col_idx || static_cast<int>(col_idx) == pk_col_idx ||
                        retained_columns == nullptr || retained_columns->Retains(col_idx);
    if (IsDecodable(column.type)) {
      plan.uses[col_idx] = retain ? ColumnUse::kRetain : ColumnUse::kSkip;
    } else if (retain) {
      plan.uses[col_idx] = ColumnUse::kRetain;
      if (!plan.refuses_retained_column) {
        plan.refuses_retained_column = true;
        plan.refused_column = col_idx;
      }
    } else {
      plan.uses[col_idx] = ColumnUse::kProbe;
    }
    selects_every_column = selects_every_column && plan.uses[col_idx] != ColumnUse::kSkip;
  }

  if (!selects_every_column) {
    plan.options.columns.resize(column_count);
    for (size_t col_idx = 0; col_idx < column_count; ++col_idx) {
      plan.options.columns[col_idx] = plan.uses[col_idx] != ColumnUse::kSkip;
    }
  }
  return plan;
}

Error ColumnError(const Error& cause, const char* event_type_label, const char* image_label, size_t col_idx,
                  const std::string& column_name) {
  mygram::utils::StructuredLog()
      .Event("mysql_binlog_error")
      .Field("type", "field_decode_error")
      .Field("event_type", event_type_label)
      .Field("image", image_label)
      .Field("column_index", static_cast<uint64_t>(col_idx))
      .Field("column_name", column_name)
      .Field("error", cause.message())
      .Error();
  return cause;
}

/// RowData for one decoded row image, whose values are moved from.
Expected<RowData, Error> ToRowData(mybinlog::Row& image, const TableMetadata& meta, const DecodePlan& plan,
                                   int pk_col_idx, int text_col_idx, const char* event_type_label,
                                   const char* image_label) {
  RowData row;
  row.table_metadata = &meta;
  row.column_values.resize(meta.columns.size());
  row.column_values_present.assign(meta.columns.size(), false);
  row.column_values_null.assign(meta.columns.size(), false);

  for (size_t col_idx = 0; col_idx < image.size(); ++col_idx) {
    const ColumnUse use = plan.uses[col_idx];
    if (use == ColumnUse::kSkip) {
      continue;
    }
    const auto& col_meta = meta.columns[col_idx];
    mybinlog::Value& value = image[col_idx];
    const bool is_null = value.kind == mybinlog::Value::Kind::kNull;
    if (use == ColumnUse::kProbe) {
      if (!is_null) {
        mygram::utils::StructuredLog()
            .Event("mysql_binlog_warning")
            .Field("type", "unsupported_column_type")
            .Field("event_type", event_type_label)
            .Field("column_type", static_cast<int64_t>(col_meta.type))
            .Field("column_name", col_meta.name)
            .Warn();
        std::string msg = std::string("Unsupported column type in ") + event_type_label + " event";
        if (image_label[0] != '\0') {
          msg += std::string(" ") + image_label + " image";
        }
        return MakeUnexpected(MakeError(ErrorCode::kMySQLUnsupportedType, msg, col_meta.name));
      }
      continue;
    }

    auto text = CanonicalizeBinlogValue(std::move(value), &col_meta.enum_set_values);
    if (!text) {
      return MakeUnexpected(ColumnError(text.error(), event_type_label, image_label, col_idx, col_meta.name));
    }

    const bool is_text_column = static_cast<int>(col_idx) == text_col_idx;
    if (is_text_column) {
      // Keep the decoded text in exactly one owning string. FindColumnValue()
      // routes this ordinal back to row.text, so filters and concatenated text
      // sources still observe the column without duplicating a potentially
      // multi-megabyte value in column_values.
      row.text = std::move(*text);
      row.text_column_index = col_idx;
    } else {
      row.column_values[col_idx] = std::move(*text);
    }
    row.column_values_present[col_idx] = true;
    row.column_values_null[col_idx] = is_null;
    if (is_null) {
      row.null_columns.insert(col_meta.name);
    }

    if (static_cast<int>(col_idx) == pk_col_idx) {
      row.primary_key = is_text_column ? row.text : row.column_values[col_idx];
      row.primary_key_present = true;
      row.primary_key_null = is_null;
      if (mygram::utils::IsDebugLogEnabled()) {
        mygram::utils::StructuredLog()
            .Event("binlog_debug")
            .Field("action", "set_pk")
            .Field("value", row.primary_key)
            .Debug();
      }
    }
  }
  return row;
}

/// Receives the rows libmybinlog decodes, as RowData.
struct RowSink {
  const TableMetadata* meta = nullptr;
  const DecodePlan* plan = nullptr;
  int pk_col_idx = -1;
  int text_col_idx = -1;
  const char* event_type_label = "";
  std::vector<RowData>* rows = nullptr;                           ///< WRITE and DELETE
  std::vector<std::pair<RowData, RowData>>* row_pairs = nullptr;  ///< UPDATE
  std::optional<Error> error;  ///< Set when a decoded row was refused here rather than by the decoder
};

mybinlog::Status CollectRow(void* ctx, mybinlog::Row* row, mybinlog::Row* after) {
  auto* sink = static_cast<RowSink*>(ctx);
  const char* first_image = after == nullptr ? "" : "before";
  auto first = ToRowData(*row, *sink->meta, *sink->plan, sink->pk_col_idx, sink->text_col_idx, sink->event_type_label,
                         first_image);
  if (!first) {
    sink->error = first.error();
    // Any status other than kOk stops the decode; the error above is what is reported.
    return mybinlog::Status::kUnsupported;
  }
  if (after == nullptr) {
    sink->rows->push_back(std::move(*first));
    return mybinlog::Status::kOk;
  }
  auto second = ToRowData(*after, *sink->meta, *sink->plan, sink->pk_col_idx, sink->text_col_idx,
                          sink->event_type_label, "after");
  if (!second) {
    sink->error = second.error();
    return mybinlog::Status::kUnsupported;
  }
  sink->row_pairs->emplace_back(std::move(*first), std::move(*second));
  return mybinlog::Status::kOk;
}

/**
 * @brief Decode every row of a checked ROWS event into @p rows or @p row_pairs.
 *
 * @param event_type_label Upper-case label for messages (e.g. "WRITE_ROWS")
 */
Expected<void, Error> DecodeRows(const RowsEventLayout& layout, const TableMetadata& meta,
                                 const RetainedColumns* retained_columns, const char* event_type_label,
                                 std::vector<RowData>* rows, std::vector<std::pair<RowData, RowData>>* row_pairs) {
  const DecodePlan plan = BuildDecodePlan(meta, retained_columns, layout.pk_col_idx, layout.text_col_idx);
  if (layout.has_rows && plan.refuses_retained_column) {
    const auto& column = meta.columns[plan.refused_column];
    return MakeUnexpected(
        ColumnError(UnsupportedTypeError(column.type), event_type_label, "", plan.refused_column, column.name));
  }

  RowSink sink;
  sink.meta = &meta;
  sink.plan = &plan;
  sink.pk_col_idx = layout.pk_col_idx;
  sink.text_col_idx = layout.text_col_idx;
  sink.event_type_label = event_type_label;
  sink.rows = rows;
  sink.row_pairs = row_pairs;

  mybinlog::RowScratch scratch;
  const mybinlog::Status status = mybinlog::VisitRowsEvent(layout.header, layout.body, layout.body_size, plan.table,
                                                           plan.options, &scratch, &CollectRow, &sink);
  if (sink.error.has_value()) {
    return MakeUnexpected(*sink.error);
  }
  if (status != mybinlog::Status::kOk) {
    mygram::utils::StructuredLog()
        .Event("mysql_binlog_error")
        .Field("type", "rows_decode_error")
        .Field("event_type", event_type_label)
        .Field("status", mybinlog::StatusName(status))
        .Error();
    return MakeUnexpected(MakeError(RowDecodeErrorCode(status), std::string(event_type_label) +
                                                                    " event: row data could not be decoded (" +
                                                                    mybinlog::StatusName(status) + ")"));
  }
  return {};
}

size_t EstimatedRows(const RowsEventLayout& layout, size_t bytes_per_row) {
  const size_t estimated = layout.body_size / bytes_per_row;
  return estimated < mygram::constants::kMaxPreReserveRows ? estimated : 0;
}

}  // namespace

// ---------------------------------------------------------------------------
// Retained columns and single values
// ---------------------------------------------------------------------------

RetainedColumns BuildRetainedColumns(const TableMetadata& metadata, const config::TableConfig& table_config) {
  RetainedColumns retained;
  retained.by_ordinal.assign(metadata.columns.size(), false);

  auto retain = [&](const std::string& column_name) {
    if (column_name.empty()) {
      return;
    }
    const auto ordinal = metadata.FindColumnOrdinal(column_name);
    if (ordinal.has_value() && *ordinal < retained.by_ordinal.size()) {
      retained.by_ordinal[*ordinal] = true;
    }
  };

  retain(table_config.primary_key);
  retain(table_config.text_source.column);
  for (const auto& column : table_config.text_source.concat) {
    retain(column);
  }
  for (const auto& filter : config::BuildUnifiedFilterConfigs(table_config)) {
    retain(filter.name);
  }
  return retained;
}

namespace {

/// Status CaptureValue() stops the decode with; a rows decode never returns it itself.
constexpr mybinlog::Status kValueCaptured = mybinlog::Status::kNeedMoreData;

/// Takes the first row's value and stops: the bytes after it are not part of the value.
mybinlog::Status CaptureValue(void* ctx, mybinlog::Row* row, mybinlog::Row* /*after*/) {
  *static_cast<mybinlog::Value*>(ctx) = std::move(row->front());
  return kValueCaptured;
}

}  // namespace

Expected<std::string, Error> internal::DecodeColumnValue(uint8_t col_type, const unsigned char* data, uint16_t metadata,
                                                         bool is_null, const unsigned char* end, bool is_unsigned,
                                                         const std::vector<std::string>* enum_set_values) {
  ColumnMetadata column;
  column.type = static_cast<ColumnType>(col_type);
  column.metadata = metadata;
  column.is_unsigned = is_unsigned;
  if (!IsDecodable(column.type)) {
    return MakeUnexpected(UnsupportedTypeError(column.type));
  }

  mybinlog::TableMap table;
  table.has_signedness = true;
  table.columns.push_back(ToTableMapColumn(column));

  // A WRITE_ROWS v1 body holding one row of one column: table id and flags,
  // the column count, the columns-present bitmap, the NULL bitmap, the value.
  std::vector<uint8_t> body(8, 0);
  body.push_back(1);
  body.push_back(0x01);
  body.push_back(is_null ? 0x01 : 0x00);
  if (!is_null && data != nullptr && end > data) {
    body.insert(body.end(), data, end);
  }
  mybinlog::EventHeader header;
  header.type_code = static_cast<uint8_t>(mybinlog::EventType::kWriteRowsEventV1);

  mybinlog::DecodeOptions options;
  options.max_value_bytes = kMaxFieldBytes;
  options.max_decompressed_bytes = kMaxFieldBytes;
  mybinlog::RowScratch scratch;
  mybinlog::Value value;
  const auto status =
      mybinlog::VisitRowsEvent(header, body.data(), body.size(), table, options, &scratch, &CaptureValue, &value);
  if (status != kValueCaptured) {
    return MakeUnexpected(MakeError(RowDecodeErrorCode(status), std::string("Column value could not be decoded (") +
                                                                    mybinlog::StatusName(status) + ")"));
  }
  return CanonicalizeBinlogValue(std::move(value), enum_set_values);
}

// ---------------------------------------------------------------------------
// ParseWriteRowsEvent
// ---------------------------------------------------------------------------

Expected<std::vector<RowData>, Error> ParseWriteRowsEvent(const unsigned char* buffer, unsigned long length,
                                                          const TableMetadata* table_metadata,
                                                          const std::string& pk_column_name,
                                                          const std::string& text_column_name,
                                                          mybinlog::EventType event_type,
                                                          const RetainedColumns* retained_columns) {
  if ((buffer == nullptr) || (table_metadata == nullptr)) {
    return MakeUnexpected(
        MakeError(ErrorCode::kMySQLBinlogError, "Null buffer or table_metadata in ParseWriteRowsEvent"));
  }

  try {
    auto layout = ReadRowsEventLayout(buffer, length, event_type, table_metadata, pk_column_name, text_column_name,
                                      /*update=*/false, "write_rows");
    if (!layout) {
      return MakeUnexpected(layout.error());
    }

    std::vector<RowData> rows;
    rows.reserve(EstimatedRows(*layout, mygram::constants::kEstimatedBytesPerBinlogRow));
    if (auto decoded = DecodeRows(*layout, *table_metadata, retained_columns, "WRITE_ROWS", &rows, nullptr); !decoded) {
      return MakeUnexpected(decoded.error());
    }

    mygram::utils::StructuredLog()
        .Event("binlog_debug")
        .Field("action", "parsed_write_rows")
        .Field("rows", static_cast<uint64_t>(rows.size()))
        .Field("database", table_metadata->database_name)
        .Field("table", table_metadata->table_name)
        .Debug();

    return rows;

  } catch (const std::exception& e) {
    mygram::utils::StructuredLog()
        .Event("mysql_binlog_error")
        .Field("type", "parse_exception")
        .Field("event_type", "write_rows")
        .Field("error", e.what())
        .Error();
    return MakeUnexpected(
        MakeError(ErrorCode::kMySQLBinlogError, std::string("Exception parsing WRITE_ROWS event: ") + e.what()));
  }
}

// ---------------------------------------------------------------------------
// ParseUpdateRowsEvent
// ---------------------------------------------------------------------------

Expected<std::vector<std::pair<RowData, RowData>>, Error> ParseUpdateRowsEvent(
    const unsigned char* buffer, unsigned long length, const TableMetadata* table_metadata,
    const std::string& pk_column_name, const std::string& text_column_name, mybinlog::EventType event_type,
    const RetainedColumns* retained_columns) {
  if ((buffer == nullptr) || (table_metadata == nullptr)) {
    return MakeUnexpected(
        MakeError(ErrorCode::kMySQLBinlogError, "Null buffer or table_metadata in ParseUpdateRowsEvent"));
  }

  try {
    auto layout = ReadRowsEventLayout(buffer, length, event_type, table_metadata, pk_column_name, text_column_name,
                                      /*update=*/true, "update_rows");
    if (!layout) {
      return MakeUnexpected(layout.error());
    }

    std::vector<std::pair<RowData, RowData>> row_pairs;
    row_pairs.reserve(EstimatedRows(*layout, 2 * mygram::constants::kEstimatedBytesPerBinlogRow));
    if (auto decoded = DecodeRows(*layout, *table_metadata, retained_columns, "UPDATE_ROWS", nullptr, &row_pairs);
        !decoded) {
      return MakeUnexpected(decoded.error());
    }

    mygram::utils::StructuredLog()
        .Event("binlog_debug")
        .Field("action", "parsed_update_rows")
        .Field("row_pairs", static_cast<uint64_t>(row_pairs.size()))
        .Field("database", table_metadata->database_name)
        .Field("table", table_metadata->table_name)
        .Debug();

    return row_pairs;

  } catch (const std::exception& e) {
    mygram::utils::StructuredLog()
        .Event("mysql_binlog_error")
        .Field("type", "parse_exception")
        .Field("event_type", "update_rows")
        .Field("error", e.what())
        .Error();
    return MakeUnexpected(
        MakeError(ErrorCode::kMySQLBinlogError, std::string("Exception parsing UPDATE_ROWS event: ") + e.what()));
  }
}

// ---------------------------------------------------------------------------
// ParseDeleteRowsEvent
// ---------------------------------------------------------------------------

Expected<std::vector<RowData>, Error> ParseDeleteRowsEvent(const unsigned char* buffer, unsigned long length,
                                                           const TableMetadata* table_metadata,
                                                           const std::string& pk_column_name,
                                                           const std::string& text_column_name,
                                                           mybinlog::EventType event_type,
                                                           const RetainedColumns* retained_columns) {
  if ((buffer == nullptr) || (table_metadata == nullptr)) {
    return MakeUnexpected(
        MakeError(ErrorCode::kMySQLBinlogError, "Null buffer or table_metadata in ParseDeleteRowsEvent"));
  }

  try {
    auto layout = ReadRowsEventLayout(buffer, length, event_type, table_metadata, pk_column_name, text_column_name,
                                      /*update=*/false, "delete_rows");
    if (!layout) {
      return MakeUnexpected(layout.error());
    }

    std::vector<RowData> rows;
    rows.reserve(EstimatedRows(*layout, mygram::constants::kEstimatedBytesPerBinlogRow));
    if (auto decoded = DecodeRows(*layout, *table_metadata, retained_columns, "DELETE_ROWS", &rows, nullptr);
        !decoded) {
      return MakeUnexpected(decoded.error());
    }

    mygram::utils::StructuredLog()
        .Event("binlog_debug")
        .Field("action", "parsed_delete_rows")
        .Field("rows", static_cast<uint64_t>(rows.size()))
        .Field("database", table_metadata->database_name)
        .Field("table", table_metadata->table_name)
        .Debug();

    return rows;

  } catch (const std::exception& e) {
    mygram::utils::StructuredLog()
        .Event("mysql_binlog_error")
        .Field("type", "parse_exception")
        .Field("event_type", "delete_rows")
        .Field("error", e.what())
        .Error();
    return MakeUnexpected(
        MakeError(ErrorCode::kMySQLBinlogError, std::string("Exception parsing DELETE_ROWS event: ") + e.what()));
  }
}

}  // namespace mygramdb::mysql

// NOLINTEND(cppcoreguidelines-pro-*,cppcoreguidelines-avoid-*,readability-magic-numbers,readability-function-cognitive-complexity,readability-else-after-return)

#endif  // USE_MYSQL
