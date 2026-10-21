/**
 * @file binlog_event_parser.cpp
 * @brief MySQL binlog event parsing implementation
 *
 * Note: This file contains MySQL binlog protocol parsing code.
 * Some modern C++ guidelines are relaxed for protocol compatibility.
 */

#include "mysql/binlog_event_parser.h"

#ifdef USE_MYSQL

#include <spdlog/spdlog.h>

#include <algorithm>
#include <array>
#include <cstdio>
#include <cstring>
#include <string_view>

#include "mysql/binlog_event_disposition.h"
#include "mysql/binlog_event_types.h"
#include "mysql/binlog_filter_evaluator.h"
#include "mysql/binlog_util.h"
#include "mysql/ddl_classifier.h"
#include "mysql/mariadb_event_parser.h"
#include "mysql/rows_parser.h"
#include "mysql/table_name_match.h"
#include "mysql/text_materializer.h"
#include "server/server_types.h"  // For TableContext definition
#include "utils/constants.h"
#include "utils/string_utils.h"
#include "utils/structured_log.h"

// NOLINTBEGIN(cppcoreguidelines-pro-*,cppcoreguidelines-avoid-*,readability-magic-numbers)

namespace mygramdb::mysql {

namespace {

// ============================================================================
// Binlog event size constants
// ============================================================================

/// Minimum length for a standard GTID_LOG_EVENT
constexpr size_t kGTIDEventMinLength = mygram::constants::kBinlogEventHeaderLen + 1 + 16 + 8;

/// Minimum length for a GTID_TAGGED_LOG_EVENT (MySQL 8.4+): the three framing integers, then the id and a
/// one-byte value for flags, GNO and tag, and the id plus 16 UUID bytes
constexpr size_t kTaggedGTIDEventMinLength =
    mygram::constants::kBinlogEventHeaderLen + 3 + (1 + 1) + (1 + 16) + (1 + 1) + (1 + 1);

/// Serialization format version GTID_TAGGED_LOG_EVENT is written with
constexpr uint64_t kTaggedGtidFormatVersion = 1;

/// Longest tag a GTID can carry
constexpr uint64_t kTaggedGtidMaxTagLength = 32;

/// sql_mode bits in the QUERY_EVENT status variable; both servers use the same positions.
/// MySQL sql/system_variables.h, MariaDB sql/sql_class.h.
constexpr uint64_t kSqlModeAnsiQuotes = 1ULL << 2;
constexpr uint64_t kSqlModeNoBackslashEscapes = 1ULL << 20;

/// Most database names a MySQL Q_UPDATED_DB_NAMES status variable lists; a larger count carries none
constexpr uint8_t kMaxUpdatedDbNames = 16;

/// Optional-metadata TLV types of a TABLE_MAP event that the parser reads
/// (Table_map_event::Optional_metadata_field_type).
constexpr uint8_t kMetadataSignedness = 1;
constexpr uint8_t kMetadataColumnName = 4;
constexpr uint8_t kMetadataSetStrValue = 5;
constexpr uint8_t kMetadataEnumStrValue = 6;

/// ENUM and SET columns arrive as STRING with these real types in the high metadata byte
constexpr uint8_t kRealTypeEnum = 0xF7;
constexpr uint8_t kRealTypeSet = 0xF8;

/// Size of the fixed-length fields in a QUERY_EVENT (thread_id + exec_time +
/// db_len + error_code + status_vars_len = 4+4+1+2+2 = 13 bytes)
constexpr size_t kQueryEventFixedFieldsSize = 13;

size_t PackedIntegerSize(uint8_t first_byte) {
  if (first_byte < 252) {
    return 1;
  }
  if (first_byte == 252) {
    return 3;
  }
  if (first_byte == 253) {
    return 4;
  }
  return 9;
}

/// COMMIT [WORK] [AND [NO] CHAIN] [[NO] RELEASE] as the event's first statement
bool IsCommitStatement(const std::vector<DdlStatement>& statements) {
  return !statements.empty() && statements.front().transaction == TransactionControl::kEnd &&
         statements.front().verb == "COMMIT";
}

// ============================================================================
// ROWS_EVENT common helper structures and functions
// ============================================================================

/**
 * @brief Context for processing ROWS events (INSERT/UPDATE/DELETE)
 *
 * Contains all the common data needed to process a rows event,
 * extracted from buffer and configuration.
 */
struct RowsEventContext {
  uint64_t table_id = 0;
  const TableMetadata* table_meta = nullptr;
  const config::TableConfig* current_config = nullptr;
  // Key under which the matched table is registered in table_contexts (the
  // database-qualified identity in multi-table mode). Downstream consumers
  // resolve the index/doc_store and invalidate caches by this exact key, so it
  // must match the registration key rather than the bare binlog table name.
  std::string table_key;
  std::string text_column;
  // Ordinals the configuration actually reads back. Everything else in the row
  // image is skipped rather than decoded and copied.
  RetainedColumns retained_columns;
};

/**
 * @brief Extract table_id from binlog event post-header
 * @param buffer Event buffer (must point to beginning of event)
 * @return Table ID (6-byte little-endian value)
 */
inline uint64_t ExtractTableId(const unsigned char* buffer) {
  const unsigned char* post_header = buffer + mygram::constants::kBinlogEventHeaderLen;  // Skip common header
  uint64_t table_id = 0;
  for (int i = 0; i < 6; i++) {
    table_id |= static_cast<uint64_t>(post_header[i]) << (i * 8);
  }
  return table_id;
}

/**
 * @brief Initialize RowsEventContext with common data for ROWS events
 *
 * Extracts table metadata, configuration, text column settings, and
 * required filters from the event buffer and configuration.
 *
 * @param buffer Event buffer
 * @param length Event buffer length
 * @param table_metadata_cache Cache of table metadata
 * @param table_contexts Map of table contexts (multi-table mode)
 * @param table_config Single table config (single-table mode)
 * @param multi_table_mode Whether multi-table mode is enabled
 * @param event_type_name Name of event type for logging (e.g., "write", "update", "delete")
 * @param names_case_insensitive Whether names match ASCII case-insensitively (lower_case_table_names != 0)
 * @return Optional context, nullopt if table not found or not monitored
 */
std::optional<RowsEventContext> InitRowsEventContext(
    const unsigned char* buffer, unsigned long length, const TableMetadataCache& table_metadata_cache,
    const std::unordered_map<std::string, server::TableContext*>& table_contexts,
    const config::TableConfig* table_config, bool multi_table_mode, const std::string& event_type_name,
    bool names_case_insensitive) {
  RowsEventContext ctx;

  constexpr auto kRowsEventTableIdEnd = mygram::constants::kBinlogEventHeaderLen + 6;
  if (buffer == nullptr || length < kRowsEventTableIdEnd) {
    mygram::utils::StructuredLog()
        .Event("binlog_error")
        .Field("type", "rows_event_truncated_before_table_id")
        .Field("event_type", event_type_name)
        .Field("event_length", static_cast<uint64_t>(length))
        .Error();
    return std::nullopt;
  }

  // Extract table_id from post-header
  ctx.table_id = ExtractTableId(buffer);

  // Get table metadata from cache
  ctx.table_meta = table_metadata_cache.Get(ctx.table_id);
  if (ctx.table_meta == nullptr) {
    mygram::utils::StructuredLog()
        .Event("binlog_debug")
        .Field("action", "unknown_table_id_" + event_type_name)
        .Field("table_id", ctx.table_id)
        .Debug();
    return std::nullopt;
  }

  // Determine config based on mode
  if (multi_table_mode) {
    // Table contexts are keyed by the database-qualified identity
    // (e.g. "testdb.articles"). Look up by the qualified key derived from the
    // binlog event's database and table names, falling back to the bare table
    // name for configurations whose effective database is empty.
    const auto table_iter = FindTableContextEntry(table_contexts, ctx.table_meta->database_name,
                                                  ctx.table_meta->table_name, names_case_insensitive);
    if (table_iter == table_contexts.end()) {
      mygram::utils::StructuredLog()
          .Event("binlog_debug")
          .Field("action", "table_not_monitored_" + event_type_name)
          .Field("table", ctx.table_meta->table_name)
          .Debug();
      return std::nullopt;
    }
    ctx.current_config = &table_iter->second->config;
    ctx.table_key = table_iter->first;
  } else {
    ctx.current_config = table_config;
    ctx.table_key = table_config != nullptr ? table_config->name : ctx.table_meta->table_name;
  }

  if (ctx.current_config == nullptr) {
    return std::nullopt;
  }

  if (!ctx.current_config->database.empty() &&
      !TableNamesEqual(ctx.table_meta->database_name, ctx.current_config->database, names_case_insensitive)) {
    mygram::utils::StructuredLog()
        .Event("binlog_debug")
        .Field("action", "database_not_monitored_" + event_type_name)
        .Field("database", ctx.table_meta->database_name)
        .Field("table", ctx.table_meta->table_name)
        .Field("target_database", ctx.current_config->database)
        .Debug();
    return std::nullopt;
  }

  // Determine text column(s)
  if (!ctx.current_config->text_source.column.empty()) {
    ctx.text_column = ctx.current_config->text_source.column;
  } else if (!ctx.current_config->text_source.concat.empty()) {
    ctx.text_column = ctx.current_config->text_source.concat[0];  // Use first for initial parse
  } else {
    ctx.text_column = "";
  }

  ctx.retained_columns = BuildRetainedColumns(*ctx.table_meta, *ctx.current_config);

  return ctx;
}

std::optional<MaterializedText> MaterializeRowText(const RowData& row, const RowsEventContext& ctx, const char* image) {
  auto materialized = MaterializeTextSource(row, ctx.current_config->text_source);
  if (materialized.state == TextValueState::kAbsent) {
    mygram::utils::StructuredLog()
        .Event("mysql_binlog_error")
        .Field("type", "configured_text_column_absent")
        .Field("table", ctx.table_key)
        .Field("image", image)
        .Field("required", "binlog_row_image=FULL")
        .Error();
    return std::nullopt;
  }
  return materialized;
}

/**
 * @brief Format a 16-byte UUID as a standard hyphenated string.
 * @param bytes Pointer to 16 bytes of UUID data
 * @return UUID string in format "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
 */
std::string FormatUUID(const unsigned char* bytes) {
  char buf[37];
  snprintf(buf, sizeof(buf), "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x", bytes[0], bytes[1],
           bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7], bytes[8], bytes[9], bytes[10], bytes[11],
           bytes[12], bytes[13], bytes[14], bytes[15]);
  return std::string(buf, 36);
}

/**
 * @brief How MySQL lays out one column's entry in the TABLE_MAP metadata block.
 */
enum class TableMapMetadataLayout : uint8_t {
  kNone,                 ///< No bytes.
  kOneByte,              ///< One byte.
  kTwoByteLittleEndian,  ///< Two bytes, first byte least significant.
  kTwoByteHighFirst,     ///< Two independent bytes, first one stored as the high half.
};

/**
 * @brief Metadata layout for a column type, as MySQL's table_def reads what
 *        Field::do_save_field_metadata wrote.
 *
 * @param type Column type code from the TABLE_MAP event
 * @return The layout, or std::nullopt for a code outside ColumnType
 */
std::optional<TableMapMetadataLayout> TableMapMetadataLayoutFor(ColumnType type) {
  switch (type) {
    case ColumnType::TINY:
    case ColumnType::SHORT:
    case ColumnType::LONG:
    case ColumnType::LONGLONG:
    case ColumnType::INT24:
    case ColumnType::DATE:
    case ColumnType::DATETIME:
    case ColumnType::TIMESTAMP:
    case ColumnType::TIME:
    case ColumnType::YEAR:
    case ColumnType::NEWDATE:
      return TableMapMetadataLayout::kNone;

    // Length-prefix width for the BLOB family (JSON and GEOMETRY are stored as
    // BLOBs), pack length for FLOAT/DOUBLE, fractional precision for the rest.
    case ColumnType::TINY_BLOB:
    case ColumnType::MEDIUM_BLOB:
    case ColumnType::LONG_BLOB:
    case ColumnType::BLOB:
    case ColumnType::BLOB_COMPRESSED:
    case ColumnType::VECTOR:
    case ColumnType::JSON:
    case ColumnType::GEOMETRY:
    case ColumnType::FLOAT:
    case ColumnType::DOUBLE:
    case ColumnType::TIMESTAMP2:
    case ColumnType::DATETIME2:
    case ColumnType::TIME2:
      return TableMapMetadataLayout::kOneByte;

    // VARCHAR: maximum byte length. BIT: bits in the partial byte, then whole bytes.
    case ColumnType::VARCHAR:
    case ColumnType::VARCHAR_COMPRESSED:
    case ColumnType::BIT:
      return TableMapMetadataLayout::kTwoByteLittleEndian;

    // STRING: real_type then length; ENUM/SET columns arrive as STRING with
    // real_type 0xF7/0xF8 and the pack length. NEWDECIMAL: precision then scale.
    case ColumnType::STRING:
    case ColumnType::VAR_STRING:
    case ColumnType::ENUM:
    case ColumnType::SET:
    case ColumnType::NEWDECIMAL:
      return TableMapMetadataLayout::kTwoByteHighFirst;
  }
  return std::nullopt;
}

/**
 * @brief Whether a column type carries a bit in the TABLE_MAP SIGNEDNESS field
 *
 * Mirrors has_signedess_information_type in the server: the integer types, YEAR,
 * the floating-point types and DECIMAL. MariaDB marks the same set.
 */
bool HasSignednessBit(ColumnType type) {
  switch (type) {
    case ColumnType::TINY:
    case ColumnType::SHORT:
    case ColumnType::INT24:
    case ColumnType::LONG:
    case ColumnType::LONGLONG:
    case ColumnType::YEAR:
    case ColumnType::FLOAT:
    case ColumnType::DOUBLE:
    case ColumnType::NEWDECIMAL:
      return true;
    default:
      return false;
  }
}

enum class EnumSetKind : uint8_t { kNone, kEnum, kSet };

EnumSetKind EnumSetKindOf(const ColumnMetadata& column) {
  if (column.type == ColumnType::ENUM) {
    return EnumSetKind::kEnum;
  }
  if (column.type == ColumnType::SET) {
    return EnumSetKind::kSet;
  }
  if (column.type == ColumnType::STRING) {
    const auto real_type = static_cast<uint8_t>(column.metadata >> 8);
    if (real_type == kRealTypeEnum) {
      return EnumSetKind::kEnum;
    }
    if (real_type == kRealTypeSet) {
      return EnumSetKind::kSet;
    }
  }
  return EnumSetKind::kNone;
}

/// Reads a packed length from an optional-metadata block; false when truncated or the NULL marker.
bool ReadMetadataLength(const unsigned char*& pos, const unsigned char* end, uint64_t& value) {
  constexpr unsigned char kNullMarker = 251;
  if (pos >= end || *pos == kNullMarker) {
    return false;
  }
  return binlog_util::read_packed_integer(&pos, end, &value);
}

/// Reads one length-prefixed string; false when the length or the bytes run past @p end.
bool ReadMetadataString(const unsigned char*& pos, const unsigned char* end, std::string& value) {
  uint64_t length = 0;
  if (!ReadMetadataLength(pos, end, length) || length > static_cast<uint64_t>(end - pos)) {
    return false;
  }
  value.assign(reinterpret_cast<const char*>(pos), static_cast<size_t>(length));
  pos += length;
  return true;
}

/// SIGNEDNESS: one bit per numeric column, most significant bit first.
bool ParseSignedness(const unsigned char* pos, const unsigned char* end, TableMetadata& metadata) {
  const auto numeric_columns =
      static_cast<size_t>(std::count_if(metadata.columns.begin(), metadata.columns.end(),
                                        [](const ColumnMetadata& column) { return HasSignednessBit(column.type); }));
  if (static_cast<size_t>(end - pos) != binlog_util::bitmap_bytes(numeric_columns)) {
    return false;
  }
  size_t bit = 0;
  for (auto& column : metadata.columns) {
    if (HasSignednessBit(column.type)) {
      column.is_unsigned = ((pos[bit / 8] >> (7 - bit % 8)) & 1) != 0;
      ++bit;
    }
  }
  return true;
}

/// COLUMN_NAME: one length-prefixed name per column.
bool ParseColumnNames(const unsigned char* pos, const unsigned char* end, TableMetadata& metadata) {
  std::vector<std::string> names;
  while (pos < end) {
    if (!ReadMetadataString(pos, end, names.emplace_back())) {
      return false;
    }
  }
  if (names.size() != metadata.columns.size()) {
    return false;
  }
  for (size_t index = 0; index < names.size(); ++index) {
    metadata.columns[index].name = std::move(names[index]);
  }
  return true;
}

/// ENUM_STR_VALUE / SET_STR_VALUE: per column of the kind, a label count and that many length-prefixed labels.
bool ParseEnumSetLabels(const unsigned char* pos, const unsigned char* end, EnumSetKind kind, TableMetadata& metadata) {
  std::vector<std::vector<std::string>> groups;
  while (pos < end) {
    uint64_t count = 0;
    // Every label costs at least its length byte.
    if (!ReadMetadataLength(pos, end, count) || count > static_cast<uint64_t>(end - pos)) {
      return false;
    }
    auto& labels = groups.emplace_back();
    labels.reserve(static_cast<size_t>(count));
    for (uint64_t index = 0; index < count; ++index) {
      if (!ReadMetadataString(pos, end, labels.emplace_back())) {
        return false;
      }
    }
  }
  size_t next_group = 0;
  for (auto& column : metadata.columns) {
    if (EnumSetKindOf(column) != kind) {
      continue;
    }
    if (next_group >= groups.size()) {
      return false;
    }
    column.enum_set_values = std::move(groups[next_group++]);
  }
  return next_group == groups.size();
}

/**
 * @brief Read the optional-metadata TLV block that follows the TABLE_MAP NULL bitmap
 *
 * Fills names, signedness and ENUM/SET labels, and records what the block
 * supplied. Unknown TLV types are skipped by their length.
 *
 * @return false when a TLV is truncated or its content does not fit the table
 */
bool ParseTableMapOptionalMetadata(const unsigned char* pos, const unsigned char* end, TableMetadata& metadata) {
  bool saw_signedness = false;
  bool saw_enum_labels = false;
  bool saw_set_labels = false;
  while (pos < end) {
    const uint8_t type = *pos++;
    uint64_t length = 0;
    if (!ReadMetadataLength(pos, end, length) || length > static_cast<uint64_t>(end - pos)) {
      return false;
    }
    const unsigned char* value_end = pos + length;
    switch (type) {
      case kMetadataSignedness:
        if (!ParseSignedness(pos, value_end, metadata)) {
          return false;
        }
        saw_signedness = true;
        break;
      case kMetadataColumnName:
        if (!ParseColumnNames(pos, value_end, metadata)) {
          return false;
        }
        metadata.has_column_names = true;
        break;
      case kMetadataEnumStrValue:
        if (!ParseEnumSetLabels(pos, value_end, EnumSetKind::kEnum, metadata)) {
          return false;
        }
        saw_enum_labels = true;
        break;
      case kMetadataSetStrValue:
        if (!ParseEnumSetLabels(pos, value_end, EnumSetKind::kSet, metadata)) {
          return false;
        }
        saw_set_labels = true;
        break;
      default:
        break;
    }
    pos = value_end;
  }

  const auto has_column = [&metadata](auto predicate) {
    return std::any_of(metadata.columns.begin(), metadata.columns.end(), predicate);
  };
  metadata.signedness_known =
      saw_signedness || !has_column([](const ColumnMetadata& column) { return HasSignednessBit(column.type); });
  metadata.enum_set_labels_known =
      (saw_enum_labels ||
       !has_column([](const ColumnMetadata& column) { return EnumSetKindOf(column) == EnumSetKind::kEnum; })) &&
      (saw_set_labels ||
       !has_column([](const ColumnMetadata& column) { return EnumSetKindOf(column) == EnumSetKind::kSet; }));
  return true;
}

/**
 * @brief Read one variable-length integer of the serialization framework
 *
 * The first byte's trailing one bits give the extra byte count; its remaining
 * high bits are the low bits of the value, and the extra bytes (little-endian)
 * supply the rest (libs/mysql/serialization/variable_length_integers.h).
 *
 * @return false when the integer runs past @p end
 */
bool ReadSerializedVarlen(const unsigned char*& pos, const unsigned char* end, uint64_t& value) {
  if (pos >= end) {
    return false;
  }
  const unsigned first = *pos;
  size_t extra_bytes = 0;
  while (extra_bytes < 8 && ((first >> extra_bytes) & 1U) != 0) {
    ++extra_bytes;
  }
  const size_t total_bytes = extra_bytes + 1;
  if (total_bytes > static_cast<size_t>(end - pos)) {
    return false;
  }
  value = total_bytes >= 9 ? 0 : (first >> total_bytes);
  if (extra_bytes > 0) {
    uint64_t high = 0;
    for (size_t index = 0; index < extra_bytes; ++index) {
      high |= static_cast<uint64_t>(pos[1 + index]) << (index * 8);
    }
    const size_t shift = 8 - total_bytes + ((total_bytes + 7) >> 4);
    value |= high << shift;
  }
  pos += total_bytes;
  return true;
}

/**
 * @brief The quoting-relevant sql_mode bits the QUERY_EVENT status variables carry
 *
 * Walks the variables in write order with the per-code lengths of both servers
 * (MySQL statement_events.cpp Query_event, MariaDB log_event.cc Query_log_event)
 * until Q_SQL_MODE_CODE. Like the servers, it stops at an unknown code; a mode
 * it cannot reach is reported as unknown, not as off. A walk that ends with no
 * mode variable at all reads as the default mode.
 */
std::optional<QueryLexOptions> StatusVarsLexOptions(const unsigned char* pos, const unsigned char* end) {
  constexpr uint8_t kSqlModeCode = 1;
  constexpr uint8_t kGtidFlags3CommitAlterE1 = 4;
  constexpr uint8_t kGtidFlags3RollbackAlterE1 = 8;

  while (pos < end) {
    const uint8_t code = *pos++;
    const auto available = static_cast<size_t>(end - pos);
    size_t skip = 0;
    switch (code) {
      case kSqlModeCode: {
        if (available < 8) {
          return std::nullopt;
        }
        const uint64_t sql_mode = binlog_util::uint8korr(pos);
        return QueryLexOptions{(sql_mode & kSqlModeAnsiQuotes) != 0, (sql_mode & kSqlModeNoBackslashEscapes) != 0};
      }
      case 0:   // Q_FLAGS2_CODE
      case 3:   // Q_AUTO_INCREMENT
      case 10:  // Q_MASTER_DATA_WRITTEN_CODE
        skip = 4;
        break;
      case 2:  // Q_CATALOG_CODE: length, name, NUL
        if (available < 1) {
          return std::nullopt;
        }
        skip = 1 + static_cast<size_t>(pos[0]) + 1;
        break;
      case 4:  // Q_CHARSET_CODE
        skip = 6;
        break;
      case 5:  // Q_TIME_ZONE_CODE
      case 6:  // Q_CATALOG_NZ_CODE
        if (available < 1) {
          return std::nullopt;
        }
        skip = 1 + static_cast<size_t>(pos[0]);
        break;
      case 7:   // Q_LC_TIME_NAMES_CODE
      case 8:   // Q_CHARSET_DATABASE_CODE
      case 18:  // Q_DEFAULT_COLLATION_FOR_UTF8MB4
        skip = 2;
        break;
      case 9:    // Q_TABLE_MAP_FOR_UPDATE_CODE
      case 17:   // Q_DDL_LOGGED_WITH_XID
      case 129:  // MariaDB Q_XID
        skip = 8;
        break;
      case 11:  // Q_INVOKER: user, host, each length-prefixed
        if (available < 1 || available < 2 + static_cast<size_t>(pos[0])) {
          return std::nullopt;
        }
        skip = 2 + static_cast<size_t>(pos[0]) + static_cast<size_t>(pos[1 + pos[0]]);
        break;
      case 12: {  // MySQL Q_UPDATED_DB_NAMES: count, then NUL-terminated names unless the count is over the cap
        if (available < 1 || pos[0] == 0) {
          return std::nullopt;
        }
        const uint8_t count = pos[0];
        skip = 1;
        if (count <= kMaxUpdatedDbNames) {
          const unsigned char* name = pos + 1;
          for (uint8_t index = 0; index < count; ++index) {
            const void* terminator = name < end ? memchr(name, 0, static_cast<size_t>(end - name)) : nullptr;
            if (terminator == nullptr) {
              return std::nullopt;
            }
            name = static_cast<const unsigned char*>(terminator) + 1;
          }
          skip = static_cast<size_t>(name - pos);
        }
        break;
      }
      case 13:   // Q_MICROSECONDS
      case 128:  // MariaDB Q_HRNOW
        skip = 3;
        break;
      case 16:  // Q_EXPLICIT_DEFAULTS_FOR_TIMESTAMP
      case 19:  // Q_SQL_REQUIRE_PRIMARY_KEY
      case 20:  // Q_DEFAULT_TABLE_ENCRYPTION
        skip = 1;
        break;
      case 130:  // MariaDB Q_GTID_FLAGS3: flags, then a sequence number for commit/rollback alter
        if (available < 1) {
          return std::nullopt;
        }
        skip = 1 + (((pos[0] & (kGtidFlags3CommitAlterE1 | kGtidFlags3RollbackAlterE1)) != 0) ? 8 : 0);
        break;
      case 131:  // MariaDB Q_CHARACTER_SET_COLLATIONS: count, then count 4-byte entries
        if (available < 1) {
          return std::nullopt;
        }
        skip = 1 + static_cast<size_t>(pos[0]) * 4;
        break;
      default:
        return std::nullopt;
    }
    if (skip > available) {
      return std::nullopt;
    }
    pos += skip;
  }
  // No mode variable at all: the statement ran under the default mode.
  return QueryLexOptions{};
}

}  // namespace

std::vector<BinlogEvent> BinlogEventParser::ParseBinlogEvent(
    const unsigned char* buffer, unsigned long length, const std::string& current_gtid,
    const TableMetadataCache& table_metadata_cache,
    const std::unordered_map<std::string, server::TableContext*>& table_contexts,
    const config::TableConfig* table_config, bool multi_table_mode, const std::string& datetime_timezone,
    bool table_names_case_insensitive) {
  if ((buffer == nullptr) || length < mygram::constants::kBinlogEventHeaderLen) {
    // Minimum event size is header length bytes (binlog header)
    return {};
  }

  // binlog_reader already skipped OK byte, buffer points to event data.
  // Binlog event header format (19 bytes):
  //   [timestamp(4)][event_type(1)][server_id(4)][event_size(4)][log_pos(4)][flags(2)]
  // (see mysql-8.4.7/libs/mysql/binlog/event/binlog_event.h: LOG_EVENT_HEADER_LEN)

  auto event_type = static_cast<MySQLBinlogEventType>(buffer[4]);

  // Log event type for debugging
  mygram::utils::StructuredLog()
      .Event("binlog_debug")
      .Field("action", "received_event")
      .Field("event_name", GetEventTypeName(event_type))
      .Field("event_type", static_cast<int64_t>(buffer[4]))
      .Debug();

  // Handle different event types
  switch (event_type) {
    case MySQLBinlogEventType::GTID_LOG_EVENT:
      // GTID events are handled by caller (UpdateCurrentGTID)
      return {};

    case MySQLBinlogEventType::TABLE_MAP_EVENT:
      // TABLE_MAP events are cached by caller
      return {};

    case MySQLBinlogEventType::OBSOLETE_WRITE_ROWS_EVENT_V1:
      // V1 format (MySQL 5.1-5.5) - fallthrough to V2 handler
      // V1 and V2 share the same post-header structure; V2 may have extra_row_info based on flags
      [[fallthrough]];
    case MySQLBinlogEventType::WRITE_ROWS_EVENT: {
      // Parse INSERT operations (V1 and V2)
      mygram::utils::StructuredLog().Event("binlog_debug").Field("action", "write_rows_detected").Debug();

      // Initialize context with common data
      auto ctx_opt = InitRowsEventContext(buffer, length, table_metadata_cache, table_contexts, table_config,
                                          multi_table_mode, "write", table_names_case_insensitive);
      if (!ctx_opt) {
        return {};
      }
      const auto& ctx = *ctx_opt;

      auto rows_result = ParseWriteRowsEvent(buffer, length, ctx.table_meta, ctx.current_config->primary_key,
                                             ctx.text_column, event_type, &ctx.retained_columns);

      if (!rows_result || rows_result->empty()) {
        return {};
      }

      // Create events for ALL rows (multi-row event support)
      std::vector<BinlogEvent> events;
      events.reserve(rows_result->size());

      for (const auto& row : *rows_result) {
        auto materialized_text = MaterializeRowText(row, ctx, "after");
        if (!materialized_text.has_value()) {
          return {};
        }
        BinlogEvent event;
        event.type = BinlogEventType::INSERT;
        event.table_name = ctx.table_key;
        event.primary_key = row.primary_key;
        event.primary_key_present = row.primary_key_present;
        event.primary_key_null = row.primary_key_null;
        event.text = std::move(materialized_text->value);
        event.text_state = materialized_text->state;
        event.gtid = current_gtid;

        // Extract all filters (required + optional) from row data
        event.filters =
            BinlogFilterEvaluator::ExtractAllFilters(row, *ctx.current_config, datetime_timezone, event.unconverted);

        events.push_back(std::move(event));
      }

      mygram::utils::StructuredLog()
          .Event("binlog_debug")
          .Field("action", "parsed_write_rows")
          .Field("row_count", static_cast<uint64_t>(events.size()))
          .Debug();

      return events;
    }

    case MySQLBinlogEventType::OBSOLETE_UPDATE_ROWS_EVENT_V1:
      // V1 format (MySQL 5.1-5.5) - fallthrough to V2 handler
      [[fallthrough]];
    case MySQLBinlogEventType::UPDATE_ROWS_EVENT: {
      // Parse UPDATE operations (V1 and V2)
      mygram::utils::StructuredLog().Event("binlog_debug").Field("action", "update_rows_detected").Debug();

      // Initialize context with common data
      auto ctx_opt = InitRowsEventContext(buffer, length, table_metadata_cache, table_contexts, table_config,
                                          multi_table_mode, "update", table_names_case_insensitive);
      if (!ctx_opt) {
        return {};
      }
      const auto& ctx = *ctx_opt;

      // Parse rows using rows_parser
      auto row_pairs_result = ParseUpdateRowsEvent(buffer, length, ctx.table_meta, ctx.current_config->primary_key,
                                                   ctx.text_column, event_type, &ctx.retained_columns);

      if (!row_pairs_result || row_pairs_result->empty()) {
        return {};
      }

      // Create events for ALL row pairs (multi-row event support)
      std::vector<BinlogEvent> events;
      events.reserve(row_pairs_result->size());

      for (const auto& row_pair : *row_pairs_result) {
        const auto& before_row = row_pair.first;  // Before image
        const auto& after_row = row_pair.second;  // After image

        auto before_text = MaterializeRowText(before_row, ctx, "before");
        auto after_text = MaterializeRowText(after_row, ctx, "after");
        if (!before_text.has_value() || !after_text.has_value()) {
          return {};
        }

        BinlogEvent event;
        event.type = BinlogEventType::UPDATE;
        event.table_name = ctx.table_key;
        event.primary_key = after_row.primary_key;
        event.primary_key_present = after_row.primary_key_present;
        event.primary_key_null = after_row.primary_key_null;
        event.old_primary_key = before_row.primary_key;
        event.old_primary_key_present = before_row.primary_key_present;
        event.old_primary_key_null = before_row.primary_key_null;
        event.text = std::move(after_text->value);
        event.old_text = std::move(before_text->value);
        event.text_state = after_text->state;
        event.old_text_state = before_text->state;
        event.gtid = current_gtid;

        // Extract all filters from after image
        event.filters = BinlogFilterEvaluator::ExtractAllFilters(after_row, *ctx.current_config, datetime_timezone,
                                                                 event.unconverted);

        events.push_back(std::move(event));
      }

      mygram::utils::StructuredLog()
          .Event("binlog_debug")
          .Field("action", "parsed_update_rows")
          .Field("row_count", static_cast<uint64_t>(events.size()))
          .Debug();

      return events;
    }

    case MySQLBinlogEventType::OBSOLETE_DELETE_ROWS_EVENT_V1:
      // V1 format (MySQL 5.1-5.5) - fallthrough to V2 handler
      [[fallthrough]];
    case MySQLBinlogEventType::DELETE_ROWS_EVENT: {
      // Parse DELETE operations
      mygram::utils::StructuredLog().Event("binlog_debug").Field("action", "delete_rows_detected").Debug();

      // Initialize context with common data
      auto ctx_opt = InitRowsEventContext(buffer, length, table_metadata_cache, table_contexts, table_config,
                                          multi_table_mode, "delete", table_names_case_insensitive);
      if (!ctx_opt) {
        return {};
      }
      const auto& ctx = *ctx_opt;

      // Parse rows using rows_parser
      auto rows_result = ParseDeleteRowsEvent(buffer, length, ctx.table_meta, ctx.current_config->primary_key,
                                              ctx.text_column, event_type, &ctx.retained_columns);

      if (!rows_result || rows_result->empty()) {
        return {};
      }

      // Create events for ALL rows (multi-row event support)
      std::vector<BinlogEvent> events;
      events.reserve(rows_result->size());

      for (const auto& row : *rows_result) {
        auto materialized_text = MaterializeRowText(row, ctx, "before");
        if (!materialized_text.has_value()) {
          return {};
        }
        BinlogEvent event;
        event.type = BinlogEventType::DELETE;
        event.table_name = ctx.table_key;
        event.primary_key = row.primary_key;
        event.primary_key_present = row.primary_key_present;
        event.primary_key_null = row.primary_key_null;
        event.text = std::move(materialized_text->value);
        event.text_state = materialized_text->state;
        event.gtid = current_gtid;

        // Extract all filters from row data (before image for DELETE)
        event.filters =
            BinlogFilterEvaluator::ExtractAllFilters(row, *ctx.current_config, datetime_timezone, event.unconverted);

        events.push_back(std::move(event));
      }

      mygram::utils::StructuredLog()
          .Event("binlog_debug")
          .Field("action", "parsed_delete_rows")
          .Field("row_count", static_cast<uint64_t>(events.size()))
          .Debug();

      return events;
    }

    case MySQLBinlogEventType::QUERY_EVENT: {
      // DDL statements (CREATE, ALTER, DROP, TRUNCATE, etc.)
      auto query_data_opt = ExtractQueryString(buffer, length);
      if (!query_data_opt) {
        return {};
      }

      const QueryEventData& query_data = *query_data_opt;
      mygram::utils::StructuredLog()
          .Event("binlog_debug")
          .Field("action", "query_event")
          .Field("database", query_data.database)
          .Field("query", query_data.query)
          .Debug();

      const std::vector<QueryLexOptions> modes = LexModes(query_data);
      std::vector<std::vector<DdlStatement>> statements_by_mode;
      statements_by_mode.reserve(modes.size());
      for (const QueryLexOptions& mode : modes) {
        statements_by_mode.push_back(ClassifyQueryEvent(query_data.query, mode));
      }

      if (!current_gtid.empty() && std::all_of(statements_by_mode.begin(), statements_by_mode.end(),
                                               [](const auto& statements) { return IsCommitStatement(statements); })) {
        BinlogEvent event;
        event.type = BinlogEventType::COMMIT;
        event.gtid = current_gtid;
        return {event};
      }

      // The DDL a configured table sees is the most destructive one any statement
      // applies to it under any mode the event may have been logged with.
      std::vector<DdlStatement> statements_in_every_mode;
      for (const auto& statements : statements_by_mode) {
        statements_in_every_mode.insert(statements_in_every_mode.end(), statements.begin(), statements.end());
      }
      const auto find_ddl = [&](const config::TableConfig& config) {
        return FindConfiguredTableDDL(statements_in_every_mode, query_data.database, config,
                                      table_names_case_insensitive);
      };
      if (multi_table_mode) {
        // Multi-table mode: check all registered tables
        std::vector<BinlogEvent> events;
        for (const auto& [table_name, ctx] : table_contexts) {
          std::optional<DDLType> ddl_match;
          if (ctx != nullptr) {
            ddl_match = find_ddl(ctx->config);
          }
          if (ddl_match) {
            BinlogEvent event;
            event.type = BinlogEventType::DDL;
            event.ddl_type = *ddl_match;
            event.table_name = table_name;
            event.text = query_data.query;  // Store the DDL query
            event.gtid = current_gtid;
            events.push_back(std::move(event));
          }
        }
        if (!events.empty()) {
          return events;
        }
      } else {
        // Single-table mode: check only our configured table
        std::optional<DDLType> ddl_match;
        if (table_config != nullptr) {
          ddl_match = find_ddl(*table_config);
        }
        if (ddl_match) {
          BinlogEvent event;
          event.type = BinlogEventType::DDL;
          event.ddl_type = *ddl_match;
          event.table_name = table_config->name;
          event.text = query_data.query;  // Store the DDL query
          event.gtid = current_gtid;
          return {event};  // Return as vector with single element
        }
      }

      return {};
    }

    case MySQLBinlogEventType::ROTATE_EVENT:
      // Binlog file rotation - indicates switch to a new binlog file
      // No action needed, handled by binlog_reader at connection level
      return {};

    case MySQLBinlogEventType::HEARTBEAT_LOG_EVENT:
    case MySQLBinlogEventType::HEARTBEAT_LOG_EVENT_V2:
      // Replication heartbeat - keepalive signal from master
      // No action needed
      return {};

    case MySQLBinlogEventType::XID_EVENT:
      // Transaction commit marker
      if (!current_gtid.empty()) {
        BinlogEvent event;
        event.type = BinlogEventType::COMMIT;
        event.gtid = current_gtid;
        return {event};
      }
      return {};

    case MySQLBinlogEventType::TRANSACTION_PAYLOAD_EVENT:
      // binlog_transaction_compression=ON wraps events in ZSTD-compressed payload.
      // This is not supported - log error and skip.
      mygram::utils::StructuredLog()
          .Event("binlog_error")
          .Field("type", "unsupported_event")
          .Field("event_type", "TRANSACTION_PAYLOAD_EVENT")
          .Field("message",
                 "binlog_transaction_compression=ON is not supported. "
                 "Disable compression or upgrade MygramDB.")
          .Error();
      return {};

    case MySQLBinlogEventType::MARIADB_QUERY_COMPRESSED_EVENT:
    case MySQLBinlogEventType::MARIADB_WRITE_ROWS_COMPRESSED_EVENT_V1:
    case MySQLBinlogEventType::MARIADB_UPDATE_ROWS_COMPRESSED_EVENT_V1:
    case MySQLBinlogEventType::MARIADB_DELETE_ROWS_COMPRESSED_EVENT_V1:
    case MySQLBinlogEventType::MARIADB_WRITE_ROWS_COMPRESSED_EVENT:
    case MySQLBinlogEventType::MARIADB_UPDATE_ROWS_COMPRESSED_EVENT:
    case MySQLBinlogEventType::MARIADB_DELETE_ROWS_COMPRESSED_EVENT:
      // The reader rejects these before dispatch so their XID cannot advance
      // the processed GTID. Keep the parser fail-closed for direct callers too.
      mygram::utils::StructuredLog()
          .Event("binlog_error")
          .Field("type", "unsupported_event")
          .Field("event_type", GetEventTypeName(event_type))
          .Field("message", "MariaDB log_bin_compress events are not supported")
          .Error();
      return {};

    case MySQLBinlogEventType::GTID_TAGGED_LOG_EVENT:
      // MySQL 8.4+ tagged GTIDs - handled by caller (binlog_reader) like GTID_LOG_EVENT
      return {};

    // MariaDB-specific events: handled by caller (binlog_reader_threads) or informational only
    case MySQLBinlogEventType::MARIADB_GTID_EVENT:
    case MySQLBinlogEventType::MARIADB_GTID_LIST_EVENT:
    case MySQLBinlogEventType::MARIADB_ANNOTATE_ROWS_EVENT:
    case MySQLBinlogEventType::MARIADB_BINLOG_CHECKPOINT_EVENT:
    case MySQLBinlogEventType::MARIADB_START_ENCRYPTION_EVENT:
      return {};

    default:
      // Every type not decoded above is dispatched by its recorded disposition
      // rather than ignored. Only the reader can stop replication, so the
      // parser reports a fail-closed type and produces nothing from it; the
      // reader rejects the same type before it ever reaches this function.
      if (ClassifyBinlogEventDisposition(event_type) == BinlogEventDisposition::kFailClosed) {
        mygram::utils::StructuredLog()
            .Event("binlog_error")
            .Field("type", "unreplayable_binlog_event")
            .Field("event_type", GetEventTypeName(event_type))
            .Field("message", UnreplayableEventRemediation(event_type))
            .Error();
      }
      return {};
  }
}

std::optional<std::string> BinlogEventParser::ExtractGTID(const unsigned char* buffer, unsigned long length) {
  if ((buffer == nullptr) || length < kGTIDEventMinLength) {
    // GTID event minimum size
    return {};
  }

  // GTID event format (after 19-byte header):
  // commit_flag (1 byte)
  // sid (16 bytes, UUID)
  // gno (8 bytes, transaction number)

  // Skip header and commit_flag (1 byte)
  const unsigned char* sid_ptr = buffer + mygram::constants::kBinlogEventHeaderLen + 1;

  // Extract GNO (8 bytes, little-endian)
  const unsigned char* gno_ptr = sid_ptr + 16;
  uint64_t gno = 0;
  for (int i = 0; i < 8; i++) {
    gno |= (uint64_t)gno_ptr[i] << (i * 8);
  }

  // Format as "UUID:GNO"
  std::string gtid = FormatUUID(sid_ptr) + ":" + std::to_string(gno);
  return gtid;
}

std::optional<std::string> BinlogEventParser::ExtractTaggedGTID(const unsigned char* buffer, unsigned long length) {
  if ((buffer == nullptr) || length < kTaggedGTIDEventMinLength) {
    return {};
  }

  // The body is a serialization-framework message with no commit_flag byte:
  //   format version, message size, last non-ignorable field id   (varlen ints)
  //   field 0 gtid_flags, 1 UUID bytes (16 varlen ints), 2 GNO (signed varlen), 3 tag (varlen length + bytes), ...
  // and every field is preceded by its id as a varlen int. Only the fields up to the tag are read.
  const unsigned char* pos = buffer + mygram::constants::kBinlogEventHeaderLen;
  const unsigned char* const buf_end = buffer + length;

  uint64_t value = 0;
  if (!ReadSerializedVarlen(pos, buf_end, value) || value != kTaggedGtidFormatVersion) {
    return {};
  }
  uint64_t message_size = 0;
  uint64_t last_non_ignorable_field = 0;
  if (!ReadSerializedVarlen(pos, buf_end, message_size) ||
      !ReadSerializedVarlen(pos, buf_end, last_non_ignorable_field)) {
    return {};
  }

  const auto read_field_id = [&pos, buf_end](uint64_t expected) {
    uint64_t field_id = 0;
    return ReadSerializedVarlen(pos, buf_end, field_id) && field_id == expected;
  };

  uint64_t gtid_flags = 0;
  if (!read_field_id(0) || !ReadSerializedVarlen(pos, buf_end, gtid_flags)) {
    return {};
  }

  std::array<unsigned char, 16> uuid{};
  if (!read_field_id(1)) {
    return {};
  }
  for (auto& byte : uuid) {
    if (!ReadSerializedVarlen(pos, buf_end, value) || value > 0xFF) {
      return {};
    }
    byte = static_cast<unsigned char>(value);
  }

  // GNO is a signed varlen integer: the low bit is the sign, the rest the (possibly complemented) magnitude.
  uint64_t encoded_gno = 0;
  if (!read_field_id(2) || !ReadSerializedVarlen(pos, buf_end, encoded_gno)) {
    return {};
  }
  const auto gno = static_cast<int64_t>((encoded_gno >> 1) ^ (~(encoded_gno & 1) + 1));
  if (gno < 1) {
    return {};
  }

  uint64_t tag_length = 0;
  if (!read_field_id(3) || !ReadSerializedVarlen(pos, buf_end, tag_length) || tag_length > kTaggedGtidMaxTagLength ||
      tag_length > static_cast<uint64_t>(buf_end - pos)) {
    return {};
  }
  const std::string tag(reinterpret_cast<const char*>(pos), static_cast<size_t>(tag_length));

  // Format as "UUID:TAG:GNO" or "UUID:GNO"
  std::string gtid = FormatUUID(uuid.data());
  if (!tag.empty()) {
    gtid += ":" + tag;
  }
  gtid += ":" + std::to_string(gno);
  return gtid;
}

mygram::utils::Expected<TableMetadata, mygram::utils::Error> BinlogEventParser::ParseTableMapEvent(
    const unsigned char* buffer, unsigned long length,
    const std::function<bool(const TableMetadata&)>& wants_row_metadata) {
  // A frame that does not add up is not proof the event is undecodable: a
  // replay can deliver it intact, so it keeps the generic binlog code.
  const auto malformed = []() {
    return mygram::utils::MakeUnexpected(
        mygram::utils::MakeError(mygram::utils::ErrorCode::kMySQLBinlogError, "Malformed TABLE_MAP_EVENT"));
  };

  if ((buffer == nullptr) || length < mygram::constants::kBinlogEventHeaderLen) {
    // The common header is read before anything else, including the event_size
    // field at offset 9, so the whole header has to be present first. The
    // post-header fields this event needs are checked against the declared
    // event size further down.
    mygram::utils::StructuredLog()
        .Event("binlog_parse_error")
        .Field("function", "ParseTableMapEvent")
        .Field("reason", "buffer_null_or_too_short")
        .Field("length", static_cast<uint64_t>(length))
        .Error();
    return malformed();
  }

  TableMetadata metadata;

  // binlog_reader already skipped OK byte, so buffer points to event data.
  // Standard binlog event header: LOG_EVENT_HEADER_LEN = 19 bytes
  //   [timestamp(4)][type(1)][server_id(4)][event_size(4)][log_pos(4)][flags(2)]
  // (see mysql-8.4.7/libs/mysql/binlog/event/binlog_event.h)
  uint32_t event_size = binlog_util::uint4korr(buffer + 9);
  if (event_size < mygram::constants::kBinlogEventHeaderLen + mygram::constants::kBinlogChecksumSize ||
      event_size > length) {
    mygram::utils::StructuredLog()
        .Event("binlog_parse_error")
        .Field("function", "ParseTableMapEvent")
        .Field("reason", "invalid_event_size")
        .Field("event_size", static_cast<uint64_t>(event_size))
        .Field("length", static_cast<uint64_t>(length))
        .Error();
    return malformed();
  }

  const unsigned char* event_end = buffer + event_size - mygram::constants::kBinlogChecksumSize;
  const unsigned char* ptr = buffer + mygram::constants::kBinlogEventHeaderLen;
  unsigned long remaining = static_cast<unsigned long>(event_end - ptr);

  mygram::utils::StructuredLog()
      .Event("binlog_debug")
      .Field("action", "parse_table_map_start")
      .Field("length", static_cast<uint64_t>(length))
      .Field("remaining", static_cast<int64_t>(remaining))
      .Debug();

  if (remaining < 8) {
    mygram::utils::StructuredLog()
        .Event("binlog_parse_error")
        .Field("function", "ParseTableMapEvent")
        .Field("reason", "insufficient_after_header")
        .Field("remaining", static_cast<uint64_t>(remaining))
        .Error();
    return malformed();
  }

  // Parse table_id (6 bytes)
  metadata.table_id = 0;
  for (int i = 0; i < 6; i++) {
    metadata.table_id |= (uint64_t)ptr[i] << (i * 8);
  }
  ptr += 6;
  remaining -= 6;

  mygram::utils::StructuredLog()
      .Event("binlog_debug")
      .Field("action", "parse_table_map_table_id")
      .Field("table_id", metadata.table_id)
      .Field("remaining", static_cast<int64_t>(remaining))
      .Debug();

  // Skip flags (2 bytes)
  ptr += 2;
  remaining -= 2;

  if (remaining < 1) {
    mygram::utils::StructuredLog()
        .Event("binlog_parse_error")
        .Field("function", "ParseTableMapEvent")
        .Field("reason", "no_space_for_db_len")
        .Field("remaining", static_cast<uint64_t>(remaining))
        .Error();
    return malformed();
  }

  // Parse database name (1 byte length + null-terminated string)
  uint8_t db_len = *ptr++;
  remaining--;

  mygram::utils::StructuredLog()
      .Event("binlog_debug")
      .Field("action", "parse_table_map_db_len")
      .Field("db_len", static_cast<uint64_t>(db_len))
      .Field("remaining", static_cast<int64_t>(remaining))
      .Debug();

  if (remaining < static_cast<size_t>(db_len) + 1) {  // +1 for null terminator
    mygram::utils::StructuredLog()
        .Event("binlog_parse_error")
        .Field("function", "ParseTableMapEvent")
        .Field("reason", "insufficient_for_db_name")
        .Field("remaining", static_cast<uint64_t>(remaining))
        .Field("db_len", static_cast<uint64_t>(db_len))
        .Error();
    return malformed();
  }

  metadata.database_name = std::string(reinterpret_cast<const char*>(ptr), db_len);
  ptr += db_len + 1;  // +1 for null terminator
  remaining -= (db_len + 1);

  mygram::utils::StructuredLog()
      .Event("binlog_debug")
      .Field("action", "parse_table_map_db_name")
      .Field("database", metadata.database_name)
      .Field("remaining", static_cast<int64_t>(remaining))
      .Debug();

  if (remaining < 1) {
    mygram::utils::StructuredLog()
        .Event("binlog_parse_error")
        .Field("function", "ParseTableMapEvent")
        .Field("reason", "no_space_for_table_len")
        .Field("remaining", static_cast<uint64_t>(remaining))
        .Error();
    return malformed();
  }

  // Parse table name (1 byte length + null-terminated string)
  uint8_t table_len = *ptr++;
  remaining--;

  mygram::utils::StructuredLog()
      .Event("binlog_debug")
      .Field("action", "parse_table_map_table_len")
      .Field("table_len", static_cast<uint64_t>(table_len))
      .Field("remaining", static_cast<int64_t>(remaining))
      .Debug();

  if (remaining < static_cast<size_t>(table_len) + 1) {
    mygram::utils::StructuredLog()
        .Event("binlog_parse_error")
        .Field("function", "ParseTableMapEvent")
        .Field("reason", "insufficient_for_table_name")
        .Field("remaining", static_cast<uint64_t>(remaining))
        .Field("table_len", static_cast<uint64_t>(table_len))
        .Error();
    return malformed();
  }

  metadata.table_name = std::string(reinterpret_cast<const char*>(ptr), table_len);
  ptr += table_len + 1;
  remaining -= (table_len + 1);

  mygram::utils::StructuredLog()
      .Event("binlog_debug")
      .Field("action", "parse_table_map_table_name")
      .Field("table", metadata.table_name)
      .Field("remaining", static_cast<int64_t>(remaining))
      .Debug();

  if (remaining < 1) {
    return malformed();
  }

  // Parse column count (packed integer)
  size_t column_count_packed_size = PackedIntegerSize(*ptr);
  if (remaining < column_count_packed_size) {
    mygram::utils::StructuredLog()
        .Event("binlog_parse_error")
        .Field("function", "ParseTableMapEvent")
        .Field("reason", "truncated_column_count")
        .Field("remaining", static_cast<uint64_t>(remaining))
        .Field("required", static_cast<uint64_t>(column_count_packed_size))
        .Error();
    return malformed();
  }
  const unsigned char* ptr_before_packed = ptr;
  uint64_t column_count = 0;
  if (!binlog_util::read_packed_integer(&ptr, ptr + remaining, &column_count)) {
    return malformed();
  }

  // SECURITY: Update remaining bytes after reading packed integer
  size_t packed_int_size = ptr - ptr_before_packed;
  if (remaining < packed_int_size) {
    return malformed();
  }
  remaining -= packed_int_size;

  // SECURITY: Validate column count to prevent integer overflow and excessive allocation
  if (column_count > mygram::constants::kMySQLMaxColumns) {
    mygram::utils::StructuredLog()
        .Event("mysql_binlog_warning")
        .Field("type", "column_count_exceeds_maximum")
        .Field("column_count", column_count)
        .Field("max_columns", mygram::constants::kMySQLMaxColumns)
        .Warn();
    return malformed();
  }

  if (remaining < column_count) {
    return malformed();
  }

  // Parse column types (1 byte per column)
  metadata.columns.reserve(column_count);
  for (uint64_t i = 0; i < column_count; i++) {
    if (remaining < 1) {
      return malformed();
    }
    ColumnMetadata col;
    col.type = static_cast<ColumnType>(*ptr++);
    remaining--;
    col.metadata = 0;
    col.is_nullable = false;
    col.is_unsigned = false;
    // Replaced by the real name when the optional metadata carries COLUMN_NAME
    col.name = "col_" + std::to_string(i);
    metadata.columns.push_back(col);
  }

  // Parse metadata length (packed integer)
  if (remaining > 0) {
    size_t metadata_len_packed_size = PackedIntegerSize(*ptr);
    if (remaining < metadata_len_packed_size) {
      mygram::utils::StructuredLog()
          .Event("binlog_parse_error")
          .Field("function", "ParseTableMapEvent")
          .Field("reason", "truncated_metadata_length")
          .Field("remaining", static_cast<uint64_t>(remaining))
          .Field("required", static_cast<uint64_t>(metadata_len_packed_size))
          .Error();
      return malformed();
    }
    const unsigned char* ptr_before_meta_len = ptr;
    uint64_t metadata_len = 0;
    if (!binlog_util::read_packed_integer(&ptr, ptr + remaining, &metadata_len)) {
      return malformed();
    }
    size_t meta_len_size = ptr - ptr_before_meta_len;

    // SECURITY: Update remaining and validate metadata length
    if (remaining < meta_len_size) {
      return malformed();
    }
    remaining -= meta_len_size;

    if (remaining < metadata_len) {
      return malformed();
    }

    const unsigned char* metadata_start = ptr;
    const unsigned char* metadata_end = metadata_start + metadata_len;

    // Every column's entry must be read at its own width: a type that cannot
    // be sized, or a block that does not add up, would shift every later
    // column's metadata, so either fails the event.
    for (uint64_t i = 0; i < column_count; i++) {
      ColumnType type = metadata.columns[i].type;
      auto layout = TableMapMetadataLayoutFor(type);
      if (!layout) {
        mygram::utils::StructuredLog()
            .Event("binlog_parse_error")
            .Field("function", "ParseTableMapEvent")
            .Field("reason", "unknown_column_type")
            .Field("column_index", i)
            .Field("column_type", static_cast<int64_t>(type))
            .Error();
        // The frame is intact but a column cannot be sized, so every replay
        // stops here the same way.
        return mygram::utils::MakeUnexpected(mygram::utils::MakeError(
            mygram::utils::ErrorCode::kMySQLUndecodableBinlogEvent,
            "TABLE_MAP_EVENT column " + std::to_string(i) + " has type code " +
                std::to_string(static_cast<unsigned>(type)) + ", which this build cannot size"));
      }

      size_t width = 0;
      if (*layout == TableMapMetadataLayout::kOneByte) {
        width = 1;
      } else if (*layout != TableMapMetadataLayout::kNone) {
        width = 2;
      }
      if (static_cast<size_t>(metadata_end - ptr) < width) {
        mygram::utils::StructuredLog()
            .Event("binlog_parse_error")
            .Field("function", "ParseTableMapEvent")
            .Field("reason", "truncated_column_metadata")
            .Field("column_index", i)
            .Field("column_type", static_cast<int64_t>(type))
            .Error();
        return malformed();
      }

      switch (*layout) {
        case TableMapMetadataLayout::kNone:
          metadata.columns[i].metadata = 0;
          break;
        case TableMapMetadataLayout::kOneByte:
          metadata.columns[i].metadata = ptr[0];
          break;
        case TableMapMetadataLayout::kTwoByteLittleEndian:
          metadata.columns[i].metadata = binlog_util::uint2korr(ptr);
          break;
        case TableMapMetadataLayout::kTwoByteHighFirst:
          metadata.columns[i].metadata = static_cast<uint16_t>((ptr[0] << 8) | ptr[1]);
          break;
      }
      ptr += width;
    }

    if (ptr != metadata_end) {
      mygram::utils::StructuredLog()
          .Event("binlog_parse_error")
          .Field("function", "ParseTableMapEvent")
          .Field("reason", "column_metadata_length_mismatch")
          .Field("declared", metadata_len)
          .Field("consumed", static_cast<uint64_t>(ptr - metadata_start))
          .Error();
      return malformed();
    }
  }

  // Parse NULL bitmap. TABLE_MAP always carries one bit per column after the
  // metadata block; do not treat trailing checksum bytes as a substitute.
  if (column_count > 0) {
    size_t null_bitmap_size = binlog_util::bitmap_bytes(column_count);
    if (ptr + null_bitmap_size <= event_end) {
      for (uint64_t i = 0; i < column_count; i++) {
        metadata.columns[i].is_nullable = binlog_util::bitmap_is_set(ptr, i);
      }
      ptr += null_bitmap_size;
    } else {
      mygram::utils::StructuredLog()
          .Event("binlog_parse_error")
          .Field("function", "ParseTableMapEvent")
          .Field("reason", "truncated_null_bitmap")
          .Field("remaining", static_cast<uint64_t>(event_end - ptr))
          .Field("required", static_cast<uint64_t>(null_bitmap_size))
          .Error();
      return malformed();
    }
  }

  // Whatever follows the NULL bitmap is the optional-metadata block. A table
  // nobody decodes rows for does not need it read.
  if (!wants_row_metadata || wants_row_metadata(metadata)) {
    if (!ParseTableMapOptionalMetadata(ptr, event_end, metadata)) {
      mygram::utils::StructuredLog()
          .Event("binlog_parse_error")
          .Field("function", "ParseTableMapEvent")
          .Field("reason", "malformed_optional_metadata")
          .Field("remaining", static_cast<uint64_t>(event_end - ptr))
          .Error();
      // The frame passed its checksum, so every replay delivers the same bytes.
      return mygram::utils::MakeUnexpected(
          mygram::utils::MakeError(mygram::utils::ErrorCode::kMySQLUndecodableBinlogEvent,
                                   "TABLE_MAP_EVENT for " + metadata.database_name + "." + metadata.table_name +
                                       " carries optional row metadata that does not parse"));
    }
  }

  mygram::utils::StructuredLog()
      .Event("binlog_debug")
      .Field("action", "table_map_complete")
      .Field("database", metadata.database_name)
      .Field("table", metadata.table_name)
      .Field("table_id", metadata.table_id)
      .Field("columns", column_count)
      .Debug();

  metadata.RebuildColumnOrdinals();
  return metadata;
}

std::optional<QueryEventData> BinlogEventParser::ExtractQueryString(const unsigned char* buffer, unsigned long length) {
  if ((buffer == nullptr) || length < mygram::constants::kBinlogEventHeaderLen) {
    // Minimum: header bytes
    return {};
  }

  // QUERY_EVENT format (after 19-byte common header):
  // thread_id (4 bytes)
  // query_exec_time (4 bytes)
  // db_len (1 byte)
  // error_code (2 bytes)
  // status_vars_len (2 bytes)
  // [status_vars (variable length)]
  // [db_name (variable length, null-terminated)]
  // [query (variable length)]

  // Event length includes a 4-byte CRC32 checksum at the end.
  // CRC32 is verified by the binlog reader before dispatch.
  // Subtract checksum size to get the actual data boundary.
  size_t effective_length = (length > mygram::constants::kBinlogChecksumSize + mygram::constants::kBinlogEventHeaderLen)
                                ? length - mygram::constants::kBinlogChecksumSize
                                : length;

  const unsigned char* pos = buffer + mygram::constants::kBinlogEventHeaderLen;  // Skip common header
  size_t remaining = effective_length - mygram::constants::kBinlogEventHeaderLen;

  if (remaining < kQueryEventFixedFieldsSize) {  // Minimum: 4+4+1+2+2
    return {};
  }

  // Skip thread_id (4 bytes)
  pos += 4;
  remaining -= 4;

  // Skip query_exec_time (4 bytes)
  pos += 4;
  remaining -= 4;

  // Get db_len (1 byte)
  uint8_t db_len = *pos;
  pos += 1;
  remaining -= 1;

  // Skip error_code (2 bytes)
  pos += 2;
  remaining -= 2;

  // Get status_vars_len (2 bytes, little-endian)
  uint16_t status_vars_len = pos[0] | (pos[1] << 8);
  pos += 2;
  remaining -= 2;

  // Status variables carry the sql_mode the statement was logged under
  if (remaining < status_vars_len) {
    return {};
  }
  const std::optional<QueryLexOptions> lex_options = StatusVarsLexOptions(pos, pos + status_vars_len);
  pos += status_vars_len;
  remaining -= status_vars_len;

  // Extract db_name (null-terminated)
  if (remaining < static_cast<size_t>(db_len) + 1) {  // +1 for null terminator
    return {};
  }
  std::string database(reinterpret_cast<const char*>(pos), db_len);
  pos += db_len + 1;
  remaining -= (db_len + 1);

  // Extract query string
  if (remaining == 0) {
    return {};
  }

  std::string query(reinterpret_cast<const char*>(pos), remaining);
  return QueryEventData{std::move(database), std::move(query), lex_options.value_or(QueryLexOptions{}),
                        lex_options.has_value()};
}

BinlogEventParser::QueryTransactionBoundary BinlogEventParser::ClassifyQueryTransactionBoundary(
    std::string_view query, QueryLexOptions lex_options) {
  const auto statements = ClassifyQueryEvent(query, lex_options);
  if (statements.empty()) {
    return QueryTransactionBoundary::kNone;
  }
  switch (statements.front().transaction) {
    case TransactionControl::kXa:
      // XA PREPARE is logged as a separate transaction group and may later be
      // rolled back. The current materializer has no prepared-state staging,
      // so every XA form is rejected by the reader before any row event can be
      // published.
      return QueryTransactionBoundary::kUnsupportedXa;
    case TransactionControl::kBegin:
      return QueryTransactionBoundary::kBegin;
    case TransactionControl::kEnd:
      return QueryTransactionBoundary::kEnd;
    case TransactionControl::kNone:
      return QueryTransactionBoundary::kNone;
  }
  return QueryTransactionBoundary::kNone;
}

bool BinlogEventParser::IsMariaDBGtidTransactionOpen(uint8_t flags) {
  // MariaDB Log_event::FL_STANDALONE. When clear, the GTID starts an event
  // group whose applied position must not advance until COMMIT/XID.
  return (flags & MariaDBEventParser::kStandaloneFlag) == 0U;
}

bool BinlogEventParser::IsRowsStatementEnd(const unsigned char* buffer, unsigned long length) {
  constexpr size_t kRowsEventFlagsOffset = mygram::constants::kBinlogEventHeaderLen + 6;
  constexpr size_t kRowsEventFlagsEnd = kRowsEventFlagsOffset + 2;
  constexpr uint16_t kStatementEndFlag = 0x0001;
  if (buffer == nullptr || length < kRowsEventFlagsEnd) {
    return false;
  }

  const auto event_type = static_cast<MySQLBinlogEventType>(buffer[4]);
  switch (event_type) {
    case MySQLBinlogEventType::OBSOLETE_WRITE_ROWS_EVENT_V1:
    case MySQLBinlogEventType::OBSOLETE_UPDATE_ROWS_EVENT_V1:
    case MySQLBinlogEventType::OBSOLETE_DELETE_ROWS_EVENT_V1:
    case MySQLBinlogEventType::WRITE_ROWS_EVENT:
    case MySQLBinlogEventType::UPDATE_ROWS_EVENT:
    case MySQLBinlogEventType::DELETE_ROWS_EVENT:
    case MySQLBinlogEventType::PARTIAL_UPDATE_ROWS_EVENT:
      break;
    default:
      return false;
  }

  const uint16_t flags = static_cast<uint16_t>(buffer[kRowsEventFlagsOffset]) |
                         (static_cast<uint16_t>(buffer[kRowsEventFlagsOffset + 1]) << 8);
  return (flags & kStatementEndFlag) != 0U;
}

std::vector<QueryLexOptions> BinlogEventParser::LexModes(const QueryEventData& query) {
  if (query.sql_mode_known) {
    return {query.lex_options};
  }
  return {QueryLexOptions{false, false}, QueryLexOptions{true, false}, QueryLexOptions{false, true},
          QueryLexOptions{true, true}};
}

BinlogEventParser::QueryTransactionBoundary BinlogEventParser::ClassifyQueryTransactionBoundary(
    const QueryEventData& query) {
  QueryTransactionBoundary result = QueryTransactionBoundary::kNone;
  for (const QueryLexOptions& mode : LexModes(query)) {
    const auto boundary = ClassifyQueryTransactionBoundary(query.query, mode);
    // kUnsupportedXa > kBegin > kEnd > kNone
    const auto rank = [](QueryTransactionBoundary value) {
      switch (value) {
        case QueryTransactionBoundary::kUnsupportedXa:
          return 3;
        case QueryTransactionBoundary::kBegin:
          return 2;
        case QueryTransactionBoundary::kEnd:
          return 1;
        case QueryTransactionBoundary::kNone:
          return 0;
      }
      return 0;
    };
    if (rank(boundary) > rank(result)) {
      result = boundary;
    }
  }
  return result;
}

bool BinlogEventParser::IsSafeIgnoredQuery(const QueryEventData& query,
                                           const std::vector<std::string>& ignored_prefixes) {
  const auto modes = LexModes(query);
  return std::all_of(modes.begin(), modes.end(), [&](const QueryLexOptions& mode) {
    return IsSafeIgnoredQuery(query.query, ignored_prefixes, mode);
  });
}

bool BinlogEventParser::IsSafeIgnoredQuery(std::string_view query, const std::vector<std::string>& ignored_prefixes,
                                           QueryLexOptions lex_options) {
  std::vector<std::vector<SqlToken>> prefixes;
  for (const auto& prefix : ignored_prefixes) {
    auto lexed = LexQueryStatements(prefix, lex_options);
    if (lexed.size() == 1) {
      prefixes.push_back(std::move(lexed.front()));
    }
  }

  for (const auto& tokens : LexQueryStatements(query, lex_options)) {
    if (std::any_of(prefixes.begin(), prefixes.end(),
                    [&tokens](const auto& prefix) { return StartsWithStatementPrefix(tokens, prefix); })) {
      continue;
    }
    const DdlStatement statement = ClassifyStatement(tokens);
    // A table DDL whose names cannot be read may name a configured table.
    if (statement.malformed) {
      return false;
    }
    // Only verbs that cannot change rows without row events are safe; anything
    // else (DML, WITH, SELECT or DO of a data-changing function, CALL) fails
    // closed. Configured-table and configured-schema DDL was emitted above this point.
    const std::string& verb = statement.verb;
    if (verb == "FLUSH") {
      if (statement.object != "PRIVILEGES") {
        return false;
      }
      continue;
    }
    // MariaDB SET STATEMENT ... FOR runs the statement that follows.
    if (verb == "SET") {
      if (statement.object == "STATEMENT") {
        return false;
      }
      continue;
    }
    static constexpr std::array<std::string_view, 17> kDataNeutralVerbs = {
        "ALTER",   "ANALYZE", "BEGIN",  "COMMIT",   "CREATE",    "DROP",  "GRANT",    "INSTALL",  "OPTIMIZE",
        "RELEASE", "RENAME",  "REVOKE", "ROLLBACK", "SAVEPOINT", "START", "TRUNCATE", "UNINSTALL"};
    if (std::find(kDataNeutralVerbs.begin(), kDataNeutralVerbs.end(), verb) == kDataNeutralVerbs.end()) {
      return false;
    }
  }
  return true;
}

DDLType BinlogEvent::ClassifyDDL(const std::string& query, const config::TableConfig& table,
                                 QueryLexOptions lex_options, bool names_case_insensitive) {
  return FindConfiguredTableDDL(ClassifyQueryEvent(query, lex_options), table.database, table, names_case_insensitive)
      .value_or(DDLType::kUnknown);
}

bool BinlogEventParser::IsTableAffectingDDL(const std::string& query, const std::string& table_name,
                                            QueryLexOptions lex_options, bool names_case_insensitive) {
  config::TableConfig table_config;
  table_config.name = table_name;
  return FindConfiguredTableDDL(ClassifyQueryEvent(query, lex_options), "", table_config, names_case_insensitive)
      .has_value();
}

}  // namespace mygramdb::mysql

// NOLINTEND(cppcoreguidelines-pro-*,cppcoreguidelines-avoid-*,readability-magic-numbers)

#endif  // USE_MYSQL
