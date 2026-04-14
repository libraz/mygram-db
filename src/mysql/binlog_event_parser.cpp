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
#include <cctype>
#include <cstdio>

#include "mysql/binlog_event_types.h"
#include "mysql/binlog_filter_evaluator.h"
#include "mysql/binlog_util.h"
#include "mysql/rows_parser.h"
#include "server/tcp_server.h"  // For TableContext definition
#include "utils/constants.h"
#include "utils/sql_utils.h"
#include "utils/structured_log.h"

// NOLINTBEGIN(cppcoreguidelines-pro-*,cppcoreguidelines-avoid-*,readability-magic-numbers)

namespace mygramdb::mysql {

namespace {

// ============================================================================
// Binlog event size constants
// ============================================================================

/// Minimum length for a standard GTID_LOG_EVENT
constexpr size_t kGTIDEventMinLength = 42;

/// Minimum length for a GTID_TAGGED_LOG_EVENT (MySQL 8.4+)
constexpr size_t kTaggedGTIDEventMinLength = 46;

/// Size of the fixed-length fields in a QUERY_EVENT (thread_id + exec_time +
/// db_len + error_code + status_vars_len = 4+4+1+2+2 = 13 bytes)
constexpr size_t kQueryEventFixedFieldsSize = 13;

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
  std::string text_column;
  bool use_concat = false;
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
 * @param table_metadata_cache Cache of table metadata
 * @param table_contexts Map of table contexts (multi-table mode)
 * @param table_config Single table config (single-table mode)
 * @param multi_table_mode Whether multi-table mode is enabled
 * @param event_type_name Name of event type for logging (e.g., "write", "update", "delete")
 * @return Optional context, nullopt if table not found or not monitored
 */
std::optional<RowsEventContext> InitRowsEventContext(
    const unsigned char* buffer, TableMetadataCache& table_metadata_cache,
    const std::unordered_map<std::string, server::TableContext*>& table_contexts,
    const config::TableConfig* table_config, bool multi_table_mode, const std::string& event_type_name) {
  RowsEventContext ctx;

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
    auto table_iter = table_contexts.find(ctx.table_meta->table_name);
    if (table_iter == table_contexts.end()) {
      mygram::utils::StructuredLog()
          .Event("binlog_debug")
          .Field("action", "table_not_monitored_" + event_type_name)
          .Field("table", ctx.table_meta->table_name)
          .Debug();
      return std::nullopt;
    }
    ctx.current_config = &table_iter->second->config;
  } else {
    ctx.current_config = table_config;
  }

  // Determine text column(s)
  if (!ctx.current_config->text_source.column.empty()) {
    ctx.text_column = ctx.current_config->text_source.column;
  } else if (!ctx.current_config->text_source.concat.empty()) {
    ctx.text_column = ctx.current_config->text_source.concat[0];  // Use first for initial parse
    ctx.use_concat = true;
  } else {
    ctx.text_column = "";
  }

  return ctx;
}

/**
 * @brief Concatenate text from multiple columns in a row
 *
 * @param row Row data containing columns
 * @param concat_columns List of column names to concatenate
 * @return Concatenated text with space separators
 */
std::string ConcatenateTextColumns(const RowData& row, const std::vector<std::string>& concat_columns) {
  std::string result;
  size_t total_size = 0;
  for (const auto& col_name : concat_columns) {
    auto col_iter = row.columns.find(col_name);
    if (col_iter != row.columns.end() && !col_iter->second.empty()) {
      total_size += col_iter->second.size() + 1;  // +1 for separator
    }
  }
  result.reserve(total_size);
  for (const auto& col_name : concat_columns) {
    auto col_iter = row.columns.find(col_name);
    if (col_iter != row.columns.end() && !col_iter->second.empty()) {
      if (!result.empty()) {
        result += " ";  // Space separator between columns
      }
      result += col_iter->second;
    }
  }
  return result;
}

/**
 * @brief Get text value from row, handling concat mode
 *
 * @param row Row data
 * @param ctx Rows event context
 * @return Text value (concatenated if use_concat is true)
 */
inline std::string GetRowText(const RowData& row, const RowsEventContext& ctx) {
  if (ctx.use_concat && !ctx.current_config->text_source.concat.empty()) {
    return ConcatenateTextColumns(row, ctx.current_config->text_source.concat);
  }
  return row.text;
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

}  // namespace

std::vector<BinlogEvent> BinlogEventParser::ParseBinlogEvent(
    const unsigned char* buffer, unsigned long length, const std::string& current_gtid,
    TableMetadataCache& table_metadata_cache,
    const std::unordered_map<std::string, server::TableContext*>& table_contexts,
    const config::TableConfig* table_config, bool multi_table_mode, const std::string& datetime_timezone) {
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
      auto ctx_opt =
          InitRowsEventContext(buffer, table_metadata_cache, table_contexts, table_config, multi_table_mode, "write");
      if (!ctx_opt) {
        return {};
      }
      const auto& ctx = *ctx_opt;

      auto rows_result = ParseWriteRowsEvent(buffer, length, ctx.table_meta, ctx.current_config->primary_key,
                                             ctx.text_column, event_type);

      if (!rows_result || rows_result->empty()) {
        return {};
      }

      // Create events for ALL rows (multi-row event support)
      std::vector<BinlogEvent> events;
      events.reserve(rows_result->size());

      for (const auto& row : *rows_result) {
        BinlogEvent event;
        event.type = BinlogEventType::INSERT;
        event.table_name = ctx.table_meta->table_name;
        event.primary_key = row.primary_key;
        event.text = GetRowText(row, ctx);
        event.gtid = current_gtid;

        // Extract all filters (required + optional) from row data
        event.filters = BinlogFilterEvaluator::ExtractAllFilters(row, *ctx.current_config, datetime_timezone);

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
      auto ctx_opt =
          InitRowsEventContext(buffer, table_metadata_cache, table_contexts, table_config, multi_table_mode, "update");
      if (!ctx_opt) {
        return {};
      }
      const auto& ctx = *ctx_opt;

      // Parse rows using rows_parser
      auto row_pairs_result = ParseUpdateRowsEvent(buffer, length, ctx.table_meta, ctx.current_config->primary_key,
                                                   ctx.text_column, event_type);

      if (!row_pairs_result || row_pairs_result->empty()) {
        return {};
      }

      // Create events for ALL row pairs (multi-row event support)
      std::vector<BinlogEvent> events;
      events.reserve(row_pairs_result->size());

      for (const auto& row_pair : *row_pairs_result) {
        const auto& before_row = row_pair.first;  // Before image
        const auto& after_row = row_pair.second;  // After image

        BinlogEvent event;
        event.type = BinlogEventType::UPDATE;
        event.table_name = ctx.table_meta->table_name;
        event.primary_key = after_row.primary_key;
        event.text = GetRowText(after_row, ctx);       // New text (after image)
        event.old_text = GetRowText(before_row, ctx);  // Old text (before image) for index update
        event.gtid = current_gtid;

        // Extract all filters from after image
        event.filters = BinlogFilterEvaluator::ExtractAllFilters(after_row, *ctx.current_config, datetime_timezone);

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
      auto ctx_opt =
          InitRowsEventContext(buffer, table_metadata_cache, table_contexts, table_config, multi_table_mode, "delete");
      if (!ctx_opt) {
        return {};
      }
      const auto& ctx = *ctx_opt;

      // Parse rows using rows_parser
      auto rows_result = ParseDeleteRowsEvent(buffer, length, ctx.table_meta, ctx.current_config->primary_key,
                                              ctx.text_column, event_type);

      if (!rows_result || rows_result->empty()) {
        return {};
      }

      // Create events for ALL rows (multi-row event support)
      std::vector<BinlogEvent> events;
      events.reserve(rows_result->size());

      for (const auto& row : *rows_result) {
        BinlogEvent event;
        event.type = BinlogEventType::DELETE;
        event.table_name = ctx.table_meta->table_name;
        event.primary_key = row.primary_key;
        event.text = GetRowText(row, ctx);
        event.gtid = current_gtid;

        // Extract all filters from row data (before image for DELETE)
        event.filters = BinlogFilterEvaluator::ExtractAllFilters(row, *ctx.current_config, datetime_timezone);

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
      auto query_opt = ExtractQueryString(buffer, length);
      if (!query_opt) {
        return {};
      }

      std::string query = query_opt.value();
      mygram::utils::StructuredLog().Event("binlog_debug").Field("action", "query_event").Field("query", query).Debug();

      // Classify DDL type once for all events
      DDLType classified_ddl_type = BinlogEvent::ClassifyDDL(query);

      // Check if this affects any of our target tables
      if (multi_table_mode) {
        // Multi-table mode: check all registered tables
        std::vector<BinlogEvent> events;
        for (const auto& [table_name, ctx] : table_contexts) {
          if (IsTableAffectingDDL(query, table_name)) {
            BinlogEvent event;
            event.type = BinlogEventType::DDL;
            event.ddl_type = classified_ddl_type;
            event.table_name = table_name;
            event.text = query;  // Store the DDL query
            events.push_back(std::move(event));
          }
        }
        if (!events.empty()) {
          return events;
        }
      } else {
        // Single-table mode: check only our configured table
        if (table_config != nullptr && IsTableAffectingDDL(query, table_config->name)) {
          BinlogEvent event;
          event.type = BinlogEventType::DDL;
          event.ddl_type = classified_ddl_type;
          event.table_name = table_config->name;
          event.text = query;  // Store the DDL query
          return {event};      // Return as vector with single element
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

    case MySQLBinlogEventType::PARTIAL_UPDATE_ROWS_EVENT:
      // binlog_row_value_options=PARTIAL_JSON causes partial updates.
      // Not supported - log warning and skip.
      mygram::utils::StructuredLog()
          .Event("binlog_warning")
          .Field("type", "unsupported_event")
          .Field("event_type", "PARTIAL_UPDATE_ROWS_EVENT")
          .Field("message",
                 "PARTIAL_UPDATE_ROWS_EVENT is not supported. "
                 "JSON column updates may be lost.")
          .Warn();
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
      // Ignore other event types
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
    // Minimum: 19 header + 1 commit_flag + 10 field1 + 2 field2_hdr + 16 UUID
    //          + 1 tag_len = 49, but 46 is a reasonable lower bound check
    return {};
  }

  // GTID_TAGGED_LOG_EVENT uses the MySQL 8.4 serialization framework.
  // After the 19-byte common header and 1-byte commit_flag, fields are encoded as:
  //   field_id(1B) + type_id(1B) + data
  //
  // type_id=1 (FIELD_TYPE_UINT): fixed 8-byte little-endian uint64
  // type_id=2 (FIELD_TYPE_VARIABLE_LENGTH): variable-length payload
  //
  // Known fields:
  //   Field 0 (COMMIT_GROUP_TICKET): type=1, uint64
  //   Field 1 (TSID): type=2, UUID(16B) + tag_len(1B) + tag_bytes
  //   Field 2 (SPEC): type=1, GNO as uint64
  //   Field 3 (COMMIT_TIMESTAMP): type=2, original(7B) + immediate(7B)

  const unsigned char* pos = buffer + mygram::constants::kBinlogEventHeaderLen;  // Skip common header
  const unsigned char* buf_end = buffer + length;

  if (pos >= buf_end) {
    return {};
  }
  pos++;  // Skip commit_flag

  // Parse serialization framework fields
  const unsigned char* uuid_ptr = nullptr;
  uint64_t gno = 0;
  std::string tag;
  bool found_uuid = false;
  bool found_gno = false;

  while (pos + 2 <= buf_end) {
    uint8_t field_id = *pos++;
    uint8_t type_id = *pos++;

    if (type_id == 1) {
      // Fixed-size uint64 field (8 bytes)
      if (pos + 8 > buf_end) {
        break;
      }
      if (field_id == 2) {
        // SPEC field: GNO
        gno = 0;
        for (int i = 0; i < 8; i++) {
          gno |= static_cast<uint64_t>(pos[i]) << (i * 8);
        }
        found_gno = true;
      }
      pos += 8;
    } else if (type_id == 2) {
      // Variable-length field
      if (field_id == 1) {
        // TSID field: UUID(16B) + tag_len(1B) + tag
        if (pos + 17 > buf_end) {
          break;
        }
        uuid_ptr = pos;
        found_uuid = true;
        pos += 16;  // Skip UUID
        uint8_t tag_len = *pos++;
        if (tag_len > 0) {
          if (pos + tag_len > buf_end) {
            break;
          }
          tag = std::string(reinterpret_cast<const char*>(pos), tag_len);
          pos += tag_len;
        }
      } else if (field_id == 3) {
        // COMMIT_TIMESTAMP: original(7B) + immediate(7B) = 14B
        if (pos + 14 > buf_end) {
          break;
        }
        pos += 14;
      } else {
        // Unknown variable-length field - cannot determine size, stop parsing
        break;
      }
    } else {
      // Unknown type - stop parsing
      break;
    }

    if (found_uuid && found_gno) {
      break;
    }
  }

  if (!found_uuid || !found_gno || uuid_ptr == nullptr) {
    return {};
  }

  // Format as "UUID:TAG:GNO" or "UUID:GNO"
  std::string gtid = FormatUUID(uuid_ptr);
  if (!tag.empty()) {
    gtid += ":" + tag;
  }
  gtid += ":" + std::to_string(gno);
  return gtid;
}

std::optional<TableMetadata> BinlogEventParser::ParseTableMapEvent(const unsigned char* buffer, unsigned long length) {
  if ((buffer == nullptr) || length < 8) {
    // Minimum TABLE_MAP event size (6 bytes table_id + 2 bytes flags)
    mygram::utils::StructuredLog()
        .Event("binlog_parse_error")
        .Field("function", "ParseTableMapEvent")
        .Field("reason", "buffer_null_or_too_short")
        .Field("length", static_cast<uint64_t>(length))
        .Error();
    return {};
  }

  TableMetadata metadata;

  // binlog_reader already skipped OK byte, so buffer points to event data.
  // Standard binlog event header: LOG_EVENT_HEADER_LEN = 19 bytes
  //   [timestamp(4)][type(1)][server_id(4)][event_size(4)][log_pos(4)][flags(2)]
  // (see mysql-8.4.7/libs/mysql/binlog/event/binlog_event.h)
  const unsigned char* ptr = buffer + mygram::constants::kBinlogEventHeaderLen;
  unsigned long remaining = length - mygram::constants::kBinlogEventHeaderLen;

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
    return {};
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
    return {};
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
    return {};
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
    return {};
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
    return {};
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
    return {};
  }

  // Parse column count (packed integer)
  const unsigned char* ptr_before_packed = ptr;
  uint64_t column_count = binlog_util::read_packed_integer(&ptr);

  // SECURITY: Update remaining bytes after reading packed integer
  size_t packed_int_size = ptr - ptr_before_packed;
  if (remaining < packed_int_size) {
    return {};
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
    return {};
  }

  if (remaining < column_count) {
    return {};
  }

  // Parse column types (1 byte per column)
  metadata.columns.reserve(column_count);
  for (uint64_t i = 0; i < column_count; i++) {
    if (remaining < 1) {
      return {};
    }
    ColumnMetadata col;
    col.type = static_cast<ColumnType>(*ptr++);
    remaining--;
    col.metadata = 0;
    col.is_nullable = false;
    col.is_unsigned = false;
    // Column name is not available in TABLE_MAP event
    // Use column index as temporary name
    col.name = "col_" + std::to_string(i);
    metadata.columns.push_back(col);
  }

  // Parse metadata length (packed integer)
  if (remaining > 0) {
    const unsigned char* ptr_before_meta_len = ptr;
    uint64_t metadata_len = binlog_util::read_packed_integer(&ptr);
    size_t meta_len_size = ptr - ptr_before_meta_len;

    // SECURITY: Update remaining and validate metadata length
    if (remaining < meta_len_size) {
      return {};
    }
    remaining -= meta_len_size;

    if (remaining < metadata_len) {
      return {};
    }

    const unsigned char* metadata_start = ptr;
    const unsigned char* metadata_end = metadata_start + metadata_len;

    // Parse type-specific metadata for each column
    for (uint64_t i = 0; i < column_count && ptr < metadata_end; i++) {
      ColumnType type = metadata.columns[i].type;

      switch (type) {
        case ColumnType::VARCHAR:
        case ColumnType::VAR_STRING:
          // 2 bytes: max length
          if (ptr + 2 <= metadata_end) {
            metadata.columns[i].metadata = binlog_util::uint2korr(ptr);
            ptr += 2;
          }
          break;

        case ColumnType::BLOB:
        case ColumnType::TINY_BLOB:
        case ColumnType::MEDIUM_BLOB:
        case ColumnType::LONG_BLOB:
        case ColumnType::VECTOR:
          // 1 byte: number of length bytes (1, 2, 3, or 4)
          if (ptr + 1 <= metadata_end) {
            metadata.columns[i].metadata = *ptr;
            ptr += 1;
          }
          break;

        case ColumnType::STRING:
          // 2 bytes: (real_type << 8) | max_length
          if (ptr + 2 <= metadata_end) {
            metadata.columns[i].metadata = binlog_util::uint2korr(ptr);
            ptr += 2;
          }
          break;

        case ColumnType::FLOAT:
        case ColumnType::DOUBLE:
          // 1 byte: pack length
          if (ptr + 1 <= metadata_end) {
            metadata.columns[i].metadata = *ptr;
            ptr += 1;
          }
          break;

        case ColumnType::NEWDECIMAL:
          // 2 bytes: (precision << 8) | scale
          if (ptr + 2 <= metadata_end) {
            metadata.columns[i].metadata = binlog_util::uint2korr(ptr);
            ptr += 2;
          }
          break;

        case ColumnType::BIT:
          // 2 bytes: (bytes << 8) | bits
          if (ptr + 2 <= metadata_end) {
            metadata.columns[i].metadata = binlog_util::uint2korr(ptr);
            ptr += 2;
          }
          break;

        case ColumnType::TIMESTAMP2:
        case ColumnType::DATETIME2:
        case ColumnType::TIME2:
          // 1 byte: fractional seconds precision (0-6)
          if (ptr + 1 <= metadata_end) {
            metadata.columns[i].metadata = *ptr;
            ptr += 1;
          }
          break;

        case ColumnType::ENUM:
        case ColumnType::SET:
          // 2 bytes: number of elements
          if (ptr + 2 <= metadata_end) {
            metadata.columns[i].metadata = binlog_util::uint2korr(ptr);
            ptr += 2;
          }
          break;

        // Types with no metadata
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
          // No metadata for these types
          metadata.columns[i].metadata = 0;
          break;

        default:
          // Unknown type - skip metadata
          mygram::utils::StructuredLog()
              .Event("mysql_binlog_warning")
              .Field("type", "unknown_column_type")
              .Field("column_type", static_cast<int64_t>(type))
              .Warn();
          break;
      }
    }

    // Skip to end of metadata block
    ptr = metadata_start + metadata_len;
  }

  // Parse NULL bitmap if present
  if (ptr < buffer + length) {
    size_t null_bitmap_size = binlog_util::bitmap_bytes(column_count);
    if (ptr + null_bitmap_size <= buffer + length) {
      for (uint64_t i = 0; i < column_count; i++) {
        metadata.columns[i].is_nullable = binlog_util::bitmap_is_set(ptr, i);
      }
      ptr += null_bitmap_size;
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

  return metadata;
}

std::optional<std::string> BinlogEventParser::ExtractQueryString(const unsigned char* buffer, unsigned long length) {
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

  // Skip status_vars
  if (remaining < status_vars_len) {
    return {};
  }
  pos += status_vars_len;
  remaining -= status_vars_len;

  // Skip db_name (null-terminated)
  if (remaining < static_cast<size_t>(db_len) + 1) {  // +1 for null terminator
    return {};
  }
  pos += db_len + 1;
  remaining -= (db_len + 1);

  // Extract query string
  if (remaining == 0) {
    return {};
  }

  std::string query(reinterpret_cast<const char*>(pos), remaining);
  return query;
}

/**
 * @brief Check if a single DDL statement affects a specific table
 *
 * @param query_upper Uppercase normalized query (single statement)
 * @param table_upper Uppercase table name
 * @return true if the statement affects the table
 */
bool IsSingleStatementAffectingTable(const std::string& query_upper, const std::string& table_upper) {
  size_t pos = 0;

  // Skip leading whitespace
  if (!mygram::utils::SkipWhitespace(query_upper, pos) || query_upper.empty()) {
    return false;
  }

  // Check for TRUNCATE TABLE
  size_t saved_start = pos;
  if (mygram::utils::MatchKeyword(query_upper, pos, "TRUNCATE")) {
    if (mygram::utils::SkipWhitespace(query_upper, pos) && mygram::utils::MatchKeyword(query_upper, pos, "TABLE")) {
      if (mygram::utils::SkipWhitespace(query_upper, pos) &&
          mygram::utils::MatchTableName(query_upper, pos, table_upper)) {
        return true;
      }
    }
  }

  // Reset position and check for DROP TABLE [IF EXISTS]
  pos = saved_start;
  if (mygram::utils::MatchKeyword(query_upper, pos, "DROP")) {
    if (mygram::utils::SkipWhitespace(query_upper, pos) && mygram::utils::MatchKeyword(query_upper, pos, "TABLE")) {
      if (mygram::utils::SkipWhitespace(query_upper, pos)) {
        // Check for optional "IF EXISTS"
        size_t saved_pos = pos;
        if (mygram::utils::MatchKeyword(query_upper, pos, "IF")) {
          if (mygram::utils::SkipWhitespace(query_upper, pos) &&
              mygram::utils::MatchKeyword(query_upper, pos, "EXISTS")) {
            mygram::utils::SkipWhitespace(query_upper, pos);
          } else {
            // "IF" without "EXISTS" - restore position
            pos = saved_pos;
          }
        }

        // Match table name
        if (mygram::utils::MatchTableName(query_upper, pos, table_upper)) {
          return true;
        }
      }
    }
  }

  // Reset position and check for ALTER TABLE
  pos = saved_start;
  if (mygram::utils::MatchKeyword(query_upper, pos, "ALTER")) {
    if (mygram::utils::SkipWhitespace(query_upper, pos) && mygram::utils::MatchKeyword(query_upper, pos, "TABLE")) {
      if (mygram::utils::SkipWhitespace(query_upper, pos) &&
          mygram::utils::MatchTableName(query_upper, pos, table_upper)) {
        return true;
      }
    }
  }

  // Reset position and check for RENAME TABLE
  // Syntax: RENAME TABLE tbl1 TO tbl2 [, tbl3 TO tbl4 ...]
  pos = saved_start;
  if (mygram::utils::MatchKeyword(query_upper, pos, "RENAME")) {
    if (mygram::utils::SkipWhitespace(query_upper, pos) && mygram::utils::MatchKeyword(query_upper, pos, "TABLE")) {
      while (mygram::utils::SkipWhitespace(query_upper, pos)) {
        // Try to match source table name
        size_t saved_pos = pos;
        bool source_match = mygram::utils::MatchTableName(query_upper, pos, table_upper);
        if (!source_match) {
          // MatchTableName may have partially advanced pos; restore and skip
          pos = saved_pos;
          if (pos < query_upper.size() && query_upper[pos] == '`') {
            size_t end_tick = query_upper.find('`', pos + 1);
            if (end_tick != std::string::npos) {
              pos = end_tick + 1;
            } else {
              break;
            }
          } else {
            while (pos < query_upper.size() && std::isalnum(static_cast<unsigned char>(query_upper[pos])) == 0 &&
                   query_upper[pos] != '_') {
              break;
            }
            while (pos < query_upper.size() &&
                   (std::isalnum(static_cast<unsigned char>(query_upper[pos])) != 0 || query_upper[pos] == '_')) {
              ++pos;
            }
          }
        }

        // Expect TO keyword
        if (!mygram::utils::SkipWhitespace(query_upper, pos) || !mygram::utils::MatchKeyword(query_upper, pos, "TO")) {
          break;
        }
        if (!mygram::utils::SkipWhitespace(query_upper, pos)) {
          break;
        }

        // Try to match target table name
        saved_pos = pos;
        bool target_match = mygram::utils::MatchTableName(query_upper, pos, table_upper);

        if (source_match || target_match) {
          return true;
        }

        // Skip over non-matching target table name
        pos = saved_pos;
        if (pos < query_upper.size() && query_upper[pos] == '`') {
          size_t end_tick = query_upper.find('`', pos + 1);
          if (end_tick != std::string::npos) {
            pos = end_tick + 1;
          } else {
            break;
          }
        } else {
          while (pos < query_upper.size() &&
                 (std::isalnum(static_cast<unsigned char>(query_upper[pos])) != 0 || query_upper[pos] == '_')) {
            ++pos;
          }
        }

        // Check for comma (multi-table rename)
        mygram::utils::SkipWhitespace(query_upper, pos);
        if (pos < query_upper.size() && query_upper[pos] == ',') {
          ++pos;
        } else {
          break;
        }
      }
    }
  }

  return false;
}

bool BinlogEventParser::IsTableAffectingDDL(const std::string& query, const std::string& table_name) {
  // Strip SQL comments first
  std::string stripped_query = mygram::utils::StripSQLComments(query);

  // Convert to uppercase for case-insensitive matching
  std::string query_upper = stripped_query;
  std::string table_upper = table_name;
  std::transform(query_upper.begin(), query_upper.end(), query_upper.begin(), ::toupper);
  std::transform(table_upper.begin(), table_upper.end(), table_upper.begin(), ::toupper);

  // Normalize whitespace (replace consecutive spaces with single space)
  query_upper = mygram::utils::NormalizeWhitespace(query_upper);

  // Split by semicolon and check each statement
  size_t start = 0;
  size_t end = query_upper.find(';');

  while (start < query_upper.length()) {
    std::string statement;
    if (end == std::string::npos) {
      statement = query_upper.substr(start);
    } else {
      statement = query_upper.substr(start, end - start);
    }

    // Check if this statement affects the table
    if (IsSingleStatementAffectingTable(statement, table_upper)) {
      return true;
    }

    // Move to next statement
    if (end == std::string::npos) {
      break;
    }
    start = end + 1;
    end = query_upper.find(';', start);
  }

  return false;
}

}  // namespace mygramdb::mysql

// NOLINTEND(cppcoreguidelines-pro-*,cppcoreguidelines-avoid-*,readability-magic-numbers)

#endif  // USE_MYSQL
