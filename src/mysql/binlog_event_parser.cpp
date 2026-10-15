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
#include <cctype>
#include <cstdio>
#include <sstream>
#include <string_view>

#include "mysql/binlog_event_disposition.h"
#include "mysql/binlog_event_types.h"
#include "mysql/binlog_filter_evaluator.h"
#include "mysql/binlog_util.h"
#include "mysql/ddl_classifier.h"
#include "mysql/mariadb_event_parser.h"
#include "mysql/rows_parser.h"
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

/// Minimum length for a GTID_TAGGED_LOG_EVENT (MySQL 8.4+)
constexpr size_t kTaggedGTIDEventMinLength =
    mygram::constants::kBinlogEventHeaderLen + 1 + (2 + 8) + (2 + 16 + 1) + (2 + 8);

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

bool IsCommitStatement(std::string_view query) {
  return mygram::utils::ToUpper(mygram::utils::TrimAsciiWhitespaceView(query)) == "COMMIT";
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

struct QueryEventData {
  std::string database;
  std::string query;
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
 * @return Optional context, nullopt if table not found or not monitored
 */
std::optional<RowsEventContext> InitRowsEventContext(
    const unsigned char* buffer, unsigned long length, const TableMetadataCache& table_metadata_cache,
    const std::unordered_map<std::string, server::TableContext*>& table_contexts,
    const config::TableConfig* table_config, bool multi_table_mode, const std::string& event_type_name) {
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
    auto table_iter =
        table_contexts.find(config::QualifiedTableName(ctx.table_meta->database_name, ctx.table_meta->table_name));
    if (table_iter == table_contexts.end()) {
      table_iter = table_contexts.find(ctx.table_meta->table_name);
    }
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

  if (!ctx.current_config->database.empty() && ctx.table_meta->database_name != ctx.current_config->database) {
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
 * @brief Whether a lexed statement defines or alters a stored program
 *
 * Matches `CREATE [OR REPLACE] [DEFINER = user[@host]] [AGGREGATE]` or
 * `ALTER [DEFINER = user[@host]]` followed by PROCEDURE, FUNCTION, TRIGGER,
 * EVENT or PACKAGE. Such a statement stores a body without running it.
 */
bool DefinesStoredProgram(const std::vector<SqlToken>& tokens) {
  size_t pos = 0;
  const auto is_word = [&tokens, &pos](std::string_view word) {
    return pos < tokens.size() && tokens[pos].kind == SqlToken::Kind::kWord &&
           mygram::utils::EqualsAsciiCaseInsensitive(tokens[pos].text, word);
  };
  const auto is_punct = [&tokens, &pos](char punct) {
    return pos < tokens.size() && tokens[pos].kind == SqlToken::Kind::kPunct && tokens[pos].text[0] == punct;
  };

  const bool create = is_word("CREATE");
  if (!create && !is_word("ALTER")) {
    return false;
  }
  ++pos;
  if (create && is_word("OR")) {
    ++pos;
    if (!is_word("REPLACE")) {
      return false;
    }
    ++pos;
  }
  if (is_word("DEFINER")) {
    ++pos;
    if (!is_punct('=')) {
      return false;
    }
    pos += 2;  // user, or CURRENT_USER
    if (is_punct('(')) {
      pos += 2;  // CURRENT_USER()
    }
    if (is_punct('@')) {
      pos += 2;  // host
    }
  }
  if (create && is_word("AGGREGATE")) {
    ++pos;
  }
  return is_word("PROCEDURE") || is_word("FUNCTION") || is_word("TRIGGER") || is_word("EVENT") || is_word("PACKAGE");
}

}  // namespace

std::optional<QueryEventData> ExtractQueryEventData(const unsigned char* buffer, unsigned long length);

std::vector<BinlogEvent> BinlogEventParser::ParseBinlogEvent(
    const unsigned char* buffer, unsigned long length, const std::string& current_gtid,
    const TableMetadataCache& table_metadata_cache,
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
      auto ctx_opt = InitRowsEventContext(buffer, length, table_metadata_cache, table_contexts, table_config,
                                          multi_table_mode, "write");
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
                                          multi_table_mode, "update");
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
                                          multi_table_mode, "delete");
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
      auto query_data_opt = ExtractQueryEventData(buffer, length);
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

      if (!current_gtid.empty() && IsCommitStatement(query_data.query)) {
        BinlogEvent event;
        event.type = BinlogEventType::COMMIT;
        event.gtid = current_gtid;
        return {event};
      }

      // Check if this affects any of our target tables
      const std::vector<DdlStatement> statements = ClassifyQueryEvent(query_data.query);
      if (multi_table_mode) {
        // Multi-table mode: check all registered tables
        std::vector<BinlogEvent> events;
        for (const auto& [table_name, ctx] : table_contexts) {
          std::optional<DDLType> ddl_match;
          if (ctx != nullptr) {
            ddl_match = FindConfiguredTableDDL(statements, query_data.database, ctx->config);
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
          ddl_match = FindConfiguredTableDDL(statements, query_data.database, *table_config);
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
    // Minimum: header + commit flag + commit-ticket field + TSID field with
    // an empty tag + GNO field.
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

mygram::utils::Expected<TableMetadata, mygram::utils::Error> BinlogEventParser::ParseTableMapEvent(
    const unsigned char* buffer, unsigned long length) {
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
    // Column name is not available in TABLE_MAP event
    // Use column index as temporary name
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

std::optional<QueryEventData> ExtractQueryEventData(const unsigned char* buffer, unsigned long length) {
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
  return QueryEventData{std::move(database), std::move(query)};
}

std::optional<std::string> BinlogEventParser::ExtractQueryString(const unsigned char* buffer, unsigned long length) {
  auto data = ExtractQueryEventData(buffer, length);
  if (!data) {
    return std::nullopt;
  }
  return data->query;
}

BinlogEventParser::QueryTransactionBoundary BinlogEventParser::ClassifyQueryTransactionBoundary(
    std::string_view query) {
  std::string normalized = mygram::utils::ToUpper(mygram::utils::TrimAsciiWhitespaceView(query));
  while (!normalized.empty() &&
         (normalized.back() == ';' || std::isspace(static_cast<unsigned char>(normalized.back())) != 0)) {
    normalized.pop_back();
  }

  std::istringstream tokens(normalized);
  std::string first;
  std::string second;
  tokens >> first >> second;
  if (first == "XA") {
    // XA PREPARE is logged as a separate transaction group and may later be
    // rolled back. The current materializer has no prepared-state staging,
    // so every XA form is rejected by the reader before any row event can be
    // published. XA BEGIN is a valid alias of XA START.
    return QueryTransactionBoundary::kUnsupportedXa;
  }
  if (first == "BEGIN" || (first == "START" && second == "TRANSACTION")) {
    return QueryTransactionBoundary::kBegin;
  }
  if (first == "COMMIT" || (first == "ROLLBACK" && second != "TO")) {
    return QueryTransactionBoundary::kEnd;
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

bool BinlogEventParser::IsSafeIgnoredQuery(std::string_view query, const std::vector<std::string>& ignored_prefixes) {
  std::vector<std::vector<SqlToken>> prefixes;
  for (const auto& prefix : ignored_prefixes) {
    auto lexed = LexQueryStatements(prefix);
    if (lexed.size() == 1) {
      prefixes.push_back(std::move(lexed.front()));
    }
  }

  const auto statements = LexQueryStatements(query);
  // A stored-program definition is logged alone with its body, whose ';' the lexer splits on.
  if (!statements.empty() && DefinesStoredProgram(statements.front())) {
    return true;
  }
  for (const auto& tokens : statements) {
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

DDLType BinlogEvent::ClassifyDDL(const std::string& query, const config::TableConfig& table) {
  return FindConfiguredTableDDL(ClassifyQueryEvent(query), table.database, table).value_or(DDLType::kUnknown);
}

bool BinlogEventParser::IsTableAffectingDDL(const std::string& query, const std::string& table_name) {
  config::TableConfig table_config;
  table_config.name = table_name;
  return FindConfiguredTableDDL(ClassifyQueryEvent(query), "", table_config).has_value();
}

}  // namespace mygramdb::mysql

// NOLINTEND(cppcoreguidelines-pro-*,cppcoreguidelines-avoid-*,readability-magic-numbers)

#endif  // USE_MYSQL
