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
#include <string_view>
#include <utility>

#include "mybinlog/disposition.h"
#include "mybinlog/mariadb_gtid.h"
#include "mybinlog/mysql_gtid.h"
#include "mybinlog/query_event.h"
#include "mybinlog/table_map.h"
#include "mysql/binlog_filter_evaluator.h"
#include "mysql/ddl_classifier.h"
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

/// sql_mode bits in the QUERY_EVENT status variable; both servers use the same positions.
/// MySQL sql/system_variables.h, MariaDB sql/sql_class.h.
constexpr uint64_t kSqlModeAnsiQuotes = 1ULL << 2;
constexpr uint64_t kSqlModeNoBackslashEscapes = 1ULL << 20;

/// Byte offset of status_vars_len in the QUERY_EVENT post-header, and the post-header size.

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
 * @brief Host metadata for a parsed TABLE_MAP
 *
 * Without @p with_row_metadata the optional block is ignored: columns keep
 * placeholder names, no signedness and no labels, and nothing is reported known.
 */
TableMetadata ToTableMetadata(const mybinlog::TableMap& map, bool with_row_metadata) {
  TableMetadata metadata;
  metadata.table_id = map.table_id;
  metadata.database_name = map.schema;
  metadata.table_name = map.table;
  metadata.columns.reserve(map.columns.size());
  for (size_t index = 0; index < map.columns.size(); ++index) {
    const mybinlog::TableMapColumn& column = map.columns[index];
    ColumnMetadata host;
    host.type = column.type;
    host.metadata = static_cast<uint16_t>(column.meta);
    host.is_nullable = column.nullable;
    if (with_row_metadata && map.has_column_names) {
      host.name = column.name;
    } else {
      host.name = "col_" + std::to_string(index);
    }
    if (with_row_metadata) {
      host.is_unsigned = column.is_unsigned;
      host.enum_set_values = column.labels;
    }
    metadata.columns.push_back(std::move(host));
  }

  if (with_row_metadata) {
    const auto has_column = [&map](auto predicate) {
      return std::any_of(map.columns.begin(), map.columns.end(), predicate);
    };
    const auto has_real_type = [](ColumnType type) {
      return [type](const mybinlog::TableMapColumn& column) { return mybinlog::RealType(column) == type; };
    };
    metadata.has_column_names = map.has_column_names;
    metadata.signedness_known = map.has_signedness || !has_column([](const mybinlog::TableMapColumn& column) {
                                  return mybinlog::HasSignednessBit(column.type);
                                });
    metadata.enum_set_labels_known = (map.has_enum_labels || !has_column(has_real_type(ColumnType::kEnum))) &&
                                     (map.has_set_labels || !has_column(has_real_type(ColumnType::kSet)));
  }
  metadata.RebuildColumnOrdinals();
  return metadata;
}

}  // namespace

ReaderEventDisposition ClassifyReaderEvent(uint8_t type_code, uint16_t flags) {
  // MariaDB numbers its own types from 160; every other type is classified as MySQL writes it.
  constexpr auto kFirstMariaDbType = static_cast<uint8_t>(mybinlog::EventType::kMariaDBAnnotateRowsEvent);
  const ServerFlavor flavor = type_code >= kFirstMariaDbType ? ServerFlavor::kMariaDB : ServerFlavor::kMySQL;
  switch (mybinlog::ClassifyEvent(type_code, flags, flavor).disposition) {
    case mybinlog::Disposition::kTableMap:
    case mybinlog::Disposition::kGtid:
    case mybinlog::Disposition::kTransactionBoundary:
    case mybinlog::Disposition::kQuery:
      return ReaderEventDisposition::kDecodeAndApply;
    case mybinlog::Disposition::kRows:
      // PARTIAL_UPDATE_ROWS carries JSON diffs the row decoder does not apply.
      return type_code == static_cast<uint8_t>(mybinlog::EventType::kPartialUpdateRowsEvent)
                 ? ReaderEventDisposition::kFailClosed
                 : ReaderEventDisposition::kDecodeAndApply;
    case mybinlog::Disposition::kFraming:
    case mybinlog::Disposition::kDataNeutral:
      return ReaderEventDisposition::kDataNeutral;
    case mybinlog::Disposition::kCompressed:
    case mybinlog::Disposition::kFailClosed:
      return ReaderEventDisposition::kFailClosed;
  }
  return ReaderEventDisposition::kFailClosed;
}

std::string DescribeEventType(uint8_t type_code) {
  const std::string_view name = mybinlog::EventTypeName(type_code);
  if (name == "UNKNOWN") {
    return "event type " + std::to_string(static_cast<unsigned>(type_code));
  }
  return std::string(name);
}

std::string UnreplayableEventRemediation(uint8_t type_code) {
  std::string reason;
  switch (static_cast<mybinlog::EventType>(type_code)) {
    case mybinlog::EventType::kTransactionPayloadEvent:
      reason =
          "Received TRANSACTION_PAYLOAD_EVENT: binlog_transaction_compression was enabled on the server after "
          "initial validation. Compressed events cannot be decoded. Disable compression with: SET GLOBAL "
          "binlog_transaction_compression=OFF";
      break;
    case mybinlog::EventType::kPartialUpdateRowsEvent:
      reason =
          "Received PARTIAL_UPDATE_ROWS_EVENT: binlog_row_value_options=PARTIAL_JSON was enabled on the server after "
          "initial validation. Partial JSON updates cannot be decoded. Disable with: SET GLOBAL "
          "binlog_row_value_options=''";
      break;
    case mybinlog::EventType::kXaPrepareLogEvent:
      reason =
          "Received XA_PREPARE_LOG_EVENT. XA transactions are unsupported because prepared rows cannot be published "
          "before a later XA COMMIT or discarded on XA ROLLBACK";
      break;
    case mybinlog::EventType::kMariaDBQueryCompressedEvent:
    case mybinlog::EventType::kMariaDBWriteRowsCompressedEventV1:
    case mybinlog::EventType::kMariaDBUpdateRowsCompressedEventV1:
    case mybinlog::EventType::kMariaDBDeleteRowsCompressedEventV1:
    case mybinlog::EventType::kMariaDBWriteRowsCompressedEvent:
    case mybinlog::EventType::kMariaDBUpdateRowsCompressedEvent:
    case mybinlog::EventType::kMariaDBDeleteRowsCompressedEvent:
      reason =
          "Received a MariaDB compressed binlog event while log_bin_compress is enabled. Compressed events cannot be "
          "decoded. Disable compression with: SET GLOBAL log_bin_compress=OFF";
      break;
    case mybinlog::EventType::kAnonymousGtidLogEvent:
      reason =
          "Received ANONYMOUS_GTID_LOG_EVENT: the source committed a transaction without a GTID, so gtid_mode is no "
          "longer ON. The transaction has no position to record, and applying its rows would attribute them to the "
          "previous transaction's GTID. Set gtid_mode=ON on the source";
      break;
    case mybinlog::EventType::kIncidentEvent:
      reason =
          "Received INCIDENT_EVENT: the source declared that it lost events, so the binlog contains a gap. The "
          "transactions inside the gap were never written and cannot be replayed from any position";
      break;
    case mybinlog::EventType::kExecuteLoadQueryEvent:
      reason =
          "Received EXECUTE_LOAD_QUERY_EVENT: a statement-based LOAD DATA INFILE. Its rows are never materialized by "
          "the row decoder, so accepting it would leave the index missing every loaded row";
      break;
    case mybinlog::EventType::kStartEventV3:
      reason =
          "Received START_EVENT_V3: the stream uses the binlog format of a server older than MySQL 5.0, which this "
          "build cannot decode";
      break;
    case mybinlog::EventType::kUnknown:
    case mybinlog::EventType::kSlaveEvent:
    case mybinlog::EventType::kEnumEndEvent:
      reason = std::string("Received ") + mybinlog::EventTypeName(type_code) +
               ", which no supported server writes to a binlog. The stream does not match the binlog protocol this "
               "build decodes";
      break;
    default:
      reason = "Received " + DescribeEventType(type_code) +
               ", whose payload this build cannot decode. Accepting it would advance the replication position past "
               "data that was never applied";
      break;
  }
  return reason + kUnreplayableEventRecovery;
}

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

  // The length check above covers the whole common header.
  mybinlog::EventHeader header;
  (void)mybinlog::ParseEventHeader(buffer, length, &header);
  const auto event_type = static_cast<mybinlog::EventType>(header.type_code);
  const uint16_t header_flags = header.flags;

  // Log event type for debugging
  mygram::utils::StructuredLog()
      .Event("binlog_debug")
      .Field("action", "received_event")
      .Field("event_name", mybinlog::EventTypeName(event_type))
      .Field("event_type", static_cast<int64_t>(buffer[4]))
      .Debug();

  // Handle different event types
  switch (event_type) {
    case mybinlog::EventType::kGtidLogEvent:
      // GTID events are handled by caller (UpdateCurrentGTID)
      return {};

    case mybinlog::EventType::kTableMapEvent:
      // TABLE_MAP events are cached by caller
      return {};

    case mybinlog::EventType::kWriteRowsEventV1:
      // V1 format (MySQL 5.1-5.5) - fallthrough to V2 handler
      // V1 and V2 share the same post-header structure; V2 may have extra_row_info based on flags
      [[fallthrough]];
    case mybinlog::EventType::kWriteRowsEvent: {
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

    case mybinlog::EventType::kUpdateRowsEventV1:
      // V1 format (MySQL 5.1-5.5) - fallthrough to V2 handler
      [[fallthrough]];
    case mybinlog::EventType::kUpdateRowsEvent: {
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

    case mybinlog::EventType::kDeleteRowsEventV1:
      // V1 format (MySQL 5.1-5.5) - fallthrough to V2 handler
      [[fallthrough]];
    case mybinlog::EventType::kDeleteRowsEvent: {
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

    case mybinlog::EventType::kQueryEvent: {
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

    case mybinlog::EventType::kRotateEvent:
      // Binlog file rotation - indicates switch to a new binlog file
      // No action needed, handled by binlog_reader at connection level
      return {};

    case mybinlog::EventType::kHeartbeatLogEvent:
    case mybinlog::EventType::kHeartbeatLogEventV2:
      // Replication heartbeat - keepalive signal from master
      // No action needed
      return {};

    case mybinlog::EventType::kXidEvent:
      // Transaction commit marker
      if (!current_gtid.empty()) {
        BinlogEvent event;
        event.type = BinlogEventType::COMMIT;
        event.gtid = current_gtid;
        return {event};
      }
      return {};

    case mybinlog::EventType::kTransactionPayloadEvent:
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

    case mybinlog::EventType::kMariaDBQueryCompressedEvent:
    case mybinlog::EventType::kMariaDBWriteRowsCompressedEventV1:
    case mybinlog::EventType::kMariaDBUpdateRowsCompressedEventV1:
    case mybinlog::EventType::kMariaDBDeleteRowsCompressedEventV1:
    case mybinlog::EventType::kMariaDBWriteRowsCompressedEvent:
    case mybinlog::EventType::kMariaDBUpdateRowsCompressedEvent:
    case mybinlog::EventType::kMariaDBDeleteRowsCompressedEvent:
      // The reader rejects these before dispatch so their XID cannot advance
      // the processed GTID. Keep the parser fail-closed for direct callers too.
      mygram::utils::StructuredLog()
          .Event("binlog_error")
          .Field("type", "unsupported_event")
          .Field("event_type", mybinlog::EventTypeName(event_type))
          .Field("message", "MariaDB log_bin_compress events are not supported")
          .Error();
      return {};

    case mybinlog::EventType::kGtidTaggedLogEvent:
      // MySQL 8.4+ tagged GTIDs - handled by caller (binlog_reader) like GTID_LOG_EVENT
      return {};

    // MariaDB-specific events: handled by caller (binlog_reader_threads) or informational only
    case mybinlog::EventType::kMariaDBGtidEvent:
    case mybinlog::EventType::kMariaDBGtidListEvent:
    case mybinlog::EventType::kMariaDBAnnotateRowsEvent:
    case mybinlog::EventType::kMariaDBBinlogCheckpointEvent:
    case mybinlog::EventType::kMariaDBStartEncryptionEvent:
      return {};

    default:
      // Every type not decoded above is dispatched by its recorded disposition
      // rather than ignored. Only the reader can stop replication, so the
      // parser reports a fail-closed type and produces nothing from it; the
      // reader rejects the same type before it ever reaches this function.
      if (ClassifyReaderEvent(buffer[4], header_flags) == ReaderEventDisposition::kFailClosed) {
        mygram::utils::StructuredLog()
            .Event("binlog_error")
            .Field("type", "unreplayable_binlog_event")
            .Field("event_type", DescribeEventType(buffer[4]))
            .Field("message", UnreplayableEventRemediation(buffer[4]))
            .Error();
      }
      return {};
  }
}

mybinlog::Status BinlogEventParser::ReadFrame(const unsigned char* buffer, unsigned long length, BinlogFrame* out) {
  BinlogFrame frame;
  if (const auto status = mybinlog::ParseEventHeader(buffer, length, &frame.header); status != mybinlog::Status::kOk) {
    return status;
  }
  const uint32_t event_length = frame.header.event_length;
  if (event_length < mybinlog::kEventHeaderSize + mybinlog::kChecksumSize || event_length > length) {
    return mybinlog::Status::kTruncated;
  }
  frame.body = buffer + mybinlog::kEventHeaderSize;
  frame.body_size = event_length - mybinlog::kEventHeaderSize - mybinlog::kChecksumSize;
  *out = frame;
  return mybinlog::Status::kOk;
}

mygram::utils::Expected<std::string, mybinlog::Status> BinlogEventParser::ExtractGTID(const unsigned char* buffer,
                                                                                      unsigned long length) {
  BinlogFrame frame;
  mybinlog::Status status = ReadFrame(buffer, length, &frame);
  mybinlog::GtidEvent event;
  if (status == mybinlog::Status::kOk) {
    status = mybinlog::ParseGtidEvent(frame.header, frame.body, frame.body_size, &event);
  }
  if (status != mybinlog::Status::kOk) {
    return mygram::utils::MakeUnexpected(status);
  }
  return event.ToString();
}

mygram::utils::Expected<TableMetadata, mygram::utils::Error> BinlogEventParser::ParseTableMapEvent(
    const unsigned char* buffer, unsigned long length,
    const std::function<bool(const TableMetadata&)>& wants_row_metadata, ServerFlavor flavor) {
  const auto log_failure = [length](const char* reason, mybinlog::Status status) {
    mygram::utils::StructuredLog()
        .Event("binlog_parse_error")
        .Field("function", "ParseTableMapEvent")
        .Field("reason", reason)
        .Field("status", mybinlog::StatusName(status))
        .Field("length", static_cast<uint64_t>(length))
        .Error();
  };

  BinlogFrame frame;
  mybinlog::Status status = ReadFrame(buffer, length, &frame);
  mybinlog::TableMap map;
  mybinlog::TableMapDiagnostics diagnostics;
  if (status == mybinlog::Status::kOk) {
    status = mybinlog::ParseTableMapEventWithDiagnostics(frame.header, frame.body, frame.body_size, flavor, &map,
                                                         &diagnostics);
  }

  if (status == mybinlog::Status::kOk) {
    TableMetadata metadata = ToTableMetadata(map, false);
    if (!wants_row_metadata || wants_row_metadata(metadata)) {
      metadata = ToTableMetadata(map, true);
    }
    mygram::utils::StructuredLog()
        .Event("binlog_debug")
        .Field("action", "table_map_complete")
        .Field("database", metadata.database_name)
        .Field("table", metadata.table_name)
        .Field("table_id", metadata.table_id)
        .Field("columns", static_cast<uint64_t>(metadata.columns.size()))
        .Debug();
    return metadata;
  }

  if (status == mybinlog::Status::kUnsupported) {
    log_failure("unknown_column_type", status);
    // The frame is intact but a column cannot be sized, so every replay stops here the same way.
    return mygram::utils::MakeUnexpected(mygram::utils::MakeError(
        mygram::utils::ErrorCode::kMySQLUndecodableBinlogEvent,
        "TABLE_MAP_EVENT has column " + std::to_string(diagnostics.column) + " with type code " +
            std::to_string(static_cast<unsigned>(diagnostics.type_code)) + ", which this build cannot size"));
  }

  if (diagnostics.in_optional_metadata) {
    TableMetadata metadata = ToTableMetadata(map, false);
    // A table nobody decodes rows for does not need the block read.
    if (wants_row_metadata && !wants_row_metadata(metadata)) {
      return metadata;
    }
    log_failure("malformed_optional_metadata", status);
    // The frame passed its checksum, so every replay delivers the same bytes.
    return mygram::utils::MakeUnexpected(
        mygram::utils::MakeError(mygram::utils::ErrorCode::kMySQLUndecodableBinlogEvent,
                                 "TABLE_MAP_EVENT for " + metadata.database_name + "." + metadata.table_name +
                                     " carries optional row metadata that does not parse"));
  }

  // A frame that does not add up is not proof the event is undecodable: a
  // replay can deliver it intact, so it keeps the generic binlog code.
  log_failure("malformed_frame", status);
  return mygram::utils::MakeUnexpected(
      mygram::utils::MakeError(mygram::utils::ErrorCode::kMySQLBinlogError,
                               std::string("Malformed TABLE_MAP_EVENT (") + mybinlog::StatusName(status) + ")"));
}

std::optional<QueryEventData> BinlogEventParser::ExtractQueryString(const unsigned char* buffer, unsigned long length) {
  BinlogFrame frame;
  if (ReadFrame(buffer, length, &frame) != mybinlog::Status::kOk) {
    return {};
  }

  // A status variable that cannot be read ends the walk, as an unknown code
  // does: the statement is still read, under an unknown mode.
  mybinlog::QueryEvent event;
  const mybinlog::Status status = mybinlog::ParseQueryEventWithPolicy(frame.header, frame.body, frame.body_size, &event,
                                                                      mybinlog::StatusVarPolicy::kStopAtMalformed);
  if (status != mybinlog::Status::kOk || event.query.empty()) {
    return {};
  }

  QueryEventData data{std::move(event.schema), std::move(event.query), QueryLexOptions{}, false};
  if (event.has_sql_mode) {
    data.lex_options =
        QueryLexOptions{(event.sql_mode & kSqlModeAnsiQuotes) != 0, (event.sql_mode & kSqlModeNoBackslashEscapes) != 0};
    data.sql_mode_known = true;
  } else {
    // A walk that ends with no mode variable at all reads as the default mode.
    data.sql_mode_known = event.status_vars_complete;
  }
  return data;
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
  mybinlog::MariaDbGtidEvent event;
  event.flags = flags;
  return !event.standalone();
}

bool BinlogEventParser::IsRowsStatementEnd(const unsigned char* buffer, unsigned long length) {
  constexpr size_t kRowsEventFlagsOffset = mygram::constants::kBinlogEventHeaderLen + 6;
  constexpr size_t kRowsEventFlagsEnd = kRowsEventFlagsOffset + 2;
  constexpr uint16_t kStatementEndFlag = 0x0001;
  if (buffer == nullptr || length < kRowsEventFlagsEnd) {
    return false;
  }

  if (!mybinlog::IsRowsEvent(buffer[4])) {
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
