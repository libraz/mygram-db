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
#include <iomanip>
#include <sstream>

#include "mysql/binlog_event_types.h"
#include "mysql/binlog_util.h"
#include "mysql/rows_parser.h"
#include "server/tcp_server.h"  // For TableContext definition
#include "utils/structured_log.h"

// NOLINTBEGIN(cppcoreguidelines-pro-*,cppcoreguidelines-avoid-*,readability-magic-numbers)

namespace mygramdb::mysql {

namespace {

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
  std::vector<config::FilterConfig> required_as_filters;
};

/**
 * @brief Extract table_id from binlog event post-header
 * @param buffer Event buffer (must point to beginning of event)
 * @return Table ID (6-byte little-endian value)
 */
inline uint64_t ExtractTableId(const unsigned char* buffer) {
  const unsigned char* post_header = buffer + 19;  // Skip common header
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

  // Prepare required_filters config
  ctx.required_as_filters.reserve(ctx.current_config->required_filters.size());
  for (const auto& req_filter : ctx.current_config->required_filters) {
    config::FilterConfig filter_config;
    filter_config.name = req_filter.name;
    filter_config.type = req_filter.type;
    filter_config.dict_compress = false;
    filter_config.bitmap_index = req_filter.bitmap_index;
    ctx.required_as_filters.push_back(filter_config);
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

// ============================================================================
// SQL parsing helper functions
// ============================================================================

/**
 * @brief Strip SQL comments from a query string
 *
 * Removes:
 * - Block comments: / * ... * / (without spaces)
 * - Line comments: -- ... (to end of line)
 *
 * @param query SQL query string
 * @return Query with comments stripped
 */
std::string StripSQLComments(const std::string& query) {
  std::string result;
  result.reserve(query.length());

  size_t pos = 0;
  while (pos < query.length()) {
    // Check for block comment start
    if (pos + 1 < query.length() && query[pos] == '/' && query[pos + 1] == '*') {
      // Skip until end of block comment
      pos += 2;
      while (pos + 1 < query.length()) {
        if (query[pos] == '*' && query[pos + 1] == '/') {
          pos += 2;
          break;
        }
        pos++;
      }
      // Add a space to preserve word boundaries
      if (!result.empty() && result.back() != ' ') {
        result += ' ';
      }
      continue;
    }

    // Check for line comment start
    if (pos + 1 < query.length() && query[pos] == '-' && query[pos + 1] == '-') {
      // Skip until end of line
      pos += 2;
      while (pos < query.length() && query[pos] != '\n' && query[pos] != '\r') {
        pos++;
      }
      // Skip the newline if present
      if (pos < query.length()) {
        pos++;
      }
      continue;
    }

    result += query[pos];
    pos++;
  }

  return result;
}

/**
 * @brief Normalize whitespace in a string by replacing consecutive spaces with a single space
 * @param str Input string
 * @return Normalized string
 */
std::string NormalizeWhitespace(const std::string& str) {
  std::string result;
  result.reserve(str.length());

  bool prev_was_space = false;
  for (char cur_char : str) {
    bool is_space = std::isspace(static_cast<unsigned char>(cur_char)) != 0;
    if (is_space) {
      if (!prev_was_space) {
        result += ' ';
        prev_was_space = true;
      }
    } else {
      result += cur_char;
      prev_was_space = false;
    }
  }

  return result;
}

/**
 * @brief Skip whitespace characters starting from a given position
 * @param str Input string
 * @param pos Starting position (updated to position after whitespace)
 * @return true if position is valid after skipping, false otherwise
 */
bool SkipWhitespace(const std::string& str, size_t& pos) {
  while (pos < str.length() && std::isspace(static_cast<unsigned char>(str[pos])) != 0) {
    ++pos;
  }
  return pos < str.length();
}

/**
 * @brief Case-insensitive keyword matching at a given position
 * @param str Input string (should be uppercase)
 * @param pos Starting position (updated to position after keyword if matched)
 * @param keyword Keyword to match (should be uppercase)
 * @return true if keyword matches, false otherwise
 */
bool MatchKeyword(const std::string& str, size_t& pos, const std::string& keyword) {
  // Check if there's enough space for the keyword
  if (pos + keyword.length() > str.length()) {
    return false;
  }

  // Check if keyword matches
  if (str.compare(pos, keyword.length(), keyword) != 0) {
    return false;
  }

  // Check that keyword is followed by whitespace, backtick, or end of string
  size_t next_pos = pos + keyword.length();
  if (next_pos < str.length()) {
    char next_char = str[next_pos];
    if (std::isspace(static_cast<unsigned char>(next_char)) == 0 && next_char != '`') {
      return false;
    }
  }

  pos = next_pos;
  return true;
}

/**
 * @brief Match table name at a given position (with optional backticks)
 * @param str Input string (should be uppercase)
 * @param pos Starting position (updated to position after table name if matched)
 * @param table_name Table name to match (should be uppercase)
 * @return true if table name matches, false otherwise
 */
bool MatchTableName(const std::string& str, size_t& pos, const std::string& table_name) {
  // Skip optional backtick
  bool has_backtick = false;
  if (pos < str.length() && str[pos] == '`') {
    has_backtick = true;
    ++pos;
  }

  // Match table name
  if (pos + table_name.length() > str.length()) {
    return false;
  }

  if (str.compare(pos, table_name.length(), table_name) != 0) {
    return false;
  }

  pos += table_name.length();

  // Skip optional closing backtick
  if (has_backtick && pos < str.length() && str[pos] == '`') {
    ++pos;
  }

  // Ensure the match is a complete word (not a prefix of a longer identifier)
  // After the table name (and optional backtick), the next character must be:
  // - End of string
  // - Whitespace
  // - Semicolon
  // - Not an identifier character (alphanumeric or underscore)
  if (pos < str.length()) {
    char next_char = str[pos];
    if (std::isalnum(static_cast<unsigned char>(next_char)) != 0 || next_char == '_') {
      return false;  // Table name is a prefix of a longer identifier
    }
  }

  return true;
}

}  // namespace

std::vector<BinlogEvent> BinlogEventParser::ParseBinlogEvent(
    const unsigned char* buffer, unsigned long length, const std::string& current_gtid,
    TableMetadataCache& table_metadata_cache,
    const std::unordered_map<std::string, server::TableContext*>& table_contexts,
    const config::TableConfig* table_config, bool multi_table_mode, const std::string& datetime_timezone) {
  if ((buffer == nullptr) || length < 19) {
    // Minimum event size is 19 bytes (binlog header)
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

      auto rows_opt =
          ParseWriteRowsEvent(buffer, length, ctx.table_meta, ctx.current_config->primary_key, ctx.text_column);

      if (!rows_opt || rows_opt->empty()) {
        return {};
      }

      // Create events for ALL rows (multi-row event support)
      std::vector<BinlogEvent> events;
      events.reserve(rows_opt->size());

      for (const auto& row : *rows_opt) {
        BinlogEvent event;
        event.type = BinlogEventType::INSERT;
        event.table_name = ctx.table_meta->table_name;
        event.primary_key = row.primary_key;
        event.text = GetRowText(row, ctx);
        event.gtid = current_gtid;

        // Extract all filters (required + optional) from row data
        event.filters = ExtractFilters(row, ctx.current_config->filters, datetime_timezone);
        auto required_filters = ExtractFilters(row, ctx.required_as_filters, datetime_timezone);
        event.filters.insert(required_filters.begin(), required_filters.end());

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
      auto row_pairs_opt =
          ParseUpdateRowsEvent(buffer, length, ctx.table_meta, ctx.current_config->primary_key, ctx.text_column);

      if (!row_pairs_opt || row_pairs_opt->empty()) {
        return {};
      }

      // Create events for ALL row pairs (multi-row event support)
      std::vector<BinlogEvent> events;
      events.reserve(row_pairs_opt->size());

      for (const auto& row_pair : *row_pairs_opt) {
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
        event.filters = ExtractFilters(after_row, ctx.current_config->filters, datetime_timezone);
        auto required_filters = ExtractFilters(after_row, ctx.required_as_filters, datetime_timezone);
        event.filters.insert(required_filters.begin(), required_filters.end());

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
      auto rows_opt =
          ParseDeleteRowsEvent(buffer, length, ctx.table_meta, ctx.current_config->primary_key, ctx.text_column);

      if (!rows_opt || rows_opt->empty()) {
        return {};
      }

      // Create events for ALL rows (multi-row event support)
      std::vector<BinlogEvent> events;
      events.reserve(rows_opt->size());

      for (const auto& row : *rows_opt) {
        BinlogEvent event;
        event.type = BinlogEventType::DELETE;
        event.table_name = ctx.table_meta->table_name;
        event.primary_key = row.primary_key;
        event.text = GetRowText(row, ctx);
        event.gtid = current_gtid;

        // Extract all filters from row data (before image for DELETE)
        event.filters = ExtractFilters(row, ctx.current_config->filters, datetime_timezone);
        auto required_filters = ExtractFilters(row, ctx.required_as_filters, datetime_timezone);
        event.filters.insert(required_filters.begin(), required_filters.end());

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

      // Check if this affects any of our target tables
      if (multi_table_mode) {
        // Multi-table mode: check all registered tables
        for (const auto& [table_name, ctx] : table_contexts) {
          if (IsTableAffectingDDL(query, table_name)) {
            BinlogEvent event;
            event.type = BinlogEventType::DDL;
            event.table_name = table_name;
            event.text = query;  // Store the DDL query
            return {event};      // Return as vector with single element
          }
        }
      } else {
        // Single-table mode: check only our configured table
        if (table_config != nullptr && IsTableAffectingDDL(query, table_config->name)) {
          BinlogEvent event;
          event.type = BinlogEventType::DDL;
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

    default:
      // Ignore other event types
      return {};
  }
}

std::optional<std::string> BinlogEventParser::ExtractGTID(const unsigned char* buffer, unsigned long length) {
  if ((buffer == nullptr) || length < 42) {
    // GTID event minimum size
    return {};
  }

  // GTID event format (after 19-byte header):
  // commit_flag (1 byte)
  // sid (16 bytes, UUID)
  // gno (8 bytes, transaction number)

  // Skip header (19 bytes) and commit_flag (1 byte)
  const unsigned char* sid_ptr = buffer + 20;

  // Format UUID as string using std::ostringstream
  std::ostringstream uuid_oss;
  uuid_oss << std::hex << std::setfill('0') << std::setw(2) << static_cast<int>(sid_ptr[0]) << std::setw(2)
           << static_cast<int>(sid_ptr[1]) << std::setw(2) << static_cast<int>(sid_ptr[2]) << std::setw(2)
           << static_cast<int>(sid_ptr[3]) << '-' << std::setw(2) << static_cast<int>(sid_ptr[4]) << std::setw(2)
           << static_cast<int>(sid_ptr[5]) << '-' << std::setw(2) << static_cast<int>(sid_ptr[6]) << std::setw(2)
           << static_cast<int>(sid_ptr[7]) << '-' << std::setw(2) << static_cast<int>(sid_ptr[8]) << std::setw(2)
           << static_cast<int>(sid_ptr[9]) << '-' << std::setw(2) << static_cast<int>(sid_ptr[10]) << std::setw(2)
           << static_cast<int>(sid_ptr[11]) << std::setw(2) << static_cast<int>(sid_ptr[12]) << std::setw(2)
           << static_cast<int>(sid_ptr[13]) << std::setw(2) << static_cast<int>(sid_ptr[14]) << std::setw(2)
           << static_cast<int>(sid_ptr[15]);

  // Extract GNO (8 bytes, little-endian)
  const unsigned char* gno_ptr = sid_ptr + 16;
  uint64_t gno = 0;
  for (int i = 0; i < 8; i++) {
    gno |= (uint64_t)gno_ptr[i] << (i * 8);
  }

  // Format as "UUID:GNO"
  std::string gtid = uuid_oss.str() + ":" + std::to_string(gno);
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
  const unsigned char* ptr = buffer + 19;
  unsigned long remaining = length - 19;

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
  constexpr uint64_t MAX_COLUMNS = 4096;  // MySQL limit is 4096 columns
  if (column_count > MAX_COLUMNS) {
    mygram::utils::StructuredLog()
        .Event("mysql_binlog_warning")
        .Field("type", "column_count_exceeds_maximum")
        .Field("column_count", column_count)
        .Field("max_columns", MAX_COLUMNS)
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
          if (ptr + 1 <= metadata_start + metadata_len) {
            metadata.columns[i].metadata = *ptr;
            ptr += 1;
          }
          break;

        case ColumnType::ENUM:
        case ColumnType::SET:
          // 2 bytes: number of elements
          if (ptr + 2 <= metadata_start + metadata_len) {
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
  if ((buffer == nullptr) || length < 19) {
    // Minimum: 19 bytes header
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

  const unsigned char* pos = buffer + 19;  // Skip common header
  size_t remaining = length - 19;

  if (remaining < 13) {  // Minimum: 4+4+1+2+2
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
  if (!SkipWhitespace(query_upper, pos) && query_upper.empty()) {
    return false;
  }

  // Check for TRUNCATE TABLE
  size_t saved_start = pos;
  if (MatchKeyword(query_upper, pos, "TRUNCATE")) {
    if (SkipWhitespace(query_upper, pos) && MatchKeyword(query_upper, pos, "TABLE")) {
      if (SkipWhitespace(query_upper, pos) && MatchTableName(query_upper, pos, table_upper)) {
        return true;
      }
    }
  }

  // Reset position and check for DROP TABLE [IF EXISTS]
  pos = saved_start;
  if (MatchKeyword(query_upper, pos, "DROP")) {
    if (SkipWhitespace(query_upper, pos) && MatchKeyword(query_upper, pos, "TABLE")) {
      if (SkipWhitespace(query_upper, pos)) {
        // Check for optional "IF EXISTS"
        size_t saved_pos = pos;
        if (MatchKeyword(query_upper, pos, "IF")) {
          if (SkipWhitespace(query_upper, pos) && MatchKeyword(query_upper, pos, "EXISTS")) {
            SkipWhitespace(query_upper, pos);
          } else {
            // "IF" without "EXISTS" - restore position
            pos = saved_pos;
          }
        }

        // Match table name
        if (MatchTableName(query_upper, pos, table_upper)) {
          return true;
        }
      }
    }
  }

  // Reset position and check for ALTER TABLE
  pos = saved_start;
  if (MatchKeyword(query_upper, pos, "ALTER")) {
    if (SkipWhitespace(query_upper, pos) && MatchKeyword(query_upper, pos, "TABLE")) {
      if (SkipWhitespace(query_upper, pos) && MatchTableName(query_upper, pos, table_upper)) {
        return true;
      }
    }
  }

  return false;
}

bool BinlogEventParser::IsTableAffectingDDL(const std::string& query, const std::string& table_name) {
  // Strip SQL comments first
  std::string stripped_query = StripSQLComments(query);

  // Convert to uppercase for case-insensitive matching
  std::string query_upper = stripped_query;
  std::string table_upper = table_name;
  std::transform(query_upper.begin(), query_upper.end(), query_upper.begin(), ::toupper);
  std::transform(table_upper.begin(), table_upper.end(), table_upper.begin(), ::toupper);

  // Normalize whitespace (replace consecutive spaces with single space)
  query_upper = NormalizeWhitespace(query_upper);

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
