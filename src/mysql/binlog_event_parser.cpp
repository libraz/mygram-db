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

std::optional<BinlogEvent> BinlogEventParser::ParseBinlogEvent(
    const unsigned char* buffer, unsigned long length, const std::string& current_gtid,
    TableMetadataCache& table_metadata_cache,
    const std::unordered_map<std::string, server::TableContext*>& table_contexts,
    const config::TableConfig* table_config, bool multi_table_mode) {
  if ((buffer == nullptr) || length < 20) {
    // Minimum event size is 20 bytes (1 byte OK packet + 19 bytes binlog header)
    return std::nullopt;
  }

  // MySQL C API prepends an OK packet byte (0x00) before the actual binlog event
  // Skip the OK byte to get to the actual binlog event data
  buffer++;
  length--;

  // Binlog event header format (19 bytes):
  // timestamp (4 bytes)
  // event_type (1 byte)
  // server_id (4 bytes)
  // event_size (4 bytes)
  // log_pos (4 bytes)
  // flags (2 bytes)

  auto event_type = static_cast<MySQLBinlogEventType>(buffer[4]);

  // Log event type for debugging
  spdlog::debug("Received binlog event: {} (type={})", GetEventTypeName(event_type), static_cast<int>(buffer[4]));

  // Handle different event types
  switch (event_type) {
    case MySQLBinlogEventType::GTID_LOG_EVENT:
      // GTID events are handled by caller (UpdateCurrentGTID)
      return std::nullopt;

    case MySQLBinlogEventType::TABLE_MAP_EVENT:
      // TABLE_MAP events are cached by caller
      return std::nullopt;

    case MySQLBinlogEventType::WRITE_ROWS_EVENT: {
      // Parse INSERT operations
      spdlog::debug("WRITE_ROWS_EVENT detected");

      // Extract table_id from post-header (skip common header 19 bytes)
      const unsigned char* post_header = buffer + 19;
      uint64_t table_id = 0;
      for (int i = 0; i < 6; i++) {
        table_id |= (uint64_t)post_header[i] << (i * 8);
      }

      // Get table metadata from cache
      const TableMetadata* table_meta = table_metadata_cache.Get(table_id);
      if (table_meta == nullptr) {
        spdlog::debug("Unknown table_id in write_rows event: {}", table_id);
        return std::nullopt;
      }

      // Determine text column and primary key from config
      const config::TableConfig* current_config = nullptr;
      if (multi_table_mode) {
        auto table_iter = table_contexts.find(table_meta->table_name);
        if (table_iter == table_contexts.end()) {
          mygram::utils::StructuredLog()
              .Event("mysql_binlog_warning")
              .Field("type", "table_not_in_context")
              .Field("event_type", "write_rows")
              .Field("table_name", table_meta->table_name)
              .Warn();
          return std::nullopt;
        }
        current_config = &table_iter->second->config;
      } else {
        current_config = table_config;
      }

      std::string text_column;
      if (!current_config->text_source.column.empty()) {
        text_column = current_config->text_source.column;
      } else if (!current_config->text_source.concat.empty()) {
        text_column = current_config->text_source.concat[0];
      } else {
        text_column = "";
      }

      auto rows_opt = ParseWriteRowsEvent(buffer, length, table_meta, current_config->primary_key, text_column);

      if (!rows_opt || rows_opt->empty()) {
        return std::nullopt;
      }

      // Create event from first row (for now, handle multi-row events later)
      const auto& row = rows_opt->front();
      BinlogEvent event;
      event.type = BinlogEventType::INSERT;
      event.table_name = table_meta->table_name;
      event.primary_key = row.primary_key;
      event.text = row.text;
      event.gtid = current_gtid;

      // Extract all filters (required + optional) from row data
      // Note: Filter extraction is done by caller (BinlogFilterEvaluator)
      event.filters = ExtractFilters(row, current_config->filters);

      // Also extract required_filters as FilterConfig format
      std::vector<config::FilterConfig> required_as_filters;
      for (const auto& req_filter : current_config->required_filters) {
        config::FilterConfig filter_config;
        filter_config.name = req_filter.name;
        filter_config.type = req_filter.type;
        filter_config.dict_compress = false;
        filter_config.bitmap_index = req_filter.bitmap_index;
        required_as_filters.push_back(filter_config);
      }
      auto required_filters = ExtractFilters(row, required_as_filters);
      event.filters.insert(required_filters.begin(), required_filters.end());

      spdlog::debug("Parsed WRITE_ROWS: pk={}, text_len={}, filters={}", event.primary_key, event.text.length(),
                    event.filters.size());

      return event;
    }

    case MySQLBinlogEventType::UPDATE_ROWS_EVENT: {
      // Parse UPDATE operations
      spdlog::debug("UPDATE_ROWS_EVENT detected");

      // Extract table_id from post-header
      const unsigned char* post_header = buffer + 19;
      uint64_t table_id = 0;
      for (int i = 0; i < 6; i++) {
        table_id |= (uint64_t)post_header[i] << (i * 8);
      }

      // Get table metadata from cache
      const TableMetadata* table_meta = table_metadata_cache.Get(table_id);
      if (table_meta == nullptr) {
        spdlog::debug("Unknown table_id in update_rows event: {}", table_id);
        return std::nullopt;
      }

      // Determine text column and primary key from config
      const config::TableConfig* current_config = nullptr;
      if (multi_table_mode) {
        auto table_iter = table_contexts.find(table_meta->table_name);
        if (table_iter == table_contexts.end()) {
          mygram::utils::StructuredLog()
              .Event("mysql_binlog_warning")
              .Field("type", "table_not_in_context")
              .Field("event_type", "update_rows")
              .Field("table_name", table_meta->table_name)
              .Warn();
          return std::nullopt;
        }
        current_config = &table_iter->second->config;
      } else {
        current_config = table_config;
      }

      std::string text_column;
      if (!current_config->text_source.column.empty()) {
        text_column = current_config->text_source.column;
      } else if (!current_config->text_source.concat.empty()) {
        text_column = current_config->text_source.concat[0];
      } else {
        text_column = "";
      }

      // Parse rows using rows_parser
      auto row_pairs_opt = ParseUpdateRowsEvent(buffer, length, table_meta, current_config->primary_key, text_column);

      if (!row_pairs_opt || row_pairs_opt->empty()) {
        return std::nullopt;
      }

      // Create event from first row pair (for now)
      const auto& row_pair = row_pairs_opt->front();
      const auto& before_row = row_pair.first;  // Before image
      const auto& after_row = row_pair.second;  // After image

      BinlogEvent event;
      event.type = BinlogEventType::UPDATE;
      event.table_name = table_meta->table_name;
      event.primary_key = after_row.primary_key;
      event.text = after_row.text;       // New text (after image)
      event.old_text = before_row.text;  // Old text (before image) for index update
      event.gtid = current_gtid;

      // Extract all filters from after image
      event.filters = ExtractFilters(after_row, current_config->filters);

      // Also extract required_filters
      std::vector<config::FilterConfig> required_as_filters;
      for (const auto& req_filter : current_config->required_filters) {
        config::FilterConfig filter_config;
        filter_config.name = req_filter.name;
        filter_config.type = req_filter.type;
        filter_config.dict_compress = false;
        filter_config.bitmap_index = req_filter.bitmap_index;
        required_as_filters.push_back(filter_config);
      }
      auto required_filters = ExtractFilters(after_row, required_as_filters);
      event.filters.insert(required_filters.begin(), required_filters.end());

      spdlog::debug("Parsed UPDATE_ROWS: pk={}, text_len={}, filters={}", event.primary_key, event.text.length(),
                    event.filters.size());

      return event;
    }

    case MySQLBinlogEventType::DELETE_ROWS_EVENT: {
      // Parse DELETE operations
      spdlog::debug("DELETE_ROWS_EVENT detected");

      // Extract table_id from post-header
      const unsigned char* post_header = buffer + 19;
      uint64_t table_id = 0;
      for (int i = 0; i < 6; i++) {
        table_id |= (uint64_t)post_header[i] << (i * 8);
      }

      // Get table metadata from cache
      const TableMetadata* table_meta = table_metadata_cache.Get(table_id);
      if (table_meta == nullptr) {
        spdlog::debug("Unknown table_id in delete_rows event: {}", table_id);
        return std::nullopt;
      }

      // Determine text column and primary key from config
      const config::TableConfig* current_config = nullptr;
      if (multi_table_mode) {
        auto table_iter = table_contexts.find(table_meta->table_name);
        if (table_iter == table_contexts.end()) {
          mygram::utils::StructuredLog()
              .Event("mysql_binlog_warning")
              .Field("type", "table_not_in_context")
              .Field("event_type", "delete_rows")
              .Field("table_name", table_meta->table_name)
              .Warn();
          return std::nullopt;
        }
        current_config = &table_iter->second->config;
      } else {
        current_config = table_config;
      }

      std::string text_column;
      if (!current_config->text_source.column.empty()) {
        text_column = current_config->text_source.column;
      } else if (!current_config->text_source.concat.empty()) {
        text_column = current_config->text_source.concat[0];
      } else {
        text_column = "";
      }

      // Parse rows using rows_parser
      auto rows_opt = ParseDeleteRowsEvent(buffer, length, table_meta, current_config->primary_key, text_column);

      if (!rows_opt || rows_opt->empty()) {
        return std::nullopt;
      }

      // Create event from first row (for now)
      const auto& row = rows_opt->front();
      BinlogEvent event;
      event.type = BinlogEventType::DELETE;
      event.table_name = table_meta->table_name;
      event.primary_key = row.primary_key;
      event.text = row.text;
      event.gtid = current_gtid;

      // Extract all filters from row data (before image for DELETE)
      event.filters = ExtractFilters(row, current_config->filters);

      // Also extract required_filters
      std::vector<config::FilterConfig> required_as_filters;
      for (const auto& req_filter : current_config->required_filters) {
        config::FilterConfig filter_config;
        filter_config.name = req_filter.name;
        filter_config.type = req_filter.type;
        filter_config.dict_compress = false;
        filter_config.bitmap_index = req_filter.bitmap_index;
        required_as_filters.push_back(filter_config);
      }
      auto required_filters = ExtractFilters(row, required_as_filters);
      event.filters.insert(required_filters.begin(), required_filters.end());

      spdlog::debug("Parsed DELETE_ROWS: pk={}, text_len={}, filters={}", event.primary_key, event.text.length(),
                    event.filters.size());

      return event;
    }

    case MySQLBinlogEventType::QUERY_EVENT: {
      // DDL statements (CREATE, ALTER, DROP, TRUNCATE, etc.)
      auto query_opt = ExtractQueryString(buffer, length);
      if (!query_opt) {
        return std::nullopt;
      }

      std::string query = query_opt.value();
      spdlog::debug("QUERY_EVENT: {}", query);

      // Check if this affects any of our target tables
      if (multi_table_mode) {
        // Multi-table mode: check all registered tables
        for (const auto& [table_name, ctx] : table_contexts) {
          if (IsTableAffectingDDL(query, table_name)) {
            BinlogEvent event;
            event.type = BinlogEventType::DDL;
            event.table_name = table_name;
            event.text = query;  // Store the DDL query
            return event;
          }
        }
      } else {
        // Single-table mode: check only our configured table
        if (table_config != nullptr && IsTableAffectingDDL(query, table_config->name)) {
          BinlogEvent event;
          event.type = BinlogEventType::DDL;
          event.table_name = table_config->name;
          event.text = query;  // Store the DDL query
          return event;
        }
      }

      return std::nullopt;
    }

    case MySQLBinlogEventType::XID_EVENT:
      // Transaction commit marker
      return std::nullopt;

    default:
      // Ignore other event types
      return std::nullopt;
  }
}

std::optional<std::string> BinlogEventParser::ExtractGTID(const unsigned char* buffer, unsigned long length) {
  if ((buffer == nullptr) || length < 42) {
    // GTID event minimum size
    return std::nullopt;
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
    return std::nullopt;
  }

  TableMetadata metadata;

  // Skip header (19 bytes) to get to post-header
  const unsigned char* ptr = buffer + 19;
  unsigned long remaining = length - 19;

  if (remaining < 8) {
    return std::nullopt;
  }

  // Parse table_id (6 bytes)
  metadata.table_id = 0;
  for (int i = 0; i < 6; i++) {
    metadata.table_id |= (uint64_t)ptr[i] << (i * 8);
  }
  ptr += 6;
  remaining -= 6;

  // Skip flags (2 bytes)
  ptr += 2;
  remaining -= 2;

  if (remaining < 1) {
    return std::nullopt;
  }

  // Parse database name (1 byte length + null-terminated string)
  uint8_t db_len = *ptr++;
  remaining--;

  if (remaining < static_cast<size_t>(db_len) + 1) {  // +1 for null terminator
    return std::nullopt;
  }

  metadata.database_name = std::string(reinterpret_cast<const char*>(ptr), db_len);
  ptr += db_len + 1;  // +1 for null terminator
  remaining -= (db_len + 1);

  if (remaining < 1) {
    return std::nullopt;
  }

  // Parse table name (1 byte length + null-terminated string)
  uint8_t table_len = *ptr++;
  remaining--;

  if (remaining < static_cast<size_t>(table_len) + 1) {
    return std::nullopt;
  }

  metadata.table_name = std::string(reinterpret_cast<const char*>(ptr), table_len);
  ptr += table_len + 1;
  remaining -= (table_len + 1);

  if (remaining < 1) {
    return std::nullopt;
  }

  // Parse column count (packed integer)
  const unsigned char* ptr_before_packed = ptr;
  uint64_t column_count = binlog_util::read_packed_integer(&ptr);

  // SECURITY: Update remaining bytes after reading packed integer
  size_t packed_int_size = ptr - ptr_before_packed;
  if (remaining < packed_int_size) {
    return std::nullopt;
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
    return std::nullopt;
  }

  if (remaining < column_count) {
    return std::nullopt;
  }

  // Parse column types (1 byte per column)
  metadata.columns.reserve(column_count);
  for (uint64_t i = 0; i < column_count; i++) {
    if (remaining < 1) {
      return std::nullopt;
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
      return std::nullopt;
    }
    remaining -= meta_len_size;

    if (remaining < metadata_len) {
      return std::nullopt;
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

  spdlog::debug("TABLE_MAP: {}.{} (table_id={}, columns={})", metadata.database_name, metadata.table_name,
                metadata.table_id, column_count);

  return metadata;
}

std::optional<std::string> BinlogEventParser::ExtractQueryString(const unsigned char* buffer, unsigned long length) {
  if ((buffer == nullptr) || length < 19) {
    // Minimum: 19 bytes header
    return std::nullopt;
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
    return std::nullopt;
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
    return std::nullopt;
  }
  pos += status_vars_len;
  remaining -= status_vars_len;

  // Skip db_name (null-terminated)
  if (remaining < static_cast<size_t>(db_len) + 1) {  // +1 for null terminator
    return std::nullopt;
  }
  pos += db_len + 1;
  remaining -= (db_len + 1);

  // Extract query string
  if (remaining == 0) {
    return std::nullopt;
  }

  std::string query(reinterpret_cast<const char*>(pos), remaining);
  return query;
}

bool BinlogEventParser::IsTableAffectingDDL(const std::string& query, const std::string& table_name) {
  // Convert to uppercase for case-insensitive matching
  std::string query_upper = query;
  std::string table_upper = table_name;
  std::transform(query_upper.begin(), query_upper.end(), query_upper.begin(), ::toupper);
  std::transform(table_upper.begin(), table_upper.end(), table_upper.begin(), ::toupper);

  // Normalize whitespace (replace consecutive spaces with single space)
  query_upper = NormalizeWhitespace(query_upper);

  // Try to match DDL statements at each position in the query
  // We need to search for patterns anywhere in the query string
  for (size_t i = 0; i < query_upper.length(); ++i) {
    size_t pos = i;

    // Check for TRUNCATE TABLE
    if (MatchKeyword(query_upper, pos, "TRUNCATE")) {
      if (SkipWhitespace(query_upper, pos) && MatchKeyword(query_upper, pos, "TABLE")) {
        if (SkipWhitespace(query_upper, pos) && MatchTableName(query_upper, pos, table_upper)) {
          return true;
        }
      }
    }

    // Reset position for next pattern
    pos = i;

    // Check for DROP TABLE [IF EXISTS]
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

    // Reset position for next pattern
    pos = i;

    // Check for ALTER TABLE
    if (MatchKeyword(query_upper, pos, "ALTER")) {
      if (SkipWhitespace(query_upper, pos) && MatchKeyword(query_upper, pos, "TABLE")) {
        if (SkipWhitespace(query_upper, pos) && MatchTableName(query_upper, pos, table_upper)) {
          return true;
        }
      }
    }
  }

  return false;
}

}  // namespace mygramdb::mysql

// NOLINTEND(cppcoreguidelines-pro-*,cppcoreguidelines-avoid-*,readability-magic-numbers)

#endif  // USE_MYSQL
