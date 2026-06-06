/**
 * @file binlog_reader_utils.cpp
 * @brief BinlogReader utility functions
 *
 * Contains ProcessEvent, FetchColumnNames, ValidateConnection,
 * ConvertSingleGtidToRange, FixGtidSetCallback, UpdateCurrentGTID,
 * and RefreshExecutedGtidSet, extracted from binlog_reader.cpp for
 * translation unit splitting.
 */

#include "mysql/binlog_reader.h"

#ifdef USE_MYSQL

#include <spdlog/spdlog.h>

#include <cstring>
#include <map>
#include <optional>
#include <sstream>
#include <utility>

#include "mysql/binlog_event_processor.h"
#include "mysql/binlog_reader_internal.h"
#include "mysql/connection_validator.h"
#include "mysql/gtid_encoder.h"
#include "mysql/mariadb_gtid.h"
#include "server/server_types.h"  // For TableContext definition
#include "utils/numeric_parse.h"
#include "utils/structured_log.h"

// NOLINTBEGIN(cppcoreguidelines-pro-*,cppcoreguidelines-avoid-*,readability-magic-numbers)

namespace mygramdb::mysql {

bool BinlogReader::ProcessEvent(const BinlogEvent& event) {
  auto table_iter = table_contexts_.find(event.table_name);
  if (table_iter == table_contexts_.end()) {
    // Event is for a table we're not tracking, skip silently. Log the first
    // few occurrences for debugging.
    int current_count = skip_log_count_.fetch_add(1);
    if (current_count < 10) {
      mygram::utils::StructuredLog()
          .Event("binlog_event_skipped")
          .Field("table", event.table_name)
          .Field("reason", "non-tracked table")
          .Field("skip_count", static_cast<uint64_t>(current_count + 1))
          .Info();
    }
    auto* stats = server_stats_.load(std::memory_order_acquire);
    if (stats != nullptr) {
      stats->IncrementReplEventsSkippedOtherTables();
    }
    return true;
  }

  auto* table_context = table_iter->second;
  const bool is_legacy_context = table_context == &legacy_table_context_;
  index::Index* current_index =
      table_context->index ? table_context->index.get() : (is_legacy_context ? legacy_index_ : nullptr);
  storage::DocumentStore* current_doc_store =
      table_context->doc_store ? table_context->doc_store.get() : (is_legacy_context ? legacy_doc_store_ : nullptr);
  const config::TableConfig* current_config = &table_context->config;
  if (current_index == nullptr || current_doc_store == nullptr) {
    mygram::utils::StructuredLog()
        .Event("binlog_error")
        .Field("type", "null_table_context")
        .Field("table", event.table_name)
        .Field("gtid", event.gtid)
        .Field("error", "Table context has null index or doc_store")
        .Error();
    return false;
  }

  // Invalidate column names cache on ALTER TABLE DDL to avoid stale column
  // name mappings. The next monitored TABLE_MAP_EVENT fetches fresh names
  // before AddOrUpdate(), allowing SchemaEquals() to detect column renames.
  if (event.type == BinlogEventType::DDL) {
    std::string query_upper = event.text;
    std::transform(query_upper.begin(), query_upper.end(), query_upper.begin(), ::toupper);
    if (query_upper.find("ALTER") != std::string::npos) {
      std::lock_guard<std::mutex> lock(column_names_cache_mutex_);
      // Invalidate all cache entries containing this table name
      for (auto it = column_names_cache_.begin(); it != column_names_cache_.end();) {
        if (it->first.find(event.table_name) != std::string::npos) {
          mygram::utils::StructuredLog()
              .Event("binlog_cache_invalidation")
              .Field("type", "alter_table_column_names")
              .Field("table", event.table_name)
              .Field("cache_key", it->first)
              .Info();
          it = column_names_cache_.erase(it);
        } else {
          ++it;
        }
      }
    }
  }

  // Get BM25 stats for the table (if available)
  server::BM25Stats* bm25_stats = &table_context->bm25_stats;

  // Delegate to BinlogEventProcessor
  return BinlogEventProcessor::ProcessEvent(event, *current_index, *current_doc_store, *current_config, mysql_config_,
                                            server_stats_.load(std::memory_order_acquire),
                                            cache_manager_.load(std::memory_order_acquire), bm25_stats);
}

bool BinlogReader::FetchColumnNames(TableMetadata& metadata) {
  std::string cache_key = metadata.database_name + "." + metadata.table_name;

  // Check cache first
  {
    std::lock_guard<std::mutex> lock(column_names_cache_mutex_);
    auto cache_it = column_names_cache_.find(cache_key);
    if (cache_it != column_names_cache_.end()) {
      // Cache hit: update column definitions from cache
      const auto& column_definitions = cache_it->second;
      if (column_definitions.size() == metadata.columns.size()) {
        for (size_t i = 0; i < metadata.columns.size(); i++) {
          metadata.columns[i].name = column_definitions[i].name;
          metadata.columns[i].is_unsigned = column_definitions[i].is_unsigned;
        }
        mygram::utils::StructuredLog()
            .Event("binlog_debug")
            .Field("action", "column_names_cache_hit")
            .Field("database", metadata.database_name)
            .Field("table", metadata.table_name)
            .Debug();
        return true;
      }
      // Cache mismatch (column count changed?), fall through to query
      mygram::utils::StructuredLog()
          .Event("binlog_warning")
          .Field("type", "column_cache_mismatch")
          .Field("database", metadata.database_name)
          .Field("table", metadata.table_name)
          .Field("cached_count", static_cast<int64_t>(column_definitions.size()))
          .Field("current_count", static_cast<int64_t>(metadata.columns.size()))
          .Message("Cached column definitions have mismatched count")
          .Warn();
      column_names_cache_.erase(cache_it);  // Remove stale cache entry
    }
  }

  // Cache miss or stale: use SHOW COLUMNS (faster than INFORMATION_SCHEMA)
  // Escape backticks in identifier names
  auto escape_identifier = [](const std::string& identifier) {
    std::string escaped;
    escaped.reserve(identifier.length());
    for (char chr : identifier) {
      if (chr == '`') {
        escaped += "``";  // Double backtick for escaping
      } else {
        escaped += chr;
      }
    }
    return escaped;
  };

  std::string query = "SHOW COLUMNS FROM `" + escape_identifier(metadata.database_name) + "`.`" +
                      escape_identifier(metadata.table_name) + "`";

  // Use dedicated metadata connection to avoid thread safety issues (#1)
  // The main connection_ must not be used from the reader thread.
  if (!metadata_connection_) {
    mygram::utils::StructuredLog()
        .Event("binlog_error")
        .Field("type", "metadata_connection_null")
        .Field("database", metadata.database_name)
        .Field("table", metadata.table_name)
        .Error();
    return false;
  }

  auto result_exp = metadata_connection_->Execute(query);
  if (!result_exp) {
    std::string first_error = result_exp.error().message();
    // Connection may have been lost (e.g., after MySQL restart).
    // Try to reconnect the metadata connection and retry once.
    mygram::utils::StructuredLog()
        .Event("binlog_warning")
        .Field("type", "column_query_failed_retrying")
        .Field("database", metadata.database_name)
        .Field("table", metadata.table_name)
        .Field("error", first_error)
        .Warn();

    auto reconnect_result = metadata_connection_->Reconnect(true /* silent */);
    if (reconnect_result) {
      result_exp = metadata_connection_->Execute(query);
    }

    if (!result_exp) {
      mygram::utils::StructuredLog()
          .Event("binlog_error")
          .Field("type", "column_query_failed")
          .Field("database", metadata.database_name)
          .Field("table", metadata.table_name)
          .Field("error", result_exp.error().message())
          .Error();
      return false;
    }
  }

  auto is_unsigned_type = [](const char* column_type) {
    if (column_type == nullptr) {
      return false;
    }
    std::string type(column_type);
    std::transform(type.begin(), type.end(), type.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return type.find("unsigned") != std::string::npos;
  };

  std::vector<BinlogReader::ColumnDefinition> column_definitions;
  column_definitions.reserve(metadata.columns.size());

  MYSQL_ROW row = nullptr;
  while ((row = mysql_fetch_row(result_exp->get())) != nullptr) {
    BinlogReader::ColumnDefinition column_definition;
    column_definition.name = row[0] == nullptr ? std::string{} : std::string(row[0]);
    column_definition.is_unsigned = is_unsigned_type(row[1]);
    column_definitions.push_back(std::move(column_definition));
  }

  // result automatically freed by MySQLResult destructor

  if (column_definitions.size() != metadata.columns.size()) {
    mygram::utils::StructuredLog()
        .Event("binlog_error")
        .Field("type", "column_count_mismatch")
        .Field("database", metadata.database_name)
        .Field("table", metadata.table_name)
        .Field("show_columns_count", static_cast<int64_t>(column_definitions.size()))
        .Field("binlog_count", static_cast<int64_t>(metadata.columns.size()))
        .Error();
    return false;
  }

  // Update metadata with actual column names
  for (size_t i = 0; i < metadata.columns.size(); i++) {
    metadata.columns[i].name = column_definitions[i].name;
    metadata.columns[i].is_unsigned = column_definitions[i].is_unsigned;
  }

  // Store in cache
  {
    std::lock_guard<std::mutex> lock(column_names_cache_mutex_);
    column_names_cache_[cache_key] = std::move(column_definitions);
  }

  mygram::utils::StructuredLog()
      .Event("binlog_debug")
      .Field("action", "fetched_column_names")
      .Field("column_count", static_cast<uint64_t>(metadata.columns.size()))
      .Field("database", metadata.database_name)
      .Field("table", metadata.table_name)
      .Debug();

  return true;
}

std::string BinlogReader::ConvertSingleGtidToRange(const std::string& gtid) {
  if (gtid.empty()) {
    return gtid;
  }

  // MariaDB GTIDs (domain-server-seq) don't need range conversion.
  // MariaDB uses @slave_connect_state which takes the GTID as-is.
  if (MariaDBGTID::IsMariaDBGtidFormat(gtid)) {
    return gtid;
  }

  // If it contains commas, it's multi-UUID - process each entry separately
  if (gtid.find(',') != std::string::npos) {
    std::string result;
    size_t start = 0;
    while (start < gtid.size()) {
      size_t comma_pos = gtid.find(',', start);
      std::string entry;
      if (comma_pos == std::string::npos) {
        entry = gtid.substr(start);
        start = gtid.size();
      } else {
        entry = gtid.substr(start, comma_pos - start);
        start = comma_pos + 1;
      }
      if (!result.empty()) {
        result += ',';
      }
      result += ConvertSingleGtidToRange(entry);
    }
    return result;
  }

  // Find the colon separating UUID from transaction number
  size_t colon_pos = gtid.find(':');
  if (colon_pos == std::string::npos) {
    return gtid;
  }

  std::string after_colon = gtid.substr(colon_pos + 1);

  // If it contains a dash, it's already a range - pass through
  if (after_colon.find('-') != std::string::npos) {
    return gtid;
  }

  // If it contains another colon, it could be tagged GTID or multiple intervals - pass through
  if (after_colon.find(':') != std::string::npos) {
    return gtid;
  }

  // It's a single transaction number (e.g., "uuid:101")
  // Convert to range "uuid:1-101" so the server excludes transactions 1 through 101
  std::string uuid = GtidEncoder::ExtractUuid(gtid);
  return uuid + ":1-" + after_colon;
}

void BinlogReader::UpdateCurrentGTID(const std::string& gtid) {
  std::scoped_lock lock(gtid_mutex_);

  if (MariaDBGTID::IsMariaDBGtidFormat(gtid)) {
    auto parsed = MariaDBGTID::Parse(gtid);
    if (parsed) {
      std::map<uint32_t, MariaDBGTID> by_domain;

      auto current_set = MariaDBGTID::ParseSet(current_gtid_);
      if (current_set) {
        for (const auto& existing : *current_set) {
          auto iter = by_domain.find(existing.domain_id);
          if (iter == by_domain.end() || existing.sequence_no > iter->second.sequence_no) {
            by_domain[existing.domain_id] = existing;
          }
        }
      }

      auto iter = by_domain.find(parsed->domain_id);
      if (iter == by_domain.end() || parsed->sequence_no >= iter->second.sequence_no) {
        by_domain[parsed->domain_id] = *parsed;
      }

      std::vector<MariaDBGTID> merged;
      merged.reserve(by_domain.size());
      for (const auto& [domain_id, domain_gtid] : by_domain) {
        (void)domain_id;
        merged.push_back(domain_gtid);
      }
      current_gtid_ = MariaDBGTID::SetToString(merged);
      return;
    }
  }

  auto merged = GtidEncoder::MergeSingleGtidIntoSet(current_gtid_, gtid);
  if (merged.has_value()) {
    current_gtid_ = *merged;
    return;
  }

  current_gtid_ = gtid;
  // Note: executed_gtid_set_ is intentionally NOT updated here.
  // UpdateCurrentGTID is called with single GTIDs from binlog events (e.g., "uuid:101").
  // The full GTID set for reconnection is maintained separately.
}

bool BinlogReader::RefreshExecutedGtidSet() {
  auto gtid_set = binlog_connection_->GetExecutedGTID();
  if (!gtid_set) {
    mygram::utils::StructuredLog()
        .Event("binlog_warning")
        .Field("type", "refresh_executed_gtid_failed")
        .Field("error", gtid_set.error().message())
        .Warn();
    return false;
  }
  {
    std::scoped_lock lock(gtid_mutex_);
    executed_gtid_set_ = *gtid_set;
  }
  mygram::utils::StructuredLog()
      .Event("binlog_debug")
      .Field("action", "refreshed_executed_gtid_set")
      .Field("gtid_set", *gtid_set)
      .Debug();
  return true;
}

bool BinlogReader::ValidateConnection() {
  // Collect required table names from configuration
  std::vector<std::string> required_tables;

  required_tables.reserve(table_contexts_.size());
  for (const auto& [table_name, ctx] : table_contexts_) {
    required_tables.push_back(ctx->config.name);
  }

  // Get expected server UUID (empty on first connection)
  std::optional<std::string> expected_uuid;
  {
    std::lock_guard<std::mutex> lock(uuid_mutex_);
    if (!last_server_uuid_.empty()) {
      expected_uuid = last_server_uuid_;
    }
  }

  // Pass current GTID position for purge check
  // Always use range format for validation so GTID_SUBSET checks the full history
  std::optional<std::string> current_gtid_for_validation;
  {
    std::lock_guard<std::mutex> lock(gtid_mutex_);
    if (!current_gtid_.empty()) {
      current_gtid_for_validation = ConvertSingleGtidToRange(current_gtid_);
    }
  }

  // Validate connection using ConnectionValidator
  auto result = ConnectionValidator::ValidateServer(*binlog_connection_, required_tables, expected_uuid,
                                                    current_gtid_for_validation);

  if (!result.valid) {
    // Validation failed - server is invalid
    SetLastError("Connection validation failed: " + result.error_message);
    mygram::utils::StructuredLog()
        .Event("binlog_connection_validation_failed")
        .Field("gtid", GetCurrentGTID())
        .Field("error", result.error_message)
        .Error();
    return false;
  }

  // Validation succeeded - update last known server UUID
  if (result.server_uuid) {
    std::lock_guard<std::mutex> lock(uuid_mutex_);
    last_server_uuid_ = *result.server_uuid;
  }

  // Log warnings if any (e.g., failover detected)
  if (!result.warnings.empty()) {
    for (const auto& warning : result.warnings) {
      mygram::utils::StructuredLog()
          .Event("binlog_warning")
          .Field("type", "connection_validation")
          .Field("warning", warning)
          .Warn();
    }
  }

  // Log failover detection if applicable
  if (result.failover_detected) {
    mygram::utils::StructuredLog()
        .Event("mysql_failover_handled")
        .Field("old_uuid", expected_uuid.value_or(""))
        .Field("new_uuid", result.server_uuid.value_or(""))
        .Field("gtid", GetCurrentGTID())
        .Info();
  }

  return true;
}

}  // namespace mygramdb::mysql

// NOLINTEND(cppcoreguidelines-pro-*,cppcoreguidelines-avoid-*,readability-magic-numbers)

#endif  // USE_MYSQL
