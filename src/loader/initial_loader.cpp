/**
 * @file initial_loader.cpp
 * @brief Initial data loader implementation
 *
 * Note on clang-tidy suppressions:
 * This file extensively uses MySQL C API which requires pointer arithmetic for result set access.
 * - MYSQL_ROW is defined as char** (array of column values)
 * - MYSQL_FIELD* is an array of field metadata
 * - Column access requires pointer arithmetic: row[column_index], fields[column_index]
 * - This is the standard and only way to access MySQL result columns
 * - Pointer arithmetic warnings are suppressed for the entire file due to MySQL C API requirements
 */

#include "loader/initial_loader.h"

#ifdef USE_MYSQL

// Disable pointer arithmetic warnings for MySQL C API usage throughout this file
// NOLINTBEGIN(cppcoreguidelines-pro-bounds-pointer-arithmetic)

#include <spdlog/spdlog.h>

#include <algorithm>
#include <chrono>
#include <sstream>
#include <unordered_set>
#include <vector>

#include "utils/datetime_converter.h"
#include "utils/string_utils.h"
#include "utils/structured_log.h"

namespace mygramdb::loader {

namespace {
// Default batch size for initial loading
constexpr size_t kDefaultBatchSize = 1000;
}  // namespace

InitialLoader::InitialLoader(mysql::Connection& connection, index::Index& index, storage::DocumentStore& doc_store,
                             config::TableConfig table_config, config::MysqlConfig mysql_config,
                             config::BuildConfig build_config)
    : connection_(connection),
      index_(index),
      doc_store_(doc_store),
      table_config_(std::move(table_config)),
      mysql_config_(std::move(mysql_config)),
      build_config_(std::move(build_config)) {}

mygram::utils::Expected<void, mygram::utils::Error> InitialLoader::Load(const ProgressCallback& progress_callback) {
  using mygram::utils::Error;
  using mygram::utils::ErrorCode;
  using mygram::utils::MakeError;
  using mygram::utils::MakeUnexpected;

  if (!connection_.IsConnected()) {
    std::string error_msg = "MySQL connection not established";
    mygram::utils::StructuredLog()
        .Event("loader_error")
        .Field("operation", "initial_load")
        .Field("error", error_msg)
        .Error();
    return MakeUnexpected(MakeError(ErrorCode::kStorageSnapshotBuildFailed, error_msg));
  }

  // Check if GTID mode is enabled
  if (!connection_.IsGTIDModeEnabled()) {
    std::string error_msg =
        "GTID mode is not enabled on MySQL server. "
        "Please enable GTID mode (gtid_mode=ON) for replication support.";
    mygram::utils::StructuredLog()
        .Event("loader_error")
        .Field("operation", "initial_load")
        .Field("type", "gtid_mode_disabled")
        .Field("error", error_msg)
        .Error();
    return MakeUnexpected(MakeError(ErrorCode::kStorageSnapshotBuildFailed, error_msg));
  }

  // Validate that the primary_key column is unique (PRIMARY KEY or single-column UNIQUE KEY)
  std::string validation_error;
  if (!connection_.ValidateUniqueColumn(connection_.GetConfig().database, table_config_.name, table_config_.primary_key,
                                        validation_error)) {
    std::string error_msg = "Primary key validation failed: " + validation_error;
    mygram::utils::StructuredLog()
        .Event("loader_error")
        .Field("operation", "initial_load")
        .Field("type", "primary_key_validation_failed")
        .Field("table", table_config_.name)
        .Field("primary_key", table_config_.primary_key)
        .Field("error", error_msg)
        .Error();
    return MakeUnexpected(MakeError(ErrorCode::kStorageSnapshotBuildFailed, error_msg));
  }

  // Start transaction with consistent snapshot for GTID consistency
  spdlog::info("Starting consistent snapshot transaction for initial load");
  if (!connection_.ExecuteUpdate("START TRANSACTION WITH CONSISTENT SNAPSHOT")) {
    std::string error_msg = "Failed to start consistent snapshot: " + connection_.GetLastError();
    mygram::utils::StructuredLog()
        .Event("loader_error")
        .Field("operation", "initial_load")
        .Field("type", "transaction_start_failed")
        .Field("error", error_msg)
        .Error();
    return MakeUnexpected(MakeError(ErrorCode::kStorageSnapshotBuildFailed, error_msg));
  }

  // Capture GTID at this point (represents load state)
  auto gtid_result_exp = connection_.Execute("SELECT @@global.gtid_executed");
  if (gtid_result_exp) {
    MYSQL_ROW row = mysql_fetch_row(gtid_result_exp->get());
    if (row != nullptr && row[0] != nullptr) {
      start_gtid_ = std::string(row[0]);
      // Remove all whitespace (newlines, spaces, tabs) from GTID string
      // MySQL may return GTID with newlines when it's long
      start_gtid_.erase(std::remove_if(start_gtid_.begin(), start_gtid_.end(),
                                       [](unsigned char character) { return std::isspace(character); }),
                        start_gtid_.end());
    }
    // gtid_result_exp automatically freed by MySQLResult destructor
  }

  // GTID must not be empty for replication to work
  if (start_gtid_.empty()) {
    std::string error_msg =
        "GTID is empty - cannot start replication from undefined position.\n"
        "This typically happens when GTID mode was recently enabled.\n"
        "To resolve this issue:\n"
        "  1. Execute any write operation on MySQL (e.g., INSERT/UPDATE/DELETE)\n"
        "  2. Verify GTID is set: SELECT @@global.gtid_executed;\n"
        "  3. Restart MygramDB\n"
        "Alternatively, disable replication by setting replication.enable=false in config.";
    mygram::utils::StructuredLog()
        .Event("loader_error")
        .Field("operation", "initial_load")
        .Field("type", "gtid_empty")
        .Field("error", error_msg)
        .Error();
    connection_.ExecuteUpdate("ROLLBACK");
    return MakeUnexpected(MakeError(ErrorCode::kStorageSnapshotBuildFailed, error_msg));
  }

  spdlog::info("Initial load starting from GTID: {}", start_gtid_);

  // Build SELECT query
  std::string query = BuildSelectQuery();
  spdlog::info("Loading initial data with query: {}", query);

  auto start_time = std::chrono::steady_clock::now();

  // Execute query (within the consistent snapshot transaction)
  auto result_exp = connection_.Execute(query);
  if (!result_exp) {
    std::string error_msg = "Failed to execute SELECT query: " + connection_.GetLastError();
    connection_.ExecuteUpdate("ROLLBACK");  // Clean up transaction
    return MakeUnexpected(MakeError(ErrorCode::kStorageSnapshotBuildFailed, error_msg));
  }

  // Get field metadata
  unsigned int num_fields = mysql_num_fields(result_exp->get());
  MYSQL_FIELD* fields = mysql_fetch_fields(result_exp->get());

  // Get total row count (approximate from result)
  uint64_t total_rows = mysql_num_rows(result_exp->get());

  spdlog::info("Processing {} rows from table {}", total_rows, table_config_.name);

  // Process rows in batches
  MYSQL_ROW row = nullptr;
  processed_rows_ = 0;

  // Determine batch size (use default if not specified)
  size_t batch_size = build_config_.batch_size > 0 ? build_config_.batch_size : kDefaultBatchSize;

  std::vector<storage::DocumentStore::DocumentItem> doc_batch;
  std::vector<index::Index::DocumentItem> index_batch;
  doc_batch.reserve(batch_size);
  index_batch.reserve(batch_size);

  while ((row = mysql_fetch_row(result_exp->get())) != nullptr && !cancelled_) {
    // Extract primary key
    std::string primary_key = ExtractPrimaryKey(row, fields, num_fields);
    if (primary_key.empty()) {
      std::string error_msg = "Failed to extract primary key";
      mygram::utils::StructuredLog()
          .Event("loader_error")
          .Field("operation", "initial_load")
          .Field("type", "primary_key_extraction_failed")
          .Field("table", table_config_.name)
          .Field("error", error_msg)
          .Error();
      // result automatically freed by MySQLResult destructor
      connection_.ExecuteUpdate("ROLLBACK");
      return MakeUnexpected(MakeError(ErrorCode::kStorageSnapshotBuildFailed, error_msg));
    }

    // Extract text
    std::string text = ExtractText(row, fields, num_fields);
    if (text.empty()) {
      spdlog::debug("Empty text for primary key {}, skipping", primary_key);
      continue;
    }

    // Normalize text
    std::string normalized_text = utils::NormalizeText(text, true, "keep", true);

    // Extract filters
    auto filters = ExtractFilters(row, fields, num_fields);

    // Add to batch
    doc_batch.push_back({primary_key, filters});
    index_batch.push_back({0, normalized_text});  // DocId will be set after AddDocumentBatch

    // Process batch when full
    if (doc_batch.size() >= batch_size) {
      // Add documents to document store
      auto doc_ids_result = doc_store_.AddDocumentBatch(doc_batch);
      if (!doc_ids_result) {
        return MakeUnexpected(doc_ids_result.error());
      }
      std::vector<storage::DocId> doc_ids = *doc_ids_result;

      // Update index_batch with assigned doc_ids
      for (size_t i = 0; i < doc_ids.size(); ++i) {
        index_batch[i].doc_id = doc_ids[i];
      }

      // Add to index
      index_.AddDocumentBatch(index_batch);

      processed_rows_ += doc_batch.size();

      // Clear batches
      doc_batch.clear();
      index_batch.clear();

      // Progress callback
      if (progress_callback) {
        auto now = std::chrono::steady_clock::now();
        double elapsed = std::chrono::duration<double>(now - start_time).count();

        LoadProgress progress;
        progress.total_rows = total_rows;
        progress.processed_rows = processed_rows_;
        progress.elapsed_seconds = elapsed;
        progress.rows_per_second = elapsed > 0 ? static_cast<double>(processed_rows_) / elapsed : 0.0;

        progress_callback(progress);
      }
    }
  }

  // Process remaining rows in batch
  if (!doc_batch.empty() && !cancelled_) {
    // Add documents to document store
    auto doc_ids_result = doc_store_.AddDocumentBatch(doc_batch);
    if (!doc_ids_result) {
      return MakeUnexpected(doc_ids_result.error());
    }
    std::vector<storage::DocId> doc_ids = *doc_ids_result;

    // Update index_batch with assigned doc_ids
    for (size_t i = 0; i < doc_ids.size(); ++i) {
      index_batch[i].doc_id = doc_ids[i];
    }

    // Add to index
    index_.AddDocumentBatch(index_batch);

    processed_rows_ += doc_batch.size();
  }

  // result automatically freed by MySQLResult destructor

  // Commit the transaction (releases the snapshot)
  if (!connection_.ExecuteUpdate("COMMIT")) {
    mygram::utils::StructuredLog()
        .Event("loader_warning")
        .Field("operation", "initial_load")
        .Field("type", "commit_failed")
        .Field("error", connection_.GetLastError())
        .Warn();
  }

  if (cancelled_) {
    std::string error_msg = "Load cancelled";
    return MakeUnexpected(MakeError(ErrorCode::kStorageSnapshotBuildFailed, error_msg));
  }

  auto end_time = std::chrono::steady_clock::now();
  double total_elapsed = std::chrono::duration<double>(end_time - start_time).count();

  spdlog::info("Initial load completed: {} rows in {:.2f}s ({:.0f} rows/s)", processed_rows_, total_elapsed,
               total_elapsed > 0 ? static_cast<double>(processed_rows_) / total_elapsed : 0.0);

  return {};  // Success
}

std::string InitialLoader::BuildSelectQuery() const {
  std::ostringstream query;
  query << "SELECT ";

  // Collect all columns to SELECT (avoiding duplicates, preserving order)
  std::vector<std::string> selected_columns;
  std::unordered_set<std::string> seen_columns;

  // Helper to add column if not already added
  auto add_column = [&](const std::string& col) {
    if (seen_columns.find(col) == seen_columns.end()) {
      selected_columns.push_back(col);
      seen_columns.insert(col);
    }
  };

  // Primary key (always first)
  add_column(table_config_.primary_key);

  // Text source columns
  if (!table_config_.text_source.column.empty()) {
    add_column(table_config_.text_source.column);
  } else {
    for (const auto& col : table_config_.text_source.concat) {
      add_column(col);
    }
  }

  // Required filter columns (for binlog replication condition checking)
  for (const auto& filter : table_config_.required_filters) {
    add_column(filter.name);
  }

  // Optional filter columns (for search-time filtering)
  for (const auto& filter : table_config_.filters) {
    add_column(filter.name);
  }

  // Build SELECT clause from collected columns
  bool first = true;
  for (const auto& col : selected_columns) {
    if (!first) {
      query << ", ";
    }
    query << col;
    first = false;
  }

  query << " FROM " << table_config_.name;

  // Add WHERE clause from required_filters
  if (!table_config_.required_filters.empty()) {
    query << " WHERE ";
    bool first = true;
    for (const auto& filter : table_config_.required_filters) {
      if (!first) {
        query << " AND ";
      }
      first = false;

      query << filter.name << " ";

      if (filter.op == "IS NULL" || filter.op == "IS NOT NULL") {
        query << filter.op;
      } else {
        query << filter.op << " ";

        // Quote string values
        if (filter.type == "string" || filter.type == "varchar" || filter.type == "text" || filter.type == "datetime" ||
            filter.type == "date" || filter.type == "timestamp") {
          query << "'" << filter.value << "'";
        } else {
          query << filter.value;
        }
      }
    }
  }

  // Add ORDER BY for efficient processing
  query << " ORDER BY " << table_config_.primary_key;

  return query.str();
}

mygram::utils::Expected<void, mygram::utils::Error> InitialLoader::ProcessRow(MYSQL_ROW row, MYSQL_FIELD* fields,
                                                                              unsigned int num_fields) {
  using mygram::utils::Error;
  using mygram::utils::ErrorCode;
  using mygram::utils::MakeError;
  using mygram::utils::MakeUnexpected;

  // Extract primary key
  std::string primary_key = ExtractPrimaryKey(row, fields, num_fields);
  if (primary_key.empty()) {
    std::string error_msg = "Failed to extract primary key";
    return MakeUnexpected(MakeError(ErrorCode::kStorageSnapshotBuildFailed, error_msg));
  }

  // Extract text
  std::string text = ExtractText(row, fields, num_fields);
  if (text.empty()) {
    spdlog::debug("Empty text for primary key {}, skipping", primary_key);
    return {};  // Skip empty documents (success)
  }

  // Normalize text
  std::string normalized_text = utils::NormalizeText(text, true, "keep", true);

  // Extract filters
  auto filters = ExtractFilters(row, fields, num_fields);

  // Add to document store
  auto doc_id_result = doc_store_.AddDocument(primary_key, filters);
  if (!doc_id_result) {
    return MakeUnexpected(doc_id_result.error());
  }
  storage::DocId doc_id = *doc_id_result;

  // Add to index
  index_.AddDocument(doc_id, normalized_text);

  return {};  // Success
}

bool InitialLoader::IsTextColumn(enum_field_types type) {
  // Support VARCHAR and TEXT types (TINY, MEDIUM, LONG, BLOB variants)
  return type == MYSQL_TYPE_VARCHAR || type == MYSQL_TYPE_VAR_STRING || type == MYSQL_TYPE_STRING ||
         type == MYSQL_TYPE_TINY_BLOB || type == MYSQL_TYPE_MEDIUM_BLOB || type == MYSQL_TYPE_LONG_BLOB ||
         type == MYSQL_TYPE_BLOB;
}

std::string InitialLoader::ExtractText(MYSQL_ROW row, MYSQL_FIELD* fields, unsigned int num_fields) const {
  if (!table_config_.text_source.column.empty()) {
    // Single column
    int idx = FindFieldIndex(table_config_.text_source.column, fields, num_fields);
    if (idx >= 0) {
      // Validate column type
      if (!IsTextColumn(fields[idx].type)) {
        mygram::utils::StructuredLog()
            .Event("loader_error")
            .Field("operation", "extract_text")
            .Field("type", "invalid_column_type")
            .Field("column", table_config_.text_source.column)
            .Field("expected", "VARCHAR/TEXT")
            .Field("actual_type_id", static_cast<uint64_t>(fields[idx].type))
            .Error();
        return "";
      }
      if (row[idx] != nullptr) {
        return {row[idx]};
      }
    }
  } else {
    // Concatenate columns
    std::ostringstream text;
    for (const auto& col : table_config_.text_source.concat) {
      int idx = FindFieldIndex(col, fields, num_fields);
      if (idx >= 0) {
        // Validate column type
        if (!IsTextColumn(fields[idx].type)) {
          mygram::utils::StructuredLog()
              .Event("loader_error")
              .Field("operation", "extract_text_concat")
              .Field("type", "invalid_column_type")
              .Field("column", col)
              .Field("expected", "VARCHAR/TEXT")
              .Field("actual_type_id", static_cast<uint64_t>(fields[idx].type))
              .Error();
          continue;  // Skip this column
        }
        if (row[idx] != nullptr) {
          if (text.tellp() > 0) {
            text << table_config_.text_source.delimiter;
          }
          text << row[idx];
        }
      }
    }
    return text.str();
  }

  return "";
}

std::string InitialLoader::ExtractPrimaryKey(MYSQL_ROW row, MYSQL_FIELD* fields, unsigned int num_fields) const {
  int idx = FindFieldIndex(table_config_.primary_key, fields, num_fields);
  if (idx >= 0 && row[idx] != nullptr) {
    return {row[idx]};
  }
  return "";
}

std::unordered_map<std::string, storage::FilterValue> InitialLoader::ExtractFilters(MYSQL_ROW row, MYSQL_FIELD* fields,
                                                                                    unsigned int num_fields) const {
  std::unordered_map<std::string, storage::FilterValue> filters;

  for (const auto& filter_config : table_config_.filters) {
    int idx = FindFieldIndex(filter_config.name, fields, num_fields);
    if (idx < 0 || row[idx] == nullptr) {
      continue;
    }

    std::string value_str(row[idx]);
    const std::string& type = filter_config.type;

    try {
      // Integer types
      if (type == "tinyint") {
        filters[filter_config.name] = static_cast<int8_t>(std::stoi(value_str));
      } else if (type == "tinyint_unsigned") {
        filters[filter_config.name] = static_cast<uint8_t>(std::stoul(value_str));
      } else if (type == "smallint") {
        filters[filter_config.name] = static_cast<int16_t>(std::stoi(value_str));
      } else if (type == "smallint_unsigned") {
        filters[filter_config.name] = static_cast<uint16_t>(std::stoul(value_str));
      } else if (type == "int") {
        filters[filter_config.name] = static_cast<int32_t>(std::stoi(value_str));
      } else if (type == "int_unsigned") {
        filters[filter_config.name] = static_cast<uint32_t>(std::stoul(value_str));
      } else if (type == "bigint") {
        filters[filter_config.name] = std::stoll(value_str);
      }
      // Float types
      else if (type == "float" || type == "double") {
        filters[filter_config.name] = std::stod(value_str);
      }
      // String types
      else if (type == "string" || type == "varchar" || type == "text") {
        filters[filter_config.name] = value_str;
      }
      // Date/time types (convert to epoch seconds)
      else if (type == "datetime" || type == "date") {
        // DATETIME/DATE: Convert using configured timezone
        auto epoch_opt = mygramdb::utils::ParseDatetimeValue(value_str, mysql_config_.datetime_timezone);
        if (epoch_opt) {
          filters[filter_config.name] = *epoch_opt;
        } else {
          mygram::utils::StructuredLog()
              .Event("loader_warning")
              .Field("operation", "extract_filters")
              .Field("type", "datetime_conversion_failed")
              .Field("value", value_str)
              .Field("field", filter_config.name)
              .Field("timezone", mysql_config_.datetime_timezone)
              .Warn();
        }
      } else if (type == "timestamp") {
        // TIMESTAMP: Already in epoch seconds (UTC)
        try {
          filters[filter_config.name] = static_cast<uint64_t>(std::stoull(value_str));
        } catch (const std::exception& e) {
          mygram::utils::StructuredLog()
              .Event("loader_warning")
              .Field("operation", "extract_filters")
              .Field("type", "timestamp_conversion_failed")
              .Field("value", value_str)
              .Field("field", filter_config.name)
              .Field("error", e.what())
              .Warn();
        }
      } else if (type == "time") {
        // TIME: Convert to seconds since midnight using DateTimeProcessor
        auto processor_result = mysql_config_.CreateDateTimeProcessor();
        if (!processor_result) {
          mygram::utils::StructuredLog()
              .Event("loader_warning")
              .Field("operation", "extract_filters")
              .Field("type", "datetime_processor_creation_failed")
              .Field("field", filter_config.name)
              .Field("error", processor_result.error().message())
              .Warn();
        } else {
          auto seconds_result = processor_result->TimeToSeconds(value_str);
          if (seconds_result) {
            filters[filter_config.name] = storage::TimeValue{*seconds_result};
          } else {
            mygram::utils::StructuredLog()
                .Event("loader_warning")
                .Field("operation", "extract_filters")
                .Field("type", "time_conversion_failed")
                .Field("value", value_str)
                .Field("field", filter_config.name)
                .Field("error", seconds_result.error().message())
                .Warn();
          }
        }
      } else {
        mygram::utils::StructuredLog()
            .Event("loader_warning")
            .Field("operation", "extract_filters")
            .Field("type", "unknown_filter_type")
            .Field("filter_type", type)
            .Field("field", filter_config.name)
            .Warn();
      }
    } catch (const std::exception& e) {
      mygram::utils::StructuredLog()
          .Event("loader_warning")
          .Field("operation", "extract_filters")
          .Field("type", "filter_parse_failed")
          .Field("filter_type", type)
          .Field("field", filter_config.name)
          .Field("value", value_str)
          .Field("error", e.what())
          .Warn();
    }
  }

  return filters;
}

int InitialLoader::FindFieldIndex(const std::string& field_name, MYSQL_FIELD* fields, unsigned int num_fields) {
  for (unsigned int i = 0; i < num_fields; ++i) {
    if (field_name == fields[i].name) {
      return static_cast<int>(i);
    }
  }
  return -1;
}

}  // namespace mygramdb::loader

// NOLINTEND(cppcoreguidelines-pro-bounds-pointer-arithmetic)

#endif  // USE_MYSQL
