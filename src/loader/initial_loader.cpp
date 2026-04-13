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
#include <cctype>
#include <charconv>
#include <chrono>
#include <cstring>
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

  // Debug: log doc_store instance address to verify same instance is used by replication
  // This helps diagnose BUG where replication uses different instance than SYNC populated
  mygram::utils::StructuredLog()
      .Event("initial_loader_start")
      // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) - Required for debug address logging
      .Field("doc_store_addr", reinterpret_cast<uint64_t>(&doc_store_))
      .Field("doc_store_size", static_cast<uint64_t>(doc_store_.Size()))
      .Field("table", table_config_.name)
      .Info();

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

  // Start transaction with consistent snapshot for GTID consistency.
  // InnoDB's consistent snapshot guarantees that @@global.gtid_executed
  // read inside the transaction reflects the snapshot point.
  mygram::utils::StructuredLog().Event("consistent_snapshot_starting").Info();
  auto start_txn_result = connection_.ExecuteUpdate("START TRANSACTION WITH CONSISTENT SNAPSHOT");
  if (!start_txn_result) {
    std::string error_msg = "Failed to start consistent snapshot: " + start_txn_result.error().message();
    mygram::utils::StructuredLog()
        .Event("loader_error")
        .Field("operation", "initial_load")
        .Field("type", "transaction_start_failed")
        .Field("error", error_msg)
        .Error();
    return MakeUnexpected(MakeError(ErrorCode::kStorageSnapshotBuildFailed, error_msg));
  }

  // Capture GTID inside the transaction — consistent with the snapshot
  auto gtid_exp = connection_.Execute("SELECT @@global.gtid_executed");
  if (gtid_exp) {
    MYSQL_ROW row = mysql_fetch_row(gtid_exp->get());
    if (row != nullptr && row[0] != nullptr) {
      start_gtid_ = std::string(row[0]);
      // Remove whitespace (MySQL may include newlines in multi-UUID sets)
      start_gtid_.erase(
          std::remove_if(start_gtid_.begin(), start_gtid_.end(), [](unsigned char chr) { return std::isspace(chr); }),
          start_gtid_.end());
    }
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
    auto rollback_result_gtid = connection_.ExecuteUpdate("ROLLBACK");
    if (!rollback_result_gtid) {
      mygram::utils::StructuredLog()
          .Event("loader_warning")
          .Field("operation", "rollback")
          .Field("error", rollback_result_gtid.error().message())
          .Warn();
    }
    return MakeUnexpected(MakeError(ErrorCode::kStorageSnapshotBuildFailed, error_msg));
  }

  mygram::utils::StructuredLog().Event("initial_load_starting").Field("gtid", start_gtid_).Info();

  // Build SELECT query
  std::string query = BuildSelectQuery();
  mygram::utils::StructuredLog().Event("initial_load_query").Field("query", query).Info();

  auto start_time = std::chrono::steady_clock::now();

  // Execute query (within the consistent snapshot transaction)
  auto result_exp = connection_.Execute(query);
  if (!result_exp) {
    std::string error_msg = "Failed to execute SELECT query: " + result_exp.error().message();
    auto rollback_result_query = connection_.ExecuteUpdate("ROLLBACK");  // Clean up transaction
    if (!rollback_result_query) {
      mygram::utils::StructuredLog()
          .Event("loader_warning")
          .Field("operation", "rollback")
          .Field("error", rollback_result_query.error().message())
          .Warn();
    }
    return MakeUnexpected(MakeError(ErrorCode::kStorageSnapshotBuildFailed, error_msg));
  }

  // Get field metadata
  unsigned int num_fields = mysql_num_fields(result_exp->get());
  MYSQL_FIELD* fields = mysql_fetch_fields(result_exp->get());

  // Get total row count (approximate from result)
  uint64_t total_rows = mysql_num_rows(result_exp->get());

  // Process rows in batches
  MYSQL_ROW row = nullptr;
  processed_rows_ = 0;

  // Determine batch size (use default if not specified)
  size_t batch_size = build_config_.batch_size > 0 ? build_config_.batch_size : kDefaultBatchSize;

  mygram::utils::StructuredLog()
      .Event("initial_load_processing")
      .Field("table", table_config_.name)
      .Field("rows", total_rows)
      .Field("batch_size", static_cast<uint64_t>(batch_size))
      .Info();

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
      auto rollback_result = connection_.ExecuteUpdate("ROLLBACK");
      if (!rollback_result) {
        mygram::utils::StructuredLog()
            .Event("loader_warning")
            .Field("operation", "rollback")
            .Field("error", rollback_result.error().message())
            .Warn();
      }
      return MakeUnexpected(MakeError(ErrorCode::kStorageSnapshotBuildFailed, error_msg));
    }

    // Extract text
    std::string text = ExtractText(row, fields, num_fields);
    if (text.empty()) {
      mygram::utils::StructuredLog()
          .Event("initial_load_skip")
          .Field("reason", "empty_text")
          .Field("primary_key", primary_key)
          .Debug();
      continue;
    }

    // Normalize text
    std::string normalized_text = index_.NormalizeText(text);

    // Extract filters
    auto filters = ExtractFilters(row, fields, num_fields);

    // Add to batch
    doc_batch.push_back({primary_key, filters, normalized_text});
    index_batch.push_back({0, normalized_text});  // DocId will be set after AddDocumentBatch

    // Process batch when full
    if (doc_batch.size() >= batch_size) {
      auto flush_result = FlushBatch(doc_batch, index_batch);
      if (!flush_result) {
        return MakeUnexpected(flush_result.error());
      }

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
  if (!doc_batch.empty() && !index_batch.empty() && !cancelled_) {
    auto flush_result = FlushBatch(doc_batch, index_batch);
    if (!flush_result) {
      return MakeUnexpected(flush_result.error());
    }
  }

  // result automatically freed by MySQLResult destructor

  // Check cancellation before committing to avoid unnecessary COMMIT
  if (cancelled_) {
    auto rollback_result_cancel = connection_.ExecuteUpdate("ROLLBACK");
    if (!rollback_result_cancel) {
      mygram::utils::StructuredLog()
          .Event("loader_warning")
          .Field("operation", "rollback")
          .Field("error", rollback_result_cancel.error().message())
          .Warn();
    }
    std::string error_msg = "Load cancelled";
    return MakeUnexpected(MakeError(ErrorCode::kStorageSnapshotBuildFailed, error_msg));
  }

  // Commit the transaction (releases the snapshot)
  auto commit_result = connection_.ExecuteUpdate("COMMIT");
  if (!commit_result) {
    std::string error_msg = "Failed to commit transaction: " + commit_result.error().message();
    mygram::utils::StructuredLog()
        .Event("loader_error")
        .Field("operation", "initial_load")
        .Field("type", "commit_failed")
        .Field("error", error_msg)
        .Error();
    return MakeUnexpected(MakeError(ErrorCode::kStorageSnapshotBuildFailed, error_msg));
  }

  auto end_time = std::chrono::steady_clock::now();
  double total_elapsed = std::chrono::duration<double>(end_time - start_time).count();
  double rows_per_second = total_elapsed > 0 ? static_cast<double>(processed_rows_) / total_elapsed : 0.0;

  mygram::utils::StructuredLog()
      .Event("initial_load_completed")
      .Field("table", table_config_.name)
      .Field("rows", processed_rows_)
      .Field("elapsed_sec", total_elapsed)
      .Field("rows_per_sec", rows_per_second)
      // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) - Required for debug address logging
      .Field("doc_store_addr", reinterpret_cast<uint64_t>(&doc_store_))
      .Field("doc_store_size", static_cast<uint64_t>(doc_store_.Size()))
      .Info();

  return {};  // Success
}

mygram::utils::Expected<void, mygram::utils::Error> InitialLoader::FlushBatch(
    std::vector<storage::DocumentStore::DocumentItem>& doc_batch,
    std::vector<index::Index::DocumentItem>& index_batch) {
  using mygram::utils::MakeError;
  using mygram::utils::MakeUnexpected;

  // Verify batch sizes match (defensive check)
  if (doc_batch.size() != index_batch.size()) {
    std::string error_msg = "Internal error: doc_batch and index_batch size mismatch (" +
                            std::to_string(doc_batch.size()) + " vs " + std::to_string(index_batch.size()) + ")";
    mygram::utils::StructuredLog()
        .Event("loader_error")
        .Field("operation", "initial_load")
        .Field("type", "batch_size_mismatch")
        .Field("doc_batch_size", static_cast<uint64_t>(doc_batch.size()))
        .Field("index_batch_size", static_cast<uint64_t>(index_batch.size()))
        .Error();
    auto rollback_result = connection_.ExecuteUpdate("ROLLBACK");
    if (!rollback_result) {
      mygram::utils::StructuredLog()
          .Event("loader_warning")
          .Field("operation", "rollback")
          .Field("error", rollback_result.error().message())
          .Warn();
    }
    return MakeUnexpected(MakeError(mygram::utils::ErrorCode::kStorageSnapshotBuildFailed, error_msg));
  }

  // Collect existing doc_ids before batch add to detect duplicates
  std::unordered_set<storage::DocId> existing_doc_ids;
  existing_doc_ids.reserve(doc_batch.size());
  for (const auto& doc : doc_batch) {
    auto existing_id = doc_store_.GetDocId(doc.primary_key);
    if (existing_id) {
      existing_doc_ids.insert(*existing_id);
    }
  }

  // Add documents to document store
  auto doc_ids_result = doc_store_.AddDocumentBatch(doc_batch);
  if (!doc_ids_result) {
    auto rollback_result = connection_.ExecuteUpdate("ROLLBACK");
    if (!rollback_result) {
      mygram::utils::StructuredLog()
          .Event("loader_warning")
          .Field("operation", "rollback")
          .Field("error", rollback_result.error().message())
          .Warn();
    }
    return MakeUnexpected(doc_ids_result.error());
  }
  std::vector<storage::DocId> doc_ids = *doc_ids_result;

  // Verify doc_ids size matches index_batch size (defensive check)
  if (doc_ids.size() != index_batch.size()) {
    std::string error_msg = "Internal error: doc_ids and index_batch size mismatch (" + std::to_string(doc_ids.size()) +
                            " vs " + std::to_string(index_batch.size()) + ")";
    mygram::utils::StructuredLog()
        .Event("loader_error")
        .Field("operation", "initial_load")
        .Field("type", "doc_ids_size_mismatch")
        .Field("doc_ids_size", static_cast<uint64_t>(doc_ids.size()))
        .Field("index_batch_size", static_cast<uint64_t>(index_batch.size()))
        .Error();
    auto rollback_result = connection_.ExecuteUpdate("ROLLBACK");
    if (!rollback_result) {
      mygram::utils::StructuredLog()
          .Event("loader_warning")
          .Field("operation", "rollback")
          .Field("error", rollback_result.error().message())
          .Warn();
    }
    return MakeUnexpected(MakeError(mygram::utils::ErrorCode::kStorageSnapshotBuildFailed, error_msg));
  }

  // Build filtered index batch excluding duplicate PKs (which returned existing doc_ids)
  std::vector<index::Index::DocumentItem> filtered_index_batch;
  filtered_index_batch.reserve(doc_ids.size());
  for (size_t i = 0; i < doc_ids.size(); ++i) {
    if (existing_doc_ids.count(doc_ids[i]) == 0) {
      index_batch[i].doc_id = doc_ids[i];
      filtered_index_batch.push_back(index_batch[i]);
    }
  }

  // Add to index (only new documents, not duplicates)
  index_.AddDocumentBatch(filtered_index_batch);

  processed_rows_ += doc_batch.size();

  // Clear batches
  doc_batch.clear();
  index_batch.clear();

  return {};
}

/**
 * @brief Validate that a string represents a valid numeric value
 *
 * Accepts optional sign, digits, and at most one decimal point.
 */
static bool IsValidNumericValue(const std::string& value) {
  if (value.empty()) {
    return false;
  }
  size_t start = 0;
  if (value[0] == '-' || value[0] == '+') {
    start = 1;
  }
  if (start >= value.size()) {
    return false;
  }
  bool has_dot = false;
  for (size_t i = start; i < value.size(); i++) {
    if (value[i] == '.') {
      if (has_dot) {
        return false;
      }
      has_dot = true;
    } else if (std::isdigit(static_cast<unsigned char>(value[i])) == 0) {
      return false;
    }
  }
  return true;
}

std::string InitialLoader::BuildSelectQuery() const {
  std::ostringstream query;
  query << "SELECT ";

  // Helper to backtick-quote SQL identifiers (column/table names)
  auto quote_identifier = [](const std::string& name) -> std::string { return "`" + name + "`"; };

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
    query << quote_identifier(col);
    first = false;
  }

  query << " FROM " << quote_identifier(table_config_.name);

  // Add WHERE clause from required_filters
  if (!table_config_.required_filters.empty()) {
    // Defense-in-depth: escape filter values to prevent SQL injection.
    // These values come from configuration, but we escape them as a safety measure.
    auto escape_sql_value = [](const std::string& value) -> std::string {
      std::string escaped;
      escaped.reserve(value.size() + value.size() / 8);  // slight overalloc for safety
      for (char chr : value) {
        switch (chr) {
          case '\0':
            escaped += "\\0";
            break;
          case '\'':
            escaped += "''";
            break;
          case '\\':
            escaped += "\\\\";
            break;
          case '\n':
            escaped += "\\n";
            break;
          case '\r':
            escaped += "\\r";
            break;
          case '\x1a':
            escaped += "\\Z";
            break;  // Ctrl+Z (EOF on Windows)
          default:
            escaped += chr;
            break;
        }
      }
      return escaped;
    };

    query << " WHERE ";
    bool first = true;
    for (const auto& filter : table_config_.required_filters) {
      if (!first) {
        query << " AND ";
      }
      first = false;

      query << quote_identifier(filter.name) << " ";

      if (filter.op == "IS NULL" || filter.op == "IS NOT NULL") {
        query << filter.op;
      } else {
        query << filter.op << " ";

        // Lambda to check if type requires quoting
        auto requires_quoting = [&filter]() -> bool {
          return filter.type == "string" || filter.type == "varchar" || filter.type == "text" ||
                 filter.type == "datetime" || filter.type == "date" || filter.type == "timestamp";
        };

        // Quote string values with escaping
        if (requires_quoting()) {
          query << "'" << escape_sql_value(filter.value) << "'";
        } else {
          // Validate numeric values to prevent SQL injection
          if (!IsValidNumericValue(filter.value)) {
            mygram::utils::StructuredLog()
                .Event("loader_error")
                .Field("operation", "build_select_query")
                .Field("type", "invalid_numeric_filter_value")
                .Field("filter_name", filter.name)
                .Field("value", filter.value)
                .Error();
            query.str("");
            query << "SELECT 1 WHERE FALSE /* invalid numeric filter value */";
            return query.str();
          }
          query << filter.value;
        }
      }
    }
  }

  // Add ORDER BY for efficient processing
  query << " ORDER BY " << quote_identifier(table_config_.primary_key);

  return query.str();
}

bool InitialLoader::IsTextColumn(enum_field_types type) {
  // Support VARCHAR and TEXT types (TINY, MEDIUM, LONG, BLOB variants)
  switch (type) {
    case MYSQL_TYPE_VARCHAR:
    case MYSQL_TYPE_VAR_STRING:
    case MYSQL_TYPE_STRING:
    case MYSQL_TYPE_TINY_BLOB:
    case MYSQL_TYPE_MEDIUM_BLOB:
    case MYSQL_TYPE_LONG_BLOB:
    case MYSQL_TYPE_BLOB:
      return true;
    default:
      return false;
  }
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
    // Estimate total size for reservation
    size_t total_estimate = 0;
    for (const auto& col : table_config_.text_source.concat) {
      int idx = FindFieldIndex(col, fields, num_fields);
      if (idx >= 0 && row[idx] != nullptr) {
        total_estimate += std::strlen(row[idx]) + table_config_.text_source.delimiter.size();
      }
    }
    std::string text;
    text.reserve(total_estimate);

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
          if (!text.empty()) {
            text += table_config_.text_source.delimiter;
          }
          text += row[idx];
        }
      }
    }
    return text;
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

storage::FilterMap InitialLoader::ExtractFilters(MYSQL_ROW row, MYSQL_FIELD* fields, unsigned int num_fields) const {
  storage::FilterMap filters;

  for (const auto& filter_config : table_config_.filters) {
    int idx = FindFieldIndex(filter_config.name, fields, num_fields);
    if (idx < 0 || row[idx] == nullptr) {
      continue;
    }

    std::string value_str(row[idx]);
    const std::string& type = filter_config.type;

    // Helper lambda for integer parsing via std::from_chars
    auto parse_integer = [&](auto& out_val) -> bool {
      auto [ptr, ec] = std::from_chars(value_str.data(), value_str.data() + value_str.size(), out_val);
      if (ec != std::errc()) {
        mygram::utils::StructuredLog()
            .Event("loader_warning")
            .Field("operation", "extract_filters")
            .Field("type", "filter_parse_failed")
            .Field("filter_type", type)
            .Field("field", filter_config.name)
            .Field("value", value_str)
            .Warn();
        return false;
      }
      return true;
    };

    {
      // Integer types (using std::from_chars)
      if (type == "tinyint") {
        int8_t val = 0;
        if (parse_integer(val))
          filters[filter_config.name] = val;
      } else if (type == "tinyint_unsigned") {
        uint8_t val = 0;
        if (parse_integer(val))
          filters[filter_config.name] = val;
      } else if (type == "smallint") {
        int16_t val = 0;
        if (parse_integer(val))
          filters[filter_config.name] = val;
      } else if (type == "smallint_unsigned") {
        uint16_t val = 0;
        if (parse_integer(val))
          filters[filter_config.name] = val;
      } else if (type == "int") {
        int32_t val = 0;
        if (parse_integer(val))
          filters[filter_config.name] = val;
      } else if (type == "int_unsigned") {
        uint32_t val = 0;
        if (parse_integer(val))
          filters[filter_config.name] = val;
      } else if (type == "bigint") {
        int64_t val = 0;
        if (parse_integer(val))
          filters[filter_config.name] = val;
      }
      // Float types (std::stod — from_chars for double not reliably available in C++17)
      else if (type == "float" || type == "double") {
        try {
          filters[filter_config.name] = std::stod(value_str);
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
      // String types
      else if (type == "string" || type == "varchar" || type == "text") {
        filters[filter_config.name] = value_str;
      }
      // Date/time types (convert to epoch seconds)
      else if (type == "datetime" || type == "date") {
        // DATETIME/DATE: Convert using configured timezone
        auto epoch_opt = mygram::utils::ParseDatetimeValue(value_str, mysql_config_.datetime_timezone);
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
        uint64_t val = 0;
        if (parse_integer(val)) {
          filters[filter_config.name] = val;
        } else {
          mygram::utils::StructuredLog()
              .Event("loader_warning")
              .Field("operation", "extract_filters")
              .Field("type", "timestamp_conversion_failed")
              .Field("value", value_str)
              .Field("field", filter_config.name)
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
