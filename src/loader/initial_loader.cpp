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
#include <chrono>
#include <optional>
#include <sstream>
#include <unordered_set>
#include <vector>

#include "mysql/column_type_support.h"
#include "mysql/required_filter_predicate.h"
#include "mysql/rows_parser.h"
#include "mysql/value_canonicalizer.h"
#include "utils/sql_utils.h"
#include "utils/string_utils.h"
#include "utils/structured_log.h"

namespace mygramdb::loader {

namespace {
// Default batch size for initial loading
constexpr size_t kDefaultBatchSize = 1000;

/// The support row for a result-set column, keyed by the code MySQL reports.
mysql::ColumnTypeSupport ColumnSupport(MYSQL_FIELD* fields, int index) {
  return mysql::DescribeColumnType(static_cast<mysql::ColumnType>(fields[index].type));
}

std::string CanonicalizeSnapshotField(MYSQL_ROW row, const unsigned long* lengths, MYSQL_FIELD* fields, int index) {
  if (row[index] == nullptr) {
    return {};
  }
  return mysql::CanonicalizeColumnValue(std::string_view(row[index], lengths[index]),
                                        ColumnSupport(fields, index).snapshot_normalization);
}
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
  return LoadInternal(progress_callback, true, nullptr);
}

mygram::utils::Expected<void, mygram::utils::Error> InitialLoader::LoadFromExistingSnapshot(
    const std::string& snapshot_gtid, const ProgressCallback& progress_callback) {
  return LoadInternal(progress_callback, false, &snapshot_gtid);
}

mygram::utils::Expected<void, mygram::utils::Error> InitialLoader::LoadInternal(
    const ProgressCallback& progress_callback, bool manage_transaction, const std::string* existing_snapshot_gtid) {
  using mygram::utils::Error;
  using mygram::utils::ErrorCode;
  using mygram::utils::MakeError;
  using mygram::utils::MakeUnexpected;

  if (cancelled_.load(std::memory_order_acquire)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageSnapshotBuildFailed, "Load cancelled"));
  }

  auto rollback_if_managed = [&]() {
    if (!manage_transaction) {
      return;
    }
    auto rollback_result = connection_.ExecuteUpdate("ROLLBACK");
    if (!rollback_result) {
      mygram::utils::StructuredLog()
          .Event("loader_warning")
          .Field("operation", "rollback")
          .Field("error", rollback_result.error().message())
          .Warn();
    }
  };

  // Debug: log doc_store instance address to verify same instance is used by replication
  // This helps diagnose replication using a different instance than SYNC populated.
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
  auto gtid_mode_enabled = connection_.IsGTIDModeEnabled();
  if (!gtid_mode_enabled) {
    mygram::utils::StructuredLog()
        .Event("loader_error")
        .Field("operation", "initial_load")
        .Field("type", "gtid_mode_query_failed")
        .Field("error", gtid_mode_enabled.error().message())
        .Error();
    return MakeUnexpected(gtid_mode_enabled.error());
  }
  if (!*gtid_mode_enabled) {
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
  auto validate_result =
      connection_.ValidateUniqueColumn(table_config_.database, table_config_.name, table_config_.primary_key);
  if (!validate_result) {
    std::string error_msg = "Primary key validation failed: " + validate_result.error().message();
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

  start_gtid_.clear();
  if (manage_transaction) {
    // Capture a conservative replication position BEFORE opening the snapshot.
    // @@GLOBAL.gtid_executed / gtid_current_pos are server-global status
    // variables, not MVCC data: reading them after START TRANSACTION can include
    // commits that are absent from the snapshot and would then be skipped by
    // replication. A pre-snapshot position intentionally permits at-least-once
    // replay for commits in the small interval before START; row application is
    // idempotent and converges to the snapshot/current binlog state.
    auto gtid_result = connection_.CaptureSnapshotLowerBoundGTID();
    if (!gtid_result) {
      return MakeUnexpected(MakeError(ErrorCode::kStorageSnapshotBuildFailed,
                                      "Failed to capture pre-snapshot GTID: " + gtid_result.error().message()));
    }
    start_gtid_ = *gtid_result;
    if (after_gtid_capture_hook_for_test_) {
      after_gtid_capture_hook_for_test_();
    }

    // Start the consistent snapshot only after the safe lower-bound GTID has
    // been captured.
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
  } else if (existing_snapshot_gtid != nullptr) {
    start_gtid_ = *existing_snapshot_gtid;
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
    rollback_if_managed();
    return MakeUnexpected(MakeError(ErrorCode::kStorageSnapshotBuildFailed, error_msg));
  }

  mygram::utils::StructuredLog().Event("initial_load_starting").Field("gtid", start_gtid_).Info();

  if (cancelled_.load(std::memory_order_acquire)) {
    rollback_if_managed();
    return MakeUnexpected(MakeError(ErrorCode::kStorageSnapshotBuildFailed, "Load cancelled"));
  }

  // Build SELECT query
  std::string query = BuildSelectQuery();
  if (query.empty()) {
    std::string error_msg = "Invalid identifier or required filter value in initial load query";
    rollback_if_managed();
    return MakeUnexpected(MakeError(ErrorCode::kStorageSnapshotBuildFailed, error_msg));
  }
  mygram::utils::StructuredLog().Event("initial_load_query").Field("query", query).Info();

  auto start_time = std::chrono::steady_clock::now();

  // Execute query (within the consistent snapshot transaction)
  auto result_exp = connection_.ExecuteStreaming(query);
  if (!result_exp) {
    std::string error_msg = "Failed to execute SELECT query: " + result_exp.error().message();
    rollback_if_managed();
    return MakeUnexpected(MakeError(ErrorCode::kStorageSnapshotBuildFailed, error_msg));
  }

  // mysql_use_result() keeps unread rows on the wire. No other statement may
  // run on this connection until EOF, so cancellation and mid-stream failures
  // discard this dedicated load connection instead of trying to ROLLBACK it.
  auto abort_stream = [&]() {
    result_exp->reset();
    connection_.Close();
  };

  // Get field metadata
  unsigned int num_fields = mysql_num_fields(result_exp->get());
  MYSQL_FIELD* fields = mysql_fetch_fields(result_exp->get());

  // Build field-name-to-index map once to avoid O(N) FindFieldIndex per column per row
  FieldIndexMap field_map = BuildFieldIndexMap(fields, num_fields);

  // An unbuffered result cannot know its row count before EOF. Report an
  // indeterminate total (zero) while continuing to publish processed rows.
  constexpr uint64_t total_rows = 0;

  // Process rows in batches
  MYSQL_ROW row = nullptr;
  processed_rows_.store(0, std::memory_order_relaxed);

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

  bool stream_exhausted = false;
  while (!cancelled_.load(std::memory_order_relaxed)) {
    row = mysql_fetch_row(result_exp->get());
    if (row == nullptr) {
      stream_exhausted = true;
      break;
    }
    if (cancelled_.load(std::memory_order_relaxed)) {
      break;
    }

    const unsigned long* lengths = mysql_fetch_lengths(result_exp->get());
    if (lengths == nullptr) {
      const std::string error_msg = "mysql_fetch_lengths failed while reading initial snapshot";
      mygram::utils::StructuredLog()
          .Event("loader_error")
          .Field("operation", "initial_load")
          .Field("type", "row_lengths_unavailable")
          .Field("table", table_config_.name)
          .Error();
      abort_stream();
      return MakeUnexpected(MakeError(ErrorCode::kStorageSnapshotBuildFailed, error_msg));
    }

    // Extract primary key (using pre-built field index map for O(1) lookup)
    auto primary_key = ExtractPrimaryKey(row, lengths, fields, field_map);
    if (!primary_key.has_value()) {
      std::string error_msg = "Primary key column '" + table_config_.primary_key +
                              "' is missing or NULL in initial snapshot for table '" + table_config_.name + "'";
      mygram::utils::StructuredLog()
          .Event("loader_error")
          .Field("operation", "initial_load")
          .Field("type", "primary_key_extraction_failed")
          .Field("table", table_config_.name)
          .Field("primary_key_column", table_config_.primary_key)
          .Field("error", error_msg)
          .Error();
      // result automatically freed by MySQLResult destructor
      abort_stream();
      return MakeUnexpected(MakeError(ErrorCode::kStorageSnapshotBuildFailed, error_msg));
    }

    // Extract text (using pre-built field index map for O(1) lookup)
    auto materialized_text = ExtractText(row, lengths, fields, field_map);
    if (!materialized_text.IsAvailable()) {
      const std::string error_msg = "Configured text source column is absent from initial snapshot row";
      mygram::utils::StructuredLog()
          .Event("loader_error")
          .Field("operation", "initial_load")
          .Field("type", "text_source_absent")
          .Field("primary_key", *primary_key)
          .Error();
      abort_stream();
      return MakeUnexpected(MakeError(ErrorCode::kStorageSnapshotBuildFailed, error_msg));
    }
    std::string text = std::move(materialized_text.value);

    // Normalize text
    std::string normalized_text = index_.NormalizeText(text);

    // Extract filters (using pre-built field index map for O(1) lookup)
    auto filters = ExtractFilters(row, lengths, fields, field_map);

    // Add to batch
    doc_batch.push_back({std::move(*primary_key), filters, normalized_text, text});
    index_batch.push_back({0, normalized_text});  // DocId will be set after AddDocumentBatch

    // Process batch when full
    if (doc_batch.size() >= batch_size) {
      auto flush_result = FlushBatch(doc_batch, index_batch);
      if (!flush_result) {
        abort_stream();
        return MakeUnexpected(flush_result.error());
      }

      // Progress callback
      if (progress_callback) {
        auto now = std::chrono::steady_clock::now();
        double elapsed = std::chrono::duration<double>(now - start_time).count();

        LoadProgress progress;
        progress.total_rows = total_rows;
        progress.processed_rows = processed_rows_.load(std::memory_order_relaxed);
        progress.elapsed_seconds = elapsed;
        progress.rows_per_second =
            elapsed > 0 ? static_cast<double>(processed_rows_.load(std::memory_order_relaxed)) / elapsed : 0.0;

        progress_callback(progress);
      }
    }
  }

  if (stream_exhausted && mysql_errno(connection_.GetHandle()) != 0) {
    const std::string error_msg =
        "Failed while streaming initial snapshot rows: " + std::string(mysql_error(connection_.GetHandle()));
    abort_stream();
    return MakeUnexpected(MakeError(ErrorCode::kStorageSnapshotBuildFailed, error_msg));
  }

  // Process remaining rows in batch
  if (!doc_batch.empty() && !index_batch.empty() && !cancelled_.load(std::memory_order_relaxed)) {
    auto flush_result = FlushBatch(doc_batch, index_batch);
    if (!flush_result) {
      result_exp->reset();
      rollback_if_managed();
      return MakeUnexpected(flush_result.error());
    }
  }

  // Release the stream before COMMIT/ROLLBACK. At EOF the connection remains
  // reusable; an early cancellation leaves unread protocol data and therefore
  // requires discarding the load connection.
  result_exp->reset();

  // Check cancellation before committing to avoid unnecessary COMMIT
  if (cancelled_.load(std::memory_order_relaxed)) {
    connection_.Close();
    std::string error_msg = "Load cancelled";
    return MakeUnexpected(MakeError(ErrorCode::kStorageSnapshotBuildFailed, error_msg));
  }

  if (manage_transaction) {
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
  }

  auto end_time = std::chrono::steady_clock::now();
  double total_elapsed = std::chrono::duration<double>(end_time - start_time).count();
  double rows_per_second =
      total_elapsed > 0 ? static_cast<double>(processed_rows_.load(std::memory_order_relaxed)) / total_elapsed : 0.0;

  mygram::utils::StructuredLog()
      .Event("initial_load_completed")
      .Field("table", table_config_.name)
      .Field("rows", processed_rows_.load(std::memory_order_relaxed))
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
    return MakeUnexpected(MakeError(mygram::utils::ErrorCode::kStorageSnapshotBuildFailed, error_msg));
  }

  // Add documents to document store (collecting duplicate info in one pass)
  std::unordered_set<storage::DocId> existing_doc_ids;
  auto doc_ids_result = doc_store_.AddDocumentBatch(doc_batch, &existing_doc_ids);
  if (!doc_ids_result) {
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

  processed_rows_.fetch_add(doc_batch.size(), std::memory_order_relaxed);

  // Clear batches
  doc_batch.clear();
  index_batch.clear();

  return {};
}

std::string InitialLoader::BuildSelectQuery() const {
  return internal::BuildInitialLoadSelectQuery(table_config_, mysql_config_);
}

std::string internal::BuildInitialLoadSelectQuery(const config::TableConfig& table_config,
                                                  const config::MysqlConfig& mysql_config) {
  std::ostringstream query;
  query << "SELECT ";

  auto quote_identifier = [](const std::string& name) { return mygramdb::utils::QuoteSQLIdentifier(name); };

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
  add_column(table_config.primary_key);

  // Text source columns
  if (!table_config.text_source.column.empty()) {
    add_column(table_config.text_source.column);
  } else {
    for (const auto& col : table_config.text_source.concat) {
      add_column(col);
    }
  }

  // Required filter columns (for binlog replication condition checking)
  for (const auto& filter : table_config.required_filters) {
    add_column(filter.name);
  }

  // Optional filter columns (for search-time filtering)
  for (const auto& filter : table_config.filters) {
    add_column(filter.name);
  }

  // Build SELECT clause from collected columns
  bool first_select_column = true;
  for (const auto& col : selected_columns) {
    if (!first_select_column) {
      query << ", ";
    }
    auto quoted_column = quote_identifier(col);
    if (!quoted_column) {
      return "";
    }
    query << *quoted_column;
    first_select_column = false;
  }

  auto quoted_table = mygramdb::utils::QuoteQualifiedSQLIdentifier(table_config.database, table_config.name);
  if (!quoted_table) {
    return "";
  }
  query << " FROM " << *quoted_table;

  // Add WHERE clause from required_filters
  if (!table_config.required_filters.empty()) {
    query << " WHERE ";
    bool first_required_filter = true;
    for (const auto& filter : table_config.required_filters) {
      if (!first_required_filter) {
        query << " AND ";
      }
      first_required_filter = false;

      auto quoted_filter = quote_identifier(filter.name);
      if (!quoted_filter) {
        return "";
      }

      // The conjunct comes from the same declaration BinlogFilterEvaluator
      // decides membership with, so the rows this SELECT returns are exactly
      // the rows replication keeps.
      auto predicate = mysql::RequiredFilterPredicate::Resolve(filter, mysql_config.datetime_timezone);
      if (!predicate) {
        mygram::utils::StructuredLog()
            .Event("loader_error")
            .Field("operation", "build_select_query")
            .Field("type", "unusable_required_filter")
            .Field("filter_name", filter.name)
            .Field("error", predicate.error().message())
            .Error();
        return "";
      }
      query << predicate->SqlPredicate(*quoted_filter);
    }
  }

  // Add ORDER BY for efficient processing
  auto quoted_primary_key = quote_identifier(table_config.primary_key);
  if (!quoted_primary_key) {
    return "";
  }
  query << " ORDER BY " << *quoted_primary_key;

  return query.str();
}

InitialLoader::FieldIndexMap InitialLoader::BuildFieldIndexMap(MYSQL_FIELD* fields, unsigned int num_fields) {
  FieldIndexMap field_map;
  field_map.reserve(num_fields);
  for (unsigned int i = 0; i < num_fields; ++i) {
    field_map[fields[i].name] = static_cast<int>(i);
  }
  return field_map;
}

mysql::MaterializedText InitialLoader::ExtractText(MYSQL_ROW row, const unsigned long* lengths, MYSQL_FIELD* fields,
                                                   const FieldIndexMap& field_map) const {
  mysql::RowData source_row;
  const auto add_source_column = [&](const std::string& column) -> bool {
    auto it = field_map.find(column);
    if (it == field_map.end()) {
      return false;
    }
    const int idx = it->second;
    // A type the two ingest paths do not agree on is refused before replication
    // starts, so reaching this is a column whose type changed under a running
    // server. Refusing it here keeps the snapshot from publishing text the
    // binlog would never reproduce.
    if (ColumnSupport(fields, idx).acceptance != mysql::ColumnAcceptance::kAccepted) {
      mygram::utils::StructuredLog()
          .Event("loader_error")
          .Field("operation", "extract_text")
          .Field("type", "unsupported_column_type")
          .Field("column", column)
          .Field("actual_type_id", static_cast<uint64_t>(fields[idx].type))
          .Error();
      return false;
    }
    if (row[idx] == nullptr) {
      source_row.columns[column] = "";
      source_row.null_columns.insert(column);
    } else {
      source_row.columns[column] = CanonicalizeSnapshotField(row, lengths, fields, idx);
    }
    return true;
  };

  if (!table_config_.text_source.column.empty()) {
    if (!add_source_column(table_config_.text_source.column)) {
      return {};
    }
  } else {
    for (const auto& column : table_config_.text_source.concat) {
      if (!add_source_column(column)) {
        return {};
      }
    }
  }
  return mysql::MaterializeTextSource(source_row, table_config_.text_source);
}

std::optional<std::string> InitialLoader::ExtractPrimaryKey(MYSQL_ROW row, const unsigned long* lengths,
                                                            MYSQL_FIELD* fields, const FieldIndexMap& field_map) const {
  auto it = field_map.find(table_config_.primary_key);
  if (it != field_map.end() && row[it->second] != nullptr) {
    return CanonicalizeSnapshotField(row, lengths, fields, it->second);
  }
  return std::nullopt;
}

storage::FilterMap InitialLoader::ExtractFilters(MYSQL_ROW row, const unsigned long* lengths, MYSQL_FIELD* fields,
                                                 const FieldIndexMap& field_map) const {
  storage::FilterMap filters;

  for (const auto& filter_config : config::BuildUnifiedFilterConfigs(table_config_)) {
    auto it = field_map.find(filter_config.name);
    if (it == field_map.end()) {
      continue;
    }
    int idx = it->second;

    const bool is_null = row[idx] == nullptr;
    std::string value_str = is_null ? std::string{} : CanonicalizeSnapshotField(row, lengths, fields, idx);
    const std::string& type = filter_config.type;

    auto converted = mysql::ConvertFilterValue(value_str, is_null, type, mysql_config_.datetime_timezone);
    if (converted.has_value()) {
      filters[filter_config.name] = std::move(*converted);
    } else {
      mygram::utils::StructuredLog()
          .Event("loader_warning")
          .Field("operation", "extract_filters")
          .Field("type", "filter_conversion_failed")
          .Field("filter_type", type)
          .Field("field", filter_config.name)
          .Warn();
    }
  }

  return filters;
}

}  // namespace mygramdb::loader

// NOLINTEND(cppcoreguidelines-pro-bounds-pointer-arithmetic)

#endif  // USE_MYSQL
