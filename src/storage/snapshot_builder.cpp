/**
 * @file snapshot_builder.cpp
 * @brief Snapshot builder implementation
 */

#include "storage/snapshot_builder.h"

#ifdef USE_MYSQL

#include <spdlog/spdlog.h>

#include <chrono>
#include <sstream>

#include "utils/string_utils.h"

namespace mygramdb {
namespace storage {

SnapshotBuilder::SnapshotBuilder(mysql::Connection& connection, index::Index& index,
                                 DocumentStore& doc_store, config::TableConfig table_config,
                                 config::BuildConfig build_config)
    : connection_(connection),
      index_(index),
      doc_store_(doc_store),
      table_config_(std::move(table_config)),
      build_config_(std::move(build_config)) {}

bool SnapshotBuilder::Build(const ProgressCallback& progress_callback) {
  if (!connection_.IsConnected()) {
    last_error_ = "MySQL connection not established";
    spdlog::error(last_error_);
    return false;
  }

  // Check if GTID mode is enabled
  if (!connection_.IsGTIDModeEnabled()) {
    last_error_ =
        "GTID mode is not enabled on MySQL server. "
        "Please enable GTID mode (gtid_mode=ON) for replication support.";
    spdlog::error(last_error_);
    return false;
  }

  // Start transaction with consistent snapshot for GTID consistency
  spdlog::info("Starting consistent snapshot transaction");
  if (!connection_.ExecuteUpdate("START TRANSACTION WITH CONSISTENT SNAPSHOT")) {
    last_error_ = "Failed to start consistent snapshot: " + connection_.GetLastError();
    spdlog::error(last_error_);
    return false;
  }

  // Capture GTID at this point (represents snapshot state)
  MYSQL_RES* gtid_result = connection_.Execute("SELECT @@global.gtid_executed");
  if (gtid_result != nullptr) {
    MYSQL_ROW row = mysql_fetch_row(gtid_result);
    if (row != nullptr && row[0] != nullptr) {
      snapshot_gtid_ = std::string(row[0]);
      spdlog::info("Snapshot GTID captured: {}", snapshot_gtid_);
    }
    mysql_free_result(gtid_result);
  } else {
    spdlog::warn("Failed to capture GTID, continuing without it");
    snapshot_gtid_ = "";
  }

  // Build SELECT query
  std::string query = BuildSelectQuery();
  spdlog::info("Building snapshot with query: {}", query);

  auto start_time = std::chrono::steady_clock::now();

  // Execute query (within the consistent snapshot transaction)
  MYSQL_RES* result = connection_.Execute(query);
  if (result == nullptr) {
    last_error_ = "Failed to execute SELECT query: " + connection_.GetLastError();
    spdlog::error(last_error_);
    connection_.ExecuteUpdate("ROLLBACK");  // Clean up transaction
    return false;
  }

  // Get field metadata
  unsigned int num_fields = mysql_num_fields(result);
  MYSQL_FIELD* fields = mysql_fetch_fields(result);

  // Get total row count (approximate from result)
  uint64_t total_rows = mysql_num_rows(result);

  spdlog::info("Processing {} rows from table {}", total_rows, table_config_.name);

  // Process rows in batches
  MYSQL_ROW row = nullptr;
  processed_rows_ = 0;

  // Determine batch size (default to 1000 if not specified)
  size_t batch_size = build_config_.batch_size > 0 ? build_config_.batch_size : 1000;

  std::vector<DocumentStore::DocumentItem> doc_batch;
  std::vector<index::Index::DocumentItem> index_batch;
  doc_batch.reserve(batch_size);
  index_batch.reserve(batch_size);

  while ((row = mysql_fetch_row(result)) != nullptr && !cancelled_) {
    // Extract primary key
    std::string primary_key = ExtractPrimaryKey(row, fields, num_fields);
    if (primary_key.empty()) {
      last_error_ = "Failed to extract primary key";
      spdlog::error(last_error_);
      mysql_free_result(result);
      connection_.ExecuteUpdate("ROLLBACK");
      return false;
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
      std::vector<DocId> doc_ids = doc_store_.AddDocumentBatch(doc_batch);

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

        SnapshotProgress progress;
        progress.total_rows = total_rows;
        progress.processed_rows = processed_rows_;
        progress.elapsed_seconds = elapsed;
        progress.rows_per_second =
            elapsed > 0 ? static_cast<double>(processed_rows_) / elapsed : 0.0;

        progress_callback(progress);
      }
    }
  }

  // Process remaining rows in batch
  if (!doc_batch.empty() && !cancelled_) {
    // Add documents to document store
    std::vector<DocId> doc_ids = doc_store_.AddDocumentBatch(doc_batch);

    // Update index_batch with assigned doc_ids
    for (size_t i = 0; i < doc_ids.size(); ++i) {
      index_batch[i].doc_id = doc_ids[i];
    }

    // Add to index
    index_.AddDocumentBatch(index_batch);

    processed_rows_ += doc_batch.size();
  }

  mysql_free_result(result);

  // Commit the transaction (releases the snapshot)
  if (!connection_.ExecuteUpdate("COMMIT")) {
    spdlog::warn("Failed to commit snapshot transaction");
  }

  if (cancelled_) {
    last_error_ = "Build cancelled";
    spdlog::warn(last_error_);
    return false;
  }

  auto end_time = std::chrono::steady_clock::now();
  double total_elapsed = std::chrono::duration<double>(end_time - start_time).count();

  spdlog::info("Snapshot build completed: {} rows in {:.2f}s ({:.0f} rows/s)", processed_rows_,
               total_elapsed,
               total_elapsed > 0 ? static_cast<double>(processed_rows_) / total_elapsed : 0.0);

  return true;
}

std::string SnapshotBuilder::BuildSelectQuery() const {
  std::ostringstream query;
  query << "SELECT ";

  // Primary key
  query << table_config_.primary_key;

  // Text source columns
  if (!table_config_.text_source.column.empty()) {
    query << ", " << table_config_.text_source.column;
  } else {
    for (const auto& col : table_config_.text_source.concat) {
      query << ", " << col;
    }
  }

  // Required filter columns (for binlog replication condition checking)
  for (const auto& filter : table_config_.required_filters) {
    query << ", " << filter.name;
  }

  // Optional filter columns (for search-time filtering)
  for (const auto& filter : table_config_.filters) {
    query << ", " << filter.name;
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
        if (filter.type == "string" || filter.type == "varchar" || filter.type == "text" ||
            filter.type == "datetime" || filter.type == "date" || filter.type == "timestamp") {
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

bool SnapshotBuilder::ProcessRow(MYSQL_ROW row, MYSQL_FIELD* fields, unsigned int num_fields) {
  // Extract primary key
  std::string primary_key = ExtractPrimaryKey(row, fields, num_fields);
  if (primary_key.empty()) {
    last_error_ = "Failed to extract primary key";
    spdlog::error(last_error_);
    return false;
  }

  // Extract text
  std::string text = ExtractText(row, fields, num_fields);
  if (text.empty()) {
    spdlog::debug("Empty text for primary key {}, skipping", primary_key);
    return true;  // Skip empty documents
  }

  // Normalize text
  std::string normalized_text = utils::NormalizeText(text, true, "keep", true);

  // Extract filters
  auto filters = ExtractFilters(row, fields, num_fields);

  // Add to document store
  DocId doc_id = doc_store_.AddDocument(primary_key, filters);

  // Add to index
  index_.AddDocument(doc_id, normalized_text);

  return true;
}

bool SnapshotBuilder::IsTextColumn(enum_field_types type) {
  // Support VARCHAR and TEXT types (TINY, MEDIUM, LONG, BLOB variants)
  return type == MYSQL_TYPE_VARCHAR || type == MYSQL_TYPE_VAR_STRING || type == MYSQL_TYPE_STRING ||
         type == MYSQL_TYPE_TINY_BLOB || type == MYSQL_TYPE_MEDIUM_BLOB ||
         type == MYSQL_TYPE_LONG_BLOB || type == MYSQL_TYPE_BLOB;
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity)
std::string SnapshotBuilder::ExtractText(MYSQL_ROW row, MYSQL_FIELD* fields,
                                         unsigned int num_fields) const {
  if (!table_config_.text_source.column.empty()) {
    // Single column
    int idx = FindFieldIndex(table_config_.text_source.column, fields, num_fields);
    if (idx >= 0) {
      // Validate column type
      if (!IsTextColumn(fields[idx].type)) {
        spdlog::error(
            "Column '{}' is not a text type (VARCHAR/TEXT). "
            "Type: {}",
            table_config_.text_source.column, static_cast<int>(fields[idx].type));
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
          spdlog::error(
              "Column '{}' is not a text type (VARCHAR/TEXT). "
              "Type: {}",
              col, static_cast<int>(fields[idx].type));
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

std::string SnapshotBuilder::ExtractPrimaryKey(MYSQL_ROW row, MYSQL_FIELD* fields,
                                               unsigned int num_fields) const {
  int idx = FindFieldIndex(table_config_.primary_key, fields, num_fields);
  if (idx >= 0 && row[idx] != nullptr) {
    return {row[idx]};
  }
  return "";
}

std::unordered_map<std::string, FilterValue> SnapshotBuilder::ExtractFilters(
    MYSQL_ROW row, MYSQL_FIELD* fields, unsigned int num_fields) const {
  std::unordered_map<std::string, FilterValue> filters;

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
      // Date/time types (store as string)
      else if (type == "datetime" || type == "date" || type == "timestamp") {
        filters[filter_config.name] = value_str;
      } else {
        spdlog::warn("Unknown filter type '{}' for field '{}'", type, filter_config.name);
      }
    } catch (const std::exception& e) {
      spdlog::warn("Failed to parse {} filter {}: {}", type, filter_config.name, value_str);
    }
  }

  return filters;
}

int SnapshotBuilder::FindFieldIndex(const std::string& field_name, MYSQL_FIELD* fields,
                                    unsigned int num_fields) {
  for (unsigned int i = 0; i < num_fields; ++i) {
    if (field_name == fields[i].name) {
      return static_cast<int>(i);
    }
  }
  return -1;
}

}  // namespace storage
}  // namespace mygramdb

#endif  // USE_MYSQL
