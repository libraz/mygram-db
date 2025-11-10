/**
 * @file snapshot_builder.h
 * @brief Snapshot builder for initial index construction from MySQL
 */

#pragma once

#ifdef USE_MYSQL

#include "mysql/connection.h"
#include "index/index.h"
#include "storage/document_store.h"
#include "config/config.h"
#include <memory>
#include <functional>
#include <atomic>

namespace mygramdb {
namespace storage {

/**
 * @brief Snapshot builder progress callback
 */
struct SnapshotProgress {
  uint64_t total_rows = 0;      // Total rows to process
  uint64_t processed_rows = 0;  // Rows processed so far
  double elapsed_seconds = 0.0; // Elapsed time
  double rows_per_second = 0.0; // Processing rate
};

using ProgressCallback = std::function<void(const SnapshotProgress&)>;

/**
 * @brief Snapshot builder for initial index construction
 *
 * Builds index and document store from MySQL SELECT query
 */
class SnapshotBuilder {
 public:
  /**
   * @brief Construct snapshot builder
   * @param connection MySQL connection
   * @param index N-gram index
   * @param doc_store Document store
   * @param table_config Table configuration
   */
  SnapshotBuilder(mysql::Connection& connection,
                  index::Index& index,
                  DocumentStore& doc_store,
                  const config::TableConfig& table_config);

  ~SnapshotBuilder() = default;

  /**
   * @brief Build snapshot from SELECT query with consistent GTID
   *
   * Uses START TRANSACTION WITH CONSISTENT SNAPSHOT to ensure
   * data consistency and captures the GTID at snapshot time.
   *
   * @param progress_callback Optional progress callback
   * @return true if successful
   */
  bool Build(ProgressCallback progress_callback = nullptr);

  /**
   * @brief Get GTID captured at snapshot time
   *
   * This GTID represents the state of the database when the snapshot
   * was taken. Binlog replication should start from this GTID.
   *
   * @return GTID string or empty if not available
   */
  const std::string& GetSnapshotGTID() const { return snapshot_gtid_; }

  /**
   * @brief Get last error message
   */
  const std::string& GetLastError() const { return last_error_; }

  /**
   * @brief Get total rows processed
   */
  uint64_t GetProcessedRows() const { return processed_rows_; }

  /**
   * @brief Cancel ongoing build
   */
  void Cancel() { cancelled_ = true; }

 private:
  mysql::Connection& connection_;
  index::Index& index_;
  DocumentStore& doc_store_;
  config::TableConfig table_config_;

  std::string last_error_;
  uint64_t processed_rows_ = 0;
  std::atomic<bool> cancelled_{false};
  std::string snapshot_gtid_;  // GTID captured at snapshot time

  /**
   * @brief Build SELECT query for snapshot
   */
  std::string BuildSelectQuery() const;

  /**
   * @brief Process single row from result set
   */
  bool ProcessRow(MYSQL_ROW row, MYSQL_FIELD* fields, unsigned int num_fields);

  /**
   * @brief Check if column type is text (VARCHAR/TEXT)
   */
  bool IsTextColumn(enum_field_types type) const;

  /**
   * @brief Extract text from row based on text_source configuration
   */
  std::string ExtractText(MYSQL_ROW row, MYSQL_FIELD* fields,
                          unsigned int num_fields) const;

  /**
   * @brief Extract primary key from row
   */
  std::string ExtractPrimaryKey(MYSQL_ROW row, MYSQL_FIELD* fields,
                                unsigned int num_fields) const;

  /**
   * @brief Extract filter values from row
   */
  std::unordered_map<std::string, FilterValue> ExtractFilters(
      MYSQL_ROW row, MYSQL_FIELD* fields, unsigned int num_fields) const;

  /**
   * @brief Find field index by name
   */
  int FindFieldIndex(const std::string& field_name, MYSQL_FIELD* fields,
                     unsigned int num_fields) const;
};

}  // namespace storage
}  // namespace mygramdb

#endif  // USE_MYSQL
