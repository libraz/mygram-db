/**
 * @file initial_loader.h
 * @brief Initial data loader for constructing index from MySQL
 */

#pragma once

#ifdef USE_MYSQL

#include <atomic>
#include <functional>
#include <memory>

#include "config/config.h"
#include "index/index.h"
#include "mysql/connection.h"
#include "storage/document_store.h"
#include "utils/error.h"
#include "utils/expected.h"

namespace mygramdb::loader {

/**
 * @brief Initial data loading progress callback
 */
struct LoadProgress {
  uint64_t total_rows = 0;       // Total rows to process
  uint64_t processed_rows = 0;   // Rows processed so far
  double elapsed_seconds = 0.0;  // Elapsed time
  double rows_per_second = 0.0;  // Processing rate
};

using ProgressCallback = std::function<void(const LoadProgress&)>;

/**
 * @brief Initial data loader for index construction
 *
 * Loads initial data from MySQL and builds index and document store.
 * This is used for initial setup before starting binlog replication.
 */
class InitialLoader {
 public:
  /**
   * @brief Construct initial loader
   * @param connection MySQL connection
   * @param index N-gram index
   * @param doc_store Document store
   * @param table_config Table configuration
   * @param mysql_config MySQL connection configuration (for datetime_timezone)
   * @param build_config Build configuration (batch_size, parallelism)
   */
  InitialLoader(mysql::Connection& connection, index::Index& index, storage::DocumentStore& doc_store,
                config::TableConfig table_config, config::MysqlConfig mysql_config = {},
                config::BuildConfig build_config = {});

  /**
   * @brief Load initial data from MySQL with consistent GTID
   *
   * Uses START TRANSACTION WITH CONSISTENT SNAPSHOT to ensure
   * data consistency and captures the GTID at snapshot time.
   *
   * @param progress_callback Optional progress callback
   * @return Expected<void, Error> on success or error
   */
  [[nodiscard]] mygram::utils::Expected<void, mygram::utils::Error> Load(
      const ProgressCallback& progress_callback = {});

  /**
   * @brief Get GTID captured at load time
   *
   * This GTID represents the state of the database when the initial load
   * was performed. Binlog replication should start from this GTID.
   *
   * @return GTID string or empty if not available
   */
  const std::string& GetStartGTID() const { return start_gtid_; }

  /**
   * @brief Get total rows processed
   */
  uint64_t GetProcessedRows() const { return processed_rows_; }

  /**
   * @brief Cancel ongoing load
   */
  void Cancel() { cancelled_ = true; }

 private:
  mysql::Connection& connection_;
  index::Index& index_;
  storage::DocumentStore& doc_store_;
  config::TableConfig table_config_;
  config::MysqlConfig mysql_config_;
  config::BuildConfig build_config_;

  uint64_t processed_rows_ = 0;
  std::atomic<bool> cancelled_{false};
  std::string start_gtid_;  // GTID captured at load time

  /**
   * @brief Build SELECT query for initial load
   */
  std::string BuildSelectQuery() const;

  /**
   * @brief Process single row from result set
   */
  [[nodiscard]] mygram::utils::Expected<void, mygram::utils::Error> ProcessRow(MYSQL_ROW row, MYSQL_FIELD* fields,
                                                                               unsigned int num_fields);

  /**
   * @brief Check if column type is text (VARCHAR/TEXT)
   */
  static bool IsTextColumn(enum_field_types type);

  /**
   * @brief Extract text from row based on text_source configuration
   */
  std::string ExtractText(MYSQL_ROW row, MYSQL_FIELD* fields, unsigned int num_fields) const;

  /**
   * @brief Extract primary key from row
   */
  std::string ExtractPrimaryKey(MYSQL_ROW row, MYSQL_FIELD* fields, unsigned int num_fields) const;

  /**
   * @brief Extract filter values from row
   */
  std::unordered_map<std::string, storage::FilterValue> ExtractFilters(MYSQL_ROW row, MYSQL_FIELD* fields,
                                                                       unsigned int num_fields) const;

  /**
   * @brief Find field index by name
   */
  static int FindFieldIndex(const std::string& field_name, MYSQL_FIELD* fields, unsigned int num_fields);
};

}  // namespace mygramdb::loader

#endif  // USE_MYSQL
