/**
 * @file binlog_reader.h
 * @brief MySQL binlog reader for replication
 */

#pragma once

#ifdef USE_MYSQL

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <unordered_map>

#include "config/config.h"
#include "index/index.h"
#include "mysql/connection.h"
#include "mysql/rows_parser.h"
#include "mysql/table_metadata.h"
#include "storage/document_store.h"

// Forward declaration
namespace mygramdb::server {
struct TableContext;
class ServerStats;
}  // namespace mygramdb::server

namespace mygramdb::mysql {

/**
 * @brief Binlog event type
 */
enum class BinlogEventType : uint8_t {
  INSERT,
  UPDATE,
  DELETE,
  DDL,  // DDL operations (TRUNCATE, ALTER, DROP)
  UNKNOWN
};

/**
 * @brief Binlog event
 */
struct BinlogEvent {
  BinlogEventType type = BinlogEventType::UNKNOWN;
  std::string table_name;
  std::string primary_key;
  std::string text;  // Normalized text for INSERT/UPDATE
  std::unordered_map<std::string, storage::FilterValue> filters;
  std::string gtid;  // GTID for this event
};

/**
 * @brief Binlog reader with event queue
 *
 * Reads binlog events from MySQL and queues them for processing
 */
class BinlogReader {
 public:
  /**
   * @brief Configuration for binlog reader
   */
  // NOLINTBEGIN(readability-magic-numbers,cppcoreguidelines-avoid-magic-numbers) - Default binlog
  // reader settings
  struct Config {
    std::string start_gtid;                 // Starting GTID
    size_t queue_size = 10000;              // Maximum queue size
    int reconnect_delay_ms = 1000;          // Reconnect delay in milliseconds
    std::string state_file_path;            // Path to GTID state file (empty = no persistence)
    int state_write_interval_events = 100;  // Write state every N events
  };
  // NOLINTEND(readability-magic-numbers,cppcoreguidelines-avoid-magic-numbers)

  /**
   * @brief Construct binlog reader (single-table mode)
   * @deprecated Use multi-table constructor instead
   */
  BinlogReader(Connection& connection, index::Index& index, storage::DocumentStore& doc_store,
               config::TableConfig table_config, const Config& config, server::ServerStats* stats = nullptr);

  /**
   * @brief Construct binlog reader (multi-table mode)
   * @param connection MySQL connection
   * @param table_contexts Map of table name to TableContext pointer
   * @param config Binlog reader configuration
   * @param stats Server statistics tracker (optional)
   */
  BinlogReader(Connection& connection, std::unordered_map<std::string, server::TableContext*> table_contexts,
               const Config& config, server::ServerStats* stats = nullptr);

  ~BinlogReader();

  // Non-copyable and non-movable (manages thread and connection state)
  BinlogReader(const BinlogReader&) = delete;
  BinlogReader& operator=(const BinlogReader&) = delete;
  BinlogReader(BinlogReader&&) = delete;
  BinlogReader& operator=(BinlogReader&&) = delete;

  /**
   * @brief Start reading binlog events
   * @return true if started successfully
   */
  bool Start();

  /**
   * @brief Stop reading binlog events
   */
  void Stop();

  /**
   * @brief Check if reader is running
   */
  bool IsRunning() const { return running_; }

  /**
   * @brief Get current GTID
   */
  std::string GetCurrentGTID() const;

  /**
   * @brief Set current GTID (used when loading from snapshot)
   * @param gtid GTID to set
   */
  void SetCurrentGTID(const std::string& gtid);

  /**
   * @brief Get queue size
   */
  size_t GetQueueSize() const;

  /**
   * @brief Get total events processed
   */
  uint64_t GetProcessedEvents() const { return processed_events_; }

  /**
   * @brief Get last error message
   */
  const std::string& GetLastError() const { return last_error_; }

  /**
   * @brief Set server statistics tracker
   * @param stats Server statistics tracker pointer
   */
  void SetServerStats(server::ServerStats* stats) { server_stats_ = stats; }

 private:
  Connection& connection_;                         // Main connection (used for queries, not binlog)
  std::unique_ptr<Connection> binlog_connection_;  // Dedicated connection for binlog reading

  // Multi-table support
  std::unordered_map<std::string, server::TableContext*> table_contexts_;
  bool multi_table_mode_ = false;

  // Single-table mode (deprecated)
  index::Index* index_ = nullptr;
  storage::DocumentStore* doc_store_ = nullptr;
  config::TableConfig table_config_;

  Config config_;

  std::atomic<bool> running_{false};
  std::atomic<bool> should_stop_{false};

  // Event queue
  std::queue<BinlogEvent> event_queue_;
  mutable std::mutex queue_mutex_;
  std::condition_variable queue_cv_;
  std::condition_variable queue_full_cv_;

  // Worker threads
  std::unique_ptr<std::thread> reader_thread_;
  std::unique_ptr<std::thread> worker_thread_;

  // Statistics
  std::atomic<uint64_t> processed_events_{0};
  std::string current_gtid_;
  mutable std::mutex gtid_mutex_;
  server::ServerStats* server_stats_ = nullptr;  // Optional server statistics tracker

  std::string last_error_;

  // Table metadata cache
  TableMetadataCache table_metadata_cache_;

  // Column names cache: key = "database.table", value = vector of column names in order
  std::unordered_map<std::string, std::vector<std::string>> column_names_cache_;
  mutable std::mutex column_names_cache_mutex_;

  // GTID encoding data (must persist during mysql_binlog_open call)
  std::vector<uint8_t> gtid_encoded_data_;

  /**
   * @brief Static callback for MySQL binlog API to encode GTID set
   * @param rpl MYSQL_RPL structure
   * @param packet_gtid_set Buffer to write encoded GTID data
   */
  static void FixGtidSetCallback(MYSQL_RPL* rpl, unsigned char* packet_gtid_set);

  /**
   * @brief Reader thread function
   */
  void ReaderThreadFunc();

  /**
   * @brief Worker thread function
   */
  void WorkerThreadFunc();

  /**
   * @brief Push event to queue (blocking if full)
   */
  void PushEvent(const BinlogEvent& event);

  /**
   * @brief Pop event from queue (blocking if empty)
   */
  bool PopEvent(BinlogEvent& event);

  /**
   * @brief Process single event
   */
  bool ProcessEvent(const BinlogEvent& event);

  /**
   * @brief Evaluate required_filters conditions for a binlog event
   * @param filters Filter values from binlog event
   * @param table_config Table configuration containing required_filters
   * @return true if all required_filters conditions are satisfied
   */
  static bool EvaluateRequiredFilters(const std::unordered_map<std::string, storage::FilterValue>& filters,
                                      const config::TableConfig& table_config);

  /**
   * @brief Compare filter value against required filter condition
   * @param value Filter value from binlog
   * @param filter Required filter configuration
   * @return true if condition is satisfied
   */
  static bool CompareFilterValue(const storage::FilterValue& value, const config::RequiredFilterConfig& filter);

  /**
   * @brief Fetch column names from INFORMATION_SCHEMA and update TableMetadata
   * @param metadata Table metadata to update with actual column names
   * @return true if successful, false otherwise
   */
  bool FetchColumnNames(TableMetadata& metadata);

  /**
   * @brief Extract all filter columns (both required and optional) from row data
   * @param row_data Row data from binlog
   * @return Map of filter name to FilterValue
   */
  std::unordered_map<std::string, storage::FilterValue> ExtractAllFilters(const RowData& row_data) const;

  /**
   * @brief Update current GTID
   */
  void UpdateCurrentGTID(const std::string& gtid);

  /**
   * @brief Write GTID to state file
   */
  void WriteGTIDToStateFile(const std::string& gtid) const;

  /**
   * @brief Parse binlog event buffer and create BinlogEvent
   * @param buffer Raw binlog event data
   * @param length Length of the buffer
   * @return BinlogEvent if successfully parsed, nullopt otherwise
   */
  std::optional<BinlogEvent> ParseBinlogEvent(const unsigned char* buffer, unsigned long length);

  /**
   * @brief Extract GTID from GTID event
   * @param buffer Event buffer
   * @param length Buffer length
   * @return GTID string if found
   */
  static std::optional<std::string> ExtractGTID(const unsigned char* buffer, unsigned long length);

  /**
   * @brief Parse TABLE_MAP event
   * @param buffer Event buffer (post-header)
   * @param length Buffer length
   * @return TableMetadata if successfully parsed
   */
  static std::optional<TableMetadata> ParseTableMapEvent(const unsigned char* buffer, unsigned long length);

  /**
   * @brief Extract SQL query string from QUERY_EVENT
   * @param buffer Event buffer
   * @param length Buffer length
   * @return Query string if successfully extracted
   */
  static std::optional<std::string> ExtractQueryString(const unsigned char* buffer, unsigned long length);

  /**
   * @brief Check if DDL affects target table
   * @param query SQL query string
   * @param table_name Target table name
   * @return true if DDL affects the table
   */
  static bool IsTableAffectingDDL(const std::string& query, const std::string& table_name);
};

}  // namespace mygramdb::mysql

#endif  // USE_MYSQL
