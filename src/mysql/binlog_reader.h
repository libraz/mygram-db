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
#include "mysql/binlog_reader_interface.h"
#include "mysql/connection.h"
#include "mysql/connection_validator.h"
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
  std::string text;      // Normalized text for INSERT/UPDATE (after image for UPDATE)
  std::string old_text;  // Before image text for UPDATE events (empty for INSERT/DELETE)
  std::unordered_map<std::string, storage::FilterValue> filters;
  std::string gtid;  // GTID for this event
};

/**
 * @brief Binlog reader with event queue
 *
 * Reads binlog events from MySQL and queues them for processing
 */
class BinlogReader : public IBinlogReader {
 public:
  /**
   * @brief Configuration for binlog reader
   */
  // NOLINTBEGIN(readability-magic-numbers,cppcoreguidelines-avoid-magic-numbers) - Default binlog
  // reader settings
  struct Config {
    std::string start_gtid;         // Starting GTID
    size_t queue_size = 10000;      // Maximum queue size
    int reconnect_delay_ms = 1000;  // Reconnect delay in milliseconds
    uint32_t server_id = 0;         // MySQL server ID for replication (must be unique)
  };
  // NOLINTEND(readability-magic-numbers,cppcoreguidelines-avoid-magic-numbers)

  /**
   * @brief Construct binlog reader (single-table mode)
   * @deprecated Use multi-table constructor instead
   */
  BinlogReader(Connection& connection, index::Index& index, storage::DocumentStore& doc_store,
               config::TableConfig table_config, config::MysqlConfig mysql_config, const Config& config,
               server::ServerStats* stats = nullptr);

  /**
   * @brief Construct binlog reader (multi-table mode)
   * @param connection MySQL connection
   * @param table_contexts Map of table name to TableContext pointer
   * @param mysql_config MySQL connection configuration (for datetime_timezone)
   * @param config Binlog reader configuration
   * @param stats Server statistics tracker (optional)
   */
  BinlogReader(Connection& connection, std::unordered_map<std::string, server::TableContext*> table_contexts,
               config::MysqlConfig mysql_config, const Config& config, server::ServerStats* stats = nullptr);

  ~BinlogReader();

  // Non-copyable and non-movable (manages thread and connection state)
  BinlogReader(const BinlogReader&) = delete;
  BinlogReader& operator=(const BinlogReader&) = delete;
  BinlogReader(BinlogReader&&) = delete;
  BinlogReader& operator=(BinlogReader&&) = delete;

  /**
   * @brief Start reading binlog events
   * @return Expected<void, Error> - success or start error
   */
  mygram::utils::Expected<void, mygram::utils::Error> Start() override;

  /**
   * @brief Start reading binlog events from specific GTID
   * @param gtid GTID position to start from
   * @return Expected<void, Error> - success or start error
   *
   * This is used for MySQL reconnection/failover to resume replication
   * from a saved GTID position.
   */
  mygram::utils::Expected<void, mygram::utils::Error> StartFromGtid(const std::string& gtid);

  /**
   * @brief Stop reading binlog events
   */
  void Stop() override;

  /**
   * @brief Check if reader is running
   */
  bool IsRunning() const override { return running_.load() && !should_stop_.load(); }

  /**
   * @brief Get current GTID
   */
  std::string GetCurrentGTID() const override;

  /**
   * @brief Set current GTID (used when loading from snapshot)
   * @param gtid GTID to set
   */
  void SetCurrentGTID(const std::string& gtid) override;

  /**
   * @brief Get queue size
   */
  size_t GetQueueSize() const override;

  /**
   * @brief Get total events processed
   */
  uint64_t GetProcessedEvents() const override { return processed_events_; }

  /**
   * @brief Get last error message
   */
  const std::string& GetLastError() const override { return last_error_; }

  /**
   * @brief Set server statistics tracker
   * @param stats Server statistics tracker pointer
   */
  void SetServerStats(server::ServerStats* stats) { server_stats_ = stats; }

 private:
  Connection& connection_;  // Reference to main connection (used for metadata queries, externally owned)
  std::unique_ptr<Connection> binlog_connection_;  // Dedicated connection for binlog reading (internally owned)

  // Multi-table support
  std::unordered_map<std::string, server::TableContext*> table_contexts_;
  bool multi_table_mode_ = false;

  // Single-table mode (deprecated)
  index::Index* index_ = nullptr;
  storage::DocumentStore* doc_store_ = nullptr;
  config::TableConfig table_config_;

  config::MysqlConfig mysql_config_;
  Config config_;

  std::atomic<bool> running_{false};
  std::atomic<bool> should_stop_{false};

  // Event queue (using unique_ptr to avoid copying large BinlogEvent objects)
  std::queue<std::unique_ptr<BinlogEvent>> event_queue_;
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

  // Failover detection: track last known server UUID
  std::string last_server_uuid_;
  mutable std::mutex uuid_mutex_;

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
   * @param event Event to push (ownership transferred)
   */
  void PushEvent(std::unique_ptr<BinlogEvent> event);

  /**
   * @brief Pop event from queue (blocking if empty)
   * @return Unique pointer to event, or nullptr if should_stop_ is true
   */
  std::unique_ptr<BinlogEvent> PopEvent();

  /**
   * @brief Process single event (delegates to BinlogEventProcessor)
   */
  bool ProcessEvent(const BinlogEvent& event);

  /**
   * @brief Fetch column names from INFORMATION_SCHEMA and update TableMetadata
   * @param metadata Table metadata to update with actual column names
   * @return true if successful, false otherwise
   */
  bool FetchColumnNames(TableMetadata& metadata);

  /**
   * @brief Update current GTID
   */
  void UpdateCurrentGTID(const std::string& gtid);

  /**
   * @brief Validate binlog connection after (re)connect
   *
   * Performs comprehensive validation including:
   * - GTID mode check
   * - Server UUID tracking for failover detection
   * - Required tables existence
   * - GTID consistency check
   *
   * @return true if validation passed, false if server is invalid (stop replication)
   */
  bool ValidateConnection();
};

}  // namespace mygramdb::mysql

#endif  // USE_MYSQL
