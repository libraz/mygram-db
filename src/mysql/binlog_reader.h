/**
 * @file binlog_reader.h
 * @brief MySQL binlog reader for replication
 */

#pragma once

#ifdef USE_MYSQL

#include <atomic>
#include <cctype>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "config/config.h"
#include "index/index.h"
#include "mysql/binlog_reader_interface.h"
#include "mysql/binlog_stream.h"
#include "mysql/connection.h"
#include "mysql/connection_validator.h"
#include "mysql/rows_parser.h"
#include "mysql/table_metadata.h"
#include "server/server_types.h"
#include "storage/document_store.h"

// Forward declarations
namespace mygramdb::server {
struct TableContext;
class ServerStats;
}  // namespace mygramdb::server

namespace mygramdb::cache {
class CacheManager;
}  // namespace mygramdb::cache

namespace mygramdb::mysql {

/**
 * @brief Binlog event type
 */
enum class BinlogEventType : uint8_t {
  INSERT,
  UPDATE,
  DELETE,
  DDL,  // DDL operations (TRUNCATE, ALTER, DROP)
  COMMIT,
  UNKNOWN
};

/**
 * @brief DDL operation type
 *
 * Classifies the DDL operation for structured handling in the event processor.
 */
enum class DDLType : uint8_t { kUnknown = 0, kTruncate, kAlter, kDrop, kRename };

/**
 * @brief Binlog event
 *
 * Represents a parsed binlog event with validation and factory methods.
 * Use factory methods (CreateInsert, CreateUpdate, etc.) for type-safe creation,
 * or direct construction for backward compatibility.
 */
struct BinlogEvent {
  BinlogEventType type = BinlogEventType::UNKNOWN;
  DDLType ddl_type = DDLType::kUnknown;  // DDL sub-type (only meaningful when type == DDL)
  std::string table_name;
  std::string primary_key;
  std::string text;      // Normalized text for INSERT/UPDATE (after image for UPDATE)
  std::string old_text;  // Before image text for UPDATE events (empty for INSERT/DELETE)
  storage::FilterMap filters;
  std::string gtid;  // GTID for this event

  /**
   * @brief Check if this event satisfies its invariants
   *
   * Invariants by event type:
   * - INSERT: requires table_name, primary_key
   * - UPDATE: requires table_name, primary_key
   * - DELETE: requires table_name, primary_key
   * - DDL: requires table_name
   * - UNKNOWN: always invalid
   *
   * @return true if the event is valid
   */
  [[nodiscard]] bool IsValid() const {
    switch (type) {
      case BinlogEventType::INSERT:
      case BinlogEventType::UPDATE:
      case BinlogEventType::DELETE:
        return !table_name.empty() && !primary_key.empty();
      case BinlogEventType::DDL:
        return !table_name.empty();
      case BinlogEventType::COMMIT:
        return !gtid.empty();
      case BinlogEventType::UNKNOWN:
      default:
        return false;
    }
  }

  /**
   * @brief Create an INSERT event
   *
   * @param table Table name
   * @param primary_key_val Primary key value
   * @param txt Text content
   * @param gtid_val GTID for this event
   * @return BinlogEvent with INSERT type
   */
  static BinlogEvent CreateInsert(const std::string& table, const std::string& primary_key_val, const std::string& txt,
                                  const std::string& gtid_val = "") {
    BinlogEvent event;
    event.type = BinlogEventType::INSERT;
    event.table_name = table;
    event.primary_key = primary_key_val;
    event.text = txt;
    event.gtid = gtid_val;
    return event;
  }

  /**
   * @brief Create an UPDATE event
   *
   * @param table Table name
   * @param primary_key_val Primary key value
   * @param new_txt New text content (after image)
   * @param old_txt Old text content (before image)
   * @param gtid_val GTID for this event
   * @return BinlogEvent with UPDATE type
   */
  static BinlogEvent CreateUpdate(const std::string& table, const std::string& primary_key_val,
                                  const std::string& new_txt, const std::string& old_txt = "",
                                  const std::string& gtid_val = "") {
    BinlogEvent event;
    event.type = BinlogEventType::UPDATE;
    event.table_name = table;
    event.primary_key = primary_key_val;
    event.text = new_txt;
    event.old_text = old_txt;
    event.gtid = gtid_val;
    return event;
  }

  /**
   * @brief Create a DELETE event
   *
   * @param table Table name
   * @param primary_key_val Primary key value
   * @param txt Text content (for index removal)
   * @param gtid_val GTID for this event
   * @return BinlogEvent with DELETE type
   */
  static BinlogEvent CreateDelete(const std::string& table, const std::string& primary_key_val,
                                  const std::string& txt = "", const std::string& gtid_val = "") {
    BinlogEvent event;
    event.type = BinlogEventType::DELETE;
    event.table_name = table;
    event.primary_key = primary_key_val;
    event.text = txt;
    event.gtid = gtid_val;
    return event;
  }

  /**
   * @brief Create a DDL event
   *
   * @param table Table name affected by DDL
   * @param query DDL query text
   * @param gtid_val GTID for this event
   * @return BinlogEvent with DDL type
   */
  static BinlogEvent CreateDDL(const std::string& table, const std::string& query = "",
                               const std::string& gtid_val = "") {
    BinlogEvent event;
    event.type = BinlogEventType::DDL;
    event.table_name = table;
    event.text = query;
    event.gtid = gtid_val;
    event.ddl_type = ClassifyDDL(query);
    return event;
  }

  /**
   * @brief Classify DDL type from query text
   * @param query DDL query string
   * @return DDLType classification
   */
  static DDLType ClassifyDDL(const std::string& query) {
    // Build uppercase copy for case-insensitive matching
    std::string upper;
    upper.reserve(query.size());
    for (char c : query) {
      upper += static_cast<char>(::toupper(static_cast<unsigned char>(c)));
    }
    std::vector<std::string> tokens;
    std::string token;
    for (char c : upper) {
      if (std::isalnum(static_cast<unsigned char>(c)) || c == '_') {
        token += c;
      } else if (!token.empty()) {
        tokens.push_back(std::move(token));
        token.clear();
      }
    }
    if (!token.empty()) {
      tokens.push_back(std::move(token));
    }

    for (size_t i = 0; i + 1 < tokens.size(); ++i) {
      if (tokens[i] == "TRUNCATE" && tokens[i + 1] == "TABLE") {
        return DDLType::kTruncate;
      }
    }
    if (tokens.size() == 1 && tokens[0] == "TRUNCATE") {
      return DDLType::kTruncate;
    }
    // Check ALTER before DROP to avoid matching "ALTER TABLE ... DROP COLUMN"
    if (upper.find("ALTER") != std::string::npos) {
      return DDLType::kAlter;
    }
    if (upper.find("DROP") != std::string::npos) {
      return DDLType::kDrop;
    }
    if (upper.find("RENAME") != std::string::npos) {
      return DDLType::kRename;
    }
    return DDLType::kUnknown;
  }
};

/**
 * @brief Binlog reader with event queue
 *
 * Reads binlog events from MySQL and queues them for processing
 */
class BinlogReader final : public IBinlogReader {
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

  ~BinlogReader() override;

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
   * @brief Get total CRC32 checksum errors detected
   */
  uint64_t GetCRCErrors() const { return crc_errors_; }

  /**
   * @brief Get last error message (IBinlogReader interface)
   */
  std::string GetLastError() const override {
    std::lock_guard<std::mutex> lock(last_error_mutex_);
    return last_error_.message();
  }

  /**
   * @brief Get last error as structured Error object
   *
   * Provides access to the full Error with code and context, unlike GetLastError()
   * which only returns the message string for interface compatibility.
   */
  mygram::utils::Error GetLastErrorObject() const {
    std::lock_guard<std::mutex> lock(last_error_mutex_);
    return last_error_;
  }

  /**
   * @brief Set server statistics tracker
   * @param stats Server statistics tracker pointer (non-owning).
   *        Caller must ensure the pointed-to object outlives this BinlogReader,
   *        or call Stop() before destroying it.
   */
  void SetServerStats(server::ServerStats* stats) { server_stats_.store(stats, std::memory_order_release); }

  /**
   * @brief Set cache manager for invalidation during binlog processing
   * @param cache_manager Cache manager pointer (non-owning, nullable).
   *        Caller must ensure the pointed-to object outlives this BinlogReader,
   *        or call Stop() before destroying it.
   */
  void SetCacheManager(cache::CacheManager* cache_manager) {
    cache_manager_.store(cache_manager, std::memory_order_release);
  }

 private:
  Connection& connection_;  // Reference to main connection (used for startup validation only, externally owned)
  std::unique_ptr<Connection> binlog_connection_;    // Dedicated connection for binlog reading (internally owned)
  std::unique_ptr<Connection> metadata_connection_;  // Dedicated connection for metadata queries (internally owned)

  // Table contexts keyed by table name. The deprecated single-table
  // constructor is normalized into this map via legacy_table_context_.
  std::unordered_map<std::string, server::TableContext*> table_contexts_;

  // Backing storage for the deprecated single-table constructor. It keeps
  // non-owning raw pointers because that legacy API receives externally owned
  // Index / DocumentStore references; production multi-table construction uses
  // TableContext::index/doc_store directly.
  server::TableContext legacy_table_context_;
  index::Index* legacy_index_ = nullptr;
  storage::DocumentStore* legacy_doc_store_ = nullptr;

  config::MysqlConfig mysql_config_;
  Config config_;

  std::atomic<bool> running_{false};
  std::atomic<bool> should_stop_{false};
  std::atomic<bool> processing_failure_reconnect_requested_{false};
  std::mutex stop_mutex_;  ///< Serializes Stop() calls to prevent concurrent join races

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
  std::atomic<uint64_t> crc_errors_{0};
  std::string current_gtid_;
  std::string pending_commit_gtid_;
  std::string executed_gtid_set_;  ///< Full GTID set for COM_BINLOG_DUMP_GTID (protected by gtid_mutex_)
  mutable std::mutex gtid_mutex_;
  std::atomic<server::ServerStats*> server_stats_{nullptr};   // Optional server statistics tracker
  std::atomic<cache::CacheManager*> cache_manager_{nullptr};  // Optional cache manager for invalidation

  // Debug log counters (instance-scoped, reset on Start())
  std::atomic<int> no_data_log_count_{0};
  std::atomic<int> skip_log_count_{0};

  // Error state is stored as a structured Error object rather than a plain string.
  // This deviates from the pure Expected<T, Error> pattern because the error is
  // produced asynchronously in background threads (ReaderThreadFunc/WorkerThreadFunc)
  // and consumed by the main thread via polling. Expected cannot be used for this
  // cross-thread error propagation pattern.
  mygram::utils::Error last_error_;
  mutable std::mutex last_error_mutex_;

  // Failover detection: track last known server UUID
  std::string last_server_uuid_;
  mutable std::mutex uuid_mutex_;

  // Table metadata cache
  TableMetadataCache table_metadata_cache_;

  struct ColumnDefinition {
    std::string name;
    bool is_unsigned = false;
  };

  // Column definition cache: key = "database.table", value = column definitions in ordinal order
  std::unordered_map<std::string, std::vector<ColumnDefinition>> column_names_cache_;
  mutable std::mutex column_names_cache_mutex_;

  // Binlog stream protocol handler (MySQL or MariaDB)
  std::unique_ptr<IBinlogStream> binlog_stream_;

  /**
   * @brief Set last error (thread-safe)
   * @param error Error object with code, message, and optional context
   */
  void SetLastError(const mygram::utils::Error& error) {
    std::lock_guard<std::mutex> lock(last_error_mutex_);
    last_error_ = error;
  }

  /**
   * @brief Set last error from a message string (convenience overload)
   *
   * Creates an Error with kMySQLBinlogError code. Prefer the Error overload
   * when a more specific error code is available.
   *
   * @param message Error message
   */
  void SetLastError(const std::string& message) {
    SetLastError(mygram::utils::Error(mygram::utils::ErrorCode::kMySQLBinlogError, message));
  }

  /**
   * @brief Convert a single GTID "uuid:N" to range "uuid:1-N"
   *
   * COM_BINLOG_DUMP_GTID semantics: "send events NOT in this set".
   * A single GTID "uuid:101" means interval [101,102), so the server
   * sends transactions 1-100 and 102+, causing duplicate delivery.
   * Converting to "uuid:1-101" excludes all transactions 1 through 101.
   *
   * @param gtid GTID string to convert
   * @return Converted GTID string (unchanged if already a range or multi-UUID)
   */
  static std::string ConvertSingleGtidToRange(const std::string& gtid);

  /**
   * @brief Reader thread function
   */
  void ReaderThreadFunc();

  /**
   * @brief Worker thread function
   */
  void WorkerThreadFunc();

  /**
   * @brief Process one queued worker event and update replication position when safe.
   * @return false when processing failed and reconnect is required
   */
  bool ProcessQueuedEvent(const BinlogEvent& event);

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
   * @brief Refresh executed GTID set from server
   * @return true if successful
   */
  bool RefreshExecutedGtidSet();

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
