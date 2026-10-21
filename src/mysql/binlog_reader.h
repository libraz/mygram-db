/**
 * @file binlog_reader.h
 * @brief MySQL binlog reader for replication
 */

#pragma once

#ifdef USE_MYSQL

#include <atomic>
#include <cctype>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>

#include "config/config.h"
#include "index/index.h"
#include "mysql/binlog_checksum.h"
#include "mysql/binlog_event_types.h"
#include "mysql/binlog_reader_interface.h"
#include "mysql/binlog_stream.h"
#include "mysql/connection.h"
#include "mysql/connection_validator.h"
#include "mysql/ddl_classifier.h"
#include "mysql/ddl_schema_validator.h"
#include "mysql/replication_position_state.h"
#include "mysql/rows_parser.h"
#include "mysql/table_metadata.h"
#include "mysql/table_schema_cache.h"
#include "mysql/text_materializer.h"
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
  // Explicit presence keeps an empty-string key distinct from a key column
  // omitted by a minimal row image or represented as SQL NULL.
  bool primary_key_present = false;
  bool primary_key_null = false;
  std::string old_primary_key;  // Before-image PK for UPDATE events that change the primary key
  bool old_primary_key_present = false;
  bool old_primary_key_null = false;
  std::string text;      // Normalized text for INSERT/UPDATE (after image for UPDATE)
  std::string old_text;  // Before image text for UPDATE events (empty for INSERT/DELETE)
  TextValueState text_state = TextValueState::kAbsent;
  TextValueState old_text_state = TextValueState::kAbsent;
  storage::FilterMap filters;
  // Raw text of the filter columns (after image for UPDATE) whose conversion
  // failed, so required_filters can judge them as the initial-load SQL does.
  UnconvertedFilterText unconverted;
  std::string gtid;  // GTID for this event

  /**
   * @brief Check if this event satisfies its invariants
   *
   * Invariants by event type:
   * - INSERT: requires table_name, primary_key
   * - UPDATE: requires table_name, primary_key, and a before-image key that is not NULL
   * - DELETE: requires table_name, primary_key
   * - DDL: requires table_name
   * - UNKNOWN: always invalid
   *
   * @return true if the event is valid
   */
  [[nodiscard]] bool IsValid() const {
    switch (type) {
      case BinlogEventType::INSERT:
      case BinlogEventType::DELETE:
        return !table_name.empty() && HasPrimaryKey();
      case BinlogEventType::UPDATE:
        return !table_name.empty() && HasPrimaryKey() && !old_primary_key_null;
      case BinlogEventType::DDL:
        return !table_name.empty();
      case BinlogEventType::COMMIT:
        return !gtid.empty();
      case BinlogEventType::UNKNOWN:
      default:
        return false;
    }
  }

  [[nodiscard]] bool HasPrimaryKey() const {
    return (primary_key_present || !primary_key.empty()) && !primary_key_null;
  }

  [[nodiscard]] bool HasOldPrimaryKey() const {
    return (old_primary_key_present || !old_primary_key.empty()) && !old_primary_key_null;
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
    event.primary_key_present = true;
    event.text = txt;
    event.text_state = TextValueState::kPresent;
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
    event.primary_key_present = true;
    event.text = new_txt;
    event.old_text = old_txt;
    event.text_state = TextValueState::kPresent;
    event.old_text_state = old_txt.empty() ? TextValueState::kAbsent : TextValueState::kPresent;
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
    event.primary_key_present = true;
    event.text = txt;
    event.text_state = txt.empty() ? TextValueState::kAbsent : TextValueState::kPresent;
    event.gtid = gtid_val;
    return event;
  }

  /**
   * @brief Create a DDL event for a configured table
   *
   * @param table Configured table the DDL is emitted for
   * @param query DDL query text, logged in the table's database
   * @param gtid_val GTID for this event
   * @param lex_options sql_mode bits the query was logged under that change its quoting
   * @param names_case_insensitive Whether the source runs with lower_case_table_names != 0
   * @return BinlogEvent whose ddl_type is what replication assigns @p table
   */
  static BinlogEvent CreateDDL(const config::TableConfig& table, const std::string& query = "",
                               const std::string& gtid_val = "", QueryLexOptions lex_options = {},
                               bool names_case_insensitive = false) {
    BinlogEvent event;
    event.type = BinlogEventType::DDL;
    event.table_name = table.name;
    event.text = query;
    event.gtid = gtid_val;
    event.ddl_type = ClassifyDDL(query, table, lex_options, names_case_insensitive);
    return event;
  }

  /**
   * @brief DDL type replication assigns a configured table for a query
   * @param query DDL query string, logged in the table's database
   * @param table Configured table
   * @param lex_options sql_mode bits the query was logged under that change its quoting
   * @param names_case_insensitive Whether the source runs with lower_case_table_names != 0
   * @return The type FindConfiguredTableDDL reports, or kUnknown when no statement names @p table
   */
  static DDLType ClassifyDDL(const std::string& query, const config::TableConfig& table,
                             QueryLexOptions lex_options = {}, bool names_case_insensitive = false);
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
    // The source runs with lower_case_table_names != 0: it folds names to lowercase in TABLE_MAP
    // events, so database and table names match ASCII case-insensitively instead of byte-exactly.
    bool table_names_case_insensitive = false;
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
   * @brief Check whether the reader lifecycle is active
   *
   * Remains true while either background thread can still mutate replication
   * state. It becomes false after both threads have actually exited, even
   * before Stop() joins their thread objects.
   */
  bool IsRunning() const override {
    return running_.load(std::memory_order_acquire) && active_threads_.load(std::memory_order_acquire) != 0;
  }

  bool IsStarting() const override { return starting_.load(std::memory_order_acquire); }

  ReplicationState GetReplicationState() const override {
    if (IsRunning()) {
      return ReplicationState::kRunning;
    }
    if (schema_incompatible_.load(std::memory_order_acquire) || !GetLastError().empty()) {
      return ReplicationState::kFailed;
    }
    return ReplicationState::kStopped;
  }

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

  std::string GetSourceServerUUID() const override {
    std::lock_guard<std::mutex> lock(uuid_mutex_);
    return last_server_uuid_;
  }

  /**
   * @brief Get total events processed
   */
  uint64_t GetProcessedEvents() const override { return processed_events_; }

  /**
   * @brief Get total CRC32 checksum errors detected
   */
  uint64_t GetCRCErrors() const override { return crc_errors_; }

  /** True after a configured table DDL changed a monitored schema contract. */
  bool HasSchemaIncompatibleError() const override { return schema_incompatible_.load(std::memory_order_acquire); }

  mygram::utils::ErrorCode GetLastErrorCode() const override {
    std::lock_guard<std::mutex> lock(last_error_mutex_);
    return last_error_.code();
  }

  int64_t GetLastAppliedUnixTime() const override { return last_applied_unix_time_.load(std::memory_order_acquire); }

  int64_t GetSecondsSinceLastApplied() const override;

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

  /** Test seam invoked after publishing a worker processing failure. */
  void SetAfterProcessingFailurePublishedHookForTest(std::function<void()> hook) {
    std::lock_guard<std::mutex> lock(processing_failure_hook_mutex_);
    after_processing_failure_published_hook_for_test_ = std::move(hook);
  }

 private:
  /** Recovery class published by reader/worker processing paths. */
  enum class ProcessingFailureKind : uint8_t { kNone = 0, kTransientTransport = 1, kDeterministic = 2 };

  Connection& connection_;  // Reference to main connection (used for startup validation only, externally owned)
  std::unique_ptr<Connection> binlog_connection_;    // Dedicated connection for binlog reading (internally owned)
  std::unique_ptr<Connection> metadata_connection_;  // Dedicated connection for metadata queries (internally owned)
  // The reader thread (reconnect) and worker thread (post-DDL schema
  // validation) use metadata_connection_. A MYSQL handle may not execute or
  // consume two result sets concurrently, so keep each complete query/result
  // lifetime under this lock.
  mutable std::mutex metadata_connection_mutex_;

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
  std::atomic<bool> starting_{false};
  std::atomic<uint8_t> active_threads_{0};
  std::atomic<bool> should_stop_{false};
  std::atomic<ProcessingFailureKind> processing_failure_reconnect_requested_{ProcessingFailureKind::kNone};
  std::atomic<bool> schema_incompatible_{false};
  std::mutex stop_mutex_;  ///< Serializes Stop() calls to prevent concurrent join races

  // Event queue (using unique_ptr to avoid copying large BinlogEvent objects)
  std::queue<std::unique_ptr<BinlogEvent>> event_queue_;
  mutable std::mutex queue_mutex_;
  std::condition_variable queue_cv_;
  std::condition_variable queue_full_cv_;
  std::mutex processing_failure_hook_mutex_;
  std::function<void()> after_processing_failure_published_hook_for_test_;

  enum class StreamStartupState : uint8_t { kIdle, kPending, kOpened, kFailed };
  StreamStartupState stream_startup_state_{StreamStartupState::kIdle};
  std::mutex stream_startup_mutex_;
  std::condition_variable stream_startup_cv_;

  // Worker threads
  std::unique_ptr<std::thread> reader_thread_;
  std::unique_ptr<std::thread> worker_thread_;

  // Statistics
  std::atomic<uint64_t> processed_events_{0};
  std::atomic<uint64_t> crc_errors_{0};
  std::string current_gtid_;
  ReplicationPositionState position_state_;
  mutable std::mutex gtid_mutex_;
  std::atomic<int64_t> last_applied_unix_time_{0};
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

  // TABLE_MAP metadata and replay-suppressed table ids; reader thread only.
  TableSchemaCache schema_cache_;
  BinlogChecksumTracker checksum_tracker_;  // reader thread only
  std::unordered_map<std::string, ConfiguredTableSchema> configured_schema_baselines_;
  // Source's executed GTID set read right after the baselines; a DDL it covers is already in them.
  std::string schema_baseline_gtid_;

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
   * @brief Stop replication for an event that can neither be applied nor skipped.
   *
   * Publishes the error before requesting the stop so a consumer that observes
   * should_stop_ can always read the reason that produced it. The code is a
   * parameter because it is what SYNC reads to decide whether replaying the
   * position is pointless; flattening it would make every such stop look like a
   * generic binlog failure.
   *
   * @param code Specific error code for the condition
   * @param message Operator-facing reason and remediation
   * @param failure_type Value of the structured log "type" field
   * @param event_type_name Wire event type name, logged when non-empty
   */
  void FailClosedOnUnreplayableEvent(mygram::utils::ErrorCode code, const std::string& message,
                                     std::string_view failure_type = "unreplayable_binlog_event",
                                     std::string_view event_type_name = {});

  /**
   * @brief Widen a bare single GTID "uuid:N" to range "uuid:1-N"
   *
   * COM_BINLOG_DUMP_GTID semantics: "send events NOT in this set".
   * A single GTID "uuid:101" means interval [101,102), so the source
   * sends transactions 1-100 and 102+, causing duplicate delivery.
   * Widening to "uuid:1-101" excludes all transactions 1 through 101.
   *
   * A start position enters the reader through this function exactly once
   * (constructor and SetCurrentGTID), so later merges such as "uuid:1-101"
   * plus 102 never produce a bare "uuid:N-M" interval the source would read
   * as having a hole below N.
   *
   * A comma-separated set is split and each entry converted independently, so a
   * multi-UUID position keeps the same exclusion semantics as a single one.
   *
   * @param gtid GTID string to convert
   * @return Converted GTID string. Entries that are already a range, tagged, or
   *         carry multiple intervals are returned unchanged, as are MariaDB
   *         positions, colon-less input and the empty string.
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

  /** Drop every queued event; the caller holds queue_mutex_. */
  void ClearQueueLocked();

  /**
   * @brief Wake every queue waiter after should_stop_ was set
   *
   * Passing through queue_mutex_ first means a waiter is either before its
   * predicate check, and sees the flag, or already blocked, and gets the notify.
   * The caller must not hold queue_mutex_.
   */
  void WakeQueueWaiters();

  /**
   * @brief Discard what the previous stream queued and return the position the next one opens from.
   *
   * Called before every stream open, so a reopen resumes exactly from the
   * applied position whatever ended the previous stream.
   */
  std::string TakeResumePosition();

  /** Mark one background thread exited without underflowing test-only direct calls. */
  void MarkThreadExited();

  /**
   * @brief Process one queued worker event and update replication position when safe.
   * @return false when processing failed and reconnect is required
   */
  bool ProcessQueuedEvent(const BinlogEvent& event, ProcessingFailureKind* failure_kind = nullptr);

  /**
   * @brief Fail closed for an event type that cannot be decoded safely.
   *
   * Covers every type classified fail-closed, so a type added to the
   * classification is rejected here without a second edit.
   *
   * @return true when replication was stopped for an unsupported event
   */
  bool RejectUnsupportedRuntimeEvent(MySQLBinlogEventType event_type);

  /**
   * @brief Fail closed for a tagged GTID event, naming the position it carried.
   *
   * A tagged position cannot be encoded back into a reconnect request, so the
   * stream can never be resumed from it.
   */
  void RejectTaggedGtidEvent(const std::optional<std::string>& tagged_gtid);

  /**
   * @brief Fail closed for a transaction the source marked XA.
   * @param source_event Wire event that revealed the XA transaction
   * @param statement Originating statement when one is available
   */
  void RejectUnsupportedXaTransaction(std::string_view source_event, const std::string& statement);

  /// What the reader loop does after offering an event to ObservePositionEvent.
  enum class PositionEventAction : uint8_t {
    kNotPositionEvent,  ///< Not a GTID-carrying event; dispatch continues.
    kContinue,          ///< Position recorded; read the next event.
    kReconnect,         ///< Frame did not decode; error published, reconnect from the last processed GTID.
    kStop,              ///< Event can never be resumed from; replication stopped.
  };

  /**
   * @brief Record the position a GTID-carrying event declares.
   *
   * Handles GTID_LOG_EVENT, GTID_TAGGED_LOG_EVENT, MARIADB_GTID_EVENT and
   * MARIADB_GTID_LIST_EVENT alike: a frame that does not decode never leaves
   * the received position silently behind.
   */
  PositionEventAction ObservePositionEvent(MySQLBinlogEventType event_type, const unsigned char* event_buffer,
                                           unsigned long event_length);

  /**
   * @brief Fail closed for a statement that may carry row data the decoder never sees.
   */
  void RejectUnsafeStatementEvent(const std::string& statement);

  /**
   * @brief Verify a fetched frame under its file's checksum algorithm before anything reads it.
   *
   * On success the arguments are replaced by the frame in the layout the
   * parsers read, which carries a 4-byte checksum trailer even when the source
   * wrote the file without checksums.
   *
   * @return false, with the error published, when the frame is shorter than a
   *         header plus checksum, its CRC32 does not match or its format
   *         description declares an unknown algorithm
   */
  bool VerifyFetchedFrame(const unsigned char*& event_buffer, size_t& event_length);

  /// What the reader loop does after offering an event to HandleTableMapEvent.
  enum class TableMapAction : uint8_t {
    kContinue,   ///< Map recorded, or deliberately dropped; read the next event.
    kReconnect,  ///< Error published; reconnect from the last processed GTID.
    kStop,       ///< Event can never be resumed from; replication stopped.
  };

  /**
   * @brief Record what a TABLE_MAP_EVENT says about its table id.
   * @param failure_kind Recovery class of a kReconnect result
   */
  TableMapAction HandleTableMapEvent(const unsigned char* event_buffer, unsigned long event_length,
                                     ProcessingFailureKind* failure_kind);

  /**
   * @brief Apply what the reader thread owes a configured-table DDL before queueing it.
   *
   * Stops on DDL that changes rows without row events unless the table's SYNC
   * snapshot already covers it.
   *
   * @return false when replication was stopped
   */
  bool ObserveConfiguredTableDdl(const BinlogEvent& event);

  /**
   * @brief Return whether a table event is already represented by its SYNC snapshot.
   *
   * Comparison errors are returned to the caller and must stop replication;
   * applying an event when the replay fence cannot be interpreted is unsafe.
   */
  mygram::utils::Expected<bool, mygram::utils::Error> ShouldSuppressTableReplay(const std::string& table_name,
                                                                                const std::string& event_gtid) const;

  /**
   * @brief Return true when an empty parse result means a monitored row event failed to decode.
   */
  [[nodiscard]] bool IsMonitoredRowsEventParseFailure(MySQLBinlogEventType event_type, const unsigned char* buffer,
                                                      unsigned long length) const;

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
  bool ProcessEvent(const BinlogEvent& event, mygram::utils::Error* error_out = nullptr);

  enum class DDLSchemaCheck : uint8_t { kCompatible, kRetryableFailure, kIncompatible };

  /** Only transport failures can be retried after a post-DDL schema check. */
  static bool IsRetryableSchemaValidationError(mygram::utils::ErrorCode code);

  /** Map a preserved error code to its processing recovery class. */
  static ProcessingFailureKind ClassifyProcessingFailure(mygram::utils::ErrorCode code);

  /** Publish a request without allowing transient failure to replace deterministic failure. */
  void PublishProcessingFailure(ProcessingFailureKind kind);

  /** Reset processing backoff only when the worker-owned applied position advances. */
  static void ResetProcessingBackoffAfterProgress(const std::string& applied_gtid, std::string& last_recovery_gtid,
                                                  int& reconnect_attempt);

  /** Validate a monitored DDL before mutating state or advancing its GTID. */
  DDLSchemaCheck ValidateSchemaAfterDDL(const BinlogEvent& event);

  /** A reconnect already owns an open transport for the next outer-loop iteration. */
  [[nodiscard]] static bool ShouldCloseStreamAfterReadLoop(bool connection_was_reestablished) {
    return !connection_was_reestablished;
  }

  /**
   * @brief Update current GTID
   */
  mygram::utils::Expected<void, mygram::utils::Error> UpdateCurrentGTID(const std::string& gtid);

  /** Advance current_gtid_ while gtid_mutex_ is held and return the merged position. */
  mygram::utils::Expected<std::string, mygram::utils::Error> UpdateCurrentGTIDLocked(const std::string& gtid);

  /**
   * @brief Calculate the bounded reconnect backoff delay.
   */
  [[nodiscard]] int64_t ReconnectBackoffDelayMs(int reconnect_attempt) const;

  /**
   * @brief Wait for a reconnect backoff interval, cancellable by Stop().
   * @return true after the full delay, false when shutdown was requested
   */
  bool WaitForReconnectBackoff(int reconnect_attempt);

  /**
   * @brief Record a retryable processing failure and stop after repeated replay at one position.
   *
   * A processing failure leaves the applied GTID unchanged so the event is
   * replayed after reconnect. Bound repeated failure at that same position to
   * avoid a healthy connection spinning forever on an unprocessable event.
   */
  static bool ShouldStopForRepeatedProcessingFailure(const std::string& applied_gtid, std::string& last_failure_gtid,
                                                     int& consecutive_failures);

  /** Apply the same-GTID replay limit only to deterministic processing failures. */
  static bool ShouldStopForProcessingFailure(ProcessingFailureKind kind, const std::string& applied_gtid,
                                             std::string& last_failure_gtid, int& consecutive_failures);

  /**
   * @brief Clear each table replay fence once the applied reader position reaches it.
   *
   * A fence that cannot be compared against the applied position is the same
   * root condition the reader and worker paths observe, so it fails closed here
   * too rather than leaving the fence in place silently.
   *
   * @return false when a fence could not be evaluated and replication was stopped
   */
  bool ClearReachedReplayWatermarks(const std::string& applied_gtid);

  /**
   * @brief Stop replication because a SYNC replay fence cannot be evaluated.
   *
   * The reader path, the worker path and the fence-clearing path all reach this
   * one exit so that an unevaluable fence produces the same code, message and
   * restartability wherever it is observed. It is never a schema condition, so
   * schema_incompatible_ stays untouched.
   */
  void FailClosedOnUnevaluableReplayWatermark(const std::string& table_name, const std::string& gtid,
                                              const mygram::utils::Error& error);

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
