/**
 * @file binlog_reader.cpp
 * @brief Binlog reader implementation
 *
 * Note: This file contains MySQL binlog protocol parsing code.
 * Some modern C++ guidelines are relaxed for protocol compatibility.
 */

#include "mysql/binlog_reader.h"

#ifdef USE_MYSQL

#include <spdlog/spdlog.h>

#include <algorithm>
#include <chrono>
#include <cstring>
#include <utility>

#include "mysql/binlog_event_parser.h"
#include "mysql/binlog_event_processor.h"
#include "mysql/binlog_event_types.h"
#include "mysql/gtid_encoder.h"
#include "server/tcp_server.h"  // For TableContext definition

// NOLINTBEGIN(cppcoreguidelines-pro-*,cppcoreguidelines-avoid-*,readability-magic-numbers)

namespace mygramdb::mysql {

// Single-table mode constructor (deprecated)
BinlogReader::BinlogReader(Connection& connection, index::Index& index, storage::DocumentStore& doc_store,
                           config::TableConfig table_config, const Config& config, server::ServerStats* stats)
    : connection_(connection),

      index_(&index),
      doc_store_(&doc_store),
      table_config_(std::move(table_config)),
      config_(config),
      current_gtid_(config.start_gtid),
      server_stats_(stats) {}

// Multi-table mode constructor
BinlogReader::BinlogReader(Connection& connection,
                           std::unordered_map<std::string, server::TableContext*> table_contexts, const Config& config,
                           server::ServerStats* stats)
    : connection_(connection),
      table_contexts_(std::move(table_contexts)),
      multi_table_mode_(true),
      config_(config),
      current_gtid_(config.start_gtid),
      server_stats_(stats) {}

BinlogReader::~BinlogReader() {
  Stop();
}

bool BinlogReader::Start() {
  // Atomically check and set running_ to prevent concurrent Start() calls
  bool expected = false;
  if (!running_.compare_exchange_strong(expected, true)) {
    last_error_ = "Binlog reader is already running";
    return false;
  }

  // RAII guard to reset running_ flag on failure
  struct RunningGuard {
    std::atomic<bool>& flag;
    bool& success;
    explicit RunningGuard(std::atomic<bool>& flag_ref, bool& success_ref) : flag(flag_ref), success(success_ref) {}
    ~RunningGuard() {
      if (!success) {
        flag = false;
      }
    }
    RunningGuard(const RunningGuard&) = delete;
    RunningGuard& operator=(const RunningGuard&) = delete;
    RunningGuard(RunningGuard&&) = delete;
    RunningGuard& operator=(RunningGuard&&) = delete;
  };
  bool start_success = false;
  RunningGuard guard(running_, start_success);

  // Check MySQL connection
  if (!connection_.IsConnected()) {
    last_error_ = "MySQL connection not established";
    spdlog::error("Cannot start binlog reader: {}", last_error_);
    return false;
  }

  // Check if GTID mode is enabled (using main connection)
  if (!connection_.IsGTIDModeEnabled()) {
    last_error_ =
        "GTID mode is not enabled on MySQL server. "
        "Please enable GTID mode (gtid_mode=ON) for binlog replication.";
    spdlog::error("Cannot start binlog reader: {}", last_error_);
    return false;
  }

  // Validate primary keys for all tables
  if (multi_table_mode_) {
    for (const auto& [table_name, ctx] : table_contexts_) {
      std::string validation_error;
      if (!connection_.ValidateUniqueColumn(connection_.GetConfig().database, ctx->config.name, ctx->config.primary_key,
                                            validation_error)) {
        last_error_ = "Primary key validation failed for table '";
        last_error_ += table_name;
        last_error_ += "': ";
        last_error_ += validation_error;
        spdlog::error("Cannot start binlog reader: {}", last_error_);
        return false;
      }
    }
  } else {
    // Single-table mode
    std::string validation_error;
    if (!connection_.ValidateUniqueColumn(connection_.GetConfig().database, table_config_.name,
                                          table_config_.primary_key, validation_error)) {
      last_error_ = "Primary key validation failed: " + validation_error;
      spdlog::error("Cannot start binlog reader: {}", last_error_);
      return false;
    }
  }

  // Create dedicated connection for binlog reading
  // We need a separate connection because mysql_binlog_* functions
  // are blocking and cannot share a connection with other queries
  spdlog::info("Creating dedicated binlog connection...");
  mysql::Connection::Config binlog_conn_config;
  binlog_conn_config.host = connection_.GetConfig().host;
  binlog_conn_config.port = connection_.GetConfig().port;
  binlog_conn_config.user = connection_.GetConfig().user;
  binlog_conn_config.password = connection_.GetConfig().password;
  binlog_conn_config.database = connection_.GetConfig().database;
  binlog_conn_config.connect_timeout = connection_.GetConfig().connect_timeout;
  binlog_conn_config.read_timeout = connection_.GetConfig().read_timeout;
  binlog_conn_config.write_timeout = connection_.GetConfig().write_timeout;

  binlog_connection_ = std::make_unique<Connection>(binlog_conn_config);
  if (!binlog_connection_->Connect("binlog worker")) {
    last_error_ = "Failed to create binlog connection: " + binlog_connection_->GetLastError();
    spdlog::error("Cannot start binlog reader: {}", last_error_);
    binlog_connection_.reset();
    return false;
  }

  should_stop_ = false;
  // Note: running_ is already set to true by compare_exchange_strong above

  try {
    // Start worker thread first
    worker_thread_ = std::make_unique<std::thread>(&BinlogReader::WorkerThreadFunc, this);

    // Start reader thread
    reader_thread_ = std::make_unique<std::thread>(&BinlogReader::ReaderThreadFunc, this);

    spdlog::info("Binlog reader started from GTID: {}", current_gtid_);
    // NOLINTNEXTLINE(clang-analyzer-deadcode.DeadStores) - Used by RunningGuard destructor
    start_success = true;  // Mark start as successful
    return true;
  } catch (const std::exception& e) {
    last_error_ = std::string("Failed to start threads: ") + e.what();
    spdlog::error("Cannot start binlog reader: {}", last_error_);

    // Clean up on failure (RunningGuard will reset running_ flag)
    should_stop_ = true;

    // Ensure threads are properly joined and cleaned up to prevent leaks
    // Even if only one thread was created before exception, both are checked
    if (worker_thread_ && worker_thread_->joinable()) {
      worker_thread_->join();
    }
    if (reader_thread_ && reader_thread_->joinable()) {
      reader_thread_->join();
    }

    // Explicitly reset thread objects to ensure cleanup
    worker_thread_.reset();
    reader_thread_.reset();

    return false;
  }
}

void BinlogReader::Stop() {
  if (!running_) {
    return;
  }

  spdlog::info("Stopping binlog reader...");
  should_stop_ = true;

  // Wake up threads
  queue_cv_.notify_all();
  queue_full_cv_.notify_all();

  // Close binlog connection BEFORE joining threads to unblock mysql_binlog_fetch()
  // This forces the reader thread to exit from its blocking call
  if (binlog_connection_) {
    spdlog::debug("Closing binlog connection to unblock reader thread");
    binlog_connection_->Close();
  }

  // Wait for threads to finish BEFORE destroying binlog_connection_
  // This ensures threads don't access connection during destruction
  if (reader_thread_ && reader_thread_->joinable()) {
    reader_thread_->join();
    reader_thread_.reset();
  }

  if (worker_thread_ && worker_thread_->joinable()) {
    worker_thread_->join();
    worker_thread_.reset();
  }

  // Now it's safe to destroy the connection
  if (binlog_connection_) {
    binlog_connection_.reset();
  }

  running_ = false;
  spdlog::info("Binlog reader stopped. Processed {} events", processed_events_.load());
}

std::string BinlogReader::GetCurrentGTID() const {
  std::scoped_lock lock(gtid_mutex_);
  return current_gtid_;
}

void BinlogReader::SetCurrentGTID(const std::string& gtid) {
  std::scoped_lock lock(gtid_mutex_);
  current_gtid_ = gtid;
  spdlog::info("Set replication GTID to: {}", gtid);
}

size_t BinlogReader::GetQueueSize() const {
  std::scoped_lock lock(queue_mutex_);
  return event_queue_.size();
}

void BinlogReader::ReaderThreadFunc() {
  spdlog::info("Binlog reader thread started");

  // Get starting GTID
  std::string gtid_set;
  {
    std::scoped_lock lock(gtid_mutex_);
    if (!current_gtid_.empty()) {
      gtid_set = current_gtid_;
      spdlog::info("Starting binlog replication from GTID: {}", gtid_set);
    }
  }

  // Main reconnection loop (infinite retries)
  int reconnect_attempt = 0;

  while (!should_stop_) {
    // Disable binlog checksums for this connection
    // We don't verify checksums yet, so ask the server to send events without them
    if (mysql_query(binlog_connection_->GetHandle(), "SET @source_binlog_checksum='NONE'") != 0) {
      last_error_ = "Failed to disable binlog checksum: " + binlog_connection_->GetLastError();
      spdlog::error("{}", last_error_);

      // Retry connection after delay
      spdlog::info("[binlog worker] Will retry connection in {} ms", config_.reconnect_delay_ms);
      std::this_thread::sleep_for(std::chrono::milliseconds(config_.reconnect_delay_ms));

      // Check if stop was requested during sleep
      if (should_stop_) {
        spdlog::debug("Stop requested during retry delay, exiting");
        break;
      }

      // Reconnect
      if (!binlog_connection_->Connect("binlog worker")) {
        spdlog::error("[binlog worker] Failed to reconnect: {}", binlog_connection_->GetLastError());
        continue;
      }
      spdlog::info("[binlog worker] Reconnected successfully");
      // NOLINTNEXTLINE(clang-analyzer-deadcode.DeadStores) - Used in next iteration after continue
      reconnect_attempt = 0;  // Reset delay counter after successful reconnection
      continue;
    }
    spdlog::info("Binlog checksums disabled for replication");

    // Initialize MYSQL_RPL structure for binlog reading
    MYSQL_RPL rpl{};
    rpl.file_name_length = 0;  // 0 means start from current position
    rpl.file_name = nullptr;
    rpl.start_position = 4;      // Skip magic number at start of binlog
    rpl.server_id = 1001;        // Use non-zero server ID for replica
    rpl.flags = MYSQL_RPL_GTID;  // Use GTID mode (allow heartbeat events)

    // Use current GTID for replication (updated after each event)
    std::string current_gtid = GetCurrentGTID();

    // Encode GTID set to binary format if we have one
    if (!current_gtid.empty()) {
      // Protect gtid_encoded_data_ access with mutex to prevent race conditions
      // if Start() is called concurrently (though compare_exchange_strong should prevent this)
      {
        std::lock_guard<std::mutex> lock(gtid_mutex_);
        // Encode GTID set using our encoder and store in member variable
        // (must persist during mysql_binlog_open call)
        gtid_encoded_data_ = mygramdb::mysql::GtidEncoder::Encode(current_gtid);
      }

      // Use callback approach: MySQL will call our callback to encode the GTID into the packet
      rpl.gtid_set_encoded_size = gtid_encoded_data_.size();
      rpl.gtid_set_arg = &gtid_encoded_data_;                // Pass pointer to our encoded data
      rpl.fix_gtid_set = &BinlogReader::FixGtidSetCallback;  // Static callback function

      spdlog::info("Using GTID set '{}' (encoded to {} bytes)", current_gtid, gtid_encoded_data_.size());
    } else {
      // Empty GTID set: receive all events from current binlog position
      rpl.gtid_set_encoded_size = 0;
      rpl.gtid_set_arg = nullptr;
      rpl.fix_gtid_set = nullptr;
      spdlog::info("Using empty GTID set (will receive all events)");
    }

    // Open binlog stream
    if (mysql_binlog_open(binlog_connection_->GetHandle(), &rpl) != 0) {
      last_error_ = "Failed to open binlog stream: " + binlog_connection_->GetLastError();
      spdlog::error("{}", last_error_);

      // Retry connection after delay
      spdlog::info("Will retry connection in {} ms", config_.reconnect_delay_ms);
      std::this_thread::sleep_for(std::chrono::milliseconds(config_.reconnect_delay_ms));

      // Check if stop was requested during sleep
      if (should_stop_) {
        spdlog::debug("Stop requested during retry delay, exiting");
        break;
      }

      // Reconnect
      if (!binlog_connection_->Connect()) {
        spdlog::error("Failed to reconnect: {}", binlog_connection_->GetLastError());
      } else {
        // NOLINTNEXTLINE(clang-analyzer-deadcode.DeadStores) - Used in next iteration after continue
        reconnect_attempt = 0;  // Reset delay counter after successful reconnection
      }
      continue;
    }

    spdlog::info("Binlog stream opened successfully");
    reconnect_attempt = 0;  // Reset reconnect counter on success

    // Read binlog events
    int event_count = 0;
    bool connection_lost = false;

    while (!should_stop_ && !connection_lost) {
      // Fetch next binlog event
      spdlog::debug("Calling mysql_binlog_fetch...");
      int result = mysql_binlog_fetch(binlog_connection_->GetHandle(), &rpl);

      // Check should_stop_ immediately after blocking call to avoid use-after-free
      // (Stop() may have closed the connection while we were blocked)
      if (should_stop_) {
        spdlog::debug("Stop requested, exiting reader loop");
        break;
      }

      if (result != 0) {
        unsigned int err_no = mysql_errno(binlog_connection_->GetHandle());
        const char* err_str = mysql_error(binlog_connection_->GetHandle());
        last_error_ =
            "Failed to fetch binlog event: " + std::string(err_str) + " (errno: " + std::to_string(err_no) + ")";

        // Check if this is a recoverable error (connection timeout/lost)
        if (err_no == 2013 || err_no == 2006) {  // Connection lost or gone away
          spdlog::info("{} (will attempt to reconnect)", last_error_);
          spdlog::debug("mysql_binlog_fetch returned: {}", result);
          connection_lost = true;

          // Close current binlog stream
          mysql_binlog_close(binlog_connection_->GetHandle(), &rpl);

          // Wait before reconnecting with exponential backoff (capped at 10x)
          reconnect_attempt = std::min(reconnect_attempt + 1, 10);
          int delay_ms = config_.reconnect_delay_ms * reconnect_attempt;
          spdlog::info("Reconnect attempt #{}, waiting {} ms", reconnect_attempt, delay_ms);
          std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));

          // Check again before reconnecting
          if (should_stop_) {
            spdlog::debug("Stop requested during reconnect delay, exiting");
            break;
          }

          // Reconnect
          if (!binlog_connection_->Connect()) {
            spdlog::error("Failed to reconnect: {}", binlog_connection_->GetLastError());
          } else {
            // NOLINTNEXTLINE(clang-analyzer-deadcode.DeadStores) - Used after break exits inner loop
            reconnect_attempt = 0;  // Reset delay counter after successful reconnection
          }
          break;  // Exit inner loop to retry from outer loop
        }
        // Non-recoverable error - log as error and stop
        spdlog::error("{}", last_error_);
        spdlog::error("mysql_binlog_fetch returned: {}", result);
        should_stop_ = true;
        break;
      }

      // Check if we have data
      if (rpl.size == 0 || rpl.buffer == nullptr) {
        // No data available (EOF or keepalive)
        spdlog::debug("No data in binlog fetch (size={}, buffer={})", rpl.size, (void*)rpl.buffer);
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        continue;
      }

      event_count++;
      spdlog::debug("Received binlog event #{}, size: {} bytes", event_count, rpl.size);

      // Check for GTID events first (need to update current_gtid)
      if (rpl.size >= 20) {
        const unsigned char* buffer_check = rpl.buffer + 1;  // Skip OK byte
        auto event_type = static_cast<MySQLBinlogEventType>(buffer_check[4]);

        if (event_type == MySQLBinlogEventType::GTID_LOG_EVENT) {
          auto gtid_opt = BinlogEventParser::ExtractGTID(rpl.buffer, rpl.size);
          if (gtid_opt) {
            UpdateCurrentGTID(gtid_opt.value());
            spdlog::debug("Updated GTID to: {}", gtid_opt.value());
          }
          continue;  // Skip to next event
        }

        if (event_type == MySQLBinlogEventType::TABLE_MAP_EVENT) {
          auto metadata_opt = BinlogEventParser::ParseTableMapEvent(rpl.buffer, rpl.size);
          if (metadata_opt) {
            if (!FetchColumnNames(metadata_opt.value())) {
              spdlog::warn("Failed to fetch column names for {}.{}, using col_N placeholders",
                           metadata_opt->database_name, metadata_opt->table_name);
            }
            table_metadata_cache_.Add(metadata_opt->table_id, metadata_opt.value());
            spdlog::debug("Cached TABLE_MAP: {}.{} (table_id={})", metadata_opt->database_name,
                          metadata_opt->table_name, metadata_opt->table_id);
          }
          continue;  // Skip to next event
        }
      }

      // Parse the binlog event using BinlogEventParser
      auto event_opt = BinlogEventParser::ParseBinlogEvent(
          rpl.buffer, rpl.size, current_gtid_, table_metadata_cache_, table_contexts_,
          multi_table_mode_ ? nullptr : &table_config_, multi_table_mode_);

      if (event_opt) {
        spdlog::debug("Parsed event: type={}, table={}", static_cast<int>(event_opt->type), event_opt->table_name);

        // Log important data events at info level
        if (event_opt->type == BinlogEventType::INSERT || event_opt->type == BinlogEventType::UPDATE ||
            event_opt->type == BinlogEventType::DELETE) {
          const char* event_type_str = "UNKNOWN";
          if (event_opt->type == BinlogEventType::INSERT) {
            event_type_str = "INSERT";
          } else if (event_opt->type == BinlogEventType::UPDATE) {
            event_type_str = "UPDATE";
          } else if (event_opt->type == BinlogEventType::DELETE) {
            event_type_str = "DELETE";
          }
          spdlog::info("Binlog event: {} on table '{}', pk={}", event_type_str, event_opt->table_name,
                       event_opt->primary_key);
        }

        PushEvent(event_opt.value());
      } else {
        spdlog::debug("Event skipped (not a data event or parse failed)");
      }
    }

    // Close binlog stream if still connected
    if (binlog_connection_ && binlog_connection_->IsConnected()) {
      mysql_binlog_close(binlog_connection_->GetHandle(), &rpl);
    }

    // If not reconnecting, exit the loop
    if (!connection_lost || should_stop_) {
      break;
    }
  }

  spdlog::info("Binlog reader thread stopped");
}

void BinlogReader::WorkerThreadFunc() {
  spdlog::info("Binlog worker thread started");

  while (!should_stop_) {
    BinlogEvent event;
    if (!PopEvent(event)) {
      continue;
    }

    if (!ProcessEvent(event)) {
      spdlog::error("Failed to process event for table {}, pk: {}", event.table_name, event.primary_key);
    }

    processed_events_++;
    UpdateCurrentGTID(event.gtid);
  }

  spdlog::info("Binlog worker thread stopped");
}

void BinlogReader::PushEvent(const BinlogEvent& event) {
  std::unique_lock<std::mutex> lock(queue_mutex_);

  // Wait if queue is full
  queue_full_cv_.wait(lock, [this] { return should_stop_ || event_queue_.size() < config_.queue_size; });

  if (should_stop_) {
    return;
  }

  event_queue_.push(event);
  queue_cv_.notify_one();
}

bool BinlogReader::PopEvent(BinlogEvent& event) {
  std::unique_lock<std::mutex> lock(queue_mutex_);

  // Wait if queue is empty
  queue_cv_.wait(lock, [this] { return should_stop_ || !event_queue_.empty(); });

  if (should_stop_ && event_queue_.empty()) {
    return false;
  }

  event = event_queue_.front();
  event_queue_.pop();

  // Notify reader thread that queue has space
  queue_full_cv_.notify_one();

  return true;
}

bool BinlogReader::ProcessEvent(const BinlogEvent& event) {
  // Determine which index/doc_store/config to use based on mode
  index::Index* current_index = nullptr;
  storage::DocumentStore* current_doc_store = nullptr;
  const config::TableConfig* current_config = nullptr;

  if (multi_table_mode_) {
    // Multi-table mode: lookup table from event
    auto table_iter = table_contexts_.find(event.table_name);
    if (table_iter == table_contexts_.end()) {
      // Event is for a table we're not tracking, skip silently
      if (server_stats_ != nullptr) {
        server_stats_->IncrementReplEventsSkippedOtherTables();
      }
      return true;
    }
    if (!table_iter->second->index || !table_iter->second->doc_store) {
      spdlog::error("Table context for '{}' has null index or doc_store", event.table_name);
      return false;
    }
    current_index = table_iter->second->index.get();
    current_doc_store = table_iter->second->doc_store.get();
    current_config = &table_iter->second->config;
  } else {
    // Single-table mode: skip events for other tables
    if (event.table_name != table_config_.name) {
      if (server_stats_ != nullptr) {
        server_stats_->IncrementReplEventsSkippedOtherTables();
      }
      return true;
    }
    current_index = index_;
    current_doc_store = doc_store_;
    current_config = &table_config_;
  }

  // Delegate to BinlogEventProcessor
  return BinlogEventProcessor::ProcessEvent(event, *current_index, *current_doc_store, *current_config, server_stats_);
}

void BinlogReader::UpdateCurrentGTID(const std::string& gtid) {
  std::scoped_lock lock(gtid_mutex_);
  current_gtid_ = gtid;
}

// FetchColumnNames implementation (remains in BinlogReader as it accesses connection_)
bool BinlogReader::FetchColumnNames(TableMetadata& metadata) {
  std::string cache_key = metadata.database_name + "." + metadata.table_name;

  // Check cache first
  {
    std::lock_guard<std::mutex> lock(column_names_cache_mutex_);
    auto cache_it = column_names_cache_.find(cache_key);
    if (cache_it != column_names_cache_.end()) {
      // Cache hit: update column names from cache
      const auto& column_names = cache_it->second;
      if (column_names.size() == metadata.columns.size()) {
        for (size_t i = 0; i < metadata.columns.size(); i++) {
          metadata.columns[i].name = column_names[i];
        }
        spdlog::debug("Column names for {}.{} loaded from cache", metadata.database_name, metadata.table_name);
        return true;
      }
      // Cache mismatch (column count changed?), fall through to query
      spdlog::warn("Cached column names for {}.{} have mismatched count (cached={}, current={})",
                   metadata.database_name, metadata.table_name, column_names.size(), metadata.columns.size());
      column_names_cache_.erase(cache_it);  // Remove stale cache entry
    }
  }

  // Cache miss or stale: use SHOW COLUMNS (faster than INFORMATION_SCHEMA)
  // Escape backticks in identifier names
  auto escape_identifier = [](const std::string& identifier) {
    std::string escaped;
    escaped.reserve(identifier.length());
    for (char chr : identifier) {
      if (chr == '`') {
        escaped += "``";  // Double backtick for escaping
      } else {
        escaped += chr;
      }
    }
    return escaped;
  };

  std::string query = "SHOW COLUMNS FROM `" + escape_identifier(metadata.database_name) + "`.`" +
                      escape_identifier(metadata.table_name) + "`";

  MySQLResult result = connection_.Execute(query);
  if (!result) {
    spdlog::error("Failed to query column names for {}.{}: {}", metadata.database_name, metadata.table_name,
                  connection_.GetLastError());
    return false;
  }

  std::vector<std::string> column_names;
  column_names.reserve(metadata.columns.size());

  MYSQL_ROW row = nullptr;
  while ((row = mysql_fetch_row(result.get())) != nullptr) {
    column_names.emplace_back(row[0]);
  }

  // result automatically freed by MySQLResult destructor

  if (column_names.size() != metadata.columns.size()) {
    spdlog::error("Column count mismatch for {}.{}: SHOW COLUMNS returned {}, binlog has {}", metadata.database_name,
                  metadata.table_name, column_names.size(), metadata.columns.size());
    return false;
  }

  // Update metadata with actual column names
  for (size_t i = 0; i < metadata.columns.size(); i++) {
    metadata.columns[i].name = column_names[i];
  }

  // Store in cache
  {
    std::lock_guard<std::mutex> lock(column_names_cache_mutex_);
    column_names_cache_[cache_key] = std::move(column_names);
  }

  spdlog::info("Fetched {} column names for {}.{} from SHOW COLUMNS", metadata.columns.size(), metadata.database_name,
               metadata.table_name);

  return true;
}

void BinlogReader::FixGtidSetCallback(MYSQL_RPL* rpl, unsigned char* packet_gtid_set) {
  // Copy pre-encoded GTID data into the packet buffer
  auto* encoded_data = static_cast<std::vector<uint8_t>*>(rpl->gtid_set_arg);
  std::memcpy(packet_gtid_set, encoded_data->data(), encoded_data->size());
}
}  // namespace mygramdb::mysql

// NOLINTEND(cppcoreguidelines-pro-*,cppcoreguidelines-avoid-*,readability-magic-numbers)

#endif  // USE_MYSQL
