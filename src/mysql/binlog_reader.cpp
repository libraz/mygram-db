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
#include <iomanip>
#include <regex>
#include <sstream>
#include <utility>

#include "mysql/binlog_event_types.h"
#include "mysql/binlog_util.h"
#include "mysql/gtid_encoder.h"
#include "mysql/rows_parser.h"
#include "server/tcp_server.h"  // For TableContext definition
#include "utils/string_utils.h"

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

      // Parse the binlog event
      auto event_opt = ParseBinlogEvent(rpl.buffer, rpl.size);
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

bool BinlogReader::EvaluateRequiredFilters(const std::unordered_map<std::string, storage::FilterValue>& filters,
                                           const config::TableConfig& table_config) {
  // If no required_filters, all data is accepted
  if (table_config.required_filters.empty()) {
    return true;
  }

  // Check each required filter condition
  return std::all_of(table_config.required_filters.begin(), table_config.required_filters.end(),
                     [&filters](const auto& required_filter) {
                       auto filter_iter = filters.find(required_filter.name);
                       if (filter_iter == filters.end()) {
                         spdlog::warn("Required filter column '{}' not found in binlog event", required_filter.name);
                         return false;
                       }
                       return CompareFilterValue(filter_iter->second, required_filter);
                     });
}

std::unordered_map<std::string, storage::FilterValue> BinlogReader::ExtractAllFilters(
    const RowData& row_data, const config::TableConfig& table_config) {
  std::unordered_map<std::string, storage::FilterValue> all_filters;

  // Convert required_filters to FilterConfig format for extraction
  std::vector<config::FilterConfig> required_as_filters;
  for (const auto& req_filter : table_config.required_filters) {
    config::FilterConfig filter_config;
    filter_config.name = req_filter.name;
    filter_config.type = req_filter.type;
    filter_config.dict_compress = false;
    filter_config.bitmap_index = req_filter.bitmap_index;
    required_as_filters.push_back(filter_config);
  }

  // Extract required_filters columns
  auto required_filters = ExtractFilters(row_data, required_as_filters);
  all_filters.insert(required_filters.begin(), required_filters.end());

  // Extract optional filters columns
  auto optional_filters = ExtractFilters(row_data, table_config.filters);
  all_filters.insert(optional_filters.begin(), optional_filters.end());

  return all_filters;
}

std::unordered_map<std::string, storage::FilterValue> BinlogReader::ExtractAllFilters(const RowData& row_data) const {
  return ExtractAllFilters(row_data, table_config_);
}

bool BinlogReader::CompareFilterValue(const storage::FilterValue& value, const config::RequiredFilterConfig& filter) {
  // Handle NULL checks
  if (filter.op == "IS NULL") {
    return std::holds_alternative<std::monostate>(value);
  }
  if (filter.op == "IS NOT NULL") {
    return !std::holds_alternative<std::monostate>(value);
  }

  // If value is NULL and operator is not IS NULL/IS NOT NULL, condition fails
  if (std::holds_alternative<std::monostate>(value)) {
    return false;
  }

  // Compare based on type
  if (std::holds_alternative<int64_t>(value)) {
    // Integer comparison
    int64_t val = std::get<int64_t>(value);
    int64_t target = std::stoll(filter.value);

    if (filter.op == "=") {
      return val == target;
    }
    if (filter.op == "!=") {
      return val != target;
    }
    if (filter.op == "<") {
      return val < target;
    }
    if (filter.op == ">") {
      return val > target;
    }
    if (filter.op == "<=") {
      return val <= target;
    }
    if (filter.op == ">=") {
      return val >= target;
    }

  } else if (std::holds_alternative<double>(value)) {
    // Float comparison
    double val = std::get<double>(value);
    double target = 0.0;
    try {
      target = std::stod(filter.value);
    } catch (const std::exception& e) {
      spdlog::warn("Invalid float value in filter: {}", filter.value);
      return false;
    }

    if (filter.op == "=") {
      return std::abs(val - target) < 1e-9;
    }
    if (filter.op == "!=") {
      return std::abs(val - target) >= 1e-9;
    }
    if (filter.op == "<") {
      return val < target;
    }
    if (filter.op == ">") {
      return val > target;
    }
    if (filter.op == "<=") {
      return val <= target;
    }
    if (filter.op == ">=") {
      return val >= target;
    }

  } else if (std::holds_alternative<std::string>(value)) {
    // String comparison
    const auto& val = std::get<std::string>(value);
    const std::string& target = filter.value;

    if (filter.op == "=") {
      return val == target;
    }
    if (filter.op == "!=") {
      return val != target;
    }
    if (filter.op == "<") {
      return val < target;
    }
    if (filter.op == ">") {
      return val > target;
    }
    if (filter.op == "<=") {
      return val <= target;
    }
    if (filter.op == ">=") {
      return val >= target;
    }

  } else if (std::holds_alternative<uint64_t>(value)) {
    // Datetime/timestamp (stored as uint64_t epoch)
    uint64_t val = std::get<uint64_t>(value);

    // For datetime comparison, we need to parse target value
    // For now, assume target is numeric (epoch timestamp)
    // TODO: Add proper datetime parsing if needed
    uint64_t target = 0;
    try {
      target = std::stoull(filter.value);
    } catch (const std::exception& e) {
      spdlog::warn("Invalid datetime value in filter: {}", filter.value);
      return false;
    }

    if (filter.op == "=") {
      return val == target;
    }
    if (filter.op == "!=") {
      return val != target;
    }
    if (filter.op == "<") {
      return val < target;
    }
    if (filter.op == ">") {
      return val > target;
    }
    if (filter.op == "<=") {
      return val <= target;
    }
    if (filter.op == ">=") {
      return val >= target;
    }
  }

  spdlog::warn("Unsupported filter value type for comparison");
  return false;
}

bool BinlogReader::ProcessEvent(const BinlogEvent& event) {
  // Determine which index/doc_store/config to use based on mode
  index::Index* current_index = nullptr;
  storage::DocumentStore* current_doc_store = nullptr;
  config::TableConfig* current_config = nullptr;

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

  try {
    // Evaluate required_filters to determine if data should exist in index
    bool matches_required = EvaluateRequiredFilters(event.filters, *current_config);

    // Check if document already exists in index
    auto doc_id_opt = current_doc_store->GetDocId(event.primary_key);
    bool exists = doc_id_opt.has_value();

    switch (event.type) {
      case BinlogEventType::INSERT: {
        if (matches_required) {
          // Condition satisfied -> add to index
          storage::DocId doc_id = current_doc_store->AddDocument(event.primary_key, event.filters);

          std::string normalized = utils::NormalizeText(event.text, true, "keep", true);
          current_index->AddDocument(doc_id, normalized);

          spdlog::debug("INSERT: pk={} (added to index)", event.primary_key);
          if (server_stats_ != nullptr) {
            server_stats_->IncrementReplInsertApplied();
          }
        } else {
          // Condition not satisfied -> do not index
          spdlog::debug("INSERT: pk={} (skipped, does not match required_filters)", event.primary_key);
          if (server_stats_ != nullptr) {
            server_stats_->IncrementReplInsertSkipped();
          }
        }
        break;
      }

      case BinlogEventType::UPDATE: {
        if (exists && !matches_required) {
          // Transitioned out of required conditions -> DELETE from index
          storage::DocId doc_id = doc_id_opt.value();

          // Extract text to remove from index
          if (!event.text.empty()) {
            std::string normalized = utils::NormalizeText(event.text, true, "keep", true);
            current_index->RemoveDocument(doc_id, normalized);
          }

          current_doc_store->RemoveDocument(doc_id);

          spdlog::info("UPDATE: pk={} (removed from index, no longer matches required_filters)", event.primary_key);
          if (server_stats_ != nullptr) {
            server_stats_->IncrementReplUpdateRemoved();
          }

        } else if (!exists && matches_required) {
          // Transitioned into required conditions -> INSERT into index
          storage::DocId doc_id = current_doc_store->AddDocument(event.primary_key, event.filters);

          std::string normalized = utils::NormalizeText(event.text, true, "keep", true);
          current_index->AddDocument(doc_id, normalized);

          spdlog::info("UPDATE: pk={} (added to index, now matches required_filters)", event.primary_key);
          if (server_stats_ != nullptr) {
            server_stats_->IncrementReplUpdateAdded();
          }

        } else if (exists && matches_required) {
          // Still matches conditions -> UPDATE
          storage::DocId doc_id = doc_id_opt.value();

          // Update document store filters
          current_doc_store->UpdateDocument(doc_id, event.filters);

          // Update full-text index if text has changed
          bool text_changed = false;
          if (!event.old_text.empty() || !event.text.empty()) {
            // Remove old text from index if available
            if (!event.old_text.empty()) {
              std::string old_normalized = utils::NormalizeText(event.old_text, true, "keep", true);
              current_index->RemoveDocument(doc_id, old_normalized);
              text_changed = true;
            }

            // Add new text to index if available
            if (!event.text.empty()) {
              std::string new_normalized = utils::NormalizeText(event.text, true, "keep", true);
              current_index->AddDocument(doc_id, new_normalized);
              text_changed = true;
            }
          }

          if (text_changed) {
            spdlog::debug("UPDATE: pk={} (filters and text updated)", event.primary_key);
          } else {
            spdlog::debug("UPDATE: pk={} (filters updated)", event.primary_key);
          }
          if (server_stats_ != nullptr) {
            server_stats_->IncrementReplUpdateModified();
          }

        } else {
          // !exists && !matches_required -> do nothing
          spdlog::debug("UPDATE: pk={} (ignored, not in index and does not match required_filters)", event.primary_key);
          if (server_stats_ != nullptr) {
            server_stats_->IncrementReplUpdateSkipped();
          }
        }
        break;
      }

      case BinlogEventType::DELETE: {
        if (exists) {
          // Remove document from index
          storage::DocId doc_id = doc_id_opt.value();

          // For deletion, we extract text from binlog DELETE event (before image)
          // The rows_parser provides the deleted row data including text column
          if (!event.text.empty()) {
            std::string normalized = utils::NormalizeText(event.text, true, "keep", true);
            current_index->RemoveDocument(doc_id, normalized);
          }

          // Remove from document store
          current_doc_store->RemoveDocument(doc_id);

          spdlog::debug("DELETE: pk={} (removed from index)", event.primary_key);
          if (server_stats_ != nullptr) {
            server_stats_->IncrementReplDeleteApplied();
          }
        } else {
          // Not in index, nothing to do
          spdlog::debug("DELETE: pk={} (not in index, ignored)", event.primary_key);
          if (server_stats_ != nullptr) {
            server_stats_->IncrementReplDeleteSkipped();
          }
        }
        break;
      }

      case BinlogEventType::DDL: {
        // Handle DDL operations (TRUNCATE, ALTER, DROP)
        std::string query = event.text;
        std::string query_upper = query;
        std::transform(query_upper.begin(), query_upper.end(), query_upper.begin(), ::toupper);

        if (query_upper.find("TRUNCATE") != std::string::npos) {
          // TRUNCATE TABLE - clear all data
          spdlog::warn("TRUNCATE TABLE detected for table {}: {}", event.table_name, query);
          current_index->Clear();
          current_doc_store->Clear();
          spdlog::info("Cleared index and document store due to TRUNCATE");
        } else if (query_upper.find("DROP") != std::string::npos) {
          // DROP TABLE - clear all data and warn
          spdlog::error("DROP TABLE detected for table {}: {}", event.table_name, query);
          current_index->Clear();
          current_doc_store->Clear();
          spdlog::error(
              "Table dropped! Index and document store cleared. Please reconfigure or stop "
              "MygramDB.");
        } else if (query_upper.find("ALTER") != std::string::npos) {
          // ALTER TABLE - log warning about potential schema mismatch
          spdlog::warn("ALTER TABLE detected for table {}: {}", event.table_name, query);
          spdlog::warn("Schema change may cause data inconsistency. Consider rebuilding from snapshot.");
          // Note: We cannot automatically detect what changed (column type, name, etc.)
          // Users should manually rebuild if text column type or PK changed
        }
        if (server_stats_ != nullptr) {
          server_stats_->IncrementReplDdlExecuted();
        }
        break;
      }

      default:
        spdlog::warn("Unknown event type for pk={}", event.primary_key);
        return false;
    }

    return true;

  } catch (const std::exception& e) {
    spdlog::error("Exception processing event: {}", e.what());
    return false;
  }
}

void BinlogReader::UpdateCurrentGTID(const std::string& gtid) {
  std::scoped_lock lock(gtid_mutex_);
  current_gtid_ = gtid;
}

std::optional<BinlogEvent> BinlogReader::ParseBinlogEvent(const unsigned char* buffer, unsigned long length) {
  if ((buffer == nullptr) || length < 20) {
    // Minimum event size is 20 bytes (1 byte OK packet + 19 bytes binlog header)
    return std::nullopt;
  }

  // MySQL C API prepends an OK packet byte (0x00) before the actual binlog event
  // Skip the OK byte to get to the actual binlog event data
  buffer++;
  length--;

  // Binlog event header format (19 bytes):
  // timestamp (4 bytes)
  // event_type (1 byte)
  // server_id (4 bytes)
  // event_size (4 bytes)
  // log_pos (4 bytes)
  // flags (2 bytes)

  auto event_type = static_cast<MySQLBinlogEventType>(buffer[4]);

  // Log event type for debugging
  spdlog::debug("Received binlog event: {} (type={})", GetEventTypeName(event_type), static_cast<int>(buffer[4]));

  // Handle different event types
  switch (event_type) {
    case MySQLBinlogEventType::GTID_LOG_EVENT:
      // Extract and update GTID
      {
        auto gtid_opt = ExtractGTID(buffer, length);
        if (gtid_opt) {
          UpdateCurrentGTID(gtid_opt.value());
          spdlog::debug("Updated GTID to: {}", gtid_opt.value());
        }
      }
      return std::nullopt;  // GTID events don't generate data events

    case MySQLBinlogEventType::TABLE_MAP_EVENT:
      // Parse table metadata and cache it
      {
        auto metadata_opt = ParseTableMapEvent(buffer, length);
        if (metadata_opt) {
          // Fetch actual column names from SHOW COLUMNS (cached per table)
          // Binlog TABLE_MAP events don't include column names, only types
          if (!FetchColumnNames(metadata_opt.value())) {
            spdlog::warn("Failed to fetch column names for {}.{}, using col_N placeholders",
                         metadata_opt->database_name, metadata_opt->table_name);
          }
          table_metadata_cache_.Add(metadata_opt->table_id, metadata_opt.value());
          spdlog::debug("Cached TABLE_MAP: {}.{} (table_id={})", metadata_opt->database_name, metadata_opt->table_name,
                        metadata_opt->table_id);
        }
      }
      return std::nullopt;  // TABLE_MAP events don't generate data events

    case MySQLBinlogEventType::WRITE_ROWS_EVENT: {
      // Parse INSERT operations
      spdlog::debug("WRITE_ROWS_EVENT detected");

      // Extract table_id from post-header (skip common header 19 bytes)
      const unsigned char* post_header = buffer + 19;
      uint64_t table_id = 0;
      for (int i = 0; i < 6; i++) {
        table_id |= (uint64_t)post_header[i] << (i * 8);
      }

      // Get table metadata from cache
      const TableMetadata* table_meta = table_metadata_cache_.Get(table_id);
      if (table_meta == nullptr) {
        spdlog::warn("WRITE_ROWS event for unknown table_id {}", table_id);
        return std::nullopt;
      }

      // Parse rows using rows_parser
      // Determine text column and primary key from config
      // In multi-table mode, get config from table_contexts_; otherwise use table_config_
      const config::TableConfig* current_config = nullptr;
      if (multi_table_mode_) {
        auto table_iter = table_contexts_.find(table_meta->table_name);
        if (table_iter == table_contexts_.end()) {
          spdlog::warn("WRITE_ROWS event for table '{}' not found in table_contexts_", table_meta->table_name);
          return std::nullopt;
        }
        current_config = &table_iter->second->config;
      } else {
        current_config = &table_config_;
      }

      std::string text_column;
      if (!current_config->text_source.column.empty()) {
        text_column = current_config->text_source.column;
      } else if (!current_config->text_source.concat.empty()) {
        text_column = current_config->text_source.concat[0];
      } else {
        text_column = "";
      }

      auto rows_opt = ParseWriteRowsEvent(buffer, length, table_meta, current_config->primary_key, text_column);

      if (!rows_opt || rows_opt->empty()) {
        return std::nullopt;
      }

      // Create event from first row (for now, handle multi-row events later)
      const auto& row = rows_opt->front();
      BinlogEvent event;
      event.type = BinlogEventType::INSERT;
      event.table_name = table_meta->table_name;
      event.primary_key = row.primary_key;
      event.text = row.text;
      event.gtid = current_gtid_;

      // Extract all filters (required + optional) from row data using the correct config
      event.filters = ExtractAllFilters(row, *current_config);

      spdlog::debug("Parsed WRITE_ROWS: pk={}, text_len={}, filters={}", event.primary_key, event.text.length(),
                    event.filters.size());

      return event;
    }

    case MySQLBinlogEventType::UPDATE_ROWS_EVENT: {
      // Parse UPDATE operations
      spdlog::debug("UPDATE_ROWS_EVENT detected");

      // Extract table_id from post-header
      const unsigned char* post_header = buffer + 19;
      uint64_t table_id = 0;
      for (int i = 0; i < 6; i++) {
        table_id |= (uint64_t)post_header[i] << (i * 8);
      }

      // Get table metadata from cache
      const TableMetadata* table_meta = table_metadata_cache_.Get(table_id);
      if (table_meta == nullptr) {
        spdlog::warn("UPDATE_ROWS event for unknown table_id {}", table_id);
        return std::nullopt;
      }

      // Determine text column and primary key from config
      // In multi-table mode, get config from table_contexts_; otherwise use table_config_
      const config::TableConfig* current_config = nullptr;
      if (multi_table_mode_) {
        auto table_iter = table_contexts_.find(table_meta->table_name);
        if (table_iter == table_contexts_.end()) {
          spdlog::warn("UPDATE_ROWS event for table '{}' not found in table_contexts_", table_meta->table_name);
          return std::nullopt;
        }
        current_config = &table_iter->second->config;
      } else {
        current_config = &table_config_;
      }

      std::string text_column;
      if (!current_config->text_source.column.empty()) {
        text_column = current_config->text_source.column;
      } else if (!current_config->text_source.concat.empty()) {
        text_column = current_config->text_source.concat[0];
      } else {
        text_column = "";
      }

      // Parse rows using rows_parser
      auto row_pairs_opt = ParseUpdateRowsEvent(buffer, length, table_meta, current_config->primary_key, text_column);

      if (!row_pairs_opt || row_pairs_opt->empty()) {
        return std::nullopt;
      }

      // Create event from first row pair (for now)
      const auto& row_pair = row_pairs_opt->front();
      const auto& before_row = row_pair.first;  // Before image
      const auto& after_row = row_pair.second;  // After image

      BinlogEvent event;
      event.type = BinlogEventType::UPDATE;
      event.table_name = table_meta->table_name;
      event.primary_key = after_row.primary_key;
      event.text = after_row.text;       // New text (after image)
      event.old_text = before_row.text;  // Old text (before image) for index update
      event.gtid = current_gtid_;

      // Extract all filters (required + optional) from after image using the correct config
      event.filters = ExtractAllFilters(after_row, *current_config);

      spdlog::debug("Parsed UPDATE_ROWS: pk={}, text_len={}, filters={}", event.primary_key, event.text.length(),
                    event.filters.size());

      return event;
    }

    case MySQLBinlogEventType::DELETE_ROWS_EVENT: {
      // Parse DELETE operations
      spdlog::debug("DELETE_ROWS_EVENT detected");

      // Extract table_id from post-header
      const unsigned char* post_header = buffer + 19;
      uint64_t table_id = 0;
      for (int i = 0; i < 6; i++) {
        table_id |= (uint64_t)post_header[i] << (i * 8);
      }

      // Get table metadata from cache
      const TableMetadata* table_meta = table_metadata_cache_.Get(table_id);
      if (table_meta == nullptr) {
        spdlog::warn("DELETE_ROWS event for unknown table_id {}", table_id);
        return std::nullopt;
      }

      // Determine text column and primary key from config
      // In multi-table mode, get config from table_contexts_; otherwise use table_config_
      const config::TableConfig* current_config = nullptr;
      if (multi_table_mode_) {
        auto table_iter = table_contexts_.find(table_meta->table_name);
        if (table_iter == table_contexts_.end()) {
          spdlog::warn("DELETE_ROWS event for table '{}' not found in table_contexts_", table_meta->table_name);
          return std::nullopt;
        }
        current_config = &table_iter->second->config;
      } else {
        current_config = &table_config_;
      }

      std::string text_column;
      if (!current_config->text_source.column.empty()) {
        text_column = current_config->text_source.column;
      } else if (!current_config->text_source.concat.empty()) {
        text_column = current_config->text_source.concat[0];
      } else {
        text_column = "";
      }

      // Parse rows using rows_parser
      auto rows_opt = ParseDeleteRowsEvent(buffer, length, table_meta, current_config->primary_key, text_column);

      if (!rows_opt || rows_opt->empty()) {
        return std::nullopt;
      }

      // Create event from first row (for now)
      const auto& row = rows_opt->front();
      BinlogEvent event;
      event.type = BinlogEventType::DELETE;
      event.table_name = table_meta->table_name;
      event.primary_key = row.primary_key;
      event.text = row.text;
      event.gtid = current_gtid_;

      // Extract all filters (required + optional) from row data (before image for DELETE) using the correct config
      event.filters = ExtractAllFilters(row, *current_config);

      spdlog::debug("Parsed DELETE_ROWS: pk={}, text_len={}, filters={}", event.primary_key, event.text.length(),
                    event.filters.size());

      return event;
    }

    case MySQLBinlogEventType::QUERY_EVENT: {
      // DDL statements (CREATE, ALTER, DROP, TRUNCATE, etc.)
      // Parse query string to handle schema changes
      auto query_opt = ExtractQueryString(buffer, length);
      if (!query_opt) {
        return std::nullopt;
      }

      std::string query = query_opt.value();
      spdlog::debug("QUERY_EVENT: {}", query);

      // Check if this affects any of our target tables
      if (multi_table_mode_) {
        // Multi-table mode: check all registered tables
        for (const auto& [table_name, ctx] : table_contexts_) {
          if (IsTableAffectingDDL(query, table_name)) {
            BinlogEvent event;
            event.type = BinlogEventType::DDL;
            event.table_name = table_name;
            event.text = query;  // Store the DDL query
            return event;
          }
        }
      } else {
        // Single-table mode: check only our configured table
        if (IsTableAffectingDDL(query, table_config_.name)) {
          BinlogEvent event;
          event.type = BinlogEventType::DDL;
          event.table_name = table_config_.name;
          event.text = query;  // Store the DDL query
          return event;
        }
      }

      return std::nullopt;
    }

    case MySQLBinlogEventType::XID_EVENT:
      // Transaction commit marker
      return std::nullopt;

    default:
      // Ignore other event types
      return std::nullopt;
  }
}

std::optional<std::string> BinlogReader::ExtractGTID(const unsigned char* buffer, unsigned long length) {
  if ((buffer == nullptr) || length < 42) {
    // GTID event minimum size
    return std::nullopt;
  }

  // GTID event format (after 19-byte header):
  // commit_flag (1 byte)
  // sid (16 bytes, UUID)
  // gno (8 bytes, transaction number)

  // Skip header (19 bytes) and commit_flag (1 byte)
  const unsigned char* sid_ptr = buffer + 20;

  // Format UUID as string using std::ostringstream
  std::ostringstream uuid_oss;
  uuid_oss << std::hex << std::setfill('0') << std::setw(2) << static_cast<int>(sid_ptr[0]) << std::setw(2)
           << static_cast<int>(sid_ptr[1]) << std::setw(2) << static_cast<int>(sid_ptr[2]) << std::setw(2)
           << static_cast<int>(sid_ptr[3]) << '-' << std::setw(2) << static_cast<int>(sid_ptr[4]) << std::setw(2)
           << static_cast<int>(sid_ptr[5]) << '-' << std::setw(2) << static_cast<int>(sid_ptr[6]) << std::setw(2)
           << static_cast<int>(sid_ptr[7]) << '-' << std::setw(2) << static_cast<int>(sid_ptr[8]) << std::setw(2)
           << static_cast<int>(sid_ptr[9]) << '-' << std::setw(2) << static_cast<int>(sid_ptr[10]) << std::setw(2)
           << static_cast<int>(sid_ptr[11]) << std::setw(2) << static_cast<int>(sid_ptr[12]) << std::setw(2)
           << static_cast<int>(sid_ptr[13]) << std::setw(2) << static_cast<int>(sid_ptr[14]) << std::setw(2)
           << static_cast<int>(sid_ptr[15]);

  // Extract GNO (8 bytes, little-endian)
  const unsigned char* gno_ptr = sid_ptr + 16;
  uint64_t gno = 0;
  for (int i = 0; i < 8; i++) {
    gno |= (uint64_t)gno_ptr[i] << (i * 8);
  }

  // Format as "UUID:GNO"
  std::string gtid = uuid_oss.str() + ":" + std::to_string(gno);
  return gtid;
}

std::optional<TableMetadata> BinlogReader::ParseTableMapEvent(const unsigned char* buffer, unsigned long length) {
  if ((buffer == nullptr) || length < 8) {
    // Minimum TABLE_MAP event size (6 bytes table_id + 2 bytes flags)
    return std::nullopt;
  }

  TableMetadata metadata;

  // Skip header (19 bytes) to get to post-header
  const unsigned char* ptr = buffer + 19;
  unsigned long remaining = length - 19;

  if (remaining < 8) {
    return std::nullopt;
  }

  // Parse table_id (6 bytes)
  metadata.table_id = 0;
  for (int i = 0; i < 6; i++) {
    metadata.table_id |= (uint64_t)ptr[i] << (i * 8);
  }
  ptr += 6;
  remaining -= 6;

  // Skip flags (2 bytes)
  ptr += 2;
  remaining -= 2;

  if (remaining < 1) {
    return std::nullopt;
  }

  // Parse database name (1 byte length + null-terminated string)
  uint8_t db_len = *ptr++;
  remaining--;

  if (remaining < static_cast<size_t>(db_len) + 1) {  // +1 for null terminator
    return std::nullopt;
  }

  metadata.database_name = std::string(reinterpret_cast<const char*>(ptr), db_len);
  ptr += db_len + 1;  // +1 for null terminator
  remaining -= (db_len + 1);

  if (remaining < 1) {
    return std::nullopt;
  }

  // Parse table name (1 byte length + null-terminated string)
  uint8_t table_len = *ptr++;
  remaining--;

  if (remaining < static_cast<size_t>(table_len) + 1) {
    return std::nullopt;
  }

  metadata.table_name = std::string(reinterpret_cast<const char*>(ptr), table_len);
  ptr += table_len + 1;
  remaining -= (table_len + 1);

  if (remaining < 1) {
    return std::nullopt;
  }

  // Parse column count (packed integer)
  uint64_t column_count = binlog_util::read_packed_integer(&ptr);

  if (remaining < column_count) {
    return std::nullopt;
  }

  // Parse column types (1 byte per column)
  metadata.columns.reserve(column_count);
  for (uint64_t i = 0; i < column_count; i++) {
    ColumnMetadata col;
    col.type = static_cast<ColumnType>(*ptr++);
    col.metadata = 0;
    col.is_nullable = false;
    col.is_unsigned = false;
    // Column name is not available in TABLE_MAP event
    // Use column index as temporary name
    col.name = "col_" + std::to_string(i);
    metadata.columns.push_back(col);
  }

  // Parse metadata length (packed integer)
  if (ptr < buffer + length) {
    uint64_t metadata_len = binlog_util::read_packed_integer(&ptr);
    const unsigned char* metadata_start = ptr;

    // Parse type-specific metadata for each column
    // Metadata format varies by type - see Table_map_event documentation
    for (uint64_t i = 0; i < column_count && ptr < metadata_start + metadata_len; i++) {
      ColumnType type = metadata.columns[i].type;

      switch (type) {
        case ColumnType::VARCHAR:
        case ColumnType::VAR_STRING:
          // 2 bytes: max length
          if (ptr + 2 <= metadata_start + metadata_len) {
            metadata.columns[i].metadata = binlog_util::uint2korr(ptr);
            ptr += 2;
          }
          break;

        case ColumnType::BLOB:
        case ColumnType::TINY_BLOB:
        case ColumnType::MEDIUM_BLOB:
        case ColumnType::LONG_BLOB:
          // 1 byte: number of length bytes (1, 2, 3, or 4)
          if (ptr + 1 <= metadata_start + metadata_len) {
            metadata.columns[i].metadata = *ptr;
            ptr += 1;
          }
          break;

        case ColumnType::STRING:
          // 2 bytes: (real_type << 8) | max_length
          if (ptr + 2 <= metadata_start + metadata_len) {
            metadata.columns[i].metadata = binlog_util::uint2korr(ptr);
            ptr += 2;
          }
          break;

        case ColumnType::FLOAT:
        case ColumnType::DOUBLE:
          // 1 byte: pack length
          if (ptr + 1 <= metadata_start + metadata_len) {
            metadata.columns[i].metadata = *ptr;
            ptr += 1;
          }
          break;

        case ColumnType::NEWDECIMAL:
          // 2 bytes: (precision << 8) | scale
          if (ptr + 2 <= metadata_start + metadata_len) {
            metadata.columns[i].metadata = binlog_util::uint2korr(ptr);
            ptr += 2;
          }
          break;

        case ColumnType::BIT:
          // 2 bytes: (bytes << 8) | bits
          if (ptr + 2 <= metadata_start + metadata_len) {
            metadata.columns[i].metadata = binlog_util::uint2korr(ptr);
            ptr += 2;
          }
          break;

        case ColumnType::TIMESTAMP2:
        case ColumnType::DATETIME2:
        case ColumnType::TIME2:
          // 1 byte: fractional seconds precision (0-6)
          if (ptr + 1 <= metadata_start + metadata_len) {
            metadata.columns[i].metadata = *ptr;
            ptr += 1;
          }
          break;

        case ColumnType::ENUM:
        case ColumnType::SET:
          // 2 bytes: number of elements
          if (ptr + 2 <= metadata_start + metadata_len) {
            metadata.columns[i].metadata = binlog_util::uint2korr(ptr);
            ptr += 2;
          }
          break;

        // Types with no metadata
        case ColumnType::TINY:
        case ColumnType::SHORT:
        case ColumnType::LONG:
        case ColumnType::LONGLONG:
        case ColumnType::INT24:
        case ColumnType::DATE:
        case ColumnType::DATETIME:
        case ColumnType::TIMESTAMP:
        case ColumnType::TIME:
        case ColumnType::YEAR:
          // No metadata for these types
          metadata.columns[i].metadata = 0;
          break;

        default:
          // Unknown type - skip metadata
          spdlog::warn("Unknown column type {} while parsing metadata", static_cast<int>(type));
          break;
      }
    }

    // Skip to end of metadata block
    ptr = metadata_start + metadata_len;
  }

  // Parse NULL bitmap if present
  if (ptr < buffer + length) {
    size_t null_bitmap_size = binlog_util::bitmap_bytes(column_count);
    if (ptr + null_bitmap_size <= buffer + length) {
      for (uint64_t i = 0; i < column_count; i++) {
        metadata.columns[i].is_nullable = binlog_util::bitmap_is_set(ptr, i);
      }
      ptr += null_bitmap_size;
    }
  }

  spdlog::debug("TABLE_MAP: {}.{} (table_id={}, columns={})", metadata.database_name, metadata.table_name,
                metadata.table_id, column_count);

  return metadata;
}

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

std::optional<std::string> BinlogReader::ExtractQueryString(const unsigned char* buffer, unsigned long length) {
  if ((buffer == nullptr) || length < 19) {
    // Minimum: 19 bytes header
    return std::nullopt;
  }

  // QUERY_EVENT format (after 19-byte common header):
  // thread_id (4 bytes)
  // query_exec_time (4 bytes)
  // db_len (1 byte)
  // error_code (2 bytes)
  // status_vars_len (2 bytes)
  // [status_vars (variable length)]
  // [db_name (variable length, null-terminated)]
  // [query (variable length)]

  const unsigned char* pos = buffer + 19;  // Skip common header
  size_t remaining = length - 19;

  if (remaining < 13) {  // Minimum: 4+4+1+2+2
    return std::nullopt;
  }

  // Skip thread_id (4 bytes)
  pos += 4;
  remaining -= 4;

  // Skip query_exec_time (4 bytes)
  pos += 4;
  remaining -= 4;

  // Get db_len (1 byte)
  uint8_t db_len = *pos;
  pos += 1;
  remaining -= 1;

  // Skip error_code (2 bytes)
  pos += 2;
  remaining -= 2;

  // Get status_vars_len (2 bytes, little-endian)
  uint16_t status_vars_len = pos[0] | (pos[1] << 8);
  pos += 2;
  remaining -= 2;

  // Skip status_vars
  if (remaining < status_vars_len) {
    return std::nullopt;
  }
  pos += status_vars_len;
  remaining -= status_vars_len;

  // Skip db_name (null-terminated)
  if (remaining < static_cast<size_t>(db_len) + 1) {  // +1 for null terminator
    return std::nullopt;
  }
  pos += db_len + 1;
  remaining -= (db_len + 1);

  // Extract query string
  if (remaining == 0) {
    return std::nullopt;
  }

  std::string query(reinterpret_cast<const char*>(pos), remaining);
  return query;
}

bool BinlogReader::IsTableAffectingDDL(const std::string& query, const std::string& table_name) {
  // Convert to uppercase for case-insensitive matching
  std::string query_upper = query;
  std::string table_upper = table_name;
  std::transform(query_upper.begin(), query_upper.end(), query_upper.begin(), ::toupper);
  std::transform(table_upper.begin(), table_upper.end(), table_upper.begin(), ::toupper);

  // Remove extra whitespace
  query_upper = std::regex_replace(query_upper, std::regex("\\s+"), " ");

  // Check for TRUNCATE TABLE
  if (std::regex_search(query_upper, std::regex("TRUNCATE\\s+TABLE\\s+`?" + table_upper + "`?"))) {
    return true;
  }

  // Check for DROP TABLE
  if (std::regex_search(query_upper, std::regex(R"(DROP\s+TABLE\s+(IF\s+EXISTS\s+)?`?)" + table_upper + "`?"))) {
    return true;
  }

  // Check for ALTER TABLE
  if (std::regex_search(query_upper, std::regex("ALTER\\s+TABLE\\s+`?" + table_upper + "`?"))) {
    return true;
  }

  return false;
}

}  // namespace mygramdb::mysql

// NOLINTEND(cppcoreguidelines-pro-*,cppcoreguidelines-avoid-*,readability-magic-numbers)

#endif  // USE_MYSQL
