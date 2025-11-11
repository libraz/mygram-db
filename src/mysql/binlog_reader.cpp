/**
 * @file binlog_reader.cpp
 * @brief Binlog reader implementation
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
#include "storage/gtid_state.h"
#include "utils/string_utils.h"

namespace mygramdb {
namespace mysql {

BinlogReader::BinlogReader(Connection& connection, index::Index& index,
                           storage::DocumentStore& doc_store, config::TableConfig table_config,
                           const Config& config)
    : connection_(connection),
      index_(index),
      doc_store_(doc_store),
      table_config_(std::move(table_config)),
      config_(config),
      current_gtid_(config.start_gtid) {}

BinlogReader::~BinlogReader() {
  Stop();
}

bool BinlogReader::Start() {
  if (running_) {
    last_error_ = "Binlog reader is already running";
    return false;
  }

  // Check MySQL connection
  if (!connection_.IsConnected()) {
    last_error_ = "MySQL connection not established";
    spdlog::error("Cannot start binlog reader: {}", last_error_);
    return false;
  }

  // Check if GTID mode is enabled (using main connection)
  if (!connection_.IsGTIDModeEnabled()) {
    last_error_ = "GTID mode is not enabled on MySQL server. "
                  "Please enable GTID mode (gtid_mode=ON) for binlog replication.";
    spdlog::error("Cannot start binlog reader: {}", last_error_);
    return false;
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
  if (!binlog_connection_->Connect()) {
    last_error_ = "Failed to create binlog connection: " + binlog_connection_->GetLastError();
    spdlog::error("Cannot start binlog reader: {}", last_error_);
    binlog_connection_.reset();
    return false;
  }
  spdlog::info("Dedicated binlog connection established");

  should_stop_ = false;
  running_ = true;

  try {
    // Start worker thread first
    worker_thread_ = std::make_unique<std::thread>(
        &BinlogReader::WorkerThreadFunc, this);

    // Start reader thread
    reader_thread_ = std::make_unique<std::thread>(
        &BinlogReader::ReaderThreadFunc, this);

    spdlog::info("Binlog reader started from GTID: {}", current_gtid_);
    return true;
  } catch (const std::exception& e) {
    last_error_ = std::string("Failed to start threads: ") + e.what();
    spdlog::error("Cannot start binlog reader: {}", last_error_);

    // Clean up on failure
    running_ = false;
    should_stop_ = true;

    // Try to join threads if they were created
    if (worker_thread_ && worker_thread_->joinable()) {
      worker_thread_->join();
    }
    if (reader_thread_ && reader_thread_->joinable()) {
      reader_thread_->join();
    }

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

  if (reader_thread_ && reader_thread_->joinable()) {
    reader_thread_->join();
  }

  if (worker_thread_ && worker_thread_->joinable()) {
    worker_thread_->join();
  }

  // Close binlog connection
  if (binlog_connection_) {
    binlog_connection_->Close();
    binlog_connection_.reset();
  }

  running_ = false;
  spdlog::info("Binlog reader stopped. Processed {} events",
               processed_events_.load());
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

// NOLINTNEXTLINE(readability-function-cognitive-complexity)
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
    if (mysql_query(binlog_connection_->GetHandle(),
                    "SET @source_binlog_checksum='NONE'") != 0) {
      last_error_ = "Failed to disable binlog checksum: " + binlog_connection_->GetLastError();
      spdlog::error("{}", last_error_);

      // Retry connection after delay
      spdlog::info("Will retry connection in {} ms", config_.reconnect_delay_ms);
      std::this_thread::sleep_for(std::chrono::milliseconds(config_.reconnect_delay_ms));

      // Reconnect
      if (!binlog_connection_->Connect()) {
        spdlog::error("Failed to reconnect: {}", binlog_connection_->GetLastError());
        continue;
      }
      continue;
    }
    spdlog::info("Binlog checksums disabled for replication");

    // Initialize MYSQL_RPL structure for binlog reading
    MYSQL_RPL rpl{};
    rpl.file_name_length = 0;    // 0 means start from current position
    rpl.file_name = nullptr;
    rpl.start_position = 4;      // Skip magic number at start of binlog
    rpl.server_id = 1001;        // Use non-zero server ID for replica
    rpl.flags = MYSQL_RPL_GTID;  // Use GTID mode (allow heartbeat events)

    // Use current GTID for replication (updated after each event)
    std::string current_gtid = GetCurrentGTID();

    // Encode GTID set to binary format if we have one
    if (!current_gtid.empty()) {
      // Encode GTID set using our encoder and store in member variable
      // (must persist during mysql_binlog_open call)
      gtid_encoded_data_ = mygram::mysql::GtidEncoder::Encode(current_gtid);

      // Use callback approach: MySQL will call our callback to encode the GTID into the packet
      rpl.gtid_set_encoded_size = gtid_encoded_data_.size();
      rpl.gtid_set_arg = &gtid_encoded_data_;  // Pass pointer to our encoded data
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

      // Reconnect
      if (!binlog_connection_->Connect()) {
        spdlog::error("Failed to reconnect: {}", binlog_connection_->GetLastError());
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

      if (result != 0) {
        unsigned int err_no = mysql_errno(binlog_connection_->GetHandle());
        const char* err_str = mysql_error(binlog_connection_->GetHandle());
        last_error_ = "Failed to fetch binlog event: " + std::string(err_str) +
                      " (errno: " + std::to_string(err_no) + ")";
        spdlog::error("{}", last_error_);
        spdlog::error("mysql_binlog_fetch returned: {}", result);

        // Check if this is a recoverable error
        if (err_no == 2013 || err_no == 2006) {  // Connection lost or gone away
          spdlog::warn("Connection lost, will attempt to reconnect...");
          connection_lost = true;

          // Close current binlog stream
          mysql_binlog_close(binlog_connection_->GetHandle(), &rpl);

          // Wait before reconnecting
          reconnect_attempt++;
          int delay_ms = config_.reconnect_delay_ms * std::min(reconnect_attempt, 10);
          spdlog::info("Reconnect attempt #{}, waiting {} ms", reconnect_attempt, delay_ms);
          std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));

          // Reconnect
          if (!binlog_connection_->Connect()) {
            spdlog::error("Failed to reconnect: {}", binlog_connection_->GetLastError());
          }
          break;  // Exit inner loop to retry from outer loop
        }  // Non-recoverable error
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
        spdlog::debug("Parsed event: type={}, table={}",
                      static_cast<int>(event_opt->type), event_opt->table_name);

        // Log important data events at info level
        if (event_opt->type == BinlogEventType::INSERT ||
            event_opt->type == BinlogEventType::UPDATE ||
            event_opt->type == BinlogEventType::DELETE) {
          const char* event_type_str = "UNKNOWN";
          if (event_opt->type == BinlogEventType::INSERT) {
            event_type_str = "INSERT";
          } else if (event_opt->type == BinlogEventType::UPDATE) {
            event_type_str = "UPDATE";
          } else if (event_opt->type == BinlogEventType::DELETE) {
            event_type_str = "DELETE";
          }
          spdlog::info("Binlog event: {} on table '{}', pk={}",
                       event_type_str, event_opt->table_name, event_opt->primary_key);
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
      spdlog::error("Failed to process event for table {}, pk: {}",
                   event.table_name, event.primary_key);
    }

    processed_events_++;
    UpdateCurrentGTID(event.gtid);

    // Periodically write GTID to state file
    if (!config_.state_file_path.empty() &&
        config_.state_write_interval_events > 0 &&
        processed_events_ % config_.state_write_interval_events == 0) {
      WriteGTIDToStateFile(event.gtid);
    }
  }

  // Write final GTID to state file before stopping
  if (!config_.state_file_path.empty()) {
    std::string final_gtid = GetCurrentGTID();
    if (!final_gtid.empty()) {
      WriteGTIDToStateFile(final_gtid);
    }
  }

  spdlog::info("Binlog worker thread stopped");
}

void BinlogReader::PushEvent(const BinlogEvent& event) {
  std::unique_lock<std::mutex> lock(queue_mutex_);

  // Wait if queue is full
  queue_full_cv_.wait(lock, [this] {
    return should_stop_ || event_queue_.size() < config_.queue_size;
  });

  if (should_stop_) {
    return;
  }

  event_queue_.push(event);
  queue_cv_.notify_one();
}

bool BinlogReader::PopEvent(BinlogEvent& event) {
  std::unique_lock<std::mutex> lock(queue_mutex_);

  // Wait if queue is empty
  queue_cv_.wait(lock, [this] {
    return should_stop_ || !event_queue_.empty();
  });

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
  // Skip events for other tables
  if (event.table_name != table_config_.name) {
    return true;
  }

  try {
    switch (event.type) {
      case BinlogEventType::INSERT: {
        // Add new document
        storage::DocId doc_id = 
            doc_store_.AddDocument(event.primary_key, event.filters);
        
        std::string normalized = utils::NormalizeText(event.text, true, "keep", true);
        index_.AddDocument(doc_id, normalized);
        
        spdlog::debug("INSERT: pk={}", event.primary_key);
        break;
      }

      case BinlogEventType::UPDATE: {
        // Update existing document
        auto doc_id_opt = doc_store_.GetDocId(event.primary_key);
        if (!doc_id_opt) {
          spdlog::warn("UPDATE: document not found for pk={}", event.primary_key);
          return false;
        }

        storage::DocId doc_id = doc_id_opt.value();

        // Update document store filters
        doc_store_.UpdateDocument(doc_id, event.filters);

        // For full-text index update, we would need the old text to remove old n-grams
        // The rows_parser provides both before and after images in UPDATE events
        // For now, we use simplified approach: update filters only
        // Future optimization: extract old text from before image and update index properly
        std::string new_normalized = utils::NormalizeText(event.text, true, "keep", true);

        spdlog::debug("UPDATE: pk={} (filters updated, text index update simplified)",
                     event.primary_key);
        break;
      }

      case BinlogEventType::DELETE: {
        // Remove document
        auto doc_id_opt = doc_store_.GetDocId(event.primary_key);
        if (!doc_id_opt) {
          spdlog::warn("DELETE: document not found for pk={}", event.primary_key);
          return false;
        }

        storage::DocId doc_id = doc_id_opt.value();

        // For deletion, we extract text from binlog DELETE event (before image)
        // The rows_parser provides the deleted row data including text column
        if (!event.text.empty()) {
          std::string normalized = utils::NormalizeText(event.text, true, "keep", true);
          index_.RemoveDocument(doc_id, normalized);
        }

        // Remove from document store
        doc_store_.RemoveDocument(doc_id);

        spdlog::debug("DELETE: pk={}", event.primary_key);
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
          index_.Clear();
          doc_store_.Clear();
          spdlog::info("Cleared index and document store due to TRUNCATE");
        } else if (query_upper.find("DROP") != std::string::npos) {
          // DROP TABLE - clear all data and warn
          spdlog::error("DROP TABLE detected for table {}: {}", event.table_name, query);
          index_.Clear();
          doc_store_.Clear();
          spdlog::error("Table dropped! Index and document store cleared. Please reconfigure or stop MygramDB.");
        } else if (query_upper.find("ALTER") != std::string::npos) {
          // ALTER TABLE - log warning about potential schema mismatch
          spdlog::warn("ALTER TABLE detected for table {}: {}", event.table_name, query);
          spdlog::warn("Schema change may cause data inconsistency. Consider rebuilding from snapshot.");
          // Note: We cannot automatically detect what changed (column type, name, etc.)
          // Users should manually rebuild if text column type or PK changed
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

void BinlogReader::WriteGTIDToStateFile(const std::string& gtid) const {
  if (gtid.empty()) {
    return;
  }

  try {
    storage::GTIDStateFile state_file(config_.state_file_path);
    if (!state_file.Write(gtid)) {
      spdlog::warn("Failed to write GTID to state file: {}", gtid);
    }
  } catch (const std::exception& e) {
    spdlog::error("Exception while writing GTID to state file: {}", e.what());
  }
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity)
std::optional<BinlogEvent> BinlogReader::ParseBinlogEvent(
    const unsigned char* buffer, unsigned long length) {
  if ((buffer == nullptr) || length < 19) {
    // Minimum event size is 19 bytes (header)
    return std::nullopt;
  }

  // Binlog event header format (19 bytes):
  // timestamp (4 bytes)
  // event_type (1 byte)
  // server_id (4 bytes)
  // event_size (4 bytes)
  // log_pos (4 bytes)
  // flags (2 bytes)

  auto event_type = static_cast<MySQLBinlogEventType>(buffer[4]);

  // Log event type for debugging
  spdlog::debug("Received binlog event: {}", GetEventTypeName(event_type));

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
          table_metadata_cache_.Add(metadata_opt->table_id, metadata_opt.value());
          spdlog::debug("Cached TABLE_MAP: {}.{} (table_id={})",
                       metadata_opt->database_name,
                       metadata_opt->table_name,
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
      // Determine text column from config
      std::string text_column;
      if (!table_config_.text_source.column.empty()) {
        text_column = table_config_.text_source.column;
      } else if (!table_config_.text_source.concat.empty()) {
        text_column = table_config_.text_source.concat[0];
      } else {
        text_column = "";
      }

      auto rows_opt = ParseWriteRowsEvent(
          buffer, length, table_meta,
          table_config_.primary_key,
          text_column);

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

      // Extract filters from row data
      event.filters = ExtractFilters(row, table_config_.filters);

      spdlog::debug("Parsed WRITE_ROWS: pk={}, text_len={}, filters={}",
                   event.primary_key, event.text.length(), event.filters.size());

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

      // Determine text column from config
      std::string text_column;
      if (!table_config_.text_source.column.empty()) {
        text_column = table_config_.text_source.column;
      } else if (!table_config_.text_source.concat.empty()) {
        text_column = table_config_.text_source.concat[0];
      } else {
        text_column = "";
      }

      // Parse rows using rows_parser
      auto row_pairs_opt = ParseUpdateRowsEvent(
          buffer, length, table_meta,
          table_config_.primary_key,
          text_column);

      if (!row_pairs_opt || row_pairs_opt->empty()) {
        return std::nullopt;
      }

      // Create event from first row pair (for now)
      const auto& row_pair = row_pairs_opt->front();
      const auto& after_row = row_pair.second;  // Use after image

      BinlogEvent event;
      event.type = BinlogEventType::UPDATE;
      event.table_name = table_meta->table_name;
      event.primary_key = after_row.primary_key;
      event.text = after_row.text;
      event.gtid = current_gtid_;

      // Extract filters from after image
      event.filters = ExtractFilters(after_row, table_config_.filters);

      // Note: For proper index update, we should use before image text
      // to remove old n-grams. For now, simplified implementation.

      spdlog::debug("Parsed UPDATE_ROWS: pk={}, text_len={}, filters={}",
                   event.primary_key, event.text.length(), event.filters.size());

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

      // Determine text column from config
      std::string text_column;
      if (!table_config_.text_source.column.empty()) {
        text_column = table_config_.text_source.column;
      } else if (!table_config_.text_source.concat.empty()) {
        text_column = table_config_.text_source.concat[0];
      } else {
        text_column = "";
      }

      // Parse rows using rows_parser
      auto rows_opt = ParseDeleteRowsEvent(
          buffer, length, table_meta,
          table_config_.primary_key,
          text_column);

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

      // Extract filters from row data (before image for DELETE)
      event.filters = ExtractFilters(row, table_config_.filters);

      spdlog::debug("Parsed DELETE_ROWS: pk={}, text_len={}, filters={}",
                   event.primary_key, event.text.length(), event.filters.size());

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

      // Check if this affects our target table
      if (IsTableAffectingDDL(query, table_config_.name)) {
        BinlogEvent event;
        event.type = BinlogEventType::DDL;
        event.table_name = table_config_.name;
        event.text = query;  // Store the DDL query
        return event;
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

std::optional<std::string> BinlogReader::ExtractGTID(
    const unsigned char* buffer, unsigned long length) {
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
  uuid_oss << std::hex << std::setfill('0')
           << std::setw(2) << static_cast<int>(sid_ptr[0])
           << std::setw(2) << static_cast<int>(sid_ptr[1])
           << std::setw(2) << static_cast<int>(sid_ptr[2])
           << std::setw(2) << static_cast<int>(sid_ptr[3]) << '-'
           << std::setw(2) << static_cast<int>(sid_ptr[4])
           << std::setw(2) << static_cast<int>(sid_ptr[5]) << '-'
           << std::setw(2) << static_cast<int>(sid_ptr[6])
           << std::setw(2) << static_cast<int>(sid_ptr[7]) << '-'
           << std::setw(2) << static_cast<int>(sid_ptr[8])
           << std::setw(2) << static_cast<int>(sid_ptr[9]) << '-'
           << std::setw(2) << static_cast<int>(sid_ptr[10])
           << std::setw(2) << static_cast<int>(sid_ptr[11])
           << std::setw(2) << static_cast<int>(sid_ptr[12])
           << std::setw(2) << static_cast<int>(sid_ptr[13])
           << std::setw(2) << static_cast<int>(sid_ptr[14])
           << std::setw(2) << static_cast<int>(sid_ptr[15]);

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

// NOLINTNEXTLINE(readability-function-cognitive-complexity)
std::optional<TableMetadata> BinlogReader::ParseTableMapEvent(
    const unsigned char* buffer, unsigned long length) {
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

  if (remaining < db_len + 1) {  // +1 for null terminator
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

  if (remaining < table_len + 1) {
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
          spdlog::warn("Unknown column type {} while parsing metadata",
                      static_cast<int>(type));
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

  spdlog::debug("TABLE_MAP: {}.{} (table_id={}, columns={})",
                metadata.database_name, metadata.table_name,
                metadata.table_id, column_count);

  return metadata;
}

void BinlogReader::FixGtidSetCallback(MYSQL_RPL* rpl, unsigned char* packet_gtid_set) {
  // Copy pre-encoded GTID data into the packet buffer
  auto* encoded_data = static_cast<std::vector<uint8_t>*>(rpl->gtid_set_arg);
  std::memcpy(packet_gtid_set, encoded_data->data(), encoded_data->size());
}

std::optional<std::string> BinlogReader::ExtractQueryString(
    const unsigned char* buffer, unsigned long length) {
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
  if (remaining < db_len + 1) {  // +1 for null terminator
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

bool BinlogReader::IsTableAffectingDDL(const std::string& query,
                                         const std::string& table_name) {
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
  if (std::regex_search(query_upper,
                        std::regex(R"(DROP\s+TABLE\s+(IF\s+EXISTS\s+)?`?)" + table_upper + "`?"))) {
    return true;
  }

  // Check for ALTER TABLE
  if (std::regex_search(query_upper, std::regex("ALTER\\s+TABLE\\s+`?" + table_upper + "`?"))) {
    return true;
  }

  return false;
}

}  // namespace mysql
}  // namespace mygramdb

#endif  // USE_MYSQL
