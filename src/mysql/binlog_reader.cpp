/**
 * @file binlog_reader.cpp
 * @brief Binlog reader implementation
 */

#include "mysql/binlog_reader.h"

#ifdef USE_MYSQL

#include "mysql/binlog_event_types.h"
#include "mysql/binlog_util.h"
#include "mysql/rows_parser.h"
#include "storage/gtid_state.h"
#include "utils/string_utils.h"
#include <spdlog/spdlog.h>
#include <chrono>

namespace mygramdb {
namespace mysql {

BinlogReader::BinlogReader(Connection& connection,
                           index::Index& index,
                           storage::DocumentStore& doc_store,
                           const config::TableConfig& table_config,
                           const Config& config)
    : connection_(connection),
      index_(index),
      doc_store_(doc_store),
      table_config_(table_config),
      config_(config) {
  current_gtid_ = config.start_gtid;
}

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

  // Try to reconnect if connection was lost
  if (!connection_.Ping()) {
    spdlog::warn("MySQL connection lost, attempting to reconnect...");
    if (!connection_.Reconnect()) {
      last_error_ = "Failed to reconnect to MySQL: " + connection_.GetLastError();
      spdlog::error("Cannot start binlog reader: {}", last_error_);
      return false;
    }
    spdlog::info("Reconnected to MySQL successfully");
  }

  // Check if GTID mode is enabled
  if (!connection_.IsGTIDModeEnabled()) {
    last_error_ = "GTID mode is not enabled on MySQL server. "
                  "Please enable GTID mode (gtid_mode=ON) for binlog replication.";
    spdlog::error("Cannot start binlog reader: {}", last_error_);
    return false;
  }

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

  running_ = false;
  spdlog::info("Binlog reader stopped. Processed {} events", 
               processed_events_.load());
}

std::string BinlogReader::GetCurrentGTID() const {
  std::lock_guard<std::mutex> lock(gtid_mutex_);
  return current_gtid_;
}

void BinlogReader::SetCurrentGTID(const std::string& gtid) {
  std::lock_guard<std::mutex> lock(gtid_mutex_);
  current_gtid_ = gtid;
  spdlog::info("Set replication GTID to: {}", gtid);
}

size_t BinlogReader::GetQueueSize() const {
  std::lock_guard<std::mutex> lock(queue_mutex_);
  return event_queue_.size();
}

void BinlogReader::ReaderThreadFunc() {
  spdlog::info("Binlog reader thread started");

  // Initialize MYSQL_RPL structure for binlog reading
  MYSQL_RPL rpl{};
  rpl.file_name_length = 0;    // 0 means start from current position
  rpl.file_name = nullptr;
  rpl.start_position = 4;      // Skip magic number at start of binlog
  rpl.server_id = 0;           // 0 means use default
  rpl.flags = MYSQL_RPL_GTID | MYSQL_RPL_SKIP_HEARTBEAT;  // Use GTID mode

  // Set GTID set if we have a starting GTID
  std::string gtid_set;
  {
    std::lock_guard<std::mutex> lock(gtid_mutex_);
    if (!current_gtid_.empty()) {
      gtid_set = current_gtid_;
      spdlog::info("Starting binlog replication from GTID: {}", gtid_set);
    }
  }

  // Open binlog stream
  if (mysql_binlog_open(connection_.GetHandle(), &rpl) != 0) {
    last_error_ = "Failed to open binlog stream: " + connection_.GetLastError();
    spdlog::error("{}", last_error_);
    should_stop_ = true;
    running_ = false;
    return;
  }

  spdlog::info("Binlog stream opened successfully");

  // Read binlog events
  while (!should_stop_) {
    // Fetch next binlog event
    int result = mysql_binlog_fetch(connection_.GetHandle(), &rpl);

    if (result != 0) {
      last_error_ = "Failed to fetch binlog event: " + connection_.GetLastError();
      spdlog::error("{}", last_error_);
      break;
    }

    // Check if we have data
    if (rpl.size == 0 || rpl.buffer == nullptr) {
      // No data available, wait a bit
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      continue;
    }

    // Parse the binlog event
    auto event_opt = ParseBinlogEvent(rpl.buffer, rpl.size);
    if (event_opt) {
      PushEvent(event_opt.value());
    }
  }

  // Close binlog stream
  mysql_binlog_close(connection_.GetHandle(), &rpl);
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
  std::lock_guard<std::mutex> lock(gtid_mutex_);
  current_gtid_ = gtid;
}

void BinlogReader::WriteGTIDToStateFile(const std::string& gtid) {
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

std::optional<BinlogEvent> BinlogReader::ParseBinlogEvent(
    const unsigned char* buffer, unsigned long length) {
  if (!buffer || length < 19) {
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
      if (!table_meta) {
        spdlog::warn("WRITE_ROWS event for unknown table_id {}", table_id);
        return std::nullopt;
      }

      // Parse rows using rows_parser
      // Determine text column from config
      std::string text_column = !table_config_.text_source.column.empty()
                                   ? table_config_.text_source.column
                                   : (table_config_.text_source.concat.empty()
                                         ? ""
                                         : table_config_.text_source.concat[0]);

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
      if (!table_meta) {
        spdlog::warn("UPDATE_ROWS event for unknown table_id {}", table_id);
        return std::nullopt;
      }

      // Determine text column from config
      std::string text_column = !table_config_.text_source.column.empty()
                                   ? table_config_.text_source.column
                                   : (table_config_.text_source.concat.empty()
                                         ? ""
                                         : table_config_.text_source.concat[0]);

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
      if (!table_meta) {
        spdlog::warn("DELETE_ROWS event for unknown table_id {}", table_id);
        return std::nullopt;
      }

      // Determine text column from config
      std::string text_column = !table_config_.text_source.column.empty()
                                   ? table_config_.text_source.column
                                   : (table_config_.text_source.concat.empty()
                                         ? ""
                                         : table_config_.text_source.concat[0]);

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

    case MySQLBinlogEventType::QUERY_EVENT:
      // DDL statements (CREATE, ALTER, DROP, etc.)
      // Not needed for row-based replication
      return std::nullopt;

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
  if (!buffer || length < 42) {
    // GTID event minimum size
    return std::nullopt;
  }

  // GTID event format (after 19-byte header):
  // commit_flag (1 byte)
  // sid (16 bytes, UUID)
  // gno (8 bytes, transaction number)

  // Skip header (19 bytes) and commit_flag (1 byte)
  const unsigned char* sid_ptr = buffer + 20;

  // Format UUID as string
  char uuid_str[37];
  snprintf(uuid_str, sizeof(uuid_str),
           "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
           sid_ptr[0], sid_ptr[1], sid_ptr[2], sid_ptr[3],
           sid_ptr[4], sid_ptr[5], sid_ptr[6], sid_ptr[7],
           sid_ptr[8], sid_ptr[9], sid_ptr[10], sid_ptr[11],
           sid_ptr[12], sid_ptr[13], sid_ptr[14], sid_ptr[15]);

  // Extract GNO (8 bytes, little-endian)
  const unsigned char* gno_ptr = sid_ptr + 16;
  uint64_t gno = 0;
  for (int i = 0; i < 8; i++) {
    gno |= (uint64_t)gno_ptr[i] << (i * 8);
  }

  // Format as "UUID:GNO"
  std::string gtid = std::string(uuid_str) + ":" + std::to_string(gno);
  return gtid;
}

std::optional<TableMetadata> BinlogReader::ParseTableMapEvent(
    const unsigned char* buffer, unsigned long length) {
  if (!buffer || length < 8) {
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

}  // namespace mysql
}  // namespace mygramdb

#endif  // USE_MYSQL
