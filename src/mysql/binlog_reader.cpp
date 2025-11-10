/**
 * @file binlog_reader.cpp
 * @brief Binlog reader implementation
 */

#include "mysql/binlog_reader.h"

#ifdef USE_MYSQL

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
    last_error_ = "Already running";
    return false;
  }

  if (!connection_.IsConnected()) {
    last_error_ = "MySQL connection not established";
    spdlog::error(last_error_);
    return false;
  }

  should_stop_ = false;
  running_ = true;

  // Start worker thread first
  worker_thread_ = std::make_unique<std::thread>(
      &BinlogReader::WorkerThreadFunc, this);

  // Start reader thread
  reader_thread_ = std::make_unique<std::thread>(
      &BinlogReader::ReaderThreadFunc, this);

  spdlog::info("Binlog reader started from GTID: {}", current_gtid_);
  return true;
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

size_t BinlogReader::GetQueueSize() const {
  std::lock_guard<std::mutex> lock(queue_mutex_);
  return event_queue_.size();
}

void BinlogReader::ReaderThreadFunc() {
  spdlog::info("Binlog reader thread started");

  while (!should_stop_) {
    // TODO: Implement actual binlog reading using MySQL replication API
    // For now, this is a placeholder that demonstrates the queue mechanism
    
    // Simulate reading binlog events
    // In real implementation, this would:
    // 1. Connect to MySQL as replication slave
    // 2. Send COM_BINLOG_DUMP command
    // 3. Parse binlog events (ROW format)
    // 4. Extract table name, operation type, row data
    // 5. Create BinlogEvent and push to queue
    
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    // Example event creation (placeholder)
    if (false) {  // Disabled for now
      BinlogEvent event;
      event.type = BinlogEventType::INSERT;
      event.table_name = table_config_.name;
      event.primary_key = "test_pk";
      event.text = "test text";
      event.gtid = current_gtid_;
      
      PushEvent(event);
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

        // For index update, we need old text from binlog event
        // In real implementation, binlog UPDATE event contains both old and new values
        // For now, we'll do a simple remove+add
        // TODO: Improve this with actual old values from binlog
        std::string new_normalized = utils::NormalizeText(event.text, true, "keep", true);

        // Simplified: Just update with new text (index handles it)
        // Note: This requires storing old text somewhere or getting from binlog
        // For MVP, we skip old text handling
        spdlog::debug("UPDATE: pk={} (simplified)", event.primary_key);
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

        // For deletion, we need the text from binlog event
        // TODO: Get old text from binlog DELETE event
        // For now, use text from event if available
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

}  // namespace mysql
}  // namespace mygramdb

#endif  // USE_MYSQL
