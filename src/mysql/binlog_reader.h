/**
 * @file binlog_reader.h
 * @brief MySQL binlog reader for replication
 */

#pragma once

#ifdef USE_MYSQL

#include "mysql/connection.h"
#include "index/index.h"
#include "storage/document_store.h"
#include "config/config.h"
#include <queue>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <atomic>
#include <memory>

namespace mygramdb {
namespace mysql {

/**
 * @brief Binlog event type
 */
enum class BinlogEventType {
  INSERT,
  UPDATE,
  DELETE,
  UNKNOWN
};

/**
 * @brief Binlog event
 */
struct BinlogEvent {
  BinlogEventType type;
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
  struct Config {
    std::string start_gtid;     // Starting GTID
    size_t queue_size = 10000;  // Maximum queue size
    int reconnect_delay_ms = 1000;
  };

  /**
   * @brief Construct binlog reader
   */
  BinlogReader(Connection& connection,
               index::Index& index,
               storage::DocumentStore& doc_store,
               const config::TableConfig& table_config,
               const Config& config);

  ~BinlogReader();

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

 private:
  Connection& connection_;
  index::Index& index_;
  storage::DocumentStore& doc_store_;
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

  std::string last_error_;

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
   * @brief Update current GTID
   */
  void UpdateCurrentGTID(const std::string& gtid);
};

}  // namespace mysql
}  // namespace mygramdb

#endif  // USE_MYSQL
