/**
 * @file snapshot_scheduler.h
 * @brief Background snapshot scheduler
 */

#pragma once

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

#include "config/config.h"
#include "server/replication_pause_counter.h"
#include "utils/error.h"
#include "utils/expected.h"

namespace mygramdb::mysql {
class IBinlogReader;
}  // namespace mygramdb::mysql

namespace mygramdb::server {

// Forward declarations
class TableCatalog;

/**
 * @brief Background snapshot scheduler
 *
 * This class periodically creates snapshots and cleans up old files.
 * It is isolated from server lifecycle for independent testing.
 *
 * Key responsibilities:
 * - Periodic snapshot creation
 * - Cleanup of old snapshot files
 * - GTID tracking (if using MySQL replication)
 * - Error handling and logging
 *
 * Design principles:
 * - Isolated from TcpServer lifecycle
 * - Can be tested with mocked time
 * - Clear ownership of dump files
 * - Thread-safe operations
 */
class SnapshotScheduler {
 public:
  /**
   * @brief Construct a SnapshotScheduler
   * @param config Dump configuration
   * @param catalog Table catalog for accessing tables
   * @param full_config Full configuration (for snapshot metadata)
   * @param dump_dir Directory for snapshot files
   * @param binlog_reader Optional binlog reader for GTID tracking
   * @param dump_save_in_progress Reference to DUMP SAVE flag for mutual exclusion with manual DUMP SAVE
   * @param replication_paused_for_dump Reference to replication-paused flag, asserted while
   *        a snapshot is in progress so that manual REPLICATION START is rejected during the dump.
   */
  SnapshotScheduler(config::DumpConfig config, TableCatalog* catalog, const config::Config* full_config,
                    std::string dump_dir, mysql::IBinlogReader* binlog_reader, std::atomic<bool>& dump_save_in_progress,
                    std::atomic<bool>& replication_paused_for_dump,
                    replication_pause::Counter* replication_pause_counter = nullptr);

  // Disable copy and move
  SnapshotScheduler(const SnapshotScheduler&) = delete;
  SnapshotScheduler& operator=(const SnapshotScheduler&) = delete;
  SnapshotScheduler(SnapshotScheduler&&) = delete;
  SnapshotScheduler& operator=(SnapshotScheduler&&) = delete;

  ~SnapshotScheduler();

  /**
   * @brief Start the scheduler
   *
   * Starts the background thread that periodically creates snapshots.
   */
  mygram::utils::Expected<void, mygram::utils::Error> Start();

  /**
   * @brief Stop the scheduler
   *
   * Stops the background thread and waits for it to finish.
   */
  void Stop();

  /**
   * @brief Check if scheduler is running
   * @return true if running
   */
  bool IsRunning() const { return running_.load(std::memory_order_acquire); }

 private:
  /**
   * @brief Scheduler loop (runs in separate thread)
   */
  void SchedulerLoop();

  /**
   * @brief Take a snapshot
   */
  void TakeSnapshot();

  /**
   * @brief Clean up old snapshots based on retention policy
   */
  void CleanupOldSnapshots();

  config::DumpConfig config_;
  TableCatalog* catalog_;
  const config::Config* full_config_;
  std::string dump_dir_;

  std::atomic<bool> running_{false};
  std::mutex stop_mutex_;
  std::condition_variable stop_cv_;
  std::unique_ptr<std::thread> scheduler_thread_;

  // Serializes Start()/Stop() so concurrent calls cannot race on
  // scheduler_thread_ creation/join. Without this, a Stop() that observes
  // running_ == true between Start()'s compare_exchange and the thread's
  // construction would skip the join and leak the thread.
  std::mutex start_stop_mutex_;

  mysql::IBinlogReader* binlog_reader_;

  // Reference to dump_save_in_progress flag (shared with DumpHandler for mutual exclusion)
  std::atomic<bool>& dump_save_in_progress_;

  // Reference to replication-paused flag (shared with DumpHandler/TcpServer).
  // Asserted while a scheduled snapshot is in progress so that concurrent
  // REPLICATION START requests are rejected, mirroring the behavior of
  // manual DUMP SAVE/LOAD.
  std::atomic<bool>& replication_paused_for_dump_;
  replication_pause::Counter local_replication_pause_counter_;
  replication_pause::Counter* replication_pause_counter_;
};

}  // namespace mygramdb::server
