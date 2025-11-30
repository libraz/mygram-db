/**
 * @file snapshot_scheduler.h
 * @brief Background snapshot scheduler
 */

#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <thread>

#include "config/config.h"

#ifdef USE_MYSQL
namespace mygramdb::mysql {
class BinlogReader;
}  // namespace mygramdb::mysql
#endif

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
   */
  SnapshotScheduler(config::DumpConfig config, TableCatalog* catalog, const config::Config* full_config,
                    std::string dump_dir,
#ifdef USE_MYSQL
                    mysql::BinlogReader* binlog_reader,
#else
                    void* binlog_reader,
#endif
                    std::atomic<bool>& dump_save_in_progress);

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
  void Start();

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
  bool IsRunning() const { return running_; }

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
  std::unique_ptr<std::thread> scheduler_thread_;

#ifdef USE_MYSQL
  mysql::BinlogReader* binlog_reader_;
#else
  void* binlog_reader_;
#endif

  // Reference to dump_save_in_progress flag (shared with DumpHandler for mutual exclusion)
  std::atomic<bool>& dump_save_in_progress_;
};

}  // namespace mygramdb::server
