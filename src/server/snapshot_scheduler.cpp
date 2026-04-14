/**
 * @file snapshot_scheduler.cpp
 * @brief Implementation of SnapshotScheduler
 */

#include "server/snapshot_scheduler.h"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <chrono>
#include <ctime>
#include <filesystem>
#include <iomanip>
#include <sstream>

#include "mysql/binlog_reader_interface.h"
#include "server/table_catalog.h"
#include "storage/dump_format_v1.h"
#include "storage/dump_format_v2.h"
#include "utils/flag_guard.h"
#include "utils/structured_log.h"

namespace mygramdb::server {

constexpr int kShutdownCheckIntervalMs = 1000;  ///< Check for shutdown every second

SnapshotScheduler::SnapshotScheduler(config::DumpConfig config, TableCatalog* catalog,
                                     const config::Config* full_config, std::string dump_dir,
                                     mysql::IBinlogReader* binlog_reader, std::atomic<bool>& dump_save_in_progress)
    : config_(std::move(config)),
      catalog_(catalog),
      full_config_(full_config),
      dump_dir_(std::move(dump_dir)),
      binlog_reader_(binlog_reader),
      dump_save_in_progress_(dump_save_in_progress) {
  if (catalog_ == nullptr) {
    mygram::utils::StructuredLog()
        .Event("server_error")
        .Field("component", "snapshot_scheduler")
        .Field("error", "catalog cannot be null")
        .Error();
  }
}

SnapshotScheduler::~SnapshotScheduler() {
  Stop();
}

void SnapshotScheduler::Start() {
  if (config_.interval_sec <= 0) {
    mygram::utils::StructuredLog().Event("snapshot_scheduler_disabled").Field("reason", "interval_sec <= 0").Info();
    return;
  }

  // Atomically try to set running_ from false to true to prevent TOCTOU race
  bool expected = false;
  if (!running_.compare_exchange_strong(expected, true)) {
    mygram::utils::StructuredLog()
        .Event("server_warning")
        .Field("component", "snapshot_scheduler")
        .Field("type", "already_running")
        .Warn();
    return;
  }

  mygram::utils::StructuredLog()
      .Event("snapshot_scheduler_starting")
      .Field("interval_sec", static_cast<uint64_t>(config_.interval_sec))
      .Field("retain", static_cast<uint64_t>(config_.retain))
      .Info();

  scheduler_thread_ = std::make_unique<std::thread>(&SnapshotScheduler::SchedulerLoop, this);
}

void SnapshotScheduler::Stop() {
  // Use compare_exchange to ensure only one thread performs the stop sequence.
  // Without this, two concurrent Stop() calls could both pass the running_
  // check and double-join the thread, causing std::terminate.
  bool expected = true;
  if (!running_.compare_exchange_strong(expected, false)) {
    return;
  }

  // Wake the scheduler loop so it exits promptly instead of sleeping
  // for up to kShutdownCheckIntervalMs.
  stop_cv_.notify_all();

  mygram::utils::StructuredLog().Event("snapshot_scheduler_stopping").Info();

  if (scheduler_thread_ && scheduler_thread_->joinable()) {
    scheduler_thread_->join();
  }

  mygram::utils::StructuredLog().Event("snapshot_scheduler_stopped").Info();
}

void SnapshotScheduler::SchedulerLoop() {
  const int interval_sec = config_.interval_sec;
  const int check_interval_ms = kShutdownCheckIntervalMs;

  mygram::utils::StructuredLog().Event("snapshot_scheduler_thread_started").Info();

  // Calculate next save time
  auto next_save_time = std::chrono::steady_clock::now() + std::chrono::seconds(interval_sec);

  while (running_) {
    auto now = std::chrono::steady_clock::now();

    // Check if it's time to save
    if (now >= next_save_time) {
      TakeSnapshot();
      CleanupOldSnapshots();

      // Schedule next save
      next_save_time = std::chrono::steady_clock::now() + std::chrono::seconds(interval_sec);
    }

    // Wait for check interval or until Stop() signals the condition variable.
    // This replaces sleep_for so that Stop() can wake the thread immediately
    // instead of blocking for up to kShutdownCheckIntervalMs.
    {
      std::unique_lock<std::mutex> lock(stop_mutex_);
      stop_cv_.wait_for(lock, std::chrono::milliseconds(check_interval_ms),
                        [this] { return !running_.load(std::memory_order_acquire); });
    }
  }

  mygram::utils::StructuredLog().Event("snapshot_scheduler_thread_exiting").Info();
}

void SnapshotScheduler::TakeSnapshot() {
  try {
    // Atomically try to acquire the dump_save_in_progress flag
    // This prevents TOCTOU race between checking and setting the flag
    bool expected = false;
    if (!dump_save_in_progress_.compare_exchange_strong(expected, true)) {
      // Another dump operation (manual or auto) is already in progress
      mygram::utils::StructuredLog()
          .Event("auto_snapshot_skipped")
          .Field("reason", "another DUMP operation is in progress")
          .Info();
      return;
    }

    // Flag successfully acquired, use RAII guard to ensure it's reset on exit
    mygram::utils::AtomicFlagResetGuard dump_save_guard(dump_save_in_progress_);

    // Generate timestamp-based filename
    auto timestamp = std::time(nullptr);
    std::tm tm_buf{};
    localtime_r(&timestamp, &tm_buf);  // Thread-safe version of localtime
    std::ostringstream filename;
    filename << "auto_" << std::put_time(&tm_buf, "%Y%m%d_%H%M%S") << ".dmp";

    std::filesystem::path dump_path = std::filesystem::path(dump_dir_) / filename.str();

    mygram::utils::StructuredLog().Event("snapshot_taking").Field("path", dump_path.string()).Info();

    // Get current GTID
    std::string gtid;
    if (binlog_reader_ != nullptr) {
      gtid = binlog_reader_->GetCurrentGTID();
    }

    // Get dumpable contexts from catalog
    auto dumpable = catalog_->GetDumpableContexts();

    // Perform save using dump API (writes V2 format)
    auto result = storage::dump_v2::WriteDump(dump_path.string(), gtid, *full_config_, dumpable);

    if (result) {
      mygram::utils::StructuredLog().Event("snapshot_completed").Field("path", dump_path.string()).Info();
    } else {
      mygram::utils::StructuredLog()
          .Event("server_error")
          .Field("operation", "snapshot_save")
          .Field("filepath", dump_path.string())
          .Field("error", result.error().message())
          .Error();
    }

  } catch (const std::exception& e) {
    mygram::utils::StructuredLog()
        .Event("server_error")
        .Field("operation", "snapshot_save")
        .Field("type", "exception")
        .Field("error", e.what())
        .Error();
  }
}

void SnapshotScheduler::CleanupOldSnapshots() {
  if (config_.retain <= 0) {
    return;
  }

  try {
    std::filesystem::path dump_path(dump_dir_);

    if (!std::filesystem::exists(dump_path) || !std::filesystem::is_directory(dump_path)) {
      return;
    }

    // Collect all .dmp files with their modification times
    std::vector<std::pair<std::filesystem::path, std::filesystem::file_time_type>> dump_files;

    for (const auto& entry : std::filesystem::directory_iterator(dump_path)) {
      if (entry.is_regular_file() && entry.path().extension() == ".dmp") {
        // Only manage auto-saved files (starting with "auto_")
        if (entry.path().filename().string().rfind("auto_", 0) == 0) {
          dump_files.emplace_back(entry.path(), std::filesystem::last_write_time(entry));
        }
      }
    }

    // Sort by modification time (newest first)
    std::sort(dump_files.begin(), dump_files.end(),
              [](const auto& lhs, const auto& rhs) { return lhs.second > rhs.second; });

    // Delete old files beyond retain count
    const auto retain_count = static_cast<size_t>(config_.retain);
    for (size_t i = retain_count; i < dump_files.size(); ++i) {
      mygram::utils::StructuredLog().Event("snapshot_removing_old").Field("path", dump_files[i].first.string()).Info();
      std::error_code ec;
      std::filesystem::remove(dump_files[i].first, ec);
      if (ec) {
        mygram::utils::StructuredLog()
            .Event("snapshot_cleanup_error")
            .Field("path", dump_files[i].first.string())
            .Field("error", ec.message())
            .Warn();
        // Continue with remaining files
      }
    }

  } catch (const std::exception& e) {
    mygram::utils::StructuredLog()
        .Event("server_error")
        .Field("operation", "snapshot_cleanup")
        .Field("type", "exception")
        .Field("error", e.what())
        .Error();
  }
}

}  // namespace mygramdb::server
