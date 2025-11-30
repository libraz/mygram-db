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

#include "server/table_catalog.h"
#include "storage/dump_format_v1.h"
#include "utils/structured_log.h"

#ifdef USE_MYSQL
#include "mysql/binlog_reader.h"
#endif

namespace mygramdb::server {

namespace {
/**
 * @brief RAII guard for atomic boolean flags
 *
 * Automatically sets flag to true on construction and resets to false on destruction.
 * Exception-safe: ensures flag is always reset even if exceptions are thrown.
 */
class FlagGuard {
 public:
  explicit FlagGuard(std::atomic<bool>& flag) : flag_(flag) { flag_ = true; }
  ~FlagGuard() { flag_ = false; }

  // Non-copyable and non-movable
  FlagGuard(const FlagGuard&) = delete;
  FlagGuard& operator=(const FlagGuard&) = delete;
  FlagGuard(FlagGuard&&) = delete;
  FlagGuard& operator=(FlagGuard&&) = delete;

 private:
  std::atomic<bool>& flag_;
};
}  // namespace

SnapshotScheduler::SnapshotScheduler(config::DumpConfig config, TableCatalog* catalog,
                                     const config::Config* full_config, std::string dump_dir,
#ifdef USE_MYSQL
                                     mysql::BinlogReader* binlog_reader,
#else
                                     void* binlog_reader,
#endif
                                     std::atomic<bool>& dump_save_in_progress)
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
  if (running_) {
    mygram::utils::StructuredLog()
        .Event("server_warning")
        .Field("component", "snapshot_scheduler")
        .Field("type", "already_running")
        .Warn();
    return;
  }

  if (config_.interval_sec <= 0) {
    spdlog::info("SnapshotScheduler disabled (interval_sec <= 0)");
    return;
  }

  spdlog::info("Starting SnapshotScheduler (interval: {}s, retain: {})", config_.interval_sec, config_.retain);

  running_ = true;
  scheduler_thread_ = std::make_unique<std::thread>(&SnapshotScheduler::SchedulerLoop, this);
}

void SnapshotScheduler::Stop() {
  if (!running_) {
    return;
  }

  spdlog::info("Stopping SnapshotScheduler...");
  running_ = false;

  if (scheduler_thread_ && scheduler_thread_->joinable()) {
    scheduler_thread_->join();
  }

  spdlog::info("SnapshotScheduler stopped");
}

void SnapshotScheduler::SchedulerLoop() {
  const int interval_sec = config_.interval_sec;
  const int check_interval_ms = 1000;  // Check for shutdown every second

  spdlog::info("SnapshotScheduler thread started");

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

    // Sleep for check interval
    std::this_thread::sleep_for(std::chrono::milliseconds(check_interval_ms));
  }

  spdlog::info("SnapshotScheduler thread exiting");
}

void SnapshotScheduler::TakeSnapshot() {
  try {
    // Check if manual DUMP SAVE is in progress (mutual exclusion)
    if (dump_save_in_progress_.load()) {
      spdlog::info("Auto-snapshot skipped: manual DUMP SAVE is in progress");
      return;
    }

    // Set dump_save_in_progress flag (RAII guard ensures it's cleared even on exceptions)
    // This prevents manual DUMP SAVE from running during auto-dump
    FlagGuard dump_save_guard(dump_save_in_progress_);

    // Generate timestamp-based filename
    auto timestamp = std::time(nullptr);
    std::tm tm_buf{};
    localtime_r(&timestamp, &tm_buf);  // Thread-safe version of localtime
    std::ostringstream filename;
    filename << "auto_" << std::put_time(&tm_buf, "%Y%m%d_%H%M%S") << ".dmp";

    std::filesystem::path dump_path = std::filesystem::path(dump_dir_) / filename.str();

    spdlog::info("Taking snapshot: {}", dump_path.string());

    // Get current GTID
    std::string gtid;
#ifdef USE_MYSQL
    if (binlog_reader_ != nullptr) {
      auto* reader = static_cast<mysql::BinlogReader*>(binlog_reader_);
      gtid = reader->GetCurrentGTID();
    }
#endif

    // Get dumpable contexts from catalog
    auto dumpable = catalog_->GetDumpableContexts();

    // Perform save using dump_v1 API
    auto result = storage::dump_v1::WriteDumpV1(dump_path.string(), gtid, *full_config_, dumpable);

    if (result) {
      spdlog::info("Snapshot completed successfully: {}", dump_path.string());
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
      spdlog::info("Removing old snapshot file: {}", dump_files[i].first.string());
      std::filesystem::remove(dump_files[i].first);
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
