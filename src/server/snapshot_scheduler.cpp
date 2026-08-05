/**
 * @file snapshot_scheduler.cpp
 * @brief Implementation of SnapshotScheduler
 */
// Logging is exclusively via mygram::utils::StructuredLog. Direct spdlog usage is prohibited in server code.

#include "server/snapshot_scheduler.h"

#include <algorithm>
#include <cctype>
#include <chrono>
#include <ctime>
#include <exception>
#include <filesystem>
#include <iomanip>
#include <sstream>

#include "mysql/binlog_reader_interface.h"
#include "server/log_field_names.h"
#include "server/replication_pause_counter.h"
#include "server/replication_pause_guard.h"
#include "server/server_stats.h"
#include "server/server_types.h"
#include "server/sync_operation_manager.h"
#include "server/table_catalog.h"
#include "storage/dump_format_v1.h"
#include "storage/dump_format_v2.h"
#include "utils/error.h"
#include "utils/expected.h"
#include "utils/fd_guard.h"
#include "utils/flag_guard.h"
#include "utils/structured_log.h"

namespace mygramdb::server {

constexpr int kShutdownCheckIntervalMs = 1000;  ///< Check for shutdown every second
constexpr auto kOrphanTempSnapshotMaxAge = std::chrono::hours(1);

bool IsDumpTempFile(const std::filesystem::path& path) {
  const std::string filename = path.filename().string();
  const auto marker = filename.find(".dmp.tmp");
  if (marker == std::string::npos) {
    return false;
  }
  const auto suffix_offset = marker + std::string_view(".dmp.tmp").size();
  if (suffix_offset == filename.size()) {
    return true;
  }
  if (filename[suffix_offset] != '.') {
    return false;
  }
  const std::string_view unique_suffix(filename.data() + suffix_offset + 1, filename.size() - suffix_offset - 1);
  const auto separator = unique_suffix.find('.');
  if (separator == std::string_view::npos || separator == 0 || separator + 1 == unique_suffix.size()) {
    return false;
  }
  const auto is_decimal = [](std::string_view value) {
    return std::all_of(value.begin(), value.end(), [](unsigned char ch) { return std::isdigit(ch) != 0; });
  };
  return is_decimal(unique_suffix.substr(0, separator)) && is_decimal(unique_suffix.substr(separator + 1));
}

SnapshotScheduler::SnapshotScheduler(config::DumpConfig config, TableCatalog* catalog,
                                     const config::Config* full_config, std::string dump_dir,
                                     mysql::IBinlogReader* binlog_reader, std::atomic<bool>& dump_save_in_progress,
                                     std::atomic<bool>& replication_paused_for_dump,
                                     replication_pause::Counter* replication_pause_counter,
                                     std::atomic<bool>* dump_load_in_progress, SyncOperationManager* sync_manager,
                                     std::function<bool()> sync_in_progress_checker,
                                     std::atomic<bool>* optimization_in_progress, std::atomic<bool>* shutdown_requested,
                                     DumpProgress* dump_progress, ServerStats* stats)
    : config_(std::move(config)),
      catalog_(catalog),
      full_config_(full_config),
      dump_dir_(std::move(dump_dir)),
      binlog_reader_(binlog_reader),
      dump_save_in_progress_(dump_save_in_progress),
      dump_load_in_progress_(dump_load_in_progress),
      optimization_in_progress_(optimization_in_progress),
      replication_paused_for_dump_(replication_paused_for_dump),
      replication_pause_counter_(replication_pause_counter != nullptr ? replication_pause_counter
                                                                      : &local_replication_pause_counter_),
      sync_manager_(sync_manager),
      sync_in_progress_checker_(std::move(sync_in_progress_checker)),
      shutdown_requested_(shutdown_requested),
      dump_progress_(dump_progress),
      stats_(stats) {
  // Precondition: catalog must be non-null. Enforced by ServerLifecycleManager::InitScheduler,
  // which is the only production caller. Tests must also provide a non-null catalog.
}

SnapshotScheduler::~SnapshotScheduler() {
  Stop();
}

mygram::utils::Expected<void, mygram::utils::Error> SnapshotScheduler::Start() {
  if (config_.interval_sec <= 0) {
    // Orphan cleanup is a dump-directory responsibility, not an auto-snapshot
    // retention feature. Manual DUMP SAVE uses the same atomic temp naming and
    // must still be reclaimed when periodic snapshots are disabled.
    CleanupOldSnapshots();
    mygram::utils::StructuredLog().Event("snapshot_scheduler_disabled").Field("reason", "interval_sec <= 0").Info();
    return {};
  }

  // Hold start_stop_mutex_ across the entire Start sequence so that a
  // concurrent Stop() cannot observe running_ == true and skip joining
  // before scheduler_thread_ has been constructed. SchedulerLoop never
  // acquires this mutex, so there is no risk of self-deadlock.
  std::lock_guard<std::mutex> lock(start_stop_mutex_);

  // Atomically try to set running_ from false to true to prevent TOCTOU race
  bool expected = false;
  if (!running_.compare_exchange_strong(expected, true)) {
    mygram::utils::StructuredLog().Event("snapshot_scheduler_already_running").Warn();
    return {};
  }

  mygram::utils::StructuredLog()
      .Event("snapshot_scheduler_starting")
      .Field("interval_sec", static_cast<uint64_t>(config_.interval_sec))
      .Field("retain", static_cast<uint64_t>(config_.retain))
      .Info();

  try {
    scheduler_thread_ = std::make_unique<std::thread>(&SnapshotScheduler::SchedulerLoop, this);
  } catch (const std::exception& e) {
    running_.store(false, std::memory_order_release);
    return mygram::utils::MakeUnexpected(
        mygram::utils::MakeError(mygram::utils::ErrorCode::kInternalError,
                                 std::string("Failed to start snapshot scheduler thread: ") + e.what()));
  }

  return {};
}

void SnapshotScheduler::Stop() {
  // Hold start_stop_mutex_ across the entire Stop sequence to serialize
  // with Start(). Without this, Stop() could observe running_ == true while
  // Start() is still mid-construction (compare_exchange has succeeded but the
  // thread has not yet been created), leak the not-yet-created thread, and
  // race Start() to completion. SchedulerLoop never acquires this mutex.
  std::lock_guard<std::mutex> lock(start_stop_mutex_);

  // Use compare_exchange to ensure only one thread performs the stop sequence.
  // Without this, two concurrent Stop() calls could both pass the running_
  // check and double-join the thread, causing std::terminate.
  bool expected = true;
  if (!running_.compare_exchange_strong(expected, false)) {
    return;
  }

  // Wake the scheduler loop so it exits promptly instead of sleeping
  // for up to kShutdownCheckIntervalMs.
  //
  // Lock discipline:
  //   - stop_cv_ is associated with stop_mutex_, not start_stop_mutex_.
  //   - Calling notify_all without holding stop_mutex_ is permitted by the
  //     C++ standard ([thread.condition.condvar]); the standard only requires
  //     the wait predicate to be observed under the cv's associated mutex.
  //   - The pattern is intentional: holding start_stop_mutex_ across notify is
  //     required to serialize against a concurrent Start(); holding stop_mutex_
  //     during notify would risk a recursive-acquisition pattern with the wait
  //     predicate that already takes stop_mutex_ inside SchedulerLoop.
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
    if (shutdown_requested_ != nullptr && shutdown_requested_->load(std::memory_order_acquire)) {
      mygram::utils::StructuredLog().Event("auto_snapshot_skipped").Field("reason", "server shutdown").Info();
      return;
    }

    OperationCoordinator::Token operation_token;
    if (sync_manager_ != nullptr) {
      auto& coordinator = sync_manager_->GetOperationCoordinator();
      auto acquired = coordinator.TryAcquire(LongOperation::kAutoSnapshot, "scheduled");
      if (!acquired.has_value()) {
        mygram::utils::StructuredLog()
            .Event("auto_snapshot_skipped")
            .Field("reason", coordinator.DescribeActive() + " is in progress")
            .Info();
        return;
      }
      operation_token = std::move(*acquired);
    }

    // Atomic test-and-set with scope-bound release via OperationGuard::TryAcquire.
    // Prevents TOCTOU race between checking and setting the flag; same contract
    // as HandleDumpSave / HandleDumpLoad in DumpHandler.
    auto dump_save_guard = mygram::utils::OperationGuard::TryAcquire(dump_save_in_progress_);
    if (!dump_save_guard.engaged()) {
      // Another dump operation (manual or auto) is already in progress.
      mygram::utils::StructuredLog()
          .Event("auto_snapshot_skipped")
          .Field("reason", "another DUMP operation is in progress")
          .Info();
      return;
    }
    if (dump_load_in_progress_ != nullptr && dump_load_in_progress_->load(std::memory_order_acquire)) {
      mygram::utils::StructuredLog().Event("auto_snapshot_skipped").Field("reason", "DUMP LOAD is in progress").Info();
      return;
    }
    if (optimization_in_progress_ != nullptr && optimization_in_progress_->load(std::memory_order_acquire)) {
      mygram::utils::StructuredLog().Event("auto_snapshot_skipped").Field("reason", "OPTIMIZE is in progress").Info();
      return;
    }
    const bool sync_in_progress = sync_in_progress_checker_  ? sync_in_progress_checker_()
                                  : sync_manager_ != nullptr ? sync_manager_->IsAnySyncing()
                                                             : false;
    if (sync_in_progress) {
      mygram::utils::StructuredLog().Event("auto_snapshot_skipped").Field("reason", "SYNC is in progress").Info();
      return;
    }

    // Generate timestamp-based filename
    auto timestamp = std::time(nullptr);
    std::tm tm_buf{};
    localtime_r(&timestamp, &tm_buf);  // Thread-safe version of localtime
    std::ostringstream filename;
    filename << "auto_" << std::put_time(&tm_buf, "%Y%m%d_%H%M%S") << ".dmp";

    std::filesystem::path dump_path = std::filesystem::path(dump_dir_) / filename.str();

    if (dump_progress_ != nullptr) {
      dump_progress_->Reset(DumpStatus::SAVING, dump_path.string(), catalog_->GetTables().size());
    }

    mygram::utils::StructuredLog()
        .Event("snapshot_taking")
        .Field(log_fields::kFieldFilepath, dump_path.string())
        .Info();

    std::string gtid;
#ifdef USE_MYSQL
    replication_pause::Guard replication_pause(binlog_reader_, *replication_pause_counter_,
                                               replication_paused_for_dump_, shutdown_requested_, "snapshot");
    const auto pause_result = replication_pause.Pause();
    gtid = pause_result.drained_gtid;
    if (pause_result.engaged) {
      mygram::utils::StructuredLog()
          .Event("replication_paused_for_dump")
          .Field("operation", "snapshot")
          .Field("filepath", dump_path.string())
          .Field("auto_resume", "true")
          .Field("first_pauser", pause_result.first_pauser)
          .Info();
    }
    if (binlog_reader_ != nullptr && gtid.empty()) {
      mygram::utils::StructuredLog()
          .Event("auto_snapshot_skipped")
          .Field("reason", "replication GTID is empty")
          .Field(log_fields::kFieldFilepath, dump_path.string())
          .Error();
      if (dump_progress_ != nullptr) {
        dump_progress_->Fail("Scheduled snapshot skipped: replication GTID is empty");
      }
      if (stats_ != nullptr) {
        stats_->IncrementDumpFailureAuto();
      }
      return;
    }
#endif

    // Get dumpable contexts from catalog
    auto dumpable = catalog_->GetDumpableContexts();
    std::string source_server_uuid;
#ifdef USE_MYSQL
    if (binlog_reader_ != nullptr) {
      source_server_uuid = binlog_reader_->GetSourceServerUUID();
    }
#endif

    // Perform save using dump API (writes V2 format)
    auto result = storage::dump_v2::WriteDump(
        dump_path.string(), gtid, *full_config_, dumpable, nullptr, nullptr,
        [this](const std::string& table_name, size_t tables_processed) {
          if (dump_progress_ != nullptr) {
            dump_progress_->UpdateTable(table_name, tables_processed);
          }
        },
        storage::dump_v2::RestoreLimits{
            static_cast<uint64_t>(full_config_->dump.restore_memory_budget_mb) * 1024ULL * 1024ULL,
            static_cast<uint64_t>(full_config_->dump.restore_max_section_mb) * 1024ULL * 1024ULL},
        source_server_uuid);

    if (result) {
      mygram::utils::StructuredLog()
          .Event("snapshot_completed")
          .Field(log_fields::kFieldFilepath, dump_path.string())
          .Info();
      if (dump_progress_ != nullptr) {
        dump_progress_->Complete(dump_path.string());
      }
      if (stats_ != nullptr) {
        stats_->RecordDumpSuccess();
      }
    } else {
      mygram::utils::StructuredLog()
          .Event("snapshot_save_failed")
          .Field(log_fields::kFieldFilepath, dump_path.string())
          .FieldError(result.error())
          .Error();
      if (dump_progress_ != nullptr) {
        dump_progress_->Fail("Scheduled snapshot failed: " + result.error().message());
      }
      if (stats_ != nullptr) {
        stats_->IncrementDumpFailureAuto();
      }
    }

    // restore_replication runs here (when in scope), resuming replication
    // before dump_save_guard releases the dump_save_in_progress flag.
  } catch (const std::exception& e) {
    mygram::utils::StructuredLog().Event("snapshot_save_exception").Field(log_fields::kFieldError, e.what()).Error();
    if (dump_progress_ != nullptr) {
      dump_progress_->Fail("Scheduled snapshot exception: " + std::string(e.what()));
    }
    if (stats_ != nullptr) {
      stats_->IncrementDumpFailureAuto();
    }
  }
}

void SnapshotScheduler::CleanupOldSnapshots() {
  try {
    std::filesystem::path dump_path(dump_dir_);

    if (!std::filesystem::exists(dump_path) || !std::filesystem::is_directory(dump_path)) {
      return;
    }

    // Collect all .dmp files with their modification times
    std::vector<std::pair<std::filesystem::path, std::filesystem::file_time_type>> dump_files;
    std::vector<std::filesystem::path> old_temp_files;
    const auto now = std::filesystem::file_time_type::clock::now();

    for (const auto& entry : std::filesystem::directory_iterator(dump_path)) {
      if (entry.is_regular_file() && entry.path().extension() == ".dmp") {
        // Only manage auto-saved files (starting with "auto_")
        if (entry.path().filename().string().rfind("auto_", 0) == 0) {
          dump_files.emplace_back(entry.path(), std::filesystem::last_write_time(entry));
        }
      } else if (entry.is_regular_file() && IsDumpTempFile(entry.path())) {
        const auto last_write_time = std::filesystem::last_write_time(entry);
        if (now - last_write_time >= kOrphanTempSnapshotMaxAge) {
          old_temp_files.push_back(entry.path());
        }
      }
    }

    // Sort by modification time (newest first)
    std::sort(dump_files.begin(), dump_files.end(),
              [](const auto& lhs, const auto& rhs) { return lhs.second > rhs.second; });

    // Delete old files beyond retain count
    if (config_.retain > 0) {
      const auto retain_count = static_cast<size_t>(config_.retain);
      for (size_t i = retain_count; i < dump_files.size(); ++i) {
        mygram::utils::StructuredLog()
            .Event("snapshot_removing_old")
            .Field(log_fields::kFieldFilepath, dump_files[i].first.string())
            .Info();
        std::error_code ec;
        std::filesystem::remove(dump_files[i].first, ec);
        if (ec) {
          mygram::utils::StructuredLog()
              .Event("snapshot_cleanup_error")
              .Field(log_fields::kFieldFilepath, dump_files[i].first.string())
              .Field(log_fields::kFieldError, ec.message())
              .Warn();
          // Continue with remaining files
        }
      }
    }

    for (const auto& temp_file : old_temp_files) {
      mygram::utils::StructuredLog()
          .Event("snapshot_removing_orphan_temp")
          .Field(log_fields::kFieldFilepath, temp_file.string())
          .Info();
      std::error_code ec;
      std::filesystem::remove(temp_file, ec);
      if (ec) {
        mygram::utils::StructuredLog()
            .Event("snapshot_cleanup_error")
            .Field(log_fields::kFieldFilepath, temp_file.string())
            .Field(log_fields::kFieldError, ec.message())
            .Warn();
      }
    }

  } catch (const std::exception& e) {
    mygram::utils::StructuredLog().Event("snapshot_cleanup_exception").Field(log_fields::kFieldError, e.what()).Error();
  }
}

}  // namespace mygramdb::server
