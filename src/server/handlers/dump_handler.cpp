/**
 * @file dump_handler.cpp
 * @brief Handler for dump-related commands
 */

#include "server/handlers/dump_handler.h"

#include <spdlog/spdlog.h>
#include <sys/stat.h>

#include <array>
#include <cstdlib>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <sstream>

#include "server/sync_operation_manager.h"
#include "storage/dump_format_v1.h"
#include "utils/structured_log.h"

#ifdef USE_MYSQL
#include "mysql/binlog_reader_interface.h"
#endif

namespace mygramdb::server {

namespace {
constexpr int kIpAddressBufferSize = 256;
constexpr size_t kGtidPrefixLength = 7;  // Length of "gtid":"

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

std::string DumpHandler::Handle(const query::Query& query, ConnectionContext& conn_ctx) {
  (void)conn_ctx;  // Unused for dump commands

  switch (query.type) {
    case query::QueryType::DUMP_SAVE:
      return HandleDumpSave(query);
    case query::QueryType::DUMP_LOAD:
      return HandleDumpLoad(query);
    case query::QueryType::DUMP_VERIFY:
      return HandleDumpVerify(query);
    case query::QueryType::DUMP_INFO:
      return HandleDumpInfo(query);
    case query::QueryType::DUMP_STATUS:
      return HandleDumpStatus();
    default:
      return ResponseFormatter::FormatError("Invalid query type for DumpHandler");
  }
}

std::string DumpHandler::HandleDumpSave(const query::Query& query) {
#ifdef USE_MYSQL
  // Check if GTID is set (required for consistent dump)
  std::string current_gtid;
  if (ctx_.binlog_reader != nullptr) {
    current_gtid = ctx_.binlog_reader->GetCurrentGTID();
    if (current_gtid.empty()) {
      return ResponseFormatter::FormatError(
          "Cannot save dump without GTID position. "
          "Please run SYNC command first to establish initial position.");
    }
  }

  // Block if any table is currently syncing
  if (ctx_.sync_manager != nullptr) {
    std::vector<std::string> syncing_tables;
    if (ctx_.sync_manager->GetSyncingTablesIfAny(syncing_tables)) {
      std::ostringstream oss;
      oss << "Cannot save dump while SYNC is in progress for tables:";
      for (const auto& table : syncing_tables) {
        oss << " " << table;
      }
      return ResponseFormatter::FormatError(oss.str());
    }
  }
#endif

  // Check if DUMP LOAD is in progress (block DUMP SAVE)
  if (ctx_.dump_load_in_progress.load()) {
    return ResponseFormatter::FormatError(
        "Cannot save dump while DUMP LOAD is in progress. "
        "Please wait for load to complete.");
  }

  // Check if another DUMP SAVE is in progress (block concurrent saves)
  if (ctx_.dump_save_in_progress.load()) {
    return ResponseFormatter::FormatError(
        "Cannot save dump while another DUMP SAVE is in progress. "
        "Please wait for current save to complete or use DUMP STATUS to check progress.");
  }

  // Check if full_config is available
  if (ctx_.full_config == nullptr) {
    std::string error_msg = "Cannot save dump: server configuration is not available";
    mygram::utils::StructuredLog()
        .Event("server_error")
        .Field("operation", "dump_save")
        .Field("reason", "config_not_available")
        .Error();
    return ResponseFormatter::FormatError(error_msg);
  }

  // Determine filepath
  std::string filepath;
  if (!query.filepath.empty()) {
    filepath = query.filepath;

    // Security check: reject relative paths containing "./" or "../"
    // Only allow: simple filename (no '/') or absolute path (starts with '/')
    if (filepath.find("./") != std::string::npos || filepath.find("../") != std::string::npos) {
      return ResponseFormatter::FormatError(
          "Invalid filepath: relative paths with './' or '../' are not allowed. "
          "Use a simple filename (saved to dump directory) or an absolute path.");
    }

    // Prepend dump_dir only for simple filenames (no path separator)
    if (filepath.find('/') == std::string::npos) {
      filepath = ctx_.dump_dir + "/" + filepath;
    }

    // For absolute paths, validate they're within dump_dir for security
    if (filepath[0] == '/') {
      try {
        std::filesystem::path canonical = std::filesystem::weakly_canonical(filepath);
        std::filesystem::path dump_canonical = std::filesystem::weakly_canonical(ctx_.dump_dir);
        auto rel = canonical.lexically_relative(dump_canonical);
        if (rel.empty() || rel.string().substr(0, 2) == "..") {
          return ResponseFormatter::FormatError("Invalid filepath: absolute path must be within dump directory (" +
                                                ctx_.dump_dir + ")");
        }
      } catch (const std::exception& e) {
        return ResponseFormatter::FormatError(std::string("Invalid filepath: ") + e.what());
      }
    }
  } else {
    auto now = std::time(nullptr);
    std::tm tm_buf{};
    localtime_r(&now, &tm_buf);  // Thread-safe version of localtime
    std::array<char, kIpAddressBufferSize> buf{};
    std::strftime(buf.data(), buf.size(), "dump_%Y%m%d_%H%M%S.dmp", &tm_buf);
    filepath = ctx_.dump_dir + "/" + std::string(buf.data());
  }

  // Set flag to indicate save is starting
  ctx_.dump_save_in_progress = true;

  // Initialize progress tracking and run async if progress tracking is available
  if (ctx_.dump_progress != nullptr) {
    mygram::utils::StructuredLog()
        .Event("dump_save_started")
        .Field("filepath", filepath)
        .Field("mode", "async")
        .Field("tables", static_cast<uint64_t>(ctx_.table_contexts.size()))
        .Info();

    // Join any previous worker thread
    ctx_.dump_progress->JoinWorker();
    ctx_.dump_progress->Reset(DumpStatus::SAVING, filepath, ctx_.table_contexts.size());

    // Start background worker thread
    ctx_.dump_progress->worker_thread = std::make_unique<std::thread>([this, filepath]() { DumpSaveWorker(filepath); });

    // Return immediately with started message (async mode)
    return "OK DUMP_STARTED " + filepath + "\r\nUse DUMP STATUS to monitor progress";
  }

  // Fallback: run synchronously if no progress tracking available (e.g., in tests)
  mygram::utils::StructuredLog()
      .Event("dump_save_started")
      .Field("filepath", filepath)
      .Field("mode", "sync")
      .Field("tables", static_cast<uint64_t>(ctx_.table_contexts.size()))
      .Info();
  DumpSaveWorker(filepath);

  // Check result and return appropriate response (sync mode)
  if (!ctx_.dump_save_in_progress.load()) {
    // Worker completed successfully (it resets the flag)
    return "OK SAVED " + filepath;
  }
  // This shouldn't happen in sync mode, but handle it gracefully
  return ResponseFormatter::FormatError("Dump save failed: unexpected state");
}

void DumpHandler::DumpSaveWorker(const std::string& filepath) {
  bool replication_was_running = false;

#ifdef USE_MYSQL
  // Get current GTID first (before stopping replication)
  std::string gtid;
  if (ctx_.binlog_reader != nullptr) {
    gtid = ctx_.binlog_reader->GetCurrentGTID();
  }

  // Check if replication is running and stop it
  if (ctx_.binlog_reader != nullptr) {
    replication_was_running = ctx_.binlog_reader->IsRunning();

    if (replication_was_running) {
      ctx_.binlog_reader->Stop();
      ctx_.replication_paused_for_dump = true;
      mygram::utils::StructuredLog()
          .Event("replication_paused_for_dump")
          .Field("operation", "dump_save")
          .Field("gtid", gtid)
          .Field("filepath", filepath)
          .Field("auto_resume", "true")
          .Info();
    }
  }
#else
  std::string gtid;
#endif

  // Convert table_contexts to format expected by dump_v1::WriteDumpV1
  std::unordered_map<std::string, std::pair<index::Index*, storage::DocumentStore*>> converted_contexts;
  size_t table_index = 0;
  for (const auto& [table_name, table_ctx] : ctx_.table_contexts) {
    converted_contexts[table_name] = {table_ctx->index.get(), table_ctx->doc_store.get()};

    // Update progress
    if (ctx_.dump_progress != nullptr) {
      ctx_.dump_progress->UpdateTable(table_name, table_index);
    }
    ++table_index;
  }

  // Call dump_v1 API
  mygram::utils::StructuredLog()
      .Event("dump_save_write_starting")
      .Field("filepath", filepath)
      .Field("gtid", gtid)
      .Field("tables", static_cast<uint64_t>(converted_contexts.size()))
      .Info();

  auto result = storage::dump_v1::WriteDumpV1(filepath, gtid, *ctx_.full_config, converted_contexts);

  mygram::utils::StructuredLog()
      .Event("dump_save_write_finished")
      .Field("filepath", filepath)
      .Field("success", result.has_value() ? "true" : "false")
      .Field("error", result.has_value() ? "" : result.error().message())
      .Info();

#ifdef USE_MYSQL
  // Auto-restart replication after DUMP SAVE (regardless of success/failure)
  if (replication_was_running && ctx_.binlog_reader != nullptr) {
    ctx_.replication_paused_for_dump = false;

    if (ctx_.binlog_reader->Start()) {
      mygram::utils::StructuredLog()
          .Event("replication_resumed_after_dump")
          .Field("operation", "dump_save")
          .Field("gtid", gtid)
          .Field("filepath", filepath)
          .Info();
    } else {
      std::string replication_error = ctx_.binlog_reader->GetLastError();
      mygram::utils::StructuredLog()
          .Event("replication_restart_failed")
          .Field("operation", "dump_save")
          .Field("gtid", gtid)
          .Field("filepath", filepath)
          .Field("error", replication_error)
          .Error();
    }
  }
#endif

  // Update progress and clear flag
  if (result) {
    mygram::utils::StructuredLog().Event("dump_save_completed").Field("filepath", filepath).Field("gtid", gtid).Info();
    if (ctx_.dump_progress != nullptr) {
      ctx_.dump_progress->Complete(filepath);
    }
  } else {
    std::string error_msg = result.error().message();
    mygram::utils::StructuredLog()
        .Event("dump_save_failed")
        .Field("filepath", filepath)
        .Field("gtid", gtid)
        .Field("error", error_msg)
        .Error();
    if (ctx_.dump_progress != nullptr) {
      ctx_.dump_progress->Fail("Failed to save dump: " + error_msg);
    }
  }

  // Clear the in-progress flag
  ctx_.dump_save_in_progress = false;
}

std::string DumpHandler::HandleDumpLoad(const query::Query& query) {
#ifdef USE_MYSQL
  // Check if replication is running (need to stop it before DUMP LOAD)
  bool replication_was_running = false;
  if (ctx_.binlog_reader != nullptr) {
    replication_was_running = ctx_.binlog_reader->IsRunning();
  }

  // Check if any table is currently syncing (block DUMP LOAD)
  if (ctx_.sync_manager != nullptr) {
    std::vector<std::string> syncing_tables;
    if (ctx_.sync_manager->GetSyncingTablesIfAny(syncing_tables)) {
      std::ostringstream oss;
      oss << "Cannot load dump while SYNC is in progress for tables:";
      for (const auto& table : syncing_tables) {
        oss << " " << table;
      }
      return ResponseFormatter::FormatError(oss.str());
    }
  }
#endif

  // Check if OPTIMIZE is in progress (block DUMP LOAD)
  if (ctx_.optimization_in_progress.load()) {
    return ResponseFormatter::FormatError(
        "Cannot load dump while OPTIMIZE is in progress. "
        "Please wait for optimization to complete.");
  }

  // Check if DUMP SAVE is in progress (block DUMP LOAD)
  if (ctx_.dump_save_in_progress.load()) {
    return ResponseFormatter::FormatError(
        "Cannot load dump while DUMP SAVE is in progress. "
        "Please wait for save to complete.");
  }

  // Check if another DUMP LOAD is in progress (block concurrent loads)
  if (ctx_.dump_load_in_progress.load()) {
    return ResponseFormatter::FormatError(
        "Cannot load dump while another DUMP LOAD is in progress. "
        "Please wait for current load to complete.");
  }

#ifdef USE_MYSQL
  // Stop replication before DUMP LOAD (if running)
  if (replication_was_running && ctx_.binlog_reader != nullptr) {
    ctx_.binlog_reader->Stop();
    ctx_.replication_paused_for_dump = true;
    mygram::utils::StructuredLog()
        .Event("replication_paused")
        .Field("operation", "dump_load")
        .Field("reason", "automatic_pause_for_consistency")
        .Info();
  }
#endif

  std::string filepath;
  if (!query.filepath.empty()) {
    filepath = query.filepath;
    if (filepath[0] != '/') {
      filepath = ctx_.dump_dir + "/" + filepath;
    }
    // Canonicalize path and validate it's within dump_dir
    try {
      std::filesystem::path canonical = std::filesystem::weakly_canonical(filepath);
      std::filesystem::path dump_canonical = std::filesystem::weakly_canonical(ctx_.dump_dir);
      auto rel = canonical.lexically_relative(dump_canonical);
      if (rel.empty() || rel.string().substr(0, 2) == "..") {
        return ResponseFormatter::FormatError("Invalid filepath: path traversal detected");
      }
    } catch (const std::exception& e) {
      return ResponseFormatter::FormatError(std::string("Invalid filepath: ") + e.what());
    }
  } else {
    return ResponseFormatter::FormatError("DUMP LOAD requires a filepath");
  }

  mygram::utils::StructuredLog().Event("dump_load_starting").Field("path", filepath).Info();

  // Set loading mode (RAII guard ensures it's cleared even on exceptions)
  FlagGuard loading_guard(ctx_.dump_load_in_progress);

  // Convert table_contexts to format expected by dump_v1::ReadDumpV1
  std::unordered_map<std::string, std::pair<index::Index*, storage::DocumentStore*>> converted_contexts;
  for (const auto& [table_name, table_ctx] : ctx_.table_contexts) {
    converted_contexts[table_name] = {table_ctx->index.get(), table_ctx->doc_store.get()};
  }

  // Variables to receive loaded data
  std::string gtid;
  config::Config loaded_config;
  storage::dump_format::IntegrityError integrity_error;

  // Call dump_v1 API
  auto result = storage::dump_v1::ReadDumpV1(filepath, gtid, loaded_config, converted_contexts, nullptr, nullptr,
                                             &integrity_error);

#ifdef USE_MYSQL
  // Update GTID from loaded dump (if load was successful and GTID is available)
  // This must be done regardless of whether replication was running before,
  // to enable manual REPLICATION START after DUMP LOAD
  if (result && !gtid.empty() && ctx_.binlog_reader != nullptr) {
    ctx_.binlog_reader->SetCurrentGTID(gtid);
    mygram::utils::StructuredLog()
        .Event("replication_gtid_updated")
        .Field("gtid", gtid)
        .Field("source", "dump_load")
        .Info();
  }

  // Auto-restart replication after DUMP LOAD (only if it was running before)
  if (replication_was_running && ctx_.binlog_reader != nullptr) {
    ctx_.replication_paused_for_dump = false;

    if (ctx_.binlog_reader->Start()) {
      mygram::utils::StructuredLog()
          .Event("replication_resumed")
          .Field("operation", "dump_load")
          .Field("reason", "automatic_restart_after_completion")
          .Field("gtid", gtid)
          .Info();
    } else {
      std::string replication_error = ctx_.binlog_reader->GetLastError();
      mygram::utils::StructuredLog()
          .Event("replication_restart_failed")
          .Field("operation", "dump_load")
          .Field("error", replication_error)
          .Error();
      // Don't fail DUMP LOAD due to replication restart failure
      // User can manually restart replication
    }
  }
#endif

  if (result) {
    mygram::utils::StructuredLog().Event("dump_load_completed").Field("path", filepath).Field("gtid", gtid).Info();
    return ResponseFormatter::FormatLoadResponse(filepath);
  }

  std::string error_msg = "Failed to load dump from " + filepath + ": " + result.error().message();
  if (!integrity_error.message.empty()) {
    error_msg += " (" + integrity_error.message + ")";
  }
  mygram::utils::StructuredLog()
      .Event("server_error")
      .Field("operation", "dump_load")
      .Field("filepath", filepath)
      .Field("error", error_msg)
      .Error();
  return ResponseFormatter::FormatError(error_msg);
}

std::string DumpHandler::HandleDumpVerify(const query::Query& query) {
  std::string filepath;
  if (!query.filepath.empty()) {
    filepath = query.filepath;
    if (filepath[0] != '/') {
      filepath = ctx_.dump_dir + "/" + filepath;
    }
    // Canonicalize path and validate it's within dump_dir
    try {
      std::filesystem::path canonical = std::filesystem::weakly_canonical(filepath);
      std::filesystem::path dump_canonical = std::filesystem::weakly_canonical(ctx_.dump_dir);
      auto rel = canonical.lexically_relative(dump_canonical);
      if (rel.empty() || rel.string().substr(0, 2) == "..") {
        return ResponseFormatter::FormatError("Invalid filepath: path traversal detected");
      }
    } catch (const std::exception& e) {
      return ResponseFormatter::FormatError(std::string("Invalid filepath: ") + e.what());
    }
  } else {
    return ResponseFormatter::FormatError("DUMP VERIFY requires a filepath");
  }

  mygram::utils::StructuredLog().Event("dump_verify_starting").Field("path", filepath).Info();

  storage::dump_format::IntegrityError integrity_error;
  auto result = storage::dump_v1::VerifyDumpIntegrity(filepath, integrity_error);

  if (result) {
    mygram::utils::StructuredLog().Event("dump_verify_succeeded").Field("path", filepath).Info();
    return "OK DUMP_VERIFIED " + filepath;
  }

  std::string error_msg = "Dump verification failed for " + filepath + ": " + result.error().message();
  if (!integrity_error.message.empty()) {
    error_msg += " (" + integrity_error.message + ")";
  }
  mygram::utils::StructuredLog()
      .Event("server_error")
      .Field("operation", "dump_verify")
      .Field("filepath", filepath)
      .Field("error", error_msg)
      .Error();
  return ResponseFormatter::FormatError(error_msg);
}

std::string DumpHandler::HandleDumpInfo(const query::Query& query) {
  std::string filepath;
  if (!query.filepath.empty()) {
    filepath = query.filepath;
    if (filepath[0] != '/') {
      filepath = ctx_.dump_dir + "/" + filepath;
    }
    // Canonicalize path and validate it's within dump_dir
    try {
      std::filesystem::path canonical = std::filesystem::weakly_canonical(filepath);
      std::filesystem::path dump_canonical = std::filesystem::weakly_canonical(ctx_.dump_dir);
      auto rel = canonical.lexically_relative(dump_canonical);
      if (rel.empty() || rel.string().substr(0, 2) == "..") {
        return ResponseFormatter::FormatError("Invalid filepath: path traversal detected");
      }
    } catch (const std::exception& e) {
      return ResponseFormatter::FormatError(std::string("Invalid filepath: ") + e.what());
    }
  } else {
    return ResponseFormatter::FormatError("DUMP INFO requires a filepath");
  }

  mygram::utils::StructuredLog().Event("dump_info_reading").Field("path", filepath).Info();

  storage::dump_v1::DumpInfo info;
  auto info_result = storage::dump_v1::GetDumpInfo(filepath, info);

  if (!info_result) {
    return ResponseFormatter::FormatError("Failed to read dump info from " + filepath + ": " +
                                          info_result.error().message());
  }

  std::ostringstream result;
  result << "OK DUMP_INFO " << filepath << "\r\n";
  result << "version: " << info.version << "\r\n";
  result << "gtid: " << info.gtid << "\r\n";
  result << "tables: " << info.table_count << "\r\n";
  result << "flags: " << info.flags << "\r\n";
  result << "file_size: " << info.file_size << "\r\n";
  result << "timestamp: " << info.timestamp << "\r\n";
  result << "has_statistics: " << (info.has_statistics ? "true" : "false") << "\r\n";
  result << "END";

  return result.str();
}

std::string DumpHandler::HandleDumpStatus() {
  std::ostringstream result;
  result << "OK DUMP_STATUS\r\n";

  // Check dump save status
  bool save_in_progress = ctx_.dump_save_in_progress.load();
  result << "save_in_progress: " << (save_in_progress ? "true" : "false") << "\r\n";

  // Check dump load status
  bool load_in_progress = ctx_.dump_load_in_progress.load();
  result << "load_in_progress: " << (load_in_progress ? "true" : "false") << "\r\n";

  // Check if replication is paused for dump
  bool replication_paused = ctx_.replication_paused_for_dump.load();
  result << "replication_paused_for_dump: " << (replication_paused ? "true" : "false") << "\r\n";

  // Overall status from DumpProgress (if available)
  std::string status;
  if (ctx_.dump_progress != nullptr) {
    std::lock_guard<std::mutex> lock(ctx_.dump_progress->mutex);
    switch (ctx_.dump_progress->status) {
      case DumpStatus::IDLE:
        status = "IDLE";
        break;
      case DumpStatus::SAVING:
        status = "SAVING";
        break;
      case DumpStatus::LOADING:
        status = "LOADING";
        break;
      case DumpStatus::COMPLETED:
        status = "COMPLETED";
        break;
      case DumpStatus::FAILED:
        status = "FAILED";
        break;
    }
    result << "status: " << status << "\r\n";

    // Show progress details if operation in progress or recently completed/failed
    if (ctx_.dump_progress->status != DumpStatus::IDLE) {
      result << "filepath: " << ctx_.dump_progress->filepath << "\r\n";
      result << "tables_processed: " << ctx_.dump_progress->tables_processed << "\r\n";
      result << "tables_total: " << ctx_.dump_progress->tables_total << "\r\n";

      if (!ctx_.dump_progress->current_table.empty()) {
        result << "current_table: " << ctx_.dump_progress->current_table << "\r\n";
      }

      // Show elapsed time
      auto now = std::chrono::steady_clock::now();
      auto end = (ctx_.dump_progress->status == DumpStatus::SAVING || ctx_.dump_progress->status == DumpStatus::LOADING)
                     ? now
                     : ctx_.dump_progress->end_time;
      double elapsed = std::chrono::duration<double>(end - ctx_.dump_progress->start_time).count();
      result << "elapsed_seconds: " << std::fixed << std::setprecision(2) << elapsed << "\r\n";

      // Show error message if failed
      if (ctx_.dump_progress->status == DumpStatus::FAILED && !ctx_.dump_progress->error_message.empty()) {
        result << "error: " << ctx_.dump_progress->error_message << "\r\n";
      }

      // Show last result filepath if completed
      if (ctx_.dump_progress->status == DumpStatus::COMPLETED && !ctx_.dump_progress->last_result_filepath.empty()) {
        result << "result_filepath: " << ctx_.dump_progress->last_result_filepath << "\r\n";
      }
    }
  } else {
    // Fallback when dump_progress not available
    if (save_in_progress) {
      status = "SAVE_IN_PROGRESS";
    } else if (load_in_progress) {
      status = "LOAD_IN_PROGRESS";
    } else {
      status = "IDLE";
    }
    result << "status: " << status << "\r\n";
  }

  result << "END";
  return result.str();
}

}  // namespace mygramdb::server
