/**
 * @file dump_handler.cpp
 * @brief Handler for dump-related commands
 */

#include "server/handlers/dump_handler.h"

#include <spdlog/spdlog.h>
#include <sys/stat.h>

#include <filesystem>
#include <fstream>
#include <iomanip>
#include <sstream>

#include "cache/cache_manager.h"
#include "server/operation_names.h"
#include "server/sync_operation_manager.h"
#include "server/table_catalog.h"
#include "storage/dump_format_v1.h"
#include "storage/dump_format_v2.h"
#include "utils/fd_guard.h"
#include "utils/flag_guard.h"
#include "utils/safe_path.h"
#include "utils/string_utils.h"
#include "utils/structured_log.h"

#ifdef USE_MYSQL
#include "mysql/binlog_reader_interface.h"
#endif

namespace mygramdb::server {

namespace {

/// Convenience: resolve a dump-handler filepath via the shared safe-path utility
/// using the "dump directory" label so traversal errors mention the dump dir.
mygram::utils::Expected<std::string, mygram::utils::Error> ResolveDumpFilepath(const std::string& input,
                                                                               const std::string& dump_dir) {
  return mygram::utils::ResolveSafePath(input, dump_dir, /*allowed_extensions=*/{},
                                        /*base_dir_label=*/"dump directory");
}

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
    auto check = ctx_.sync_manager->CheckNoSyncInProgress(ops::kSaveDump);
    if (!check) {
      return ResponseFormatter::FormatError(check.error().message());
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
        .Field("error", error_msg)
        .Error();
    return ResponseFormatter::FormatError(error_msg);
  }

  // Determine filepath
  std::string filepath;
  if (!query.filepath.empty()) {
    auto resolved = ResolveDumpFilepath(query.filepath, ctx_.dump_dir);
    if (!resolved) {
      return ResponseFormatter::FormatError(resolved.error().message());
    }
    filepath = std::move(*resolved);
  } else {
    filepath = ctx_.dump_dir + "/" + ctx_.full_config->dump.default_filename;
  }

  // Set flag to indicate save is starting.
  // Use ScopeGuard so the flag is automatically reset if thread creation fails
  // or if the synchronous path throws an exception.
  ctx_.dump_save_in_progress.store(true, std::memory_order_release);
  auto flag_guard =
      mygram::utils::ScopeGuard([this]() { ctx_.dump_save_in_progress.store(false, std::memory_order_release); });

  // Capture the current GTID once for both async/sync log paths so the
  // dump_save_started event records the position the operator would expect
  // the dump to anchor against. We keep the field optional: empty/null reader
  // means we omit it rather than emit an empty string.
  std::string started_gtid;
#ifdef USE_MYSQL
  if (ctx_.binlog_reader != nullptr) {
    started_gtid = ctx_.binlog_reader->GetCurrentGTID();
  }
#endif

  auto log_dump_save_started = [&](const char* mode) {
    auto log = mygram::utils::StructuredLog()
                   .Event("dump_save_started")
                   .Field("filepath", filepath)
                   .Field("mode", mode)
                   .Field("tables", static_cast<uint64_t>(ctx_.table_catalog->GetTables().size()));
    if (!started_gtid.empty()) {
      log.Field("gtid", started_gtid);
    }
    log.Info();
  };

  // Initialize progress tracking and run async if progress tracking is available
  if (ctx_.dump_progress != nullptr) {
    log_dump_save_started("async");

    // Join any previous worker thread
    ctx_.dump_progress->JoinWorker();
    ctx_.dump_progress->Reset(DumpStatus::SAVING, filepath, ctx_.table_catalog->GetTables().size());

    // Start background worker thread.
    // NOTE: flag_guard.Release() is intentionally AFTER thread creation.
    // If make_unique<thread> throws, the guard auto-resets the flag.
    ctx_.dump_progress->worker_thread = std::make_unique<std::thread>([this, filepath]() { DumpSaveWorker(filepath); });

    // Thread created successfully - the worker thread now owns the flag cleanup
    // (DumpSaveWorker resets dump_save_in_progress at the end)
    flag_guard.Release();

    // Return immediately with started message (async mode)
    // Do NOT embed \r\n in the response -- the TCP protocol uses \r\n as the
    // frame terminator, so the client would truncate at the first \r\n.
    return ResponseFormatter::FormatStatus("DUMP_STARTED " + filepath);
  }

  // Fallback: run synchronously if no progress tracking available (e.g., in tests)
  log_dump_save_started("sync");

  // DumpSaveWorker resets the flag at the end, so release the guard
  flag_guard.Release();
  bool success = DumpSaveWorker(filepath);

  // Check result and return appropriate response (sync mode)
  if (success) {
    return ResponseFormatter::FormatStatus("SAVED " + filepath);
  }
  return ResponseFormatter::FormatError("Dump save failed");
}

bool DumpHandler::DumpSaveWorker(const std::string& filepath) {
  bool replication_was_running = false;

#ifdef USE_MYSQL
  std::string gtid;

  // Stop replication first, then capture GTID.
  // This ensures the worker thread has drained all queued events
  // before we capture the final processed GTID position.
  if (ctx_.binlog_reader != nullptr) {
    replication_was_running = ctx_.binlog_reader->IsRunning();

    if (replication_was_running) {
      ctx_.binlog_reader->Stop();
      ctx_.replication_paused_for_dump.store(true, std::memory_order_release);
    }
  }

  // Capture GTID after stopping replication (last processed position)
  if (ctx_.binlog_reader != nullptr) {
    gtid = ctx_.binlog_reader->GetCurrentGTID();
  }

  if (replication_was_running) {
    mygram::utils::StructuredLog()
        .Event("replication_paused_for_dump")
        .Field("operation", "dump_save")
        .Field("gtid", gtid)
        .Field("filepath", filepath)
        .Field("auto_resume", "true")
        .Info();
  }
#else
  std::string gtid;
#endif

  // Convert table contexts to format expected by dump API
  auto converted_contexts = ctx_.table_catalog->GetDumpableContexts();
  {
    size_t table_index = 0;
    for (const auto& [table_name, ctx_pair] : converted_contexts) {
      if (ctx_.dump_progress != nullptr) {
        ctx_.dump_progress->UpdateTable(table_name, table_index);
      }
      ++table_index;
    }
  }

  // Call dump API (writes V2 format)
  mygram::utils::StructuredLog()
      .Event("dump_save_write_starting")
      .Field("filepath", filepath)
      .Field("gtid", gtid)
      .Field("tables", static_cast<uint64_t>(converted_contexts.size()))
      .Info();

  auto result = storage::dump_v2::WriteDump(filepath, gtid, *ctx_.full_config, converted_contexts);

  mygram::utils::StructuredLog()
      .Event("dump_save_write_finished")
      .Field("filepath", filepath)
      .Field("success", result.has_value() ? "true" : "false")
      .Field("error", result.has_value() ? "" : result.error().message())
      .Info();

#ifdef USE_MYSQL
  // Auto-restart replication after DUMP SAVE (regardless of success/failure).
  //
  // The replication_paused_for_dump flag is cleared BEFORE the binlog
  // Start() call so that external operator-initiated REPLICATION START
  // commands continue to work even if Start() itself fails internally.
  // If the flag stayed set on a Start() failure, REPLICATION STATUS would
  // misreport the dump as still in progress and a subsequent REPLICATION
  // START might be rejected by the "paused for dump" guard. Clearing the
  // flag first decouples the dump-pause state from binlog-restart errors.
  if (replication_was_running && ctx_.binlog_reader != nullptr) {
    ctx_.replication_paused_for_dump.store(false, std::memory_order_release);

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
  bool success = result.has_value();
  if (success) {
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
        .FieldError(result.error())
        .Error();
    if (ctx_.dump_progress != nullptr) {
      ctx_.dump_progress->Fail("Failed to save dump: " + error_msg);
    }
  }

  // Use explicit store with release semantics for consistency with
  // the corresponding load(memory_order_acquire) in HandleDumpSave.
  ctx_.dump_save_in_progress.store(false, std::memory_order_release);
  return success;
}

std::string DumpHandler::HandleDumpLoad(const query::Query& query) {
#ifdef USE_MYSQL
  // Check if any table is currently syncing (block DUMP LOAD)
  if (ctx_.sync_manager != nullptr) {
    auto check = ctx_.sync_manager->CheckNoSyncInProgress(ops::kLoadDump);
    if (!check) {
      return ResponseFormatter::FormatError(check.error().message());
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

  // Validate filepath BEFORE mutating any replication or load-flag state.
  // Previously, the empty()/ResolveDumpFilepath check was performed AFTER
  // stopping replication, which left replication permanently paused on
  // validation failure (P0-A). Failing fast here keeps the server in a clean
  // state.
  if (query.filepath.empty()) {
    return ResponseFormatter::FormatError("DUMP LOAD requires a filepath");
  }
  auto resolved = ResolveDumpFilepath(query.filepath, ctx_.dump_dir);
  if (!resolved) {
    return ResponseFormatter::FormatError(resolved.error().message());
  }
  std::string filepath = std::move(*resolved);

#ifdef USE_MYSQL
  // Check if replication is running (need to stop it before DUMP LOAD)
  bool replication_was_running = false;
  if (ctx_.binlog_reader != nullptr) {
    replication_was_running = ctx_.binlog_reader->IsRunning();
  }

  // Stop replication before DUMP LOAD (if running)
  if (replication_was_running && ctx_.binlog_reader != nullptr) {
    ctx_.binlog_reader->Stop();
    ctx_.replication_paused_for_dump.store(true, std::memory_order_release);
    mygram::utils::StructuredLog()
        .Event("replication_paused")
        .Field("operation", "dump_load")
        .Field("reason", "automatic_pause_for_consistency")
        .Info();
  }

  // Fallback ScopeGuard that restarts replication and clears the
  // replication_paused_for_dump flag on every error-path exit. The success
  // path explicitly performs the restart and dismisses this guard so the
  // restart is not duplicated. This guarantees that any early-return from
  // here onward (validation failure inside ReadDump, exception, etc.) does
  // not leave the server with replication permanently stopped (P0-A).
  auto restore_replication = mygram::utils::ScopeGuard([this, replication_was_running]() {
    ctx_.replication_paused_for_dump.store(false, std::memory_order_release);
    if (replication_was_running && ctx_.binlog_reader != nullptr) {
      auto restart_result = ctx_.binlog_reader->Start();
      if (!restart_result) {
        mygram::utils::StructuredLog()
            .Event("dump_load_replication_restart_failed")
            .Field("operation", "dump_load")
            .Field("error", restart_result.error().message())
            .Error();
      }
    }
  });
#endif

  mygram::utils::StructuredLog().Event("dump_load_starting").Field("path", filepath).Info();

  // Set loading mode (RAII guard ensures it's cleared even on exceptions or
  // early-return error paths in ReadDump). Release()d only on the success
  // path, after all post-load steps complete.
  mygram::utils::AtomicFlagGuard loading_guard(ctx_.dump_load_in_progress);

  // Convert table contexts to format expected by dump API
  auto converted_contexts = ctx_.table_catalog->GetDumpableContexts();

  // Variables to receive loaded data
  std::string gtid;
  config::Config loaded_config;
  storage::dump_format::IntegrityError integrity_error;

  // Call dump API (auto-detects V1 or V2 format)
  auto result =
      storage::dump_v2::ReadDump(filepath, gtid, loaded_config, converted_contexts, nullptr, nullptr, &integrity_error);

  // The loading guard remains active through replication restart and cache
  // rebuild. It is released only after the success path completes, ensuring
  // dump_load_in_progress stays true if the load failed.

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

  // Auto-restart replication after DUMP LOAD (only if it was running before).
  // The success path performs the restart explicitly and then dismisses the
  // ScopeGuard so the restart is not duplicated. Error paths fall through to
  // the guard's destructor, which performs the same restart.
  if (result && replication_was_running && ctx_.binlog_reader != nullptr) {
    ctx_.replication_paused_for_dump.store(false, std::memory_order_release);

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
    restore_replication.Release();
  } else if (result) {
    // Success path with no replication to restart: clear the paused flag
    // here and dismiss the guard so it does not redundantly clear it.
    ctx_.replication_paused_for_dump.store(false, std::memory_order_release);
    restore_replication.Release();
  }
#endif

  if (result) {
    // Clear search cache after successful load — cached results reference old data
    if (ctx_.cache_manager != nullptr) {
      ctx_.cache_manager->Clear();
    }

    // Rebuild BM25 corpus statistics from loaded documents.
    // Use batch API to minimize per-document lock acquisitions.
    for (const auto& [table_name, table_ctx] : ctx_.table_catalog->GetTables()) {
      if (table_ctx->doc_store) {
        auto all_doc_ids = table_ctx->doc_store->GetAllDocIds();
        auto all_texts = table_ctx->doc_store->GetNormalizedTextBatch(all_doc_ids);
        uint64_t total_length = 0;
        uint64_t doc_count = 0;
        for (const auto& text_opt : all_texts) {
          if (text_opt.has_value() && !text_opt->empty()) {
            total_length += mygram::utils::CountCodePoints(*text_opt);
            ++doc_count;
          }
        }
        table_ctx->bm25_stats.total_doc_length.store(total_length, std::memory_order_relaxed);
        table_ctx->bm25_stats.doc_count.store(doc_count, std::memory_order_relaxed);
      }
    }

    mygram::utils::StructuredLog().Event("dump_load_completed").Field("path", filepath).Field("gtid", gtid).Info();
    loading_guard.Release();
    return ResponseFormatter::FormatLoadResponse(filepath);
  }

  // Failure path: loading_guard and restore_replication will run via their
  // destructors, restoring replication and clearing dump_load_in_progress.
  std::string error_msg = "Failed to load dump from " + filepath + ": " + result.error().message();
  if (!integrity_error.message.empty()) {
    error_msg += " (" + integrity_error.message + ")";
  }
  // Dedicated event name (formerly server_error + operation=dump_load) so log
  // pipelines can filter dump_load failures without parsing a sub-field.
  mygram::utils::StructuredLog()
      .Event("dump_load_failed")
      .Field("filepath", filepath)
      .Field("error", error_msg)
      .Field("error_code", static_cast<int64_t>(result.error().code()))
      .Error();
  return ResponseFormatter::FormatError(error_msg);
}

std::string DumpHandler::HandleDumpVerify(const query::Query& query) {
  if (query.filepath.empty()) {
    return ResponseFormatter::FormatError("DUMP VERIFY requires a filepath");
  }
  auto resolved = ResolveDumpFilepath(query.filepath, ctx_.dump_dir);
  if (!resolved) {
    return ResponseFormatter::FormatError(resolved.error().message());
  }
  std::string filepath = std::move(*resolved);

  mygram::utils::StructuredLog().Event("dump_verify_starting").Field("path", filepath).Info();

  storage::dump_format::IntegrityError integrity_error;
  auto result = storage::dump_v2::VerifyDumpIntegrity(filepath, integrity_error);

  if (result) {
    mygram::utils::StructuredLog().Event("dump_verify_succeeded").Field("path", filepath).Info();
    return ResponseFormatter::FormatStatus("DUMP_VERIFIED " + filepath);
  }

  std::string error_msg = "Dump verification failed for " + filepath + ": " + result.error().message();
  if (!integrity_error.message.empty()) {
    error_msg += " (" + integrity_error.message + ")";
  }
  // Dedicated event name (formerly server_error + operation=dump_verify).
  mygram::utils::StructuredLog()
      .Event("dump_verify_failed")
      .Field("filepath", filepath)
      .Field("error", error_msg)
      .Field("error_code", static_cast<int64_t>(result.error().code()))
      .Error();
  return ResponseFormatter::FormatError(error_msg);
}

std::string DumpHandler::HandleDumpInfo(const query::Query& query) {
  if (query.filepath.empty()) {
    return ResponseFormatter::FormatError("DUMP INFO requires a filepath");
  }
  auto resolved = ResolveDumpFilepath(query.filepath, ctx_.dump_dir);
  if (!resolved) {
    return ResponseFormatter::FormatError(resolved.error().message());
  }
  std::string filepath = std::move(*resolved);

  mygram::utils::StructuredLog().Event("dump_info_reading").Field("path", filepath).Info();

  storage::dump_v2::DumpV2Info info;
  auto info_result = storage::dump_v2::GetDumpInfo(filepath, info);

  if (!info_result) {
    return ResponseFormatter::FormatError("Failed to read dump info from " + filepath + ": " +
                                          info_result.error().message());
  }

  // Note: returning the full canonical filepath is intentional. TCP clients
  // connecting to MygramDB are assumed authenticated by network ACL (see
  // connection_acceptor.cpp CIDR check); they need the absolute path to use
  // it with subsequent DUMP LOAD. Do NOT redact this without first changing
  // DUMP LOAD to accept the basename and resolve it against the server-side
  // dump_dir.
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
