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
#include "server/log_field_names.h"
#include "server/operation_names.h"
#include "server/replication_pause_counter.h"
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

  // Check if full_config is available
  if (ctx_.full_config == nullptr) {
    std::string error_msg = "Cannot save dump: server configuration is not available";
    mygram::utils::StructuredLog()
        .Event("dump_save_failed")
        .Field("reason", "config_not_available")
        .Field(log_fields::kFieldError, error_msg)
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

  // Atomic test-and-set on dump_save_in_progress, bundled with a release-on-
  // scope-exit RAII guard via OperationGuard::TryAcquire (Phase 4 H-D1).
  //
  // CR-2: do NOT split this into a separate load() + store(true) — that race
  // lets two concurrent DUMP SAVE clients both observe false and then both
  // store true, spawning duplicate worker threads. OperationGuard collapses
  // the test-and-set into a single atomic step. This matches the pattern in
  // SnapshotScheduler::TakeSnapshot, which is the other place that competes
  // for this flag (auto-snapshot vs. manual DUMP SAVE).
  //
  // On the async path, ownership of the flag transfers to the worker thread
  // (via flag_guard.Release() below) and the worker's own RAII guard resets
  // it (H-C1). On the sync path / thread-creation-failure path, the guard's
  // destructor resets the flag.
  auto flag_guard = mygram::utils::OperationGuard::TryAcquire(ctx_.dump_save_in_progress);
  if (!flag_guard.engaged()) {
    return ResponseFormatter::FormatError(
        "Cannot save dump while another DUMP SAVE is in progress. "
        "Please wait for current save to complete or use DUMP STATUS to check progress.");
  }

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
    // NOTE: flag_guard.Dismiss() is intentionally AFTER thread creation.
    // If make_unique<thread> throws, the guard auto-resets the flag.
    ctx_.dump_progress->worker_thread = std::make_unique<std::thread>([this, filepath]() { DumpSaveWorker(filepath); });

    // Thread created successfully — ownership of the dump_save_in_progress
    // flag transfers to the worker (DumpSaveWorker has its own RAII guard
    // that resets it at the end). Dismiss(), NOT Release(): we must NOT
    // clear the flag here, otherwise a concurrent client can observe false
    // and slip through compare_exchange before the worker even starts work.
    flag_guard.Dismiss();

    // Return immediately with started message (async mode)
    // Do NOT embed \r\n in the response -- the TCP protocol uses \r\n as the
    // frame terminator, so the client would truncate at the first \r\n.
    return ResponseFormatter::FormatStatus("DUMP_STARTED " + filepath);
  }

  // Fallback: run synchronously if no progress tracking available (e.g., in tests)
  log_dump_save_started("sync");

  // DumpSaveWorker has its own RAII guard that resets the flag at the end,
  // so dismiss this guard (don't clear) and hand ownership to the worker.
  flag_guard.Dismiss();
  bool success = DumpSaveWorker(filepath);

  // Check result and return appropriate response (sync mode)
  if (success) {
    return ResponseFormatter::FormatStatus("SAVED " + filepath);
  }
  return ResponseFormatter::FormatError("Dump save failed");
}

bool DumpHandler::DumpSaveWorker(const std::string& filepath) {
  // H-C1: Release dump_save_in_progress at thread exit, AFTER Complete()/Fail()
  // notifications and AFTER any binlog Start() restart. The previous code
  // released the flag at a fixed `store(false)` line at the end of this
  // function, but Complete()/Fail() were called BEFORE that store, leaving
  // a window where:
  //   1. worker thread runs `dump_progress_->Complete()` (unblocks waiters)
  //   2. another client observes "completed", calls HandleDumpSave, sees
  //      the flag still true, and gets ERROR busy.
  // OR worse:
  //   1. worker thread runs Complete()
  //   2. another client observes "completed", calls HandleDumpSave,
  //      compare_exchange_strong fails because the flag is still true,
  //      they get a misleading busy error.
  //
  // Releasing via ScopeGuard at the END of the worker scope makes the flag
  // visible-as-false strictly after every other worker side-effect, so any
  // post-Complete() observer correctly sees the slot as free.
  //
  // The release still uses memory_order_release to pair with the
  // acquire on the next compare_exchange_strong in HandleDumpSave.
  auto flag_release =
      mygram::utils::ScopeGuard([this]() { ctx_.dump_save_in_progress.store(false, std::memory_order_release); });

  bool replication_was_running = false;
  bool first_pauser = false;
  // RAII scope: if any path between Acquire() and the explicit Release()
  // below early-returns or throws, the destructor drops the counter so we
  // do not leak a pause increment. The destructor does NOT call Start();
  // the explicit Release() path below owns that side-effect.
  replication_pause::Scope pause_scope;

#ifdef USE_MYSQL
  std::string gtid;

  // Stop replication first, then capture GTID.
  // This ensures the worker thread has drained all queued events
  // before we capture the final processed GTID position.
  //
  // H-C3: Coordinate the actual Stop() with concurrent pause requests via
  // the process-wide replication_pause counter. Only the first pauser
  // calls Stop(); later pausers piggy-back on the existing pause. The
  // ctx_.replication_paused_for_dump flag is set by the first pauser as
  // the read-only "is paused" indicator used by REPLICATION STATUS / DUMP
  // STATUS responses, and cleared by the last releaser below.
  if (ctx_.binlog_reader != nullptr) {
    replication_was_running = ctx_.binlog_reader->IsRunning();

    if (replication_was_running) {
      first_pauser = pause_scope.Acquire();
      if (first_pauser) {
        ctx_.binlog_reader->Stop();
        ctx_.replication_paused_for_dump.store(true, std::memory_order_release);
      }
    }
  }

  // Capture GTID after stopping replication (last processed position).
  // If we were not the first pauser the reader is already stopped by an
  // earlier pauser, so this still reads the last-processed position.
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
        .Field("first_pauser", first_pauser)
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
  //
  // CR-10 shutdown check: skip the auto-restart if TcpServer::Stop() has
  // already announced shutdown. The binlog_reader_ is guaranteed alive at
  // this point (Stop() joins this worker BEFORE dropping binlog_reader_),
  // but a Start() call here would just spawn replication threads that
  // Stop() has to immediately tear down via member-destruction-order, and
  // worse, those threads would try to call back into Index/DocumentStore
  // that may be racing their own destructors. Just clear the paused flag
  // and skip the restart.
  //
  // H-C3: pause_scope.Release() undoes the Acquire() at the top of this
  // worker, but we only call binlog Start() when we are the LAST releaser
  // (counter transitioned 1 -> 0). If a concurrent operation (auto-
  // snapshot, DUMP LOAD) is still holding the pause, we leave the reader
  // stopped and the paused flag asserted; that operation's release will
  // perform the Start().
  const bool shutting_down = (ctx_.shutdown_flag != nullptr) && ctx_.shutdown_flag->load(std::memory_order_acquire);
  if (replication_was_running) {
    const bool last_releaser = pause_scope.Release();
    if (!last_releaser) {
      // Another operation is still paused — leave the reader stopped and
      // the flag set (set by the first pauser, cleared by whoever
      // releases last). Log so operators can correlate pause/release.
      mygram::utils::StructuredLog()
          .Event("replication_pause_released")
          .Field("operation", "dump_save")
          .Field("gtid", gtid)
          .Field("filepath", filepath)
          .Field("last_releaser", false)
          .Info();
    } else if (ctx_.binlog_reader != nullptr) {
      ctx_.replication_paused_for_dump.store(false, std::memory_order_release);

      if (shutting_down) {
        mygram::utils::StructuredLog()
            .Event("replication_restart_skipped")
            .Field("operation", "dump_save")
            .Field("reason", "server_shutting_down")
            .Field("gtid", gtid)
            .Field("filepath", filepath)
            .Info();
      } else {
        // H-D4: standardize on the Expected<void, Error> chain rather than
        // the legacy bool + GetLastError() shape. The bool overload of
        // Expected and the explicit error()-message branch are the same
        // pattern used by the DUMP LOAD restore_replication ScopeGuard, so
        // both restart paths now read identically.
        auto start_result = ctx_.binlog_reader->Start();
        if (start_result) {
          mygram::utils::StructuredLog()
              .Event("replication_resumed_after_dump")
              .Field("operation", "dump_save")
              .Field(log_fields::kFieldGtid, gtid)
              .Field(log_fields::kFieldFilepath, filepath)
              .Info();
        } else {
          mygram::utils::StructuredLog()
              .Event("replication_restart_failed")
              .Field("operation", "dump_save")
              .Field(log_fields::kFieldGtid, gtid)
              .Field(log_fields::kFieldFilepath, filepath)
              .FieldError(start_result.error())
              .Error();
        }
      }
    } else {
      // No reader; just clear the flag (we were the last releaser).
      ctx_.replication_paused_for_dump.store(false, std::memory_order_release);
    }
  }
#endif

  // Update progress. The dump_save_in_progress flag is released by the
  // ScopeGuard installed at the top of this function (H-C1). Releasing
  // AFTER Complete()/Fail() ensures any client that observed completion
  // sees the slot as free on the next compare_exchange_strong.
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

  // Atomic test-and-set on dump_load_in_progress, bundled with a release-on-
  // scope-exit RAII guard via OperationGuard::TryAcquire (Phase 4 H-D1).
  //
  // CR-2: do NOT split this into a separate load() + later AtomicFlagGuard —
  // that race lets two concurrent DUMP LOAD clients both observe false and
  // then both proceed to stop replication / clear data, corrupting state.
  // OperationGuard::TryAcquire collapses the test-and-set into a single
  // atomic step, mirroring HandleDumpSave / TakeSnapshot.
  //
  // The guard below releases the flag in the failure path (its destructor)
  // and is dismissed via Release() on the success path AFTER post-load steps
  // complete.
  auto loading_guard = mygram::utils::OperationGuard::TryAcquire(ctx_.dump_load_in_progress);
  if (!loading_guard.engaged()) {
    return ResponseFormatter::FormatError(
        "Cannot load dump while another DUMP LOAD is in progress. "
        "Please wait for current load to complete.");
  }

#ifdef USE_MYSQL
  // Check if replication is running (need to stop it before DUMP LOAD)
  bool replication_was_running = false;
  if (ctx_.binlog_reader != nullptr) {
    replication_was_running = ctx_.binlog_reader->IsRunning();
  }

  // Stop replication before DUMP LOAD (if running).
  //
  // H-C3: Coordinate with concurrent pausers via the process-wide
  // replication_pause counter. Only the first pauser actually calls
  // Stop() and asserts the observable flag; subsequent pausers piggy-
  // back. The matching Release() lives in restore_replication / the
  // explicit success path further down. pause_scope is the RAII safety
  // net: if any path between here and the success-path Release() exits
  // without releasing, the destructor drops the counter so we do not
  // leak the increment. The destructor itself does NOT call Start();
  // the ScopeGuard / explicit-success branches own that side-effect.
  bool first_pauser = false;
  replication_pause::Scope pause_scope;
  if (replication_was_running && ctx_.binlog_reader != nullptr) {
    first_pauser = pause_scope.Acquire();
    if (first_pauser) {
      ctx_.binlog_reader->Stop();
      ctx_.replication_paused_for_dump.store(true, std::memory_order_release);
    }
    mygram::utils::StructuredLog()
        .Event("replication_paused")
        .Field("operation", "dump_load")
        .Field("reason", "automatic_pause_for_consistency")
        .Field("first_pauser", first_pauser)
        .Info();
  }

  // Fallback ScopeGuard that releases the pause counter and (when we
  // are the last releaser) restarts replication on every error-path
  // exit. The success path explicitly performs the restart and dismisses
  // this guard so the restart is not duplicated. This guarantees that
  // any early-return from here onward (validation failure inside
  // ReadDump, exception, etc.) does not leave the server with
  // replication permanently stopped (P0-A).
  //
  // CR-10 shutdown check inside the lambda: if TcpServer::Stop() has
  // announced shutdown, skip the binlog Start() entirely. The reader is
  // guaranteed alive (TcpServer::Stop joins this worker before the
  // reader is dropped) but a Start() during teardown would just spawn
  // threads Stop() has to immediately tear down.
  auto restore_replication = mygram::utils::ScopeGuard([this, replication_was_running, &pause_scope]() {
    if (!replication_was_running) {
      // We did not enter the pause counter; nothing to release.
      return;
    }
    const bool last_releaser = pause_scope.Release();
    if (!last_releaser) {
      // Another operation still holds the pause. Leave the reader
      // stopped and the flag asserted; the other op's release will
      // perform the eventual Start().
      mygram::utils::StructuredLog()
          .Event("replication_pause_released")
          .Field("operation", "dump_load")
          .Field("last_releaser", false)
          .Info();
      return;
    }
    ctx_.replication_paused_for_dump.store(false, std::memory_order_release);
    if (ctx_.binlog_reader == nullptr) {
      return;
    }
    const bool shutting_down = (ctx_.shutdown_flag != nullptr) && ctx_.shutdown_flag->load(std::memory_order_acquire);
    if (shutting_down) {
      mygram::utils::StructuredLog()
          .Event("replication_restart_skipped")
          .Field("operation", "dump_load")
          .Field("reason", "server_shutting_down")
          .Info();
      return;
    }
    auto restart_result = ctx_.binlog_reader->Start();
    if (!restart_result) {
      mygram::utils::StructuredLog()
          .Event("dump_load_replication_restart_failed")
          .Field("operation", "dump_load")
          .Field("error", restart_result.error().message())
          .Error();
    }
  });
#endif

  mygram::utils::StructuredLog().Event("dump_load_starting").Field(log_fields::kFieldFilepath, filepath).Info();

  // The loading_guard (AtomicFlagResetGuard) was installed above immediately
  // after compare_exchange_strong succeeded. It is Release()d only on the
  // success path, after all post-load steps complete; on failure it falls
  // through to the destructor and clears dump_load_in_progress.

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
  //
  // CR-10: skip the explicit restart when shutdown is in progress (the
  // ScopeGuard performs the same shutdown-aware skip).
  //
  // H-C3: As in the ScopeGuard above, the explicit restart goes through
  // pause_scope.Release() so concurrent pausers (auto-snapshot, DUMP
  // SAVE) cannot have the binlog reader Start()ed under them. We only
  // call binlog Start() when we are the LAST releaser. After Release()
  // returns the scope is "spent" — the ScopeGuard's lambda will see
  // pause_scope.Release() return false and skip its branch, so we do
  // not need to call Release() on the ScopeGuard explicitly for counter
  // bookkeeping. We still Release() the ScopeGuard below to suppress
  // its (now-redundant) flag-clear and Start() side-effects.
  if (result && replication_was_running) {
    const bool last_releaser = pause_scope.Release();
    if (!last_releaser) {
      // Another operation still holds the pause. Leave the reader
      // stopped and the flag asserted. We still dismiss restore_replication
      // because we have already released the counter and must not double-
      // release in the destructor.
      mygram::utils::StructuredLog()
          .Event("replication_pause_released")
          .Field("operation", "dump_load")
          .Field("last_releaser", false)
          .Field("gtid", gtid)
          .Info();
      restore_replication.Release();
    } else if (ctx_.binlog_reader != nullptr) {
      ctx_.replication_paused_for_dump.store(false, std::memory_order_release);

      const bool shutting_down = (ctx_.shutdown_flag != nullptr) && ctx_.shutdown_flag->load(std::memory_order_acquire);
      if (shutting_down) {
        mygram::utils::StructuredLog()
            .Event("replication_restart_skipped")
            .Field("operation", "dump_load")
            .Field("reason", "server_shutting_down")
            .Field("gtid", gtid)
            .Info();
      } else {
        // H-D4: use Expected<void, Error> chain (same pattern as the
        // restore_replication ScopeGuard above).
        auto start_result = ctx_.binlog_reader->Start();
        if (start_result) {
          mygram::utils::StructuredLog()
              .Event("replication_resumed")
              .Field("operation", "dump_load")
              .Field("reason", "automatic_restart_after_completion")
              .Field(log_fields::kFieldGtid, gtid)
              .Info();
        } else {
          mygram::utils::StructuredLog()
              .Event("replication_restart_failed")
              .Field("operation", "dump_load")
              .FieldError(start_result.error())
              .Error();
          // Don't fail DUMP LOAD due to replication restart failure
          // User can manually restart replication
        }
      }
      restore_replication.Release();
    } else {
      // Last releaser but no reader; just clear the flag and dismiss.
      ctx_.replication_paused_for_dump.store(false, std::memory_order_release);
      restore_replication.Release();
    }
  } else if (result) {
    // Success path with no replication to pause/restart (replication was
    // already stopped at the start of DUMP LOAD): nothing to release on
    // the counter, the flag was never set by us, dismiss the guard.
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

    mygram::utils::StructuredLog()
        .Event("dump_load_completed")
        .Field(log_fields::kFieldFilepath, filepath)
        .Field(log_fields::kFieldGtid, gtid)
        .Info();
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

  mygram::utils::StructuredLog().Event("dump_verify_starting").Field(log_fields::kFieldFilepath, filepath).Info();

  storage::dump_format::IntegrityError integrity_error;
  auto result = storage::dump_v2::VerifyDumpIntegrity(filepath, integrity_error);

  if (result) {
    mygram::utils::StructuredLog().Event("dump_verify_succeeded").Field(log_fields::kFieldFilepath, filepath).Info();
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

  mygram::utils::StructuredLog().Event("dump_info_reading").Field(log_fields::kFieldFilepath, filepath).Info();

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
