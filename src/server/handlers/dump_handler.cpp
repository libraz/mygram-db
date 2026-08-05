/**
 * @file dump_handler.cpp
 * @brief Handler for dump-related commands
 */

#include "server/handlers/dump_handler.h"

#include <spdlog/spdlog.h>
#include <sys/stat.h>

#include <cassert>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <optional>
#include <shared_mutex>
#include <sstream>

#include "cache/cache_manager.h"
#include "server/log_field_names.h"
#include "server/operation_coordinator.h"
#include "server/operation_names.h"
#include "server/protocol_constants.h"
#include "server/replication_pause_counter.h"
#include "server/replication_pause_guard.h"
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

const config::TableConfig* FindTableConfigByName(const config::Config& config, const std::string& table_name) {
  for (const auto& table : config.tables) {
    if (table.name == table_name) {
      return &table;
    }
  }
  return nullptr;
}

std::optional<std::string> FindTokenizerConfigMismatch(const config::Config& loaded_config,
                                                       const config::Config& live_config) {
  if (loaded_config.memory.verify_text.empty()) {
    if (live_config.memory.verify_text != "off") {
      return "legacy dump is missing memory.verify_text compatibility metadata; load with verify_text=off or rebuild "
             "the dump from the source database";
    }
  } else if (loaded_config.memory.verify_text != live_config.memory.verify_text) {
    return "memory.verify_text mismatch between dump and running config";
  }
  if (loaded_config.memory.normalize.nfkc != live_config.memory.normalize.nfkc) {
    return "memory.normalize.nfkc mismatch between dump and running config";
  }
  if (loaded_config.memory.normalize.width != live_config.memory.normalize.width) {
    return "memory.normalize.width mismatch between dump and running config";
  }
  if (loaded_config.memory.normalize.lower != live_config.memory.normalize.lower) {
    return "memory.normalize.lower mismatch between dump and running config";
  }

  for (const auto& loaded_table : loaded_config.tables) {
    const auto* live_table = FindTableConfigByName(live_config, loaded_table.name);
    if (live_table == nullptr) {
      continue;
    }
    if (loaded_table.ngram_size != live_table->ngram_size) {
      return "table '" + loaded_table.name + "' ngram_size mismatch between dump and running config";
    }
    if (loaded_table.kanji_ngram_size != live_table->kanji_ngram_size) {
      return "table '" + loaded_table.name + "' kanji_ngram_size mismatch between dump and running config";
    }
    if (loaded_table.cross_boundary_ngrams != live_table->cross_boundary_ngrams) {
      return "table '" + loaded_table.name + "' cross_boundary_ngrams mismatch between dump and running config";
    }
  }

  return std::nullopt;
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

  std::shared_ptr<OperationCoordinator::Token> operation_token;
  if (ctx_.operation_coordinator != nullptr) {
    auto acquired = ctx_.operation_coordinator->TryAcquire(LongOperation::kDumpSave, filepath);
    if (!acquired.has_value()) {
      return ResponseFormatter::FormatError("Cannot save dump while " + ctx_.operation_coordinator->DescribeActive() +
                                            " is in progress");
    }
    operation_token = std::make_shared<OperationCoordinator::Token>(std::move(*acquired));
  }

  // Atomic test-and-set on dump_save_in_progress, bundled with a release-on-
  // scope-exit RAII guard via OperationGuard::TryAcquire.
  //
  // Do NOT split this into a separate load() + store(true) — that race
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
  if (ctx_.after_dump_save_flag_acquired) {
    ctx_.after_dump_save_flag_acquired();
  }
  if (ctx_.dump_load_in_progress.load()) {
    flag_guard.Release();
    return ResponseFormatter::FormatError(
        "Cannot save dump while DUMP LOAD is in progress. "
        "Please wait for load to complete.");
  }
  if (ctx_.optimization_in_progress.load()) {
    flag_guard.Release();
    return ResponseFormatter::FormatError(
        "Cannot save dump while OPTIMIZE is in progress. "
        "Please wait for optimization to complete.");
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
    //
    // StartWorker() performs the worker_thread assignment under
    // DumpProgress::mutex so it cannot race with JoinWorker() (called
    // from TcpServer::Stop()) on the unique_ptr itself. JoinWorker()
    // above has already drained any prior worker, satisfying the
    // StartWorker pre-condition.
    ctx_.dump_progress->StartWorker([this, filepath, operation_token]() {
      (void)operation_token;  // Holds the coordinator slot through worker completion.
      DumpSaveWorker(filepath);
    });

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

#ifdef USE_MYSQL
  assert(ctx_.replication_pause_counter != nullptr);
  replication_pause::Guard replication_pause(ctx_.binlog_reader, *ctx_.replication_pause_counter,
                                             ctx_.replication_paused_for_dump, ctx_.shutdown_flag, "dump_save");
  const auto pause_result = replication_pause.Pause();
  std::string gtid;
  gtid = pause_result.drained_gtid;
  if (pause_result.engaged) {
    mygram::utils::StructuredLog()
        .Event("replication_paused_for_dump")
        .Field("operation", "dump_save")
        .Field("gtid", gtid)
        .Field("filepath", filepath)
        .Field("auto_resume", "true")
        .Field("first_pauser", pause_result.first_pauser)
        .Info();
  }
#else
  std::string gtid;
#endif

  // Convert table contexts to format expected by dump API
  auto converted_contexts = ctx_.table_catalog->GetDumpableContexts();
  std::string source_server_uuid;
#ifdef USE_MYSQL
  if (ctx_.binlog_reader != nullptr) {
    source_server_uuid = ctx_.binlog_reader->GetSourceServerUUID();
  }
#endif

  // Call dump API (writes V2 format)
  mygram::utils::StructuredLog()
      .Event("dump_save_write_starting")
      .Field("filepath", filepath)
      .Field("gtid", gtid)
      .Field("tables", static_cast<uint64_t>(converted_contexts.size()))
      .Info();

  auto result = storage::dump_v2::WriteDump(
      filepath, gtid, *ctx_.full_config, converted_contexts, nullptr, nullptr,
      [this](const std::string& table_name, size_t tables_processed) {
        if (ctx_.dump_progress != nullptr) {
          ctx_.dump_progress->UpdateTable(table_name, tables_processed);
        }
      },
      storage::dump_v2::RestoreLimits{
          static_cast<uint64_t>(ctx_.full_config->dump.restore_memory_budget_mb) * 1024ULL * 1024ULL,
          static_cast<uint64_t>(ctx_.full_config->dump.restore_max_section_mb) * 1024ULL * 1024ULL},
      source_server_uuid);

  mygram::utils::StructuredLog()
      .Event("dump_save_write_finished")
      .Field("filepath", filepath)
      .Field("success", result.has_value() ? "true" : "false")
      .Field("error", result.has_value() ? "" : result.error().message())
      .Info();

#ifdef USE_MYSQL
  auto restart_result = replication_pause.Restore();
  if (!restart_result) {
    mygram::utils::StructuredLog()
        .Event("replication_restart_failed")
        .Field("operation", "dump_save")
        .Field(log_fields::kFieldGtid, gtid)
        .Field(log_fields::kFieldFilepath, filepath)
        .FieldError(restart_result.error())
        .Error();
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
    ctx_.stats.RecordDumpSuccess();
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
    ctx_.stats.IncrementDumpFailureManual();
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

  OperationCoordinator::Token operation_token;
  if (ctx_.operation_coordinator != nullptr) {
    auto acquired = ctx_.operation_coordinator->TryAcquire(LongOperation::kDumpLoad, filepath);
    if (!acquired.has_value()) {
      return ResponseFormatter::FormatError("Cannot load dump while " + ctx_.operation_coordinator->DescribeActive() +
                                            " is in progress");
    }
    operation_token = std::move(*acquired);
  }

  // Atomic test-and-set on dump_load_in_progress, bundled with a release-on-
  // scope-exit RAII guard via OperationGuard::TryAcquire.
  //
  // Do NOT split this into a separate load() + later AtomicFlagGuard —
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
  if (ctx_.dump_save_in_progress.load()) {
    loading_guard.Release();
    return ResponseFormatter::FormatError(
        "Cannot load dump while DUMP SAVE is in progress. "
        "Please wait for save to complete.");
  }
  if (ctx_.optimization_in_progress.load()) {
    loading_guard.Release();
    return ResponseFormatter::FormatError(
        "Cannot load dump while OPTIMIZE is in progress. "
        "Please wait for optimization to complete.");
  }

  // Publish the new operation before taking the generation locks below.
  // DUMP LOAD can wait there for an in-flight request, and without this
  // transition DUMP STATUS would keep reporting the previous DUMP SAVE as
  // COMPLETED for the entire wait (including its stale result filepath).
  if (ctx_.dump_progress != nullptr) {
    ctx_.dump_progress->Reset(DumpStatus::LOADING, filepath, ctx_.table_catalog->GetTables().size());
  }

  std::string previous_gtid;

#ifdef USE_MYSQL
  assert(ctx_.replication_pause_counter != nullptr);
  replication_pause::Guard replication_pause(ctx_.binlog_reader, *ctx_.replication_pause_counter,
                                             ctx_.replication_paused_for_dump, ctx_.shutdown_flag, "dump_load");
  const auto pause_result = replication_pause.Pause();
  if (pause_result.engaged) {
    mygram::utils::StructuredLog()
        .Event("replication_paused")
        .Field("operation", "dump_load")
        .Field("reason", "automatic_pause_for_consistency")
        .Field("first_pauser", pause_result.first_pauser)
        .Info();
  }

  // Capture the comparison position only after a running reader has been
  // stopped and drained. The loaded position is supplied by ReadDump from the
  // same open/read operation, so validation does not depend on a TOCTOU-prone
  // DUMP INFO pre-read.
  previous_gtid = pause_result.drained_gtid;
#endif

  mygram::utils::StructuredLog().Event("dump_load_starting").Field(log_fields::kFieldFilepath, filepath).Info();

  // The loading_guard (AtomicFlagResetGuard) was installed above immediately
  // after compare_exchange_strong succeeded. It is Release()d only on the
  // success path, after all post-load steps complete; on failure it falls
  // through to the destructor and clears dump_load_in_progress.

  // Convert table contexts to format expected by dump API
  auto converted_contexts = ctx_.table_catalog->GetDumpableContexts();

  // Block pre-existing requests and publish the complete restored table set
  // as one generation. New requests are already rejected by the loading
  // flag. Keep these locks through state replacement, replay-fence reset,
  // cache invalidation, and BM25 rebuild.
  std::vector<std::unique_lock<std::shared_mutex>> generation_locks;
  generation_locks.reserve(ctx_.table_catalog->GetTables().size());
  for (const auto& [table_name, table_ctx] : ctx_.table_catalog->GetTables()) {
    (void)table_name;
    generation_locks.emplace_back(*table_ctx->generation_mutex);
  }

  // Variables to receive loaded data
  std::string gtid;
  std::string loaded_source_server_uuid;
  std::string expected_source_server_uuid;
#ifdef USE_MYSQL
  if (ctx_.binlog_reader != nullptr) {
    expected_source_server_uuid = ctx_.binlog_reader->GetSourceServerUUID();
  }
#endif
  config::Config loaded_config;
  storage::dump_format::IntegrityError integrity_error;

  // Call dump API (auto-detects V1 or V2 format)
  auto result = storage::dump_v2::ReadDump(
      filepath, gtid, loaded_config, converted_contexts, nullptr, nullptr, &integrity_error,
      [this, &previous_gtid, &expected_source_server_uuid, &loaded_source_server_uuid](
          const config::Config& dump_config,
          const std::string& loaded_gtid) -> mygram::utils::Expected<void, mygram::utils::Error> {
        if (ctx_.full_config == nullptr) {
          // GTID validation below does not depend on the server config.
        } else {
          if (auto mismatch = FindTokenizerConfigMismatch(dump_config, *ctx_.full_config); mismatch.has_value()) {
            mygram::utils::StructuredLog()
                .Event("dump_load_rejected")
                .Field("reason", "tokenizer_config_mismatch")
                .Field("detail", *mismatch)
                .Error();
            return mygram::utils::MakeUnexpected(
                mygram::utils::MakeError(mygram::utils::ErrorCode::kStorageVersionMismatch, *mismatch));
          }
          const auto& expected_mysql = ctx_.full_config->mysql;
          const auto& dump_mysql = dump_config.mysql;
          if (dump_mysql.host != expected_mysql.host || dump_mysql.port != expected_mysql.port ||
              dump_mysql.database != expected_mysql.database) {
            const std::string detail = "dump MySQL source does not match the configured host/port/database";
            mygram::utils::StructuredLog()
                .Event("dump_load_rejected")
                .Field("reason", "mysql_source_mismatch")
                .Field("detail", detail)
                .Error();
            return mygram::utils::MakeUnexpected(
                mygram::utils::MakeError(mygram::utils::ErrorCode::kStorageVersionMismatch, detail));
          }
          if (!expected_source_server_uuid.empty() && loaded_source_server_uuid.empty()) {
            const std::string detail = "dump does not record its MySQL source server UUID";
            mygram::utils::StructuredLog()
                .Event("dump_load_rejected")
                .Field("reason", "missing_source_server_uuid")
                .Field("detail", detail)
                .Error();
            return mygram::utils::MakeUnexpected(
                mygram::utils::MakeError(mygram::utils::ErrorCode::kStorageVersionMismatch, detail));
          }
          if (!expected_source_server_uuid.empty() && loaded_source_server_uuid != expected_source_server_uuid) {
            const std::string detail = "dump MySQL source server UUID does not match the running source";
            mygram::utils::StructuredLog()
                .Event("dump_load_rejected")
                .Field("reason", "source_server_uuid_mismatch")
                .Field("detail", detail)
                .Error();
            return mygram::utils::MakeUnexpected(
                mygram::utils::MakeError(mygram::utils::ErrorCode::kStorageVersionMismatch, detail));
          }
        }

#ifdef USE_MYSQL
        if (ctx_.binlog_reader != nullptr && !previous_gtid.empty() && loaded_gtid.empty()) {
          const std::string detail = "loaded GTID='" + loaded_gtid + "', previous GTID='" + previous_gtid + "'";
          mygram::utils::StructuredLog()
              .Event("dump_load_rejected")
              .Field("reason", "empty_loaded_gtid")
              .Field("loaded_gtid", loaded_gtid)
              .Field("previous_gtid", previous_gtid)
              .Field("detail", detail)
              .Error();
          return mygram::utils::MakeUnexpected(mygram::utils::MakeError(mygram::utils::ErrorCode::kStorageDumpReadError,
                                                                        "Cannot replace a non-empty GTID: " + detail));
        }
#else
        (void)loaded_gtid;
        (void)previous_gtid;
#endif
        return {};
      },
      ctx_.full_config == nullptr
          ? storage::dump_v2::RestoreLimits{}
          : storage::dump_v2::RestoreLimits{static_cast<uint64_t>(ctx_.full_config->dump.restore_memory_budget_mb) *
                                                1024ULL * 1024ULL,
                                            static_cast<uint64_t>(ctx_.full_config->dump.restore_max_section_mb) *
                                                1024ULL * 1024ULL},
      &loaded_source_server_uuid);

  // The loading guard remains active through replication restart and cache
  // rebuild. It is released only after the success path completes, ensuring
  // dump_load_in_progress stays true if the load failed.

#ifdef USE_MYSQL
  if (result) {
    // A DUMP LOAD is an explicit position replacement even when the dump was
    // created before GTID was available. Any replay fence belongs to the
    // discarded generation and must not survive the load.
    for (const auto& [table_name, table_ctx] : ctx_.table_catalog->GetTables()) {
      (void)table_name;
      if (table_ctx->replay_watermark != nullptr) {
        std::lock_guard<std::mutex> replay_lock(table_ctx->replay_watermark->mutex);
        table_ctx->replay_watermark->snapshot_gtid.clear();
      }
    }
  }
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
#endif

  if (result) {
    // Clear search cache after successful load before replication can append
    // new mutations to the freshly loaded state.
    if (ctx_.cache_manager != nullptr) {
      ctx_.cache_manager->Clear();
    }

    // Rebuild BM25 corpus statistics from loaded documents while replication
    // is still paused. This prevents binlog-applied deltas from being
    // overwritten by the rebuild stores below.
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
        table_ctx->bm25_stats.SetCorpusStats(total_length, doc_count);
      }
    }
  }

#ifdef USE_MYSQL
  if (result) {
    auto restart_result = replication_pause.Restore();
    if (!restart_result) {
      mygram::utils::StructuredLog()
          .Event("replication_restart_failed")
          .Field("operation", "dump_load")
          .FieldError(restart_result.error())
          .Error();
      // The dump contents are already installed, but reporting success would
      // hide that replication remains stopped.
      result = mygram::utils::MakeUnexpected(restart_result.error());
    }
  }
#endif

  if (result) {
    mygram::utils::StructuredLog()
        .Event("dump_load_completed")
        .Field(log_fields::kFieldFilepath, filepath)
        .Field(log_fields::kFieldGtid, gtid)
        .Info();
    if (ctx_.dump_progress != nullptr) {
      ctx_.dump_progress->Complete(filepath);
    }
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
  if (ctx_.dump_progress != nullptr) {
    ctx_.dump_progress->Fail(error_msg);
  }
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
    return ResponseFormatter::FormatStatus("DUMP_VERIFIED " + ResponseFormatter::SanitizeDelimitedField(filepath));
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
  result << protocol::kOkDumpInfoPrefix << " " << ResponseFormatter::SanitizeDelimitedField(filepath) << "\r\n";
  result << "version: " << info.version << "\r\n";
  result << "gtid: " << ResponseFormatter::SanitizeDelimitedField(info.gtid) << "\r\n";
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
  result << protocol::kOkDumpStatusPrefix << "\r\n";

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
    auto progress_snapshot = ctx_.dump_progress->GetSnapshot();
    switch (progress_snapshot.status) {
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
    if (progress_snapshot.status != DumpStatus::IDLE) {
      result << "filepath: " << ResponseFormatter::SanitizeDelimitedField(progress_snapshot.filepath) << "\r\n";
      result << "tables_processed: " << progress_snapshot.tables_processed << "\r\n";
      result << "tables_total: " << progress_snapshot.tables_total << "\r\n";

      if (!progress_snapshot.current_table.empty()) {
        result << "current_table: " << ResponseFormatter::SanitizeDelimitedField(progress_snapshot.current_table)
               << "\r\n";
      }

      // Show elapsed time
      result << "elapsed_seconds: " << std::fixed << std::setprecision(2) << progress_snapshot.elapsed_seconds
             << "\r\n";

      // Show error message if failed
      if (progress_snapshot.status == DumpStatus::FAILED && !progress_snapshot.error_message.empty()) {
        result << "error: " << ResponseFormatter::SanitizeDelimitedField(progress_snapshot.error_message) << "\r\n";
      }

      // Show last result filepath if completed
      if (progress_snapshot.status == DumpStatus::COMPLETED && !progress_snapshot.last_result_filepath.empty()) {
        result << "result_filepath: "
               << ResponseFormatter::SanitizeDelimitedField(progress_snapshot.last_result_filepath) << "\r\n";
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
