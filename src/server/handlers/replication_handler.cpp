/**
 * @file replication_handler.cpp
 * @brief Handler for replication commands
 */

#include "server/handlers/replication_handler.h"

#include <spdlog/spdlog.h>

#include "server/sync_operation_manager.h"
#include "utils/structured_log.h"

#ifdef USE_MYSQL
#include "mysql/binlog_reader.h"
#endif

namespace mygramdb::server {

std::string ReplicationHandler::Handle(const query::Query& query, ConnectionContext& conn_ctx) {
  (void)conn_ctx;  // Unused for replication commands

  switch (query.type) {
    case query::QueryType::REPLICATION_STATUS:
      return ResponseFormatter::FormatReplicationStatusResponse(ctx_.binlog_reader);

    case query::QueryType::REPLICATION_STOP: {
#ifdef USE_MYSQL
      if (ctx_.binlog_reader != nullptr) {
        if (ctx_.binlog_reader->IsRunning()) {
          mygram::utils::StructuredLog().Event("replication_stopping").Field("source", "user_request").Info();
          ctx_.binlog_reader->Stop();
          return ResponseFormatter::FormatReplicationStopResponse();
        }
        return ResponseFormatter::FormatError("Replication is not running");
      }
      return ResponseFormatter::FormatError("Replication is not configured");
#else
      return ResponseFormatter::FormatError("MySQL support not compiled");
#endif
    }

    case query::QueryType::REPLICATION_START: {
#ifdef USE_MYSQL
      // Check if MySQL reconnection is in progress (block manual restart)
      if (ctx_.mysql_reconnecting.load()) {
        return ResponseFormatter::FormatError(
            "Cannot start replication while MySQL reconnection is in progress. "
            "Replication will automatically restart after reconnection completes.");
      }

      // Check if replication is paused for DUMP operation (block manual restart)
      if (ctx_.replication_paused_for_dump.load()) {
        return ResponseFormatter::FormatError(
            "Cannot start replication while DUMP SAVE/LOAD is in progress. "
            "Replication will automatically restart after DUMP completes.");
      }

      // Check if any table is currently syncing
      if (ctx_.sync_manager != nullptr && ctx_.sync_manager->IsAnySyncing()) {
        return ResponseFormatter::FormatError(
            "Cannot start replication while SYNC is in progress. "
            "SYNC will automatically start replication when complete.");
      }

      // Check if DUMP LOAD is in progress (block REPLICATION START)
      if (ctx_.dump_load_in_progress.load()) {
        return ResponseFormatter::FormatError(
            "Cannot start replication while DUMP LOAD is in progress. "
            "Please wait for load to complete.");
      }

      // Check if DUMP SAVE is in progress (block REPLICATION START)
      if (ctx_.dump_save_in_progress.load()) {
        return ResponseFormatter::FormatError(
            "Cannot start replication while DUMP SAVE is in progress. "
            "Please wait for save to complete.");
      }

      if (ctx_.binlog_reader != nullptr) {
        if (!ctx_.binlog_reader->IsRunning()) {
          // Check if GTID is set (required for replication)
          std::string current_gtid = ctx_.binlog_reader->GetCurrentGTID();
          if (current_gtid.empty()) {
            return ResponseFormatter::FormatError(
                "Cannot start replication without GTID position. "
                "Please run SYNC command first to establish initial position.");
          }

          mygram::utils::StructuredLog()
              .Event("replication_start")
              .Field("source", "user_request")
              .Field("gtid", current_gtid)
              .Info();

          if (ctx_.binlog_reader->Start()) {
            return ResponseFormatter::FormatReplicationStartResponse();
          }

          std::string error = ctx_.binlog_reader->GetLastError();
          mygram::utils::StructuredLog()
              .Event("replication_start_failed")
              .Field("source", "user_request")
              .Field("gtid", current_gtid)
              .Field("error", error)
              .Error();
          return ResponseFormatter::FormatError("Failed to start replication: " + error);
        }
        return ResponseFormatter::FormatError("Replication is already running");
      }
      return ResponseFormatter::FormatError("Replication is not configured");
#else
      return ResponseFormatter::FormatError("MySQL support not compiled");
#endif
    }

    default:
      return ResponseFormatter::FormatError("Invalid query type for ReplicationHandler");
  }
}

}  // namespace mygramdb::server
