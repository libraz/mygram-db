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
        auto* reader = static_cast<mysql::BinlogReader*>(ctx_.binlog_reader);
        if (reader->IsRunning()) {
          spdlog::info("Stopping binlog replication by user request");
          reader->Stop();
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
      // Check if any table is currently syncing
      if (ctx_.sync_manager != nullptr && ctx_.sync_manager->IsAnySyncing()) {
        return ResponseFormatter::FormatError(
            "Cannot start replication while SYNC is in progress. "
            "SYNC will automatically start replication when complete.");
      }

      // Check if DUMP LOAD is in progress (block REPLICATION START)
      if (ctx_.loading.load()) {
        return ResponseFormatter::FormatError(
            "Cannot start replication while DUMP LOAD is in progress. "
            "Please wait for load to complete.");
      }

      // Note: DUMP SAVE (read_only flag) is allowed during REPLICATION START
      // as they are both read operations and can run concurrently

      if (ctx_.binlog_reader != nullptr) {
        auto* reader = static_cast<mysql::BinlogReader*>(ctx_.binlog_reader);
        if (!reader->IsRunning()) {
          // Check if GTID is set (required for replication)
          std::string current_gtid = reader->GetCurrentGTID();
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

          if (reader->Start()) {
            return ResponseFormatter::FormatReplicationStartResponse();
          }

          std::string error = reader->GetLastError();
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
