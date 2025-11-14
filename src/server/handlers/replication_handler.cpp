/**
 * @file replication_handler.cpp
 * @brief Handler for replication commands
 */

#include "server/handlers/replication_handler.h"

#include <spdlog/spdlog.h>

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
      {
        std::lock_guard<std::mutex> lock(ctx_.syncing_tables_mutex);
        if (!ctx_.syncing_tables.empty()) {
          return ResponseFormatter::FormatError(
              "Cannot start replication while SYNC is in progress. "
              "SYNC will automatically start replication when complete.");
        }
      }

      if (ctx_.binlog_reader != nullptr) {
        auto* reader = static_cast<mysql::BinlogReader*>(ctx_.binlog_reader);
        if (!reader->IsRunning()) {
          spdlog::info("Starting binlog replication by user request");
          if (reader->Start()) {
            return ResponseFormatter::FormatReplicationStartResponse();
          }
          return ResponseFormatter::FormatError("Failed to start replication: " + reader->GetLastError());
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
