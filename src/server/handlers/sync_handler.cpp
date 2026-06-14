/**
 * @file sync_handler.cpp
 * @brief SYNC command handler implementation
 */

#ifdef USE_MYSQL

#include "server/handlers/sync_handler.h"

#include "query/query_parser.h"
#include "server/response_formatter.h"
#include "server/sync_operation_manager.h"

namespace mygramdb::server {

std::string SyncHandler::Handle(const query::Query& query, ConnectionContext& conn_ctx) {
  if (query.type == query::QueryType::SYNC) {
    return HandleSync(query);
  }
  if (query.type == query::QueryType::SYNC_STATUS) {
    return HandleSyncStatus(query);
  }
  if (query.type == query::QueryType::SYNC_STOP) {
    return HandleSyncStop(query);
  }
  return ResponseFormatter::FormatError("Unknown SYNC command");
}

std::string SyncHandler::HandleSync(const query::Query& query) {
  auto resolved = ResolveTableName(query.table);
  if (!resolved) {
    return ResponseFormatter::FormatError(resolved.error().message());
  }
  auto result = sync_manager_->StartSync(*resolved);
  if (!result) {
    return ResponseFormatter::FormatError(result.error().message());
  }
  return *result;
}

std::string SyncHandler::HandleSyncStatus(const query::Query& /*query*/) {
  return sync_manager_->GetSyncStatus();
}

std::string SyncHandler::HandleSyncStop(const query::Query& query) {
  // SYNC STOP with an empty table stops all in-flight syncs; only resolve when
  // a specific table was named so the stop-all path keeps working.
  if (query.table.empty()) {
    return sync_manager_->StopSync(query.table);
  }
  auto resolved = ResolveTableName(query.table);
  if (!resolved) {
    return ResponseFormatter::FormatError(resolved.error().message());
  }
  return sync_manager_->StopSync(*resolved);
}

}  // namespace mygramdb::server

#endif  // USE_MYSQL
