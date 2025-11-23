/**
 * @file sync_handler.cpp
 * @brief SYNC command handler implementation
 */

#ifdef USE_MYSQL

#include "server/handlers/sync_handler.h"

#include <spdlog/spdlog.h>

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
  if (sync_manager_ == nullptr) {
    return ResponseFormatter::FormatError("SYNC manager not initialized");
  }
  return sync_manager_->StartSync(query.table);
}

std::string SyncHandler::HandleSyncStatus(const query::Query& query) {
  if (sync_manager_ == nullptr) {
    return "status=IDLE message=\"SYNC manager not initialized\"";
  }
  return sync_manager_->GetSyncStatus();
}

std::string SyncHandler::HandleSyncStop(const query::Query& query) {
  if (sync_manager_ == nullptr) {
    return ResponseFormatter::FormatError("SYNC manager not initialized");
  }
  return sync_manager_->StopSync(query.table);
}

}  // namespace mygramdb::server

#endif  // USE_MYSQL
