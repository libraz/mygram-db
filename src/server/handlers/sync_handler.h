/**
 * @file sync_handler.h
 * @brief Handler for SYNC commands
 */

#pragma once

#ifdef USE_MYSQL

#include "server/handlers/command_handler.h"

namespace mygramdb::server {

// Forward declarations
struct SyncState;
class TcpServer;

/**
 * @brief Handler for SYNC commands
 *
 * Handles SYNC and SYNC STATUS commands for manual snapshot synchronization.
 */
class SyncHandler : public CommandHandler {
 public:
  SyncHandler(HandlerContext& ctx, TcpServer& server) : CommandHandler(ctx), server_(server) {}

  std::string Handle(const query::Query& query, ConnectionContext& conn_ctx) override;

 private:
  /**
   * @brief Handle SYNC command (trigger snapshot build)
   */
  std::string HandleSync(const query::Query& query);

  /**
   * @brief Handle SYNC STATUS command (query sync progress)
   */
  std::string HandleSyncStatus(const query::Query& query);

  TcpServer& server_;
};

}  // namespace mygramdb::server

#endif  // USE_MYSQL
