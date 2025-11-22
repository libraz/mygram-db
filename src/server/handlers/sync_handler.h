/**
 * @file sync_handler.h
 * @brief Handler for SYNC commands
 */

#pragma once

#ifdef USE_MYSQL

#include "server/handlers/command_handler.h"

namespace mygramdb::server {

// Forward declarations
class SyncOperationManager;

/**
 * @brief Handler for SYNC commands
 *
 * Handles SYNC and SYNC STATUS commands for manual snapshot synchronization.
 *
 * Design Pattern: Dependency Injection
 * - Takes SyncOperationManager* instead of TcpServer&
 * - Eliminates circular dependency between handlers and server
 * - Handler depends only on what it actually needs
 */
class SyncHandler : public CommandHandler {
 public:
  /**
   * @brief Construct SyncHandler with dependencies
   *
   * @param ctx Handler context with shared server state
   * @param sync_manager SyncOperationManager for SYNC operations (non-owning pointer, MUST be non-null)
   *
   * @throws std::invalid_argument if sync_manager is nullptr
   */
  SyncHandler(HandlerContext& ctx, SyncOperationManager* sync_manager)
      : CommandHandler(ctx), sync_manager_(sync_manager) {
    if (sync_manager_ == nullptr) {
      throw std::invalid_argument("SyncHandler: sync_manager must be non-null");
    }
  }

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

  SyncOperationManager* sync_manager_;  // Non-owning pointer
};

}  // namespace mygramdb::server

#endif  // USE_MYSQL
