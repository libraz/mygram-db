/**
 * @file sync_handler.h
 * @brief Handler for SYNC commands
 */

#pragma once

#ifdef USE_MYSQL

#include <memory>

#include "server/handlers/command_handler.h"
#include "utils/error.h"
#include "utils/expected.h"

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
   * @brief Create SyncHandler with dependencies
   *
   * @param ctx Handler context with shared server state
   * @param sync_manager SyncOperationManager for SYNC operations (non-owning pointer, MUST be non-null)
   *
   * @return Expected with unique_ptr to SyncHandler, or Error if sync_manager is nullptr
   */
  static mygram::utils::Expected<std::unique_ptr<SyncHandler>, mygram::utils::Error> Create(
      HandlerContext& ctx, SyncOperationManager* sync_manager) {
    if (sync_manager == nullptr) {
      // SyncHandler is a SYNC/Index-domain component (its operations live in
      // the 4000-4999 Index/Business range). Reporting the null-dependency
      // error with kSyncManagerNull (4014) keeps the error code aligned with
      // the module that surfaces it, instead of borrowing a 6xxx Network code.
      return mygram::utils::MakeUnexpected(mygram::utils::MakeError(mygram::utils::ErrorCode::kSyncManagerNull,
                                                                    "SyncHandler: sync_manager must be non-null"));
    }
    return std::unique_ptr<SyncHandler>(new SyncHandler(ctx, sync_manager));
  }

  std::string Handle(const query::Query& query, ConnectionContext& conn_ctx) override;

 private:
  /**
   * @brief Private constructor - use Create() factory method instead
   */
  SyncHandler(HandlerContext& ctx, SyncOperationManager* sync_manager)
      : CommandHandler(ctx), sync_manager_(sync_manager) {}

  /**
   * @brief Handle SYNC command (trigger snapshot build)
   */
  std::string HandleSync(const query::Query& query);

  /**
   * @brief Handle SYNC STATUS command (query sync progress)
   */
  std::string HandleSyncStatus(const query::Query& query);

  /**
   * @brief Handle SYNC STOP command (cancel ongoing SYNC)
   */
  std::string HandleSyncStop(const query::Query& query);

  SyncOperationManager* sync_manager_;  // Non-owning pointer
};

}  // namespace mygramdb::server

#endif  // USE_MYSQL
