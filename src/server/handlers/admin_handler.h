/**
 * @file admin_handler.h
 * @brief Handler for administrative commands (INFO, CONFIG)
 */

#pragma once

#include "server/handlers/command_handler.h"
#include "server/statistics_service.h"

namespace mygramdb::server {

/**
 * @brief Handler for administrative commands
 *
 * Handles INFO and CONFIG commands for server administration.
 */
class AdminHandler : public CommandHandler {
 public:
  explicit AdminHandler(HandlerContext& ctx) : CommandHandler(ctx) {}

  std::string Handle(const query::Query& query, ConnectionContext& conn_ctx) override;

 private:
  /// INFO reports memory accounting derived by walking retained document and
  /// index data under the store's shared lock, which excludes the writer that
  /// replication apply needs. A monitoring agent polls far faster than those
  /// figures can meaningfully move, so polls share a bounded snapshot instead
  /// of each repeating the walk.
  StatisticsSnapshotCache statistics_snapshot_cache_;

  /**
   * @brief Handle CONFIG HELP command
   * @param path Configuration path (empty for root)
   * @return Response string
   */
  static std::string HandleConfigHelp(const std::string& path);

  /**
   * @brief Handle CONFIG SHOW command
   * @param path Configuration path (empty for all)
   * @return Response string
   */
  std::string HandleConfigShow(const std::string& path);

  /**
   * @brief Handle CONFIG VERIFY command
   * @param filepath Path to configuration file
   * @return Response string
   */
  std::string HandleConfigVerify(const std::string& filepath) const;
};

}  // namespace mygramdb::server
