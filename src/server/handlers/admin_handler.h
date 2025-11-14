/**
 * @file admin_handler.h
 * @brief Handler for administrative commands (INFO, CONFIG)
 */

#pragma once

#include "server/handlers/command_handler.h"

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
  static std::string HandleConfigVerify(const std::string& filepath);
};

}  // namespace mygramdb::server
