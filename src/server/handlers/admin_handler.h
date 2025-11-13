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
};

}  // namespace mygramdb::server
