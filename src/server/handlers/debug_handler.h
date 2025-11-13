/**
 * @file debug_handler.h
 * @brief Handler for debug and maintenance commands
 */

#pragma once

#include "server/handlers/command_handler.h"

namespace mygramdb::server {

/**
 * @brief Handler for debug and maintenance commands
 *
 * Handles DEBUG ON/OFF and OPTIMIZE commands.
 */
class DebugHandler : public CommandHandler {
 public:
  explicit DebugHandler(HandlerContext& ctx) : CommandHandler(ctx) {}

  std::string Handle(const query::Query& query, ConnectionContext& conn_ctx) override;
};

}  // namespace mygramdb::server
