/**
 * @file replication_handler.h
 * @brief Handler for replication commands
 */

#pragma once

#include "server/handlers/command_handler.h"

namespace mygramdb::server {

/**
 * @brief Handler for replication commands
 *
 * Handles REPLICATION STATUS, REPLICATION STOP, and REPLICATION START commands.
 */
class ReplicationHandler : public CommandHandler {
 public:
  explicit ReplicationHandler(HandlerContext& ctx) : CommandHandler(ctx) {}

  std::string Handle(const query::Query& query, ConnectionContext& conn_ctx) override;
};

}  // namespace mygramdb::server
