/**
 * @file document_handler.h
 * @brief Handler for GET command
 */

#pragma once

#include "server/handlers/command_handler.h"

namespace mygramdb::server {

/**
 * @brief Handler for GET queries
 *
 * Retrieves a single document by primary key.
 */
class DocumentHandler : public CommandHandler {
 public:
  explicit DocumentHandler(HandlerContext& ctx) : CommandHandler(ctx) {}

  std::string Handle(const query::Query& query, ConnectionContext& conn_ctx) override;
};

}  // namespace mygramdb::server
