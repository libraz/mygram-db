/**
 * @file facet_handler.h
 * @brief Handler for FACET command
 */

#pragma once

#include <string>

#include "server/handlers/command_handler.h"

namespace mygramdb::server {

/**
 * @brief Handler for FACET queries
 *
 * Enumerates distinct values of a filter column with document counts.
 * Optionally restricted to search results with full clause support.
 */
class FacetHandler : public CommandHandler {
 public:
  explicit FacetHandler(HandlerContext& ctx) : CommandHandler(ctx) {}

  std::string Handle(const query::Query& query, ConnectionContext& conn_ctx) override;
};

}  // namespace mygramdb::server
