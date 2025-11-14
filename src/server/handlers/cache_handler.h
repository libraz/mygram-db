/**
 * @file cache_handler.h
 * @brief Handler for CACHE commands
 */

#pragma once

#include <string>

#include "query/query_parser.h"
#include "server/handlers/command_handler.h"
#include "server/server_types.h"

namespace mygramdb::server {

/**
 * @brief Handler for CACHE commands
 *
 * Handles:
 * - CACHE CLEAR [table] - Clear all cache or table-specific cache
 * - CACHE STATS - Show cache statistics
 * - CACHE ENABLE - Enable cache
 * - CACHE DISABLE - Disable cache
 */
class CacheHandler : public CommandHandler {
 public:
  /**
   * @brief Constructor
   * @param ctx Handler context
   */
  explicit CacheHandler(HandlerContext& ctx) : CommandHandler(ctx) {}

  /**
   * @brief Handle CACHE command
   * @param query Parsed query
   * @param conn_ctx Connection context
   * @return Response string
   */
  std::string Handle(const query::Query& query, ConnectionContext& conn_ctx) override;

 private:
  /**
   * @brief Handle CACHE CLEAR command
   * @param query Parsed query
   * @return Response string
   */
  std::string HandleClear(const query::Query& query);

  /**
   * @brief Handle CACHE STATS command
   * @return Response string
   */
  std::string HandleStats();

  /**
   * @brief Handle CACHE ENABLE command
   * @return Response string
   */
  std::string HandleEnable();

  /**
   * @brief Handle CACHE DISABLE command
   * @return Response string
   */
  std::string HandleDisable();
};

}  // namespace mygramdb::server
