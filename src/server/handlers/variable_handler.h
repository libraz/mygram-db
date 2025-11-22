/**
 * @file variable_handler.h
 * @brief Handler for variable commands (SET, SHOW VARIABLES)
 */

#pragma once

#include <map>
#include <string>

#include "config/runtime_variable_manager.h"
#include "server/handlers/command_handler.h"

namespace mygramdb::server {

/**
 * @brief Handler for runtime variable commands
 *
 * Handles MySQL-compatible SET and SHOW VARIABLES commands.
 *
 * Supported commands:
 * - SET variable = value [, variable2 = value2 ...]
 * - SHOW VARIABLES [LIKE 'pattern']
 *
 * Examples:
 * - SET logging.level = 'debug'
 * - SET api.default_limit = 50, cache.enabled = true
 * - SHOW VARIABLES
 * - SHOW VARIABLES LIKE 'logging%'
 */
class VariableHandler : public CommandHandler {
 public:
  explicit VariableHandler(HandlerContext& ctx) : CommandHandler(ctx) {}

  std::string Handle(const query::Query& query, ConnectionContext& conn_ctx) override;

 private:
  /**
   * @brief Handle SET command
   * @param query Parsed query
   * @return Response string
   */
  std::string HandleSet(const query::Query& query);

  /**
   * @brief Handle SHOW VARIABLES command
   * @param query Parsed query
   * @return Response string (MySQL table format)
   */
  std::string HandleShowVariables(const query::Query& query);

  /**
   * @brief Format variables in MySQL table format
   * @param variables Map of variable_name -> (value, is_mutable)
   * @return Formatted table string
   */
  static std::string FormatVariablesTable(const std::map<std::string, config::VariableInfo>& variables);

  /**
   * @brief Check if pattern matches value (MySQL LIKE operator)
   * @param value String to match against
   * @param pattern Pattern with % wildcards
   * @return True if matches
   */
  static bool MatchLikePattern(const std::string& value, const std::string& pattern);
};

}  // namespace mygramdb::server
