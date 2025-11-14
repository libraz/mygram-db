/**
 * @file request_dispatcher.h
 * @brief Request dispatcher for routing queries to handlers
 */

#pragma once

#include <string>
#include <unordered_map>

#include "query/query_parser.h"
#include "server/server_types.h"

namespace mygramdb::server {

// Forward declarations
class TableCatalog;
class CommandHandler;

/**
 * @brief Request dispatcher
 *
 * This class parses queries and routes them to registered command handlers.
 * It contains pure application logic with no network dependencies.
 *
 * Key responsibilities:
 * - Parse incoming request strings
 * - Validate queries (table existence, etc.)
 * - Route to appropriate command handlers
 * - Handle errors and exceptions
 *
 * Design principles:
 * - Pure logic, no threading or I/O
 * - Easy to unit test
 * - Handler registry pattern (extensible)
 * - Clear separation from network layer
 */
class RequestDispatcher {
 public:
  /**
   * @brief Construct a RequestDispatcher
   * @param ctx Handler context (contains dependencies)
   * @param config Server configuration
   */
  RequestDispatcher(HandlerContext& ctx, ServerConfig config);

  // Disable copy and move
  RequestDispatcher(const RequestDispatcher&) = delete;
  RequestDispatcher& operator=(const RequestDispatcher&) = delete;
  RequestDispatcher(RequestDispatcher&&) = delete;
  RequestDispatcher& operator=(RequestDispatcher&&) = delete;

  ~RequestDispatcher() = default;

  /**
   * @brief Register a command handler
   * @param type Query type
   * @param handler Handler instance (not owned, must outlive dispatcher)
   */
  void RegisterHandler(query::QueryType type, CommandHandler* handler);

  /**
   * @brief Dispatch a request to appropriate handler
   * @param request Request string
   * @param conn_ctx Connection context
   * @return Response string
   */
  std::string Dispatch(const std::string& request, ConnectionContext& conn_ctx);

 private:
  HandlerContext& ctx_;
  ServerConfig config_;
  query::QueryParser parser_;
  std::unordered_map<query::QueryType, CommandHandler*> handlers_;
};

}  // namespace mygramdb::server
