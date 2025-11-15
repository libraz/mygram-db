/**
 * @file request_dispatcher.cpp
 * @brief Implementation of RequestDispatcher
 */

#include "server/request_dispatcher.h"

#include <spdlog/spdlog.h>

#include "server/handlers/command_handler.h"
#include "server/response_formatter.h"
#include "server/table_catalog.h"

namespace mygramdb::server {

RequestDispatcher::RequestDispatcher(HandlerContext& ctx, const ServerConfig& config)
    : ctx_(ctx),
      config_(config),
      max_query_length_(config.max_query_length <= 0 ? 0 : static_cast<size_t>(config.max_query_length)) {}

void RequestDispatcher::RegisterHandler(query::QueryType type, CommandHandler* handler) {
  handlers_[type] = handler;
}

std::string RequestDispatcher::Dispatch(const std::string& request, ConnectionContext& conn_ctx) {
  spdlog::debug("Dispatching request: {}", request);

  // Create a thread-local parser for this request
  query::QueryParser parser;
  parser.SetMaxQueryLength(max_query_length_);

  // Parse query
  auto query = parser.Parse(request);

  if (!query.IsValid()) {
    return ResponseFormatter::FormatError(parser.GetError());
  }

  // Apply configured default LIMIT if not explicitly specified
  if (!query.limit_explicit && (query.type == query::QueryType::SEARCH)) {
    query.limit = static_cast<uint32_t>(config_.default_limit);
  }

  // Increment command statistics
  ctx_.stats.IncrementCommand(query.type);

  // For queries that require a table, validate table exists
  if (!query.table.empty()) {
    // Use TableCatalog if available (new path)
    if (ctx_.table_catalog != nullptr) {
      if (!ctx_.table_catalog->TableExists(query.table)) {
        return ResponseFormatter::FormatError("Table not found: " + query.table);
      }
    } else {
      // Fallback to legacy path
      auto table_iter = ctx_.table_contexts.find(query.table);
      if (table_iter == ctx_.table_contexts.end()) {
        return ResponseFormatter::FormatError("Table not found: " + query.table);
      }
    }
  }

  // Find handler
  auto handler_iter = handlers_.find(query.type);
  if (handler_iter == handlers_.end()) {
    return ResponseFormatter::FormatError("Unknown query type");
  }

  // Dispatch to handler
  try {
    return handler_iter->second->Handle(query, conn_ctx);
  } catch (const std::exception& e) {
    return ResponseFormatter::FormatError(std::string("Exception: ") + e.what());
  }
}

}  // namespace mygramdb::server
