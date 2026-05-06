/**
 * @file request_dispatcher.cpp
 * @brief Implementation of RequestDispatcher
 */
// Logging is exclusively via mygram::utils::StructuredLog. Direct spdlog usage is prohibited in server code.

#include "server/request_dispatcher.h"

#include "server/handlers/command_handler.h"
#include "server/response_formatter.h"
#include "server/table_catalog.h"
#include "utils/structured_log.h"

namespace mygramdb::server {

RequestDispatcher::RequestDispatcher(HandlerContext& ctx, const ServerConfig& config)
    : ctx_(ctx),
      config_(config),
      max_query_length_(config.max_query_length <= 0 ? 0 : static_cast<size_t>(config.max_query_length)) {}

void RequestDispatcher::RegisterHandler(query::QueryType type, CommandHandler* handler) {
  handlers_[type] = handler;
}

bool RequestDispatcher::HasHandler(query::QueryType type) const {
  auto it = handlers_.find(type);
  return it != handlers_.end() && it->second != nullptr;
}

std::string RequestDispatcher::Dispatch(const std::string& request, ConnectionContext& conn_ctx) {
  // Untrusted client input may contain log-injection sequences. Truncation also
  // bounds log volume on long requests. The full byte length is preserved in a
  // separate numeric field so log consumers can detect truncation and never
  // assume the logged string is complete.
  std::string truncated_request = request.substr(0, mygram::utils::kMaxQueryLogLength);
  if (request.size() > mygram::utils::kMaxQueryLogLength) {
    truncated_request += "...";
  }
  mygram::utils::StructuredLog()
      .Event("request_dispatching")
      .Field("request", truncated_request)
      .Field("request_full_length", static_cast<int64_t>(request.size()))
      .Debug();

  // Create a thread-local parser for this request
  query::QueryParser parser;
  parser.SetMaxQueryLength(max_query_length_);

  // Parse query
  auto query = parser.Parse(request);

  if (!query) {
    return ResponseFormatter::FormatError(query.error().message());
  }

  // Apply configured default LIMIT if not explicitly specified
  if (!query->limit_explicit && (query->type == query::QueryType::SEARCH)) {
    query->limit = static_cast<uint32_t>(config_.default_limit);
  }

  // Increment command statistics. IncrementRequests bumps total_requests in
  // the INFO output; without it, total_requests is stuck at 0 even though
  // commands are being dispatched.
  ctx_.stats.IncrementCommand(query->type);
  ctx_.stats.IncrementRequests();

  // Catalog existence is intentionally NOT validated here. Each handler's
  // GetTableContext() (CommandHandler::GetTableContext) performs the same
  // lookup and returns a more specific error code (kTableNotFound vs the
  // dispatcher's generic string). Doing both costs an extra catalog hit per
  // request without changing semantics; keeping it in the handler keeps the
  // error wording consistent across TCP and HTTP paths.

  // Find handler
  auto handler_iter = handlers_.find(query->type);
  if (handler_iter == handlers_.end()) {
    return ResponseFormatter::FormatError("Unknown query type");
  }

  // Dispatch to handler. Handlers are required by contract to convert errors
  // into ResponseFormatter::FormatError(...) strings via Expected<T, Error>,
  // so they must not throw. Wrapping in try/catch here would violate the
  // project's "no exceptions" policy and silently mask handler bugs.
  return handler_iter->second->Handle(*query, conn_ctx);
}

}  // namespace mygramdb::server
