/**
 * @file request_dispatcher.cpp
 * @brief Implementation of RequestDispatcher
 */
// Logging is exclusively via mygram::utils::StructuredLog. Direct spdlog usage is prohibited in server code.

#include "server/request_dispatcher.h"

#include <algorithm>
#include <cctype>

#include "server/handlers/command_handler.h"
#include "server/rate_limiter.h"
#include "server/response_formatter.h"
#include "server/table_catalog.h"
#include "utils/structured_log.h"

namespace mygramdb::server {

namespace {

bool IsAdministrativeCommand(query::QueryType type) {
  switch (type) {
    case query::QueryType::DUMP_SAVE:
    case query::QueryType::DUMP_LOAD:
    case query::QueryType::DUMP_VERIFY:
    case query::QueryType::DUMP_INFO:
    case query::QueryType::DUMP_STATUS:
    case query::QueryType::REPLICATION_STATUS:
    case query::QueryType::REPLICATION_STOP:
    case query::QueryType::REPLICATION_START:
    case query::QueryType::SYNC:
    case query::QueryType::SYNC_STATUS:
    case query::QueryType::SYNC_STOP:
    case query::QueryType::CONFIG_HELP:
    case query::QueryType::CONFIG_SHOW:
    case query::QueryType::CONFIG_VERIFY:
    case query::QueryType::OPTIMIZE:
    case query::QueryType::DEBUG_ON:
    case query::QueryType::DEBUG_OFF:
    case query::QueryType::CACHE_CLEAR:
    case query::QueryType::CACHE_STATS:
    case query::QueryType::CACHE_ENABLE:
    case query::QueryType::CACHE_DISABLE:
    case query::QueryType::SET:
    case query::QueryType::SHOW_VARIABLES:
      return true;
    default:
      return false;
  }
}

bool ConstantTimeEqual(std::string_view lhs, std::string_view rhs) {
  size_t difference = lhs.size() ^ rhs.size();
  const size_t length = std::max(lhs.size(), rhs.size());
  for (size_t i = 0; i < length; ++i) {
    const unsigned char left = i < lhs.size() ? static_cast<unsigned char>(lhs[i]) : 0;
    const unsigned char right = i < rhs.size() ? static_cast<unsigned char>(rhs[i]) : 0;
    difference |= static_cast<size_t>(left ^ right);
  }
  return difference == 0;
}

bool IsAuthCommandForLogging(std::string_view request) {
  size_t offset = 0;
  while (offset < request.size() && std::isspace(static_cast<unsigned char>(request[offset]))) {
    ++offset;
  }

  constexpr std::string_view kAuth = "AUTH";
  if (request.size() - offset < kAuth.size()) {
    return false;
  }
  for (size_t i = 0; i < kAuth.size(); ++i) {
    if (std::toupper(static_cast<unsigned char>(request[offset + i])) != kAuth[i]) {
      return false;
    }
  }

  return offset + kAuth.size() == request.size() ||
         std::isspace(static_cast<unsigned char>(request[offset + kAuth.size()]));
}

}  // namespace

RequestDispatcher::RequestDispatcher(HandlerContext& ctx, const ServerConfig& config)
    : ctx_(ctx),
      default_limit_(config.default_limit),
      max_query_length_(config.max_query_length <= 0 ? 0 : static_cast<size_t>(config.max_query_length)),
      admin_token_(config.admin_token) {}

void RequestDispatcher::RegisterHandler(query::QueryType type, CommandHandler* handler) {
  handlers_[type] = handler;
}

bool RequestDispatcher::HasHandler(query::QueryType type) const {
  auto it = handlers_.find(type);
  return it != handlers_.end() && it->second != nullptr;
}

std::string RequestDispatcher::Dispatch(const std::string& request, ConnectionContext& conn_ctx) {
  // Count every received TCP request, including rate-limited and malformed
  // input, so this matches the HTTP surface's request accounting.
  ctx_.stats.IncrementRequests();

  // An unavailable peer address must share a conservative fallback bucket,
  // not disable rate limiting for the connection.
  static const std::string kUnknownPeer = "unknown";
  const std::string& rate_limit_key = conn_ctx.client_ip.empty() ? kUnknownPeer : conn_ctx.client_ip;
  if (ctx_.rate_limiter != nullptr && !ctx_.rate_limiter->AllowRequest(rate_limit_key)) {
    ctx_.stats.IncrementRequestsDeniedRateLimitTcp();
    const auto decision = ctx_.rate_limiter->RecordDenialLog("tcp:" + rate_limit_key);
    if (decision.should_log) {
      mygram::utils::StructuredLog()
          .Event("rate_limit_exceeded")
          .Field("client_ip", rate_limit_key)
          .Field("suppressed_since_last_log", decision.suppressed_count)
          .Warn();
    }
    return ResponseFormatter::FormatError("Rate limit exceeded");
  }

  // Untrusted client input may contain log-injection sequences. Truncation also
  // bounds log volume on long requests. The full byte length is preserved in a
  // separate numeric field so log consumers can detect truncation and never
  // assume the logged string is complete.
  std::string truncated_request = IsAuthCommandForLogging(request) ? "AUTH <redacted>"
                                                                   : mygram::utils::StructuredLog::TruncateUtf8Prefix(
                                                                         request, mygram::utils::kMaxQueryLogLength);
  if (!IsAuthCommandForLogging(request) && request.size() > mygram::utils::kMaxQueryLogLength) {
    truncated_request += "...";
  }
  mygram::utils::StructuredLog()
      .Event("request_dispatching")
      .Field("request", truncated_request)
      .Field("request_full_length", static_cast<int64_t>(request.size()))
      .Debug();

  // Create a thread-local parser for this request
  query::QueryParser parser;
  parser.SetMaxQueryLength(max_query_length_.load(std::memory_order_acquire));

  // Parse query
  auto query = parser.Parse(request);

  if (!query) {
    return ResponseFormatter::FormatError(query.error().message());
  }

  if (query->type == query::QueryType::AUTH) {
    if (admin_token_.empty() || !ConstantTimeEqual(query->auth_token, admin_token_)) {
      conn_ctx.admin_authenticated.store(false, std::memory_order_release);
      return ResponseFormatter::FormatError("Authentication failed");
    }
    conn_ctx.admin_authenticated.store(true, std::memory_order_release);
    return ResponseFormatter::FormatStatus("AUTHENTICATED");
  }

  if (IsAdministrativeCommand(query->type) && !admin_token_.empty() &&
      !conn_ctx.admin_authenticated.load(std::memory_order_acquire)) {
    return ResponseFormatter::FormatError("Administrative command requires AUTH");
  }

  // Apply configured default LIMIT if not explicitly specified
  if (!query->limit_explicit && (query->type == query::QueryType::SEARCH || query->type == query::QueryType::FACET)) {
    query->limit = static_cast<uint32_t>(default_limit_.load(std::memory_order_acquire));
  }

  // Increment command statistics for successfully parsed commands.
  ctx_.stats.IncrementCommand(query->type);

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

void RequestDispatcher::UpdateApiConfig(int default_limit, int max_query_length) {
  default_limit_.store(default_limit, std::memory_order_release);
  max_query_length_.store(max_query_length <= 0 ? 0 : static_cast<size_t>(max_query_length), std::memory_order_release);
}

}  // namespace mygramdb::server
