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

namespace {

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

/**
 * @brief True when a request mentions AUTH anywhere, ignoring case.
 *
 * Used only for requests the parser rejected. A rejected request has no token
 * boundaries the parser can vouch for, so anything that could be an
 * authentication attempt is treated as one and redacted whole. The scan is
 * deliberately broader than the grammar in that direction only.
 */
bool MentionsAuth(std::string_view request) {
  constexpr std::string_view kAuth = "AUTH";
  if (request.size() < kAuth.size()) {
    return false;
  }
  for (size_t offset = 0; offset + kAuth.size() <= request.size(); ++offset) {
    bool matched = true;
    for (size_t i = 0; i < kAuth.size(); ++i) {
      if (std::toupper(static_cast<unsigned char>(request[offset + i])) != kAuth[i]) {
        matched = false;
        break;
      }
    }
    if (matched) {
      return true;
    }
  }
  return false;
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
    return ResponseFormatter::FormatError("Rate limit exceeded", mygram::utils::ErrorCode::kServerBusy);
  }

  // Create a thread-local parser for this request
  query::QueryParser parser;
  parser.SetMaxQueryLength(max_query_length_.load(std::memory_order_acquire));

  // Parse query
  auto query = parser.Parse(request);

  // Logged after parsing so the redaction decision is the parser's own verdict
  // rather than a second reading of the bytes. A separate scanner would have to
  // reproduce the grammar's quoting and Unicode whitespace rules exactly, and
  // every spelling where the two disagreed would write a token to the log.
  // Requests the parser rejected are still logged — a malformed request is the
  // diagnostically interesting one — but are redacted whole when they mention
  // AUTH, because a rejected request has no token boundaries to trust.
  //
  // Untrusted client input may contain log-injection sequences. Truncation also
  // bounds log volume on long requests. The full byte length is preserved in a
  // separate numeric field so log consumers can detect truncation and never
  // assume the logged string is complete.
  std::string logged_request;
  if (query && query->type == query::QueryType::AUTH) {
    logged_request = "AUTH <redacted>";
  } else if (!query && MentionsAuth(request)) {
    logged_request = "<redacted>";
  } else {
    logged_request = mygram::utils::StructuredLog::TruncateUtf8Prefix(request, mygram::utils::kMaxQueryLogLength);
    if (request.size() > mygram::utils::kMaxQueryLogLength) {
      logged_request += "...";
    }
  }
  mygram::utils::StructuredLog()
      .Event("request_dispatching")
      .Field("request", logged_request)
      .Field("request_full_length", static_cast<int64_t>(request.size()))
      .Debug();

  if (!query) {
    return ResponseFormatter::FormatError(query.error());
  }

  if (query->type == query::QueryType::AUTH) {
    // Counted here rather than at the shared site below, which this branch
    // returns before reaching. An uncounted AUTH would let repeated token
    // guesses run without appearing in command traffic at all.
    ctx_.stats.IncrementCommand(query->type);
    if (admin_token_.empty() || !ConstantTimeEqual(query->auth_token, admin_token_)) {
      conn_ctx.admin_authenticated.store(false, std::memory_order_release);
      return ResponseFormatter::FormatError("Authentication failed", mygram::utils::ErrorCode::kPermissionDenied);
    }
    conn_ctx.admin_authenticated.store(true, std::memory_order_release);
    return ResponseFormatter::FormatStatus("AUTHENTICATED");
  }

  if (IsAdministrativeCommand(query->type) && !admin_token_.empty() &&
      !conn_ctx.admin_authenticated.load(std::memory_order_acquire)) {
    return ResponseFormatter::FormatError("Administrative command requires AUTH",
                                          mygram::utils::ErrorCode::kPermissionDenied);
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
    return ResponseFormatter::FormatError("Unknown query type", mygram::utils::ErrorCode::kQuerySyntaxError);
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
