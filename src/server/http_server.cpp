/**
 * @file http_server.cpp
 * @brief HTTP server implementation
 */

#include "server/http_server.h"

#include <algorithm>
#include <array>
#include <cctype>
#include <chrono>
#include <iterator>
#include <limits>
#include <nlohmann/json.hpp>
#include <sstream>
#include <string_view>
#include <type_traits>
#include <variant>

#include "cache/cache_manager.h"
#include "config/config.h"
#include "index/bm25_scorer.h"
#include "query/highlighter.h"
#include "query/query_parser.h"
#include "query/result_sorter.h"
#include "server/handlers/search_handler.h"
#include "server/log_field_names.h"
#include "server/response_formatter.h"
#include "server/search_pipeline.h"
#include "server/statistics_service.h"
#include "server/tcp_server.h"  // For TableContext definition
#include "storage/document_store.h"
#include "storage/filter_index.h"
#include "utils/memory_utils.h"
#include "utils/network_utils.h"
#include "utils/numeric_parse.h"
#include "utils/roaring_bitmap_ptr.h"
#include "utils/string_utils.h"
#include "utils/structured_log.h"
#include "version.h"

// Fix for httplib missing NI_MAXHOST on some platforms
#ifndef NI_MAXHOST
#define NI_MAXHOST 1025
#endif

#include "mysql/binlog_reader_interface.h"

using json = nlohmann::json;

namespace mygramdb::server {

namespace {
// HTTP status codes
constexpr int kHttpOk = 200;
constexpr int kHttpNoContent = 204;
constexpr int kHttpBadRequest = 400;
constexpr int kHttpForbidden = 403;
constexpr int kHttpNotFound = 404;
constexpr int kHttpTooManyRequests = 429;
constexpr int kHttpInternalServerError = 500;
constexpr int kHttpServiceUnavailable = 503;
constexpr auto kHttpServerReadyTimeout = std::chrono::seconds(5);

json FilterValueToJson(const storage::FilterValue& value) {
  json serialized = nullptr;
  std::visit(
      [&](const auto& arg) {
        using T = std::decay_t<decltype(arg)>;
        if constexpr (std::is_same_v<T, std::monostate>) {
          serialized = nullptr;
        } else if constexpr (std::is_same_v<T, storage::TimeValue>) {
          // TimeValue: serialize as seconds
          serialized = arg.seconds;
        } else {
          serialized = arg;
        }
      },
      value);
  return serialized;
}
/**
 * @brief Convert a JSON filter value to its string representation.
 *
 * Handles string, integer, float, and boolean types with appropriate coercion.
 * Returns std::nullopt if the value type is unsupported.
 */
std::optional<std::string> JsonFilterValueToString(const json& val) {
  if (val.is_string()) {
    return val.get<std::string>();
  }
  if (val.is_number_integer()) {
    return std::to_string(val.get<int64_t>());
  }
  if (val.is_number_float()) {
    return std::to_string(val.get<double>());
  }
  if (val.is_boolean()) {
    return val.get<bool>() ? "1" : "0";
  }
  return std::nullopt;
}

bool IsValidUtf8ContinuationByte(unsigned char byte) {
  return (byte & 0xC0U) == 0x80U;
}

bool ConsumeValidUtf8CodePoint(std::string_view text, size_t& index) {
  const auto first = static_cast<unsigned char>(text[index]);
  size_t needed = 0;
  uint32_t code_point = 0;

  if (first >= 0xC2U && first <= 0xDFU) {
    needed = 2;
    code_point = first & 0x1FU;
  } else if (first >= 0xE0U && first <= 0xEFU) {
    needed = 3;
    code_point = first & 0x0FU;
  } else if (first >= 0xF0U && first <= 0xF4U) {
    needed = 4;
    code_point = first & 0x07U;
  } else {
    return false;
  }

  if (index + needed > text.size()) {
    return false;
  }

  for (size_t offset = 1; offset < needed; ++offset) {
    const auto byte = static_cast<unsigned char>(text[index + offset]);
    if (!IsValidUtf8ContinuationByte(byte)) {
      return false;
    }
    code_point = (code_point << 6U) | (byte & 0x3FU);
  }

  if ((needed == 3 && code_point < 0x800U) || (needed == 4 && code_point < 0x10000U)) {
    return false;
  }
  if (code_point >= 0xD800U && code_point <= 0xDFFFU) {
    return false;
  }
  if (code_point > 0x10FFFFU) {
    return false;
  }

  index += needed;
  return true;
}

/**
 * @brief Validate a table name supplied via the HTTP API.
 *
 * Permitted characters:
 * - ASCII letters, digits, underscore, hyphen, and dot.
 * - Well-formed UTF-8 non-ASCII code points, which lets names such as
 *   "テーブル" pass through unchanged.
 *
 * Rejected: empty names, ASCII whitespace, ASCII control characters, and any
 * other ASCII punctuation. The goal is to prevent the value from breaking the
 * QueryParser command grammar (e.g. `articles foo` would inject an extra
 * token, and `articles;` would inject a stray punctuation token).
 *
 * @param table Table name from the request URL.
 * @return true if the name is safe to embed in a parser command, false
 *         otherwise.
 */
bool IsValidTableName(std::string_view table) {
  if (table.empty()) {
    return false;
  }
  // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
  constexpr size_t kMaxTableNameLength = 256;
  if (table.size() > kMaxTableNameLength) {
    return false;
  }
  for (size_t i = 0; i < table.size();) {
    auto u = static_cast<unsigned char>(table[i]);
    bool ascii_safe =
        (u >= 'a' && u <= 'z') || (u >= 'A' && u <= 'Z') || (u >= '0' && u <= '9') || u == '_' || u == '-' || u == '.';
    if (ascii_safe) {
      ++i;
      continue;
    }
    if (u < 0x80 || !ConsumeValidUtf8CodePoint(table, i)) {
      return false;
    }
  }
  return true;
}

bool IsQualifiedTableRoute(const httplib::Request& req) {
  return req.matches.size() >= 4 && req.matches[1] == "tables";
}

bool RequiresQualifiedTableReferences(const config::Config* full_config) {
  return full_config != nullptr && !full_config->tables.empty();
}

bool IsDatabaseQualifiedTableName(std::string_view table_name) {
  const auto separator = table_name.find('.');
  return separator != std::string_view::npos && separator != 0 && separator + 1 < table_name.size();
}

std::string ExtractRouteTableKey(const httplib::Request& req) {
  if (IsQualifiedTableRoute(req)) {
    return config::QualifiedTableName(req.matches[2], req.matches[3]);
  }
  return req.matches[1];
}

std::string ExtractRoutePrimaryKey(const httplib::Request& req) {
  if (IsQualifiedTableRoute(req)) {
    return req.matches[4];
  }
  return req.matches[2];
}

/**
 * @brief Detect QueryParser clause keywords inside a JSON-supplied query string.
 *
 * The HTTP API exposes `limit`, `offset`, and `filters` as dedicated JSON
 * fields. Allowing the same keywords to appear inside the search expression
 * (`q`) would let a caller silently override those JSON values, which is a
 * parameter pollution vulnerability (P1-9). This helper rejects such inputs by
 * scanning for the dangerous keywords as standalone tokens, ignoring contents
 * inside single- or double-quoted regions so that legitimate phrase searches
 * such as `"foo LIMIT bar"` remain valid.
 *
 * Boolean operators (`AND`, `OR`, `NOT`) are intentionally NOT rejected: they
 * are first-class search syntax with no JSON equivalent, and the existing
 * tests/clients depend on them being usable inside `q`.
 *
 * @param query_text Raw query string from the JSON `q` field.
 * @param[out] offending_keyword Set to the matched keyword (uppercase) on
 *             rejection.
 * @return true if the query is safe; false if it embeds a forbidden keyword.
 */
bool ValidateQueryTextNoReservedClauses(std::string_view query_text, std::string& offending_keyword) {
  static const std::array<std::string_view, 7> kForbiddenKeywords = {"LIMIT", "OFFSET",    "ORDER", "FILTER",
                                                                     "SORT",  "HIGHLIGHT", "FUZZY"};

  auto match_keyword = [&](std::string_view token) -> std::string_view {
    for (auto kw : kForbiddenKeywords) {
      if (token.size() != kw.size()) {
        continue;
      }
      bool eq = true;
      for (size_t i = 0; i < kw.size(); ++i) {
        auto a = static_cast<unsigned char>(token[i]);
        auto b = static_cast<unsigned char>(kw[i]);
        if (std::toupper(a) != b) {
          eq = false;
          break;
        }
      }
      if (eq) {
        return kw;
      }
    }
    return {};
  };

  size_t i = 0;
  const size_t n = query_text.size();
  char quote = '\0';

  while (i < n) {
    char c = query_text[i];

    if (quote != '\0') {
      // Inside quotes: skip everything until matching close (honor backslash escape).
      if (c == '\\' && i + 1 < n) {
        i += 2;
        continue;
      }
      if (c == quote) {
        quote = '\0';
      }
      ++i;
      continue;
    }

    if (c == '"' || c == '\'') {
      quote = c;
      ++i;
      continue;
    }

    // Skip ASCII whitespace.
    auto u = static_cast<unsigned char>(c);
    if (std::isspace(u) != 0) {
      ++i;
      continue;
    }

    // Collect a token of non-whitespace, non-quote characters.
    size_t start = i;
    while (i < n) {
      char tc = query_text[i];
      auto tu = static_cast<unsigned char>(tc);
      if (std::isspace(tu) != 0 || tc == '"' || tc == '\'') {
        break;
      }
      ++i;
    }

    std::string_view token = query_text.substr(start, i - start);
    auto matched = match_keyword(token);
    if (!matched.empty()) {
      offending_keyword.assign(matched.begin(), matched.end());
      return false;
    }
  }

  return true;
}

/**
 * @brief Parse filter conditions from a JSON "filters" object into a query
 *
 * Supports two formats:
 * - Format 1: {"col": "value"} or {"col": 123} - defaults to EQ operator
 * - Format 2: {"col": {"op": "GT", "value": "10"}} - full operator support
 *
 * @param filters_json The JSON object containing filter definitions
 * @param query The query to populate with parsed filter conditions
 * @param[out] error_message Set to error description on failure
 * @return true on success, false on parse error (error_message is set)
 */
bool ParseFiltersFromJson(const json& filters_json, query::Query& query, std::string& error_message) {
  query.filters.clear();
  for (const auto& [key, val] : filters_json.items()) {
    query::FilterCondition filter;
    filter.column = key;

    if (val.is_object() && val.contains("value")) {
      // Format 2: full operator support
      std::string op_str = val.value("op", "EQ");
      auto parsed_op = query::QueryParser::ParseFilterOp(op_str);
      if (!parsed_op.has_value()) {
        error_message = "Invalid filter operator: " + op_str;
        return false;
      }
      filter.op = parsed_op.value();

      auto str_val = JsonFilterValueToString(val["value"]);
      if (!str_val.has_value()) {
        error_message = "Invalid filter value type for column: " + key;
        return false;
      }
      filter.value = std::move(str_val.value());
    } else {
      // Format 1: backward compatible (defaults to EQ)
      filter.op = query::FilterOp::EQ;
      auto str_val = JsonFilterValueToString(val);
      if (!str_val.has_value()) {
        error_message = "Invalid filter value type for column: " + key;
        return false;
      }
      filter.value = std::move(str_val.value());
    }

    query.filters.push_back(std::move(filter));
  }
  return true;
}

bool IsSafeJsonColumnName(std::string_view column) {
  if (column.empty() || column.size() > query::QueryParser::kMaxFilterColumnNameLength) {
    return false;
  }
  for (char c : column) {
    auto u = static_cast<unsigned char>(c);
    const bool ascii_safe = (u >= 'a' && u <= 'z') || (u >= 'A' && u <= 'Z') || (u >= '0' && u <= '9') || u == '_' ||
                            u == '-' || u == '.' || u == '$';
    if (!ascii_safe) {
      return false;
    }
  }
  return true;
}

bool EqualsAsciiIgnoreCase(std::string_view lhs, std::string_view rhs) {
  if (lhs.size() != rhs.size()) {
    return false;
  }
  for (size_t i = 0; i < lhs.size(); ++i) {
    auto a = static_cast<unsigned char>(lhs[i]);
    auto b = static_cast<unsigned char>(rhs[i]);
    if (std::tolower(a) != std::tolower(b)) {
      return false;
    }
  }
  return true;
}

bool ParseSortFromJson(const json& sort_json, query::Query& query, std::string& error_message) {
  if (!sort_json.is_object()) {
    error_message = "Field 'sort' must be an object";
    return false;
  }
  if (!sort_json.contains("column") || !sort_json["column"].is_string()) {
    error_message = "Field 'sort.column' must be a string";
    return false;
  }

  std::string column = sort_json["column"].get<std::string>();
  if (column != "_score" && column != "id" && !IsSafeJsonColumnName(column)) {
    error_message = "Invalid sort column";
    return false;
  }

  query::SortOrder order = query::SortOrder::DESC;
  if (sort_json.contains("order")) {
    if (!sort_json["order"].is_string()) {
      error_message = "Field 'sort.order' must be a string";
      return false;
    }
    std::string order_str = sort_json["order"].get<std::string>();
    if (EqualsAsciiIgnoreCase(order_str, "ASC")) {
      order = query::SortOrder::ASC;
    } else if (EqualsAsciiIgnoreCase(order_str, "DESC")) {
      order = query::SortOrder::DESC;
    } else {
      error_message = "Invalid sort order: " + order_str;
      return false;
    }
  }

  if (column == "id") {
    column.clear();  // Query AST convention: empty column means primary key.
  }
  query.order_by = query::OrderByClause{std::move(column), order};
  return true;
}

bool ParseHighlightUint(const json& highlight_json, const char* field_name, uint32_t min_value, uint32_t max_value,
                        uint32_t& out, std::string& error_message) {
  if (!highlight_json.contains(field_name)) {
    return true;
  }
  const auto& value = highlight_json[field_name];
  if (!value.is_number_unsigned() && !value.is_number_integer()) {
    error_message = std::string("Field 'highlight.") + field_name + "' must be an integer";
    return false;
  }
  int64_t parsed = value.get<int64_t>();
  if (parsed < static_cast<int64_t>(min_value) || parsed > static_cast<int64_t>(max_value)) {
    std::ostringstream oss;
    oss << "Field 'highlight." << field_name << "' must be between " << min_value << " and " << max_value;
    error_message = oss.str();
    return false;
  }
  out = static_cast<uint32_t>(parsed);
  return true;
}

constexpr size_t kMaxHighlightTagLength = 256;

bool ParseHighlightFromJson(const json& highlight_json, query::Query& query, std::string& error_message) {
  if (!highlight_json.is_object()) {
    error_message = "Field 'highlight' must be an object";
    return false;
  }

  query::HighlightOptions opts;
  if (highlight_json.contains("open_tag")) {
    if (!highlight_json["open_tag"].is_string()) {
      error_message = "Field 'highlight.open_tag' must be a string";
      return false;
    }
    opts.open_tag = highlight_json["open_tag"].get<std::string>();
    if (opts.open_tag.size() > kMaxHighlightTagLength) {
      error_message = "Field 'highlight.open_tag' must be at most 256 bytes";
      return false;
    }
  }
  if (highlight_json.contains("close_tag")) {
    if (!highlight_json["close_tag"].is_string()) {
      error_message = "Field 'highlight.close_tag' must be a string";
      return false;
    }
    opts.close_tag = highlight_json["close_tag"].get<std::string>();
    if (opts.close_tag.size() > kMaxHighlightTagLength) {
      error_message = "Field 'highlight.close_tag' must be at most 256 bytes";
      return false;
    }
  }

  if (!ParseHighlightUint(highlight_json, "snippet_length", 1, 10000, opts.snippet_length, error_message)) {
    return false;
  }
  if (!ParseHighlightUint(highlight_json, "max_fragments", 1, 100, opts.max_fragments, error_message)) {
    return false;
  }

  query.highlight = std::move(opts);
  return true;
}

bool ParseFuzzyFromJson(const json& fuzzy_json, query::Query& query, std::string& error_message) {
  if (!fuzzy_json.is_number_unsigned() && !fuzzy_json.is_number_integer()) {
    error_message = "Field 'fuzzy' must be an integer";
    return false;
  }
  int64_t distance = fuzzy_json.get<int64_t>();
  if (distance < 1 || distance > 2) {
    error_message = "Field 'fuzzy' must be 1 or 2";
    return false;
  }
  query.fuzzy_max_distance = static_cast<uint32_t>(distance);
  return true;
}

std::vector<std::string> BuildHighlightTerms(const std::vector<std::string>& search_terms, TableContext& table_ctx) {
  return search_pipeline::BuildHighlightTerms(search_terms, table_ctx.index.get(), table_ctx.synonym_dict.get());
}

mygram::utils::Expected<std::vector<storage::DocId>, mygram::utils::Error> SortHttpResults(
    std::vector<storage::DocId>& results, const query::Query& query, TableContext& table_ctx,
    search_pipeline::FullPipelineOutput& pipeline_output, const config::Config* full_config,
    const std::string& primary_key_column) {
  using mygram::utils::ErrorCode;
  using mygram::utils::MakeError;
  using mygram::utils::MakeUnexpected;

  const bool is_score_sort = query.order_by.has_value() && query.order_by->IsScoreSort();
  if (!is_score_sort) {
    return query::ResultSorter::SortAndPaginate(results, *table_ctx.doc_store, query, primary_key_column);
  }

  if (full_config == nullptr || !full_config->bm25.enable) {
    return MakeUnexpected(
        MakeError(ErrorCode::kInvalidArgument, "SORT _score requires BM25 to be enabled in configuration"));
  }
  if (table_ctx.index == nullptr || table_ctx.doc_store == nullptr) {
    return MakeUnexpected(MakeError(ErrorCode::kInvalidArgument, "Table is not available"));
  }

  if (pipeline_output.term_infos.empty() && !pipeline_output.all_search_terms.empty()) {
    pipeline_output.term_infos = search_pipeline::GenerateTermInfos(
        pipeline_output.all_search_terms, table_ctx.index.get(), table_ctx.config.ngram_size,
        table_ctx.config.kanji_ngram_size, table_ctx.config.cross_boundary_ngrams);
  }

  std::vector<std::string> normalized_terms;
  std::vector<uint64_t> term_dfs;
  normalized_terms.reserve(pipeline_output.term_infos.size());
  term_dfs.reserve(pipeline_output.term_infos.size());
  for (size_t i = 0; i < pipeline_output.term_infos.size(); ++i) {
    const auto& term_info = pipeline_output.term_infos[i];
    if (!term_info.normalized_term.empty()) {
      normalized_terms.push_back(term_info.normalized_term);
    } else if (i < pipeline_output.all_search_terms.size()) {
      normalized_terms.push_back(table_ctx.index->NormalizeText(pipeline_output.all_search_terms[i]));
    }
    term_dfs.push_back(term_info.term_doc_freq);
  }

  const auto& bm25_config = full_config->bm25;
  const index::BM25Params bm25_params{bm25_config.k1, bm25_config.b};
  auto scored = index::BM25Scorer::ScoreDocuments(results, normalized_terms, term_dfs, *table_ctx.doc_store,
                                                  table_ctx.bm25_stats.doc_count.load(std::memory_order_relaxed),
                                                  table_ctx.bm25_stats.avg_doc_length(), bm25_params);

  std::vector<double> scores;
  scores.reserve(scored.size());
  for (const auto& scored_doc : scored) {
    scores.push_back(scored_doc.score);
  }
  return query::ResultSorter::SortByScore(results, scores, query.order_by->order, query.limit, query.offset);
}

}  // namespace

using storage::DocId;

HttpServer::HttpServer(HttpServerConfig config, std::unordered_map<std::string, TableContext*> table_contexts,
                       const config::Config* full_config, mysql::IBinlogReader* binlog_reader,
                       cache::CacheManager* cache_manager, std::atomic<bool>* loading, ServerStats* tcp_stats,
                       std::shared_ptr<RateLimiter> rate_limiter, std::atomic<bool>* replication_paused_for_dump)
    : config_(std::move(config)),
      table_contexts_(std::move(table_contexts)),
      full_config_(full_config),
      binlog_reader_(binlog_reader),
      cache_manager_(cache_manager),
      rate_limiter_(std::move(rate_limiter)),
      loading_(loading),
      tcp_stats_(tcp_stats),
      replication_paused_for_dump_(replication_paused_for_dump) {
  parsed_allow_cidrs_ = mygram::utils::ParseAllowCidrs(config_.allow_cidrs);

  if (full_config_ != nullptr) {
    const auto configured_limit = full_config_->api.max_query_length;
    default_limit_.store(full_config_->api.default_limit, std::memory_order_release);
    max_query_length_.store(configured_limit <= 0 ? 0 : static_cast<size_t>(configured_limit),
                            std::memory_order_release);

    // Rate limiter resolution:
    //   - If the embedder injected a shared instance (ServerLifecycleManager
    //     does this so quotas apply across TCP+HTTP), use it.
    //   - Otherwise fall back to constructing a private one from config so
    //     standalone HttpServer instances (and unit tests that pre-date the
    //     shared rate limiter wiring) still rate-limit.
    if (!rate_limiter_ && full_config_->api.rate_limiting.enable) {
      rate_limiter_ = std::make_shared<RateLimiter>(static_cast<size_t>(full_config_->api.rate_limiting.capacity),
                                                    static_cast<size_t>(full_config_->api.rate_limiting.refill_rate),
                                                    static_cast<size_t>(full_config_->api.rate_limiting.max_clients));
      mygram::utils::StructuredLog()
          .Event("http_rate_limiter_initialized")
          .Field("capacity", static_cast<uint64_t>(full_config_->api.rate_limiting.capacity))
          .Field("refill_rate", static_cast<uint64_t>(full_config_->api.rate_limiting.refill_rate))
          .Field("max_clients", static_cast<uint64_t>(full_config_->api.rate_limiting.max_clients))
          .Info();
    }
  }

  server_ = std::make_unique<httplib::Server>();

  // Set timeouts
  server_->set_read_timeout(config_.read_timeout_sec, 0);
  server_->set_write_timeout(config_.write_timeout_sec, 0);

  // Cap the maximum HTTP body size (Fix N-2). cpp-httplib rejects oversize
  // POST bodies with 413 Payload Too Large before any handler runs, which
  // protects /search and /count from memory-exhaustion attacks via giant
  // JSON payloads. Default 16 MiB; configurable via api.http.max_body_bytes.
  if (config_.max_body_bytes > 0) {
    server_->set_payload_max_length(config_.max_body_bytes);
  }

  // Setup network ACL before registering routes
  SetupAccessControl();

  // Setup routes
  SetupRoutes();

  // Setup CORS if enabled
  if (config_.enable_cors) {
    SetupCors();
  }
}

HttpServer::~HttpServer() {
  Stop();
}

void HttpServer::SetupRoutes() {
  // POST /tables/{database}/{table}/search - DB-qualified full-text search
  server_->Post(R"(/(tables)/([^/]+)/([^/]+)/search)",
                [this](const httplib::Request& req, httplib::Response& res) { HandleSearch(req, res); });

  // POST /tables/{database}/{table}/count - DB-qualified count
  server_->Post(R"(/(tables)/([^/]+)/([^/]+)/count)",
                [this](const httplib::Request& req, httplib::Response& res) { HandleCount(req, res); });

  // POST /tables/{database}/{table}/facet - DB-qualified facet
  server_->Post(R"(/(tables)/([^/]+)/([^/]+)/facet)",
                [this](const httplib::Request& req, httplib::Response& res) { HandleFacet(req, res); });

  // POST /{table}/search - Full-text search
  // Route pattern: match any non-slash characters to support table names with dashes, dots, or unicode
  server_->Post(R"(/([^/]+)/search)",
                [this](const httplib::Request& req, httplib::Response& res) { HandleSearch(req, res); });

  // POST /{table}/count - Count matching documents
  server_->Post(R"(/([^/]+)/count)",
                [this](const httplib::Request& req, httplib::Response& res) { HandleCount(req, res); });

  // POST /{table}/facet - Facet value counts
  server_->Post(R"(/([^/]+)/facet)",
                [this](const httplib::Request& req, httplib::Response& res) { HandleFacet(req, res); });

  // GET /info - Server information
  server_->Get("/info", [this](const httplib::Request& req, httplib::Response& res) { HandleInfo(req, res); });

  // GET /health - Health check
  // Health check endpoints
  server_->Get("/health", [this](const httplib::Request& req, httplib::Response& res) { HandleHealth(req, res); });
  server_->Get("/health/live",
               [this](const httplib::Request& req, httplib::Response& res) { HandleHealthLive(req, res); });
  server_->Get("/health/ready",
               [this](const httplib::Request& req, httplib::Response& res) { HandleHealthReady(req, res); });
  server_->Get("/health/detail",
               [this](const httplib::Request& req, httplib::Response& res) { HandleHealthDetail(req, res); });

  // GET /config - Configuration
  server_->Get("/config", [this](const httplib::Request& req, httplib::Response& res) { HandleConfig(req, res); });

  // GET /replication/status - Replication status
  server_->Get("/replication/status",
               [this](const httplib::Request& req, httplib::Response& res) { HandleReplicationStatus(req, res); });

  // GET /metrics - Prometheus metrics
  server_->Get("/metrics", [this](const httplib::Request& req, httplib::Response& res) { HandleMetrics(req, res); });

  // GET /{table}/{primary_key} - Get document by primary key.
  // Register this catch-all route last so fixed endpoints such as
  // /health/live and /replication/status are not interpreted as table/PK.
  server_->Get(R"(/(tables)/([^/]+)/([^/]+)/([^/]+))",
               [this](const httplib::Request& req, httplib::Response& res) { HandleGet(req, res); });
  server_->Get(R"(/([^/]+)/([^/]+))",
               [this](const httplib::Request& req, httplib::Response& res) { HandleGet(req, res); });
}

void HttpServer::SetupAccessControl() {
  server_->set_pre_routing_handler([this](const httplib::Request& req, httplib::Response& res) {
    const std::string& client_ip = req.remote_addr.empty() ? "unknown" : req.remote_addr;

    // Health check endpoints bypass CIDR and rate limit restrictions
    // (required for Docker HEALTHCHECK, load balancers, and orchestrator probes)
    if (req.path == "/health" || req.path == "/health/live" || req.path == "/health/ready" ||
        req.path == "/health/detail") {
      return httplib::Server::HandlerResponse::Unhandled;
    }

    // Check CIDR-based access control first
    if (!parsed_allow_cidrs_.empty() && !mygram::utils::IsIPAllowed(req.remote_addr, parsed_allow_cidrs_)) {
      RecordRequest();
      mygram::utils::StructuredLog()
          .Event("http_request_rejected_acl")
          .Field(log_fields::kFieldClientIp, client_ip)
          .Warn();
      SendError(res, kHttpForbidden, "Access denied by network.allow_cidrs");
      return httplib::Server::HandlerResponse::Handled;
    }

    // Check rate limit (if enabled)
    if (rate_limiter_ && !rate_limiter_->AllowRequest(client_ip)) {
      RecordRequest();
      mygram::utils::StructuredLog()
          .Event("http_rate_limit_exceeded")
          .Field(log_fields::kFieldClientIp, client_ip)
          .Warn();
      SendError(res, kHttpTooManyRequests, "Rate limit exceeded");
      return httplib::Server::HandlerResponse::Handled;
    }

    return httplib::Server::HandlerResponse::Unhandled;
  });
}

void HttpServer::SetupCors() {
  // NOTE: Default to "*" rather than the literal "null" string. A literal "null"
  // origin is dangerous because browsers treat null-origin requests from
  // sandboxed iframes as matching, enabling CORS bypass attacks.
  const std::string allow_origin = config_.cors_allow_origin.empty() ? "*" : config_.cors_allow_origin;

  // CORS preflight
  server_->Options(".*", [allow_origin](const httplib::Request& /*req*/, httplib::Response& res) {
    res.set_header("Access-Control-Allow-Origin", allow_origin);
    res.set_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
    res.set_header("Access-Control-Allow-Headers", "Content-Type");
    res.status = kHttpNoContent;
  });

  // Add CORS headers to all responses
  server_->set_post_routing_handler([allow_origin](const httplib::Request& /*req*/, httplib::Response& res) {
    res.set_header("Access-Control-Allow-Origin", allow_origin);
  });
}

mygram::utils::Expected<void, mygram::utils::Error> HttpServer::Start() {
  using mygram::utils::ErrorCode;
  using mygram::utils::MakeError;
  using mygram::utils::MakeUnexpected;

  // Atomically transition running_ from false -> true. This replaces the prior
  // check-then-set pattern, which let two concurrent Start() calls both pass
  // the load and both proceed to spawn the server thread (P0-C). The CAS uses
  // memory_order_acq_rel on success so that subsequent stores to server_thread_
  // happen-after this acquire, and memory_order_relaxed on failure since the
  // failure path only reads `expected` for diagnostic purposes.
  bool expected = false;
  if (!running_.compare_exchange_strong(expected, true, std::memory_order_acq_rel, std::memory_order_relaxed)) {
    auto error = MakeError(ErrorCode::kNetworkAlreadyRunning, "Server already running");
    mygram::utils::StructuredLog()
        .Event("http_server_start_failed")
        .Field(log_fields::kFieldError, error.to_string())
        .Error();
    return MakeUnexpected(error);
  }

  mygram::utils::StructuredLog()
      .Event("http_server_starting")
      .Field("bind", config_.bind)
      .Field("port", static_cast<uint64_t>(config_.port))
      .Info();

  // Bind synchronously on the calling thread.
  //
  // Rationale (Fix H-N1): the previous design spawned a worker thread that
  // called `bind_to_port` then signalled completion through a promise, with
  // the parent waiting on `start_future.wait_for(timeout)`. That introduced
  // a join-deadlock window: on `wait_for` timeout the parent called
  // `server_->stop()` (a no-op when the worker had not yet reached
  // `listen_after_bind`) and then `server_thread_->join()`, which could
  // block indefinitely if the worker was wedged inside `bind_to_port`. The
  // destructor's chained `Stop()` would then run `terminate()` from the
  // joinable-thread invariant.
  //
  // cpp-httplib exposes `bind_to_port` as a synchronous call: it just runs
  // the socket/bind/listen syscalls and returns. There is no benefit to
  // hopping into a worker for that step, and doing so synchronously
  // eliminates the timeout entirely. The worker thread only owns the
  // long-running `listen_after_bind` accept loop, which `Stop()`'s
  // `server_->stop()` reliably tears down via the documented
  // shutdown(svr_sock_) path.
  if (!server_->bind_to_port(config_.bind, config_.port)) {
    std::string error_msg = "Failed to bind to " + config_.bind + ":" + std::to_string(config_.port);
    mygram::utils::StructuredLog()
        .Event("http_server_listen_failed")
        .Field("bind", config_.bind)
        .Field("port", static_cast<uint64_t>(config_.port))
        .Field(log_fields::kFieldError, error_msg)
        .Error();
    // Release the running_ gate so subsequent Start()s can retry. Bind
    // happened on this thread, so no worker exists to clean up.
    running_.store(false, std::memory_order_release);
    auto error = MakeError(ErrorCode::kNetworkBindFailed, std::move(error_msg));
    return MakeUnexpected(error);
  }

  // Spawn the worker thread to drive the accept loop. By the time we reach
  // here, the listening socket is bound and ready; `server_->stop()` (called
  // from Stop()) will reliably interrupt `listen_after_bind` by closing the
  // listening socket — but only AFTER the worker reaches the
  // `is_running_=true` flip inside `listen_internal()`. Pre-flip, cpp-httplib's
  // own `Server::stop()` is a no-op (it gates on `is_running_`), so a
  // racing Stop() right after Start() returned would leak the worker thread.
  // We therefore call `server_->wait_until_ready()` immediately after the
  // spawn: it spins on `is_running_` with 1ms sleeps and returns within a
  // few milliseconds in all observed runs. By the time Start() returns,
  // both bind_to_port and the accept-loop entry are committed.
  server_thread_ = std::make_unique<std::thread>([this]() {
    if (!server_->listen_after_bind()) {
      // Abnormal exit from the accept loop. Release the running_ gate so a
      // subsequent Start() can attempt a fresh bind. Stop()'s CAS handles
      // the case where Stop() is the one tearing us down: its CAS observes
      // running_=false here and short-circuits.
      running_.store(false, std::memory_order_release);
    }
  });
  const auto ready_deadline = std::chrono::steady_clock::now() + kHttpServerReadyTimeout;
  while (!server_->is_running() && std::chrono::steady_clock::now() < ready_deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  if (!server_->is_running()) {
    server_->stop();
    running_.store(false, std::memory_order_release);
    if (server_thread_ && server_thread_->joinable()) {
      server_thread_->join();
    }
    auto error = MakeError(ErrorCode::kNetworkBindFailed, "HTTP server did not become ready before timeout");
    mygram::utils::StructuredLog()
        .Event("http_server_ready_timeout")
        .Field(log_fields::kFieldError, error.to_string())
        .Error();
    return MakeUnexpected(error);
  }

  mygram::utils::StructuredLog()
      .Event("http_server_started")
      .Field("bind", config_.bind)
      .Field("port", static_cast<uint64_t>(config_.port))
      .Info();
  return {};
}

void HttpServer::Stop() {
  // Use compare_exchange to prevent concurrent double-stop, matching the
  // pattern in ConnectionAcceptor::Stop() and SnapshotScheduler::Stop().
  bool expected = true;
  if (!running_.compare_exchange_strong(expected, false)) {
    return;
  }

  mygram::utils::StructuredLog().Event("http_server_stopping").Info();

  if (server_) {
    server_->stop();
  }

  if (server_thread_ && server_thread_->joinable()) {
    server_thread_->join();
  }

  mygram::utils::StructuredLog().Event("http_server_stopped").Info();
}

void HttpServer::UpdateApiConfig(int default_limit, int max_query_length) {
  default_limit_.store(default_limit, std::memory_order_release);
  max_query_length_.store(max_query_length <= 0 ? 0 : static_cast<size_t>(max_query_length), std::memory_order_release);
}

HttpServer::TableContextLookup HttpServer::ResolveHttpTableContext(const std::string& table_name,
                                                                   bool database_qualified_route) {
  TableContextLookup result;

  if (!IsValidTableName(table_name)) {
    result.status = kHttpBadRequest;
    result.message = "Invalid table name (allowed characters: letters, digits, '_', '-', '.')";
    return result;
  }

  if (RequiresQualifiedTableReferences(full_config_)) {
    if (!database_qualified_route) {
      result.status = kHttpBadRequest;
      result.message = "Bare HTTP table routes are not supported; use /tables/{database}/{table}/...";
      return result;
    }
    if (!IsDatabaseQualifiedTableName(table_name)) {
      result.status = kHttpBadRequest;
      result.message = "Bare table names are not supported; use <database>.<table>: " + table_name;
      return result;
    }
  }

  auto table_iter = table_contexts_.find(table_name);
  if (table_iter == table_contexts_.end()) {
    result.status = kHttpNotFound;
    result.message = "Table not found: " + table_name;
    return result;
  }

  if (!table_iter->second->index || !table_iter->second->doc_store) {
    result.status = kHttpInternalServerError;
    result.message = "Table context has null index or doc_store";
    return result;
  }

  result.table_ctx = table_iter->second;
  result.status = kHttpOk;
  return result;
}

std::optional<HttpServer::PreparedHttpQuery> HttpServer::PrepareHttpSearchQuery(const httplib::Request& req,
                                                                                httplib::Response& res,
                                                                                const std::string& command,
                                                                                bool apply_pagination) {
  // Check if server is loading
  if (loading_ != nullptr && loading_->load()) {
    SendError(res, kHttpServiceUnavailable, "Server is loading, please try again later");
    return std::nullopt;
  }

  // Validate the URL-bound table name and resolve its context. Errors are
  // surfaced with the precise HTTP status code computed by the helper so
  // callers do not need to know the underlying reason.
  std::string table = ExtractRouteTableKey(req);
  auto lookup = ResolveHttpTableContext(table, IsQualifiedTableRoute(req));
  if (lookup.table_ctx == nullptr) {
    SendError(res, lookup.status, lookup.message);
    return std::nullopt;
  }
  auto* table_ctx = lookup.table_ctx;

  // Parse JSON body
  json body;
  try {
    body = json::parse(req.body);
  } catch (const json::parse_error& e) {
    SendError(res, kHttpBadRequest, "Invalid JSON: " + std::string(e.what()));
    return std::nullopt;
  }

  // Validate required field
  if (!body.contains("q")) {
    SendError(res, kHttpBadRequest, "Missing required field: q");
    return std::nullopt;
  }

  // Validate field type before extraction
  if (!body["q"].is_string()) {
    SendError(res, kHttpBadRequest, "Field 'q' must be a string");
    return std::nullopt;
  }

  if (!apply_pagination) {
    static constexpr std::array<std::string_view, 5> kCountRejectedFields = {"limit", "offset", "sort", "highlight",
                                                                             "fuzzy"};
    for (const auto field : kCountRejectedFields) {
      if (body.contains(std::string(field))) {
        SendError(res, kHttpBadRequest,
                  "Field '" + std::string(field) +
                      "' is not supported by COUNT; use /search for ranked or paginated "
                      "results");
        return std::nullopt;
      }
    }
  }

  // Validate query text for control characters (CRLF injection prevention)
  std::string query_text = body["q"].get<std::string>();
  for (char c : query_text) {
    if (c == '\r' || c == '\n' || c == '\0') {
      SendError(res, kHttpBadRequest, "Query text contains invalid control characters");
      return std::nullopt;
    }
  }

  // Reject parser clause keywords smuggled into `q`. JSON-supplied `limit`,
  // `offset`, and `filters` would otherwise be silently overridden by tokens
  // such as `LIMIT 0 OFFSET 999999` embedded in the search text (P1-9).
  {
    std::string offending;
    if (!ValidateQueryTextNoReservedClauses(query_text, offending)) {
      const std::string field_hint = apply_pagination ? "(limit, offset, filters)" : "(filters)";
      SendError(res, kHttpBadRequest,
                "Reserved keyword '" + offending + "' is not allowed in 'q'. Use the dedicated JSON fields " +
                    field_hint + " instead.");
      return std::nullopt;
    }
  }

  // Build query string for QueryParser
  std::ostringstream query_str;
  query_str << command << " " << table << " " << query_text;

  if (apply_pagination) {
    // Add limit
    if (body.contains("limit")) {
      if (!body["limit"].is_number_integer()) {
        SendError(res, kHttpBadRequest, "Invalid limit: must be an integer");
        return std::nullopt;
      }
      const int64_t limit = body["limit"].get<int64_t>();
      if (limit <= 0 || limit > config::defaults::kMaxLimit) {
        SendError(res, kHttpBadRequest,
                  "Invalid limit: must be between 1 and " + std::to_string(config::defaults::kMaxLimit));
        return std::nullopt;
      }
      query_str << " LIMIT " << limit;
    }

    // Add offset
    if (body.contains("offset")) {
      if (!body["offset"].is_number_integer()) {
        SendError(res, kHttpBadRequest, "Invalid offset: must be an integer");
        return std::nullopt;
      }
      const int64_t offset = body["offset"].get<int64_t>();
      if (offset < 0 || offset > std::numeric_limits<uint32_t>::max()) {
        SendError(res, kHttpBadRequest,
                  "Invalid offset: must be between 0 and " + std::to_string(std::numeric_limits<uint32_t>::max()));
        return std::nullopt;
      }
      query_str << " OFFSET " << offset;
    }
  }

  // Parse query (use per-request parser to avoid data race)
  query::QueryParser query_parser;
  const auto max_query_length = max_query_length_.load(std::memory_order_acquire);
  if (max_query_length > 0) {
    query_parser.SetMaxQueryLength(max_query_length);
  }
  auto parsed_query = query_parser.Parse(query_str.str());
  if (!parsed_query) {
    SendError(res, kHttpBadRequest, "Invalid query: " + parsed_query.error().message());
    return std::nullopt;
  }

  // Apply default limit if LIMIT was not explicitly specified in the request
  if (apply_pagination && !parsed_query->limit_explicit) {
    parsed_query->limit = static_cast<size_t>(default_limit_.load(std::memory_order_acquire));
  }

  // Apply filters from JSON payload
  if (body.contains("filters") && !body["filters"].is_object()) {
    SendError(res, kHttpBadRequest, "Field 'filters' must be an object");
    return std::nullopt;
  }
  if (body.contains("filters")) {
    std::string filter_error;
    if (!ParseFiltersFromJson(body["filters"], *parsed_query, filter_error)) {
      SendError(res, kHttpBadRequest, filter_error);
      return std::nullopt;
    }
  }

  if (body.contains("sort")) {
    std::string sort_error;
    if (!ParseSortFromJson(body["sort"], *parsed_query, sort_error)) {
      SendError(res, kHttpBadRequest, sort_error);
      return std::nullopt;
    }
  }

  if (body.contains("highlight")) {
    std::string highlight_error;
    if (!ParseHighlightFromJson(body["highlight"], *parsed_query, highlight_error)) {
      SendError(res, kHttpBadRequest, highlight_error);
      return std::nullopt;
    }
  }

  if (body.contains("fuzzy")) {
    std::string fuzzy_error;
    if (!ParseFuzzyFromJson(body["fuzzy"], *parsed_query, fuzzy_error)) {
      SendError(res, kHttpBadRequest, fuzzy_error);
      return std::nullopt;
    }
  }

  PreparedHttpQuery prepared;
  prepared.table_ctx = table_ctx;
  prepared.body = std::move(body);
  prepared.query = std::move(*parsed_query);
  return prepared;
}

std::optional<HttpServer::PreparedHttpQuery> HttpServer::PrepareHttpFacetQuery(const httplib::Request& req,
                                                                               httplib::Response& res) {
  if (loading_ != nullptr && loading_->load()) {
    SendError(res, kHttpServiceUnavailable, "Server is loading, please try again later");
    return std::nullopt;
  }

  std::string table = ExtractRouteTableKey(req);
  auto lookup = ResolveHttpTableContext(table, IsQualifiedTableRoute(req));
  if (lookup.table_ctx == nullptr) {
    SendError(res, lookup.status, lookup.message);
    return std::nullopt;
  }
  auto* table_ctx = lookup.table_ctx;

  json body;
  try {
    body = json::parse(req.body);
  } catch (const json::parse_error& e) {
    SendError(res, kHttpBadRequest, "Invalid JSON: " + std::string(e.what()));
    return std::nullopt;
  }

  if (!body.contains("column")) {
    SendError(res, kHttpBadRequest, "Missing required field: column");
    return std::nullopt;
  }
  if (!body["column"].is_string()) {
    SendError(res, kHttpBadRequest, "Field 'column' must be a string");
    return std::nullopt;
  }

  if (body.contains("q") && !body["q"].is_string()) {
    SendError(res, kHttpBadRequest, "Field 'q' must be a string");
    return std::nullopt;
  }

  static constexpr std::array<std::string_view, 4> kFacetRejectedFields = {"offset", "sort", "highlight", "fuzzy"};
  for (const auto field : kFacetRejectedFields) {
    if (body.contains(std::string(field))) {
      SendError(res, kHttpBadRequest, "Field '" + std::string(field) + "' is not supported by FACET");
      return std::nullopt;
    }
  }

  const std::string column = body["column"].get<std::string>();
  if (column.empty()) {
    SendError(res, kHttpBadRequest, "Field 'column' must be non-empty");
    return std::nullopt;
  }
  for (unsigned char c : column) {
    if (std::iscntrl(c) != 0 || std::isspace(c) != 0) {
      SendError(res, kHttpBadRequest, "Field 'column' must not contain whitespace or control characters");
      return std::nullopt;
    }
  }

  std::ostringstream query_str;
  query_str << "FACET " << table << " " << column;

  if (body.contains("q")) {
    std::string query_text = body["q"].get<std::string>();
    for (char c : query_text) {
      if (c == '\r' || c == '\n' || c == '\0') {
        SendError(res, kHttpBadRequest, "Query text contains invalid control characters");
        return std::nullopt;
      }
    }

    if (!query_text.empty()) {
      std::string offending;
      if (!ValidateQueryTextNoReservedClauses(query_text, offending)) {
        SendError(res, kHttpBadRequest,
                  "Reserved keyword '" + offending +
                      "' is not allowed in 'q'. Use the dedicated JSON fields (filters) instead.");
        return std::nullopt;
      }
      query_str << " " << query_text;
    }
  }

  if (body.contains("limit")) {
    if (!body["limit"].is_number_integer()) {
      SendError(res, kHttpBadRequest, "Invalid limit: must be an integer");
      return std::nullopt;
    }
    const int64_t limit = body["limit"].get<int64_t>();
    if (limit <= 0 || limit > config::defaults::kMaxLimit) {
      SendError(res, kHttpBadRequest,
                "Invalid limit: must be between 1 and " + std::to_string(config::defaults::kMaxLimit));
      return std::nullopt;
    }
    query_str << " LIMIT " << limit;
  }

  query::QueryParser query_parser;
  const auto max_query_length = max_query_length_.load(std::memory_order_acquire);
  if (max_query_length > 0) {
    query_parser.SetMaxQueryLength(max_query_length);
  }
  auto parsed_query = query_parser.Parse(query_str.str());
  if (!parsed_query) {
    SendError(res, kHttpBadRequest, "Invalid query: " + parsed_query.error().message());
    return std::nullopt;
  }

  if (body.contains("filters") && !body["filters"].is_object()) {
    SendError(res, kHttpBadRequest, "Field 'filters' must be an object");
    return std::nullopt;
  }
  if (body.contains("filters")) {
    std::string filter_error;
    if (!ParseFiltersFromJson(body["filters"], *parsed_query, filter_error)) {
      SendError(res, kHttpBadRequest, filter_error);
      return std::nullopt;
    }
  }

  PreparedHttpQuery prepared;
  prepared.table_ctx = table_ctx;
  prepared.body = std::move(body);
  prepared.query = std::move(*parsed_query);
  return prepared;
}

void HttpServer::HandleSearch(const httplib::Request& req, httplib::Response& res) {
  RecordRequest();

  try {
    auto prepared = PrepareHttpSearchQuery(req, res, "SEARCH", /*apply_pagination=*/true);
    if (!prepared) {
      return;
    }
    RecordCommand(query::QueryType::SEARCH);
    auto* table_ctx = prepared->table_ctx;
    auto* current_doc_store = table_ctx->doc_store.get();
    auto& query_ref = prepared->query;
    auto* query = &query_ref;

    // Build pipeline parameters via the shared helper. SEARCH attaches BM25
    // stats so the pipeline can score `_score` sorts; COUNT does not.
    auto params = search_pipeline::BuildPipelineParamsFromContext(*table_ctx, full_config_, cache_manager_,
                                                                  SearchHandler::GetFilterThreshold(),
                                                                  /*attach_bm25_stats=*/true);

    // Execute the unified search pipeline
    auto pipeline_output = search_pipeline::ExecuteFullPipeline(*query, params);
    if (!pipeline_output.success) {
      SendError(res, kHttpBadRequest, pipeline_output.error_message);
      return;
    }

    auto& results = pipeline_output.results;
    size_t total_count = results.size();
    auto topn =
        search_pipeline::ApplySearchTopNOptimization(*query, params.current_index, pipeline_output.term_infos,
                                                     pipeline_output.cache_hit, params.primary_key_column, results);
    if (topn.applicable) {
      total_count = topn.total_results;
    }

    auto sorted_result =
        SortHttpResults(results, *query, *table_ctx, pipeline_output, full_config_, params.primary_key_column);
    if (!sorted_result.has_value()) {
      SendError(res, kHttpBadRequest, sorted_result.error().message());
      return;
    }
    auto sorted_results = std::move(sorted_result.value());

    // Build JSON response
    json response;
    response["count"] = total_count;
    response["limit"] = query->limit;
    response["offset"] = query->offset;

    json results_array = json::array();
    auto docs = current_doc_store->GetDocumentsBatch(sorted_results);
    std::vector<std::optional<std::string>> highlight_texts;
    std::vector<std::string> highlight_terms;
    if (query->highlight.has_value()) {
      if (!current_doc_store->IsStoreTextsEnabled()) {
        SendError(res, kHttpBadRequest,
                  "HIGHLIGHT requires normalized text storage. Set memory.verify_text to \"ascii\" or \"all\" in "
                  "configuration.");
        return;
      }
      highlight_texts = current_doc_store->GetNormalizedTextBatch(sorted_results);
      highlight_terms = BuildHighlightTerms(pipeline_output.all_search_terms, *table_ctx);
    }
    for (size_t i = 0; i < docs.size(); ++i) {
      if (docs[i]) {
        json doc_obj;
        doc_obj["doc_id"] = docs[i]->doc_id;
        doc_obj["primary_key"] = docs[i]->primary_key;

        // Add filters
        if (!docs[i]->filters.empty()) {
          json filters_obj;
          for (const auto& [key, val] : docs[i]->filters) {
            filters_obj[key] = FilterValueToJson(val);
          }
          doc_obj["filters"] = filters_obj;
        }

        if (query->highlight.has_value() && i < highlight_texts.size()) {
          if (highlight_texts[i].has_value()) {
            auto hl = query::Highlighter::Generate(*highlight_texts[i], highlight_terms, *query->highlight);
            doc_obj["highlight"] = hl.snippet;
          } else {
            doc_obj["highlight"] = "";
          }
        }

        results_array.push_back(doc_obj);
      }
    }
    response["results"] = results_array;

    SendJson(res, kHttpOk, response);

  } catch (const std::exception& e) {
    mygram::utils::StructuredLog()
        .Event("http_handler_error")
        .Field("handler", "search")
        .Field("error", e.what())
        .Error();
    SendError(res, kHttpInternalServerError, "Internal server error");
  }
}

void HttpServer::HandleCount(const httplib::Request& req, httplib::Response& res) {
  RecordRequest();

  try {
    auto prepared = PrepareHttpSearchQuery(req, res, "COUNT", /*apply_pagination=*/false);
    if (!prepared) {
      return;
    }
    RecordCommand(query::QueryType::COUNT);
    auto* table_ctx = prepared->table_ctx;
    auto& query_ref = prepared->query;
    auto* query = &query_ref;

    // COUNT does not need BM25 stats (no `_score` sort), so leave them off.
    auto params = search_pipeline::BuildPipelineParamsFromContext(*table_ctx, full_config_, cache_manager_,
                                                                  SearchHandler::GetFilterThreshold(),
                                                                  /*attach_bm25_stats=*/false);

    // Execute the unified search pipeline
    auto pipeline_output = search_pipeline::ExecuteFullPipeline(*query, params);
    if (!pipeline_output.success) {
      SendError(res, kHttpBadRequest, pipeline_output.error_message);
      return;
    }

    // Build JSON response - just return count
    json response;
    response["count"] = pipeline_output.results.size();

    SendJson(res, kHttpOk, response);

  } catch (const std::exception& e) {
    mygram::utils::StructuredLog()
        .Event("http_handler_error")
        .Field("handler", "count")
        .Field("error", e.what())
        .Error();
    SendError(res, kHttpInternalServerError, "Internal server error");
  }
}

void HttpServer::HandleFacet(const httplib::Request& req, httplib::Response& res) {
  RecordRequest();

  try {
    auto prepared = PrepareHttpFacetQuery(req, res);
    if (!prepared) {
      return;
    }
    RecordCommand(query::QueryType::FACET);

    auto* table_ctx = prepared->table_ctx;
    auto& query_ref = prepared->query;
    auto* query = &query_ref;
    auto* current_doc_store = table_ctx->doc_store.get();
    auto* current_index = table_ctx->index.get();

    auto filter_index = current_doc_store->GetFilterIndex();
    if (!filter_index) {
      SendError(res, kHttpInternalServerError, "Filter index not available");
      return;
    }

    std::vector<std::pair<std::string, uint64_t>> value_counts;
    const bool has_search = !query->search_text.empty() || !query->and_terms.empty();
    const bool has_not = !query->not_terms.empty();
    const bool has_filters = !query->filters.empty();

    if (has_search || has_not || has_filters) {
      std::vector<storage::DocId> results;
      if (has_search) {
        auto params = search_pipeline::BuildPipelineParamsFromContext(*table_ctx, full_config_, cache_manager_,
                                                                      SearchHandler::GetFilterThreshold(),
                                                                      /*attach_bm25_stats=*/true);
        auto pipeline_output = search_pipeline::ExecuteFullPipeline(*query, params);
        if (!pipeline_output.success) {
          SendError(res, kHttpBadRequest, pipeline_output.error_message);
          return;
        }
        results = std::move(pipeline_output.results);
      } else {
        results = current_doc_store->GetAllDocIds();

        if (has_not) {
          results = search_pipeline::ApplyNotFilter(results, query->not_terms, current_index,
                                                    table_ctx->config.ngram_size, table_ctx->config.kanji_ngram_size,
                                                    table_ctx->config.cross_boundary_ngrams);
        }

        if (has_filters) {
          results = search_pipeline::ApplyFiltersWithBitmap(results, query->filters, current_doc_store);
        }
      }

      if (!results.empty()) {
        auto result_bitmap = mygram::utils::MakeRoaringFromVector(results);
        if (result_bitmap == nullptr) {
          SendError(res, kHttpInternalServerError, "Failed to allocate result bitmap");
          return;
        }
        value_counts = filter_index->GetColumnValueCountsFiltered(query->facet_column, result_bitmap.get());
      }
    } else {
      value_counts = filter_index->GetColumnValueCounts(query->facet_column);
    }

    if (query->limit_explicit && value_counts.size() > query->limit) {
      value_counts.resize(query->limit);
    }

    json facets = json::array();
    for (auto& [serialized, count] : value_counts) {
      facets.push_back({
          {"value", storage::FilterIndex::DeserializeToDisplayString(serialized)},
          {"count", count},
      });
    }

    json response;
    response["column"] = query->facet_column;
    response["count"] = facets.size();
    response["facets"] = std::move(facets);

    SendJson(res, kHttpOk, response);
  } catch (const std::exception& e) {
    mygram::utils::StructuredLog()
        .Event("http_handler_error")
        .Field("handler", "facet")
        .Field("error", e.what())
        .Error();
    SendError(res, kHttpInternalServerError, "Internal server error");
  }
}

void HttpServer::HandleGet(const httplib::Request& req, httplib::Response& res) {
  RecordRequest();

  try {
    // Check if server is loading
    if (loading_ != nullptr && loading_->load()) {
      SendError(res, kHttpServiceUnavailable, "Server is loading, please try again later");
      return;
    }

    // Extract table name and primary key from URL. Use the shared resolution helper so
    // GET applies the same table-name whitelist and null-context guards as
    // SEARCH and COUNT.
    std::string primary_key = ExtractRoutePrimaryKey(req);
    auto lookup = ResolveHttpTableContext(ExtractRouteTableKey(req), IsQualifiedTableRoute(req));
    if (lookup.table_ctx == nullptr) {
      SendError(res, lookup.status, lookup.message);
      return;
    }
    auto* current_doc_store = lookup.table_ctx->doc_store.get();

    RecordCommand(query::QueryType::GET);

    auto doc_id = current_doc_store->GetDocId(primary_key);
    if (!doc_id.has_value()) {
      SendError(res, kHttpNotFound, "Document not found");
      return;
    }

    auto doc = current_doc_store->GetDocument(*doc_id);
    if (!doc) {
      SendError(res, kHttpNotFound, "Document not found");
      return;
    }

    // Build JSON response
    json response;
    response["doc_id"] = doc->doc_id;
    response["primary_key"] = doc->primary_key;

    if (!doc->filters.empty()) {
      json filters_obj;
      for (const auto& [key, val] : doc->filters) {
        filters_obj[key] = FilterValueToJson(val);
      }
      response["filters"] = filters_obj;
    }

    SendJson(res, kHttpOk, response);

  } catch (const std::exception& e) {
    mygram::utils::StructuredLog().Event("http_handler_error").Field("handler", "get").Field("error", e.what()).Error();
    SendError(res, kHttpInternalServerError, "Internal server error");
  }
}

void HttpServer::HandleInfo(const httplib::Request& /*req*/, httplib::Response& res) {
  // Increment request counter on the effective stats instance
  RecordRequest();
  RecordCommand(query::QueryType::INFO);

  try {
    json response;

    // Use TCP server's stats if available (includes all protocol stats), otherwise use HTTP-only stats
    ServerStats& effective_stats = GetEffectiveStats();

    // Server info
    response["server"] = "MygramDB";
    response["version"] = ::mygramdb::Version::String();
    response["uptime_seconds"] = effective_stats.GetUptimeSeconds();

    // Statistics (from TCP server if available)
    auto srv_stats = effective_stats.GetStatistics();
    response["total_requests"] = srv_stats.total_requests;
    response["total_commands_processed"] = srv_stats.total_commands_processed;

    // Aggregate memory and index statistics across all tables
    size_t total_index_memory = 0;
    size_t total_doc_memory = 0;
    size_t total_documents = 0;
    size_t total_terms = 0;
    size_t total_postings = 0;
    size_t total_delta_encoded = 0;
    size_t total_roaring_bitmap = 0;

    json tables_obj;
    for (const auto& [table_name, ctx] : table_contexts_) {
      size_t index_mem = ctx->index->MemoryUsage();
      size_t doc_mem = ctx->doc_store->MemoryUsage();
      auto idx_stats = ctx->index->GetStatistics();

      total_index_memory += index_mem;
      total_doc_memory += doc_mem;
      total_documents += ctx->doc_store->Size();
      total_terms += idx_stats.total_terms;
      total_postings += idx_stats.total_postings;
      total_delta_encoded += idx_stats.delta_encoded_lists;
      total_roaring_bitmap += idx_stats.roaring_bitmap_lists;

      // Per-table stats
      json table_obj;
      table_obj["documents"] = ctx->doc_store->Size();
      table_obj["terms"] = idx_stats.total_terms;
      table_obj["postings"] = idx_stats.total_postings;
      table_obj["ngram_size"] = ctx->config.ngram_size;
      table_obj["memory_bytes"] = index_mem + doc_mem;
      table_obj["memory_human"] = mygram::utils::FormatBytes(index_mem + doc_mem);
      tables_obj[table_name] = table_obj;
    }

    size_t total_memory = total_index_memory + total_doc_memory;

    // Update memory usage on the effective stats instance
    effective_stats.UpdateMemoryUsage(total_memory);

    json memory_obj;
    memory_obj["used_memory_bytes"] = total_memory;
    memory_obj["used_memory_human"] = mygram::utils::FormatBytes(total_memory);
    const auto peak_memory = effective_stats.GetPeakMemoryUsage();
    memory_obj["peak_memory_bytes"] = peak_memory;
    memory_obj["peak_memory_human"] = mygram::utils::FormatBytes(peak_memory);
    memory_obj["used_memory_index"] = mygram::utils::FormatBytes(total_index_memory);
    memory_obj["used_memory_documents"] = mygram::utils::FormatBytes(total_doc_memory);

    // System memory information
    auto sys_info = mygram::utils::GetSystemMemoryInfo();
    if (sys_info) {
      memory_obj["total_system_memory"] = sys_info->total_physical_bytes;
      memory_obj["total_system_memory_human"] = mygram::utils::FormatBytes(sys_info->total_physical_bytes);
      memory_obj["available_system_memory"] = sys_info->available_physical_bytes;
      memory_obj["available_system_memory_human"] = mygram::utils::FormatBytes(sys_info->available_physical_bytes);
      if (sys_info->total_physical_bytes > 0) {
        double usage_ratio = 1.0 - static_cast<double>(sys_info->available_physical_bytes) /
                                       static_cast<double>(sys_info->total_physical_bytes);
        memory_obj["system_memory_usage_ratio"] = usage_ratio;
      }
    }

    // Process memory information
    auto proc_info = mygram::utils::GetProcessMemoryInfo();
    if (proc_info) {
      memory_obj["process_rss"] = proc_info->rss_bytes;
      memory_obj["process_rss_human"] = mygram::utils::FormatBytes(proc_info->rss_bytes);
      memory_obj["process_rss_peak"] = proc_info->peak_rss_bytes;
      memory_obj["process_rss_peak_human"] = mygram::utils::FormatBytes(proc_info->peak_rss_bytes);
    }

    // Memory health status
    auto health = mygram::utils::GetMemoryHealthStatus();
    memory_obj["memory_health"] = mygram::utils::MemoryHealthStatusToString(health);

    response["memory"] = memory_obj;

    // Aggregated index statistics
    json index_obj;
    index_obj["total_documents"] = total_documents;
    index_obj["total_terms"] = total_terms;
    index_obj["total_postings"] = total_postings;
    if (total_terms > 0) {
      index_obj["avg_postings_per_term"] = static_cast<double>(total_postings) / static_cast<double>(total_terms);
    }
    index_obj["delta_encoded_lists"] = total_delta_encoded;
    index_obj["roaring_bitmap_lists"] = total_roaring_bitmap;
    response["index"] = index_obj;

    // Per-table breakdown
    response["tables"] = tables_obj;

    // Cache statistics
    json cache_obj;
    if (cache_manager_ != nullptr && cache_manager_->IsEnabled()) {
      auto cache_stats = cache_manager_->GetStatistics();
      cache_obj["enabled"] = true;
      cache_obj["hits"] = cache_stats.cache_hits;
      cache_obj["misses"] = cache_stats.cache_misses;
      cache_obj["misses_not_found"] = cache_stats.cache_misses_not_found;
      cache_obj["misses_ttl_expired"] = cache_stats.cache_misses_ttl_expired;
      cache_obj["misses_invalidated"] = cache_stats.cache_misses_invalidated;
      cache_obj["total_queries"] = cache_stats.total_queries;
      cache_obj["hit_rate"] = cache_stats.HitRate();
      cache_obj["current_entries"] = cache_stats.current_entries;
      cache_obj["memory_bytes"] = cache_stats.current_memory_bytes;
      cache_obj["memory_human"] = mygram::utils::FormatBytes(cache_stats.current_memory_bytes);
      cache_obj["evictions"] = cache_stats.evictions;
      cache_obj["ttl_expirations"] = cache_stats.ttl_expirations;
      cache_obj["invalidations_immediate"] = cache_stats.invalidations_immediate;
      cache_obj["invalidations_deferred"] = cache_stats.invalidations_deferred;
      cache_obj["invalidations_batches"] = cache_stats.invalidations_batches;
      cache_obj["avg_hit_latency_ms"] = cache_stats.AverageCacheHitLatency();
      cache_obj["avg_miss_latency_ms"] = cache_stats.AverageCacheMissLatency();
      cache_obj["total_time_saved_ms"] = cache_stats.TotalTimeSaved();
    } else {
      cache_obj["enabled"] = false;
    }
    response["cache"] = cache_obj;

    SendJson(res, kHttpOk, response);

  } catch (const std::exception& e) {
    mygram::utils::StructuredLog()
        .Event("http_handler_error")
        .Field("handler", "info")
        .Field("error", e.what())
        .Error();
    SendError(res, kHttpInternalServerError, "Internal server error");
  }
}

void HttpServer::HandleHealth(const httplib::Request& /*req*/, httplib::Response& res) {
  // Health probes are intentionally NOT counted in total_requests:
  // they are typically driven by orchestrators (Kubernetes liveness/readiness)
  // at high frequency and would distort QPS metrics for actual application traffic.
  // If you need a separate counter for probe rate, add a dedicated metric instead
  // of resurrecting RecordRequest() here.
  json response;
  response["status"] = "ok";
  response["timestamp"] =
      std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();

  SendJson(res, kHttpOk, response);
}

void HttpServer::HandleHealthLive(const httplib::Request& /*req*/, httplib::Response& res) {
  // Health probe — not counted in total_requests; see HandleHealth.
  // Liveness probe: Always return 200 OK if the process is running
  // This is used by orchestrators (Kubernetes, Docker) to detect deadlocks
  json response;
  response["status"] = "alive";
  response["timestamp"] =
      std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();

  SendJson(res, kHttpOk, response);
}

void HttpServer::HandleHealthReady(const httplib::Request& /*req*/, httplib::Response& res) {
  // Health probe — not counted in total_requests; see HandleHealth.
  // Readiness probe: Return 200 OK if ready to accept traffic, 503 otherwise
  const bool is_loading = (loading_ != nullptr && loading_->load());
  const bool replication_paused_for_dump =
      replication_paused_for_dump_ != nullptr && replication_paused_for_dump_->load(std::memory_order_acquire);
#ifdef USE_MYSQL
  const bool replication_unavailable =
      (binlog_reader_ != nullptr && !binlog_reader_->IsRunning() && !replication_paused_for_dump);
#else
  const bool replication_unavailable = false;
#endif
  bool is_ready = !is_loading && !replication_unavailable;

  json response;
  response["loading"] = is_loading;
#ifdef USE_MYSQL
  if (binlog_reader_ != nullptr) {
    response["replication_running"] = !replication_unavailable;
    response["replication_paused_for_dump"] = replication_paused_for_dump;
  }
#endif

  if (is_ready) {
    response["status"] = "ready";
    response["timestamp"] =
        std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    SendJson(res, kHttpOk, response);
  } else {
    response["status"] = "not_ready";
    response["reason"] = is_loading ? "Server is loading" : "Replication is not running";
    response["timestamp"] =
        std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    SendJson(res, kHttpServiceUnavailable, response);
  }
}

void HttpServer::HandleHealthDetail(const httplib::Request& /*req*/, httplib::Response& res) {
  // Health probe — not counted in total_requests; see HandleHealth.
  // Detailed health: Return comprehensive component status
  json response;

  // Overall status
  bool is_loading = (loading_ != nullptr && loading_->load());
  const bool replication_paused_for_dump =
      replication_paused_for_dump_ != nullptr && replication_paused_for_dump_->load(std::memory_order_acquire);
#ifdef USE_MYSQL
  const bool replication_unavailable =
      (binlog_reader_ != nullptr && !binlog_reader_->IsRunning() && !replication_paused_for_dump);
#else
  const bool replication_unavailable = false;
#endif
  response["status"] = (is_loading || replication_unavailable) ? "degraded" : "healthy";
  response["timestamp"] =
      std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();

  // Uptime from this HttpServer instance's construction time
  auto uptime =
      std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - start_time_).count();
  response["uptime_seconds"] = uptime;

  // Components status
  json components;

  // Server component
  json server_comp;
  server_comp["status"] = is_loading ? "loading" : "ready";
  server_comp["loading"] = is_loading;
  components["server"] = server_comp;

  // Index component (aggregate from all tables)
  json index_comp;
  size_t total_terms = 0;
  size_t total_documents = 0;
  for (const auto& [table_name, ctx] : table_contexts_) {
    if (ctx != nullptr && ctx->index) {
      total_terms += ctx->index->TermCount();
      // Note: Index doesn't have document count method, use doc_store instead
      if (ctx->doc_store != nullptr) {
        total_documents += ctx->doc_store->Size();
      }
    }
  }
  index_comp["status"] = "ok";
  index_comp["total_terms"] = total_terms;
  index_comp["total_documents"] = total_documents;
  components["index"] = index_comp;

  // Cache component (if available)
  if (cache_manager_ != nullptr) {
    json cache_comp;
    auto cache_stats = cache_manager_->GetStatistics();
    const bool cache_enabled = cache_manager_->IsEnabled();
    cache_comp["status"] = cache_enabled ? "ok" : "disabled";
    cache_comp["enabled"] = cache_enabled;
    cache_comp["hit_rate"] = cache_stats.HitRate();
    cache_comp["total_hits"] = cache_stats.cache_hits;
    cache_comp["total_misses"] = cache_stats.cache_misses;
    cache_comp["current_entries"] = cache_stats.current_entries;
    components["cache"] = cache_comp;
  }

#ifdef USE_MYSQL
  // Binlog component (if available)
  if (binlog_reader_ != nullptr) {
    json binlog_comp;
    if (binlog_reader_->IsRunning()) {
      binlog_comp["status"] = "connected";
      binlog_comp["running"] = true;
      binlog_comp["current_gtid"] = binlog_reader_->GetCurrentGTID();
      binlog_comp["processed_events"] = binlog_reader_->GetProcessedEvents();
      binlog_comp["queue_size"] = binlog_reader_->GetQueueSize();
    } else {
      binlog_comp["status"] = replication_paused_for_dump ? "paused_for_dump" : "disconnected";
      binlog_comp["running"] = false;
      binlog_comp["paused_for_dump"] = replication_paused_for_dump;
    }
    components["binlog"] = binlog_comp;
  }
#endif

  response["components"] = components;

  SendJson(res, kHttpOk, response);
}

void HttpServer::HandleConfig(const httplib::Request& /*req*/, httplib::Response& res) {
  RecordRequest();
  RecordCommand(query::QueryType::CONFIG_SHOW);

  if (full_config_ == nullptr) {
    SendError(res, kHttpInternalServerError, "Configuration not available");
    return;
  }

  try {
    json response;

    // MySQL config summary (no credentials)
    json mysql_obj;
    mysql_obj["configured"] = !full_config_->mysql.user.empty() || !full_config_->mysql.host.empty();
    mysql_obj["database_defined"] = !full_config_->mysql.database.empty();
    response["mysql"] = mysql_obj;

    // API config summary (no bind/port exposure)
    json api_obj;
    api_obj["tcp"]["enabled"] = true;
    api_obj["http"]["enabled"] = full_config_->api.http.enable;
    api_obj["http"]["cors_enabled"] = full_config_->api.http.enable_cors;
    response["api"] = api_obj;

    // Network ACL status
    json net_obj;
    net_obj["allow_cidrs_configured"] = !full_config_->network.allow_cidrs.empty();
    response["network"] = net_obj;

    // Replication config summary
    json repl_obj;
    repl_obj["enable"] = full_config_->replication.enable;
    response["replication"] = repl_obj;

    response["notes"] =
        "Sensitive configuration values are redacted over HTTP. Use CONFIG SHOW over a secured connection for details.";

    SendJson(res, kHttpOk, response);

  } catch (const std::exception& e) {
    mygram::utils::StructuredLog()
        .Event("http_handler_error")
        .Field("handler", "config")
        .Field("error", e.what())
        .Error();
    SendError(res, kHttpInternalServerError, "Internal server error");
  }
}

void HttpServer::HandleReplicationStatus(const httplib::Request& /*req*/, httplib::Response& res) {
  RecordRequest();
  RecordCommand(query::QueryType::REPLICATION_STATUS);

#ifdef USE_MYSQL
  if (binlog_reader_ == nullptr) {
    SendError(res, kHttpServiceUnavailable, "Replication not configured");
    return;
  }

  try {
    json response;
    const bool is_running = binlog_reader_->IsRunning();
    response["enabled"] = is_running;
    response["status"] = is_running ? "running" : "stopped";
    response["current_gtid"] = binlog_reader_->GetCurrentGTID();
    response["processed_events"] = binlog_reader_->GetProcessedEvents();
    response["queue_size"] = binlog_reader_->GetQueueSize();

    SendJson(res, kHttpOk, response);

  } catch (const std::exception& e) {
    mygram::utils::StructuredLog()
        .Event("http_handler_error")
        .Field("handler", "replication_status")
        .Field("error", e.what())
        .Error();
    SendError(res, kHttpInternalServerError, "Internal server error");
  }
#else
  SendError(res, kHttpServiceUnavailable, "MySQL replication not compiled");
#endif
}

void HttpServer::HandleMetrics(const httplib::Request& /*req*/, httplib::Response& res) {
  RecordRequest();

  try {
    ServerStats& effective_stats = GetEffectiveStats();

    // Aggregate metrics
    auto aggregated_metrics = StatisticsService::AggregateMetrics(table_contexts_);

    // Update server statistics
    StatisticsService::UpdateServerStatistics(effective_stats, aggregated_metrics);

    // Format response
    std::string metrics = ResponseFormatter::FormatPrometheusMetrics(aggregated_metrics, effective_stats,
                                                                     table_contexts_, binlog_reader_, cache_manager_);
    res.status = kHttpOk;
    res.set_content(metrics, "text/plain; version=0.0.4; charset=utf-8");
  } catch (const std::exception& e) {
    mygram::utils::StructuredLog()
        .Event("http_handler_error")
        .Field("handler", "metrics")
        .Field("error", e.what())
        .Error();
    SendError(res, kHttpInternalServerError, "Internal server error");
  }
}

void HttpServer::SendJson(httplib::Response& res, int status_code, const nlohmann::json& body) {
  res.status = status_code;
  res.set_content(body.dump(-1, ' ', false, nlohmann::json::error_handler_t::replace), "application/json");
}

void HttpServer::SendError(httplib::Response& res, int status_code, const std::string& message) {
  json error_obj;
  error_obj["error"] = message;
  SendJson(res, status_code, error_obj);
}

}  // namespace mygramdb::server
