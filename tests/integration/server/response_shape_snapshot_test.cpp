/**
 * @file response_shape_snapshot_test.cpp
 * @brief Golden snapshot of the client-visible response shapes of both
 *        protocol surfaces.
 *
 * Every TCP command and every HTTP route is exercised against one fixed
 * in-memory dataset, in a fixed order, and the rendered request/response pairs
 * are compared against `spec/response-shapes.snapshot.txt`. A diff therefore
 * means a client receives different bytes than it used to, regardless of
 * whether any behavioural unit test still passes.
 *
 * The golden records what the server ACTUALLY emits, including output that may
 * look wrong. Fixing a response is a separate change that must update the
 * golden deliberately.
 *
 * Regenerating: run with `MYGRAMDB_UPDATE_SNAPSHOT=1` in the environment. The
 * test then rewrites the golden and reports itself as skipped, so a
 * regeneration run can never be mistaken for a passing verification run.
 */

#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <httplib.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <functional>
#include <memory>
#include <nlohmann/json.hpp>
#include <regex>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "client/protocol_detection.h"
#include "config/config.h"
#include "server/http_server.h"
#include "server/tcp_server.h"
#include "server/tcp_server_test_helpers.h"
#include "support/network_test_utils.h"

#ifndef MYGRAMDB_RESPONSE_SHAPE_GOLDEN
#error "MYGRAMDB_RESPONSE_SHAPE_GOLDEN must be injected by CMake"
#endif

using json = nlohmann::json;

namespace mygramdb {
namespace server {
namespace {

// ---------------------------------------------------------------------------
// Normalization contract
// ---------------------------------------------------------------------------
//
// This table is the real contract of the test: anything listed here is
// deliberately NOT pinned, everything else is. Each entry replaces a volatile
// value with a TYPED placeholder so that a field which changes type (int ->
// string, scalar -> object) still fails the comparison, and so that the
// field's presence and position stay pinned.
//
// One volatile property cannot be expressed as a pattern and is handled by
// `CanonicalizeDocumentFilterOrder` immediately below this table: the TCP
// `OK DOC` frame lists a document's filter columns in `absl::flat_hash_map`
// iteration order, which the allocator randomizes per process.
//
// Patterns run over the fully rendered text in table order. `(\n|^)` is used
// instead of a multiline anchor because `std::regex_constants::multiline` is
// not available on every standard library the project builds against.
// `[<\s]*` absorbs the `< ` response-line prefix and JSON indentation, so one
// entry covers both the text protocol and the JSON surface.
struct Normalization {
  const char* why;          ///< Why this value cannot be pinned.
  const char* pattern;      ///< ECMAScript regex over the rendered text.
  const char* replacement;  ///< Typed placeholder preserving the field's shape.
};

const std::vector<Normalization>& NormalizationRules() {
  static const std::vector<Normalization> kRules = {
      // --- Build identity ---------------------------------------------------
      // Matched on a semver-shaped value so the dump format's integer
      // `version:` field stays pinned.
      {"the server version is derived from the git tag (or $MYGRAMDB_VERSION) at build time",
       R"RE((\n|^)([<\s]*"?version"?"?\s*:\s*"?)[0-9]+\.[0-9]+\.[0-9]+[^"\\\r\n,]*)RE", "$1$2<VERSION>"},
      // The TCP banner spells the same value as `MygramDB v<semver>`, which the
      // rule above cannot match because the field does not open on a digit.
      {"the TCP INFO banner carries the same build-time version",
       R"RE((\n|^)([<\s]*version\s*:\s*MygramDB v)[0-9]+\.[0-9]+\.[0-9]+[^\\\r\n]*)RE", "$1$2<VERSION>"},
      {"the Prometheus server_info label carries the same build-time version",
       R"RE(mygramdb_server_info\{version="[^"]*"\})RE", "mygramdb_server_info{version=\"<VERSION>\"}"},

      // --- Wall clock -------------------------------------------------------
      {"uptime grows with the wall clock between fixture setup and the request",
       R"RE((\n|^)([<\s]*"?uptime_seconds"?"?\s*:\s*)-?[0-9]+)RE", "$1$2<INT>"},
      {"health payloads and dump metadata stamp the absolute time they were produced",
       R"RE((\n|^)([<\s]*"?timestamp"?"?\s*:\s*)-?[0-9]+)RE", "$1$2<TIMESTAMP>"},
      {"the Prometheus uptime gauge is the same wall-clock derived value",
       R"RE((\n|^)(< mygramdb_server_uptime_seconds )-?[0-9]+)RE", "$1$2<INT>"},

      // --- Elapsed-time measurements ---------------------------------------
      // The unit suffix is left in place: only the measured number is volatile.
      {"DUMP STATUS reports how long the running or last operation took",
       R"RE((\n|^)([<\s]*"?elapsed_seconds"?"?\s*:\s*)[0-9]+\.[0-9]+)RE", "$1$2<DURATION_S>"},
      {"DEBUG blocks report the per-stage timings of the query that produced them",
       R"RE((\n|^)([<\s]*(query_time|parse_time|index_time|filter_time|cache_cost_ms|cache_age_ms|cache_saved_ms)\s*:\s*)[0-9]+(\.[0-9]+)?)RE",
       "$1$2<DURATION_MS>"},
      {"cache latency averages are measured from the queries earlier cases ran",
       R"RE((\n|^)([<\s]*"?(avg_hit_latency_ms|avg_miss_latency_ms|avg_cache_miss_time_ms|total_time_saved_ms|cache_avg_hit_latency_ms|cache_avg_miss_latency_ms|cache_total_time_saved_ms)"?"?\s*:\s*)[0-9]+(\.[0-9]+)?(e-?[0-9]+)?)RE",
       "$1$2<DURATION_MS>"},

      // --- Memory accounting ------------------------------------------------
      // The human-readable form is normalized first: it starts with the same
      // digits as the byte-count form, so the integer rule would otherwise
      // consume the number and leave a bare unit suffix behind.
      {"human-readable memory renders allocator- and host-dependent numbers",
       R"RE((\n|^)([<\s]*"?(used_memory_human|used_memory_peak_human|peak_memory_human|used_memory_index|used_memory_documents|memory_human|total_system_memory|total_system_memory_human|available_system_memory|available_system_memory_human|process_rss|process_rss_human|process_rss_peak|process_rss_peak_human)"?"?\s*:\s*"?)[0-9]+(\.[0-9]+)? ?[KMGT]?i?B)RE",
       "$1$2<BYTES_HUMAN>"},
      {"memory byte counts reflect allocator and host state, not response shape",
       R"RE((\n|^)([<\s]*"?(used_memory_bytes|used_memory_peak_bytes|peak_memory_bytes|memory_bytes|total_system_memory|available_system_memory|total_physical_bytes|available_physical_bytes|process_rss|process_rss_peak|rss_bytes|peak_rss_bytes)"?"?\s*:\s*)-?[0-9]+)RE",
       "$1$2<INT>"},
      {"fragmentation and system-usage ratios are computed from live memory",
       R"RE((\n|^)([<\s]*"?(memory_fragmentation_ratio|system_memory_usage_ratio)"?"?\s*:\s*)[0-9]+\.[0-9]+)RE",
       "$1$2<FLOAT>"},
      {"memory health is classified from live host memory pressure",
       R"RE((\n|^)([<\s]*"?memory_health"?"?\s*:\s*"?)[A-Za-z_]+)RE", "$1$2<ENUM>"},
      {"the OPTIMIZE summary reports the rebuilt index's allocator-dependent size",
       R"RE(memory=[0-9]+(\.[0-9]+)? ?[KMGT]?i?B)RE", "memory=<BYTES_HUMAN>"},
      {"the Prometheus memory ratios are computed from live memory",
       R"RE((\n|^)(< mygramdb_memory_(fragmentation_ratio|system_usage_ratio) )[0-9]+(\.[0-9]+)?)RE", "$1$2<FLOAT>"},
      {"the Prometheus memory gauges expose the same allocator-dependent numbers",
       R"RE((\n|^)(< mygramdb_memory_[a-z_]+(\{[^}]*\})? )-?[0-9]+(\.[0-9]+)?)RE", "$1$2<INT>"},

      // --- Filesystem -------------------------------------------------------
      {"dump artifacts are written to a temporary directory unique to each run",
       R"RE(/[^\s"\\]*mygramdb_response_shapes[^\s"\\]*)RE", "<PATH>"},
      {"the dump embeds the run's temporary directory, so its byte size varies with that path",
       R"RE((\n|^)([<\s]*file_size\s*:\s*)[0-9]+)RE", "$1$2<INT>"},

      // --- Request-derived counters ----------------------------------------
      // These pin the field but never its value: they accumulate over whatever
      // cases ran earlier in the request set.
      {"traffic counters accumulate over the cases that ran before this one",
       R"RE((\n|^)([<\s]*"?(total_requests|total_commands_processed|total_connections_received|connected_clients|cmd_search|cmd_count|cmd_get|cmd_info|cmd_save|cmd_load|cmd_config|cmd_other|cmd_unknown|cmd_replication_status|cmd_replication_stop|cmd_replication_start)"?"?\s*:\s*)-?[0-9]+)RE",
       "$1$2<INT>"},
      {"cache counters accumulate over the queries the earlier cases issued",
       R"RE((\n|^)([<\s]*"?(cache_hits|cache_misses|cache_misses_not_found|cache_misses_ttl_expired|cache_misses_invalidated|cache_total_queries|cache_current_entries|cache_memory_bytes|cache_memory_human|cache_invalidation_queue_memory_bytes|cache_accounted_memory_bytes|cache_accounted_memory_human|cache_evictions|cache_ttl_expirations|cache_rejections|cache_rejection_oversize|cache_rejection_memory_budget|cache_rejection_duplicate|cache_stale_entry_removals|cache_decompression_failures|cache_stale_lru_entries|cache_forced_clears|cache_invalidations_immediate|cache_invalidations_deferred|cache_invalidations_batches|total_queries|hits|misses|misses_not_found|misses_ttl_expired|misses_invalidated|total_hits|total_misses|current_entries|current_memory_bytes|accounted_memory_bytes|accounted_memory_human|invalidation_index_memory_bytes|invalidation_queue_memory_bytes|evictions|ttl_expirations|rejection_count|rejection_oversize|rejection_memory_budget|rejection_duplicate|stale_entry_removals|decompression_failures|stale_lru_entries|invalidations_immediate|invalidations_deferred|invalidations_batches)"?"?\s*:\s*"?)[0-9]+(\.[0-9]+)?( ?[KMGT]?i?B)?)RE",
       "$1$2<INT>"},
      {"the cache hit rate is derived from those same accumulated counters",
       R"RE((\n|^)([<\s]*"?(hit_rate|cache_hit_rate)"?"?\s*:\s*)[0-9]+(\.[0-9]+)?)RE", "$1$2<FLOAT>"},
      {"Prometheus request, command and client counters accumulate the same way",
       R"RE((\n|^)(< mygramdb_(requests|commands|connections|clients|command|server_commands)[a-z_]*(\{[^}]*\})? )-?[0-9]+)RE",
       "$1$2<INT>"},
      {"the Prometheus cache hit rate is derived from accumulated counters",
       R"RE((\n|^)(< mygramdb_cache_hit_rate )[0-9]+(\.[0-9]+)?)RE", "$1$2<FLOAT>"},
      {"Prometheus cache counters accumulate too; the four configured ceilings stay pinned",
       R"RE((\n|^)(< mygramdb_cache_(?!max_memory_bytes|min_query_cost_ms|ttl_seconds|compression_enabled)[a-z_]+(\{[^}]*\})? )-?[0-9]+(\.[0-9]+)?)RE",
       "$1$2<INT>"},

      // --- Concurrency ------------------------------------------------------
      {"the request-drain worker count follows the host CPU count and the queue drains asynchronously",
       R"RE((\n|^)(< mygramdb_thread_pool_(workers|queue_depth) )-?[0-9]+)RE", "$1$2<INT>"},
  };
  return kRules;
}

/// Sort the `column=value` tokens of a TCP `OK DOC` frame.
///
/// The document's filter columns are stored in an `absl::flat_hash_map`, whose
/// iteration order is randomized per process, so the server emits them in a
/// different order between runs. This canonicalizes the order only: the set of
/// columns, their values and the frame's shape all stay pinned, and a column
/// that appears or disappears still fails the comparison.
std::string CanonicalizeDocumentFilterOrder(const std::string& text) {
  static const std::string kPrefix = "< OK DOC ";
  std::string result;
  std::istringstream stream(text);
  std::string line;
  while (std::getline(stream, line)) {
    if (line.rfind(kPrefix, 0) == 0) {
      std::istringstream fields(line.substr(kPrefix.size()));
      std::string primary_key;
      fields >> primary_key;
      std::vector<std::string> columns;
      std::string column;
      while (fields >> column) {
        columns.push_back(column);
      }
      // The final token carries the rendered CR marker; sorting it with the
      // rest would move the frame terminator, so it is held back.
      std::string terminator;
      if (!columns.empty() && columns.back().size() >= 2 &&
          columns.back().compare(columns.back().size() - 2, 2, "\\r") == 0) {
        terminator = "\\r";
        columns.back().erase(columns.back().size() - 2);
      }
      std::sort(columns.begin(), columns.end());
      line = kPrefix + primary_key;
      for (const auto& sorted_column : columns) {
        line += " " + sorted_column;
      }
      line += terminator;
    }
    result += line;
    result += '\n';
  }
  return result;
}

std::string Normalize(const std::string& text) {
  std::string result = CanonicalizeDocumentFilterOrder(text);
  for (const auto& rule : NormalizationRules()) {
    result = std::regex_replace(result, std::regex(rule.pattern), rule.replacement);
  }
  return result;
}

// ---------------------------------------------------------------------------
// Rendering helpers
// ---------------------------------------------------------------------------

/// Render a raw protocol frame so every line is prefixed and the frame's line
/// terminators stay visible: CR is emitted as the two characters `\r`, so a
/// response that loses its CRLF framing fails the comparison.
std::string RenderFrame(const std::string& raw, const char* prefix) {
  if (raw.empty()) {
    return std::string(prefix, 1) + "\n";
  }
  std::string out;
  std::string line;
  auto flush = [&]() {
    // A blank line renders as the bare marker: a trailing space would be at the
    // mercy of any whitespace-stripping tool that touches the golden file.
    out += line.empty() ? std::string(prefix, 1) : prefix + line;
    out += '\n';
    line.clear();
  };
  for (const char chr : raw) {
    if (chr == '\n') {
      flush();
    } else if (chr == '\r') {
      line += "\\r";
    } else if (static_cast<unsigned char>(chr) < 0x20) {
      char buf[8];
      std::snprintf(buf, sizeof(buf), "\\x%02X", static_cast<unsigned char>(chr));
      line += buf;
    } else {
      line += chr;
    }
  }
  if (!line.empty()) {
    flush();
  }
  return out;
}

/// Pretty-print a JSON body so key order is canonical (nlohmann objects are
/// ordered maps) and diffs point at a single field.
std::string RenderBody(const std::string& body, const std::string& content_type) {
  if (content_type.find("json") != std::string::npos) {
    json parsed = json::parse(body, nullptr, false);
    if (!parsed.is_discarded()) {
      return RenderFrame(parsed.dump(2), "< ");
    }
  }
  return RenderFrame(body, "< ");
}

// ---------------------------------------------------------------------------
// TCP session
// ---------------------------------------------------------------------------

/// One persistent TCP connection. Persistence matters: AUTH and DEBUG ON are
/// per-connection state, so administrative cases must share a socket with the
/// AUTH that authorized them.
class TcpSession {
 public:
  ~TcpSession() { Close(); }

  bool Open(uint16_t port) {
    fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd_ < 0) {
      return false;
    }
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    ::inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
    if (::connect(fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
      Close();
      return false;
    }
    timeval timeout{};
    timeout.tv_sec = 5;
    ::setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    return true;
  }

  void Close() {
    if (fd_ >= 0) {
      ::close(fd_);
      fd_ = -1;
    }
  }

  /// Send one request and read until the client-side framing rule says the
  /// response is complete. Returns a marker instead of blocking forever when
  /// the server never completes a frame.
  std::string Send(const std::string& request) {
    const std::string wire = request + "\r\n";
    if (::send(fd_, wire.data(), wire.size(), 0) < 0) {
      return "<SEND_FAILED>";
    }
    std::string response;
    client::detail::ResponseCompletionState state;
    std::array<char, 8192> buffer{};
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (std::chrono::steady_clock::now() < deadline) {
      const ssize_t received = ::recv(fd_, buffer.data(), buffer.size(), 0);
      if (received <= 0) {
        break;
      }
      response.append(buffer.data(), static_cast<size_t>(received));
      if (client::detail::IsResponseComplete(response, state)) {
        return response;
      }
    }
    return response.empty() ? "<NO_RESPONSE>" : response;
  }

 private:
  int fd_ = -1;
};

}  // namespace

// ---------------------------------------------------------------------------
// Fixture
// ---------------------------------------------------------------------------

class ResponseShapeSnapshotTest : public ::testing::Test {
 protected:
  static constexpr const char* kAdminToken = "shape-admin-token";

  void SetUp() override {
    mygramdb::test::SkipIfSocketCreationBlocked();

    temp_dir_ = std::filesystem::temp_directory_path() / ("mygramdb_response_shapes_" + std::to_string(::getpid()));
    std::filesystem::create_directories(temp_dir_);

    BuildDataset();
    BuildConfig();
    StartServers();
  }

  void TearDown() override {
    if (limited_http_ && limited_http_->IsRunning()) {
      limited_http_->Stop();
    }
    if (limited_tcp_ && limited_tcp_->IsRunning()) {
      static_cast<void>(limited_tcp_->Stop());
    }
    if (http_server_ && http_server_->IsRunning()) {
      http_server_->Stop();
    }
    if (tcp_server_ && tcp_server_->IsRunning()) {
      static_cast<void>(tcp_server_->Stop());
    }
    std::error_code ignored;
    std::filesystem::remove_all(temp_dir_, ignored);
  }

  /// A deterministic dataset: stable primary keys, ASCII text chosen so that
  /// search, count, get, facet and highlight all return non-empty results, and
  /// facet values with distinct counts so their order cannot tie.
  void BuildDataset() {
    auto index = std::make_unique<index::Index>(2);
    auto doc_store = std::make_unique<storage::DocumentStore>();

    struct Seed {
      const char* primary_key;
      const char* category;
      const char* status;
      const char* text;
    };
    static constexpr std::array<Seed, 6> kSeeds = {{
        {"doc_1", "ai", "published", "machine learning models"},
        {"doc_2", "ai", "published", "machine production lines"},
        {"doc_3", "ai", "draft", "deep learning research"},
        {"doc_4", "industry", "published", "industrial machine safety"},
        {"doc_5", "industry", "draft", "unrelated topic"},
        {"doc_6", "misc", "published", "learning to write documentation"},
    }};

    for (const auto& seed : kSeeds) {
      storage::FilterMap filters;
      filters["category"] = std::string(seed.category);
      filters["status"] = std::string(seed.status);
      auto doc_id = doc_store->AddDocument(seed.primary_key, filters, seed.text);
      ASSERT_TRUE(doc_id);
      index->AddDocument(*doc_id, seed.text);
    }

    table_ctx_.name = "articles";
    table_ctx_.config.name = "articles";
    table_ctx_.config.database = "app";
    table_ctx_.config.ngram_size = 2;
    table_ctx_.config.primary_key = "id";
    table_ctx_.index = std::move(index);
    table_ctx_.doc_store = std::move(doc_store);
    table_contexts_["app.articles"] = &table_ctx_;
  }

  void BuildConfig() {
    config_ = std::make_unique<config::Config>();
    config_->api.tcp.bind = "127.0.0.1";
    config_->api.tcp.port = 0;
    config_->api.http.enable = true;
    config_->api.http.bind = "127.0.0.1";
    config_->api.http.port = 0;
    config_->api.default_limit = 10;
    config_->api.admin_token = kAdminToken;
    config_->network.allow_cidrs = {"127.0.0.1/32"};
    config_->tables.push_back(table_ctx_.config);
  }

  void StartServers() {
    ServerConfig tcp_cfg;
    tcp_cfg.host = "127.0.0.1";
    tcp_cfg.port = 0;
    tcp_cfg.allow_cidrs = {"127.0.0.1/32"};
    tcp_cfg.admin_token = kAdminToken;
    tcp_cfg.default_limit = config_->api.default_limit;
    tcp_server_ = std::make_unique<TcpServer>(tcp_cfg, table_contexts_, temp_dir_.string(), config_.get());
    ASSERT_TRUE(tcp_server_->Start());
    tcp_port_ = tcp_server_->GetPort();
    ASSERT_GT(tcp_port_, 0);

    // cpp-httplib's bind_to_port(…, 0) does not publish the chosen port back
    // through HttpServer::GetPort(), so an explicit free port is probed and
    // retried on the (rare) race with another process.
    for (int attempt = 0; attempt < 8 && !http_server_; ++attempt) {
      const uint16_t candidate = testing::FindAvailableLoopbackPort();
      if (candidate == 0) {
        continue;
      }
      HttpServerConfig http_cfg;
      http_cfg.bind = "127.0.0.1";
      http_cfg.port = candidate;
      http_cfg.allow_cidrs = {"127.0.0.1/32"};
      // Wired the way ServerLifecycleManager wires it in production: shared
      // statistics, the TCP server's query cache and its request-drain pool.
      // Without those the /info and /metrics surfaces would report an empty
      // HTTP-only view that no deployed server ever emits.
      auto candidate_server = std::make_unique<HttpServer>(
          http_cfg, table_contexts_, config_.get(), /*binlog_reader=*/nullptr, tcp_server_->GetCacheManager(),
          /*loading=*/nullptr, tcp_server_->GetMutableStats(), /*rate_limiter=*/nullptr,
          tcp_server_->GetReplicationPausedForDumpFlag(), /*sync_manager=*/nullptr,
          std::function<bool(const std::string&)>{}, std::function<bool()>{}, std::function<bool()>{},
          tcp_server_->GetThreadPool());
      candidate_server->SetOptimizeCallback(
          [this](const std::string& table) { return tcp_server_->HandleOptimizeRequest(table); });
      if (candidate_server->Start()) {
        http_server_ = std::move(candidate_server);
        http_port_ = candidate;
      }
    }
    ASSERT_TRUE(http_server_) << "could not bind an HTTP test port";
  }

  // -------------------------------------------------------------------------
  // Case recording
  // -------------------------------------------------------------------------

  void RecordTcp(TcpSession& session, const std::string& label, const std::string& request) {
    std::ostringstream block;
    block << "### " << NextIndex() << " TCP " << label << "\n";
    block << RenderFrame(request, "> ");
    block << RenderFrame(session.Send(request), "< ");
    blocks_.push_back(block.str());
  }

  void RecordHttp(const std::string& label, const std::string& method, const std::string& path,
                  const httplib::Headers& headers, const std::string& body, const std::string& content_type) {
    RecordHttpOn(http_port_, label, method, path, headers, body, content_type);
  }

  void RecordHttpOn(uint16_t port, const std::string& label, const std::string& method, const std::string& path,
                    const httplib::Headers& headers, const std::string& body, const std::string& content_type) {
    httplib::Client client("127.0.0.1", port);
    client.set_read_timeout(10, 0);
    httplib::Result result = method == "GET" ? client.Get(path.c_str(), headers)
                                             : client.Post(path.c_str(), headers, body, content_type.c_str());

    std::ostringstream block;
    block << "### " << NextIndex() << " HTTP " << label << "\n";
    block << "> " << method << " " << path << "\n";
    for (const auto& header : headers) {
      // Only the request headers that change routing/authorization decisions
      // are recorded; the client's transport headers are not part of the shape.
      block << "> " << header.first << ": " << header.second << "\n";
    }
    if (!body.empty()) {
      block << "> content-type: " << content_type << "\n";
      block << RenderFrame(body, "> ");
    }
    if (!result) {
      block << "< <NO_RESPONSE>\n";
      blocks_.push_back(block.str());
      return;
    }
    block << "< status: " << result->status << "\n";
    // The 404 fallback answers with no Content-Type at all; the value is
    // omitted rather than written as a trailing space so the golden survives
    // any whitespace-stripping tool.
    const std::string content_type_header = result->get_header_value("Content-Type");
    block << "< content-type:" << (content_type_header.empty() ? "" : " " + content_type_header) << "\n";
    if (result->has_header("WWW-Authenticate")) {
      block << "< www-authenticate: " << result->get_header_value("WWW-Authenticate") << "\n";
    }
    block << RenderBody(result->body, result->get_header_value("Content-Type"));
    blocks_.push_back(block.str());
  }

  std::string NextIndex() {
    char buf[8];
    std::snprintf(buf, sizeof(buf), "%03zu", ++case_index_);
    return buf;
  }

  /// Rate limiting is provoked on a dedicated pair of servers so the quota
  /// cannot reach any other case. A one-token bucket refilling at one token per
  /// second means the second request within the same second is always rejected,
  /// with no sleep and no wall-clock dependency.
  void RecordRateLimitCases() {
    config::Config limited_config = *config_;
    limited_config.api.rate_limiting.enable = true;
    limited_config.api.rate_limiting.capacity = 1;
    limited_config.api.rate_limiting.refill_rate = 1;
    limited_config.api.rate_limiting.max_clients = 16;
    limited_config_ = std::make_unique<config::Config>(std::move(limited_config));

    ServerConfig tcp_cfg;
    tcp_cfg.host = "127.0.0.1";
    tcp_cfg.port = 0;
    tcp_cfg.allow_cidrs = {"127.0.0.1/32"};
    tcp_cfg.admin_token = kAdminToken;
    limited_tcp_ = std::make_unique<TcpServer>(tcp_cfg, table_contexts_, temp_dir_.string(), limited_config_.get());
    ASSERT_TRUE(limited_tcp_->Start());

    for (int attempt = 0; attempt < 8 && !limited_http_; ++attempt) {
      const uint16_t candidate = testing::FindAvailableLoopbackPort();
      if (candidate == 0) {
        continue;
      }
      HttpServerConfig http_cfg;
      http_cfg.bind = "127.0.0.1";
      http_cfg.port = candidate;
      http_cfg.allow_cidrs = {"127.0.0.1/32"};
      auto candidate_server = std::make_unique<HttpServer>(http_cfg, table_contexts_, limited_config_.get(), nullptr);
      if (candidate_server->Start()) {
        limited_http_ = std::move(candidate_server);
        limited_http_port_ = candidate;
      }
    }
    ASSERT_TRUE(limited_http_) << "could not bind a rate-limited HTTP test port";

    TcpSession limited_session;
    ASSERT_TRUE(limited_session.Open(limited_tcp_->GetPort()));
    RecordTcp(limited_session, "rate limit first request allowed", "COUNT app.articles learning");
    RecordTcp(limited_session, "rate limit second request rejected", "COUNT app.articles learning");
    limited_session.Close();

    const httplib::Headers no_headers{};
    RecordHttpOn(limited_http_port_, "rate limit first request allowed", "POST", "/tables/app.articles/count",
                 no_headers, R"({"q":"learning"})", "application/json");
    RecordHttpOn(limited_http_port_, "rate limit second request rejected", "POST", "/tables/app.articles/count",
                 no_headers, R"({"q":"learning"})", "application/json");

    limited_http_->Stop();
    static_cast<void>(limited_tcp_->Stop());
  }

  /// Drain an asynchronous DUMP SAVE without recording the poll responses, so
  /// the number of polls never leaks into the golden.
  void WaitForDumpIdle(TcpSession& session) {
    for (int attempt = 0; attempt < 200; ++attempt) {
      const std::string status = session.Send("DUMP STATUS");
      if (status.find("save_in_progress: false") != std::string::npos &&
          status.find("status: SAVING") == std::string::npos) {
        return;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
  }

  TableContext table_ctx_;
  std::unordered_map<std::string, TableContext*> table_contexts_;
  std::unique_ptr<config::Config> config_;
  std::unique_ptr<TcpServer> tcp_server_;
  std::unique_ptr<HttpServer> http_server_;
  std::unique_ptr<config::Config> limited_config_;
  std::unique_ptr<TcpServer> limited_tcp_;
  std::unique_ptr<HttpServer> limited_http_;
  std::filesystem::path temp_dir_;
  uint16_t tcp_port_ = 0;
  uint16_t http_port_ = 0;
  uint16_t limited_http_port_ = 0;
  std::vector<std::string> blocks_;
  size_t case_index_ = 0;
};

namespace {

std::string Join(const std::vector<std::string>& blocks) {
  std::string joined;
  for (const auto& block : blocks) {
    joined += block;
  }
  return joined;
}

/// Split a rendered snapshot back into its `### NNN …` blocks so a mismatch can
/// be reported as one request/response pair instead of a whole-file diff.
std::vector<std::string> SplitBlocks(const std::string& text) {
  std::vector<std::string> blocks;
  std::istringstream stream(text);
  std::string line;
  std::string current;
  while (std::getline(stream, line)) {
    if (line.rfind("### ", 0) == 0 && !current.empty()) {
      blocks.push_back(current);
      current.clear();
    }
    current += line;
    current += '\n';
  }
  if (!current.empty()) {
    blocks.push_back(current);
  }
  return blocks;
}

const char* const kDiffGuidance =
    "The client-visible output of a protocol surface changed.\n"
    "A diff here is not a test failure to be silenced: it means a client that\n"
    "worked against the previous build receives different bytes now. Confirm\n"
    "the change is intended (and, if it is a wire-format change, that it is\n"
    "released as such) before regenerating with MYGRAMDB_UPDATE_SNAPSHOT=1.";

}  // namespace

// ---------------------------------------------------------------------------
// The request set
// ---------------------------------------------------------------------------
//
// Ordering rule: cases are grouped so that no case's side effects can reach a
// later case's response. Read-only cases run first on both surfaces; every
// state-mutating case runs last and, where it changes a setting, is paired
// with the command that restores the previous value (CACHE DISABLE ->
// CACHE ENABLE, DEBUG ON -> DEBUG OFF, SET writes back the value already in
// effect). Nothing is reset between cases; the order is the isolation
// mechanism.
TEST_F(ResponseShapeSnapshotTest, ResponseShapesMatchGolden) {
  // A normalization rule without a stated reason is an unexplained hole in the
  // snapshot, so the contract is enforced rather than merely documented.
  for (const auto& rule : NormalizationRules()) {
    ASSERT_NE(rule.why, nullptr);
    ASSERT_FALSE(std::string(rule.why).empty()) << "normalization pattern without a reason: " << rule.pattern;
  }

  const std::string dump_path = (temp_dir_ / "shapes.dump").string();
  const std::string legacy_dump_path = (temp_dir_ / "legacy.dump").string();

  TcpSession anon;
  ASSERT_TRUE(anon.Open(tcp_port_));
  TcpSession admin;
  ASSERT_TRUE(admin.Open(tcp_port_));

  // --- TCP: read-only query commands ---------------------------------------
  RecordTcp(anon, "SEARCH success", "SEARCH app.articles machine");
  RecordTcp(anon, "SEARCH pagination and sort", "SEARCH app.articles learning LIMIT 2 OFFSET 1 SORT id ASC");
  RecordTcp(anon, "SEARCH filter", "SEARCH app.articles machine FILTER category = ai");
  RecordTcp(anon, "SEARCH highlight", "SEARCH app.articles learning HIGHLIGHT TAG <b> </b> LIMIT 2");
  RecordTcp(anon, "SEARCH fuzzy", "SEARCH app.articles machime FUZZY 1 LIMIT 2");
  RecordTcp(anon, "SEARCH boolean", "SEARCH app.articles machine AND learning");
  RecordTcp(anon, "SEARCH no match", "SEARCH app.articles zzzznotpresent");
  RecordTcp(anon, "SEARCH bad arity", "SEARCH");
  RecordTcp(anon, "SEARCH missing search text", "SEARCH app.articles");
  RecordTcp(anon, "SEARCH unknown table", "SEARCH app.missing machine");
  RecordTcp(anon, "SEARCH limit out of range", "SEARCH app.articles machine LIMIT 0");
  RecordTcp(anon, "SEARCH malformed limit", "SEARCH app.articles machine LIMIT notanumber");
  RecordTcp(anon, "SEARCH unknown filter column", "SEARCH app.articles machine FILTER nosuchcolumn = 1");

  RecordTcp(anon, "COUNT success", "COUNT app.articles learning");
  RecordTcp(anon, "COUNT bad arity", "COUNT");
  RecordTcp(anon, "COUNT unknown table", "COUNT app.missing learning");

  RecordTcp(anon, "GET success", "GET app.articles doc_1");
  RecordTcp(anon, "GET missing document", "GET app.articles doc_999");
  RecordTcp(anon, "GET bad arity", "GET app.articles");
  RecordTcp(anon, "GET unknown table", "GET app.missing doc_1");

  RecordTcp(anon, "FACET success", "FACET app.articles category");
  RecordTcp(anon, "FACET with search text", "FACET app.articles category learning");
  RecordTcp(anon, "FACET pagination", "FACET app.articles category LIMIT 1 OFFSET 1");
  RecordTcp(anon, "FACET bad arity", "FACET app.articles");
  RecordTcp(anon, "FACET unknown column", "FACET app.articles nosuchcolumn");
  RecordTcp(anon, "FACET unknown table", "FACET app.missing category");

  RecordTcp(anon, "INFO success", "INFO");
  RecordTcp(anon, "unknown command", "NOSUCHCOMMAND app.articles");
  RecordTcp(anon, "empty request", "");

  // --- TCP: authentication --------------------------------------------------
  RecordTcp(anon, "admin command without AUTH", "CACHE STATS");
  RecordTcp(anon, "AUTH bad arity", "AUTH");
  RecordTcp(anon, "AUTH wrong token", "AUTH not-the-token");
  RecordTcp(admin, "AUTH success", std::string("AUTH ") + kAdminToken);

  // --- TCP: read-only administrative commands ------------------------------
  RecordTcp(admin, "CONFIG SHOW success", "CONFIG SHOW api.default_limit");
  RecordTcp(admin, "CONFIG SHOW unknown path", "CONFIG SHOW no.such.path");
  RecordTcp(admin, "CONFIG HELP success", "CONFIG HELP api.default_limit");
  RecordTcp(admin, "CONFIG HELP unknown path", "CONFIG HELP no.such.path");
  RecordTcp(admin, "CONFIG VERIFY missing file", "CONFIG VERIFY /nonexistent/mygramdb.yaml");
  RecordTcp(admin, "CONFIG VERIFY bad arity", "CONFIG VERIFY");

  RecordTcp(admin, "CONFIG SHOW subtree", "CONFIG SHOW api.tcp");
  RecordTcp(admin, "SHOW VARIABLES filtered", "SHOW VARIABLES LIKE 'api.default_limit'");
  RecordTcp(admin, "SHOW VARIABLES group", "SHOW VARIABLES LIKE 'dump.%'");
  RecordTcp(admin, "SHOW VARIABLES no match", "SHOW VARIABLES LIKE 'no.such.variable'");

  RecordTcp(admin, "CACHE STATS success", "CACHE STATS");
  RecordTcp(admin, "CACHE unknown subcommand", "CACHE NOSUCHSUBCOMMAND");

  RecordTcp(admin, "REPLICATION STATUS success", "REPLICATION STATUS");
  RecordTcp(admin, "REPLICATION unknown subcommand", "REPLICATION NOSUCHSUBCOMMAND");
  RecordTcp(admin, "SYNC STATUS success", "SYNC STATUS");
  RecordTcp(admin, "SYNC unknown table", "SYNC app.missing");
  RecordTcp(admin, "DUMP STATUS idle", "DUMP STATUS");
  RecordTcp(admin, "DUMP INFO missing file", "DUMP INFO no-such-file.dump");
  RecordTcp(admin, "DUMP VERIFY bad arity", "DUMP VERIFY");
  RecordTcp(admin, "DUMP LOAD missing file", "DUMP LOAD no-such-file.dump");
  RecordTcp(admin, "DUMP unknown subcommand", "DUMP NOSUCHSUBCOMMAND");

  // --- HTTP: read-only routes ----------------------------------------------
  const httplib::Headers kNoHeaders{};
  const httplib::Headers kAdminHeaders{{"Authorization", std::string("Bearer ") + kAdminToken}};
  const std::string kJson = "application/json";

  RecordHttp("search success", "POST", "/tables/app.articles/search", kNoHeaders, R"({"q":"machine"})", kJson);
  RecordHttp("search pagination and sort", "POST", "/tables/app.articles/search", kNoHeaders,
             R"({"q":"learning","limit":2,"offset":1,"sort":{"column":"id","order":"asc"}})", kJson);
  RecordHttp("search filter", "POST", "/tables/app.articles/search", kNoHeaders,
             R"({"q":"machine","filters":{"category":"ai"}})", kJson);
  RecordHttp("search highlight", "POST", "/tables/app.articles/search", kNoHeaders,
             R"({"q":"learning","limit":2,"highlight":{"open_tag":"<b>","close_tag":"</b>"}})", kJson);
  RecordHttp("search fuzzy", "POST", "/tables/app.articles/search", kNoHeaders,
             R"({"q":"machime","fuzzy":1,"limit":2})", kJson);
  RecordHttp("search boolean mode", "POST", "/tables/app.articles/search", kNoHeaders,
             R"({"q":"machine AND learning","mode":"boolean"})", kJson);
  RecordHttp("search no match", "POST", "/tables/app.articles/search", kNoHeaders, R"({"q":"zzzznotpresent"})", kJson);
  RecordHttp("search unknown table", "POST", "/tables/app.missing/search", kNoHeaders, R"({"q":"machine"})", kJson);
  RecordHttp("search missing q", "POST", "/tables/app.articles/search", kNoHeaders, "{}", kJson);
  RecordHttp("search limit out of range", "POST", "/tables/app.articles/search", kNoHeaders,
             R"({"q":"machine","limit":0})", kJson);
  RecordHttp("search malformed json", "POST", "/tables/app.articles/search", kNoHeaders, "{not json", kJson);
  RecordHttp("search wrong content type", "POST", "/tables/app.articles/search", kNoHeaders, R"({"q":"machine"})",
             "text/plain");
  RecordHttp("search unknown filter column", "POST", "/tables/app.articles/search", kNoHeaders,
             R"({"q":"machine","filters":{"nosuchcolumn":"1"}})", kJson);

  RecordHttp("count success", "POST", "/tables/app.articles/count", kNoHeaders, R"({"q":"learning"})", kJson);
  RecordHttp("count unknown table", "POST", "/tables/app.missing/count", kNoHeaders, R"({"q":"learning"})", kJson);
  RecordHttp("count rejects pagination", "POST", "/tables/app.articles/count", kNoHeaders,
             R"({"q":"learning","limit":2})", kJson);
  RecordHttp("count malformed json", "POST", "/tables/app.articles/count", kNoHeaders, "{not json", kJson);

  RecordHttp("facet success", "POST", "/tables/app.articles/facet", kNoHeaders, R"({"column":"category"})", kJson);
  RecordHttp("facet with search text", "POST", "/tables/app.articles/facet", kNoHeaders,
             R"({"column":"category","q":"learning"})", kJson);
  RecordHttp("facet pagination", "POST", "/tables/app.articles/facet", kNoHeaders,
             R"({"column":"category","limit":1,"offset":1})", kJson);
  RecordHttp("facet missing column", "POST", "/tables/app.articles/facet", kNoHeaders, "{}", kJson);
  RecordHttp("facet unknown column", "POST", "/tables/app.articles/facet", kNoHeaders, R"({"column":"nosuchcolumn"})",
             kJson);
  RecordHttp("facet unknown table", "POST", "/tables/app.missing/facet", kNoHeaders, R"({"column":"category"})", kJson);

  RecordHttp("get success", "GET", "/tables/app.articles/doc_1", kNoHeaders, "", "");
  RecordHttp("get missing document", "GET", "/tables/app.articles/doc_999", kNoHeaders, "", "");
  RecordHttp("get unknown table", "GET", "/tables/app.missing/doc_1", kNoHeaders, "", "");

  RecordHttp("info", "GET", "/info", kNoHeaders, "", "");
  RecordHttp("health", "GET", "/health", kNoHeaders, "", "");
  RecordHttp("health live", "GET", "/health/live", kNoHeaders, "", "");
  RecordHttp("health ready", "GET", "/health/ready", kNoHeaders, "", "");
  RecordHttp("health detail", "GET", "/health/detail", kNoHeaders, "", "");
  RecordHttp("config", "GET", "/config", kNoHeaders, "", "");
  RecordHttp("replication status", "GET", "/replication/status", kNoHeaders, "", "");
  RecordHttp("metrics", "GET", "/metrics", kNoHeaders, "", "");
  RecordHttp("unrouted path", "GET", "/no-such-route", kNoHeaders, "", "");

  // --- HTTP: administrative route ------------------------------------------
  RecordHttp("optimize without credentials", "POST", "/optimize", kNoHeaders, "{}", kJson);
  RecordHttp("optimize with wrong credentials", "POST", "/optimize", {{"Authorization", "Bearer wrong-token"}}, "{}",
             kJson);
  RecordHttp("optimize invalid table type", "POST", "/optimize", kAdminHeaders, R"({"table":42})", kJson);
  RecordHttp("optimize unknown table", "POST", "/optimize", kAdminHeaders, R"({"table":"app.missing"})", kJson);
  RecordHttp("optimize success", "POST", "/optimize", kAdminHeaders, "{}", kJson);

  // --- TCP: state-mutating commands ----------------------------------------
  // DEBUG mode is per-connection, so the DEBUG block shape is captured between
  // DEBUG ON and DEBUG OFF on the administrative session only.
  RecordTcp(admin, "DEBUG ON", "DEBUG ON");
  RecordTcp(admin, "SEARCH with debug block", "SEARCH app.articles machine LIMIT 1");
  RecordTcp(admin, "DEBUG OFF", "DEBUG OFF");

  // SET writes back the value already in effect, so later cases see the same
  // runtime configuration as the cases before it.
  RecordTcp(admin, "SET success", "SET api.default_limit = 10");
  RecordTcp(admin, "SET unknown variable", "SET no.such.variable = 1");
  RecordTcp(admin, "SET bad arity", "SET");

  // CACHE DISABLE is immediately followed by CACHE ENABLE, restoring the
  // configured state before any later case runs.
  RecordTcp(admin, "CACHE DISABLE", "CACHE DISABLE");
  RecordTcp(admin, "CACHE ENABLE", "CACHE ENABLE");
  RecordTcp(admin, "CACHE CLEAR all tables", "CACHE CLEAR");
  RecordTcp(admin, "CACHE CLEAR unknown table", "CACHE CLEAR app.missing");

  RecordTcp(admin, "OPTIMIZE all tables", "OPTIMIZE");
  RecordTcp(admin, "OPTIMIZE unknown table", "OPTIMIZE app.missing");

  RecordTcp(admin, "REPLICATION STOP", "REPLICATION STOP");
  RecordTcp(admin, "REPLICATION START", "REPLICATION START");
  RecordTcp(admin, "SYNC STOP", "SYNC STOP");

  // The dump group runs last: DUMP SAVE writes into the per-run temporary
  // directory and DUMP LOAD replaces the live index from that same file.
  RecordTcp(admin, "DUMP SAVE", "DUMP SAVE " + dump_path);
  WaitForDumpIdle(admin);
  RecordTcp(admin, "DUMP STATUS after save", "DUMP STATUS");
  RecordTcp(admin, "DUMP INFO success", "DUMP INFO " + dump_path);
  RecordTcp(admin, "DUMP VERIFY success", "DUMP VERIFY " + dump_path);
  RecordTcp(admin, "DUMP LOAD success", "DUMP LOAD " + dump_path);
  RecordTcp(admin, "SAVE legacy", "SAVE " + legacy_dump_path);
  WaitForDumpIdle(admin);
  RecordTcp(admin, "LOAD legacy", "LOAD " + legacy_dump_path);

  anon.Close();
  admin.Close();

  RecordRateLimitCases();

  // --- Compare --------------------------------------------------------------
  const std::string actual = Normalize(Join(blocks_));
  const std::filesystem::path golden_path{MYGRAMDB_RESPONSE_SHAPE_GOLDEN};

  const char* update_env = std::getenv("MYGRAMDB_UPDATE_SNAPSHOT");
  if (update_env != nullptr && std::string(update_env) == "1") {
    std::filesystem::create_directories(golden_path.parent_path());
    std::ofstream out(golden_path, std::ios::binary | std::ios::trunc);
    ASSERT_TRUE(out) << "cannot write " << golden_path;
    out << actual;
    out.close();
    GTEST_SKIP() << "Snapshot updated: " << golden_path << " (" << blocks_.size()
                 << " cases). Review the diff before committing.";
  }

  std::ifstream in(golden_path, std::ios::binary);
  ASSERT_TRUE(in) << "Golden file missing: " << golden_path << "\nGenerate it with MYGRAMDB_UPDATE_SNAPSHOT=1.";
  const std::string expected((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());

  if (expected == actual) {
    SUCCEED();
    return;
  }

  const auto expected_blocks = SplitBlocks(expected);
  const auto actual_blocks = SplitBlocks(actual);
  const size_t common = std::min(expected_blocks.size(), actual_blocks.size());
  for (size_t i = 0; i < common; ++i) {
    if (expected_blocks[i] != actual_blocks[i]) {
      FAIL() << kDiffGuidance << "\n\nFirst differing case (" << (i + 1) << " of " << actual_blocks.size()
             << "), golden " << golden_path << "\n\n--- expected ---\n"
             << expected_blocks[i] << "\n--- actual ---\n"
             << actual_blocks[i];
    }
  }
  FAIL() << kDiffGuidance << "\n\nThe request set changed size: golden has " << expected_blocks.size()
         << " cases, this run produced " << actual_blocks.size() << " (golden " << golden_path << ").";
}

}  // namespace server
}  // namespace mygramdb
