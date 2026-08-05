/**
 * @file response_formatter_test.cpp
 * @brief Unit tests for ResponseFormatter
 */

#include "server/response_formatter.h"

#include <gtest/gtest.h>

#include <future>

#include "cache/cache_manager.h"
#include "cache/cache_types.h"
#include "client/protocol_detection.h"
#include "config/config.h"
#include "index/index.h"
#include "mysql/binlog_reader_interface.h"
#include "server/server_stats.h"
#include "server/statistics_service.h"
#include "server/tcp_server.h"  // For TableContext
#include "server/thread_pool.h"
#include "storage/document_store.h"

using namespace mygramdb::server;
using namespace mygramdb;

#ifdef USE_MYSQL
class MockResponseBinlogReader final : public mysql::IBinlogReader {
 public:
  mygram::utils::Expected<void, mygram::utils::Error> Start() override {
    running = true;
    return {};
  }
  void Stop() override { running = false; }
  bool IsRunning() const override { return running; }
  std::string GetCurrentGTID() const override { return current_gtid; }
  void SetCurrentGTID(const std::string& gtid) override { current_gtid = gtid; }
  std::string GetLastError() const override { return last_error; }
  uint64_t GetProcessedEvents() const override { return processed_events; }
  size_t GetQueueSize() const override { return queue_size; }
  uint64_t GetCRCErrors() const override { return crc_errors; }
  bool HasSchemaIncompatibleError() const override { return schema_incompatible; }
  mygram::utils::ErrorCode GetLastErrorCode() const override { return last_error_code; }
  int64_t GetLastAppliedUnixTime() const override { return last_applied_unixtime; }
  int64_t GetSecondsSinceLastApplied() const override { return seconds_since_last_applied; }

  bool running = false;
  std::string current_gtid = "uuid:1-10";
  std::string last_error;
  uint64_t processed_events = 42;
  size_t queue_size = 9;
  uint64_t crc_errors = 0;
  bool schema_incompatible = false;
  mygram::utils::ErrorCode last_error_code = mygram::utils::ErrorCode::kSuccess;
  int64_t last_applied_unixtime = 0;
  int64_t seconds_since_last_applied = -1;
};
#endif

class ResponseFormatterTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create table context
    auto index = std::make_unique<index::Index>(1);
    auto doc_store = std::make_unique<storage::DocumentStore>();

    table_context_.name = "test";
    table_context_.config.ngram_size = 1;
    table_context_.index = std::move(index);
    table_context_.doc_store = std::move(doc_store);

    table_contexts_["test"] = &table_context_;
  }

  TableContext table_context_;
  std::unordered_map<std::string, TableContext*> table_contexts_;
};

/**
 * @brief Test INFO response without cache manager (cache disabled)
 */
TEST_F(ResponseFormatterTest, FormatInfoResponseNoCacheManager) {
  ServerStats stats;

  // Aggregate metrics
  auto metrics = StatisticsService::AggregateMetrics(table_contexts_);

  std::string response = ResponseFormatter::FormatInfoResponse(metrics, stats, table_contexts_, nullptr, nullptr);

  // Should contain cache section with disabled status
  EXPECT_TRUE(response.find("# Cache") != std::string::npos);
  EXPECT_TRUE(response.find("cache_enabled: 0") != std::string::npos);

  // Should not contain cache statistics when disabled
  EXPECT_TRUE(response.find("cache_hits:") == std::string::npos);
  EXPECT_TRUE(response.find("cache_misses:") == std::string::npos);
}

/**
 * @brief Test INFO response with cache manager enabled
 */
TEST_F(ResponseFormatterTest, FormatInfoResponseWithCacheManager) {
  ServerStats stats;

  // Create cache manager
  config::CacheConfig cache_config;
  cache_config.enabled = true;
  cache_config.max_memory_bytes = 100 * 1024 * 1024;
  cache_config.min_query_cost_ms = 1.0;

  cache::NgramConfigMap ngram_configs;
  for (const auto& [name, ctx] : table_contexts_) {
    ngram_configs[name] =
        cache::NgramConfig{ctx->config.ngram_size, ctx->config.kanji_ngram_size, ctx->config.cross_boundary_ngrams};
  }
  cache::CacheManager cache_manager(cache_config, std::move(ngram_configs));

  // Aggregate metrics
  auto metrics = StatisticsService::AggregateMetrics(table_contexts_);

  std::string response =
      ResponseFormatter::FormatInfoResponse(metrics, stats, table_contexts_, nullptr, &cache_manager);

  // Should contain cache section with enabled status
  EXPECT_TRUE(response.find("# Cache") != std::string::npos);
  EXPECT_TRUE(response.find("cache_enabled: 1") != std::string::npos);

  // Should contain all cache statistics fields
  EXPECT_TRUE(response.find("cache_hits: ") != std::string::npos);
  EXPECT_TRUE(response.find("cache_misses: ") != std::string::npos);
  EXPECT_TRUE(response.find("cache_misses_not_found: ") != std::string::npos);
  EXPECT_TRUE(response.find("cache_misses_invalidated: ") != std::string::npos);
  EXPECT_TRUE(response.find("cache_total_queries: ") != std::string::npos);
  EXPECT_TRUE(response.find("cache_hit_rate: ") != std::string::npos);
  EXPECT_TRUE(response.find("cache_current_entries: ") != std::string::npos);
  EXPECT_TRUE(response.find("cache_memory_bytes: ") != std::string::npos);
  EXPECT_TRUE(response.find("cache_memory_human: ") != std::string::npos);
  EXPECT_TRUE(response.find("cache_accounted_memory_bytes: ") != std::string::npos);
  EXPECT_TRUE(response.find("cache_rejection_oversize: ") != std::string::npos);
  EXPECT_TRUE(response.find("cache_rejection_duplicate: ") != std::string::npos);
  EXPECT_TRUE(response.find("cache_decompression_failures: ") != std::string::npos);
  EXPECT_TRUE(response.find("cache_stale_lru_entries: ") != std::string::npos);
  EXPECT_TRUE(response.find("cache_evictions: ") != std::string::npos);
  EXPECT_TRUE(response.find("cache_invalidations_immediate: ") != std::string::npos);
  EXPECT_TRUE(response.find("cache_invalidations_deferred: ") != std::string::npos);
  EXPECT_TRUE(response.find("cache_invalidations_batches: ") != std::string::npos);
  EXPECT_TRUE(response.find("cache_avg_hit_latency_ms: ") != std::string::npos);
  EXPECT_TRUE(response.find("cache_avg_miss_latency_ms: ") != std::string::npos);
  EXPECT_TRUE(response.find("cache_total_time_saved_ms: ") != std::string::npos);
}

/**
 * @brief Test INFO response with cache manager but disabled
 */
TEST_F(ResponseFormatterTest, FormatInfoResponseWithCacheManagerDisabled) {
  ServerStats stats;

  // Create cache manager but disabled
  config::CacheConfig cache_config;
  cache_config.enabled = false;
  cache_config.max_memory_bytes = 100 * 1024 * 1024;

  cache::NgramConfigMap ngram_configs;
  for (const auto& [name, ctx] : table_contexts_) {
    ngram_configs[name] =
        cache::NgramConfig{ctx->config.ngram_size, ctx->config.kanji_ngram_size, ctx->config.cross_boundary_ngrams};
  }
  cache::CacheManager cache_manager(cache_config, std::move(ngram_configs));
  cache_manager.Disable();

  // Aggregate metrics
  auto metrics = StatisticsService::AggregateMetrics(table_contexts_);

  std::string response =
      ResponseFormatter::FormatInfoResponse(metrics, stats, table_contexts_, nullptr, &cache_manager);

  // Should contain cache section with disabled status
  EXPECT_TRUE(response.find("# Cache") != std::string::npos);
  EXPECT_TRUE(response.find("cache_enabled: 0") != std::string::npos);

  // Should not contain detailed statistics when disabled
  EXPECT_TRUE(response.find("cache_hits:") == std::string::npos);
}

/**
 * @brief Test SEARCH response with empty results
 */
TEST_F(ResponseFormatterTest, FormatSearchResponseEmpty) {
  std::vector<index::DocId> results;
  size_t total_results = 0;

  std::string response =
      ResponseFormatter::FormatSearchResponse(results, total_results, table_context_.doc_store.get());

  EXPECT_TRUE(response.find("OK RESULTS") != std::string::npos);
  EXPECT_TRUE(response.find(" 0") != std::string::npos || response.find("0") != std::string::npos);
}

/**
 * @brief Test SEARCH response with results
 */
TEST_F(ResponseFormatterTest, FormatSearchResponseWithResults) {
  // Add documents to store
  auto doc_id1 = table_context_.doc_store->AddDocument("pk1");
  auto doc_id2 = table_context_.doc_store->AddDocument("pk2");
  auto doc_id3 = table_context_.doc_store->AddDocument("pk3");

  std::vector<index::DocId> results = {*doc_id1, *doc_id2, *doc_id3};
  size_t total_results = 3;

  std::string response =
      ResponseFormatter::FormatSearchResponse(results, total_results, table_context_.doc_store.get());

  EXPECT_TRUE(response.find("OK RESULTS") != std::string::npos);
  EXPECT_TRUE(response.find(" 3") != std::string::npos);
  EXPECT_TRUE(response.find("pk1") != std::string::npos);
  EXPECT_TRUE(response.find("pk2") != std::string::npos);
  EXPECT_TRUE(response.find("pk3") != std::string::npos);
}

TEST_F(ResponseFormatterTest, FormatSearchResponseEscapesPrimaryKeyDelimitersReversibly) {
  auto doc_id = table_context_.doc_store->AddDocument("pk with\r\nnewline\tand tab");

  std::vector<index::DocId> results = {*doc_id};

  std::string response = ResponseFormatter::FormatSearchResponse(results, 1, table_context_.doc_store.get());

  EXPECT_EQ(response, "OK RESULTS 1 \"pk with\\r\\nnewline\\tand tab\"");
}

TEST_F(ResponseFormatterTest, FormatSearchResponseDoesNotCollapseSpaceAndUnderscorePrimaryKeys) {
  auto spaced_doc_id = table_context_.doc_store->AddDocument("a b");
  auto underscored_doc_id = table_context_.doc_store->AddDocument("a_b");

  std::vector<index::DocId> results = {*spaced_doc_id, *underscored_doc_id};

  std::string response = ResponseFormatter::FormatSearchResponse(results, 2, table_context_.doc_store.get());

  EXPECT_EQ(response, "OK RESULTS 2 \"a b\" a_b");
}

TEST_F(ResponseFormatterTest, FormatSearchResponsePreservesEmptyPrimaryKey) {
  auto doc_id = table_context_.doc_store->AddDocument("");
  ASSERT_TRUE(doc_id.has_value());

  std::string response = ResponseFormatter::FormatSearchResponse({*doc_id}, 1, table_context_.doc_store.get());

  EXPECT_EQ(response, "OK RESULTS 1 \"\"");
}

TEST_F(ResponseFormatterTest, FormatSearchResponseQuotesUnicodeWhitespaceInPrimaryKey) {
  auto doc_id = table_context_.doc_store->AddDocument("a　b");

  std::string response = ResponseFormatter::FormatSearchResponse({*doc_id}, 1, table_context_.doc_store.get());

  EXPECT_EQ(response, "OK RESULTS 1 \"a　b\"");
}

TEST_F(ResponseFormatterTest, FormatSearchResponseWithHighlightsTerminatesMultilineResponse) {
  auto doc_id1 = table_context_.doc_store->AddDocument("pk1");

  std::vector<index::DocId> results = {*doc_id1};
  std::vector<std::string> snippets = {"hello <em>world</em>"};

  std::string response =
      ResponseFormatter::FormatSearchResponseWithHighlights(results, 1, table_context_.doc_store.get(), snippets);

  EXPECT_EQ(response, "OK RESULTS 1\r\npk1\thello <em>world</em>\r\n");
}

TEST_F(ResponseFormatterTest, FormatSearchResponseWithHighlightsSanitizesLineDelimiters) {
  auto doc_id1 = table_context_.doc_store->AddDocument("pk1");

  std::vector<index::DocId> results = {*doc_id1};
  std::vector<std::string> snippets = {"line1\r\nline2\tline3"};

  std::string response =
      ResponseFormatter::FormatSearchResponseWithHighlights(results, 1, table_context_.doc_store.get(), snippets);

  EXPECT_EQ(response, "OK RESULTS 1\r\npk1\tline1  line2 line3\r\n");
}

TEST_F(ResponseFormatterTest, FormatSearchResponseWithHighlightsEscapesPrimaryKeyReversibly) {
  auto doc_id = table_context_.doc_store->AddDocument("a b\tc");

  std::string response =
      ResponseFormatter::FormatSearchResponseWithHighlights({*doc_id}, 1, table_context_.doc_store.get(), {"snippet"});

  EXPECT_EQ(response, "OK RESULTS 1\r\n\"a b\\tc\"\tsnippet\r\n");
}

TEST_F(ResponseFormatterTest, FormatFacetResponseSanitizesLineDelimiters) {
  std::vector<std::pair<std::string, uint64_t>> value_counts = {{"value\r\nnext\tpart", 3}};

  std::string response = ResponseFormatter::FormatFacetResponse(value_counts);

  EXPECT_EQ(response, "OK FACET 1\r\nvalue  next part\t3\r\n\r\n");
}

TEST_F(ResponseFormatterTest, FormatFacetResponseTerminatesTransportFrame) {
  std::vector<std::pair<std::string, uint64_t>> value_counts = {{"alpha", 2}, {"beta", 1}};

  std::string response = ResponseFormatter::FormatFacetResponse(value_counts);

  EXPECT_TRUE(client::detail::IsResponseComplete(response)) << response;
}

/**
 * @brief Test SEARCH response with pagination (total > returned)
 */
TEST_F(ResponseFormatterTest, FormatSearchResponseWithPagination) {
  // Add documents
  auto doc_id1 = table_context_.doc_store->AddDocument("pk1");
  auto doc_id2 = table_context_.doc_store->AddDocument("pk2");

  // Simulate pagination: 2 results returned, but 100 total
  std::vector<index::DocId> results = {*doc_id1, *doc_id2};
  size_t total_results = 100;

  std::string response =
      ResponseFormatter::FormatSearchResponse(results, total_results, table_context_.doc_store.get());

  EXPECT_TRUE(response.find("OK RESULTS") != std::string::npos);
  EXPECT_TRUE(response.find(" 100") != std::string::npos);
  EXPECT_TRUE(response.find("pk1") != std::string::npos);
  EXPECT_TRUE(response.find("pk2") != std::string::npos);
}

/**
 * @brief Test SEARCH response with debug info
 */
TEST_F(ResponseFormatterTest, FormatSearchResponseWithDebugInfo) {
  auto doc_id1 = table_context_.doc_store->AddDocument("pk1");

  std::vector<index::DocId> results = {*doc_id1};
  size_t total_results = 1;

  query::DebugInfo debug_info;
  debug_info.query_time_ms = 1.234;
  debug_info.index_time_ms = 0.5;
  debug_info.filter_time_ms = 0.2;

  std::string response =
      ResponseFormatter::FormatSearchResponse(results, total_results, table_context_.doc_store.get(), &debug_info);

  EXPECT_TRUE(response.find("OK RESULTS") != std::string::npos);
  EXPECT_TRUE(response.find("DEBUG") != std::string::npos || response.find("query_time") != std::string::npos);
}

/**
 * @brief Test COUNT response
 */
TEST_F(ResponseFormatterTest, FormatCountResponse) {
  std::string response = ResponseFormatter::FormatCountResponse(42);

  EXPECT_TRUE(response.find("OK COUNT") != std::string::npos);
  EXPECT_TRUE(response.find("42") != std::string::npos);
}

/**
 * @brief Test COUNT response with zero
 */
TEST_F(ResponseFormatterTest, FormatCountResponseZero) {
  std::string response = ResponseFormatter::FormatCountResponse(0);

  EXPECT_TRUE(response.find("OK COUNT") != std::string::npos);
  EXPECT_TRUE(response.find("0") != std::string::npos);
}

/**
 * @brief Test COUNT response with debug info
 */
TEST_F(ResponseFormatterTest, FormatCountResponseWithDebugInfo) {
  query::DebugInfo debug_info;
  debug_info.query_time_ms = 0.5;

  std::string response = ResponseFormatter::FormatCountResponse(100, &debug_info);

  EXPECT_TRUE(response.find("OK COUNT") != std::string::npos);
  EXPECT_TRUE(response.find("100") != std::string::npos);
  EXPECT_TRUE(response.find("DEBUG") != std::string::npos || response.find("query_time_ms") != std::string::npos);
}

namespace {

// Extract the cache-debug section (lines starting with "cache" or "cache_*")
// out of a SEARCH or COUNT debug response so two protocols can be diffed
// directly. The section ends at the first non-cache line we encounter, which
// is sufficient because the other debug fields are emitted in different
// orders by the two formatters.
std::string ExtractCacheLines(const std::string& response) {
  std::string out;
  size_t pos = 0;
  while (pos < response.size()) {
    size_t end = response.find("\r\n", pos);
    if (end == std::string::npos) {
      end = response.size();
    }
    auto line = response.substr(pos, end - pos);
    if (line.rfind("cache", 0) == 0) {
      out += line;
      out += "\n";
    }
    if (end == response.size()) {
      break;
    }
    pos = end + 2;
  }
  return out;
}

query::DebugInfo MakeCacheDebugInfo(query::CacheDebugInfo::Status status) {
  query::DebugInfo debug_info;
  debug_info.cache_info.status = status;
  debug_info.cache_info.cache_age_ms = 1.5;
  debug_info.cache_info.cache_saved_ms = 2.5;
  debug_info.cache_info.query_cost_ms = 3.5;
  return debug_info;
}

}  // namespace

/**
 * @brief Regression: SEARCH and COUNT must emit the same cache-debug lines
 *        for any given cache state.
 *
 * Pre-unification, FormatSearchResponse used "cache: miss\r\ncache_reason:
 * not_found\r\ncache_cost_ms: ..." while FormatCountResponse used
 * "cache: miss (not found)\r\nquery_cost_ms: ...". Both responses now flow
 * through the shared WriteCacheDebugLines helper, so the cache section
 * extracted from the two responses must compare equal.
 */
TEST_F(ResponseFormatterTest, CacheDebugLinesAreConsistentBetweenSearchAndCount) {
  using Status = query::CacheDebugInfo::Status;
  for (auto status : {Status::HIT, Status::MISS_NOT_FOUND, Status::MISS_INVALIDATED, Status::MISS_DISABLED}) {
    auto debug = MakeCacheDebugInfo(status);

    std::vector<index::DocId> empty_results;
    std::string search_resp =
        ResponseFormatter::FormatSearchResponse(empty_results, 0, table_context_.doc_store.get(), &debug);
    std::string count_resp = ResponseFormatter::FormatCountResponse(0, &debug);

    auto search_cache = ExtractCacheLines(search_resp);
    auto count_cache = ExtractCacheLines(count_resp);

    EXPECT_EQ(search_cache, count_cache) << "SEARCH and COUNT cache sections diverge for status="
                                         << static_cast<int>(status) << "\n"
                                         << "SEARCH:\n"
                                         << search_cache << "COUNT:\n"
                                         << count_cache;
  }
}

/**
 * @brief Test SAVE response
 */
TEST_F(ResponseFormatterTest, FormatSaveResponse) {
  std::string response = ResponseFormatter::FormatSaveResponse("/path/to/snapshot.dump");

  EXPECT_TRUE(response.find("OK SAVE") != std::string::npos || response.find("OK") != std::string::npos);
  EXPECT_TRUE(response.find("/path/to/snapshot.dump") != std::string::npos);
}

/**
 * @brief Test LOAD response
 */
TEST_F(ResponseFormatterTest, FormatLoadResponse) {
  std::string response = ResponseFormatter::FormatLoadResponse("/path/to/snapshot.dump");

  EXPECT_TRUE(response.find("OK LOAD") != std::string::npos || response.find("OK") != std::string::npos);
  EXPECT_TRUE(response.find("/path/to/snapshot.dump") != std::string::npos);
}

/**
 * @brief Test REPLICATION STOP response
 */
TEST_F(ResponseFormatterTest, FormatReplicationStopResponse) {
  std::string response = ResponseFormatter::FormatReplicationStopResponse();

  EXPECT_TRUE(response.find("OK") != std::string::npos);
  EXPECT_TRUE(response.find("REPLICATION") != std::string::npos || response.find("STOP") != std::string::npos ||
              response.find("stopped") != std::string::npos);
}

/**
 * @brief Test REPLICATION START response
 */
TEST_F(ResponseFormatterTest, FormatReplicationStartResponse) {
  std::string response = ResponseFormatter::FormatReplicationStartResponse();

  EXPECT_TRUE(response.find("OK") != std::string::npos);
  EXPECT_TRUE(response.find("REPLICATION") != std::string::npos || response.find("START") != std::string::npos ||
              response.find("started") != std::string::npos);
}

#ifdef USE_MYSQL
TEST_F(ResponseFormatterTest, FormatReplicationStatusIncludesQueueSizeWhenStopped) {
  MockResponseBinlogReader reader;
  reader.running = false;
  reader.current_gtid = "uuid:1-100";
  reader.processed_events = 321;
  reader.queue_size = 11;
  reader.crc_errors = 4;
  reader.schema_incompatible = true;
  reader.last_error_code = mygram::utils::ErrorCode::kMySQLInvalidSchema;
  reader.last_error = "schema changed";
  reader.last_applied_unixtime = 1722840000;
  reader.seconds_since_last_applied = 42;

  std::string response = ResponseFormatter::FormatReplicationStatusResponse(&reader);

  EXPECT_NE(response.find("status: failed\r\n"), std::string::npos);
  EXPECT_NE(response.find("current_gtid: uuid:1-100\r\n"), std::string::npos);
  EXPECT_NE(response.find("processed_events: 321\r\n"), std::string::npos);
  EXPECT_NE(response.find("queue_size: 11\r\n"), std::string::npos);
  EXPECT_NE(response.find("crc_errors: 4\r\n"), std::string::npos);
  EXPECT_NE(response.find("schema_incompatible: true\r\n"), std::string::npos);
  EXPECT_NE(response.find("last_error_code: 2012\r\n"), std::string::npos);
  EXPECT_NE(response.find("last_error: schema changed\r\n"), std::string::npos);
  EXPECT_NE(response.find("last_applied_unixtime: 1722840000\r\n"), std::string::npos);
  EXPECT_NE(response.find("seconds_since_last_applied: 42\r\n"), std::string::npos);
}

TEST_F(ResponseFormatterTest, FormatPrometheusMetricsExposesReplicationDiagnostics) {
  AggregatedMetrics metrics;
  ServerStats stats;
  MockResponseBinlogReader reader;
  reader.running = false;
  reader.crc_errors = 4;
  reader.schema_incompatible = true;
  reader.last_error_code = mygram::utils::ErrorCode::kMySQLInvalidSchema;
  reader.last_error = "schema changed";
  reader.last_applied_unixtime = 1722840000;
  reader.seconds_since_last_applied = 42;
  stats.RecordDumpSuccess();
  stats.IncrementDumpFailureManual();
  stats.IncrementDumpFailureAuto();
  stats.IncrementRequestsDeniedRateLimitTcp();
  stats.IncrementRequestsDeniedAclHttp();
  stats.IncrementRequestsDeniedConnectionLimitTcp();
  stats.IncrementRequestsDeniedPoolFullTcp();

  const std::string response = ResponseFormatter::FormatPrometheusMetrics(metrics, stats, table_contexts_, &reader);

  EXPECT_NE(response.find("mygramdb_replication_state{state=\"failed\"} 1"), std::string::npos);
  EXPECT_NE(response.find("mygramdb_replication_crc_errors_total 4"), std::string::npos);
  EXPECT_NE(response.find("mygramdb_replication_schema_incompatible 1"), std::string::npos);
  EXPECT_NE(response.find("mygramdb_replication_last_error_code 2012"), std::string::npos);
  EXPECT_NE(response.find("mygramdb_replication_last_applied_unixtime 1722840000"), std::string::npos);
  EXPECT_NE(response.find("mygramdb_replication_seconds_since_last_applied 42"), std::string::npos);
  EXPECT_NE(response.find("mygramdb_dump_last_success_timestamp_seconds "), std::string::npos);
  EXPECT_NE(response.find("mygramdb_dump_failures_total{trigger=\"manual\"} 1"), std::string::npos);
  EXPECT_NE(response.find("mygramdb_dump_failures_total{trigger=\"auto\"} 1"), std::string::npos);
  EXPECT_NE(response.find("mygramdb_requests_denied_total{reason=\"rate_limit\",surface=\"tcp\"} 1"),
            std::string::npos);
  EXPECT_NE(response.find("mygramdb_requests_denied_total{reason=\"acl\",surface=\"http\"} 1"), std::string::npos);
  EXPECT_NE(response.find("mygramdb_requests_denied_total{reason=\"connection_limit\",surface=\"tcp\"} 1"),
            std::string::npos);
  EXPECT_NE(response.find("mygramdb_requests_denied_total{reason=\"pool_full\",surface=\"tcp\"} 1"), std::string::npos);
}
#endif

/**
 * @brief Test error response formatting
 */
TEST_F(ResponseFormatterTest, FormatError) {
  std::string response = ResponseFormatter::FormatError("Invalid query syntax");

  EXPECT_TRUE(response.find("ERROR") != std::string::npos);
  EXPECT_TRUE(response.find("Invalid query syntax") != std::string::npos);
}

TEST_F(ResponseFormatterTest, FormatTypedErrorIncludesStableNumericCode) {
  const auto error =
      mygram::utils::MakeError(mygram::utils::ErrorCode::kQueryExpressionParseError, "Invalid boolean expression");

  EXPECT_EQ(ResponseFormatter::FormatError(error), "ERROR [Expression parse error (3010)] Invalid boolean expression");
}

/**
 * @brief Test error response with special characters
 */
TEST_F(ResponseFormatterTest, FormatErrorWithSpecialCharacters) {
  std::string response = ResponseFormatter::FormatError("Error: \"quoted\" value");

  EXPECT_TRUE(response.find("ERROR") != std::string::npos);
  EXPECT_TRUE(response.find("quoted") != std::string::npos);
}

TEST_F(ResponseFormatterTest, FormatErrorSanitizesLineBreaksForSingleLineProtocol) {
  std::string response = ResponseFormatter::FormatError("Configuration validation failed:\r\n  missing table\tname");

  EXPECT_TRUE(response.find("ERROR") == 0);
  EXPECT_EQ(response.find('\r'), std::string::npos);
  EXPECT_EQ(response.find('\n'), std::string::npos);
  EXPECT_EQ(response.find('\t'), std::string::npos);
  EXPECT_NE(response.find("Configuration validation failed:"), std::string::npos);
  EXPECT_NE(response.find("missing table name"), std::string::npos);
}

TEST_F(ResponseFormatterTest, SanitizeDelimitedFieldRemovesFrameDelimiters) {
  std::string sanitized = ResponseFormatter::SanitizeDelimitedField("bad\r\nEND\r\nvalue\tmore");

  EXPECT_EQ(sanitized.find('\r'), std::string::npos);
  EXPECT_EQ(sanitized.find('\n'), std::string::npos);
  EXPECT_EQ(sanitized.find('\t'), std::string::npos);
  EXPECT_NE(sanitized.find("END"), std::string::npos);
}

/**
 * @brief Test error response with empty message
 */
TEST_F(ResponseFormatterTest, FormatErrorEmpty) {
  std::string response = ResponseFormatter::FormatError("");

  EXPECT_TRUE(response.find("ERROR") != std::string::npos);
}

/**
 * @brief FormatOk with no body returns the bare "+OK" status reply
 */
TEST_F(ResponseFormatterTest, FormatOkNoBody) {
  EXPECT_EQ(ResponseFormatter::FormatOk(), "+OK");
  EXPECT_EQ(ResponseFormatter::FormatOk(""), "+OK");
}

/**
 * @brief FormatOk with body produces "+OK <body>" with no trailing CRLF
 */
TEST_F(ResponseFormatterTest, FormatOkWithBody) {
  EXPECT_EQ(ResponseFormatter::FormatOk("hello"), "+OK hello");
  EXPECT_EQ(ResponseFormatter::FormatOk("Variable 'x' set to '1'"), "+OK Variable 'x' set to '1'");
}

/**
 * @brief FormatOk preserves bytes for combined "+OK\r\n<body>" call sites
 */
TEST_F(ResponseFormatterTest, FormatOkComposesWithCRLF) {
  // Mirrors the admin_handler.cpp pattern: FormatOk() + "\r\n" + body
  std::string composed = ResponseFormatter::FormatOk() + "\r\n" + "payload\r\n";
  EXPECT_EQ(composed, "+OK\r\npayload\r\n");
}

/**
 * @brief FormatStatus produces "OK <body>" without leading "+"
 */
TEST_F(ResponseFormatterTest, FormatStatusBasic) {
  EXPECT_EQ(ResponseFormatter::FormatStatus("CACHE_CLEARED"), "OK CACHE_CLEARED");
  EXPECT_EQ(ResponseFormatter::FormatStatus("DEBUG_ON"), "OK DEBUG_ON");
  EXPECT_EQ(ResponseFormatter::FormatStatus("SAVED /tmp/dump.bin"), "OK SAVED /tmp/dump.bin");
  EXPECT_EQ(ResponseFormatter::FormatStatus("DUMP_STARTED /tmp/x"), "OK DUMP_STARTED /tmp/x");
}

/**
 * @brief Test Prometheus metrics response
 */
TEST_F(ResponseFormatterTest, FormatPrometheusMetrics) {
  ServerStats stats;

  // Aggregate metrics
  auto metrics = StatisticsService::AggregateMetrics(table_contexts_);

  std::string response = ResponseFormatter::FormatPrometheusMetrics(metrics, stats, table_contexts_, nullptr);

  // Should contain Prometheus format metrics
  EXPECT_TRUE(response.find("#") != std::string::npos);  // Prometheus comments
  EXPECT_TRUE(response.find("mygramdb_") != std::string::npos || response.find("mygram_") != std::string::npos);
}

TEST_F(ResponseFormatterTest, FormatInfoAndPrometheusExposeOtherAndUnknownCommandCounters) {
  ServerStats stats;
  stats.IncrementCommand(query::QueryType::FACET);
  stats.IncrementCommand(query::QueryType::UNKNOWN);

  auto metrics = StatisticsService::AggregateMetrics(table_contexts_);

  std::string info = ResponseFormatter::FormatInfoResponse(metrics, stats, table_contexts_, nullptr, nullptr);
  EXPECT_NE(info.find("cmd_other: 1"), std::string::npos) << info;
  EXPECT_NE(info.find("cmd_unknown: 1"), std::string::npos) << info;

  std::string prometheus = ResponseFormatter::FormatPrometheusMetrics(metrics, stats, table_contexts_, nullptr);
  EXPECT_NE(prometheus.find("mygramdb_command_total{command=\"other\"} 1"), std::string::npos) << prometheus;
  EXPECT_NE(prometheus.find("mygramdb_command_total{command=\"unknown\"} 1"), std::string::npos) << prometheus;
}

TEST_F(ResponseFormatterTest, FormatPrometheusMetricsEscapesTableLabelValues) {
  TableContext special_table;
  special_table.name = "line\nbad\\name\"quote";
  special_table.config.ngram_size = 1;
  special_table.index = std::make_unique<index::Index>(1);
  special_table.doc_store = std::make_unique<storage::DocumentStore>();

  std::unordered_map<std::string, TableContext*> contexts;
  contexts[special_table.name] = &special_table;

  ServerStats stats;
  auto metrics = StatisticsService::AggregateMetrics(contexts);

  std::string response = ResponseFormatter::FormatPrometheusMetrics(metrics, stats, contexts, nullptr);

  EXPECT_NE(response.find("table=\"line\\nbad\\\\name\\\"quote\""), std::string::npos);
  EXPECT_EQ(response.find("table=\"line\nbad"), std::string::npos)
      << "Prometheus label values must not contain raw newlines";
  EXPECT_EQ(response.find("table=\"line\\nbad\\name\"quote\""), std::string::npos)
      << "Prometheus label values must escape backslash and quote";
}

/**
 * @brief Test Prometheus metrics with cache manager
 */
TEST_F(ResponseFormatterTest, FormatPrometheusMetricsWithCache) {
  ServerStats stats;

  // Create cache manager
  config::CacheConfig cache_config;
  cache_config.enabled = true;
  cache_config.max_memory_bytes = 100 * 1024 * 1024;
  cache::NgramConfigMap ngram_configs;
  for (const auto& [name, ctx] : table_contexts_) {
    ngram_configs[name] =
        cache::NgramConfig{ctx->config.ngram_size, ctx->config.kanji_ngram_size, ctx->config.cross_boundary_ngrams};
  }
  cache::CacheManager cache_manager(cache_config, std::move(ngram_configs));

  // Aggregate metrics
  auto metrics = StatisticsService::AggregateMetrics(table_contexts_);

  std::string response =
      ResponseFormatter::FormatPrometheusMetrics(metrics, stats, table_contexts_, nullptr, &cache_manager);

  // Should contain cache-specific Prometheus metrics
  EXPECT_TRUE(response.find("mygramdb_cache_hits_total") != std::string::npos);
  EXPECT_TRUE(response.find("mygramdb_cache_memory_bytes") != std::string::npos);
  EXPECT_TRUE(response.find("mygramdb_cache_entries") != std::string::npos);
  EXPECT_TRUE(response.find("mygramdb_cache_evictions_total") != std::string::npos);
  EXPECT_TRUE(response.find("mygramdb_cache_invalidations_total") != std::string::npos);
  EXPECT_TRUE(response.find("mygramdb_cache_hit_rate") != std::string::npos);
  EXPECT_TRUE(response.find("mygramdb_cache_misses_total") != std::string::npos);
  EXPECT_TRUE(response.find("mygramdb_cache_insert_rejections_total") != std::string::npos);
  EXPECT_TRUE(response.find("reason=\"memory_budget\"") != std::string::npos);
  EXPECT_TRUE(response.find("mygramdb_cache_stale_entry_removals_total") != std::string::npos);
  EXPECT_TRUE(response.find("mygramdb_cache_decompression_failures_total") != std::string::npos);
  EXPECT_TRUE(response.find("mygramdb_cache_stale_lru_entries_total") != std::string::npos);
}

TEST_F(ResponseFormatterTest, FormatPrometheusMetricsRetainsCacheSeriesWhenDisabled) {
  ServerStats stats;

  config::CacheConfig cache_config;
  cache_config.enabled = true;
  cache_config.max_memory_bytes = 100 * 1024 * 1024;
  cache::NgramConfigMap ngram_configs;
  for (const auto& [name, ctx] : table_contexts_) {
    ngram_configs[name] =
        cache::NgramConfig{ctx->config.ngram_size, ctx->config.kanji_ngram_size, ctx->config.cross_boundary_ngrams};
  }
  cache::CacheManager cache_manager(cache_config, std::move(ngram_configs));
  cache_manager.Disable();
  ASSERT_FALSE(cache_manager.IsEnabled());

  const auto metrics = StatisticsService::AggregateMetrics(table_contexts_);
  const std::string response =
      ResponseFormatter::FormatPrometheusMetrics(metrics, stats, table_contexts_, nullptr, &cache_manager);

  EXPECT_NE(response.find("mygramdb_cache_hits_total 0"), std::string::npos);
  EXPECT_NE(response.find("mygramdb_cache_misses_total{reason=\"not_found\"} 0"), std::string::npos);
  EXPECT_NE(response.find("mygramdb_cache_entries 0"), std::string::npos);
  EXPECT_NE(response.find("mygramdb_cache_memory_bytes{type=\"cache\"} 0"), std::string::npos);
  EXPECT_NE(response.find("mygramdb_cache_evictions_total 0"), std::string::npos);
  EXPECT_NE(response.find("mygramdb_cache_invalidations_total{phase=\"immediate\"} 0"), std::string::npos);
  EXPECT_NE(response.find("mygramdb_cache_hit_rate 0.0000"), std::string::npos);
}

TEST_F(ResponseFormatterTest, FormatPrometheusMetricsExposesThreadPoolSaturation) {
  ServerStats stats;
  ThreadPool pool(1, 7);
  std::promise<void> worker_started;
  std::promise<void> release_promise;
  const std::shared_future<void> release_worker = release_promise.get_future().share();

  ASSERT_TRUE(pool.Submit([&worker_started, release_worker]() {
    worker_started.set_value();
    release_worker.wait();
  }));
  worker_started.get_future().wait();
  ASSERT_TRUE(pool.Submit([] {}));
  ASSERT_EQ(pool.GetQueueSize(), 1U);

  const auto metrics = StatisticsService::AggregateMetrics(table_contexts_);
  const std::string response =
      ResponseFormatter::FormatPrometheusMetrics(metrics, stats, table_contexts_, nullptr, nullptr, &pool);

  EXPECT_NE(response.find("mygramdb_thread_pool_queue_depth 1"), std::string::npos);
  EXPECT_NE(response.find("mygramdb_thread_pool_queue_capacity 7"), std::string::npos);
  EXPECT_NE(response.find("mygramdb_thread_pool_workers 1"), std::string::npos);

  release_promise.set_value();
  pool.Shutdown();
}
