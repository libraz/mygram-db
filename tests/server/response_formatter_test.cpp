/**
 * @file response_formatter_test.cpp
 * @brief Unit tests for ResponseFormatter
 */

#include "server/response_formatter.h"

#include <gtest/gtest.h>

#include "cache/cache_manager.h"
#include "cache/cache_types.h"
#include "config/config.h"
#include "index/index.h"
#include "server/server_stats.h"
#include "server/statistics_service.h"
#include "server/tcp_server.h"  // For TableContext
#include "storage/document_store.h"

using namespace mygramdb::server;
using namespace mygramdb;

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

TEST_F(ResponseFormatterTest, FormatSearchResponseSanitizesPrimaryKeyDelimiters) {
  auto doc_id = table_context_.doc_store->AddDocument("pk with\r\nnewline\tand tab");

  std::vector<index::DocId> results = {*doc_id};

  std::string response = ResponseFormatter::FormatSearchResponse(results, 1, table_context_.doc_store.get());

  EXPECT_EQ(response, "OK RESULTS 1 pk_with__newline_and_tab");
}

TEST_F(ResponseFormatterTest, FormatSearchResponseWithHighlightsTerminatesMultilineResponse) {
  auto doc_id1 = table_context_.doc_store->AddDocument("pk1");

  std::vector<index::DocId> results = {*doc_id1};
  std::vector<std::string> snippets = {"hello <em>world</em>"};

  std::string response =
      ResponseFormatter::FormatSearchResponseWithHighlights(results, 1, table_context_.doc_store.get(), snippets);

  EXPECT_EQ(response, "OK RESULTS 1\r\npk1\thello <em>world</em>\r\n");
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

/**
 * @brief Test error response formatting
 */
TEST_F(ResponseFormatterTest, FormatError) {
  std::string response = ResponseFormatter::FormatError("Invalid query syntax");

  EXPECT_TRUE(response.find("ERROR") != std::string::npos);
  EXPECT_TRUE(response.find("Invalid query syntax") != std::string::npos);
}

/**
 * @brief Test error response with special characters
 */
TEST_F(ResponseFormatterTest, FormatErrorWithSpecialCharacters) {
  std::string response = ResponseFormatter::FormatError("Error: \"quoted\" value");

  EXPECT_TRUE(response.find("ERROR") != std::string::npos);
  EXPECT_TRUE(response.find("quoted") != std::string::npos);
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
}
