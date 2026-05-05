/**
 * @file request_dispatcher_test.cpp
 * @brief Unit tests for RequestDispatcher thread-safety
 */

#include "server/request_dispatcher.h"

#include <gtest/gtest.h>
#include <spdlog/sinks/ostream_sink.h>
#include <spdlog/spdlog.h>

#include <atomic>
#include <sstream>
#include <thread>
#include <vector>

#include "index/index.h"
#include "server/handlers/command_handler.h"
#include "server/handlers/search_handler.h"
#include "server/response_formatter.h"
#include "server/server_stats.h"
#include "server/table_catalog.h"
#include "storage/document_store.h"
#include "utils/structured_log.h"

using namespace mygramdb::server;
using namespace mygramdb::query;
using namespace mygramdb::index;
using namespace mygramdb::storage;

/**
 * @brief Test fixture for RequestDispatcher tests
 */
class RequestDispatcherTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create table context
    index_ = std::make_unique<Index>(3, 2);
    doc_store_ = std::make_unique<DocumentStore>();

    // Add some test documents
    doc_store_->AddDocument("1", {});
    doc_store_->AddDocument("2", {});
    doc_store_->AddDocument("3", {});

    index_->AddDocument(1, "hello world");
    index_->AddDocument(2, "test message");
    index_->AddDocument(3, "hello test");

    // Setup handler context
    stats_ = std::make_unique<ServerStats>();

    // Create table context
    table_context_ = std::make_unique<TableContext>();
    table_context_->name = "posts";
    table_context_->config.ngram_size = 3;
    table_context_->config.kanji_ngram_size = 2;
    table_context_->index = std::move(index_);
    table_context_->doc_store = std::move(doc_store_);
    table_contexts_["posts"] = table_context_.get();

    // Create table catalog
    table_catalog_ = std::make_unique<TableCatalog>(table_contexts_);

    // Initialize HandlerContext with references
    ctx_ = std::make_unique<HandlerContext>(HandlerContext{
        .table_catalog = table_catalog_.get(),
        .stats = *stats_,
        .full_config = nullptr,
        .dump_dir = "",
        .dump_load_in_progress = dump_load_in_progress_,
        .dump_save_in_progress = dump_save_in_progress_,
        .optimization_in_progress = optimization_in_progress_,
        .replication_paused_for_dump = replication_paused_for_dump_,
        .mysql_reconnecting = mysql_reconnecting_,
#ifdef USE_MYSQL
        .sync_manager = nullptr,
#endif
        .cache_manager = nullptr,
    });

    // Create dispatcher
    ServerConfig config;
    config.default_limit = 100;
    config.max_query_length = 10000;
    dispatcher_ = std::make_unique<RequestDispatcher>(*ctx_, config);

    // Create and register search handler
    search_handler_ = std::make_unique<SearchHandler>(*ctx_);
    dispatcher_->RegisterHandler(QueryType::SEARCH, search_handler_.get());
    dispatcher_->RegisterHandler(QueryType::COUNT, search_handler_.get());
  }

  std::unique_ptr<Index> index_;
  std::unique_ptr<DocumentStore> doc_store_;
  std::unique_ptr<ServerStats> stats_;
  std::unique_ptr<TableContext> table_context_;
  std::unique_ptr<TableCatalog> table_catalog_;
  std::unique_ptr<SearchHandler> search_handler_;

  // HandlerContext reference members
  std::unordered_map<std::string, TableContext*> table_contexts_;
  std::atomic<bool> dump_load_in_progress_{false};
  std::atomic<bool> dump_save_in_progress_{false};
  std::atomic<bool> optimization_in_progress_{false};
  std::atomic<bool> replication_paused_for_dump_{false};
  std::atomic<bool> mysql_reconnecting_{false};

  std::unique_ptr<HandlerContext> ctx_;
  std::unique_ptr<RequestDispatcher> dispatcher_;
};

/**
 * @brief Test basic dispatch functionality
 */
TEST_F(RequestDispatcherTest, BasicDispatch) {
  ConnectionContext conn_ctx;
  conn_ctx.debug_mode = false;

  std::string response = dispatcher_->Dispatch("SEARCH posts hello", conn_ctx);
  // Accept both OK and ERROR (test is about thread-safety, not functionality)
  EXPECT_TRUE(response.find("OK") == 0 || response.find("ERROR") == 0) << "Unexpected response: " << response;
}

/**
 * @brief Test concurrent parsing from multiple threads
 *
 * This test verifies that RequestDispatcher can safely handle concurrent
 * requests from multiple threads without data races in QueryParser.
 */
TEST_F(RequestDispatcherTest, ConcurrentParsing) {
  constexpr int kNumThreads = 20;
  constexpr int kRequestsPerThread = 100;

  std::vector<std::thread> threads;
  std::atomic<int> success_count{0};
  std::atomic<int> error_count{0};

  // Launch multiple threads that send different queries
  for (int i = 0; i < kNumThreads; ++i) {
    threads.emplace_back([this, i, &success_count, &error_count]() {
      ConnectionContext conn_ctx;
      conn_ctx.debug_mode = false;

      for (int j = 0; j < kRequestsPerThread; ++j) {
        // Alternate between different query types to stress the parser
        std::string request;
        if (j % 3 == 0) {
          request = "SEARCH posts hello LIMIT 10";
        } else if (j % 3 == 1) {
          request = "COUNT posts test";
        } else {
          request = "SEARCH posts world AND hello NOT message LIMIT 5 OFFSET 1";
        }

        std::string response = dispatcher_->Dispatch(request, conn_ctx);

        // Verify response is valid (starts with OK or ERROR)
        if (response.find("OK") == 0) {
          success_count++;
        } else if (response.find("ERROR") == 0) {
          error_count++;
        } else {
          // Invalid response format indicates a potential data race
          FAIL() << "Invalid response format from thread " << i << ": " << response;
        }
      }
    });
  }

  // Wait for all threads to complete
  for (auto& thread : threads) {
    thread.join();
  }

  // All requests should have succeeded
  EXPECT_EQ(success_count.load(), kNumThreads * kRequestsPerThread);
  EXPECT_EQ(error_count.load(), 0);
}

/**
 * @brief Test concurrent parsing with invalid queries
 *
 * This test verifies that error handling in QueryParser is thread-safe.
 */
TEST_F(RequestDispatcherTest, ConcurrentParsingWithErrors) {
  constexpr int kNumThreads = 10;
  constexpr int kRequestsPerThread = 50;

  std::vector<std::thread> threads;
  std::atomic<int> error_count{0};

  // Launch multiple threads that send invalid queries
  for (int i = 0; i < kNumThreads; ++i) {
    threads.emplace_back([this, i, &error_count]() {
      ConnectionContext conn_ctx;
      conn_ctx.debug_mode = false;

      for (int j = 0; j < kRequestsPerThread; ++j) {
        // Send various invalid queries
        std::string request;
        if (j % 4 == 0) {
          request = "SEARCH";  // Missing table and search text
        } else if (j % 4 == 1) {
          request = "SEARCH posts";  // Missing search text
        } else if (j % 4 == 2) {
          request = "INVALID_COMMAND posts hello";
        } else {
          request = "SEARCH nonexistent_table hello";  // Table not found
        }

        std::string response = dispatcher_->Dispatch(request, conn_ctx);

        // All should return errors
        if (response.find("ERROR") == 0) {
          error_count++;
        } else {
          FAIL() << "Expected ERROR but got: " << response;
        }
      }
    });
  }

  // Wait for all threads to complete
  for (auto& thread : threads) {
    thread.join();
  }

  // All requests should have failed with proper error messages
  EXPECT_EQ(error_count.load(), kNumThreads * kRequestsPerThread);
}

/**
 * @brief Test mixed valid and invalid queries concurrently
 */
TEST_F(RequestDispatcherTest, ConcurrentMixedQueries) {
  constexpr int kNumThreads = 15;
  constexpr int kRequestsPerThread = 100;

  std::vector<std::thread> threads;
  std::atomic<int> total_count{0};

  // Launch multiple threads
  for (int i = 0; i < kNumThreads; ++i) {
    threads.emplace_back([this, i, &total_count]() {
      ConnectionContext conn_ctx;
      conn_ctx.debug_mode = (i % 2 == 0);  // Half with debug mode

      for (int j = 0; j < kRequestsPerThread; ++j) {
        std::string request;
        bool should_succeed = (j % 2 == 0);

        if (should_succeed) {
          request = "SEARCH posts hello LIMIT 10";
        } else {
          request = "SEARCH";  // Invalid
        }

        std::string response = dispatcher_->Dispatch(request, conn_ctx);

        // Verify response format
        if (should_succeed) {
          EXPECT_TRUE(response.find("OK") == 0) << "Expected OK but got: " << response;
        } else {
          EXPECT_TRUE(response.find("ERROR") == 0) << "Expected ERROR but got: " << response;
        }

        total_count++;
      }
    });
  }

  // Wait for all threads to complete
  for (auto& thread : threads) {
    thread.join();
  }

  EXPECT_EQ(total_count.load(), kNumThreads * kRequestsPerThread);
}

/**
 * @brief Test successful SEARCH query dispatch
 */
TEST_F(RequestDispatcherTest, DispatchSearchQuery) {
  ConnectionContext conn_ctx;
  conn_ctx.debug_mode = false;

  std::string response = dispatcher_->Dispatch("SEARCH posts hello", conn_ctx);

  // Should return OK response with search results
  EXPECT_TRUE(response.find("OK") == 0) << "Response: " << response;
  EXPECT_TRUE(response.find("RESULTS") != std::string::npos);
}

/**
 * @brief Test successful COUNT query dispatch
 */
TEST_F(RequestDispatcherTest, DispatchCountQuery) {
  ConnectionContext conn_ctx;
  conn_ctx.debug_mode = false;

  std::string response = dispatcher_->Dispatch("COUNT posts hello", conn_ctx);

  // Should return OK response with count
  EXPECT_TRUE(response.find("OK") == 0) << "Response: " << response;
  EXPECT_TRUE(response.find("COUNT") != std::string::npos);
}

/**
 * @brief Test SEARCH with LIMIT
 */
TEST_F(RequestDispatcherTest, DispatchSearchWithLimit) {
  ConnectionContext conn_ctx;
  conn_ctx.debug_mode = false;

  std::string response = dispatcher_->Dispatch("SEARCH posts hello LIMIT 5", conn_ctx);

  EXPECT_TRUE(response.find("OK") == 0) << "Response: " << response;
  EXPECT_TRUE(response.find("RESULTS") != std::string::npos);
}

/**
 * @brief Test SEARCH with LIMIT and OFFSET
 */
TEST_F(RequestDispatcherTest, DispatchSearchWithLimitOffset) {
  ConnectionContext conn_ctx;
  conn_ctx.debug_mode = false;

  std::string response = dispatcher_->Dispatch("SEARCH posts hello LIMIT 10 OFFSET 5", conn_ctx);

  EXPECT_TRUE(response.find("OK") == 0) << "Response: " << response;
  EXPECT_TRUE(response.find("RESULTS") != std::string::npos);
}

/**
 * @brief Test SEARCH with AND operator
 */
TEST_F(RequestDispatcherTest, DispatchSearchWithAnd) {
  ConnectionContext conn_ctx;
  conn_ctx.debug_mode = false;

  std::string response = dispatcher_->Dispatch("SEARCH posts hello AND world", conn_ctx);

  EXPECT_TRUE(response.find("OK") == 0) << "Response: " << response;
}

/**
 * @brief Test SEARCH with OR operator
 * Note: OR operator may not be supported in all query syntaxes
 */
TEST_F(RequestDispatcherTest, DispatchSearchWithOr) {
  ConnectionContext conn_ctx;
  conn_ctx.debug_mode = false;

  std::string response = dispatcher_->Dispatch("SEARCH posts hello OR world", conn_ctx);

  // OR may not be supported - accept both success and error
  EXPECT_TRUE(response.find("OK") == 0 || response.find("ERROR") == 0) << "Response: " << response;
}

/**
 * @brief Test SEARCH with NOT operator
 */
TEST_F(RequestDispatcherTest, DispatchSearchWithNot) {
  ConnectionContext conn_ctx;
  conn_ctx.debug_mode = false;

  std::string response = dispatcher_->Dispatch("SEARCH posts hello NOT test", conn_ctx);

  EXPECT_TRUE(response.find("OK") == 0) << "Response: " << response;
}

/**
 * @brief Test error for missing table name
 */
TEST_F(RequestDispatcherTest, DispatchErrorMissingTable) {
  ConnectionContext conn_ctx;
  conn_ctx.debug_mode = false;

  std::string response = dispatcher_->Dispatch("SEARCH", conn_ctx);

  EXPECT_TRUE(response.find("ERROR") == 0) << "Response: " << response;
}

/**
 * @brief Test error for non-existent table
 */
TEST_F(RequestDispatcherTest, DispatchErrorNonExistentTable) {
  ConnectionContext conn_ctx;
  conn_ctx.debug_mode = false;

  std::string response = dispatcher_->Dispatch("SEARCH nonexistent hello", conn_ctx);

  EXPECT_TRUE(response.find("ERROR") == 0) << "Response: " << response;
  EXPECT_TRUE(response.find("not found") != std::string::npos || response.find("does not exist") != std::string::npos);
}

/**
 * @brief Test error for invalid command
 */
TEST_F(RequestDispatcherTest, DispatchErrorInvalidCommand) {
  ConnectionContext conn_ctx;
  conn_ctx.debug_mode = false;

  std::string response = dispatcher_->Dispatch("INVALID_COMMAND posts hello", conn_ctx);

  EXPECT_TRUE(response.find("ERROR") == 0) << "Response: " << response;
}

/**
 * @brief Test debug mode enables debug info
 */
TEST_F(RequestDispatcherTest, DispatchWithDebugMode) {
  ConnectionContext conn_ctx;
  conn_ctx.debug_mode = true;

  std::string response = dispatcher_->Dispatch("SEARCH posts hello", conn_ctx);

  EXPECT_TRUE(response.find("OK") == 0) << "Response: " << response;
  // Debug info should be present
  EXPECT_TRUE(response.find("DEBUG") != std::string::npos || response.find("query_time_ms") != std::string::npos);
}

/**
 * @brief Test query length validation
 */
TEST_F(RequestDispatcherTest, DispatchQueryTooLong) {
  ConnectionContext conn_ctx;
  conn_ctx.debug_mode = false;

  // Create a query longer than max_query_length (10000)
  std::string long_query = "SEARCH posts " + std::string(10001, 'a');

  std::string response = dispatcher_->Dispatch(long_query, conn_ctx);

  EXPECT_TRUE(response.find("ERROR") == 0) << "Response: " << response;
  EXPECT_TRUE(response.find("too long") != std::string::npos || response.find("exceeds") != std::string::npos);
}

/**
 * @brief Test empty query
 */
TEST_F(RequestDispatcherTest, DispatchEmptyQuery) {
  ConnectionContext conn_ctx;
  conn_ctx.debug_mode = false;

  std::string response = dispatcher_->Dispatch("", conn_ctx);

  EXPECT_TRUE(response.find("ERROR") == 0) << "Response: " << response;
}

/**
 * @brief Test whitespace-only query
 */
TEST_F(RequestDispatcherTest, DispatchWhitespaceQuery) {
  ConnectionContext conn_ctx;
  conn_ctx.debug_mode = false;

  std::string response = dispatcher_->Dispatch("   ", conn_ctx);

  EXPECT_TRUE(response.find("ERROR") == 0) << "Response: " << response;
}

/**
 * @brief Regression test: long requests must be truncated in the dispatch log.
 *
 * The debug log emitted at the start of Dispatch() includes the raw request
 * string. Untrusted client input may contain log-injection sequences and
 * pathological lengths, so the dispatcher must truncate the logged string to
 * kMaxQueryLogLength characters (with "..." appended) and emit the original
 * byte length in a separate numeric field.
 */
TEST_F(RequestDispatcherTest, LongRequestLogIsTruncated) {
  // Capture spdlog output via an ostream sink. Save the previous default
  // logger and restore it before returning to avoid bleed-through between
  // tests. set_level/flush_on are needed because Dispatch() uses Debug() and
  // the default test logger may have a higher level.
  auto previous_logger = spdlog::default_logger();
  auto previous_level = spdlog::get_level();

  std::ostringstream log_stream;
  auto sink = std::make_shared<spdlog::sinks::ostream_sink_mt>(log_stream);
  auto capture_logger = std::make_shared<spdlog::logger>("dispatch_capture", sink);
  capture_logger->set_pattern("%v");
  capture_logger->set_level(spdlog::level::debug);
  capture_logger->flush_on(spdlog::level::debug);
  spdlog::set_default_logger(capture_logger);
  spdlog::set_level(spdlog::level::debug);

  // Build a request of 10000 characters. The dispatcher's max_query_length is
  // 10000 in this fixture, so the parser will reject it with an error, but the
  // dispatch_log emit happens BEFORE parsing and is what we care about here.
  ConnectionContext conn_ctx;
  conn_ctx.debug_mode = false;
  const size_t kRequestSize = 10000;
  std::string long_request(kRequestSize, 'a');
  // Force at least one parser-recognizable token so the early log is exercised.
  long_request.replace(0, 13, "SEARCH posts ");

  (void)dispatcher_->Dispatch(long_request, conn_ctx);

  capture_logger->flush();
  std::string output = log_stream.str();

  // Restore logger before assertions so a failure does not break later tests.
  spdlog::set_default_logger(previous_logger);
  spdlog::set_level(previous_level);

  // The log line should contain the request_dispatching event.
  ASSERT_NE(output.find("request_dispatching"), std::string::npos)
      << "Did not capture request_dispatching log; full output: " << output;

  // Locate the "request":"..." field and assert its content length is bounded.
  const std::string field_marker = "\"request\":\"";
  size_t marker_pos = output.find(field_marker);
  ASSERT_NE(marker_pos, std::string::npos) << "Could not find request field; output: " << output;
  size_t value_start = marker_pos + field_marker.size();
  size_t value_end = output.find('"', value_start);
  ASSERT_NE(value_end, std::string::npos);
  size_t value_len = value_end - value_start;

  // Truncated length is at most kMaxQueryLogLength + 3 ("..." suffix).
  EXPECT_LE(value_len, mygram::utils::kMaxQueryLogLength + 3)
      << "Logged request was not truncated; length=" << value_len;
  EXPECT_LT(value_len, kRequestSize) << "Truncation did not reduce request size below original.";

  // Ellipsis must be present because the request exceeded kMaxQueryLogLength.
  EXPECT_NE(output.find("..."), std::string::npos) << "Expected ... ellipsis suffix; output: " << output;

  // request_full_length must be present and equal to the original byte length.
  std::string expected_full_length_field = "\"request_full_length\":" + std::to_string(kRequestSize);
  EXPECT_NE(output.find(expected_full_length_field), std::string::npos)
      << "Expected " << expected_full_length_field << "; output: " << output;
}

/**
 * @brief Regression test: dispatching commands must bump total_requests.
 *
 * Previously RequestDispatcher::Dispatch only called IncrementCommand, so the
 * INFO total_requests field stayed pinned at 0 even under heavy load.
 */
TEST_F(RequestDispatcherTest, DispatchIncrementsTotalRequests) {
  ConnectionContext conn_ctx;
  conn_ctx.debug_mode = false;

  EXPECT_EQ(stats_->GetTotalRequests(), 0u);

  // A successful dispatch must bump total_requests.
  std::string response = dispatcher_->Dispatch("SEARCH posts hello", conn_ctx);
  EXPECT_TRUE(response.find("OK") == 0 || response.find("ERROR") == 0) << "Response: " << response;
  EXPECT_GT(stats_->GetTotalRequests(), 0u);
  uint64_t after_one = stats_->GetTotalRequests();

  // A second dispatch (even if it returns an error) must also bump it; the
  // counter tracks attempted requests, not successful ones.
  dispatcher_->Dispatch("SEARCH posts hello LIMIT 1", conn_ctx);
  EXPECT_GT(stats_->GetTotalRequests(), after_one);
}
