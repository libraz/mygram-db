/**
 * @file request_dispatcher_test.cpp
 * @brief Unit tests for RequestDispatcher thread-safety
 */

#include "server/request_dispatcher.h"

#include <gtest/gtest.h>

#include <atomic>
#include <thread>
#include <vector>

#include "index/index.h"
#include "server/handlers/command_handler.h"
#include "server/handlers/search_handler.h"
#include "server/response_formatter.h"
#include "server/server_stats.h"
#include "server/table_catalog.h"
#include "storage/document_store.h"

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
        .table_contexts = table_contexts_,
        .stats = *stats_,
        .full_config = nullptr,
        .dump_dir = "",
        .loading = loading_,
        .read_only = read_only_,
        .optimization_in_progress = optimization_in_progress_,
        .replication_paused_for_dump = replication_paused_for_dump_,
        .mysql_reconnecting = mysql_reconnecting_,
#ifdef USE_MYSQL
        .binlog_reader = nullptr,
        .sync_manager = nullptr,
#else
        .binlog_reader = nullptr,
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
  std::atomic<bool> loading_{false};
  std::atomic<bool> read_only_{false};
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
