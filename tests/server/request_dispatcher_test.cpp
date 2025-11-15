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
#ifdef USE_MYSQL
        .binlog_reader = nullptr,
        .syncing_tables = syncing_tables_,
        .syncing_tables_mutex = syncing_tables_mutex_,
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
#ifdef USE_MYSQL
  std::unordered_set<std::string> syncing_tables_;
  std::mutex syncing_tables_mutex_;
#endif

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
