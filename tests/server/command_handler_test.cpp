/**
 * @file command_handler_test.cpp
 * @brief Tests for CommandHandler base helpers (loading-state guard).
 *
 * Verifies that the CheckNotLoading() helper used by SearchHandler,
 * DocumentHandler, and FacetHandler returns the unified ERROR response
 * when DUMP LOAD is in progress, and that the corresponding handler
 * Handle() entry points propagate that response unchanged.
 */

#include <gtest/gtest.h>

#include <atomic>
#include <memory>
#include <string>
#include <unordered_map>

#include "config/config.h"
#include "index/index.h"
#include "query/query_parser.h"
#include "server/handlers/document_handler.h"
#include "server/handlers/facet_handler.h"
#include "server/handlers/search_handler.h"
#include "server/server_stats.h"
#include "server/server_types.h"
#include "server/table_catalog.h"
#include "storage/document_store.h"

namespace mygramdb::server {

namespace {
constexpr const char* kLoadingError = "ERROR Server is loading, please try again later";
}  // namespace

class CommandHandlerLoadingTest : public ::testing::Test {
 protected:
  void SetUp() override {
    table_ctx_ = std::make_unique<TableContext>();
    table_ctx_->name = "articles";
    table_ctx_->config.ngram_size = 2;
    table_ctx_->index = std::make_unique<index::Index>(2);
    table_ctx_->doc_store = std::make_unique<storage::DocumentStore>();
    table_contexts_["articles"] = table_ctx_.get();

    // Add a single document so handlers operating on a real table do not
    // short-circuit on a missing table before the loading guard runs.
    auto doc_id = table_ctx_->doc_store->AddDocument("doc-1", {});
    ASSERT_TRUE(doc_id.has_value());
    table_ctx_->index->AddDocument(static_cast<index::DocId>(*doc_id), "hello world");

    config_ = std::make_unique<config::Config>();
    stats_ = std::make_unique<ServerStats>();
    table_catalog_ = std::make_unique<TableCatalog>(table_contexts_);

    handler_ctx_ = std::make_unique<HandlerContext>(HandlerContext{
        .table_catalog = table_catalog_.get(),
        .stats = *stats_,
        .full_config = config_.get(),
        .dump_dir = "",
        .dump_load_in_progress = dump_load_in_progress_,
        .dump_save_in_progress = dump_save_in_progress_,
        .optimization_in_progress = optimization_in_progress_,
        .replication_paused_for_dump = replication_paused_for_dump_,
        .mysql_reconnecting = mysql_reconnecting_,
#ifdef USE_MYSQL
        .sync_manager = nullptr,
#endif
    });
  }

  std::unique_ptr<TableContext> table_ctx_;
  std::unordered_map<std::string, TableContext*> table_contexts_;
  std::unique_ptr<config::Config> config_;
  std::unique_ptr<ServerStats> stats_;
  std::unique_ptr<TableCatalog> table_catalog_;
  std::atomic<bool> dump_load_in_progress_{false};
  std::atomic<bool> dump_save_in_progress_{false};
  std::atomic<bool> optimization_in_progress_{false};
  std::atomic<bool> replication_paused_for_dump_{false};
  std::atomic<bool> mysql_reconnecting_{false};
  std::unique_ptr<HandlerContext> handler_ctx_;
  ConnectionContext conn_ctx_;
};

// SEARCH should be rejected with the unified loading error message.
TEST_F(CommandHandlerLoadingTest, SearchHandlerReturnsLoadingErrorWhenDumpLoadInProgress) {
  dump_load_in_progress_.store(true);

  query::QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello");
  ASSERT_TRUE(query.has_value()) << query.error().message();

  SearchHandler handler(*handler_ctx_);
  std::string response = handler.Handle(*query, conn_ctx_);

  EXPECT_EQ(response, kLoadingError);
}

// COUNT shares ExecuteSearchPipeline with SEARCH; the same guard applies.
TEST_F(CommandHandlerLoadingTest, CountHandlerReturnsLoadingErrorWhenDumpLoadInProgress) {
  dump_load_in_progress_.store(true);

  query::QueryParser parser;
  auto query = parser.Parse("COUNT articles hello");
  ASSERT_TRUE(query.has_value()) << query.error().message();

  SearchHandler handler(*handler_ctx_);
  std::string response = handler.Handle(*query, conn_ctx_);

  EXPECT_EQ(response, kLoadingError);
}

// GET should be rejected before the document store is accessed.
TEST_F(CommandHandlerLoadingTest, DocumentHandlerReturnsLoadingErrorWhenDumpLoadInProgress) {
  dump_load_in_progress_.store(true);

  query::QueryParser parser;
  auto query = parser.Parse("GET articles doc-1");
  ASSERT_TRUE(query.has_value()) << query.error().message();

  DocumentHandler handler(*handler_ctx_);
  std::string response = handler.Handle(*query, conn_ctx_);

  EXPECT_EQ(response, kLoadingError);
}

// FACET should also short-circuit on the loading flag.
TEST_F(CommandHandlerLoadingTest, FacetHandlerReturnsLoadingErrorWhenDumpLoadInProgress) {
  dump_load_in_progress_.store(true);

  query::QueryParser parser;
  auto query = parser.Parse("FACET articles category");
  ASSERT_TRUE(query.has_value()) << query.error().message();

  FacetHandler handler(*handler_ctx_);
  std::string response = handler.Handle(*query, conn_ctx_);

  EXPECT_EQ(response, kLoadingError);
}

// Sanity check: when the flag is clear, none of the handlers emit the
// loading-error response. They may still produce other ERRORs (e.g. an empty
// FACET column or a missing GET document), but the loading message must not
// appear.
TEST_F(CommandHandlerLoadingTest, HandlersDoNotReturnLoadingErrorWhenFlagIsClear) {
  ASSERT_FALSE(dump_load_in_progress_.load());

  query::QueryParser parser;

  {
    auto query = parser.Parse("SEARCH articles hello");
    ASSERT_TRUE(query.has_value());
    SearchHandler handler(*handler_ctx_);
    EXPECT_NE(handler.Handle(*query, conn_ctx_), kLoadingError);
  }

  {
    auto query = parser.Parse("COUNT articles hello");
    ASSERT_TRUE(query.has_value());
    SearchHandler handler(*handler_ctx_);
    EXPECT_NE(handler.Handle(*query, conn_ctx_), kLoadingError);
  }

  {
    auto query = parser.Parse("GET articles doc-1");
    ASSERT_TRUE(query.has_value());
    DocumentHandler handler(*handler_ctx_);
    EXPECT_NE(handler.Handle(*query, conn_ctx_), kLoadingError);
  }
}

}  // namespace mygramdb::server
