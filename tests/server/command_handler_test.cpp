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
#ifdef USE_MYSQL
#include "server/handlers/sync_handler.h"
#endif
#include "server/server_stats.h"
#include "server/server_types.h"
#include "server/sync_operation_manager.h"
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
#ifdef USE_MYSQL
    config_->mysql.host = "localhost";
    config_->mysql.port = 3306;
    config_->mysql.user = "test";
    config_->mysql.password = "test";
    config_->mysql.database = "testdb";
    sync_manager_ = std::make_unique<SyncOperationManager>(table_contexts_, config_.get(), nullptr);
#endif
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
        .sync_manager = sync_manager_.get(),
#endif
    });
  }

  void TearDown() override {
#ifdef USE_MYSQL
    if (sync_manager_ != nullptr) {
      sync_manager_->ClearSyncingTableForTest("articles");
    }
#endif
  }

  std::unique_ptr<TableContext> table_ctx_;
  std::unordered_map<std::string, TableContext*> table_contexts_;
  std::unique_ptr<config::Config> config_;
#ifdef USE_MYSQL
  std::unique_ptr<SyncOperationManager> sync_manager_;
#endif
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

#ifdef USE_MYSQL
TEST_F(CommandHandlerLoadingTest, SearchHandlerReturnsNotReadyWhenTableIsSyncing) {
  sync_manager_->MarkSyncingTableForTest("articles");

  query::QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello");
  ASSERT_TRUE(query.has_value()) << query.error().message();

  SearchHandler handler(*handler_ctx_);
  std::string response = handler.Handle(*query, conn_ctx_);

  EXPECT_TRUE(response.find("ERROR") == 0) << response;
  EXPECT_NE(response.find("synchronizing"), std::string::npos) << response;
}

TEST_F(CommandHandlerLoadingTest, CountHandlerReturnsNotReadyWhenTableIsSyncing) {
  sync_manager_->MarkSyncingTableForTest("articles");

  query::QueryParser parser;
  auto query = parser.Parse("COUNT articles hello");
  ASSERT_TRUE(query.has_value()) << query.error().message();

  SearchHandler handler(*handler_ctx_);
  std::string response = handler.Handle(*query, conn_ctx_);

  EXPECT_TRUE(response.find("ERROR") == 0) << response;
  EXPECT_NE(response.find("synchronizing"), std::string::npos) << response;
}

TEST_F(CommandHandlerLoadingTest, DocumentHandlerReturnsNotReadyWhenTableIsSyncing) {
  sync_manager_->MarkSyncingTableForTest("articles");

  query::QueryParser parser;
  auto query = parser.Parse("GET articles doc-1");
  ASSERT_TRUE(query.has_value()) << query.error().message();

  DocumentHandler handler(*handler_ctx_);
  std::string response = handler.Handle(*query, conn_ctx_);

  EXPECT_TRUE(response.find("ERROR") == 0) << response;
  EXPECT_NE(response.find("synchronizing"), std::string::npos) << response;
}

TEST_F(CommandHandlerLoadingTest, FacetHandlerReturnsNotReadyWhenTableIsSyncing) {
  sync_manager_->MarkSyncingTableForTest("articles");

  query::QueryParser parser;
  auto query = parser.Parse("FACET articles category");
  ASSERT_TRUE(query.has_value()) << query.error().message();

  FacetHandler handler(*handler_ctx_);
  std::string response = handler.Handle(*query, conn_ctx_);

  EXPECT_TRUE(response.find("ERROR") == 0) << response;
  EXPECT_NE(response.find("synchronizing"), std::string::npos) << response;
}
#endif

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

namespace {

/// @brief Build a populated TableContext keyed by a qualified identity.
std::unique_ptr<TableContext> MakeTableContext(const std::string& database, const std::string& name) {
  auto ctx = std::make_unique<TableContext>();
  ctx->name = name;
  ctx->config.name = name;
  ctx->config.database = database;
  ctx->config.ngram_size = 2;
  ctx->index = std::make_unique<index::Index>(2);
  ctx->doc_store = std::make_unique<storage::DocumentStore>();
  auto doc_id = ctx->doc_store->AddDocument("doc-1", {});
  if (doc_id.has_value()) {
    ctx->index->AddDocument(static_cast<index::DocId>(*doc_id), "hello world");
  }
  return ctx;
}

/// @brief Return true when a handler response is the unified ERROR form.
bool IsErrorResponse(const std::string& response) {
  return response.rfind("ERROR", 0) == 0;
}

}  // namespace

/**
 * @brief Single-database resolution: bare identifiers resolve to the unique
 *        `database.table`, and qualified identifiers also work.
 */
class SingleDbResolutionTest : public ::testing::Test {
 protected:
  void SetUp() override {
    table_ctx_ = MakeTableContext("appdb", "articles");
    table_contexts_["appdb.articles"] = table_ctx_.get();

    config_ = std::make_unique<config::Config>();
    config_->mysql.database = "appdb";
    {
      config::TableConfig tc;
      tc.name = "articles";
      tc.database = "appdb";
      config_->tables.push_back(tc);
    }

    stats_ = std::make_unique<ServerStats>();
    table_catalog_ = std::make_unique<TableCatalog>(table_contexts_);

    handler_ctx_ = std::make_unique<HandlerContext>(HandlerContext{
        .table_catalog = table_catalog_.get(),
        .stats = *stats_,
        .full_config = config_.get(),
        .dump_dir = "",
        .dump_load_in_progress = flag_,
        .dump_save_in_progress = flag_,
        .optimization_in_progress = flag_,
        .replication_paused_for_dump = flag_,
        .mysql_reconnecting = flag_,
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
  std::atomic<bool> flag_{false};
  std::unique_ptr<HandlerContext> handler_ctx_;
  ConnectionContext conn_ctx_;
};

TEST_F(SingleDbResolutionTest, BareAndQualifiedSearchSucceed) {
  query::QueryParser parser;

  auto bare = parser.Parse("SEARCH articles hello");
  ASSERT_TRUE(bare.has_value());
  SearchHandler handler_bare(*handler_ctx_);
  EXPECT_FALSE(IsErrorResponse(handler_bare.Handle(*bare, conn_ctx_)));

  auto qualified = parser.Parse("SEARCH appdb.articles hello");
  ASSERT_TRUE(qualified.has_value());
  SearchHandler handler_qual(*handler_ctx_);
  EXPECT_FALSE(IsErrorResponse(handler_qual.Handle(*qualified, conn_ctx_)));
}

TEST_F(SingleDbResolutionTest, BareCountSucceeds) {
  query::QueryParser parser;
  auto query = parser.Parse("COUNT articles hello");
  ASSERT_TRUE(query.has_value());
  SearchHandler handler(*handler_ctx_);
  EXPECT_FALSE(IsErrorResponse(handler.Handle(*query, conn_ctx_)));
}

TEST_F(SingleDbResolutionTest, BareGetSucceeds) {
  query::QueryParser parser;
  auto query = parser.Parse("GET articles doc-1");
  ASSERT_TRUE(query.has_value());
  DocumentHandler handler(*handler_ctx_);
  EXPECT_FALSE(IsErrorResponse(handler.Handle(*query, conn_ctx_)));
}

TEST_F(SingleDbResolutionTest, BareFacetResolves) {
  query::QueryParser parser;
  auto query = parser.Parse("FACET articles category");
  ASSERT_TRUE(query.has_value());
  FacetHandler handler(*handler_ctx_);
  // The column may be absent, but resolution must not fail with "Table not
  // found": a bare name in single-db config resolves to appdb.articles.
  std::string response = handler.Handle(*query, conn_ctx_);
  EXPECT_EQ(response.find("Table not found"), std::string::npos) << response;
}

/**
 * @brief Multi-database resolution: bare identifiers are rejected; qualified
 *        identifiers resolve; an ambiguous bare name is rejected.
 */
class MultiDbResolutionTest : public ::testing::Test {
 protected:
  void SetUp() override {
    ctx_a_ = MakeTableContext("db1", "articles");
    ctx_b_ = MakeTableContext("db2", "comments");
    ctx_c_ = MakeTableContext("db2", "articles");  // Same bare name in db2.
    table_contexts_["db1.articles"] = ctx_a_.get();
    table_contexts_["db2.comments"] = ctx_b_.get();
    table_contexts_["db2.articles"] = ctx_c_.get();

    config_ = std::make_unique<config::Config>();
    config_->mysql.database = "db1";
    for (const auto& [db, name] :
         {std::pair{"db1", "articles"}, std::pair{"db2", "comments"}, std::pair{"db2", "articles"}}) {
      config::TableConfig tc;
      tc.name = name;
      tc.database = db;
      config_->tables.push_back(tc);
    }

    stats_ = std::make_unique<ServerStats>();
    table_catalog_ = std::make_unique<TableCatalog>(table_contexts_);

    handler_ctx_ = std::make_unique<HandlerContext>(HandlerContext{
        .table_catalog = table_catalog_.get(),
        .stats = *stats_,
        .full_config = config_.get(),
        .dump_dir = "",
        .dump_load_in_progress = flag_,
        .dump_save_in_progress = flag_,
        .optimization_in_progress = flag_,
        .replication_paused_for_dump = flag_,
        .mysql_reconnecting = flag_,
#ifdef USE_MYSQL
        .sync_manager = nullptr,
#endif
    });
  }

  std::unique_ptr<TableContext> ctx_a_;
  std::unique_ptr<TableContext> ctx_b_;
  std::unique_ptr<TableContext> ctx_c_;
  std::unordered_map<std::string, TableContext*> table_contexts_;
  std::unique_ptr<config::Config> config_;
  std::unique_ptr<ServerStats> stats_;
  std::unique_ptr<TableCatalog> table_catalog_;
  std::atomic<bool> flag_{false};
  std::unique_ptr<HandlerContext> handler_ctx_;
  ConnectionContext conn_ctx_;
};

TEST_F(MultiDbResolutionTest, BareNameRejected) {
  query::QueryParser parser;
  auto query = parser.Parse("SEARCH comments hello");
  ASSERT_TRUE(query.has_value());
  SearchHandler handler(*handler_ctx_);
  std::string response = handler.Handle(*query, conn_ctx_);
  ASSERT_TRUE(IsErrorResponse(response)) << response;
  EXPECT_NE(response.find("Bare table names are not supported"), std::string::npos) << response;
}

TEST_F(MultiDbResolutionTest, QualifiedNameResolves) {
  query::QueryParser parser;
  auto query = parser.Parse("SEARCH db2.comments hello");
  ASSERT_TRUE(query.has_value());
  SearchHandler handler(*handler_ctx_);
  EXPECT_FALSE(IsErrorResponse(handler.Handle(*query, conn_ctx_)));
}

TEST_F(MultiDbResolutionTest, AmbiguousBareNameRejected) {
  // "articles" exists in both db1 and db2; even ignoring the multi-db gate the
  // resolver cannot disambiguate it.
  query::QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello");
  ASSERT_TRUE(query.has_value());
  SearchHandler handler(*handler_ctx_);
  EXPECT_TRUE(IsErrorResponse(handler.Handle(*query, conn_ctx_)));
}

}  // namespace mygramdb::server
