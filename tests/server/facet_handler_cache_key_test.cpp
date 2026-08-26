/**
 * @file facet_handler_cache_key_test.cpp
 * @brief FACET must cache under the same table key the invalidation side uses
 *
 * A client may address a table by its bare name, while binlog apply, DDL, SYNC
 * completion and CACHE CLEAR all invalidate by the qualified `database.table`
 * key. A FACET cached under the bare name would survive those and keep serving
 * counts from before the mutation.
 */

#include <gtest/gtest.h>
#include <spdlog/spdlog.h>

#include <atomic>
#include <memory>
#include <string>
#include <unordered_map>

#include "cache/cache_manager.h"
#include "cache/cache_types.h"
#include "config/config.h"
#include "index/index.h"
#include "query/query_parser.h"
#include "query/synonym_dictionary.h"
#include "server/handlers/facet_handler.h"
#include "server/server_stats.h"
#include "server/server_types.h"
#include "server/table_catalog.h"
#include "storage/document_store.h"

namespace mygramdb::server {

namespace {

constexpr const char* kQualifiedTable = "app_db.articles";
constexpr const char* kBareTable = "articles";

}  // namespace

class FacetHandlerCacheKeyTest : public ::testing::Test {
 protected:
  void SetUp() override {
    spdlog::set_level(spdlog::level::off);

    table_ctx_ = std::make_unique<TableContext>();
    table_ctx_->name = "articles";
    table_ctx_->config.name = "articles";
    table_ctx_->config.database = "app_db";
    table_ctx_->config.primary_key = "id";
    table_ctx_->config.ngram_size = 2;
    table_ctx_->config.kanji_ngram_size = 2;
    table_ctx_->index = std::make_unique<index::Index>(2);
    table_ctx_->doc_store = std::make_unique<storage::DocumentStore>();
    table_ctx_->synonym_dict = std::make_unique<query::SynonymDictionary>();

    table_contexts_[kQualifiedTable] = table_ctx_.get();

    config_ = std::make_unique<config::Config>();
    config_->cache.enabled = true;
    // Test queries cost far less than the production cost floor, which would
    // otherwise keep every result out of the cache.
    config_->cache.min_query_cost_ms = 0.0;
    config::TableConfig table_cfg;
    table_cfg.name = "articles";
    table_cfg.database = "app_db";
    table_cfg.primary_key = "id";
    table_cfg.ngram_size = 2;
    config_->tables.push_back(table_cfg);

    cache::NgramConfigMap ngram_configs;
    ngram_configs[kQualifiedTable] = cache::NgramConfig{2, 2, false};
    cache_manager_ = std::make_unique<cache::CacheManager>(config_->cache, std::move(ngram_configs));

    stats_ = std::make_unique<ServerStats>();
    table_catalog_ = std::make_unique<TableCatalog>(table_contexts_);

    handler_ctx_ = std::make_unique<HandlerContext>(HandlerContext{
        .table_catalog = table_catalog_.get(),
        .stats = *stats_,
        .full_config = config_.get(),
        .dump_dir = "/tmp",
        .dump_load_in_progress = dump_load_in_progress_,
        .dump_save_in_progress = dump_save_in_progress_,
        .optimization_in_progress = optimization_in_progress_,
        .replication_paused_for_dump = replication_paused_for_dump_,
        .mysql_reconnecting = mysql_reconnecting_,
#ifdef USE_MYSQL
        .sync_manager = nullptr,
#endif
        .cache_manager = cache_manager_.get(),
        .variable_manager = nullptr,
    });

    handler_ = std::make_unique<FacetHandler>(*handler_ctx_);
  }

  /// Add a document to the index and doc store, then invalidate the cache the
  /// way binlog apply does: by the qualified table key.
  void AddDocumentAndInvalidate(const std::string& pk, const std::string& text, const std::string& category) {
    storage::FilterMap filters;
    filters["category"] = std::string(category);
    const auto normalized = table_ctx_->index->NormalizeText(text);
    auto doc_id = table_ctx_->doc_store->AddDocument(pk, filters, normalized);
    ASSERT_TRUE(doc_id.has_value());
    table_ctx_->index->AddDocument(static_cast<index::DocId>(*doc_id), normalized);
    cache_manager_->Invalidate(kQualifiedTable, /*old_text=*/"", normalized);
  }

  static query::Query MakeFacetQuery(const std::string& table) {
    query::Query query;
    query.type = query::QueryType::FACET;
    query.table = table;
    query.facet_column = "category";
    query.search_text = "car";
    return query;
  }

  std::unique_ptr<TableContext> table_ctx_;
  std::unordered_map<std::string, TableContext*> table_contexts_;
  std::unique_ptr<config::Config> config_;
  std::unique_ptr<cache::CacheManager> cache_manager_;
  std::unique_ptr<ServerStats> stats_;
  std::unique_ptr<TableCatalog> table_catalog_;
  std::atomic<bool> dump_load_in_progress_{false};
  std::atomic<bool> dump_save_in_progress_{false};
  std::atomic<bool> optimization_in_progress_{false};
  std::atomic<bool> replication_paused_for_dump_{false};
  std::atomic<bool> mysql_reconnecting_{false};
  std::unique_ptr<HandlerContext> handler_ctx_;
  std::unique_ptr<FacetHandler> handler_;
  ConnectionContext conn_ctx_;
};

TEST_F(FacetHandlerCacheKeyTest, BareTableFacetSeesMutationInvalidatedByQualifiedKey) {
  ASSERT_NO_FATAL_FAILURE(AddDocumentAndInvalidate("d1", "fast car review", "vehicles"));
  ASSERT_NO_FATAL_FAILURE(AddDocumentAndInvalidate("d2", "car maintenance tips", "vehicles"));

  auto query = MakeFacetQuery(kBareTable);
  const std::string first = handler_->Handle(query, conn_ctx_);
  ASSERT_NE(first.find("vehicles\t2"), std::string::npos) << first;

  ASSERT_NO_FATAL_FAILURE(AddDocumentAndInvalidate("d3", "another car listing", "vehicles"));

  const std::string second = handler_->Handle(query, conn_ctx_);
  EXPECT_NE(second.find("vehicles\t3"), std::string::npos) << second;
}

TEST_F(FacetHandlerCacheKeyTest, QualifiedTableFacetSeesMutationInvalidatedByQualifiedKey) {
  ASSERT_NO_FATAL_FAILURE(AddDocumentAndInvalidate("d1", "fast car review", "vehicles"));
  ASSERT_NO_FATAL_FAILURE(AddDocumentAndInvalidate("d2", "car maintenance tips", "vehicles"));

  auto query = MakeFacetQuery(kQualifiedTable);
  const std::string first = handler_->Handle(query, conn_ctx_);
  ASSERT_NE(first.find("vehicles\t2"), std::string::npos) << first;

  ASSERT_NO_FATAL_FAILURE(AddDocumentAndInvalidate("d3", "another car listing", "vehicles"));

  const std::string second = handler_->Handle(query, conn_ctx_);
  EXPECT_NE(second.find("vehicles\t3"), std::string::npos) << second;
}

TEST_F(FacetHandlerCacheKeyTest, BareAndQualifiedNamesShareTheSameCacheEntry) {
  ASSERT_NO_FATAL_FAILURE(AddDocumentAndInvalidate("d1", "fast car review", "vehicles"));

  auto bare_query = MakeFacetQuery(kBareTable);
  const std::string bare_response = handler_->Handle(bare_query, conn_ctx_);
  ASSERT_NE(bare_response.find("vehicles\t1"), std::string::npos) << bare_response;

  const auto entries_after_bare = cache_manager_->GetStatistics().current_entries;
  const auto hits_after_bare = cache_manager_->GetStatistics().cache_hits;

  auto qualified_query = MakeFacetQuery(kQualifiedTable);
  const std::string qualified_response = handler_->Handle(qualified_query, conn_ctx_);
  ASSERT_NE(qualified_response.find("vehicles\t1"), std::string::npos) << qualified_response;

  EXPECT_EQ(cache_manager_->GetStatistics().current_entries, entries_after_bare);
  EXPECT_EQ(cache_manager_->GetStatistics().cache_hits, hits_after_bare + 1);
}

}  // namespace mygramdb::server
