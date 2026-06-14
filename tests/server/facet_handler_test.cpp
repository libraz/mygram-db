/**
 * @file facet_handler_test.cpp
 * @brief Unit tests for FacetHandler pipeline integration
 *
 * Validates that FacetHandler now routes through ExecuteFullPipeline so
 * facet-scoped searches share synonym expansion, fuzzy matching, and
 * result caching with SearchHandler. Before this fix, FACET went through
 * search_pipeline::Execute directly and silently bypassed those features.
 */

#include "server/handlers/facet_handler.h"

#include <gtest/gtest.h>
#include <spdlog/spdlog.h>

#include <atomic>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <unordered_map>

#include "cache/cache_manager.h"
#include "cache/cache_types.h"
#include "config/config.h"
#include "index/index.h"
#include "query/query_parser.h"
#include "query/synonym_dictionary.h"
#include "server/server_stats.h"
#include "server/server_types.h"
#include "server/table_catalog.h"
#include "storage/document_store.h"

namespace mygramdb::server {

class FacetHandlerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    spdlog::set_level(spdlog::level::off);

    // Build a table with both index and document store. ngram_size=2 keeps
    // posting lists small while still exercising the n-gram pipeline.
    table_ctx_ = std::make_unique<TableContext>();
    table_ctx_->name = "articles";
    table_ctx_->config.name = "articles";
    table_ctx_->config.database = "app_db";
    table_ctx_->config.primary_key = "id";
    table_ctx_->config.ngram_size = 2;
    table_ctx_->config.kanji_ngram_size = 2;
    table_ctx_->index = std::make_unique<index::Index>(2);
    table_ctx_->doc_store = std::make_unique<storage::DocumentStore>();

    // Synonym dictionary: "car" <-> "automobile". Loaded later in the test
    // that needs it so other tests run with no synonyms (cleaner baseline).
    table_ctx_->synonym_dict = std::make_unique<query::SynonymDictionary>();

    table_contexts_["app_db.articles"] = table_ctx_.get();

    config_ = std::make_unique<config::Config>();
    config_->cache.enabled = true;
    config::TableConfig table_cfg;
    table_cfg.name = "articles";
    table_cfg.database = "app_db";
    table_cfg.primary_key = "id";
    table_cfg.ngram_size = 2;
    config_->tables.push_back(table_cfg);

    // Cache manager wired identically to production: per-table N-gram config
    // is required so cache invalidation can produce the right keys.
    cache::NgramConfigMap ngram_configs;
    ngram_configs["app_db.articles"] = cache::NgramConfig{2, 2, false};
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

  /// Add a single document to both the index and the doc store with the
  /// given normalized text and a category filter (for facet aggregation).
  storage::DocId AddDoc(const std::string& pk, const std::string& text, const std::string& category) {
    storage::FilterMap filters;
    filters["category"] = std::string(category);
    auto normalized = table_ctx_->index->NormalizeText(text);
    auto doc_id = table_ctx_->doc_store->AddDocument(pk, filters, normalized);
    EXPECT_TRUE(doc_id.has_value());
    table_ctx_->index->AddDocument(static_cast<index::DocId>(*doc_id), normalized);
    return *doc_id;
  }

  /// Write a synonym TSV file and load it into the table's synonym dictionary
  /// using the index's normalizer (matches production wiring).
  void LoadSynonymTSV(const std::string& tsv_content) {
    auto path = std::filesystem::temp_directory_path() / "facet_handler_synonyms.tsv";
    {
      std::ofstream ofs(path);
      ofs << tsv_content;
    }
    auto normalizer = [this](std::string_view sv) { return table_ctx_->index->NormalizeText(sv); };
    auto result = table_ctx_->synonym_dict->LoadFromFile(path.string(), normalizer);
    ASSERT_TRUE(result.has_value()) << "Failed to load synonym dict";
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

/**
 * @brief FACET must apply the table's synonym dictionary.
 *
 * This is the central H-1 regression test. With the old code (direct call
 * to search_pipeline::Execute), querying "automobile" returned zero results
 * because no document literally contained the substring; the synonym
 * "car" was never expanded. After the fix, FACET goes through
 * ExecuteFullPipeline which performs synonym expansion identically to
 * SearchHandler, so "automobile" matches the documents indexed under "car".
 */
TEST_F(FacetHandlerTest, FacetSearchUsesSynonymExpansion) {
  // Synonym group: "car" and "automobile" are interchangeable.
  LoadSynonymTSV("car\tautomobile\n");

  // Documents only literally contain "car"; synonyms must expand the query.
  AddDoc("d1", "fast car review", "vehicles");
  AddDoc("d2", "car maintenance tips", "vehicles");
  AddDoc("d3", "kitchen recipes", "food");

  query::Query query;
  query.type = query::QueryType::FACET;
  query.table = "app_db.articles";
  query.facet_column = "category";
  query.search_text = "automobile";

  std::string response = handler_->Handle(query, conn_ctx_);

  // Two car/automobile documents both have category=vehicles. With synonym
  // expansion, the facet response must reflect that count; without it, the
  // response is "OK FACET 0" (no matches).
  EXPECT_TRUE(response.find("OK FACET") == 0) << "Response: " << response;
  EXPECT_TRUE(response.find("vehicles") != std::string::npos) << "Response: " << response;
  EXPECT_TRUE(response.find("\t2") != std::string::npos) << "Response: " << response;
}

/**
 * @brief Sanity check: queries with no synonym match still work normally.
 *
 * Ensures the synonym path doesn't regress the no-expansion case.
 */
TEST_F(FacetHandlerTest, FacetSearchWithoutSynonymStillWorks) {
  AddDoc("d1", "fast car review", "vehicles");
  AddDoc("d2", "car maintenance tips", "vehicles");
  AddDoc("d3", "kitchen recipes", "food");

  query::Query query;
  query.type = query::QueryType::FACET;
  query.table = "app_db.articles";
  query.facet_column = "category";
  query.search_text = "car";

  std::string response = handler_->Handle(query, conn_ctx_);

  EXPECT_TRUE(response.find("OK FACET") == 0) << "Response: " << response;
  EXPECT_TRUE(response.find("vehicles") != std::string::npos) << "Response: " << response;
  EXPECT_TRUE(response.find("\t2") != std::string::npos) << "Response: " << response;
}

/**
 * @brief No-search facet path still aggregates over all docs.
 *
 * The pipeline routing only changes the has_search branch; this test pins
 * down the non-search behaviour (FACET <table> <column>) so the refactor
 * doesn't accidentally affect it.
 */
TEST_F(FacetHandlerTest, FacetWithoutSearchAggregatesAllDocs) {
  AddDoc("d1", "fast car review", "vehicles");
  AddDoc("d2", "kitchen recipes", "food");
  AddDoc("d3", "more recipes", "food");

  query::Query query;
  query.type = query::QueryType::FACET;
  query.table = "app_db.articles";
  query.facet_column = "category";
  // No search_text, no and_terms, no filters — facet over all docs.

  std::string response = handler_->Handle(query, conn_ctx_);

  EXPECT_TRUE(response.find("OK FACET") == 0) << "Response: " << response;
  EXPECT_TRUE(response.find("food") != std::string::npos) << "Response: " << response;
  EXPECT_TRUE(response.find("vehicles") != std::string::npos) << "Response: " << response;
}

}  // namespace mygramdb::server
