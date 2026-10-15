/**
 * @file cache_handler_test.cpp
 * @brief Unit tests for CacheHandler response formatting
 *
 * Validates that CACHE_ENABLE / CACHE_DISABLE responses are routed through
 * ResponseFormatter::FormatStatus so the framing is identical to other OK
 * status replies (e.g., CACHE_CLEARED, DEBUG_ON, DEBUG_OFF).
 */

#include "server/handlers/cache_handler.h"

#include <gtest/gtest.h>
#include <spdlog/spdlog.h>

#include <atomic>
#include <functional>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>

#include "cache/cache_manager.h"
#include "config/config.h"
#include "config/runtime_variable_manager.h"
#include "query/query_parser.h"
#include "server/handlers/variable_handler.h"
#include "server/response_formatter.h"
#include "server/server_stats.h"
#include "server/server_types.h"
#include "server/table_catalog.h"

namespace mygramdb::server {

class CacheHandlerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    spdlog::set_level(spdlog::level::off);

    // Initialize cache manager so Enable()/Disable() reach the production
    // code path. The startup-enabled flag must be true for Enable() to
    // succeed at runtime (the CacheManager only allows enabling caches that
    // were initialized at startup).
    config_ = std::make_unique<config::Config>();
    config_->replication.enable = false;  // This fixture has no MySQL reader.
    config_->cache.enabled = true;
    cache_manager_ = std::make_unique<cache::CacheManager>(config_->cache, cache::NgramConfigMap{});

    stats_ = std::make_unique<ServerStats>();
    catalog_ =
        std::make_unique<TableCatalog>(std::unordered_map<std::string, TableContext*>{{"app_db.articles", nullptr}});

    handler_ctx_ = std::make_unique<HandlerContext>(HandlerContext{
        .table_catalog = catalog_.get(),
        .stats = *stats_,
        .full_config = config_.get(),
        .dump_dir = "/tmp",
        .dump_load_in_progress = dump_load_in_progress_,
        .dump_save_in_progress = dump_save_in_progress_,
        .optimization_in_progress = optimization_in_progress_,
        .replication_paused_for_dump = replication_paused_for_dump_,
#ifdef USE_MYSQL
        .sync_manager = nullptr,
#endif
        .cache_manager = cache_manager_.get(),
        .variable_manager = nullptr,
    });

    handler_ = std::make_unique<CacheHandler>(*handler_ctx_);
  }

  std::unique_ptr<config::Config> config_;
  std::unique_ptr<cache::CacheManager> cache_manager_;
  std::unique_ptr<config::RuntimeVariableManager> variable_manager_;
  std::unique_ptr<ServerStats> stats_;
  std::unique_ptr<TableCatalog> catalog_;
  std::atomic<bool> dump_load_in_progress_{false};
  std::atomic<bool> dump_save_in_progress_{false};
  std::atomic<bool> optimization_in_progress_{false};
  std::atomic<bool> replication_paused_for_dump_{false};
  std::unique_ptr<HandlerContext> handler_ctx_;
  std::unique_ptr<CacheHandler> handler_;
  ConnectionContext conn_ctx_;
};

/**
 * @brief CACHE_ENABLE and CACHE_DISABLE must be routed through FormatStatus.
 *
 * Before the H-2 fix, these handlers built bare "OK CACHE_ENABLED" /
 * "OK CACHE_DISABLED" strings instead of going through
 * ResponseFormatter::FormatStatus. Aligning all status responses under the
 * single formatter means any future framing change (e.g., CRLF policy) only
 * has to be updated in one place.
 */
TEST_F(CacheHandlerTest, EnableDisableUsesFormatStatus) {
  // Disable first (cache starts enabled because of cache.enabled = true)
  {
    query::Query query;
    query.type = query::QueryType::CACHE_DISABLE;
    std::string response = handler_->Handle(query, conn_ctx_);
    EXPECT_EQ(response, ResponseFormatter::FormatStatus("CACHE_DISABLED")) << "Response: " << response;
    EXPECT_FALSE(cache_manager_->IsEnabled());
  }

  // Re-enable
  {
    query::Query query;
    query.type = query::QueryType::CACHE_ENABLE;
    std::string response = handler_->Handle(query, conn_ctx_);
    EXPECT_EQ(response, ResponseFormatter::FormatStatus("CACHE_ENABLED")) << "Response: " << response;
    EXPECT_TRUE(cache_manager_->IsEnabled());
  }
}

/**
 * @brief Both responses must begin with the canonical "OK " prefix.
 *
 * Lower-level invariant in case the FormatStatus helper changes shape: any
 * status reply (success or otherwise) starts with "OK " so existing CLI and
 * library parsers continue to work.
 */
TEST_F(CacheHandlerTest, EnableDisableResponsesShareOkPrefix) {
  query::Query enable_query;
  enable_query.type = query::QueryType::CACHE_ENABLE;
  std::string enable_response = handler_->Handle(enable_query, conn_ctx_);
  EXPECT_EQ(enable_response.rfind("OK ", 0), 0U) << "Response: " << enable_response;

  query::Query disable_query;
  disable_query.type = query::QueryType::CACHE_DISABLE;
  std::string disable_response = handler_->Handle(disable_query, conn_ctx_);
  EXPECT_EQ(disable_response.rfind("OK ", 0), 0U) << "Response: " << disable_response;
}

TEST_F(CacheHandlerTest, EnableDisableStayInSyncWithRuntimeVariables) {
  auto variable_manager_result = config::RuntimeVariableManager::Create(*config_);
  ASSERT_TRUE(variable_manager_result);
  variable_manager_ = std::move(*variable_manager_result);
  variable_manager_->SetCacheManager(cache_manager_.get());
  handler_ctx_->variable_manager = variable_manager_.get();
  VariableHandler variable_handler(*handler_ctx_);
  query::Query show_query;
  show_query.type = query::QueryType::SHOW_VARIABLES;
  show_query.variable_like_pattern = "cache.enabled";

  const auto expect_state = [this, &variable_handler, &show_query](bool enabled) {
    EXPECT_EQ(cache_manager_->IsEnabled(), enabled);
    EXPECT_EQ(variable_manager_->GetCurrentConfig().cache.enabled, enabled);

    const auto expected_value = enabled ? "true" : "false";
    auto current = variable_manager_->GetVariable("cache.enabled");
    ASSERT_TRUE(current);
    EXPECT_EQ(*current, expected_value);

    const auto variables = variable_manager_->GetAllVariables();
    ASSERT_NE(variables.find("cache.enabled"), variables.end());
    EXPECT_EQ(variables.at("cache.enabled").value, expected_value);

    const auto show_response = variable_handler.Handle(show_query, conn_ctx_);
    const auto name_offset = show_response.find("cache.enabled");
    ASSERT_NE(name_offset, std::string::npos) << show_response;
    const auto line_end = show_response.find_first_of("\r\n", name_offset);
    const auto row_length = line_end == std::string::npos ? std::string::npos : line_end - name_offset;
    const auto row = show_response.substr(name_offset, row_length);
    EXPECT_NE(row.find(expected_value), std::string::npos) << show_response;
  };

  query::Query disable_query;
  disable_query.type = query::QueryType::CACHE_DISABLE;
  EXPECT_EQ(handler_->Handle(disable_query, conn_ctx_), ResponseFormatter::FormatStatus("CACHE_DISABLED"));
  expect_state(false);

  query::Query enable_query;
  enable_query.type = query::QueryType::CACHE_ENABLE;
  EXPECT_EQ(handler_->Handle(enable_query, conn_ctx_), ResponseFormatter::FormatStatus("CACHE_ENABLED"));
  expect_state(true);

  auto set_disabled = variable_manager_->SetVariable("cache.enabled", "false");
  ASSERT_TRUE(set_disabled) << set_disabled.error().to_string();
  expect_state(false);
  EXPECT_EQ(handler_->Handle(enable_query, conn_ctx_), ResponseFormatter::FormatStatus("CACHE_ENABLED"));
  expect_state(true);

  auto set_enabled = variable_manager_->SetVariable("cache.enabled", "true");
  ASSERT_TRUE(set_enabled) << set_enabled.error().to_string();
  expect_state(true);
  EXPECT_EQ(handler_->Handle(disable_query, conn_ctx_), ResponseFormatter::FormatStatus("CACHE_DISABLED"));
  expect_state(false);
}

TEST_F(CacheHandlerTest, FailedEnableKeepsRuntimeVariableAndCacheDisabled) {
  cache_manager_.reset();
  config_->cache.enabled = false;
  cache_manager_ = std::make_unique<cache::CacheManager>(
      config_->cache, cache::NgramConfigMap{},
      [](std::function<void()>) -> std::thread { throw std::runtime_error("deterministic thread factory failure"); });
  handler_ctx_->cache_manager = cache_manager_.get();

  auto variable_manager_result = config::RuntimeVariableManager::Create(*config_);
  ASSERT_TRUE(variable_manager_result);
  variable_manager_ = std::move(*variable_manager_result);
  variable_manager_->SetCacheManager(cache_manager_.get());
  handler_ctx_->variable_manager = variable_manager_.get();

  query::Query query;
  query.type = query::QueryType::CACHE_ENABLE;
  const auto response = handler_->Handle(query, conn_ctx_);

  EXPECT_EQ(response,
            ResponseFormatter::FormatError("Cache cannot be enabled", mygram::utils::ErrorCode::kCacheDisabled));
  EXPECT_FALSE(cache_manager_->IsEnabled());
  EXPECT_FALSE(variable_manager_->GetCurrentConfig().cache.enabled);

  auto current = variable_manager_->GetVariable("cache.enabled");
  ASSERT_TRUE(current);
  EXPECT_EQ(*current, "false");

  const auto variables = variable_manager_->GetAllVariables();
  ASSERT_NE(variables.find("cache.enabled"), variables.end());
  EXPECT_EQ(variables.at("cache.enabled").value, "false");
}

TEST_F(CacheHandlerTest, StatsExposeAccountedMemoryAndRejectionReasons) {
  query::Query query;
  query.type = query::QueryType::CACHE_STATS;

  const std::string response = handler_->Handle(query, conn_ctx_);

  EXPECT_NE(response.find("current_memory_bytes:"), std::string::npos);
  EXPECT_NE(response.find("invalidation_index_memory_bytes:"), std::string::npos);
  EXPECT_NE(response.find("invalidation_queue_memory_bytes:"), std::string::npos);
  EXPECT_NE(response.find("accounted_memory_bytes:"), std::string::npos);
  EXPECT_NE(response.find("rejection_count:"), std::string::npos);
  EXPECT_NE(response.find("rejection_oversize:"), std::string::npos);
  EXPECT_NE(response.find("rejection_memory_budget:"), std::string::npos);
  EXPECT_NE(response.find("rejection_duplicate:"), std::string::npos);
  EXPECT_NE(response.find("stale_entry_removals:"), std::string::npos);
  EXPECT_NE(response.find("decompression_failures:"), std::string::npos);
  EXPECT_NE(response.find("stale_lru_entries:"), std::string::npos);
}

TEST_F(CacheHandlerTest, ClearRejectsUnknownTableAndCanonicalizesKnownTable) {
  query::Query unknown;
  unknown.type = query::QueryType::CACHE_CLEAR;
  unknown.table = "missing";
  EXPECT_EQ(handler_->Handle(unknown, conn_ctx_),
            ResponseFormatter::FormatError("Table not found or ambiguous: missing",
                                           mygram::utils::ErrorCode::kTableNotFound));

  query::Query known;
  known.type = query::QueryType::CACHE_CLEAR;
  known.table = "articles";
  EXPECT_EQ(handler_->Handle(known, conn_ctx_), ResponseFormatter::FormatStatus("CACHE_CLEARED table=app_db.articles"));
}

TEST_F(CacheHandlerTest, ClearRejectsRequestsWhileCacheIsDisabled) {
  cache_manager_->Disable();

  query::Query clear_all;
  clear_all.type = query::QueryType::CACHE_CLEAR;
  EXPECT_EQ(handler_->Handle(clear_all, conn_ctx_),
            ResponseFormatter::FormatError("Cache is disabled", mygram::utils::ErrorCode::kCacheDisabled));

  query::Query clear_table;
  clear_table.type = query::QueryType::CACHE_CLEAR;
  clear_table.table = "articles";
  EXPECT_EQ(handler_->Handle(clear_table, conn_ctx_),
            ResponseFormatter::FormatError("Cache is disabled", mygram::utils::ErrorCode::kCacheDisabled));
}

}  // namespace mygramdb::server
