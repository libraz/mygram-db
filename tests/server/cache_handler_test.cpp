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
#include <memory>
#include <string>

#include "cache/cache_manager.h"
#include "config/config.h"
#include "query/query_parser.h"
#include "server/response_formatter.h"
#include "server/server_stats.h"
#include "server/server_types.h"

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
    config_->cache.enabled = true;
    cache_manager_ = std::make_unique<cache::CacheManager>(config_->cache, cache::NgramConfigMap{});

    stats_ = std::make_unique<ServerStats>();

    handler_ctx_ = std::make_unique<HandlerContext>(HandlerContext{
        .table_catalog = nullptr,
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

    handler_ = std::make_unique<CacheHandler>(*handler_ctx_);
  }

  std::unique_ptr<config::Config> config_;
  std::unique_ptr<cache::CacheManager> cache_manager_;
  std::unique_ptr<ServerStats> stats_;
  std::atomic<bool> dump_load_in_progress_{false};
  std::atomic<bool> dump_save_in_progress_{false};
  std::atomic<bool> optimization_in_progress_{false};
  std::atomic<bool> replication_paused_for_dump_{false};
  std::atomic<bool> mysql_reconnecting_{false};
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

}  // namespace mygramdb::server
