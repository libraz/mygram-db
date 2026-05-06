/**
 * @file admin_handler_test.cpp
 * @brief Unit tests for AdminHandler defensive guards
 *
 * Validates that AdminHandler::Handle returns a clear ERROR response when
 * required dependencies (e.g., the table catalog) are null, instead of
 * crashing.
 */

#include "server/handlers/admin_handler.h"

#include <gtest/gtest.h>
#include <spdlog/spdlog.h>

#include <atomic>
#include <memory>
#include <string>

#include "config/config.h"
#include "query/query_parser.h"
#include "server/server_stats.h"
#include "server/server_types.h"

namespace mygramdb::server {

class AdminHandlerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    spdlog::set_level(spdlog::level::off);

    config_ = std::make_unique<config::Config>();
    stats_ = std::make_unique<ServerStats>();

    // Construct a HandlerContext with table_catalog == nullptr to verify
    // AdminHandler short-circuits with a clear error instead of segfaulting.
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
        .cache_manager = nullptr,
        .variable_manager = nullptr,
    });
  }

  std::unique_ptr<config::Config> config_;
  std::unique_ptr<ServerStats> stats_;
  std::atomic<bool> dump_load_in_progress_{false};
  std::atomic<bool> dump_save_in_progress_{false};
  std::atomic<bool> optimization_in_progress_{false};
  std::atomic<bool> replication_paused_for_dump_{false};
  std::atomic<bool> mysql_reconnecting_{false};
  std::unique_ptr<HandlerContext> handler_ctx_;
  ConnectionContext conn_ctx_;
};

/**
 * @brief Handle should return an ERROR response when table_catalog is null.
 *
 * Before the defensive guard was added, AdminHandler::Handle dereferenced
 * ctx_.table_catalog unconditionally, so a null catalog crashed the process.
 * The fix returns a formatted ERROR instead of segfaulting.
 */
TEST_F(AdminHandlerTest, HandleReturnsErrorWhenCatalogNull) {
  AdminHandler handler(*handler_ctx_);

  query::Query query;
  query.type = query::QueryType::INFO;

  std::string response = handler.Handle(query, conn_ctx_);

  EXPECT_TRUE(response.find("ERROR") == 0) << "Response: " << response;
  EXPECT_TRUE(response.find("Table catalog not initialized") != std::string::npos) << "Response: " << response;
}

/**
 * @brief CONFIG_HELP must NOT be blocked by the table_catalog guard.
 *
 * CONFIG HELP/SHOW/VERIFY operate purely on the loaded config — they do not
 * inspect the catalog. The original ConfigHandlerTest fixture relies on this
 * by constructing AdminHandler with table_catalog=nullptr. Scoping the guard
 * to INFO (the only command that dereferences the catalog) keeps that contract
 * intact while still preventing the segfault that prompted the guard.
 */
TEST_F(AdminHandlerTest, HandleConfigHelpWorksWhenCatalogNull) {
  AdminHandler handler(*handler_ctx_);

  query::Query query;
  query.type = query::QueryType::CONFIG_HELP;
  query.filepath = "";

  std::string response = handler.Handle(query, conn_ctx_);

  // Should NOT hit the catalog-null guard; should produce a normal +OK response.
  EXPECT_NE(response.find("+OK"), std::string::npos) << "Response: " << response;
  EXPECT_EQ(response.find("Table catalog not initialized"), std::string::npos) << "Response: " << response;
}

}  // namespace mygramdb::server
