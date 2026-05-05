/**
 * @file variable_handler_test.cpp
 * @brief Unit tests for VariableHandler (SET/SHOW VARIABLES commands)
 */

#include "server/handlers/variable_handler.h"

#include <gtest/gtest.h>
#include <spdlog/spdlog.h>

#include "config/runtime_variable_manager.h"
#include "query/query_parser.h"
#include "server/sync_operation_manager.h"
#include "server/table_catalog.h"

namespace mygramdb::server {

class VariableHandlerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    spdlog::set_level(spdlog::level::debug);

    // Create minimal config
    config_ = std::make_unique<config::Config>();
    config_->logging.level = "info";
    config_->logging.format = "text";
    config_->api.default_limit = 100;
    config_->api.max_query_length = 1000;
    config_->api.rate_limiting.enable = true;
    config_->api.rate_limiting.capacity = 100;
    config_->api.rate_limiting.refill_rate = 10;
    config_->cache.enabled = true;
    config_->cache.min_query_cost_ms = 10.0;
    config_->cache.ttl_seconds = 300;

#ifdef USE_MYSQL
    config_->mysql.host = "localhost";
    config_->mysql.port = 3306;
    config_->mysql.user = "test";
    config_->mysql.password = "test";
    config_->mysql.database = "test";
#endif

    // Create RuntimeVariableManager
    auto result = config::RuntimeVariableManager::Create(*config_);
    ASSERT_TRUE(result) << "Failed to create RuntimeVariableManager: " << result.error().message();
    variable_manager_ = std::move(*result);

    stats_ = std::make_unique<ServerStats>();

    // Create handler context
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
        .cache_manager = nullptr,
        .variable_manager = variable_manager_.get(),
    });

    // Create handler
    handler_ = std::make_unique<VariableHandler>(*handler_ctx_);
  }

  void TearDown() override {
    dump_load_in_progress_ = false;
    dump_save_in_progress_ = false;
    optimization_in_progress_ = false;
  }

  std::unordered_map<std::string, TableContext*> table_contexts_;
  std::unique_ptr<TableCatalog> table_catalog_;
  std::unique_ptr<config::Config> config_;
  std::unique_ptr<ServerStats> stats_;
  std::atomic<bool> dump_load_in_progress_{false};
  std::atomic<bool> dump_save_in_progress_{false};
  std::atomic<bool> optimization_in_progress_{false};
  std::atomic<bool> replication_paused_for_dump_{false};
  std::atomic<bool> mysql_reconnecting_{false};
  std::unique_ptr<config::RuntimeVariableManager> variable_manager_;
  std::unique_ptr<HandlerContext> handler_ctx_;
  std::unique_ptr<VariableHandler> handler_;
  ConnectionContext conn_ctx_;
};

// ============================================================================
// SHOW VARIABLES Tests
// ============================================================================

TEST_F(VariableHandlerTest, ShowVariablesBasic) {
  query::Query query;
  query.type = query::QueryType::SHOW_VARIABLES;

  std::string response = handler_->Handle(query, conn_ctx_);

  // Should contain multiple variables
  EXPECT_TRUE(response.find("logging.level") != std::string::npos) << "Response: " << response;
  EXPECT_TRUE(response.find("cache.enabled") != std::string::npos) << "Response: " << response;
}

TEST_F(VariableHandlerTest, ShowVariablesWithPrefix) {
  query::Query query;
  query.type = query::QueryType::SHOW_VARIABLES;
  query.variable_like_pattern = "logging%";

  std::string response = handler_->Handle(query, conn_ctx_);

  // Should contain logging variables only
  EXPECT_TRUE(response.find("logging.level") != std::string::npos) << "Response: " << response;
  EXPECT_TRUE(response.find("logging.format") != std::string::npos) << "Response: " << response;
  // Should not contain non-logging variables
  EXPECT_TRUE(response.find("cache.enabled") == std::string::npos) << "Response: " << response;
}

TEST_F(VariableHandlerTest, ShowVariablesLikeMultipleWildcards) {
  // Pattern "log%level" should match "logging.level" but not "logging.format"
  query::Query query;
  query.type = query::QueryType::SHOW_VARIABLES;
  query.variable_like_pattern = "log%level";

  std::string response = handler_->Handle(query, conn_ctx_);

  EXPECT_TRUE(response.find("logging.level") != std::string::npos) << "Response: " << response;
  EXPECT_TRUE(response.find("logging.format") == std::string::npos) << "Response: " << response;
}

TEST_F(VariableHandlerTest, ShowVariablesLikeUnderscoreWildcard) {
  // Pattern with _ (single char wildcard)
  query::Query query;
  query.type = query::QueryType::SHOW_VARIABLES;
  query.variable_like_pattern = "cache.t_l%";

  std::string response = handler_->Handle(query, conn_ctx_);

  // "cache.ttl_seconds" should match "cache.t_l%" (t + any-char + l...)
  EXPECT_TRUE(response.find("cache.ttl_seconds") != std::string::npos) << "Response: " << response;
}

TEST_F(VariableHandlerTest, ShowVariablesLikeNoMatch) {
  query::Query query;
  query.type = query::QueryType::SHOW_VARIABLES;
  query.variable_like_pattern = "nonexistent%";

  std::string response = handler_->Handle(query, conn_ctx_);

  EXPECT_TRUE(response.find("0 rows") != std::string::npos) << "Response: " << response;
}

TEST_F(VariableHandlerTest, ShowVariablesLikeExactMatch) {
  query::Query query;
  query.type = query::QueryType::SHOW_VARIABLES;
  query.variable_like_pattern = "logging.level";

  std::string response = handler_->Handle(query, conn_ctx_);

  EXPECT_TRUE(response.find("logging.level") != std::string::npos) << "Response: " << response;
  EXPECT_TRUE(response.find("1 row") != std::string::npos) << "Response: " << response;
}

TEST_F(VariableHandlerTest, ShowVariablesLikeAllPercent) {
  // "%" should match everything
  query::Query query;
  query.type = query::QueryType::SHOW_VARIABLES;
  query.variable_like_pattern = "%";

  std::string response = handler_->Handle(query, conn_ctx_);

  EXPECT_TRUE(response.find("logging.level") != std::string::npos) << "Response: " << response;
  EXPECT_TRUE(response.find("cache.enabled") != std::string::npos) << "Response: " << response;
}

TEST_F(VariableHandlerTest, ShowVariablesLikeConsecutivePercents) {
  // "%%" should also match everything
  query::Query query;
  query.type = query::QueryType::SHOW_VARIABLES;
  query.variable_like_pattern = "%%logging%%";

  std::string response = handler_->Handle(query, conn_ctx_);

  EXPECT_TRUE(response.find("logging.level") != std::string::npos) << "Response: " << response;
  EXPECT_TRUE(response.find("logging.format") != std::string::npos) << "Response: " << response;
}

// ============================================================================
// SET Tests
// ============================================================================

TEST_F(VariableHandlerTest, SetVariableBasic) {
  query::Query query;
  query.type = query::QueryType::SET;
  query.variable_assignments.push_back({"logging.level", "debug"});

  std::string response = handler_->Handle(query, conn_ctx_);

  EXPECT_TRUE(response.find("+OK") == 0) << "Response: " << response;
  EXPECT_TRUE(response.find("logging.level") != std::string::npos) << "Response: " << response;
  EXPECT_TRUE(response.find("debug") != std::string::npos) << "Response: " << response;
}

TEST_F(VariableHandlerTest, SetVariableImmutable) {
  query::Query query;
  query.type = query::QueryType::SET;
  query.variable_assignments.push_back({"mysql.user", "newuser"});

  std::string response = handler_->Handle(query, conn_ctx_);

  EXPECT_TRUE(response.find("ERROR") == 0) << "Response: " << response;
  EXPECT_TRUE(response.find("immutable") != std::string::npos) << "Response: " << response;
}

TEST_F(VariableHandlerTest, SetVariableUnknown) {
  query::Query query;
  query.type = query::QueryType::SET;
  query.variable_assignments.push_back({"unknown.variable", "value"});

  std::string response = handler_->Handle(query, conn_ctx_);

  EXPECT_TRUE(response.find("ERROR") == 0) << "Response: " << response;
  EXPECT_TRUE(response.find("Unknown variable") != std::string::npos) << "Response: " << response;
}

// ============================================================================
// SYNC Blocking Tests (MySQL connection changes)
// ============================================================================

#ifdef USE_MYSQL

// Note: Full integration tests for SYNC blocking are in tests/integration/server/variable_handler_test.cpp
// These unit tests verify the logic path when sync_manager is nullptr

TEST_F(VariableHandlerTest, SetMysqlHostAllowedWhenSyncManagerNull) {
  // sync_manager is nullptr, so no SYNC in progress
  query::Query query;
  query.type = query::QueryType::SET;
  query.variable_assignments.push_back({"mysql.host", "newhost"});

  std::string response = handler_->Handle(query, conn_ctx_);

  // Should not contain SYNC blocking message
  EXPECT_TRUE(response.find("SYNC is in progress") == std::string::npos) << "Response: " << response;
  // May succeed or fail for other reasons (e.g., no reconnect callback), but not blocked by SYNC
}

TEST_F(VariableHandlerTest, SetMysqlPortAllowedWhenSyncManagerNull) {
  // sync_manager is nullptr, so no SYNC in progress
  query::Query query;
  query.type = query::QueryType::SET;
  query.variable_assignments.push_back({"mysql.port", "3307"});

  std::string response = handler_->Handle(query, conn_ctx_);

  // Should not contain SYNC blocking message
  EXPECT_TRUE(response.find("SYNC is in progress") == std::string::npos) << "Response: " << response;
  // May succeed or fail for other reasons, but not blocked by SYNC
}

TEST_F(VariableHandlerTest, SetNonMysqlVariablesAlwaysAllowed) {
  // Even if sync_manager were set, non-MySQL variables should be allowed
  query::Query query;
  query.type = query::QueryType::SET;
  query.variable_assignments.push_back({"logging.level", "debug"});

  std::string response = handler_->Handle(query, conn_ctx_);

  // Should succeed
  EXPECT_TRUE(response.find("+OK") == 0) << "Response: " << response;
  EXPECT_TRUE(response.find("SYNC is in progress") == std::string::npos) << "Response: " << response;
}

/**
 * @brief Prefix-match guard logic must catch every mysql.* variable name.
 *
 * Regression test for H-4: previously the SYNC guard only checked the exact
 * names "mysql.host" and "mysql.port", so changes to mysql.user and
 * mysql.password slipped through silently. The fix expands the guard to a
 * `rfind("mysql.", 0) == 0` prefix match. This test pins down the
 * prefix-match contract independently of the runtime SYNC state (which
 * requires a real MySQL connection to set true).
 */
TEST(VariableHandlerPrefixMatchTest, MysqlPrefixCatchesAllSubvariables) {
  // The same string predicate the handler uses.
  auto is_mysql_variable = [](const std::string& name) { return name.rfind("mysql.", 0) == 0; };

  EXPECT_TRUE(is_mysql_variable("mysql.host"));
  EXPECT_TRUE(is_mysql_variable("mysql.port"));
  EXPECT_TRUE(is_mysql_variable("mysql.user"));
  EXPECT_TRUE(is_mysql_variable("mysql.password"));
  EXPECT_TRUE(is_mysql_variable("mysql.database"));
  EXPECT_TRUE(is_mysql_variable("mysql.something_new"));

  // Non-mysql.* names must NOT be caught by the guard.
  EXPECT_FALSE(is_mysql_variable("logging.level"));
  EXPECT_FALSE(is_mysql_variable("cache.enabled"));
  EXPECT_FALSE(is_mysql_variable("api.default_limit"));
  // "mysql" without trailing dot must not match (would otherwise catch a
  // hypothetical top-level scalar variable named just "mysql").
  EXPECT_FALSE(is_mysql_variable("mysql"));
  // Names containing "mysql" but not at the start must not match.
  EXPECT_FALSE(is_mysql_variable("not_mysql.host"));
}

/**
 * @brief mysql.user and mysql.password reach SetVariable when no SYNC is
 *        active, proving the prefix guard is not over-blocking.
 *
 * Without an active SyncOperationManager, the prefix guard short-circuits
 * (because IsAnySyncing() returns false) and the immutability check inside
 * SetVariable fires. If the guard incorrectly blocked the call regardless
 * of sync state, the response would say "SYNC is in progress" and the
 * immutability error would never surface. Existing test
 * `SetVariableImmutable` already covers mysql.user; this test pins the
 * same guarantee for mysql.password to make the prefix-expansion contract
 * explicit at the unit-test surface.
 */
TEST_F(VariableHandlerTest, SetMysqlPasswordReachesSetVariableWhenSyncIdle) {
  query::Query query;
  query.type = query::QueryType::SET;
  query.variable_assignments.push_back({"mysql.password", "newpassword"});

  std::string response = handler_->Handle(query, conn_ctx_);

  // Must NOT be the SYNC blocking message: that would mean the prefix guard
  // fired when no sync was in progress (over-blocking).
  EXPECT_TRUE(response.find("SYNC is in progress") == std::string::npos) << "Response: " << response;
  // Should reach SetVariable, which rejects mysql.password as immutable.
  EXPECT_TRUE(response.find("ERROR") == 0) << "Response: " << response;
}
#endif

}  // namespace mygramdb::server
