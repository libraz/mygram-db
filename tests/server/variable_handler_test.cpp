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
    handler_ctx_ = std::make_unique<HandlerContext>(HandlerContext{
        .table_contexts = table_contexts_,
        .stats = *stats_,
        .full_config = config_.get(),
        .dump_dir = "/tmp",
        .loading = loading_,
        .read_only = read_only_,
        .optimization_in_progress = optimization_in_progress_,
#ifdef USE_MYSQL
        .binlog_reader = nullptr,
        .sync_manager = nullptr,
#else
        .binlog_reader = nullptr,
#endif
        .cache_manager = nullptr,
        .variable_manager = variable_manager_.get(),
    });

    // Create handler
    handler_ = std::make_unique<VariableHandler>(*handler_ctx_);
  }

  void TearDown() override {
    loading_ = false;
    read_only_ = false;
    optimization_in_progress_ = false;
  }

  std::unordered_map<std::string, TableContext*> table_contexts_;
  std::unique_ptr<config::Config> config_;
  std::unique_ptr<ServerStats> stats_;
  std::atomic<bool> loading_{false};
  std::atomic<bool> read_only_{false};
  std::atomic<bool> optimization_in_progress_{false};
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

  EXPECT_TRUE(response.find("-ERR") == 0) << "Response: " << response;
  EXPECT_TRUE(response.find("immutable") != std::string::npos) << "Response: " << response;
}

TEST_F(VariableHandlerTest, SetVariableUnknown) {
  query::Query query;
  query.type = query::QueryType::SET;
  query.variable_assignments.push_back({"unknown.variable", "value"});

  std::string response = handler_->Handle(query, conn_ctx_);

  EXPECT_TRUE(response.find("-ERR") == 0) << "Response: " << response;
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
#endif

}  // namespace mygramdb::server
