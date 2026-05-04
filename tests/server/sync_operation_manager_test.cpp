/**
 * @file sync_operation_manager_test.cpp
 * @brief Unit tests for SyncOperationManager helper APIs (non-deadlock paths)
 */

#ifdef USE_MYSQL

#include "server/sync_operation_manager.h"

#include <gtest/gtest.h>

#include <memory>
#include <unordered_map>

#include "config/config.h"
#include "index/index.h"
#include "server/server_types.h"
#include "storage/document_store.h"

namespace mygramdb::server {
namespace {

class SyncOperationManagerApiTest : public ::testing::Test {
 protected:
  void SetUp() override {
    auto ctx = std::make_unique<TableContext>();
    ctx->name = "users";
    ctx->config.name = "users";
    ctx->config.primary_key = "id";
    ctx->config.ngram_size = 2;
    ctx->config.kanji_ngram_size = 1;
    ctx->index = std::make_unique<index::Index>(2, 1);
    ctx->doc_store = std::make_unique<storage::DocumentStore>();

    table_ptrs_["users"] = ctx.get();
    table_owners_["users"] = std::move(ctx);

    config_ = std::make_unique<config::Config>();
    config_->mysql.host = "localhost";
    config_->mysql.port = 3306;
    config_->mysql.user = "test";
    config_->mysql.password = "test";
    config_->mysql.database = "testdb";

    manager_ = std::make_unique<SyncOperationManager>(table_ptrs_, config_.get(), nullptr);
  }

  void TearDown() override {
    manager_.reset();
    table_ptrs_.clear();
    table_owners_.clear();
  }

  std::unordered_map<std::string, TableContext*> table_ptrs_;
  std::unordered_map<std::string, std::unique_ptr<TableContext>> table_owners_;
  std::unique_ptr<config::Config> config_;
  std::unique_ptr<SyncOperationManager> manager_;
};

// CheckNoSyncInProgress should return success when no syncs are active.
TEST_F(SyncOperationManagerApiTest, CheckNoSyncReturnsOkWhenIdle) {
  auto result = manager_->CheckNoSyncInProgress("save dump");
  EXPECT_TRUE(result.has_value());
}

// GetSyncingTablesIfAny should report no tables when idle.
TEST_F(SyncOperationManagerApiTest, GetSyncingTablesIfAnyReturnsFalseWhenIdle) {
  std::vector<std::string> tables;
  EXPECT_FALSE(manager_->GetSyncingTablesIfAny(tables));
  EXPECT_TRUE(tables.empty());
}

// IsAnySyncing returns false on a fresh manager.
TEST_F(SyncOperationManagerApiTest, IsAnySyncingReturnsFalseWhenIdle) {
  EXPECT_FALSE(manager_->IsAnySyncing());
}

}  // namespace
}  // namespace mygramdb::server

#endif  // USE_MYSQL
