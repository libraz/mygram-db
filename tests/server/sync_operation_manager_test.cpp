/**
 * @file sync_operation_manager_test.cpp
 * @brief Unit tests for SyncOperationManager helper APIs (non-deadlock paths)
 */

#ifdef USE_MYSQL

#include "server/sync_operation_manager.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <thread>
#include <unordered_map>
#include <vector>

#include "client/protocol_detection.h"
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

TEST_F(SyncOperationManagerApiTest, ActiveSyncStatusUsesClientCompletableFrame) {
  auto start_result = manager_->StartSync("users");
  ASSERT_TRUE(start_result.has_value());

  const std::string status = manager_->GetSyncStatus();
  EXPECT_TRUE(status.rfind("OK SYNC_STATUS\r\n", 0) == 0) << status;
  EXPECT_TRUE(client::detail::IsResponseComplete(status)) << status;

  manager_->RequestShutdown();
  EXPECT_TRUE(manager_->WaitForCompletion(/*timeout_sec=*/30));
}

/**
 * @brief P0-D regression: concurrent StartSync calls for the same table must
 *        be serialized.
 *
 * Pre-fix StartSync had this sequence under sync_mutex_:
 *   1. mark sync_states_[t].is_running = true
 *   2. move out the previous std::thread for table t
 *   3. unlock sync_mutex_
 *   4. join the previous thread
 *   5. relock sync_mutex_
 *   6. spawn the new std::thread
 *
 * During the unlock window, sync_states_[t].is_running was true but
 * sync_threads_ no longer contained the table - a partially-modified state
 * that another StartSync/StopSync racing in could observe. The fix uses a
 * "JOINING_PREVIOUS" status flag and re-validates state after the join.
 *
 * This test spawns several threads each calling StartSync("users") at the
 * same time. With the fix, exactly one of them wins (returns OK or a
 * MySQL-connection failure error from the spawned thread; both are fine
 * from this manager's point of view), and all other concurrent callers
 * receive an "already in progress" error rather than racing into a
 * duplicate thread launch or a deadlock.
 *
 * NOTE: BuildSnapshotAsync attempts a real MySQL connection that will fail
 * (no MySQL is available in unit tests). That is fine - the worker thread
 * will set state to FAILED and exit. The point of this test is the
 * StartSync return values from the racing callers, not the snapshot
 * outcome.
 */
TEST_F(SyncOperationManagerApiTest, ConcurrentStartSyncIsRaceFree) {
  constexpr int kThreadCount = 8;

  std::atomic<int> ready{0};
  std::atomic<bool> go{false};
  std::vector<std::future<bool>> futures;
  futures.reserve(kThreadCount);

  for (int i = 0; i < kThreadCount; ++i) {
    futures.push_back(std::async(std::launch::async, [this, &ready, &go]() {
      ready.fetch_add(1, std::memory_order_release);
      while (!go.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
      auto r = manager_->StartSync("users");
      return r.has_value();
    }));
  }

  while (ready.load(std::memory_order_acquire) < kThreadCount) {
    std::this_thread::yield();
  }
  go.store(true, std::memory_order_release);

  int success_count = 0;
  int rejected_count = 0;
  for (auto& f : futures) {
    if (f.get()) {
      ++success_count;
    } else {
      ++rejected_count;
    }
  }

  // Exactly one StartSync should succeed; all others must report that the
  // sync is already in progress (or that the slot is being joined).
  EXPECT_EQ(success_count, 1) << "Only one concurrent StartSync should win the slot";
  EXPECT_EQ(rejected_count, kThreadCount - 1) << "Other concurrent StartSync calls should be cleanly rejected";

  // After the race resolves, the manager should agree that exactly one
  // sync is active (or recently active) for "users". We allow either
  // is_running=true (worker still trying to connect) or is_running=false
  // (worker already gave up on MySQL connection failure); either way, no
  // OTHER table has spuriously become syncing, and there is no deadlock.
  EXPECT_LE(manager_->GetSyncingTables().size(), static_cast<size_t>(1))
      << "Only 'users' should ever appear in syncing_tables_";

  // Wait for the background sync thread to terminate so the destructor's
  // shutdown path is exercised cleanly. WaitForCompletion returns once the
  // worker (which is failing to connect to MySQL) exits.
  manager_->RequestShutdown();
  EXPECT_TRUE(manager_->WaitForCompletion(/*timeout_sec=*/30))
      << "Sync worker should exit within timeout (likely via MySQL connection failure)";
}

}  // namespace
}  // namespace mygramdb::server

#endif  // USE_MYSQL
