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
#include "config/runtime_variable_manager.h"
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
    config_->replication.server_id = 123;
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

// StartSync claims operation_coordinator_'s kSync token (Step 1) before it
// publishes the table into syncing_tables_ (Step 3). CheckNoSyncInProgress
// must see the conflict from the token alone, not just from syncing_tables_,
// or a caller landing in that window gets past this check and only fails
// later with the generic OperationCoordinator busy error instead of
// kSyncAlreadyInProgress.
TEST_F(SyncOperationManagerApiTest, CheckNoSyncInProgressSeesCoordinatorTokenBeforeSyncingTablesPublish) {
  auto token = manager_->GetOperationCoordinator().TryAcquire(LongOperation::kSync, "users");
  ASSERT_TRUE(token.has_value());

  std::vector<std::string> syncing_tables;
  ASSERT_FALSE(manager_->GetSyncingTablesIfAny(syncing_tables))
      << "the token is held directly here, bypassing StartSync's syncing_tables_ insert";

  auto result = manager_->CheckNoSyncInProgress("save dump");
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kSyncAlreadyInProgress);
  EXPECT_NE(result.error().message().find("users"), std::string::npos);
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

TEST_F(SyncOperationManagerApiTest, CurrentConfigSnapshotUsesRuntimeConfigurationWithoutMutableMysqlEndpoints) {
  auto variable_manager_result = config::RuntimeVariableManager::Create(*config_);
  ASSERT_TRUE(variable_manager_result.has_value());
  auto variable_manager = std::move(*variable_manager_result);

  manager_->SetCurrentConfigProvider(
      [variable_manager_ptr = variable_manager.get()] { return variable_manager_ptr->GetCurrentConfig(); });

  ASSERT_TRUE(variable_manager->SetVariable("logging.level", "debug").has_value());
  EXPECT_FALSE(variable_manager->SetVariable("mysql.host", "new-primary.internal").has_value());
  EXPECT_FALSE(variable_manager->SetVariable("mysql.port", "4407").has_value());

  // The startup object deliberately remains stale. A SYNC snapshot must come
  // from RuntimeVariableManager, while MySQL endpoints remain startup-only.
  EXPECT_EQ(config_->mysql.host, "localhost");
  EXPECT_EQ(config_->mysql.port, 3306);
  EXPECT_EQ(config_->logging.level, "info");

  const auto current_config = manager_->GetCurrentConfigSnapshotForTest();
  ASSERT_TRUE(current_config.has_value());
  EXPECT_EQ(current_config->mysql.host, "localhost");
  EXPECT_EQ(current_config->mysql.port, 3306);
  EXPECT_EQ(current_config->mysql.user, "test");
  EXPECT_EQ(current_config->mysql.database, "testdb");
  EXPECT_EQ(current_config->logging.level, "debug");
}

TEST_F(SyncOperationManagerApiTest, ScopedLoaderRegistrationUnregistersDuringExceptionUnwind) {
  EXPECT_EQ(manager_->ActiveLoaderCountForTest(), 0U);

  try {
    auto registration = manager_->RegisterNullLoaderScopedForTest("users");
    EXPECT_EQ(manager_->ActiveLoaderCountForTest(), 1U);
    throw std::runtime_error("injected loader failure");
  } catch (const std::runtime_error&) {
  }

  EXPECT_EQ(manager_->ActiveLoaderCountForTest(), 0U);
  manager_->RequestShutdown();
}

TEST_F(SyncOperationManagerApiTest, ActiveSyncStatusUsesClientCompletableFrame) {
  auto start_result = manager_->StartSync("users");
  ASSERT_TRUE(start_result.has_value());
  EXPECT_EQ(*start_result, "OK SYNC STARTED table=users");
  EXPECT_EQ(start_result->find("job_id"), std::string::npos);

  const std::string status = manager_->GetSyncStatus();
  EXPECT_TRUE(status.rfind("OK SYNC_STATUS\r\n", 0) == 0) << status;
  // The transport appends one CRLF to every response body.
  auto frame = client::detail::BeginResponseFrame("SYNC STATUS", /*debug_mode=*/false);
  EXPECT_TRUE(client::detail::IsResponseComplete(status + "\r\n", frame)) << status;

  manager_->RequestShutdown();
  EXPECT_TRUE(manager_->WaitForCompletion(/*timeout_sec=*/30));
}

TEST_F(SyncOperationManagerApiTest, CompletedSyncStatusKeepsItsCompletionDuration) {
  using Clock = std::chrono::steady_clock;
  const auto start = Clock::now() - std::chrono::seconds(3);
  const auto end = start + std::chrono::milliseconds(1500);
  manager_->SetTerminalSyncStateForTest("users", "COMPLETED", start, end, 7);

  const auto read_elapsed = [this]() {
    const std::string status = manager_->GetSyncStatus();
    const auto begin = status.find(" time=");
    EXPECT_NE(begin, std::string::npos) << status;
    if (begin == std::string::npos) {
      return std::string{};
    }
    const auto value_begin = begin + std::string(" time=").size();
    const auto value_end = status.find('s', value_begin);
    EXPECT_NE(value_end, std::string::npos) << status;
    return status.substr(value_begin, value_end - value_begin);
  };

  const std::string first = read_elapsed();
  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  const std::string second = read_elapsed();

  EXPECT_EQ(first, "1.5");
  EXPECT_EQ(second, first);
}

TEST_F(SyncOperationManagerApiTest, StopSyncSignalsCancellationWithoutJoiningWorker) {
  constexpr auto kWorkerDuration = std::chrono::milliseconds(500);
  manager_->InstallSyncThreadForTest("users",
                                     std::thread([] { std::this_thread::sleep_for(std::chrono::milliseconds(500)); }));

  const auto start = std::chrono::steady_clock::now();
  const std::string response = manager_->StopSync("users");
  const auto elapsed = std::chrono::steady_clock::now() - start;

  EXPECT_NE(response.find("SYNC CANCELLATION REQUESTED table=users"), std::string::npos) << response;
  EXPECT_LT(elapsed, std::chrono::milliseconds(200))
      << "StopSync joined the worker on the request thread instead of returning after signalling";
  const std::string status = manager_->GetSyncStatus();
  EXPECT_NE(status.find("status=CANCELLING"), std::string::npos) << status;

  manager_->ClearSyncingTableForTest("users");
  EXPECT_TRUE(manager_->WaitForCompletion(/*timeout_sec=*/2));
}

/** Race eight StartSync calls while the winning worker holds the coordinator. */
TEST_F(SyncOperationManagerApiTest, ConcurrentStartSyncIsRaceFree) {
  constexpr int kThreadCount = 8;

  std::atomic<int> ready{0};
  std::atomic<bool> go{false};
  std::atomic<int> config_provider_calls{0};
  std::atomic<int> worker_starts{0};
  std::promise<void> worker_entered_promise;
  auto worker_entered = worker_entered_promise.get_future();
  std::promise<void> release_worker_promise;
  const auto release_worker = release_worker_promise.get_future().share();
  manager_->SetCurrentConfigProvider(
      [this, &config_provider_calls, &worker_starts, &worker_entered_promise, &release_worker]() {
        const int call = config_provider_calls.fetch_add(1, std::memory_order_relaxed);
        if (call == 1) {
          worker_starts.fetch_add(1, std::memory_order_relaxed);
          worker_entered_promise.set_value();
          release_worker.wait();
        }
        return *config_;
      });

  std::atomic<int> success_count{0};
  std::atomic<int> already_running_count{0};
  std::vector<std::future<bool>> futures;
  futures.reserve(kThreadCount);

  for (int i = 0; i < kThreadCount; ++i) {
    futures.push_back(std::async(std::launch::async, [this, &ready, &go, &success_count, &already_running_count]() {
      ready.fetch_add(1, std::memory_order_release);
      while (!go.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
      auto r = manager_->StartSync("users");
      if (r.has_value()) {
        success_count.fetch_add(1, std::memory_order_relaxed);
      } else if (r.error().code() == mygram::utils::ErrorCode::kSyncAlreadyInProgress) {
        already_running_count.fetch_add(1, std::memory_order_relaxed);
      }
      return r.has_value();
    }));
  }

  while (ready.load(std::memory_order_acquire) < kThreadCount) {
    std::this_thread::yield();
  }
  go.store(true, std::memory_order_release);

  const bool worker_held = worker_entered.wait_for(std::chrono::seconds(2)) == std::future_status::ready;
  bool release_sent = false;
  if (!worker_held) {
    release_worker_promise.set_value();
    release_sent = true;
  }

  int completed = 0;
  for (auto& f : futures) {
    f.get();
    ++completed;
  }

  EXPECT_TRUE(worker_held) << "the winning SYNC worker did not reach the test gate";
  EXPECT_EQ(worker_starts.load(), 1);
  EXPECT_EQ(success_count.load(), 1) << "exactly one caller must start while its worker holds the slot";
  EXPECT_EQ(already_running_count.load(), kThreadCount - 1);
  EXPECT_EQ(completed, kThreadCount);

  if (!release_sent) {
    release_worker_promise.set_value();
  }
  manager_->RequestShutdown();
  EXPECT_TRUE(manager_->WaitForCompletion(/*timeout_sec=*/30));
}

}  // namespace
}  // namespace mygramdb::server

#endif  // USE_MYSQL
