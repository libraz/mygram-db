/**
 * @file sync_operation_manager_deadlock_test.cpp
 * @brief Tests for SyncOperationManager deadlock prevention
 *
 * These tests verify that the destructor of SyncOperationManager does not
 * deadlock when joining threads that may be holding or waiting for mutexes.
 */

#ifdef USE_MYSQL

#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <thread>

#include "config/config.h"
#include "index/index.h"
#include "server/sync_operation_manager.h"
#include "server/tcp_server.h"
#include "storage/document_store.h"

using namespace mygramdb::server;
using namespace mygramdb;

/**
 * @brief Test fixture for SyncOperationManager tests
 */
class SyncOperationManagerDeadlockTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create test table context
    auto ctx = std::make_unique<TableContext>();
    ctx->name = "test_table";
    ctx->config.name = "test_table";
    ctx->config.primary_key = "id";
    ctx->config.ngram_size = 2;
    ctx->config.kanji_ngram_size = 1;
    ctx->index = std::make_unique<index::Index>(2, 1);
    ctx->doc_store = std::make_unique<storage::DocumentStore>();

    table_contexts_ptrs_["test_table"] = ctx.get();
    table_contexts_["test_table"] = std::move(ctx);

    // Create minimal config
    config_ = std::make_unique<config::Config>();
    config_->mysql.host = "localhost";
    config_->mysql.port = 3306;
    config_->mysql.user = "test";
    config_->mysql.password = "test";
    config_->mysql.database = "testdb";
  }

  void TearDown() override {
    table_contexts_.clear();
    table_contexts_ptrs_.clear();
  }

  std::unordered_map<std::string, TableContext*> table_contexts_ptrs_;
  std::unordered_map<std::string, std::unique_ptr<TableContext>> table_contexts_;
  std::unique_ptr<config::Config> config_;
};

/**
 * @brief Test that destructor completes without deadlock
 *
 * This test creates a SyncOperationManager and immediately destroys it
 * to verify that the destructor doesn't deadlock.
 */
TEST_F(SyncOperationManagerDeadlockTest, DestructorNoDeadlock) {
  auto manager = std::make_unique<SyncOperationManager>(table_contexts_ptrs_, config_.get(), nullptr);

  // Destroy immediately - should not deadlock
  auto start = std::chrono::steady_clock::now();
  manager.reset();
  auto elapsed = std::chrono::steady_clock::now() - start;

  // Destructor should complete quickly (within 1 second)
  EXPECT_LT(std::chrono::duration_cast<std::chrono::seconds>(elapsed).count(), 1);
}

/**
 * @brief Test that destructor handles graceful shutdown correctly
 *
 * This test verifies that calling RequestShutdown before destruction
 * allows the destructor to complete without deadlock.
 */
TEST_F(SyncOperationManagerDeadlockTest, RequestShutdownBeforeDestruction) {
  auto manager = std::make_unique<SyncOperationManager>(table_contexts_ptrs_, config_.get(), nullptr);

  // Request shutdown explicitly
  manager->RequestShutdown();

  // Give a small delay to allow shutdown to propagate
  std::this_thread::sleep_for(std::chrono::milliseconds(10));

  // Destroy - should not deadlock
  auto start = std::chrono::steady_clock::now();
  manager.reset();
  auto elapsed = std::chrono::steady_clock::now() - start;

  // Destructor should complete quickly
  EXPECT_LT(std::chrono::duration_cast<std::chrono::seconds>(elapsed).count(), 1);
}

/**
 * @brief Test concurrent access patterns that could cause deadlock
 *
 * This test simulates concurrent access to the SyncOperationManager
 * to verify that the mutex locking strategy doesn't cause deadlock.
 */
TEST_F(SyncOperationManagerDeadlockTest, ConcurrentStatusChecks) {
  auto manager = std::make_unique<SyncOperationManager>(table_contexts_ptrs_, config_.get(), nullptr);

  // Start multiple threads checking status
  std::vector<std::thread> status_threads;
  std::atomic<bool> should_stop{false};

  for (int i = 0; i < 5; ++i) {
    status_threads.emplace_back([&manager, &should_stop]() {
      while (!should_stop.load()) {
        // Check status repeatedly
        std::string status = manager->GetSyncStatus();
        (void)status;  // Suppress unused variable warning
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
      }
    });
  }

  // Let threads run for a short time
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  // Signal threads to stop
  should_stop = true;

  // Join status threads
  for (auto& thread : status_threads) {
    thread.join();
  }

  // Now destroy manager - should not deadlock
  auto start = std::chrono::steady_clock::now();
  manager.reset();
  auto elapsed = std::chrono::steady_clock::now() - start;

  // Destructor should complete quickly even with concurrent access
  EXPECT_LT(std::chrono::duration_cast<std::chrono::seconds>(elapsed).count(), 1);
}

/**
 * @brief Test that RequestShutdown can be called from multiple threads
 */
TEST_F(SyncOperationManagerDeadlockTest, ConcurrentShutdownRequests) {
  auto manager = std::make_unique<SyncOperationManager>(table_contexts_ptrs_, config_.get(), nullptr);

  // Start multiple threads requesting shutdown
  std::vector<std::thread> shutdown_threads;

  for (int i = 0; i < 10; ++i) {
    shutdown_threads.emplace_back([&manager]() { manager->RequestShutdown(); });
  }

  // Join all threads
  for (auto& thread : shutdown_threads) {
    thread.join();
  }

  // Destroy manager - should not deadlock
  auto start = std::chrono::steady_clock::now();
  manager.reset();
  auto elapsed = std::chrono::steady_clock::now() - start;

  // Destructor should complete quickly
  EXPECT_LT(std::chrono::duration_cast<std::chrono::seconds>(elapsed).count(), 1);
}

/**
 * @brief Test WaitForCompletion timeout behavior
 *
 * This test verifies that WaitForCompletion returns false when
 * the timeout is reached, rather than hanging indefinitely.
 */
TEST_F(SyncOperationManagerDeadlockTest, WaitForCompletionTimeout) {
  auto manager = std::make_unique<SyncOperationManager>(table_contexts_ptrs_, config_.get(), nullptr);

  // Wait with very short timeout when nothing is running
  auto start = std::chrono::steady_clock::now();
  bool result = manager->WaitForCompletion(0);  // 0 second timeout
  auto elapsed = std::chrono::steady_clock::now() - start;

  // Should return quickly (true because nothing is running)
  EXPECT_TRUE(result);
  EXPECT_LT(std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count(), 500);

  manager.reset();
}

/**
 * @brief Test syncing tables tracking
 *
 * This test verifies that the syncing_tables_ set is properly
 * managed and thread-safe.
 */
TEST_F(SyncOperationManagerDeadlockTest, SyncingTablesTracking) {
  auto manager = std::make_unique<SyncOperationManager>(table_contexts_ptrs_, config_.get(), nullptr);

  // Initially, no tables should be syncing
  EXPECT_FALSE(manager->IsAnySyncing());
  EXPECT_TRUE(manager->GetSyncingTables().empty());

  // After manager is destroyed, it should not deadlock
  manager.reset();
}

/**
 * @brief Test GetSyncStatus uses CRLF line endings for TCP protocol compatibility
 *
 * This test verifies that GetSyncStatus returns responses with proper CRLF
 * line endings when there are multiple sync operations, preventing mixed
 * line ending issues that can cause client timeouts.
 */
TEST_F(SyncOperationManagerDeadlockTest, GetSyncStatusUsesCRLFLineEndings) {
  auto manager = std::make_unique<SyncOperationManager>(table_contexts_ptrs_, config_.get(), nullptr);

  std::string status = manager->GetSyncStatus();

  // Even for idle status, verify no bare LF (LF not preceded by CR)
  for (size_t i = 0; i < status.size(); ++i) {
    if (status[i] == '\n' && (i == 0 || status[i - 1] != '\r')) {
      FAIL() << "Found bare LF at position " << i << " in status: " << status;
    }
  }

  // Verify response does not end with trailing CRLF (SendResponse adds it)
  if (status.size() >= 2) {
    EXPECT_FALSE(status[status.size() - 2] == '\r' && status[status.size() - 1] == '\n')
        << "Response should not end with CRLF (SendResponse adds it)";
  }

  manager.reset();
}

#endif  // USE_MYSQL
