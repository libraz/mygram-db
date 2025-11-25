/**
 * @file signal_manager_test.cpp
 * @brief Tests for SignalManager SIGUSR1 log rotation support
 */

#include "app/signal_manager.h"

#include <gtest/gtest.h>

#include <chrono>
#include <csignal>
#include <thread>

namespace mygramdb::app {
namespace {

class SignalManagerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Reset signal flags before each test
    SignalManager::signal_flags_.shutdown_requested = 0;
    SignalManager::signal_flags_.log_reopen_requested = 0;
  }
};

TEST_F(SignalManagerTest, CreateSucceeds) {
  auto result = SignalManager::Create();
  ASSERT_TRUE(result.has_value());
  EXPECT_NE(result.value(), nullptr);
}

TEST_F(SignalManagerTest, LogReopenInitiallyFalse) {
  auto signal_mgr = SignalManager::Create();
  ASSERT_TRUE(signal_mgr.has_value());

  // Initially should be false
  EXPECT_FALSE(SignalManager::ConsumeLogReopenRequest());
}

TEST_F(SignalManagerTest, SIGUSR1SetsLogReopenFlag) {
  auto signal_mgr = SignalManager::Create();
  ASSERT_TRUE(signal_mgr.has_value());

  // Send SIGUSR1 to self
  raise(SIGUSR1);

  // Small delay for signal delivery
  std::this_thread::sleep_for(std::chrono::milliseconds(10));

  // Should now be true
  EXPECT_TRUE(SignalManager::ConsumeLogReopenRequest());
}

TEST_F(SignalManagerTest, ConsumeLogReopenRequestClearsFlag) {
  auto signal_mgr = SignalManager::Create();
  ASSERT_TRUE(signal_mgr.has_value());

  // Send SIGUSR1 to self
  raise(SIGUSR1);
  std::this_thread::sleep_for(std::chrono::milliseconds(10));

  // First consume should return true
  EXPECT_TRUE(SignalManager::ConsumeLogReopenRequest());

  // Second consume should return false (flag was cleared)
  EXPECT_FALSE(SignalManager::ConsumeLogReopenRequest());
}

TEST_F(SignalManagerTest, MultipleSIGUSR1OnlyRequiresOneConsume) {
  auto signal_mgr = SignalManager::Create();
  ASSERT_TRUE(signal_mgr.has_value());

  // Send multiple SIGUSR1
  raise(SIGUSR1);
  raise(SIGUSR1);
  raise(SIGUSR1);
  std::this_thread::sleep_for(std::chrono::milliseconds(10));

  // One consume clears the flag
  EXPECT_TRUE(SignalManager::ConsumeLogReopenRequest());
  EXPECT_FALSE(SignalManager::ConsumeLogReopenRequest());
}

TEST_F(SignalManagerTest, SIGUSR1DoesNotAffectShutdownFlag) {
  auto signal_mgr = SignalManager::Create();
  ASSERT_TRUE(signal_mgr.has_value());

  // Send SIGUSR1
  raise(SIGUSR1);
  std::this_thread::sleep_for(std::chrono::milliseconds(10));

  // Shutdown should still be false
  EXPECT_FALSE(SignalManager::IsShutdownRequested());

  // Log reopen should be true
  EXPECT_TRUE(SignalManager::ConsumeLogReopenRequest());
}

}  // namespace
}  // namespace mygramdb::app
