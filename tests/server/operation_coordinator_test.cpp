#include "server/operation_coordinator.h"

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <chrono>
#include <future>
#include <thread>

namespace mygramdb::server {

namespace {

struct OperationPair {
  LongOperation first;
  LongOperation second;
};

void ExpectExactlyOneAccepted(LongOperation first, LongOperation second) {
  OperationCoordinator coordinator;
  std::atomic<int> ready{0};
  std::atomic<bool> start{false};
  std::atomic<int> attempted{0};
  std::atomic<int> accepted{0};
  std::atomic<int> mutations{0};

  auto contender = [&](LongOperation operation) {
    ready.fetch_add(1, std::memory_order_release);
    while (!start.load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }
    auto token = coordinator.TryAcquire(operation);
    if (token.has_value()) {
      accepted.fetch_add(1, std::memory_order_relaxed);
      mutations.fetch_add(1, std::memory_order_relaxed);
    }
    attempted.fetch_add(1, std::memory_order_release);
    while (attempted.load(std::memory_order_acquire) != 2) {
      std::this_thread::yield();
    }
  };

  std::thread first_thread(contender, first);
  std::thread second_thread(contender, second);
  while (ready.load(std::memory_order_acquire) != 2) {
    std::this_thread::yield();
  }
  start.store(true, std::memory_order_release);
  first_thread.join();
  second_thread.join();

  EXPECT_EQ(accepted.load(), 1);
  EXPECT_EQ(mutations.load(), 1) << "the rejected operation must not mutate state";
  EXPECT_FALSE(coordinator.GetActive().has_value());
}

}  // namespace

TEST(OperationCoordinatorTest, StatefulOperationMatrixAcceptsExactlyOneConcurrentStarter) {
  constexpr std::array<OperationPair, 16> pairs = {
      OperationPair{LongOperation::kDumpSave, LongOperation::kSync},
      OperationPair{LongOperation::kDumpLoad, LongOperation::kSync},
      OperationPair{LongOperation::kOptimize, LongOperation::kSync},
      OperationPair{LongOperation::kAutoSnapshot, LongOperation::kSync},
      OperationPair{LongOperation::kDumpSave, LongOperation::kOptimize},
      OperationPair{LongOperation::kDumpSave, LongOperation::kDumpLoad},
      OperationPair{LongOperation::kMysqlReconnect, LongOperation::kSync},
      OperationPair{LongOperation::kMysqlReconnect, LongOperation::kDumpSave},
      OperationPair{LongOperation::kMysqlReconnect, LongOperation::kDumpLoad},
      OperationPair{LongOperation::kMysqlReconnect, LongOperation::kAutoSnapshot},
      OperationPair{LongOperation::kReplicationStart, LongOperation::kSync},
      OperationPair{LongOperation::kReplicationStart, LongOperation::kDumpSave},
      OperationPair{LongOperation::kReplicationStart, LongOperation::kDumpLoad},
      OperationPair{LongOperation::kReplicationStop, LongOperation::kSync},
      OperationPair{LongOperation::kReplicationStop, LongOperation::kDumpSave},
      OperationPair{LongOperation::kReplicationStop, LongOperation::kDumpLoad},
  };

  for (const auto& pair : pairs) {
    for (int iteration = 0; iteration < 100; ++iteration) {
      ExpectExactlyOneAccepted(pair.first, pair.second);
    }
  }
}

TEST(OperationCoordinatorTest, TokenReleaseMakesNextOperationEligible) {
  OperationCoordinator coordinator;
  auto sync = coordinator.TryAcquire(LongOperation::kSync, "articles");
  ASSERT_TRUE(sync.has_value());
  EXPECT_FALSE(coordinator.TryAcquire(LongOperation::kDumpLoad).has_value());
  sync->Release();
  EXPECT_TRUE(coordinator.TryAcquire(LongOperation::kDumpLoad).has_value());
}

TEST(OperationCoordinatorTest, ShutdownAdmissionBlockIsPersistent) {
  OperationCoordinator coordinator;
  coordinator.BlockNewOperationsForShutdown();
  EXPECT_FALSE(coordinator.TryAcquire(LongOperation::kSync).has_value());
  EXPECT_FALSE(coordinator.TryAcquire(LongOperation::kDumpSave).has_value());
  EXPECT_EQ(coordinator.DescribeActive(), "server shutdown");
}

TEST(OperationCoordinatorTest, StartupReopensAdmissionOnlyAfterTheActiveTokenDrains) {
  OperationCoordinator coordinator;
  std::atomic<bool> shutdown_requested{false};
  auto old_token = coordinator.TryAcquire(LongOperation::kDumpSave, "before restart");
  ASSERT_TRUE(old_token.has_value());

  coordinator.BlockNewOperationsForShutdown(&shutdown_requested);
  EXPECT_FALSE(coordinator.ReopenForStartup(&shutdown_requested));
  EXPECT_TRUE(shutdown_requested.load(std::memory_order_acquire));
  EXPECT_FALSE(coordinator.TryAcquire(LongOperation::kSync).has_value());

  old_token->Release();
  ASSERT_TRUE(coordinator.ReopenForStartup(&shutdown_requested));
  EXPECT_FALSE(shutdown_requested.load(std::memory_order_acquire));

  auto new_token = coordinator.TryAcquire(LongOperation::kSync, "after restart");
  ASSERT_TRUE(new_token.has_value());
  old_token->Release();
  const auto active = coordinator.GetActive();
  ASSERT_TRUE(active.has_value());
  EXPECT_EQ(active->type, LongOperation::kSync);
  EXPECT_EQ(active->detail, "after restart");
}

TEST(OperationCoordinatorTest, ShutdownPublicationClosesAdmissionAndPublishesFlag) {
  OperationCoordinator coordinator;
  std::atomic<bool> shutting_down{false};

  coordinator.BlockNewOperationsForShutdown(&shutting_down);

  EXPECT_TRUE(shutting_down.load(std::memory_order_acquire));
  EXPECT_FALSE(coordinator.TryAcquire(LongOperation::kDumpSave).has_value());
  EXPECT_EQ(coordinator.DescribeActive(), "server shutdown");
}

TEST(OperationCoordinatorTest, WaitForIdleCompletesWhenTheActiveTokenIsReleased) {
  OperationCoordinator coordinator;
  auto token = coordinator.TryAcquire(LongOperation::kDumpSave);
  ASSERT_TRUE(token.has_value());
  coordinator.BlockNewOperationsForShutdown();

  auto waiter =
      std::async(std::launch::async, [&coordinator] { return coordinator.WaitForIdle(std::chrono::seconds(2)); });
  EXPECT_EQ(waiter.wait_for(std::chrono::milliseconds(20)), std::future_status::timeout);

  token->Release();
  EXPECT_TRUE(waiter.get());
}

TEST(OperationCoordinatorTest, WaitForIdleTimesOutWhileATokenRemainsActive) {
  OperationCoordinator coordinator;
  auto token = coordinator.TryAcquire(LongOperation::kSync, "users");
  ASSERT_TRUE(token.has_value());
  coordinator.BlockNewOperationsForShutdown();

  EXPECT_FALSE(coordinator.WaitForIdle(std::chrono::milliseconds(1)));
  token->Release();
}

}  // namespace mygramdb::server
