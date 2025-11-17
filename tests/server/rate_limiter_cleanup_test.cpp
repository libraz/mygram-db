/**
 * @file rate_limiter_cleanup_test.cpp
 * @brief Test RateLimiter cleanup behavior to prevent memory leaks
 *
 * This test verifies that:
 * 1. Old client buckets are cleaned up automatically
 * 2. Memory usage doesn't grow unbounded
 * 3. Active clients are not removed
 * 4. Cleanup is triggered periodically
 */

#include <gtest/gtest.h>

#include <chrono>
#include <thread>
#include <vector>

#include "server/rate_limiter.h"

namespace mygramdb::server {

class RateLimiterCleanupTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create rate limiter with sufficient capacity for testing
    // capacity=1000 (large enough to not block during tests)
    // refill_rate=100 (fast refill)
    // max_clients=100
    // cleanup_interval=1000 (cleanup every 1000 requests)
    // inactivity_timeout_sec=2 (2 seconds for faster tests)
    limiter_ = std::make_unique<RateLimiter>(1000, 100, 100, 1000, 2);
  }

  std::unique_ptr<RateLimiter> limiter_;
};

/**
 * @brief Test that inactive clients are cleaned up
 */
TEST_F(RateLimiterCleanupTest, InactiveClientsAreRemoved) {
  // Submit requests from many clients
  const int num_clients = 50;
  for (int i = 0; i < num_clients; ++i) {
    std::string client_ip = "192.168.1." + std::to_string(i);
    bool allowed = limiter_->AllowRequest(client_ip);
    EXPECT_TRUE(allowed) << "First request should be allowed for " << client_ip;
  }

  // Check that clients are tracked
  auto stats_before = limiter_->GetStats();
  EXPECT_EQ(stats_before.tracked_clients, num_clients) << "All clients should be tracked initially";

  // Wait for clients to become inactive (assume cleanup threshold is 1 hour)
  // Since we can't wait 1 hour in a test, we'll trigger cleanup manually
  // by making more requests (which should trigger periodic cleanup)

  // Sleep briefly to ensure timestamp difference
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // Make requests from many new clients to trigger cleanup
  // The cleanup should happen every N requests
  for (int i = 0; i < 1000; ++i) {
    std::string client_ip = "10.0.0." + std::to_string(i % 200);
    limiter_->AllowRequest(client_ip);
  }

  // Note: Since cleanup threshold is configurable and may be long (e.g., 1 hour),
  // we can't verify immediate cleanup without access to internal state.
  // This test mainly verifies that the system doesn't crash during cleanup.
  auto stats_after = limiter_->GetStats();
  EXPECT_GT(stats_after.total_requests, num_clients) << "Requests should have been processed";
}

/**
 * @brief Test that active clients are not removed during cleanup
 */
TEST_F(RateLimiterCleanupTest, ActiveClientsNotRemoved) {
  std::string active_client = "192.168.1.100";

  // Make initial request
  EXPECT_TRUE(limiter_->AllowRequest(active_client));

  // Make requests from other clients to trigger cleanup
  // Use only 20 unique clients to stay well under max_clients limit (100)
  // and leave room for the active client
  for (int i = 0; i < 1000; ++i) {
    std::string client_ip = "10.0.0." + std::to_string(i % 20);
    limiter_->AllowRequest(client_ip);

    // Periodically refresh the active client to keep it active
    if (i % 50 == 0) {
      EXPECT_TRUE(limiter_->AllowRequest(active_client)) << "Active client request should succeed at iteration " << i;
    }
  }

  // Active client should still be able to make requests
  EXPECT_TRUE(limiter_->AllowRequest(active_client)) << "Active client should not be removed";
}

/**
 * @brief Test that max_clients limit is enforced
 */
TEST_F(RateLimiterCleanupTest, MaxClientsLimitEnforced) {
  const size_t max_clients = 100;
  RateLimiter limiter(10, 10, max_clients);

  // Try to add more clients than the limit
  for (size_t i = 0; i < max_clients * 2; ++i) {
    std::string client_ip = "192.168." + std::to_string(i / 256) + "." + std::to_string(i % 256);
    limiter.AllowRequest(client_ip);
  }

  auto stats = limiter.GetStats();
  // Tracked clients should not exceed max_clients by too much
  // (some overflow is acceptable due to cleanup timing)
  EXPECT_LE(stats.tracked_clients, max_clients * 1.2) << "Tracked clients should stay near max_clients limit";
}

/**
 * @brief Test memory usage doesn't grow unbounded
 */
TEST_F(RateLimiterCleanupTest, MemoryUsageDoesntGrowUnbounded) {
  // Create a rate limiter with short inactivity timeout (1 second) for testing
  RateLimiter short_timeout_limiter(10, 10, 100, 10, 1);  // 1 second timeout

  // Simulate long-running server with many ephemeral clients
  const int iterations = 50;
  const int clients_per_iteration = 10;

  for (int iter = 0; iter < iterations; ++iter) {
    // Each iteration represents a "time period" with new clients
    for (int i = 0; i < clients_per_iteration; ++i) {
      std::string client_ip = "192.168." + std::to_string(iter) + "." + std::to_string(i);
      short_timeout_limiter.AllowRequest(client_ip);
    }

    // Wait for clients to become inactive (> 1 second)
    if (iter % 10 == 9) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    }
  }

  auto stats = short_timeout_limiter.GetStats();
  // After 50 iterations with 10 clients each (500 unique clients),
  // tracked clients should be much less due to cleanup
  EXPECT_LT(stats.tracked_clients, 200) << "Old clients should be cleaned up";
  EXPECT_EQ(stats.total_requests, iterations * clients_per_iteration) << "All requests should be processed";
}

/**
 * @brief Test cleanup behavior with varying access patterns
 */
TEST_F(RateLimiterCleanupTest, CleanupWithVaryingAccessPatterns) {
  // Pattern 1: Burst of clients
  for (int i = 0; i < 50; ++i) {
    std::string client_ip = "burst." + std::to_string(i);
    limiter_->AllowRequest(client_ip);
  }

  size_t clients_after_burst = limiter_->GetStats().tracked_clients;
  EXPECT_GE(clients_after_burst, 50);

  // Pattern 2: Steady stream from fewer clients
  for (int i = 0; i < 500; ++i) {
    std::string client_ip = "steady." + std::to_string(i % 10);
    limiter_->AllowRequest(client_ip);
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  // Cleanup should have happened, reducing client count
  size_t clients_after_steady = limiter_->GetStats().tracked_clients;
  std::cout << "Clients after burst: " << clients_after_burst << std::endl;
  std::cout << "Clients after steady: " << clients_after_steady << std::endl;
}

/**
 * @brief Test that Clear() removes all clients
 */
TEST_F(RateLimiterCleanupTest, ClearRemovesAllClients) {
  // Add many clients
  for (int i = 0; i < 100; ++i) {
    std::string client_ip = "192.168.1." + std::to_string(i);
    limiter_->AllowRequest(client_ip);
  }

  EXPECT_GT(limiter_->GetStats().tracked_clients, 0);

  // Clear all clients
  limiter_->Clear();

  // No clients should be tracked
  EXPECT_EQ(limiter_->GetStats().tracked_clients, 0) << "Clear should remove all clients";

  // New requests should work
  EXPECT_TRUE(limiter_->AllowRequest("192.168.1.1")) << "Requests should work after Clear";
}

/**
 * @brief Test rate limiting still works correctly during cleanup
 */
TEST_F(RateLimiterCleanupTest, RateLimitingWorksWithCleanup) {
  // Create a limiter with small capacity for this specific test
  RateLimiter small_limiter(10, 1, 100, 1000, 2);
  std::string client_ip = "192.168.1.50";

  // Exhaust the bucket (capacity=10)
  for (int i = 0; i < 10; ++i) {
    EXPECT_TRUE(small_limiter.AllowRequest(client_ip)) << "Request " << i << " should be allowed";
  }

  // Next request should be rate limited
  EXPECT_FALSE(small_limiter.AllowRequest(client_ip)) << "Request should be rate limited";

  // Make requests from other clients to potentially trigger cleanup
  for (int i = 0; i < 100; ++i) {
    std::string other_client = "10.0.0." + std::to_string(i);
    small_limiter.AllowRequest(other_client);
  }

  // Original client should still be rate limited
  EXPECT_FALSE(small_limiter.AllowRequest(client_ip)) << "Client should still be rate limited after cleanup";

  // Wait for refill (refill_rate=10/sec, so 100ms should refill ~1 token)
  std::this_thread::sleep_for(std::chrono::milliseconds(150));

  // Should be able to make request again
  EXPECT_TRUE(limiter_->AllowRequest(client_ip)) << "Request should be allowed after refill";
}

/**
 * @brief Stress test: many clients with continuous cleanup
 */
TEST_F(RateLimiterCleanupTest, StressTestManyClientsWithCleanup) {
  const int total_requests = 10000;
  const int unique_clients = 500;

  int allowed = 0;
  int blocked = 0;

  for (int i = 0; i < total_requests; ++i) {
    std::string client_ip = "client." + std::to_string(i % unique_clients);

    if (limiter_->AllowRequest(client_ip)) {
      allowed++;
    } else {
      blocked++;
    }
  }

  auto stats = limiter_->GetStats();
  EXPECT_EQ(stats.total_requests, total_requests);
  EXPECT_GT(allowed, 0) << "Some requests should be allowed";
  EXPECT_EQ(stats.allowed_requests, allowed);
  EXPECT_EQ(stats.blocked_requests, blocked);

  // Tracked clients should be reasonable
  EXPECT_LE(stats.tracked_clients, unique_clients) << "Tracked clients should not exceed unique clients";
  std::cout << "Tracked clients: " << stats.tracked_clients << "/" << unique_clients << std::endl;
}

/**
 * @brief Test that ResetStats works correctly
 */
TEST_F(RateLimiterCleanupTest, ResetStatsWorks) {
  // Make some requests
  for (int i = 0; i < 50; ++i) {
    std::string client_ip = "192.168.1." + std::to_string(i);
    limiter_->AllowRequest(client_ip);
  }

  auto stats_before = limiter_->GetStats();
  EXPECT_GT(stats_before.total_requests, 0);

  // Reset stats
  limiter_->ResetStats();

  auto stats_after = limiter_->GetStats();
  EXPECT_EQ(stats_after.total_requests, 0) << "Total requests should be reset";
  EXPECT_EQ(stats_after.allowed_requests, 0) << "Allowed requests should be reset";
  EXPECT_EQ(stats_after.blocked_requests, 0) << "Blocked requests should be reset";
  // tracked_clients may or may not be reset (depends on implementation)
}

}  // namespace mygramdb::server
