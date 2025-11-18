/**
 * @file rate_limiter_test.cpp
 * @brief Tests for rate limiting functionality
 */

#include "server/rate_limiter.h"

#include <gtest/gtest.h>

#include <chrono>
#include <thread>

using namespace mygramdb::server;

/**
 * @brief Test fixture for TokenBucket tests
 */
class TokenBucketTest : public ::testing::Test {
 protected:
  // Helper to wait for tokens to refill
  void WaitForRefill(size_t milliseconds) { std::this_thread::sleep_for(std::chrono::milliseconds(milliseconds)); }
};

/**
 * @brief Test basic token consumption
 */
TEST_F(TokenBucketTest, BasicConsumption) {
  TokenBucket bucket(10, 10);  // 10 tokens, refill 10 tokens/sec

  // Should be able to consume all tokens
  for (int i = 0; i < 10; ++i) {
    EXPECT_TRUE(bucket.TryConsume());
  }

  // Next attempt should fail (bucket empty)
  EXPECT_FALSE(bucket.TryConsume());
}

/**
 * @brief Test token refill
 */
TEST_F(TokenBucketTest, TokenRefill) {
  TokenBucket bucket(10, 10);  // 10 tokens, refill 10 tokens/sec

  // Consume all tokens
  for (int i = 0; i < 10; ++i) {
    EXPECT_TRUE(bucket.TryConsume());
  }

  // Bucket should be empty
  EXPECT_FALSE(bucket.TryConsume());

  // Wait 500ms -> should refill ~5 tokens
  WaitForRefill(500);

  // Should be able to consume approximately 5 tokens
  int consumed = 0;
  for (int i = 0; i < 10; ++i) {
    if (bucket.TryConsume()) {
      consumed++;
    } else {
      break;
    }
  }

  // Should have consumed between 4-6 tokens (accounting for timing variance)
  EXPECT_GE(consumed, 4);
  EXPECT_LE(consumed, 6);
}

/**
 * @brief Test multi-token consumption
 */
TEST_F(TokenBucketTest, MultiTokenConsumption) {
  TokenBucket bucket(10, 10);

  // Consume 5 tokens at once
  EXPECT_TRUE(bucket.TryConsume(5));

  // Should have 5 tokens left
  EXPECT_TRUE(bucket.TryConsume(5));

  // Should be empty now
  EXPECT_FALSE(bucket.TryConsume(1));
}

/**
 * @brief Test capacity limit
 */
TEST_F(TokenBucketTest, CapacityLimit) {
  TokenBucket bucket(10, 10);

  // Wait for longer than needed to fill bucket
  WaitForRefill(2000);  // 2 seconds -> would add 20 tokens if no limit

  // Should only have 10 tokens (capacity limit)
  for (int i = 0; i < 10; ++i) {
    EXPECT_TRUE(bucket.TryConsume());
  }

  EXPECT_FALSE(bucket.TryConsume());
}

/**
 * @brief Test reset functionality
 */
TEST_F(TokenBucketTest, Reset) {
  TokenBucket bucket(10, 10);

  // Consume all tokens
  for (int i = 0; i < 10; ++i) {
    EXPECT_TRUE(bucket.TryConsume());
  }

  EXPECT_FALSE(bucket.TryConsume());

  // Reset bucket
  bucket.Reset();

  // Should be full again
  for (int i = 0; i < 10; ++i) {
    EXPECT_TRUE(bucket.TryConsume());
  }

  EXPECT_FALSE(bucket.TryConsume());
}

/**
 * @brief Test fixture for RateLimiter tests
 */
class RateLimiterTest : public ::testing::Test {
 protected:
  void WaitForRefill(size_t milliseconds) { std::this_thread::sleep_for(std::chrono::milliseconds(milliseconds)); }
};

/**
 * @brief Test per-client rate limiting
 */
TEST_F(RateLimiterTest, PerClientLimiting) {
  RateLimiter limiter(10, 10);  // 10 tokens/client, refill 10/sec

  // Client 1 should be able to make 10 requests
  for (int i = 0; i < 10; ++i) {
    EXPECT_TRUE(limiter.AllowRequest("192.168.1.1"));
  }

  // 11th request from client 1 should be blocked
  EXPECT_FALSE(limiter.AllowRequest("192.168.1.1"));

  // Client 2 should still have full quota
  for (int i = 0; i < 10; ++i) {
    EXPECT_TRUE(limiter.AllowRequest("192.168.1.2"));
  }

  EXPECT_FALSE(limiter.AllowRequest("192.168.1.2"));
}

/**
 * @brief Test rate limiter statistics
 */
TEST_F(RateLimiterTest, Statistics) {
  RateLimiter limiter(5, 5);

  // Make some requests
  limiter.AllowRequest("192.168.1.1");  // Allowed
  limiter.AllowRequest("192.168.1.1");  // Allowed
  limiter.AllowRequest("192.168.1.1");  // Allowed
  limiter.AllowRequest("192.168.1.2");  // Allowed
  limiter.AllowRequest("192.168.1.1");  // Allowed
  limiter.AllowRequest("192.168.1.1");  // Allowed
  limiter.AllowRequest("192.168.1.1");  // Blocked (exceeded limit)

  auto stats = limiter.GetStats();

  EXPECT_EQ(stats.total_requests, 7);
  EXPECT_EQ(stats.allowed_requests, 6);
  EXPECT_EQ(stats.blocked_requests, 1);
  EXPECT_EQ(stats.tracked_clients, 2);

  // Reset stats
  limiter.ResetStats();

  stats = limiter.GetStats();
  EXPECT_EQ(stats.total_requests, 0);
  EXPECT_EQ(stats.allowed_requests, 0);
  EXPECT_EQ(stats.blocked_requests, 0);
  // tracked_clients not reset by ResetStats
  EXPECT_EQ(stats.tracked_clients, 2);
}

/**
 * @brief Test max clients limit
 */
TEST_F(RateLimiterTest, MaxClientsLimit) {
  RateLimiter limiter(10, 10, 3);  // Max 3 clients

  // Create 3 clients
  EXPECT_TRUE(limiter.AllowRequest("192.168.1.1"));
  EXPECT_TRUE(limiter.AllowRequest("192.168.1.2"));
  EXPECT_TRUE(limiter.AllowRequest("192.168.1.3"));

  // 4th client should be rejected
  EXPECT_FALSE(limiter.AllowRequest("192.168.1.4"));

  auto stats = limiter.GetStats();
  EXPECT_EQ(stats.tracked_clients, 3);
}

/**
 * @brief Test clear functionality
 */
TEST_F(RateLimiterTest, Clear) {
  RateLimiter limiter(5, 5);

  // Exhaust quota for client
  for (int i = 0; i < 5; ++i) {
    EXPECT_TRUE(limiter.AllowRequest("192.168.1.1"));
  }

  EXPECT_FALSE(limiter.AllowRequest("192.168.1.1"));

  // Clear all clients
  limiter.Clear();

  // Client should have fresh quota
  for (int i = 0; i < 5; ++i) {
    EXPECT_TRUE(limiter.AllowRequest("192.168.1.1"));
  }
}

/**
 * @brief Test concurrent access
 */
TEST_F(RateLimiterTest, ConcurrentAccess) {
  RateLimiter limiter(100, 100);

  std::atomic<int> allowed_count{0};
  std::atomic<int> blocked_count{0};

  // Launch multiple threads making requests
  std::vector<std::thread> threads;
  for (int i = 0; i < 10; ++i) {
    threads.emplace_back([&limiter, &allowed_count, &blocked_count, i]() {
      std::string client_ip = "192.168.1." + std::to_string(i);
      for (int j = 0; j < 20; ++j) {
        if (limiter.AllowRequest(client_ip)) {
          allowed_count++;
        } else {
          blocked_count++;
        }
      }
    });
  }

  // Wait for all threads
  for (auto& thread : threads) {
    thread.join();
  }

  // Total requests = 10 threads * 20 requests = 200
  auto stats = limiter.GetStats();
  EXPECT_EQ(stats.total_requests, 200);
  EXPECT_EQ(allowed_count.load(), stats.allowed_requests);
  EXPECT_EQ(blocked_count.load(), stats.blocked_requests);

  // Each client has 100 token capacity, making 20 requests each
  // All 200 requests should be allowed (well within capacity)
  EXPECT_EQ(stats.allowed_requests, 200);
  EXPECT_EQ(stats.blocked_requests, 0);
}

/**
 * @brief Test refill with realistic scenario
 */
TEST_F(RateLimiterTest, RealisticRefill) {
  RateLimiter limiter(10, 5);  // 10 burst, 5 tokens/sec refill

  // Burst: consume 10 tokens
  for (int i = 0; i < 10; ++i) {
    EXPECT_TRUE(limiter.AllowRequest("192.168.1.1"));
  }

  // Should be blocked
  EXPECT_FALSE(limiter.AllowRequest("192.168.1.1"));

  // Wait 1 second -> should refill ~5 tokens
  WaitForRefill(1000);

  // Should be able to consume ~5 more
  int consumed = 0;
  for (int i = 0; i < 10; ++i) {
    if (limiter.AllowRequest("192.168.1.1")) {
      consumed++;
    } else {
      break;
    }
  }

  // Should have consumed between 4-6 tokens
  EXPECT_GE(consumed, 4);
  EXPECT_LE(consumed, 6);
}

/**
 * @brief Test no deadlock when calling GetStats() and AllowRequest() concurrently
 *
 * This test ensures that concurrent calls to GetStats() and AllowRequest()
 * do not cause deadlock due to lock ordering issues. It spawns multiple
 * threads that continuously call both methods.
 *
 * Run with ThreadSanitizer to detect potential deadlocks:
 *   cmake -DCMAKE_BUILD_TYPE=Debug -DENABLE_TSAN=ON ..
 */
TEST_F(RateLimiterTest, NoDeadlockUnderConcurrentLoad) {
  RateLimiter limiter(100, 10, 1000, 1000, 60);

  std::atomic<bool> stop{false};
  std::vector<std::thread> threads;

  // Threads continuously calling AllowRequest
  for (int i = 0; i < 5; ++i) {
    threads.emplace_back([&, i]() {
      std::string client_ip = "192.168.1." + std::to_string(i);
      while (!stop.load()) {
        limiter.AllowRequest(client_ip);
        std::this_thread::sleep_for(std::chrono::microseconds(10));
      }
    });
  }

  // Threads continuously calling GetStats
  for (int i = 0; i < 3; ++i) {
    threads.emplace_back([&]() {
      while (!stop.load()) {
        auto stats = limiter.GetStats();
        (void)stats;  // Suppress unused variable warning
        std::this_thread::sleep_for(std::chrono::microseconds(10));
      }
    });
  }

  // Let threads run for a short period
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  // Signal threads to stop
  stop.store(true);

  // Wait for all threads to finish
  for (auto& thread : threads) {
    thread.join();
  }

  // If we reach here without hanging, the test passes
  auto final_stats = limiter.GetStats();
  EXPECT_GT(final_stats.total_requests, 0);
}
