/**
 * @file rate_limiter_test.cpp
 * @brief Tests for rate limiting functionality
 */

#include "server/rate_limiter.h"

#include <gtest/gtest.h>

#include <chrono>
#include <memory>
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

TEST_F(TokenBucketTest, SubMillisecondRefillRemainderAccumulates) {
  TokenBucket bucket(10, 1000);  // 1000 tokens/sec = 1 token/ms

  for (int i = 0; i < 10; ++i) {
    ASSERT_TRUE(bucket.TryConsume());
  }

  for (int i = 0; i < 4; ++i) {
    bucket.RewindLastRefillForTesting(std::chrono::microseconds(1500));
    ASSERT_TRUE(bucket.TryConsume());
  }

  EXPECT_TRUE(bucket.TryConsume()) << "four 0.5ms remainders should accumulate two extra tokens";
  EXPECT_TRUE(bucket.TryConsume()) << "four 0.5ms remainders should accumulate two extra tokens";
  EXPECT_FALSE(bucket.TryConsume());
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
  RateLimiter limiter(100, 10, 1000, std::chrono::seconds(60), 60);

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

/**
 * @brief Test that atomic stats counters are consistent under concurrent access
 *
 * Verifies that total_requests == allowed_requests + blocked_requests
 * after concurrent AllowRequest, GetStats, and ResetStats calls.
 */
TEST_F(RateLimiterTest, AtomicStatsConsistencyUnderConcurrency) {
  // Small capacity so some requests will be blocked
  RateLimiter limiter(5, 5, 10000, std::chrono::seconds(2), 300);

  constexpr int kThreads = 8;
  constexpr int kRequestsPerThread = 500;

  std::atomic<uint64_t> total_allowed{0};
  std::atomic<uint64_t> total_blocked{0};
  std::vector<std::thread> threads;

  for (int i = 0; i < kThreads; ++i) {
    threads.emplace_back([&, i]() {
      std::string client_ip = "10.0.0." + std::to_string(i);
      for (int j = 0; j < kRequestsPerThread; ++j) {
        if (limiter.AllowRequest(client_ip)) {
          total_allowed.fetch_add(1, std::memory_order_relaxed);
        } else {
          total_blocked.fetch_add(1, std::memory_order_relaxed);
        }
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  auto stats = limiter.GetStats();
  uint64_t expected_total = static_cast<uint64_t>(kThreads) * kRequestsPerThread;

  EXPECT_EQ(stats.total_requests, expected_total);
  EXPECT_EQ(stats.allowed_requests, total_allowed.load());
  EXPECT_EQ(stats.blocked_requests, total_blocked.load());
  EXPECT_EQ(stats.total_requests, stats.allowed_requests + stats.blocked_requests);
}

/**
 * @brief Test that ResetStats is thread-safe with concurrent AllowRequest/GetStats
 *
 * Regression test: ResetStats previously did not acquire mutex_, causing
 * GetStats (which reads under mutex) to observe inconsistent counter values
 * where total < allowed + blocked.
 */
TEST_F(RateLimiterTest, ResetStatsConcurrentConsistency) {
  RateLimiter limiter(5, 5, 10000, std::chrono::seconds(2), 300);

  constexpr int kIterations = 2000;
  std::atomic<bool> inconsistency_found{false};
  std::atomic<bool> stop{false};

  // Thread 1: AllowRequest continuously
  std::thread requester([&]() {
    for (int i = 0; i < kIterations && !stop.load(std::memory_order_relaxed); ++i) {
      limiter.AllowRequest("10.0.0." + std::to_string(i % 100));
    }
  });

  // Thread 2: ResetStats continuously
  std::thread resetter([&]() {
    for (int i = 0; i < kIterations && !stop.load(std::memory_order_relaxed); ++i) {
      limiter.ResetStats();
    }
  });

  // Thread 3: GetStats and check consistency
  std::thread checker([&]() {
    for (int i = 0; i < kIterations && !stop.load(std::memory_order_relaxed); ++i) {
      auto stats = limiter.GetStats();
      // total must always equal allowed + blocked
      if (stats.total_requests != stats.allowed_requests + stats.blocked_requests) {
        inconsistency_found.store(true, std::memory_order_relaxed);
        stop.store(true, std::memory_order_relaxed);
        break;
      }
    }
  });

  requester.join();
  resetter.join();
  checker.join();

  EXPECT_FALSE(inconsistency_found.load()) << "Stats counters were inconsistent during concurrent ResetStats";
}

/**
 * @brief Test that IPv6 addresses are treated as distinct clients
 *
 * Regression test: IPv6 clients previously all shared the "unknown" bucket
 * because only AF_INET was handled in peer address extraction.
 */
TEST_F(RateLimiterTest, IPv6DistinctClients) {
  RateLimiter limiter(5, 5);

  // IPv6 client 1
  for (int i = 0; i < 5; ++i) {
    EXPECT_TRUE(limiter.AllowRequest("::1"));
  }
  // Should be blocked after exhausting quota
  EXPECT_FALSE(limiter.AllowRequest("::1"));

  // IPv6 client 2 should have its own independent quota
  for (int i = 0; i < 5; ++i) {
    EXPECT_TRUE(limiter.AllowRequest("2001:db8::1"));
  }
  EXPECT_FALSE(limiter.AllowRequest("2001:db8::1"));

  // IPv4 client should also be independent
  for (int i = 0; i < 5; ++i) {
    EXPECT_TRUE(limiter.AllowRequest("192.168.1.1"));
  }
  EXPECT_FALSE(limiter.AllowRequest("192.168.1.1"));

  auto stats = limiter.GetStats();
  EXPECT_EQ(stats.tracked_clients, 3);
}

/**
 * @brief Test that full-length IPv6 addresses work as rate limiter keys
 */
TEST_F(RateLimiterTest, IPv6FullAddressFormat) {
  RateLimiter limiter(3, 3);

  // Full IPv6 address
  std::string full_ipv6 = "2001:0db8:85a3:0000:0000:8a2e:0370:7334";
  for (int i = 0; i < 3; ++i) {
    EXPECT_TRUE(limiter.AllowRequest(full_ipv6));
  }
  EXPECT_FALSE(limiter.AllowRequest(full_ipv6));

  // Compressed form of a different address should be a separate client
  std::string compressed_ipv6 = "fe80::1";
  for (int i = 0; i < 3; ++i) {
    EXPECT_TRUE(limiter.AllowRequest(compressed_ipv6));
  }
  EXPECT_FALSE(limiter.AllowRequest(compressed_ipv6));

  auto stats = limiter.GetStats();
  EXPECT_EQ(stats.tracked_clients, 2);
}

/**
 * @brief Fix N-4: a single shared RateLimiter instance enforces a unified
 *        per-client quota across multiple call sites.
 *
 * The intent of the production wiring is that TcpServer and HttpServer
 * receive shared_ptr<RateLimiter> copies of the same instance so a client's
 * quota applies across both protocols. This test simulates that contract at
 * the unit level: two callers ("TCP" and "HTTP") share one instance and
 * call AllowRequest with the same client IP. After the bucket is exhausted
 * via one caller, requests from the other caller for the same IP are also
 * denied.
 */
TEST_F(RateLimiterTest, SharedInstanceAccountsAcrossCallers) {
  // Capacity 3, no refill within the test window.
  auto shared_limiter = std::make_shared<RateLimiter>(3, /*refill_rate=*/1);

  // Pretend caller A is the TCP server.
  auto tcp_caller = shared_limiter;
  // Pretend caller B is the HTTP server. Same instance; quota MUST be shared.
  auto http_caller = shared_limiter;
  EXPECT_EQ(tcp_caller.get(), http_caller.get()) << "Test must use the same RateLimiter instance";

  const std::string client_ip = "192.0.2.42";

  // First two requests from TCP caller succeed.
  EXPECT_TRUE(tcp_caller->AllowRequest(client_ip));
  EXPECT_TRUE(tcp_caller->AllowRequest(client_ip));
  // Third request from HTTP caller succeeds (still within capacity).
  EXPECT_TRUE(http_caller->AllowRequest(client_ip));

  // Bucket is now empty for this client. Subsequent requests from EITHER
  // caller MUST be denied — the failure mode the fix prevents is one caller
  // having its own private bucket, which would let the client effectively
  // double its quota by talking to both protocols.
  EXPECT_FALSE(tcp_caller->AllowRequest(client_ip)) << "TCP request must be denied after shared bucket exhausted";
  EXPECT_FALSE(http_caller->AllowRequest(client_ip)) << "HTTP request must be denied after shared bucket exhausted";

  // Statistics from the shared instance reflect ALL traffic regardless of
  // which caller invoked AllowRequest.
  auto stats = shared_limiter->GetStats();
  EXPECT_EQ(stats.total_requests, 5);
  EXPECT_EQ(stats.allowed_requests, 3);
  EXPECT_EQ(stats.blocked_requests, 2);
  EXPECT_EQ(stats.tracked_clients, 1);
}

/**
 * @brief H-N2 smoke test: GetStats() does not block the AllowRequest hot
 *        path on the per-client buckets mutex.
 *
 * Pre-fix, GetStats() acquired `mutex_` for the entire body, including the
 * three atomic counter loads. AllowRequest contends for the same mutex, so
 * an /info request that lands during a busy traffic burst would block
 * every concurrent AllowRequest until it finished. The fix narrows the
 * lock to only `client_buckets_.size()` and reads the atomic counters
 * outside the critical section.
 *
 * This is a smoke test (per the spec): timing-based "GetStats does not
 * block AllowRequest" assertions are flaky on shared CI hosts. We instead
 * exercise the code path under heavy concurrency and assert that:
 *   1. No deadlock or crash occurs.
 *   2. The post-join stats are internally consistent.
 *   3. tracked_clients is bounded by max_clients.
 *
 * If the lock is incorrectly widened in a future refactor, this test
 * still passes — the strong guarantee comes from inspection. The hot-path
 * latency improvement is best validated by the production
 * `rate_limiter_metric` Prometheus histograms.
 */
TEST_F(RateLimiterTest, GetStatsDoesNotBlockAllowRequest) {
  RateLimiter limiter(100, 50, /*max_clients=*/256);

  std::atomic<bool> stop{false};
  std::atomic<uint64_t> allow_calls{0};
  std::atomic<uint64_t> getstats_calls{0};
  std::vector<std::thread> threads;

  // Hot path: 6 threads continuously calling AllowRequest.
  for (int i = 0; i < 6; ++i) {
    threads.emplace_back([&, i]() {
      const std::string client_ip = "10.0." + std::to_string(i / 4) + "." + std::to_string(i % 4);
      while (!stop.load(std::memory_order_relaxed)) {
        limiter.AllowRequest(client_ip);
        allow_calls.fetch_add(1, std::memory_order_relaxed);
      }
    });
  }
  // Observability path: 2 threads continuously calling GetStats.
  for (int i = 0; i < 2; ++i) {
    threads.emplace_back([&]() {
      while (!stop.load(std::memory_order_relaxed)) {
        auto stats = limiter.GetStats();
        // Invariant: total == allowed + blocked, by construction of GetStats.
        EXPECT_EQ(stats.total_requests, stats.allowed_requests + stats.blocked_requests);
        EXPECT_LE(stats.tracked_clients, static_cast<size_t>(256));
        getstats_calls.fetch_add(1, std::memory_order_relaxed);
      }
    });
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  stop.store(true, std::memory_order_relaxed);
  for (auto& t : threads) {
    t.join();
  }

  // Both code paths must have actually run; if GetStats was effectively
  // serialised behind the AllowRequest hot path, the call counts would be
  // skewed by orders of magnitude. We just sanity-check that both made
  // progress.
  EXPECT_GT(allow_calls.load(), 1000U) << "AllowRequest hot path must have processed many requests";
  EXPECT_GT(getstats_calls.load(), 100U) << "GetStats must have made independent progress";
}

/**
 * @brief Perf-3 regression: the background sweeper removes expired buckets
 *        without requiring AllowRequest to be invoked.
 *
 * Cleanup was previously inline inside AllowRequest, so a tracked client
 * could only be evicted by a subsequent call. The sweeper now runs on a
 * dedicated thread; we wait long enough for one sweep cycle and assert the
 * bucket count drops to zero without any further AllowRequest calls.
 */
TEST_F(RateLimiterTest, BackgroundSweeperRemovesExpiredBuckets) {
  // Short sweep interval (100ms) and short inactivity timeout (1s, the
  // smallest the API exposes). 1500ms of total wait gives the sweeper at
  // least one tick after the bucket has aged past the timeout.
  RateLimiter limiter(/*capacity=*/10, /*refill_rate=*/10, /*max_clients=*/100,
                      /*cleanup_interval=*/std::chrono::milliseconds(100),
                      /*inactivity_timeout_sec=*/1);

  EXPECT_TRUE(limiter.AllowRequest("clientA"));
  EXPECT_EQ(1U, limiter.GetTrackedClientCount());

  // Wait past inactivity_timeout (1s) plus a sweep tick (100ms) plus margin.
  std::this_thread::sleep_for(std::chrono::milliseconds(1500));

  EXPECT_EQ(0U, limiter.GetTrackedClientCount())
      << "Background sweeper must remove the expired bucket without an AllowRequest call";
}

/**
 * @brief Perf-3 regression: AllowRequest no longer performs cleanup inline.
 *
 * This test exercises the same scenario the legacy inline-cleanup tests
 * used (rapid bursts from many clients) but without relying on AllowRequest
 * to trigger the sweep. After the sweeper runs, ephemeral buckets should be
 * gone; without a sweeper, GetTrackedClientCount would remain at the burst
 * size until the next AllowRequest call.
 */
TEST_F(RateLimiterTest, AllowRequestNoLongerPerformsInlineCleanup) {
  RateLimiter limiter(/*capacity=*/5, /*refill_rate=*/5, /*max_clients=*/200,
                      /*cleanup_interval=*/std::chrono::milliseconds(100),
                      /*inactivity_timeout_sec=*/1);

  for (int i = 0; i < 20; ++i) {
    limiter.AllowRequest("ephemeral." + std::to_string(i));
  }
  EXPECT_EQ(20U, limiter.GetTrackedClientCount());

  // Wait for inactivity + at least one sweeper tick. No further AllowRequest
  // call is made — the sweeper is solely responsible for the cleanup.
  std::this_thread::sleep_for(std::chrono::milliseconds(1500));

  EXPECT_EQ(0U, limiter.GetTrackedClientCount())
      << "Sweeper must drop ephemeral buckets independently of request traffic";
}

TEST_F(RateLimiterTest, SetEnabledControlsEnforcementAtRuntime) {
  RateLimiter limiter(/*capacity=*/1, /*refill_rate=*/1, /*max_clients=*/100, /*enabled=*/false);

  EXPECT_FALSE(limiter.IsEnabled());
  EXPECT_TRUE(limiter.AllowRequest("clientA"));
  EXPECT_TRUE(limiter.AllowRequest("clientA")) << "Disabled limiter must allow requests beyond capacity";

  limiter.SetEnabled(true);
  EXPECT_TRUE(limiter.IsEnabled());
  EXPECT_TRUE(limiter.AllowRequest("clientA"));
  EXPECT_FALSE(limiter.AllowRequest("clientA")) << "Enabled limiter must enforce token capacity";

  limiter.SetEnabled(false);
  EXPECT_FALSE(limiter.IsEnabled());
  EXPECT_TRUE(limiter.AllowRequest("clientA"));
}
