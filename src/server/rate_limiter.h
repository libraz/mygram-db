/**
 * @file rate_limiter.h
 * @brief Rate limiting using token bucket algorithm
 */

#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>

namespace mygramdb::server {

/**
 * @brief Token bucket for rate limiting
 *
 * Implements the token bucket algorithm for rate limiting.
 * Tokens are added at a fixed rate up to a maximum capacity.
 * Each request consumes one token.
 *
 * Not thread-safe on its own. Callers must hold an external lock
 * (e.g., RateLimiter::mutex_) before accessing any member function.
 */
class TokenBucket {
 public:
  /**
   * @brief Construct token bucket
   * @param capacity Maximum number of tokens (burst size)
   * @param refill_rate Tokens added per second
   */
  TokenBucket(size_t capacity, size_t refill_rate);

  /**
   * @brief Try to consume one token
   * @return true if token was available and consumed, false otherwise
   */
  bool TryConsume();

  /**
   * @brief Try to consume N tokens
   * @param tokens Number of tokens to consume
   * @return true if tokens were available and consumed, false otherwise
   */
  bool TryConsume(size_t tokens);

  /**
   * @brief Get current token count
   */
  size_t GetTokenCount() const;

  /**
   * @brief Reset bucket to full capacity
   */
  void Reset();

 private:
  /**
   * @brief Refill tokens based on elapsed time
   */
  void Refill();

  size_t capacity_;     ///< Maximum tokens
  size_t refill_rate_;  ///< Tokens per second
  // tokens_ is double precision intentionally. The fractional component
  // represents partial token accumulation between refills. Tests should NOT
  // assert exact equality on token counts -- floating-point determinism is
  // not guaranteed across compilers/platforms. Use approximate comparisons
  // (e.g. EXPECT_NEAR) where token counts matter.
  double tokens_;                                      ///< Current tokens (float for fractional refill)
  std::chrono::steady_clock::time_point last_refill_;  ///< Last refill time
};

/**
 * @brief Per-client IP rate limiter
 *
 * Maintains separate token buckets for each client IP address.
 * Automatically cleans up old entries to prevent memory leaks.
 */
class RateLimiter {
 public:
  static constexpr size_t kDefaultMaxClients = 10000;  ///< Default maximum number of tracked clients
  static constexpr std::chrono::milliseconds kDefaultCleanupInterval{60000};  ///< Default cleanup interval (60s)
  static constexpr uint32_t kDefaultInactivityTimeout = 300;                  ///< Default inactivity timeout (seconds)

  /**
   * @brief Construct rate limiter
   * @param capacity Maximum tokens per client (burst size)
   * @param refill_rate Tokens added per second per client
   * @param max_clients Maximum number of tracked clients (for memory management)
   * @param cleanup_interval Cleanup check interval (time between cleanup sweeps).
   *        Accepts millisecond resolution so tests can run a faster sweep
   *        cycle; production callers may continue to pass `std::chrono::seconds`
   *        which converts implicitly.
   * @param inactivity_timeout Client inactivity timeout in seconds
   */
  RateLimiter(size_t capacity, size_t refill_rate, size_t max_clients = kDefaultMaxClients,
              std::chrono::milliseconds cleanup_interval = kDefaultCleanupInterval,
              uint32_t inactivity_timeout_sec = kDefaultInactivityTimeout);

  /**
   * @brief Destructor - stops the background sweeper thread.
   */
  ~RateLimiter();

  // Non-copyable, non-movable (owns a background thread).
  RateLimiter(const RateLimiter&) = delete;
  RateLimiter& operator=(const RateLimiter&) = delete;
  RateLimiter(RateLimiter&&) = delete;
  RateLimiter& operator=(RateLimiter&&) = delete;

  /**
   * @brief Check if request from client_ip is allowed
   * @param client_ip Client IP address
   * @return true if request is allowed, false if rate limited
   */
  bool AllowRequest(const std::string& client_ip);

  /**
   * @brief Update rate limiting parameters for new clients
   * @param capacity New maximum tokens per client (burst size)
   * @param refill_rate New tokens added per second per client
   *
   * Note: This updates the parameters used for creating new TokenBuckets.
   * Existing client buckets are not affected.
   */
  void UpdateParameters(size_t capacity, size_t refill_rate);

  /**
   * @brief Get statistics for monitoring
   */
  struct Stats {
    uint64_t total_requests = 0;    ///< Total requests checked
    uint64_t allowed_requests = 0;  ///< Requests allowed
    uint64_t blocked_requests = 0;  ///< Requests blocked (rate limited)
    size_t tracked_clients = 0;     ///< Number of clients currently tracked
  };

  /**
   * @brief Get rate limiter statistics
   */
  Stats GetStats() const;

  /**
   * @brief Reset statistics
   */
  void ResetStats();

  /**
   * @brief Clear all client buckets
   */
  void Clear();

  /**
   * @brief Number of clients currently tracked (for tests / observability).
   *
   * Equivalent to `GetStats().tracked_clients` but exposed directly so tests
   * can introspect cleanup behavior without copying the full Stats struct.
   */
  [[nodiscard]] size_t GetTrackedClientCount() const;

 private:
  size_t capacity_;                             ///< Token bucket capacity
  size_t refill_rate_;                          ///< Refill rate (tokens/sec)
  size_t max_clients_;                          ///< Maximum tracked clients
  std::chrono::milliseconds cleanup_interval_;  ///< Cleanup check interval (time)
  std::chrono::seconds inactivity_timeout_;     ///< Client inactivity timeout

  struct ClientBucket {
    std::unique_ptr<TokenBucket> bucket;
    std::chrono::steady_clock::time_point last_access;

    ClientBucket(size_t capacity, size_t refill_rate)
        : bucket(std::make_unique<TokenBucket>(capacity, refill_rate)), last_access(std::chrono::steady_clock::now()) {}
  };

  std::unordered_map<std::string, std::unique_ptr<ClientBucket>> client_buckets_;  ///< Per-client buckets
  mutable std::mutex mutex_;                                                       ///< Protects client_buckets_

  // Statistics (atomic counters avoid the need for a separate stats_mutex_)
  std::atomic<uint64_t> total_requests_{0};
  std::atomic<uint64_t> allowed_requests_{0};
  std::atomic<uint64_t> blocked_requests_{0};

  // Background sweeper. Cleanup is offloaded to a dedicated thread to avoid
  // O(n) latency spikes on the request hot path. The previous implementation
  // swept inside AllowRequest under mutex_, blocking all rate-limit checks
  // (including for unrelated clients) while every expired bucket was removed.
  // Now AllowRequest is strictly O(1) on the bucket count.
  std::thread sweeper_thread_;
  std::mutex sweeper_mutex_;
  std::condition_variable sweeper_cv_;
  std::atomic<bool> stop_{false};

  /**
   * @brief Sweeper thread loop.
   *
   * Wakes every `cleanup_interval_` to remove buckets older than
   * `inactivity_timeout_`. Wakes immediately when `stop_` is set so the
   * destructor doesn't have to wait a full interval.
   */
  void SweeperLoop();

  /**
   * @brief Single sweep over client_buckets_, removing expired entries.
   *
   * Caller must NOT hold `mutex_`. Acquires `mutex_` internally.
   */
  void SweepExpiredBuckets();
};

}  // namespace mygramdb::server
