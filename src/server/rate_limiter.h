/**
 * @file rate_limiter.h
 * @brief Rate limiting using token bucket algorithm
 */

#pragma once

#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

namespace mygramdb::server {

/**
 * @brief Token bucket for rate limiting
 *
 * Implements the token bucket algorithm for rate limiting.
 * Tokens are added at a fixed rate up to a maximum capacity.
 * Each request consumes one token.
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

  size_t capacity_;                                    ///< Maximum tokens
  size_t refill_rate_;                                 ///< Tokens per second
  double tokens_;                                      ///< Current tokens (float for fractional refill)
  std::chrono::steady_clock::time_point last_refill_;  ///< Last refill time
  mutable std::mutex mutex_;                           ///< Protects token count
};

/**
 * @brief Per-client IP rate limiter
 *
 * Maintains separate token buckets for each client IP address.
 * Automatically cleans up old entries to prevent memory leaks.
 */
class RateLimiter {
 public:
  static constexpr size_t kDefaultMaxClients = 10000;         ///< Default maximum number of tracked clients
  static constexpr size_t kDefaultCleanupInterval = 1000;     ///< Default cleanup interval (requests)
  static constexpr uint32_t kDefaultInactivityTimeout = 300;  ///< Default inactivity timeout (seconds)

  /**
   * @brief Construct rate limiter
   * @param capacity Maximum tokens per client (burst size)
   * @param refill_rate Tokens added per second per client
   * @param max_clients Maximum number of tracked clients (for memory management)
   * @param cleanup_interval Cleanup check interval (number of requests)
   * @param inactivity_timeout Client inactivity timeout in seconds
   */
  RateLimiter(size_t capacity, size_t refill_rate, size_t max_clients = kDefaultMaxClients,
              size_t cleanup_interval = kDefaultCleanupInterval,
              uint32_t inactivity_timeout_sec = kDefaultInactivityTimeout);

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
    size_t total_requests = 0;    ///< Total requests checked
    size_t allowed_requests = 0;  ///< Requests allowed
    size_t blocked_requests = 0;  ///< Requests blocked (rate limited)
    size_t tracked_clients = 0;   ///< Number of clients currently tracked
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

 private:
  /**
   * @brief Clean up old client entries to prevent memory leak
   */
  void CleanupOldClients();

  size_t capacity_;                          ///< Token bucket capacity
  size_t refill_rate_;                       ///< Refill rate (tokens/sec)
  size_t max_clients_;                       ///< Maximum tracked clients
  size_t cleanup_interval_;                  ///< Cleanup check interval (requests)
  std::chrono::seconds inactivity_timeout_;  ///< Client inactivity timeout

  struct ClientBucket {
    std::unique_ptr<TokenBucket> bucket;
    std::chrono::steady_clock::time_point last_access;

    ClientBucket(size_t capacity, size_t refill_rate)
        : bucket(std::make_unique<TokenBucket>(capacity, refill_rate)), last_access(std::chrono::steady_clock::now()) {}
  };

  std::unordered_map<std::string, std::unique_ptr<ClientBucket>> client_buckets_;  ///< Per-client buckets
  mutable std::mutex mutex_;                                                       ///< Protects client_buckets_

  // Statistics
  mutable std::mutex stats_mutex_;
  size_t total_requests_ = 0;
  size_t allowed_requests_ = 0;
  size_t blocked_requests_ = 0;
};

}  // namespace mygramdb::server
