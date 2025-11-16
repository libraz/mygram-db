/**
 * @file rate_limiter.cpp
 * @brief Rate limiting implementation
 */

#include "server/rate_limiter.h"

#include <spdlog/spdlog.h>

#include <algorithm>

namespace mygramdb::server {

namespace {
// Clean up clients inactive for more than this duration
constexpr auto kClientInactivityTimeout = std::chrono::minutes(5);
// Check for cleanup every N requests
constexpr size_t kCleanupInterval = 1000;
}  // namespace

//
// TokenBucket implementation
//

TokenBucket::TokenBucket(size_t capacity, size_t refill_rate)
    : capacity_(capacity),
      refill_rate_(refill_rate),
      tokens_(static_cast<double>(capacity)),
      last_refill_(std::chrono::steady_clock::now()) {}

bool TokenBucket::TryConsume() {
  return TryConsume(1);
}

bool TokenBucket::TryConsume(size_t tokens_to_consume) {
  std::lock_guard<std::mutex> lock(mutex_);

  Refill();

  if (tokens_ >= static_cast<double>(tokens_to_consume)) {
    tokens_ -= static_cast<double>(tokens_to_consume);
    return true;
  }

  return false;
}

size_t TokenBucket::GetTokenCount() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return static_cast<size_t>(tokens_);
}

void TokenBucket::Reset() {
  std::lock_guard<std::mutex> lock(mutex_);
  tokens_ = static_cast<double>(capacity_);
  last_refill_ = std::chrono::steady_clock::now();
}

void TokenBucket::Refill() {
  auto now = std::chrono::steady_clock::now();
  auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - last_refill_).count();

  if (elapsed > 0) {
    // Add tokens based on elapsed time
    constexpr double kMillisecondsPerSecond = 1000.0;
    double tokens_to_add = (static_cast<double>(refill_rate_) * static_cast<double>(elapsed)) / kMillisecondsPerSecond;
    tokens_ = std::min(static_cast<double>(capacity_), tokens_ + tokens_to_add);
    last_refill_ = now;
  }
}

//
// RateLimiter implementation
//

RateLimiter::RateLimiter(size_t capacity, size_t refill_rate, size_t max_clients)
    : capacity_(capacity), refill_rate_(refill_rate), max_clients_(max_clients) {}

bool RateLimiter::AllowRequest(const std::string& client_ip) {
  std::lock_guard<std::mutex> lock(mutex_);

  // Update statistics
  {
    std::lock_guard<std::mutex> stats_lock(stats_mutex_);
    total_requests_++;

    // Periodic cleanup to prevent memory leak
    if (total_requests_ % kCleanupInterval == 0) {
      // Release mutex during cleanup (cleanup acquires mutex internally)
      // Note: We can't call CleanupOldClients() here because it would deadlock
      // (it tries to acquire mutex_). Instead, we'll do inline cleanup.
      auto now = std::chrono::steady_clock::now();
      for (auto it = client_buckets_.begin(); it != client_buckets_.end();) {
        if (now - it->second->last_access > kClientInactivityTimeout) {
          it = client_buckets_.erase(it);
        } else {
          ++it;
        }
      }
    }
  }

  // Get or create bucket for this client
  auto bucket_iter = client_buckets_.find(client_ip);
  if (bucket_iter == client_buckets_.end()) {
    // Check if we've reached max_clients limit
    if (client_buckets_.size() >= max_clients_) {
      // Enforce hard limit: reject new clients
      std::lock_guard<std::mutex> stats_lock(stats_mutex_);
      blocked_requests_++;
      spdlog::warn("Rate limiter: max clients ({}) reached, rejecting new client: {}", max_clients_, client_ip);
      return false;
    }

    // Create new bucket for this client
    bucket_iter = client_buckets_.emplace(client_ip, std::make_unique<ClientBucket>(capacity_, refill_rate_)).first;
  }

  // Update last access time
  bucket_iter->second->last_access = std::chrono::steady_clock::now();

  // Try to consume token
  bool allowed = bucket_iter->second->bucket->TryConsume();

  // Update statistics
  {
    std::lock_guard<std::mutex> stats_lock(stats_mutex_);
    if (allowed) {
      allowed_requests_++;
    } else {
      blocked_requests_++;
    }
  }

  return allowed;
}

RateLimiter::Stats RateLimiter::GetStats() const {
  std::lock_guard<std::mutex> stats_lock(stats_mutex_);
  std::lock_guard<std::mutex> lock(mutex_);

  return Stats{.total_requests = total_requests_,
               .allowed_requests = allowed_requests_,
               .blocked_requests = blocked_requests_,
               .tracked_clients = client_buckets_.size()};
}

void RateLimiter::ResetStats() {
  std::lock_guard<std::mutex> stats_lock(stats_mutex_);
  total_requests_ = 0;
  allowed_requests_ = 0;
  blocked_requests_ = 0;
}

void RateLimiter::Clear() {
  std::lock_guard<std::mutex> lock(mutex_);
  client_buckets_.clear();
}

void RateLimiter::CleanupOldClients() {
  std::lock_guard<std::mutex> lock(mutex_);

  auto now = std::chrono::steady_clock::now();
  size_t removed = 0;

  for (auto it = client_buckets_.begin(); it != client_buckets_.end();) {
    if (now - it->second->last_access > kClientInactivityTimeout) {
      it = client_buckets_.erase(it);
      removed++;
    } else {
      ++it;
    }
  }

  if (removed > 0) {
    spdlog::debug("Rate limiter: cleaned up {} inactive clients", removed);
  }
}

}  // namespace mygramdb::server
