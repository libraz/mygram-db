/**
 * @file rate_limiter.cpp
 * @brief Rate limiting implementation
 */

#include "server/rate_limiter.h"

#include <spdlog/spdlog.h>

#include <algorithm>

#include "utils/structured_log.h"

namespace mygramdb::server {

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

RateLimiter::RateLimiter(size_t capacity, size_t refill_rate, size_t max_clients, size_t cleanup_interval,
                         uint32_t inactivity_timeout_sec)
    : capacity_(capacity),
      refill_rate_(refill_rate),
      max_clients_(max_clients),
      cleanup_interval_(cleanup_interval),
      inactivity_timeout_(inactivity_timeout_sec) {
  spdlog::debug(
      "RateLimiter created: capacity={}, refill_rate={}, max_clients={}, cleanup_interval={}, "
      "inactivity_timeout={}s",
      capacity, refill_rate, max_clients, cleanup_interval, inactivity_timeout_sec);
}

bool RateLimiter::AllowRequest(const std::string& client_ip) {
  std::lock_guard<std::mutex> lock(mutex_);

  // Update statistics and check for cleanup
  bool should_cleanup = false;
  {
    std::lock_guard<std::mutex> stats_lock(stats_mutex_);
    total_requests_++;

    // Check if cleanup is needed
    if (total_requests_ % cleanup_interval_ == 0) {
      should_cleanup = true;
    }
  }

  // Periodic cleanup to prevent memory leak
  // Note: We do this while holding mutex_ to avoid race conditions
  if (should_cleanup) {
    auto now = std::chrono::steady_clock::now();
    size_t removed = 0;

    for (auto it = client_buckets_.begin(); it != client_buckets_.end();) {
      if (now - it->second->last_access > inactivity_timeout_) {
        it = client_buckets_.erase(it);
        removed++;
      } else {
        ++it;
      }
    }

    if (removed > 0) {
      spdlog::debug("Rate limiter: cleaned up {} inactive clients (total tracked: {})", removed,
                    client_buckets_.size());
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
      mygram::utils::StructuredLog()
          .Event("server_warning")
          .Field("type", "rate_limiter_max_clients")
          .Field("max_clients", static_cast<uint64_t>(max_clients_))
          .Field("client_ip", client_ip)
          .Warn();
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
    if (now - it->second->last_access > inactivity_timeout_) {
      it = client_buckets_.erase(it);
      removed++;
    } else {
      ++it;
    }
  }

  if (removed > 0) {
    spdlog::debug("Rate limiter: cleaned up {} inactive clients (total tracked: {})", removed, client_buckets_.size());
  }
}

}  // namespace mygramdb::server
