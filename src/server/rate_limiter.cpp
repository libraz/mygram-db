/**
 * @file rate_limiter.cpp
 * @brief Rate limiting implementation
 */

#include "server/rate_limiter.h"

#include <spdlog/spdlog.h>

#include <algorithm>

#include "utils/constants.h"
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
  Refill();

  if (tokens_ >= static_cast<double>(tokens_to_consume)) {
    tokens_ -= static_cast<double>(tokens_to_consume);
    return true;
  }

  return false;
}

size_t TokenBucket::GetTokenCount() const {
  return static_cast<size_t>(tokens_);
}

void TokenBucket::Reset() {
  tokens_ = static_cast<double>(capacity_);
  last_refill_ = std::chrono::steady_clock::now();
}

void TokenBucket::Refill() {
  auto now = std::chrono::steady_clock::now();
  auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - last_refill_).count();

  if (elapsed > 0) {
    // Add tokens based on elapsed time
    double tokens_to_add = (static_cast<double>(refill_rate_) * static_cast<double>(elapsed)) /
                           static_cast<double>(mygram::constants::kMillisecondsPerSecond);
    tokens_ = std::min(static_cast<double>(capacity_), tokens_ + tokens_to_add);
    last_refill_ = now;
  }
}

//
// RateLimiter implementation
//

RateLimiter::RateLimiter(size_t capacity, size_t refill_rate, size_t max_clients, std::chrono::seconds cleanup_interval,
                         uint32_t inactivity_timeout_sec)
    : capacity_(capacity),
      refill_rate_(refill_rate),
      max_clients_(max_clients),
      cleanup_interval_(cleanup_interval),
      inactivity_timeout_(inactivity_timeout_sec),
      last_cleanup_time_(std::chrono::steady_clock::now()) {
  mygram::utils::StructuredLog()
      .Event("rate_limiter_created")
      .Field("capacity", static_cast<uint64_t>(capacity))
      .Field("refill_rate", static_cast<uint64_t>(refill_rate))
      .Field("max_clients", static_cast<uint64_t>(max_clients))
      .Field("cleanup_interval_sec", static_cast<uint64_t>(cleanup_interval.count()))
      .Field("inactivity_timeout_sec", static_cast<uint64_t>(inactivity_timeout_sec))
      .Debug();
}

bool RateLimiter::AllowRequest(const std::string& client_ip) {
  std::lock_guard<std::mutex> lock(mutex_);

  // Update statistics
  total_requests_.fetch_add(1, std::memory_order_relaxed);

  // Time-based cleanup to prevent memory leak
  // Note: We do this while holding mutex_ to avoid race conditions
  auto now = std::chrono::steady_clock::now();
  bool should_cleanup = (now - last_cleanup_time_ >= cleanup_interval_);

  if (should_cleanup) {
    last_cleanup_time_ = now;
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
      mygram::utils::StructuredLog()
          .Event("rate_limiter_cleanup")
          .Field("removed_clients", static_cast<uint64_t>(removed))
          .Field("total_tracked", static_cast<uint64_t>(client_buckets_.size()))
          .Debug();
    }
  }

  // Get or create bucket for this client
  auto bucket_iter = client_buckets_.find(client_ip);
  if (bucket_iter == client_buckets_.end()) {
    // Check if we've reached max_clients limit
    if (client_buckets_.size() >= max_clients_) {
      // Enforce hard limit: reject new clients
      blocked_requests_.fetch_add(1, std::memory_order_relaxed);
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
  if (allowed) {
    allowed_requests_.fetch_add(1, std::memory_order_relaxed);
  } else {
    blocked_requests_.fetch_add(1, std::memory_order_relaxed);
  }

  return allowed;
}

RateLimiter::Stats RateLimiter::GetStats() const {
  std::lock_guard<std::mutex> lock(mutex_);

  return Stats{.total_requests = total_requests_.load(std::memory_order_relaxed),
               .allowed_requests = allowed_requests_.load(std::memory_order_relaxed),
               .blocked_requests = blocked_requests_.load(std::memory_order_relaxed),
               .tracked_clients = client_buckets_.size()};
}

void RateLimiter::ResetStats() {
  total_requests_.store(0, std::memory_order_relaxed);
  allowed_requests_.store(0, std::memory_order_relaxed);
  blocked_requests_.store(0, std::memory_order_relaxed);
}

void RateLimiter::Clear() {
  std::lock_guard<std::mutex> lock(mutex_);
  client_buckets_.clear();
}

void RateLimiter::UpdateParameters(size_t capacity, size_t refill_rate) {
  std::lock_guard<std::mutex> lock(mutex_);
  capacity_ = capacity;
  refill_rate_ = refill_rate;
  // Existing client buckets keep their old parameters
  // New clients will use the updated parameters
}

}  // namespace mygramdb::server
