/**
 * @file rate_limiter.cpp
 * @brief Rate limiting implementation
 */

#include "server/rate_limiter.h"

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

void TokenBucket::UpdateParameters(size_t capacity, size_t refill_rate) {
  Refill();
  capacity_ = capacity;
  refill_rate_ = refill_rate;
  tokens_ = std::min(tokens_, static_cast<double>(capacity_));
}

void TokenBucket::RewindLastRefillForTesting(std::chrono::microseconds delta) {
  last_refill_ -= delta;
}

void TokenBucket::Refill() {
  auto now = std::chrono::steady_clock::now();
  std::chrono::duration<double> elapsed = now - last_refill_;

  if (elapsed.count() > 0) {
    // Add tokens based on elapsed time
    double tokens_to_add = static_cast<double>(refill_rate_) * elapsed.count();
    tokens_ = std::min(static_cast<double>(capacity_), tokens_ + tokens_to_add);
    last_refill_ = now;
  }
}

//
// RateLimiter implementation
//

RateLimiter::RateLimiter(size_t capacity, size_t refill_rate, size_t max_clients, bool enabled,
                         std::chrono::milliseconds cleanup_interval, uint32_t inactivity_timeout_sec)
    : capacity_(capacity),
      refill_rate_(refill_rate),
      max_clients_(max_clients),
      enabled_(enabled),
      cleanup_interval_(cleanup_interval),
      inactivity_timeout_(inactivity_timeout_sec) {
  mygram::utils::StructuredLog()
      .Event("rate_limiter_created")
      .Field("capacity", static_cast<uint64_t>(capacity))
      .Field("refill_rate", static_cast<uint64_t>(refill_rate))
      .Field("max_clients", static_cast<uint64_t>(max_clients))
      .Field("cleanup_interval_ms", static_cast<uint64_t>(cleanup_interval.count()))
      .Field("inactivity_timeout_sec", static_cast<uint64_t>(inactivity_timeout_sec))
      .Debug();

  // Start background sweeper. Started last so all members are fully
  // initialized before the worker observes them. Start failure here is
  // logged by PeriodicWorker itself; we deliberately ignore the
  // Expected return because (a) failure modes are interval<=0 (compile-
  // time guarded by kDefaultCleanupInterval) or already-running (a
  // fresh PeriodicWorker can never be), so the only realistic failure
  // is OOM during std::thread construction, which we cannot recover
  // from at this layer.
  (void)sweeper_.Start([this] { SweepExpiredBuckets(); }, cleanup_interval_);
}

RateLimiter::RateLimiter(size_t capacity, size_t refill_rate, size_t max_clients,
                         std::chrono::milliseconds cleanup_interval, uint32_t inactivity_timeout_sec)
    : RateLimiter(capacity, refill_rate, max_clients, true, cleanup_interval, inactivity_timeout_sec) {}

RateLimiter::~RateLimiter() {
  sweeper_.Stop();
}

bool RateLimiter::AllowRequest(const std::string& client_ip) {
  if (!enabled_.load(std::memory_order_acquire)) {
    allowed_requests_.fetch_add(1, std::memory_order_relaxed);
    return true;
  }

  std::lock_guard<std::mutex> lock(mutex_);

  // Cleanup is offloaded to a background thread to avoid O(n) latency spikes
  // on the request hot path. The previous implementation swept inside
  // AllowRequest under mutex_, blocking all rate-limit checks during the
  // sweep. AllowRequest is now strictly O(1) on the bucket count.
  auto now = std::chrono::steady_clock::now();

  // Get or create bucket for this client
  auto bucket_iter = client_buckets_.find(client_ip);
  if (bucket_iter == client_buckets_.end()) {
    // max_clients_ bounds memory, not admission. A client with no bucket has
    // consumed no tokens, so denying it here would report "rate limit
    // exceeded" for a condition the client had no part in. Make room by
    // dropping the least-recently-seen bucket instead; that client is either
    // gone or will simply be re-tracked with a fresh allowance on its next
    // request, which is what the periodic sweep would have done anyway.
    while (client_buckets_.size() >= max_clients_ && !lru_order_.empty()) {
      client_buckets_.erase(lru_order_.back());
      lru_order_.pop_back();
      evicted_clients_.fetch_add(1, std::memory_order_relaxed);
      const auto decision = denial_log_limiter_.Record("rate-limiter-evict");
      if (decision.should_log) {
        mygram::utils::StructuredLog()
            .Event("rate_limiter_client_evicted")
            .Field("max_clients", static_cast<uint64_t>(max_clients_))
            .Field("client_ip", client_ip)
            .Field("suppressed_since_last_log", decision.suppressed_count)
            .Warn();
      }
    }

    // Create new bucket for this client
    bucket_iter = client_buckets_.try_emplace(client_ip, capacity_, refill_rate_).first;
    lru_order_.push_front(client_ip);
    bucket_iter->second.lru_position = lru_order_.begin();
  } else {
    lru_order_.splice(lru_order_.begin(), lru_order_, bucket_iter->second.lru_position);
  }

  // Update last access time
  bucket_iter->second.last_access = now;

  // Try to consume token
  bool allowed = bucket_iter->second.bucket.TryConsume();

  // Update statistics
  if (allowed) {
    allowed_requests_.fetch_add(1, std::memory_order_relaxed);
  } else {
    blocked_requests_.fetch_add(1, std::memory_order_relaxed);
  }

  return allowed;
}

void RateLimiter::SweepExpiredBuckets() {
  std::lock_guard<std::mutex> lock(mutex_);

  auto now = std::chrono::steady_clock::now();
  size_t removed = 0;

  for (auto it = client_buckets_.begin(); it != client_buckets_.end();) {
    if (now - it->second.last_access > inactivity_timeout_) {
      lru_order_.erase(it->second.lru_position);
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

size_t RateLimiter::GetTrackedClientCount() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return client_buckets_.size();
}

RateLimiter::Stats RateLimiter::GetStats() const {
  // Acquire `mutex_` only long enough to snapshot `client_buckets_.size()`
  // (the one piece of data not already exposed via atomics). The atomic
  // counter loads happen outside the lock so /info, /metrics, and similar
  // observability endpoints do not contend with the AllowRequest hot path.
  //
  // Derive total from the same allowed/blocked snapshots returned to callers,
  // so the reported total always equals their sum.
  size_t tracked = 0;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    tracked = client_buckets_.size();
  }
  const uint64_t allowed = allowed_requests_.load(std::memory_order_relaxed);
  const uint64_t blocked = blocked_requests_.load(std::memory_order_relaxed);
  Stats stats{};
  stats.total_requests = allowed + blocked;
  stats.allowed_requests = allowed;
  stats.blocked_requests = blocked;
  stats.tracked_clients = tracked;
  stats.evicted_clients = evicted_clients_.load(std::memory_order_relaxed);
  return stats;
}

void RateLimiter::ResetStats() {
  // Keep enabled AllowRequest counter updates on one side of the reset. The
  // disabled fast path updates only allowed_requests_ atomically and does not
  // take mutex_. GetStats derives total_requests from the two counter values.
  std::lock_guard<std::mutex> lock(mutex_);
  allowed_requests_.store(0, std::memory_order_relaxed);
  blocked_requests_.store(0, std::memory_order_relaxed);
}

void RateLimiter::Clear() {
  std::lock_guard<std::mutex> lock(mutex_);
  client_buckets_.clear();
  lru_order_.clear();
}

void RateLimiter::UpdateParameters(size_t capacity, size_t refill_rate) {
  std::lock_guard<std::mutex> lock(mutex_);
  capacity_ = capacity;
  refill_rate_ = refill_rate;
  for (auto& [client_ip, client_bucket] : client_buckets_) {
    (void)client_ip;
    client_bucket.bucket.UpdateParameters(capacity, refill_rate);
  }
}

void RateLimiter::SetEnabled(bool enabled) {
  enabled_.store(enabled, std::memory_order_release);
  if (!enabled) {
    Clear();
  }
}

}  // namespace mygramdb::server
