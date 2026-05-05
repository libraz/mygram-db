/**
 * @file rate_limiter.cpp
 * @brief Rate limiting implementation
 */

#include "server/rate_limiter.h"

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

RateLimiter::RateLimiter(size_t capacity, size_t refill_rate, size_t max_clients,
                         std::chrono::milliseconds cleanup_interval, uint32_t inactivity_timeout_sec)
    : capacity_(capacity),
      refill_rate_(refill_rate),
      max_clients_(max_clients),
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
  // initialized before the thread observes them.
  sweeper_thread_ = std::thread(&RateLimiter::SweeperLoop, this);
}

RateLimiter::~RateLimiter() {
  // Signal stop and wake the sweeper.
  {
    std::lock_guard<std::mutex> lock(sweeper_mutex_);
    stop_.store(true, std::memory_order_release);
  }
  sweeper_cv_.notify_all();
  if (sweeper_thread_.joinable()) {
    sweeper_thread_.join();
  }
}

bool RateLimiter::AllowRequest(const std::string& client_ip) {
  std::lock_guard<std::mutex> lock(mutex_);

  // Update statistics
  total_requests_.fetch_add(1, std::memory_order_relaxed);

  // Cleanup is offloaded to a background thread to avoid O(n) latency spikes
  // on the request hot path. The previous implementation swept inside
  // AllowRequest under mutex_, blocking all rate-limit checks during the
  // sweep. AllowRequest is now strictly O(1) on the bucket count.
  auto now = std::chrono::steady_clock::now();

  // Get or create bucket for this client
  auto bucket_iter = client_buckets_.find(client_ip);
  if (bucket_iter == client_buckets_.end()) {
    // Check if we've reached max_clients limit
    if (client_buckets_.size() >= max_clients_) {
      // Enforce hard limit: reject new clients
      blocked_requests_.fetch_add(1, std::memory_order_relaxed);
      mygram::utils::StructuredLog()
          .Event("rate_limiter_max_clients")
          .Field("max_clients", static_cast<uint64_t>(max_clients_))
          .Field("client_ip", client_ip)
          .Warn();
      return false;
    }

    // Create new bucket for this client
    bucket_iter = client_buckets_.emplace(client_ip, std::make_unique<ClientBucket>(capacity_, refill_rate_)).first;
  }

  // Update last access time
  bucket_iter->second->last_access = now;

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

void RateLimiter::SweeperLoop() {
  std::unique_lock<std::mutex> lock(sweeper_mutex_);
  while (!stop_.load(std::memory_order_acquire)) {
    // wait_for releases sweeper_mutex_ while sleeping. The predicate makes
    // the wait stop-aware so the destructor doesn't have to wait a full
    // interval for shutdown.
    sweeper_cv_.wait_for(lock, cleanup_interval_, [this] { return stop_.load(std::memory_order_acquire); });
    if (stop_.load(std::memory_order_acquire)) {
      break;
    }
    // Release sweeper_mutex_ around the actual sweep so notify_all() from
    // the destructor can preempt us promptly even if the sweep is large.
    lock.unlock();
    SweepExpiredBuckets();
    lock.lock();
  }
}

void RateLimiter::SweepExpiredBuckets() {
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
  // Consistency note: AllowRequest unconditionally increments
  // total_requests_ AND exactly one of {allowed_requests_, blocked_requests_}
  // under `mutex_`, so the invariant `total == allowed + blocked` holds
  // post-write. To preserve that invariant on the lock-free GetStats path,
  // we read allowed/blocked first and derive total as their sum rather than
  // loading total directly. Reading the three counters independently across
  // a concurrent fetch_add can otherwise expose a momentary `total > allowed
  // + blocked` skew in a way the strict equality check in tests treats as a
  // bug. ResetStats still holds `mutex_` to keep its three zeros mutually
  // atomic w.r.t. AllowRequest.
  size_t tracked = 0;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    tracked = client_buckets_.size();
  }
  const uint64_t allowed = allowed_requests_.load(std::memory_order_relaxed);
  const uint64_t blocked = blocked_requests_.load(std::memory_order_relaxed);
  return Stats{.total_requests = allowed + blocked,
               .allowed_requests = allowed,
               .blocked_requests = blocked,
               .tracked_clients = tracked};
}

void RateLimiter::ResetStats() {
  // Holds `mutex_` to keep the {allowed, blocked, total} reset atomic with
  // respect to AllowRequest, which performs its three counter mutations
  // under the same lock. Without this, a ResetStats can interleave between
  // an AllowRequest's `total_requests_++` and the matching
  // `allowed_requests_++` / `blocked_requests_++`, leaving the post-reset
  // counters in the inconsistent state `total > allowed + blocked` --
  // exactly what `ResetStatsConcurrentConsistency` regresses against.
  // GetStats() reads atomics outside this lock; that is safe because each
  // load is independent and there are no inter-counter invariants the
  // observability path needs (callers that care about consistency derive it
  // from a single atomic).
  std::lock_guard<std::mutex> lock(mutex_);
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
