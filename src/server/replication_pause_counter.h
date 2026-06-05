/**
 * @file replication_pause_counter.h
 * @brief Reference counter for replication-pause requests.
 *
 * Multiple long-running operations (manual DUMP SAVE, manual DUMP LOAD,
 * automatic snapshot via SnapshotScheduler) may need to pause the binlog
 * reader for the duration of their work. A shared ReplicationPauseCounter
 * coordinates those operations so Stop() is invoked only on the 0->1
 * transition and Start() only on the 1->0 transition.
 *
 * The counter is intentionally instance-owned, not process-global. TcpServer
 * owns one counter and injects it into DumpHandler and SnapshotScheduler so
 * tests and independent server instances cannot contaminate each other.
 */

#pragma once

#include <atomic>

namespace mygramdb::server::replication_pause {

class Counter {
 public:
  bool RequestPause() noexcept { return count_.fetch_add(1, std::memory_order_acq_rel) == 0; }

  bool ReleasePause() noexcept {
    int prev = count_.fetch_sub(1, std::memory_order_acq_rel);
    if (prev <= 0) {
      count_.fetch_add(1, std::memory_order_acq_rel);
      return false;
    }
    return prev == 1;
  }

  bool IsPaused() const noexcept { return count_.load(std::memory_order_acquire) > 0; }

  void ResetForTesting() noexcept { count_.store(0, std::memory_order_release); }

 private:
  std::atomic<int> count_{0};
};

class Scope {
 public:
  explicit Scope(Counter& counter) noexcept : counter_(&counter) {}

  Scope(const Scope&) = delete;
  Scope& operator=(const Scope&) = delete;

  Scope(Scope&& other) noexcept : counter_(other.counter_), held_(other.held_), released_(other.released_) {
    other.counter_ = nullptr;
    other.held_ = false;
    other.released_ = true;
  }

  Scope& operator=(Scope&&) = delete;

  ~Scope() {
    if (counter_ != nullptr && held_ && !released_) {
      counter_->ReleasePause();
    }
  }

  bool Acquire() noexcept {
    if (counter_ == nullptr || held_) {
      return false;
    }
    held_ = true;
    return counter_->RequestPause();
  }

  bool Release() noexcept {
    if (counter_ == nullptr || !held_ || released_) {
      return false;
    }
    released_ = true;
    return counter_->ReleasePause();
  }

  bool held() const noexcept { return held_ && !released_; }

 private:
  Counter* counter_;
  bool held_ = false;
  bool released_ = false;
};

}  // namespace mygramdb::server::replication_pause
