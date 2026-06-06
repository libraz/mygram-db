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
#include <condition_variable>
#include <mutex>
#include <string>

namespace mygramdb::server::replication_pause {

class Counter {
 public:
  bool RequestPause() noexcept {
    const bool first_pauser = count_.fetch_add(1, std::memory_order_acq_rel) == 0;
    if (first_pauser) {
      std::lock_guard<std::mutex> lock(drain_mutex_);
      drained_gtid_.clear();
      drain_published_ = false;
    }
    return first_pauser;
  }

  bool ReleasePause() noexcept {
    int prev = count_.fetch_sub(1, std::memory_order_acq_rel);
    if (prev <= 0) {
      count_.fetch_add(1, std::memory_order_acq_rel);
      return false;
    }
    const bool last_releaser = prev == 1;
    if (last_releaser) {
      drain_cv_.notify_all();
    }
    return last_releaser;
  }

  bool IsPaused() const noexcept { return count_.load(std::memory_order_acquire) > 0; }

  void ResetForTesting() noexcept {
    count_.store(0, std::memory_order_release);
    {
      std::lock_guard<std::mutex> lock(drain_mutex_);
      drained_gtid_.clear();
      drain_published_ = false;
    }
    drain_cv_.notify_all();
  }

  void PublishDrainedGTID(std::string gtid) {
    {
      std::lock_guard<std::mutex> lock(drain_mutex_);
      drained_gtid_ = std::move(gtid);
      drain_published_ = true;
    }
    drain_cv_.notify_all();
  }

  std::string WaitForDrainedGTID() {
    std::unique_lock<std::mutex> lock(drain_mutex_);
    drain_cv_.wait(lock, [this]() { return drain_published_ || count_.load(std::memory_order_acquire) <= 0; });
    return drained_gtid_;
  }

 private:
  std::atomic<int> count_{0};
  std::mutex drain_mutex_;
  std::condition_variable drain_cv_;
  bool drain_published_ = false;
  std::string drained_gtid_;
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
