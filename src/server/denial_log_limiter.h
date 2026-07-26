/**
 * @file denial_log_limiter.h
 * @brief Bounded suppression for attacker-controlled denial log events
 */

#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_set>

namespace mygramdb::server {

/**
 * @brief Bounds denial logs while retaining an initial sample and aggregates.
 *
 * At most max_initial_logs distinct keys are emitted in each interval.
 * Repeated keys and excess distinct keys are counted. The first event after
 * the interval emits the previous interval's aggregate count.
 */
class DenialLogLimiter {
 public:
  using Clock = std::chrono::steady_clock;

  struct Decision {
    bool should_log = false;
    uint64_t suppressed_count = 0;
  };

  explicit DenialLogLimiter(std::chrono::seconds interval = std::chrono::seconds(60), size_t max_initial_logs = 10)
      : interval_(interval), max_initial_logs_(max_initial_logs), window_started_(Clock::now()) {}

  Decision Record(const std::string& key) { return RecordAt(key, Clock::now()); }

  Decision RecordAt(const std::string& key, Clock::time_point now) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (now - window_started_ >= interval_) {
      const uint64_t previous_suppressed = suppressed_count_;
      window_started_ = now;
      emitted_keys_.clear();
      suppressed_count_ = 0;
      if (max_initial_logs_ == 0) {
        suppressed_count_ = 1;
        return {};
      }
      emitted_keys_.insert(key);
      return Decision{true, previous_suppressed};
    }

    if (emitted_keys_.find(key) != emitted_keys_.end() || emitted_keys_.size() >= max_initial_logs_) {
      ++suppressed_count_;
      return {};
    }

    emitted_keys_.insert(key);
    return Decision{true, 0};
  }

 private:
  const std::chrono::seconds interval_;
  const size_t max_initial_logs_;
  Clock::time_point window_started_;
  std::unordered_set<std::string> emitted_keys_;
  uint64_t suppressed_count_ = 0;
  std::mutex mutex_;
};

}  // namespace mygramdb::server
