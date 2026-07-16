#pragma once

#include <chrono>
#include <condition_variable>
#include <mutex>

namespace mygramdb::testing {

/** One-shot gate for fixing a concurrency test at a production hook. */
class DeterministicGate {
 public:
  void ArriveAndWait() {
    std::unique_lock<std::mutex> lock(mutex_);
    arrived_ = true;
    cv_.notify_all();
    cv_.wait(lock, [this]() { return released_; });
  }

  template <typename Rep, typename Period>
  bool WaitUntilArrived(const std::chrono::duration<Rep, Period>& timeout) {
    std::unique_lock<std::mutex> lock(mutex_);
    return cv_.wait_for(lock, timeout, [this]() { return arrived_; });
  }

  void Release() {
    std::lock_guard<std::mutex> lock(mutex_);
    released_ = true;
    cv_.notify_all();
  }

 private:
  std::mutex mutex_;
  std::condition_variable cv_;
  bool arrived_ = false;
  bool released_ = false;
};

}  // namespace mygramdb::testing
