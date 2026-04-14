/**
 * @file flag_guard.h
 * @brief RAII guards for atomic boolean flags
 */

#pragma once

#include <atomic>

namespace mygram::utils {

/**
 * @brief RAII guard that sets an atomic<bool> flag to true on construction
 * and resets it to false on destruction.
 *
 * Exception-safe: ensures flag is always reset even if exceptions are thrown.
 *
 * Usage:
 * @code
 * std::atomic<bool> in_progress{false};
 * {
 *   AtomicFlagGuard guard(in_progress);
 *   // in_progress is true here
 *   // ... operations ...
 * }
 * // in_progress is false here
 * @endcode
 */
class AtomicFlagGuard {
 public:
  explicit AtomicFlagGuard(std::atomic<bool>& flag) : flag_(flag) { flag_.store(true, std::memory_order_release); }

  ~AtomicFlagGuard() {
    if (!released_) {
      flag_.store(false, std::memory_order_release);
    }
  }

  /**
   * @brief Manually release the guard, clearing the flag early.
   *
   * After calling Release(), the destructor becomes a no-op.
   * This is useful when the flag must be cleared before the guard's
   * natural scope ends.
   */
  void Release() {
    if (!released_) {
      flag_.store(false, std::memory_order_release);
      released_ = true;
    }
  }

  AtomicFlagGuard(const AtomicFlagGuard&) = delete;
  AtomicFlagGuard& operator=(const AtomicFlagGuard&) = delete;
  AtomicFlagGuard(AtomicFlagGuard&&) = delete;
  AtomicFlagGuard& operator=(AtomicFlagGuard&&) = delete;

 private:
  std::atomic<bool>& flag_;
  bool released_ = false;
};

/**
 * @brief RAII guard that only resets an atomic<bool> flag to false on
 * destruction.
 *
 * Does NOT set the flag on construction. Use after successfully acquiring
 * the flag via compare_exchange_strong.
 *
 * Usage:
 * @code
 * std::atomic<bool> in_progress{false};
 * bool expected = false;
 * if (in_progress.compare_exchange_strong(expected, true)) {
 *   AtomicFlagResetGuard guard(in_progress);
 *   // ... operations ...
 * }
 * // in_progress is false here
 * @endcode
 */
class AtomicFlagResetGuard {
 public:
  explicit AtomicFlagResetGuard(std::atomic<bool>& flag) : flag_(flag) {}

  ~AtomicFlagResetGuard() {
    if (!released_) {
      flag_.store(false, std::memory_order_release);
    }
  }

  /**
   * @brief Manually release the guard, clearing the flag early.
   *
   * After calling Release(), the destructor becomes a no-op.
   */
  void Release() {
    if (!released_) {
      flag_.store(false, std::memory_order_release);
      released_ = true;
    }
  }

  AtomicFlagResetGuard(const AtomicFlagResetGuard&) = delete;
  AtomicFlagResetGuard& operator=(const AtomicFlagResetGuard&) = delete;
  AtomicFlagResetGuard(AtomicFlagResetGuard&&) = delete;
  AtomicFlagResetGuard& operator=(AtomicFlagResetGuard&&) = delete;

 private:
  std::atomic<bool>& flag_;
  bool released_ = false;
};

}  // namespace mygram::utils
