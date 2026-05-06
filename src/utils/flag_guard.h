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

/**
 * @brief Atomic test-and-set + scope-bound release in one move-only RAII type.
 *
 * Combines the well-known
 * @code
 *   bool expected = false;
 *   if (!flag.compare_exchange_strong(expected, true,
 *                                     std::memory_order_acq_rel,
 *                                     std::memory_order_acquire)) {
 *     return ResponseFormatter::FormatError("Already in progress");
 *   }
 *   AtomicFlagResetGuard reset_guard(flag);
 * @endcode
 * pattern that several handlers (DUMP SAVE, DUMP LOAD, automatic
 * snapshot, OPTIMIZE, ...) repeated almost verbatim. Phase 4 H-D1
 * folds the test-and-set into a single factory call:
 *
 * @code
 *   auto guard = OperationGuard::TryAcquire(ctx_.dump_save_in_progress);
 *   if (!guard.engaged()) {
 *     return ResponseFormatter::FormatError("Already in progress");
 *   }
 *   // guard auto-releases the flag on scope exit; explicit Release()
 *   // can be called before scope exit for the dismissal pattern.
 * @endcode
 *
 * Why a factory rather than a constructor?
 *   - The "engaged-or-not" outcome is conceptually a tagged union; a
 *     constructor that may or may not own a flag is awkward to reason
 *     about (especially around move semantics) and tempts callers to
 *     forget the engaged() check. The factory makes the bifurcation
 *     visible at the call site.
 *   - Disengaged guards are still useful (e.g. as a default-constructed
 *     placeholder before the actual TryAcquire call), but they should
 *     be uncommon.
 *
 * Memory ordering: TryAcquire uses acq_rel on success (matches the
 * existing handler call sites) and acquire on the failure path so a
 * concurrent release becomes visible to subsequent retries. The dtor
 * uses release, mirroring AtomicFlagResetGuard.
 *
 * Move-only: a guard owns at most one engaged-flag; copying would
 * silently double-release. Move construction transfers ownership;
 * move-assignment is deleted because none of the current call sites
 * need it and supporting it would require an additional self-release
 * branch.
 */
class OperationGuard {
 public:
  /**
   * @brief Try to atomically transition @p flag from false to true.
   * @return Engaged guard if the transition succeeded; disengaged guard
   *         otherwise. Inspect via engaged() before doing any work.
   */
  static OperationGuard TryAcquire(std::atomic<bool>& flag) noexcept {
    bool expected = false;
    if (flag.compare_exchange_strong(expected, true, std::memory_order_acq_rel, std::memory_order_acquire)) {
      return OperationGuard(&flag);
    }
    return OperationGuard();
  }

  /// Disengaged guard. Useful as a default-constructed placeholder.
  OperationGuard() noexcept = default;

  ~OperationGuard() {
    if (flag_ != nullptr && !released_) {
      flag_->store(false, std::memory_order_release);
    }
  }

  OperationGuard(const OperationGuard&) = delete;
  OperationGuard& operator=(const OperationGuard&) = delete;

  OperationGuard(OperationGuard&& other) noexcept : flag_(other.flag_), released_(other.released_) {
    other.flag_ = nullptr;
    other.released_ = true;
  }

  OperationGuard& operator=(OperationGuard&&) = delete;

  /// True iff this guard currently holds the flag.
  bool engaged() const noexcept { return flag_ != nullptr && !released_; }

  /**
   * @brief Manually release the flag and disengage the guard. Idempotent
   *        (a second Release() is a no-op).
   */
  void Release() noexcept {
    if (flag_ != nullptr && !released_) {
      flag_->store(false, std::memory_order_release);
      released_ = true;
    }
  }

  /**
   * @brief Disengage the guard WITHOUT clearing the flag. Use when ownership
   *        of the held flag is being transferred to a different release path
   *        (e.g. a worker thread that runs its own RAII reset at the end of
   *        its work).
   *
   * After Dismiss() the destructor is a no-op and engaged() returns false.
   * The flag remains true, and the new owner is responsible for clearing it.
   *
   * Symmetric with utils::ScopeGuard::Release(): Dismiss "lets go without
   * cleaning up". Use Release() (clear+disengage) when this scope is the
   * sole owner, and Dismiss() (just disengage) when ownership is moving
   * elsewhere. Mixing the two semantics in one method (the original
   * mistake during the OperationGuard rollout) caused the DUMP SAVE async
   * path to clear the flag before the worker thread observed it, allowing
   * a second concurrent DUMP SAVE to slip through.
   */
  void Dismiss() noexcept {
    flag_ = nullptr;
    released_ = true;
  }

 private:
  explicit OperationGuard(std::atomic<bool>* flag) noexcept : flag_(flag) {}

  std::atomic<bool>* flag_ = nullptr;
  bool released_ = false;
};

}  // namespace mygram::utils
