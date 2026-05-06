/**
 * @file replication_pause_counter.h
 * @brief Process-wide reference counter for replication-pause requests.
 *
 * Multiple long-running operations (manual DUMP SAVE, manual DUMP LOAD,
 * automatic snapshot via SnapshotScheduler) may need to pause the binlog
 * reader for the duration of their work. Without coordination they would
 * each independently call Stop() / Start() on the reader, with two failure
 * modes:
 *
 *   1. The first operation finishes and calls Start() while a second
 *      operation is still iterating the index/document store, racing the
 *      worker thread with concurrent reads.
 *   2. The boolean "replication_paused_for_dump" flag in HandlerContext is
 *      cleared by the first operation to finish, even though a second
 *      operation is still holding the pause. REPLICATION STATUS / manual
 *      REPLICATION START then misreport the dump-paused state.
 *
 * This counter centralises the bookkeeping so that the binlog reader's
 * Stop() is invoked only on the 0->1 transition and Start() only on the
 * 1->0 transition, regardless of how many operations are paused
 * simultaneously. The matching std::atomic<bool>
 * "replication_paused_for_dump" flag in HandlerContext / TcpServer remains
 * the read-only "is paused" indicator queried by replication_handler /
 * dump_handler status responses; it is updated alongside the counter (set
 * on first pause, cleared on last release).
 *
 * Phase 4 M-6 will replace these inline helpers with a proper
 * ReplicationPauseScope RAII type. For now (Phase 2 batch E) we keep the
 * helpers intentionally small and minimal so the cross-module flag type
 * surface (HandlerContext.replication_paused_for_dump,
 * SnapshotScheduler ctor parameter, ServerLifecycleManager wiring) does
 * not have to change in the same patch.
 *
 * Header-only by design: the counter has process-wide scope because there
 * is exactly one binlog reader per MygramDB process and all pausers share
 * that single reader. Wrapping the std::atomic<int> in an inline function
 * with a function-local static gives us a single shared instance across
 * translation units without adding a new .cpp to the build target.
 */

#pragma once

#include <atomic>

namespace mygramdb::server::replication_pause {

namespace detail {

/**
 * @brief Returns the process-wide pause counter.
 *
 * Wrapping a function-local static in an inline function gives us cross-
 * TU sharing without a separate .cpp; the C++ standard guarantees that
 * the function-local static is initialized exactly once and that all
 * inline-function definitions in the program refer to the same object.
 */
inline std::atomic<int>& Counter() noexcept {
  static std::atomic<int> counter{0};
  return counter;
}

}  // namespace detail

/**
 * @brief Increment the pause counter and report whether this caller is
 *        the first pauser.
 *
 * Callers must use the returned value to decide whether to invoke binlog
 * Stop() themselves: only the first pauser should stop the reader.
 *
 * acq_rel ordering on the read-modify-write ensures that side effects
 * performed by the first pauser before the 0->1 transition (e.g.
 * capturing a GTID snapshot) happen-before any later pauser observes
 * counter > 0.
 *
 * @return true if the counter transitioned from 0 to 1 (this caller is
 *         the first pauser and should call Stop() on the binlog reader).
 *         false if another operation already holds the pause; the caller
 *         must NOT call Stop() (it is already stopped).
 */
inline bool RequestPause() noexcept {
  // fetch_add returns the PRIOR value, so a return of 0 means we are the
  // first pauser (transitioned 0 -> 1).
  return detail::Counter().fetch_add(1, std::memory_order_acq_rel) == 0;
}

/**
 * @brief Decrement the pause counter and report whether this caller is
 *        the last releaser.
 *
 * @return true if the counter transitioned from 1 to 0 (this caller is
 *         the last releaser and should call Start() on the binlog reader
 *         to resume replication). false if other operations still hold
 *         the pause; the caller must NOT call Start().
 *
 * @warning It is undefined behavior to call ReleasePause() without a
 *          matching prior RequestPause(). Callers MUST pair the two using
 *          RAII (or explicit success/failure paths) so the counter is
 *          never dropped. The implementation defends against an
 *          unbalanced Release by saturating back to 0, but this is a
 *          best-effort recovery, not a contract.
 */
inline bool ReleasePause() noexcept {
  // fetch_sub returns the PRIOR value, so a return of 1 means we are the
  // last releaser (transitioned 1 -> 0). A prior value of 0 would
  // indicate an unbalanced Release — that would be a programming bug,
  // but we still saturate the counter back to 0 by undoing the underflow
  // so subsequent RequestPause behavior is well-defined. We deliberately
  // do NOT abort here because production servers should not crash on a
  // stale flag.
  int prev = detail::Counter().fetch_sub(1, std::memory_order_acq_rel);
  if (prev <= 0) {
    detail::Counter().fetch_add(1, std::memory_order_acq_rel);
    return false;
  }
  return prev == 1;
}

/**
 * @brief Returns true if any operation currently holds the pause.
 *
 * Used by tests (and potentially observability tooling) to confirm the
 * counter is in the expected state. Not used for any production decision
 * — production callers query the std::atomic<bool>
 * "replication_paused_for_dump" flag in HandlerContext directly.
 */
inline bool IsPaused() noexcept {
  return detail::Counter().load(std::memory_order_acquire) > 0;
}

/**
 * @brief Test-only: reset the counter to zero.
 *
 * Provided for test fixtures that exercise multiple paths through the
 * counter and need a clean slate between cases. Production code must
 * never call this — it would silently un-pair a real operation's
 * RequestPause() / ReleasePause() and lead to either premature Start()
 * (if reset to 0 while an operation still holds the pause) or a stuck
 * paused flag (if reset above 0 without a corresponding release).
 */
inline void ResetForTesting() noexcept {
  detail::Counter().store(0, std::memory_order_release);
}

/**
 * @brief RAII scope guard around the process-wide pause counter (Phase 4 M-6).
 *
 * Pairs RequestPause() with a guaranteed ReleasePause() on scope exit, so
 * an early return / exception between Acquire() and the manual release
 * can no longer leak counter increments. Replaces the
 * RequestPause + ScopeGuard{ReleasePause} idiom that several handlers
 * carried inline.
 *
 * Usage shape:
 * @code
 *   replication_pause::Scope pause;
 *   if (replication_was_running) {
 *     if (pause.Acquire()) {           // first pauser?
 *       binlog_reader->Stop();          // only the first pauser stops
 *       paused_flag.store(true, ...);
 *     }
 *   }
 *   ... do work ...
 *   if (replication_was_running) {
 *     if (pause.Release()) {            // last releaser?
 *       paused_flag.store(false, ...);
 *       binlog_reader->Start();
 *     }
 *   }
 *   // If Release() is omitted (early return / throw), the destructor
 *   // still drops the counter — but does NOT call Start(): callers that
 *   // need the Start side-effect must call Release() explicitly so they
 *   // see the last_releaser bool.
 * @endcode
 *
 * Move-only by design: the counter increment "belongs" to a single
 * Scope instance at any time, so allowing copies would silently
 * double-decrement on destruction.
 *
 * Calling Acquire() twice on the same Scope is a programming bug; the
 * second call is a no-op that returns false (matching ReleasePause's
 * unbalanced-call defense) so the destructor still only releases once.
 */
class Scope {
 public:
  Scope() noexcept = default;

  Scope(const Scope&) = delete;
  Scope& operator=(const Scope&) = delete;

  Scope(Scope&& other) noexcept : held_(other.held_), released_(other.released_) {
    // Transfer ownership: source must not release in its destructor.
    other.held_ = false;
    other.released_ = true;
  }

  // Move-assign would have to release the current state and then steal
  // the source's, which is more rope than current call sites need. Skip
  // until a use case appears.
  Scope& operator=(Scope&&) = delete;

  ~Scope() {
    if (held_ && !released_) {
      // Best-effort: drop the counter so we do not leak the increment.
      // Intentionally ignore the last_releaser bool; callers that need
      // to perform side-effects on the 1->0 transition (e.g. binlog
      // Start()) MUST use the explicit Release() path so they see the
      // bool. The destructor is the failure-safety net, not the primary
      // release point.
      ReleasePause();
    }
  }

  /**
   * @brief Increment the counter and report whether this scope is the
   *        first pauser.
   *
   * @return same semantics as RequestPause(): true if 0 -> 1 transition.
   *         A second Acquire() on the same Scope is a no-op returning
   *         false (defensive against double-acquire bugs).
   */
  bool Acquire() noexcept {
    if (held_) {
      return false;  // Already held; do not double-increment.
    }
    held_ = true;
    return RequestPause();
  }

  /**
   * @brief Explicit release. Decrements the counter and reports whether
   *        this scope is the last releaser.
   *
   * After Release() the destructor will not release again. Calling
   * Release() without a prior Acquire(), or calling Release() more than
   * once, is a no-op returning false.
   *
   * @return same semantics as ReleasePause(): true if 1 -> 0 transition.
   */
  bool Release() noexcept {
    if (!held_ || released_) {
      return false;
    }
    released_ = true;
    return ReleasePause();
  }

  /// True if Acquire() has been called and Release() has not yet run.
  bool held() const noexcept { return held_ && !released_; }

 private:
  bool held_ = false;
  bool released_ = false;
};

}  // namespace mygramdb::server::replication_pause
