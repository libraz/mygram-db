/**
 * @file signal_manager.h
 * @brief RAII signal handler manager
 */

#ifndef MYGRAMDB_APP_SIGNAL_MANAGER_H_
#define MYGRAMDB_APP_SIGNAL_MANAGER_H_

#include <signal.h>

#include <atomic>
#include <csignal>
#include <memory>

#include "utils/error.h"
#include "utils/expected.h"

namespace mygramdb::app {

// Import Expected from utils namespace
using mygram::utils::Error;
using mygram::utils::Expected;

/**
 * @brief Signal flags (async-signal-safe)
 *
 * This struct contains only lock-free atomic flags, which signal handlers may
 * modify and any thread may read or consume without a data race, whichever
 * thread the process-directed signal is delivered to.
 *
 * Note: Global state is unavoidable for POSIX signal handlers because:
 * - Signal handlers must be async-signal-safe
 * - Cannot use mutexes, heap allocations, or member access
 */
struct SignalFlags {
  static_assert(std::atomic<int>::is_always_lock_free, "signal flags must be lock-free to be async-signal-safe");

  // NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
  std::atomic<int> shutdown_requested{0};
  // NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
  std::atomic<int> log_reopen_requested{0};
};

/**
 * @brief RAII signal handler manager
 *
 * Manages signal handler registration and cleanup.
 * Uses static storage for signal flags (unavoidable for async-signal-safety).
 *
 * Thread Safety:
 * - Signal handlers write to lock-free atomic flags
 * - Application thread reads flags via member methods (safe polling)
 * - No mutexes or locks required
 *
 * Lifecycle:
 * - Create() registers signal handlers (SIGINT, SIGTERM)
 * - Destructor restores original signal handlers
 * - RAII ensures cleanup even on exception
 *
 * Process invariant:
 * - Exactly one live SignalManager instance is supported per process.
 *   POSIX signal handlers and signal_flags_ are process-global, and each
 *   instance stores the handlers that were current at registration time.
 *   Overlapping instances would make destructor restore order ambiguous and
 *   can reinstall stale handlers.
 */
class SignalManager {
 public:
  using SigactionFunction = int (*)(int, const struct sigaction*, struct sigaction*);

  /**
   * @brief Construct and register signal handlers
   * @return Expected with manager instance or error
   *
   * This factory method creates a SignalManager and registers handlers for:
   * - SIGINT (Ctrl+C): Sets shutdown_requested flag
   * - SIGTERM (kill): Sets shutdown_requested flag
   *
   * Original signal handlers are saved and restored in destructor.
   */
  static Expected<std::unique_ptr<SignalManager>, mygram::utils::Error> Create();

  /**
   * @brief Construct and register signal handlers with an injectable syscall
   * @param sigaction_function Signal action function used to install handlers
   * @return Expected with manager instance or error
   *
   * The overload is primarily useful for exercising partial-registration
   * failures. Handler restoration always uses the real POSIX sigaction call.
   */
  static Expected<std::unique_ptr<SignalManager>, mygram::utils::Error> Create(SigactionFunction sigaction_function);

  /**
   * @brief Destructor restores original signal handlers
   */
  ~SignalManager();

  // Non-copyable, non-movable
  SignalManager(const SignalManager&) = delete;
  SignalManager& operator=(const SignalManager&) = delete;
  SignalManager(SignalManager&&) = delete;
  SignalManager& operator=(SignalManager&&) = delete;

  /**
   * @brief Check if shutdown was requested (SIGINT/SIGTERM)
   * @return True if shutdown signal was received
   *
   * This method reads the shutdown_requested flag without resetting it.
   * Typical usage: poll in main loop to detect shutdown signal.
   */
  static bool IsShutdownRequested();

  /**
   * @brief Check if log reopen was requested (SIGUSR1)
   * @return True if log reopen signal was received
   *
   * This method reads and clears the log_reopen_requested flag in one atomic
   * exchange, so a signal arriving on any thread is never lost.
   * Typical usage: poll in main loop, then reopen log files.
   * Used for log rotation: mv log log.1 && kill -USR1 pid
   */
  static bool ConsumeLogReopenRequest();

  // Static signal flags (shared with signal handler)
  // NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
  static SignalFlags signal_flags_;

 private:
  explicit SignalManager(SigactionFunction sigaction_function) : sigaction_function_(sigaction_function) {}

  /**
   * @brief Register signal handlers
   * @return Expected with void or error
   */
  Expected<void, mygram::utils::Error> RegisterHandlers();

  /**
   * @brief Restore original signal handlers
   */
  void RestoreHandlers();

  // Original signal handlers (for restoration in destructor)
  struct sigaction original_sigint_ {};
  struct sigaction original_sigterm_ {};
  struct sigaction original_sigusr1_ {};
  struct sigaction original_sigpipe_ {};

  SigactionFunction sigaction_function_;
  bool sigint_installed_ = false;
  bool sigterm_installed_ = false;
  bool sigusr1_installed_ = false;
  bool sigpipe_installed_ = false;
};

}  // namespace mygramdb::app

#endif  // MYGRAMDB_APP_SIGNAL_MANAGER_H_
