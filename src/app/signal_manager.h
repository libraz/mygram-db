/**
 * @file signal_manager.h
 * @brief RAII signal handler manager
 */

#ifndef MYGRAMDB_APP_SIGNAL_MANAGER_H_
#define MYGRAMDB_APP_SIGNAL_MANAGER_H_

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
 * This struct contains only sig_atomic_t flags that can be safely
 * modified by signal handlers. It must remain POD (Plain Old Data).
 *
 * Note: Global state is unavoidable for POSIX signal handlers because:
 * - Signal handlers must be async-signal-safe
 * - Cannot use mutexes, heap allocations, or member access
 * - sig_atomic_t provides atomic read/write guarantees
 */
struct SignalFlags {
  // NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
  volatile std::sig_atomic_t shutdown_requested = 0;
  // NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
  volatile std::sig_atomic_t log_reopen_requested = 0;
};

/**
 * @brief RAII signal handler manager
 *
 * Manages signal handler registration and cleanup.
 * Uses static storage for signal flags (unavoidable for async-signal-safety).
 *
 * Thread Safety:
 * - Signal handlers write to sig_atomic_t flags (atomic by definition)
 * - Application thread reads flags via member methods (safe polling)
 * - No mutexes or locks required
 *
 * Lifecycle:
 * - Create() registers signal handlers (SIGINT, SIGTERM)
 * - Destructor restores original signal handlers
 * - RAII ensures cleanup even on exception
 */
class SignalManager {
 public:
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
   * This method reads and clears the log_reopen_requested flag.
   * Typical usage: poll in main loop, then reopen log files.
   * Used for log rotation: mv log log.1 && kill -USR1 pid
   */
  static bool ConsumeLogReopenRequest();

  // Static signal flags (shared with signal handler)
  // NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
  static SignalFlags signal_flags_;

 private:
  SignalManager() = default;  // Private: use Create()

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
};

}  // namespace mygramdb::app

#endif  // MYGRAMDB_APP_SIGNAL_MANAGER_H_
