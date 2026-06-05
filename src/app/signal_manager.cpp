/**
 * @file signal_manager.cpp
 * @brief RAII signal handler manager implementation
 */

#include "app/signal_manager.h"

#include <csignal>
#include <cstring>  // for memset

namespace mygramdb::app {

// Define static signal flags
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
SignalFlags SignalManager::signal_flags_;

namespace {

/**
 * @brief Async-signal-safe signal handler
 * @param signal Signal number
 *
 * This handler ONLY sets atomic flags. It performs NO other operations:
 * - No mutex locks
 * - No heap allocations
 * - No function calls (except assignment to sig_atomic_t)
 * - No logging
 *
 * This is required for async-signal-safety (POSIX.1-2008 compliance).
 */
void SignalHandlerFunction(int signal) {
  if (signal == SIGINT || signal == SIGTERM) {
    SignalManager::signal_flags_.shutdown_requested = 1;
  } else if (signal == SIGUSR1) {
    SignalManager::signal_flags_.log_reopen_requested = 1;
  }
}

}  // namespace

Expected<std::unique_ptr<SignalManager>, mygram::utils::Error> SignalManager::Create() {
  // Process invariant: SignalManager is designed for one live instance per
  // process. POSIX handlers are process-global and this instance captures the
  // previous handlers for RAII restoration; overlapping instances would make
  // destructor restore order ambiguous.
  auto manager = std::unique_ptr<SignalManager>(new SignalManager());

  auto register_result = manager->RegisterHandlers();
  if (!register_result) {
    return MakeUnexpected(register_result.error());
  }

  return manager;
}

SignalManager::~SignalManager() {
  RestoreHandlers();
}

bool SignalManager::IsShutdownRequested() {  // static
  return signal_flags_.shutdown_requested != 0;
}

bool SignalManager::ConsumeLogReopenRequest() {  // static
  // Block SIGUSR1 around the check-and-clear to prevent a signal from arriving
  // between the read and the write (non-atomic read-modify-write on sig_atomic_t).
  sigset_t block_set;
  sigset_t old_set;
  sigemptyset(&block_set);
  sigaddset(&block_set, SIGUSR1);
  pthread_sigmask(SIG_BLOCK, &block_set, &old_set);

  bool was_requested = (signal_flags_.log_reopen_requested != 0);
  signal_flags_.log_reopen_requested = 0;

  pthread_sigmask(SIG_SETMASK, &old_set, nullptr);
  return was_requested;
}

Expected<void, mygram::utils::Error> SignalManager::RegisterHandlers() {
  // Prepare signal action structure
  struct sigaction sig_action {};
  std::memset(&sig_action, 0, sizeof(sig_action));
  sig_action.sa_handler = SignalHandlerFunction;
  sigemptyset(&sig_action.sa_mask);
  sig_action.sa_flags = 0;  // No special flags

  // Register SIGINT handler
  if (sigaction(SIGINT, &sig_action, &original_sigint_) != 0) {
    return MakeUnexpected(
        mygram::utils::MakeError(mygram::utils::ErrorCode::kInternalError,
                                 "Failed to register SIGINT handler: " + std::string(std::strerror(errno))));
  }

  // Register SIGTERM handler
  if (sigaction(SIGTERM, &sig_action, &original_sigterm_) != 0) {
    // Restore SIGINT before returning error
    sigaction(SIGINT, &original_sigint_, nullptr);
    return MakeUnexpected(
        mygram::utils::MakeError(mygram::utils::ErrorCode::kInternalError,
                                 "Failed to register SIGTERM handler: " + std::string(std::strerror(errno))));
  }

  // Register SIGUSR1 handler (for log rotation)
  if (sigaction(SIGUSR1, &sig_action, &original_sigusr1_) != 0) {
    // Restore SIGINT and SIGTERM before returning error
    sigaction(SIGINT, &original_sigint_, nullptr);
    sigaction(SIGTERM, &original_sigterm_, nullptr);
    return MakeUnexpected(
        mygram::utils::MakeError(mygram::utils::ErrorCode::kInternalError,
                                 "Failed to register SIGUSR1 handler: " + std::string(std::strerror(errno))));
  }

  // Ignore SIGPIPE to prevent crashes when writing to closed connections
  // writev() doesn't support MSG_NOSIGNAL, so we must ignore SIGPIPE process-wide
  // macOS also uses SO_NOSIGPIPE per-socket, but this provides a fallback
  struct sigaction sigpipe_action {};
  std::memset(&sigpipe_action, 0, sizeof(sigpipe_action));
  sigpipe_action.sa_handler = SIG_IGN;
  sigemptyset(&sigpipe_action.sa_mask);
  sigpipe_action.sa_flags = 0;

  if (sigaction(SIGPIPE, &sigpipe_action, &original_sigpipe_) != 0) {
    // Restore all handlers before returning error
    sigaction(SIGINT, &original_sigint_, nullptr);
    sigaction(SIGTERM, &original_sigterm_, nullptr);
    sigaction(SIGUSR1, &original_sigusr1_, nullptr);
    return MakeUnexpected(
        mygram::utils::MakeError(mygram::utils::ErrorCode::kInternalError,
                                 "Failed to register SIGPIPE handler: " + std::string(std::strerror(errno))));
  }

  return {};
}

void SignalManager::RestoreHandlers() {
  // Restore original signal handlers (ignore errors - best effort cleanup)
  sigaction(SIGINT, &original_sigint_, nullptr);
  sigaction(SIGTERM, &original_sigterm_, nullptr);
  sigaction(SIGUSR1, &original_sigusr1_, nullptr);
  sigaction(SIGPIPE, &original_sigpipe_, nullptr);
}

}  // namespace mygramdb::app
