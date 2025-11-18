/**
 * @file signal_manager.cpp
 * @brief RAII signal handler manager implementation
 */

#include "app/signal_manager.h"

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
  } else if (signal == SIGHUP) {
    SignalManager::signal_flags_.reload_config_requested = 1;
  }
}

}  // namespace

Expected<std::unique_ptr<SignalManager>, mygram::utils::Error> SignalManager::Create() {
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

bool SignalManager::CheckAndResetConfigReload() {  // static
  if (signal_flags_.reload_config_requested != 0) {
    signal_flags_.reload_config_requested = 0;
    return true;
  }
  return false;
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

  // Register SIGHUP handler
  if (sigaction(SIGHUP, &sig_action, &original_sighup_) != 0) {
    // Restore SIGINT and SIGTERM before returning error
    sigaction(SIGINT, &original_sigint_, nullptr);
    sigaction(SIGTERM, &original_sigterm_, nullptr);
    return MakeUnexpected(
        mygram::utils::MakeError(mygram::utils::ErrorCode::kInternalError,
                                 "Failed to register SIGHUP handler: " + std::string(std::strerror(errno))));
  }

  return {};
}

void SignalManager::RestoreHandlers() {
  // Restore original signal handlers (ignore errors - best effort cleanup)
  sigaction(SIGINT, &original_sigint_, nullptr);
  sigaction(SIGTERM, &original_sigterm_, nullptr);
  sigaction(SIGHUP, &original_sighup_, nullptr);
}

}  // namespace mygramdb::app
