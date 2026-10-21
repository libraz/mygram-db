/**
 * @file signal_manager_test.cpp
 * @brief Tests for SignalManager SIGUSR1 log rotation support
 */

#include "app/signal_manager.h"

#include <gtest/gtest.h>
#include <sys/wait.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <thread>

namespace mygramdb::app {
namespace {

int g_sigaction_call_count = 0;
int g_sigaction_fail_at = 0;

void PriorSignalHandler(int) {}

int FailSigactionAtConfiguredCall(int signal, const struct sigaction* action, struct sigaction* old_action) {
  ++g_sigaction_call_count;
  if (g_sigaction_call_count == g_sigaction_fail_at) {
    errno = EIO;
    return -1;
  }
  return ::sigaction(signal, action, old_action);
}

bool HasExpectedDisposition(int signal, const struct sigaction& expected) {
  struct sigaction actual {};
  if (::sigaction(signal, nullptr, &actual) != 0 || actual.sa_handler != expected.sa_handler ||
      actual.sa_flags != expected.sa_flags) {
    return false;
  }
  for (const int masked_signal : {SIGINT, SIGTERM, SIGUSR1, SIGPIPE}) {
    if (sigismember(&actual.sa_mask, masked_signal) != sigismember(&expected.sa_mask, masked_signal)) {
      return false;
    }
  }
  return true;
}

int CheckRegistrationFailureRestoresDispositions() {
  constexpr std::array<int, 4> signals = {SIGINT, SIGTERM, SIGUSR1, SIGPIPE};
  std::array<struct sigaction, signals.size()> expected_actions{};

  for (size_t i = 0; i < signals.size(); ++i) {
    struct sigaction action {};
    action.sa_handler = (signals[i] == SIGPIPE) ? SIG_IGN : PriorSignalHandler;
    sigemptyset(&action.sa_mask);
    action.sa_flags = 0;
    if (::sigaction(signals[i], &action, nullptr) != 0 || ::sigaction(signals[i], nullptr, &expected_actions[i]) != 0) {
      return 10 + static_cast<int>(i);
    }
  }

  for (int fail_at = 1; fail_at <= static_cast<int>(signals.size()); ++fail_at) {
    g_sigaction_call_count = 0;
    g_sigaction_fail_at = fail_at;
    auto result = SignalManager::Create(&FailSigactionAtConfiguredCall);
    if (result.has_value()) {
      result.value().reset();
      return 20 + fail_at;
    }
    if (g_sigaction_call_count != fail_at) {
      return 30 + fail_at;
    }
    for (size_t i = 0; i < signals.size(); ++i) {
      if (!HasExpectedDisposition(signals[i], expected_actions[i])) {
        return 40 + static_cast<int>(i);
      }
    }
  }
  return 0;
}

class SignalManagerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Reset signal flags before each test
    SignalManager::signal_flags_.shutdown_requested.store(0);
    SignalManager::signal_flags_.log_reopen_requested.store(0);
  }
};

TEST_F(SignalManagerTest, RegistrationFailureRestoresOnlySuccessfullyInstalledHandlers) {
  const pid_t child = ::fork();
  ASSERT_NE(child, -1) << "fork failed";
  if (child == 0) {
    ::_exit(CheckRegistrationFailureRestoresDispositions());
  }

  int status = 0;
  ASSERT_EQ(::waitpid(child, &status, 0), child);
  ASSERT_TRUE(WIFEXITED(status));
  EXPECT_EQ(WEXITSTATUS(status), 0) << "child failure code";
}

TEST_F(SignalManagerTest, CreateSucceeds) {
  auto result = SignalManager::Create();
  ASSERT_TRUE(result.has_value());
  EXPECT_NE(result.value(), nullptr);
}

TEST_F(SignalManagerTest, ShutdownNotRequestedBeforeAnySignal) {
  auto signal_mgr = SignalManager::Create();
  ASSERT_TRUE(signal_mgr.has_value());

  EXPECT_FALSE(SignalManager::IsShutdownRequested());
}

TEST_F(SignalManagerTest, SIGINTRequestsShutdown) {
  auto signal_mgr = SignalManager::Create();
  ASSERT_TRUE(signal_mgr.has_value());

  raise(SIGINT);
  std::this_thread::sleep_for(std::chrono::milliseconds(10));

  EXPECT_TRUE(SignalManager::IsShutdownRequested());
}

TEST_F(SignalManagerTest, SIGTERMRequestsShutdown) {
  auto signal_mgr = SignalManager::Create();
  ASSERT_TRUE(signal_mgr.has_value());

  raise(SIGTERM);
  std::this_thread::sleep_for(std::chrono::milliseconds(10));

  EXPECT_TRUE(SignalManager::IsShutdownRequested());
}

/**
 * @brief The shutdown request stays set so a polling loop cannot miss it
 *
 * Unlike the log-reopen request, reading the shutdown flag does not clear it,
 * and a repeated signal cannot turn it back off.
 */
TEST_F(SignalManagerTest, RepeatedShutdownSignalsKeepTheFlagSet) {
  auto signal_mgr = SignalManager::Create();
  ASSERT_TRUE(signal_mgr.has_value());

  raise(SIGTERM);
  raise(SIGINT);
  raise(SIGTERM);
  std::this_thread::sleep_for(std::chrono::milliseconds(10));

  EXPECT_TRUE(SignalManager::IsShutdownRequested());
  EXPECT_TRUE(SignalManager::IsShutdownRequested()) << "polling must not consume the shutdown request";
}

/**
 * @brief A shutdown signal does not raise the log rotation request
 */
TEST_F(SignalManagerTest, ShutdownSignalDoesNotRequestLogReopen) {
  auto signal_mgr = SignalManager::Create();
  ASSERT_TRUE(signal_mgr.has_value());

  raise(SIGTERM);
  std::this_thread::sleep_for(std::chrono::milliseconds(10));

  EXPECT_TRUE(SignalManager::IsShutdownRequested());
  EXPECT_FALSE(SignalManager::ConsumeLogReopenRequest());
}

TEST_F(SignalManagerTest, LogReopenInitiallyFalse) {
  auto signal_mgr = SignalManager::Create();
  ASSERT_TRUE(signal_mgr.has_value());

  // Initially should be false
  EXPECT_FALSE(SignalManager::ConsumeLogReopenRequest());
}

TEST_F(SignalManagerTest, SIGUSR1SetsLogReopenFlag) {
  auto signal_mgr = SignalManager::Create();
  ASSERT_TRUE(signal_mgr.has_value());

  // Send SIGUSR1 to self
  raise(SIGUSR1);

  // Small delay for signal delivery
  std::this_thread::sleep_for(std::chrono::milliseconds(10));

  // Should now be true
  EXPECT_TRUE(SignalManager::ConsumeLogReopenRequest());
}

TEST_F(SignalManagerTest, ConsumeLogReopenRequestClearsFlag) {
  auto signal_mgr = SignalManager::Create();
  ASSERT_TRUE(signal_mgr.has_value());

  // Send SIGUSR1 to self
  raise(SIGUSR1);
  std::this_thread::sleep_for(std::chrono::milliseconds(10));

  // First consume should return true
  EXPECT_TRUE(SignalManager::ConsumeLogReopenRequest());

  // Second consume should return false (flag was cleared)
  EXPECT_FALSE(SignalManager::ConsumeLogReopenRequest());
}

TEST_F(SignalManagerTest, MultipleSIGUSR1OnlyRequiresOneConsume) {
  auto signal_mgr = SignalManager::Create();
  ASSERT_TRUE(signal_mgr.has_value());

  // Send multiple SIGUSR1
  raise(SIGUSR1);
  raise(SIGUSR1);
  raise(SIGUSR1);
  std::this_thread::sleep_for(std::chrono::milliseconds(10));

  // One consume clears the flag
  EXPECT_TRUE(SignalManager::ConsumeLogReopenRequest());
  EXPECT_FALSE(SignalManager::ConsumeLogReopenRequest());
}

TEST_F(SignalManagerTest, SIGUSR1DoesNotAffectShutdownFlag) {
  auto signal_mgr = SignalManager::Create();
  ASSERT_TRUE(signal_mgr.has_value());

  // Send SIGUSR1
  raise(SIGUSR1);
  std::this_thread::sleep_for(std::chrono::milliseconds(10));

  // Shutdown should still be false
  EXPECT_FALSE(SignalManager::IsShutdownRequested());

  // Log reopen should be true
  EXPECT_TRUE(SignalManager::ConsumeLogReopenRequest());
}

TEST_F(SignalManagerTest, ConsumeLogReopenRequestAtomicClearFlag) {
  auto signal_mgr = SignalManager::Create();
  ASSERT_TRUE(signal_mgr.has_value());

  // Directly set the flag (simulating signal delivery)
  SignalManager::signal_flags_.log_reopen_requested.store(1);

  // ConsumeLogReopenRequest should atomically read-and-clear
  EXPECT_TRUE(SignalManager::ConsumeLogReopenRequest());

  // Flag should be cleared after consume
  EXPECT_EQ(SignalManager::signal_flags_.log_reopen_requested.load(), 0);

  // Second call should return false
  EXPECT_FALSE(SignalManager::ConsumeLogReopenRequest());
}

TEST_F(SignalManagerTest, ConsumeLogReopenRequestClearsEvenWhenFalse) {
  auto signal_mgr = SignalManager::Create();
  ASSERT_TRUE(signal_mgr.has_value());

  // Flag is already 0
  EXPECT_FALSE(SignalManager::ConsumeLogReopenRequest());

  // Flag should still be 0
  EXPECT_EQ(SignalManager::signal_flags_.log_reopen_requested.load(), 0);
}

/**
 * @brief A SIGUSR1 handled on another thread while the main thread consumes is never lost
 *
 * The worker raises SIGUSR1 on itself only after the previous request was
 * consumed, so every raise must be observed by exactly one later consume.
 */
TEST_F(SignalManagerTest, LogReopenRequestFromAnotherThreadIsNeverLost) {
  auto signal_mgr = SignalManager::Create();
  ASSERT_TRUE(signal_mgr.has_value());

  constexpr int kRequests = 2000;
  std::atomic<int> consumed{0};
  std::atomic<bool> stalled{false};
  std::thread worker([&consumed, &stalled]() {
    for (int i = 1; i <= kRequests; ++i) {
      raise(SIGUSR1);
      const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
      while (consumed.load() < i) {
        if (std::chrono::steady_clock::now() > deadline) {
          stalled.store(true);
          return;
        }
        std::this_thread::yield();
      }
    }
  });

  while (consumed.load() < kRequests && !stalled.load()) {
    if (SignalManager::ConsumeLogReopenRequest()) {
      consumed.fetch_add(1);
    }
  }
  worker.join();

  EXPECT_FALSE(stalled.load()) << "a request was lost after " << consumed.load() << " consumes";
  EXPECT_EQ(consumed.load(), kRequests);
}

}  // namespace
}  // namespace mygramdb::app
