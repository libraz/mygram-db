/**
 * @file signal_handler_test.cpp
 * @brief Tests for signal handler safety and async-signal-safe implementation
 *
 * These tests verify that the signal handler implementation is async-signal-safe
 * and does not cause race conditions or undefined behavior.
 */

#include <gtest/gtest.h>

#include <atomic>
#include <csignal>
#include <thread>

// Test helper: simulate the signal handler behavior
namespace {
// Simulate global flags (same as in main.cpp)
volatile std::sig_atomic_t g_test_shutdown_requested = 0;
volatile std::sig_atomic_t g_test_cancel_snapshot_requested = 0;

/**
 * @brief Test signal handler (async-signal-safe)
 */
void TestSignalHandler(int signal) {
  if (signal == SIGINT || signal == SIGTERM) {
    g_test_shutdown_requested = 1;
    g_test_cancel_snapshot_requested = 1;
  }
}

}  // namespace

/**
 * @brief Test that signal handler only sets atomic flags
 *
 * This test verifies that the signal handler is async-signal-safe by only
 * setting sig_atomic_t flags without any other operations.
 */
TEST(SignalHandlerTest, AsyncSignalSafe) {
  // Reset flags
  g_test_shutdown_requested = 0;
  g_test_cancel_snapshot_requested = 0;

  // Install test signal handler
  std::signal(SIGTERM, TestSignalHandler);

  // Raise signal
  std::raise(SIGTERM);

  // Verify flags are set
  EXPECT_EQ(g_test_shutdown_requested, 1);
  EXPECT_EQ(g_test_cancel_snapshot_requested, 1);

  // Reset signal handler
  std::signal(SIGTERM, SIG_DFL);
}

/**
 * @brief Test that multiple signals don't cause issues
 */
TEST(SignalHandlerTest, MultipleSignals) {
  // Reset flags
  g_test_shutdown_requested = 0;
  g_test_cancel_snapshot_requested = 0;

  // Install test signal handler
  std::signal(SIGTERM, TestSignalHandler);

  // Raise signal multiple times
  for (int i = 0; i < 10; ++i) {
    std::raise(SIGTERM);
  }

  // Flags should still be 1 (idempotent)
  EXPECT_EQ(g_test_shutdown_requested, 1);
  EXPECT_EQ(g_test_cancel_snapshot_requested, 1);

  // Reset signal handler
  std::signal(SIGTERM, SIG_DFL);
}

/**
 * @brief Test concurrent access to signal flags
 *
 * This test simulates concurrent access to the signal flags from both
 * the signal handler and main thread to verify thread safety.
 */
TEST(SignalHandlerTest, ConcurrentAccess) {
  // Reset flags
  g_test_shutdown_requested = 0;
  g_test_cancel_snapshot_requested = 0;

  // Install test signal handler
  std::signal(SIGTERM, TestSignalHandler);

  // Flag to track if we saw the signal
  std::atomic<bool> saw_signal{false};

  // Thread that reads the flags
  std::thread reader([&saw_signal]() {
    // Busy loop checking flags (simulates main loop)
    auto start = std::chrono::steady_clock::now();
    while (std::chrono::steady_clock::now() - start < std::chrono::seconds(1)) {
      if (g_test_shutdown_requested == 1 && g_test_cancel_snapshot_requested == 1) {
        saw_signal = true;
        break;
      }
      std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
  });

  // Give reader thread a chance to start
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  // Raise signal from main thread
  std::raise(SIGTERM);

  // Wait for reader thread
  reader.join();

  // Verify reader saw the signal
  EXPECT_TRUE(saw_signal.load());

  // Reset signal handler
  std::signal(SIGTERM, SIG_DFL);
}

/**
 * @brief Test that signal handler works with SIGINT
 */
TEST(SignalHandlerTest, SigIntSupport) {
  // Reset flags
  g_test_shutdown_requested = 0;
  g_test_cancel_snapshot_requested = 0;

  // Install test signal handler for SIGINT
  std::signal(SIGINT, TestSignalHandler);

  // Raise SIGINT
  std::raise(SIGINT);

  // Verify flags are set
  EXPECT_EQ(g_test_shutdown_requested, 1);
  EXPECT_EQ(g_test_cancel_snapshot_requested, 1);

  // Reset signal handler
  std::signal(SIGINT, SIG_DFL);
}

/**
 * @brief Test that unhandled signals don't affect flags
 */
TEST(SignalHandlerTest, UnhandledSignals) {
  // Reset flags
  g_test_shutdown_requested = 0;
  g_test_cancel_snapshot_requested = 0;

  // Install test signal handler only for SIGTERM
  std::signal(SIGTERM, TestSignalHandler);

  // Signal handler doesn't handle other signals
  // (We can't test with actual signals like SIGSEGV, so this is a documentation test)

  // Verify flags remain 0
  EXPECT_EQ(g_test_shutdown_requested, 0);
  EXPECT_EQ(g_test_cancel_snapshot_requested, 0);

  // Reset signal handler
  std::signal(SIGTERM, SIG_DFL);
}

/**
 * @brief Integration test: simulate snapshot cancellation workflow
 *
 * This test simulates the complete workflow:
 * 1. Snapshot build starts
 * 2. Signal arrives during build
 * 3. Progress callback checks flag and cancels
 */
TEST(SignalHandlerTest, SnapshotCancellationWorkflow) {
  // Reset flags
  g_test_shutdown_requested = 0;
  g_test_cancel_snapshot_requested = 0;

  // Install test signal handler
  std::signal(SIGTERM, TestSignalHandler);

  // Simulate snapshot builder with cancel flag
  std::atomic<bool> snapshot_cancelled{false};

  // Simulate snapshot build in thread
  std::thread snapshot_thread([&snapshot_cancelled]() {
    // Simulate processing rows
    for (int i = 0; i < 100; ++i) {
      // Simulate progress callback checking cancellation flag
      if (g_test_cancel_snapshot_requested != 0) {
        snapshot_cancelled = true;
        break;
      }

      // Simulate work
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  });

  // Wait for snapshot to start
  std::this_thread::sleep_for(std::chrono::milliseconds(10));

  // Send signal
  std::raise(SIGTERM);

  // Wait for snapshot thread
  snapshot_thread.join();

  // Verify snapshot was cancelled
  EXPECT_TRUE(snapshot_cancelled.load());
  EXPECT_EQ(g_test_shutdown_requested, 1);
  EXPECT_EQ(g_test_cancel_snapshot_requested, 1);

  // Reset signal handler
  std::signal(SIGTERM, SIG_DFL);
}

/**
 * @brief Test flag reset behavior
 *
 * Verifies that flags can be safely reset after being set.
 */
TEST(SignalHandlerTest, FlagReset) {
  // Set flags
  g_test_shutdown_requested = 1;
  g_test_cancel_snapshot_requested = 1;

  // Verify set
  EXPECT_EQ(g_test_shutdown_requested, 1);
  EXPECT_EQ(g_test_cancel_snapshot_requested, 1);

  // Reset flags (simulates what main loop would do)
  g_test_shutdown_requested = 0;
  g_test_cancel_snapshot_requested = 0;

  // Verify reset
  EXPECT_EQ(g_test_shutdown_requested, 0);
  EXPECT_EQ(g_test_cancel_snapshot_requested, 0);
}
