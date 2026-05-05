/**
 * @file mock_event_multiplexer.h
 * @brief Deterministic in-process EventMultiplexer for unit and integration
 *        tests that must run without kernel poll primitives.
 *
 * Thread-safety contract: every public method (including the test hooks) takes
 * `mu_` before touching shared state, so the mock is safe to drive from a
 * separate test thread concurrently with a reactor-loop thread calling Poll().
 */

#pragma once

#include <chrono>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "server/reactor/event_multiplexer.h"

namespace mygramdb::server::reactor {

/**
 * @brief Fully controllable EventMultiplexer backend for tests.
 *
 * Unlike the real backends, MockEventMultiplexer never touches the kernel.
 * Tests drive it by injecting ReadyEvents via InjectReadable/etc. and verifying
 * behaviour via the accessor hooks.
 */
class MockEventMultiplexer final : public EventMultiplexer {
 public:
  MockEventMultiplexer() = default;
  ~MockEventMultiplexer() override;

  MockEventMultiplexer(const MockEventMultiplexer&) = delete;
  MockEventMultiplexer& operator=(const MockEventMultiplexer&) = delete;
  MockEventMultiplexer(MockEventMultiplexer&&) = delete;
  MockEventMultiplexer& operator=(MockEventMultiplexer&&) = delete;

  // -------------------------------------------------------------------------
  // EventMultiplexer interface
  // -------------------------------------------------------------------------

  mygram::utils::Expected<void, mygram::utils::Error> Open() override;
  mygram::utils::Expected<void, mygram::utils::Error> Add(int fd, uint8_t interest) override;
  mygram::utils::Expected<void, mygram::utils::Error> Modify(int fd, uint8_t interest) override;
  mygram::utils::Expected<void, mygram::utils::Error> Remove(int fd) override;
  mygram::utils::Expected<void, mygram::utils::Error> Poll(int timeout_ms, std::vector<ReadyEvent>& out) override;
  const char* Name() const override { return "mock"; }

  // -------------------------------------------------------------------------
  // Test hooks (non-virtual)
  // -------------------------------------------------------------------------

  /// Enqueue a kReadable event for @p fd to be returned on the next Poll().
  void InjectReadable(int fd);

  /// Enqueue a kWritable event for @p fd.
  void InjectWritable(int fd);

  /// Enqueue a kHangup event for @p fd.
  void InjectHangup(int fd);

  /// Enqueue a kError event for @p fd. @p errno_val is recorded but not
  /// currently surfaced beyond the event bitmask.
  void InjectError(int fd, int errno_val);

  /// Generic inject — the caller supplies the full ReadyEvent.
  void InjectRaw(ReadyEvent ev);

  /**
   * @brief Block until Poll() has been called at least @p min_calls times, or
   *        @p timeout elapses.
   * @return true if the target call-count was reached before the deadline.
   */
  bool WaitForPollCalled(int min_calls, std::chrono::milliseconds timeout);

  /// Snapshot of all currently-registered fd keys.
  std::vector<int> RegisteredFds() const;

  /// Current interest mask for @p fd, or 0 if unknown.
  uint8_t InterestFor(int fd) const;

  /// Total number of completed Poll() invocations.
  int PollCallCount() const;

  /**
   * @brief Signal all threads blocked inside Poll(-1, …) to wake and return
   *        with an empty event set.  Call from TearDown to avoid hangs.
   */
  void Shutdown();

  /**
   * @brief Force the next (and subsequent) Add() calls to return
   *        kNetworkReactorRegisterFailed until cleared.
   *
   * Used to validate IoReactor::Register's failure path (Fix N-5):
   * connections_ must remain untouched if mux_->Add fails.
   */
  void SetAddShouldFail(bool should_fail);

 private:
  mutable std::mutex mu_;
  std::condition_variable poll_cv_;

  bool opened_{false};
  bool shutdown_{false};
  bool add_should_fail_{false};
  std::unordered_map<int, uint8_t> interest_;
  std::deque<ReadyEvent> injected_;
  int poll_call_count_{0};
};

}  // namespace mygramdb::server::reactor
