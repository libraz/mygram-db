/**
 * @file mock_event_multiplexer.cpp
 * @brief MockEventMultiplexer implementation.
 */

#include "mock_event_multiplexer.h"

#include <algorithm>

#include "utils/error.h"
#include "utils/expected.h"

namespace mygramdb::server::reactor {

namespace {
using mygram::utils::Error;
using mygram::utils::ErrorCode;
using mygram::utils::Expected;
using mygram::utils::MakeError;
using mygram::utils::MakeUnexpected;
}  // namespace

MockEventMultiplexer::~MockEventMultiplexer() {
  Shutdown();
}

// ---------------------------------------------------------------------------
// EventMultiplexer interface
// ---------------------------------------------------------------------------

Expected<void, Error> MockEventMultiplexer::Open() {
  std::unique_lock<std::mutex> lock(mu_);
  if (opened_) {
    return MakeUnexpected(MakeError(ErrorCode::kNetworkReactorAlreadyOpen,
                                    "MockEventMultiplexer::Open called on already-open multiplexer"));
  }
  opened_ = true;
  return {};
}

Expected<void, Error> MockEventMultiplexer::Add(int fd, uint8_t interest) {
  std::unique_lock<std::mutex> lock(mu_);
  if (add_should_fail_) {
    return MakeUnexpected(
        MakeError(ErrorCode::kNetworkReactorRegisterFailed, "MockEventMultiplexer::Add: forced failure for test"));
  }
  if (interest_.count(fd) != 0U) {
    return MakeUnexpected(
        MakeError(ErrorCode::kNetworkReactorRegisterFailed, "MockEventMultiplexer::Add: fd already registered"));
  }
  interest_[fd] = interest;
  return {};
}

Expected<void, Error> MockEventMultiplexer::Modify(int fd, uint8_t interest) {
  std::unique_lock<std::mutex> lock(mu_);
  auto it = interest_.find(fd);
  if (it == interest_.end()) {
    return MakeUnexpected(
        MakeError(ErrorCode::kNetworkReactorModifyFailed, "MockEventMultiplexer::Modify: unknown fd"));
  }
  it->second = interest;
  return {};
}

Expected<void, Error> MockEventMultiplexer::Remove(int fd) {
  std::unique_lock<std::mutex> lock(mu_);
  // Idempotent: removing an unknown fd is a no-op.
  interest_.erase(fd);
  return {};
}

Expected<void, Error> MockEventMultiplexer::Poll(int timeout_ms, std::vector<ReadyEvent>& out) {
  out.clear();

  std::unique_lock<std::mutex> lock(mu_);

  auto has_events_or_done = [this]() { return !injected_.empty() || shutdown_ || wake_pending_; };

  if (timeout_ms == 0) {
    // Non-blocking: drain whatever is already queued, then return.
  } else if (timeout_ms < 0) {
    // Infinite wait: block until events arrive, Shutdown(), or Wake().
    poll_cv_.wait(lock, has_events_or_done);
  } else {
    // Timed wait.
    poll_cv_.wait_for(lock, std::chrono::milliseconds(timeout_ms), has_events_or_done);
  }

  // Consume the wake flag so subsequent blocking Polls revert to normal
  // cv-wait behaviour. Mirrors the real backends, which drain their wake
  // primitive (eventfd / EVFILT_USER) inside Poll().
  wake_pending_ = false;

  // Drain the injected queue into the output buffer.
  while (!injected_.empty()) {
    out.push_back(injected_.front());
    injected_.pop_front();
  }

  ++poll_call_count_;
  // Wake any WaitForPollCalled() callers.
  poll_cv_.notify_all();

  return {};
}

Expected<void, Error> MockEventMultiplexer::Wake() {
  {
    std::unique_lock<std::mutex> lock(mu_);
    wake_pending_ = true;
  }
  poll_cv_.notify_all();
  return {};
}

// ---------------------------------------------------------------------------
// Test hooks
// ---------------------------------------------------------------------------

void MockEventMultiplexer::InjectReadable(int fd) {
  InjectRaw(ReadyEvent{fd, event::kReadable});
}

void MockEventMultiplexer::InjectWritable(int fd) {
  InjectRaw(ReadyEvent{fd, event::kWritable});
}

void MockEventMultiplexer::InjectHangup(int fd) {
  InjectRaw(ReadyEvent{fd, event::kHangup});
}

void MockEventMultiplexer::InjectError(int fd, int /*errno_val*/) {
  InjectRaw(ReadyEvent{fd, event::kError});
}

void MockEventMultiplexer::InjectRaw(ReadyEvent ev) {
  {
    std::unique_lock<std::mutex> lock(mu_);
    injected_.push_back(ev);
  }
  poll_cv_.notify_all();
}

bool MockEventMultiplexer::WaitForPollCalled(int min_calls, std::chrono::milliseconds timeout) {
  std::unique_lock<std::mutex> lock(mu_);
  return poll_cv_.wait_for(lock, timeout, [this, min_calls]() { return poll_call_count_ >= min_calls; });
}

std::vector<int> MockEventMultiplexer::RegisteredFds() const {
  std::unique_lock<std::mutex> lock(mu_);
  std::vector<int> fds;
  fds.reserve(interest_.size());
  for (const auto& kv : interest_) {
    fds.push_back(kv.first);
  }
  return fds;
}

uint8_t MockEventMultiplexer::InterestFor(int fd) const {
  std::unique_lock<std::mutex> lock(mu_);
  auto it = interest_.find(fd);
  return it != interest_.end() ? it->second : 0U;
}

int MockEventMultiplexer::PollCallCount() const {
  std::unique_lock<std::mutex> lock(mu_);
  return poll_call_count_;
}

void MockEventMultiplexer::Shutdown() {
  {
    std::unique_lock<std::mutex> lock(mu_);
    shutdown_ = true;
  }
  poll_cv_.notify_all();
}

void MockEventMultiplexer::SetAddShouldFail(bool should_fail) {
  std::unique_lock<std::mutex> lock(mu_);
  add_should_fail_ = should_fail;
}

}  // namespace mygramdb::server::reactor
