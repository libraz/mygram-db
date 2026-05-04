/**
 * @file io_reactor.cpp
 * @brief Phase 2 IoReactor implementation — single-threaded event loop.
 */

#include "server/io_reactor.h"

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <mutex>
#include <shared_mutex>
#include <utility>
#include <vector>

#include "server/reactor/event_multiplexer.h"
#include "utils/error.h"
#include "utils/expected.h"
#include "utils/structured_log.h"

namespace mygramdb::server {

using mygram::utils::Error;
using mygram::utils::ErrorCode;
using mygram::utils::Expected;
using mygram::utils::MakeError;
using mygram::utils::MakeUnexpected;

namespace {
constexpr size_t kReadyEventReserve = 64;
}

IoReactor::IoReactor(ThreadPool* pool, RequestDispatcher* dispatcher, ReactorConfig cfg)
    : pool_(pool), dispatcher_(dispatcher), config_(std::move(cfg)) {}

IoReactor::~IoReactor() {
  Stop();
}

Expected<void, Error> IoReactor::Start() {
  // Serialise against Stop() and any concurrent Start() invocation. Without
  // this lock, a Stop() racing with Start can observe running_=true after
  // Start sets it, exchange it back to false, attempt to join() the
  // event_loop_thread_ before Start assigns it, and silently leak the
  // worker thread Start subsequently spawns.
  std::lock_guard<std::mutex> lock(start_stop_mutex_);

  if (running_.load(std::memory_order_acquire)) {
    return {};
  }

  if (config_.event_loop_threads != 1) {
    mygram::utils::StructuredLog()
        .Event("reactor_config_warning")
        .Field("event_loop_threads", static_cast<int64_t>(config_.event_loop_threads))
        .Field("effective_threads", static_cast<int64_t>(1))
        .Field("note", "ReactorConfig::event_loop_threads is currently ignored (hardcoded to 1)")
        .Warn();
  }

  // In tests, mux_factory_ is set via SetMultiplexerFactoryForTest() to
  // inject a MockEventMultiplexer. In production this branch is never taken.
  auto mux = mux_factory_ ? mux_factory_() : reactor::CreateEventMultiplexer();
  if (!mux) {
    return MakeUnexpected(
        MakeError(ErrorCode::kNetworkReactorUnsupported, "No event multiplexer available on this platform"));
  }
  if (auto r = mux->Open(); !r) {
    return MakeUnexpected(r.error());
  }

  // Take mux_lifecycle_ exclusively to publish mux_ atomically with
  // running_=true. Lock order: start_stop_mutex_ -> mux_lifecycle_.
  {
    std::unique_lock<std::shared_mutex> mux_lock(mux_lifecycle_);
    mux_ = std::move(mux);
  }
  running_.store(true, std::memory_order_release);
  event_loop_thread_ = std::thread([this]() { EventLoop(); });

  mygram::utils::StructuredLog()
      .Event("reactor_started")
      .Field("backend", mux_->Name())
      .Field("poll_timeout_ms", static_cast<int64_t>(config_.poll_timeout_ms))
      .Info();
  return {};
}

void IoReactor::Stop() {
  // Serialise against Start() and any concurrent Stop(). See start_stop_mutex_
  // commentary in the header for the race this guards.
  //
  // The event-loop thread MUST NOT take start_stop_mutex_ — Stop() joins it
  // while holding the lock. EventLoop only acquires mux_lifecycle_ (shared),
  // which is consistent with the global ordering
  // start_stop_mutex_ -> mux_lifecycle_ -> connections_mutex_.
  std::lock_guard<std::mutex> lock(start_stop_mutex_);

  if (!running_.exchange(false, std::memory_order_acq_rel)) {
    // Never started, or already stopped.
    if (event_loop_thread_.joinable()) {
      event_loop_thread_.join();
    }
    return;
  }

  if (event_loop_thread_.joinable()) {
    event_loop_thread_.join();
  }

  // Drop all registered connections. Drain tasks that still hold a
  // shared_ptr copy will keep their connection alive until they finish.
  {
    std::unique_lock<std::shared_mutex> conn_lock(connections_mutex_);
    connections_.clear();
  }
  {
    // Exclusive lock: wait for any in-flight Register/Unregister/ArmWrite to
    // finish before destroying the multiplexer. The event-loop thread has
    // already been joined, so the only contenders are other threads.
    std::unique_lock<std::shared_mutex> mux_lock(mux_lifecycle_);
    mux_.reset();
  }

  mygram::utils::StructuredLog().Event("reactor_stopped").Info();
}

Expected<void, Error> IoReactor::Register(std::shared_ptr<ReactorConnection> conn) {
  if (!conn) {
    return MakeUnexpected(MakeError(ErrorCode::kInvalidArgument, "Register called with null connection"));
  }

  const int fd = conn->Fd();
  if (fd < 0) {
    return MakeUnexpected(MakeError(ErrorCode::kInvalidArgument, "Register called with negative fd"));
  }

  // Put the socket in non-blocking mode before handing it to the event loop.
  // Without this, a recv() inside OnReadable would block the entire reactor.
  // This is done outside any reactor lock because fcntl(2) is independent of
  // reactor state.
  const int flags = ::fcntl(fd, F_GETFL, 0);
  if (flags < 0) {
    return MakeUnexpected(MakeError(ErrorCode::kNetworkSocketCreationFailed,
                                    std::string("fcntl(F_GETFL) failed: ") + std::strerror(errno)));
  }
  if ((flags & O_NONBLOCK) == 0) {
    if (::fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
      return MakeUnexpected(MakeError(ErrorCode::kNetworkSocketCreationFailed,
                                      std::string("fcntl(F_SETFL, O_NONBLOCK) failed: ") + std::strerror(errno)));
    }
  }

  // Acquire mux_lifecycle_ (shared) FIRST and perform every reactor-state
  // mutation — running_ check, connections_ insert, mux_->Add — inside this
  // critical section. Stop() takes mux_lifecycle_ exclusively at the very end
  // of its shutdown sequence; while we hold this shared lock, Stop() cannot
  // proceed past `connections_.clear()` (which happens BEFORE the exclusive
  // mux_lifecycle_ acquisition in Stop). That eliminates the window where
  // Register's connections_.emplace could leak past Stop's clear().
  //
  // Lock ordering: mux_lifecycle_ (shared) -> connections_mutex_ (unique).
  std::shared_lock<std::shared_mutex> mux_lock(mux_lifecycle_);

  if (!running_.load(std::memory_order_acquire) || !mux_) {
    return MakeUnexpected(MakeError(ErrorCode::kNetworkServerNotStarted, "IoReactor::Register before Start"));
  }

  // Insert into connections_ first so an event delivered between mux_->Add
  // and the map insert never sees a missing entry.
  {
    std::unique_lock<std::shared_mutex> conn_lock(connections_mutex_);
    if (connections_.count(fd) != 0U) {
      return MakeUnexpected(MakeError(ErrorCode::kInternalError, "IoReactor::Register duplicate fd"));
    }
    connections_.emplace(fd, conn);
  }

  auto add_result = mux_->Add(fd, reactor::event::kReadable);
  if (!add_result) {
    // Roll back the map insert so a subsequent Register attempt with the
    // same fd sees a clean slate.
    std::unique_lock<std::shared_mutex> conn_lock(connections_mutex_);
    connections_.erase(fd);
    return MakeUnexpected(add_result.error());
  }

  return {};
}

void IoReactor::Unregister(int fd) {
  // Remove from the multiplexer first so the event loop stops reporting
  // events for this fd, then drop the shared_ptr from the map. Drain tasks
  // that captured a copy keep the ReactorConnection alive until they
  // finish, and only then does the destructor close(2) the socket.
  bool was_registered = false;
  {
    std::shared_lock<std::shared_mutex> mux_lock(mux_lifecycle_);
    if (mux_) {
      // Remove() is idempotent from the caller's perspective.
      (void)mux_->Remove(fd);
    }
  }
  {
    std::unique_lock<std::shared_mutex> lock(connections_mutex_);
    was_registered = connections_.erase(fd) > 0;
  }
  // Close callback runs outside all locks so callers cannot deadlock the
  // reactor by taking their own mutexes inside the callback.
  if (was_registered && close_callback_) {
    close_callback_(fd);
  }
}

void IoReactor::SetCloseCallback(std::function<void(int)> cb) {
  close_callback_ = std::move(cb);
}

void IoReactor::SetMultiplexerFactoryForTest(MultiplexerFactory f) {
  mux_factory_ = std::move(f);
}

Expected<void, Error> IoReactor::ArmWrite(int fd) {
  std::shared_lock<std::shared_mutex> mux_lock(mux_lifecycle_);
  if (!mux_) {
    return MakeUnexpected(MakeError(ErrorCode::kNetworkServerNotStarted, "ArmWrite while reactor stopped"));
  }
  return mux_->Modify(fd, reactor::event::kReadable | reactor::event::kWritable);
}

Expected<void, Error> IoReactor::DisarmWrite(int fd) {
  std::shared_lock<std::shared_mutex> mux_lock(mux_lifecycle_);
  if (!mux_) {
    return MakeUnexpected(MakeError(ErrorCode::kNetworkServerNotStarted, "DisarmWrite while reactor stopped"));
  }
  return mux_->Modify(fd, reactor::event::kReadable);
}

size_t IoReactor::ConnectionCount() const {
  std::shared_lock<std::shared_mutex> lock(connections_mutex_);
  return connections_.size();
}

const char* IoReactor::BackendName() const {
  std::shared_lock<std::shared_mutex> lock(mux_lifecycle_);
  return mux_ ? mux_->Name() : "unavailable";
}

std::shared_ptr<ReactorConnection> IoReactor::Lookup(int fd) const {
  std::shared_lock<std::shared_mutex> lock(connections_mutex_);
  auto it = connections_.find(fd);
  if (it == connections_.end()) {
    return nullptr;
  }
  return it->second;
}

void IoReactor::EventLoop() {
  std::vector<reactor::ReadyEvent> ready;
  ready.reserve(kReadyEventReserve);

  while (running_.load(std::memory_order_acquire)) {
    Expected<void, Error> poll_result;
    {
      // Hold the mux mutex for the duration of Poll so that concurrent
      // Add/Modify/Remove calls do not race with the backend's internal
      // state. The shared EventMultiplexer contract is single-threaded.
      std::shared_lock<std::shared_mutex> mux_lock(mux_lifecycle_);
      if (!mux_) {
        break;
      }
      poll_result = mux_->Poll(config_.poll_timeout_ms, ready);
    }
    if (!poll_result) {
      // Promoted from Warn to Error: a poll syscall failure indicates a real
      // I/O multiplexer fault that should raise operator alerts; the reactor
      // continues but events may be dropped until the next successful poll.
      mygram::utils::StructuredLog().Event("reactor_poll_failed").FieldError(poll_result.error()).Error();
      continue;
    }
    for (const auto& ev : ready) {
      DispatchEvent(ev);
    }
  }
}

void IoReactor::DispatchEvent(const reactor::ReadyEvent& ev) {
  auto conn = Lookup(ev.fd);
  if (!conn) {
    // Stale event (connection was unregistered between Poll and dispatch).
    return;
  }

  bool keep = true;
  // Hard error events short-circuit straight to OnError: the socket is no
  // longer usable for either read or write.
  if ((ev.events & reactor::event::kError) != 0) {
    keep = conn->OnError();
  } else {
    // Hangup alone (EV_EOF on kqueue / EPOLLRDHUP on epoll) means the peer
    // half-closed the write side of its socket. The *read* side of the
    // server->client direction is still open, and the kernel may still have
    // buffered payload bytes waiting to be drained. Fall through into
    // OnReadable: its recv()==0 path sets read_eof_, finishes processing
    // any pending frames, flushes the response via the drain task, and
    // only then unregisters. Treating kHangup as a fatal error here causes
    // the server to drop half-closed clients' responses on the floor.
    if ((ev.events & (reactor::event::kReadable | reactor::event::kHangup)) != 0 && keep) {
      keep = conn->OnReadable();
    }
    if ((ev.events & reactor::event::kWritable) != 0 && keep) {
      keep = conn->OnWritable();
    }
  }

  if (!keep) {
    Unregister(ev.fd);
  }
}

}  // namespace mygramdb::server
