/**
 * @file io_reactor.cpp
 * @brief Phase 2 IoReactor implementation — single-threaded event loop.
 */

#include "server/io_reactor.h"

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
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
  std::string backend_name;
  {
    std::unique_lock<std::shared_mutex> mux_lock(mux_lifecycle_);
    mux_ = std::move(mux);
    backend_name = mux_->Name();
    running_.store(true, std::memory_order_release);
  }
  event_loop_thread_ = std::thread([this]() { EventLoop(); });

  mygram::utils::StructuredLog()
      .Event("reactor_started")
      .Field("backend", backend_name)
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
  //
  // CR-3 caller contract: when this reactor is owned by a TcpServer, the
  // owner MUST shut down `thread_pool_` AFTER reactor_->Stop() returns. The
  // reactor's close_callback_ may capture pointers (`accept_ptr`,
  // `close_stats_ptr`) that point at TcpServer-owned objects; drain tasks
  // queued on the pool may still be executing those callbacks at the moment
  // Stop() runs. If the pool were torn down before the reactor, the callback
  // bodies would race their captured-pointer destruction; if the pool were
  // torn down before connections_.clear() but after the reactor, in-flight
  // tasks would observe a half-cleared connection map. The current order
  // (TcpServer::Stop) is:
  //   reactor_->Stop()  // joins event loop + clears connection map
  //   acceptor_->Stop()
  //   thread_pool_->Shutdown()  // joins drain tasks last
  // Any caller that owns this reactor MUST follow the same order.
  std::lock_guard<std::mutex> lock(start_stop_mutex_);

  if (!running_.exchange(false, std::memory_order_acq_rel)) {
    // Never started, or already stopped.
    if (event_loop_thread_.joinable()) {
      event_loop_thread_.join();
    }
    return;
  }

  // Kick the event-loop thread out of any in-progress Poll() so Stop()
  // doesn't have to wait up to `poll_timeout_ms` (default 100ms, but
  // operators can configure it as high as several seconds) for the loop to
  // observe `running_=false`. The Wake() failure path is logged but not
  // fatal: the worst case is the prior behaviour of waiting for the timeout
  // to elapse. Wake() takes the shared mux_lifecycle_ lock to read mux_;
  // since we hold start_stop_mutex_ but not mux_lifecycle_, lock ordering
  // (start_stop_mutex_ -> mux_lifecycle_) is preserved.
  {
    std::shared_lock<std::shared_mutex> mux_lock(mux_lifecycle_);
    if (mux_) {
      auto wake_result = mux_->Wake();
      if (!wake_result) {
        mygram::utils::StructuredLog()
            .Event("reactor_stop_wake_failed")
            .FieldError(wake_result.error())
            .Field("note", "falling back to poll-timeout-bounded shutdown")
            .Warn();
      }
    }
  }

  if (event_loop_thread_.joinable()) {
    event_loop_thread_.join();
  }

  // Drop all registered connections. Drain tasks that still hold a
  // shared_ptr copy will keep their connection alive until they finish.
  // Note: the close_callback_ is intentionally NOT invoked from this clear()
  // path — Unregister() invokes it for naturally-closed connections, but
  // mass-clear during Stop() leaves the callback un-invoked because
  // the captured ServerStats / ConnectionAcceptor objects may not be in a
  // state that tolerates being decremented (TcpServer is in shutdown).
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
  // Multiplexer registration happens BEFORE map insertion. This ensures that
  // any concurrent Lookup(fd) only sees the connection after it is fully
  // observable to the multiplexer. Reverse order would expose a window where
  // Lookup returns a connection whose fd is not yet in the kernel poll set,
  // which is harmless in this codepath today (DispatchEvent only calls
  // Lookup when the kernel reports a ready event, and the kernel cannot
  // report events for a fd that is not registered yet) but fragile against
  // future changes. As a side benefit, this order eliminates the rollback
  // path that the previous implementation needed: if mux_->Add fails, the
  // map is never touched, so there is nothing to clean up.
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
  // mutation — running_ check, mux_->Add, connections_ insert — inside this
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

  // Step 1: pre-check for duplicate fd under the connections_ lock. We have
  // to do this before mux_->Add to keep the failure path side-effect free
  // (otherwise we'd Add then immediately Remove on duplicate detection).
  {
    std::shared_lock<std::shared_mutex> conn_lock(connections_mutex_);
    if (connections_.count(fd) != 0U) {
      return MakeUnexpected(MakeError(ErrorCode::kInternalError, "IoReactor::Register duplicate fd"));
    }
  }

  // Step 2: register with the multiplexer. On failure return immediately
  // without ever touching the connections_ map.
  auto add_result = mux_->Add(fd, reactor::event::kReadable);
  if (!add_result) {
    return MakeUnexpected(add_result.error());
  }

  // Step 3: publish into connections_. We re-check the duplicate guard in
  // case another Register raced us between Step 1 and Step 3; if so, undo
  // the mux_->Add we just performed so the kernel poll set stays clean.
  {
    std::unique_lock<std::shared_mutex> conn_lock(connections_mutex_);
    auto [it, inserted] = connections_.emplace(fd, conn);
    if (!inserted) {
      // Lost the race against a concurrent Register with the same fd. Pull
      // our entry back out of the multiplexer to avoid leaking interest.
      // Rollback failure is not itself fatal — the duplicate-fd error is
      // still returned to the caller — but a kernel-level inconsistency
      // (interest record leaked, the fd is closed but the multiplexer
      // still has it armed) must be visible to operators so they can
      // correlate it with later kqueue/epoll syscall failures. Logging at
      // Error matches the severity of the kernel-state divergence.
      conn_lock.unlock();
      auto remove_result = mux_->Remove(fd);
      if (!remove_result) {
        mygram::utils::StructuredLog()
            .Event("reactor_register_rollback_remove_failed")
            .Field("fd", static_cast<int64_t>(fd))
            .FieldError(remove_result.error())
            .Field("note", "duplicate-fd rollback could not unregister mux interest; kernel poll set may be stale")
            .Error();
      }
      return MakeUnexpected(MakeError(ErrorCode::kInternalError, "IoReactor::Register duplicate fd (race)"));
    }
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

    // Reap idle connections at most once per `reaper_interval_sec`. Running
    // in-loop avoids spawning a dedicated reaper thread; the cost is one
    // shared-lock acquisition per interval over connections_, which is
    // negligible compared to Poll itself.
    if ((config_.idle_timeout_sec > 0 || config_.initial_read_timeout_sec > 0) && config_.reaper_interval_sec > 0) {
      const auto now = std::chrono::steady_clock::now();
      if (now - last_reaper_run_ >= std::chrono::seconds(config_.reaper_interval_sec)) {
        last_reaper_run_ = now;
        ReapIdleConnections();
      }
    }
  }
}

void IoReactor::ReapIdleConnections() {
  // Snapshot the candidate fds under a shared lock so we don't hold the
  // mutex across Unregister(), which itself acquires both mux_lifecycle_
  // (shared) and connections_mutex_ (unique). Holding the shared lock here
  // and then upgrading to unique inside Unregister would deadlock.
  const auto now = std::chrono::steady_clock::now();
  const auto idle_deadline = std::chrono::seconds(config_.idle_timeout_sec);
  const auto initial_read_deadline = std::chrono::seconds(config_.initial_read_timeout_sec);
  struct ReapCandidate {
    int fd;
    std::chrono::seconds age;
    const char* reason;
    int timeout_sec;
  };
  std::vector<ReapCandidate> to_close;
  {
    std::shared_lock<std::shared_mutex> lock(connections_mutex_);
    to_close.reserve(connections_.size());
    for (const auto& [fd, conn] : connections_) {
      if (!conn) {
        continue;
      }
      if (config_.initial_read_timeout_sec > 0 && !conn->HasReceivedFrame()) {
        const auto initial_age = now - conn->CreatedAt();
        if (initial_age >= initial_read_deadline) {
          to_close.push_back({fd, std::chrono::duration_cast<std::chrono::seconds>(initial_age), "initial_read_timeout",
                              config_.initial_read_timeout_sec});
          continue;
        }
      }
      if (config_.idle_timeout_sec > 0) {
        const auto idle_for = now - conn->LastActive();
        if (idle_for >= idle_deadline) {
          to_close.push_back({fd, std::chrono::duration_cast<std::chrono::seconds>(idle_for), "idle_timeout",
                              config_.idle_timeout_sec});
        }
      }
    }
  }

  for (const auto& candidate : to_close) {
    mygram::utils::StructuredLog()
        .Event("connection_reaped")
        .Field("fd", static_cast<int64_t>(candidate.fd))
        .Field("reason", candidate.reason)
        .Field("age_seconds", static_cast<int64_t>(candidate.age.count()))
        .Field("timeout_sec", static_cast<int64_t>(candidate.timeout_sec))
        .Info();
    Unregister(candidate.fd);
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
