/**
 * @file reactor_connection.cpp
 * @brief Per-connection state + drain-task-per-connection pattern.
 *
 * Implements the Phase 2 read side + Phase 3 non-blocking write queue of
 * the reactor refactor described in docs/ja/design/reactor-io-refactor.md
 * §4.3/§7 R3.
 */

#include "server/reactor_connection.h"

#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <array>
#include <cassert>
#include <cerrno>
#include <cstddef>
#include <cstring>
#include <mutex>
#include <string>
#include <string_view>
#include <utility>

#include "server/io_reactor.h"
#include "server/request_dispatcher.h"
#include "server/server_stats.h"
#include "server/thread_pool.h"
#include "utils/structured_log.h"

namespace mygramdb::server {

namespace {
constexpr size_t kRecvChunkBytes = 4096;
constexpr const char kFrameDelimiter[] = "\r\n";
constexpr size_t kFrameDelimiterLen = 2;
constexpr const char kResponseTerminator[] = "\r\n";
constexpr size_t kResponseTerminatorLen = 2;

#ifdef MSG_NOSIGNAL
constexpr int kSendFlags = MSG_NOSIGNAL;
#else
constexpr int kSendFlags = 0;
#endif

void BestEffortSendError(int fd, std::string_view message) {
  std::string response = "ERROR ";
  response.append(message);
  response.append(kResponseTerminator, kResponseTerminatorLen);

  size_t sent = 0;
  while (sent < response.size()) {
    ssize_t n = ::send(fd, response.data() + sent, response.size() - sent, kSendFlags);
    if (n > 0) {
      sent += static_cast<size_t>(n);
      continue;
    }
    if (n < 0 && errno == EINTR) {
      continue;
    }
    break;
  }
}
}  // namespace

std::shared_ptr<ReactorConnection> ReactorConnection::Create(int fd, IoReactor* reactor, RequestDispatcher* dispatcher,
                                                             ThreadPool* thread_pool, ServerStats* stats,
                                                             size_t max_write_queue_bytes) {
  return std::make_shared<ReactorConnection>(fd, reactor, dispatcher, thread_pool, stats, max_write_queue_bytes);
}

ReactorConnection::ReactorConnection(int fd, IoReactor* reactor, RequestDispatcher* dispatcher, ThreadPool* thread_pool,
                                     ServerStats* stats, size_t max_write_queue_bytes)
    : fd_(fd),
      max_write_queue_bytes_(max_write_queue_bytes),
      reactor_(reactor),
      dispatcher_(dispatcher),
      thread_pool_(thread_pool),
      stats_(stats) {
  const auto now = std::chrono::steady_clock::now();
  created_at_.store(now, std::memory_order_relaxed);
  last_active_.store(now, std::memory_order_relaxed);
  conn_ctx_.client_fd = fd_;
  read_buf_.reserve(kDefaultReadBufferBytes);
}

ReactorConnection::~ReactorConnection() {
  if (!closed_ && fd_ >= 0) {
    ::close(fd_);
    closed_ = true;
  }
}

bool ReactorConnection::OnReadable() {
  // Refresh idle-timer baseline. Any inbound event counts as activity for
  // the reaper, even if recv() ultimately returns 0 (peer half-close): the
  // peer just spoke to us, so we are not idle.
  last_active_.store(std::chrono::steady_clock::now(), std::memory_order_relaxed);

  if (closing_.load(std::memory_order_acquire)) {
    return false;
  }

  // If the peer already half-closed (we saw recv()==0 on a previous readable
  // event), suppress further recv() calls. The write side may still be open
  // while the drain task flushes responses, so we remain registered until
  // DrainTask finishes and marks us closing_.
  if (read_eof_.load(std::memory_order_acquire)) {
    return true;
  }

  // 1. Drain the socket until EAGAIN / EWOULDBLOCK.
  std::array<char, kRecvChunkBytes> chunk{};
  while (true) {
    ssize_t n = ::recv(fd_, chunk.data(), chunk.size(), 0);
    if (n > 0) {
      // Hard cap on the accumulation buffer — slow-reader / malformed frame
      // protection.
      if (read_buf_.size() + static_cast<size_t>(n) > kMaxReadBufferBytes) {
        mygram::utils::StructuredLog()
            .Event("reactor_read_buf_overflow")
            .Field("fd", static_cast<int64_t>(fd_))
            .Field("buf_bytes", static_cast<uint64_t>(read_buf_.size() + static_cast<size_t>(n)))
            .Field("cap_bytes", static_cast<uint64_t>(kMaxReadBufferBytes))
            .Warn();
        BestEffortSendError(fd_, "request too large");
        closing_.store(true, std::memory_order_release);
        return false;
      }
      read_buf_.insert(read_buf_.end(), chunk.data(), chunk.data() + n);
      continue;
    }
    if (n == 0) {
      // Peer performed orderly close or half-close (shutdown(SHUT_WR)). The
      // write side of the socket may still be open, so we must not tear down
      // the connection here — we have to finish dispatching any already
      // framed requests and flush the response. Set read_eof_ so subsequent
      // OnReadable calls short-circuit, then fall through to frame
      // extraction + drain task scheduling below. The drain task closes the
      // connection after the last response has been queued for send.
      read_eof_.store(true, std::memory_order_release);
      break;
    }
    // n < 0
    if (errno == EINTR) {
      continue;
    }
    if (errno == EAGAIN || errno == EWOULDBLOCK) {
      break;
    }
    mygram::utils::StructuredLog()
        .Event("reactor_recv_failed")
        .Field("fd", static_cast<int64_t>(fd_))
        .Field("errno", static_cast<int64_t>(errno))
        .Field("error", std::strerror(errno))
        .Warn();
    closing_.store(true, std::memory_order_release);
    return false;
  }

  // 2. Extract complete frames and push onto the drain queue.
  size_t enqueued = 0;
  {
    std::lock_guard<std::mutex> lock(frame_mutex_);
    enqueued = ExtractFramesLocked();
  }

  // 3. If we parsed at least one frame, make sure a drain task is running.
  //    The drain task will close the connection on behalf of the read path
  //    once read_eof_ is set and there is nothing left to flush.
  if (enqueued > 0) {
    if (!ScheduleDrainTask()) {
      return false;
    }
  }

  // If the peer half-closed and there are no frames in flight and nothing
  // pending in the write queue, we can tear down immediately. Otherwise the
  // drain task (or OnWritable, after the write queue drains) will do the
  // close for us.
  //
  // Bug fix (P1-3): the empty-queue test must hold BOTH mutexes
  // simultaneously. Releasing frame_mutex_ before acquiring write_mutex_
  // opens a window where an in-flight DrainTask can finish dispatching the
  // last frame and call EnqueueResponse → write_queue_.push_back AFTER we
  // observed pending_frames_/drain_scheduled_ both empty but BEFORE we read
  // write_queue_. The result was a closing_=true decision while a fresh
  // response sat in the write queue, dropping the response on the floor.
  //
  // Lock ordering (consistent with the rest of this file):
  //   frame_mutex_ -> write_mutex_
  // No code path takes write_mutex_ then frame_mutex_; OnWritable
  // deliberately avoids acquiring frame_mutex_ while holding write_mutex_
  // (see commentary in OnWritable).
  if (read_eof_.load(std::memory_order_acquire)) {
    bool should_close = false;
    {
      std::lock_guard<std::mutex> frame_lock(frame_mutex_);
      std::lock_guard<std::mutex> write_lock(write_mutex_);
      if (pending_frames_.empty() && !drain_scheduled_.load(std::memory_order_acquire) && write_queue_.empty()) {
        closing_.store(true, std::memory_order_release);
        should_close = true;
      }
    }
    if (should_close) {
      return false;
    }
  }

  return true;
}

bool ReactorConnection::OnWritable() {
  // Outbound progress also resets the idle-timer (Fix N-3): a slow client
  // that is steadily draining its socket is not "idle" even if it never
  // sends another request.
  last_active_.store(std::chrono::steady_clock::now(), std::memory_order_relaxed);

  std::unique_lock<std::mutex> lock(write_mutex_);

  if (!DrainWriteQueueLocked()) {
    // Fatal send error during drain.
    closing_.store(true, std::memory_order_release);
    return false;
  }

  if (!write_queue_.empty()) {
    // Partial drain: leave the queue armed, fire again on next writable event.
    return true;
  }

  // Fully drained: disarm kWritable so the event loop stops spinning on
  // this fd. If we had never actually armed (edge case — OnWritable fired
  // spuriously), skip the disarm call.
  if (write_armed_ && reactor_ != nullptr) {
    (void)reactor_->DisarmWrite(fd_);
    write_armed_ = false;
  }

  if (closing_.load(std::memory_order_acquire)) {
    return false;
  }

  // Peer already half-closed and the drain task has no more work in flight:
  // we just flushed the last response, so unregister now. Release
  // write_mutex_ before taking frame_mutex_ to preserve the file-wide lock
  // order (frame_mutex_ -> write_mutex_).
  lock.unlock();
  bool should_close = false;
  if (read_eof_.load(std::memory_order_acquire)) {
    std::lock_guard<std::mutex> frame_lock(frame_mutex_);
    should_close = pending_frames_.empty() && !drain_scheduled_.load(std::memory_order_acquire);
  }
  if (should_close) {
    closing_.store(true, std::memory_order_release);
    return false;
  }

  return true;
}

bool ReactorConnection::OnError() {
  closing_.store(true, std::memory_order_release);
  return false;
}

size_t ReactorConnection::ExtractFramesLocked() {
  size_t enqueued = 0;
  size_t scan_start = 0;
  size_t consumed = 0;
  while (scan_start + kFrameDelimiterLen <= read_buf_.size()) {
    // Search for the next delimiter.
    const char* begin = read_buf_.data() + scan_start;
    const size_t remaining = read_buf_.size() - scan_start;
    const char* found = static_cast<const char*>(std::memchr(begin, kFrameDelimiter[0], remaining));
    if (found == nullptr) {
      break;
    }
    const size_t found_off = static_cast<size_t>(found - read_buf_.data());
    if (found_off + kFrameDelimiterLen > read_buf_.size()) {
      break;  // delimiter straddles the buffer end; wait for more bytes
    }
    if (read_buf_[found_off + 1] != kFrameDelimiter[1]) {
      // Lone CR without LF — skip past the CR and keep scanning.
      scan_start = found_off + 1;
      continue;
    }
    // Frame is [consumed, found_off); delimiter is [found_off, found_off+2).
    const size_t frame_len = found_off - consumed;
    pending_frames_.emplace_back(read_buf_.data() + consumed, frame_len);
    ++enqueued;
    consumed = found_off + kFrameDelimiterLen;
    scan_start = consumed;
  }
  if (consumed > 0) {
    // Single splice at the end to avoid quadratic erase-per-frame cost.
    read_buf_.erase(read_buf_.begin(), read_buf_.begin() + static_cast<std::ptrdiff_t>(consumed));
  }
  if (enqueued > 0) {
    received_frame_.store(true, std::memory_order_release);
  }
  return enqueued;
}

bool ReactorConnection::ScheduleDrainTask() {
  bool expected = false;
  if (!drain_scheduled_.compare_exchange_strong(expected, true, std::memory_order_acq_rel, std::memory_order_acquire)) {
    // A drain task is already running or queued; it will pick up the new
    // frames when it next checks `pending_frames_`.
    return true;
  }

  if (thread_pool_ == nullptr || dispatcher_ == nullptr) {
    // Misconfiguration — no way to process the frames.
    drain_scheduled_.store(false, std::memory_order_release);
    closing_.store(true, std::memory_order_release);
    return false;
  }

  auto self = shared_from_this();
  const bool submitted = thread_pool_->Submit([self]() { self->DrainTask(); });
  if (!submitted) {
    drain_scheduled_.store(false, std::memory_order_release);
    mygram::utils::StructuredLog().Event("reactor_drain_submit_failed").Field("fd", static_cast<int64_t>(fd_)).Warn();
    closing_.store(true, std::memory_order_release);
    return false;
  }
  return true;
}

void ReactorConnection::DrainTask() {
  while (!closing_.load(std::memory_order_acquire)) {
    std::string frame;
    {
      std::lock_guard<std::mutex> lock(frame_mutex_);
      if (pending_frames_.empty()) {
        break;
      }
      frame = std::move(pending_frames_.front());
      pending_frames_.pop_front();
    }

    // Dispatch. `Dispatch` is synchronous and returns the full response.
    // The per-request counter is incremented inside RequestDispatcher::Dispatch
    // so all dispatch paths agree on a single canonical site; do not call
    // stats_->IncrementRequests() here or the request count will double.
    std::string response = dispatcher_->Dispatch(frame, conn_ctx_);

    // Enqueue the response for non-blocking send. The fast path in
    // EnqueueResponse attempts an inline drain before returning; only on
    // EAGAIN does it hand off to the event loop via ArmWrite.
    if (!EnqueueResponse(std::move(response))) {
      closing_.store(true, std::memory_order_release);
      break;
    }
  }

  // Netty/Vert.x "clear-then-recheck": before releasing the drain slot,
  // confirm that no new frames arrived in the window between the last
  // queue-empty check and now. If frames did arrive, reschedule ourselves.
  //
  // Bug fix (P1-4): the previous version cleared `drain_scheduled_=false`
  // OUTSIDE the frame_mutex_ critical section and BEFORE calling
  // ScheduleDrainTask. That created a window where another thread's
  // ScheduleDrainTask CAS could succeed (because drain_scheduled_ was
  // momentarily false) AND this task's subsequent ScheduleDrainTask CAS
  // would also succeed (after the other task's CAS reset it to false), so
  // two drain tasks ran concurrently against the same connection,
  // violating the "at most one drain task per connection" invariant.
  //
  // The fix: do the empty/closing test under frame_mutex_, and only flip
  // drain_scheduled_ to false in the path where we are NOT going to
  // reschedule. When we ARE going to reschedule, leave drain_scheduled_
  // as true so any concurrent ScheduleDrainTask CAS fails; this task then
  // submits the follow-up drain externally and returns.
  bool reschedule = false;
  {
    std::lock_guard<std::mutex> lock(frame_mutex_);
    if (pending_frames_.empty() || closing_.load(std::memory_order_acquire)) {
      drain_scheduled_.store(false, std::memory_order_release);
    } else {
      // Keep drain_scheduled_ = true so concurrent ScheduleDrainTask
      // attempts fail their CAS — we own the next drain submission.
      reschedule = true;
    }
  }
  if (reschedule) {
    // Submit the follow-up task directly. We deliberately bypass
    // ScheduleDrainTask's CAS because drain_scheduled_ is already true and
    // belongs to us; calling ScheduleDrainTask here would early-return
    // without submitting. If submission fails (e.g., thread pool full),
    // we must clear drain_scheduled_ so the connection can recover, then
    // close it because we can no longer guarantee progress on its frames.
    if (thread_pool_ == nullptr || dispatcher_ == nullptr) {
      drain_scheduled_.store(false, std::memory_order_release);
      closing_.store(true, std::memory_order_release);
    } else {
      auto self = shared_from_this();
      const bool submitted = thread_pool_->Submit([self]() { self->DrainTask(); });
      if (!submitted) {
        drain_scheduled_.store(false, std::memory_order_release);
        mygram::utils::StructuredLog()
            .Event("reactor_drain_resubmit_failed")
            .Field("fd", static_cast<int64_t>(fd_))
            .Warn();
        closing_.store(true, std::memory_order_release);
      }
    }
    return;
  }

  // If the peer half-closed (recv()==0) and we just finished dispatching the
  // last buffered frame, we own the close. Wait for the write queue to
  // drain first — the last response may still be in flight via
  // EnqueueResponse's EPOLLOUT fallback, in which case OnWritable will
  // perform the unregister once the queue empties.
  if (read_eof_.load(std::memory_order_acquire) && !closing_.load(std::memory_order_acquire)) {
    bool write_queue_empty = false;
    {
      std::lock_guard<std::mutex> lock(write_mutex_);
      write_queue_empty = write_queue_.empty();
    }
    if (write_queue_empty) {
      closing_.store(true, std::memory_order_release);
    }
  }

  if (closing_.load(std::memory_order_acquire) && reactor_ != nullptr) {
    // Ask the reactor to unregister us. This is safe from a worker: the
    // IoReactor::Unregister acquires the connections_ write lock, and the
    // event loop will observe the erase on its next Poll iteration. The
    // shared_ptr held by this lambda capture keeps the object alive until
    // DrainTask returns.
    reactor_->Unregister(fd_);
  }
}

bool ReactorConnection::EnqueueResponse(std::string response) {
  // Payload + CRLF terminator. We hold write_mutex_ across the entire
  // enqueue + optional inline drain + optional ArmWrite sequence so that
  // the event loop's OnWritable cannot race and pop frames out from under
  // us mid-drain, and so OnWritable cannot observe write_armed_ in an
  // inconsistent state relative to the multiplexer's interest mask.
  //
  // Holding `write_mutex_` across `reactor_->ArmWrite` is safe: ArmWrite
  // only acquires the reactor's `mux_lifecycle_` (shared). No IoReactor
  // method ever takes `write_mutex_`, so there is no reverse lock order.
  const size_t payload_bytes = response.size() + kResponseTerminatorLen;

  std::unique_lock<std::mutex> lock(write_mutex_);

  if (closing_.load(std::memory_order_acquire)) {
    return false;
  }

  // Slow-reader backpressure: cap the per-connection unsent byte budget.
  // Design doc §7 R3: exceeding the cap means the peer cannot keep up and
  // the server forcibly closes the connection to protect its own memory.
  if (write_queue_bytes_ + payload_bytes > max_write_queue_bytes_) {
    mygram::utils::StructuredLog()
        .Event("reactor_write_queue_overflow")
        .Field("fd", static_cast<int64_t>(fd_))
        .Field("current_bytes", static_cast<uint64_t>(write_queue_bytes_))
        .Field("attempted_bytes", static_cast<uint64_t>(payload_bytes))
        .Field("cap_bytes", static_cast<uint64_t>(max_write_queue_bytes_))
        .Warn();
    closing_.store(true, std::memory_order_release);
    return false;
  }

  response.append(kResponseTerminator, kResponseTerminatorLen);
  write_queue_.emplace_back(std::move(response));
  write_queue_bytes_ += payload_bytes;
  pending_write_bytes_.store(write_queue_bytes_, std::memory_order_relaxed);

  // Fast path: if the queue is not currently armed for EPOLLOUT, the
  // event loop is NOT going to drain us. Try an inline non-blocking drain
  // right here on the worker thread (design doc §4.2 D6: attempt write
  // immediately, register EPOLLOUT on EAGAIN).
  if (!write_armed_) {
    if (!DrainWriteQueueLocked()) {
      closing_.store(true, std::memory_order_release);
      return false;  // fatal send error
    }
    if (write_queue_.empty()) {
      return true;  // fully drained inline — no arming required
    }
    // Residue remains → ask the reactor to arm kWritable so the event
    // loop takes over.
    if (reactor_ == nullptr) {
      // Unit-test harness with no reactor and residue we cannot arm on.
      closing_.store(true, std::memory_order_release);
      return false;
    }
    auto arm_result = reactor_->ArmWrite(fd_);
    if (!arm_result) {
      mygram::utils::StructuredLog()
          .Event("reactor_arm_write_failed")
          .Field("fd", static_cast<int64_t>(fd_))
          .Field("error", arm_result.error().to_string())
          .Warn();
      closing_.store(true, std::memory_order_release);
      return false;
    }
    write_armed_ = true;
  }
  // Queue was already armed — event loop's OnWritable will pick up the
  // new entries when it next fires.
  return true;
}

bool ReactorConnection::DrainWriteQueueLocked() {
  while (!write_queue_.empty()) {
    const std::string& front = write_queue_.front();
    const char* data = front.data() + front_offset_;
    const size_t remaining = front.size() - front_offset_;

    ssize_t n = ::send(fd_, data, remaining, kSendFlags);
    if (n > 0) {
      front_offset_ += static_cast<size_t>(n);
      // Defensive underflow guard: a bug elsewhere that drives
      // write_queue_bytes_ below the actually-queued byte count would wrap
      // the size_t to ~SIZE_MAX, hiding the bug from the slow-reader gate
      // in EnqueueResponse. assert() catches it in debug builds; the
      // release-build clamp + structured log keeps the connection healthy
      // and surfaces the bug to operators without crashing.
      const auto sent_bytes = static_cast<size_t>(n);
      assert(write_queue_bytes_ >= sent_bytes);
      if (write_queue_bytes_ < sent_bytes) {
        mygram::utils::StructuredLog()
            .Event("reactor_write_accounting_underflow")
            .Field("fd", static_cast<int64_t>(fd_))
            .Field("write_queue_bytes", static_cast<uint64_t>(write_queue_bytes_))
            .Field("sent_bytes", static_cast<uint64_t>(sent_bytes))
            .Warn();
        write_queue_bytes_ = sent_bytes;
      }
      write_queue_bytes_ -= sent_bytes;
      pending_write_bytes_.store(write_queue_bytes_, std::memory_order_relaxed);
      if (front_offset_ == front.size()) {
        write_queue_.pop_front();
        front_offset_ = 0;
      }
      continue;
    }
    if (n == 0) {
      // send() returning 0 on a non-zero-length buffer is undefined per POSIX
      // but defensively treat as a fatal peer state.
      return false;
    }
    // n < 0
    if (errno == EINTR) {
      continue;
    }
    if (errno == EAGAIN || errno == EWOULDBLOCK) {
      return true;  // partial — event loop will finish via OnWritable
    }
    // EPIPE / ECONNRESET / ENOTCONN / etc.
    mygram::utils::StructuredLog()
        .Event("reactor_send_failed")
        .Field("fd", static_cast<int64_t>(fd_))
        .Field("errno", static_cast<int64_t>(errno))
        .Field("error", std::strerror(errno))
        .Debug();
    return false;
  }
  return true;
}

}  // namespace mygramdb::server
