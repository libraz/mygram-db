/**
 * @file reactor_connection.cpp
 * @brief Per-connection state + drain-task-per-connection pattern.
 *
 * Implements the Step 2 read side + Step 3 non-blocking write queue of
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
#include <limits>
#include <mutex>
#include <string>
#include <string_view>
#include <utility>

#include "server/io_reactor.h"
#include "server/request_dispatcher.h"
#include "server/response_formatter.h"
#include "server/server_stats.h"
#include "server/thread_pool.h"
#include "utils/fd_guard.h"
#include "utils/network_utils.h"
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

void BestEffortSendError(int fd, std::string_view message, mygram::utils::ErrorCode code) {
  std::string response = ResponseFormatter::FormatError(message, code);
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
                                                             size_t max_write_queue_bytes,
                                                             std::shared_ptr<ReactorMemoryBudget> memory_budget,
                                                             size_t max_pending_frames,
                                                             size_t max_pending_frame_bytes) {
  if (memory_budget == nullptr && reactor != nullptr) {
    memory_budget = reactor->MemoryBudget();
  }
  return std::make_shared<ReactorConnection>(fd, reactor, dispatcher, thread_pool, stats, max_write_queue_bytes,
                                             std::move(memory_budget), max_pending_frames, max_pending_frame_bytes);
}

ReactorConnection::ReactorConnection(int fd, IoReactor* reactor, RequestDispatcher* dispatcher, ThreadPool* thread_pool,
                                     ServerStats* stats, size_t max_write_queue_bytes,
                                     std::shared_ptr<ReactorMemoryBudget> memory_budget, size_t max_pending_frames,
                                     size_t max_pending_frame_bytes)
    : fd_(fd),
      max_write_queue_bytes_(max_write_queue_bytes),
      max_pending_frames_(std::max<size_t>(max_pending_frames, 1)),
      max_pending_frame_bytes_(std::max<size_t>(max_pending_frame_bytes, 1)),
      pending_frames_high_watermark_(std::max<size_t>(max_pending_frames_ * 3 / 4, 1)),
      pending_frames_low_watermark_(max_pending_frames_ / 2),
      pending_frame_bytes_high_watermark_(std::max<size_t>(max_pending_frame_bytes_ * 3 / 4, 1)),
      pending_frame_bytes_low_watermark_(max_pending_frame_bytes_ / 2),
      memory_budget_(std::move(memory_budget)),
      reactor_(reactor),
      dispatcher_(dispatcher),
      thread_pool_(thread_pool),
      stats_(stats) {
  const auto now = std::chrono::steady_clock::now();
  created_at_.store(now, std::memory_order_relaxed);
  last_active_.store(now, std::memory_order_relaxed);
  conn_ctx_.client_fd = fd_;
  // Use the stable "unknown" key when peer lookup fails. Leaving this empty
  // would make RequestDispatcher skip rate limiting entirely (fail-open).
  conn_ctx_.client_ip = mygram::utils::GetPeerIP(fd_);
}

ReactorConnection::~ReactorConnection() {
  if (memory_budget_ != nullptr) {
    std::scoped_lock lock(frame_mutex_, write_mutex_);
    memory_budget_->Release(read_buffer_budget_bytes_ + pending_frame_bytes_ + pending_frame_overhead_bytes_ +
                            write_queue_bytes_ + write_queue_overhead_bytes_);
    read_buffer_budget_bytes_ = 0;
    pending_frame_bytes_ = 0;
    pending_frame_overhead_bytes_ = 0;
    write_queue_bytes_ = 0;
    write_queue_overhead_bytes_ = 0;
  }
  if (!closed_ && fd_ >= 0) {
    ::close(fd_);
    closed_ = true;
  }
}

int ReactorConnection::ReleaseFd() noexcept {
  const int released_fd = fd_;
  fd_ = -1;
  closed_ = true;
  return released_fd;
}

bool ReactorConnection::OnReadable() {
  // Refresh idle-timer baseline. Any inbound event counts as activity for
  // the reaper, even if recv() ultimately returns 0 (peer half-close): the
  // peer just spoke to us, so we are not idle.
  last_active_.store(std::chrono::steady_clock::now(), std::memory_order_relaxed);

  if (closing_.load(std::memory_order_acquire)) {
    // Some other path (CloseWithServerBusy, a DrainTask exception handler,
    // ...) already decided to close and may have left a response queued
    // for send. Only report "safe to unregister" once that queue has
    // actually drained when that path chose a graceful terminal drain. Hard
    // I/O/cap/interest failures remain immediate closes even with residue.
    std::lock_guard<std::mutex> lock(write_mutex_);
    return close_after_write_drain_ && !write_queue_.empty();
  }

  {
    std::lock_guard<std::mutex> lock(frame_mutex_);
    // A readiness result can become stale before the event loop dispatches
    // it. Never bypass frame-queue backpressure, and never issue recv() after
    // EOF even if such an event was already present in the poll batch.
    if (read_paused_ || read_eof_.load(std::memory_order_acquire)) {
      return true;
    }
  }

  // 1. Drain the socket until EAGAIN / EWOULDBLOCK.
  std::array<char, kRecvChunkBytes> chunk{};
  size_t enqueued = 0;
  size_t received_bytes = 0;
  while (true) {
    ssize_t n = ::recv(fd_, chunk.data(), chunk.size(), 0);
    if (n > 0) {
      received_bytes += static_cast<size_t>(n);
      const ReadAppendStatus append_status = AppendReadBytes(chunk.data(), static_cast<size_t>(n), enqueued);
      if (append_status != ReadAppendStatus::kOk) {
        size_t read_buffer_size = 0;
        {
          std::lock_guard<std::mutex> lock(frame_mutex_);
          read_buffer_size = read_buf_.size();
        }
        // Each rejection keeps its own event name and wire error: an operator
        // reading the log, and a client branching on the numeric code, must
        // both be able to tell a payload problem from a multiplexer fault.
        const char* overflow_event = "reactor_read_buf_overflow";
        std::string_view wire_message = "request too large";
        mygram::utils::ErrorCode wire_code = mygram::utils::ErrorCode::kNetworkInvalidRequest;
        if (append_status == ReadAppendStatus::kFrameQueueOverflow) {
          overflow_event = "reactor_pending_frames_overflow";
          wire_message = "server busy";
          wire_code = mygram::utils::ErrorCode::kServerBusy;
        } else if (append_status == ReadAppendStatus::kInterestUpdateFailed) {
          overflow_event = "reactor_read_interest_update_failed";
          wire_message = "event multiplexer modify failed";
          wire_code = mygram::utils::ErrorCode::kNetworkReactorModifyFailed;
        }
        mygram::utils::StructuredLog()
            .Event(overflow_event)
            .Field("fd", static_cast<int64_t>(fd_))
            .Field("buf_bytes", static_cast<uint64_t>(read_buffer_size))
            .Field("cap_bytes", static_cast<uint64_t>(kMaxReadBufferBytes))
            .Field("pending_frames", static_cast<uint64_t>(PendingFrameCountForTest()))
            .Field("pending_frame_bytes", static_cast<uint64_t>(PendingFrameBytesForTest()))
            .Warn();
        // The helper publishes closing_ while holding write_mutex_, before
        // checking the queue or sending. An in-flight handler therefore
        // cannot enqueue a later response ahead of or after this error.
        const bool close_now = CloseWithError(wire_message, wire_code);
        return !close_now;
      }
      {
        std::lock_guard<std::mutex> lock(frame_mutex_);
        // Backpressure can be published by this recv() chunk. Stop draining
        // immediately so the event loop never writes read_buf_ while the
        // worker is allowed to extract its queued suffix.
        if (read_paused_) {
          break;
        }
      }
      // epoll and kqueue are level-triggered, so yielding here cannot lose
      // unread socket data. This prevents a single hot fd from monopolizing
      // the event loop even when it continuously streams valid frames.
      if (received_bytes >= kReadEventByteBudget || enqueued >= kReadEventFrameBudget) {
        break;
      }
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
      //
      // Publish EOF and remove read interest while holding frame_mutex_.
      // DrainTask takes the same lock before MaybeResumeReadsLocked(), so it
      // can neither miss EOF nor re-arm a permanently-readable EOF socket
      // between these two operations.
      bool interest_update_ok = false;
      {
        std::lock_guard<std::mutex> lock(frame_mutex_);
        interest_update_ok = PublishReadEofLocked();
      }
      if (!interest_update_ok) {
        MarkClosingImmediately();
        return false;
      }
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
    MarkClosingImmediately();
    return false;
  }

  // 2. If we parsed at least one frame, make sure a drain task is running.
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
  // The empty-queue test must hold BOTH mutexes simultaneously. Releasing
  // frame_mutex_ before acquiring write_mutex_ would let an in-flight
  // DrainTask enqueue a response after pending_frames_/drain_scheduled_ were
  // observed empty but before write_queue_ is read, closing with a response
  // still queued.
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
        MarkClosingImmediatelyLocked();
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
  // Outbound progress also resets the idle-timer: a slow client
  // that is steadily draining its socket is not "idle" even if it never
  // sends another request.
  last_active_.store(std::chrono::steady_clock::now(), std::memory_order_relaxed);

  std::unique_lock<std::mutex> lock(write_mutex_);

  // Hard terminal failures must not send any additional bytes, even if older
  // responses remain queued. Graceful terminal paths set
  // close_after_write_drain_ and are the only closes that OnWritable drains.
  if (closing_.load(std::memory_order_acquire) && !close_after_write_drain_) {
    return false;
  }

  if (!DrainWriteQueueLocked()) {
    // Fatal send error during drain.
    MarkClosingImmediatelyLocked();
    return false;
  }

  if (!write_queue_.empty()) {
    // Partial drain: leave the queue armed, fire again on next writable event.
    return true;
  }

  // Fully drained: disarm kWritable so the event loop stops spinning on
  // this fd. If we had never actually armed (edge case — OnWritable fired
  // spuriously), skip the disarm call.
  //
  // Clear write_armed_ only when the disarm reached the multiplexer. Clearing
  // it after a failure would leave the kernel holding writable interest that
  // nothing will ever retract: OnWritable would then fire every loop
  // iteration, find an empty queue, and skip the disarm because the guard
  // above already reads false. Treat the failure as fatal for this connection,
  // matching the ArmWrite / SetReadEnabled paths.
  if (write_armed_ && reactor_ != nullptr) {
    auto disarm_result = reactor_->DisarmWrite(fd_, this);
    if (!disarm_result) {
      mygram::utils::StructuredLog()
          .Event("reactor_disarm_write_failed")
          .Field("fd", static_cast<int64_t>(fd_))
          .Field("error", disarm_result.error().to_string())
          .Warn();
      MarkClosingImmediatelyLocked();
      return false;
    }
    write_armed_ = false;
  }

  if (closing_.load(std::memory_order_acquire)) {
    return false;
  }

  // Peer already half-closed and the drain task has no more work in flight:
  // we just flushed the last response, so unregister now. Release
  // write_mutex_ before taking frame_mutex_ to preserve the file-wide lock
  // order (frame_mutex_ -> write_mutex_); reacquire in that order for the
  // final empty-queue check and close publication.
  lock.unlock();
  if (read_eof_.load(std::memory_order_acquire)) {
    std::lock_guard<std::mutex> frame_lock(frame_mutex_);
    std::lock_guard<std::mutex> write_lock(write_mutex_);
    if (closing_.load(std::memory_order_acquire)) {
      return close_after_write_drain_ && !write_queue_.empty();
    }
    if (pending_frames_.empty() && !drain_scheduled_.load(std::memory_order_acquire) && write_queue_.empty()) {
      MarkClosingImmediatelyLocked();
      return false;
    }
  }

  return true;
}

bool ReactorConnection::OnError() {
  MarkClosingImmediately();
  return false;
}

ReactorConnection::ReadAppendStatus ReactorConnection::AppendReadBytes(const char* data, size_t len, size_t& enqueued) {
  // The drain worker may call ExtractFramesLocked() while read_paused_. Keep
  // the insert, capacity check, accounting, and extraction in one critical
  // section so vector reallocation cannot race its data()/swap operations.
  std::lock_guard<std::mutex> lock(frame_mutex_);
  if (len > read_buf_.max_size() - read_buf_.size()) {
    return ReadAppendStatus::kReadBufferOverflow;
  }
  if (memory_budget_ != nullptr && !memory_budget_->TryReserve(len)) {
    frame_queue_overflow_ = true;
    mygram::utils::StructuredLog()
        .Event("reactor_global_buffer_budget_exhausted")
        .Field("fd", static_cast<int64_t>(fd_))
        .Field("kind", "incomplete_request")
        .Field("attempted_bytes", static_cast<uint64_t>(len))
        .Field("used_bytes", static_cast<uint64_t>(memory_budget_->UsedBytes()))
        .Field("cap_bytes", static_cast<uint64_t>(memory_budget_->LimitBytes()))
        .Warn();
    return ReadAppendStatus::kFrameQueueOverflow;
  }
  auto read_reservation_guard = mygram::utils::ScopeGuard([this, len]() {
    if (memory_budget_ != nullptr) {
      memory_budget_->Release(len);
    }
  });
  read_buf_.insert(read_buf_.end(), data, data + len);
  read_buffer_budget_bytes_ += len;
  read_reservation_guard.Release();
  bool interest_update_ok = true;
  enqueued += ExtractFramesLocked();
  if (!read_paused_ && (pending_frames_.size() >= pending_frames_high_watermark_ ||
                        pending_frame_bytes_ >= pending_frame_bytes_high_watermark_)) {
    // Serialize the state transition and mux update with DrainTask's low-
    // watermark rearm so a fast drain cannot re-enable reads before this
    // disarm reaches the kernel.
    read_paused_ = true;
    if (reactor_ != nullptr) {
      auto result = reactor_->SetReadEnabled(fd_, this, false);
      interest_update_ok = result.has_value();
    }
  }

  // A failed interest update is an engine-layer fault, not a property of the
  // bytes just received; report it ahead of the request-shaped conditions so
  // the caller never attributes a syscall failure to the peer's payload.
  if (!interest_update_ok) {
    return ReadAppendStatus::kInterestUpdateFailed;
  }
  if (frame_queue_overflow_) {
    return ReadAppendStatus::kFrameQueueOverflow;
  }

  // Hard cap on the unframed tail only. Completed CRLF-delimited frames are
  // moved to pending_frames_ above before this check, so a valid pipelined
  // burst larger than 1 MiB is not mistaken for one oversized request.
  if (read_buf_.size() > kMaxReadBufferBytes) {
    return ReadAppendStatus::kReadBufferOverflow;
  }
  return ReadAppendStatus::kOk;
}

bool ReactorConnection::ShouldSendReadOverflowError() {
  std::lock_guard<std::mutex> lock(write_mutex_);
  return write_queue_.empty();
}

void ReactorConnection::MarkClosingImmediately() {
  std::lock_guard<std::mutex> lock(write_mutex_);
  MarkClosingImmediatelyLocked();
}

void ReactorConnection::MarkClosingImmediatelyLocked() {
  close_after_write_drain_ = false;
  closing_.store(true, std::memory_order_release);
}

bool ReactorConnection::CloseWithError(std::string_view message, mygram::utils::ErrorCode code,
                                       const std::function<void()>& under_lock_hook) {
  std::lock_guard<std::mutex> lock(write_mutex_);

  // A previous terminal path owns the close. Never append a second error;
  // report whether its accepted write queue still needs the reactor.
  if (closing_.load(std::memory_order_acquire)) {
    return !(close_after_write_drain_ && !write_queue_.empty());
  }

  // Publish terminal state before observing the queue. EnqueueResponse uses
  // the same mutex, so no later normal response can slip in after this point.
  closing_.store(true, std::memory_order_release);
  if (!write_queue_.empty()) {
    close_after_write_drain_ = true;
    return false;
  }
  close_after_write_drain_ = false;

  if (under_lock_hook) {
    under_lock_hook();
  }

  // Frame construction can allocate. closing_ is already published, so an
  // exception cannot let a later handler response through or trigger a
  // second error from DrainTask's catch path.
  try {
    BestEffortSendError(fd_, message, code);
  } catch (...) {
    // This is explicitly best effort; the caller will close the connection.
  }
  return true;
}

bool ReactorConnection::CloseWithServerBusy() {
  // Publish closing before constructing the error frame: even allocation
  // failure must prevent an in-flight handler from enqueueing afterward.
  std::unique_lock<std::mutex> lock(write_mutex_);
  if (closing_.load(std::memory_order_acquire)) {
    return close_after_write_drain_ && !write_queue_.empty();
  }
  closing_.store(true, std::memory_order_release);
  close_after_write_drain_ = false;

  try {
    std::string response = ResponseFormatter::FormatError("SERVER_BUSY Server is too busy, please try again later",
                                                          mygram::utils::ErrorCode::kServerBusy);
    if (!EnqueueResponseLocked(std::move(response), /*terminal=*/true)) {
      return false;
    }
  } catch (...) {
    MarkClosingImmediatelyLocked();
    return false;
  }
  close_after_write_drain_ = !write_queue_.empty();
  return close_after_write_drain_;
}

bool ReactorConnection::SubmitDrainTaskToPool(std::string_view failure_event) {
  auto self = shared_from_this();
  if (thread_pool_->Submit([self]() { self->DrainTask(); })) {
    return true;
  }

  drain_scheduled_.store(false, std::memory_order_release);
  if (stats_ != nullptr) {
    stats_->IncrementRequestsDeniedPoolFullTcp();
  }
  mygram::utils::StructuredLog().Event(std::string(failure_event)).Field("fd", static_cast<int64_t>(fd_)).Warn();
  const bool response_pending = CloseWithServerBusy();
  if (response_pending) {
    // The SERVER_BUSY response is now queued with write interest armed
    // (EnqueueResponse arms it whenever it cannot complete inline). Report
    // success so the caller does not unregister out from under it;
    // OnWritable drains the queue and finishes the close on its own.
    return true;
  }
  if (reactor_ != nullptr) {
    reactor_->Unregister(fd_, this);
  }
  return false;
}

bool ReactorConnection::MaybeResumeReadsLocked() {
  // EOF sockets remain level-readable forever. OnReadable publishes EOF and
  // disarms reads under frame_mutex_, so observing EOF here means the disarm
  // happened-before this drain-side low-watermark check.
  if (read_eof_.load(std::memory_order_acquire)) {
    return true;
  }
  if (!read_paused_ || pending_frames_.size() > pending_frames_low_watermark_ ||
      pending_frame_bytes_ > pending_frame_bytes_low_watermark_) {
    return true;
  }

  // A single recv() chunk can contain more complete tiny frames than the
  // high watermark. ExtractFramesLocked deliberately leaves that suffix in
  // read_buf_. While reads are disarmed, this drain worker owns the buffer;
  // refill the pending queue before deciding whether kernel reads may resume.
  (void)ExtractFramesLocked();
  if (frame_queue_overflow_) {
    return false;
  }
  if (pending_frames_.size() >= pending_frames_high_watermark_ ||
      pending_frame_bytes_ >= pending_frame_bytes_high_watermark_) {
    return true;
  }
  if (reactor_ != nullptr) {
    auto result = reactor_->SetReadEnabled(fd_, this, true);
    if (!result) {
      return false;
    }
  }
  read_paused_ = false;
  return true;
}

bool ReactorConnection::PublishReadEofLocked() {
  if (read_eof_.load(std::memory_order_acquire)) {
    return true;
  }
  read_eof_.store(true, std::memory_order_release);
  if (reactor_ == nullptr) {
    return true;
  }
  return reactor_->SetReadEnabled(fd_, this, false).has_value();
}

size_t ReactorConnection::ExtractFramesLocked() {
  size_t enqueued = 0;
  size_t scan_start = std::min(read_scan_start_, read_buf_.size());
  size_t consumed = 0;
  while (scan_start + kFrameDelimiterLen <= read_buf_.size()) {
    if (pending_frames_.size() >= pending_frames_high_watermark_ ||
        pending_frame_bytes_ >= pending_frame_bytes_high_watermark_) {
      break;
    }
    // Search for the next delimiter.
    const char* begin = read_buf_.data() + scan_start;
    const size_t remaining = read_buf_.size() - scan_start;
    const char* found = static_cast<const char*>(std::memchr(begin, kFrameDelimiter[0], remaining));
    if (found == nullptr) {
      scan_start = read_buf_.size();
      break;
    }
    const size_t found_off = static_cast<size_t>(found - read_buf_.data());
    if (found_off + kFrameDelimiterLen > read_buf_.size()) {
      scan_start = found_off;
      break;  // delimiter straddles the buffer end; wait for more bytes
    }
    if (read_buf_[found_off + 1] != kFrameDelimiter[1]) {
      // Lone CR without LF — skip past the CR and keep scanning.
      scan_start = found_off + 1;
      continue;
    }
    // Frame is [consumed, found_off); delimiter is [found_off, found_off+2).
    const size_t frame_len = found_off - consumed;
    if (frame_len > kMaxReadBufferBytes || pending_frames_.size() >= max_pending_frames_ ||
        frame_len > max_pending_frame_bytes_ - std::min(pending_frame_bytes_, max_pending_frame_bytes_)) {
      frame_queue_overflow_ = true;
      break;
    }
    const size_t frame_budget_bytes = frame_len + kQueueEntryOverheadBytes;
    if (memory_budget_ != nullptr && !memory_budget_->TryReserve(frame_budget_bytes)) {
      frame_queue_overflow_ = true;
      mygram::utils::StructuredLog()
          .Event("reactor_global_buffer_budget_exhausted")
          .Field("fd", static_cast<int64_t>(fd_))
          .Field("kind", "request_frame")
          .Field("attempted_bytes", static_cast<uint64_t>(frame_budget_bytes))
          .Field("used_bytes", static_cast<uint64_t>(memory_budget_->UsedBytes()))
          .Field("cap_bytes", static_cast<uint64_t>(memory_budget_->LimitBytes()))
          .Warn();
      break;
    }
    auto reservation_guard = mygram::utils::ScopeGuard([this, frame_budget_bytes]() {
      if (memory_budget_ != nullptr) {
        memory_budget_->Release(frame_budget_bytes);
      }
    });
    pending_frames_.emplace_back(read_buf_.data() + consumed, frame_len);
    reservation_guard.Release();
    pending_frame_bytes_ += frame_len;
    pending_frame_overhead_bytes_ += kQueueEntryOverheadBytes;
    ++enqueued;
    consumed = found_off + kFrameDelimiterLen;
    scan_start = consumed;
  }
  if (scan_start + kFrameDelimiterLen > read_buf_.size() &&
      (read_buf_.empty() || read_buf_.back() != kFrameDelimiter[0])) {
    scan_start = read_buf_.size();
  }
  // Preserve the point already searched on incomplete input. Retain a final
  // CR so a following LF appended by the next recv() can complete its frame.
  size_t next_scan_start = scan_start;
  if (next_scan_start == read_buf_.size() && !read_buf_.empty() && read_buf_.back() == kFrameDelimiter[0]) {
    --next_scan_start;
  }

  if (consumed > 0) {
    // Compact the unframed tail so bytes transferred to pending strings do
    // not remain hidden in vector capacity outside the global budget. Charge
    // the replacement tail before allocating it, then release the complete
    // old-buffer ownership after the swap.
    const size_t tail_size = read_buf_.size() - consumed;
    bool compacted = false;
    const bool tail_reserved = memory_budget_ != nullptr && tail_size > 0 && memory_budget_->TryReserve(tail_size);
    if (memory_budget_ == nullptr || tail_size == 0 || tail_reserved) {
      auto tail_reservation_guard = mygram::utils::ScopeGuard([this, tail_size, tail_reserved]() {
        if (tail_reserved) {
          memory_budget_->Release(tail_size);
        }
      });
      std::vector<char> tail(read_buf_.begin() + static_cast<std::ptrdiff_t>(consumed), read_buf_.end());
      read_buf_.swap(tail);
      if (memory_budget_ != nullptr) {
        memory_budget_->Release(read_buffer_budget_bytes_);
      }
      read_buffer_budget_bytes_ = tail_size;
      tail_reservation_guard.Release();
      compacted = true;
    }
    if (!compacted) {
      // Keeping the old capacity and its full charge is conservative. The
      // logical prefix is erased, but the budget is released only after a
      // later successful compaction or destruction.
      read_buf_.erase(read_buf_.begin(), read_buf_.begin() + static_cast<std::ptrdiff_t>(consumed));
    }
    read_scan_start_ = next_scan_start - consumed;
  } else {
    read_scan_start_ = next_scan_start;
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
    MarkClosingImmediately();
    return false;
  }

  return SubmitDrainTaskToPool("reactor_drain_submit_failed");
}

void ReactorConnection::DrainTask() {
  try {
    while (!closing_.load(std::memory_order_acquire)) {
      std::string frame;
      size_t frame_budget_bytes = 0;
      bool resume_failed = false;
      {
        std::lock_guard<std::mutex> lock(frame_mutex_);
        if (pending_frames_.empty()) {
          break;
        }
        frame_budget_bytes = pending_frames_.front().size();
        pending_frame_bytes_ -= frame_budget_bytes;
        pending_frame_overhead_bytes_ -= kQueueEntryOverheadBytes;
        frame = std::move(pending_frames_.front());
        pending_frames_.pop_front();
        resume_failed = !MaybeResumeReadsLocked();
      }
      auto frame_budget_guard = mygram::utils::ScopeGuard([this, frame_budget_bytes]() {
        if (memory_budget_ != nullptr) {
          memory_budget_->Release(frame_budget_bytes + kQueueEntryOverheadBytes);
        }
      });
      if (resume_failed) {
        MarkClosingImmediately();
        break;
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
        // EnqueueResponse either observed an already-published terminal close
        // or published an immediate hard failure itself. Do not overwrite a
        // graceful terminal-drain decision here.
        break;
      }
    }
  } catch (const std::exception& error) {
    mygram::utils::StructuredLog()
        .Event("reactor_drain_exception")
        .Field("fd", static_cast<int64_t>(fd_))
        .Field("error", error.what())
        .Error();
    (void)CloseWithError("internal server error", mygram::utils::ErrorCode::kInternalError);
  } catch (...) {
    mygram::utils::StructuredLog()
        .Event("reactor_drain_unknown_exception")
        .Field("fd", static_cast<int64_t>(fd_))
        .Error();
    (void)CloseWithError("internal server error", mygram::utils::ErrorCode::kInternalError);
  }

  // Netty/Vert.x "clear-then-recheck": before releasing the drain slot,
  // confirm that no new frames arrived in the window between the last
  // queue-empty check and now. If frames did arrive, reschedule ourselves.
  //
  // The empty/closing test runs under frame_mutex_, and drain_scheduled_ is
  // cleared only on the path that does NOT reschedule. When rescheduling,
  // drain_scheduled_ stays true so any concurrent ScheduleDrainTask CAS
  // fails; this task then submits the follow-up drain and returns. This keeps
  // the "at most one drain task per connection" invariant.
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
      MarkClosingImmediately();
    } else {
      (void)SubmitDrainTaskToPool("reactor_drain_resubmit_failed");
    }
    return;
  }

  // If the peer half-closed (recv()==0) and we just finished dispatching the
  // last buffered frame, we own the close. Wait for the write queue to
  // drain first — the last response may still be in flight via
  // EnqueueResponse's EPOLLOUT fallback, in which case OnWritable will
  // perform the unregister once the queue empties.
  if (read_eof_.load(std::memory_order_acquire) && !closing_.load(std::memory_order_acquire)) {
    std::lock_guard<std::mutex> lock(write_mutex_);
    if (write_queue_.empty()) {
      MarkClosingImmediatelyLocked();
    }
  }

  bool should_unregister = false;
  if (closing_.load(std::memory_order_acquire)) {
    std::lock_guard<std::mutex> lock(write_mutex_);
    should_unregister = !close_after_write_drain_ || write_queue_.empty();
  }
  if (should_unregister && reactor_ != nullptr) {
    // Ask the reactor to unregister us. This is safe from a worker: the
    // IoReactor::Unregister acquires the connections_ write lock, and the
    // event loop will observe the erase on its next Poll iteration. The
    // shared_ptr held by this lambda capture keeps the object alive until
    // DrainTask returns.
    reactor_->Unregister(fd_, this);
  }
}

bool ReactorConnection::EnqueueResponse(std::string response) {
  std::unique_lock<std::mutex> lock(write_mutex_);
  return EnqueueResponseLocked(std::move(response), /*terminal=*/false);
}

bool ReactorConnection::EnqueueResponseLocked(std::string response, bool terminal) {
  // Payload + CRLF terminator. We hold write_mutex_ across the entire
  // enqueue + optional inline drain + optional ArmWrite sequence so that
  // the event loop's OnWritable cannot race and pop frames out from under
  // us mid-drain, and so OnWritable cannot observe write_armed_ in an
  // inconsistent state relative to the multiplexer's interest mask.
  //
  // Holding `write_mutex_` across `reactor_->ArmWrite` is safe: ArmWrite
  // only acquires the reactor's `mux_lifecycle_` (shared). No IoReactor
  // method ever takes `write_mutex_`, so there is no reverse lock order.
  if (response.size() > std::numeric_limits<size_t>::max() - kResponseTerminatorLen) {
    MarkClosingImmediatelyLocked();
    return false;
  }
  const size_t payload_bytes = response.size() + kResponseTerminatorLen;

  if (terminal) {
    closing_.store(true, std::memory_order_release);
    close_after_write_drain_ = false;
  } else if (closing_.load(std::memory_order_acquire)) {
    return false;
  }

  // Slow-reader backpressure: cap the per-connection unsent byte budget.
  // Design doc §7 R3: exceeding the cap means the peer cannot keep up and
  // the server forcibly closes the connection to protect its own memory.
  if (write_queue_bytes_ > max_write_queue_bytes_ || payload_bytes > max_write_queue_bytes_ - write_queue_bytes_) {
    mygram::utils::StructuredLog()
        .Event("reactor_write_queue_overflow")
        .Field("fd", static_cast<int64_t>(fd_))
        .Field("current_bytes", static_cast<uint64_t>(write_queue_bytes_))
        .Field("attempted_bytes", static_cast<uint64_t>(payload_bytes))
        .Field("cap_bytes", static_cast<uint64_t>(max_write_queue_bytes_))
        .Warn();
    MarkClosingImmediatelyLocked();
    return false;
  }

  if (payload_bytes > std::numeric_limits<size_t>::max() - kQueueEntryOverheadBytes) {
    MarkClosingImmediatelyLocked();
    return false;
  }
  const size_t response_budget_bytes = payload_bytes + kQueueEntryOverheadBytes;
  if (memory_budget_ != nullptr && !memory_budget_->TryReserve(response_budget_bytes)) {
    mygram::utils::StructuredLog()
        .Event("reactor_global_buffer_budget_exhausted")
        .Field("fd", static_cast<int64_t>(fd_))
        .Field("kind", "write_queue")
        .Field("attempted_bytes", static_cast<uint64_t>(response_budget_bytes))
        .Field("used_bytes", static_cast<uint64_t>(memory_budget_->UsedBytes()))
        .Field("cap_bytes", static_cast<uint64_t>(memory_budget_->LimitBytes()))
        .Warn();
    MarkClosingImmediatelyLocked();
    return false;
  }
  auto reservation_guard = mygram::utils::ScopeGuard([this, response_budget_bytes]() {
    if (memory_budget_ != nullptr) {
      memory_budget_->Release(response_budget_bytes);
    }
  });

  response.append(kResponseTerminator, kResponseTerminatorLen);
  write_queue_.emplace_back(std::move(response));
  write_queue_bytes_ += payload_bytes;
  write_queue_overhead_bytes_ += kQueueEntryOverheadBytes;
  reservation_guard.Release();
  pending_write_bytes_.store(write_queue_bytes_, std::memory_order_relaxed);

  // Fast path: if the queue is not currently armed for EPOLLOUT, the
  // event loop is NOT going to drain us. Try an inline non-blocking drain
  // right here on the worker thread (design doc §4.2 D6: attempt write
  // immediately, register EPOLLOUT on EAGAIN).
  if (!write_armed_) {
    if (!DrainWriteQueueLocked()) {
      MarkClosingImmediatelyLocked();
      return false;  // fatal send error
    }
    if (write_queue_.empty()) {
      close_after_write_drain_ = false;
      return true;  // fully drained inline — no arming required
    }
    // Residue remains → ask the reactor to arm kWritable so the event
    // loop takes over.
    if (reactor_ == nullptr) {
      // Unit-test harness with no reactor and residue we cannot arm on.
      MarkClosingImmediatelyLocked();
      return false;
    }
    auto arm_result = reactor_->ArmWrite(fd_, this);
    if (!arm_result) {
      mygram::utils::StructuredLog()
          .Event("reactor_arm_write_failed")
          .Field("fd", static_cast<int64_t>(fd_))
          .Field("error", arm_result.error().to_string())
          .Warn();
      MarkClosingImmediatelyLocked();
      return false;
    }
    write_armed_ = true;
  }
  // Queue was already armed — event loop's OnWritable will pick up the
  // new entries when it next fires.
  close_after_write_drain_ = terminal && !write_queue_.empty();
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
      if (memory_budget_ != nullptr) {
        memory_budget_->Release(sent_bytes);
      }
      pending_write_bytes_.store(write_queue_bytes_, std::memory_order_relaxed);
      if (front_offset_ == front.size()) {
        write_queue_.pop_front();
        write_queue_overhead_bytes_ -= kQueueEntryOverheadBytes;
        if (memory_budget_ != nullptr) {
          memory_budget_->Release(kQueueEntryOverheadBytes);
        }
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
