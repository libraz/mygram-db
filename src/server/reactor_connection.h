/**
 * @file reactor_connection.h
 * @brief Heap-allocated per-connection state for the reactor I/O model.
 *
 * This is the Step 2/3 implementation of the per-connection state object
 * described in docs/ja/design/reactor-io-refactor.md §4.3. An instance is
 * created per accepted client socket and lives on the heap as a
 * `std::shared_ptr`, jointly owned by:
 *   - `IoReactor`'s connection map (primary owner), and
 *   - any in-flight drain task captured by the thread pool.
 *
 * The shared ownership is deliberate (design doc §7 R5): once a worker has
 * started draining a connection's frame queue we must keep the object alive
 * until the worker finishes writing, even if the event loop has already
 * observed EPOLLHUP and unregistered the fd.
 *
 * Naming note: the design document calls this class `ConnectionContext`, but
 * that name is already used by `mygramdb::server::ConnectionContext` in
 * `server_types.h` for the per-request dispatch struct passed to command
 * handlers. To avoid a disruptive rename across ~36 files, the reactor
 * per-connection state type is introduced here as `ReactorConnection`.
 * Semantically it is exactly the type described in §4.3 of the design doc.
 *
 * -----------------------------------------------------------------------
 * Thread-safety contract
 * -----------------------------------------------------------------------
 *   - `read_buf_` and the frame queue are protected by `frame_mutex_`.
 *     Normally the event loop appends/extracts frames, but a drain worker may
 *     extract an already-buffered suffix while reads are paused.
 *   - `pending_frames_` is shared between the event-loop thread (producer)
 *     and a worker thread (consumer) and is protected by `frame_mutex_`.
 *   - `write_queue_`, `write_queue_bytes_`, `front_offset_`, `write_armed_`
 *     are shared between the worker thread (via `EnqueueResponse` → inline
 *     drain) and the event-loop thread (via `OnWritable`) and are protected
 *     by `write_mutex_`. The contract is: holders of `write_mutex_` may
 *     call `reactor_->ArmWrite/DisarmWrite` while the mutex is held. The
 *     reverse is never done (no IoReactor method acquires `write_mutex_`).
 *   - `closing_` and `drain_scheduled_` are atomics.
 *   - `fd_` is owned by this object after construction unless `ReleaseFd()`
 *     is called before reactor registration succeeds. The destructor closes
 *     owned fds exactly once via `closed_` guard.
 *   - `reactor_`, `dispatcher_`, `thread_pool_` are set once at construction
 *     and read-only thereafter.
 */

#pragma once

#include <sys/types.h>

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <vector>

#include "server/server_types.h"
#include "utils/constants.h"

namespace mygramdb::server {

class IoReactor;
class RequestDispatcher;
class ServerStats;
class ThreadPool;

/** Shared admission budget for request frames and unsent responses. */
class ReactorMemoryBudget {
 public:
  explicit ReactorMemoryBudget(size_t limit_bytes) : limit_bytes_(limit_bytes) {}

  [[nodiscard]] bool TryReserve(size_t bytes) {
    size_t current = used_bytes_.load(std::memory_order_relaxed);
    while (bytes <= limit_bytes_ - std::min(current, limit_bytes_)) {
      if (used_bytes_.compare_exchange_weak(current, current + bytes, std::memory_order_acq_rel,
                                            std::memory_order_relaxed)) {
        return true;
      }
    }
    return false;
  }

  void Release(size_t bytes) {
    const size_t previous = used_bytes_.fetch_sub(bytes, std::memory_order_acq_rel);
    assert(previous >= bytes);
    if (previous < bytes) {
      used_bytes_.store(0, std::memory_order_release);
    }
  }

  [[nodiscard]] size_t UsedBytes() const { return used_bytes_.load(std::memory_order_relaxed); }
  [[nodiscard]] size_t LimitBytes() const { return limit_bytes_; }

 private:
  const size_t limit_bytes_;
  std::atomic<size_t> used_bytes_{0};
};

/**
 * @brief Per-connection state owned jointly by the reactor and drain tasks.
 *
 * Lifetime:
 *   1. `IoReactor::Register` inserts the shared_ptr into its map and arms
 *      `kReadable` on the multiplexer.
 *   2. The event loop calls `OnReadable`/`OnError`. When `OnReadable` parses
 *      at least one complete frame it schedules a drain task via the
 *      `ThreadPool`; the task captures a shared_ptr copy.
 *   3. If the connection is torn down (peer close, error, write failure),
 *      either the event loop or the drain task calls
 *      `IoReactor::Unregister(fd)`, which removes the shared_ptr from the
 *      map. The object is destroyed when the last shared_ptr (typically the
 *      drain task's) drops, and the destructor closes `fd_`.
 */
class ReactorConnection : public std::enable_shared_from_this<ReactorConnection> {
 public:
  /**
   * @brief Outcome of appending one recv() chunk to the read buffer.
   *
   * The rejection cases are kept apart because they belong to different
   * layers and must not be reported to the client under the same error code:
   * two are request-shaped conditions the peer can act on, the third is an
   * engine-layer syscall failure the peer cannot influence.
   */
  enum class ReadAppendStatus {
    kOk,                    ///< Bytes buffered; framing may have advanced.
    kReadBufferOverflow,    ///< Unframed tail exceeds `kMaxReadBufferBytes`.
    kFrameQueueOverflow,    ///< Pending-frame caps or the shared read budget are exhausted.
    kInterestUpdateFailed,  ///< The multiplexer rejected the backpressure interest update.
  };

  /// Default read buffer reservation. Grows on demand up to kMaxReadBufferBytes.
  static constexpr size_t kDefaultReadBufferBytes = 4096;

  /// Hard cap on the read accumulation buffer. This is an OOM safety rail
  /// only — per-query size enforcement (`api.max_query_length`) is the
  /// responsibility of the downstream query parser, which rejects oversized
  /// requests with a structured error. 1 MiB is comfortably above the
  /// default `max_query_length` (~64 KiB) and is deliberately decoupled
  /// from config so that lowering `max_query_length` at runtime cannot make
  /// the reactor drop well-formed but large requests that are still in
  /// flight on an existing connection.
  static constexpr size_t kMaxReadBufferBytes = mygram::constants::kBytesPerMegabyte;  // 1 MiB

  /// Hard upper bound on unsent response bytes; once exceeded the reactor
  /// forcibly closes the connection to protect against slow-reader OOM
  /// (see design doc §7 R3). Step 3 enforces this cap in `EnqueueResponse`:
  /// a push that would exceed the cap sets `closing_` and causes the drain
  /// task to tear down the connection.
  static constexpr size_t kDefaultMaxWriteQueueBytes = 16 * mygram::constants::kBytesPerMegabyte;  // 16 MiB

  /// Fairness budget for one level-triggered readable event. Remaining
  /// socket bytes stay readable and are reported on the next poll cycle.
  static constexpr size_t kReadEventByteBudget = 64 * 1024;
  static constexpr size_t kReadEventFrameBudget = 64;

  /// Hard limits for completed request frames awaiting worker dispatch.
  /// Admission is checked before allocating the next std::string.
  static constexpr size_t kMaxPendingFrames = 1024;
  static constexpr size_t kMaxPendingFrameBytes = 4 * mygram::constants::kBytesPerMegabyte;
  static constexpr size_t kPendingFramesHighWatermark = kMaxPendingFrames * 3 / 4;
  static constexpr size_t kPendingFramesLowWatermark = kMaxPendingFrames / 2;
  static constexpr size_t kPendingFrameBytesHighWatermark = kMaxPendingFrameBytes * 3 / 4;
  static constexpr size_t kPendingFrameBytesLowWatermark = kMaxPendingFrameBytes / 2;

  /// Conservative charge for deque node/string bookkeeping, including empty frames.
  static constexpr size_t kQueueEntryOverheadBytes = 64;

  /**
   * @brief Factory. Must be used instead of a bare constructor because
   *        `std::enable_shared_from_this` requires the object to live inside
   *        a `shared_ptr` from the moment it is born.
   *
   * @param stats  Optional non-owning pointer to `ServerStats`. Currently
   *               unused for per-request bookkeeping (the request counter is
   *               incremented inside `RequestDispatcher::Dispatch` so all
   *               dispatch paths converge on a single site). Retained for
   *               future per-connection stats and parity with other
   *               connection adapters; may be null in unit tests.
   */
  static std::shared_ptr<ReactorConnection> Create(int fd, IoReactor* reactor, RequestDispatcher* dispatcher,
                                                   ThreadPool* thread_pool, ServerStats* stats = nullptr,
                                                   size_t max_write_queue_bytes = kDefaultMaxWriteQueueBytes,
                                                   std::shared_ptr<ReactorMemoryBudget> memory_budget = nullptr,
                                                   size_t max_pending_frames = kMaxPendingFrames,
                                                   size_t max_pending_frame_bytes = kMaxPendingFrameBytes);

  /**
   * @brief Public constructor (required by `std::make_shared`). Prefer
   *        `Create()` at call sites for clarity.
   */
  ReactorConnection(int fd, IoReactor* reactor, RequestDispatcher* dispatcher, ThreadPool* thread_pool,
                    ServerStats* stats, size_t max_write_queue_bytes,
                    std::shared_ptr<ReactorMemoryBudget> memory_budget = nullptr,
                    size_t max_pending_frames = kMaxPendingFrames,
                    size_t max_pending_frame_bytes = kMaxPendingFrameBytes);

  ~ReactorConnection();

  ReactorConnection(const ReactorConnection&) = delete;
  ReactorConnection& operator=(const ReactorConnection&) = delete;
  ReactorConnection(ReactorConnection&&) = delete;
  ReactorConnection& operator=(ReactorConnection&&) = delete;

  /// Returns the raw client fd. The reactor still owns close(2).
  [[nodiscard]] int Fd() const { return fd_; }

  /**
   * @brief Release socket ownership without closing it.
   *
   * Used only when reactor registration fails before IoReactor has accepted
   * ownership. The acceptor then remains the sole closer and may send a
   * best-effort SERVER_BUSY response before close(2).
   */
  [[nodiscard]] int ReleaseFd() noexcept;

  // ---- Reactor event callbacks (event-loop thread) --------------------

  /**
   * @brief Handle `event::kReadable` for this connection.
   *
   * Drains the socket via non-blocking recv() into `read_buf_`, scans for
   * "\r\n"-delimited frames, enqueues each complete frame onto
   * `pending_frames_`, and schedules a single drain task on the thread pool
   * if one is not already in flight.
   *
   * @return false if the reactor should close and unregister this fd.
   */
  bool OnReadable();

  /**
   * @brief Handle `event::kWritable` for this connection.
   *
   * Step 3: drain `write_queue_` via non-blocking `send()` until EAGAIN
   * or empty. On full drain, call `reactor_->DisarmWrite(fd_)` and return
   * true (or false if `closing_` was also set, so the reactor tears down
   * the fd). On partial drain, leave the queue armed and return true. On
   * fatal send error (EPIPE / ECONNRESET / etc.), return false.
   *
   * A failing `DisarmWrite` is also fatal: `write_armed_` mirrors the
   * multiplexer's writable-interest bit for `fd_`, and clearing it while the
   * multiplexer still holds the bit would spin the event loop on this fd
   * forever. Every exit of this function preserves `write_armed_ == true` iff
   * the multiplexer holds `kWritable` for `fd_`.
   */
  bool OnWritable();

  /**
   * @brief Handle `event::kError` / `event::kHangup` for this connection.
   * Always returns false so the reactor tears the fd down.
   */
  bool OnError();

  /// Current bytes held in the pending write accounting (for metrics / tests).
  [[nodiscard]] size_t PendingWriteBytes() const { return pending_write_bytes_.load(std::memory_order_relaxed); }

  /// Whether `closing_` has been set. Exposed for tests.
  [[nodiscard]] bool IsClosing() const { return closing_.load(std::memory_order_acquire); }

  /// Timestamp of the last inbound socket event (read or write activity).
  /// Used by `IoReactor`'s idle-connection reaper. Updated by `OnReadable`
  /// and `OnWritable`. Atomic load is sufficient — relaxed ordering is fine
  /// because the reaper compares against `now()` and a slightly stale value
  /// only delays reaping by one tick.
  [[nodiscard]] std::chrono::steady_clock::time_point LastActive() const {
    return last_active_.load(std::memory_order_relaxed);
  }

  /// Timestamp captured at construction/accept time. Used for initial-read
  /// timeouts so a client cannot keep a socket forever by slowly dripping
  /// partial bytes without ever completing a frame.
  [[nodiscard]] std::chrono::steady_clock::time_point CreatedAt() const {
    return created_at_.load(std::memory_order_relaxed);
  }

  /// Whether this connection has completed at least one CRLF-delimited frame.
  [[nodiscard]] bool HasReceivedFrame() const { return received_frame_.load(std::memory_order_acquire); }

  /// Whether a complete request is queued for, or is currently executing in,
  /// the drain worker. Idle reaping must not close such a connection merely
  /// because a long-running handler has not performed socket I/O recently.
  [[nodiscard]] bool HasInFlightRequest() const { return drain_scheduled_.load(std::memory_order_acquire); }

  /// Whether recv() has observed an orderly EOF from the peer.
  ///
  /// IoReactor uses this to discard stale readable/hangup notifications
  /// already returned by the kernel before OnReadable() disarmed read
  /// interest. The write side may still be active while queued responses
  /// drain, so EOF is deliberately distinct from IsClosing().
  [[nodiscard]] bool HasReadEof() const { return read_eof_.load(std::memory_order_acquire); }

  /// Returns the number of frames currently in `pending_frames_`. Exposed for tests only.
  [[nodiscard]] size_t PendingFrameCountForTest() const {
    std::lock_guard<std::mutex> lock(frame_mutex_);
    return pending_frames_.size();
  }

  [[nodiscard]] size_t PendingFrameBytesForTest() const {
    std::lock_guard<std::mutex> lock(frame_mutex_);
    return pending_frame_bytes_;
  }

  [[nodiscard]] size_t GlobalBufferedBytesForTest() const {
    return memory_budget_ == nullptr ? 0 : memory_budget_->UsedBytes();
  }

  [[nodiscard]] bool ReadPausedForTest() const {
    std::lock_guard<std::mutex> lock(frame_mutex_);
    return read_paused_;
  }

  /// Current number of entries in the write queue. TEST ONLY.
  [[nodiscard]] size_t WriteQueueDepthForTest() const {
    std::lock_guard<std::mutex> lock(write_mutex_);
    return write_queue_.size();
  }

  /// Whether the reactor currently has `kWritable` armed for this fd.
  /// TEST ONLY.
  [[nodiscard]] bool WriteArmedForTest() const {
    std::lock_guard<std::mutex> lock(write_mutex_);
    return write_armed_;
  }

#ifdef MYGRAMDB_REACTOR_CONNECTION_TEST_HOOKS
  [[nodiscard]] const std::string& ClientIdentityForTest() const { return conn_ctx_.client_ip; }

  [[nodiscard]] bool AppendReadBytesForTest(std::string_view bytes, size_t& enqueued) {
    return AppendReadBytes(bytes.data(), bytes.size(), enqueued) == ReadAppendStatus::kOk;
  }

  [[nodiscard]] size_t ReadBufferSizeForTest() const {
    std::lock_guard<std::mutex> lock(frame_mutex_);
    return read_buf_.size();
  }

  [[nodiscard]] size_t ReadScanStartForTest() const {
    std::lock_guard<std::mutex> lock(frame_mutex_);
    return read_scan_start_;
  }

  [[nodiscard]] bool ShouldSendReadOverflowErrorForTest() { return ShouldSendReadOverflowError(); }

  void SetDrainScheduledForTest(bool value) { drain_scheduled_.store(value, std::memory_order_release); }

  [[nodiscard]] bool SendReadOverflowErrorForTest(std::string_view message, mygram::utils::ErrorCode code,
                                                  const std::function<void()>& under_lock_hook = {}) {
    return TrySendErrorIfWriteQueueEmpty(message, code, under_lock_hook);
  }

  void DrainPendingFramesForTest(size_t count) {
    std::lock_guard<std::mutex> lock(frame_mutex_);
    while (count-- > 0 && !pending_frames_.empty()) {
      pending_frame_bytes_ -= pending_frames_.front().size();
      pending_frame_overhead_bytes_ -= kQueueEntryOverheadBytes;
      if (memory_budget_ != nullptr) {
        memory_budget_->Release(pending_frames_.front().size() + kQueueEntryOverheadBytes);
      }
      pending_frames_.pop_front();
    }
    (void)MaybeResumeReadsLocked();
  }
#endif

  /**
   * @brief Enqueue a response for non-blocking send on this connection.
   *
   * The CRLF terminator is appended internally; callers should pass just
   * the response body. This is the Step 3 write-path entry point used by
   * `DrainTask`.
   *
   * The worker thread (drain task) owns the fast path: if the queue was
   * empty and `kWritable` was not armed, this call attempts an inline
   * non-blocking drain before returning. Only on EAGAIN does it ask the
   * reactor to arm `kWritable`, at which point the event loop takes over
   * and finishes draining via `OnWritable`.
   *
   * Failure modes (returns false AND sets `closing_`):
   *   - enqueue would exceed `max_write_queue_bytes_` (slow reader
   *     backpressure; see design doc §7 R3)
   *   - fatal send error during the inline drain (EPIPE / ECONNRESET)
   *   - `ArmWrite` failed (reactor was stopped mid-Enqueue)
   *   - Called with `reactor_` null AND the inline drain could not fully
   *     drain the queue (unit-test corner case)
   *
   * Called exclusively from worker threads; never from the event loop.
   */
  bool EnqueueResponse(std::string response);

 private:
  ReadAppendStatus AppendReadBytes(const char* data, size_t len, size_t& enqueued);
  bool ShouldSendReadOverflowError();
  bool TrySendErrorIfWriteQueueEmpty(std::string_view message, mygram::utils::ErrorCode code,
                                     const std::function<void()>& under_lock_hook = {});
  [[nodiscard]] bool CloseWithServerBusy();
  bool MaybeResumeReadsLocked();
  bool PublishReadEofLocked();
  bool SubmitDrainTaskToPool(std::string_view failure_event);

  /**
   * @brief Attempt to submit a drain task to the thread pool.
   *
   * Uses a compare-exchange on `drain_scheduled_` to ensure at most one
   * drain task is in flight per connection at any time. If the slot is
   * already claimed (a previous drain task is still running and will pick
   * up the newly enqueued frames when it next checks `pending_frames_`),
   * this is a no-op.
   *
   * @return false if submission failed (thread pool queue full). The caller
   *         should treat this as a fatal condition for the connection.
   */
  bool ScheduleDrainTask();

  /**
   * @brief Drain task body: runs in a worker thread.
   *
   * Pops frames from `pending_frames_`, dispatches each through
   * `RequestDispatcher`, and enqueues each response for non-blocking send
   * via `EnqueueResponse` (Step 3 write path). Implements the Netty/Vert.x
   * "clear-then-recheck" idiom to guarantee progress when new frames arrive
   * during the window between observing an empty queue and clearing
   * `drain_scheduled_`.
   */
  void DrainTask();

  /**
   * @brief Drain the write queue via non-blocking `send()` until the queue
   *        is empty or the socket reports EAGAIN. Must be called with
   *        `write_mutex_` held.
   *
   * Updates `front_offset_` for partial sends of the head frame.
   *
   * @return false on fatal send error (EPIPE / ECONNRESET / etc.);
   *         true on EAGAIN or when the queue was fully drained.
   */
  bool DrainWriteQueueLocked();

  /**
   * @brief Scan `read_buf_` for complete "\r\n"-terminated frames, move
   *        them into `pending_frames_`, and erase the consumed prefix in a
   *        single splice.
   *
   * @return number of frames newly enqueued (0 if no complete frame).
   */
  size_t ExtractFramesLocked();

  int fd_;
  bool closed_ = false;  // destructor close(2) guard
  const size_t max_write_queue_bytes_;
  const size_t max_pending_frames_;
  const size_t max_pending_frame_bytes_;
  const size_t pending_frames_high_watermark_;
  const size_t pending_frames_low_watermark_;
  const size_t pending_frame_bytes_high_watermark_;
  const size_t pending_frame_bytes_low_watermark_;
  std::shared_ptr<ReactorMemoryBudget> memory_budget_;

  // Non-owning collaborators. Set at construction, read-only afterwards.
  IoReactor* reactor_;
  RequestDispatcher* dispatcher_;
  ThreadPool* thread_pool_;
  ServerStats* stats_;  ///< Optional; null in unit tests.

  // Per-request context passed to `RequestDispatcher::Dispatch`. Filled in at
  // construction with client_fd = fd_.
  // Thread safety: `conn_ctx_.debug_mode` is `std::atomic<bool>`, safe for
  // cross-thread reads (event-loop) and writes (drain task / command handler).
  ConnectionContext conn_ctx_{};

  // Read-side state, protected by frame_mutex_. The event loop normally owns
  // appends while reads are enabled; once read_paused_ is published, the drain
  // worker may extract already-buffered frames before re-enabling interest.
  std::vector<char> read_buf_;
  size_t read_buffer_budget_bytes_ = 0;
  // First byte that has not yet been searched for a CRLF delimiter. Kept
  // relative to read_buf_ and adjusted whenever a consumed prefix is erased.
  size_t read_scan_start_ = 0;

  // Frame queue: event loop produces, drain task consumes.
  mutable std::mutex frame_mutex_;
  std::deque<std::string> pending_frames_;
  size_t pending_frame_bytes_ = 0;
  size_t pending_frame_overhead_bytes_ = 0;
  bool frame_queue_overflow_ = false;
  bool read_paused_ = false;

  // Write queue: drain task (worker) produces; either the worker itself
  // (inline fast path) or the event-loop thread (OnWritable slow path)
  // consumes. Protected by `write_mutex_`.
  mutable std::mutex write_mutex_;
  std::deque<std::string> write_queue_;
  size_t write_queue_bytes_ = 0;  ///< Sum of byte lengths in write_queue_.
  size_t write_queue_overhead_bytes_ = 0;
  size_t front_offset_ = 0;   ///< Bytes of write_queue_.front() already sent.
  bool write_armed_ = false;  ///< Whether reactor_->ArmWrite was called for this fd.

  // Atomic flags.
  /// Hard-close flag: connection must be torn down as soon as inflight work
  /// completes. Set on I/O errors, overflow, or after a half-closed client's
  /// pending frames have been fully dispatched *and* the write queue has
  /// drained. `EnqueueResponse` refuses to accept new writes when this is set.
  std::atomic<bool> closing_{false};
  /// Peer-has-stopped-writing flag: set when recv() returns 0 (orderly FIN
  /// from the client, including shutdown(SHUT_WR) half-close). Distinct from
  /// `closing_` because the server must still be allowed to flush the
  /// response for any already-buffered frames back to the peer — a TCP
  /// half-close keeps the write side open. Once set, OnReadable stops
  /// issuing recv() calls; the drain task closes the connection after the
  /// last response has been queued for send.
  std::atomic<bool> read_eof_{false};
  std::atomic<bool> drain_scheduled_{false};
  std::atomic<bool> received_frame_{false};

  // Mirror of `write_queue_bytes_` for lock-free metric readers.
  std::atomic<size_t> pending_write_bytes_{0};

  // Last-activity timestamp for idle reaping. Initialised to
  // construction time; refreshed at the start of OnReadable/OnWritable so a
  // connection that is actively performing I/O is never reaped, while a
  // connection that connected but never sent or read a byte ages out after
  // `IoReactor::idle_timeout_`.
  std::atomic<std::chrono::steady_clock::time_point> created_at_{std::chrono::steady_clock::now()};
  std::atomic<std::chrono::steady_clock::time_point> last_active_{std::chrono::steady_clock::now()};
};

}  // namespace mygramdb::server
