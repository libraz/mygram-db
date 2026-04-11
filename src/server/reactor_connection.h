/**
 * @file reactor_connection.h
 * @brief Heap-allocated per-connection state for the reactor I/O model.
 *
 * This is the Phase 2/3 implementation of the per-connection state object
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
 *   - `read_buf_` is touched exclusively by the event-loop thread (via
 *     `OnReadable`) and requires no locking.
 *   - `pending_frames_` is shared between the event-loop thread (producer)
 *     and a worker thread (consumer) and is protected by `frame_mutex_`.
 *   - `write_queue_`, `write_queue_bytes_`, `front_offset_`, `write_armed_`
 *     are shared between the worker thread (via `EnqueueResponse` → inline
 *     drain) and the event-loop thread (via `OnWritable`) and are protected
 *     by `write_mutex_`. The contract is: holders of `write_mutex_` may
 *     call `reactor_->ArmWrite/DisarmWrite` while the mutex is held. The
 *     reverse is never done (no IoReactor method acquires `write_mutex_`).
 *   - `closing_` and `drain_scheduled_` are atomics.
 *   - `fd_` is immutable after construction; the destructor closes it
 *     exactly once via `closed_` guard.
 *   - `reactor_`, `dispatcher_`, `thread_pool_` are set once at construction
 *     and read-only thereafter.
 */

#pragma once

#include <sys/types.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "server/server_types.h"

namespace mygramdb::server {

class IoReactor;
class RequestDispatcher;
class ServerStats;
class ThreadPool;

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
  /// Default read buffer reservation. Grows on demand up to kMaxReadBufferBytes.
  static constexpr size_t kDefaultReadBufferBytes = 4096;

  /// Hard cap on the read accumulation buffer. A single frame larger than
  /// this is rejected (peer is either misbehaving or under attack). 1 MiB is
  /// comfortably above the configured MygramDB query length limit (~64 KiB)
  /// while still providing OOM protection.
  static constexpr size_t kMaxReadBufferBytes = 1 * 1024 * 1024;  // 1 MiB

  /// Hard upper bound on unsent response bytes; once exceeded the reactor
  /// forcibly closes the connection to protect against slow-reader OOM
  /// (see design doc §7 R3). Phase 3 enforces this cap in `EnqueueResponse`:
  /// a push that would exceed the cap sets `closing_` and causes the drain
  /// task to tear down the connection.
  static constexpr size_t kDefaultMaxWriteQueueBytes = 16 * 1024 * 1024;  // 16 MiB

  /**
   * @brief Factory. Must be used instead of a bare constructor because
   *        `std::enable_shared_from_this` requires the object to live inside
   *        a `shared_ptr` from the moment it is born.
   *
   * @param stats  Optional non-owning pointer to `ServerStats`. If non-null,
   *               the drain task calls `stats->IncrementRequests()` after
   *               each successful Dispatch, matching the blocking path's
   *               per-request counter. May be null in unit tests.
   */
  static std::shared_ptr<ReactorConnection> Create(int fd, IoReactor* reactor, RequestDispatcher* dispatcher,
                                                   ThreadPool* thread_pool, ServerStats* stats = nullptr,
                                                   size_t max_write_queue_bytes = kDefaultMaxWriteQueueBytes);

  /**
   * @brief Public constructor (required by `std::make_shared`). Prefer
   *        `Create()` at call sites for clarity.
   */
  ReactorConnection(int fd, IoReactor* reactor, RequestDispatcher* dispatcher, ThreadPool* thread_pool,
                    ServerStats* stats, size_t max_write_queue_bytes);

  ~ReactorConnection();

  ReactorConnection(const ReactorConnection&) = delete;
  ReactorConnection& operator=(const ReactorConnection&) = delete;
  ReactorConnection(ReactorConnection&&) = delete;
  ReactorConnection& operator=(ReactorConnection&&) = delete;

  /// Returns the raw client fd. The reactor still owns close(2).
  [[nodiscard]] int Fd() const { return fd_; }

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
   * Phase 3: drain `write_queue_` via non-blocking `send()` until EAGAIN
   * or empty. On full drain, call `reactor_->DisarmWrite(fd_)` and return
   * true (or false if `closing_` was also set, so the reactor tears down
   * the fd). On partial drain, leave the queue armed and return true. On
   * fatal send error (EPIPE / ECONNRESET / etc.), return false.
   */
  bool OnWritable();

  /**
   * @brief Handle `event::kError` / `event::kHangup` for this connection.
   * Always returns false so the reactor tears the fd down.
   */
  bool OnError();

  /// Current bytes held in the pending write accounting (for metrics / tests).
  [[nodiscard]] size_t PendingWriteBytes() const {
    return pending_write_bytes_.load(std::memory_order_relaxed);
  }

  /// Whether `closing_` has been set. Exposed for tests.
  [[nodiscard]] bool IsClosing() const { return closing_.load(std::memory_order_acquire); }

  /// Returns the number of frames currently in `pending_frames_`. Exposed for tests only.
  [[nodiscard]] size_t PendingFrameCountForTest() const {
    std::lock_guard<std::mutex> lock(frame_mutex_);
    return pending_frames_.size();
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

  /**
   * @brief Enqueue a response for non-blocking send on this connection.
   *
   * The CRLF terminator is appended internally; callers should pass just
   * the response body. This is the Phase 3 write-path entry point used by
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
   * via `EnqueueResponse` (Phase 3 write path). Implements the Netty/Vert.x
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

  // Non-owning collaborators. Set at construction, read-only afterwards.
  IoReactor* reactor_;
  RequestDispatcher* dispatcher_;
  ThreadPool* thread_pool_;
  ServerStats* stats_;  ///< Optional; null in unit tests.

  // Per-request context passed to `RequestDispatcher::Dispatch`. Filled in at
  // construction with client_fd = fd_.
  ConnectionContext conn_ctx_{};

  // Read-side state — touched only by the event-loop thread.
  std::vector<char> read_buf_;

  // Frame queue: event loop produces, drain task consumes.
  mutable std::mutex frame_mutex_;
  std::deque<std::string> pending_frames_;

  // Write queue: drain task (worker) produces; either the worker itself
  // (inline fast path) or the event-loop thread (OnWritable slow path)
  // consumes. Protected by `write_mutex_`.
  mutable std::mutex write_mutex_;
  std::deque<std::string> write_queue_;
  size_t write_queue_bytes_ = 0;  ///< Sum of byte lengths in write_queue_.
  size_t front_offset_ = 0;       ///< Bytes of write_queue_.front() already sent.
  bool write_armed_ = false;      ///< Whether reactor_->ArmWrite was called for this fd.

  // Atomic flags.
  std::atomic<bool> closing_{false};
  std::atomic<bool> drain_scheduled_{false};

  // Mirror of `write_queue_bytes_` for lock-free metric readers.
  std::atomic<size_t> pending_write_bytes_{0};
};

}  // namespace mygramdb::server
