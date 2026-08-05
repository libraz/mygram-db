/**
 * @file binlog_reader_interface.h
 * @brief Abstract interface for BinlogReader to enable unit testing
 */

#pragma once

#include <cstdint>
#include <string>

#include "utils/error.h"
#include "utils/expected.h"

namespace mygramdb::mysql {

enum class ReplicationState : uint8_t {
  kRunning,
  kStopped,
  kFailed,
};

inline const char* ToString(ReplicationState state) {
  switch (state) {
    case ReplicationState::kRunning:
      return "running";
    case ReplicationState::kStopped:
      return "stopped";
    case ReplicationState::kFailed:
      return "failed";
  }
  return "failed";
}

/**
 * @brief Abstract interface for BinlogReader
 *
 * This interface enables unit testing of components that depend on BinlogReader
 * without requiring actual MySQL connections.
 */
class IBinlogReader {
 public:
  virtual ~IBinlogReader() = default;

  // Non-copyable and non-movable (polymorphic base class)
  IBinlogReader(const IBinlogReader&) = delete;
  IBinlogReader& operator=(const IBinlogReader&) = delete;
  IBinlogReader(IBinlogReader&&) = delete;
  IBinlogReader& operator=(IBinlogReader&&) = delete;

  /**
   * @brief Start reading binlog events
   * @return Expected<void, Error> - success or start error
   */
  virtual mygram::utils::Expected<void, mygram::utils::Error> Start() = 0;

  /**
   * @brief Stop reading binlog events.
   *
   * @note SYNCHRONOUS contract: Stop() MUST NOT return until the reader's
   * internal worker thread(s) have fully terminated and are guaranteed to
   * make no further calls into the index/document store. Implementations
   * MUST join their worker thread(s) (or otherwise verify quiescence)
   * before returning.
   *
   * Callers (DumpHandler::DumpSaveWorker, DumpHandler::HandleDumpLoad,
   * SyncOperationManager::BuildSnapshotAsync, SnapshotScheduler::TakeSnapshot)
   * rely on this synchronous contract to safely Clear()/rebuild downstream
   * state immediately after Stop(). A non-synchronous Stop would leave a
   * window where binlog worker threads continue mutating the index and
   * document store while the caller assumes quiescence — a data race that
   * silently corrupts dumps and SYNC rebuilds.
   *
   * @note Stop() MUST be idempotent: calling Stop() on an already-stopped
   * reader is a no-op success.
   */
  virtual void Stop() = 0;

  /**
   * @brief Check if reader is running
   */
  virtual bool IsRunning() const = 0;

  /**
   * @brief Check whether Start() has reserved the lifecycle but has not yet
   * opened the initial binlog stream.
   *
   * The default keeps lightweight implementations source-compatible. Health
   * checks use this state to avoid a transient readiness failure while a
   * synchronous Start() is validating the source server.
   */
  virtual bool IsStarting() const { return false; }

  /**
   * @brief Lifecycle state for diagnostics.
   *
   * IsRunning() retains its thread-liveness contract. Consumers that need to
   * distinguish an operator stop from an error use this three-state view.
   */
  virtual ReplicationState GetReplicationState() const {
    if (IsRunning()) {
      return ReplicationState::kRunning;
    }
    return GetLastError().empty() ? ReplicationState::kStopped : ReplicationState::kFailed;
  }

  /**
   * @brief Get current GTID
   */
  virtual std::string GetCurrentGTID() const = 0;

  /**
   * @brief Set current GTID (used when loading from snapshot)
   * @param gtid GTID to set
   */
  virtual void SetCurrentGTID(const std::string& gtid) = 0;

  /**
   * @brief Get last error message
   *
   * NOTE: This method provides diagnostic error messages for logging purposes.
   * Error propagation for control flow should use Expected<T, Error> (e.g., from
   * Start()). This method is retained because it is widely used in reconnection
   * and thread management logging throughout the binlog reader implementation.
   */
  virtual std::string GetLastError() const = 0;

  /**
   * @brief Get total events processed
   */
  virtual uint64_t GetProcessedEvents() const = 0;

  /**
   * @brief Get queue size
   */
  virtual size_t GetQueueSize() const = 0;

  /** Diagnostics consumed by the TCP, HTTP, and Prometheus surfaces. */
  virtual uint64_t GetCRCErrors() const { return 0; }
  virtual bool HasSchemaIncompatibleError() const { return false; }
  virtual mygram::utils::ErrorCode GetLastErrorCode() const { return mygram::utils::ErrorCode::kSuccess; }

  /** Unix timestamp of the most recent successfully applied position, or 0 when unknown. */
  virtual int64_t GetLastAppliedUnixTime() const { return 0; }

  /** Seconds elapsed since the most recently applied position, or -1 when unknown. */
  virtual int64_t GetSecondsSinceLastApplied() const { return -1; }

  /**
   * @brief Return the UUID of the source server currently validated by this reader.
   *
   * An empty value means that the implementation has not established a source
   * identity yet. This default preserves lightweight test and no-op readers.
   */
  virtual std::string GetSourceServerUUID() const { return {}; }

 protected:
  IBinlogReader() = default;
};

}  // namespace mygramdb::mysql
