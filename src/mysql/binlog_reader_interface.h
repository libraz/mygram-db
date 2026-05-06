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
   * silently corrupts dumps and SYNC rebuilds (see CR-9 audit, May 2026).
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

 protected:
  IBinlogReader() = default;
};

}  // namespace mygramdb::mysql
