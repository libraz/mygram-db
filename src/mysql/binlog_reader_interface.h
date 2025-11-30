/**
 * @file binlog_reader_interface.h
 * @brief Abstract interface for BinlogReader to enable unit testing
 */

#pragma once

#include <string>

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

  /**
   * @brief Start reading binlog events
   * @return Expected<void, Error> - success or start error
   */
  virtual mygram::utils::Expected<void, mygram::utils::Error> Start() = 0;

  /**
   * @brief Stop reading binlog events
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
   */
  virtual const std::string& GetLastError() const = 0;

  /**
   * @brief Get total events processed
   */
  virtual uint64_t GetProcessedEvents() const = 0;

  /**
   * @brief Get queue size
   */
  virtual size_t GetQueueSize() const = 0;
};

}  // namespace mygramdb::mysql
