/**
 * @file null_binlog_reader.h
 * @brief No-op IBinlogReader implementation for non-MySQL builds
 */

#pragma once

#include "mysql/binlog_reader_interface.h"

namespace mygramdb::mysql {

/**
 * @brief No-op IBinlogReader for builds without MySQL support
 *
 * All methods return safe default values. This eliminates void* casts
 * in the non-MySQL build path.
 */
class NullBinlogReader final : public IBinlogReader {
 public:
  NullBinlogReader() = default;
  ~NullBinlogReader() override = default;

  mygram::utils::Expected<void, mygram::utils::Error> Start() override {
    return mygram::utils::MakeUnexpected(
        mygram::utils::MakeError(mygram::utils::ErrorCode::kInternalError, "MySQL support not compiled"));
  }

  void Stop() override {}
  bool IsRunning() const override { return false; }
  std::string GetCurrentGTID() const override { return ""; }
  void SetCurrentGTID(const std::string& /*gtid*/) override {}
  std::string GetLastError() const override { return "MySQL support not compiled"; }
  uint64_t GetProcessedEvents() const override { return 0; }
  size_t GetQueueSize() const override { return 0; }
};

}  // namespace mygramdb::mysql
