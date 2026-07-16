/**
 * @file gtid_waiter.cpp
 * @brief Bounded wait for an applied GTID position.
 */

#include "mysql/gtid_waiter.h"

#include <algorithm>
#include <string>
#include <thread>

#include "mysql/gtid_encoder.h"

namespace mygramdb::mysql {

mygram::utils::Expected<void, mygram::utils::Error> WaitForAppliedPosition(const IBinlogReader& reader,
                                                                           std::string_view target_gtid,
                                                                           std::chrono::milliseconds timeout,
                                                                           std::chrono::milliseconds poll_interval) {
  using mygram::utils::ErrorCode;
  using mygram::utils::MakeError;
  using mygram::utils::MakeUnexpected;

  if (target_gtid.empty()) {
    return {};
  }
  timeout = std::max(timeout, std::chrono::milliseconds::zero());
  poll_interval = std::max(poll_interval, std::chrono::milliseconds(1));
  const auto deadline = std::chrono::steady_clock::now() + timeout;

  while (true) {
    const std::string current = reader.GetCurrentGTID();
    auto covers = GtidEncoder::PositionCoversAuto(target_gtid, current);
    if (!covers) {
      return MakeUnexpected(covers.error());
    }
    if (*covers) {
      return {};
    }
    if (!reader.IsRunning()) {
      return MakeUnexpected(
          MakeError(ErrorCode::kMySQLBinlogError, "Binlog reader stopped before reaching required GTID " +
                                                      std::string(target_gtid) + " (current: " + current + ")"));
    }
    if (std::chrono::steady_clock::now() >= deadline) {
      return MakeUnexpected(
          MakeError(ErrorCode::kMySQLBinlogError, "Timed out waiting for binlog reader to reach required GTID " +
                                                      std::string(target_gtid) + " (current: " + current + ")"));
    }
    std::this_thread::sleep_for(poll_interval);
  }
}

}  // namespace mygramdb::mysql
